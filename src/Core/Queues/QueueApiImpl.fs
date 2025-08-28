namespace Nrk.Oddjob.Core.Queues

/// Contains IRabbitMqApi implementation based on QueueActors.
module QueueApiImpl =

    open System
    open System.Collections.Concurrent
    open Akkling

    open Nrk.Oddjob.Core
    open QueueActors
    open Config
    open QueueTypes
    open QueueTypes.RmqSettings

    type internal QueueOrExchangeCategory = string

    module QueueName =
        let get (config: QueuesConfig) (category: QueueCategory) =
            config.Queues
            |> Seq.tryFind (fun x -> x.Category = category.ToString())
            |> Option.map (fun details -> QueueName.create details.QueueName)
            |> Option.getOrFail $"Could not get queue name from config (category %A{category})"

    module ExchangeName =
        let get (config: QueuesConfig) (category: ExchangeCategory) =
            config.Queues
            |> Seq.tryFind (fun x -> x.Category = category.ToString())
            |> Option.bind (fun details -> details.BindFrom |> Seq.tryHead)
            |> Option.map (fun exDetails -> ExchangeName.create exDetails.exchangeName)
            |> Option.getOrFail $"Could not get exchange name from config (category %A{category})"

    /// Encapsulates all QueueActors and provides a simple way to get access to exchanges and queues
    type OddjobRabbitMqApi(oddjobConfig: OddjobConfig, queueConfig: QueuesConfig, activityContext, ?limits: QueueApiLimits) =
        let limits = limits |> Option.defaultValue QueueApiLimits.Default
        let queueFactories = ConcurrentDictionary<string, IActorRef<FactoryCommand>>()

        let verifyRetryPatternIsUsed (main: QueueName) =
            let retryRelatedQueueNames =
                [ QueueName.withRetries; QueueName.withErrors; QueueName.withRejected ] |> List.map (fun f -> f main)
            let missingQueues =
                retryRelatedQueueNames
                |> List.filter (fun name -> queueConfig.Queues |> Seq.notExists (fun x -> x.QueueName = QueueName.value name))
            if missingQueues |> Seq.isNotEmpty then
                failwithf $"Queue configuration is missing one of retries-errors-rejected for %A{main}"

        let spawnQueueFactory system (queueServer: QueuesConfig.QueueServer) =
            let connection =
                {
                    Hostname = queueServer.Hosts.Amqp
                    VirtualHost = queueServer.VirtualHost
                    Username = queueServer.Username
                    Password = queueServer.Password
                    Port = queueServer.Port
                }
            let actorName = makeActorName [ "queues"; connection.Hostname; connection.Username ]
            spawn system actorName (propsNamed "queue-factory" (queueFactoryActor connection limits.ChannelErrorRecoveryLimits activityContext))

        let getOrCreateQueueFactory system category =
            let queueServer =
                let serverName = queueConfig.Queues |> Seq.find (fun details -> details.Category = category) |> _.Server
                queueConfig.QueueServers |> Seq.find (fun server -> server.Name = serverName)
            lock queueFactories (fun _ -> queueFactories.GetOrAdd(queueServer.Name, (fun _ -> spawnQueueFactory system queueServer)))

        interface IRabbitMqApi with

            member _.AttachConsumer(system, category, msgHandler: IActorRef<QueueMessage<'payload>>, ?deserialize) =
                let factory = getOrCreateQueueFactory system (getUnionCaseName category)
                let mainQueue = QueueName.get queueConfig category
                verifyRetryPatternIsUsed mainQueue

                let readerName = makeActorName [ category.ToString(); "reader" ]
                let readerProps =
                    {
                        MessageExpiryTime = Some <| TimeSpan.FromMinutes(float oddjobConfig.Limits.QueueMessageExpiryTimeInMinutes)
                        MessageExpiryCheckInterval = TimeSpan.FromMinutes(float oddjobConfig.Limits.QueueMessageExpiryCheckEveryMinutes)
                        MessageExpiryCheckStartDelay = TimeSpan.FromMinutes 1.0
                    }
                let deserialize =
                    defaultArg deserialize (Serialization.Newtonsoft.deserializeObject<'payload> SerializationSettings.PascalCase)
                let queueReader =
                    spawn system readerName <| propsNamed "queue-reader" (queueReaderActor<'payload> readerProps deserialize msgHandler)

                let queueToExchange queueName =
                    QueueName.value queueName |> ExchangeName.create
                let consumerSettings =
                    { Consumer.Settings.Default with
                        Prefetch =
                            { Consumer.Prefetch.Default with
                                CountPerConsumer = uint16 oddjobConfig.Limits.QueuePrefetchCount
                            }
                        QueueConfiguration =
                            Consumer.QueueConfiguration.RetryErrorRejected
                                {
                                    RetriesExchangeName = QueueName.withRetries mainQueue |> queueToExchange
                                    ErrorsExchangeName = QueueName.withErrors mainQueue |> queueToExchange
                                    Limits = limits.RetryErrorLimits
                                    RejectedExchangeName = QueueName.withRejected mainQueue |> queueToExchange
                                }
                    }
                let consumerName = makeActorName [ category.ToString(); "consumer" ]
                // The QueueConsumerCommand is ignored because we don't need to send it anything in particular.
                // It will start listening to queue and push incoming messages to the "reader" actor, which will
                // deserialize them and push to "msgHandler" actor wrapped in a Message<> type.
                let (_: IActorRef<QueueConsumerCommand>) =
                    factory
                    <<? GetOrCreateConsumer
                            {
                                ActorName = consumerName
                                QueueName = mainQueue
                                ConsumerSettings = consumerSettings
                                Subscriber = queueReader
                            }
                ()

            // This function returns a publisher actor lazily, so can be called multiple times (reference to same actor
            // will be returned)
            member _.GetPublisher(system, category) =
                let factory = getOrCreateQueueFactory system (getUnionCaseName category)
                let mainName = ExchangeName.get queueConfig category
                let actorName = makeActorName [ ExchangeName.value mainName; "publisher" ]
                let (result: IActorRef<QueuePublisherCommand>) =
                    factory
                    <<? GetOrCreatePublisher
                            {
                                ActorName = actorName
                                ExchangeName = mainName
                            }
                result

    /// Encapsulates all QueueActors and provides a simple way to get access to exchanges and queues
    type OddjobRabbitMqConsumer(queueFactory: IActorRef<FactoryCommand>) =

        interface IQueueConsumerApi with

            member _.AttachConsumer(system, queueName, msgHandler: IActorRef<QueueMessage<'payload>>, ?deserialize) =
                let readerName = makeActorName [ queueName; "reader" ]
                let readerProps =
                    {
                        MessageExpiryTime = None
                        MessageExpiryCheckInterval = TimeSpan.FromMinutes 1.0
                        MessageExpiryCheckStartDelay = TimeSpan.FromMinutes 1.0
                    }
                let deserialize =
                    defaultArg deserialize (Serialization.Newtonsoft.deserializeObject<'payload> SerializationSettings.PascalCase)
                let queueReader =
                    spawn system readerName <| propsNamed "queue-reader" (queueReaderActor<'payload> readerProps deserialize msgHandler)

                let consumerSettings =
                    { Consumer.Settings.Default with
                        QueueConfiguration = Consumer.QueueConfiguration.JustQueue
                    }
                let consumerName = makeActorName [ queueName; "consumer" ]
                // The QueueConsumerCommand is ignored because we don't need to send it anything in particular.
                // It will start listening to queue and push incoming messages to the "reader" actor, which will
                // deserialize them and push to "msgHandler" actor wrapped in a Message<> type.
                let (_: IActorRef<QueueConsumerCommand>) =
                    queueFactory
                    <<? GetOrCreateConsumer
                            {
                                ActorName = consumerName
                                QueueName = QueueName.create queueName
                                ConsumerSettings = consumerSettings
                                Subscriber = queueReader
                            }
                ()
