namespace Nrk.Oddjob.Core.Queues

open System
open System.Threading.Tasks
open Akka.Event
open Akkling
open FsHttp
open RabbitMQ.Client

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Config

/// Contains infrequently used utilities related to RabbitMQ
module QueueUtils =

    module AcknowledgmentActors =
        /// Spawns actor that acknowledges single queue message.
        /// Message comes from queue A, gets processed, gets sent to queue B, A is ack/nacked.
        let spawnQueueAcknowledger ack reason mailbox =

            let handleAcknowledgment mailbox (message: QueuePublishResult) =
                match message with
                | Result.Ok _ -> sendAck mailbox ack reason
                | Result.Error _ -> sendNack mailbox ack reason
                |> stop

            spawnAnonymous mailbox (propsNamed "queue-acknowledger" (actorOf2 handleAcknowledgment))

        /// Spawns actor that acknowledges single queue message.
        /// Message comes from queue A, gets processed, gets sent to queues (B,C,...), A is ack/nacked.
        let spawnQueueMultiAcknowledger ack numOfQueues mailbox =

            let multicastAcknowledgementActor (mailbox: Actor<_>) =
                let rec loop responses =
                    actor {
                        let! (r: QueuePublishResult) = mailbox.Receive()
                        let responses = r :: responses
                        return!
                            if responses.Length = numOfQueues then
                                if responses |> List.forall Result.isOk then
                                    sendAck mailbox ack "Dispatched multicast messages"
                                else
                                    sendNack mailbox ack "Dispatched multicast messages"
                                stop ()
                            else
                                loop responses
                    }

                loop []

            spawnAnonymous mailbox (propsNamed "queue-multi-acknowledger" multicastAcknowledgementActor)

    module Config =

        open Akka.Event
        open RabbitMQ.Client.Exceptions


        /// Gets the IConnection and IModel objects from underlying RMQ library
        /// The user of this function is responsible for closing the connections
        let getRawApiObjects (queuesConfig: QueuesConfig) (category: QueueCategory) =
            match queuesConfig.Queues |> Seq.tryFind (fun queue -> queue.Category = category.ToString()) with
            | None -> failwithf $"Could not find queue %A{category} in the config"
            | Some queue ->
                match queuesConfig.QueueServers |> Seq.tryFind (fun server -> server.Name = queue.Server) with
                | None -> failwithf $"Could not find server for queue %A{category} in the config"
                | Some server ->
                    let connectionFactory =
                        ConnectionFactory(
                            HostName = server.Hosts.Amqp,
                            VirtualHost = server.VirtualHost,
                            UserName = server.Username,
                            Password = server.Password,
                            Port = server.Port
                        )
                    task {
                        let! connection = connectionFactory.CreateConnectionAsync()
                        let channelCreateOptions = CreateChannelOptions(true, true)
                        let! channel = connection.CreateChannelAsync(channelCreateOptions)
                        return connection, channel
                    }

        let declareQueuesAndExchanges (log: ILoggingAdapter) (oddjobConfig: OddjobConfig) (queuesConfig: QueuesConfig) =

            let createConnections () : Task<(string * IConnection) list> =
                task {
                    let! x =
                        queuesConfig.QueueServers
                        |> Seq.map (fun x ->
                            task {
                                let connectionFactory =
                                    ConnectionFactory(
                                        HostName = x.Hosts.Amqp,
                                        VirtualHost = x.VirtualHost,
                                        UserName = x.Username,
                                        Password = x.Password,
                                        Port = x.Port
                                    )
                                let! connection = connectionFactory.CreateConnectionAsync()
                                return (x.Name, connection)
                            })
                        |> Task.WhenAll
                    return List.ofArray x
                }

            let closeConnections (connections: Task<(string * IConnection) list>) : Task =
                task {
                    let! connections = connections
                    do! connections |> List.map (fun (_, connection: IConnection) -> connection.CloseAsync()) |> Task.WhenAll
                }

            let declareExchanges (connections: Task<(string * IConnection) list>) =
                task {
                    let! connections = connections
                    let! _ =
                        queuesConfig.Exchanges
                        |> Seq.map (fun exchange ->
                            task {
                                let (connection: IConnection) = connections |> List.find (fun (name, _) -> name = exchange.Server) |> snd
                                use! channel = connection.CreateChannelAsync()
                                do! channel.ExchangeDeclareAsync(exchange.Name, exchange.Type.ToLower(), true, false, Map.empty)
                                log.Debug($"[Queues] Declared exchange %s{exchange.Name}")
                                let! _ =
                                    exchange.Siblings
                                    |> function // TODO: Difference in parsing for old and new bootstrapping. IConfigurationManager defaults to null when it finds the definition of an empty array
                                        | null -> [||]
                                        | x -> x
                                    |> Seq.map (fun siblingName ->
                                        task {
                                            do! channel.ExchangeDeclareAsync(siblingName, exchange.Type.ToLower(), true, false, Map.empty)
                                            log.Debug($"[Queues] Declared exchange %s{siblingName}")
                                        })
                                    |> Task.WhenAll
                                return (exchange.Server, connection)
                            })
                        |> Seq.map Async.AwaitTask
                        |> Async.Parallel
                        |> Async.map List.ofArray
                    return connections
                }


            let bindExchanges (connections: Task<(string * IConnection) list>) =
                task {
                    let! connections = connections
                    let! x =
                        queuesConfig.Exchanges
                        |> Seq.map (fun sourceExchange ->
                            task {
                                let connection: IConnection = connections |> List.find (fun (name, _) -> name = sourceExchange.Server) |> snd
                                use! channel = connection.CreateChannelAsync()
                                let! _ =
                                    sourceExchange.BindTo
                                    |> function // TODO: Difference in parsing for old and new bootstrapping. IConfigurationManager defaults to null when it finds the definition of an empty array
                                        | null -> [||]
                                        | x -> x
                                    |> Seq.map (fun destinationExchange ->
                                        task {
                                            do!
                                                channel.ExchangeBindAsync(
                                                    destinationExchange.exchangeName,
                                                    sourceExchange.Name,
                                                    destinationExchange.routingKey,
                                                    Map.empty
                                                )
                                            log.Debug($"[Queues] Bound exchange %s{sourceExchange.Name} to %s{destinationExchange.exchangeName}")
                                        })
                                    |> Task.WhenAll
                                return ()
                            })
                        |> Seq.map Async.AwaitTask
                        |> Async.Parallel
                    return connections
                }

            let declareQueues (connections: Task<(string * IConnection) list>) =

                let bindQueue (channel: IChannel) queueName (bindings: QueuesConfig.QueueBinding array) : Task =
                    if not (String.IsNullOrEmpty(queueName)) then
                        bindings
                        |> Seq.map (fun exchange ->
                            task {
                                do! channel.QueueBindAsync(queueName, exchange.exchangeName, exchange.routingKey)
                                log.Debug($"[Queues] Bound exchange %s{exchange.exchangeName} to queue %s{queueName}")
                            })
                        |> Task.WhenAll
                        :> Task
                    else
                        Task.CompletedTask

                let unbindQueue (channel: IChannel) queueName (bindings: QueuesConfig.QueueBinding array) : Task =
                    if not (String.IsNullOrEmpty(queueName)) then
                        bindings
                        |> Seq.map (fun exchange ->
                            task {
                                do! channel.QueueUnbindAsync(queueName, exchange.exchangeName, exchange.routingKey)
                                log.Debug($"[Queues] Unbound exchange %s{exchange.exchangeName} from queue %s{queueName}")
                            })
                        |> Task.WhenAll
                        :> Task
                    else
                        Task.CompletedTask

                let declareAndBindQueue (channel: IChannel) queueName (bindings: QueuesConfig.QueueBinding array) args : Task =
                    task {
                        if not (String.IsNullOrEmpty(queueName)) then
                            let! _ = channel.QueueDeclareAsync(queueName, true, false, false, args)
                            log.Debug($"[Queues] Declared queue %s{queueName}")
                            do! bindQueue channel queueName bindings
                    }

                let rec moveMessages (channel: IChannel) srcQueueName dstQueueName (bindings: QueuesConfig.QueueBinding array) count : Task<int> =
                    task {
                        match! channel.BasicGetAsync(srcQueueName, false) with
                        | null -> return count
                        | result ->
                            return!
                                task {
                                    let! x =
                                        bindings
                                        |> Seq.map (fun exchange ->
                                            task {
                                                let properties = BasicProperties(result.BasicProperties)
                                                let! _ = channel.BasicPublishAsync(exchange.exchangeName, exchange.routingKey, true, properties, result.Body)
                                                let! _ = channel.BasicAckAsync(result.DeliveryTag, false)
                                                return ()
                                            })
                                        |> Task.WhenAll
                                    return! moveMessages channel srcQueueName dstQueueName bindings (count + 1)
                                }
                    }

                let deleteQueue (channel: IChannel) queueName : Task = channel.QueueDeleteAsync(queueName)

                let migrateQueue (connection: IConnection) queueName bindings args : Task =
                    task {
                        // The original channel wss closed on exception so we must create a new one
                        use! channel = connection.CreateChannelAsync()
                        log.Info($"[Queues] Migrating {queueName}")
                        let backupQueueName = $"{queueName}.backup"
                        do! deleteQueue channel backupQueueName
                        do! declareAndBindQueue channel backupQueueName bindings args
                        do! unbindQueue channel queueName bindings
                        let! msgCount = moveMessages channel queueName backupQueueName bindings 0
                        log.Info($"[Queues] Moved %d{msgCount} message from {queueName} to {backupQueueName}")
                        do! deleteQueue channel queueName
                        // We also need a new channel for a queue with a new type, otherwise RabbitMQ may throw an exception with INTERNAL_ERROR
                        // See https://github.com/rabbitmq/rabbitmq-server/issues/4976
                        use! channel = connection.CreateChannelAsync()
                        do! declareAndBindQueue channel queueName bindings args
                        do! unbindQueue channel backupQueueName bindings
                        let! msgCount = moveMessages channel backupQueueName queueName bindings 0
                        log.Info($"[Queues] Moved %d{msgCount} message from {backupQueueName} to {queueName}")
                    }

                task {
                    let! connections = connections
                    let! x =
                        queuesConfig.Queues
                        |> Seq.map (fun queue ->
                            task {
                                let connection: IConnection = connections |> List.find (fun (name, _) -> name = queue.Server) |> snd
                                use! channel = connection.CreateChannelAsync()
                                let args =
                                    Map.empty
                                    |> fun args ->
                                        if not (String.IsNullOrEmpty queue.Features.DLX) then
                                            args.Add(Headers.XDeadLetterExchange, box queue.Features.DLX)
                                        else
                                            args
                                    |> fun args ->
                                        if queue.Features.TTL > 0 then
                                            args.Add(Headers.XMessageTTL, box queue.Features.TTL)
                                        else
                                            args
                                    |> fun args ->
                                        if not oddjobConfig.Features.UseQuorumQueues && queue.Features.MaxPriority > 0 then
                                            args.Add(Headers.XMaxPriority, box queue.Features.MaxPriority)
                                        else
                                            args
                                    |> fun args ->
                                        if oddjobConfig.Features.UseQuorumQueues then
                                            args.Add(Headers.XQueueType, box "quorum")
                                        else
                                            args

                                try
                                    do! declareAndBindQueue channel queue.QueueName queue.BindFrom args
                                with :? OperationInterruptedException as exn ->
                                    log.Error($"[Queues] Failed to declare queue %s{queue.QueueName} {exn.Message}")
                                    if oddjobConfig.Features.RecreateIncompatibleQueues then
                                        try
                                            do! migrateQueue connection queue.QueueName queue.BindFrom args
                                        with exn ->
                                            log.Error($"[Queues] Failed to recreate queue %s{queue.QueueName} {exn.Message}")
                            })
                        |> Task.WhenAll

                    return connections
                }

            task { do! createConnections () |> declareExchanges |> bindExchanges |> declareQueues |> closeConnections }
            |> Async.AwaitTask
            |> Async.RunSynchronously

    module Metrics =

#if !INTERACTIVE
        open QueueResources
#endif

        type MessageStats =
            {
                Total: int
                WithinInterval: int
                Avg: decimal
                Rate: decimal
                AvgRate: decimal
            }

        type StatsType =
            | DeliverGet
            | Ack
            | Redeliver
            | DeliverNoAck
            | Deliver
            | GetNoAck
            | Get
            | Publish

        type QueueStats =
            {
                ConsumerCount: int
                MessageCount: int
                MessageStats: Map<StatsType, MessageStats>
            }

        [<Literal>]
        let MsgRatesAge = "msg_rates_age"

        [<Literal>]
        let MsgRatesIncr = "msg_rates_incr"

        let getStatsUrl (serverConfig: QueuesConfig.QueueServer) queueName intervalInSeconds =
            $"%s{serverConfig.Hosts.Http}:%d{serverConfig.ManagementPort}/api/queues/%s{Uri.EscapeDataString serverConfig.VirtualHost}/%s{queueName}?%s{MsgRatesAge}=%d{intervalInSeconds}&%s{MsgRatesIncr}=%d{intervalInSeconds}"

        let private getStats (serverConfig: QueuesConfig.QueueServer) queueName intervalInSeconds (logger: ILoggingAdapter) =
            let url = getStatsUrl serverConfig queueName intervalInSeconds
            http {
                GET url
                Authorization(Http.BasicAuthorization(serverConfig.Username, serverConfig.Password))
            }
            |> Request.send
            |> Response.toResult
            |> Result.map (fun res ->
                try
                    res.ToText() |> QueueStats.Parse
                with exn ->
                    logger.Error(exn, $"Unable to parse queuestats: {res.ToText()}")
                    reraise ()

            )
            |> Result.mapError snd

        let getQueueStats serverConfig queueName intervalInSeconds logger =
            getStats serverConfig queueName intervalInSeconds logger
            |> Result.map (fun stats ->
                let deliverGetMessageStats =
                    let details = stats.MessageStats.DeliverGetDetails
                    let hasMultipleSamples = details.Samples.Length > 1
                    {
                        Total = stats.MessageStats.DeliverGet
                        WithinInterval =
                            if hasMultipleSamples then
                                details.Samples[0].Sample - details.Samples[1].Sample
                            else
                                0
                        Avg = if hasMultipleSamples then details.Avg else 0m
                        Rate = details.Rate
                        AvgRate = if hasMultipleSamples then details.AvgRate else 0m
                    }
                let ackMessageStats =
                    let details = stats.MessageStats.AckDetails
                    let hasMultipleSamples = details.Samples.Length > 1
                    {
                        Total = stats.MessageStats.Ack
                        WithinInterval =
                            if hasMultipleSamples then
                                details.Samples[0].Sample - details.Samples[1].Sample
                            else
                                0
                        Avg = if hasMultipleSamples then details.Avg else 0m
                        Rate = details.Rate
                        AvgRate = if hasMultipleSamples then details.AvgRate else 0m
                    }
                let redeliverMessageStats =
                    let details = stats.MessageStats.RedeliverDetails
                    let hasMultipleSamples = details.Samples.Length > 1
                    {
                        Total = stats.MessageStats.Redeliver
                        WithinInterval =
                            if hasMultipleSamples then
                                details.Samples[0].Sample - details.Samples[1].Sample
                            else
                                0
                        Avg = if hasMultipleSamples then details.Avg else 0m
                        Rate = details.Rate
                        AvgRate = if hasMultipleSamples then details.AvgRate else 0m
                    }
                let deliverNoAckMessageStats =
                    let details = stats.MessageStats.DeliverNoAckDetails
                    let hasMultipleSamples = details.Samples.Length > 1
                    {
                        Total = stats.MessageStats.DeliverNoAck
                        WithinInterval =
                            if hasMultipleSamples then
                                details.Samples[0].Sample - details.Samples[1].Sample
                            else
                                0
                        Avg = if hasMultipleSamples then details.Avg else 0m
                        Rate = details.Rate
                        AvgRate = if hasMultipleSamples then details.AvgRate else 0m
                    }
                let deliverMessageStats =
                    let details = stats.MessageStats.DeliverDetails
                    let hasMultipleSamples = details.Samples.Length > 1
                    {
                        Total = stats.MessageStats.Deliver
                        WithinInterval =
                            if hasMultipleSamples then
                                details.Samples[0].Sample - details.Samples[1].Sample
                            else
                                0
                        Avg = if hasMultipleSamples then details.Avg else 0m
                        Rate = details.Rate
                        AvgRate = if hasMultipleSamples then details.AvgRate else 0m
                    }
                let getNoAckMessageStats =
                    let details = stats.MessageStats.GetNoAckDetails
                    let hasMultipleSamples = details.Samples.Length > 1
                    {
                        Total = stats.MessageStats.GetNoAck
                        WithinInterval =
                            if hasMultipleSamples then
                                details.Samples[0].Sample - details.Samples[1].Sample
                            else
                                0
                        Avg = if hasMultipleSamples then details.Avg else 0m
                        Rate = details.Rate
                        AvgRate = if hasMultipleSamples then details.AvgRate else 0m
                    }
                let getMessageStats =
                    let details = stats.MessageStats.GetDetails
                    let hasMultipleSamples = details.Samples.Length > 1
                    {
                        Total = stats.MessageStats.Get
                        WithinInterval =
                            if hasMultipleSamples then
                                details.Samples[0].Sample - details.Samples[1].Sample
                            else
                                0
                        Avg = if hasMultipleSamples then details.Avg else 0m
                        Rate = details.Rate
                        AvgRate = if hasMultipleSamples then details.AvgRate else 0m
                    }
                let publishMessageStats =
                    let details = stats.MessageStats.PublishDetails
                    let hasMultipleSamples = details.Samples.Length > 1
                    {
                        Total = stats.MessageStats.Publish
                        WithinInterval =
                            if hasMultipleSamples then
                                details.Samples[0].Sample - details.Samples[1].Sample
                            else
                                0
                        Avg = if hasMultipleSamples then details.Avg else 0m
                        Rate = details.Rate
                        AvgRate = if hasMultipleSamples then details.AvgRate else 0m
                    }
                {
                    ConsumerCount = stats.Consumers
                    MessageCount = stats.Messages
                    MessageStats =
                        Map.ofList
                            [
                                (StatsType.DeliverGet, deliverGetMessageStats)
                                (StatsType.Ack, ackMessageStats)
                                (StatsType.Redeliver, redeliverMessageStats)
                                (StatsType.DeliverNoAck, deliverNoAckMessageStats)
                                (StatsType.Deliver, deliverMessageStats)
                                (StatsType.GetNoAck, getNoAckMessageStats)
                                (StatsType.Get, getMessageStats)
                                (StatsType.Publish, publishMessageStats)
                            ]
                })
