namespace Nrk.Oddjob.Core.Queues

open System.Diagnostics

module ServiceBusAdapter =

    open System
    open System.Text.Json
    open System.Threading.Tasks
    open Akkling
    open Azure.Messaging.ServiceBus
    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Queues

    type MessageProcessor<'a> =
        | Forward of IActorRef<QueueMessage<'a>>
        | Skip

    type ServiceBusQueueConfig =
        {
            QueueName: string
            MaxRetries: int
            NextTimeout: int -> TimeSpan
        }

    let private serializeMessage<'a> (payload: 'a) =
        JsonSerializer.SerializeToUtf8Bytes(payload, JsonSerializerOptions(PropertyNamingPolicy = JsonNamingPolicy.CamelCase))

    let private deserializeMessage<'a> (payload: BinaryData) =
        JsonSerializer.Deserialize<'a>(payload, JsonSerializerOptions(PropertyNameCaseInsensitive = true))

    let serviceBusAdapterActor (messageProcessor: MessageProcessor<'a>) (mailbox: Actor<_>) =

        let handleLifecycle mailbox e =
            match e with
            | PreRestart(exn, message) ->
                logErrorWithExn mailbox exn $"PreRestart due to error when processing message {message.GetType()} [%A{message}]"
                match message with
                | :? ProcessMessageEventArgs as msg ->
                    logErrorWithExn mailbox exn $"Rejecting invalid message {msg.Message.MessageId}"
                    msg.DeadLetterMessageAsync(msg.Message) |> Async.AwaitTask |> Async.RunSynchronously
                    ignored ()
                | _ -> unhandled ()
            | _ -> ignored ()

        let rec idle messages =
            logDebug mailbox $"idle ({Map.count messages} pending messages)"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? ProcessMessageEventArgs as msg ->
                        let tag = uint64 msg.Message.SequenceNumber
                        match messageProcessor with
                        | Forward forwardTo ->
                            let ack =
                                {
                                    AckActor = mailbox.Self |> retype
                                    Tag =
                                        {
                                            AckId = AckId tag
                                            Redelivered = false
                                            Priority = byte 1
                                            ForwardedFrom = None
                                        }
                                    Timestamp = DateTimeOffset.Now
                                }
                            try
                                let queueMessage =
                                    {
                                        Payload = deserializeMessage<'a> msg.Message.Body
                                        Ack = Some ack
                                    }
                                logDebug mailbox $"Forwarded #{tag}: {msg.Message.MessageId}"
                                forwardTo <! queueMessage
                                idle (messages |> Map.add tag msg)
                            with exn ->
                                logErrorWithExn mailbox exn $"Failed to deserialize #{tag}: {msg.Message.MessageId}"
                                msg.DeadLetterMessageAsync(msg.Message) |> Async.AwaitTask |> Async.RunSynchronously
                                idle messages
                        | Skip ->
                            logDebug mailbox $"Skipped #{tag}: {msg.Message.MessageId}"
                            msg.CompleteMessageAsync(msg.Message) |> Async.AwaitTask |> Async.RunSynchronously
                            idle messages
                    | :? ProcessErrorEventArgs as msg ->
                        logErrorWithExn mailbox msg.Exception "Error in ServiceBus adapter"
                        ignored ()
                    | :? AcknowledgementCommand as ack ->
                        let (AckId tag) = ack.Tag.AckId
                        match messages |> Map.tryFind tag with
                        | Some msg ->
                            logInfo mailbox $"Acknowledging #{tag} [{ack.AckType}]"
                            match ack.AckType with
                            | Ack -> msg.CompleteMessageAsync(msg.Message)
                            | Nack -> msg.DeadLetterMessageAsync(msg.Message)
                            | Reject -> msg.DeadLetterMessageAsync(msg.Message)
                            |> Async.AwaitTask
                            |> Async.RunSynchronously
                            idle (messages |> Map.remove tag)
                        | None -> ignored ()
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | _ -> unhandled ()
            }
        idle Map.empty

    let serviceBusDeadLetterActor (config: ServiceBusQueueConfig) (sender: ServiceBusSender) (mailbox: Actor<_>) =
        let rec loop () =
            actor {
                let! (msg: obj) = mailbox.Receive()
                return!
                    match msg with
                    | :? ProcessMessageEventArgs as msg ->
                        let retriesCount =
                            if msg.Message.ApplicationProperties.ContainsKey RetriesCountHeader then
                                unbox msg.Message.ApplicationProperties[RetriesCountHeader]
                            else
                                0
                        task {
                            if retriesCount < config.MaxRetries then
                                let retriesCount = retriesCount + 1
                                logDebug mailbox $"Retry #{retriesCount}"
                                let interval = config.NextTimeout retriesCount
                                let message = ServiceBusMessage(Body = msg.Message.Body)
                                message.ApplicationProperties.Add(RetriesCountHeader, retriesCount)
                                let! _ = sender.ScheduleMessageAsync(message, DateTimeOffset.Now + interval)
                                ()
                            do! msg.CompleteMessageAsync(msg.Message)
                        }
                        |> Async.AwaitTask
                        |> Async.RunSynchronously
                        ignored ()
                    | _ -> unhandled ()
            }
        loop ()

    let private processHandler (adapter: IActorRef<obj>) (args: ProcessMessageEventArgs) : Task =
        adapter <! args
        Task.CompletedTask

    let private errorHandler (adapter: IActorRef<obj>) (args: ProcessErrorEventArgs) : Task =
        adapter <! args
        Task.CompletedTask

    let start (serviceBusProcessor: ServiceBusProcessor) messageAdapter =
        serviceBusProcessor.add_ProcessMessageAsync <| processHandler messageAdapter
        serviceBusProcessor.add_ProcessErrorAsync <| errorHandler messageAdapter
        serviceBusProcessor.StartProcessingAsync() |> Async.AwaitTask |> Async.RunSynchronously

    let stop (serviceBusProcessor: ServiceBusProcessor) =
        serviceBusProcessor.StopProcessingAsync() |> Async.AwaitTask |> Async.RunSynchronously

    let stopAll processors =
        processors
        |> Seq.map (fun (x: ServiceBusProcessor) -> x.StopProcessingAsync() |> Async.AwaitTask)
        |> Async.Parallel
        |> Async.RunSynchronously
        |> ignore

    type ServiceBusMessageProcessor(serviceBusClient: ServiceBusClient, system, config: ServiceBusQueueConfig) =
        let messageProcessor =
            serviceBusClient.CreateProcessor(config.QueueName, ServiceBusProcessorOptions(AutoCompleteMessages = false))
        let deadLetterProcessor =
            serviceBusClient.CreateProcessor(config.QueueName, ServiceBusProcessorOptions(SubQueue = SubQueue.DeadLetter, AutoCompleteMessages = false))
        let sender = serviceBusClient.CreateSender config.QueueName
        let deadLetterHandler =
            spawn system $"{config.QueueName}-deadletter"
            <| propsNamed "servicebus-deadletter" (serviceBusDeadLetterActor config sender)

        member _.Start(messageAdapter) =
            start messageProcessor messageAdapter
            start deadLetterProcessor deadLetterHandler

        member _.Stop() =
            stop messageProcessor
            stop deadLetterProcessor
