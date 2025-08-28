namespace Nrk.Oddjob.Core.Queues

open System
open System.Text
open System.Threading
open System.Threading.Tasks
open Akkling
open RabbitMQ.Client.Events

open Nrk.Oddjob.Core

module QueueTypes =

    /// A name of a queue in RabbitMQ
    type QueueName = private QueueName of string

    [<RequireQualifiedAccess>]
    module QueueName =
        let create name =
            if String.isNullOrEmpty name then
                invalidArg "name" "Queue name cannot be empty"
            QueueName name

        let value (QueueName str) = str

        let withRetries name =
            QueueName <| sprintf "%s.%s" (value name) "retries"

        let withRejected name =
            QueueName <| sprintf "%s.%s" (value name) "rejected"

        let withErrors name =
            QueueName <| sprintf "%s.%s" (value name) "errors"

    type ExchangeName = private ExchangeName of string

    [<RequireQualifiedAccess>]
    module ExchangeName =
        let create name =
            if String.isNullOrEmpty name then
                invalidArg "name" "Exchange name cannot be empty"
            ExchangeName name

        let value (ExchangeName str) = str

    module RmqSettings =

        type ConnectionDetails =
            {
                Hostname: string
                VirtualHost: string
                Username: string
                Password: string
                Port: int
            }

        module Consumer =

            type Prefetch =
                {
                    CountPerChannel: uint16
                    CountPerConsumer: uint16
                }

                static member Default =
                    {
                        CountPerChannel = 0us
                        CountPerConsumer = 0us
                    }

            type Acknowledge =
                | ConsumeWithAck
                | ConsumeWithoutAck

            type RetryErrorRejectedConfig =
                {
                    RetriesExchangeName: ExchangeName
                    ErrorsExchangeName: ExchangeName
                    RejectedExchangeName: ExchangeName
                    Limits: RetryErrorLimits
                }

            type QueueConfiguration =
                | JustQueue
                /// Creates a set of queues realizing the "MainQueue - RetriesQueue - ErrorsQueue - RejectedQueue" policy.
                /// Nacked messages from Queue are moved to RetriesQueue on redelivery.
                /// Messages from RetriesQueue return to Queue after some delay (via DeadLetterExchange).
                /// After several unsuccesfull retries, Nacked messages are moved to ErrorsQueue on redelivery.
                /// Messages from ErrorsQueue return to Queue after some delay (via DeadLetterExchange).
                /// When message is rejected, it gets moved to Rejected queue (via DeadLetterExchange).
                | RetryErrorRejected of config: RetryErrorRejectedConfig

            type Settings =
                {
                    Acknowledge: Acknowledge
                    Prefetch: Prefetch
                    QueueConfiguration: QueueConfiguration
                }

                static member Default =
                    {
                        Acknowledge = Acknowledge.ConsumeWithAck
                        Prefetch = Prefetch.Default
                        QueueConfiguration = QueueConfiguration.JustQueue
                    }

    type QueueExchange =
        | Queue of name: QueueName * prefetch: RmqSettings.Consumer.Prefetch
        | Exchange of name: ExchangeName

    type SubscriberMessage =
        | StartWith of queueConsumer: IActorRef<QueueConsumerCommand>
        | Stop
        | IncomingMessage of payload: string * tag: MessageTag option

module QueueActors =

    open System.Collections.Generic
    open Akka.Actor
    open RabbitMQ.Client
    open RabbitMQ.Client.Exceptions

    open QueueTypes
    open RmqSettings.Consumer

    [<NoComparison>]
    type private RmqConnection =
        {
            Connection: IConnection
            Channel: IChannel

        }

        interface IDisposable with
            member this.Dispose() =
                this.Connection.Dispose()
                this.Channel.Dispose()

    let private createMessageProperties priority headers =
        let msgProperties = BasicProperties()
        msgProperties.Persistent <- true
        if priority > 0uy then
            msgProperties.Priority <- priority
        if headers <> null && Seq.isNotEmpty headers then
            msgProperties.Headers <- headers
        msgProperties

    type private DeliveryDetails =
        {
            DeliveryTag: uint64
            Redelivered: bool
            BasicProperties: IReadOnlyBasicProperties
            Body: byte array
        }

    type private CancellationDetails = ShutdownEventArgs

    [<NoComparison>]
    type private RmqActorConsumerCommand =
        | Delivered of DeliveryDetails
        | Canceled of CancellationDetails

    type private RmqActorConsumer(queueName, model: IChannel, target: IActorRef<RmqActorConsumerCommand>, activityContext: ActivitySourceContext) =

        inherit AsyncDefaultBasicConsumer(model)

        member _.HandleBasicDeliverAsync'
            (
                consumerTag: string,
                deliveryTag: uint64,
                redelivered: bool,
                exchange: string,
                routingKey: string,
                properties: IReadOnlyBasicProperties,
                body: ReadOnlyMemory<byte>
            ) : Task =
            base.HandleBasicDeliverAsync(consumerTag, deliveryTag, redelivered, exchange, routingKey, properties, body)

        override this.HandleBasicDeliverAsync(consumerTag, deliveryTag, redelivered, exchange, routingKey, properties, body, cancellationToken) : Task =
            task {
                let extractTraceContext (properties: IReadOnlyBasicProperties) (key: string) =
                    try
                        let result, value = properties.Headers.TryGetValue key
                        if result then
                            seq { Encoding.UTF8.GetString(value :?> byte[]) }
                        else
                            Seq.empty
                    with _ ->
                        Seq.empty

                do! this.HandleBasicDeliverAsync'(consumerTag, deliveryTag, redelivered, exchange, routingKey, properties, body)
                let args = BasicDeliverEventArgs(consumerTag, deliveryTag, redelivered, exchange, routingKey, properties, body)

                target
                <! Delivered
                    {
                        DeliveryTag = args.DeliveryTag
                        Redelivered = args.Redelivered
                        BasicProperties = args.BasicProperties
                        Body = body.ToArray()
                    }
            }

        member _.OnCancelAsync' consumerTags cancellationToken =
            base.OnCancelAsync(consumerTags, cancellationToken)

        override this.OnCancelAsync([<ParamArray>] consumerTags, cancellationToken) : Task =
            task {
                do! this.OnCancelAsync' consumerTags cancellationToken
                target <! Canceled this.ShutdownReason
            }


    type private ConsecutiveChannelErrors =
        {
            Count: int
            FirstOccurence: DateTime
        }

        static member Increment(err: ConsecutiveChannelErrors option) =
            match err with
            | Some err -> { err with Count = err.Count + 1 }
            | None ->
                {
                    Count = 1
                    FirstOccurence = DateTime.Now
                }

    type private ChannelErrorRecoverability =
        | Recoverable
        | NonRecoverable


    let private isChannelErrorRecoverable (closeReason: CancellationDetails) (errors: ConsecutiveChannelErrors) (limits: ChannelErrorRecoveryLimits) =
        let isNonrecoverableFromErrors =
            let timeSinceFirstError = DateTime.Now - errors.FirstOccurence
            timeSinceFirstError > limits.TotalTimeout
            || timeSinceFirstError > fst limits.MixedTimeoutCount && errors.Count > snd limits.MixedTimeoutCount
            || errors.Count > limits.TotalCount
        if isNonrecoverableFromErrors then
            NonRecoverable
        elif isNull closeReason then
            Recoverable
        elif closeReason.ReplyText.StartsWith "PRECONDITION_FAILED" then
            NonRecoverable
        else
            Recoverable

    let private queuePublisherOrConsumerRestartDelay = TimeSpan.FromSeconds 5.

    let private logChannelError mailbox (ex: Exception) (errors: ConsecutiveChannelErrors) =
        let message =
            match ex with
            | :? System.IO.IOException -> $"Channel error: IOException. [{errors}]."
            | :? AlreadyClosedException as ex -> $"Channel error: AlreadyClosedException [{errors}]. Reason {ex.ShutdownReason}"
            | :? OperationInterruptedException as ex -> $"Channel error: OperationInterruptedException [{errors}]. Reason {ex.ShutdownReason}"
            | ex -> $"Channel error, [{errors}]. {ex.Message}"
        logErrorWithExn mailbox ex message

    /// Sends messages and sets routing keys in RabbitMQ
    let queuePublisherActor
        (connectionFactory: ConnectionFactory)
        (exchangeName: ExchangeName)
        (limits: ChannelErrorRecoveryLimits)
        (activityContext: ActivitySourceContext)
        (mailbox: Actor<obj>)
        =

        let emptyHeaders = Dictionary<string, obj>()

        let injectTraceContext =
            Action<BasicProperties, string, string>(fun (properties: BasicProperties) (key: string) (value: string) ->
                if properties.Headers = null then
                    properties.Headers <- emptyHeaders
                properties.Headers.TryAdd(key, value) |> ignore)

        let publishMessage (message: OutgoingMessage) (channel: IChannel) : Task =
            let msgProperties = createMessageProperties message.Priority emptyHeaders
            let msgBody = Encoding.UTF8.GetBytes(message.Payload)

            channel
                .BasicPublishAsync(
                    ExchangeName.value exchangeName,
                    message.RoutingKey,
                    true,
                    msgProperties,
                    ReadOnlyMemory(msgBody),
                    (new CancellationTokenSource(TimeSpan.FromSeconds 10.)).Token
                )
                .AsTask()

        let publishMessages (messages: OutgoingMessage list) context (channel: IChannel) (acknowledge: IActorRef<QueuePublishResult> option) : Task =
            let acknowledgeResult result =
                acknowledge |> Option.iter (fun acknowledge -> acknowledge <! result)
            task {
                try
                    for msg in messages do
                        do! publishMessage msg channel
                    acknowledgeResult <| QueuePublishResult.Ok context
                with exn ->
                    acknowledgeResult <| QueuePublishResult.Error(exn, context)
                    raise exn
            }


        let rec initialize () =
            let createAsyncEventHandler callback =
                AsyncEventHandler(fun _ args -> task { callback args })

            task {
                let! connection = connectionFactory.CreateConnectionAsync()

                connection.add_CallbackExceptionAsync (
                    createAsyncEventHandler (fun args -> logErrorWithExn mailbox args.Exception "Connection Callback Exception")
                )
                connection.add_ConnectionBlockedAsync (createAsyncEventHandler (fun args -> logDebug mailbox $"Connection Blocked due to {args.Reason}"))
                connection.add_ConnectionUnblockedAsync (createAsyncEventHandler (fun _ -> logDebug mailbox "Connection Unblocked"))
                connection.add_ConnectionShutdownAsync (createAsyncEventHandler (fun args -> logDebug mailbox $"Connection Shutdown due to {args.Cause}"))
                logDebug mailbox "Connection created"

                let channelCreateOptions = CreateChannelOptions(true, true)
                let! channel = connection.CreateChannelAsync(channelCreateOptions)

                //TODO: channel.add_BasicRecoverOk (fun _ -> logDebug mailbox "Channel Basic Recover OK")
                channel.add_CallbackExceptionAsync (createAsyncEventHandler (fun args -> logErrorWithExn mailbox args.Exception "Channel Callback Exception"))
                channel.add_ChannelShutdownAsync (createAsyncEventHandler (fun _ -> logDebug mailbox "Channel Model Shutdown"))

                logDebug mailbox "Channel created"

                return
                    {
                        Connection = connection
                        Channel = channel
                    }
            }
            |> Async.AwaitTask
            |!> mailbox.Self.Retype()

            awaitInitialize ()
        and awaitInitialize () =
            actor {
                let! msg = mailbox.Receive()
                return!
                    match msg with
                    | :? RmqConnection as connection -> operate connection None
                    | :? Status.Failure as exn ->
                        logErrorWithExn mailbox exn.Cause "Initialization failed"
                        waitForRestart exn.Cause None
                    | LifecycleEvent _ -> ignored ()
                    | _ ->
                        logDebug mailbox "stashing message"
                        mailbox.Stash()
                        ignored ()
            }
        and operate (conn: RmqConnection) (errors: ConsecutiveChannelErrors option) =
            mailbox.UnstashAll()
            actor {
                let! msg = mailbox.Receive()
                return!
                    match msg with
                    | :? QueuePublisherCommand as msg ->
                        logDebug mailbox $"Publishing [{msg}]"
                        match msg with
                        | PublishWithoutConfirmation msg -> publishMessages [ msg ] () conn.Channel None
                        | PublishOne(msg, context, acknowledge) -> publishMessages [ msg ] context conn.Channel (Some acknowledge)
                        | PublishMany(msgs, context, acknowledge) -> publishMessages msgs context conn.Channel (Some acknowledge)
                        |> Async.AwaitTask
                        |> Async.map (fun _ -> AsyncResponse.Success)
                        |!> mailbox.Self.Retype()

                        awaitPublish conn errors
                    | LifecycleEvent e ->
                        match e with
                        | PostStop -> (conn :> IDisposable).Dispose()
                        | _ -> ()
                        ignored ()
                    | _ -> unhandled ()
            }
        and awaitPublish (conn: RmqConnection) (errors: ConsecutiveChannelErrors option) =
            actor {
                let! msg = mailbox.Receive()

                return!
                    match msg with
                    | :? AsyncResponse as res ->
                        match res with
                        | Success -> operate conn None
                    | :? Status.Failure as exn ->
                        let errors = ConsecutiveChannelErrors.Increment errors
                        logChannelError mailbox exn.Cause errors
                        match isChannelErrorRecoverable conn.Channel.CloseReason errors limits with
                        | Recoverable -> operate conn (Some errors)
                        | NonRecoverable -> waitForRestart exn.Cause (Some conn)
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and waitForRestart (ex: exn) (conn: RmqConnection option) =
            mailbox.UnstashAll()
            actor {
                logErrorWithExn mailbox ex "Waiting for restart due to exception"
                let waitMsg = "wait"
                mailbox.Schedule queuePublisherOrConsumerRestartDelay (retype mailbox.Self) waitMsg |> ignore
                let! msg = mailbox.Receive()
                return!
                    match msg with
                    | :? string as str when str = waitMsg -> raise ex // Bubbles up exception to the supervisor actor
                    | LifecycleEvent e ->
                        match e with
                        | PostStop -> conn |> Option.iter (fun conn -> (conn :> IDisposable).Dispose())
                        | _ -> ()
                        ignored ()
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }

        initialize ()

    /// Consumes messages from RabbitMQ queue and forwards them to provided subscriber. It registers itself in subscriber
    /// for receiving acknowledgments and then forwards queue messages to subscriber.
    /// This actor is created by queueFactoryActor
    let queueConsumerActor
        (connectionFactory: ConnectionFactory)
        (queueName: QueueName)
        (consumerSettings: Settings)
        (limits: ChannelErrorRecoveryLimits)
        (subscriber: IActorRef<SubscriberMessage>)
        (activityContext: ActivitySourceContext)
        (mailbox: Actor<_>)
        =

        let getPriority (e: DeliveryDetails) =
            if e.BasicProperties.IsPriorityPresent() then
                e.BasicProperties.Priority
            else
                0uy

        let tryFindHeader (headers: obj) headerName =
            if headers :? List<obj> then
                match headers :?> List<obj> |> Seq.tryHead with
                | Some elem when (elem :? Dictionary<string, obj>) -> elem :?> Dictionary<string, obj> |> Seq.tryFind (fun x -> x.Key = headerName)
                | _ -> None
            else
                None

        /// x-death header is described here https://www.rabbitmq.com/dlx.html
        /// It is a LIST of entries, and the enries keep information about both count AND the queue on which the dead-lettering occured
        /// This means that the header will count separately dead-letterings for retry and error queues
        /// This function returns the value for the first entry, which corresponds to the last used queue
        /// (the queue from which the message was shoveled most recently)
        let getXDeathCountForLastQueueMessageWasDLXedFrom (e: DeliveryDetails) =
            if e.BasicProperties.Headers <> null then
                e.BasicProperties.Headers
                |> Seq.tryFind (fun x -> x.Key = "x-death")
                |> Option.map (fun header -> tryFindHeader header.Value "count" |> Option.map (fun kv -> (kv.Value :?> int64) |> int) |> Option.defaultValue 0)
                |> Option.defaultValue 0
            else
                0

        let getHeaderNumValue (e: DeliveryDetails) headerName =
            if e.BasicProperties.Headers <> null then
                e.BasicProperties.Headers
                |> Seq.tryFind (fun x -> x.Key = headerName)
                |> Option.map (_.Value >> unbox)
                |> Option.defaultValue 0
            else
                0

        let getShovelSource (e: DeliveryDetails) =
            let removePrefix (s: string) =
                if s.StartsWith("amqp://") then
                    s.Substring("amqp://".Length)
                else
                    s
            let bytesToString (bytes: obj) =
                Encoding.Default.GetString(bytes :?> byte[])
            if e.BasicProperties.Headers <> null then
                match e.BasicProperties.Headers |> Seq.tryFind (fun x -> x.Key = "x-shovelled") with
                | Some header ->
                    match
                        tryFindHeader header.Value "src-uri",
                        (tryFindHeader header.Value "src-queue" |> Option.orElse (tryFindHeader header.Value "src-exchange"))
                    with
                    | Some kv1, Some kv2 -> ((bytesToString kv2.Value) + "@" + (bytesToString kv1.Value |> removePrefix)) |> Some
                    | _, _ -> None
                | _ -> None
            else
                None

        let (|ProcessNormally|MoveToOtherQueue|RejectMessage|) (e: DeliveryDetails) =
            match consumerSettings.QueueConfiguration with
            | JustQueue -> ProcessNormally
            | RetryErrorRejected cfg ->
                if not e.Redelivered then
                    // either new message, or just was shoveled back to main queue from Retries or Errors
                    ProcessNormally
                else
                    // Because we don't select here count separately for retries and errors, the n (look below) will hold a value for
                    // the queue that the message was in most recently.

                    // The table shows how the algorithm determines where to move nacked (redelivered) messsage.
                    // cfg.``Number of times message is moved to retries queue, before it gets moved to errors queue (during the first cycle of retries/errors moves)`` is set to 3.
                    // The [number] means that this is the value returned by ``get XDeath count for the last queue the message was shoveled from``, i.e. it is value of n.
                    // Scenario: Message was Nacked and redelivered to main queue.

                    //     "dm-retries-count"            "dm-errors-count"   "decision (move to queue...)"      "comment"
                    //             [0]                            0	                    retries                 no headers available
                    //             [1]                            0	                    retries
                    //             [2]                            0	                    retries
                    //             [3]                            0                     errors                  retries count big enough, move to errors
                    //              3                            [1]                    errors
                    //              3                            [2]                    errors
                    //              3                            [3]                    errors
                    //                                                                                          move to errors until eventually reject

                    match getHeaderNumValue e RetriesCountHeader, getHeaderNumValue e ErrorsCountHeader with
                    | n1, n2 when n1 < cfg.Limits.NumberOfMovesToRetriesQueueBeforeFirstMoveToErrorsQueue ->
                        MoveToOtherQueue(cfg.RetriesExchangeName, n1 + n2, RetriesCountHeader)
                    | n1, n2 when n2 < cfg.Limits.NumberOfMovesToErrorsQueueBeforeMoveToRejectedQueue ->
                        MoveToOtherQueue(cfg.ErrorsExchangeName, n1 + n2, ErrorsCountHeader)
                    | n1, n2 -> RejectMessage(n1 + n2)

        let receiveMessage (channel: IChannel) (e: DeliveryDetails) =
            task {
                match e with
                | ProcessNormally ->
                    let message = Encoding.UTF8.GetString(e.Body)
                    let tag =
                        match consumerSettings.Acknowledge with
                        | ConsumeWithAck ->
                            Some
                                {
                                    AckId = AckId e.DeliveryTag
                                    Redelivered = e.Redelivered
                                    Priority = getPriority e
                                    ForwardedFrom = getShovelSource e
                                }
                        | ConsumeWithoutAck -> None
                    subscriber <! IncomingMessage(message, tag)
                | MoveToOtherQueue(exchangeName, retriesCount, headerName) ->
                    logDebug mailbox $"Message with tag %d{e.DeliveryTag} has retry count of %d{retriesCount} and is moved to {exchangeName} exchange"
                    let headers = e.BasicProperties.Headers
                    // Increase the death count
                    let ok, lastCount = e.BasicProperties.Headers.TryGetValue(headerName)
                    if ok then
                        headers[headerName] <- Convert.ToInt32(lastCount) + 1
                    else
                        headers.TryAdd(headerName, 1) |> ignore
                    let msgProperties = createMessageProperties e.BasicProperties.Priority headers
                    do!
                        channel.BasicPublishAsync(
                            ExchangeName.value exchangeName,
                            "",
                            true,
                            msgProperties,
                            ReadOnlyMemory(e.Body),
                            (new CancellationTokenSource(TimeSpan.FromSeconds 10.)).Token
                        )
                    do! channel.BasicAckAsync(e.DeliveryTag, false)
                | RejectMessage(retriesCount) ->
                    logDebug mailbox $"Message with tag %d{e.DeliveryTag} has retry count of %d{retriesCount} is rejected"
                    do! channel.BasicRejectAsync(e.DeliveryTag, false)
            }

        let rec initialize () =
            let createAsyncEventHandler callback =
                AsyncEventHandler(fun _ args -> task { callback args })

            task {
                let! connection = connectionFactory.CreateConnectionAsync()

                connection.add_CallbackExceptionAsync (
                    createAsyncEventHandler (fun args -> logErrorWithExn mailbox args.Exception "Connection Callback Exception")
                )
                connection.add_ConnectionBlockedAsync (createAsyncEventHandler (fun args -> logDebug mailbox $"Connection Blocked due to {args.Reason}"))
                connection.add_ConnectionUnblockedAsync (createAsyncEventHandler (fun _ -> logDebug mailbox "Connection Unblocked"))
                connection.add_ConnectionShutdownAsync (createAsyncEventHandler (fun args -> logDebug mailbox $"Connection Shutdown due to {args.Cause}"))

                logDebug mailbox "Connection created"

                let channelCreateOptions = CreateChannelOptions(true, true)
                let! channel = connection.CreateChannelAsync(channelCreateOptions)


                if consumerSettings.Prefetch.CountPerChannel > 0us then
                    do! channel.BasicQosAsync(0u, consumerSettings.Prefetch.CountPerChannel, true)
                if consumerSettings.Prefetch.CountPerConsumer > 0us then
                    do! channel.BasicQosAsync(0u, consumerSettings.Prefetch.CountPerConsumer, false)

                //TODO: channel.add_BasicRecoverOk(createAsyncEventHandler (fun _ -> logDebug mailbox "Channel Basic Recover OK"))
                channel.add_CallbackExceptionAsync (createAsyncEventHandler (fun args -> logErrorWithExn mailbox args.Exception "Channel Callback Exception"))
                channel.add_ChannelShutdownAsync (createAsyncEventHandler (fun _ -> logDebug mailbox "Channel Model Shutdown"))

                logDebug mailbox "Channel created"

                subscriber <! StartWith(retype mailbox.Self)

                let consumer = RmqActorConsumer(QueueName.value queueName, channel, retype mailbox.Self, activityContext)
                let autoAck = consumerSettings.Acknowledge.IsConsumeWithoutAck
                let! consumerTag = channel.BasicConsumeAsync(QueueName.value queueName, autoAck, consumer)
                logDebug mailbox $"Consumer with tag [%s{consumerTag}] created"

                return
                    {
                        Connection = connection
                        Channel = channel
                    }
            }
            |> Async.AwaitTask
            |!> mailbox.Self.Retype()

            awaitInitialize ()
        and awaitInitialize () =
            actor {
                let! (msg: obj) = mailbox.Receive()
                return!
                    match msg with
                    | :? RmqConnection as connection -> operate connection None
                    | :? Status.Failure as exn ->
                        logErrorWithExn mailbox exn.Cause "Initialization failed"
                        waitForRestart exn.Cause None
                    | LifecycleEvent _ -> ignored ()
                    | _ ->
                        logDebug mailbox "Stashing message"
                        mailbox.Stash()
                        ignored ()
            }
        and operate (conn: RmqConnection) (errors: ConsecutiveChannelErrors option) =
            mailbox.UnstashAll()
            actor {
                let! (msg: obj) = mailbox.Receive()
                return!
                    match msg with
                    | :? QueueConsumerCommand as message ->
                        logDebug mailbox $"Acknowledging message [{message}]"
                        match message with
                        | QueueConsumerCommand.Ack ackId -> conn.Channel.BasicAckAsync(ackId.ToNumber(), false)
                        | QueueConsumerCommand.Nack ackId -> conn.Channel.BasicNackAsync(ackId.ToNumber(), false, true)
                        | QueueConsumerCommand.NackAll -> conn.Channel.BasicNackAsync(0UL, true, true)
                        | QueueConsumerCommand.Reject ackId -> conn.Channel.BasicRejectAsync(ackId.ToNumber(), false)
                        |> _.AsTask()
                        |> Async.AwaitTask
                        |> Async.map (fun _ -> AsyncResponse.Success)
                        |!> mailbox.Self.Retype()
                        awaitCommandResult conn errors
                    | :? RmqActorConsumerCommand as cmd ->
                        match cmd with
                        | Delivered args ->
                            receiveMessage conn.Channel args |> Async.AwaitTask |> Async.map (fun _ -> AsyncResponse.Success)
                            |!> mailbox.Self.Retype()
                            awaitCommandResult conn errors
                        | Canceled args -> handleChannelError args (Exception("Channel cancel detected by queue consumer")) conn errors
                    | LifecycleEvent e ->
                        match e with
                        | PostStop ->
                            subscriber <! Stop
                            (conn :> IDisposable).Dispose()
                        | _ -> ()
                        ignored ()
                    | _ -> unhandled ()
            }
        and awaitCommandResult (conn: RmqConnection) (errors: ConsecutiveChannelErrors option) =
            actor {
                let! msg = mailbox.Receive()
                return!
                    match msg with
                    | :? AsyncResponse as res ->
                        match res with
                        | Success -> operate conn errors
                    | :? Status.Failure as exn ->
                        logErrorWithExn mailbox exn.Cause "Command failed"
                        handleChannelError conn.Channel.CloseReason exn.Cause conn errors
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and handleChannelError (closeReason: CancellationDetails) (ex: Exception) (conn: RmqConnection) errors =
            let errors = ConsecutiveChannelErrors.Increment errors
            logChannelError mailbox ex errors
            match isChannelErrorRecoverable closeReason errors limits with
            | Recoverable -> operate (conn: RmqConnection) (Some errors)
            | NonRecoverable -> waitForRestart ex (Some conn)
        and waitForRestart (ex: exn) (conn: RmqConnection option) =
            mailbox.UnstashAll()
            actor {
                logDebug mailbox $"Waiting for restart due to exception [{ex}]"
                let waitMsg = "wait"
                mailbox.Schedule queuePublisherOrConsumerRestartDelay (retype mailbox.Self) waitMsg |> ignore
                subscriber <! Stop
                let! (msg: obj) = mailbox.Receive()
                return!
                    match msg with
                    | :? string as str when str = waitMsg -> raise ex // Bubbles up exception to the supervisor actor and causes restart
                    | LifecycleEvent e ->
                        match e with
                        | PostStop -> conn |> Option.iter (fun conn -> (conn :> IDisposable).Dispose())
                        | _ -> ()
                        ignored ()
                    | _ ->
                        // at this point, we don't care about any messages at all, because channel is soon to be recreated
                        ignored ()
            }

        initialize ()

    type QueueAcknowledgeProps =
        {
            MessageExpiryTime: TimeSpan option
            MessageExpiryCheckInterval: TimeSpan
            MessageExpiryCheckStartDelay: TimeSpan
        }

    type GetOrCreatePublisherCommand =
        {
            ActorName: string
            ExchangeName: ExchangeName
        }

    [<NoEquality; NoComparison>]
    type GetOrCreateConsumerCommand =
        {
            ActorName: string
            QueueName: QueueName
            ConsumerSettings: Settings
            Subscriber: IActorRef<SubscriberMessage>
        }

    [<NoEquality; NoComparison>]
    type FactoryCommand =
        | GetOrCreatePublisher of GetOrCreatePublisherCommand
        | GetOrCreateConsumer of GetOrCreateConsumerCommand

    let queueFactoryActor (connectionDetails: RmqSettings.ConnectionDetails) (limits: ChannelErrorRecoveryLimits) activityContext (mailbox: Actor<_>) =
        let connectionFactory =
            ConnectionFactory(
                HostName = connectionDetails.Hostname,
                VirtualHost = connectionDetails.VirtualHost,
                UserName = connectionDetails.Username,
                Password = connectionDetails.Password,
                Port = connectionDetails.Port
            )
        connectionFactory.AutomaticRecoveryEnabled <- true // this is very important
        connectionFactory.RequestedHeartbeat <- Timeout.InfiniteTimeSpan

        let rec loop (connectionFactory: ConnectionFactory) =
            actor {
                let! message = mailbox.Receive()
                logDebug mailbox $"{message}"
                match message with
                | GetOrCreatePublisher cmd ->
                    let aprops = propsNamed "queue-publisher" (queuePublisherActor connectionFactory cmd.ExchangeName limits activityContext)
                    let actor: IActorRef<QueuePublisherCommand> = getOrSpawnChildActor mailbox.UntypedContext cmd.ActorName aprops
                    mailbox.Sender() <! actor
                | GetOrCreateConsumer cmd ->
                    let aprops =
                        propsNamed
                            "queue-consumer"
                            (queueConsumerActor connectionFactory cmd.QueueName cmd.ConsumerSettings limits cmd.Subscriber activityContext)
                    let actor: IActorRef<QueueConsumerCommand> = getOrSpawnChildActor mailbox.UntypedContext cmd.ActorName aprops
                    mailbox.Sender() <! actor
                return! ignored ()
            }

        loop connectionFactory

    type private InternalAcknowledgmentCommand =
        /// Sent repeatedly by queueAcknowledgeActor itself to trigger cleanup
        | CleanupTags
        /// Sent by queueReaderActor, registers each new message in queueAcknowledgeActor
        | RegisterProcessedMessage of MessageTag

    /// queueAcknowledgeActor keeps track of messages that are currently processed by the system.
    /// There is one queueAcknowledgeActor per queue.
    /// After fetching message from the queue, RegisterProcessedMessage is sent to this actor to register AckId of a processed message.
    /// The actor forwards Ack | Nack | Reject messages to the queue itself. If neither Ack nor Nack is sent for a specified period of time,
    /// the actor will perform a cleanup and Nack the expired message itself.
    let private queueAcknowledgeActor (props: QueueAcknowledgeProps) queue (mailbox: Actor<_>) =
        if Option.isSome props.MessageExpiryTime then
            mailbox.System.Scheduler.ScheduleTellRepeatedly(
                props.MessageExpiryCheckStartDelay,
                props.MessageExpiryCheckInterval,
                (retype mailbox.Self),
                InternalAcknowledgmentCommand.CleanupTags
            )

        let cleanupTags (tags: Map<MessageTag, DateTimeOffset>) =
            let expiredTags, remainingTags =
                tags |> Map.partition (fun _ addedAt -> addedAt < DateTimeOffset.Now - props.MessageExpiryTime.Value)
            for KeyValue(tag, _) in expiredTags do
                logWarning mailbox $"Nacking expired message with tag {tag}"
                queue <! QueueConsumerCommand.Nack tag.AckId
            remainingTags

        let forwardToQueue queueCommand tag tags =
            if Map.containsKey tag tags then
                queue <! queueCommand
                Map.remove tag tags
            else
                logWarning mailbox <| $"Message with tag {tag} is not found"
                tags

        let rec loop (tags: Map<MessageTag, DateTimeOffset>) =
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? AcknowledgementCommand as message ->
                        let tag = message.Tag
                        match message.AckType with
                        | Ack -> forwardToQueue (QueueConsumerCommand.Ack tag.AckId) tag tags
                        | Nack -> forwardToQueue (QueueConsumerCommand.Nack tag.AckId) tag tags
                        | Reject -> forwardToQueue (QueueConsumerCommand.Reject tag.AckId) tag tags
                        |> loop
                    | :? InternalAcknowledgmentCommand as message ->
                        match message with
                        | CleanupTags -> cleanupTags tags
                        | RegisterProcessedMessage tag -> Map.add tag DateTimeOffset.Now tags
                        |> loop
                    | LifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }
        loop Map.empty

    let private getSupervisorStrategy mailbox queue =
        Strategy.OneForOne(fun exn ->
            logErrorWithExn mailbox exn "Invoking supervisor strategy. Unhandled messages will be redelivered"
            queue <! QueueConsumerCommand.NackAll
            Directive.Restart)

    let private spawnAcknowledger mailbox aprops queue : IActorRef<AcknowledgementCommand> =
        spawn
            mailbox
            (makeActorName [ "Queue Acknowledge" ])
            { propsNamed "queue-acknowledger" (queueAcknowledgeActor aprops queue) with
                SupervisionStrategy = Some <| getSupervisorStrategy mailbox queue
            }
        |> retype

    let queueReaderActor<'a>
        (aprops: QueueAcknowledgeProps)
        (deserializeMessage: string -> 'a)
        (messageHandler: IActorRef<QueueMessage<'a>>)
        (mailbox: Actor<_>)
        =

        let rec idle () =
            actor {
                let! message = mailbox.Receive()
                return!
                    match message with
                    | StartWith queue ->
                        let acknowledger = spawnAcknowledger mailbox aprops queue
                        listening acknowledger
                    | _ ->
                        logWarning mailbox <| $"Client: invalid operation in idle state: {message}"
                        ignored ()
            }
        and listening acknowledger =
            actor {
                let! message = mailbox.Receive()
                return!
                    match message with
                    | StartWith _ ->
                        logWarning mailbox <| $"Client: invalid operation in listening state: {message}"
                        ignored ()
                    | Stop ->
                        // We don't want to ack or nack those messages, because channel does not exist at this point.
                        mailbox.UntypedContext.Stop(untyped acknowledger)
                        idle ()
                    | IncomingMessage(msg, tag) ->
                        try
                            let ack =
                                tag
                                |> Option.map (fun tag ->
                                    (retype acknowledger) <! InternalAcknowledgmentCommand.RegisterProcessedMessage tag
                                    {
                                        AckActor = acknowledger
                                        Tag = tag
                                        Timestamp = DateTimeOffset.Now
                                    })
                            if String.IsNullOrEmpty msg then
                                logError mailbox "Incoming queue message is empty"
                                tag |> Option.iter (fun tag -> acknowledger <! { AckType = Reject; Tag = tag })
                            else
                                let message = deserializeMessage msg
                                logInfo mailbox $"Incoming queue message [{message}]"
                                messageHandler <! QueueMessage.createWithAck message ack
                        with exn ->
                            logErrorWithExn mailbox exn $"Unable to deserialize queue message [%s{msg}]"
                            tag |> Option.iter (fun tag -> acknowledger <! { AckType = Reject; Tag = tag })
                        ignored ()
            }

        idle ()
