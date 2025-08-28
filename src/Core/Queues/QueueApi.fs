namespace Nrk.Oddjob.Core.Queues

/// Contains public types that are used by code that communicates with RabbitMQ queues and exchanges
[<AutoOpen>]
module QueueApi =
    open System
    open Akkling
    open Nrk.Oddjob.Core

    [<Literal>]
    let RetriesCountHeader = "dm-retries-count"

    [<Literal>]
    let ErrorsCountHeader = "dm-errors-count"

    [<AutoOpen>]
    module ActorCommands =

        /// Message published to RabbitMQ
        type OutgoingMessage =
            {
                Payload: string
                RoutingKey: string
                Priority: byte
            }

        module OutgoingMessage =
            let create payload =
                {
                    Payload = payload
                    RoutingKey = ""
                    Priority = 0uy
                }

            let withRoutingKey key msg =
                { msg with
                    OutgoingMessage.RoutingKey = key
                }

            let withPriority priority msg =
                { msg with
                    OutgoingMessage.Priority = priority
                }

        /// obj is a context provided in QueuePublisherCommand
        type QueuePublishResult = Result<obj, exn * obj>

        [<NoComparison>]
        type QueuePublisherCommand =
            /// Fire and forget operation. Does not guarantee the message will actually be sent.
            | PublishWithoutConfirmation of OutgoingMessage
            | PublishOne of msg: OutgoingMessage * context: obj * confirmPublished: IActorRef<QueuePublishResult>
            | PublishMany of msg: OutgoingMessage list * context: obj * confirmPublished: IActorRef<QueuePublishResult>

        [<RequireQualifiedAccess>]
        type QueueConsumerCommand =
            | Ack of ackId: AckId
            | Nack of ackId: AckId
            | NackAll
            | Reject of ackId: AckId

    [<AutoOpen>]
    module AcknowledgeHelpers =

        let private acknowledgeQueue mailbox ack reason ackType =
            ack
            |> Option.iter (fun ack' ->
                logInfo mailbox $"Acknowledging #{ack'.Tag.AckId} [%s{reason}] [{ackType}]"
                ack'.AckActor <! { Tag = ack'.Tag; AckType = ackType })

        /// Uses basic.ack
        let sendAck mailbox ack reason = acknowledgeQueue mailbox ack reason Ack

        /// Uses basic.nack to requeue the message.
        let sendNack mailbox ack reason =
            acknowledgeQueue mailbox ack reason Nack

        /// Uses basic.reject if rejection is enabled, otherwise fallbacks to basic.nack
        let sendReject mailbox ack reason =
            acknowledgeQueue mailbox ack reason Reject

    [<AutoOpen>]
    module OddjobRabbitMqApi =

        /// Categories for incoming queue messages, should match those defined in Oddjob settings
        [<RequireQualifiedAccess>]
        type QueueCategory =
            | PsTranscodingWatch
            | PsRadioWatch
            | PsProgramsWatch
            | PsSubtitlesWatch
            | PsUsageRightsPsWatch
            | PsProgramStatusWatch
            | PotionWatch

            static member FromString s = parseUnionCaseName<QueueCategory> s
            static member TryFromString = tryParseUnionCaseName<QueueCategory> true
            override x.ToString() = getUnionCaseName x

        /// Categories for publishing outgoing messages, should match those defined in Oddjob settings
        [<RequireQualifiedAccess>]
        type ExchangeCategory =
            | PsProgramsWatch
            | PsSubtitlesWatch
            | PotionWatch
            | OddjobEvents
            | OdaSubtitles
            | PsTranscodingWatch
            | PsProgramStatusWatch

            static member FromString s = parseUnionCaseName<ExchangeCategory> s
            static member TryFromString = tryParseUnionCaseName<ExchangeCategory> true
            override x.ToString() = getUnionCaseName x

        type IQueuesApi =
            abstract AttachConsumer<'msg> :
                system: Akka.Actor.ActorSystem * category: QueueCategory * msgHandler: IActorRef<QueueMessage<'msg>> * ?deserialize: (string -> 'msg) -> unit

        type IExchangesApi =
            abstract GetPublisher: Akka.Actor.ActorSystem * category: ExchangeCategory -> IActorRef<QueuePublisherCommand>

        type IQueueConsumerApi =
            abstract AttachConsumer<'msg> :
                system: Akka.Actor.ActorSystem * queueName: string * msgHandler: IActorRef<QueueMessage<'msg>> * ?deserialize: (string -> 'msg) -> unit

        type IRabbitMqApi =
            inherit IQueuesApi
            inherit IExchangesApi

        type RetryErrorLimits =
            {
                /// Number of times message is moved to retries queue, before it gets moved to errors queue (during the first cycle of retries/errors moves)
                NumberOfMovesToRetriesQueueBeforeFirstMoveToErrorsQueue: int
                /// Number of times message was in errors queue before it gets rejected (has to be bigger than "NumberOfMovesToRetriesQueue..." for it to work correctly)
                NumberOfMovesToErrorsQueueBeforeMoveToRejectedQueue: int
            }

            static member Default =
                {
                    NumberOfMovesToRetriesQueueBeforeFirstMoveToErrorsQueue = 3
                    NumberOfMovesToErrorsQueueBeforeMoveToRejectedQueue = 100
                }

        type ChannelErrorRecoveryLimits =
            {
                TotalTimeout: TimeSpan
                MixedTimeoutCount: TimeSpan * int
                TotalCount: int
            }

            static member Default =
                {
                    TotalTimeout = TimeSpan.FromMinutes 6.
                    MixedTimeoutCount = (TimeSpan.FromMinutes 3., 50)
                    TotalCount = 500
                }

        type QueueApiLimits =
            {
                RetryErrorLimits: RetryErrorLimits
                ChannelErrorRecoveryLimits: ChannelErrorRecoveryLimits
            }

            static member Default =
                {
                    RetryErrorLimits = RetryErrorLimits.Default
                    ChannelErrorRecoveryLimits = ChannelErrorRecoveryLimits.Default
                }
