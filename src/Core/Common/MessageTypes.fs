namespace Nrk.Oddjob.Core

[<AutoOpen>]
module MessageTypes =

    open System
    open Akkling

    /// Uuniquely identifies the delivery (scoped per channel). See https://www.rabbitmq.com/confirms.html#consumer-acks-delivery-tags
    type AckId =
        | AckId of uint64

        member this.ToNumber() =
            match this with
            | AckId x -> x
        override this.ToString() =
            match this with
            | AckId x -> $"{x}"

    type MessageTag =
        {
            AckId: AckId
            /// RMQ redelivered flag, set to true when message has been requeued before being fetched now. See https://www.rabbitmq.com/reliability.html#consumer
            Redelivered: bool
            /// RMQ message priority. See https://www.rabbitmq.com/priority.html
            Priority: byte
            /// For RMQ messages shovelled from other queues/exchanges
            ForwardedFrom: string option
        }

    type AckType =
        | Ack
        | Nack
        | Reject

    type AcknowledgementCommand = { Tag: MessageTag; AckType: AckType }

    /// Stuff required to ack/nack/reject message in RabbitMQ
    type MessageAck =
        {
            AckActor: IActorRef<AcknowledgementCommand>
            Tag: MessageTag
            Timestamp: DateTimeOffset
        }

    /// A message that was read from RabbitMQ
    type Message<'a> =
        {
            Payload: 'a
            Ack: MessageAck option // for queue acknowledgement
        } // for actor supervisor and at-least-one delivery

    [<RequireQualifiedAccess>]
    module Message =
        let create payload = { Payload = payload; Ack = None }

        let createWithAck payload ack = { Payload = payload; Ack = ack }

        let transform fn (message: Message<_>) =
            {
                Payload = fn message.Payload
                Ack = message.Ack
            }

    /// A message that was read from RabbitMQ
    type QueueMessage<'a> = { Payload: 'a; Ack: MessageAck option } // for queue acknowledgement

    [<RequireQualifiedAccess>]
    module QueueMessage =
        let create payload = { Payload = payload; Ack = None }
        let createWithAck payload ack = { Payload = payload; Ack = ack }

        let transform fn (message: QueueMessage<_>) =
            {
                Payload = fn message.Payload
                Ack = message.Ack
            }

        let toMessage (message: QueueMessage<_>) : Message<_> =
            {
                Payload = message.Payload
                Ack = message.Ack
            }

    let multiAckerWrapperActor (ackerActor: IActorRef<AcknowledgementCommand>) (expectedAcks: int) (_mailbox: Actor<_>) =
        if expectedAcks < 1 then
            invalidArg "expectedAcks" "expectedAcks should be at least 1"

        let rec impl (received: AcknowledgementCommand list) (msg: AcknowledgementCommand) =
            let received = msg :: received
            match msg.AckType with
            | AckType.Nack
            | AckType.Reject -> ackerActor <! msg
            | AckType.Ack -> ()
            if received.Length = expectedAcks then
                if received |> List.forall (fun r -> r.AckType = AckType.Ack) then
                    // we checked that we expected at least 1 ack, so the list won't be empty
                    ackerActor <! List.head received
                stop ()
            else
                become (impl received)
        impl []
