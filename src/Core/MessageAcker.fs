namespace Nrk.Oddjob.Core

module MessageAcker =

    open System
    open Akkling

    open Nrk.Oddjob.Core.Queues.QueueApi

    type AckerCommand =
        | RegisterAck of id: Guid * ack: MessageAck option
        | Acknowledge of id: Guid
        | Nack of id: Guid * reason: string
        | Reject of id: Guid * reason: string

    let messageAcker (mailbox: Actor<_>) =
        let rec loop (ackInProgress: Map<Guid, MessageAck option>) =
            actor {
                let! (msg: obj) = mailbox.Receive()
                return!
                    match msg with
                    | LifecycleEvent _ -> ignored ()
                    | :? AckerCommand as cmd ->
                        match cmd with
                        | RegisterAck(id, ack) -> loop (ackInProgress.Add(id, ack))
                        | Acknowledge id ->
                            match ackInProgress.TryFind id with
                            | Some ack ->
                                sendAck mailbox ack "Event stored in DB"
                                loop (ackInProgress.Remove id)
                            | None -> ignored ()
                        | Nack(id, reason) ->
                            match ackInProgress.TryFind id with
                            | Some ack ->
                                sendReject mailbox ack reason
                                loop (ackInProgress.Remove id)
                            | None -> ignored ()
                        | Reject(id, reason) ->
                            match ackInProgress.TryFind id with
                            | Some ack ->
                                sendReject mailbox ack reason
                                loop (ackInProgress.Remove id)
                            | None -> ignored ()
                    | _ -> unhandled ()
            }
        loop Map.empty
