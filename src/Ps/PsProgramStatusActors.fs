namespace Nrk.Oddjob.Ps

module PsProgramStatusActors =

    open System
    open Akka.Actor
    open Akkling

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Queues
    open Nrk.Oddjob.Core.ShardMessages

    open PsTypes
    open PsJobCreation
    open PsShardMessages

    [<NoEquality; NoComparison>]
    type PsProgramStatusHandlerProps =
        {
            SubtitlesConfig: PsSubtitlesConfig
            MediaSetController: IActorRef<Message<MediaSetShardMessage>>
            PsMediator: IActorRef<QueueMessage<PsShardMessage>>
            SaveToEventStore: PiProgId -> string -> string -> PsProgramStatusEvent -> unit
            PlayabilityReminderInterval: TimeSpan
            ReminderAckTimeout: TimeSpan
            PsScheduler: IActorRef
        }

    let handleLifecycle mailbox e =
        match e with
        | PreRestart(exn, message) ->
            logErrorWithExn mailbox exn $"PreRestart due to error when processing message {message.GetType()} [%A{message}]"
            match message with
            | :? QueueMessage<PsProgramStatusEvent> as message ->
                match exn with
                | :? ArgumentException -> sendReject mailbox message.Ack "Rejecting invalid command"
                | _ -> sendNack mailbox message.Ack "Failed processing Program Status message"
                ignored ()
            | _ -> unhandled ()
        | _ -> ignored ()

    let psProgramStatusHandlerActor (aprops: PsProgramStatusHandlerProps) (psMediator: IActorRef<obj>) (mailbox: Actor<_>) =

        let jobCreator = PsJobCreator([], aprops.SubtitlesConfig, logDebug mailbox)

        let handleMessage (message: QueueMessage<PsProgramStatusEvent>) =
            let piProgId = PiProgId.create message.Payload.ProgramId
            use _ =
                createTraceSpan mailbox "psProgramStatusHandlerActor.handleMessage" [ ("oddjob.ps.programId", PiProgId.value piProgId) ]
            aprops.SaveToEventStore piProgId null (getUnionCaseName message.Payload) message.Payload
            match message.Payload with
            | ProgramDeactivated event ->
                let details: PartsStatusJobDetails =
                    {
                        RequestSource = Helpers.prfRequestSource
                        ForwardedFrom = message.Ack |> Option.bind (_.Tag.ForwardedFrom)
                        Priority = 0
                        PiProgId = piProgId
                        Carriers = event.Carriers
                    }
                let job = jobCreator.CreateDeactivatePartsJob details
                let msg = message |> QueueMessage.transform (fun _ -> MediaSetShardMessage.MediaSetJob job) |> QueueMessage.toMessage
                aprops.MediaSetController <! msg
            | ProgramActivated event ->
                let details: PartsStatusJobDetails =
                    {
                        RequestSource = Helpers.prfRequestSource
                        ForwardedFrom = message.Ack |> Option.bind (_.Tag.ForwardedFrom)
                        Priority = 0
                        PiProgId = piProgId
                        Carriers = event.Carriers
                    }
                let job = jobCreator.CreateActivatePartsJob details
                let msg = message |> QueueMessage.transform (fun _ -> MediaSetShardMessage.MediaSetJob job) |> QueueMessage.toMessage
                aprops.MediaSetController <! msg
            | ProgramGotActiveCarriers activeCarriers ->
                let msg = QueueMessage.createWithAck (PsShardMessage.ProgramGotActiveCarriersEvent activeCarriers) message.Ack
                aprops.PsMediator <! msg

        let rec idle initiating =
            if (not initiating) then // Unit tests have problems with UnstashAll before an actor receives first message
                mailbox.UnstashAll()
            logDebug mailbox "idle"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? QueueMessage<PsProgramStatusEvent> as queueMessage ->
                        logDebug mailbox $"Queue message: {queueMessage.Payload}"
                        Utils.startPlayabilityReminder
                            aprops.PsScheduler
                            queueMessage.Payload.ProgramId
                            psMediator.Path
                            aprops.PlayabilityReminderInterval
                            (logDebug mailbox)
                        awaiting_reminder_ack queueMessage
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | _ ->
                        logWarning mailbox $"Unhandled program status handler message in idle state: %A{message}"
                        unhandled ()
            }
        and awaiting_reminder_ack queueMessage =
            logDebug mailbox "awaiting_reminder_ack"
            mailbox.SetReceiveTimeout(Some aprops.ReminderAckTimeout)
            actor {
                let! message = mailbox.Receive()
                return!
                    match message with
                    | :? Reminders.ReminderCreated ->
                        mailbox.SetReceiveTimeout None
                        handleMessage queueMessage
                        idle false
                    | Reminders.LifetimeEvent e ->
                        logDebug mailbox $"Reminder lifetime event {e}"
                        ignored ()
                    | :? ReceiveTimeout -> raise <| TimeoutException "Timeout awaiting reminder ack"
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        idle true
