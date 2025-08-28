namespace Nrk.Oddjob.Potion

open Nrk.Oddjob.Core.Dto.Events


module PotionWorkerActors =

    open System
    open Akka.Actor
    open Akka.Persistence
    open Akkling
    open Akkling.Persistence

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Queues
    open Nrk.Oddjob.Core.ShardMessages

    open PotionTypes
    open PotionUtils
    open PotionUtils.Priority

    [<NoEquality; NoComparison>]
    type PotionHandlerProps =
        {
            MediaSetController: IActorRef<Message<MediaSetShardMessage>>
            ClearMediaSetReminderInterval: TimeSpan
            NotificationPublisher: IActorRef<QueuePublisherCommand>
            RetentionPeriods: Map<string, TimeSpan>
            CalculateJobPriority: MessageSource -> Priority
            PendingCommandsRetryInterval: TimeSpan list
            PotionMediator: IActorRef<obj>
            CompletionReminderInterval: TimeSpan
            CompletionExpiryInterval: TimeSpan
            ReminderAckTimeout: TimeSpan option
            IdleStateTimeout: TimeSpan option
            PotionScheduler: IActorRef
            UploadScheduler: IActorRef
            ExternalRequestTimeout: TimeSpan option
            SaveToEventStore: PotionCommand -> unit
            CreateCommandId: PotionCommand -> string
            Origins: Origin list
            SkipNotificationForSources: string list
            EnableDefaultGeoBlock: unit -> bool
            CorrectGeoBlockOnActivation: unit -> bool
        }

    type ExecutingCommand =
        {
            Command: PotionCommand
            CommandId: string
            Event: PotionPersistence.IProtoBufSerializableEvent
        }
    type DispatchingCommand =
        {
            Command: PotionCommand
            CommandId: string
        }
    type PersistentCommands =
        {
            PendingCommands: PendingCommand list
            ExecutingCommands: ExecutingCommand list
        }
    type RetryCommand = RetryCommand of PendingCommand
    type CancelCommands = CancelCommands of PendingCommand list
    type CompletedCommands = CompletedCommands of PotionPersistence.CompletedCommand list
    type CancelledCommands = CancelledCommands of PotionPersistence.CancelledCommand list

    let private throwIfCommandInvalid (command: PotionCommand) =
        match command with
        | AddFile x -> [ x.FileId; x.FilePath; x.GroupId ]
        | AddSubtitle x -> [ x.GroupId ]
        | SetGeoBlock x -> [ x.GroupId ]
        | DeleteGroup x -> [ x.GroupId ]
        | DeleteSubtitle x -> [ x.GroupId; x.Language ]
        |> List.filter String.IsNullOrEmpty
        |> function
            | [] -> ()
            | _ -> invalidArg "PotionCommand" "Missing mandatory fields"

    let potionHandlerActor (aprops: PotionHandlerProps) (mailbox: Eventsourced<_>) =

        let scheduleCompletionReminder shortGroupId traceContext =
            // For repeatedly waking up Potion actor until there are no pending commands
            use _ =
                createTraceSpanForContext
                    mailbox
                    traceContext
                    "potionHandlerActor.scheduleCompletionReminder"
                    [ ("oddjob.workflow", "Scheduled completion reminder") ]
            let bumpMessage = { PotionBump.ContentId = shortGroupId }
            let interval = aprops.CompletionReminderInterval
            let triggerTime = DateTimeOffset.Now.AddSafely(interval)
            Reminders.rescheduleRepeatingReminderTask
                aprops.PotionScheduler
                Reminders.PotionBump
                shortGroupId
                bumpMessage
                aprops.PotionMediator.Path
                triggerTime
                interval
                (logDebug mailbox)

        let applyPersistentEvent (state: PotionSetState) (event: PotionPersistence.IProtoBufSerializable) =

            let validateGroupId groupId =
                match state.Group with
                | Some group when groupId <> group.GroupId -> invalidOp $"GroupId can not be changed (attempt to change from %s{group.GroupId} to %s{groupId}"
                | _ -> ()

            let validateMediaType mediaType =
                match state.Group with
                | Some group when group.MediaType.IsSome && Some mediaType <> group.MediaType ->
                    invalidOp $"MediaType can not be changed (attempt to change from %A{group.MediaType} to %A{mediaType}"
                | _ -> ()

            let removePendingCommand commandId origin =
                if origin = 0 then
                    state.PendingCommands |> PendingCommands.removeCommand commandId
                else
                    state.PendingCommands |> PendingCommands.removeCommandForOrigin commandId (PotionPersistence.convertToOrigin origin)

            let shouldSkipNotification eventSource eventForwardedFrom =
                aprops.SkipNotificationForSources
                |> Seq.intersect ([ eventSource; eventForwardedFrom ] |> List.map Option.ofObj |> List.choose id)
                |> Seq.isNotEmpty

            let setCreationTime timestamp (state: PotionSetState) =
                if state.CreationTime = DateTimeOffset.MinValue then
                    timestamp
                else
                    state.CreationTime

            try
                match event with
                | :? PotionPersistence.AssignedGeoBlock as event ->
                    let cmd = event.ToDomain()
                    validateGroupId cmd.GroupId
                    match state.LastVersion, event.Version with
                    | Some stateVersion, eventVersion when stateVersion >= eventVersion -> state
                    | _ ->
                        { state with
                            Group =
                                state.Group
                                |> PotionGroup.updateGeoBlock cmd.GroupId (Some cmd.GeoBlock) None (CorrelationId.isInternal cmd.CorrelationId)
                            LastVersion = if event.Version = 0 then None else Some event.Version
                            CreationTime = state |> setCreationTime event.Timestamp
                            ClientCommands = (SetGeoBlock cmd) :: state.ClientCommands
                            ShouldSkipNotifications = state.ShouldSkipNotifications || shouldSkipNotification event.Source event.ForwardedFrom
                        }

                | :? PotionPersistence.AssignedFile as event ->
                    let cmd = event.ToDomain()
                    validateGroupId cmd.GroupId
                    validateMediaType cmd.MediaType
                    let group =
                        state.Group
                        |> PotionGroup.updateGroup cmd.GroupId None (Some cmd.MediaType) (CorrelationId.isInternal cmd.CorrelationId)
                    { state with
                        Group = group
                        Files = state.Files |> PotionFiles.addFile cmd group
                        CreationTime = state |> setCreationTime event.Timestamp
                        ClientCommands = (AddFile cmd) :: state.ClientCommands
                        ShouldSkipNotifications = state.ShouldSkipNotifications || shouldSkipNotification event.Source event.ForwardedFrom
                    }

                | :? PotionPersistence.AssignedSubtitles as event ->
                    let cmd = event.ToDomain()
                    validateGroupId cmd.GroupId
                    { state with
                        Group = state.Group |> PotionGroup.updateGroup cmd.GroupId None None (CorrelationId.isInternal cmd.CorrelationId)
                        Subtitles = state.Subtitles |> PotionSubtitles.addSubtitles cmd
                        CreationTime = state |> setCreationTime event.Timestamp
                        ClientCommands = (AddSubtitle cmd) :: state.ClientCommands
                        ShouldSkipNotifications = state.ShouldSkipNotifications || shouldSkipNotification event.Source event.ForwardedFrom
                    }

                | :? PotionPersistence.DeletedGroup as event ->
                    let cmd = event.ToDomain()
                    validateGroupId cmd.GroupId
                    { state with
                        Files = Map.empty
                        Subtitles = Map.empty
                        CreationTime = state |> setCreationTime event.Timestamp
                        ClientCommands = (DeleteGroup cmd) :: state.ClientCommands
                        ShouldSkipNotifications = state.ShouldSkipNotifications || shouldSkipNotification event.Source event.ForwardedFrom
                    }

                | :? PotionPersistence.DeletedSubtitles as event ->
                    let cmd = event.ToDomain()
                    validateGroupId cmd.GroupId
                    { state with
                        Subtitles = state.Subtitles |> PotionSubtitles.removeSubtitles cmd
                        CreationTime = state |> setCreationTime event.Timestamp
                        ClientCommands = (DeleteSubtitle cmd) :: state.ClientCommands
                        ShouldSkipNotifications = state.ShouldSkipNotifications || shouldSkipNotification event.Source event.ForwardedFrom
                    }

                | :? PotionPersistence.AssignedGroupId as event ->
                    let groupId, mediaSetId = event.ToDomain()
                    { state with
                        Group = state.Group |> PotionGroup.assignGroupId groupId mediaSetId
                        Files = state.Files |> PotionFiles.assignMediaSetFileId mediaSetId
                        CreationTime = state |> setCreationTime event.Timestamp
                    }

                | :? PotionPersistence.AssignedFileId as event ->
                    let fileId, mediaSetFileId, qualityId = event.ToDomain()
                    { state with
                        Files = state.Files |> PotionFiles.assignFileId fileId mediaSetFileId qualityId
                        CreationTime = state |> setCreationTime event.Timestamp
                    }

                | :? PotionPersistence.PendingCommand as event when PotionPersistence.deprecatedOrigins |> List.contains event.Origin || event.Origin = 0 ->
                    state

                | :? PotionPersistence.PendingCommand as event ->
                    let command = event.ToDomain()
                    { state with
                        PendingCommands = state.PendingCommands |> PendingCommands.addCommand command
                    }

                | :? PotionPersistence.CompletedCommand as event ->
                    { state with
                        PendingCommands = removePendingCommand event.CommandId event.Origin
                    }

                | :? PotionPersistence.CancelledCommand as event ->
                    { state with
                        PendingCommands = removePendingCommand event.CommandId event.Origin
                    }

                | :? PotionPersistence.CompletedMigration -> state

                | :? PotionPersistence.PotionSetState as event ->
                    let newState = event.ToDomain()
                    match state.Group, newState.Group with
                    | Some group, Some newGroup when group.GroupId <> newGroup.GroupId ->
                        invalidOp $"GroupId can not be changed (attempt to change from %s{group.GroupId} to %s{newGroup.GroupId}"
                    | _ -> ()
                    newState

                | _ -> invalidOp $"Unexpected persistent event [{event}]"

            with exn ->
                logErrorWithExn mailbox exn $"Failed to recover actor state, last sequenceNr: {mailbox.LastSequenceNr()}"
                reraise ()

        let handleLifecycle mailbox e =
            match e with
            | PreRestart(exn, message) ->
                logErrorWithExn mailbox exn $"PreRestart due to error when processing message {message.GetType()} [%A{message}]"
                match message with
                | :? QueueMessage<PotionCommand> as message ->
                    logErrorWithExn mailbox exn $"PreRestart due to error when processing message {message.GetType()} [%A{message}]"
                    match exn with
                    | :? ArgumentException -> sendReject mailbox message.Ack "Rejecting invalid command"
                    | _ -> sendNack mailbox message.Ack "Failed processing Potion message"
                    ignored ()
                | _ -> unhandled ()
            | _ -> ignored ()

        let createDefaultGeoBlockCommand groupId =
            PotionCommand.SetGeoBlock
                {
                    GeoBlock = GeoRestriction.NRK
                    GroupId = groupId
                    CorrelationId = CorrelationId.InternalSetGeoBlock // Should not send notification back to Potion
                    Source = OddjobSystem.ToLower() |> Some
                    Version = None
                }

        // UNDO: this is a temporary field to be removed once geoblocks are fixed
        let createCorrectGeoBlockCommand groupId geoBlock =
            PotionCommand.SetGeoBlock
                {
                    GeoBlock = geoBlock
                    GroupId = groupId
                    CorrelationId = CorrelationId.InternalRepair // Should send notification back to Potion
                    Source = OddjobSystem.ToLower() |> Some
                    Version = None
                }

        let createPersistentCommands (state: PotionSetState) cmd commandId ack =
            let forwardedFrom = ack |> Option.bind _.Tag.ForwardedFrom
            let commandPriority = ack |> Option.map _.Tag.Priority |> Option.defaultValue 0uy
            let timestamp = DateTimeOffset.Now
            let geoBlockCommand =
                match cmd with
                | PotionCommand.AddFile _
                | PotionCommand.AddSubtitle _ when aprops.EnableDefaultGeoBlock() ->
                    let geoBlockCommand = createDefaultGeoBlockCommand (cmd.GetGroupId())
                    let geoBlockCommandId = aprops.CreateCommandId geoBlockCommand
                    match state.Group with
                    | Some group when group.GeoBlock = GeoRestriction.Unspecified -> Some(geoBlockCommand, geoBlockCommandId)
                    | None -> Some(geoBlockCommand, geoBlockCommandId)
                    | _ -> None
                | _ -> None
            let getPendingCommands origin =
                match geoBlockCommand with
                | Some geoBlockCommand ->
                    [
                        PendingCommand.create (snd geoBlockCommand) (fst geoBlockCommand) origin timestamp commandPriority forwardedFrom
                        PendingCommand.create commandId cmd origin timestamp commandPriority forwardedFrom
                    ]
                | None ->
                    [
                        PendingCommand.create commandId cmd origin timestamp commandPriority forwardedFrom
                    ]
            let pendingCommands = aprops.Origins |> List.map getPendingCommands |> List.concat
            let executingCommands =
                match cmd, geoBlockCommand with
                | AddFile addFileCommand, Some(PotionCommand.SetGeoBlock geoBlockCommand, geoBlockCommandId) ->
                    [
                        {
                            Command = PotionCommand.SetGeoBlock geoBlockCommand
                            Event =
                                PotionPersistence.AssignedGeoBlock.FromDomain(geoBlockCommand, forwardedFrom, timestamp)
                                :> PotionPersistence.IProtoBufSerializableEvent
                            CommandId = geoBlockCommandId
                        }
                        {
                            Command = cmd
                            Event = PotionPersistence.AssignedFile.FromDomain(addFileCommand, forwardedFrom, timestamp)
                            CommandId = commandId
                        }
                    ]
                | AddFile addFileCommand, _ ->
                    [
                        {
                            Command = cmd
                            Event = PotionPersistence.AssignedFile.FromDomain(addFileCommand, forwardedFrom, timestamp)
                            CommandId = commandId
                        }
                    ]
                | AddSubtitle addSubtitleCommand, Some(PotionCommand.SetGeoBlock cmdGeoBlock, geoBlockCommandId) ->
                    [
                        {
                            Command = cmd
                            Event = PotionPersistence.AssignedGeoBlock.FromDomain(cmdGeoBlock, forwardedFrom, timestamp)
                            CommandId = geoBlockCommandId
                        }
                        {
                            Command = cmd
                            Event = PotionPersistence.AssignedSubtitles.FromDomain(addSubtitleCommand, forwardedFrom, timestamp)
                            CommandId = commandId
                        }
                    ]
                | AddSubtitle addSubtitleCommand, _ ->
                    [
                        {
                            Command = cmd
                            Event = PotionPersistence.AssignedSubtitles.FromDomain(addSubtitleCommand, forwardedFrom, timestamp)
                            CommandId = commandId
                        }
                    ]
                | SetGeoBlock setGeoBlockCommand, _ ->
                    [
                        {
                            Command = cmd
                            Event = PotionPersistence.AssignedGeoBlock.FromDomain(setGeoBlockCommand, forwardedFrom, timestamp)
                            CommandId = commandId
                        }
                    ]
                | DeleteGroup deleteGroupCommand, _ ->
                    [
                        {
                            Command = cmd
                            Event = PotionPersistence.DeletedGroup.FromDomain(deleteGroupCommand, forwardedFrom, timestamp)
                            CommandId = commandId
                        }
                    ]
                | DeleteSubtitle deleteSubtitleCommand, _ ->
                    [
                        {
                            Command = cmd
                            Event = PotionPersistence.DeletedSubtitles.FromDomain(deleteSubtitleCommand, forwardedFrom, timestamp)
                            CommandId = commandId
                        }
                    ]
            {
                PendingCommands = pendingCommands
                ExecutingCommands = executingCommands
            }

        let getCompletedCommands (state: PotionSetState) (event: PotionDto.PotionCommandStatusEvent) =
            match event.Status with
            | JobStatus.Unchanged
            | JobStatus.Completed
            | JobStatus.Rejected ->
                state.PendingCommands
                |> PendingCommands.getMatchingCommands event.CorrelationId event.ClientFileId event.StorageProvider
            | _ -> []

        let reducePendingCommands (state: PotionSetState) =
            let pendingCommands =
                state.PendingCommands
                |> List.choose (fun cmd ->
                    match state.Group, cmd.Command with
                    | Some group, SetGeoBlock cmd' when group.GeoBlock <> cmd'.GeoBlock -> None // Old geoblock, retrying it would set incorrect value
                    | _ -> Some cmd)
            let cancelledCommands = state.PendingCommands |> List.except pendingCommands
            pendingCommands, cancelledCommands

        let retryCommands pendingCommands =
            pendingCommands
            |> List.filter (fun cmd -> cmd |> PendingCommand.isTimeToRetry aprops.PendingCommandsRetryInterval DateTimeOffset.Now)
            |> List.iter (fun cmd -> (retype mailbox.Self) <! RetryCommand cmd)

        let cancelCommands cancelledCommands =
            (retype mailbox.Self) <! CancelCommands cancelledCommands

        let stopReminderOnCompletionOrExpiry (state: PotionSetState) =
            if aprops.CompletionReminderInterval > TimeSpan.MinValue && aprops.CompletionExpiryInterval > TimeSpan.MinValue then
                state.Group
                |> Option.iter (fun group ->
                    match state.PendingCommands |> List.tryMaxBy _.Timestamp |> Option.map _.Timestamp with
                    | Some timestamp when DateTimeOffset.Now - timestamp < aprops.CompletionExpiryInterval -> ()
                    | _ ->
                        let shortGroupId = getShortGroupId group.GroupId
                        Reminders.cancelReminderTask aprops.PotionScheduler Reminders.PotionBump shortGroupId (logDebug mailbox))

        let createAndDispatchJob state (executingCommands: DispatchingCommand list) forwardedFrom traceContext =
            // In most cases executingCommands is just a single command except for the case of missing geoblock
            // when executingCommands consist of a SetGeoblock command followed by the main command (AddFile or AddSubtitles)
            let geoBlockCommand, mainCommand =
                match executingCommands with
                | [ mainCommand ] -> None, mainCommand
                | _ -> executingCommands |> List.head |> Some, executingCommands |> List.last
            let priority = aprops.CalculateJobPriority(mainCommand.Command.GetSource())
            let geoBlockJob =
                geoBlockCommand |> Option.map (fun geoBlockCommand -> createJob state geoBlockCommand.Command priority forwardedFrom)
            let mainJob = createJob state mainCommand.Command priority forwardedFrom
            logDebug mailbox $"Created job %A{mainJob} for command %A{mainCommand}"
            let jobs =
                match geoBlockJob with
                | Some geoBlockJob -> [ geoBlockJob; mainJob ]
                | None -> [ mainJob ]
            jobs
            |> List.iter (fun job ->
                use _ =
                    createTraceSpanForContext mailbox traceContext "potionHandlerActor.dispatchJob" [ ("oddjob.mediaset.command", mainJob.CommandName) ]
                let msg = Message.create (MediaSetShardMessage.MediaSetJob job)
                aprops.MediaSetController <! msg)

        let createQueueMessage routingKey priority event =
            event
            |> Serialization.Newtonsoft.serializeObject SerializationSettings.PascalCase
            |> OutgoingMessage.create
            |> OutgoingMessage.withRoutingKey routingKey
            |> OutgoingMessage.withPriority priority

        let tryCreateNotificationMessage (event: PotionDto.PotionCommandStatusEvent) =
            match event.Status with
            | JobStatus.Completed
            | JobStatus.Unchanged
            | JobStatus.Rejected ->
                let routingKey = "potion.responses.v3"
                PotionDto.MediaSetEvent.tryCreateFromCommandStatus event |> Option.map (createQueueMessage routingKey event.Priority)
            | _ -> None

        let persistEvents events =
            events |> Seq.iter (fun event -> logDebug mailbox $"Persisting %s{event.GetType().Name}")
            PersistAll events

        let isOutdatedCommand (cmd: PotionCommand) (state: PotionSetState) =
            if cmd.GetCorrelationId() |> CorrelationId.isInternal then
                false
            else
                match cmd with
                | PotionCommand.SetGeoBlock cmd' ->
                    match state.LastVersion, cmd'.Version with
                    | Some stateVersion, Some eventVersion when stateVersion >= eventVersion -> true
                    | Some _, None -> true
                    | _ -> false
                | _ -> false

        let getTraceTagsForCommand (cmd: PotionCommand) workflowTag =
            List.concat
                [
                    [
                        ("oddjob.potion.groupId", cmd.GetGroupId())
                        ("oddjob.potion.correlationId", cmd.GetCorrelationId())
                        ("oddjob.potion.messageSource", cmd.GetSource())
                        ("oddjob.workflow", workflowTag)
                    ]
                    match cmd.GetFileId(), cmd.GetQualityId() with
                    | Some fileId, Some qualityId ->
                        [
                            ("oddjob.potion.fileId", fileId)
                            ("oddjob.potion.qualityId", $"{qualityId}")
                            ("oddjob.mediaset.qualityId", $"{qualityId}")
                        ]
                    | _ -> []
                ]

        let getTraceTagsForCommandStatus (evt: PotionDto.PotionCommandStatusEvent) workflowTag =
            List.concat
                [
                    [
                        ("oddjob.potion.groupId", evt.ClientGroupId)
                        ("oddjob.potion.correlationId", evt.CorrelationId)
                        ("oddjob.potion.command", evt.ClientCommand)
                        ("oddjob.mediaset.jobStatus", getUnionCaseName evt.Status)
                        ("oddjob.workflow", workflowTag)
                    ]
                    match evt.ClientFileId with
                    | Some fileId -> [ ("oddjob.potion.fileId", fileId) ]
                    | _ -> []
                ]

        let saveToEventStore cmd (traceContext: OpenTelemetry.Trace.SpanContext) =
            use _ =
                createTraceSpanForContext mailbox traceContext "potionHandlerActor.saveToEventStore"
                <| getTraceTagsForCommand cmd "Saved to EventStore"
            aprops.SaveToEventStore cmd

        let rec init state =
            if state = PotionSetState.Zero then
                logDebug mailbox "init"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? PotionPersistence.IProtoBufSerializableEvent as event -> init (applyPersistentEvent state event)
                    | :? RecoveryCompleted ->
                        logDebug mailbox "Persistence recovery completed"
                        if aprops.CorrectGeoBlockOnActivation() then
                            match state.Group with
                            | Some group when group.GeoBlock = GeoRestriction.Unspecified && Seq.isNotEmpty state.Files ->
                                let geoBlockCommand = createDefaultGeoBlockCommand group.GroupId |> QueueMessage.create
                                mailbox.Self <! geoBlockCommand
                            | Some group when group.CorrectGeoBlock <> GeoRestriction.Unspecified && group.GeoBlock <> group.CorrectGeoBlock ->
                                let geoBlockCommand = createCorrectGeoBlockCommand group.GroupId group.CorrectGeoBlock |> QueueMessage.create
                                mailbox.Self <! geoBlockCommand
                            | _ -> ()
                        let pendingCommands, cancelledCommands = reducePendingCommands state
                        retryCommands pendingCommands
                        cancelCommands cancelledCommands
                        idle
                            { state with
                                PendingCommands = pendingCommands
                            }
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | PersistentLifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }
        and idle state =
            logDebug mailbox "idle"
            mailbox.UnstashAll()
            actor {
                let! (message: obj) = mailbox.Receive()
                logDebug mailbox $"idle message: {message}"
                return!
                    match message with
                    | :? QueueMessage<PotionCommand> as message' ->
                        let cmd = message'.Payload
                        logDebug mailbox $"Queue message: {cmd}"
                        let traceContext = getTraceContext mailbox
                        let traceSpan =
                            createTraceSpanForContext mailbox traceContext "QueueMessage.PotionCommand"
                            <| getTraceTagsForCommand cmd "Received queued command"
                        throwIfCommandInvalid cmd
                        if isOutdatedCommand cmd state then
                            if not (cmd.GetCorrelationId() |> CorrelationId.isInternal) then
                                saveToEventStore cmd traceContext
                            sendAck mailbox message'.Ack "Outdated command is skipped"
                            ignored ()
                        else
                            let persistentCommands =
                                createPersistentCommands state message'.Payload (aprops.CreateCommandId message'.Payload) message'.Ack
                            mailbox.Self <! persistentCommands
                            let persistentCommandsCount = persistentCommands.PendingCommands.Length + persistentCommands.ExecutingCommands.Length
                            persist_commands state persistentCommands.ExecutingCommands persistentCommandsCount message'.Ack traceContext traceSpan
                    | :? GetPotionSetState ->
                        mailbox.Sender() <! PotionPersistence.PotionSetState.FromDomain state
                        ignored ()
                    | :? Dto.Events.OddjobEventDto as event ->
                        match PotionDto.PotionCommandStatusEvent.tryCreate state event with
                        | Some status ->
                            let traceContext = getTraceContext mailbox
                            use _ =
                                createTraceSpanOnConditionForContext mailbox traceContext "CommandStatusEvent"
                                <| getTraceTagsForCommandStatus status "Received command status"
                                <| fun () ->
                                    match event with
                                    | OddjobEventDto.CommandStatus status' when status'.Status.IsFinal() -> true
                                    | _ -> false
                            if state.ShouldSkipNotifications then
                                logDebug mailbox $"Skipped publishing of [{status}] due to event source filtering"
                                let completedCommands = getCompletedCommands state status
                                update_completed_commands state completedCommands
                            else
                                match PotionDto.PotionCommandStatusEvent.validateDetails status with
                                | Result.Ok() ->
                                    let completedCommands = getCompletedCommands state status
                                    match tryCreateNotificationMessage status with
                                    | Some message ->
                                        use _ =
                                            createTraceSpanForContext mailbox traceContext "NotificationPublisher"
                                            <| getTraceTagsForCommandStatus status "Publishing notification"
                                        aprops.NotificationPublisher <! PublishOne(message, (), retype mailbox.Self)
                                        awaiting_queue_result state completedCommands
                                    | None -> update_completed_commands state completedCommands
                                | Result.Error error ->
                                    logWarning mailbox error
                                    ignored ()
                        | None -> ignored ()
                    | :? PotionPersistence.CompletedCommand as event -> idle (applyPersistentEvent state event)
                    | :? PotionPersistence.CancelledCommand as event -> idle (applyPersistentEvent state event)
                    | :? RetryCommand as cmd ->
                        let (RetryCommand cmd) = cmd
                        let state =
                            { state with
                                PendingCommands = state.PendingCommands |> PendingCommands.bumpRetryCount cmd.CommandId
                            }
                        match cmd.Command with
                        | SetGeoBlock _ when state.ShouldNotRetryGeoBlockCommand -> idle state
                        | _ -> retry_command state cmd
                    | :? CancelCommands as commands ->
                        let (CancelCommands cancelledCommands) = commands
                        update_cancelled_commands state cancelledCommands
                    | :? CompletedCommands as commands ->
                        let (CompletedCommands commands) = commands
                        commands |> List.map box |> persistEvents :> Effect<_>
                    | :? CancelledCommands as commands ->
                        let (CancelledCommands commands) = commands
                        commands |> List.map box |> persistEvents :> Effect<_>
                    | :? PotionBump ->
                        logDebug mailbox "Received PotionBump reminder"
                        let pendingCommands, cancelledCommands = reducePendingCommands state
                        retryCommands pendingCommands
                        cancelCommands cancelledCommands
                        let state =
                            { state with
                                PendingCommands = pendingCommands
                            }
                        stopReminderOnCompletionOrExpiry state
                        idle state
                    | Reminders.LifetimeEvent e ->
                        logDebug mailbox $"Reminder lifetime event {e}"
                        ignored ()
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | _ -> unhandled ()
            }
        and persist_commands state executingCommands pendingCount ack traceContext traceSpan =
            logDebug mailbox "persist_commands"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? PersistentCommands as msg ->
                        let events =
                            [
                                msg.PendingCommands |> List.map (PotionPersistence.PendingCommand.FromDomain >> box)
                                msg.ExecutingCommands |> List.map (_.Event >> box)
                            ]
                            |> List.concat
                        persistEvents events :> Effect<_>
                    | :? PotionPersistence.IProtoBufSerializableEvent as event ->
                        let state = (applyPersistentEvent state event)
                        let state =
                            // In case we have commands to retry we should not execute geoblock changes in case there is a fresh geoblock command
                            if event :? PotionPersistence.AssignedGeoBlock then
                                { state with
                                    ShouldNotRetryGeoBlockCommand = true
                                }
                            else
                                state
                        let pendingCount = pendingCount - 1
                        if pendingCount > 0 then
                            persist_commands state executingCommands pendingCount ack traceContext traceSpan
                        else
                            traceSpan.Dispose()
                            execute_commands state executingCommands ack traceContext
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and execute_commands state executingCommands ack traceContext =
            logDebug mailbox "execute_commands"
            use _ = createTraceSpanForContext mailbox traceContext "potionHandlerActor.executeCommands" []
            let forwardedFrom = ack |> Option.bind _.Tag.ForwardedFrom
            let dispatchingCommands =
                executingCommands
                |> List.map (fun cmd ->
                    {
                        Command = cmd.Command
                        CommandId = cmd.CommandId
                    })
            let isCleanupScheduled = createAndDispatchJob state dispatchingCommands forwardedFrom traceContext
            // If there are multiple commands, only the last one comes from Potion, previous is internal
            let commandToStore = executingCommands |> List.last |> _.Command
            if not (commandToStore.GetCorrelationId() |> CorrelationId.isInternal) then
                saveToEventStore commandToStore traceContext
            match state.Group with
            | Some group when aprops.CompletionReminderInterval > TimeSpan.MinValue ->
                scheduleCompletionReminder (getShortGroupId group.GroupId) traceContext
                awaiting_reminders_acks state ack traceContext
            | _ ->
                sendAck mailbox ack "Command is handled"
                idle state
        and retry_command state cmd =
            logDebug mailbox "retry_command"
            let dispatchingCommand =
                {
                    Command = cmd.Command
                    CommandId = cmd.CommandId
                }
            let traceContext = getTraceContext mailbox
            createAndDispatchJob state [ dispatchingCommand ] cmd.ForwardedFrom traceContext
            idle state
        and awaiting_reminders_acks state ack traceContext =
            logDebug mailbox $"awaiting_reminders_acks"
            let traceSpan = createTraceSpanForContext mailbox traceContext "potionHandlerActor.awaitingCompletionReminder" []
            mailbox.SetReceiveTimeout aprops.ReminderAckTimeout
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? Reminders.ReminderCreated ->
                        logDebug mailbox $"Received reminder ack ({message})"
                        mailbox.SetReceiveTimeout None
                        traceSpan.Dispose()
                        sendAck mailbox ack "Command is handled"
                        idle state
                    | Reminders.LifetimeEvent e ->
                        logDebug mailbox $"Reminder lifetime event {e}"
                        ignored ()
                    | :? ReceiveTimeout ->
                        traceSpan.Dispose()
                        raise <| TimeoutException "Timeout awaiting reminder ack"
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and awaiting_queue_result state completedCommands =
            logDebug mailbox $"awaiting_queue_result for {completedCommands.Length} completed commands"
            mailbox.SetReceiveTimeout aprops.ExternalRequestTimeout
            mailbox.UnstashAll()
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? GetPotionSetState ->
                        mailbox.Sender() <! PotionPersistence.PotionSetState.FromDomain state
                        ignored ()
                    | :? QueuePublishResult as result ->
                        mailbox.SetReceiveTimeout None
                        match result with
                        | Ok _ ->
                            logDebug mailbox "Queue messages successfully published"
                            update_completed_commands state completedCommands
                        | Error(exn, _) ->
                            logErrorWithExn mailbox exn "Error while publishing queue messages"
                            idle state
                    | :? ReceiveTimeout ->
                        let message = "Timeout awaiting queue result"
                        logError mailbox message
                        mailbox.SetReceiveTimeout None
                        raise <| TimeoutException message
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and update_completed_commands state completedCommands =
            match completedCommands with
            | [] -> idle state
            | commands ->
                logDebug mailbox $"Completed commands: {commands |> List.map _.CommandId}"
                let completedCommands =
                    commands
                    |> List.map (fun command -> PotionPersistence.CompletedCommand.FromDomain(command, DateTimeOffset.Now))
                    |> CompletedCommands
                mailbox.Self <! box completedCommands
                idle state
        and update_cancelled_commands state cancelledCommands =
            match cancelledCommands with
            | [] -> idle state
            | commands ->
                logDebug mailbox $"Cancelled commands: {commands |> List.map _.CommandId}"
                let cancelledCommands =
                    commands
                    |> List.map (fun command -> PotionPersistence.CancelledCommand.FromDomain(command, DateTimeOffset.Now))
                    |> CancelledCommands
                mailbox.Self <! box cancelledCommands
                idle state

        init PotionSetState.Zero

    let potionShardActor potionHandlerProps (mailbox: Actor<_>) =

        let potionActorName = $"potion:%s{mailbox.Self.Path.Name}"
        let potionHandler =
            spawn mailbox potionActorName (propsPersistNamed "potion-command" (potionHandlerActor potionHandlerProps))

        let rec loop () =
            actor {
                let! (message: obj) = mailbox.Receive()
                match message with
                | :? QueueMessage<PotionCommand> ->
                    retype potionHandler <<! message
                    ignored ()
                | :? GetPotionSetState ->
                    retype potionHandler <<! message
                    ignored ()
                | :? Dto.Events.OddjobEventDto ->
                    retype potionHandler <<! message
                    ignored ()
                | :? PotionBump ->
                    retype potionHandler <<! message
                    ignored ()
                | LifecycleEvent _ -> ignored ()
                | _ -> unhandled ()
            }

        loop ()
