namespace Nrk.Oddjob.Upload

module MediaSetController =

    open System
    open Akka.Actor
    open Akka.Persistence
    open Akkling
    open Akkling.Persistence

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Core.Queues
    open Nrk.Oddjob.Core.ShardMessages
    open Nrk.Oddjob.Core.PubSub

    open UploadTypes
    open MediaSetStateCache
    open MediaSetStatusPersistence
    open MediaSetPublisher

    /// Used only for bootstrapping
    type IOriginSpecificUploadActorPropsFactory =
        abstract member GetUploadActorProps: MediaSetId -> IActorRef<MediaSetCommand> -> ClientRef -> ClientContentId -> Props<obj>

    type IUploadActorPropsFactory =
        /// May return None, because upload is configurable via config files
        abstract member GetUploadActorProps: Origin -> MediaSetId -> IActorRef<MediaSetCommand> -> ClientRef -> ClientContentId -> Props<obj> option

    [<NoEquality; NoComparison>]
    type MediaSetControllerProps =
        {
            AkkaConnectionString: string
            UploadActorPropsFactory: IUploadActorPropsFactory
            Origins: Origin list
            ActionEnvironment: ActionEnvironment
            PersistMediaSetStatus: StatusUpdateCommand -> unit
            PersistMediaSetState: MediaSetStateCacheCommand -> unit
            MediaSetStatusPublisher: IPublishMediaSetStatus
            MediaSetResourceStatePublisher: IPublishMediaSetResourceState
            StartCompletionReminder: MediaSetId -> (MediaSetId -> (string -> unit) -> unit) option
            UploadScheduler: IActorRef
            PsScheduler: IActorRef
            ClientRef: ClientRef
            FileSystem: IFileSystemInfo
            DestinationRoot: string
            ArchiveRoot: string
            AlternativeArchiveRoot: string
            SubtitlesSourceRootWindows: string
            SubtitlesSourceRootLinux: string
            PassivationTimeoutOnActivation: TimeSpan option
            PassivationTimeoutOnCommands: TimeSpan option
            ReminderAckTimeout: TimeSpan option
            PathMappings: OddjobConfig.PathMapping list
            StorageCleanupDelay: TimeSpan
            ActivityContext: ActivitySourceContext
        }

    // This exception is raised in the actor persistent events need migration, but its state is recovered from a snapshot,
    // So the snapshot is deleted and the actor restarted
    exception RestartedForMigrationException of unit

    [<Literal>]
    let DeletionBatchSize = 25

    [<RequireQualifiedAccess>]
    type StatusUpdates =
        | Enabled
        | Disabled

    type private SessionState =
        {
            Since: DateTimeOffset
            StatusUpdates: StatusUpdates
            PassivationTimeout: TimeSpan option
            RestoredSnapshot: bool
            SavedSnapshot: bool
            DeprecatedEvents: int64 list
            ContentIsRepaired: bool
            TraceContext: OpenTelemetry.Trace.SpanContext option
        }

        static member Zero =
            {
                Since = DateTimeOffset.Now
                StatusUpdates = StatusUpdates.Disabled
                PassivationTimeout = None
                RestoredSnapshot = false
                SavedSnapshot = false
                DeprecatedEvents = List.empty
                ContentIsRepaired = false
                TraceContext = None
            }

    let mediaSetControllerActor (aprops: MediaSetControllerProps) (uploadMediator: IActorRef<_>) (mailbox: Eventsourced<_>) =

        logDebug mailbox "Spawned MediaSetController actor"

        let tryGetChildActor (origin: Origin) =
            let actor = mailbox.UntypedContext.Child(makeActorName [ getUnionCaseName origin ])
            if actor.IsNobody() then None else Some actor

        let getOriginState (state: MediaSetState) origin =
            match origin with
            | Origin.GlobalConnect -> OriginState.GlobalConnect state.Current.GlobalConnect

        let createRepairJob requestSource mediaSetId priority =
            let header =
                {
                    RequestSource = requestSource
                    ForwardedFrom = None
                    Priority = priority
                    Timestamp = DateTimeOffset.Now
                    OverwriteMode = OverwriteMode.IfNewer
                }
            MediaSetJob.RepairMediaSet
                {
                    Header = header
                    MediaSetId = mediaSetId
                }

        let evaluateRemainingActions mediaSetId state =
            let validationSettings =
                {
                    ValidationMode = MediaSetValidation.Local
                    OverwriteMode = OverwriteMode.IfNewer
                    StorageCleanupDelay = aprops.StorageCleanupDelay
                }
            let actionSelection =
                seq {
                    if aprops.Origins |> List.contains Origin.GlobalConnect then
                        ActionSelection.GlobalConnect
                }
                |> List.ofSeq
            let logger = getLoggerForMailbox mailbox
            try
                state |> MediaSetState.getRemainingActions logger actionSelection aprops.ActionEnvironment validationSettings mediaSetId
            with exn ->
                logErrorWithExn mailbox exn $"Failed to evaluate remaining actions, LastSequenceNr: {mailbox.LastSequenceNr()}"
                RemainingActions.Zero

        let evaluateMediaSetStatus state (remainingActions: RemainingActions) =
            if aprops.Origins |> List.contains Origin.GlobalConnect then
                remainingActions.GlobalConnect |> GlobalConnectActions.evaluateCompletionStatus state
            else
                MediaSetStatus.Completed

        let mediaSetStatusData (mediaSetId: MediaSetId) mediaSetStatus =
            {
                MediaSetId = mediaSetId.Value
                Status = MediaSetStatus.toInt mediaSetStatus
                Timestamp = DateTime.Now
            }

        let mediaSetStateData (mediaSetId: MediaSetId) mediaSetState =
            {
                MediaSetId = mediaSetId.Value
                State = Dto.MediaSet.MediaSetState.FromDomain mediaSetState
                Timestamp = DateTime.Now
            }

        let tryGetOriginUploadActor mediaSetId (state: MediaSetState) origin =
            aprops.UploadActorPropsFactory.GetUploadActorProps origin mediaSetId (retype mailbox.Self) aprops.ClientRef (ClientContentId state.ClientContentId)
            |> Option.map (fun props ->
                tryGetChildActor origin
                |> Option.defaultWith (fun () ->
                    let actorName = makeActorName [ getUnionCaseName origin ]
                    let actor = spawn mailbox actorName <| props
                    monitor mailbox actor)
                |> typed)

        let forwardJobToOrigins (job: MediaSetJob) (state: MediaSetState) origins ack traceContext =
            let traceContext = traceContext |> Option.defaultValue (getTraceContext mailbox)
            origins
            |> List.iter (fun origin ->
                let traceTags =
                    [
                        ("oddjob.mediaset.mediaSetId", job.MediaSetId.Value)
                        ("oddjob.mediaset.command", job.CommandName)
                    ]
                use _ = createTraceSpanForContext mailbox traceContext $"mediaSetController.forwardJob:{job.CommandName}" traceTags
                let originState = getOriginState state origin
                let msg =
                    Message.createWithAck
                        {
                            ExecutionContext = MediaSetJobContext.fromMediaSetJob job |> ExecutionContext.MediaSetJob
                            DesiredState = state.Desired
                            OriginState = originState
                        }
                        ack
                match tryGetOriginUploadActor job.MediaSetId state origin with
                | Some uploadActor -> uploadActor <! msg
                | None -> logWarning mailbox $"Upload factory for origin {origin} not found")

        let forwardOriginStateUpdateToOrigin event (newState: MediaSetState) mediaSetId origins traceContext =
            let traceContext = traceContext |> Option.defaultValue (getTraceContext mailbox)
            origins
            |> List.iter (fun origin ->
                match event with
                | OriginResourceStateUpdateEvent origin (resourceRef, resourceState) ->
                    match resourceState with
                    | DistributionState.Completed
                    | DistributionState.Deleted ->
                        let originState = getOriginState newState origin
                        match tryGetOriginUploadActor mediaSetId newState origin with
                        | Some uploadActor ->
                            let traceTags = [ ("oddjob.mediaset.mediaSetId", mediaSetId.Value) ]
                            use _ = createTraceSpanForContext mailbox traceContext $"mediaSetController.forwardOriginStateUpdate:{event}" traceTags
                            let msg =
                                Message.create
                                    {
                                        ExecutionContext =
                                            OriginStateUpdateContext.create mediaSetId resourceRef resourceState |> ExecutionContext.OriginStateUpdate
                                        DesiredState = newState.Desired
                                        OriginState = originState
                                    }
                            uploadActor <! msg
                        | None -> logWarning mailbox $"Upload factory for origin {origin} not found"
                    | _ -> ()
                | _ -> ())

        let dispatchJob (job: MediaSetJob) state jobOrigins ack traceContext =
            let remainingActions = evaluateRemainingActions job.MediaSetId state
            let mediaSetStatus = evaluateMediaSetStatus state remainingActions
            let data = (mediaSetStatusData job.MediaSetId mediaSetStatus, job.Header.Priority)
            let cmd =
                match job with
                | MediaSetJob.RepairMediaSet _ -> SetStatusOnRepair data
                | _ -> SetStatusOnActivation data
            aprops.PersistMediaSetStatus cmd
            forwardJobToOrigins job state jobOrigins ack traceContext
            sendAck mailbox ack "Job is dispatched to origins"

        let reschedulePassivation (sessionState: SessionState) newTimeout =
            // Never reduce timeout once it's set
            let timeout =
                match sessionState.PassivationTimeout, newTimeout with
                | Some x, Some y when y >= x -> Some y
                | None, y -> y
                | _ -> None
            match timeout with
            | Some timeout ->
                mailbox.SetReceiveTimeout(Some timeout)
                { sessionState with
                    PassivationTimeout = Some timeout
                }
            | None -> sessionState

        let publishMediaSetStatusUpdate state mediaSetId timestamp =
            let remainingActions = evaluateRemainingActions mediaSetId state
            let mediaSetStatus = evaluateMediaSetStatus state remainingActions
            aprops.PersistMediaSetStatus <| SetStatusOnUpdate(mediaSetStatusData mediaSetId mediaSetStatus)
            aprops.MediaSetStatusPublisher.UpdateMediaSetStatus
                {
                    MediaSetId = mediaSetId.Value
                    Status = MediaSetStatus.toInt mediaSetStatus
                    RemainingActions = Dto.MediaSet.RemainingActions.fromDomain remainingActions
                    Timestamp = timestamp
                }

        let publishPlayabilityEvent msg =
            match aprops.ClientRef with
            | ClientRef.Potion _ -> ()

        let shouldPublishRemoteStateChange distributionState oldDistributionState =
            match distributionState, oldDistributionState with
            | x, y when x = y -> false
            | _, DistributionState.None -> true
            | DistributionState.Completed, _
            | DistributionState.Rejected, _
            | DistributionState.Deleted, _ -> true
            | _, DistributionState.Completed
            | _, DistributionState.Rejected
            | _, DistributionState.Deleted -> true
            | _, _ -> false

        let publishOnDemandPlaybackEvent state (mediaSetId: MediaSetId) timestamp priority traceContext =
            if mediaSetId.ClientId = Alphanumeric PsClientId then
                use _ =
                    createTraceSpanForContext
                        mailbox
                        (traceContext |> Option.defaultValue (getTraceContext mailbox))
                        "mediaSetController.publishOnDemandPlaybackEvent"
                        [ ("oddjob.mediaset.mediaSetId", mediaSetId.Value) ]
                logDebug mailbox "Sending on-demand playback event"
                Events.MediaSetPlayabilityEvent.createOnDemand mediaSetId state aprops.Origins timestamp priority
                |> Dto.Events.MediaSetPlayabilityEventDto.fromDomain
                |> publishPlayabilityEvent

        let tryResolveClientContentId mediaSetId state =
            if mediaSetId.ClientId = Alphanumeric PotionClientId then
                let clientContentId =
                    match Guid.TryParse(mediaSetId.ContentId.Value) with
                    | true, clientContentId -> clientContentId.ToString()
                    | _ -> mediaSetId.ContentId.Value
                { state with
                    ClientContentId = state.ClientContentId |> Option.defaultValue clientContentId |> Some
                }
            else
                state

        let forceLegacySubtitlesOverwrite state job =
            let replaceHeader job =
                MediaSetJob.PublishSubtitles
                    { job with
                        Header.OverwriteMode = OverwriteMode.Always
                    }
            match job with
            | MediaSetJob.PublishSubtitles job' ->
                match state.Desired.Content with
                | ContentSet.Parts(_, part) when part.Subtitles = job'.SubtitlesFiles -> replaceHeader job'
                | _ -> job
            | _ -> job

        let publishOnDemandPlaybackEventOnStateChange
            newMediaSetState
            oldMediaSetState
            (newRemoteState: RemoteState)
            (oldRemoteState: RemoteState option)
            mediaSetId
            timestamp
            traceContext
            =
            let shouldPublish =
                match oldRemoteState with
                | Some oldRemoteState ->
                    (CurrentMediaSetState.isPlayable newMediaSetState.Current aprops.Origins
                     || CurrentMediaSetState.isPlayable oldMediaSetState.Current aprops.Origins)
                    && shouldPublishRemoteStateChange oldRemoteState.State newRemoteState.State
                | None -> true
            if shouldPublish then
                publishOnDemandPlaybackEvent newMediaSetState mediaSetId timestamp Events.PlayabilityEventPriority.Normal traceContext

        let handleOriginEvent oldState newState mediaSetId (event: MediaSetEvent) traceContext =
            let timestamp = DateTimeOffset.Now
            forwardOriginStateUpdateToOrigin event newState mediaSetId aprops.Origins traceContext
            publishMediaSetStatusUpdate newState mediaSetId timestamp
            match event with
            | MediaSetEvent.ReceivedRemoteFileState(origin, fileRef, remoteState, remoteResult) ->
                let msg = MediaSetRemoteFileUpdate.fromDomain mediaSetId origin fileRef remoteState remoteResult timestamp
                aprops.MediaSetResourceStatePublisher.ReceivedRemoteResourceState msg
                let fileState = oldState.Current |> CurrentMediaSetState.tryGetRemoteFileState origin fileRef
                publishOnDemandPlaybackEventOnStateChange newState oldState remoteState fileState mediaSetId timestamp traceContext
            | MediaSetEvent.ReceivedRemoteSubtitlesFileState(origin, subRef, remoteState, remoteResult) ->
                let msg = MediaSetRemoteSubtitlesUpdate.fromDomain mediaSetId origin subRef remoteState remoteResult timestamp
                aprops.MediaSetResourceStatePublisher.ReceivedRemoteResourceState msg
                let subState = oldState.Current |> CurrentMediaSetState.tryGetRemoteSubtitlesState subRef
                publishOnDemandPlaybackEventOnStateChange newState oldState remoteState subState mediaSetId timestamp traceContext
            | MediaSetEvent.ReceivedRemoteSmilState(origin, _, remoteState, remoteResult) when origin = Origin.GlobalConnect ->
                let msg =
                    MediaSetRemoteSmilUpdate.fromDomain mediaSetId origin newState.Current.GlobalConnect.Smil.Smil.Version remoteState remoteResult timestamp
                aprops.MediaSetResourceStatePublisher.ReceivedRemoteResourceState msg
                let smilState = Some oldState.Current.GlobalConnect.Smil.RemoteState
                publishOnDemandPlaybackEventOnStateChange newState oldState remoteState smilState mediaSetId timestamp traceContext
            | _ -> ()

        let applyPersistentEvent (event: obj) state (mediaSetId: MediaSetId option) (sessionState: SessionState) =
            let event = event :?> Dto.IProtoBufSerializableEvent |> PersistentEvent.toDomain
            let oldState = state
            let state =
                { MediaSetState.update event state with
                    LastSequenceNr = mailbox.LastSequenceNr()
                }
            mediaSetId
            |> Option.iter (fun mediaSetId ->
                aprops.PersistMediaSetState <| SaveState(mediaSetStateData mediaSetId state)
                match event with
                | CurrentStateEvent -> handleOriginEvent oldState state mediaSetId event sessionState.TraceContext
                | _ -> ())
            let passivationTimeout = mediaSetId |> Option.bind (fun _ -> aprops.PassivationTimeoutOnCommands)
            if state.LastSequenceNr % int64 SnapshotFrequency = 0L then
                mailbox.Self <! box (TakeSnapshotCommand state.LastSequenceNr)
            let sessionState = reschedulePassivation sessionState passivationTimeout
            state, sessionState

        let persistEvent event =
            logDebug mailbox $"Persisting event %s{getUnionCaseName event} (LastSequenceNr = {mailbox.LastSequenceNr()})"
            event |> PersistentEvent.fromDomain |> Persist :> Effect<_>

        let applyCommand _ cmd =
            logDebug mailbox $"Applying command %s{getUnionCaseName cmd}"
            MediaSetEvent.fromCommand cmd |> persistEvent

        let passivate reason (sessionState: SessionState) =
            logDebug mailbox $"Passivating (reason: %s{reason}), lifetime: {DateTimeOffset.Now - sessionState.Since}"
            Akkling.Cluster.Sharding.ClusterSharding.passivate ()

        let handleSnapshotCommand state (message: obj) =
            match message with
            | :? TakeSnapshotCommand ->
                logDebug mailbox $"Taking snapshot at {mailbox.LastSequenceNr()}"
                Dto.MediaSet.MediaSetState.FromDomain state |> box |> SaveSnapshot :> Effect<_>
            | :? DeleteSnapshotsCommand as msg ->
                let (DeleteSnapshotsCommand maxSequenceNr) = msg
                logDebug mailbox $"Deleting snapshots up to {maxSequenceNr}"
                SnapshotSelectionCriteria(maxSequenceNr) |> DeleteSnapshots :> Effect<_>
            | _ -> unhandled ()

        let handleSnapshotEvent (message: obj) =
            match message with
            | :? SaveSnapshotSuccess as msg ->
                mailbox.Self <! box (DeleteSnapshotsCommand(msg.Metadata.SequenceNr - 1L))
                ignored ()
            | :? SaveSnapshotFailure as e ->
                logErrorWithExn mailbox e.Cause "Error saving snapshot"
                ignored ()
            | :? DeleteSnapshotsSuccess -> ignored ()
            | :? DeleteSnapshotsFailure as e ->
                logErrorWithExn mailbox e.Cause "Error deleting snapshots"
                ignored ()
            | _ -> unhandled ()

        let rec init state sessionState =
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? Dto.IProtoBufSerializableEvent as event when mailbox.IsRecovering() ->
                        try
                            let domainEvent =
                                try
                                    event |> PersistentEvent.toDomain
                                with exn ->
                                    logErrorWithExn mailbox exn $"Skipping unsupported event: {exn} (LastSequenceNr = {mailbox.LastSequenceNr()})"
                                    MediaSetEvent.Deprecated
                            let sessionState =
                                match domainEvent with
                                | DeprecatedEvent _ ->
                                    { sessionState with
                                        DeprecatedEvents = mailbox.LastSequenceNr() :: sessionState.DeprecatedEvents
                                    }
                                | _ -> sessionState
                            let state =
                                { MediaSetState.update domainEvent state with
                                    LastSequenceNr = mailbox.LastSequenceNr()
                                }
                            init state sessionState
                        with exn ->
                            logErrorWithExn mailbox exn $"Failed to recover actor state (LastSequenceNr = {mailbox.LastSequenceNr()})"
                            reraise ()
                    | Persisted mailbox event when (event :? Dto.IProtoBufSerializableEvent) ->
                        let state, sessionState = applyPersistentEvent event state None sessionState
                        if event :? Dto.MediaSet.SetSchemaVersion then
                            state_recovered state sessionState
                        else
                            init state sessionState
                    | :? RecoveryCompleted ->
                        logDebug mailbox "Persistence recovery completed"
                        if not sessionState.RestoredSnapshot && Seq.isNotEmpty sessionState.DeprecatedEvents then
                            logDebug mailbox $"Found {sessionState.DeprecatedEvents.Length} deprecated events, media set needs migration"
                        if state = MediaSetState.Zero then
                            MediaSetEvent.SetSchemaVersion CurrentSchemaVersion |> persistEvent
                        else if sessionState.RestoredSnapshot || state.LastSequenceNr < SnapshotFrequency then
                            state_recovered (state |> MediaSetState.purgeUnusedResources) sessionState
                        else
                            logDebug mailbox $"Taking snapshot at {mailbox.LastSequenceNr()}"
                            Dto.MediaSet.MediaSetState.FromDomain state |> box |> SaveSnapshot :> Effect<_>
                    | :? SnapshotOffer as s ->
                        logDebug mailbox $"Restoring state from snapshot at {s.Metadata.SequenceNr}"
                        let state = (s.Snapshot :?> Dto.MediaSet.MediaSetState).ToDomain()
                        let sessionState =
                            { sessionState with
                                RestoredSnapshot = true
                            }
                        init state sessionState
                    | :? SaveSnapshotSuccess ->
                        let sessionState =
                            { sessionState with
                                SavedSnapshot = true
                            }
                        state_recovered (state |> MediaSetState.purgeUnusedResources) sessionState
                    | :? SaveSnapshotFailure as msg ->
                        logErrorWithExn mailbox msg.Cause "Failed to create snapshot on recovery"
                        state_recovered (state |> MediaSetState.purgeUnusedResources) sessionState
                    | LifecycleEvent e -> ignored ()
                    | PersistentLifecycleEvent _ -> ignored ()
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and state_recovered state sessionState =
            logDebug mailbox "state_recovered"
            mailbox.UnstashAll()
            actor {
                let! (message: obj) = mailbox.Receive()
                let traceContext = getTraceContext mailbox
                return!
                    match message with
                    // shard messages wrapped in Message come from PS/Potion
                    | :? Message<MediaSetShardMessage> as msg -> handle_shard_message_state_recovered state sessionState msg.Payload
                    // unwrapped shard messages come from reminders
                    | :? MediaSetShardMessage as msg -> handle_shard_message_state_recovered state sessionState msg
                    | :? MediaSetMessage as msg ->
                        match msg with
                        | MediaSetMessage.GetState ->
                            mailbox.Sender() <! Dto.MediaSet.MediaSetState.FromDomain state
                            ignored ()
                    | :? MediaSetCommand as msg -> applyCommand state msg
                    | :? MediaSetEvent as msg -> persistEvent msg
                    | :? ActorLifecycleCommand as msg ->
                        match msg with
                        | ActorLifecycleCommand.KeepAlive ->
                            let sessionState = reschedulePassivation sessionState aprops.PassivationTimeoutOnCommands
                            state_recovered state sessionState
                    | Persisted mailbox event when (event :? Dto.IProtoBufSerializableEvent) ->
                        let state, sessionState = applyPersistentEvent event state None sessionState
                        state_recovered state sessionState
                    | SnapshotCommand cmd -> handleSnapshotCommand state cmd
                    | SnapshotEvent event -> handleSnapshotEvent event
                    | LifecycleEvent _ -> ignored ()
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and handle_shard_message_state_recovered state sessionState message =
            let traceContext = getTraceContext mailbox
            match message with
            | MediaSetShardMessage.GetMediaSetState _ ->
                mailbox.Sender() <! Dto.MediaSet.MediaSetState.FromDomain state
                ignored ()
            | _ ->
                mailbox.Stash()
                let sessionState =
                    { sessionState with
                        TraceContext = Some traceContext
                    }
                operating (state |> tryResolveClientContentId message.MediaSetId) message.MediaSetId aprops.Origins sessionState
        and operating state mediaSetId origins sessionState =
            logDebug mailbox "operating"
            mailbox.UnstashAll()
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? Message<MediaSetShardMessage> as msg -> handle_shard_message_operating state mediaSetId origins sessionState msg.Payload msg.Ack
                    | :? MediaSetShardMessage as msg -> handle_shard_message_operating state mediaSetId origins sessionState msg None
                    | :? MediaSetMessage as msg ->
                        match msg with
                        | MediaSetMessage.GetState ->
                            mailbox.Sender() <! Dto.MediaSet.MediaSetState.FromDomain state
                            ignored ()
                    | :? MediaSetCommand as msg -> applyCommand state msg
                    | :? MediaSetEvent as msg -> persistEvent msg
                    | :? ActorLifecycleCommand as msg ->
                        match msg with
                        | ActorLifecycleCommand.KeepAlive ->
                            let sessionState = reschedulePassivation sessionState aprops.PassivationTimeoutOnCommands
                            operating state mediaSetId origins sessionState
                    | Persisted mailbox event when (event :? Dto.IProtoBufSerializableEvent) ->
                        let state, sessionState = applyPersistentEvent event state (Some mediaSetId) sessionState
                        operating state mediaSetId origins sessionState
                    | SnapshotCommand cmd -> handleSnapshotCommand state cmd
                    | SnapshotEvent event -> handleSnapshotEvent event
                    | Reminders.LifetimeEvent e ->
                        logDebug mailbox $"Reminder lifetime event {e}"
                        ignored ()
                    | :? ReceiveTimeout -> passivate "Timeout" sessionState
                    | LifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }
        and handle_shard_message_operating state mediaSetId origins sessionState message ack =
            logDebug mailbox $"handle_shard_message {getUnionCaseName message}"
            let traceContext = getTraceContext mailbox
            let sessionState =
                { sessionState with
                    TraceContext = Some traceContext
                }
            match message with
            | MediaSetShardMessage.MediaSetJob job ->
                let job = forceLegacySubtitlesOverwrite state job
                let requestedActions, jobOrigins = MediaSetActions.fromMediaSetJob job state.Desired, aprops.Origins
                logDebug mailbox $"Evaluated actions for MediaSet job: {requestedActions}"
                // Filtered actions represent activities to be applied to update the desired state according to the job content
                let filteredActions =
                    requestedActions |> MediaSetActions.filter state.Desired (job.Header.OverwriteMode = OverwriteMode.Always)
                match filteredActions with
                | [] ->
                    // Empty filtered actions means desired state is updated according to requested actions
                    // This is where job processing begins
                    let sessionState =
                        { sessionState with
                            StatusUpdates = StatusUpdates.Enabled
                            TraceContext = Some traceContext
                        }
                    start_completion_reminder state mediaSetId origins sessionState (fun state -> dispatchJob job state jobOrigins ack)
                | actions ->
                    let sessionState =
                        { sessionState with
                            TraceContext = Some traceContext
                        }
                    // Wait until the desired state is updated according to the job content
                    actions |> List.iter (fun msg -> (retype mailbox.Self) <! msg)
                    awaiting_desired_state state mediaSetId origins job jobOrigins ack (actions |> List.map MediaSetEvent.fromCommand) sessionState
            | MediaSetShardMessage.GetMediaSetState _ ->
                mailbox.Sender() <! Dto.MediaSet.MediaSetState.FromDomain state
                ignored ()
        and awaiting_desired_state state mediaSetId origins job jobOrigins ack pendingActions sessionState =
            logDebug mailbox "awaiting_desired_state"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? MediaSetCommand as msg -> applyCommand state msg
                    | :? MediaSetEvent as msg -> persistEvent msg
                    | :? ActorLifecycleCommand as msg ->
                        match msg with
                        | ActorLifecycleCommand.KeepAlive ->
                            let sessionState = reschedulePassivation sessionState aprops.PassivationTimeoutOnCommands
                            awaiting_desired_state state mediaSetId origins job jobOrigins ack pendingActions sessionState
                    | Persisted mailbox event when (event :? Dto.IProtoBufSerializableEvent) ->
                        let state, sessionState = applyPersistentEvent event state (Some mediaSetId) sessionState
                        let domainEvt = event :?> Dto.IProtoBufSerializableEvent |> PersistentEvent.toDomain
                        match domainEvt with
                        | DesiredStateEvent when pendingActions |> List.contains domainEvt ->
                            match pendingActions |> List.filter (fun x -> x <> domainEvt) with
                            | [] ->
                                let sessionState =
                                    { sessionState with
                                        StatusUpdates = StatusUpdates.Enabled
                                    }
                                start_completion_reminder state mediaSetId origins sessionState (fun state -> dispatchJob job state jobOrigins ack)
                            | remainingActions -> awaiting_desired_state state mediaSetId origins job jobOrigins ack remainingActions sessionState
                        | _ -> awaiting_desired_state state mediaSetId origins job jobOrigins ack pendingActions sessionState
                    | SnapshotCommand cmd -> handleSnapshotCommand state cmd
                    | SnapshotEvent event -> handleSnapshotEvent event
                    | LifecycleEvent _ -> ignored ()
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and start_completion_reminder state mediaSetId origins sessionState dispatchJob' =
            logDebug mailbox "start_completion_reminder"
            match aprops.StartCompletionReminder mediaSetId with
            | Some startReminder ->
                let traceContext = sessionState.TraceContext |> Option.defaultValue (getTraceContext mailbox)
                let traceSpan =
                    createTraceSpanForContext
                        mailbox
                        traceContext
                        "mediaSetController.awaitingCompletionReminder"
                        [ ("oddjob.mediaset.mediaSetId", mediaSetId.Value) ]
                startReminder mediaSetId (logDebug mailbox)
                awaiting_reminder_ack state mediaSetId origins sessionState dispatchJob' traceSpan
            | None -> dispatch_job state mediaSetId origins sessionState dispatchJob'
        and awaiting_reminder_ack state mediaSetId origins sessionState dispatchJob' traceSpan =
            logDebug mailbox "awaiting_reminder_ack"
            mailbox.SetReceiveTimeout(aprops.ReminderAckTimeout)
            actor {
                let! message = mailbox.Receive()
                return!
                    match message with
                    | :? Reminders.ReminderCreated ->
                        traceSpan.Dispose()
                        mailbox.SetReceiveTimeout None
                        dispatch_job state mediaSetId origins sessionState dispatchJob'
                    | Reminders.LifetimeEvent e ->
                        logDebug mailbox $"Reminder lifetime event {e}"
                        ignored ()
                    | :? ReceiveTimeout ->
                        traceSpan.Dispose()
                        raise <| TimeoutException "Timeout awaiting reminder ack"
                    | LifecycleEvent _ -> ignored ()
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and dispatch_job state mediaSetId origins sessionState dispatchJob' =
            dispatchJob' state sessionState.TraceContext
            operating state mediaSetId origins sessionState

        init MediaSetState.Zero SessionState.Zero
