namespace Nrk.Oddjob.Ps

open Nrk.Oddjob.Core.Granitt
open Nrk.Oddjob.Ps.PsTypes

module PsFilesHandlerActor =

    open System
    open Akka.Actor
    open Akka.Persistence
    open Akkling
    open Akkling.Persistence

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Core.SchedulerActors
    open Nrk.Oddjob.Core.Queues
    open Nrk.Oddjob.Core.ShardMessages

    open PsTypes
    open PsDto
    open PsJobCreation
    open PsGranittActors
    open Priority
    open Retention
    open Rights
    open PsShardMessages
    open PsFileArchive

    [<NoEquality; NoComparison>]
    type PsFilesHandlerProps =
        {
            SubtitlesConfig: PsSubtitlesConfig
            GetGranittAccess: IActorContext -> IActorRef<GranittCommand>
            MediaSetController: IActorRef<Message<MediaSetShardMessage>>
            ReschedulePsChangeMessage: ScheduleMessage<PsChangeJob> -> unit
            ForceOperationForSources: string list
            MissingFilesRetryInterval: int array
            EnableFileArchivePreparation: bool
            CalculatePriority: CalculatePriority
            RightsAnalyzer: ProgramRightsAnalyzer
            RetentionExceptions: RetentionExceptions
            QueuePublisher: IActorRef<QueuePublisherCommand>
            GetPsMediator: unit -> IActorRef<PsShardMessage>
            ClearMediaSetReminderInterval: TimeSpan
            PsScheduler: IActorRef
            UploadScheduler: IActorRef
            ArchiveConfig: PrepareArchiveConfig
            ArchiveFileLocator: string -> string
            PathMappings: OddjobConfig.PathMapping list
            FileSystem: IFileSystemOperations
            DropFolder: string
            PlayabilityReminderInterval: TimeSpan
            ReminderAckTimeout: TimeSpan
            ExternalRequestTimeout: TimeSpan option
        }

    type private PrepareArchiveResult =
        | ProcessingFailed
        | WaitForFiles

    let handleLifecycle mailbox e =
        match e with
        | PreRestart(exn, message) ->
            logErrorWithExn mailbox exn $"PreRestart due to error when processing message {message.GetType()} [%A{message}]"
            match message with
            | :? QueueMessage<PsChangeJobDto> as message ->
                match exn with
                | :? ArgumentException -> sendReject mailbox message.Ack "Rejecting invalid command"
                | _ -> sendNack mailbox message.Ack "Failed processing PS change message"
                ignored ()
            | _ -> unhandled ()
        | _ -> ignored ()

    type private PersistRightsCommand = PersistRightsCommand
    type private PersistVideoTranscodingDetailsCommand = PersistVideoTranscodingDetailsCommand of PsVideoTranscodingJob
    type private PersistVideoMigrationDetailsCommand = PersistVideoMigrationDetailsCommand of PsLegacyVideoDetails
    type private PersistAudioTranscodingDetailsCommand = PersistAudioTranscodingDetailsCommand of PsAudioTranscodingJob
    type private PersistAudioMigrationDetailsCommand = PersistAudioMigrationDetailsCommand of PsLegacyAudioDetails
    type private PersistTvSubtitlesJobCommand = PersistTvSubtitlesJobCommand of PsSubtitleFilesDetails
    type private PersistActiveCarrierEvent = PersistActiveCarrierEvent
    type private PersistActiveCarrierStatus = PersistActiveCarrierStatus
    type private PersistArchiveCommand = PersistArchiveCommand of PsArchivedFile list
    type private PersistUsageRightsEvent = PersistUsageRightsEvent

    let psFilesHandlerActor (aprops: PsFilesHandlerProps) (psMediator: IActorRef<obj>) (mailbox: Eventsourced<_>) =
        let granitt = aprops.GetGranittAccess mailbox.UntypedContext
        let jobCreator = PsJobCreator(aprops.ForceOperationForSources, aprops.SubtitlesConfig, logDebug mailbox)

        let scheduleRightsChange (job: PsChangeJob) scheduledTime ack =
            aprops.ReschedulePsChangeMessage
                {
                    Message = job
                    Id = job.PiProgId
                    TriggerDateTime = scheduledTime
                    Ack = ack
                }

        let tryScheduleCleanup (job: PsChangeJob) rights forwardedFrom =
            let piProgId = PiProgId.create job.PiProgId
            let scheduleRetentionCleanup publishEnd =
                let mediaSetId =
                    {
                        ClientId = Alphanumeric PsClientId
                        ContentId = Helpers.toContentId piProgId
                    }
                let job = MediaSetRetention.createRetentionJob (publishEnd - DateTime.Now)
                let interval = aprops.ClearMediaSetReminderInterval
                let message = MediaSetShardMessage.ClearMediaSetReminder mediaSetId.Value
                Reminders.scheduleRepeatingReminderTask
                    aprops.UploadScheduler
                    Reminders.ClearMediaSet
                    mediaSetId.Value
                    message
                    aprops.MediaSetController.Path
                    job.TriggerTime
                    interval
                    (logDebug mailbox)

            let forcedCleanupTime = RetentionExceptions.calculateExpiration aprops.RetentionExceptions piProgId job.Source forwardedFrom
            let result =
                match forcedCleanupTime, rights.ScheduleCleanup with
                | RetentionExpiration.Expire publishEnd, None ->
                    scheduleRetentionCleanup publishEnd
                    true
                | RetentionExpiration.Expire publishEnd, Some cleanupDate when publishEnd < cleanupDate && publishEnd > DateTime.Now ->
                    scheduleRetentionCleanup publishEnd
                    true
                | _ -> false

            rights.ScheduleCleanup
            |> Option.iter (fun scheduledTime ->
                logDebug mailbox $"Scheduled removing of program files for program %A{piProgId} due to rights expiration until %A{scheduledTime}"
                scheduleRightsChange job scheduledTime None)
            result

        let dispatchJob filesState rights (job: PsChangeJob) ack traceContext =
            let piProgId = PiProgId.create job.PiProgId
            try
                let forwardedFrom = ack |> Option.bind (_.Tag.ForwardedFrom)
                let priority =
                    aprops.CalculatePriority(job.Source, PublishedStatus.ActiveSince rights.FirstPublishStart, job.PublishingPriority)
                let job =
                    jobCreator.CreatePublishMediaSetJob filesState aprops.ArchiveFileLocator rights job.Source forwardedFrom (int priority) piProgId
                use _ =
                    createTraceSpanForContext mailbox traceContext "psFilesHandlerActor.dispatchJob" [ ("oddjob.ps.programId", PiProgId.value piProgId) ]
                logDebug mailbox $"Created upload job {job} for program {piProgId}"
                aprops.MediaSetController <! Message.createWithAck (MediaSetShardMessage.MediaSetJob job) ack
            with
            | :? OverflowException as exn ->
                logErrorWithExn mailbox exn "Failed to create PublishMediaSet job due to invalid job data"
                sendReject mailbox ack "Invalid PublishMediaSet job data"
            | exn ->
                logErrorWithExn mailbox exn $"Failed to create PublishMediaSet job ({exn})"
                sendNack mailbox ack $"{exn}"

        let scheduleUpload (message: QueueMessage<PsChangeJob>) (scheduledTime: DateTime) =
            scheduleRightsChange message.Payload scheduledTime message.Ack

        let createPsChangeDto piProgId source publishingPriority : PsChangeJobDto =
            {
                ProgrammeId = piProgId |> PiProgId.value
                Source = source
                TimeStamp = DateTime.Now
                RetryCount = None
                PublishingPriority = Option.ofObj publishingPriority
            }

        let createProgramCarrier carrierId =
            {
                PsProgramCarrier.CarrierId = carrierId
                PartNumber = 1
            }

        let createPsChangeQueueMessage piProgId source publishingPriority =
            createPsChangeDto piProgId source publishingPriority
            |> Serialization.Newtonsoft.serializeObject SerializationSettings.PascalCase
            |> OutgoingMessage.create

        let applyPersistentEvent (state: PsFilesState) (event: PsPersistence.IProtoBufSerializableEvent) =
            try
                let state =
                    match event with
                    | :? PsPersistence.AssignedProgramId as event ->
                        let piProgId = event.ToDomain()
                        { state with ProgramId = Some piProgId }
                    | :? PsPersistence.AssignedMediaType as event ->
                        let mediaType = event.ToDomain()
                        { state with
                            MediaType = Some mediaType
                        }
                    | :? PsPersistence.ReceivedVideoTranscodingJob as event ->
                        let job = event.ToDomain()
                        let carrierId = job.CarrierIds |> List.head |> CarrierId.create
                        match state.FileSets |> Map.tryFind carrierId with
                        | Some fs when job.Version <= fs.Version ->
                            logDebug mailbox $"Skipping job with version {job.Version} not greater than {fs.Version}"
                            state
                        | _ ->
                            let fileSet =
                                {
                                    ProgramId =
                                        match job.ProgramId, state.ProgramId with
                                        | null, Some programId -> PiProgId.value programId
                                        | _, _ -> job.ProgramId
                                    Carriers = job.Carriers
                                    TranscodedFiles = PsTranscodedFiles.Video job.Files
                                    PublishingPriority = job.PublishingPriority
                                    Version = job.Version
                                }
                            let fileSets = state.FileSets |> Map.add carrierId fileSet
                            // Filter out subtitles with transcoding version that is lower than versions in the current file set
                            let transcodingVersions = fileSets |> Map.values |> Seq.map _.Version
                            let subtitleSets =
                                state.SubtitlesSets
                                |> List.choose (function
                                    | SubtitlesSet subset when
                                        transcodingVersions |> Seq.contains subset.TranscodingVersion || subset.TranscodingVersion > Seq.max transcodingVersions
                                        ->
                                        Some(SubtitlesSet subset)
                                    | LegacySubtitlesSet subs -> Some(LegacySubtitlesSet subs)
                                    | _ -> None)
                            { state with
                                MediaType = Some MediaType.Video
                                FileSets = fileSets
                                SubtitlesSets = subtitleSets
                            }
                    | :? PsPersistence.ReceivedVideoMigrationJob as event ->
                        let job = event.ToDomain()
                        match state.FileSets |> Map.tryFind (CarrierId.create job.CarrierId) with
                        | Some fs when fs.TranscodedFiles.IsVideo -> // migration should not overwrite regular transcoding
                            logDebug mailbox $"Skipping job with existing files for carrier {fs.CarrierIds}"
                            state
                        | _ ->
                            let fileSet =
                                {
                                    ProgramId =
                                        match job.ProgramId, state.ProgramId with
                                        | null, Some programId -> PiProgId.value programId
                                        | _, _ -> job.ProgramId
                                    Carriers = [ PsTranscodedCarrier.fromCarrierId job.CarrierId ]
                                    TranscodedFiles = PsTranscodedFiles.LegacyVideo job.Files
                                    PublishingPriority = None
                                    Version = 0
                                }
                            let fileSets = state.FileSets |> Map.add (CarrierId.create job.CarrierId) fileSet
                            { state with
                                MediaType = Some MediaType.Video
                                FileSets = fileSets
                            }
                    | :? PsPersistence.ReceivedAudioTranscodingJob as event ->
                        let job = event.ToDomain()
                        let carrierId = job.CarrierId |> CarrierId.create
                        match state.FileSets |> Map.tryFind carrierId with
                        | Some fs when not (PsTranscodedFiles.isEmpty fs.TranscodedFiles) && job.Version <= fs.Version ->
                            logDebug mailbox $"Skipping job with version {job.Version} not greater than {fs.Version}"
                            state
                        | _ ->
                            let fileSet =
                                {
                                    ProgramId =
                                        match job.ProgramId, state.ProgramId with
                                        | null, Some programId -> PiProgId.value programId
                                        | _, _ -> job.ProgramId
                                    Carriers = [ PsTranscodedCarrier.fromCarrierId job.CarrierId ]
                                    TranscodedFiles = PsTranscodedFiles.Audio job.Files
                                    PublishingPriority = None
                                    Version = job.Version
                                }
                            { state with
                                MediaType = Some MediaType.Audio
                                FileSets = state.FileSets |> Map.add carrierId fileSet
                                ActiveCarriers = Some([ createProgramCarrier job.CarrierId ], event.Timestamp)
                            }
                    | :? PsPersistence.ReceivedAudioMigrationJob as event ->
                        let job = event.ToDomain()
                        match state.FileSets |> Map.tryFind (CarrierId.create job.CarrierId) with
                        | Some fs when fs.TranscodedFiles.IsAudio -> // migration should not overwrite regular transcoding
                            logDebug mailbox $"Skipping job with existing files for carrier {fs.CarrierIds}"
                            state
                        | _ ->
                            let fileSet =
                                {
                                    ProgramId =
                                        match job.ProgramId, state.ProgramId with
                                        | null, Some programId -> PiProgId.value programId
                                        | _, _ -> job.ProgramId
                                    Carriers = [ PsTranscodedCarrier.fromCarrierId job.CarrierId ]
                                    TranscodedFiles = PsTranscodedFiles.LegacyAudio job.Files
                                    PublishingPriority = None
                                    Version = 0
                                }
                            { state with
                                MediaType = Some MediaType.Audio
                                FileSets = state.FileSets |> Map.add (CarrierId.create job.CarrierId) fileSet
                                ActiveCarriers = Some([ createProgramCarrier job.CarrierId ], event.Timestamp)
                            }
                    | :? PsPersistence.ReceivedTvSubtitlesJob as event ->
                        let job = event.ToDomain()
                        let subtitlesSet: PsTvSubtitlesSet =
                            {
                                ProgramId = job.ProgramId
                                CarrierIds = job.CarrierIds
                                Subtitles = job.Subtitles
                                Priority = job.Priority
                                Version = job.Version
                                TranscodingVersion = job.TranscodingVersion
                            }
                        match state.TryGetSubtitlesSet(Some job.TranscodingVersion) with
                        | Some(SubtitlesSet currentSet) when currentSet.Version >= subtitlesSet.Version -> state
                        | Some(SubtitlesSet _) ->
                            let subtitleSet =
                                (SubtitlesSet subtitlesSet)
                                :: (state.SubtitlesSets
                                    |> List.filter (fun subs ->
                                        match subs with
                                        | SubtitlesSet s -> s.TranscodingVersion <> job.TranscodingVersion
                                        | LegacySubtitlesSet _ -> true))
                            { state with
                                MediaType = Some MediaType.Video
                                SubtitlesSets = subtitleSet
                            }
                        | _ ->
                            let subtitleSet = (SubtitlesSet subtitlesSet) :: state.SubtitlesSets
                            { state with
                                MediaType = Some MediaType.Video
                                SubtitlesSets = subtitleSet
                            }
                    | :? PsPersistence.ReceivedSubtitlesMigrationJob as event ->
                        let job = event.ToDomain()
                        let subtitleSet = (LegacySubtitlesSet job) :: (state.SubtitlesSets |> List.filter _.IsSubtitlesSet)
                        { state with
                            MediaType = Some MediaType.Video
                            SubtitlesSets = subtitleSet
                        }
                    | :? PsPersistence.AssignedActiveCarriers as event ->
                        let carriersEvent = event.ToDomain()
                        match state.ActiveCarriers with
                        | None ->
                            { state with
                                MediaType = Some MediaType.Video
                                ActiveCarriers = Some(carriersEvent.Carriers, carriersEvent.Timestamp)
                            }
                        | Some(_, lastUpdateTime) when lastUpdateTime < carriersEvent.Timestamp ->
                            { state with
                                MediaType = Some MediaType.Video
                                ActiveCarriers = Some(carriersEvent.Carriers, carriersEvent.Timestamp)
                            }
                        | _ -> state
                    | :? PsPersistence.ArchivedTranscodedFile as event ->
                        let archiveFile = event.ToDomain()
                        { state with
                            FileSets =
                                state.FileSets
                                |> Map.map (fun _ fs ->
                                    { fs with
                                        TranscodedFiles = fs.TranscodedFiles |> PsTranscodedFiles.setArchivePath archiveFile
                                    })
                        }
                    | :? PsPersistence.AssignedUsageRights as event ->
                        let rights = event.ToDomain()
                        { state with Rights = rights }
                    | :? PsPersistence.DeprecatedEvent as event -> state
                    | _ -> invalidOp <| $"Unexpected event %A{event}"
                let state =
                    { state with
                        LastSequenceNr = mailbox.LastSequenceNr()
                    }
                if state.LastSequenceNr % int64 SnapshotFrequency = 0L then
                    mailbox.Self <! box (TakeSnapshotCommand state.LastSequenceNr)
                state
            with exn ->
                logErrorWithExn mailbox exn $"Failed to recover actor state, last sequenceNr: {mailbox.LastSequenceNr()}"
                reraise ()

        let persistEvent event =
            logDebug mailbox $"Persisting %s{event.GetType().Name}"
            Persist event

        let persistEvents events =
            events |> Seq.iter (fun event -> logDebug mailbox $"Persisting %s{event.GetType().Name}")
            PersistAll events

        let handleSnapshotCommand state (message: obj) =
            match message with
            | :? TakeSnapshotCommand ->
                logDebug mailbox $"Taking snapshot at {mailbox.LastSequenceNr()}"
                PsPersistence.FilesState.FromDomain state |> box |> SaveSnapshot :> Effect<_>
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

        let rec init state =
            if state = PsFilesState.Zero then
                logDebug mailbox "init"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? PsPersistence.IProtoBufSerializableEvent as event -> init (applyPersistentEvent state event)
                    | :? RecoveryCompleted ->
                        logDebug mailbox "Persistence recovery completed"
                        state_initialized state
                    | :? SnapshotOffer as s ->
                        logDebug mailbox $"Restoring state from snapshot at {s.Metadata.SequenceNr}"
                        let state = (s.Snapshot :?> PsPersistence.FilesState).ToDomain()
                        init { state with RestoredSnapshot = true }
                    | :? SaveSnapshotSuccess -> state_initialized { state with SavedSnapshot = true }
                    | :? SaveSnapshotFailure as msg ->
                        logErrorWithExn mailbox msg.Cause "Failed to create snapshot on recovery"
                        state_initialized state
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | PersistentLifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }
        and state_initialized state =
            logDebug mailbox "state_initialized"
            let state =
                match state.ProgramId, state.MediaType, state.ActiveCarriers with
                | Some piProgId, Some MediaType.Audio, None ->
                    { state with
                        ActiveCarriers = Some([ createProgramCarrier <| PiProgId.value piProgId ], DateTimeOffset.MinValue)
                    }
                | _ -> state
            idle state
        and idle state =
            logDebug mailbox "idle"
            mailbox.UnstashAll()
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? QueueMessage<PsChangeJob> as job ->
                        use _ = createTraceSpan mailbox "psFilesHandlerActor.QueueMessage" [ ("oddjob.ps.programId", job.Payload.PiProgId) ]
                        logDebug mailbox $"Queue message: {job.Payload}"
                        match state.ProgramId with
                        | Some piProgId when not state.FileSets.IsEmpty ->
                            granitt <! GranittCommand.GetProgramRights piProgId
                            if Seq.isNotEmpty state.Rights then
                                let rights: PsProgramRights list =
                                    state.Rights
                                    |> List.choose (fun x ->
                                        tryParseUnionCaseName true x.Region
                                        |> Option.bind (fun (geolocation: GranittGeorestriction) ->
                                            RightsPeriod.tryCreate x.PublishStart (x.PublishEnd |> Option.defaultValue DateTime.MaxValue)
                                            |> Option.map (fun rights ->
                                                {
                                                    Geolocation = geolocation
                                                    RightsPeriod = rights
                                                })))
                                process_change_job state job rights
                            else
                                awaiting_rights state job
                        | _ ->
                            sendAck mailbox job.Ack "Skipping PS change queue message for empty file set"
                            ignored ()
                    | :? QueueMessage<PsProgramGotActiveCarriersEvent> as queueMsg ->
                        logDebug mailbox $"Received {queueMsg.Payload}"
                        match state.ActiveCarriers with
                        | Some(_, timestamp) when queueMsg.Payload.Timestamp <= timestamp ->
                            logWarning mailbox $"Skipping active carrier assignment with timestamp {queueMsg.Payload.Timestamp} not newer than {timestamp}"
                            sendAck mailbox queueMsg.Ack "Handled assign active carriers event"
                            idle state
                        | _ ->
                            mailbox.Self <! (PersistActiveCarrierEvent :> obj)
                            persist_active_carrier_event state queueMsg.Payload queueMsg.Ack
                    | :? PsPersistence.IProtoBufSerializableEvent as event -> idle (applyPersistentEvent state event)
                    | :? PsShardMessage as msg ->
                        match msg with
                        | PsShardMessage.GetFilesState _ ->
                            mailbox.Sender() <! PsPersistence.FilesState.FromDomain state
                            ignored ()
                        | PsShardMessage.VideoTranscodingDetails details ->
                            logDebug mailbox $"Received {details}"
                            mailbox.Self <! (PersistVideoTranscodingDetailsCommand details :> obj)
                            persist_files_job state
                        | PsShardMessage.AudioTranscodingDetails details ->
                            logDebug mailbox $"Received {details}"
                            mailbox.Self <! (PersistAudioTranscodingDetailsCommand details :> obj)
                            let state =
                                match state.ActiveCarriers with
                                | Some _ -> state
                                | None ->
                                    { state with
                                        ActiveCarriers = Some([ createProgramCarrier details.ProgramId ], DateTimeOffset.MinValue)
                                    }
                            persist_files_job state
                        | PsShardMessage.AudioMigrationDetails details ->
                            logDebug mailbox $"Received {details}"
                            mailbox.Self <! (PersistAudioMigrationDetailsCommand details :> obj)
                            let state =
                                match state.ActiveCarriers with
                                | Some _ -> state
                                | None ->
                                    { state with
                                        ActiveCarriers = Some([ createProgramCarrier details.ProgramId ], DateTimeOffset.MinValue)
                                    }
                            persist_files_job state
                        | PsShardMessage.SubtitleFilesDetails details ->
                            logDebug mailbox $"Received {details}"
                            mailbox.Self <! (PersistTvSubtitlesJobCommand details :> obj)
                            persist_files_job state
                        | PsShardMessage.UsageRights rights ->
                            logDebug mailbox $"Received {rights}"
                            mailbox.Self <! (PersistUsageRightsEvent :> obj)
                            persist_usage_rights_event state rights
                        | _ -> unhandled ()
                    | SnapshotCommand cmd -> handleSnapshotCommand state cmd
                    | SnapshotEvent event -> handleSnapshotEvent event
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | PersistentLifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }
        and persist_files_job state =
            logDebug mailbox "persist_files_job"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? PersistVideoTranscodingDetailsCommand as msg ->
                        let (PersistVideoTranscodingDetailsCommand details) = msg
                        seq {
                            if state.ProgramId.IsNone then
                                yield PsPersistence.AssignedProgramId.FromDomain(PiProgId.create details.ProgramId) |> box
                            if state.MediaType.IsNone then
                                yield PsPersistence.AssignedMediaType.FromDomain(MediaType.Video) |> box
                            yield PsPersistence.ReceivedVideoTranscodingJob.FromDomain(details) |> box
                        }
                        |> persistEvents
                        :> Effect<_>
                    | :? PersistVideoMigrationDetailsCommand as msg ->
                        let (PersistVideoMigrationDetailsCommand details) = msg
                        seq {
                            if state.ProgramId.IsNone then
                                yield PsPersistence.AssignedProgramId.FromDomain(PiProgId.create details.ProgramId) |> box
                            if state.MediaType.IsNone then
                                yield PsPersistence.AssignedMediaType.FromDomain(MediaType.Video) |> box
                            yield PsPersistence.ReceivedVideoMigrationJob.FromDomain(details) |> box
                        }
                        |> persistEvents
                        :> Effect<_>
                    | :? PersistAudioTranscodingDetailsCommand as msg ->
                        let (PersistAudioTranscodingDetailsCommand details) = msg
                        seq {
                            if state.ProgramId.IsNone then
                                yield PsPersistence.AssignedProgramId.FromDomain(PiProgId.create details.ProgramId) |> box
                            if state.MediaType.IsNone then
                                yield PsPersistence.AssignedMediaType.FromDomain(MediaType.Audio) |> box
                            yield PsPersistence.ReceivedAudioTranscodingJob.FromDomain(details) |> box
                        }
                        |> persistEvents
                        :> Effect<_>
                    | :? PersistAudioMigrationDetailsCommand as msg ->
                        let (PersistAudioMigrationDetailsCommand details) = msg
                        seq {
                            if state.ProgramId.IsNone then
                                yield PsPersistence.AssignedProgramId.FromDomain(PiProgId.create details.ProgramId) |> box
                            if state.MediaType.IsNone then
                                yield PsPersistence.AssignedMediaType.FromDomain(MediaType.Audio) |> box
                            yield PsPersistence.ReceivedAudioMigrationJob.FromDomain(details) |> box
                        }
                        |> persistEvents
                        :> Effect<_>
                    | :? PersistTvSubtitlesJobCommand as msg ->
                        let (PersistTvSubtitlesJobCommand details) = msg
                        seq {
                            if state.ProgramId.IsNone then
                                yield PsPersistence.AssignedProgramId.FromDomain(PiProgId.create details.ProgramId) |> box
                            if state.MediaType.IsNone then
                                yield PsPersistence.AssignedMediaType.FromDomain(MediaType.Video) |> box
                            yield PsPersistence.ReceivedTvSubtitlesJob.FromDomain(details) |> box
                        }
                        |> persistEvents
                        :> Effect<_>
                    | :? PsPersistence.IProtoBufSerializableEvent as event ->
                        let state = applyPersistentEvent state event
                        match event with
                        | :? PsPersistence.ReceivedVideoTranscodingJob as event -> processing_video_transcoding_job state event
                        | :? PsPersistence.ReceivedAudioTranscodingJob as event -> processing_audio_transcoding_job state event
                        | :? PsPersistence.ReceivedTvSubtitlesJob as event -> processing_subtitles_job state event
                        | :? PsPersistence.ReceivedVideoMigrationJob -> idle state
                        | :? PsPersistence.ReceivedAudioMigrationJob -> idle state
                        | _ -> persist_files_job state
                    | SnapshotCommand cmd -> handleSnapshotCommand state cmd
                    | SnapshotEvent event -> handleSnapshotEvent event
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and persist_active_carrier_event state details ack =
            logDebug mailbox "persist_active_carrier_event"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? PersistActiveCarrierEvent ->
                        seq {
                            if state.ProgramId.IsNone then
                                yield PsPersistence.AssignedProgramId.FromDomain(PiProgId.create details.ProgramId) |> box
                            yield PsPersistence.AssignedActiveCarriers.FromDomain(details) |> box
                        }
                        |> persistEvents
                        :> Effect<_>
                    | :? PsPersistence.IProtoBufSerializableEvent as event ->
                        match event with
                        | :? PsPersistence.AssignedActiveCarriers as event ->
                            sendAck mailbox ack "Handled assign active carriers event"
                            let state = (applyPersistentEvent state event)
                            idle state
                        | _ -> ignored ()
                    | SnapshotCommand cmd -> handleSnapshotCommand state cmd
                    | SnapshotEvent event -> handleSnapshotEvent event
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and persist_usage_rights_event state rights =
            logDebug mailbox "persist_usage_rights_event"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? PersistUsageRightsEvent ->
                        seq {
                            if state.ProgramId.IsNone then
                                yield PsPersistence.AssignedProgramId.FromDomain(PiProgId.create rights.ProgramId) |> box
                            yield PsPersistence.AssignedUsageRights.FromDomain(rights.Rights) |> box
                        }
                        |> persistEvents
                        :> Effect<_>
                    | :? PsPersistence.IProtoBufSerializableEvent as event ->
                        match event with
                        | :? PsPersistence.AssignedUsageRights as event ->
                            let state = (applyPersistentEvent state event)
                            idle state
                        | _ -> ignored ()
                    | SnapshotCommand cmd -> handleSnapshotCommand state cmd
                    | SnapshotEvent event -> handleSnapshotEvent event
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and processing_video_transcoding_job state jobEvent =
            logDebug mailbox "processing_video_transcoding_job"
            match state.ProgramId with
            | Some piProgId ->
                let queueMessage = createPsChangeQueueMessage piProgId "bridge" jobEvent.PublishingPriority
                let transcodingAck: VideoTranscodingDetailsAck =
                    {
                        Version = jobEvent.Files.Version
                        ProgramId = jobEvent.Files.ProgramId
                    }
                aprops.QueuePublisher <! PublishOne(queueMessage, box transcodingAck, retype mailbox.Self)
                awaiting_queue_publish_result state
            | None -> failwith "Should have non-empty ProgramId"
        and processing_audio_transcoding_job state jobEvent =
            logDebug mailbox "processing_audio_transcoding_job"
            match state.ProgramId with
            | Some piProgId ->
                let queueMessage = createPsChangeQueueMessage piProgId "radioarkiv" (getUnionCaseName PsPublishingPriority.Medium)
                let transcodingAck: AudioTranscodingDetailsAck =
                    {
                        Version = jobEvent.Files.Version
                        ProgramId = jobEvent.Files.ProgramId
                    }
                aprops.QueuePublisher <! PublishOne(queueMessage, box transcodingAck, retype mailbox.Self)
                awaiting_queue_publish_result state
            | None -> failwith "Should have non-empty ProgramId"
        and processing_subtitles_job state jobEvent =
            logDebug mailbox "processing_tv_subtitles_job"
            match state.ProgramId with
            | Some piProgId ->
                let priority = PsPersistence.convertToPublishingPriority jobEvent.TvSubtitles.PublishingPriority |> getUnionCaseName
                let queueMessage = createPsChangeQueueMessage piProgId "bridge" priority
                aprops.QueuePublisher <! PublishOne(queueMessage, box jobEvent, retype mailbox.Self)
                awaiting_queue_publish_result state
            | None -> failwith "Should have non-empty ProgramId"
        and awaiting_queue_publish_result state =
            logDebug mailbox "awaiting_queue_publish_result"
            mailbox.SetReceiveTimeout aprops.ExternalRequestTimeout
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? QueuePublishResult as publishResult ->
                        mailbox.SetReceiveTimeout None
                        match publishResult with
                        | Ok ctx ->
                            match ctx with
                            | :? VideoTranscodingDetailsAck as transcodingAck ->
                                let message = PsShardMessage.VideoTranscodingDetailsAck transcodingAck
                                aprops.GetPsMediator() <! message
                            | :? AudioTranscodingDetailsAck as transcodingAck ->
                                let message = PsShardMessage.AudioTranscodingDetailsAck transcodingAck
                                aprops.GetPsMediator() <! message
                            | :? PsPersistence.ReceivedTvSubtitlesJob -> ()
                            | _ -> logWarning mailbox $"Unexpected publish result (ctx {ctx})"
                        | Error(exn, ctx) -> logWarning mailbox $"Failed to publish PS change message (exn {exn}) (ctx {ctx})"
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
        and awaiting_rights state job =
            logDebug mailbox "awaiting_rights"
            mailbox.SetReceiveTimeout aprops.ExternalRequestTimeout
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? Result<PsProgramRights list, exn> as result ->
                        mailbox.SetReceiveTimeout None
                        match result with
                        | Ok granittRights ->
                            logInfo mailbox $"Found rights: %A{granittRights}"
                            process_change_job state job granittRights
                        | Error exn -> failwithf $"Failed to get rights from Granitt: {exn}"
                    | :? ReceiveTimeout ->
                        let message = "Timeout awaiting Granitt"
                        logError mailbox message
                        mailbox.SetReceiveTimeout None
                        raise <| TimeoutException message
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and process_change_job state job rights =
            logDebug mailbox "process_change_job"
            match aprops.RightsAnalyzer.GetUploadAction rights with
            | Ok action ->
                logDebug mailbox $"Upload action: {action}"
                match action.ScheduleUpload, action.DeleteOrUpload with
                | None, None ->
                    sendNack mailbox job.Ack "Handled PS files message [no actions to be executed]"
                    idle state
                | Some dt, Some deleteOrUpload ->
                    // In case there is two actions to be executed, we have to inject here a little actor that will handle ack
                    // on the queue in graceful way - that is, it will only ack the message when both actions have executed
                    // (with some optimizations).
                    let ack =
                        job.Ack
                        |> Option.map (fun ack ->
                            let wrapper = spawnAnonymous mailbox (propsNamed "ps-multi-acknowledger" (actorOf2 (multiAckerWrapperActor ack.AckActor 2)))
                            { ack with AckActor = wrapper })
                    let message = { job with Ack = ack }
                    scheduleUpload message dt
                    match deleteOrUpload with
                    | DeleteOrUploadProgram.Delete -> delete_program state job
                    | DeleteOrUploadProgram.Upload rightsUploadDetails -> upload_program state job rightsUploadDetails
                | Some dt, None ->
                    scheduleUpload job dt
                    idle state
                | None, Some deleteOrUpload ->
                    match deleteOrUpload with
                    | DeleteOrUploadProgram.Delete -> delete_program state job
                    | DeleteOrUploadProgram.Upload rightsUploadDetails -> upload_program state job rightsUploadDetails
            | Error text ->
                sendReject mailbox job.Ack $"Invalid program rights: [%s{text}]"
                idle state
        and upload_program state job rightsUploadDetails =
            logDebug mailbox "upload_program"
            let job' = job.Payload
            let ack = job.Ack

            //verify fileset contains active carrier
            if
                state.FileSets
                |> Map.keys
                |> Seq.exists (fun carrierId ->
                    (state.ActiveCarriers |> Option.bind (fst >> Some) |> Option.defaultValue List.empty)
                    |> List.exists (fun fs -> fs.CarrierId = CarrierId.value carrierId))
            then
                let forwardedFrom = ack |> Option.bind _.Tag.ForwardedFrom
                let isCleanupScheduled = tryScheduleCleanup job.Payload rightsUploadDetails forwardedFrom
                let traceContext = getTraceContext mailbox

                Utils.startPlayabilityReminder aprops.PsScheduler job'.PiProgId psMediator.Path aprops.PlayabilityReminderInterval (logDebug mailbox)
                let reminderCount = if isCleanupScheduled then 2 else 1
                awaiting_reminders_acks state job' rightsUploadDetails ack reminderCount traceContext
            else
                let text = $"No active carrier in transcoded sets {state.FileSets |> Map.keys |> Seq.toList}"
                sendAck mailbox ack text
                idle state
        and delete_program state job =
            logDebug mailbox "delete_program"
            let piProgId = PiProgId.create job.Payload.PiProgId
            let source = job.Payload.Source
            let priority = aprops.CalculatePriority(source, PublishedStatus.Inactive, job.Payload.PublishingPriority)
            let details =
                {
                    ClearMediaSetJobDetails.RequestSource = source
                    ForwardedFrom = job.Ack |> Option.bind (_.Tag.ForwardedFrom)
                    Priority = int priority
                    PiProgId = piProgId
                }
            let mediaSetJob = jobCreator.CreateClearMediaSetJob details
            logDebug mailbox $"Created delete job %A{job} for program %A{details.PiProgId}"
            aprops.MediaSetController <! Message.createWithAck (MediaSetShardMessage.MediaSetJob mediaSetJob) job.Ack
            idle state
        and awaiting_reminders_acks state job rights ack reminderCount traceContext =
            logDebug mailbox $"awaiting_reminders_acks ({reminderCount} remains)"
            mailbox.SetReceiveTimeout(Some aprops.ReminderAckTimeout)
            actor {
                let! message = mailbox.Receive()
                return!
                    match message with
                    | :? Reminders.ReminderCreated ->
                        logDebug mailbox $"Received reminder ack ({message})"
                        let reminderCount = reminderCount - 1
                        if reminderCount = 0 then
                            mailbox.SetReceiveTimeout None
                            let state' =
                                { state with
                                    FileSets = state.GetActiveFileSets()
                                }
                            // message will be acked upon dispatch
                            dispatchJob state' rights job ack traceContext
                            idle state
                        else
                            awaiting_reminders_acks state job rights ack reminderCount traceContext
                    | Reminders.LifetimeEvent e ->
                        logDebug mailbox $"Reminder lifetime event {e}"
                        ignored ()
                    | :? ReceiveTimeout ->
                        let message = "Timeout awaiting reminder ack"
                        sendNack mailbox ack message
                        raise <| TimeoutException message
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        init PsFilesState.Zero
