namespace Nrk.Oddjob.Ps

module PsTranscodingActors =

    open System
    open Akka.Actor
    open Akkling
    open Akkling.Persistence

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Queues

    open PsTypes
    open PsDto
    open PsGranittActors
    open PsShardMessages
    open PsFileArchive

    [<NoEquality; NoComparison>]
    type PsTranscodingHandlerProps =
        {
            GetGranittAccess: IActorContext -> IActorRef<GranittCommand>
            GetPsMediator: unit -> IActorRef<PsShardMessage>
            SaveToEventStore: PiProgId -> string -> string -> PsVideoTranscodingJob -> unit
            DisableGranittUpdate: unit -> bool
            ReminderIntervalInMinutes: int
            ReminderTimeoutInDays: int
            ReminderAckTimeout: TimeSpan option
            ArchiveConfig: PrepareArchiveConfig
            ArchiveTimeouts: Config.OddjobConfig.PsArchiveTimeout array
            FileSystem: IFileSystemOperations
            ExternalRequestTimeout: TimeSpan option
            ActivityContext: ActivitySourceContext
        }

    type private Job =
        {
            Transcoding: PsVideoTranscodingJob
            ReceiveTime: DateTimeOffset
        }

    type CarrierStatus =
        | Active
        | Inactive

    module private PrepareArchive =
        type PrepareArchiveCommand = Job
        type PrepareArchiveResult = Result<Job, exn>

        [<NoEquality; NoComparison>]
        type PrepareArchiveActorProps =
            {
                ArchiveConfig: PrepareArchiveConfig
                FileSystem: IFileSystemOperations
            }

        let transcodedFilesFromArchive (archiveFiles: PsTranscodedFile list) (transcodedFiles: PsTranscodedVideoFile list) =
            transcodedFiles
            |> List.map (fun x ->
                let fileName = IO.getFileName x.FilePath
                let filePath =
                    match archiveFiles |> List.tryFind (fun y -> String.Equals(fileName, y.FileName, StringComparison.InvariantCultureIgnoreCase)) with
                    | Some archiveFile -> archiveFile.FilePath
                    | None -> x.FilePath
                { x with FilePath = filePath })

        let prepareArchiveActor (aprops: PrepareArchiveActorProps) (mailbox: Actor<_>) =

            let rec idle () =
                actor {
                    let! (job: PrepareArchiveCommand) = mailbox.Receive()
                    let sender = mailbox.Sender()

                    try
                        logDebug mailbox $"Preparing archive for {job.Transcoding.ProgramId}"
                        let archiveFiles =
                            job.Transcoding.Files
                            |> List.map (fun file ->
                                {
                                    FileName = IO.getFileName file.FilePath
                                    FilePath = file.FilePath
                                })
                        let archiveFiles =
                            PsPrepareArchive.prepareArchiveForVideo
                                aprops.ArchiveConfig
                                (PiProgId.create job.Transcoding.ProgramId)
                                archiveFiles
                                aprops.FileSystem
                                mailbox.Log.Value
                        let job =
                            { job with
                                Transcoding.Files = job.Transcoding.Files |> transcodedFilesFromArchive archiveFiles
                            }
                        logDebug mailbox $"Completed archive preparation for {job.Transcoding.ProgramId}"
                        sender <! PrepareArchiveResult.Ok job
                    with exn ->
                        sender <! PrepareArchiveResult.Error exn

                    return! ignored ()
                }

            idle ()

    let psTranscodingHandlerActor (aprops: PsTranscodingHandlerProps) (mailbox: Actor<_>) =

        let granitt = aprops.GetGranittAccess mailbox.UntypedContext

        let handleLifecycle mailbox e =
            match e with
            | PreRestart(exn, message) ->
                logErrorWithExn mailbox exn $"PreRestart due to error when processing message {message.GetType()} [{message}]"
                match message with
                | :? QueueMessage<PsTranscodingFinishedDto> as message ->
                    match exn with
                    | :? ArgumentException -> sendReject mailbox message.Ack "Rejecting invalid command"
                    | _ -> sendNack mailbox message.Ack "Failed processing message"
                    ignored ()
                | _ -> unhandled ()
            | _ -> ignored ()

        let messageIsValid (message: PsVideoTranscodingJob) =
            try
                message.Files |> List.tryHead |> Option.iter (fun file -> System.Xml.XmlConvert.ToTimeSpan(file.Duration) |> ignore)
                true
            with :? FormatException ->
                false

        let prepareArchive =
            let aprops: PrepareArchive.PrepareArchiveActorProps =
                {
                    ArchiveConfig = aprops.ArchiveConfig
                    FileSystem = aprops.FileSystem
                }
            getOrSpawnChildActor
                mailbox.UntypedContext
                (makeActorName [ "Prepare archive" ])
                (propsNamed "ps-prepare-archive" <| PrepareArchive.prepareArchiveActor aprops)

        let rec computeArchiveTimeout totalLength (timeouts: Config.OddjobConfig.PsArchiveTimeout list) =
            match timeouts with
            | [ x ] -> Some x.timeout
            | [] -> None
            | x :: _ when totalLength <= uint64 x.size * 1024UL * 1024UL * 1024UL -> Some x.timeout
            | _ :: timeouts -> computeArchiveTimeout totalLength timeouts

        let rec idle initiating =
            if (not initiating) then // Unit tests have problems with UnstashAll before an actor receives first message
                mailbox.UnstashAll()
            logDebug mailbox $"idle"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? QueueMessage<PsVideoTranscodingJob> as message ->
                        logDebug mailbox $"Received PS transcoding message {message}"
                        if messageIsValid message.Payload then
                            let transcoding, receiveTime = message.Payload, DateTimeOffset.Now // TODO: remove time?
                            let job =
                                {
                                    Transcoding = transcoding
                                    ReceiveTime = receiveTime
                                }
                            logDebug mailbox "Persisted TranscodingFinished job"
                            process_message job message.Ack
                        else
                            sendReject mailbox message.Ack $"Invalid message (version {message.Payload.Version})"
                            idle false
                    | Reminders.LifetimeEvent e ->
                        logDebug mailbox $"Reminder lifetime event {e}"
                        ignored ()
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | PersistentLifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }
        and process_message job ack =
            logDebug mailbox $"process_message"
            if aprops.DisableGranittUpdate() then
                prepareArchive <! job
                prepare_archive job ack
            else
                granitt <! GranittCommand.UpdateForTranscodingJob(job.Transcoding)
                awaiting_granitt_updated job ack
        and awaiting_granitt_updated job ack =
            logDebug mailbox $"awaiting_granitt_updated"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? Result<TranscodingStatusUpdated, exn> as result ->
                        match result with
                        | Ok TranscodingStatusUpdated ->
                            prepareArchive <! job
                            prepare_archive job ack
                        | Error exn ->
                            let error = "Failed to update transcoding status"
                            logErrorWithExn mailbox exn error
                            sendNack mailbox ack error
                            idle false
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | PersistentLifecycleEvent _ -> ignored ()
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and prepare_archive originalJob ack =
            logDebug mailbox $"prepare_archive"
            let totalLength = originalJob.Transcoding.Files |> Seq.map _.FilePath |> Seq.map aprops.FileSystem.GetFileLength |> Seq.sum
            let requestTimeout =
                computeArchiveTimeout totalLength (aprops.ArchiveTimeouts |> Seq.toList) |> Option.map (float >> TimeSpan.FromSeconds)
            logDebug mailbox $"Using file archive timeout of {requestTimeout} for total length of {totalLength}"
            mailbox.SetReceiveTimeout requestTimeout
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? PrepareArchive.PrepareArchiveResult as result ->
                        mailbox.SetReceiveTimeout None
                        match result with
                        | Ok job ->
                            sendAck mailbox ack "Archive is prepared"
                            let carrierId = job.Transcoding |> PsVideoTranscodingJob.tryGetSingleCarrierId |> Option.toObj
                            aprops.SaveToEventStore (PiProgId.create originalJob.Transcoding.ProgramId) carrierId "TranscodingFinished" originalJob.Transcoding
                            aprops.GetPsMediator() <! PsShardMessage.VideoTranscodingDetails(job.Transcoding)
                            awaiting_details_ack ()
                        | Error exn ->
                            let error = $"Failed to prepare archive for job {originalJob}"
                            logErrorWithExn mailbox exn error
                            sendNack mailbox ack error
                            idle false
                    | :? ReceiveTimeout -> raise <| TimeoutException "Timeout awaiting archive preparation"
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | PersistentLifecycleEvent _ -> ignored ()
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and awaiting_details_ack () =
            logDebug mailbox $"awaiting_details_ack"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? VideoTranscodingDetailsAck ->
                        logDebug mailbox $"Received VideoTranscodingDetailsAck"
                        idle false
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | PersistentLifecycleEvent _ -> ignored ()
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        idle true
