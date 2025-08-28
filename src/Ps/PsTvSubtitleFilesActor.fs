namespace Nrk.Oddjob.Ps

module PsTvSubtitleFilesActor =

    open System
    open System.IO
    open System.Net
    open System.Runtime.InteropServices
    open Akka.Actor
    open Akkling
    open Azure.Storage.Blobs
    open Azure.Storage.Blobs.Models
    open FsHttp

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Queues
    open Nrk.Oddjob.Core.Config
    open PsTypes
    open PsShardMessages

    [<NoEquality; NoComparison>]
    type PsTvSubtitleFilesProps =
        {
            Environment: string
            SubtitlesConfig: PsSubtitlesConfig
            TekstebankenConfig: Tekstebanken.Config
            PsMediator: IActorRef<PsShardMessage>
            SaveToEventStore: PiProgId -> string -> string -> PsSubtitleFilesJob -> unit
            SideloadingContainerClient: BlobContainerClient
            PlayabilityReminderInterval: TimeSpan
            ReminderAckTimeout: TimeSpan
            ArchivalTimeout: TimeSpan
            PsScheduler: IActorRef
            FileSystem: IFileSystemInfo
        }

    let handleLifecycle mailbox e =
        match e with
        | PreRestart(exn, message) ->
            logErrorWithExn mailbox exn $"PreRestart due to error when processing message {message.GetType()} [%A{message}]"
            match message with
            | :? QueueMessage<PsSubtitleFilesJob> as message ->
                match exn with
                | :? ArgumentException -> sendReject mailbox message.Ack "Rejecting invalid command"
                | _ -> sendNack mailbox message.Ack "Failed processing PS TV subtitles message"
                ignored ()
            | _ -> unhandled ()
        | _ -> ignored ()

    let psTvSubtitleFilesActor (aprops: PsTvSubtitleFilesProps) (psMediator: IActorRef<obj>) (mailbox: Actor<_>) =

        let saveToEventStore (message: QueueMessage<PsSubtitleFilesJob>) =
            let piProgId = PiProgId.create message.Payload.ProgramId
            aprops.SaveToEventStore piProgId (message.Payload.CarrierIds |> List.tryHead |> Option.defaultValue null) "AssignProgramSubtitles" message.Payload

        let getRelativeDestinationFolder (progId: String) =
            Path.Combine(progId.Substring(0, 6), progId.Substring(6, 2), progId)

        let getDestinationPath subtitlesConfig (progId: string) =
            let destinationRoot =
                if RuntimeInformation.IsOSPlatform(OSPlatform.Windows) then
                    subtitlesConfig.DestinationRootWindows
                else
                    subtitlesConfig.DestinationRootLinux
            Path.Combine(destinationRoot, getRelativeDestinationFolder progId)

        let sideloadUploadAsync programId (fileName: string) (archivalPath: string) =
            async {
                let sideloadingPath =
                    Path.Combine(aprops.Environment |> String.toLower, "v2", getRelativeDestinationFolder programId, fileName)
                let blobClient = aprops.SideloadingContainerClient.GetBlobClient(sideloadingPath)
                try
                    let! _ = blobClient.UploadAsync(archivalPath, true) |> Async.AwaitTask
                    let headers = BlobHttpHeaders(ContentType = "text/vtt; charset=utf-8", CacheControl = "max-age=300, public")
                    let! _ = blobClient.SetHttpHeadersAsync(headers) |> Async.AwaitTask
                    let sideloadingUrl = Uri(aprops.SubtitlesConfig.BaseUrlTvSubtitles, sideloadingPath).AbsoluteUri
                    return Ok sideloadingUrl
                with exn ->
                    let message = $"Unable to upload sideloading subtitle from {archivalPath} to {sideloadingPath}"
                    logErrorWithExn mailbox exn message
                    return Error message
            }

        let archiveSubtitlesAsync
            programId
            transcodingVersion
            version
            (apiKey: Tekstebanken.ApiKeyMapping)
            (subtitles: PsTvSubtitleFile)
            : Async<Result<PsTvSubtitleFileDetails, string>> =
            async {
                try
                    let destinationFolder = getDestinationPath aprops.SubtitlesConfig programId
                    Directory.CreateDirectory(destinationFolder) |> ignore

                    let fileName =
                        $"{programId}-{apiKey.EnvShortname}-{subtitles.LanguageCode}-{subtitles.Name}-{transcodingVersion}-v{version}.{aprops.TekstebankenConfig.Format}"
                    let archivalPath = Path.Combine(destinationFolder, fileName)

                    if File.Exists archivalPath then
                        logInfo mailbox $"Duplicate subtitles message, already downloaded subtitles for {fileName}"
                    else
                        let sourceUrl = $"%s{subtitles.ContentUri}?format=%s{aprops.TekstebankenConfig.Format}"
                        let! response =
                            http {
                                GET sourceUrl
                                header "x-api-key" apiKey.ApiKey
                            }
                            |> Request.sendAsync
                        if response.statusCode = HttpStatusCode.OK then
                            logInfo mailbox $"Downloaded Subtitles from {sourceUrl} to {archivalPath}"
                            let subtitleStream = response.ToStream()
                            let fileStream = File.Create(archivalPath)
                            do! subtitleStream.CopyToAsync(fileStream) |> Async.AwaitTask
                            fileStream.Close()
                        else
                            logWarning mailbox $"Unable to download subtitlesfile from {sourceUrl}"
                    let archiveSuccess = File.Exists archivalPath

                    let! uploadResult =
                        if archiveSuccess then
                            sideloadUploadAsync programId fileName archivalPath
                        else
                            Async.fromResult (Error "Cant Upload sideloading subtitles, since archival failed")

                    return
                        match uploadResult with
                        | Ok sideloadingUrl when archiveSuccess ->
                            Ok(
                                PsTvSubtitleFileDetails.V2
                                    {
                                        LanguageCode = subtitles.LanguageCode
                                        FilePath = archivalPath
                                        SubtitlesLinks = sideloadingUrl
                                        Version = version
                                        Name = subtitles.Name
                                        Roles = subtitles.Roles
                                    }
                            )
                        | Ok _ -> Error $"Unable to archive subtitle {archivalPath}"
                        | Error errorValue -> Error errorValue

                with exn ->
                    let message = $"Failed to archive subtitles ({exn})"
                    logErrorWithExn mailbox exn message
                    return Error message
            }

        let rec idle initiating =
            if (not initiating) then // Unit tests have problems with UnstashAll before an actor receives first message
                mailbox.UnstashAll()
            logDebug mailbox "idle"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? QueueMessage<PsSubtitleFilesJob> as queueMessage ->
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
                        logWarning mailbox $"Unhandled subtitles handler message in idle state: %A{message}"
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

                        let piProgId = PiProgId.create queueMessage.Payload.ProgramId
                        use _ = createTraceSpan mailbox "psTvSubtitlesFilesActor.handleMessage" [ ("oddjob.ps.programId", PiProgId.value piProgId) ]

                        archive_subtitles queueMessage
                    | Reminders.LifetimeEvent e ->
                        logDebug mailbox $"Reminder lifetime event {e}"
                        ignored ()
                    | :? ReceiveTimeout ->
                        let message = "Timeout awaiting reminder ack"
                        sendNack mailbox queueMessage.Ack message
                        raise <| TimeoutException message
                    | LifecycleEvent e -> handleLifecycle mailbox e
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and archive_subtitles (queueMessage: QueueMessage<PsSubtitleFilesJob>) =
            logDebug mailbox "archive_subtitles"
            let num_subtitles = queueMessage.Payload.Subtitles.Length
            let programId = queueMessage.Payload.ProgramId
            let transcodingVersion = queueMessage.Payload.TranscodingVersion
            let version = queueMessage.Payload.Version
            let tekstebankenApiEnvironment = queueMessage.Payload.Subtitles |> Seq.head |> _.ContentUri
            match
                aprops.TekstebankenConfig.ApiKeyMappings
                |> Seq.tryFind (fun mapping -> tekstebankenApiEnvironment.Contains(mapping.Url))
            with
            | Some apiKey ->
                let dispatchJob = archiveSubtitlesAsync programId transcodingVersion version apiKey >> ((<!|) (mailbox.Self |> retype))
                queueMessage.Payload.Subtitles |> Seq.iter dispatchJob
                collect_archive_subtitles List.Empty num_subtitles queueMessage
            | None ->
                let message = $"Unable to archive subtitle. No API-Key configured for {tekstebankenApiEnvironment}"
                saveToEventStore queueMessage
                sendReject mailbox queueMessage.Ack message
                idle false
        and collect_archive_subtitles (transformedSubtitles: PsTvSubtitleFileDetails list) (num: int) (queueMessage: QueueMessage<PsSubtitleFilesJob>) =
            logDebug mailbox $"collect_archive_subtitles; awaiting {num} subtitles"
            mailbox.SetReceiveTimeout(Some aprops.ArchivalTimeout)
            actor {
                let! message = mailbox.Receive()
                return!
                    match message with
                    | :? Result<PsTvSubtitleFileDetails, string> as archivedSubtitleResult ->
                        match archivedSubtitleResult with
                        | Ok archivedSub ->
                            let transformedSubtitles' = (archivedSub :: transformedSubtitles)
                            let num' = num - 1
                            if num' = 0 then
                                let job = queueMessage.Payload
                                let transformedJob: PsSubtitleFilesDetails =
                                    {
                                        ProgramId = job.ProgramId
                                        CarrierIds = job.CarrierIds
                                        TranscodingVersion = job.TranscodingVersion
                                        Version = job.Version
                                        Subtitles = transformedSubtitles'
                                        Priority = job.Priority
                                    }

                                logDebug mailbox $"Forwarding TV Subtitles for %A{queueMessage.Payload.ProgramId}"
                                aprops.PsMediator <! PsShardMessage.SubtitleFilesDetails transformedJob

                                saveToEventStore queueMessage
                                sendAck mailbox queueMessage.Ack "Message is handled"
                                idle false
                            else
                                collect_archive_subtitles transformedSubtitles' num' queueMessage
                        | Error message ->
                            sendNack mailbox queueMessage.Ack message
                            idle false
                    | :? Status.Failure as status ->
                        let message = $"Subtitle archival failed with: {status.Cause.Message}"
                        sendNack mailbox queueMessage.Ack message
                        idle false
                    | :? ReceiveTimeout ->
                        mailbox.SetReceiveTimeout None
                        let message = "Timeout awaiting Subtitle Archival"
                        sendNack mailbox queueMessage.Ack message
                        idle false
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }

        idle true
