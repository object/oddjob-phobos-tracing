namespace Nrk.Oddjob.Ps

module PsRadioActors =

    open System
    open Akka.Actor
    open Akkling

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Queues
    open PsFileArchive
    open PsTypes
    open PsShardMessages

    [<NoEquality; NoComparison>]
    type PsRadioHandlerProps =
        {
            GetPsMediator: unit -> IActorRef<PsShardMessage>
            SaveToEventStore: PiProgId -> string -> string -> PsAudioTranscodingJob -> unit
            ArchiveConfig: PrepareArchiveConfig
            FileSystem: IFileSystemOperations
            DetailsAckTimeout: TimeSpan option
            ActivityContext: ActivitySourceContext
        }

    module private PrepareArchive =
        type PrepareArchiveResult = Result<PsAudioTranscodingJob, exn>

        [<NoEquality; NoComparison>]
        type PrepareArchiveActorProps =
            {
                ArchiveConfig: PrepareArchiveConfig
                FileSystem: IFileSystemOperations
            }

        let getDestinationFileName programId version bitRate extension =
            $"{programId}_{version}_{bitRate}.{extension}"

        let transcodedFilesFromArchive (job: PsAudioTranscodingJob) (archiveFiles: PsTranscodedFile list) =
            job.Files
            |> List.map (fun x ->
                let fileName = getDestinationFileName job.ProgramId job.Version x.BitRate (x.FilePath.Split('.') |> Array.last)
                let filePath =
                    match archiveFiles |> List.tryFind (fun y -> String.Equals(fileName, y.FileName, StringComparison.InvariantCultureIgnoreCase)) with
                    | Some archiveFile -> archiveFile.FilePath
                    | None -> x.FilePath
                { x with FilePath = filePath })

        let prepareArchiveActor (aprops: PrepareArchiveActorProps) (mailbox: Actor<_>) =

            let rec idle () =
                actor {
                    let! (job: PsAudioTranscodingJob) = mailbox.Receive()
                    let sender = mailbox.Sender()

                    try
                        let archiveFiles =
                            job.Files
                            |> List.map (fun file ->
                                {
                                    FileName = getDestinationFileName job.ProgramId job.Version file.BitRate (file.FilePath.Split('.') |> Array.last)
                                    FilePath = file.FilePath
                                })
                        let archiveFiles =
                            PsPrepareArchive.prepareArchiveForAudio
                                aprops.ArchiveConfig
                                (PiProgId.create job.ProgramId)
                                archiveFiles
                                aprops.FileSystem
                                mailbox.Log.Value
                        let job =
                            { job with
                                Files = transcodedFilesFromArchive job archiveFiles
                            }
                        sender <! PrepareArchiveResult.Ok job
                    with exn ->
                        sender <! PrepareArchiveResult.Error exn

                    return! ignored ()
                }

            idle ()

    let psRadioHandlerActor (aprops: PsRadioHandlerProps) (mailbox: Actor<_>) =

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

        let rec idle () =
            logDebug mailbox "idle"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? QueueMessage<PsAudioTranscodingJob> as message' ->
                        let job = message'.Payload
                        logDebug mailbox $"Job: {job}"

                        prepareArchive <! job

                        await_archive_files job message'.Ack
                    | LifecycleEvent _ -> ignored ()
                    | _ ->
                        logWarning mailbox $"Unhandled radio handler message in idle state: %A{message}"
                        unhandled ()
            }
        and await_archive_files originalJob ack =
            logDebug mailbox "await_archive_files"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? PrepareArchive.PrepareArchiveResult as result ->
                        match result with
                        | Ok job ->
                            aprops.GetPsMediator() <! PsShardMessage.AudioTranscodingDetails(job)
                            awaiting_details_ack originalJob ack
                        | Error exn ->
                            let error = $"Failed to prepare archive for job {originalJob}"
                            logErrorWithExn mailbox exn error
                            sendNack mailbox ack error
                            idle ()
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and awaiting_details_ack originalJob ack =
            logDebug mailbox $"awaiting_details_ack"
            mailbox.SetReceiveTimeout aprops.DetailsAckTimeout
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? AudioTranscodingDetailsAck ->
                        mailbox.SetReceiveTimeout None
                        aprops.SaveToEventStore (PiProgId.create originalJob.ProgramId) originalJob.CarrierId "TranscodingDetails" originalJob
                        sendAck mailbox ack $"Forwarded audio transcoding details for %s{originalJob.ProgramId}"
                        idle ()
                    | :? ReceiveTimeout -> raise <| TimeoutException "Timeout awaiting details ack"
                    | LifecycleEvent e -> ignored ()
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }

        idle ()
