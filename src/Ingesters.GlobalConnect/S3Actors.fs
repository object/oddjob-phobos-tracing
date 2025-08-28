namespace Nrk.Oddjob.Ingesters.GlobalConnect


module S3Actors =

    open System
    open Akkling
    open FSharpx.Collections
    open System.Net

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Core.Events
    open Nrk.Oddjob.Core.Queues
    open Nrk.Oddjob.Core.S3.S3Types
    open Nrk.Oddjob.Core.HelperActors.PriorityQueueActor
    open IngesterMetrics

    type private S3JobContext =
        {
            SinceTime: DateTimeOffset
            BytesToTransfer: UInt64 option
        } // Some in case of files, None in case of subtitles

    let private toJobStatus s3event =
        match s3event with
        | S3Event.Initiated -> JobStatus.Initiated
        | S3Event.Error _ -> JobStatus.Failed
        | S3Event.Rejected _ -> JobStatus.Rejected
        | S3Event.Completed -> JobStatus.Completed

    let private toResult s3event =
        match s3event with
        | S3Event.Error(code, error)
        | S3Event.Rejected(code, error) -> Result.Error(code, error)
        | S3Event.Completed
        | _ -> Result.Ok()

    let private sendFileJobEvent (ctx: S3MessageContext) jobStatus result =
        let now = DateTimeOffset.Now
        match ctx.Message.Payload with
        | S3Command.UploadFile cmd -> cmd.JobContext
        | S3Command.UploadFileFromUrl cmd -> cmd.JobContext
        | S3Command.UploadText cmd -> cmd.JobContext
        | S3Command.MoveFile cmd -> cmd.JobContext
        | S3Command.UpdateText cmd -> cmd.JobContext
        | S3Command.DeleteFile cmd -> cmd.JobContext
        | S3Command.DeleteFiles cmd -> cmd.JobContext
        |> Option.iter (fun jobContext ->
            let fileJobEvent =
                {
                    JobContext = jobContext
                    JobStatus = jobStatus
                    Origin = Origin.GlobalConnect
                    Result = result
                    Timestamp = now
                    Ack = ctx.Message.Ack
                }
            ctx.Sender <! fileJobEvent)

    let getOnEnqueueDequeueForCommonQueue (instrumentationPublisher: IIngesterInstrumentation) (contentChangePublisher: IPublishQueueContentChange) queueName =

        let enqueueMessage (messageCtx: S3MessageContext) (messages: IPriorityQueue<S3MessageContext>) mailbox =
            let message = messageCtx.Message
            logDebug mailbox $"Queuing command [%s{message.Payload.GetCommandTarget()}] while actor is busy"
            logDebug mailbox $"%s{getUnionCaseName message.Payload} %s{message.Payload.GetCommandTarget()}"
            logDebug mailbox $"Queued message count: %d{messages.Length}"
            instrumentationPublisher.WriteQueueLength queueName messages.Length
            contentChangePublisher.ItemAdded(messageCtx)
            sendFileJobEvent messageCtx JobStatus.Queued (Result.Ok())

        let dequeueMessage (messageCtx: S3MessageContext) (messages: IPriorityQueue<S3MessageContext>) mailbox =
            logDebug mailbox $"Queued message count: %d{messages.Length}"
            messages |> Seq.iter (fun x -> sendFileJobEvent x JobStatus.Queued (Result.Ok()))
            instrumentationPublisher.WriteQueueLength queueName messages.Length
            contentChangePublisher.ItemRemoved(messageCtx)

        enqueueMessage, dequeueMessage

    [<NoEquality; NoComparison>]
    type S3Props =
        {
            ServiceResolver: (Type -> obj) option
            FileSystem: IFileSystemInfo
            HttpGet: string -> Result<string, int * string>
            PathMappings: OddjobConfig.PathMapping list
            PriorityQueue: IActorRef<PriorityQueueCommand<S3MessageContext>>
        }

    type private CompletionStatus = | Completed

    let s3Actor (aprops: S3Props) (mailbox: Actor<_>) =

        let serviceResolver =
            aprops.ServiceResolver
            |> Option.defaultWith (fun () ->
                fun t ->
                    let sp = Akka.DependencyInjection.DependencyResolver.For(mailbox.System)
                    sp.Resolver.GetService(t))

        let s3Client = serviceResolver typedefof<IS3Client> :?> IS3Client
        let instrumentationPublisher = serviceResolver typedefof<IIngesterInstrumentation> :?> IIngesterInstrumentation
        let commandStatusPublisher = serviceResolver typedefof<IPublishCommandStatus> :?> IPublishCommandStatus

        let ingesterId = mailbox.Self.Path.Name.Split '/' |> Seq.last

        let writeIngesterStatus (ingesterStatus: GlobalConnectIngesterStatus) =
            instrumentationPublisher.WriteStatus ingesterId (GlobalConnectIngesterStatus.toInt ingesterStatus)

        let writeCommandStatus s3Event message =
            match s3Event with
            | S3Event.Initiated -> commandStatusPublisher.CommandInitiated ingesterId message
            | S3Event.Completed -> commandStatusPublisher.CommandCompleted ingesterId message
            | S3Event.Error(_, error) -> commandStatusPublisher.CommandFailed ingesterId message error
            | S3Event.Rejected(_, error) -> commandStatusPublisher.CommandRejected ingesterId message error

        let logActorState actorState =
            logDebug mailbox $"Actor state is [%s{actorState}]"

        /// This is done so that when the ActorSystem is shut down, the graphs don't show ingesters as busy.
        let onLifecycle (e: LifecycleEvent) =
            match e with
            | LifecycleEvent.PostStop ->
                writeIngesterStatus GlobalConnectIngesterStatus.EnteredIdle
                instrumentationPublisher.WriteIdle ingesterId
                instrumentationPublisher.WriteProgress ingesterId 0UL
            | _ -> ()

        let handleS3ApiResult operationName (result: S3ClientResult) message targetKey ack =
            match result with
            | Result.Ok _ ->
                logDebug mailbox $"%s{operationName} for %s{targetKey} is completed"
                let s3Event = S3Event.Completed
                sendFileJobEvent message (toJobStatus s3Event) (toResult s3Event)
                writeIngesterStatus GlobalConnectIngesterStatus.CommandCompleted
                writeCommandStatus s3Event message
            | Result.Error error ->
                sendNack mailbox ack $"Failed to execute %s{operationName} for {targetKey}"
                logDebug mailbox $"%s{operationName} failed [%A{error}]"
                let errorMessage = error.ToString()
                let s3Event =
                    match error with
                    | :? System.IO.FileNotFoundException
                    | :? System.IO.InvalidDataException -> S3Event.Rejected(int HttpStatusCode.NotFound, errorMessage)
                    | :? System.Net.Http.HttpRequestException as ex when ex.StatusCode.HasValue && ex.StatusCode.Value = HttpStatusCode.NotFound ->
                        S3Event.Rejected(int HttpStatusCode.NotFound, errorMessage)
                    | _ -> S3Event.Error(int HttpStatusCode.InternalServerError, errorMessage)

                sendFileJobEvent message (toJobStatus s3Event) (toResult s3Event)
                writeIngesterStatus GlobalConnectIngesterStatus.CommandFailed
                writeCommandStatus s3Event message

        let handleCommand (message: S3MessageContext) =
            async {
                try
                    instrumentationPublisher.WriteBusy ingesterId
                    let s3Event = S3Event.Initiated
                    sendFileJobEvent message (toJobStatus s3Event) (toResult s3Event)
                    writeCommandStatus s3Event message
                    match message.Message.Payload with
                    | S3Command.UploadFile cmd ->
                        writeIngesterStatus GlobalConnectIngesterStatus.UploadFileReceived
                        match cmd.SourcePath with
                        | Some sourcePath ->
                            let localPath = sourcePath |> FilePath.tryMap aprops.PathMappings |> Option.getOrFail $"Unable to map path {cmd.SourcePath}"
                            let fileLength = aprops.FileSystem.GetFileLength localPath
                            instrumentationPublisher.WriteProgress ingesterId 0UL
                            use _ =
                                createTraceSpan
                                    mailbox
                                    "S3.UploadFile"
                                    [
                                        ("oddjob.s3.remotePath", $"{cmd.RemotePath.Value}")
                                        ("oddjob.workflow", "Uploading file to S3")
                                    ]
                            let timer = System.Diagnostics.Stopwatch.StartNew()
                            let result = s3Client.UploadFile cmd.RemotePath.Value localPath
                            handleS3ApiResult "UploadFile" result message cmd.RemotePath.Value message.Message.Ack
                            timer.Stop()
                            if Result.isOk result then
                                instrumentationPublisher.WriteFinal ingesterId fileLength timer.Elapsed
                        | None ->
                            let result = S3ClientResult.Error(System.IO.InvalidDataException("Missing source path"))
                            handleS3ApiResult "UploadFile" result message cmd.RemotePath.Value message.Message.Ack
                    | S3Command.UploadFileFromUrl cmd ->
                        writeIngesterStatus GlobalConnectIngesterStatus.UploadFileReceived
                        use _ =
                            createTraceSpan
                                mailbox
                                "S3.UploadFileFromUrl"
                                [
                                    ("oddjob.s3.remotePath", $"{cmd.RemotePath.Value}")
                                    ("oddjob.workflow", "Uploading file from URL to S3")
                                ]
                        let timer = System.Diagnostics.Stopwatch.StartNew()
                        let result: S3ClientResult =
                            match aprops.HttpGet cmd.SourceUrl.Value with
                            | Ok contentBody -> s3Client.UploadContent cmd.RemotePath.Value contentBody
                            | Result.Error(statusCode, message) ->
                                System.Net.Http.HttpRequestException(message, null, enum<HttpStatusCode> statusCode) :> exn |> S3ClientResult.Error
                        handleS3ApiResult "UploadFileFromUrl" result message cmd.RemotePath.Value message.Message.Ack
                        timer.Stop()
                        instrumentationPublisher.WriteFinal ingesterId 0UL timer.Elapsed
                    | S3Command.UploadText cmd ->
                        writeIngesterStatus GlobalConnectIngesterStatus.UploadFileReceived
                        use _ =
                            createTraceSpan
                                mailbox
                                "S3.UploadText"
                                [
                                    ("oddjob.s3.remotePath", $"{cmd.RemotePath.Value}")
                                    ("oddjob.workflow", "Uploading text")
                                ]
                        let timer = System.Diagnostics.Stopwatch.StartNew()
                        let result = s3Client.UploadContent cmd.RemotePath.Value cmd.ContentBody
                        handleS3ApiResult "UploadText" result message cmd.RemotePath.Value message.Message.Ack
                        timer.Stop()
                        instrumentationPublisher.WriteFinal ingesterId 0UL timer.Elapsed
                    | S3Command.MoveFile cmd ->
                        writeIngesterStatus GlobalConnectIngesterStatus.MoveFileReceived
                        let sourcePath = cmd.SourcePath.Value
                        let destinationPath = cmd.DestinationPath.Value
                        instrumentationPublisher.WriteProgress ingesterId 0UL
                        use _ =
                            createTraceSpan
                                mailbox
                                "S3.MoveFile"
                                [
                                    ("oddjob.s3.remotePath", $"{destinationPath}")
                                    ("oddjob.workflow", "Moving file in S3")
                                ]
                        let timer = System.Diagnostics.Stopwatch.StartNew()
                        let result = s3Client.MoveFile sourcePath destinationPath
                        handleS3ApiResult "MoveFile" result message cmd.SourcePath.Value message.Message.Ack
                        timer.Stop()
                        instrumentationPublisher.WriteFinal ingesterId 0UL timer.Elapsed
                    | S3Command.UpdateText cmd ->
                        writeIngesterStatus GlobalConnectIngesterStatus.MoveFileReceived
                        let sourcePath = cmd.SourcePath.Value
                        let destinationPath = cmd.DestinationPath.Value
                        if sourcePath = destinationPath then
                            invalidOp $"Source and destination path must differ ({sourcePath})"
                        instrumentationPublisher.WriteProgress ingesterId 0UL
                        use _ =
                            createTraceSpan
                                mailbox
                                "S3.MoveFile"
                                [
                                    ("oddjob.s3.remotePath", $"{destinationPath}")
                                    ("oddjob.workflow", "Updating text in S3")
                                ]
                        let timer = System.Diagnostics.Stopwatch.StartNew()
                        let result = s3Client.UploadContent destinationPath cmd.ContentBody |> Result.bind (fun _ -> s3Client.DeleteFile sourcePath)
                        handleS3ApiResult "MoveFileWithText" result message cmd.SourcePath.Value message.Message.Ack
                        timer.Stop()
                        instrumentationPublisher.WriteFinal ingesterId 0UL timer.Elapsed
                    | S3Command.DeleteFile cmd ->
                        writeIngesterStatus GlobalConnectIngesterStatus.DeleteFileReceived
                        instrumentationPublisher.WriteProgress ingesterId 0UL
                        use _ =
                            createTraceSpan
                                mailbox
                                "S3.DeleteFile"
                                [
                                    ("oddjob.s3.remotePath", $"{cmd.RemotePath.Value}")
                                    ("oddjob.workflow", "Deleting file from S3")
                                ]
                        let timer = System.Diagnostics.Stopwatch.StartNew()
                        let result = s3Client.DeleteFile cmd.RemotePath.Value
                        handleS3ApiResult "DeleteFile" result message cmd.RemotePath.Value message.Message.Ack
                        timer.Stop()
                        instrumentationPublisher.WriteFinal ingesterId 0UL timer.Elapsed
                    | S3Command.DeleteFiles cmd ->
                        use _ = createTraceSpan mailbox "S3.DeleteFiles" [ ("oddjob.workflow", $"Deleting {cmd.RemotePaths.Length} files from S3") ]
                        let result =
                            cmd.RemotePaths
                            |> List.fold
                                (fun (acc: Result<unit, AggregateException>) (remotePath: RelativeUrl) ->
                                    writeIngesterStatus GlobalConnectIngesterStatus.DeleteFileReceived
                                    instrumentationPublisher.WriteProgress ingesterId 0UL
                                    let timer = System.Diagnostics.Stopwatch.StartNew()
                                    let result = s3Client.DeleteFile remotePath.Value
                                    timer.Stop()
                                    instrumentationPublisher.WriteFinal ingesterId 0UL timer.Elapsed
                                    match acc, result with
                                    | _, Result.Ok _ -> acc
                                    | Result.Ok _, Result.Error exn -> AggregateException(exn) |> Result.Error
                                    | Result.Error acc_error, Result.Error result_error ->
                                        AggregateException(result_error :: (List.ofSeq acc_error.InnerExceptions)) |> Result.Error)
                                (Result.Ok())
                            |> Result.mapError (fun error -> error :> exn)
                        handleS3ApiResult "DeleteFiles" result message (String.Join(",", cmd.RemotePaths |> List.map _.Value)) message.Message.Ack
                with exn ->
                    logErrorWithExn mailbox exn $"Failed executing command {message.Message.Payload}"
                    let s3Event = S3Event.Error(int HttpStatusCode.InternalServerError, exn.Message)
                    sendFileJobEvent message (toJobStatus s3Event) (toResult s3Event)
                    writeIngesterStatus GlobalConnectIngesterStatus.CommandFailed
                    instrumentationPublisher.WriteFinal ingesterId 0UL (TimeSpan.FromSeconds 0.)
            }

        logInfo mailbox "Initiating S3 actor"

        let rec init () =
            logActorState "init"

            // Notify priority queue about the new S3 actor instance
            aprops.PriorityQueue <! PriorityQueueCommand.TryDequeue
            idle ()

        and idle () =
            logActorState "idle"
            writeIngesterStatus GlobalConnectIngesterStatus.EnteredIdle
            instrumentationPublisher.WriteIdle ingesterId
            instrumentationPublisher.WriteProgress ingesterId 0UL

            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? S3MessageContext as message ->
                        logDebug mailbox $"S3MessageContext in idle for {message.Message.Payload.GetCommandTarget()}"
                        execute message
                    | :? QueueWorkerEvent as message ->
                        match message with
                        | QueueNonEmpty ->
                            aprops.PriorityQueue <! PriorityQueueCommand.TryDequeue
                            idle ()
                    | LifecycleEvent e ->
                        onLifecycle e
                        ignored ()
                    | _ -> unhandled ()
            }

        and execute currentMessage =
            logActorState "execute"

            async {
                do! handleCommand currentMessage
                return CompletionStatus.Completed
            }
            |!> (retype mailbox.Self)

            awaitingCompletion currentMessage

        and awaitingCompletion currentMessage =
            logActorState "awaitingCompletion"

            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? CompletionStatus ->
                        logDebug mailbox $"Command for {currentMessage.Message.Payload.GetCommandTarget()} is completed"
                        aprops.PriorityQueue <! PriorityQueueCommand.TryDequeue
                        idle ()
                    | :? S3MessageContext as message ->
                        logDebug mailbox $"S3MessageContext in awaitingCompletion for {message.Message.Payload.GetCommandTarget()}"
                        logDebug mailbox "S3 Queue Item Bounce Back"
                        aprops.PriorityQueue <! PriorityQueueCommand.Enqueue message
                        awaitingCompletion currentMessage
                    | :? QueueWorkerEvent -> ignored ()
                    | LifecycleEvent e ->
                        onLifecycle e
                        ignored ()
                    | _ -> unhandled ()
            }

        init ()
