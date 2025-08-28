namespace Nrk.Oddjob.Upload.GlobalConnect

module GlobalConnectFileUploadActor =

    open System
    open Akka.Actor
    open Akkling

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Events
    open Nrk.Oddjob.Core.Queues
    open Nrk.Oddjob.Core.S3.S3Types
    open Nrk.Oddjob.Core.HelperActors.PriorityQueueActor
    open Nrk.Oddjob.Upload
    open Nrk.Oddjob.Upload.UploadTypes
    open Nrk.Oddjob.Upload.UploadUtils

    open GlobalConnectUtils
    open GlobalConnectFileEventProducer

    [<RequireQualifiedAccess>]
    type ValidationResult =
        | Discard of reason: string
        | Skip
        | Continue of GlobalConnectCommand

    type CommandContext =
        {
            Command: S3Command
            JobContext: FileJobContext
            Action: GlobalConnectCommand
            RetryCount: int
        }

    [<Literal>]
    let Gigabyte = 1024UL * 1024UL * 1024UL
    [<Literal>]
    let MinFileCompletionTimeout = 5L
    [<Literal>]
    let FileSizeFactor = 2UL

    let rec globalConnectFileUploadActor (aprops: GlobalConnectFileUploadProps) (mailbox: Actor<_>) =

        let eventPublisher =
            spawn
                mailbox
                (makeActorName [ EventPublisherActorName ])
                (propsNamed "upload-globalconnect-file-eventpublisher" (UploadEventPublisher.uploadEventsPublisherActor aprops.UploadEventsPublisherProps))
            |> retype

        let getActionContext (executionContext: ExecutionContext) =
            GlobalConnectActionContext.create
                mailbox.Log.Value
                {
                    ValidationMode = MediaSetValidation.LocalAndRemoteWithCleanup
                    OverwriteMode = executionContext.Header.OverwriteMode
                    StorageCleanupDelay = aprops.StorageCleanupDelay
                }
                aprops.ActionEnvironment

        let evaluateActions state externalState executionContext =
            let actionContext = getActionContext executionContext
            match aprops.ResourceRef, state with
            | ResourceRef.File fileRef, GlobalConnectResourceState.File state' ->
                let originState =
                    { CurrentGlobalConnectState.Zero with
                        Files = [ fileRef, state' ] |> Map.ofList
                    }
                GlobalConnectActions.forFile actionContext executionContext.MediaSetId originState fileRef state' externalState.Desired
            | ResourceRef.Subtitles subRef, GlobalConnectResourceState.Subtitles state' ->
                let originState =
                    { CurrentGlobalConnectState.Zero with
                        Subtitles = [ subRef, state' ] |> Map.ofList
                    }
                GlobalConnectActions.forSubtitles actionContext executionContext.MediaSetId originState subRef state' externalState.Desired
            | ResourceRef.Smil, GlobalConnectResourceState.Smil state' ->
                GlobalConnectActions.forSmil actionContext executionContext.MediaSetId externalState.Origin state' externalState.Desired
            | _, _ -> invalidOp $"Invalid resource/state combination {aprops.ResourceRef}, {state}"

        let validateActions actions state externalState =
            match actions, state with
            | [], GlobalConnectResourceState.File state' when state'.File = GlobalConnectFile.Zero -> ValidationResult.Discard "No actions and empty file"
            | [], _ -> ValidationResult.Skip
            | [ action ], _ ->
                let hasContent =
                    match aprops.ResourceRef with
                    | ResourceRef.File fileRef -> externalState.Desired.Content |> ContentSet.tryGetFile fileRef |> Option.isSome
                    | ResourceRef.Subtitles subRef -> externalState.Desired.Content |> ContentSet.tryGetSubtitles subRef |> Option.isSome
                    | ResourceRef.Smil -> true
                match action with
                | GlobalConnectCommand.DeleteFile _
                | GlobalConnectCommand.DeleteSubtitles _
                | GlobalConnectCommand.DeleteSmil _ -> ValidationResult.Continue action
                | _ when hasContent -> ValidationResult.Continue action
                | _ -> ValidationResult.Discard "No content file found"
            | actions, _ ->
                logDebug mailbox $"Unable to process multiple file actions at once ({actions})"
                ValidationResult.Discard "Multiple file actions found"

        let createCommand (jobContext: FileJobContext) (resource: GlobalConnectResource) (oldResource: GlobalConnectResource) =

            let getDestinationPath remotePath =
                GlobalConnectAccessRestrictions.apply jobContext.DesiredGeoRestriction remotePath

            let createDeleteFileCommand remotePath =
                S3Command.DeleteFile
                    {
                        RemotePath = remotePath
                        JobContext = Some jobContext
                    }

            match jobContext.Job with
            | OriginJob.GlobalConnectCommand cmd ->
                match cmd.Command, resource, oldResource with
                | GlobalConnectCommand.UploadFile _, GlobalConnectResource.File file, _ ->
                    S3Command.UploadFile
                        {
                            SourcePath = file.SourcePath
                            RemotePath = file.RemotePath
                            JobContext = Some jobContext
                        }
                | GlobalConnectCommand.UploadSubtitles _, GlobalConnectResource.Subtitles file, _ ->
                    match file.Subtitles.SourcePath with
                    | SubtitlesLocation.FilePath fp ->
                        S3Command.UploadFile
                            {
                                SourcePath = fp
                                RemotePath = file.RemotePath
                                JobContext = Some jobContext
                            }
                    | SubtitlesLocation.AbsoluteUrl url ->
                        S3Command.UploadFileFromUrl
                            {
                                SourceUrl = url
                                RemotePath = file.RemotePath
                                JobContext = Some jobContext
                            }
                | GlobalConnectCommand.UploadSmil _, GlobalConnectResource.Smil smil, _ ->
                    S3Command.UploadText
                        {
                            ContentBody = smil.Content.Serialize()
                            RemotePath = smil.RemotePath
                            JobContext = Some jobContext
                        }
                | GlobalConnectCommand.MoveFile _, GlobalConnectResource.File file, _ ->
                    let sourcePath = oldResource.RemotePath
                    let destinationPath = getDestinationPath file.RemotePath
                    if sourcePath = destinationPath then
                        invalidOp $"Source and destination path are the same ({sourcePath})"
                    S3Command.MoveFile
                        {
                            SourcePath = sourcePath
                            DestinationPath = destinationPath
                            JobContext = Some jobContext
                        }
                | GlobalConnectCommand.MoveSubtitles _, GlobalConnectResource.Subtitles file, _ ->
                    let sourcePath = oldResource.RemotePath
                    let destinationPath = getDestinationPath file.RemotePath
                    if sourcePath = destinationPath then
                        invalidOp $"Source and destination path are the same ({sourcePath})"
                    S3Command.MoveFile
                        {
                            SourcePath = sourcePath
                            DestinationPath = destinationPath
                            JobContext = Some jobContext
                        }
                | GlobalConnectCommand.MoveSmil _, GlobalConnectResource.Smil smil, GlobalConnectResource.Smil oldSmil ->
                    let oldPath = oldSmil.RemotePath
                    let newPath = getDestinationPath smil.RemotePath
                    S3Command.UpdateText
                        {
                            SourcePath = oldPath
                            DestinationPath = newPath
                            ContentBody = smil.Content.Serialize()
                            JobContext = Some jobContext
                        }
                | GlobalConnectCommand.DeleteFile _, GlobalConnectResource.File file, _ -> createDeleteFileCommand file.RemotePath
                | GlobalConnectCommand.DeleteSubtitles _, GlobalConnectResource.Subtitles file, _ -> createDeleteFileCommand file.RemotePath
                | GlobalConnectCommand.DeleteSmil _, GlobalConnectResource.Smil smil, _ -> createDeleteFileCommand smil.RemotePath
                | _ -> notSupported $"Unsupported combination %s{jobContext.Job.CommandName}, {resource}"

        let createGlobalConnectSmil (mediaSetId: MediaSetId) externalState newVersion =
            let content = GlobalConnectSmil.fromCurrentState externalState.Desired externalState.Origin
            let smilFileName = GlobalConnectSmil.generateSmilFileName content newVersion
            let remotePathBase = GlobalConnectPathBase.fromGeoRestriction mediaSetId externalState.Desired.GeoRestriction
            let remotePath = $"{remotePathBase}/{smilFileName}"
            {
                FileName = FileName smilFileName
                Content = content
                RemotePath = RelativeUrl remotePath
                Version = newVersion
            }

        let assignGlobalConnectResource state mediaSetId action (externalState: ExternalState) =
            match action, state with
            | GlobalConnectCommand.UploadFile(UploadFileCommand fileRef), GlobalConnectResourceState.File state' ->
                match externalState.Desired.Content |> ContentSet.tryGetFile fileRef with
                | Some file ->
                    let remotePathBase = GlobalConnectPathBase.fromGeoRestriction mediaSetId externalState.Desired.GeoRestriction
                    let globalConnectFile = GlobalConnectFile.fromContentFile remotePathBase file
                    aprops.MediaSetController <! MediaSetCommand.AssignGlobalConnectFile(fileRef, globalConnectFile)
                    GlobalConnectResourceState.File { state' with File = globalConnectFile }
                | None -> state
            | GlobalConnectCommand.MoveFile(MoveFileCommand fileRef), GlobalConnectResourceState.File state' ->
                let globalConnectFile =
                    { state'.File with
                        RemotePath = GlobalConnectAccessRestrictions.apply externalState.Desired.GeoRestriction state'.File.RemotePath
                    }
                aprops.MediaSetController <! MediaSetCommand.AssignGlobalConnectFile(fileRef, globalConnectFile)
                state
            | GlobalConnectCommand.UploadSubtitles(UploadSubtitlesCommand subRef), GlobalConnectResourceState.Subtitles state' ->
                match externalState.Desired.Content |> ContentSet.tryGetSubtitles subRef with
                | Some file ->
                    let remotePathBase = GlobalConnectPathBase.fromGeoRestriction mediaSetId externalState.Desired.GeoRestriction
                    let globalConnectSubtitles = GlobalConnectSubtitles.fromSubtitlesFile remotePathBase file
                    aprops.MediaSetController <! MediaSetCommand.AssignGlobalConnectSubtitles globalConnectSubtitles
                    GlobalConnectResourceState.Subtitles
                        { state' with
                            Subtitles = globalConnectSubtitles
                        }
                | None -> state
            | GlobalConnectCommand.MoveSubtitles(MoveSubtitlesCommand _), GlobalConnectResourceState.Subtitles state' ->
                let globalConnectSubtitles: GlobalConnectSubtitles =
                    { state'.Subtitles with
                        RemotePath = GlobalConnectAccessRestrictions.apply externalState.Desired.GeoRestriction state'.Subtitles.RemotePath
                    }
                aprops.MediaSetController <! MediaSetCommand.AssignGlobalConnectSubtitles globalConnectSubtitles
                state
            | GlobalConnectCommand.UploadSmil _, GlobalConnectResourceState.Smil state'
            | GlobalConnectCommand.MoveSmil _, GlobalConnectResourceState.Smil state' ->
                let currentVersion =
                    if state'.Smil.IsEmpty() then
                        externalState.Origin.Version
                    else
                        state'.Smil.Version
                let smil = createGlobalConnectSmil mediaSetId externalState (currentVersion + 1)
                aprops.MediaSetController <! MediaSetCommand.AssignGlobalConnectSmil smil
                GlobalConnectResourceState.Smil { state' with Smil = smil }
            | _ -> state

        let ensurePersistentStateIsUpToDate (state: GlobalConnectResourceState) (desiredState: DesiredMediaSetState) =
            if desiredState.Content = ContentSet.Empty && state.RemoteState.State <> DistributionState.Deleted then
                // Repair local file state
                let remoteState =
                    {
                        State = DistributionState.Deleted
                        Timestamp = DateTimeOffset.Now
                    }
                let remoteResult = RemoteResult.Ok()
                match aprops.ResourceRef, state with
                | ResourceRef.File fileRef, GlobalConnectResourceState.File state ->
                    (retype aprops.MediaSetController)
                    <! MediaSetEvent.ReceivedRemoteFileState(Origin.GlobalConnect, fileRef, remoteState, remoteResult)
                    GlobalConnectResourceState.File
                        { state with
                            RemoteState = remoteState
                            LastResult = remoteResult
                        }
                | ResourceRef.Subtitles subRef, GlobalConnectResourceState.Subtitles state ->
                    (retype aprops.MediaSetController)
                    <! MediaSetEvent.ReceivedRemoteSubtitlesFileState(Origin.GlobalConnect, subRef, remoteState, remoteResult)
                    GlobalConnectResourceState.Subtitles
                        { state with
                            RemoteState = remoteState
                            LastResult = remoteResult
                        }
                | ResourceRef.Smil, GlobalConnectResourceState.Smil state ->
                    (retype aprops.MediaSetController)
                    <! MediaSetEvent.ReceivedRemoteSmilState(Origin.GlobalConnect, state.Smil.Version, remoteState, remoteResult)
                    GlobalConnectResourceState.Smil
                        { state with
                            RemoteState = remoteState
                            LastResult = remoteResult
                        }
                | _, _ -> invalidOp $"Invalid resource/state combination {aprops.ResourceRef}, {state}"
            else
                state

        let handleConcurrentJob (msg: Message<ApplyState>) state externalState =
            let actions = evaluateActions state externalState msg.Payload.ExecutionContext
            let newAction =
                match validateActions actions state externalState with
                | ValidationResult.Continue action -> Some action
                | _ -> None
            match newAction with
            | Some _ -> newAction
            | None ->
                let message = "Skipping message with no evaluated action"
                logDebug mailbox message
                sendAck mailbox msg.Ack message
                None

        let getFileCompletionTimeout (commandContext: CommandContext) =
            match commandContext.Command with
            | S3Command.UploadFile cmd when Option.isSome cmd.SourcePath ->
                let fileLength = aprops.ActionEnvironment.LocalFileSystem.GetFileLength(FilePath.value cmd.SourcePath.Value)
                Math.Max(MinFileCompletionTimeout, (fileLength / Gigabyte) * FileSizeFactor) |> float |> TimeSpan.FromMinutes
            | _ -> TimeSpan.FromMinutes MinFileCompletionTimeout
            |> Some

        let fileEventProducer = GlobalConnectFileEventProducer(aprops, eventPublisher)

        let publishSkippedEvents state executionContext =
            let clientGroupId = getClientGroupIdFromContentId aprops.ClientRef aprops.ClientContentId
            match aprops.ResourceRef, state with
            | ResourceRef.File _, GlobalConnectResourceState.File _ ->
                fileEventProducer.PublishSkippedEvents executionContext aprops.ResourceRef state clientGroupId
            | ResourceRef.Subtitles _, GlobalConnectResourceState.Subtitles _ ->
                fileEventProducer.PublishSkippedEvents executionContext aprops.ResourceRef state clientGroupId
            | ResourceRef.Smil, GlobalConnectResourceState.Smil _ ->
                fileEventProducer.PublishSkippedEvents executionContext aprops.ResourceRef state clientGroupId
            | _, _ -> invalidOp $"Invalid resource/state combination {aprops.ResourceRef}, {state}"

        let logActorState stageName (remoteState: GlobalConnectResourceState) (command: string) =
            let text = $"%s{stageName} ({remoteState.RemoteState.State})"
            logActorState mailbox text (Some command)

        let getTraceTags (job: OriginJob) (resourceRef: ResourceRef) (originEvent: FileJobOriginEvent option) =
            let mediaSetTag = ("oddjob.mediaset.mediaSetId", job.MediaSetId.Value)
            let resourceTags =
                match resourceRef, resourceRef.PartId with
                | ResourceRef.File fileRef, Some partId ->
                    [
                        ("oddjob.mediaset.partId", partId.Value)
                        ("oddjob.mediaset.qualityId", $"{fileRef.QualityId.Value}")
                    ]
                | ResourceRef.File fileRef, None -> [ ("oddjob.mediaset.qualityId", $"{fileRef.QualityId.Value}") ]
                | _, Some partId -> [ ("oddjob.mediaset.partId", partId.Value) ]
                | _, None -> []
            match originEvent with
            | Some event -> ("oddjob.mediaset.jobStatus", $"{event.JobStatus}") :: mediaSetTag :: resourceTags
            | None -> mediaSetTag :: resourceTags

        let rec initiating (state: GlobalConnectResourceState) = idle' state true

        and idle state = idle' state false

        and idle' state initiating =
            if initiating then
                logDebug mailbox $"Initial actor state: {state}"
            else
                mailbox.UnstashAll()
            UploadUtils.logActorState mailbox "idle" None
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? Message<ApplyState> as msg ->
                        let externalState = createGlobalConnectState msg.Payload
                        logDebug mailbox (getMessageSummary msg.Payload.ExecutionContext)
                        validation state externalState msg.Payload.ExecutionContext msg.Ack
                    | :? FileJobOriginEvent as event ->
                        logWarning mailbox $"Skipping provider file event %A{event.GetSummary()} from the previous actor incarnation."
                        idle state
                    | LifecycleEvent e -> lifecycle e
                    | _ ->
                        logWarning mailbox $"Unexpected message of type {message.GetType()} while actor is idle"
                        unhandled ()
            }

        and validation state externalState executionContext ack =
            logActorState "validation" state executionContext.CommandName
            let actions = evaluateActions state externalState executionContext
            match validateActions actions state externalState with
            | ValidationResult.Discard reason -> discard state executionContext reason ack
            | ValidationResult.Skip -> skip state externalState.Desired executionContext ack
            | ValidationResult.Continue action -> prepareCommand state externalState executionContext action ack

        and discard state executionContext reason ack =
            logActorState "discard" state executionContext.CommandName
            sendAck mailbox ack <| $"Discarded job (%s{reason})"
            idle state

        and skip state desiredState executionContext ack =
            logActorState "skip" state executionContext.CommandName
            let state = ensurePersistentStateIsUpToDate state desiredState
            match executionContext with
            | ExecutionContext.MediaSetJob _ -> publishSkippedEvents state executionContext
            | ExecutionContext.OriginStateUpdate _ -> ()
            sendAck mailbox ack "Skipped job"
            idle state

        and prepareCommand state externalState executionContext action ack =
            logActorState $"prepareCommand {action}" state executionContext.CommandName
            let oldState = state
            let newState = assignGlobalConnectResource state executionContext.MediaSetId action externalState
            let fileAccessRestrictions = GlobalConnectAccessRestrictions.parse newState.Resource.RemotePath
            let clientGroupId = getClientGroupIdFromContentId aprops.ClientRef aprops.ClientContentId
            let jobContext =
                OriginJob.GlobalConnectCommand
                    {
                        Header = executionContext.Header
                        MediaSetId = executionContext.MediaSetId
                        Command = action
                    }
                |> FileJobContext.create externalState.Desired.GeoRestriction fileAccessRestrictions clientGroupId
            fileEventProducer.PublishInitialEvents jobContext aprops.ResourceRef newState
            let command = createCommand jobContext newState.Resource oldState.Resource
            let commandContext =
                {
                    Command = command
                    JobContext = jobContext
                    Action = action
                    RetryCount = 0
                }
            executeCommand newState externalState commandContext ack

        and executeCommand state externalState commandContext ack =
            logActorState $"executeCommand %s{commandContext.JobContext.Job.CommandName}" state commandContext.JobContext.Job.CommandName
            logInfo mailbox $"Executing {commandContext.Command}"
            let traceSpan =
                createTraceSpan mailbox "globalConnectFileUploadActor.executeCommand" (getTraceTags commandContext.JobContext.Job aprops.ResourceRef None)
            aprops.S3Queue
            <! PriorityQueueCommand.Enqueue
                {
                    Message = Message.createWithAck commandContext.Command ack
                    Sender = retype mailbox.Self
                }
            let traceContext = getTraceContext mailbox
            waitForCompletion state externalState commandContext ack traceContext traceSpan

        and waitForCompletion state externalState commandContext ack traceContext traceSpan =
            mailbox.UnstashAll()
            mailbox.SetReceiveTimeout(getFileCompletionTimeout commandContext)
            awaitingCompletion state externalState commandContext ack traceContext traceSpan

        and awaitingCompletion state externalState commandContext ack traceContext traceSpan =
            logActorState $"awaitingCompletion {getMessageTitle commandContext.JobContext.Job}" state commandContext.JobContext.Job.CommandName
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? Message<ApplyState> as msg ->
                        match handleConcurrentJob msg state externalState with
                        | Some newAction ->
                            logDebug mailbox $"Stashing action {newAction} while actor is busy with {commandContext.Action}"
                            mailbox.Stash()
                        | None -> ()
                        ignored ()
                    | :? FileJobOriginEvent as originEvent when ackHasMatch originEvent.Ack mailbox ack ->
                        let remoteState = fileEventProducer.HandleOriginEvents originEvent state
                        if originEvent.JobStatus = JobStatus.Queued then
                            mailbox.SetReceiveTimeout(getFileCompletionTimeout commandContext)
                            ignored ()
                        else
                            use _ =
                                createTraceSpanOnCondition mailbox "globalConnectFileUploadActor.awaitingCompletion"
                                <| getTraceTags commandContext.JobContext.Job aprops.ResourceRef (Some originEvent)
                                <| fun () -> originEvent.JobStatus.IsFinal()
                            let events = fileEventProducer.CreateProgressEvents originEvent aprops.ResourceRef state
                            if events.Length > 0 then
                                logDebug mailbox $"Publishing %d{events.Length} progress events"
                                fileEventProducer.PublishProgressEvents events
                                mailbox.SetReceiveTimeout aprops.PublishConfirmationTimeout
                                awaitingEventsConfirmation
                                    state
                                    externalState
                                    remoteState
                                    originEvent
                                    commandContext
                                    events.Length
                                    false
                                    ack
                                    traceContext
                                    traceSpan
                            else
                                updateStateOnOriginEvent state externalState remoteState originEvent commandContext ack traceContext
                    | :? ReceiveTimeout ->
                        logWarning mailbox $"Retrying {commandContext.Command} after completion timeout"
                        mailbox.SetReceiveTimeout None
                        executeCommand
                            state
                            externalState
                            { commandContext with
                                RetryCount = commandContext.RetryCount + 1
                            }
                            ack
                    | LifecycleEvent e -> lifecycle e
                    | _ ->
                        logWarning mailbox $"Unexpected message of type {message.GetType()} while actor is awaiting completion"
                        unhandled ()
            }

        and awaitingEventsConfirmation state externalState remoteState originEvent commandContext eventCount hasErrors ack traceContext traceSpan =
            logActorState $"awaitingEventsConfirmation for {eventCount} events" state commandContext.JobContext.Job.CommandName
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? UploadEventPublisher.PublishEventResult as msg ->
                        let eventCount = eventCount - 1
                        let hasErrors =
                            match msg with
                            | Ok _ -> hasErrors
                            | UploadEventPublisher.PublishEventResult.Error exn ->
                                logErrorWithExn mailbox exn "Error while publishing event"
                                true
                        if eventCount = 0 then
                            if hasErrors then
                                sendNack mailbox ack "Could not publish events"
                            else
                                sendAck mailbox ack "Job is completed"
                            mailbox.SetReceiveTimeout None
                            updateStateOnOriginEvent state externalState remoteState originEvent commandContext ack traceContext
                        else
                            awaitingEventsConfirmation
                                state
                                externalState
                                remoteState
                                originEvent
                                commandContext
                                eventCount
                                hasErrors
                                ack
                                traceContext
                                traceSpan
                    | :? ReceiveTimeout ->
                        logWarning mailbox $"Retrying {commandContext.Command} after events confirmation timeout"
                        mailbox.SetReceiveTimeout None
                        executeCommand
                            state
                            externalState
                            { commandContext with
                                RetryCount = commandContext.RetryCount + 1
                            }
                            ack
                    | LifecycleEvent e -> lifecycle e
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }

        and updateStateOnOriginEvent state externalState remoteState originEvent commandContext ack traceContext =
            logActorState "updateStateOnOriginEvent" state commandContext.JobContext.Job.CommandName
            let traceSpan =
                createTraceSpanForContext
                    mailbox
                    traceContext
                    $"globalConnectFileUploadActor.updateState:{originEvent.JobStatus}"
                    (getTraceTags commandContext.JobContext.Job aprops.ResourceRef (Some originEvent))
            let newState =
                match state with
                | GlobalConnectResourceState.File file ->
                    let remotePath =
                        if remoteState.State = DistributionState.Completed then
                            GlobalConnectAccessRestrictions.apply commandContext.JobContext.DesiredGeoRestriction file.File.RemotePath
                        else
                            file.File.RemotePath
                    GlobalConnectResourceState.File
                        { file with
                            File.RemotePath = remotePath
                            RemoteState = remoteState
                        }
                | GlobalConnectResourceState.Subtitles sub ->
                    let remotePath =
                        if remoteState.State = DistributionState.Completed then
                            GlobalConnectAccessRestrictions.apply commandContext.JobContext.DesiredGeoRestriction sub.Subtitles.RemotePath
                        else
                            sub.Subtitles.RemotePath
                    GlobalConnectResourceState.Subtitles
                        { sub with
                            Subtitles.RemotePath = remotePath
                            RemoteState = remoteState
                        }
                | GlobalConnectResourceState.Smil smil when remoteState.State = DistributionState.Deleted ->
                    GlobalConnectResourceState.Smil
                        { smil with
                            Smil = GlobalConnectSmil.Zero
                            RemoteState = remoteState
                        }
                | GlobalConnectResourceState.Smil smil -> GlobalConnectResourceState.Smil { smil with RemoteState = remoteState }
            if newState.RemoteState.State = DistributionState.Failed then
                traceSpan.Dispose()
                if commandContext.RetryCount < aprops.FailingFilesRetryInterval.Length then
                    awaitingRetry newState externalState commandContext ack
                else
                    idle newState
            else if newState.RemoteState.State.IsFinal() then
                traceSpan.Dispose()
                idle newState
            else
                waitForCompletion newState externalState commandContext ack traceContext traceSpan

        and awaitingRetry state externalState commandContext ack =
            let text = sprintf "%s %A" "awaitingRetry" commandContext.JobContext.Job
            logActorState text state commandContext.JobContext.Job.CommandName
            let delay = aprops.FailingFilesRetryInterval[commandContext.RetryCount]
            if delay > TimeSpan.MinValue then
                logDebug mailbox $"Awaiting retry in {delay}"
                mailbox.SetReceiveTimeout <| Some delay
                actor {
                    let! (message: obj) = mailbox.Receive()
                    return!
                        match message with
                        | :? Message<ApplyState> as msg ->
                            match handleConcurrentJob msg state externalState with
                            | Some newAction ->
                                logDebug mailbox $"Cancelled retry of {commandContext.Action} due to new evaluated action {newAction}"
                                mailbox.Stash()
                                mailbox.SetReceiveTimeout None
                                idle state
                            | None -> ignored ()
                        | :? ReceiveTimeout ->
                            logWarning mailbox $"Retrying {commandContext.Action} after command completion timeout"
                            mailbox.SetReceiveTimeout None
                            executeCommand
                                state
                                externalState
                                { commandContext with
                                    RetryCount = commandContext.RetryCount + 1
                                }
                                ack
                        | LifecycleEvent e -> lifecycle e
                        | _ ->
                            logWarning mailbox $"Unexpected message of type {message.GetType()} while actor is awaiting retry"
                            unhandled ()
                }
            else
                logWarning mailbox $"Retrying {commandContext.Action} immediately"
                executeCommand
                    state
                    externalState
                    { commandContext with
                        RetryCount = commandContext.RetryCount + 1
                    }
                    ack

        and lifecycle e =
            match e with
            | PreRestart(exn, message) ->
                logErrorWithExn mailbox exn $"PreRestart due to error when processing message {message.GetType()} [{message}]"
                match message with
                | :? Message<ApplyState> as msg ->
                    sendNack mailbox msg.Ack "Failed processing upload message"
                    ignored ()
                | _ -> unhandled ()
            | _ -> ignored ()

        initiating aprops.InitialState
