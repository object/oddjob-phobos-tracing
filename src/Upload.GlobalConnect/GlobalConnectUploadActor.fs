namespace Nrk.Oddjob.Upload.GlobalConnect

module GlobalConnectUploadActor =

    open Akkling

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Queues
    open Nrk.Oddjob.Core.Events
    open Nrk.Oddjob.Core.S3.S3Types
    open Nrk.Oddjob.Core.HelperActors.PriorityQueueActor
    open Nrk.Oddjob.Upload
    open Nrk.Oddjob.Upload.UploadTypes
    open Nrk.Oddjob.Upload.UploadUtils

    open GlobalConnectUtils
    open GlobalConnectStorageEventProducer
    open GlobalConnectFileUploadActor

    let rec globalConnectUploadActor (aprops: GlobalConnectUploadProps) (mailbox: Actor<_>) =

        let uploadEventsPublisherProps: UploadEventPublisher.UploadEventsPublisherProps =
            {
                ClientPublisher = retype aprops.ClientRef.ClientPublisher
            }

        let createActorProps resourceRef initialState =
            propsNamed
                "upload-globalconnect-file"
                (globalConnectFileUploadActor
                    {
                        ResourceRef = resourceRef
                        MediaSetController = aprops.MediaSetController
                        InitialState = initialState
                        ClientRef = aprops.ClientRef
                        ClientContentId = aprops.ClientContentId
                        BucketName = aprops.BucketName
                        FailingFilesRetryInterval = aprops.FailingFilesRetryInterval
                        PublishConfirmationTimeout = aprops.PublishConfirmationTimeout
                        S3Queue = aprops.S3Queue
                        ActionEnvironment = aprops.ActionEnvironment
                        StorageCleanupDelay = aprops.StorageCleanupDelay
                        UploadEventsPublisherProps = uploadEventsPublisherProps
                    })

        let createFileActorProps fileRef initialState =
            createActorProps (ResourceRef.File fileRef) (GlobalConnectResourceState.File initialState)

        let createSubtitlesActorProps subRef initialState =
            createActorProps (ResourceRef.Subtitles subRef) (GlobalConnectResourceState.Subtitles initialState)

        let createSmilActorProps initialState =
            createActorProps ResourceRef.Smil (GlobalConnectResourceState.Smil initialState)

        let eventPublisher =
            spawn mailbox.UntypedContext
            <| makeActorName [ EventPublisherActorName ]
            <| propsNamed "upload-globalconnect-eventpublisher" (UploadEventPublisher.uploadEventsPublisherActor uploadEventsPublisherProps)
            |> retype

        let getActionContext executionContext =
            let validationSettings =
                {
                    ValidationMode =
                        match executionContext with
                        | ExecutionContext.MediaSetJob ctx ->
                            match ctx.Job with
                            | MediaSetJob.RepairMediaSet _ -> MediaSetValidation.LocalAndRemoteWithCleanup
                            | _ -> MediaSetValidation.LocalAndRemote
                        | _ -> MediaSetValidation.LocalAndRemote
                    OverwriteMode = executionContext.Header.OverwriteMode
                    StorageCleanupDelay = aprops.StorageCleanupDelay
                }
            GlobalConnectActionContext.create mailbox.Log.Value validationSettings aprops.ActionEnvironment

        let getDefaultFileState externalState (executionContext: ExecutionContext) fileRef =
            let remotePathBase =
                GlobalConnectPathBase.fromGeoRestriction executionContext.MediaSetId externalState.Desired.GeoRestriction
            let globalConnectFile =
                externalState.Desired.Content
                |> ContentSet.tryGetFile fileRef
                |> function
                    | Some file -> GlobalConnectFile.fromContentFile remotePathBase file
                    | None -> GlobalConnectFile.Zero
            { GlobalConnectFileState.Zero with
                File = globalConnectFile
            }

        let getDefaultSubtitlesState externalState (executionContext: ExecutionContext) subRef =
            let globalConnectSubtitles =
                let remotePathBase =
                    GlobalConnectPathBase.fromGeoRestriction executionContext.MediaSetId externalState.Desired.GeoRestriction
                externalState.Desired.Content
                |> ContentSet.tryGetSubtitles subRef
                |> function
                    | Some sub -> GlobalConnectSubtitles.fromSubtitlesFile remotePathBase sub
                    | None -> GlobalConnectSubtitles.Zero
            { GlobalConnectSubtitlesState.Zero with
                Subtitles = globalConnectSubtitles
            }

        let forwardJobToFileActor (externalState: ExternalState) executionContext props partId fileSegment =
            let fileActor = findOrCreateFileActor partId fileSegment props mailbox
            let msg =
                {
                    ExecutionContext = executionContext
                    DesiredState = externalState.Desired
                    OriginState = OriginState.GlobalConnect externalState.Origin
                }
            fileActor <! Message.create msg

        let forwardFileJobToFileActor externalState (executionContext: ExecutionContext) fileRef createFileActorProps =
            let fileSegment = UploadActorPathSegment.forFileRef executionContext.MediaSetId fileRef
            let initialState: GlobalConnectFileState =
                externalState.Origin.Files
                |> Map.tryFind fileRef
                |> Option.defaultWith (fun () -> getDefaultFileState externalState executionContext fileRef)
            let props = createFileActorProps fileRef initialState
            forwardJobToFileActor externalState executionContext props fileRef.PartId fileSegment

        let forwardSubtitlesJobToFileActor externalState (executionContext: ExecutionContext) subRef createFileActorProps =
            let fileSegment = UploadActorPathSegment.forSubtitles subRef
            let initialState: GlobalConnectSubtitlesState =
                externalState.Origin.Subtitles
                |> Map.tryFind subRef
                |> Option.defaultWith (fun () -> getDefaultSubtitlesState externalState executionContext subRef)
            let props = createFileActorProps subRef initialState
            forwardJobToFileActor externalState executionContext props None fileSegment

        let forwardSmilJobToFileActor externalState (executionContext: ExecutionContext) (smil: GlobalConnectSmilState) createFileActorProps =
            let fileSegment = UploadActorPathSegment.forSmil ()
            let initialState = smil
            let props = createFileActorProps initialState
            forwardJobToFileActor externalState executionContext props None fileSegment

        let forwardSkipToChildActors externalState executionContext =
            let isCompletedOrRejected state =
                [ DistributionState.Completed; DistributionState.Rejected ] |> List.contains state
            // Files
            externalState.Origin.Files
            |> Map.toList
            |> List.filter (fun (_, resource) -> isCompletedOrRejected resource.RemoteState.State)
            |> List.map fst
            |> List.iter (fun fileRef ->
                if shouldForwardJobToFileActor executionContext (ResourceRef.File fileRef) then
                    forwardFileJobToFileActor externalState executionContext fileRef createFileActorProps)
            // Subtitles
            externalState.Origin.Subtitles
            |> Map.toList
            |> List.filter (fun (_, resource) -> isCompletedOrRejected resource.RemoteState.State)
            |> List.map fst
            |> List.iter (fun subRef ->
                if shouldForwardJobToFileActor executionContext (ResourceRef.Subtitles subRef) then
                    forwardSubtitlesJobToFileActor externalState executionContext subRef createSubtitlesActorProps)
            // Smil
            if
                externalState.Origin.Smil.RemoteState.State = DistributionState.Completed
                && shouldForwardJobToFileActor executionContext ResourceRef.Smil
            then
                forwardSmilJobToFileActor externalState executionContext externalState.Origin.Smil createSmilActorProps

        let extractStorageAndFileActions actions =
            let storageActions, fileActions = actions |> List.partition (fun (action: GlobalConnectCommand) -> action.IsCleanupStorage)
            storageActions |> List.tryHead, fileActions

        let storageEventProducer = GlobalConnectStorageEventProducer(aprops, eventPublisher)

        let logActorState stageName job =
            logActorState mailbox stageName (Some job)

        let rec initiating () = idle' true

        and idle () = idle' false

        and idle' initiating =
            if initiating then
                logDebug mailbox "Initial actor state"
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
                        validation externalState msg.Payload.ExecutionContext msg.Ack
                    | Reminders.LifetimeEvent e ->
                        logDebug mailbox $"Reminder lifetime event {e}"
                        ignored ()
                    | LifecycleEvent e -> lifecycle e
                    | _ -> unhandled ()
            }

        and validation externalState executionContext ack =
            logActorState "validation" executionContext.CommandName
            if executionContext |> shouldDiscard aprops.SkipProcessingForSources aprops.MinimumMessagePriority then
                discard executionContext ack
            else
                let globalConnectActions =
                    externalState.Desired
                    |> GlobalConnectActions.forMediaSet (getActionContext executionContext) executionContext.MediaSetId externalState.Origin
                validateActions externalState executionContext globalConnectActions ack

        and discard executionContext ack =
            logActorState "discard" executionContext.CommandName
            sendAck mailbox ack "Discarded job"
            idle ()

        and validateActions externalState executionContext globalConnectActions ack =
            logActorState "validateActions" executionContext.CommandName
            logInfo mailbox $"Evaluated GlobalConnect actions: {globalConnectActions}"
            let storageAction, fileActions = extractStorageAndFileActions globalConnectActions
            match storageAction, fileActions with
            | Some storageAction, _ -> executeStorageAction externalState executionContext storageAction fileActions ack
            | None, fileActions -> executeFileActions externalState executionContext fileActions ack

        and executeFileActions externalState executionContext fileActions ack =
            logActorState "executeFileActions" executionContext.CommandName
            match fileActions with
            | [] -> skip externalState executionContext ack
            | actions -> forwardFiles externalState executionContext actions ack

        and executeStorageAction externalState executionContext storageAction fileActions ack =
            logActorState "executeStorageAction" executionContext.CommandName
            match storageAction with
            | GlobalConnectCommand.CleanupStorage(CleanupStorageCommand inactiveFiles) ->
                let accessRestrictions = externalState.Desired.GeoRestriction
                let clientGroupId = getClientGroupIdFromContentId aprops.ClientRef aprops.ClientContentId
                let jobContext =
                    OriginJob.GlobalConnectCommand
                        {
                            Header = executionContext.Header
                            MediaSetId = executionContext.MediaSetId
                            Command = storageAction
                        }
                    |> FileJobContext.create accessRestrictions accessRestrictions clientGroupId
                let command =
                    S3Command.DeleteFiles
                        {
                            RemotePaths = inactiveFiles |> List.map RelativeUrl
                            JobContext = Some jobContext
                        }
                aprops.S3Queue
                <! PriorityQueueCommand.Enqueue
                    {
                        Message = Message.createWithAck command ack
                        Sender = retype mailbox.Self
                    }
                awaitingCompletion externalState executionContext fileActions ack
            | _ -> executeFileActions externalState executionContext fileActions ack

        and skip externalState executionContext ack =
            logActorState "skip" executionContext.CommandName
            match executionContext with
            | ExecutionContext.MediaSetJob _ -> storageEventProducer.PublishSkippedEvents executionContext
            | ExecutionContext.OriginStateUpdate _ -> ()
            forwardSkipToChildActors externalState executionContext
            sendAck mailbox ack "Skipped job"
            idle ()

        and awaitingCompletion externalState executionContext fileActions ack =
            logActorState "awaitingCompletion" executionContext.CommandName
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? Message<ApplyState> ->
                        mailbox.Stash()
                        ignored ()
                    | :? FileJobOriginEvent as originEvent when ackHasMatch originEvent.Ack mailbox ack ->
                        executeFileActions externalState executionContext fileActions ack
                    | Reminders.LifetimeEvent e ->
                        logDebug mailbox $"Reminder lifetime event {e}"
                        ignored ()
                    | LifecycleEvent e -> lifecycle e
                    | _ ->
                        logWarning mailbox $"Unexpected message of type {message.GetType()} while actor is awaiting completion"
                        unhandled ()
            }

        and forwardFiles externalState executionContext fileActions ack =
            logActorState "forwardFiles" executionContext.CommandName
            logDebug mailbox $"Job {executionContext.CommandName} is split into {fileActions.Length} file jobs"
            fileActions
            |> List.iter (fun action ->
                match action with
                | GlobalConnectFileCommand fileRef ->
                    if shouldForwardJobToFileActor executionContext (ResourceRef.File fileRef) then
                        forwardFileJobToFileActor externalState executionContext fileRef createFileActorProps
                | GlobalConnectSubtitlesCommand subRef ->
                    if shouldForwardJobToFileActor executionContext (ResourceRef.Subtitles subRef) then
                        forwardSubtitlesJobToFileActor externalState executionContext subRef createSubtitlesActorProps
                | GlobalConnectSmilCommand _ -> forwardSmilJobToFileActor externalState executionContext externalState.Origin.Smil createSmilActorProps
                | _ -> ())
            sendAck mailbox ack "Forwarded files for processing"
            idle ()

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

        initiating ()
