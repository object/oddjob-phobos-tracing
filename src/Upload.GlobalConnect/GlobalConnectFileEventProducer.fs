namespace Nrk.Oddjob.Upload.GlobalConnect

module GlobalConnectFileEventProducer =

    open System
    open Akkling

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Events
    open Nrk.Oddjob.Core.Dto.Events
    open Nrk.Oddjob.Upload
    open Nrk.Oddjob.Upload.UploadUtils

    type GlobalConnectFileEventProducer(aprops: GlobalConnectFileUploadProps, eventPublisher) =

        let forwardFileStateToController (state: GlobalConnectResourceState) remoteState remoteResult =
            match aprops.ResourceRef, state with
            | ResourceRef.File fileRef, _ ->
                (retype aprops.MediaSetController)
                <! MediaSetEvent.ReceivedRemoteFileState(Origin.GlobalConnect, fileRef, remoteState, remoteResult)
            | ResourceRef.Subtitles subRef, _ ->
                (retype aprops.MediaSetController)
                <! MediaSetEvent.ReceivedRemoteSubtitlesFileState(Origin.GlobalConnect, subRef, remoteState, remoteResult)
            | ResourceRef.Smil, GlobalConnectResourceState.Smil smil ->
                (retype aprops.MediaSetController)
                <! MediaSetEvent.ReceivedRemoteSmilState(Origin.GlobalConnect, smil.Smil.Version, remoteState, remoteResult)
            | _, _ -> invalidOp $"Invalid resource/state combination {aprops.ResourceRef}, {state}"
        let sendKeepAliveToController () =
            (retype aprops.MediaSetController) <! ActorLifecycleCommand.KeepAlive

        let tryCreateFileCompletionEvent job resourceState desiredAccessRestrictions timestamp =
            match job with
            | OriginJob.GlobalConnectCommand job ->
                match job.Command, resourceState with
                | GlobalConnectCommand.UploadFile cmd, GlobalConnectResourceState.File fileState ->
                    OddjobEvent.PublishedGlobalConnectFile <| PublishedGlobalConnectFileEvent.fromCommand cmd fileState.File timestamp
                    |> Some
                | GlobalConnectCommand.MoveFile cmd, GlobalConnectResourceState.File fileState ->
                    OddjobEvent.MovedGlobalConnectFile
                    <| MovedGlobalConnectFileEvent.fromCommand cmd fileState.File desiredAccessRestrictions timestamp
                    |> Some
                | GlobalConnectCommand.DeleteFile cmd, GlobalConnectResourceState.File _ ->
                    OddjobEvent.DeletedGlobalConnectFile <| DeletedGlobalConnectFileEvent.fromCommand cmd timestamp |> Some
                | GlobalConnectCommand.UploadSubtitles cmd, GlobalConnectResourceState.Subtitles subState ->
                    OddjobEvent.PublishedGlobalConnectSubtitles
                    <| PublishedGlobalConnectSubtitlesEvent.fromCommand cmd subState.Subtitles timestamp
                    |> Some
                | GlobalConnectCommand.MoveSubtitles cmd, GlobalConnectResourceState.Subtitles subState ->
                    OddjobEvent.MovedGlobalConnectSubtitles
                    <| MovedGlobalConnectSubtitlesEvent.fromCommand cmd subState.Subtitles desiredAccessRestrictions timestamp
                    |> Some
                | GlobalConnectCommand.DeleteSubtitles cmd, GlobalConnectResourceState.Subtitles _ ->
                    OddjobEvent.DeletedGlobalConnectSubtitles <| DeletedGlobalConnectSubtitlesEvent.fromCommand cmd timestamp |> Some
                | GlobalConnectCommand.UploadSmil _, GlobalConnectResourceState.Smil smilState ->
                    OddjobEvent.PublishedGlobalConnectSmil <| PublishedGlobalConnectSmilEvent.fromCommand smilState.Smil timestamp |> Some
                | GlobalConnectCommand.MoveSmil cmd, GlobalConnectResourceState.Smil smilState ->
                    OddjobEvent.MovedGlobalConnectSmil
                    <| MovedGlobalConnectSmilEvent.fromCommand cmd smilState.Smil desiredAccessRestrictions timestamp
                    |> Some
                | GlobalConnectCommand.DeleteSmil cmd, GlobalConnectResourceState.Smil _ ->
                    OddjobEvent.DeletedGlobalConnectSmil <| DeletedGlobalConnectSmilEvent.fromCommand cmd timestamp |> Some
                | _, _ -> None

        let tryCreateFileRejectionEvent job reason timestamp =
            match job with
            | OriginJob.GlobalConnectCommand job ->
                match job.Command with
                | GlobalConnectCommand.UploadFile(UploadFileCommand fileRef) ->
                    OddjobEvent.RejectedGlobalConnectFile
                        {
                            FileRef = fileRef
                            Reason = reason
                            Timestamp = timestamp
                        }
                    |> Some
                | GlobalConnectCommand.MoveFile(MoveFileCommand fileRef) ->
                    OddjobEvent.RejectedGlobalConnectFile
                        {
                            FileRef = fileRef
                            Reason = reason
                            Timestamp = timestamp
                        }
                    |> Some
                | GlobalConnectCommand.DeleteFile(DeleteFileCommand fileRef) ->
                    OddjobEvent.RejectedGlobalConnectFile
                        {
                            FileRef = fileRef
                            Reason = reason
                            Timestamp = timestamp
                        }
                    |> Some
                | GlobalConnectCommand.UploadSubtitles(UploadSubtitlesCommand subRef) ->
                    OddjobEvent.RejectedGlobalConnectSubtitles
                        {
                            SubtitlesRef = subRef
                            Reason = reason
                            Timestamp = timestamp
                        }
                    |> Some
                | GlobalConnectCommand.MoveSubtitles(MoveSubtitlesCommand subRef) ->
                    OddjobEvent.RejectedGlobalConnectSubtitles
                        {
                            SubtitlesRef = subRef
                            Reason = reason
                            Timestamp = timestamp
                        }
                    |> Some
                | GlobalConnectCommand.DeleteSubtitles(DeleteSubtitlesCommand subRef) ->
                    OddjobEvent.RejectedGlobalConnectSubtitles
                        {
                            SubtitlesRef = subRef
                            Reason = reason
                            Timestamp = timestamp
                        }
                    |> Some
                | _ -> None

        let getOddjobResource resourceRef resourceState =
            match resourceRef, resourceState with
            | ResourceRef.File fileRef, GlobalConnectResourceState.File file -> MediaResource.GlobalConnectFile(fileRef, file.File)
            | ResourceRef.Subtitles subRef, GlobalConnectResourceState.Subtitles sub -> MediaResource.GlobalConnectSubtitles(subRef, sub.Subtitles)
            | ResourceRef.Smil, GlobalConnectResourceState.Smil smil -> MediaResource.GlobalConnectSmil smil.Smil
            | _ -> invalidOp $"Unsupported combination {resourceRef} / {resourceState}"

        let createResourceStateEvent (job: OriginJob) fileEvent =
            fileEvent |> OddjobEventDto.fromOriginEvent job.MediaSetId |> UploadEventPublisher.ResourceStateEvent

        let createCommandProgressEvent (job: OriginJob) status errorMessage resourceRef (resourceState: GlobalConnectResourceState) clientGroupId timestamp =
            job
            |> OddjobCommandEvent.fromOriginJob Origin.GlobalConnect status errorMessage (Some <| getOddjobResource resourceRef resourceState) timestamp
            |> OddjobEventDto.fromCommandEvent job.MediaSetId clientGroupId
            |> UploadEventPublisher.CommandStatusEvent

        let createSkippedCommandEvent ctx status errorMessage resourceRef (resourceState: GlobalConnectResourceState) clientGroupId timestamp =
            ctx
            |> OddjobCommandEvent.fromExecutionContext Origin.GlobalConnect status errorMessage (Some <| getOddjobResource resourceRef resourceState) timestamp
            |> OddjobEventDto.fromCommandEvent ctx.MediaSetId clientGroupId
            |> UploadEventPublisher.CommandStatusEvent

        let createCommandOriginEvent (originEvent: FileJobOriginEvent) resourceRef (resourceState: GlobalConnectResourceState) clientGroupId =
            originEvent.JobContext.Job
            |> OddjobCommandEvent.fromOriginEvent Origin.GlobalConnect (Some <| getOddjobResource resourceRef resourceState) originEvent
            |> OddjobEventDto.fromCommandEvent originEvent.JobContext.Job.MediaSetId clientGroupId
            |> UploadEventPublisher.CommandStatusEvent

        member _.PublishInitialEvents (jobContext: FileJobContext) resourceRef resourceState =
            sendKeepAliveToController ()
            let timestamp = DateTimeOffset.Now
            [
                createCommandProgressEvent jobContext.Job JobStatus.Assigned None resourceRef resourceState jobContext.ClientGroupId timestamp
            ]
            |> UploadEventPublisher.publishEventsWithoutConfirmation eventPublisher

        member _.PublishSkippedEvents executionContext resourceRef (resource: GlobalConnectResourceState) clientGroupId =
            let jobStatus, errorMessage =
                if resource.RemoteState.State = DistributionState.Rejected then
                    JobStatus.Rejected, Some "Previously rejected file is not repaired"
                else
                    JobStatus.Unchanged, None
            let timestamp = DateTimeOffset.Now
            [
                createSkippedCommandEvent executionContext jobStatus errorMessage resourceRef resource clientGroupId timestamp
            ]
            |> UploadEventPublisher.publishEventsWithoutConfirmation eventPublisher

        member _.HandleOriginEvents (originEvent: FileJobOriginEvent) (state: GlobalConnectResourceState) =
            if originEvent.JobStatus = JobStatus.Queued then
                sendKeepAliveToController ()
                state.RemoteState
            else
                let newState = getDistributionStateFromJobStatus originEvent.JobContext.Job originEvent.JobStatus state.RemoteState.State

                match originEvent.JobStatus with
                | JobStatus.Unchanged -> ()
                | _ ->
                    let remoteState =
                        {
                            State = newState
                            Timestamp = originEvent.Timestamp
                        }
                    let remoteResult =
                        match originEvent.Result with
                        | Result.Ok _ -> RemoteResult.create 0 null
                        | Result.Error(errorCode, errorMessage) -> RemoteResult.create errorCode errorMessage
                    forwardFileStateToController state remoteState remoteResult

                {
                    State = newState
                    Timestamp = originEvent.Timestamp
                }

        member _.CreateProgressEvents (originEvent: FileJobOriginEvent) resourceRef resourceState =
            let job = originEvent.JobContext.Job
            let commandStatusEvent = createCommandOriginEvent originEvent resourceRef resourceState originEvent.JobContext.ClientGroupId
            match originEvent.JobStatus, originEvent.Result with
            | JobStatus.Completed, _ -> tryCreateFileCompletionEvent job resourceState originEvent.JobContext.DesiredGeoRestriction originEvent.Timestamp
            | JobStatus.Rejected, Result.Ok _ -> tryCreateFileRejectionEvent job "Unspecified error" originEvent.Timestamp
            | JobStatus.Rejected, Result.Error(_, reason) -> tryCreateFileRejectionEvent job reason originEvent.Timestamp
            | _ -> None
            |> Option.map (createResourceStateEvent job)
            |> function
                | Some resourceCompletionEvent -> [ commandStatusEvent; resourceCompletionEvent ]
                | None -> [ commandStatusEvent ]

        member _.PublishProgressEvents events =
            events |> List.iter (fun e -> eventPublisher <! (UploadEventPublisher.PublishEvent e))
