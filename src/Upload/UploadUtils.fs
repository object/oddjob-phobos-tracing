namespace Nrk.Oddjob.Upload

module UploadUtils =

    open Akka.Actor
    open Akkling

    open Nrk.Oddjob.Core

    open UploadTypes

    [<Literal>]
    let EventPublisherActorName = "Event Publisher"

    let getMessageTitle message =
        match message: obj with
        | :? OriginJob as job -> $"%s{job.CommandName}"
        | :? Message<OriginJob> as msg -> $"%s{msg.Payload.CommandName}"
        | _ -> message.GetType().Name

    let getMessageSummary<'ResourceState> message =
        let getJobSummary job =
            match job with
            | FileJob fileRef -> $"%s{job.CommandName} %s{job.MediaSetId.Value} {fileRef |> FileRef.toFileName job.MediaSetId}"
            | StorageJob -> $"%s{job.CommandName} %s{job.MediaSetId.Value}"
            | _ -> $"%s{job.CommandName} %s{job.MediaSetId.Value}"
        let getMediaSetJobContextSummary (ctx: MediaSetJobContext) =
            $"%s{ctx.Job.CommandName} %s{ctx.MediaSetId.Value}"
        let getOriginStateUpdateContextSummary (ctx: OriginStateUpdateContext) = ctx.MediaSetId.Value
        match message: obj with
        | :? OriginJob as job -> getJobSummary job
        | :? MediaSetJobContext as ctx -> getMediaSetJobContextSummary ctx
        | :? Message<OriginJob> as msg -> getJobSummary msg.Payload
        | :? Message<ApplyState> as msg ->
            match msg.Payload.ExecutionContext with
            | ExecutionContext.MediaSetJob ctx -> getMediaSetJobContextSummary ctx
            | ExecutionContext.OriginStateUpdate ctx -> getOriginStateUpdateContextSummary ctx
        | _ -> $"{message}"

    let logActorState mailbox actorState (command: string option) =
        let message =
            match command with
            | Some command -> $"Actor state is [%s{actorState}] (%s{command})"
            | None -> $"Actor state is [%s{actorState}]"
        logDebug mailbox message

    let ackHasMatch eventAck mailbox ack =
        match (eventAck, ack) with
        | Some ack1, Some ack2 when ack1 <> ack2 ->
            logWarning mailbox $"Skipping own event with unexpected ack info (%A{eventAck})"
            false
        | _ -> true

    let getDistributionStateFromJobStatus (job: OriginJob) jobStatus currentState =
        match jobStatus with
        | JobStatus.Unchanged
        | JobStatus.Assigned -> DistributionState.None
        | JobStatus.Queued -> currentState
        | JobStatus.Initiated -> DistributionState.Initiated
        | JobStatus.Cancelled -> DistributionState.Cancelled
        | JobStatus.Failed -> DistributionState.Failed
        | JobStatus.Rejected -> DistributionState.Rejected
        | JobStatus.Completed ->
            match job with
            | DeleteJob -> DistributionState.Deleted
            | _ -> DistributionState.Completed

    let shouldDiscard skipSources minMessagePriority (executionContext: ExecutionContext) =
        if executionContext.Header.Priority < minMessagePriority then
            true
        else
            skipSources
            |> Seq.intersect (
                [
                    Some executionContext.Header.RequestSource
                    executionContext.Header.ForwardedFrom
                ]
                |> List.choose id
            )
            |> Seq.isNotEmpty

    let private getFileActorName (partId: PartId option) (fileSegment: UploadActorPathSegment) =
        match partId with
        | Some partId -> makeActorName [ partId.Value; UploadActorPathSegment.value fileSegment ]
        | None -> makeActorName [ UploadActorPathSegment.value fileSegment ]

    let findOrCreateFileActor (partId: PartId option) (fileSegment: UploadActorPathSegment) props (mailbox: Actor<_>) =
        let actorName = getFileActorName partId fileSegment
        getOrSpawnChildActor mailbox.UntypedContext actorName props

    let stopFileActor (partId: PartId option) (fileSegment: UploadActorPathSegment) (mailbox: Actor<_>) =
        let actorName = getFileActorName partId fileSegment
        let actor = mailbox.UntypedContext.Child(actorName)
        if not (actor.IsNobody()) then
            logDebug mailbox $"Stopping file actor %s{actorName}"
            actor.Tell PoisonPill.Instance

    let shouldForwardJobToFileActor (executionContext: ExecutionContext) resourceRef =
        match executionContext.ResourceRef with
        | Some ref -> ref = resourceRef
        | None -> true

    let getClientGroupIdFromContentId clientRef clientContentId =
        let (ClientContentId clientContentId) = clientContentId
        clientContentId
        |> Option.bind (fun clientContentId ->
            match clientRef with
            | ClientRef.Potion p -> p.ExternalGroupIdResolver clientContentId |> Some)
