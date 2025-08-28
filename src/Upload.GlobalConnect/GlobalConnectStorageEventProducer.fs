namespace Nrk.Oddjob.Upload.GlobalConnect

module GlobalConnectStorageEventProducer =

    open System

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Events
    open Nrk.Oddjob.Core.Dto.Events
    open Nrk.Oddjob.Upload
    open Nrk.Oddjob.Upload.UploadUtils

    type GlobalConnectStorageEventProducer(aprops: GlobalConnectUploadProps, eventPublisher) =

        let createSkippedCommandEvent (ctx: ExecutionContext) status errorMessage clientGroupId timestamp =
            if [ "ClearMediaSet"; "SetGeoRestriction" ] |> List.contains ctx.CommandName then
                ctx
                |> OddjobCommandEvent.fromExecutionContext Origin.GlobalConnect status errorMessage None timestamp
                |> OddjobEventDto.fromCommandEvent ctx.MediaSetId clientGroupId
                |> UploadEventPublisher.CommandStatusEvent
                |> Some
            else
                None

        member _.PublishSkippedEvents executionContext =
            let clientGroupId = getClientGroupIdFromContentId aprops.ClientRef aprops.ClientContentId
            let timestamp = DateTimeOffset.Now
            [
                createSkippedCommandEvent executionContext JobStatus.Unchanged None clientGroupId timestamp
            ]
            |> List.choose id
            |> UploadEventPublisher.publishEventsWithoutConfirmation eventPublisher
