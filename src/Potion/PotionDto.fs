namespace Nrk.Oddjob.Potion

open System
open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Dto.Events
open PotionTypes

module PotionDto =

    type PotionCommandStatusEvent =
        {
            Timestamp: DateTimeOffset
            StorageProvider: Origin
            Status: JobStatus
            ErrorMessage: string
            InternalOperation: string
            RequestSource: string
            ForwardedFrom: string option
            CorrelationId: string
            MediaSetId: string
            MediaResource: MediaResourceDto option
            ClientId: string
            ClientCommand: string
            ClientGroupId: string
            ClientFileId: string option
            ClientCommandId: string option
            Priority: byte
        }

    type PotionCommandDto =
        | AddFile of AddFileCommand
        | AddSubtitle of AddSubtitleCommand
        | SetGeoBlock of SetGeoBlockCommand
        | DeleteGroup of DeleteGroupCommand
        | DeleteSubtitle of DeleteSubtitleCommand
        | HealthCheck of HealthCheckCommand

    let (|PotionCommandDto|_|) dto =
        match dto with
        | PotionCommandDto.AddFile cmd -> PotionCommand.AddFile cmd |> Some
        | PotionCommandDto.AddSubtitle cmd -> PotionCommand.AddSubtitle cmd |> Some
        | PotionCommandDto.SetGeoBlock cmd -> PotionCommand.SetGeoBlock cmd |> Some
        | PotionCommandDto.DeleteGroup cmd -> PotionCommand.DeleteGroup cmd |> Some
        | PotionCommandDto.DeleteSubtitle cmd -> PotionCommand.DeleteSubtitle cmd |> Some
        | _ -> None

    let (|PotionHealthCheckDto|_|) dto =
        match dto with
        | PotionCommandDto.HealthCheck cmd -> Some cmd
        | _ -> None

    [<RequireQualifiedAccess>]
    type LegacyStatusEvent = CommandStatus of PotionCommandStatusEvent

    module PotionCommandStatusEvent =
        let private fromOddjobEvent (event: CommandStatusEvent) =
            {
                Timestamp = event.Timestamp
                StorageProvider = event.StorageProvider
                Status = event.Status
                ErrorMessage = event.ErrorMessage
                InternalOperation = event.InternalOperation
                RequestSource = event.RequestSource
                ForwardedFrom = event.ForwardedFrom
                CorrelationId = null
                MediaSetId = event.MediaSetId
                MediaResource = event.MediaResource
                ClientId = event.ClientId
                ClientCommand = null
                ClientGroupId = event.ClientGroupId
                ClientFileId = None
                ClientCommandId = None
                Priority = 0uy
            }

        let private hasSameQualityId (cmd: PotionCommand) (mediaResource: MediaResourceDto option) =
            match cmd.GetQualityId(), mediaResource with
            | Some q1, Some res -> // If the original command has qualityId, resource qualityId should match it
                match res.QualityId with
                | Some q2 -> q1 = q2
                | None -> false
            | _ -> false

        let private hasNoQualityId (cmd: PotionCommand) (mediaResource: MediaResourceDto option) =
            match cmd, mediaResource with
            | PotionCommand.AddSubtitle _, Some(MediaResourceDto.GlobalConnectSubtitles _) -> true
            | _, None when cmd.GetQualityId().IsNone -> true
            | _, _ -> false

        let private tryGetClientFileId (state: PotionSetState) (cmd: PotionCommand) (status: PotionCommandStatusEvent) =
            if Option.isSome status.ClientFileId then
                status.ClientFileId
            else if Option.isSome <| cmd.GetFileId() then
                cmd.GetFileId()
            else
                maybe {
                    let! res = status.MediaResource
                    let! qualityId = res.QualityId
                    let! file = state.Files |> Map.tryFind qualityId
                    return file.FileId
                }

        let tryFindPendingCommand (pendingCommands: PendingCommand list) (event: CommandStatusEvent) =
            let pendingCommands = pendingCommands |> List.filter (fun cmd -> cmd.Origin = event.StorageProvider)
            pendingCommands
            |> List.tryFind (fun cmd -> hasSameQualityId cmd.Command event.MediaResource) // First try to find most recent file command
            |> Option.orElseWith (fun () -> pendingCommands |> List.tryFind (fun cmd -> hasNoQualityId cmd.Command event.MediaResource)) // Otherwise try to find most recent group or subtitles command
            |> Option.map (fun x -> x.Command, x.Priority)

        let tryFindCompletedCommand (clientCommands: PotionCommand list) (event: CommandStatusEvent) =
            let clientCommands = clientCommands |> List.rev
            clientCommands
            |> List.tryFind (fun cmd -> hasSameQualityId cmd event.MediaResource) // First try to find most recent file command
            |> Option.orElseWith (fun () -> clientCommands |> List.tryFind _.GetQualityId().IsNone) // Fallback is to use the most recent group level client command
            |> Option.map (fun x -> x, MessagePriority.Low)

        let tryCreate (state: PotionSetState) (event: Dto.Events.OddjobEventDto) : PotionCommandStatusEvent option =
            match event with
            | Dto.Events.OddjobEventDto.CommandStatus event' ->
                let cmd =
                    tryFindPendingCommand state.PendingCommands event'
                    |> Option.orElseWith (fun () -> tryFindCompletedCommand state.ClientCommands event')
                match cmd with
                | Some(cmd, priority) ->
                    let status = fromOddjobEvent event'
                    { status with
                        CorrelationId = cmd.GetCorrelationId()
                        ClientCommand = getUnionCaseName cmd
                        ClientFileId = tryGetClientFileId state cmd status
                        Priority = priority
                    }
                    |> Some
                | None -> None
            | _ -> None

        let validateDetails (event: PotionCommandStatusEvent) =
            if String.isNullOrEmpty event.ClientCommand then
                Result.Error "CommandStatus event has no ClientCommand"
            else if String.isNullOrEmpty event.CorrelationId then
                Result.Error "CommandStatus event has no CorrelationId"
            else if Option.isNone event.ClientFileId then
                match event.MediaResource with
                | Some(MediaResourceDto.GlobalConnectFile _) -> Result.Error $"CommandStatus event has no ClientFileId for {event.MediaResource}"
                | _ -> Result.Ok()
            else
                Result.Ok()

    type MediaSetChangedDetails =
        {
            Timestamp: DateTimeOffset
            Origin: string
            GeoBlock: string
            CorrelationId: string
            Version: int
            RemotePath: string
            ClientGroupId: string
        }

    type MediaFileUploadedDetails =
        {
            Timestamp: DateTimeOffset
            Origin: string
            CorrelationId: string
            ClientGroupId: string
            ClientFileId: string
        }

    type MediaFileFailedDetails =
        {
            Timestamp: DateTimeOffset
            Origin: string
            CorrelationId: string
            Reason: string
            ClientGroupId: string
            ClientFileId: string
        }

    type MediaSetEvent =
        | MediaSetChanged of MediaSetChangedDetails
        | MediaSetFileUploaded of MediaFileUploadedDetails
        | MediaSetFileFailed of MediaFileFailedDetails

    module MediaSetEvent =

        let private createMediaSetChangedDetails (event: PotionCommandStatusEvent) (smil: GlobalConnectSmilResource) =
            let geoRestriction = smil.RemotePath |> RelativeUrl |> GlobalConnectAccessRestrictions.parse
            {
                Timestamp = event.Timestamp
                Origin = getUnionCaseName Origin.GlobalConnect
                GeoBlock = getUnionCaseName geoRestriction
                CorrelationId = event.CorrelationId
                Version = smil.MediaSetVersion
                RemotePath = smil.RemotePath
                ClientGroupId = event.ClientGroupId
            }

        let private createMediaFileUploadedDetails (event: PotionCommandStatusEvent) clientFileId =
            {
                Timestamp = event.Timestamp
                Origin = getUnionCaseName Origin.GlobalConnect
                CorrelationId = event.CorrelationId
                ClientGroupId = event.ClientGroupId
                ClientFileId = clientFileId
            }

        let private createMediaFileFailedDetails (event: PotionCommandStatusEvent) clientFileId =
            {
                Timestamp = event.Timestamp
                Origin = getUnionCaseName Origin.GlobalConnect
                CorrelationId = event.CorrelationId
                Reason = event.ErrorMessage
                ClientGroupId = event.ClientGroupId
                ClientFileId = clientFileId
            }

        let tryCreateFromCommandStatus (event: PotionCommandStatusEvent) =
            match event.MediaResource with
            | Some(MediaResourceDto.GlobalConnectSmil smil) when event.Status = JobStatus.Completed || event.Status = JobStatus.Unchanged ->
                createMediaSetChangedDetails event smil |> MediaSetChanged |> Some
            | Some(MediaResourceDto.GlobalConnectFile _) when event.Status = JobStatus.Completed || event.Status = JobStatus.Unchanged ->
                match event.ClientFileId with
                | Some clientFileId -> createMediaFileUploadedDetails event clientFileId |> MediaSetFileUploaded |> Some
                | None -> None
            | Some(MediaResourceDto.GlobalConnectFile _) when event.Status = JobStatus.Rejected ->
                match event.ClientFileId with
                | Some clientFileId -> createMediaFileFailedDetails event clientFileId |> MediaSetFileFailed |> Some
                | None -> None
            | _ -> None
