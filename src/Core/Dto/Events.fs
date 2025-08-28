namespace Nrk.Oddjob.Core.Dto.Events

open System

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Events

type PublishedGlobalConnectFileEvent =
    {
        MediaSetId: string
        PartId: string
        QualityId: int
        LocalPath: string
        RemotePath: string
        Timestamp: DateTimeOffset
    }

type MovedGlobalConnectFileEvent =
    {
        MediaSetId: string
        PartId: string
        QualityId: int
        SourcePath: string
        DestinationPath: string
        Timestamp: DateTimeOffset
    }

type DeletedGlobalConnectFileEvent =
    {
        MediaSetId: string
        PartId: string
        QualityId: int
        Timestamp: DateTimeOffset
    }

type RejectedGlobalConnectFileEvent =
    {
        MediaSetId: string
        PartId: string
        QualityId: int
        Reason: string
        Timestamp: DateTimeOffset
    }

type PublishedGlobalConnectSubtitlesEvent =
    {
        MediaSetId: string
        LanguageCode: string
        Name: string
        LocalPath: string
        RemotePath: string
        Timestamp: DateTimeOffset
    }

type MovedGlobalConnectSubtitlesEvent =
    {
        MediaSetId: string
        LanguageCode: string
        Name: string
        SourcePath: string
        DestinationPath: string
        Timestamp: DateTimeOffset
    }

type DeletedGlobalConnectSubtitlesEvent =
    {
        MediaSetId: string
        LanguageCode: string
        Name: string
        Timestamp: DateTimeOffset
    }

type RejectedGlobalConnectSubtitlesEvent =
    {
        MediaSetId: string
        LanguageCode: string
        Name: string
        Reason: string
        Timestamp: DateTimeOffset
    }

type PublishedGlobalConnectSmilEvent =
    {
        MediaSetId: string
        Version: int
        RemotePath: string
        Timestamp: DateTimeOffset
    }

type MovedGlobalConnectSmilEvent =
    {
        MediaSetId: string
        OldVersion: int
        NewVersion: int
        RemotePath: string
        Timestamp: DateTimeOffset
    }

type DeletedGlobalConnectSmilEvent =
    {
        MediaSetId: string
        Version: int
        Timestamp: DateTimeOffset
    }

type GlobalConnectFileResource =
    {
        MediaSetId: string
        PartId: string
        QualityId: int
        RemotePath: string
    }

type GlobalConnectSubtitlesResource =
    {
        MediaSetId: string
        LanguageCode: string
        Name: string
        RemotePath: string
    }

type GlobalConnectSmilResource =
    {
        MediaSetId: string
        MediaSetVersion: int
        RemotePath: string
    }

[<RequireQualifiedAccess>]
type MediaResourceDto =
    | GlobalConnectFile of GlobalConnectFileResource
    | GlobalConnectSubtitles of GlobalConnectSubtitlesResource
    | GlobalConnectSmil of GlobalConnectSmilResource

    member this.RemotePath =
        match this with
        | GlobalConnectFile e -> e.RemotePath
        | GlobalConnectSubtitles e -> e.RemotePath
        | GlobalConnectSmil e -> e.RemotePath
    member this.QualityId =
        match this with
        | GlobalConnectFile e -> Some e.QualityId
        | GlobalConnectSubtitles _ -> None
        | GlobalConnectSmil _ -> None

type CommandStatusEvent =
    {
        Timestamp: DateTimeOffset
        StorageProvider: Origin
        Status: JobStatus
        ErrorMessage: string
        InternalOperation: string
        RequestSource: string
        ForwardedFrom: string option
        MediaSetId: string
        MediaResource: MediaResourceDto option
        ClientId: string
        ClientGroupId: string
    }

[<RequireQualifiedAccess>]
type OddjobEventDto =
    | PublishedGlobalConnectFile of PublishedGlobalConnectFileEvent
    | RejectedGlobalConnectFile of RejectedGlobalConnectFileEvent
    | MovedGlobalConnectFile of MovedGlobalConnectFileEvent
    | DeletedGlobalConnectFile of DeletedGlobalConnectFileEvent
    | PublishedGlobalConnectSubtitles of PublishedGlobalConnectSubtitlesEvent
    | MovedGlobalConnectSubtitles of MovedGlobalConnectSubtitlesEvent
    | DeletedGlobalConnectSubtitles of DeletedGlobalConnectSubtitlesEvent
    | RejectedGlobalConnectSubtitles of RejectedGlobalConnectSubtitlesEvent
    | PublishedGlobalConnectSmil of PublishedGlobalConnectSmilEvent
    | MovedGlobalConnectSmil of MovedGlobalConnectSmilEvent
    | DeletedGlobalConnectSmil of DeletedGlobalConnectSmilEvent
    | CommandStatus of CommandStatusEvent

    member this.EventName = getUnionCaseName this
    member this.MediaSetId =
        match this with
        | PublishedGlobalConnectFile e -> e.MediaSetId
        | MovedGlobalConnectFile e -> e.MediaSetId
        | DeletedGlobalConnectFile e -> e.MediaSetId
        | RejectedGlobalConnectFile e -> e.MediaSetId
        | PublishedGlobalConnectSubtitles e -> e.MediaSetId
        | MovedGlobalConnectSubtitles e -> e.MediaSetId
        | DeletedGlobalConnectSubtitles e -> e.MediaSetId
        | RejectedGlobalConnectSubtitles e -> e.MediaSetId
        | PublishedGlobalConnectSmil e -> e.MediaSetId
        | MovedGlobalConnectSmil e -> e.MediaSetId
        | DeletedGlobalConnectSmil e -> e.MediaSetId
        | CommandStatus e -> e.MediaSetId
    member this.ClientId = String.split '~' this.MediaSetId |> Seq.head
    member this.Timestamp =
        match this with
        | PublishedGlobalConnectFile e -> e.Timestamp
        | MovedGlobalConnectFile e -> e.Timestamp
        | DeletedGlobalConnectFile e -> e.Timestamp
        | RejectedGlobalConnectFile e -> e.Timestamp
        | PublishedGlobalConnectSubtitles e -> e.Timestamp
        | MovedGlobalConnectSubtitles e -> e.Timestamp
        | DeletedGlobalConnectSubtitles e -> e.Timestamp
        | RejectedGlobalConnectSubtitles e -> e.Timestamp
        | PublishedGlobalConnectSmil e -> e.Timestamp
        | MovedGlobalConnectSmil e -> e.Timestamp
        | DeletedGlobalConnectSmil e -> e.Timestamp
        | CommandStatus e -> e.Timestamp
    member this.GetSummary() =
        $"%s{this.EventName} %s{this.MediaSetId}"

module OddjobResourceDto =
    let toDto (mediaSetId: MediaSetTypes.MediaSetId) res =
        match res with
        | MediaResource.GlobalConnectFile(fileRef, file) ->
            MediaResourceDto.GlobalConnectFile
                {
                    MediaSetId = mediaSetId.Value
                    PartId = fileRef.PartId |> Option.map _.Value |> Option.defaultValue null
                    QualityId = fileRef.QualityId.Value
                    RemotePath = file.RemotePath.Value
                }
        | MediaResource.GlobalConnectSubtitles(subRef, sub) ->
            MediaResourceDto.GlobalConnectSubtitles
                {
                    MediaSetId = mediaSetId.Value
                    LanguageCode = subRef.LanguageCode.Value
                    Name = subRef.Name.Value
                    RemotePath = sub.RemotePath.Value
                }
        | MediaResource.GlobalConnectSmil smil ->
            MediaResourceDto.GlobalConnectSmil
                {
                    MediaSetId = mediaSetId.Value
                    MediaSetVersion = smil.Version
                    RemotePath = smil.RemotePath.Value
                }

module OddjobEventDto =
    let fromOriginEvent (mediaSetId: MediaSetTypes.MediaSetId) e =
        match e with
        | OddjobEvent.PublishedGlobalConnectFile e ->
            OddjobEventDto.PublishedGlobalConnectFile
                {
                    MediaSetId = mediaSetId.Value
                    PartId = e.FileRef.PartId |> Option.map _.Value |> Option.defaultValue null
                    QualityId = e.FileRef.QualityId.Value
                    LocalPath = e.LocalPath |> Option.map FilePath.value |> Option.defaultValue String.Empty
                    RemotePath = e.RemotePath.Value
                    Timestamp = e.Timestamp
                }
        | OddjobEvent.MovedGlobalConnectFile e ->
            OddjobEventDto.MovedGlobalConnectFile
                {
                    MediaSetId = mediaSetId.Value
                    PartId = e.FileRef.PartId |> Option.map _.Value |> Option.defaultValue null
                    QualityId = e.FileRef.QualityId.Value
                    SourcePath = e.SourcePath.Value
                    DestinationPath = e.DestinationPath.Value
                    Timestamp = e.Timestamp
                }
        | OddjobEvent.DeletedGlobalConnectFile e ->
            OddjobEventDto.DeletedGlobalConnectFile
                {
                    MediaSetId = mediaSetId.Value
                    PartId = e.FileRef.PartId |> Option.map _.Value |> Option.defaultValue null
                    QualityId = e.FileRef.QualityId.Value
                    Timestamp = e.Timestamp
                }
        | OddjobEvent.RejectedGlobalConnectFile e ->
            OddjobEventDto.RejectedGlobalConnectFile
                {
                    MediaSetId = mediaSetId.Value
                    PartId = e.FileRef.PartId |> Option.map _.Value |> Option.defaultValue null
                    QualityId = e.FileRef.QualityId.Value
                    Reason = e.Reason
                    Timestamp = e.Timestamp
                }
        | OddjobEvent.PublishedGlobalConnectSubtitles e ->
            OddjobEventDto.PublishedGlobalConnectSubtitles
                {
                    MediaSetId = mediaSetId.Value
                    LanguageCode = e.SubtitlesRef.LanguageCode.Value
                    Name = e.SubtitlesRef.Name.Value
                    LocalPath = e.LocalPath.Value
                    RemotePath = e.RemotePath.Value
                    Timestamp = e.Timestamp
                }
        | OddjobEvent.MovedGlobalConnectSubtitles e ->
            OddjobEventDto.MovedGlobalConnectSubtitles
                {
                    MediaSetId = mediaSetId.Value
                    LanguageCode = e.SubtitlesRef.LanguageCode.Value
                    Name = e.SubtitlesRef.Name.Value
                    SourcePath = e.SourcePath.Value
                    DestinationPath = e.DestinationPath.Value
                    Timestamp = e.Timestamp
                }
        | OddjobEvent.DeletedGlobalConnectSubtitles e ->
            OddjobEventDto.DeletedGlobalConnectSubtitles
                {
                    MediaSetId = mediaSetId.Value
                    LanguageCode = e.SubtitlesRef.LanguageCode.Value
                    Name = e.SubtitlesRef.Name.Value
                    Timestamp = e.Timestamp
                }
        | OddjobEvent.RejectedGlobalConnectSubtitles e ->
            OddjobEventDto.RejectedGlobalConnectSubtitles
                {
                    MediaSetId = mediaSetId.Value
                    LanguageCode = e.SubtitlesRef.LanguageCode.Value
                    Name = e.SubtitlesRef.Name.Value
                    Reason = e.Reason
                    Timestamp = e.Timestamp
                }
        | OddjobEvent.PublishedGlobalConnectSmil e ->
            OddjobEventDto.PublishedGlobalConnectSmil
                {
                    MediaSetId = mediaSetId.Value
                    Version = e.Version
                    RemotePath = e.RemotePath.Value
                    Timestamp = e.Timestamp
                }
        | OddjobEvent.MovedGlobalConnectSmil e ->
            OddjobEventDto.MovedGlobalConnectSmil
                {
                    MediaSetId = mediaSetId.Value
                    OldVersion = e.OldVersion
                    NewVersion = e.NewVersion
                    RemotePath = e.RemotePath.Value
                    Timestamp = e.Timestamp
                }
        | OddjobEvent.DeletedGlobalConnectSmil e ->
            OddjobEventDto.DeletedGlobalConnectSmil
                {
                    MediaSetId = mediaSetId.Value
                    Version = e.Version
                    Timestamp = e.Timestamp
                }

    let fromCommandEvent (mediaSetId: MediaSetTypes.MediaSetId) clientGroupId (event: OddjobCommandEvent) =
        OddjobEventDto.CommandStatus
            {
                Timestamp = event.Timestamp
                StorageProvider = event.Origin
                Status = event.Status
                ErrorMessage = event.ErrorMessage |> Option.defaultValue null
                InternalOperation = event.CommandName
                RequestSource = event.RequestSource
                ForwardedFrom = event.ForwardedFrom
                MediaSetId = mediaSetId.Value
                MediaResource = event.Resource |> Option.map (fun e -> e |> OddjobResourceDto.toDto mediaSetId)
                ClientId = event.ClientId
                ClientGroupId = clientGroupId |> Option.defaultValue null
            }

module PlayabilityEventPriorityDto =
    let fromDomain priority =
        match priority with
        | PlayabilityEventPriority.Normal -> 0
        | PlayabilityEventPriority.High -> 1

    let toDomain priority =
        match priority with
        | 1 -> PlayabilityEventPriority.High
        | _ -> PlayabilityEventPriority.Normal

type NonPlayableMediaSetDto =
    {
        MediaSetId: string
        Timestamp: DateTimeOffset
        Priority: int
    }

module NonPlayableMediaSetDto =
    let fromDomain (e: NonPlayableMediaSet) =
        {
            MediaSetId = e.MediaSetId.Value
            Timestamp = e.Timestamp
            Priority = PlayabilityEventPriorityDto.fromDomain e.Priority
        }

    let toDomain (e: NonPlayableMediaSetDto) : NonPlayableMediaSet =
        {
            MediaSetId = MediaSetId.parse e.MediaSetId
            Timestamp = e.Timestamp
            Priority = PlayabilityEventPriorityDto.toDomain e.Priority
        }

type PlayableOnDemandMediaSetDto =
    {
        MediaSetId: string
        Timestamp: DateTimeOffset
        Priority: int
    }

module PlayableOnDemandMediaSetDto =
    let fromDomain (e: PlayableOnDemandMediaSet) =
        {
            MediaSetId = e.MediaSetId.Value
            Timestamp = e.Timestamp
            Priority = PlayabilityEventPriorityDto.fromDomain e.Priority
        }

    let toDomain (e: PlayableOnDemandMediaSetDto) : PlayableOnDemandMediaSet =
        {
            MediaSetId = MediaSetId.parse e.MediaSetId
            Timestamp = e.Timestamp
            Priority = PlayabilityEventPriorityDto.toDomain e.Priority
        }

type MediaSetPlayabilityEventDto =
    | MediaSetPlayableOnDemand of PlayableOnDemandMediaSetDto
    | MediaSetNotPlayableOnDemand of NonPlayableMediaSetDto

    member this.MediaSetId =
        match this with
        | MediaSetPlayableOnDemand msg -> msg.MediaSetId
        | MediaSetNotPlayableOnDemand msg -> msg.MediaSetId
    member this.Priority =
        match this with
        | MediaSetPlayableOnDemand msg -> msg.Priority
        | MediaSetNotPlayableOnDemand msg -> msg.Priority

module MediaSetPlayabilityEventDto =
    let fromDomain (event: MediaSetPlayabilityEvent) =
        match event with
        | MediaSetPlayabilityEvent.MediaSetPlayableOnDemand e -> e |> PlayableOnDemandMediaSetDto.fromDomain |> MediaSetPlayableOnDemand
        | MediaSetPlayabilityEvent.MediaSetNotPlayableOnDemand e -> e |> NonPlayableMediaSetDto.fromDomain |> MediaSetNotPlayableOnDemand

    let toDomain (event: MediaSetPlayabilityEventDto) : MediaSetPlayabilityEvent =
        match event with
        | MediaSetPlayableOnDemand e -> e |> PlayableOnDemandMediaSetDto.toDomain |> MediaSetPlayabilityEvent.MediaSetPlayableOnDemand
        | MediaSetNotPlayableOnDemand e -> e |> NonPlayableMediaSetDto.toDomain |> MediaSetPlayabilityEvent.MediaSetNotPlayableOnDemand

type MediaSetPlayabilityBump = { ProgramId: string }
