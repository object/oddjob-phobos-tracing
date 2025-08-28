namespace Nrk.Oddjob.Core

[<AutoOpen>]
module MediaSetMessages =

    [<RequireQualifiedAccess>]
    type MediaSetCommand =
        | SetGeoRestriction of GeoRestriction
        | SetMediaType of MediaType
        | AssignPart of PartId * int
        | AssignFile of PartId option * ContentFile
        | AssignSubtitlesFile of SubtitlesFile
        | AssignSubtitlesFiles of SubtitlesFile list
        | AssignSubtitlesLinks of SubtitlesLink list
        | RemovePart of PartId
        | RevokePart of PartId
        | RestorePart of PartId
        | RemoveFile of PartId option * ContentFile
        | RemoveSubtitlesFile of languageCode: Alphanumeric * name: Alphanumeric
        | AssignDesiredState of DesiredMediaSetState
        | MergeDesiredState of DesiredMediaSetState
        | ClearDesiredState
        | ClearRemoteFile of Origin * FileRef
        | AssignGlobalConnectFile of FileRef * GlobalConnectFile
        | AssignGlobalConnectSubtitles of GlobalConnectSubtitles
        | AssignGlobalConnectSmil of GlobalConnectSmil
        | ClearCurrentState

    [<RequireQualifiedAccess>]
    type MediaSetEvent =
        | AssignedClientContentId of string option
        | SetSchemaVersion of int
        | SetMediaType of MediaType
        | SetGeoRestriction of GeoRestriction
        | AssignedPart of PartId * int
        | AssignedFile of PartId option * ContentFile
        | AssignedSubtitlesFile of SubtitlesFile
        | AssignedSubtitlesFiles of SubtitlesFile list
        | AssignedSubtitlesLinks of SubtitlesLink list
        | RemovedPart of PartId
        | RevokedPart of PartId
        | RestoredPart of PartId
        | RemovedFile of PartId option * ContentFile
        | RemovedSubtitlesFile of languageCode: Alphanumeric * name: Alphanumeric
        | AssignedDesiredState of DesiredMediaSetState
        | MergedDesiredState of DesiredMediaSetState
        | ClearedDesiredState
        | ReceivedRemoteFileState of Origin * FileRef * RemoteState * RemoteResult
        | ReceivedRemoteSubtitlesFileState of Origin * SubtitlesRef * RemoteState * RemoteResult
        | ReceivedRemoteSmilState of Origin * version: int * RemoteState * RemoteResult
        | ClearedRemoteFile of Origin * FileRef
        | AssignedGlobalConnectFile of FileRef * GlobalConnectFile
        | AssignedGlobalConnectSubtitles of GlobalConnectSubtitles
        | AssignedGlobalConnectSmil of GlobalConnectSmil
        | ClearedCurrentState
        | Deprecated

    module MediaSetEvent =
        let fromCommand cmd =
            match cmd with
            | MediaSetCommand.SetMediaType x -> MediaSetEvent.SetMediaType x
            | MediaSetCommand.SetGeoRestriction x -> MediaSetEvent.SetGeoRestriction x
            | MediaSetCommand.AssignPart(x, y) -> MediaSetEvent.AssignedPart(x, y)
            | MediaSetCommand.AssignFile(x, y) -> MediaSetEvent.AssignedFile(x, y)
            | MediaSetCommand.AssignSubtitlesFile x -> MediaSetEvent.AssignedSubtitlesFile x
            | MediaSetCommand.AssignSubtitlesFiles x -> MediaSetEvent.AssignedSubtitlesFiles x
            | MediaSetCommand.AssignSubtitlesLinks x -> MediaSetEvent.AssignedSubtitlesLinks x
            | MediaSetCommand.RemovePart x -> MediaSetEvent.RemovedPart x
            | MediaSetCommand.RevokePart x -> MediaSetEvent.RevokedPart x
            | MediaSetCommand.RestorePart x -> MediaSetEvent.RestoredPart x
            | MediaSetCommand.RemoveFile(x, y) -> MediaSetEvent.RemovedFile(x, y)
            | MediaSetCommand.RemoveSubtitlesFile(x, y) -> MediaSetEvent.RemovedSubtitlesFile(x, y)
            | MediaSetCommand.AssignDesiredState x -> MediaSetEvent.AssignedDesiredState x
            | MediaSetCommand.MergeDesiredState x -> MediaSetEvent.MergedDesiredState x
            | MediaSetCommand.ClearDesiredState -> MediaSetEvent.ClearedDesiredState
            | MediaSetCommand.ClearRemoteFile(x, y) -> MediaSetEvent.ClearedRemoteFile(x, y)
            | MediaSetCommand.AssignGlobalConnectFile(x, y) -> MediaSetEvent.AssignedGlobalConnectFile(x, y)
            | MediaSetCommand.AssignGlobalConnectSubtitles x -> MediaSetEvent.AssignedGlobalConnectSubtitles x
            | MediaSetCommand.AssignGlobalConnectSmil x -> MediaSetEvent.AssignedGlobalConnectSmil x
            | MediaSetCommand.ClearCurrentState -> MediaSetEvent.ClearedCurrentState

    let (|DesiredStateEvent|CurrentStateEvent|) event =
        match event with
        | MediaSetEvent.AssignedClientContentId _
        | MediaSetEvent.SetSchemaVersion _
        | MediaSetEvent.SetMediaType _
        | MediaSetEvent.SetGeoRestriction _
        | MediaSetEvent.AssignedPart _
        | MediaSetEvent.AssignedFile _
        | MediaSetEvent.AssignedSubtitlesFile _
        | MediaSetEvent.AssignedSubtitlesFiles _
        | MediaSetEvent.AssignedSubtitlesLinks _
        | MediaSetEvent.RemovedPart _
        | MediaSetEvent.RevokedPart _
        | MediaSetEvent.RestoredPart _
        | MediaSetEvent.RemovedFile _
        | MediaSetEvent.RemovedSubtitlesFile _
        | MediaSetEvent.AssignedDesiredState _
        | MediaSetEvent.MergedDesiredState _
        | MediaSetEvent.ClearedDesiredState -> DesiredStateEvent
        | MediaSetEvent.ReceivedRemoteFileState _
        | MediaSetEvent.ReceivedRemoteSubtitlesFileState _
        | MediaSetEvent.ReceivedRemoteSmilState _
        | MediaSetEvent.ClearedRemoteFile _
        | MediaSetEvent.AssignedGlobalConnectFile _
        | MediaSetEvent.AssignedGlobalConnectSubtitles _
        | MediaSetEvent.AssignedGlobalConnectSmil _
        | MediaSetEvent.ClearedCurrentState
        | MediaSetEvent.Deprecated -> CurrentStateEvent

    let (|OriginEvent|_|) origin event =
        match event with
        | MediaSetEvent.ReceivedRemoteFileState(o, _, _, _) when origin = o -> true
        | MediaSetEvent.ReceivedRemoteSubtitlesFileState(o, _, _, _) when origin = o -> true
        | MediaSetEvent.ReceivedRemoteSmilState(o, _, _, _) when origin = o -> true
        | MediaSetEvent.ClearedRemoteFile(o, _) when origin = o -> true
        | MediaSetEvent.AssignedGlobalConnectFile _
        | MediaSetEvent.AssignedGlobalConnectSubtitles _
        | MediaSetEvent.AssignedGlobalConnectSmil _ when origin = Origin.GlobalConnect -> true
        | _ -> false

    let (|OriginResourceStateUpdateEvent|_|) origin event =
        match event with
        | MediaSetEvent.ReceivedRemoteFileState(o, fileRef, state, _) when origin = o -> Some(ResourceRef.File fileRef, state.State)
        | MediaSetEvent.ReceivedRemoteSubtitlesFileState(o, subRef, state, _) when origin = o -> Some(ResourceRef.Subtitles subRef, state.State)
        | MediaSetEvent.ReceivedRemoteSmilState(o, _, state, _) when origin = o -> Some(ResourceRef.Smil, state.State)
        | MediaSetEvent.ClearedRemoteFile(o, fileRef) when origin = o -> Some(ResourceRef.File fileRef, DistributionState.Deleted)
        | _ -> None

    let (|DeprecatedEvent|_|) event =
        match event with
        | MediaSetEvent.Deprecated -> Some event
        | _ -> None

    [<RequireQualifiedAccess>]
    type MediaSetMessage = | GetState
