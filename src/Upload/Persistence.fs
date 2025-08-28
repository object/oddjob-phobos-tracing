namespace Nrk.Oddjob.Upload

[<RequireQualifiedAccess>]
module Persistence =

    open Nrk.Oddjob.Core

    type EventAdapter(system: Akka.Actor.ExtendedActorSystem) =

        let ser = Dto.Serialization.ProtobufSerializer(system)

        interface Akka.Persistence.Journal.IEventAdapter with

            member _.Manifest(evt: obj) = ser.Manifest(evt)

            member _.ToJournal(evt: obj) : obj =
                Akka.Persistence.Journal.Tagged(box evt, [| "any" |]) :> obj

            member _.FromJournal(evt: obj, _: string) : Akka.Persistence.Journal.IEventSequence =
                if evt :? Dto.IProtoBufSerializable then
                    Akka.Persistence.Journal.EventSequence.Single(evt :?> Dto.IProtoBufSerializable)
                else
                    Akka.Persistence.Journal.EventSequence.Empty

[<RequireQualifiedAccess>]
module PersistentEvent =

    open Nrk.Oddjob.Core

    let fromDomain msg =
        match msg with
        | MediaSetEvent.AssignedClientContentId contentId -> Dto.MediaSet.AssignClientContentId.FromDomain contentId |> box
        | MediaSetEvent.SetSchemaVersion version -> Dto.MediaSet.SetSchemaVersion.FromDomain version |> box
        | MediaSetEvent.SetMediaType mt -> Dto.MediaSet.SetMediaType.FromDomain mt |> box
        | MediaSetEvent.SetGeoRestriction gr -> Dto.MediaSet.SetGeoRestriction.FromDomain gr |> box
        | MediaSetEvent.AssignedPart(partId, partNumber) -> Dto.MediaSet.AssignPart.FromDomain(partId, partNumber) |> box
        | MediaSetEvent.AssignedFile(partId, file) -> Dto.MediaSet.AssignFile.FromDomain(partId, file) |> box
        | MediaSetEvent.AssignedSubtitlesFile subtitles -> Dto.MediaSet.AssignSubtitlesFile.FromDomain subtitles |> box
        | MediaSetEvent.AssignedSubtitlesFiles subtitles -> Dto.MediaSet.AssignSubtitlesFiles.FromDomain subtitles |> box
        | MediaSetEvent.AssignedSubtitlesLinks links -> Dto.MediaSet.AssignSubtitlesLinks.FromDomain links |> box
        | MediaSetEvent.RemovedPart(partId) -> Dto.MediaSet.RemovePart.FromDomain(partId) |> box
        | MediaSetEvent.RevokedPart(partId) -> Dto.MediaSet.RevokePart.FromDomain(partId) |> box
        | MediaSetEvent.RestoredPart(partId) -> Dto.MediaSet.RestorePart.FromDomain(partId) |> box
        | MediaSetEvent.RemovedFile(partId, file) -> Dto.MediaSet.RemoveFile.FromDomain(partId, file) |> box
        | MediaSetEvent.RemovedSubtitlesFile(lang, kind) -> Dto.MediaSet.RemoveSubtitlesFile.FromDomain(lang, kind) |> box
        | MediaSetEvent.AssignedDesiredState desiredState -> Dto.MediaSet.AssignDesiredState.FromDomain desiredState |> box
        | MediaSetEvent.MergedDesiredState desiredState -> Dto.MediaSet.MergeDesiredState.FromDomain desiredState |> box
        | MediaSetEvent.ClearedDesiredState -> Dto.MediaSet.ClearDesiredState.FromDomain() |> box
        | MediaSetEvent.ReceivedRemoteFileState(origin, fileRef, state, result) ->
            Dto.MediaSet.ReceivedRemoteFileState.FromDomain(origin, fileRef, state, result) |> box
        | MediaSetEvent.ReceivedRemoteSubtitlesFileState(origin, subRef, state, result) ->
            Dto.MediaSet.ReceivedRemoteSubtitlesFileState.FromDomain(origin, subRef, state, result) |> box
        | MediaSetEvent.ReceivedRemoteSmilState(origin, version, state, result) ->
            Dto.MediaSet.ReceivedRemoteSmilState.FromDomain(origin, version, state, result) |> box
        | MediaSetEvent.ClearedRemoteFile(origin, fileRef) -> Dto.MediaSet.ClearRemoteFile.FromDomain(origin, fileRef) |> box
        | MediaSetEvent.AssignedGlobalConnectFile(fileRef, file) -> Dto.MediaSet.AssignGlobalConnectFile.FromDomain(fileRef, file) |> box
        | MediaSetEvent.AssignedGlobalConnectSubtitles sub -> Dto.MediaSet.AssignGlobalConnectSubtitles.FromDomain sub |> box
        | MediaSetEvent.AssignedGlobalConnectSmil smil -> Dto.MediaSet.AssignGlobalConnectSmil.FromDomain(smil) |> box
        | MediaSetEvent.ClearedCurrentState -> Dto.MediaSet.ClearCurrentState.FromDomain() |> box
        | MediaSetEvent.Deprecated -> Dto.MediaSet.DeprecatedEvent.Instance |> box

    let toDomain (event: Dto.IProtoBufSerializableEvent) =
        match event with
        | :? Dto.MediaSet.AssignClientContentId as event -> MediaSetEvent.AssignedClientContentId <| event.ToDomain()
        | :? Dto.MediaSet.SetSchemaVersion as event -> MediaSetEvent.SetSchemaVersion <| event.ToDomain()
        | :? Dto.MediaSet.SetMediaType as event -> MediaSetEvent.SetMediaType <| event.ToDomain()
        | :? Dto.MediaSet.SetMediaTypeFromMediaMode as event -> MediaSetEvent.SetMediaType <| event.ToDomain()
        | :? Dto.MediaSet.SetGeoRestriction as event -> MediaSetEvent.SetGeoRestriction <| event.ToDomain()
        | :? Dto.MediaSet.SetAccessRestrictions as event -> MediaSetEvent.SetGeoRestriction <| event.ToDomain()
        | :? Dto.MediaSet.AssignPart as event -> MediaSetEvent.AssignedPart <| event.ToDomain()
        | :? Dto.MediaSet.AssignFile as event -> MediaSetEvent.AssignedFile <| event.ToDomain()
        | :? Dto.MediaSet.AssignSubtitlesFile as event -> MediaSetEvent.AssignedSubtitlesFile <| event.ToDomain()
        | :? Dto.MediaSet.AssignSubtitlesFiles as event -> MediaSetEvent.AssignedSubtitlesFiles <| event.ToDomain()
        | :? Dto.MediaSet.AssignSubtitlesLinks as event -> MediaSetEvent.AssignedSubtitlesLinks <| event.ToDomain()
        | :? Dto.MediaSet.RemovePart as event -> MediaSetEvent.RemovedPart <| event.ToDomain()
        | :? Dto.MediaSet.RevokePart as event -> MediaSetEvent.RevokedPart <| event.ToDomain()
        | :? Dto.MediaSet.RestorePart as event -> MediaSetEvent.RestoredPart <| event.ToDomain()
        | :? Dto.MediaSet.RemoveFile as event -> MediaSetEvent.RemovedFile <| event.ToDomain()
        | :? Dto.MediaSet.RemoveSubtitlesFile as event -> MediaSetEvent.RemovedSubtitlesFile <| event.ToDomain()
        | :? Dto.MediaSet.AssignDesiredState as event when event.DesiredState.Content <> null && event.DesiredState.Content.Length > 1 ->
            MediaSetEvent.Deprecated
        | :? Dto.MediaSet.AssignDesiredState as event -> MediaSetEvent.AssignedDesiredState <| event.ToDomain()
        | :? Dto.MediaSet.MergeDesiredState as event when event.DesiredState.Content <> null && event.DesiredState.Content.Length > 1 ->
            MediaSetEvent.Deprecated
        | :? Dto.MediaSet.MergeDesiredState as event -> MediaSetEvent.MergedDesiredState <| event.ToDomain()
        | :? Dto.MediaSet.ClearDesiredState -> MediaSetEvent.ClearedDesiredState
        | :? Dto.MediaSet.ReceivedRemoteFileState as event -> MediaSetEvent.ReceivedRemoteFileState <| event.ToDomain()
        | :? Dto.MediaSet.ReceivedRemoteSubtitlesFileState as event -> MediaSetEvent.ReceivedRemoteSubtitlesFileState <| event.ToDomain()
        | :? Dto.MediaSet.ReceivedRemoteSmilState as event -> MediaSetEvent.ReceivedRemoteSmilState <| event.ToDomain()
        | :? Dto.MediaSet.ClearRemoteFile as event -> MediaSetEvent.ClearedRemoteFile <| event.ToDomain()
        | :? Dto.MediaSet.AssignGlobalConnectFile as event -> MediaSetEvent.AssignedGlobalConnectFile <| event.ToDomain()
        | :? Dto.MediaSet.AssignGlobalConnectSubtitles as event -> MediaSetEvent.AssignedGlobalConnectSubtitles <| event.ToDomain()
        | :? Dto.MediaSet.AssignGlobalConnectSmil as event -> MediaSetEvent.AssignedGlobalConnectSmil <| event.ToDomain()
        | :? Dto.MediaSet.ClearCurrentState -> MediaSetEvent.ClearedCurrentState
        | :? Dto.MediaSet.DeprecatedEvent -> MediaSetEvent.Deprecated
        | _ -> invalidOp <| $"Unexpected event %A{event}"
