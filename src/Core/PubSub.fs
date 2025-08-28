namespace Nrk.Oddjob.Core.PubSub

open System
open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.S3.S3Types

[<AutoOpen>]
module PubSubMessages =

    type MediaSetStatusUpdate =
        {
            MediaSetId: string
            Status: int
            RemainingActions: Dto.MediaSet.RemainingActions
            Timestamp: DateTimeOffset
        }

    type MediaSetRemoteFileUpdate =
        {
            MediaSetId: string
            StorageProvider: string
            FileRef: Dto.MediaSet.FileRef
            RemoteState: int
            RemoteResult: Dto.MediaSet.RemoteResult
            Timestamp: DateTimeOffset
        }

    type MediaSetRemoteSmilUpdate =
        {
            MediaSetId: string
            StorageProvider: string
            VersionNumber: int
            RemoteState: int
            RemoteResult: Dto.MediaSet.RemoteResult
            Timestamp: DateTimeOffset
        }

    type MediaSetRemoteSubtitlesUpdate =
        {
            MediaSetId: string
            StorageProvider: string
            SubtitlesRef: Dto.MediaSet.SubtitlesRef
            RemoteState: int
            RemoteResult: Dto.MediaSet.RemoteResult
            Timestamp: DateTimeOffset
        }

    [<RequireQualifiedAccess>]
    type MediaSetRemoteResourceUpdate =
        | File of MediaSetRemoteFileUpdate
        | Smil of MediaSetRemoteSmilUpdate
        | Subtitles of MediaSetRemoteSubtitlesUpdate

    module MediaSetRemoteFileUpdate =
        let fromDomain (mediaSetId: MediaSetId) origin fileRef remoteState remoteResult timestamp =
            MediaSetRemoteResourceUpdate.File
                {
                    MediaSetId = mediaSetId.Value
                    StorageProvider = $"%A{origin}"
                    FileRef = Dto.MediaSet.FileRef.fromDomain fileRef
                    RemoteState = Dto.MediaSet.RemoteState.FromDomain remoteState
                    RemoteResult = Dto.MediaSet.RemoteResult.FromDomain remoteResult
                    Timestamp = timestamp
                }

    module MediaSetRemoteSmilUpdate =
        let fromDomain (mediaSetId: MediaSetId) origin versionNumber remoteState remoteResult timestamp =
            MediaSetRemoteResourceUpdate.Smil
                {
                    MediaSetId = mediaSetId.Value
                    StorageProvider = $"%A{origin}"
                    VersionNumber = versionNumber
                    RemoteState = Dto.MediaSet.RemoteState.FromDomain remoteState
                    RemoteResult = Dto.MediaSet.RemoteResult.FromDomain remoteResult
                    Timestamp = timestamp
                }

    module MediaSetRemoteSubtitlesUpdate =
        let fromDomain (mediaSetId: MediaSetId) origin subRef remoteState remoteResult timestamp =
            MediaSetRemoteResourceUpdate.Subtitles
                {
                    MediaSetId = mediaSetId.Value
                    StorageProvider = $"%A{origin}"
                    SubtitlesRef = Dto.MediaSet.SubtitlesRef.fromDomain subRef
                    RemoteState = Dto.MediaSet.RemoteState.FromDomain remoteState
                    RemoteResult = Dto.MediaSet.RemoteResult.FromDomain remoteResult
                    Timestamp = timestamp
                }

    type S3CommandItem =
        {
            TargetPath: string
            Priority: int
            Timestamp: DateTimeOffset
        }

    module S3CommandItem =
        let fromDomain (item: S3MessageContext) =
            {
                TargetPath = item.TargetPath |> Option.defaultValue "<empty>"
                Priority = item.Priority |> Option.defaultValue 0
                Timestamp = DateTimeOffset.Now
            }

    type PriorityQueueEvent =
        | PriorityQueueItemAdded of S3CommandItem
        | PriorityQueueItemRemoved of S3CommandItem

        member this.Command =
            match this with
            | PriorityQueueItemAdded cmd -> cmd
            | PriorityQueueItemRemoved cmd -> cmd

    type IngesterCommandStatus =
        | IngesterCommandInitiated of ingesterId: string * S3CommandItem
        | IngesterCommandCompleted of ingesterId: string * S3CommandItem
        | IngesterCommandFailed of ingesterId: string * S3CommandItem * error: string
        | IngesterCommandRejected of ingesterId: string * S3CommandItem * error: string

        member this.IngesterId =
            match this with
            | IngesterCommandInitiated(ingesterId, _) -> ingesterId
            | IngesterCommandCompleted(ingesterId, _) -> ingesterId
            | IngesterCommandFailed(ingesterId, _, _) -> ingesterId
            | IngesterCommandRejected(ingesterId, _, _) -> ingesterId
        member this.Command =
            match this with
            | IngesterCommandInitiated(_, cmd) -> cmd
            | IngesterCommandCompleted(_, cmd) -> cmd
            | IngesterCommandFailed(_, cmd, _) -> cmd
            | IngesterCommandRejected(_, cmd, _) -> cmd
        member this.Error =
            match this with
            | IngesterCommandInitiated _ -> None
            | IngesterCommandCompleted _ -> None
            | IngesterCommandFailed(_, _, error) -> Some error
            | IngesterCommandRejected(_, _, error) -> Some error

module Topic =
    [<Literal>]
    let GlobalConnectIngesterStatus = "GlobalConnectIngesterStatus"

    [<Literal>]
    let GlobalConnectCommandStatus = "GlobalConnectCommandStatus"

    [<Literal>]
    let GlobalConnectQueue = "GlobalConnectQueue"

    [<Literal>]
    let MediaSetStatus = "MediaSetStatus"

    [<Literal>]
    let MediaSetResourceState = "MediaSetRemoteState"

module PubSub =

    open Akka.Actor
    open Akka.Event
    open Akka.Cluster.Tools.PublishSubscribe

    let publish (system: ActorSystem) topic msg =
        system.Log.Debug $"PubSub/{topic}: {msg}"
        DistributedPubSub.Get(system).Mediator.Tell(Publish(topic, msg))
