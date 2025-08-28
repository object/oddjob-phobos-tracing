namespace Nrk.Oddjob.Upload.GlobalConnect

open System
open Akkling

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.HelperActors.PriorityQueueActor
open Nrk.Oddjob.Core.S3.S3Types
open Nrk.Oddjob.Upload
open Nrk.Oddjob.Upload.UploadTypes

[<NoEquality; NoComparison>]
type GlobalConnectUploadProps =
    {
        MediaSetId: MediaSetId
        MediaSetController: IActorRef<MediaSetCommand>
        ClientRef: ClientRef
        ClientContentId: ClientContentId
        BucketName: string
        FailingFilesRetryInterval: TimeSpan array
        PublishConfirmationTimeout: TimeSpan option
        S3Queue: ICanTell<PriorityQueueCommand<S3MessageContext>>
        ActionEnvironment: GlobalConnectEnvironment
        StorageCleanupDelay: TimeSpan
        SkipProcessingForSources: string list
        MinimumMessagePriority: int
    }

[<NoEquality; NoComparison>]
type GlobalConnectFileUploadProps =
    {
        ResourceRef: ResourceRef
        MediaSetController: IActorRef<MediaSetCommand>
        InitialState: GlobalConnectResourceState
        ClientRef: ClientRef
        ClientContentId: ClientContentId
        BucketName: string
        FailingFilesRetryInterval: TimeSpan array
        PublishConfirmationTimeout: TimeSpan option
        S3Queue: ICanTell<PriorityQueueCommand<S3MessageContext>>
        ActionEnvironment: GlobalConnectEnvironment
        StorageCleanupDelay: TimeSpan
        UploadEventsPublisherProps: UploadEventPublisher.UploadEventsPublisherProps
    }
