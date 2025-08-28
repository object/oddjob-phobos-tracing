namespace Nrk.Oddjob.Ps

module PsShardMessages =

    open Akkling

    open Nrk.Oddjob.Core
    open PsTypes

    [<RequireQualifiedAccess>]
    type PsShardMessage =
        | VideoTranscodingJob of PsVideoTranscodingJob
        | VideoTranscodingDetails of PsVideoTranscodingJob
        | VideoTranscodingDetailsAck of VideoTranscodingDetailsAck
        | VideoMigrationDetails of PsLegacyVideoDetails
        | FilesJob of PsChangeJob
        | SubtitleFilesJob of PsSubtitleFilesJob
        | SubtitleFilesDetails of PsSubtitleFilesDetails
        | GetFilesState of MediaSetId
        | PlayabilityEvent of Dto.Events.MediaSetPlayabilityEventDto
        | GetPlayabilityHandlerState of MediaSetId
        | ProgramStatusEvent of PsProgramStatusEvent
        | ProgramGotActiveCarriersEvent of PsProgramGotActiveCarriersEvent
        | AudioTranscodingJob of PsAudioTranscodingJob
        | AudioTranscodingDetails of PsAudioTranscodingJob
        | AudioTranscodingDetailsAck of AudioTranscodingDetailsAck
        | AudioMigrationDetails of PsLegacyAudioDetails
        | UsageRights of PsUsageRightsJob

        member this.EntityId =
            let toMediaSetId programId =
                (MediaSetId.create (Alphanumeric PsClientId) (ContentId programId)).Value
            match this with
            | VideoTranscodingJob job -> job.ProgramId |> toMediaSetId
            | VideoTranscodingDetails details -> details.ProgramId |> toMediaSetId
            | VideoTranscodingDetailsAck ack -> ack.ProgramId |> toMediaSetId
            | VideoMigrationDetails details -> details.ProgramId |> toMediaSetId
            | FilesJob job -> job.PiProgId |> toMediaSetId
            | SubtitleFilesJob job -> job.ProgramId |> toMediaSetId
            | SubtitleFilesDetails job -> job.ProgramId |> toMediaSetId
            | GetFilesState mediaSetId -> mediaSetId.Value
            | PlayabilityEvent evt -> evt.MediaSetId
            | GetPlayabilityHandlerState mediaSetId -> mediaSetId.Value
            | ProgramStatusEvent evt -> evt.GetMediaSetId().Value
            | ProgramGotActiveCarriersEvent evt -> evt.ProgramId |> toMediaSetId
            | AudioTranscodingJob job -> job.ProgramId |> toMediaSetId
            | AudioTranscodingDetails details -> details.ProgramId |> toMediaSetId
            | AudioTranscodingDetailsAck ack -> ack.ProgramId |> toMediaSetId
            | AudioMigrationDetails details -> details.ProgramId |> toMediaSetId
            | UsageRights job -> job.ProgramId |> toMediaSetId
            |> normalizeActorNameSegment

    type PsShardExtractor(numOfShards) =
        inherit Akka.Cluster.Sharding.HashCodeMessageExtractor(numOfShards)

        let (|MessageWithEntityId|_|) (message: obj) =
            match message with
            | :? Akka.Cluster.Sharding.ShardRegion.StartEntity as se -> Some se.EntityId
            | :? PsShardMessage as message -> Some message.EntityId
            | :? Message<PsShardMessage> as message -> Some message.Payload.EntityId
            | :? QueueMessage<PsShardMessage> as message -> Some message.Payload.EntityId
            | _ -> None

        let (|MessageWithMediaSetId|_|) (message: obj) =
            match message with
            | :? Dto.Events.OddjobEventDto as message -> Some message.MediaSetId
            | :? Dto.Events.MediaSetPlayabilityEventDto as message -> Some message.MediaSetId
            | _ -> None

        let (|MessageWithProgramId|_|) (message: obj) =
            match message with
            | :? PsDto.TranscodingBump as message -> Some message.ProgramId
            | :? PsDto.PlayabilityBump as message -> Some message.ProgramId
            | :? Dto.Events.MediaSetPlayabilityBump as message -> Some message.ProgramId
            | :? PsPersistence.FilesState as message -> Some message.ProgramId
            | _ -> None

        override this.EntityId(message: obj) : string =
            let invalidMessage message =
                invalidArg "message" $"Unable to extract EntityId from a message [%A{message}] of type [%s{message.GetType().FullName}]"
            match message with
            | MessageWithEntityId entityId -> entityId |> normalizeActorNameSegment
            | MessageWithMediaSetId mediaSetId -> mediaSetId |> normalizeActorNameSegment
            | MessageWithProgramId programId ->
                let mediaSetId = MediaSetId.create (Alphanumeric PsClientId) (ContentId programId)
                mediaSetId.Value |> normalizeActorNameSegment
            | LifecycleEvent msg -> msg.GetType().Name // Occasionally PreStart comes when the actor system starts up
            | _ -> invalidMessage message

    [<RequireQualifiedAccess>]
    type PsMediatorRef =
        | Self
        | Proxy of IActorRef<obj>
