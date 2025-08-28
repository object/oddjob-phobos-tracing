namespace Nrk.Oddjob.Core


module ShardMessages =

    // MediaSet shard messages are used in persistence reminders, so for backward compatibility DU cases declared below must retain their order,
    // i.e. MediaSetJob should remain at #1, GetMediaSetState at #3, LiveStreamStatusChangedReminder at #10 etc.
    // Alternatively, when changing the MediaSetShardMessage cases, all pending MediaSet reminder messages must be migrated or replayed.
    [<RequireQualifiedAccess>]
    type MediaSetShardMessage =
        | MediaSetJob of MediaSetJob // Not to be used in persistent reminders, does not serialize properly
        | GetMediaSetState of MediaSetId

        member this.MediaSetId =
            match this with
            | MediaSetJob job -> job.MediaSetId
            | GetMediaSetState mediaSetId -> mediaSetId

        member this.EntityId = this.MediaSetId.Value |> String.toLower |> normalizeActorNameSegment
