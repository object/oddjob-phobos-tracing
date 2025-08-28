namespace Nrk.Oddjob.Core

[<RequireQualifiedAccess>]
module ClusterShards =

    open Akka.Cluster.Sharding
    open Akkling
    open Akkling.Cluster.Sharding

    [<Literal>]
    let PsMediaSetRegion = "psmediaset"

    [<Literal>]
    let PsCarrierRegion = "pscarrier"

    [<Literal>]
    let UploadMediaSetRegion = "uploadmediaset"

    [<Literal>]
    let PotionSetRegion = "potionset"

    [<Literal>]
    let UploadRole = "Upload"

    [<Literal>]
    let PsRole = "Ps"

    [<Literal>]
    let PotionRole = "Potion"

    let private getShardProxy system regionName role (shardExtractor: IMessageExtractor) =
        let extractor msg =
            (shardExtractor.ShardId msg, shardExtractor.EntityId msg, shardExtractor.EntityMessage msg)
        retype <| spawnShardedProxy extractor system regionName (Some role)

    let getUploadMediator system extractor =
        extractor :> HashCodeMessageExtractor |> getShardProxy system UploadMediaSetRegion UploadRole

    let getPsMediator system extractor =
        extractor :> HashCodeMessageExtractor |> getShardProxy system PsMediaSetRegion PsRole

    let getPotionMediator system extractor =
        extractor :> HashCodeMessageExtractor |> getShardProxy system PotionSetRegion PotionRole
