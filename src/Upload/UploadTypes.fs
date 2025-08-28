namespace Nrk.Oddjob.Upload

module UploadTypes =

    open Akka.Actor
    open Akkling

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.ShardMessages

    /// Used in actor name generation
    /// Examples:
    ///     ps~nnfa41016618 (group)
    ///     nnfa41016618aa (part)
    ///     nnfa41016618aa_2250.mp4 (file)
    type UploadActorPathSegment =
        private
        | UploadActorPathSegment of string

        override this.ToString() =
            let (UploadActorPathSegment str) = this in str

    [<RequireQualifiedAccess>]
    module UploadActorPathSegment =

        let private create str =
            if String.isNullOrEmpty str then
                invalidArg "str" "PersistentIdSegment cannot be empty"
            else
                UploadActorPathSegment(normalizeActorNameSegment str)

        let forFileRef mediaSetId fileRef =
            (fileRef |> FileRef.toFileName mediaSetId).Value |> create

        let forSubtitlesLinks () = UploadActorPathSegment "subtitles"

        let forSubtitles (subRef: SubtitlesTrackRef) =
            $"subtitles-{subRef.LanguageCode.Value}-{subRef.Name.Value}" |> create

        let forSmil () = UploadActorPathSegment "smil"

        let forResourceRef mediaSetId resourceRef =
            match resourceRef with
            | ResourceRef.File fileRef -> forFileRef mediaSetId fileRef
            | ResourceRef.Subtitles subRef -> forSubtitles subRef
            | ResourceRef.Smil -> forSmil ()

        let value (UploadActorPathSegment str) = str

    type ApplyState =
        {
            ExecutionContext: ExecutionContext
            DesiredState: DesiredMediaSetState
            OriginState: OriginState
        }

    [<NoEquality; NoComparison>]
    type PotionClientRef =
        {
            PotionMediator: IActorRef<obj>
            ExternalGroupIdResolver: string -> string
        }

    type ClientContentId = ClientContentId of string option

    [<RequireQualifiedAccess>]
    type ClientRef =
        | Potion of PotionClientRef

        member this.ClientPublisher =
            match this with
            | Potion p -> retype p.PotionMediator

    type UploadShardExtractor(numOfShards) =
        inherit Akka.Cluster.Sharding.HashCodeMessageExtractor(numOfShards)

        override this.EntityId(message: obj) : string =
            match message with
            | :? Akka.Cluster.Sharding.ShardRegion.StartEntity as se -> se.EntityId
            | :? MediaSetShardMessage as message -> message.EntityId
            | :? Message<MediaSetShardMessage> as message -> message.Payload.EntityId
            | :? QueueMessage<MediaSetShardMessage> as message -> message.Payload.EntityId
            | LifecycleEvent _ -> null
            | _ -> invalidArg "message" $"Unable to extract EntityId from a message [%A{message}] of type [%s{message.GetType().FullName}]"
