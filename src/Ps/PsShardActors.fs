namespace Nrk.Oddjob.Ps

module PsShardActors =

    open Akkling
    open Akkling.Persistence

    open Nrk.Oddjob.Core

    open PsFilesHandlerActor
    open PsTranscodingActors
    open PsRadioActors
    open PsTvSubtitleFilesActor
    open PsProgramStatusActors
    open PsPlaybackActors
    open PsShardMessages

    [<NoEquality; NoComparison>]
    type PsMediaSetShardActorProps =
        {
            FilesHandlerProps: PsFilesHandlerProps
            TvSubtitleFilesHandlerProps: PsTvSubtitleFilesProps
            PlaybackHandlerProps: PsPlayabilityNotificationProps
            ProgramStatusHandlerProps: PsProgramStatusHandlerProps
            TranscodingHandlerProps: PsTranscodingHandlerProps
            RadioHandlerProps: PsRadioHandlerProps
            MediaSetRef: PsMediatorRef
            ActivityContext: ActivitySourceContext
        }

    let psMediaSetShardActor (aprops: PsMediaSetShardActorProps) (mailbox: Actor<_>) =

        let mediaSetRef =
            match aprops.MediaSetRef with
            | PsMediatorRef.Self -> mailbox.Self
            | PsMediatorRef.Proxy proxy -> proxy

        let getFilesHandler () =
            let actorName = sprintf "psf:%s" mailbox.Self.Path.Name
            getOrSpawnChildActor mailbox.UntypedContext actorName (psFilesHandlerActor aprops.FilesHandlerProps mediaSetRef |> propsPersistNamed "ps-files")

        let getTvSubtitleFilesHandler () =
            getOrSpawnChildActor
                mailbox.UntypedContext
                "tv_subtitle_files"
                (psTvSubtitleFilesActor aprops.TvSubtitleFilesHandlerProps mediaSetRef |> propsNamed "ps-tv-subtitles")

        let getPlayabilityHandler () =
            let actorName = sprintf "psp:%s" mailbox.Self.Path.Name
            getOrSpawnChildActor
                mailbox.UntypedContext
                actorName
                (psPlayabilityNotificationActor aprops.PlaybackHandlerProps |> propsPersistNamed "ps-playability")

        let getProgramStatusHandler () =
            getOrSpawnChildActor
                mailbox.UntypedContext
                "program_status"
                (psProgramStatusHandlerActor aprops.ProgramStatusHandlerProps mediaSetRef |> propsNamed "ps-program-status")

        let getVideoTranscodingHandler () =
            getOrSpawnChildActor
                mailbox.UntypedContext
                "video_transcoding"
                (psTranscodingHandlerActor aprops.TranscodingHandlerProps |> propsNamed "ps-video-transcoding")

        let getRadioTranscodingHandler () =
            getOrSpawnChildActor mailbox.UntypedContext "radio" (psRadioHandlerActor aprops.RadioHandlerProps |> propsNamed "ps-radio-transcoding")

        let handleShardMessage (message: PsShardMessage) ack =
            addToTraceBaggage "MediaSetId" message.EntityId
            let programId = (MediaSetId.parse message.EntityId).ContentId.Value.ToUpper()
            updateTraceSpanForMessage mailbox "PS.Shard.MediaSet" message [ ("MediaSetId", message.EntityId); ("ProgramId", programId) ]
            match message with
            | PsShardMessage.FilesJob msg when Option.isSome ack ->
                getFilesHandler () <<! QueueMessage.createWithAck msg ack
                ignored ()
            | PsShardMessage.GetFilesState _ ->
                getFilesHandler () <<! message
                ignored ()
            | PsShardMessage.SubtitleFilesJob msg when Option.isSome ack ->
                getTvSubtitleFilesHandler () <<! QueueMessage.createWithAck msg ack
                ignored ()
            | PsShardMessage.SubtitleFilesDetails _ ->
                getFilesHandler () <<! message
                ignored ()
            | PsShardMessage.PlayabilityEvent msg ->
                getPlayabilityHandler () <<! msg
                ignored ()
            | PsShardMessage.GetPlayabilityHandlerState _ ->
                getPlayabilityHandler () <<! message
                ignored ()
            | PsShardMessage.VideoMigrationDetails _ ->
                getFilesHandler () <<! message
                ignored ()
            | PsShardMessage.VideoTranscodingDetails _ ->
                getFilesHandler () <<! message
                ignored ()
            | PsShardMessage.AudioTranscodingDetails _ ->
                getFilesHandler () <<! message
                ignored ()
            | PsShardMessage.AudioMigrationDetails _ ->
                getFilesHandler () <<! message
                ignored ()
            | PsShardMessage.ProgramStatusEvent msg when Option.isSome ack ->
                getProgramStatusHandler () <<! QueueMessage.createWithAck msg ack
                ignored ()
            | PsShardMessage.VideoTranscodingJob msg when Option.isSome ack ->
                getVideoTranscodingHandler () <<! QueueMessage.createWithAck msg ack
                ignored ()
            | PsShardMessage.AudioTranscodingJob job ->
                getRadioTranscodingHandler () <<! QueueMessage.createWithAck job ack
                ignored ()
            | PsShardMessage.VideoTranscodingDetailsAck ack ->
                getVideoTranscodingHandler () <<! ack
                ignored ()
            | PsShardMessage.ProgramGotActiveCarriersEvent msg when Option.isSome ack ->
                getFilesHandler () <<! QueueMessage.createWithAck msg ack
                ignored ()
            | PsShardMessage.UsageRights _ ->
                getFilesHandler () <<! message
                ignored ()
            | PsShardMessage.FilesJob _
            | PsShardMessage.SubtitleFilesJob _
            | PsShardMessage.ProgramStatusEvent _
            | PsShardMessage.VideoTranscodingJob _
            | PsShardMessage.ProgramGotActiveCarriersEvent _ ->
                logWarning mailbox $"Attempt to forward non-queued message {message}"
                ignored ()
            | PsShardMessage.AudioTranscodingDetailsAck ack ->
                getRadioTranscodingHandler () <<! ack
                ignored ()

        let rec loop () =
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? QueueMessage<PsShardMessage> as message -> handleShardMessage message.Payload message.Ack
                    | :? PsShardMessage as message -> // Unwrapped shard messages can come from Web API, persistent reminders and transcoding actor
                        handleShardMessage message None
                    | :? Dto.Events.MediaSetPlayabilityEventDto as message ->
                        let msg = PsShardMessage.PlayabilityEvent message
                        handleShardMessage msg None
                    | :? PsDto.PlayabilityBump as message ->
                        addToTraceBaggage "ProgramId" message.ProgramId
                        getPlayabilityHandler () <! message
                        ignored ()
                    | :? PsDto.TranscodingBump as message ->
                        addToTraceBaggage "ProgramId" message.ProgramId
                        getVideoTranscodingHandler () <! message
                        ignored ()
                    | :? Dto.Events.MediaSetPlayabilityBump as message ->
                        addToTraceBaggage "ProgramId" message.ProgramId
                        getPlayabilityHandler ()
                        <! {
                               PsDto.PlayabilityBump.ProgramId = message.ProgramId
                           }
                        ignored ()
                    | :? Dto.Events.OddjobEventDto -> ignored () // No handler for raw OddjobEventDto
                    | LifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }

        loop ()
