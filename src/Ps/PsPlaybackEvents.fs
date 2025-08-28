namespace Nrk.Oddjob.Ps

open System.Xml

module PsPlaybackEvents =

    open System
    open System.Text.Json
    open FSharpx.Collections
    open FsToolkit.ErrorHandling
    open Akkling

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Manifests
    open Nrk.Oddjob.Core.Dto.Events

    open PsTypes
    open PsGranittActors

    [<Literal>]
    let private StreamingTypeOnDemand = "onDemand"

    [<Literal>]
    let private StreamingTypeLive = "live"

    [<Literal>]
    let private MessageTypeUpdate = "update"

    [<Literal>]
    let private MessageTypeDelete = "delete"

    [<Literal>]
    let private MessageTypeStarted = "started"

    [<Literal>]
    let private MessageTypeEnded = "ended"

    [<Literal>]
    let private SourceMediumAudio = "audio"

    [<Literal>]
    let private SourceMediumVideo = "video"

    [<Literal>]
    let private ProgramScopeBroadcastingRights = "broadcastingRights"

    [<Literal>]
    let private PriorityNormal = "normal"

    [<Literal>]
    let private PriorityHigh = "high"

    [<Literal>]
    let private AvailabilityRegionNorway = "No"

    [<Literal>]
    let private AvailabilityRegionWorld = "World"

    [<Literal>]
    let private EncryptionLevelNone = "None"

    [<Literal>]
    let private EncryptionLevelAES128 = "AES128"

    [<Literal>]
    let private EncryptionLevelDRM = "DRM"

    [<Literal>]
    let private DisplayAspectRatio_16_9 = "16:9"

    [<Literal>]
    let private DisplayAspectRatio_4_3 = "4:3"

    [<Literal>]
    let private StreamingFormatHLSv3 = "HLSv3"

    [<Literal>]
    let private StreamingFormatHLSv4 = "HLSv4"

    [<Literal>]
    let private StreamingFormatHLSv7 = "HLSv7"

    [<Literal>]
    let private StreamingFormatDash = "Dash"

    [<Literal>]
    let private AudioFormatMono = "Mono"

    [<Literal>]
    let private AudioFormatStereo = "Stereo"

    [<Literal>]
    let private AudioFormatSurround = "Surround"

    [<Literal>]
    let private VideoResolutionFormatSD = "SD"

    [<Literal>]
    let private VideoResolutionFormatHD = "HD"

    [<Literal>]
    let private VideoResolutionFormatTwoK = "TwoK"

    [<Literal>]
    let private VideoResolutionFormatFourK = "FourK"

    [<Literal>]
    let private VideoDynamicRangeSDR = "SDR"

    [<Literal>]
    let private VideoDynamicRangeHDR = "HDR"

    [<Literal>]
    let private SubtitlesFormatM3U8 = "m3u8"

    [<Literal>]
    let private SubtitlesFormatTTML = "ttml"

    [<Literal>]
    let private SubtitlesFormatWebVTT = "webvtt"

    [<Literal>]
    let private UploadStatusPending = "Pending"

    [<Literal>]
    let private UploadStatusFailed = "Failed"

    [<Literal>]
    let private UploadStatusCompleted = "Completed"

    [<Literal>]
    let private VideoQuality180p25 = "180p25"

    [<Literal>]
    let private VideoQuality270p25 = "270p25"

    [<Literal>]
    let private VideoQuality360p25 = "360p25"

    [<Literal>]
    let private VideoQuality540p25 = "540p25"

    [<Literal>]
    let private VideoQuality720p25 = "720p25"

    [<Literal>]
    let private VideoQuality720p50 = "720p50"

    [<Literal>]
    let private VideoQuality1080p25 = "1080p25"

    [<Literal>]
    let private VideoQuality1080p50 = "1080p50"

    [<Literal>]
    let private VideoQuality1440p25 = "1440p25"

    [<Literal>]
    let private VideoQuality1440p50 = "1440p50"

    [<Literal>]
    let private VideoQuality2160p25 = "2160p25"

    [<Literal>]
    let private VideoQuality2160p50 = "2160p50"

    [<Literal>]
    let private BroadcastingRightsLimited = "Limited"

    [<Literal>]
    let private BroadcastingRightsPerpetual = "Perpetual"

    type MessageHeaders =
        {
            SourceMedium: string
            StreamingType: string
            MessageType: string
            ProgramScope: string
            Priority: string
        }

        static member Empty =
            {
                SourceMedium = null
                StreamingType = null
                MessageType = null
                ProgramScope = null
                Priority = null
            }

    type EventSinkType =
        | PlaybackEventSink
        | DistributionStatusEventSink

    type ServiceBusMessage =
        {
            EventSinkType: EventSinkType
            MediaSetId: string
            Payload: byte[]
            Headers: MessageHeaders
        }

    let getProgramId mediaSetId =
        (MediaSetId.parse mediaSetId).ContentId.Value.ToUpper()

    let getSourceMedium mediaType =
        match mediaType with
        | MediaType.Video -> Some SourceMediumVideo
        | MediaType.Audio -> Some SourceMediumAudio

    let getEventPriority priority =
        match priority with
        | Events.PlayabilityEventPriority.Normal -> PriorityNormal
        | Events.PlayabilityEventPriority.High -> PriorityHigh

    let formatTime (time: DateTimeOffset) =
        time.DateTime.ToUniversalTime().ToString("u", Globalization.CultureInfo.InvariantCulture).Replace(" ", "T")

    let formatDuration (duration: NodaTime.Duration) =
        duration.ToString("H:mm:ss.FF", System.Globalization.CultureInfo.InvariantCulture)

    let getAvailabilityRegion geoRestriction =
        geoRestriction
        |> Option.map (fun geoRestriction ->
            match geoRestriction with
            | GeoRestriction.World -> AvailabilityRegionWorld
            | GeoRestriction.Norway
            | _ -> AvailabilityRegionNorway)
        |> Option.defaultValue AvailabilityRegionWorld

    let getStreamingFormat manifest =
        match manifest with
        | MediaManifest.Hls manifest ->
            match manifest.Version with
            | 3 -> StreamingFormatHLSv3
            | 4 -> StreamingFormatHLSv4
            | 7 -> StreamingFormatHLSv7
            | _ -> StreamingFormatHLSv3
        | MediaManifest.Dash _ -> StreamingFormatDash

    let getAudioFormat () = AudioFormatStereo // TODO

    let getVideoResolutionFormat (manifest: MediaManifest) =
        if manifest.Streams |> Seq.isEmpty then
            VideoResolutionFormatSD
        else
            let maxBitRate = manifest.Streams |> Seq.map _.Resolution |> Seq.map snd |> Seq.max
            if maxBitRate >= 1080 then
                VideoResolutionFormatHD
            else
                VideoResolutionFormatSD

    let getVideoDynamicRange (manifest: MediaManifest) =
        match manifest with
        | MediaManifest.Dash _ -> VideoDynamicRangeSDR
        | MediaManifest.Hls manifest ->
            if
                manifest.Streams |> Seq.exists (fun x -> x.VideoRange = HlsVideoRange.HDR) // TODO: verify selection logic
            then
                VideoDynamicRangeHDR
            else
                VideoDynamicRangeSDR

    let getDisplayAspectRatio () = DisplayAspectRatio_16_9 // Use as default for now

    let getDuration (state: MediaSetState) (psFilesState: PsFilesState) =

        let tryGetFileDuration contentFile =
            match contentFile.MediaProperties with
            | MediaPropertiesV1 _
            | Unspecified -> None
            | MediaPropertiesV2 properties -> System.Xml.XmlConvert.ToTimeSpan(properties.Duration).TotalMilliseconds |> Some

        let tryGetPartDuration (chunk: ContentChunk) =
            chunk.Files |> Seq.tryHead |> Option.bind tryGetFileDuration

        match state.Desired.Content with
        | Empty
        | NoParts _ -> None
        | Parts(_, part) -> part |> tryGetPartDuration
        |> Option.defaultWith (fun () ->
            psFilesState.GetActiveFileSets()
            |> Map.values
            |> Seq.map _.TranscodedFiles
            |> Seq.map (fun files ->
                match files with
                | PsTranscodedFiles.Video files -> XmlConvert.ToTimeSpan(files[0].Duration).TotalMilliseconds
                | PsTranscodedFiles.LegacyVideo files -> XmlConvert.ToTimeSpan(files[0].Duration).TotalMilliseconds
                | PsTranscodedFiles.Audio files -> files[0].Duration
                | PsTranscodedFiles.LegacyAudio files -> files[0].Duration)
            |> Seq.sumBy float)
        |> NodaTime.Duration.FromMilliseconds
        |> formatDuration

    let getTranscodedAudioQualities content =
        content
        |> ContentSet.getFiles
        |> Map.keys
        |> Seq.map (_.QualityId)
        |> Seq.map (fun qualityId -> qualityId.Value * 1000)
        |> Seq.toArray

    let tryParseVideoQualityFromFilePath (filePath: string) =
        if String.isNullOrEmpty filePath then
            None
        else
            IO.getFileName(filePath).ToUpper()
            |> String.split '_'
            |> Seq.last // Extract IDnnn, e.g. ID180, ID1080-1
            |> String.split '.'
            |> Seq.head // Trim possible version suffix, e.g. ID180.1
            |> function
                | "ID180" -> Some VideoQuality180p25
                | "ID270" -> Some VideoQuality270p25
                | "ID360" -> Some VideoQuality360p25
                | "ID540" -> Some VideoQuality540p25
                | "ID720"
                | "ID720-1" -> Some VideoQuality720p25
                | "ID720-2" -> Some VideoQuality720p50
                | "ID1080" -> Some VideoQuality1080p25
                | "ID1080-1" -> Some VideoQuality1080p25
                | "ID1080-2" -> Some VideoQuality1080p50
                | "ID1440-1" -> Some VideoQuality1440p25
                | "ID1440-2" -> Some VideoQuality1440p50
                | "ID2160-1" -> Some VideoQuality2160p25
                | "ID2160-2" -> Some VideoQuality2160p50
                | _ -> None

    let getVideoQuality (contentFile: ContentFile) =
        match contentFile.MediaProperties with
        | MediaPropertiesV2 mediaProperties ->
            let height = mediaProperties.Video.Height
            let frameRate = mediaProperties.Video.FrameRate
            let adjustedFrameRate = if frameRate < 37.5m then 25 else 50
            $"%d{height}p%d{adjustedFrameRate}" |> Some
        | MediaPropertiesV1 _
        | Unspecified ->
            // Legacy
            contentFile.SourcePath |> Option.bind (fun x -> tryParseVideoQualityFromFilePath (FilePath.value x))

    let getTranscodedVideoQualities content =
        content |> ContentSet.getFiles |> Map.values |> Seq.map getVideoQuality |> Seq.choose id |> Seq.toArray

    let getSubtitlesFormat format =
        match format with
        | M3U8 -> SubtitlesFormatM3U8
        | TTML -> SubtitlesFormatTTML
        | WebVTT -> SubtitlesFormatWebVTT

    let isSubtitlesDefaultTrack languageType hasMultipleFiles =
        not hasMultipleFiles || String.toLower languageType = "ttv"

    let getGlobalConnectOnDemandStatus (state: MediaSetState) =
        let files =
            state.Current.GlobalConnect.Files |> Map.filter (fun _ file -> file.RemoteState.State = DistributionState.Completed)
        if files = Map.empty then
            Result.Ok [||]
        else
            Result.Ok [| () |]

    let getUploadStatus remoteState =
        match remoteState with
        | DistributionState.Completed -> UploadStatusCompleted
        | DistributionState.Rejected -> UploadStatusFailed
        | _ -> UploadStatusPending

    let getAggregatedQualityState state accState =
        match accState, state with
        | x, y when x = y -> accState
        | DistributionState.Rejected, _ -> accState
        | _, DistributionState.Rejected -> state
        | DistributionState.Failed, _ -> accState
        | _, DistributionState.Failed -> state
        | DistributionState.Completed, _ -> state
        | _, _ -> accState

    let getAggregatedAudioQualityStates initialState (remoteStates: Map<FileRef, DistributionState>) =
        remoteStates
        |> Map.toList
        |> List.map (fun (x, y) -> x.QualityId, y)
        |> List.groupBy fst
        |> List.map (fun (x, y) -> x.Value * 1000, y |> List.map snd |> List.fold getAggregatedQualityState initialState)
        |> Map.ofList

    let getAggregatedVideoQualityStates initialState (remoteStates: Map<string, DistributionState>) =
        remoteStates
        |> Map.toList
        |> List.groupBy fst
        |> List.map (fun (x, y) -> x, y |> List.map snd |> List.fold getAggregatedQualityState initialState)
        |> Map.ofList

    let getGlobalConnectAudioQualityStates initialState (state: MediaSetState) =
        state.Current.GlobalConnect.Files
        |> Map.map (fun _ file -> file.RemoteState.State)
        |> getAggregatedAudioQualityStates initialState

    let getGlobalConnectVideoQualityStates initialState (state: MediaSetState) =
        state.Current.GlobalConnect.Files
        |> Map.toList
        |> List.map (fun (_, file) ->
            file.File.SourcePath |> Option.map (FilePath.value >> tryParseVideoQualityFromFilePath) |> Option.flatten, file.RemoteState.State)
        |> List.choose (fun (x, y) -> x |> Option.map (fun x -> x, y))
        |> Map.ofList
        |> getAggregatedVideoQualityStates initialState

    type AudioQualityStatus(audioBitrate: int, state: DistributionState) =
        member this.AudioBitrate = audioBitrate
        member this.UploadStatus = getUploadStatus state

    type AudioFileMetadata(audioBitrate: int) =
        member this.Format: string = getAudioFormat ()
        member this.Bitrate: int = audioBitrate

    let getAudioQualityStatusFromManifest content remoteStates (manifest: MediaManifest) =
        let completedQualities = manifest.Streams |> List.map (_.Bandwidth) |> List.toArray
        let incompleteQualities =
            content |> getTranscodedAudioQualities |> Array.filter (fun x -> not (completedQualities |> Array.contains x))
        Array.concat
            [|
                completedQualities |> Array.map (fun quality -> AudioQualityStatus(quality, DistributionState.Completed))
                incompleteQualities
                |> Array.map (fun quality -> AudioQualityStatus(quality, remoteStates |> Map.tryFind quality |> Option.defaultValue DistributionState.None))
            |]

    let getAudioFileMetadata content remoteStates =
        content
        |> getTranscodedAudioQualities
        |> Array.map (fun quality -> AudioQualityStatus(quality, remoteStates |> Map.tryFind quality |> Option.defaultValue DistributionState.None))

    type VideoQualityBitRates(videoQuality: string, bitRates: int seq) =
        member this.VideoQuality: string = videoQuality
        member this.MinBitrate: int = if bitRates |> Seq.isEmpty then 0 else bitRates |> Seq.min
        member this.MaxBitrate: int = if bitRates |> Seq.isEmpty then 0 else bitRates |> Seq.max

    type VideoQualityStatusWithBitRates(videoQuality: string, bitRates: int seq, state: DistributionState) =
        let videoQualityBitRates = VideoQualityBitRates(videoQuality, bitRates)
        member this.VideoQuality: string = videoQuality
        member this.MinBitrate: int = videoQualityBitRates.MinBitrate
        member this.MaxBitrate: int = videoQualityBitRates.MaxBitrate
        member this.UploadStatus: string = getUploadStatus state

    type VideoQualityStatus(videoQuality: string, state: DistributionState) =
        member this.VideoQuality: string = videoQuality
        member this.UploadStatus: string = getUploadStatus state

    let tryGetVideoQuality height frameRate bandwidths =
        // Originally we only allowed frame rates 25 and 50 but received from some live channels 30 and 60 (rounded up).
        // To support frame rates that were not originally planned we adjust input values to match either 25 or 50.
        // This may result in lower playback quality.
        let adjustedFrameRate = if frameRate < 37.5m then 25 else 50
        match height, adjustedFrameRate with
        | 180, 25 -> Some(VideoQuality180p25, bandwidths)
        | 270, 25 -> Some(VideoQuality270p25, bandwidths)
        | 360, 25 -> Some(VideoQuality360p25, bandwidths)
        | 540, 25 -> Some(VideoQuality540p25, bandwidths)
        | 720, 25 -> Some(VideoQuality720p25, bandwidths)
        | 720, 50 -> Some(VideoQuality720p50, bandwidths)
        | 1080, 25 -> Some(VideoQuality1080p25, bandwidths)
        | 1080, 50 -> Some(VideoQuality1080p50, bandwidths)
        | 1440, 25 -> Some(VideoQuality1440p25, bandwidths)
        | 1440, 50 -> Some(VideoQuality1440p50, bandwidths)
        | 2160, 25 -> Some(VideoQuality2160p25, bandwidths)
        | 2160, 50 -> Some(VideoQuality2160p50, bandwidths)
        | _ -> None

    let assignMissingFrameRate (stream: MediaManifestStream) =
        let defaultFrameRate = 25m
        if stream.FrameRate = 0m then
            match stream with
            | MediaManifestStream.Hls x -> MediaManifestStream.Hls { x with FrameRate = defaultFrameRate }
            | MediaManifestStream.Dash x -> MediaManifestStream.Dash { x with FrameRate = defaultFrameRate }
        else
            stream

    let getManifestVideoQualities (manifest: MediaManifest) =
        manifest.Streams
        |> List.filter (fun x -> x.Resolution <> (0, 0))
        |> List.map assignMissingFrameRate
        |> List.groupBy (fun x -> snd x.Resolution, x.FrameRate)
        |> List.map (fun (x, y) -> fst x, snd x, y |> List.map (_.Bandwidth))
        |> List.map (fun (x, y, z) -> tryGetVideoQuality x y z)
        |> List.choose id
        |> List.toArray

    let getVideoQualityBitRates (manifest: MediaManifest) =
        getManifestVideoQualities manifest |> Array.map VideoQualityBitRates

    let getVideoQualityStatusFromManifest content remoteStates (manifest: MediaManifest) =
        let completedQualities = getManifestVideoQualities manifest
        let incompleteQualities =
            content
            |> getTranscodedVideoQualities
            |> Array.filter (fun x -> not (completedQualities |> Array.map fst |> Array.contains x))
        Array.concat
            [|
                completedQualities
                |> Array.map (fun (quality, bitRates) -> VideoQualityStatusWithBitRates(quality, bitRates, DistributionState.Completed))
                incompleteQualities
                |> Array.map (fun quality ->
                    VideoQualityStatusWithBitRates(quality, Seq.empty, remoteStates |> Map.tryFind quality |> Option.defaultValue DistributionState.None))
            |]

    let getVideoQualityStatus content remoteStates =
        content
        |> getTranscodedVideoQualities
        |> Array.map (fun quality -> VideoQualityStatus(quality, remoteStates |> Map.tryFind quality |> Option.defaultValue DistributionState.None))

    type SideloadingSubtitles(subtitleUrl, languageCode, languageType, format, hasMultipleFiles) =
        member this.Url: string = subtitleUrl // e.g. "https://undertekst.nrk.no/KOIF21/00/KOIF21008420/NOR/KOIF21008420.vtt"
        member this.SubtitleLanguage: string = languageCode // "NO", "EN"
        member this.Type: string = languageType // "NOR", "TTV", "MIX"
        member this.Format: string = getSubtitlesFormat format
        member this.DefaultTrack: bool = isSubtitlesDefaultTrack languageType hasMultipleFiles

    let getSideloadingSubtitlesLinks (subtitlesLinks: SubtitlesLink list) =
        subtitlesLinks
        |> List.map (fun link ->
            SideloadingSubtitles(link.SourcePath.Value, link.LanguageCode.Value, link.Name.Value, link.Format, Seq.length subtitlesLinks > 1))
        |> List.toArray

    type VideoFileMetadata(contentFile: ContentFile) =
        let height, frameRate, audioFormat, dynamicRangeProfile, codec =
            let getAudioFormat mixdown =
                match mixdown with
                | Stereo -> "Stereo"
                | Surround -> "5.1"
            let getLegacyMediaProperties audioFormat =
                tryParseVideoQualityFromFilePath (contentFile.SourcePath |> Option.getOrFail "SourcePath not found" |> FilePath.value)
                |> Option.getOrFail "Incorrect format of filename"
                |> String.split 'p'
                |> fun items -> Seq.head items, Seq.last items
                |> fun (height, frameRate) -> height |> Int32.Parse, frameRate |> Decimal.Parse, audioFormat, "SDR", "unknown"
            match contentFile.MediaProperties with
            | MediaPropertiesV2 props ->
                let audioFormat = getAudioFormat props.Audio.Mixdown
                let codec =
                    if String.isNullOrEmpty props.Video.Codec then
                        "unknown"
                    else
                        props.Video.Codec
                props.Video.Height, props.Video.FrameRate, audioFormat, props.Video.DynamicRangeProfile, codec
            | MediaPropertiesV1 props ->
                let audioFormat = getAudioFormat props
                getLegacyMediaProperties audioFormat
            | Unspecified -> getLegacyMediaProperties "Stereo"

        member this.Height: int = height
        member this.FrameRate: decimal = frameRate
        member this.AudioFormat: string = audioFormat
        member this.DynamicRangeProfile: string = dynamicRangeProfile
        member this.Codec: string = codec

    let getVideoFileMetadata state =
        state.Current.GlobalConnect.Files
        |> Map.filter (fun _ file -> file.RemoteState.State = DistributionState.Completed)
        |> Map.map (fun ref _ -> ContentSet.tryGetFile ref state.Desired.Content)
        |> Map.values
        |> Seq.choose id
        |> Seq.map VideoFileMetadata
        |> Seq.toList

    let isGlobalConnectReady state =
        let hasFiles =
            state.Current.GlobalConnect.Files
            |> Map.filter (fun _ file -> file.RemoteState.State = DistributionState.Completed)
            |> Map.isNotEmpty
        let hasSmil = String.isNotNullOrEmpty state.Current.GlobalConnect.Smil.Smil.RemotePath.Value
        hasFiles && hasSmil

    type OnDemandVideoAsset(state: MediaSetState) =
        member this.ManifestPath: string = state.Current.GlobalConnect.Smil.Smil.RemotePath.Value
        member this.EncryptionLevel: string = EncryptionLevelNone
        member this.FileMetadata: VideoFileMetadata list = getVideoFileMetadata state

    type OnDemandAudioAsset(state: MediaSetState) =
        member this.ManifestPath: string = state.Current.GlobalConnect.Smil.Smil.RemotePath.Value
        member this.EncryptionLevel: string = EncryptionLevelNone
        member this.FileMetadata: AudioFileMetadata array =
            getAudioFileMetadata state.Desired.Content (state |> getGlobalConnectAudioQualityStates DistributionState.None)
            |> Array.map (fun x -> AudioFileMetadata(x.AudioBitrate))

    type VideoOnDemandUpdateEvent(msg: Events.PlayableOnDemandMediaSet, state: MediaSetState, psFilesState: PsFilesState) =
        member this.ProgramId: string = getProgramId msg.MediaSetId.Value
        member this.AvailabilityRegion: string = getAvailabilityRegion (Some state.Desired.GeoRestriction)
        member this.Duration: string = getDuration state psFilesState
        member this.DisplayAspectRatio: string = getDisplayAspectRatio ()
        member this.TranscodedVideoQualities: string array = getTranscodedVideoQualities state.Desired.Content
        member this.VideoAsset: OnDemandVideoAsset option =
            if isGlobalConnectReady state then
                Some(OnDemandVideoAsset(state))
            else
                None
        member this.Subtitles: SideloadingSubtitles array =
            let subtitlesLinks =
                match state.Desired.Content with
                | Empty -> List.Empty
                | NoParts contentChunk
                | Parts(_, contentChunk) -> contentChunk.SubtitlesLinks
            getSideloadingSubtitlesLinks subtitlesLinks

    type AudioOnDemandUpdateEvent(msg: Events.PlayableOnDemandMediaSet, state: MediaSetState, psFilesState: PsFilesState) =
        member this.ProgramId: string = getProgramId msg.MediaSetId.Value
        member this.AvailabilityRegion: string = getAvailabilityRegion (Some state.Desired.GeoRestriction)
        member this.Duration: string = getDuration state psFilesState
        member this.TranscodedAudioQualities: int array = getTranscodedAudioQualities state.Desired.Content
        member this.AudioAsset: OnDemandAudioAsset option =
            if isGlobalConnectReady state then
                Some(OnDemandAudioAsset(state))
            else
                None

    type AudioVideoDeleteEvent(msg: Events.NonPlayableMediaSet, isRevoked: bool) =
        member this.ProgramId: string = getProgramId msg.MediaSetId.Value
        member this.IsRevoked: bool = isRevoked

    type OriginDistributionStatus(origin, hasManifests) =
        member this.Origin: string = getUnionCaseName origin
        member this.HasManifests: bool = hasManifests

    type VideoOnDemandStatusUpdateEvent(mediaSetId, originManifestStatus) =
        member this.ProgramId: string = getProgramId mediaSetId
        member this.DistributionStatus: OriginDistributionStatus list =
            originManifestStatus |> Map.toList |> List.map OriginDistributionStatus

    type AudioOnDemandStatusUpdateEvent(mediaSetId, originManifestStatus) =
        member this.ProgramId: string = getProgramId mediaSetId
        member this.DistributionStatus: OriginDistributionStatus list =
            originManifestStatus |> Map.toList |> List.map OriginDistributionStatus

    [<RequireQualifiedAccess>]
    type ResultWithRetry<'T, 'TError> =
        | Ok of 'T
        | Retry of 'T
        | Error of 'TError

    module ResultWithRetry =
        let map (f: 'T -> 'U) result =
            match result with
            | ResultWithRetry.Ok x -> f x |> ResultWithRetry.Ok
            | ResultWithRetry.Retry x -> f x |> ResultWithRetry.Retry
            | ResultWithRetry.Error x -> x |> ResultWithRetry.Error

    type IPlaybackEventCreator =
        abstract member CreatePlayabilityEvent: MediaSetPlayabilityEventDto -> MediaSetState -> PsFilesState -> ResultWithRetry<ServiceBusMessage, unit>
        abstract member TryCreateOnDemandStatusEvent: MediaSetPlayabilityEventDto -> MediaSetState -> ServiceBusMessage option

    type PlaybackEventCreator(mailbox: Actor<_>, origins) =

        let serializeEvent value =
            let serializationOptions = JsonSerializerOptions(PropertyNamingPolicy = JsonNamingPolicy.CamelCase)
            JsonSerializer.SerializeToUtf8Bytes(value, serializationOptions)

        let serializeWithHeaders evt (sourceMedium: string option) streamingType messageType programScope priority =
            let payload = serializeEvent evt
            let headers =
                {
                    SourceMedium = Option.toObj sourceMedium
                    StreamingType = streamingType
                    MessageType = messageType
                    ProgramScope = programScope
                    Priority = getEventPriority priority
                }
            (payload, headers)

        let createServiceBusMessage evt eventSinkType mediaSetId sourceMedium streamingType messageType programScope priority =
            let payload, headers = serializeWithHeaders evt sourceMedium streamingType messageType programScope priority
            {
                EventSinkType = eventSinkType
                MediaSetId = mediaSetId
                Payload = payload
                Headers = headers
            }

        let verifyManifests manifests eventDescription originName =
            if Seq.isEmpty manifests then
                logWarning mailbox $"{eventDescription} has empty {originName} manifests"

        let verifyDuration (duration: string) eventDescription =
            if duration = "null" then
                logWarning mailbox $"{eventDescription} has null duration"
            if duration.StartsWith "24" then
                logWarning mailbox $"{eventDescription} has 24h duration"

        let createOnDemandAudioEvent (ms: Events.PlayableOnDemandMediaSet) state psFilesState =
            let evtDescription = "Playable onDemand audio event"
            let evt = AudioOnDemandUpdateEvent(ms, state, psFilesState)
            verifyDuration evt.Duration evtDescription
            ResultWithRetry.Ok evt

        let createOnDemandVideoEvent (ms: Events.PlayableOnDemandMediaSet) state psFilesState =
            let evtDescription = "Playable onDemand video event"
            let evt = VideoOnDemandUpdateEvent(ms, state, psFilesState)
            verifyDuration evt.Duration evtDescription
            ResultWithRetry.Ok evt

        let createPlayableOnDemandEvent (ms: Events.PlayableOnDemandMediaSet) mediaType state psFilesState =
            match mediaType with
            | MediaType.Audio ->
                createOnDemandAudioEvent ms state psFilesState
                |> ResultWithRetry.map (fun evt ->
                    createServiceBusMessage
                        evt
                        PlaybackEventSink
                        ms.MediaSetId.Value
                        (Some SourceMediumAudio)
                        StreamingTypeOnDemand
                        MessageTypeUpdate
                        null
                        ms.Priority)
            | MediaType.Video ->
                createOnDemandVideoEvent ms state psFilesState
                |> ResultWithRetry.map (fun evt ->
                    createServiceBusMessage
                        evt
                        PlaybackEventSink
                        ms.MediaSetId.Value
                        (Some SourceMediumVideo)
                        StreamingTypeOnDemand
                        MessageTypeUpdate
                        null
                        ms.Priority)

        let createNotPlayableEvent (ms: Events.NonPlayableMediaSet) state (psFilesState: PsFilesState) streamingType =
            let isActiveCarrierRevoked =
                psFilesState.ActiveCarriers
                |> Option.map (fun activeCarriers ->
                    fst activeCarriers
                    |> List.map _.CarrierId.ToUpper()
                    |> List.exists (fun carrierId -> state.Desired.RevokedParts |> List.map _.Value.ToUpper() |> List.contains carrierId))
                |> Option.defaultValue false

            createServiceBusMessage
                (AudioVideoDeleteEvent(ms, isActiveCarrierRevoked))
                PlaybackEventSink
                ms.MediaSetId.Value
                (getSourceMedium state.Desired.MediaType)
                streamingType
                MessageTypeDelete
                null
                ms.Priority
            |> ResultWithRetry.Ok

        let createPlayableOnDemandStatusEvent (ms: Events.PlayableOnDemandMediaSet) mediaType state origins =
            let originManifests =
                origins
                |> List.map (fun origin ->
                    match origin with
                    | Origin.GlobalConnect -> getGlobalConnectOnDemandStatus state |> Result.map Seq.isNotEmpty
                    |> function
                        | Result.Ok result -> (origin, result)
                        | Result.Error _ -> (origin, false))
                |> Map.ofList
            match mediaType with
            | MediaType.Audio ->
                let evt = AudioOnDemandStatusUpdateEvent(ms.MediaSetId.Value, originManifests)
                createServiceBusMessage
                    evt
                    DistributionStatusEventSink
                    ms.MediaSetId.Value
                    (Some SourceMediumAudio)
                    StreamingTypeOnDemand
                    MessageTypeUpdate
                    null
                    ms.Priority
            | MediaType.Video ->
                let evt = VideoOnDemandStatusUpdateEvent(ms.MediaSetId.Value, originManifests)
                createServiceBusMessage
                    evt
                    DistributionStatusEventSink
                    ms.MediaSetId.Value
                    (Some SourceMediumVideo)
                    StreamingTypeOnDemand
                    MessageTypeUpdate
                    null
                    ms.Priority

        let createNonPlayableOnDemandStatusEvent (ms: Events.NonPlayableMediaSet) mediaType origins =
            let originManifests = origins |> List.map (fun origin -> (origin, false)) |> Map.ofList
            match mediaType with
            | MediaType.Audio ->
                let evt = AudioOnDemandStatusUpdateEvent(ms.MediaSetId.Value, originManifests)
                createServiceBusMessage
                    evt
                    DistributionStatusEventSink
                    ms.MediaSetId.Value
                    (Some SourceMediumAudio)
                    StreamingTypeOnDemand
                    MessageTypeDelete
                    null
                    ms.Priority
            | MediaType.Video ->
                let evt = VideoOnDemandStatusUpdateEvent(ms.MediaSetId.Value, originManifests)
                createServiceBusMessage
                    evt
                    DistributionStatusEventSink
                    ms.MediaSetId.Value
                    (Some SourceMediumVideo)
                    StreamingTypeOnDemand
                    MessageTypeDelete
                    null
                    ms.Priority

        interface IPlaybackEventCreator with
            member _.CreatePlayabilityEvent message state psFilesState =
                let evt = message |> MediaSetPlayabilityEventDto.toDomain
                match evt with
                | Events.MediaSetPlayableOnDemand ms -> createPlayableOnDemandEvent ms state.Desired.MediaType state psFilesState
                | Events.MediaSetNotPlayableOnDemand ms -> createNotPlayableEvent ms state psFilesState StreamingTypeOnDemand
            member _.TryCreateOnDemandStatusEvent message state =
                let evt = message |> MediaSetPlayabilityEventDto.toDomain
                match evt with
                | Events.MediaSetPlayableOnDemand ms -> createPlayableOnDemandStatusEvent ms state.Desired.MediaType state origins |> Some
                | Events.MediaSetNotPlayableOnDemand ms -> createNonPlayableOnDemandStatusEvent ms state.Desired.MediaType origins |> Some
