namespace Nrk.Oddjob.Core.Manifests

[<RequireQualifiedAccess>]
type HlsVideoRange =
    | SDR
    | HDR

type HlsManifestStream =
    {
        Bandwidth: int
        AverageBandwidth: int
        Resolution: int * int
        FrameRate: decimal
        VideoRange: HlsVideoRange
        Codecs: string
        Subtitles: string
        Uri: string
    }

[<RequireQualifiedAccess>]
type HlsMediaType =
    | Audio
    | Video
    | ClosedCaptions
    | Subtitles

type HlsManifestMedia =
    {
        Type: HlsMediaType
        Uri: string
        GroupId: string
        Language: string
        AssocLanguage: string
        Name: string
        Default: bool
        AutoSelect: bool
        Forced: bool
        InStreamId: string
        Characteristics: string
        Channels: string
    }

[<RequireQualifiedAccess>]
type HlsManifestSection =
    | Stream of HlsManifestStream
    | Media of HlsManifestMedia

type HlsManifest =
    {
        Version: int
        Streams: HlsManifestStream list
        Media: HlsManifestMedia list
    }

    static member Zero =
        {
            Version = 0
            Streams = []
            Media = []
        }

type HlsManifestStreamHeader =
    {
        PlaylistType: string
        TargetDuration: int
        MediaSequence: int
    }

    static member Zero =
        {
            PlaylistType = ""
            TargetDuration = 0
            MediaSequence = 0
        }

module HlsManifest =

    open System
    open System.Globalization
    open Nrk.Oddjob.Core

    [<Literal>]
    let DefaultHlsManifestVersion = 3

    let parse (text: string) =

        let parseStringItem name items =
            items |> List.tryFind (fun (k, _) -> k = name) |> Option.map snd |> Option.defaultValue null

        let parseIntItem name items =
            items |> List.tryFind (fun (k, _) -> k = name) |> Option.map snd |> Option.map Int32.Parse |> Option.defaultValue 0

        let parseDecimalItem name items =
            items
            |> List.tryFind (fun (k, _) -> k = name)
            |> Option.map snd
            |> Option.map (fun (x: string) -> Decimal.Parse(x, CultureInfo.InvariantCulture))
            |> Option.defaultValue 0m

        let parseBooleanItem name items =
            items
            |> List.tryFind (fun (k, _) -> k = name)
            |> Option.map snd
            |> Option.map (fun x -> x = "YES")
            |> Option.defaultValue false

        let lines = text.Split([| "\r\n"; "\r"; "\n" |], StringSplitOptions.RemoveEmptyEntries) |> List.ofArray |> List.map _.Trim()
        if (List.head lines) <> "#EXTM3U" then
            invalidOp "Invalid manifest file"

        let parseStreamSection (text: string) =
            let items = parseKeyValueString text ',' '\"' []
            // Example of a bandwidth spec: BANDWIDTH=328000
            let bandwidth = items |> parseIntItem "BANDWIDTH"
            let avgBandwidth = items |> parseIntItem "AVERAGE-BANDWIDTH"
            // Example of a resolution spec: RESOLUTION=480x270
            let resolution =
                items
                |> List.tryFind (fun (k, _) -> k = "RESOLUTION")
                |> Option.map (fun (_, v) ->
                    let v = v.Split('x')
                    Int32.Parse(v[0]), Int32.Parse(v[1]))
                |> Option.defaultValue (0, 0)
            // Example of a frame rate spec: FRAME-RATE=25.000
            let frameRate = items |> parseDecimalItem "FRAME-RATE"
            let videoRange =
                items
                |> parseStringItem "VIDEO-RANGE"
                |> function
                    | "HDR"
                    | "PQ"
                    | "HLG" -> HlsVideoRange.HDR
                    | _ -> HlsVideoRange.SDR
            // Example of a codecs spec: CODECS="avc1.42E015,mp4a.40.2"
            let codecs = items |> parseStringItem "CODECS"
            // Example of a subtitles spec: SUBTITLES="100wvtt.vtt"
            let subtitles = items |> parseStringItem "SUBTITLES"
            {
                Bandwidth = bandwidth
                AverageBandwidth = avgBandwidth
                Resolution = resolution
                FrameRate = frameRate
                VideoRange = videoRange
                Codecs = codecs
                Subtitles = subtitles
                Uri = String.Empty
            }

        let parseMediaSection (text: string) =
            let items = parseKeyValueString text ',' '\"' []
            // Example of a type spec: TYPE=SUBTITLES
            let mediaType =
                match items |> parseStringItem "TYPE" with
                | "AUDIO" -> HlsMediaType.Audio
                | "VIDEO" -> HlsMediaType.Video
                | "SUBTITLES" -> HlsMediaType.Subtitles
                | "CLOSED-CAPTIONS" -> HlsMediaType.ClosedCaptions
                | _ -> failwithf "Must specify valid media type in [%s]" text
            {
                Type = mediaType
                Uri = items |> parseStringItem "URI"
                GroupId = items |> parseStringItem "GROUP-ID"
                Language = items |> parseStringItem "LANGUAGE"
                AssocLanguage = items |> parseStringItem "ASSOC_LANGUAGE"
                Name = items |> parseStringItem "NAME"
                Default = items |> parseBooleanItem "DEFAULT"
                AutoSelect = items |> parseBooleanItem "AUTOSELECT"
                Forced = items |> parseBooleanItem "FORCED"
                InStreamId = items |> parseStringItem "INSTREAM_ID"
                Characteristics = items |> parseStringItem "CHARACTERISTICS"
                Channels = items |> parseStringItem "CHANNELS"
            }

        let extractVersion (lines: string list) =
            let versionMarker = "#EXT-X-VERSION:"
            lines
            |> List.tryFind (_.StartsWith(versionMarker))
            |> Option.map (_.Substring(versionMarker.Length).Trim())
            |> Option.bind (fun x -> Int32.TryParse(x) |> fun (ok, result) -> if ok then Some result else None)
            |> Option.defaultValue DefaultHlsManifestVersion

        let extractSections (lines: string list) : HlsManifestSection list =
            let streamsMarker = "#EXT-X-STREAM-INF:"
            let mediaMarker = "#EXT-X-MEDIA:"
            lines
            |> List.fold
                (fun (acc, stream) line ->
                    match line with
                    | x when x.StartsWith(streamsMarker) ->
                        // Example of a video file spec: #EXT-X-STREAM-INF:BANDWIDTH=328000,RESOLUTION=480x270,CODECS="avc1.42E015,mp4a.40.2"
                        // If a media set has subtitles, then it will have an extra item like this: SUBTITLES="100wvtt.vtt"
                        // Example of an audio file spec: #EXT-X-STREAM-INF:BANDWIDTH=64124,AVERAGE-BANDWIDTH=64124,CODECS="mp4a.40.2"
                        let stream = parseStreamSection <| x.Substring(streamsMarker.Length)
                        acc, Some stream
                    | x when x.StartsWith(mediaMarker) ->
                        // Example of a video file spec: #EXT-X-STREAM-INF:BANDWIDTH=328000,RESOLUTION=480x270,CODECS="avc1.42E015,mp4a.40.2"
                        // If a media set has subtitles, then it will have an extra item like this: SUBTITLES="100wvtt.vtt"
                        // Example of an audio file spec: #EXT-X-STREAM-INF:BANDWIDTH=64124,AVERAGE-BANDWIDTH=64124,CODECS="mp4a.40.2"
                        let media = parseMediaSection <| x.Substring(mediaMarker.Length)
                        (HlsManifestSection.Media media) :: acc, None
                    | x ->
                        match stream with
                        | Some stream -> (HlsManifestSection.Stream { stream with Uri = x }) :: acc, None
                        | None -> acc, None)
                ([], None)
            |> fst
            |> List.rev

        let version = extractVersion lines
        let sections = extractSections lines
        let streams =
            sections
            |> List.choose (fun x ->
                match x with
                | HlsManifestSection.Stream x -> Some x
                | _ -> None)
        let media =
            sections
            |> List.choose (fun x ->
                match x with
                | HlsManifestSection.Media x -> Some x
                | _ -> None)
        {
            Version = version
            Streams = streams
            Media = media
        }

    let parseStreamHeader (text: string) =

        let extractPlaylistType (lines: string list) =
            let marker = "#EXT-X-PLAYLIST-TYPE:"
            lines |> List.tryFind (_.StartsWith(marker)) |> Option.map (_.Substring(marker.Length).Trim()) |> Option.defaultValue ""

        let extractTargetDuration (lines: string list) =
            let marker = "#EXT-X-TARGETDURATION:"
            lines
            |> List.tryFind (_.StartsWith(marker))
            |> Option.map (_.Substring(marker.Length).Trim())
            |> Option.bind (fun x -> Int32.TryParse(x) |> fun (ok, result) -> if ok then Some result else None)
            |> Option.defaultValue 0

        let extractmediaSequence (lines: string list) =
            let marker = "#EXT-X-MEDIA-SEQUENCE:"
            lines
            |> List.tryFind (_.StartsWith(marker))
            |> Option.map (_.Substring(marker.Length).Trim())
            |> Option.bind (fun x -> Int32.TryParse(x) |> fun (ok, result) -> if ok then Some result else None)
            |> Option.defaultValue 0

        let lines = text.Split([| "\r\n"; "\r"; "\n" |], StringSplitOptions.RemoveEmptyEntries) |> List.ofArray |> List.map _.Trim()
        if (List.head lines) <> "#EXTM3U" then
            invalidOp "Invalid manifest file"

        let playlistType = extractPlaylistType lines
        let targetDuration = extractTargetDuration lines
        let mediaSequence = extractmediaSequence lines
        {
            PlaylistType = playlistType
            TargetDuration = targetDuration
            MediaSequence = mediaSequence
        }
