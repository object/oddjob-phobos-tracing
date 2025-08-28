namespace Nrk.Oddjob.Ps

open Nrk.Oddjob.Ps.Utils

module PsJobCreation =

    open System
    open System.IO
    open System.Runtime.InteropServices
    open FSharpx.Collections

    open Nrk.Oddjob.Core
    open PsTypes

    type UploadJobFilesDetails =
        {
            RequestSource: string
            ForwardedFrom: string option
            Priority: int
            PiProgId: PiProgId
            CarrierId: string
            MediaType: MediaType
            Geolocation: string
            PartNumber: int
            FilePath: string
            FileName: FileName
            BitRate: int
            Transcoding: MediaProperties
        }

    type ClearMediaSetJobDetails =
        {
            RequestSource: string
            ForwardedFrom: string option
            Priority: int
            PiProgId: PiProgId
        }

    type PartsStatusJobDetails =
        {
            RequestSource: string
            ForwardedFrom: string option
            Priority: int
            PiProgId: PiProgId
            Carriers: PsProgramCarrier list
        }

    [<NoComparison>]
    type JobProgramDetails =
        {
            RequestSource: string
            ForwardedFrom: string option
            Priority: int
            PiProgId: PiProgId
            ProgramSubtitles: PsSubtitles list
        }

    module Helpers =
        [<Literal>]
        let private RequestSourcePrf = "prf"

        let normalizeId (s: string) = s.Replace("-", "").ToLower()

        let toContentId piProgId =
            ContentId.create <| normalizeId (PiProgId.value piProgId)

        let prfRequestSource = RequestSourcePrf

        let createMixdown filesetParts =
            let calcMixdown audioDefinitions =
                if audioDefinitions |> List.exists ((=) Mixdown.Surround) then
                    Mixdown.Surround
                else
                    Mixdown.Stereo
            filesetParts |> List.map _.AudioDefinition |> calcMixdown

    let createMediaPropertiesForTranscodedFiles (filesState: PsFilesState) carrierId bitRate filesetParts =
        filesState.FileSets
        |> Map.tryFind (CarrierId.create carrierId)
        |> Option.bind (fun fileSet ->
            match fileSet.TranscodedFiles with
            | PsTranscodedFiles.Video files -> files |> List.tryFind (fun file -> file.BitRate = bitRate) |> Option.map (fun file -> (file, fileSet.Version))
            | _ -> None)
        |> Option.map (fun (file, version) ->
            {
                MediaPropertiesV2.BitRate = file.BitRate
                Duration = file.Duration
                Video =
                    {
                        Codec = file.Video.Codec
                        DynamicRangeProfile = file.Video.DynamicRangeProfile
                        DisplayAspectRatio = file.Video.DisplayAspectRatio
                        Width = file.Video.Width
                        Height = file.Video.Height
                        FrameRate = file.Video.FrameRate
                    }
                Audio =
                    {
                        Mixdown = Mixdown.parse file.Audio.Mixdown
                    }
                TranscodingVersion = version
            })
        |> Option.map MediaPropertiesV2
        |> Option.defaultValue (MediaPropertiesV1(Helpers.createMixdown filesetParts))

    let getMediaType transcodedFiles =
        match transcodedFiles with
        | PsTranscodedFiles.Video _
        | PsTranscodedFiles.LegacyVideo _ -> MediaType.Video
        | PsTranscodedFiles.Audio _
        | PsTranscodedFiles.LegacyAudio _ -> MediaType.Audio

    let getFileBitRate fileName transcodedFiles =
        let hasMatch fileName filePath =
            String.Equals(IO.getFileName filePath, fileName, StringComparison.InvariantCultureIgnoreCase)
        match transcodedFiles with
        | PsTranscodedFiles.Video files -> files |> Seq.tryFind (fun file -> hasMatch fileName file.FilePath) |> Option.map _.BitRate
        | PsTranscodedFiles.LegacyVideo files -> files |> Seq.tryFind (fun file -> hasMatch fileName file.FilePath) |> Option.map _.BitRate
        | PsTranscodedFiles.Audio files -> files |> Seq.tryFind (fun file -> hasMatch fileName file.FilePath) |> Option.map _.BitRate
        | PsTranscodedFiles.LegacyAudio files -> files |> Seq.tryFind (fun file -> hasMatch fileName file.FilePath) |> Option.map _.BitRate
        |> Option.defaultWith (fun () -> failwith $"File {fileName} is not found in file sets")

    let createTranscodedFilesDetails
        (filesState: PsFilesState)
        (rights: Rights.RightsUploadDetails)
        requestSource
        forwardedFrom
        priority
        piProgId
        logger
        : UploadJobFilesDetails list =
        logger $"Found {Seq.length filesState.FileSets} transcoded file sets"
        filesState.FileSets
        |> Map.toList
        |> List.collect (fun (carrierId, fileSet) ->

            match fileSet.TranscodedFiles with
            | PsTranscodedFiles.Video psTranscodedVideoFiles ->
                psTranscodedVideoFiles
                |> List.map (fun file ->

                    let mediaProperties =
                        {
                            MediaPropertiesV2.BitRate = file.BitRate
                            Duration = file.Duration
                            Video =
                                {
                                    Codec = file.Video.Codec
                                    DynamicRangeProfile = file.Video.DynamicRangeProfile
                                    DisplayAspectRatio = file.Video.DisplayAspectRatio
                                    Width = file.Video.Width
                                    Height = file.Video.Height
                                    FrameRate = file.Video.FrameRate
                                }
                            Audio =
                                {
                                    Mixdown = Mixdown.parse file.Audio.Mixdown
                                }
                            TranscodingVersion = fileSet.Version
                        }

                    {
                        UploadJobFilesDetails.RequestSource = requestSource
                        ForwardedFrom = forwardedFrom
                        Priority = int priority
                        PiProgId = piProgId
                        CarrierId = CarrierId.value carrierId
                        Geolocation = Granitt.Types.GranittGeorestriction.toGeolocationsString rights.Geolocation
                        MediaType = MediaType.Video
                        PartNumber = 1
                        FilePath = file.FilePath
                        FileName = createFileName (IO.getFileName file.FilePath) file.BitRate
                        BitRate = file.BitRate
                        Transcoding = MediaPropertiesV2 mediaProperties
                    })
            | PsTranscodedFiles.LegacyVideo psLegacyVideoFiles ->
                psLegacyVideoFiles
                |> List.map (fun file ->
                    {
                        UploadJobFilesDetails.RequestSource = requestSource
                        ForwardedFrom = forwardedFrom
                        Priority = int priority
                        PiProgId = piProgId
                        CarrierId = CarrierId.value carrierId
                        Geolocation = Granitt.Types.GranittGeorestriction.toGeolocationsString rights.Geolocation
                        MediaType = MediaType.Video
                        PartNumber = 1
                        FilePath = file.FilePath
                        FileName = createFileName (IO.getFileName file.FilePath) file.BitRate
                        BitRate = file.BitRate
                        Transcoding = MediaPropertiesV1(Mixdown.parse file.Mixdown)
                    })
            | PsTranscodedFiles.Audio psTranscodedAudioFiles ->
                psTranscodedAudioFiles
                |> List.map (fun file ->
                    {
                        UploadJobFilesDetails.RequestSource = requestSource
                        ForwardedFrom = forwardedFrom
                        Priority = int priority
                        PiProgId = piProgId
                        CarrierId = CarrierId.value carrierId
                        Geolocation = Granitt.Types.GranittGeorestriction.toGeolocationsString rights.Geolocation
                        MediaType = MediaType.Audio
                        PartNumber = 1
                        FilePath = file.FilePath
                        FileName = createFileName (IO.getFileName file.FilePath) file.BitRate
                        BitRate = file.BitRate
                        Transcoding = MediaPropertiesV1 MediaPropertiesV1.Stereo //No Radio files in granit with other than stereo
                    })
            | PsTranscodedFiles.LegacyAudio psLegacyAudioFiles ->
                psLegacyAudioFiles
                |> List.map (fun file ->
                    {
                        UploadJobFilesDetails.RequestSource = requestSource
                        ForwardedFrom = forwardedFrom
                        Priority = int priority
                        PiProgId = piProgId
                        CarrierId = CarrierId.value carrierId
                        Geolocation = Granitt.Types.GranittGeorestriction.toGeolocationsString rights.Geolocation
                        MediaType = MediaType.Audio
                        PartNumber = 1
                        FilePath = file.FilePath
                        FileName = createFileName (IO.getFileName file.FilePath) file.BitRate
                        BitRate = file.BitRate
                        Transcoding = MediaPropertiesV1 MediaPropertiesV1.Stereo //No Radio files in granit with other than stereo
                    }))

    let private createTranscodedSubtitlesFiles (subtitlesConfig: PsSubtitlesConfig) (filesState: PsFilesState) transcodingVersion : SubtitlesFile list option =

        let getSourcePath subtitlesConfig (url: string) =
            if RuntimeInformation.IsOSPlatform(OSPlatform.Windows) then
                Path.Combine(subtitlesConfig.SourceRootWindows, url.Replace("/", "\\"))
            else
                Path.Combine(subtitlesConfig.SourceRootLinux, url)

        maybe {
            let! subtitleSet = filesState.TryGetSubtitlesSet transcodingVersion
            return
                match subtitleSet with
                | SubtitlesSet subtitleSet ->
                    match subtitleSet.Subtitles |> List.tryHead with
                    | Some sub ->
                        match sub with
                        | PsTvSubtitleFileDetails.V1 _ ->
                            let kinds = Map [ "over", "nor"; "tdhh", "ttv" ]
                            subtitleSet.Subtitles
                            |> List.choose (function
                                | PsTvSubtitleFileDetails.V1 sub -> Some sub
                                | _ -> None)
                            |> List.filter (fun sub -> kinds |> Map.containsKey (String.toLower sub.Function))
                            |> Seq.map (fun sub ->
                                let code = sub.LanguageCode
                                let kind = kinds[String.toLower sub.Function]
                                let sourcePath = sub.FilePath |> String.toLower
                                let fileName = $"{Path.GetFileNameWithoutExtension sourcePath}-{code}-{kind}{Path.GetExtension sourcePath}"
                                SubtitlesFile.V1
                                    {
                                        LanguageCode = Alphanumeric code
                                        Name = Alphanumeric kind
                                        FileName = fileName |> FileName
                                        SourcePath = SubtitlesLocation.fromFilePath sub.FilePath
                                        Version = subtitleSet.Version
                                    })
                            |> Seq.toList
                        | PsTvSubtitleFileDetails.V2 _ ->
                            let filteredSubs =
                                subtitleSet.Subtitles
                                |> List.choose (function
                                    | PsTvSubtitleFileDetails.V2 sub -> Some sub
                                    | _ -> None)
                                |> List.filter (fun s -> List.exists (fun sc -> List.contains s.LanguageCode (fst sc)) SubtitlesRules.V2.subtitlesConfig) //remove unsupported languages

                            SubtitlesRules.V2.subtitlesConfig
                            |> List.choose (fun (languages, rule) -> //tryFind corresponding subtitle track for each rule
                                filteredSubs
                                |> List.tryFind (fun s -> List.contains s.LanguageCode languages && s.Name = rule.Name)
                                |> Option.map (fun sub -> sub, rule))
                            |> List.groupBy (fun (sub, rule) -> rule.LanguageCode, (sub.Roles |> List.sort))
                            |> List.map (snd >> List.head >> fst) //Group by languagecode and roles and take first to only get unique rolesets per language.
                            |> List.map (fun sub ->
                                let code = sub.LanguageCode
                                let name = sub.Name
                                let sourcePath = sub.FilePath |> String.toLower
                                let fileName = $"{Path.GetFileNameWithoutExtension sourcePath}-{code}-{name}{Path.GetExtension sourcePath}"
                                SubtitlesFile.V2
                                    {
                                        SubtitlesFileV2.LanguageCode = code
                                        Name = name
                                        Roles = sub.Roles
                                        FileName = fileName |> FileName
                                        SourcePath = SubtitlesLocation.fromFilePath sub.FilePath
                                        Version = subtitleSet.Version
                                    })
                    | None -> List.empty


                | LegacySubtitlesSet legacySubtitles ->
                    legacySubtitles
                    |> List.map (fun sub ->
                        let code = sub.LanguageCode
                        let kind = sub.LanguageType
                        let sourcePath = getSourcePath subtitlesConfig sub.FilePath
                        let fileName = $"{Path.GetFileNameWithoutExtension sourcePath}-{code}-{kind}{Path.GetExtension sourcePath}"
                        SubtitlesFile.V1
                            {
                                LanguageCode = Alphanumeric code
                                Name = Alphanumeric kind
                                FileName = fileName |> FileName
                                SourcePath = SubtitlesLocation.fromFilePath sourcePath
                                Version = 0
                            })
        }

    let private createSideloadingSubtitlesLinks (subtitlesConfig: PsSubtitlesConfig) (filesState: PsFilesState) transcodingVersion =
        transcodingVersion
        |> filesState.TryGetSubtitlesSet
        |> Option.map (fun subtitleSet ->
            match subtitleSet with
            | SubtitlesSet subtitleSet ->
                match subtitleSet.Subtitles |> List.tryHead with
                | None -> List.empty
                | Some sub ->
                    match sub with
                    | PsTvSubtitleFileDetails.V1 _ ->
                        let kinds = Map [ "over", "nor"; "tdhh", "ttv" ]
                        let codes = Map [ "nb", "no"; "nn", "no"; "no", "no"; "smi", "no" ]
                        subtitleSet.Subtitles
                        |> List.choose (function
                            | PsTvSubtitleFileDetails.V1 sub -> Some sub
                            | _ -> None)
                        |> Seq.filter (fun sub -> kinds |> Map.containsKey (String.toLower sub.Function))
                        |> Seq.filter (fun sub -> codes |> Map.containsKey (String.toLower sub.LanguageCode))
                        |> Seq.map (fun sub ->
                            let code = codes[String.toLower sub.LanguageCode] |> Alphanumeric
                            let kind = kinds[String.toLower sub.Function] |> Alphanumeric
                            {
                                SubtitlesLink.Format = SubtitlesLinkFormat.WebVTT
                                Name = kind
                                LanguageCode = code
                                SourcePath = sub.SubtitlesLinks |> AbsoluteUrl
                                Version = sub.Version
                            })
                        |> Seq.toList
                    | PsTvSubtitleFileDetails.V2 _ ->
                        let sideloadingRules =
                            [
                                (([ "no"; "nb"; "nn" ], "non-sdh-translated"), "nor")
                                (([ "no"; "nb"; "nn" ], "sdh"), "ttv")
                            ]

                        let filteredSubs =
                            subtitleSet.Subtitles
                            |> List.choose (function
                                | PsTvSubtitleFileDetails.V2 sub -> Some sub
                                | _ -> None)
                            |> List.filter (fun s -> List.exists (fun sc -> List.contains s.LanguageCode (fst (fst sc))) sideloadingRules)

                        sideloadingRules
                        |> List.choose (fun ((languages, name), sideloadFunction) -> //tryFind corresponding subtitle track for each rule
                            filteredSubs
                            |> List.tryFind (fun s -> List.contains s.LanguageCode languages && s.Name = name)
                            |> Option.map (fun sub -> sub, sideloadFunction))
                        |> List.groupBy (fun (sub, _) -> (sub.Roles |> List.sort))
                        |> List.map (snd >> List.head) //Group by roles and take first to only get unique rolesets per language.
                        |> List.map (fun (sub, sideloadFunction) ->
                            let code = sub.LanguageCode
                            let name = sideloadFunction
                            {
                                SubtitlesLink.Format = SubtitlesLinkFormat.WebVTT
                                Name = name |> Alphanumeric
                                LanguageCode = code |> Alphanumeric
                                SourcePath = sub.SubtitlesLinks |> AbsoluteUrl
                                Version = sub.Version
                            })

            | LegacySubtitlesSet subtitles ->
                subtitles
                |> List.map (fun sub ->
                    let kind = sub.LanguageType |> Alphanumeric
                    let code = sub.LanguageCode |> Alphanumeric
                    let sideloadingUri = Uri(subtitlesConfig.BaseUrl, sub.FilePath).AbsoluteUri |> AbsoluteUrl
                    {
                        SubtitlesLink.Format = SubtitlesLinkFormat.WebVTT
                        Name = kind
                        LanguageCode = code
                        SourcePath = sideloadingUri
                        Version = 0
                    }))

    type PsJobCreator(forceOperationForSources: string list, subtitlesConfig: PsSubtitlesConfig, logger: string -> unit) =

        let createJobHeader requestSource forwardedFrom priority =
            {
                RequestSource = requestSource
                ForwardedFrom = forwardedFrom
                Priority = priority
                Timestamp = DateTimeOffset.Now
                OverwriteMode =
                    if List.contains requestSource forceOperationForSources then
                        OverwriteMode.Always
                    else
                        OverwriteMode.IfNewer
            }

        member _.CreatePublishMediaSetJob filesState archiveFileLocator rights requestedSource forwardedFrom priority piProgId =
            if filesState.FileSets |> Seq.isEmpty then
                invalidOp "filesSets must have non-empty content"
            let filesDetails =
                createTranscodedFilesDetails filesState rights requestedSource forwardedFrom (int priority) piProgId logger

            let commonData = List.head filesDetails
            let geolocation = commonData.Geolocation // TODO - this is taken from Geolocations table and should be strongly typed
            let geoRestriction =
                Granitt.Types.GranittGeorestriction.tryCreate geolocation
                |> Option.map (fun geo ->
                    match geo with
                    | Granitt.Types.GranittGeorestriction.World -> GeoRestriction.World
                    | Granitt.Types.GranittGeorestriction.Norway -> GeoRestriction.Norway)
                |> Option.defaultValue GeoRestriction.Norway
            let partFiles =
                filesDetails
                |> List.map (fun details ->
                    let partId = details.CarrierId |> PartId.create
                    let contentFile =
                        {
                            QualityId = QualityId.create details.BitRate
                            FileName = details.FileName
                            SourcePath = details.FilePath |> archiveFileLocator |> FilePath.tryCreate
                            MediaProperties = details.Transcoding
                        }
                    ((partId, details.PartNumber), contentFile))
                |> List.groupBy fst
                |> Map.ofList
                |> Map.map (fun _ v -> v |> List.map snd)

            let transcodingVersion =
                match commonData.Transcoding with
                | MediaPropertiesV2 props -> Some props.TranscodingVersion
                | Unspecified
                | MediaPropertiesV1 _ -> None

            let subtitlesFiles =
                transcodingVersion |> createTranscodedSubtitlesFiles subtitlesConfig filesState |> Option.defaultValue List.empty

            let sideLoadingSubtitlesLinks =
                transcodingVersion
                |> createSideloadingSubtitlesLinks subtitlesConfig filesState
                |> Option.defaultValue List.empty
                |> List.filter (fun x -> not (x.SourcePath.IsEmpty()))

            let contentParts =
                partFiles
                |> Map.keys
                |> Seq.toList
                |> List.map (fun part ->
                    let contentPart =
                        {
                            PartNumber = snd part
                            Content =
                                {
                                    Files = partFiles |> Map.tryFind part |> Option.defaultValue List.empty
                                    Subtitles = subtitlesFiles
                                    SubtitlesLinks = sideLoadingSubtitlesLinks
                                }
                        }
                    (fst part, contentPart))
                |> Map.ofList
            let content =
                if filesState.FileSets |> Seq.isSingleton then
                    ContentSet.Parts(
                        filesState.FileSets
                        |> Map.values
                        |> Seq.head
                        |> (fun chunk -> chunk.Carriers |> List.map (fun x -> PartId.create x.CarrierId)),
                        contentParts |> Map.values |> Seq.head |> (_.Content)
                    )
                else
                    ContentSet.Parts(contentParts |> Map.keys |> Seq.head |> List.singleton, contentParts |> Map.values |> Seq.head |> (_.Content))
            {
                PublishMediaSetJob.Header = createJobHeader commonData.RequestSource commonData.ForwardedFrom commonData.Priority
                MediaSetId =
                    {
                        ClientId = Alphanumeric PsClientId
                        ContentId = Helpers.toContentId commonData.PiProgId
                    }
                GeoRestriction = geoRestriction
                Content = content
                MediaType = commonData.MediaType
            }
            |> MediaSetJob.PublishMediaSet

        member _.CreateClearMediaSetJob(data: ClearMediaSetJobDetails) =
            {
                ClearMediaSetJob.Header = createJobHeader data.RequestSource data.ForwardedFrom data.Priority
                MediaSetId =
                    {
                        ClientId = Alphanumeric PsClientId
                        ContentId = Helpers.toContentId data.PiProgId
                    }
            }
            |> MediaSetJob.ClearMediaSet

        member _.CreateActivatePartsJob(data: PartsStatusJobDetails) =
            {
                ActivatePartsJob.Header = createJobHeader data.RequestSource data.ForwardedFrom data.Priority
                MediaSetId =
                    {
                        ClientId = Alphanumeric PsClientId
                        ContentId = Helpers.toContentId data.PiProgId
                    }
                Parts = data.Carriers |> List.map (fun c -> PartId.create c.CarrierId, c.PartNumber)
            }
            |> MediaSetJob.ActivateParts

        member _.CreateDeactivatePartsJob(data: PartsStatusJobDetails) =
            {
                DeactivatePartsJob.Header = createJobHeader data.RequestSource data.ForwardedFrom data.Priority
                MediaSetId =
                    {
                        ClientId = Alphanumeric PsClientId
                        ContentId = Helpers.toContentId data.PiProgId
                    }
                Parts = data.Carriers |> List.map (fun c -> PartId.create c.CarrierId)
            }
            |> MediaSetJob.DeactivateParts
