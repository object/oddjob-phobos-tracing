namespace Nrk.Oddjob.WebApi

module PsUtils =

    open System
    open System.IO
    open System.Net
    open Akka.Event
    open Akkling
    open FsHttp

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Core.Granitt.Types
    open Nrk.Oddjob.Ps
    open Nrk.Oddjob.Ps.PsTypes
    open Nrk.Oddjob.Ps.Utils

    open PsGranitt.MySql
    open HealthCheckTypes
    open EventJournalUtils

    let getArchiveRoots (oddjobConfig: OddjobConfig) =
        if String.isNotNullOrEmpty oddjobConfig.Ps.MediaFiles.AlternativeArchiveRoot then
            [
                oddjobConfig.Ps.MediaFiles.AlternativeArchiveRoot
                oddjobConfig.Ps.MediaFiles.ArchiveRoot
            ]
        else
            [ oddjobConfig.Ps.MediaFiles.ArchiveRoot ]

    let getArchiveComponentSpec name checkInterval (oddjobConfig: OddjobConfig) isEnabled =
        let getArchiveRootsHealth (log: ILoggingAdapter) =
            let mappedSourceRoots = getArchiveRoots oddjobConfig |> FilePath.mapSourceRoots oddjobConfig.PathMappings
            if mappedSourceRoots |> Seq.forall Directory.Exists then
                HealthCheckResult.Ok("", [])
            else
                log.Error $"Error checking archive health ${mappedSourceRoots}"
                HealthCheckResult.Error(name, "Archive media root is not accessible", [])

        let getRadioArchiveHealth (log: ILoggingAdapter) =
            try
                let url = "https://lydteam-manasproxy.nrk.no/radiolager-trans/verify.txt"
                let response = http { GET url } |> Request.send
                if response.statusCode = HttpStatusCode.OK then
                    HealthCheckResult.Ok("", [])
                else
                    log.Error $"Error checking radio health (error={response.statusCode}, {response.reasonPhrase})"
                    HealthCheckResult.Error(name, response.reasonPhrase, [])
            with exn ->
                log.Error(exn, "Error checking radio health")
                HealthCheckResult.Error(name, exn.Message, [])

        let rootsName = "SourceRoots"
        let radioName = "RadioArchive"

        ComponentSpec.WithComponents(
            name,
            [
                if isEnabled rootsName then
                    yield ComponentSpec.WithChecker(rootsName, getArchiveRootsHealth, checkInterval rootsName, None)
                if isEnabled radioName then
                    yield ComponentSpec.WithChecker(radioName, getRadioArchiveHealth, checkInterval radioName, None)
            ],
            Strategies.singleFailureFailsAll name
        )

    let getGranittContext granittConnectionString (log: ILoggingAdapter) =
        let ctx =
            {
                MySqlContext.Connection = new MySqlConnector.MySqlConnection(granittConnectionString)
                Log = log
            }
        new Disposable<_>(ctx, ctx.Connection.Dispose)

    let getGranittHealth granittConnectionString (log: ILoggingAdapter) =
        use ctx = getGranittContext granittConnectionString log
        tryGetGeolocationId ctx.Value "world"
        |> function
            | Some _ -> HealthCheckResult.Ok("", [])
            | None ->
                log.Error "Error checking Granitt health"
                HealthCheckResult.Error("Granitt", "Geolocations are empty", [])

    let getRightsInfo usageRightsConnectionString (filesState: PsFilesState) piProgId =
        if filesState.Rights |> Seq.isNotEmpty then
            filesState.Rights
            |> List.map<_, RightsInfo> (fun x ->
                {
                    Geolocation = x.Region
                    PublishStart = x.PublishStart
                    PublishEnd = x.PublishEnd
                })
            |> List.toArray
            |> Result.Ok
        else
            let usageRightsConnection = usageRightsConnectionString |> parseConnectionString
            let baseUrl = usageRightsConnection["Url"]
            let apiKey = usageRightsConnection["ApiKeyCached"]
            let rightsService = Rights.UsageRightsService(baseUrl, apiKey)
            rightsService.GetUsageRights piProgId
            |> Result.map (fun rights ->
                rights
                |> Array.map<_, RightsInfo> (fun x ->
                    {
                        Geolocation = x.Region
                        PublishStart = x.PublishStart
                        PublishEnd = x.PublishEnd
                    }))

    let getTranscodedFiles piProgId carrierId (files: PsTranscodedFiles) =
        match files with
        | PsTranscodedFiles.Video files ->
            files
            |> List.map (fun file ->
                MediaType.Video, file.FilePath, file.BitRate, (Xml.XmlConvert.ToTimeSpan file.Duration).TotalMilliseconds |> Math.Round |> int)
        | PsTranscodedFiles.LegacyVideo files ->
            files
            |> List.map (fun file ->
                MediaType.Video, file.FilePath, file.BitRate, (Xml.XmlConvert.ToTimeSpan file.Duration).TotalMilliseconds |> Math.Round |> int)
        | PsTranscodedFiles.Audio files -> files |> List.map (fun file -> MediaType.Audio, file.FilePath, file.BitRate, file.Duration)
        | PsTranscodedFiles.LegacyAudio files -> files |> List.map (fun file -> MediaType.Audio, file.FilePath, file.BitRate, file.Duration)
        |> Seq.map (fun (mediaType, filePath, bitRate, duration) ->
            {
                PiProgId = piProgId
                MediaType = mediaType
                CarrierId = CarrierId.value carrierId
                PartNumber = 1
                FileName = Path.GetFileName filePath
                BitRate = bitRate
                DurationMilliseconds = duration
                DurationISO8601 = TimeSpan.FromMilliseconds duration |> Xml.XmlConvert.ToString
            })

    let getProgramFiles fileSets piProgId =
        fileSets
        |> Map.toList
        |> Seq.map (fun (carrierId, fileSet) -> getTranscodedFiles piProgId carrierId fileSet.TranscodedFiles)
        |> Seq.concat
        |> Seq.toList

    let getTranscodingInfo (filesState: PsFilesState) piProgId =
        let activeCarriers =
            match filesState.ActiveCarriers with
            | Some activeCarriers -> activeCarriers |> fst |> List.map _.CarrierId |> List.toArray
            | None -> Array.empty
        let transcodedFiles =
            filesState.FileSets
            |> Map.toSeq
            |> Seq.map (fun (carrierId, fileSet) ->
                CarrierId.value carrierId,
                getTranscodedFiles piProgId carrierId fileSet.TranscodedFiles
                |> Seq.map (fun x ->
                    {
                        FileName = x.FileName
                        BitRate = x.BitRate
                        DurationMilliseconds = x.DurationMilliseconds
                        DurationISO8601 = x.DurationISO8601
                    })
                |> Seq.toArray)
            |> Map.ofSeq
        {
            ActiveCarriers = activeCarriers
            TranscodedFiles = transcodedFiles
        }

    let getArchiveFiles (filesState: PsFilesState) (log: ILoggingAdapter) piProgId =
        filesState.FileSets
        |> Map.toArray
        |> Array.map (fun (carrierId, fileSet) ->
            {
                ArchivePart.PartId = CarrierId.value carrierId |> String.toLower
                Files =
                    fileSet.TranscodedFiles
                    |> getFilesFromTranscodedFiles
                    |> List.map (fun x ->
                        {
                            ArchiveFile.Path = x.Path
                            Length = x.Length
                            Modified = x.Modified
                        })
                    |> List.toArray
            })

    let getFilesInfo (reader: IStateReader) log piProgId =
        let mediaSetId = MediaSetId.create (Alphanumeric PsClientId) (ContentId.create (PiProgId.value piProgId))
        let filesState = reader.PsFilesState mediaSetId.Value |> Async.RunSynchronously
        getProgramFiles filesState.FileSets piProgId

    let getSubtitlesInfo (filesState: PsFilesState) log piProgId =
        filesState.SubtitlesSets
        |> List.choose (function
            | SubtitlesSet x ->
                {
                    PartId = x.CarrierIds.Head
                    Subtitles = x.Subtitles |> List.map _.FilePath |> List.toArray
                }
                |> Some
            | _ -> None)
        |> Seq.toArray

    let getLegacySubtitlesInfo (filesState: PsFilesState) log piProgId =
        filesState.SubtitlesSets
        |> List.choose (function
            | LegacySubtitlesSet x -> x |> List.map _.FilePath |> Some
            | _ -> None)
        |> List.concat
        |> List.toArray

    [<RequireQualifiedAccess>]
    type RightsStatus =
        | Active
        | Future
        | Expired
        | None

    let getRightsResult (rights: RightsInfo array) =
        let now = DateTime.Now
        if Array.isEmpty rights then
            RightsStatus.None
        else
            let intervals = rights |> Array.map (fun x -> x.PublishStart, x.PublishEnd |> Option.defaultValue DateTime.MaxValue)
            let hasCurrentRights = intervals |> Array.exists (fun (publishStart, publishEnd) -> publishStart < now && publishEnd > now)
            let hasFutureRights =
                intervals |> Array.exists (fun (publishStart, publishEnd) -> publishStart > now && publishEnd > publishStart)
            if hasCurrentRights then RightsStatus.Active
            else if hasFutureRights then RightsStatus.Future
            else RightsStatus.Expired

    let rightsStatusToDistributionStatus rights =
        match rights with
        | RightsStatus.Active
        | RightsStatus.Future -> DistributionStatus.Ok
        | RightsStatus.Expired -> DistributionStatus.Warning "Expired rights"
        | RightsStatus.None -> DistributionStatus.Warning "No rights"

    let getRightsStatusSummary rights =
        match getRightsResult rights with
        | RightsStatus.Active -> "Active rights"
        | RightsStatus.Future -> "Future rights"
        | RightsStatus.Expired -> "Expired rights"
        | RightsStatus.None -> "No rights"

    let getTranscodingStatusSummary (transcoding: TranscodingInfo) =
        if transcoding.ActiveCarriers |> Seq.isEmpty then
            "None"
        else
            $"%d{transcoding.ActiveCarriers.Length} active carriers, %d{transcoding.TranscodedFiles.Count} total part(s) transcoded"

    let updateProgramRights (psProxy: IActorRef<_>) piProgId (rights: RightsInfo) =
        psProxy
        <! PsShardMessages.PsShardMessage.UsageRights
            {
                ProgramId = piProgId
                Rights =
                    [
                        {
                            Region = rights.Geolocation
                            PublishStart = rights.PublishStart
                            PublishEnd = rights.PublishEnd
                        }
                    ]
            }

    let updateProgramVideoFiles (psProxy: IActorRef<_>) (filesState: PsFilesState) (fileSets: PsDto.PsVideoTranscodingDto list) =
        fileSets
        |> Seq.iter (fun fileSet ->
            let job: PsVideoTranscodingJob =
                {
                    ProgramId = fileSet.ProgramId
                    Carriers =
                        fileSet.Carriers
                        |> List.map (fun x ->
                            {
                                CarrierId = x.CarrierId
                                Duration = x.Duration
                            })
                    Files =
                        fileSet.Files
                        |> List.map (fun x ->
                            {
                                RungName = x.RungName
                                BitRate = x.Bitrate
                                Duration = x.Duration
                                Video = PsDto.PsTranscodedVideoDto.toDomain x.Video
                                Audio = PsDto.PsTranscodedAudioDto.toDomain x.Audio
                                FilePath = x.FilePath
                            })
                    PublishingPriority = None
                    Version = fileSet.Version
                }
            psProxy <! PsShardMessages.PsShardMessage.VideoTranscodingDetails job)

    let updateProgramAudioFiles (psProxy: IActorRef<_>) (filesState: PsFilesState) (fileSets: PsAudioTranscodingJob list) =
        fileSets
        |> Seq.iter (fun fileSet ->
            let job: PsAudioTranscodingJob =
                {
                    ProgramId = fileSet.ProgramId
                    CarrierId = fileSet.ProgramId
                    Files =
                        fileSet.Files
                        |> List.map (fun x ->
                            {
                                BitRate = x.BitRate
                                Duration = x.Duration
                                FilePath = x.FilePath
                            })
                    Version = fileSet.Version
                }
            psProxy <! PsShardMessages.PsShardMessage.AudioTranscodingDetails job)

    let updateActiveCarriers configRoot (psProxy: IActorRef<_>) piProgId (carriers: PsDto.PsProgramCarrierDto list) log =
        use ctx = getGranittContext configRoot log
        let msg: PsProgramGotActiveCarriersEvent =
            {
                ProgramId = PiProgId.value piProgId
                Carriers = carriers |> List.map PsDto.PsProgramCarrierDto.toDomain
                PublishingPriority = PsPublishingPriority.Low
                Timestamp = DateTimeOffset.Now
            }
        psProxy <! PsShardMessages.PsShardMessage.ProgramGotActiveCarriersEvent msg

    let getMediaSetInfo usageRightsConnectionString (reader: IStateReader) (contentId: string) log =
        async {
            let piProgId = PiProgId.create contentId
            let mediaSetId = MediaSetId.create (Alphanumeric PsClientId) (ContentId.create (PiProgId.value piProgId))
            let! filesState = reader.PsFilesState mediaSetId.Value
            let rights =
                match getRightsInfo usageRightsConnectionString filesState piProgId with
                | Result.Ok rights ->
                    rights
                    |> Array.map (fun x ->
                        {
                            Geolocation = x.Geolocation
                            PublishStart = x.PublishStart
                            PublishEnd = x.PublishEnd
                        })
                | Result.Error(_, reasonPhrase) ->
                    [|
                        {
                            Geolocation = reasonPhrase
                            PublishStart = DateTime.MinValue
                            PublishEnd = None
                        }
                    |]
            let transcoding = getTranscodingInfo filesState piProgId
            let files = getArchiveFiles filesState log piProgId
            let subtitles = getSubtitlesInfo filesState log piProgId
            let legacySubtitles = getLegacySubtitlesInfo filesState log piProgId
            return
                {
                    Rights = rights
                    Transcoding = transcoding
                    Files = files
                    Subtitles = subtitles
                    LegacySubtitles = legacySubtitles
                }
        }

    let updateRights psMediator (eventId: string) (rights: RightsInfo) =
        let piProgId = eventId.ToUpper()
        updateProgramRights psMediator piProgId rights

    let updateVideoFiles psProxy (eventId: string) (filesState: PsFilesState) files reader log =
        let piProgId = PiProgId.create eventId
        updateProgramVideoFiles psProxy filesState files
        getFilesInfo reader log piProgId

    let updateAudioFiles psProxy (eventId: string) (filesState: PsFilesState) files reader log =
        let piProgId = PiProgId.create eventId
        updateProgramAudioFiles psProxy filesState files
        getFilesInfo reader log piProgId

    let getArchiveResult (ps: PsInfo) (parts: PartInfo array) = // TODO: ask psf?
        let activeParts = parts |> Array.map _.PartIds |> Array.concat |> Array.distinct
        let archiveFiles = ps.Files |> Array.filter (fun part -> activeParts |> Array.contains part.PartId) |> Array.collect _.Files
        match archiveFiles with
        | [||] -> DistributionStatus.Error "No files in archive"
        | _ ->
            let maxRemoteFileTime =
                parts
                |> Array.collect PartInfo.fileStates
                |> Array.fold (fun acc file -> if file.Timestamp > acc then file.Timestamp else acc) DateTimeOffset.MinValue
            let maxArchiveFileTime =
                archiveFiles |> Array.fold (fun acc file -> if file.Modified > acc then file.Modified else acc) DateTimeOffset.MinValue
            if maxArchiveFileTime > maxRemoteFileTime then
                DistributionStatus.Error "Outdated files"
            else
                DistributionStatus.Ok

    let getArchiveStatusSummary (archive: ArchivePart array) =
        let files = archive |> Array.map (fun x -> x.Files |> Seq.toArray) |> Array.concat
        $"%d{files.Length} files"

    let getStatusSummary (ps: PsInfo) =
        {
            PsStatusSummary.Rights = getRightsStatusSummary ps.Rights
            Transcoding = getTranscodingStatusSummary ps.Transcoding
            Archive = getArchiveStatusSummary ps.Files
        }
