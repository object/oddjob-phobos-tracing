namespace Nrk.Oddjob.Ps

module PsTypes =

    open System

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Granitt

    [<RequireQualifiedAccess>]
    type PsPublishingPriority =
        | Low
        | Medium
        | High

    type PsChangeJob =
        {
            PiProgId: string
            Source: string
            Timestamp: DateTime
            RetryCount: int option
            PublishingPriority: PsPublishingPriority option
        }

    type PsTranscodedVideo =
        {
            Codec: string
            DynamicRangeProfile: string
            DisplayAspectRatio: string
            Width: int
            Height: int
            FrameRate: decimal
        }

    type PsTranscodedAudio = { Mixdown: string }

    type PsTranscodedCarrier = { CarrierId: string; Duration: string }

    module PsTranscodedCarrier =
        let fromCarrierId carrierId =
            {
                CarrierId = carrierId
                Duration = null
            }

        let fromCarrierIdAndDuration carrierId duration =
            {
                CarrierId = carrierId
                Duration = duration
            }

    type PsTranscodedVideoFile =
        {
            RungName: string
            BitRate: int
            Duration: string
            Video: PsTranscodedVideo
            Audio: PsTranscodedAudio
            FilePath: string
        }

    type PsLegacyVideoFile =
        {
            RungName: string
            BitRate: int
            Duration: string
            Mixdown: string
            FilePath: string
        }

    type PsTranscodedAudioFile =
        {
            BitRate: int
            Duration: int
            FilePath: string
        }

    type PsLegacyAudioFile =
        {
            BitRate: int
            Duration: int
            FilePath: string
        }

    type PsVideoTranscodingJob =
        {
            ProgramId: string
            Carriers: PsTranscodedCarrier list
            Files: PsTranscodedVideoFile list
            PublishingPriority: PsPublishingPriority option
            Version: int
        }

        member this.CarrierIds = this.Carriers |> List.map _.CarrierId

    type PsLegacyVideoDetails =
        {
            ProgramId: string
            CarrierId: string
            Files: PsLegacyVideoFile list
        }

    type PsLegacyAudioDetails =
        {
            ProgramId: string
            CarrierId: string
            Files: PsLegacyAudioFile list
        }

    module PsVideoTranscodingJob =
        let tryGetSingleCarrierId (job: PsVideoTranscodingJob) =
            if Seq.isSingleton job.Carriers then
                job.Carriers |> Seq.head |> (fun x -> Some x.CarrierId)
            else
                None

    type PsAudioTranscodingJob =
        {
            ProgramId: string
            CarrierId: string
            Files: PsTranscodedAudioFile list
            Version: int
        }

    type PsTvSubtitleFile =
        {
            LanguageCode: string
            ContentUri: string
            Name: string
            Roles: string list
        }

    // Used in Subtitles Actor, need to archive subtitles to convert to details below
    type PsSubtitleFilesJob =
        {
            ProgramId: string
            CarrierIds: string list
            TranscodingVersion: int
            Version: int
            Subtitles: PsTvSubtitleFile list
            Priority: PsPublishingPriority
        }

    type PsTvSubtitleFileDetailsV1 =
        {
            Function: string
            LanguageCode: string
            FilePath: string
            SubtitlesLinks: string
            Version: int
        }

    type PsTvSubtitleFileDetailsV2 =
        {
            LanguageCode: string
            FilePath: string
            SubtitlesLinks: string
            Name: string
            Roles: string list
            Version: int
        }

    [<RequireQualifiedAccess>]
    type PsTvSubtitleFileDetails =
        | V1 of PsTvSubtitleFileDetailsV1
        | V2 of PsTvSubtitleFileDetailsV2

        member this.AsV1 =
            match this with
            | V1 x -> x
            | V2 x ->
                {
                    Function = x.Name
                    LanguageCode = x.LanguageCode
                    FilePath = x.FilePath
                    SubtitlesLinks = x.SubtitlesLinks
                    Version = x.Version
                }

        member this.FilePath =
            match this with
            | V1 x -> x.FilePath
            | V2 x -> x.FilePath

    //Used in Ps Files Actor
    type PsSubtitleFilesDetails =
        {
            ProgramId: string
            CarrierIds: string list
            TranscodingVersion: int
            Version: int
            Subtitles: PsTvSubtitleFileDetails list
            Priority: PsPublishingPriority
        }

    type PsProgramCarrier = { CarrierId: string; PartNumber: int }

    type PsProgramActivatedEvent =
        {
            ProgramId: string
            Carriers: PsProgramCarrier list
            PublishingPriority: PsPublishingPriority
            Timestamp: DateTimeOffset
        }

    type PsProgramDeactivatedEvent =
        {
            ProgramId: string
            Carriers: PsProgramCarrier list
            Timestamp: DateTimeOffset
        }

    type PsProgramGotActiveCarriersEvent =
        {
            ProgramId: string
            Carriers: PsProgramCarrier list
            PublishingPriority: PsPublishingPriority
            Timestamp: DateTimeOffset
        }

    type PsProgramStatusEvent =
        | ProgramActivated of PsProgramActivatedEvent
        | ProgramDeactivated of PsProgramDeactivatedEvent
        | ProgramGotActiveCarriers of PsProgramGotActiveCarriersEvent

        member this.ProgramId =
            match this with
            | ProgramActivated x -> x.ProgramId
            | ProgramDeactivated x -> x.ProgramId
            | ProgramGotActiveCarriers x -> x.ProgramId
        member this.Timestamp =
            match this with
            | ProgramActivated x -> x.Timestamp
            | ProgramDeactivated x -> x.Timestamp
            | ProgramGotActiveCarriers x -> x.Timestamp
        member this.GetMediaSetId() =
            MediaSetId.create (Alphanumeric PsClientId) (ContentId.create this.ProgramId)

    type PsTranscodedFile = { FileName: string; FilePath: string }

    type PsProgramArchiveFiles =
        {
            MediaType: MediaType
            Files: PsTranscodedFile list
        }

    type PsProgramFiles =
        {
            PiProgId: PiProgId
            MediaType: MediaType
            CarrierId: string
            PartNumber: int
            FileName: string
            BitRate: int
            DurationMilliseconds: int
            DurationISO8601: string
        }

    type PsProgramFilesBitRates = { FileName: string; BitRate: int }

    type PsProfile =
        {
            BitRate: int
            MimeType: string
            Media: string
            FriendlyId: string
            Description: string
        }

    type LimitedRights =
        {
            StartTime: DateTimeOffset
            EndTime: DateTimeOffset
            GeoRestriction: GeoRestriction
        }

    type PerpetualRights =
        {
            StartTime: DateTimeOffset
            GeoRestriction: GeoRestriction
        }

    [<RequireQualifiedAccess>]
    type PsBroadcastingRights =
        | Unspecified
        | Limited of LimitedRights
        | Perpetual of PerpetualRights

        member this.AccessRestrictions =
            match this with
            | Unspecified -> GeoRestriction.Unspecified
            | Limited r -> r.GeoRestriction
            | Perpetual r -> r.GeoRestriction

    module PsBroadcastingRights =
        let limited startTime endTime geoRestriction =
            if startTime = DateTimeOffset.MinValue then
                invalidArg "StartTime" "Invalid StartTime"
            if endTime = DateTimeOffset.MinValue then
                invalidArg "EndTime" "Invalid EndTime"
            if endTime <= startTime then
                invalidOp "EndTime should be greater than StartTime"
            PsBroadcastingRights.Limited
                {
                    StartTime = startTime
                    EndTime = endTime
                    GeoRestriction = geoRestriction
                }

        let perpetual startTime geoRestriction =
            if startTime = DateTimeOffset.MinValue then
                invalidArg "StartTime" "Invalid StartTime"
            PsBroadcastingRights.Perpetual
                {
                    StartTime = startTime
                    GeoRestriction = geoRestriction
                }

        let create (startTime: DateTimeOffset) (endTime: DateTimeOffset) geoRestriction =
            if endTime.Year >= ThePerpetualEndOfTime then
                perpetual startTime geoRestriction
            else
                limited startTime endTime geoRestriction

    /// Do not create these directly; use the `RightsPeriod.create` function
    [<RequireQualifiedAccess>]
    type RightsPeriod =
        | Unlimited of publishStart: DateTime
        | Limited of publishStart: DateTime * publishEnd: DateTime

    module RightsPeriod =

        let tryCreate (publishStart: DateTime) (publishEnd: DateTime) =
            if publishEnd <= publishStart then
                None
            else if publishStart.Year >= ThePerpetualEndOfTime && publishEnd.Year >= ThePerpetualEndOfTime then
                None
            else if publishEnd.Year >= ThePerpetualEndOfTime then
                Some(RightsPeriod.Unlimited publishStart)
            else
                Some(RightsPeriod.Limited(publishStart, publishEnd))

        // Only for testing
        let debugCreate (publishStart: DateTime) (publishEnd: DateTime) =
            if publishEnd <= publishStart then
                invalidArg "publishEnd" "publishEnd has to be later than publishStart"
            else if publishStart.Year >= ThePerpetualEndOfTime && publishEnd.Year >= ThePerpetualEndOfTime then
                invalidArg "publishStart" "publishStart can not refer to the end of time"
            if publishEnd.Year >= ThePerpetualEndOfTime then
                RightsPeriod.Unlimited publishStart
            else
                RightsPeriod.Limited(publishStart, publishEnd)

        let getPublishStart (period: RightsPeriod) =
            match period with
            | RightsPeriod.Unlimited publishStart -> publishStart
            | RightsPeriod.Limited(publishStart, _) -> publishStart

        let tryGetPublishEnd (period: RightsPeriod) =
            match period with
            | RightsPeriod.Unlimited _ -> None
            | RightsPeriod.Limited(_, publishEnd) -> Some publishEnd

        let getPublishEnd (period: RightsPeriod) =
            match period with
            | RightsPeriod.Unlimited _ -> DateTime.MaxValue
            | RightsPeriod.Limited(_, publishEnd) -> publishEnd

        let movePublishStart (ts: TimeSpan) (period: RightsPeriod) =
            match period with
            | RightsPeriod.Unlimited publishStart -> RightsPeriod.Unlimited(publishStart.AddSafely ts)
            | RightsPeriod.Limited(publishStart, publishEnd) -> RightsPeriod.Limited(publishStart.AddSafely ts, publishEnd)

        let movePublishEnd (ts: TimeSpan) (period: RightsPeriod) =
            match period with
            | RightsPeriod.Unlimited _ -> period
            | RightsPeriod.Limited(publishStart, publishEnd) -> RightsPeriod.Limited(publishStart, publishEnd.AddSafely ts)

        let contains (dt: DateTime) (period: RightsPeriod) =
            match period with
            | RightsPeriod.Unlimited publishStart -> publishStart <= dt
            | RightsPeriod.Limited(publishStart, publishEnd) -> publishStart <= dt && dt <= publishEnd

    type PsProgramRights =
        {
            Geolocation: GranittGeorestriction
            RightsPeriod: RightsPeriod
        }

    type DebugPsProgramRights =
        {
            PiProgId: PiProgId
            Geolocation: string
            /// Exactly as appears in Granitt. This has both date and time.
            RawPublishStart: DateTime
            /// Exactly as appears in Granitt. This has both date and time.
            RawPublishEnd: DateTime
            /// As interpreted by Oddjob
            PublishStart: DateTime option
            /// As interpreted by Oddjob
            PublishEnd: DateTime option
            HighSecurity: bool
        }

    type PsProgramTranscoding =
        {
            PiProgId: PiProgId
            CarrierId: string
            Inactive: bool
            TranscodingStatus: decimal
        }

    type PsProgramFilesetParts =
        {
            CarrierId: string
            PartNumber: int
            DurationMs: decimal
            AudioDefinition: Mixdown
        }

    type PsProgramDistributions =
        {
            PiProgId: PiProgId
            ProgrammeId: decimal
            ProgrammeDistributionId: decimal
            Geolocation: string
            ServiceId: decimal
        }

    type PsProgramSubtitles =
        {
            PiProgId: PiProgId
            ProgrammeId: decimal
            ProgrammeSubtitleId: decimal
            LanguageId: decimal
            FilePath: string option
            ServiceId: decimal option
        }

    type PsSubtitles =
        {
            LanguageCode: string
            LanguageType: string
            SideloadingUri: string
        }

    [<NoComparison>]
    type PsSubtitlesConfig =
        {
            BaseUrl: Uri
            BaseUrlTvSubtitles: Uri
            SourceRootWindows: string
            SourceRootLinux: string
            DestinationRootWindows: string
            DestinationRootLinux: string
        }

    type PsProfileMimeType = { Id: string; MimeType: string }

    type File =
        {
            Path: string
            Length: uint64
            Modified: DateTimeOffset
        }

    type FileSet = { FileSetId: string; Files: File list }

    type FileSetInfo =
        {
            /// This should be PiProgId, but this type is currenlty used in WebApi so it needs to be nicely serializable
            ProgrammeId: string
            FileSets: FileSet list
        }

    [<Literal>]
    let SubtitleTypeTtv = "ttv"

    [<Literal>]
    let SubtitleTypeNor = "nor"

    [<Literal>]
    let SubtitleTypeMix = "mix"

    type SoonOrActiveRights = PsProgramRights

    type FutureRights =
        {
            PiProgId: PiProgId
            PublishStart: DateTime
        }

    ///
    ///      A'  A              B
    /// -----|---|==============|---------
    ///
    /// from -inf to A' is FutureRights
    /// from A' to B is SoonOrActiveRights
    /// from A' to A is due to advanced publishing
    /// from A to B is the rights as in Granitt
    /// from B to +inf is PastRights
    ///
    /// The rights periods here already include eventual advanced publishing.
    ///
    type SelectedRights =
        | NoRights
        | PastRights
        | SoonOrActiveRights of SoonOrActiveRights
        | FutureRights of FutureRights

    type EventStoreMessage =
        {
            Program: string
            Carrier: string
            Description: string
            Content: string
            Created: DateTime
        }

    [<RequireQualifiedAccess>]
    type PsTranscodedFiles =
        | Video of PsTranscodedVideoFile list
        | LegacyVideo of PsLegacyVideoFile list
        | Audio of PsTranscodedAudioFile list
        | LegacyAudio of PsLegacyAudioFile list

        member this.Length =
            match this with
            | Video files -> files.Length
            | LegacyVideo files -> files.Length
            | Audio files -> files.Length
            | LegacyAudio files -> files.Length

    type PsArchivedFile =
        {
            SourcePath: string
            ArchivePath: string
        }

    module PsTranscodedFiles =
        let setArchivePath (archivedFile: PsArchivedFile) (files: PsTranscodedFiles) =
            match files with
            | PsTranscodedFiles.Video files ->
                files
                |> List.map (fun x ->
                    if x.FilePath = archivedFile.SourcePath then
                        { x with
                            FilePath = archivedFile.ArchivePath
                        }
                    else
                        x)
                |> PsTranscodedFiles.Video
            | PsTranscodedFiles.LegacyVideo files ->
                files
                |> List.map (fun x ->
                    if x.FilePath = archivedFile.SourcePath then
                        { x with
                            FilePath = archivedFile.ArchivePath
                        }
                    else
                        x)
                |> PsTranscodedFiles.LegacyVideo
            | PsTranscodedFiles.Audio files ->
                files
                |> List.map (fun x ->
                    if x.FilePath = archivedFile.SourcePath then
                        { x with
                            FilePath = archivedFile.ArchivePath
                        }
                    else
                        x)
                |> PsTranscodedFiles.Audio
            | PsTranscodedFiles.LegacyAudio files ->
                files
                |> List.map (fun x ->
                    if x.FilePath = archivedFile.SourcePath then
                        { x with
                            FilePath = archivedFile.ArchivePath
                        }
                    else
                        x)
                |> PsTranscodedFiles.LegacyAudio

        let isEmpty (files: PsTranscodedFiles) =
            match files with
            | PsTranscodedFiles.Video files -> Seq.isEmpty files
            | PsTranscodedFiles.LegacyVideo files -> Seq.isEmpty files
            | PsTranscodedFiles.Audio files -> Seq.isEmpty files
            | PsTranscodedFiles.LegacyAudio files -> Seq.isEmpty files

    type PsFileSet =
        {
            ProgramId: string
            Carriers: PsTranscodedCarrier list
            TranscodedFiles: PsTranscodedFiles
            PublishingPriority: PsPublishingPriority option
            Version: int
        }

        member this.CarrierIds = this.Carriers |> List.map _.CarrierId

    type PsTvSubtitlesSet =
        {
            ProgramId: string
            CarrierIds: string list
            TranscodingVersion: int
            Version: int
            Subtitles: PsTvSubtitleFileDetails list
            Priority: PsPublishingPriority
        }

    type PsLegacySubtitlesFile =
        {
            LanguageType: string
            LanguageCode: string
            FilePath: string
        }

    type PsSubtitlesSet =
        | SubtitlesSet of PsTvSubtitlesSet
        | LegacySubtitlesSet of PsLegacySubtitlesFile list

    type PsUsageRights =
        {
            Region: string
            PublishStart: DateTime
            PublishEnd: DateTime option
        }

    type PsUsageRightsJob =
        {
            ProgramId: string
            Rights: PsUsageRights list
        }

    type PsFilesState =
        {
            ProgramId: PiProgId option
            MediaType: MediaType option
            FileSets: Map<CarrierId, PsFileSet>
            SubtitlesSets: PsSubtitlesSet list
            ActiveCarriers: (PsProgramCarrier list * DateTimeOffset) option
            Rights: PsUsageRights list
            LastSequenceNr: int64
            RestoredSnapshot: bool
            SavedSnapshot: bool
        }

        static member Zero =
            {
                ProgramId = None
                MediaType = None
                FileSets = Map.empty
                SubtitlesSets = List.empty
                ActiveCarriers = None
                Rights = List.empty
                LastSequenceNr = 0L
                RestoredSnapshot = false
                SavedSnapshot = false
            }

        member this.GetActiveFileSets() =
            match this.ActiveCarriers with
            | Some(activeCarriers, _) ->
                this.FileSets
                |> Map.filter (fun carrierId _ -> activeCarriers |> List.map _.CarrierId |> List.contains (CarrierId.value carrierId))
            | None -> Map.empty

        static member GetActiveFiles piProgId fileSets =
            fileSets
            |> Map.toList
            |> List.map (fun (carrierId, fileSet) ->
                match fileSet.TranscodedFiles with
                | PsTranscodedFiles.Video files ->
                    files
                    |> List.map (fun file ->
                        {
                            PiProgId = piProgId
                            CarrierId = CarrierId.value carrierId
                            PartNumber = 1
                            MediaType = MediaType.Video
                            FileName = IO.getFileName file.FilePath
                            BitRate = file.BitRate
                            DurationMilliseconds = (Xml.XmlConvert.ToTimeSpan file.Duration).TotalMilliseconds |> Math.Round |> int
                            DurationISO8601 = file.Duration
                        })
                | PsTranscodedFiles.LegacyVideo files ->
                    files
                    |> List.map (fun file ->
                        {
                            PiProgId = piProgId
                            CarrierId = CarrierId.value carrierId
                            PartNumber = 1
                            MediaType = MediaType.Video
                            FileName = IO.getFileName file.FilePath
                            BitRate = file.BitRate
                            DurationMilliseconds = (Xml.XmlConvert.ToTimeSpan file.Duration).TotalMilliseconds |> Math.Round |> int
                            DurationISO8601 = file.Duration
                        })
                | PsTranscodedFiles.Audio files ->
                    files
                    |> List.map (fun file ->
                        {
                            PiProgId = piProgId
                            CarrierId = CarrierId.value carrierId
                            PartNumber = 1
                            MediaType = MediaType.Audio
                            FileName = IO.getFileName file.FilePath
                            BitRate = file.BitRate
                            DurationMilliseconds = file.Duration
                            DurationISO8601 = TimeSpan.FromMilliseconds file.Duration |> Xml.XmlConvert.ToString
                        })
                | PsTranscodedFiles.LegacyAudio files ->
                    files
                    |> List.map (fun file ->
                        {
                            PiProgId = piProgId
                            CarrierId = CarrierId.value carrierId
                            PartNumber = 1
                            MediaType = MediaType.Audio
                            FileName = IO.getFileName file.FilePath
                            BitRate = file.BitRate
                            DurationMilliseconds = file.Duration
                            DurationISO8601 = TimeSpan.FromMilliseconds file.Duration |> Xml.XmlConvert.ToString
                        }))
            |> List.concat

        member this.TryGetSubtitlesSet transcodingVersion =
            match transcodingVersion with
            | Some transcodingVersion ->
                this.SubtitlesSets
                |> List.tryFind (function
                    | SubtitlesSet set -> set.TranscodingVersion = transcodingVersion
                    | _ -> false)
                |> Option.orElse (this.SubtitlesSets |> List.tryFind _.IsLegacySubtitlesSet)
            | None -> this.SubtitlesSets |> List.tryFind _.IsLegacySubtitlesSet

    type PsArchiveLookupCommand = LocateFiles of PiProgId * Map<CarrierId, PsFileSet>

    type PsArchiveLookupResult = Result<FileSetInfo, Exception>

    type PendingPlayabilityEvent =
        {
            EventId: string
            Event: Dto.Events.MediaSetPlayabilityEventDto
            Timestamp: DateTimeOffset
        }

    type PersistedEvent =
        {
            EventId: string
            Timestamp: DateTimeOffset
            SequenceNr: int64
        }

    type PsPlayabilityHandlerState =
        {
            ProgramId: PiProgId option
            MediaType: MediaType option
            PendingEvent: PendingPlayabilityEvent option
            ReminderStarted: bool
            PersistedEvents: PersistedEvent list
        }

        static member Zero =
            {
                ProgramId = None
                MediaType = None
                PendingEvent = None
                ReminderStarted = false
                PersistedEvents = List.empty
            }

    type VideoTranscodingDetailsAck = { ProgramId: string; Version: int }
    type AudioTranscodingDetailsAck = { ProgramId: string; Version: int }
