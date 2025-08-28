namespace Nrk.Oddjob.WebApi

[<AutoOpen>]
module PublicTypes =

    open System
    open Nrk.Oddjob.Core

    [<Literal>]
    let JsonMimeType = "application/json; charset=utf-8"

    [<Literal>]
    let TextMimeType = "text/plain; charset=utf-8"

    [<Literal>]
    let AccessControlHeader = "Access-Control-Allow-Origin"

    type HealthMetrics = { Name: string; Value: int64 }

    type HealthStatus =
        {
            Name: string
            Updated: DateTimeOffset
            Status: string
            StatusMessage: string
            Metrics: HealthMetrics list
            Components: HealthStatus list
        }

    module HealthCheckStatus =
        let Ok = "OK"
        let Warning = "WARNING"
        let Fail = "FAIL"

    [<RequireQualifiedAccess>]
    type DistributionStatus =
        | Ok
        | Warning of string
        | Error of string
        | Errors of string array
        | NotStarted
        | InProgress

    type RemoteActorInfo = { ActorPath: string }

    type RightsInfo =
        {
            Geolocation: string
            PublishStart: DateTime
            PublishEnd: DateTime option
        }

    type RemoteResourceResult =
        | Ok
        | Error of int * string

        static member Convert(result: MediaSetTypes.RemoteResult) =
            match result with
            | Result.Ok() -> Ok
            | Result.Error(code, text) -> Error(code, text)

    [<RequireQualifiedAccess>]
    type ReminderMessage =
        | ClearMediaSet of mediaSetId: string
        | StorageCleanup of mediaSetId: string
        | TranscodingBump of programId: string
        | PlayabilityBump of programId: string
        | PotionBump of contentId: string

    type CreateReminderInfo =
        {
            GroupName: string
            TaskId: string
            Message: ReminderMessage
            StartTime: DateTimeOffset
        }

    type TranscodedFile =
        {
            FileName: string
            BitRate: int
            DurationMilliseconds: int
            DurationISO8601: string
        }

    type TranscodingInfo =
        {
            ActiveCarriers: string array
            TranscodedFiles: Map<string, TranscodedFile array>
        }

    type FileInfo =
        {
            CarrierId: string
            FileName: string
            FilePath: string
            BitRate: int
        }

    type GlobalConnectFileInfo =
        {
            SourcePath: string
            RemotePath: string
        }

    type GlobalConnectFileStateInfo =
        {
            QualityId: int
            File: GlobalConnectFileInfo
            RemoteState: RemoteState
            LastResult: RemoteResourceResult
        }

    type GlobalConnectSubtitlesInfo =
        {
            SourcePath: string
            RemotePath: string
        }

    type GlobalConnectSubtitlesStateInfo =
        {
            LanguageCode: string
            Name: string
            File: GlobalConnectSubtitlesInfo
            RemoteState: RemoteState
            LastResult: RemoteResourceResult
        }

    type GlobalConnectSmilInfo = { Content: string; RemotePath: string }

    type GlobalConnectSmilStateInfo =
        {
            Smil: GlobalConnectSmilInfo
            RemoteState: RemoteState
            LastResult: RemoteResourceResult
        }

    type GlobalConnectPartInfo =
        {
            Files: GlobalConnectFileStateInfo array
            Subtitles: GlobalConnectSubtitlesStateInfo array
            Smil: GlobalConnectSmilStateInfo
        }

    [<NoEquality; NoComparison>]
    type PartInfo =
        {
            PartIds: string array
            PartNumber: uint64
            GlobalConnect: GlobalConnectPartInfo
        }

    module PartInfo =
        let fileStates (partInfo: PartInfo) =
            partInfo.GlobalConnect.Files |> Array.map _.RemoteState
        let subtitlesStates (partInfo: PartInfo) =
            partInfo.GlobalConnect.Subtitles |> Array.map _.RemoteState

    type ArchiveFile =
        {
            Path: string
            Length: uint64
            Modified: DateTimeOffset
        }

    type ArchivePart =
        {
            PartId: string
            Files: ArchiveFile array
        }

    type ArchiveSubtitles =
        {
            PartId: string
            Subtitles: string array
        }

    type PsStatusSummary =
        {
            Rights: string
            Transcoding: string
            Archive: string
        }

    type PotionStatusSummary =
        {
            PotionGroupId: string
            PublicationUrl: string
        }

    type MediaSetStatusInfo =
        {
            MediaSetId: string
            Status: MediaSetStatus
            ActivationTime: DateTime
            UpdateTime: DateTime
            RetryCount: int
            Priority: int
            RemainingActions: Dto.MediaSet.RemainingActions option
        }

    type MediaSetStatusCount =
        {
            Count: int
            Query: Map<string, string>
        }

    [<NoEquality; NoComparison>]
    type GeneralSummary =
        {
            Status: string
            LastUpdateTime: string
            Files: string
            Format: string
            Mixdown: string
            GlobalConnect: string
            Ps: PsStatusSummary option
            Potion: PotionStatusSummary option
            Errors: string array
            Warnings: string array
            RepairActions: Dto.MediaSet.RemainingActions
        }

        static member Zero =
            {
                Status = null
                LastUpdateTime = null
                Files = null
                Format = null
                Mixdown = null
                GlobalConnect = null
                Ps = None
                Potion = None
                Errors = Array.empty
                Warnings = Array.empty
                RepairActions = Dto.MediaSet.RemainingActions.Zero
            }

    type PsInfo =
        {
            Rights: RightsInfo array
            Transcoding: TranscodingInfo
            Files: ArchivePart array
            Subtitles: ArchiveSubtitles array
            LegacySubtitles: string array
        }

    type PotionMapping =
        {
            InternalGroupId: string
            ExternalGroupId: string
        }

    type PotionPublication =
        {
            PublicationMediaObjectUrl: string
            PublicationEventUrl: string
            MasterEditorialObjectUrl: string
            PublicationUrl: string
        }

        static member Empty =
            {
                PublicationMediaObjectUrl = ""
                PublicationEventUrl = ""
                MasterEditorialObjectUrl = ""
                PublicationUrl = ""
            }

    type PotionInfo =
        {
            Mapping: PotionMapping
            Publication: PotionPublication
        }

    type GlobalConnectRemoteFile =
        {
            Path: string
            Length: uint64
            Modified: DateTimeOffset
            Content: string
        }

    type RemoteGlobalConnect =
        {
            Files: GlobalConnectRemoteFile array
            Subtitles: GlobalConnectRemoteFile array
            Smil: GlobalConnectRemoteFile option
        }

    type GlobalConnectInfo = { Remote: RemoteGlobalConnect }

    [<NoEquality; NoComparison>]
    type MediaSetSummary =
        {
            Summary: GeneralSummary
            MediaSetId: string
            GlobalConnect: GlobalConnectInfo option
            Parts: PartInfo array
            Ps: PsInfo option
            Potion: PotionInfo option
        }

        static member Zero =
            {
                Summary = GeneralSummary.Zero
                MediaSetId = null
                GlobalConnect = None
                Parts = Array.empty
                Ps = None
                Potion = None
            }

    type ProgramStatus = { Status: string }

    type ReminderTrigger =
        {
            Name: string
            StartTimeUtc: DateTime option
            EndTimeUtc: DateTime option
            RepeatCount: int
            RepeatInterval: int
            TimesTriggered: int
            NextFireTimeUtc: DateTime option
            PrevFireTimeUtc: DateTime option
            State: string
        }

    type ReminderGroupResult =
        | Count of int
        | Trigger of ReminderTrigger

    type ReminderGroup =
        {
            GroupName: string
            Items: ReminderGroupResult list
        }

    let extractPiProgId (qualifiedPersistenceId: string) =
        let items = qualifiedPersistenceId.Split([| '/'; '~' |])
        if items.Length = 2 && items[0] = PsClientId then
            Some items[1]
        else
            None
