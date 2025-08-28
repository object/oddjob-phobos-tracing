namespace Nrk.Oddjob.WebApi

module SwaggerTypes =

    open System
    open System.Text.Json.Serialization
    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Ps
    open Nrk.Oddjob.Potion

    [<Flags>]
    [<JsonConverter(typedefof<JsonStringEnumConverter>)>]
    type MediaSetStatus =
        | Pending = 0
        | Completed = 1
        | Rejected = 2
        | Expired = 3

    [<Flags>]
    [<JsonConverter(typedefof<JsonStringEnumConverter>)>]
    type GlobalConnectCommand =
        | UploadFile = 0
        | MoveFile = 1
        | DeleteFile = 2
        | UploadSubtitles = 3
        | MoveSubtitles = 4
        | DeleteSubtitles = 5
        | UploadSmil = 6
        | MoveSmil = 7
        | DeleteSmil = 8
        | CleanupStorage = 9

    [<Flags>]
    [<JsonConverter(typedefof<JsonStringEnumConverter>)>]
    type ContentCommand =
        | RepairMediaType = 0
        | RepairSourceFiles = 1
        | RepairGlobalConnectFile = 2

    type RemainingActions =
        {
            GlobalConnect: GlobalConnectCommand array
            Content: ContentCommand array
        }

    type MediaSetStatusInfo =
        {
            MediaSetId: string
            Status: MediaSetStatus
            ActivationTime: DateTime
            UpdateTime: DateTime
            RetryCount: int
            Priority: int
            RemainingActions: RemainingActions
        }

    type RightsInfo =
        {
            Geolocation: string
            PublishStart: DateTime
            PublishEnd: DateTime
        }

    type GeneralSummary =
        {
            Status: string
            LastUpdateTime: string
            Files: string
            Format: string
            Mixdown: string
            GlobalConnect: string
            Ps: PsStatusSummary
            Potion: PotionStatusSummary
            Errors: string array
            Warnings: string array
            RepairActions: RemainingActions
        }

    type PsInfo =
        {
            Rights: RightsInfo array
            Transcoding: TranscodingInfo
            Files: ArchivePart array
            Subtitles: ArchiveSubtitles array
            LegacySubtitles: string array
        }

    type PotionInfo =
        {
            Mapping: PotionMapping
            Publication: PotionPublication
        }

    type MediaSetSummary =
        {
            Summary: GeneralSummary
            MediaSetId: string
            GlobalConnect: GlobalConnectInfo
            Parts: PartInfo array
            Ps: PsInfo
            Potion: PotionInfo
        }

    type MediaSetPersistenceState =
        {
            PersistenceId: string
            State: Dto.MediaSet.MediaSetState
            RepairActions: RemainingActions
        }

    type PsAudioTranscodingDto =
        {
            ProgramId: string
            CarrierId: string
            Files: PsTypes.PsTranscodedAudioFile list
            Version: int
        }

    type PsVideoTranscodingDto =
        {
            ProgramId: string
            Carriers: PsDto.PsTranscodedCarrierDto list
            Files: PsDto.PsTranscodedVideoFileDto list
            Priority: string
            Version: int
        }

    type PsTranscodingDto =
        {
            Audio: PsAudioTranscodingDto list
            Video: PsVideoTranscodingDto list
        }

    type PsChangeJobDto =
        {
            ProgrammeId: string
            Source: string
            TimeStamp: DateTime
            RetryCount: int
            PublishingPriority: string
        }

    type AddFileCommand =
        {
            MediaType: string
            GroupId: string
            FileId: string
            FilePath: string
            QualityId: int
            CorrelationId: string
            Source: string
        }

    type AddSubtitleCommand =
        {
            GroupId: string
            CorrelationId: string
            Subtitles: PotionTypes.SubtitleInfo list
            Source: string
        }

    type SetGeoBlockCommand =
        {
            GeoBlock: string
            GroupId: string
            CorrelationId: string
            Version: int
            Source: string
        }

    type DeleteGroupCommand =
        {
            GroupId: string
            CorrelationId: string
            Source: string
        }

    type DeleteSubtitleCommand =
        {
            GroupId: string
            CorrelationId: string
            Language: string
            Source: string
        }

    type PotionCommand =
        {
            AddFile: AddFileCommand
            AddSubtitle: AddSubtitleCommand
            SetGeoBlock: SetGeoBlockCommand
            DeleteGroup: DeleteGroupCommand
            DeleteSubtitle: DeleteSubtitleCommand
        }
