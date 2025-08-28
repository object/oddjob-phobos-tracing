namespace Nrk.Oddjob.Ps

module PsDto =

    open System

    open Nrk.Oddjob.Core
    open PsTypes

    type PsChangeJobDto =
        {
            ProgrammeId: string
            /// INCORRECT NAME - this is in fact PiProgId. It has nothing to do with ProgrammeId from Granitt
            Source: string
            TimeStamp: DateTime
            RetryCount: int option
            PublishingPriority: string option
        }

    module PsChangeJobDto =
        let fromPsJob (msg: PsChangeJob) =
            {
                ProgrammeId = msg.PiProgId
                Source = msg.Source
                TimeStamp = msg.Timestamp
                RetryCount = msg.RetryCount
                PublishingPriority = msg.PublishingPriority |> Option.map getUnionCaseName
            }

        let toPsJob (msg: PsChangeJobDto) =
            {
                PiProgId = msg.ProgrammeId
                Source = msg.Source
                Timestamp = msg.TimeStamp
                RetryCount = msg.RetryCount
                PublishingPriority = msg.PublishingPriority |> Option.map parseUnionCaseName
            }

    type PsTranscodedVideoDto =
        {
            Codec: string
            DynamicRangeProfile: string
            DisplayAspectRatio: string
            Width: int
            Height: int
            FrameRate: decimal
        }

    module PsTranscodedVideoDto =
        let fromDomain (video: PsTranscodedVideo) =
            {
                Codec = video.Codec
                DynamicRangeProfile = video.DynamicRangeProfile
                DisplayAspectRatio = video.DisplayAspectRatio
                Width = video.Width
                Height = video.Height
                FrameRate = video.FrameRate
            }
        let toDomain (video: PsTranscodedVideoDto) : PsTranscodedVideo =
            {
                Codec = video.Codec
                DynamicRangeProfile = video.DynamicRangeProfile
                DisplayAspectRatio = video.DisplayAspectRatio
                Width = video.Width
                Height = video.Height
                FrameRate = video.FrameRate
            }

    type PsTranscodedAudioDto = { Mixdown: string }

    module PsTranscodedAudioDto =
        let fromDomain (audio: PsTranscodedAudio) = { Mixdown = audio.Mixdown }
        let toDomain (audio: PsTranscodedAudioDto) : PsTranscodedAudio = { Mixdown = audio.Mixdown }

    type PsTranscodedCarrierDto = { CarrierId: string; Duration: string }

    module PsTranscodedCarrierDto =
        let fromDomain (carrier: PsTranscodedCarrier) =
            {
                CarrierId = carrier.CarrierId
                Duration = carrier.Duration
            }
        let toDomain (carrier: PsTranscodedCarrierDto) : PsTranscodedCarrier =
            {
                CarrierId = carrier.CarrierId
                Duration = carrier.Duration
            }

    type PsTranscodedVideoFileDto =
        {
            RungName: string
            Bitrate: int
            Duration: string
            Video: PsTranscodedVideoDto
            Audio: PsTranscodedAudioDto
            FilePath: string
        }

    type PsVideoTranscodingDto =
        {
            [<Newtonsoft.Json.JsonRequired>]
            ProgramId: string
            [<Newtonsoft.Json.JsonRequired>]
            Carriers: PsTranscodedCarrierDto list
            [<Newtonsoft.Json.JsonRequired>]
            Files: PsTranscodedVideoFileDto list
            Priority: string option
            [<Newtonsoft.Json.JsonRequired>]
            Version: int
        }

    type PsTranscodingFinishedDto = TranscodingFinished of PsVideoTranscodingDto

    module PsTranscodingFinishedDto =
        let toPsJob (job: PsTranscodingFinishedDto) : PsVideoTranscodingJob =
            let (TranscodingFinished job) = job
            {
                ProgramId = job.ProgramId
                Carriers = job.Carriers |> List.map PsTranscodedCarrierDto.toDomain
                PublishingPriority = job.Priority |> Option.bind (tryParseUnionCaseName true)
                Version = job.Version
                Files =
                    job.Files
                    |> List.map (fun file ->
                        {
                            RungName = file.RungName
                            BitRate = file.Bitrate
                            Duration = file.Duration
                            Video = PsTranscodedVideoDto.toDomain file.Video
                            Audio = PsTranscodedAudioDto.toDomain file.Audio
                            FilePath = file.FilePath
                        })
            }

    type PsAudioTranscodingDto = PsAudioTranscodingJob

    [<RequireQualifiedAccess>]
    type PsTranscodingDto =
        | Audio of PsAudioTranscodingDto list
        | Video of PsVideoTranscodingDto list

    type PsRadioChangeDto =
        {
            Type: string
            EventType: string
            Uri: string
            ProgId: string
            Version: int
            Changed: DateTime
        }

    type PsRadioFormatDto = { Label: string }

    type PsRadioLocatorDto = { Identifier: string; Title: string }

    type PsRadioEssenceDto =
        {
            Duration: int
            Formats: PsRadioFormatDto array
            Locator: PsRadioLocatorDto
        }

    type PsRadioEssenceGroupDto =
        {
            Duration: int
            EssenceType: string
            Essences: PsRadioEssenceDto array
        }

    type PsRadioAudioDto =
        {
            EssenceGroups: PsRadioEssenceGroupDto array
        }

    type PsRadioObjectDto =
        {
            ProgId: string
            Duration: int
            Audio: PsRadioAudioDto
        }

    type PsRadioMediaDto =
        {
            ChangeType: string
            VersionNumber: int
            Object: PsRadioObjectDto
        }

    module PsRadioMediaDto =
        let toPsJob (filesSelector: Map<string, int>) (dto: PsRadioMediaDto) : PsAudioTranscodingJob =
            {
                ProgramId = dto.Object.ProgId
                CarrierId = dto.Object.ProgId
                Version = dto.VersionNumber
                Files =
                    dto.Object.Audio.EssenceGroups
                    |> Seq.filter (fun group -> group.EssenceType = "PROGRAMME")
                    |> Seq.map _.Essences
                    |> Seq.concat
                    |> Seq.map (fun essence ->
                        essence,
                        essence.Formats
                        |> Seq.map (fun format -> format.Label)
                        |> Seq.map (fun label -> filesSelector |> Map.tryFind label)
                        |> Seq.choose id)
                    |> Seq.choose (fun (essence, bitRates) -> Seq.tryHead bitRates |> Option.map (fun bitRate -> essence, bitRate))
                    |> Seq.map (fun (essence, bitRate) ->
                        {
                            PsTranscodedAudioFile.BitRate = bitRate
                            Duration = essence.Duration
                            FilePath = essence.Locator.Identifier
                        })
                    |> Seq.toList
            }

    type PsProgramCarrierDto = { CarrierId: string; PartNumber: int }

    module PsProgramCarrierDto =
        let fromDomain (carrier: PsProgramCarrier) =
            {
                PsProgramCarrierDto.CarrierId = carrier.CarrierId
                PsProgramCarrierDto.PartNumber = carrier.PartNumber
            }

        let toDomain (carrier: PsProgramCarrierDto) =
            {
                PsProgramCarrier.CarrierId = carrier.CarrierId
                PsProgramCarrier.PartNumber = carrier.PartNumber
            }

    type PsProgramActivatedEventDto =
        {
            ProgramId: string
            Carriers: PsProgramCarrierDto array
            PublishingPriority: string
            Timestamp: DateTimeOffset
        }

    type PsProgramDeactivatedEventDto =
        {
            ProgramId: string
            Carriers: PsProgramCarrierDto array
            Timestamp: DateTimeOffset
        }

    type PsProgramGotActiveCarriersEventDto =
        {
            ProgramId: string
            Carriers: PsProgramCarrierDto array
            PublishingPriority: string
            Timestamp: DateTimeOffset
        }

    type PsProgramStatusEventDto =
        | ProgramActivated of PsProgramActivatedEventDto
        | ProgramDeactivated of PsProgramDeactivatedEventDto
        | ProgramGotActiveCarriers of PsProgramGotActiveCarriersEventDto

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

    module PsProgramStatusEventDto =
        let fromDomain evt : PsProgramStatusEventDto =
            match evt with
            | PsProgramStatusEvent.ProgramActivated x ->
                ProgramActivated
                    {
                        ProgramId = x.ProgramId
                        Carriers = x.Carriers |> Seq.map PsProgramCarrierDto.fromDomain |> Seq.toArray
                        PublishingPriority = getUnionCaseName x.PublishingPriority
                        Timestamp = x.Timestamp
                    }
            | PsProgramStatusEvent.ProgramDeactivated x ->
                ProgramDeactivated
                    {
                        ProgramId = x.ProgramId
                        Carriers = x.Carriers |> Seq.map PsProgramCarrierDto.fromDomain |> Seq.toArray
                        Timestamp = x.Timestamp
                    }
            | PsProgramStatusEvent.ProgramGotActiveCarriers x ->
                ProgramGotActiveCarriers
                    {
                        ProgramId = x.ProgramId
                        Carriers = x.Carriers |> Seq.map PsProgramCarrierDto.fromDomain |> Seq.toArray
                        PublishingPriority = getUnionCaseName x.PublishingPriority
                        Timestamp = x.Timestamp
                    }

        let toDomain evt : PsProgramStatusEvent =
            match evt with
            | ProgramActivated x ->
                PsProgramStatusEvent.ProgramActivated
                    {
                        ProgramId = x.ProgramId
                        Carriers = x.Carriers |> Seq.map PsProgramCarrierDto.toDomain |> Seq.toList
                        PublishingPriority = x.PublishingPriority |> tryParseUnionCaseName true |> Option.defaultValue PsPublishingPriority.Medium
                        Timestamp = x.Timestamp
                    }
            | ProgramDeactivated x ->
                PsProgramStatusEvent.ProgramDeactivated
                    {
                        ProgramId = x.ProgramId
                        Carriers = x.Carriers |> Seq.map PsProgramCarrierDto.toDomain |> Seq.toList
                        Timestamp = x.Timestamp
                    }
            | ProgramGotActiveCarriers x ->
                PsProgramStatusEvent.ProgramGotActiveCarriers
                    {
                        ProgramId = x.ProgramId
                        Carriers = x.Carriers |> Seq.map PsProgramCarrierDto.toDomain |> Seq.toList
                        PublishingPriority = x.PublishingPriority |> tryParseUnionCaseName true |> Option.defaultValue PsPublishingPriority.Medium
                        Timestamp = x.Timestamp
                    }

    type PsTvSubtitleFileDto =
        {
            [<Newtonsoft.Json.JsonRequired>]
            LanguageCode: string
            [<Newtonsoft.Json.JsonRequired>]
            ContentUri: string
            [<Newtonsoft.Json.JsonRequired>]
            Name: string
            [<Newtonsoft.Json.JsonRequired>]
            Roles: string list
        }

    type PsAssignProgramSubtitlesDto =
        {
            [<Newtonsoft.Json.JsonRequired>]
            ProgramId: string
            [<Newtonsoft.Json.JsonRequired>]
            CarrierIds: string list
            [<Newtonsoft.Json.JsonRequired>]
            TranscodingVersion: int
            [<Newtonsoft.Json.JsonRequired>]
            Version: int
            [<Newtonsoft.Json.JsonRequired>]
            Subtitles: PsTvSubtitleFileDto list
            Priority: string option
        }

    type PsTvSubtitlesDto =
        | AssignProgramSubtitles of PsAssignProgramSubtitlesDto

        member this.ProgramId =
            match this with
            | AssignProgramSubtitles x -> x.ProgramId

    module PsTvSubtitlesDto =
        let toPsJob dto : PsSubtitleFilesJob =
            let toPsTvSubtitleFile (dto: PsTvSubtitleFileDto) : PsTvSubtitleFile =
                {
                    LanguageCode = dto.LanguageCode
                    ContentUri = dto.ContentUri
                    Name = dto.Name
                    Roles = dto.Roles
                }

            match dto with
            | AssignProgramSubtitles dto' ->
                {
                    ProgramId = dto'.ProgramId
                    CarrierIds = dto'.CarrierIds
                    TranscodingVersion = dto'.TranscodingVersion
                    Version = dto'.Version
                    Subtitles = dto'.Subtitles |> List.map toPsTvSubtitleFile
                    Priority = dto'.Priority |> Option.bind (tryParseUnionCaseName true) |> Option.defaultValue PsPublishingPriority.Medium
                }

    type PsUsageRightsChangeDto = { ProductCode: string }

    type PlayabilityBump = { ProgramId: string }

    type TranscodingBump = { ProgramId: string }
