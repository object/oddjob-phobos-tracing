namespace Nrk.Oddjob.Ps

open Nrk.Oddjob.Ps.PsTypes

[<RequireQualifiedAccess>]
module PsPersistence =

    open System
    open System.IO
    open ProtoBuf
    open Akka.Actor
    open Akka.Util

    open Nrk.Oddjob.Core

    [<Literal>]
    let private UnspecifiedValue = 0

    let private immutableGeoRestrictionMapping =
        // The following mappings are immutable: new values can be added, but once a numeric value is mapped it can not be changed
        [
            (0, GeoRestriction.Unspecified)
            (1, GeoRestriction.World)
            (2, GeoRestriction.NRK)
            (3, GeoRestriction.Norway)
        ]

    let private immutableMediaTypeMapping =
        // The following mappings are immutable: new values can be added, but once a numeric value is mapped it can not be changed
        [ (1, MediaType.Video); (2, MediaType.Audio) ]

    let private immutablePublishingPriorityMapping =
        // The following mappings are immutable: new values can be added, but once a numeric value is mapped it can not be changed
        [
            (1, PsPublishingPriority.Low)
            (2, PsPublishingPriority.Medium)
            (3, PsPublishingPriority.High)
            (0, PsPublishingPriority.Medium)
        ] // Fallback, set to medium if not specified

    let convertFromGeoRestriction value =
        value |> convertOrFail (tryConvertFromDomainValue immutableGeoRestrictionMapping)

    let convertToGeoRestriction value =
        value |> convertOrFail (tryConvertToDomainValue immutableGeoRestrictionMapping)

    let convertFromMediaType value =
        value |> convertOrFail (tryConvertFromDomainValue immutableMediaTypeMapping)

    let convertToMediaType value =
        value |> convertOrFail (tryConvertToDomainValue immutableMediaTypeMapping)

    let convertFromPublishingPriority value =
        value |> convertOrFail (tryConvertFromDomainValue immutablePublishingPriorityMapping)

    let convertToPublishingPriority value =
        value |> convertOrFail (tryConvertToDomainValue immutablePublishingPriorityMapping)

    [<AllowNullLiteral>]
    type IProtoBufSerializable = interface end

    [<AllowNullLiteral>]
    type IProtoBufSerializableEvent =
        inherit IProtoBufSerializable
        abstract member Timestamp: DateTimeOffset

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignedProgramId =
        {
            [<ProtoMember(1)>]
            ProgramId: string
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(piProgId, timestamp) =
            {
                ProgramId = PiProgId.value piProgId
                Timestamp = timestamp
            }
        static member FromDomain piProgId =
            AssignedProgramId.FromDomainWithTimestamp(piProgId, DateTimeOffset.Now)
        member this.ToDomain() = PiProgId.create this.ProgramId

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignedMediaType =
        {
            [<ProtoMember(1)>]
            MediaType: int
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(mediaType, timestamp) =
            {
                MediaType = convertFromMediaType mediaType
                Timestamp = timestamp
            }
        static member FromDomain mediaType =
            AssignedMediaType.FromDomainWithTimestamp(mediaType, DateTimeOffset.Now)
        member this.ToDomain() = convertToMediaType this.MediaType

    [<ProtoContract; CLIMutable; NoComparison>]
    type TranscodedVideo =
        {
            [<ProtoMember(6)>]
            Codec: string
            [<ProtoMember(1)>]
            DynamicRangeProfile: string
            [<ProtoMember(2)>]
            DisplayAspectRatio: string
            [<ProtoMember(3)>]
            Width: int
            [<ProtoMember(4)>]
            Height: int
            [<ProtoMember(5)>]
            FrameRate: decimal
        } // Last protobuf index: 6

        interface IProtoBufSerializable
        static member FromDomain(ptv: PsTranscodedVideo) =
            {
                Codec = ptv.Codec
                DynamicRangeProfile = ptv.DynamicRangeProfile
                DisplayAspectRatio = ptv.DisplayAspectRatio
                Width = ptv.Width
                Height = ptv.Height
                FrameRate = ptv.FrameRate
            }
        member this.ToDomain() : PsTranscodedVideo =
            {
                Codec = this.Codec
                DynamicRangeProfile = this.DynamicRangeProfile
                DisplayAspectRatio = this.DisplayAspectRatio
                Width = this.Width
                Height = this.Height
                FrameRate = this.FrameRate
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type TranscodedAudio =
        {
            [<ProtoMember(1)>]
            Mixdown: string
        }

        interface IProtoBufSerializable
        static member FromDomain(pta: PsTranscodedAudio) = { Mixdown = pta.Mixdown }
        member this.ToDomain() : PsTranscodedAudio = { Mixdown = this.Mixdown }

    [<ProtoContract; CLIMutable; NoComparison>]
    type TranscodedCarrier =
        {
            [<ProtoMember(1)>]
            CarrierId: string
            [<ProtoMember(2)>]
            Duration: string
        }

        interface IProtoBufSerializable
        static member FromDomain(ptc: PsTranscodedCarrier) =
            {
                CarrierId = ptc.CarrierId
                Duration = ptc.Duration
            }
        member this.ToDomain() : PsTranscodedCarrier =
            {
                CarrierId = this.CarrierId
                Duration = this.Duration
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type TranscodedVideoFile =
        {
            [<ProtoMember(1)>]
            RungName: string
            [<ProtoMember(2)>]
            BitRate: int
            [<ProtoMember(3)>]
            Duration: string
            [<ProtoMember(4)>]
            Video: TranscodedVideo
            [<ProtoMember(5)>]
            Audio: TranscodedAudio
            [<ProtoMember(6)>]
            FilePath: string
        }

        interface IProtoBufSerializable
        static member FromDomain(ptf: PsTranscodedVideoFile) =
            {
                RungName = ptf.RungName
                BitRate = ptf.BitRate
                Duration = ptf.Duration
                Video = TranscodedVideo.FromDomain ptf.Video
                Audio = TranscodedAudio.FromDomain ptf.Audio
                FilePath = ptf.FilePath
            }
        member this.ToDomain() : PsTranscodedVideoFile =
            {
                RungName = this.RungName
                BitRate = this.BitRate
                Duration = this.Duration
                Video = this.Video.ToDomain()
                Audio = this.Audio.ToDomain()
                FilePath = this.FilePath
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type LegacyVideoFile =
        {
            [<ProtoMember(1)>]
            RungName: string
            [<ProtoMember(2)>]
            BitRate: int
            [<ProtoMember(4)>]
            Duration: string
            [<ProtoMember(5)>]
            Mixdown: string
            [<ProtoMember(3)>]
            FilePath: string
        } // Last protobuf index: 5

        interface IProtoBufSerializable
        static member FromDomain(ptf: PsLegacyVideoFile) =
            {
                RungName = ptf.RungName
                BitRate = ptf.BitRate
                Duration = ptf.Duration
                Mixdown = ptf.Mixdown
                FilePath = ptf.FilePath
            }
        member this.ToDomain() : PsLegacyVideoFile =
            {
                RungName = this.RungName
                // Most of bitrates in Granitt were already represented in Kbits so they didn't have to be multiplied by 1000
                BitRate =
                    if this.BitRate < 0 || this.BitRate % 100000 = 0 then
                        if this.RungName.EndsWith "180" then 141
                        else if this.RungName.EndsWith "270" then 316
                        else if this.RungName.EndsWith "360" then 563
                        else if this.RungName.EndsWith "540" then 1266
                        else if this.RungName.EndsWith "720" then 2250
                        else if this.RungName.EndsWith "720-1" then 2251
                        else if this.RungName.EndsWith "720-2" then 2252
                        else if this.RungName.EndsWith "1080" then 6000
                        else if this.RungName.EndsWith "1080-1" then 6001
                        else if this.RungName.EndsWith "1080-2" then 6002
                        else (Math.Abs this.BitRate) / 1000
                    else
                        this.BitRate
                Duration = this.Duration
                Mixdown = this.Mixdown
                FilePath = this.FilePath
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type TranscodedVideoFileSet =
        {
            [<ProtoMember(5)>]
            ProgramId: string
            [<ProtoMember(6)>]
            CarrierIds_Deprecated: string array
            [<ProtoMember(1)>]
            CarrierId_Deprecated: string
            [<ProtoMember(7)>]
            Carriers: TranscodedCarrier array
            [<ProtoMember(2)>]
            Files: TranscodedVideoFile array
            [<ProtoMember(3)>]
            Version: int
            [<ProtoMember(4)>]
            PublishingPriority: int
        } // Last protobuf index: 7

        interface IProtoBufSerializable
        static member FromDomain(job: PsVideoTranscodingJob) =
            {
                ProgramId = job.ProgramId
                CarrierIds_Deprecated = job.CarrierIds |> List.toArrayOrNull
                CarrierId_Deprecated = job.CarrierIds |> List.tryHead |> Option.toObj
                Carriers = job.Carriers |> List.map TranscodedCarrier.FromDomain |> List.toArrayOrNull
                Files = job.Files |> List.map TranscodedVideoFile.FromDomain |> List.toArrayOrNull
                Version = job.Version
                PublishingPriority = job.PublishingPriority |> Option.map convertFromPublishingPriority |> Option.defaultValue 0
            }
        member this.ToDomain() : PsVideoTranscodingJob =
            {
                ProgramId = this.ProgramId
                Carriers =
                    if this.Carriers <> null && Seq.isNotEmpty this.Carriers then
                        this.Carriers |> List.ofArrayOrNull |> List.map _.ToDomain()
                    else
                        if this.CarrierIds_Deprecated = null then
                            [ this.CarrierId_Deprecated ]
                        else
                            this.CarrierIds_Deprecated |> List.ofArrayOrNull
                        |> List.map (fun x -> { CarrierId = x; Duration = null })
                Files = this.Files |> List.ofArrayOrNull |> List.map _.ToDomain()
                Version = this.Version
                PublishingPriority =
                    match this.PublishingPriority with
                    | 0 -> None
                    | x -> convertToPublishingPriority x |> Some
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type LegacyVideoFileSet =
        {
            [<ProtoMember(1)>]
            ProgramId: string
            [<ProtoMember(2)>]
            CarrierId: string
            [<ProtoMember(3)>]
            Files: LegacyVideoFile array
        }

        interface IProtoBufSerializable
        static member FromDomain(job: PsLegacyVideoDetails) =
            {
                ProgramId = job.ProgramId
                CarrierId = job.CarrierId
                Files = job.Files |> List.map LegacyVideoFile.FromDomain |> List.toArrayOrNull
            }
        member this.ToDomain() : PsLegacyVideoDetails =
            {
                ProgramId = this.ProgramId
                CarrierId = this.CarrierId
                Files = this.Files |> List.ofArrayOrNull |> List.map _.ToDomain()
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    [<ProtoReserved(4, "ContentUri")>]
    type TvSubtitlesFileDetails =
        {
            [<ProtoMember(1)>]
            Function: string
            [<ProtoMember(2)>]
            LanguageCode: string
            [<ProtoMember(3)>]
            FilePath: string
            [<ProtoMember(5)>]
            SubtitlesLinks: string
            [<ProtoMember(6)>]
            Version: int
            [<ProtoMember(7)>]
            Name: string
            [<ProtoMember(8)>]
            Roles: string array
        }

        interface IProtoBufSerializable
        static member FromDomain(file: PsTvSubtitleFileDetails) =
            match file with
            | PsTvSubtitleFileDetails.V1 file ->
                {
                    TvSubtitlesFileDetails.Function = file.Function
                    LanguageCode = file.LanguageCode
                    FilePath = file.FilePath
                    SubtitlesLinks = file.SubtitlesLinks
                    Name = null
                    Roles = null
                    Version = file.Version
                }
            | PsTvSubtitleFileDetails.V2 file ->
                {
                    TvSubtitlesFileDetails.Function = null
                    LanguageCode = file.LanguageCode
                    FilePath = file.FilePath
                    SubtitlesLinks = file.SubtitlesLinks
                    Name = file.Name
                    Roles = file.Roles |> List.toArrayOrNull
                    Version = file.Version
                }


        member this.ToDomain() : PsTvSubtitleFileDetails =
            if this.Name |> String.isNullOrEmpty then
                PsTvSubtitleFileDetails.V1
                    {
                        Function = this.Function
                        LanguageCode = this.LanguageCode
                        FilePath = this.FilePath
                        SubtitlesLinks = this.SubtitlesLinks
                        Version = this.Version
                    }
            else
                PsTvSubtitleFileDetails.V2
                    {
                        LanguageCode = this.LanguageCode
                        FilePath = this.FilePath
                        SubtitlesLinks = this.SubtitlesLinks
                        Name = this.Name
                        Roles = this.Roles |> List.ofArrayOrNull
                        Version = this.Version
                    }

    [<ProtoContract; CLIMutable; NoComparison>]
    type TvSubtitlesFileSetDetails =
        {
            [<ProtoMember(1)>]
            ProgramId: string
            [<ProtoMember(2)>]
            CarrierIds: string array
            [<ProtoMember(3)>]
            TranscodingVersion: int
            [<ProtoMember(4)>]
            Version: int
            [<ProtoMember(5)>]
            SubtitlesFiles: TvSubtitlesFileDetails array
            [<ProtoMember(6)>]
            PublishingPriority: int
        }

        interface IProtoBufSerializable
        static member FromDomain(job: PsSubtitleFilesDetails) =
            {
                ProgramId = job.ProgramId
                CarrierIds = job.CarrierIds |> List.toArrayOrNull
                TranscodingVersion = job.TranscodingVersion
                Version = job.Version
                SubtitlesFiles = job.Subtitles |> List.map TvSubtitlesFileDetails.FromDomain |> List.toArrayOrNull
                PublishingPriority = job.Priority |> convertFromPublishingPriority
            }
        member this.ToDomain() : PsSubtitleFilesDetails =
            {
                ProgramId = this.ProgramId
                CarrierIds = this.CarrierIds |> List.ofArrayOrNull
                TranscodingVersion = this.TranscodingVersion
                Version = this.Version
                Subtitles = this.SubtitlesFiles |> List.ofArrayOrNull |> List.map _.ToDomain()
                Priority = this.PublishingPriority |> convertToPublishingPriority
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type LegacySubtitlesFile =
        {
            [<ProtoMember(1)>]
            LanguageType: string
            [<ProtoMember(2)>]
            LanguageCode: string
            [<ProtoMember(3)>]
            FilePath: string
        }

        interface IProtoBufSerializable
        static member FromDomain(file: PsLegacySubtitlesFile) =
            {
                LanguageType = file.LanguageType
                LanguageCode = file.LanguageCode
                FilePath = file.FilePath
            }

        member this.ToDomain() : PsLegacySubtitlesFile =
            {
                LanguageType = this.LanguageType
                LanguageCode = this.LanguageCode
                FilePath = this.FilePath
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type TranscodedAudioFile =
        {
            [<ProtoMember(1)>]
            BitRate: int
            [<ProtoMember(2)>]
            Duration: int
            [<ProtoMember(3)>]
            FilePath: string
        }

        interface IProtoBufSerializable
        static member FromDomain(ptf: PsTranscodedAudioFile) =
            {
                BitRate = ptf.BitRate
                Duration = ptf.Duration
                FilePath = ptf.FilePath
            }
        member this.ToDomain() : PsTranscodedAudioFile =
            {
                Duration = this.Duration
                BitRate = this.BitRate
                FilePath = this.FilePath
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type LegacyAudioFile =
        {
            [<ProtoMember(1)>]
            BitRate: int
            [<ProtoMember(3)>]
            Duration: int
            [<ProtoMember(2)>]
            FilePath: string
        } // Last protobuf index: 3

        interface IProtoBufSerializable
        static member FromDomain(ptf: PsLegacyAudioFile) =
            {
                BitRate = ptf.BitRate
                Duration = ptf.Duration
                FilePath = ptf.FilePath
            }
        member this.ToDomain() : PsLegacyAudioFile =
            {
                BitRate = this.BitRate
                Duration = this.Duration
                FilePath = this.FilePath
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type TranscodedAudioFileSet =
        {
            [<ProtoMember(1)>]
            ProgramId: string
            [<ProtoMember(2)>]
            CarrierId: string
            [<ProtoMember(3)>]
            Files: TranscodedAudioFile array
            [<ProtoMember(4)>]
            Version: int
        }

        interface IProtoBufSerializable
        static member FromDomain(job: PsAudioTranscodingJob) =
            {
                ProgramId = job.ProgramId
                CarrierId = job.CarrierId
                Files = job.Files |> List.map TranscodedAudioFile.FromDomain |> List.toArrayOrNull
                Version = job.Version
            }
        member this.ToDomain() : PsAudioTranscodingJob =
            {
                ProgramId = this.ProgramId
                CarrierId = this.CarrierId
                Files = this.Files |> List.ofArrayOrNull |> List.map _.ToDomain()
                Version = this.Version
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type LegacyAudioFileSet =
        {
            [<ProtoMember(1)>]
            ProgramId: string
            [<ProtoMember(2)>]
            CarrierId: string
            [<ProtoMember(3)>]
            Files: LegacyAudioFile array
        }

        interface IProtoBufSerializable
        static member FromDomain(job: PsLegacyAudioDetails) =
            {
                ProgramId = job.ProgramId
                CarrierId = job.CarrierId
                Files = job.Files |> List.map LegacyAudioFile.FromDomain |> List.toArrayOrNull
            }
        member this.ToDomain() : PsLegacyAudioDetails =
            {
                ProgramId = this.ProgramId
                CarrierId = this.CarrierId
                Files = this.Files |> List.ofArrayOrNull |> List.map _.ToDomain()
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    [<ProtoReserved(2, "AckId")>]
    [<ProtoReserved(4, "ConfirmationId")>]
    type ReceivedVideoTranscodingJob =
        {
            [<ProtoMember(1)>]
            Files: TranscodedVideoFileSet
            [<ProtoMember(3)>]
            Timestamp: DateTimeOffset // Added 29.01.2021
            [<ProtoMember(5)>] // Added 24.11.2022
            PublishingPriority: string
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(job, timestamp) : ReceivedVideoTranscodingJob =
            {
                Files = TranscodedVideoFileSet.FromDomain(job)
                Timestamp = timestamp
                PublishingPriority = job.PublishingPriority |> Option.map getUnionCaseName |> Option.toObj
            }
        static member FromDomain(job: PsVideoTranscodingJob) =
            ReceivedVideoTranscodingJob.FromDomainWithTimestamp(job, DateTimeOffset.Now)
        member this.ToDomain() : PsVideoTranscodingJob = this.Files.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type ReceivedVideoMigrationJob =
        {
            [<ProtoMember(1)>]
            Files: LegacyVideoFileSet
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(job, timestamp) : ReceivedVideoMigrationJob =
            {
                Files = LegacyVideoFileSet.FromDomain(job)
                Timestamp = timestamp
            }
        static member FromDomain(job) =
            ReceivedVideoMigrationJob.FromDomainWithTimestamp(job, DateTimeOffset.Now)
        member this.ToDomain() = this.Files.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type ReceivedAudioMigrationJob =
        {
            [<ProtoMember(1)>]
            Files: LegacyAudioFileSet
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(job, timestamp) : ReceivedAudioMigrationJob =
            {
                Files = LegacyAudioFileSet.FromDomain(job)
                Timestamp = timestamp
            }
        static member FromDomain(job) =
            ReceivedAudioMigrationJob.FromDomainWithTimestamp(job, DateTimeOffset.Now)
        member this.ToDomain() = this.Files.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type ReceivedTvSubtitlesJob =
        {
            [<ProtoMember(1)>]
            TvSubtitles: TvSubtitlesFileSetDetails
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(job: PsSubtitleFilesDetails) =
            {
                TvSubtitles = job |> TvSubtitlesFileSetDetails.FromDomain
                Timestamp = DateTimeOffset.Now
            }
        member this.ToDomain() : PsSubtitleFilesDetails = this.TvSubtitles.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type ReceivedSubtitlesMigrationJob =
        {
            [<ProtoMember(1)>]
            Subtitles: LegacySubtitlesFile array
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(job, timestamp) : ReceivedSubtitlesMigrationJob =
            {
                Subtitles = job |> List.map LegacySubtitlesFile.FromDomain |> List.toArrayOrNull
                Timestamp = timestamp
            }
        static member FromDomain(job) =
            ReceivedSubtitlesMigrationJob.FromDomainWithTimestamp(job, DateTimeOffset.Now)
        member this.ToDomain() =
            this.Subtitles |> List.ofArrayOrNull |> List.map _.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type ProgramCarrier =
        {
            [<ProtoMember(1)>]
            CarrierId: string
            [<ProtoMember(2)>]
            PartNumber: int
        }

        static member FromDomain(carrier: PsProgramCarrier) =
            {
                CarrierId = carrier.CarrierId
                PartNumber = carrier.PartNumber
            }
        member this.ToDomain() : PsProgramCarrier =
            {
                CarrierId = this.CarrierId
                PartNumber = this.PartNumber
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignedActiveCarriers =
        {
            [<ProtoMember(1)>]
            ProgramId: string
            [<ProtoMember(2)>]
            Carriers: ProgramCarrier array
            [<ProtoMember(3)>]
            Timestamp: DateTimeOffset
            [<ProtoMember(4)>]
            PublishingPriority: int
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp

        static member FromDomain(event: PsProgramGotActiveCarriersEvent) =
            {
                ProgramId = event.ProgramId
                Carriers = event.Carriers |> List.map ProgramCarrier.FromDomain |> List.toArray
                Timestamp = event.Timestamp
                PublishingPriority = event.PublishingPriority |> convertFromPublishingPriority
            }

        member this.ToDomain() : PsProgramGotActiveCarriersEvent =
            {
                ProgramId = this.ProgramId
                Carriers = this.Carriers |> List.ofArrayOrNull |> List.map _.ToDomain()
                Timestamp = this.Timestamp
                PublishingPriority = this.PublishingPriority |> convertToPublishingPriority
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type ReceivedAudioTranscodingJob =
        {
            [<ProtoMember(1)>]
            Files: TranscodedAudioFileSet
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(job, timestamp) =
            {
                Files = TranscodedAudioFileSet.FromDomain(job)
                Timestamp = timestamp
            }
        static member FromDomain(job: PsAudioTranscodingJob) =
            ReceivedAudioTranscodingJob.FromDomainWithTimestamp(job, DateTimeOffset.Now)
        member this.ToDomain() : PsAudioTranscodingJob = this.Files.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type ArchivedTranscodedFile =
        {
            [<ProtoMember(1)>]
            SourcePath: string
            [<ProtoMember(2)>]
            ArchivePath: string
            [<ProtoMember(3)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(file: PsArchivedFile, timestamp) : ArchivedTranscodedFile =
            {
                SourcePath = file.SourcePath
                ArchivePath = file.ArchivePath
                Timestamp = timestamp
            }
        static member FromDomain(file) =
            ArchivedTranscodedFile.FromDomainWithTimestamp(file, DateTimeOffset.Now)
        member this.ToDomain() : PsArchivedFile =
            {
                SourcePath = this.SourcePath
                ArchivePath = this.ArchivePath
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type UsageRights =
        {
            [<ProtoMember(1)>]
            Region: string
            [<ProtoMember(2)>]
            PublishStart: DateTime
            [<ProtoMember(3)>]
            PublishEnd: DateTime
        }

        static member FromDomain(rights: PsUsageRights) =
            {
                Region = rights.Region
                PublishStart = rights.PublishStart
                PublishEnd = rights.PublishEnd |> Option.defaultValue DateTime.MinValue
            }
        member this.ToDomain() : PsUsageRights =
            {
                Region = this.Region
                PublishStart = this.PublishStart
                PublishEnd =
                    if this.PublishEnd = DateTime.MinValue then
                        None
                    else
                        Some this.PublishEnd
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignedUsageRights =
        {
            [<ProtoMember(1)>]
            Rights: UsageRights array
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(rights, timestamp) =
            {
                Rights = rights |> List.map UsageRights.FromDomain |> List.toArrayOrNull
                Timestamp = timestamp
            }
        static member FromDomain rights =
            AssignedUsageRights.FromDomainWithTimestamp(rights, DateTimeOffset.Now)
        member this.ToDomain() =
            this.Rights |> List.ofArrayOrNull |> List.map _.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    [<ProtoReserved(3, "Rights")>]
    type FilesState =
        {
            [<ProtoMember(1)>]
            ProgramId: string
            [<ProtoMember(5)>]
            MediaType: int
            [<ProtoMember(2)>]
            VideoFiles: TranscodedVideoFileSet array
            [<ProtoMember(10)>]
            LegacyVideoFiles: LegacyVideoFileSet array
            [<ProtoMember(4)>]
            AudioFiles: TranscodedAudioFileSet array
            [<ProtoMember(11)>]
            LegacyAudioFiles: LegacyAudioFileSet array
            [<ProtoMember(6)>]
            Subtitles: TvSubtitlesFileSetDetails array
            [<ProtoMember(12)>]
            LegacySubtitles: LegacySubtitlesFile array
            [<ProtoMember(7)>]
            ActiveCarriers: ProgramCarrier array
            [<ProtoMember(8)>]
            ActiveCarrierAssignmentTimestamp: DateTimeOffset
            [<ProtoMember(13)>]
            Rights: UsageRights array
            [<ProtoMember(9)>]
            LastSequenceNr: int64
        } // Last protobuf index: 13

        interface IProtoBufSerializable
        static member FromDomain(state: PsFilesState) =
            {
                ProgramId = state.ProgramId |> Option.map PiProgId.value |> Option.defaultValue null
                MediaType = state.MediaType |> Option.map convertFromMediaType |> Option.defaultValue UnspecifiedValue
                VideoFiles =
                    let videoSets = state.FileSets |> Map.filter (fun _ v -> v.TranscodedFiles.IsVideo)
                    if videoSets = Map.empty then
                        null
                    else
                        videoSets
                        |> Map.toList
                        |> List.map (fun (carrierId, fileSet) ->
                            {
                                ProgramId = fileSet.ProgramId
                                CarrierIds_Deprecated = fileSet.CarrierIds |> List.toArrayOrNull
                                CarrierId_Deprecated = CarrierId.value carrierId
                                Carriers = fileSet.Carriers |> List.map TranscodedCarrier.FromDomain |> List.toArrayOrNull
                                Files =
                                    match fileSet.TranscodedFiles with
                                    | PsTranscodedFiles.Video files -> files |> List.map TranscodedVideoFile.FromDomain |> List.toArray
                                    | _ -> null
                                PublishingPriority = fileSet.PublishingPriority |> Option.map convertFromPublishingPriority |> Option.defaultValue 0
                                Version = fileSet.Version
                            })
                        |> List.toArray
                LegacyVideoFiles =
                    let legacyVideoSets = state.FileSets |> Map.filter (fun _ v -> v.TranscodedFiles.IsLegacyVideo)
                    if legacyVideoSets = Map.empty then
                        null
                    else
                        legacyVideoSets
                        |> Map.toList
                        |> List.map (fun (carrierId, fileSet) ->
                            {
                                LegacyVideoFileSet.ProgramId = fileSet.ProgramId
                                CarrierId = CarrierId.value carrierId
                                Files =
                                    match fileSet.TranscodedFiles with
                                    | PsTranscodedFiles.LegacyVideo files -> files |> List.map LegacyVideoFile.FromDomain |> List.toArray
                                    | _ -> null
                            })
                        |> List.toArray
                AudioFiles =
                    let audioSets = state.FileSets |> Map.filter (fun _ v -> v.TranscodedFiles.IsAudio)
                    if audioSets = Map.empty then
                        null
                    else
                        audioSets
                        |> Map.toList
                        |> List.map (fun (carrierId, fileSet) ->
                            {
                                ProgramId = fileSet.ProgramId
                                CarrierId = CarrierId.value carrierId
                                Files =
                                    match fileSet.TranscodedFiles with
                                    | PsTranscodedFiles.Audio files -> files |> List.map TranscodedAudioFile.FromDomain |> List.toArray
                                    | _ -> null
                                Version = fileSet.Version
                            })
                        |> List.toArray
                LegacyAudioFiles =
                    let legacyAudioSets = state.FileSets |> Map.filter (fun _ v -> v.TranscodedFiles.IsLegacyAudio)
                    if legacyAudioSets = Map.empty then
                        null
                    else
                        legacyAudioSets
                        |> Map.toList
                        |> List.map (fun (carrierId, fileSet) ->
                            {
                                LegacyAudioFileSet.ProgramId = fileSet.ProgramId
                                CarrierId = CarrierId.value carrierId
                                Files =
                                    match fileSet.TranscodedFiles with
                                    | PsTranscodedFiles.LegacyAudio files -> files |> List.map LegacyAudioFile.FromDomain |> List.toArray
                                    | _ -> null
                            })
                        |> List.toArray
                Subtitles =
                    state.SubtitlesSets
                    |> List.choose (function
                        | PsSubtitlesSet.SubtitlesSet x -> Some x
                        | _ -> None)
                    |> List.map (fun subtitlesSet ->
                        {
                            TvSubtitlesFileSetDetails.ProgramId = subtitlesSet.ProgramId
                            CarrierIds = subtitlesSet.CarrierIds |> List.toArray
                            TranscodingVersion = subtitlesSet.TranscodingVersion
                            Version = subtitlesSet.Version
                            SubtitlesFiles = subtitlesSet.Subtitles |> List.map TvSubtitlesFileDetails.FromDomain |> List.toArray
                            PublishingPriority = convertFromPublishingPriority subtitlesSet.Priority
                        })
                    |> List.toArrayOrNull
                LegacySubtitles =
                    state.SubtitlesSets
                    |> List.choose (function
                        | PsSubtitlesSet.LegacySubtitlesSet x -> Some x
                        | _ -> None)
                    |> List.concat
                    |> List.map LegacySubtitlesFile.FromDomain
                    |> List.toArrayOrNull

                ActiveCarriers =
                    (state.ActiveCarriers
                     |> function
                         | None -> []
                         | Some(ac, _) -> ac)
                    |> List.map ProgramCarrier.FromDomain
                    |> List.toArrayOrNull
                ActiveCarrierAssignmentTimestamp =
                    match state.ActiveCarriers with
                    | None -> DateTimeOffset.MinValue
                    | Some(_, dt) -> dt
                Rights = state.Rights |> List.map UsageRights.FromDomain |> List.toArrayOrNull
                LastSequenceNr = state.LastSequenceNr
            }
        member this.ToDomain() : PsFilesState =
            let mediaType =
                if this.MediaType = UnspecifiedValue then
                    if this.VideoFiles <> null || this.LegacyVideoFiles <> null then
                        Some MediaType.Video
                    else if this.AudioFiles <> null then
                        Some MediaType.Audio
                    else
                        None
                else
                    this.MediaType |> convertToMediaType |> Some
            let fileSets =
                if this.VideoFiles <> null then
                    this.VideoFiles
                    |> List.ofArrayOrNull
                    |> List.map _.ToDomain()
                    |> List.map (fun x ->
                        if x.Carriers.Length = 0 then
                            invalidOp "Empty part list"
                        else
                            CarrierId.create (x.CarrierIds |> List.head),
                            {
                                PsFileSet.TranscodedFiles = PsTranscodedFiles.Video x.Files
                                ProgramId = this.ProgramId
                                Carriers = x.Carriers
                                PublishingPriority = x.PublishingPriority
                                Version = x.Version
                            })
                else if this.LegacyVideoFiles <> null then
                    this.LegacyVideoFiles
                    |> List.ofArrayOrNull
                    |> List.map _.ToDomain()
                    |> List.map (fun x ->
                        CarrierId.create x.CarrierId,
                        {
                            PsFileSet.TranscodedFiles = PsTranscodedFiles.LegacyVideo x.Files
                            ProgramId = this.ProgramId
                            Carriers = [ PsTranscodedCarrier.fromCarrierId x.CarrierId ]
                            PublishingPriority = None
                            Version = 0
                        })
                else if this.AudioFiles <> null then
                    this.AudioFiles
                    |> List.ofArrayOrNull
                    |> List.map _.ToDomain()
                    |> List.map (fun x ->
                        CarrierId.create x.CarrierId,
                        {
                            PsFileSet.TranscodedFiles = PsTranscodedFiles.Audio x.Files
                            ProgramId = this.ProgramId
                            Carriers = [ PsTranscodedCarrier.fromCarrierId x.CarrierId ]
                            PublishingPriority = None
                            Version = 0
                        })
                else if this.LegacyAudioFiles <> null then
                    this.LegacyAudioFiles
                    |> List.ofArrayOrNull
                    |> List.map _.ToDomain()
                    |> List.map (fun x ->
                        CarrierId.create x.CarrierId,
                        {
                            PsFileSet.TranscodedFiles = PsTranscodedFiles.LegacyAudio x.Files
                            ProgramId = this.ProgramId
                            Carriers = [ PsTranscodedCarrier.fromCarrierId x.CarrierId ]
                            PublishingPriority = None
                            Version = 0
                        })
                else
                    []
                |> Map.ofList
            let subtitlesSets =
                this.Subtitles
                |> List.ofArrayOrNull
                |> List.map (fun x ->
                    {
                        PsTvSubtitlesSet.Priority = x.PublishingPriority |> convertToPublishingPriority
                        Subtitles = x.SubtitlesFiles |> List.ofArrayOrNull |> List.map _.ToDomain()
                        Version = x.Version
                        CarrierIds = x.CarrierIds |> List.ofArrayOrNull
                        ProgramId = x.ProgramId
                        TranscodingVersion = x.TranscodingVersion
                    })
                |> List.map PsSubtitlesSet.SubtitlesSet
            let legacySubtitlesSetList = this.LegacySubtitles |> List.ofArrayOrNull |> List.map _.ToDomain()

            {
                ProgramId = Option.ofObj this.ProgramId |> Option.map PiProgId.create
                MediaType = mediaType
                FileSets = fileSets
                SubtitlesSets =
                    if Seq.isEmpty legacySubtitlesSetList then
                        subtitlesSets
                    else
                        (PsSubtitlesSet.LegacySubtitlesSet legacySubtitlesSetList) :: subtitlesSets
                ActiveCarriers =
                    if this.ActiveCarriers = null then
                        None
                    else
                        Some(this.ActiveCarriers |> List.ofArrayOrNull |> List.map (_.ToDomain()), this.ActiveCarrierAssignmentTimestamp)
                Rights = this.Rights |> List.ofArrayOrNull |> List.map _.ToDomain()
                LastSequenceNr = this.LastSequenceNr
                RestoredSnapshot = false
                SavedSnapshot = false
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type ScheduledExecution =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializable
        static member FromDomain(timestamp: DateTimeOffset) = { Timestamp = timestamp }

    [<ProtoContract; CLIMutable; NoComparison>]
    type PendingEvent =
        {
            [<ProtoMember(1)>]
            EventId: string
            [<ProtoMember(2)>]
            Event: string
            [<ProtoMember(3)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromPlayabilityEvent(event: PendingPlayabilityEvent) =
            {
                EventId = event.EventId
                Event = event.Event |> Serialization.Newtonsoft.serializeObject SerializationSettings.PascalCase
                Timestamp = event.Timestamp
            }
        member this.ToPlayabilityEvent() : PendingPlayabilityEvent =
            {
                EventId = this.EventId
                Event = this.Event |> Serialization.Newtonsoft.deserializeObject SerializationSettings.PascalCase
                Timestamp = this.Timestamp
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type CompletedEvent =
        {
            [<ProtoMember(1)>]
            EventId: string
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(eventId, timestamp) =
            {
                EventId = eventId
                Timestamp = timestamp
            }
        member this.ToDomain() = this.EventId, this.Timestamp

    [<ProtoContract; CLIMutable; NoComparison>]
    type CancelledEvent =
        {
            [<ProtoMember(1)>]
            EventId: string
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(eventId, timestamp) =
            {
                EventId = eventId
                Timestamp = timestamp
            }
        member this.ToDomain() = this.EventId, this.Timestamp

    [<ProtoContract; CLIMutable; NoComparison>]
    type PlayabilityHandlerState =
        {
            [<ProtoMember(1)>]
            ProgramId: string
            [<ProtoMember(2)>]
            PendingEvent: PendingEvent array
            [<ProtoMember(3)>]
            MediaType: int
        }

        interface IProtoBufSerializable
        static member FromDomain(state: PsPlayabilityHandlerState) =
            {
                ProgramId = state.ProgramId |> Option.map PiProgId.value |> Option.defaultValue null
                PendingEvent = state.PendingEvent |> Option.map PendingEvent.FromPlayabilityEvent |> Option.toArrayOrNull
                MediaType = state.MediaType |> Option.map convertFromMediaType |> Option.defaultValue UnspecifiedValue
            }
        member this.ToDomain() : PsPlayabilityHandlerState =
            {
                ProgramId = Option.ofObj this.ProgramId |> Option.map PiProgId.create
                MediaType =
                    match this.MediaType with
                    | UnspecifiedValue -> None
                    | mediaType -> convertToMediaType mediaType |> Some
                PendingEvent = this.PendingEvent |> Option.ofArrayOrNull |> Option.map _.ToPlayabilityEvent()
                ReminderStarted = false
                PersistedEvents = List.empty
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type DeprecatedEvent =
        {
            [<ProtoMember(0)>]
            Deprecated: string
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = DateTimeOffset.MinValue

        static member Instance = { Deprecated = null }

    type ProtobufSerializer(system: ExtendedActorSystem) =
        inherit Akka.Serialization.SerializerWithStringManifest(system)

        static member SupportedTypes =
            [
                typedefof<AssignedProgramId>
                typedefof<AssignedMediaType>
                typedefof<ReceivedVideoTranscodingJob>
                typedefof<ReceivedVideoMigrationJob>
                typedefof<ReceivedAudioTranscodingJob>
                typedefof<ReceivedAudioMigrationJob>
                typedefof<ReceivedTvSubtitlesJob>
                typedefof<ReceivedSubtitlesMigrationJob>
                typedefof<ArchivedTranscodedFile>
                typedefof<FilesState>
                typedefof<ScheduledExecution>
                typedefof<PendingEvent>
                typedefof<CompletedEvent>
                typedefof<CancelledEvent>
                typedefof<PlayabilityHandlerState>
                typedefof<AssignedActiveCarriers>
                typedefof<AssignedUsageRights>
                typedefof<DeprecatedEvent>
            ]

        static member private DeprecatedManifests = [ ("AssignedBroadcastingRights", typedefof<DeprecatedEvent>) ]

        override this.ToBinary(o: obj) =
            use stream = new MemoryStream()
            Serializer.Serialize(stream, o)
            stream.ToArray()

        override this.FromBinary(bytes: byte[], manifest: string) =

            let typ =
                ProtobufSerializer.SupportedTypes
                |> List.tryFind (fun t -> manifest = t.Name || manifest = t.FullName || manifest = t.TypeQualifiedName())
                |> Option.defaultWith (fun () ->
                    ProtobufSerializer.DeprecatedManifests
                    |> List.tryFind (fun (m, _) -> m = manifest)
                    |> Option.map snd
                    |> Option.defaultWith (fun () -> notSupported $"Serializer doesn't support manifest %s{manifest}"))

            use stream = new MemoryStream(bytes)
            Serializer.Deserialize(typ, stream)

        override this.Manifest(o: obj) =
            if ProtobufSerializer.SupportedTypes |> List.contains (o.GetType()) then
                o.GetType().Name
            else
                notSupported $"Serializer doesn't support type %s{o.GetType().FullName}"

        override this.Identifier = 129

    type EventAdapter(system: ExtendedActorSystem) =

        let ser = ProtobufSerializer(system)

        interface Akka.Persistence.Journal.IEventAdapter with

            member _.Manifest(evt: obj) = ser.Manifest(evt)

            member _.ToJournal(evt: obj) : obj =
                Akka.Persistence.Journal.Tagged(box evt, [| "any" |]) :> obj

            member _.FromJournal(evt: obj, _: string) : Akka.Persistence.Journal.IEventSequence =
                if evt :? IProtoBufSerializable then
                    Akka.Persistence.Journal.EventSequence.Single(evt :?> IProtoBufSerializable)
                else
                    Akka.Persistence.Journal.EventSequence.Empty
