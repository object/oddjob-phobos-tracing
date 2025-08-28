namespace Nrk.Oddjob.Core.Dto

[<RequireQualifiedAccess>]
module MediaSet =

    open System
    open ProtoBuf
    open Nrk.Oddjob.Core.MediaSetTypes
    open Nrk.Oddjob.Core

    [<Literal>]
    let private UnspecifiedValue = 0

    let private immutableDistributionStateMapping =
        // The following mappings are immutable: new values can be added, but once a numeric value is mapped it can not be changed
        [
            (0, DistributionState.None)
            (1, DistributionState.Requested_Deprecated)
            (2, DistributionState.Initiated)
            (3, DistributionState.Ingesting_Deprecated)
            (4, DistributionState.Segmenting_Deprecated)
            (5, DistributionState.Completed)
            (6, DistributionState.Deleted)
            (7, DistributionState.Cancelled)
            (8, DistributionState.Failed)
            (9, DistributionState.Rejected)
        ]

    let private immutableAudioModeMapping =
        // The following mappings are immutable: new values can be added, but once a numeric value is mapped it can not be changed
        [ (0, AudioMode.Default); (1, AudioMode.Audio51) ]

    let private immutableMediaTypeMapping =
        // The following mappings are immutable: new values can be added, but once a numeric value is mapped it can not be changed
        [ (1, MediaType.Video); (2, MediaType.Audio) ]

    let private immutableMixdownMapping =
        // The following mappings are immutable: new values can be added, but once a numeric value is mapped it can not be changed
        [ (1, Mixdown.Stereo); (2, Mixdown.Surround) ]

    let private immutableGeoRestrictionMapping =
        // The following mappings are immutable: new values can be added, but once a numeric value is mapped it can not be changed
        [
            (0, GeoRestriction.Unspecified)
            (1, GeoRestriction.World)
            (2, GeoRestriction.NRK)
            (3, GeoRestriction.Norway)
        ]

    let private immutableOriginMapping =
        // The following mappings are immutable: new values can be added, but once a numeric value is mapped it can not be changed
        // Value 1 is deprecated Origin.Akamai
        // Value 2 is deprecated Origin.Nep
        [ (3, Origin.GlobalConnect) ]

    let private immutableStreamingTypeMapping =
        // The following mappings are immutable: new values can be added, but once a numeric value is mapped it can not be changed
        [ (0, StreamingType.OnDemand); (1, StreamingType.Live) ]

    let private immutableSubtitlesLinksFormatMapping =
        // The following mappings are immutable: new values can be added, but once a numeric value is mapped it can not be changed
        [
            (0, SubtitlesLinkFormat.M3U8)
            (1, SubtitlesLinkFormat.TTML)
            (2, SubtitlesLinkFormat.WebVTT)
        ]

    // These are frozen deprecated literals from the settings. They were in use when
    // FilePath held a RootPath token and are to be removed on the next migration.
    [<Literal>]
    let private PsArchive = @"\\felles.ds.nrk.no\nrk\produksjonsdata\odadistribusjon\"

    [<Literal>]
    let private DropFolder = @"\\felles.ds.nrk.no\nrk\produksjonsdata\odadistribusjon\_incoming\"

    [<Literal>]
    let private PsTestArchive = @"\\maodatest03.felles.ds.nrk.no\MediaShare\"

    [<Literal>]
    let private PotionArchive = @"\\felles.ds.nrk.no\produksjon\potion\"

    [<Literal>]
    let private LocalFolder = @"C:\"

    [<Literal>]
    let private PotionTestArchive = @"\\manas01.felles.ds.nrk.no\Media_share\Potion"

    let private immutableRootPathMapping =
        // The following mappings are immutable: new values can be added, but once a numeric value is mapped it can not be changed
        [
            (0, None)
            (1, Some PsArchive)
            (2, Some DropFolder)
            (3, Some PsTestArchive)
            (4, Some PotionArchive)
            (5, Some LocalFolder)
            (6, Some PotionTestArchive)
        ]

    let convertFromDistributionState value =
        value |> convertOrFail (tryConvertFromDomainValue immutableDistributionStateMapping)

    let convertToDistributionState value =
        value |> convertOrFail (tryConvertToDomainValue immutableDistributionStateMapping)

    let convertFromAudioMode value =
        value |> convertOrFail (tryConvertFromDomainValue immutableAudioModeMapping)

    let convertToAudioMode value =
        value |> convertOrFail (tryConvertToDomainValue immutableAudioModeMapping)

    let convertFromMediaType value =
        value |> convertOrFail (tryConvertFromDomainValue immutableMediaTypeMapping)

    let convertToMediaType value =
        value |> convertOrFail (tryConvertToDomainValue immutableMediaTypeMapping)

    let convertFromMixdown value =
        value |> convertOrFail (tryConvertFromDomainValue immutableMixdownMapping)

    let convertToMixdown value =
        value |> convertOrFail (tryConvertToDomainValue immutableMixdownMapping)

    let convertFromGeoRestriction value =
        value |> convertOrFail (tryConvertFromDomainValue immutableGeoRestrictionMapping)

    let convertToGeoRestriction value =
        value |> convertOrFail (tryConvertToDomainValue immutableGeoRestrictionMapping)

    let convertFromOrigin value =
        value |> convertOrFail (tryConvertFromDomainValue immutableOriginMapping)

    let convertToOrigin value =
        value |> convertOrFail (tryConvertToDomainValue immutableOriginMapping)

    let convertFromStreamingType value =
        value |> convertOrFail (tryConvertFromDomainValue immutableStreamingTypeMapping)

    let convertToStreamingType value =
        value |> convertOrFail (tryConvertToDomainValue immutableStreamingTypeMapping)

    let convertFromSubtitlesLinksFormat value =
        value |> convertOrFail (tryConvertFromDomainValue immutableSubtitlesLinksFormatMapping)

    let convertToSubtitlesLinksFormat value =
        value |> convertOrFail (tryConvertToDomainValue immutableSubtitlesLinksFormatMapping)

    let private convertToRootPath value =
        value |> convertOrFail (tryConvertToDomainValue immutableRootPathMapping)

    [<ProtoContract; CLIMutable; NoComparison>]
    type DeprecatedEvent =
        {
            [<ProtoMember(0)>]
            Deprecated: string
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = DateTimeOffset.MinValue

        static member Instance = { Deprecated = null }

    type RemoteState =
        static member FromDomain(remoteState: MediaSetTypes.RemoteState) =
            remoteState.State |> convertFromDistributionState

    module MediaMode_Legacy =
        [<Literal>]
        let UnspecifiedValue = -1

    [<ProtoContract; CLIMutable; NoComparison>]
    type MediaMode_Legacy =
        {
            [<ProtoMember(1)>]
            AudioMode: int
            [<ProtoMember(2)>]
            VideoMode: int
        }

        interface IProtoBufSerializable
        static member Zero =
            {
                AudioMode = MediaMode_Legacy.UnspecifiedValue
                VideoMode = MediaMode_Legacy.UnspecifiedValue
            }
        member this.ToDomain() =
            match this.AudioMode, this.VideoMode with
            | MediaMode_Legacy.UnspecifiedValue, _ -> MediaType.Default
            | _, MediaMode_Legacy.UnspecifiedValue -> MediaType.Audio
            | _ -> MediaType.Video
        static member Mixdown(mode: MediaMode_Legacy) =
            match mode.AudioMode, mode.VideoMode with
            | MediaMode_Legacy.UnspecifiedValue, _ -> None
            | _ ->
                match convertToAudioMode mode.AudioMode with
                | AudioMode.Default -> Some Mixdown.Stereo
                | AudioMode.Audio51 -> Some Mixdown.Surround

    [<ProtoContract; CLIMutable; NoComparison>]
    type SetMediaTypeFromMediaMode =
        {
            [<ProtoMember(1)>]
            MediaMode: MediaMode_Legacy
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        member this.ToDomain() = this.MediaMode.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type SetMediaType =
        {
            [<ProtoMember(1)>]
            MediaType: int
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(mediaType: MediaType, timestamp: DateTimeOffset) =
            {
                MediaType = mediaType |> convertFromMediaType
                Timestamp = timestamp
            }
        static member FromDomain(mediaType: MediaType) =
            SetMediaType.FromDomainWithTimestamp(mediaType, DateTimeOffset.Now)
        member this.ToDomain() = this.MediaType |> convertToMediaType

    [<ProtoContract; CLIMutable; NoComparison>]
    type SetGeoRestriction =
        {
            [<ProtoMember(1)>]
            GeoRestriction: int
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(geoRestriction: GeoRestriction, timestamp: DateTimeOffset) =
            {
                GeoRestriction = geoRestriction |> convertFromGeoRestriction
                Timestamp = timestamp
            }
        static member FromDomain(geoRestriction: GeoRestriction) =
            SetGeoRestriction.FromDomainWithTimestamp(geoRestriction, DateTimeOffset.Now)
        member this.ToDomain() =
            this.GeoRestriction |> convertToGeoRestriction

    [<ProtoContract; CLIMutable; NoComparison>]
    [<ProtoReserved(2, "MediaAccess")>]
    type AccessRestrictions =
        {
            [<ProtoMember(1)>]
            GeoRestriction: int
        }

        interface IProtoBufSerializable
        static member FromDomain(geoRestriction: CoreTypes.GeoRestriction) =
            {
                GeoRestriction = convertFromGeoRestriction geoRestriction
            }
        member this.ToDomain() =
            this.GeoRestriction |> convertToGeoRestriction

    [<ProtoContract; CLIMutable; NoComparison>]
    type SetAccessRestrictions =
        {
            [<ProtoMember(1)>]
            AccessRestrictions: AccessRestrictions
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(getRestriction: CoreTypes.GeoRestriction, timestamp: DateTimeOffset) =
            {
                AccessRestrictions = AccessRestrictions.FromDomain getRestriction
                Timestamp = timestamp
            }
        static member FromDomain(geoRestriction: CoreTypes.GeoRestriction) =
            SetAccessRestrictions.FromDomainWithTimestamp(geoRestriction, DateTimeOffset.Now)
        member this.ToDomain() = this.AccessRestrictions.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type ThumbnailResolution =
        {
            [<ProtoMember(1)>]
            Width: int
            [<ProtoMember(2)>]
            Height: int
        }

        interface IProtoBufSerializable
        static member FromDomain(tr: MediaSetTypes.ThumbnailResolution) =
            { Width = tr.Width; Height = tr.Height }
        member this.ToDomain() : MediaSetTypes.ThumbnailResolution =
            {
                Width = this.Width
                Height = this.Height
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignPart =
        {
            [<ProtoMember(1)>]
            PartId: string
            [<ProtoMember(2)>]
            PartNumber: int
            [<ProtoMember(3)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(partId: PartId, partNumber: int, timestamp: DateTimeOffset) =
            {
                PartId = partId.Value
                PartNumber = partNumber
                Timestamp = timestamp
            }
        static member FromDomain(partId: PartId, partNumber: int) =
            AssignPart.FromDomainWithTimestamp(partId, partNumber, DateTimeOffset.Now)
        member this.ToDomain() =
            PartId.create this.PartId, this.PartNumber

    [<ProtoContract; CLIMutable; NoComparison>]
    type VideoProperties =
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
        static member FromDomain(video: MediaSetTypes.VideoProperties) =
            {
                Codec = video.Codec
                DynamicRangeProfile = video.DynamicRangeProfile
                DisplayAspectRatio = video.DisplayAspectRatio
                Width = video.Width
                Height = video.Height
                FrameRate = video.FrameRate
            }
        member this.ToDomain() : MediaSetTypes.VideoProperties =
            {
                Codec = this.Codec
                DynamicRangeProfile = this.DynamicRangeProfile
                DisplayAspectRatio = this.DisplayAspectRatio
                Width = this.Width
                Height = this.Height
                FrameRate = this.FrameRate
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type AudioProperties =
        {
            [<ProtoMember(1)>]
            Mixdown_Deprecated: string // Replaced with Mixdown of int
            [<ProtoMember(2)>]
            Mixdown: int
        }

        interface IProtoBufSerializable
        static member FromDomain(audio: MediaSetTypes.AudioProperties) =
            {
                Mixdown_Deprecated = null
                Mixdown = convertFromMixdown audio.Mixdown
            }
        member this.ToDomain() : MediaSetTypes.AudioProperties =
            {
                Mixdown =
                    if this.Mixdown = UnspecifiedValue then
                        Mixdown.parse this.Mixdown_Deprecated
                    else
                        convertToMixdown this.Mixdown
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type MediaPropertiesV2 =
        {
            [<ProtoMember(1)>]
            BitRate: int
            [<ProtoMember(2)>]
            Duration: string
            [<ProtoMember(3)>]
            Video: VideoProperties
            [<ProtoMember(4)>]
            Audio: AudioProperties
            [<ProtoMember(5)>]
            TranscodingVersion: int
        }

        interface IProtoBufSerializable
        static member FromDomain(file: MediaSetTypes.MediaPropertiesV2) =
            {
                BitRate = file.BitRate
                Duration = file.Duration
                Video = VideoProperties.FromDomain file.Video
                Audio = AudioProperties.FromDomain file.Audio
                TranscodingVersion = file.TranscodingVersion
            }
        member this.ToDomain() : MediaSetTypes.MediaPropertiesV2 =
            {
                BitRate = this.BitRate
                Duration = this.Duration
                Video = this.Video.ToDomain()
                Audio = this.Audio.ToDomain()
                TranscodingVersion = this.TranscodingVersion
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type MediaPropertiesV1 =
        {
            [<ProtoMember(1)>]
            Mixdown: int
        }

        interface IProtoBufSerializable
        static member FromDomain(mixdown: MediaSetTypes.MediaPropertiesV1) =
            { Mixdown = convertFromMixdown mixdown }
        member this.ToDomain() : MediaSetTypes.MediaPropertiesV1 = convertToMixdown this.Mixdown

    [<ProtoContract; CLIMutable; NoComparison>]
    type FilePath =
        {
            [<ProtoMember(1)>]
            RelativePath: string
            [<ProtoMember(2)>]
            RootPath: int
        }

        interface IProtoBufSerializable
        static member FromDomain() = { RelativePath = null; RootPath = 0 }
        member this.ToDomain() : Nrk.Oddjob.Core.FilePath option =
            convertToRootPath this.RootPath
            |> Option.map (fun rootPath ->
                let relativePath = this.RelativePath.Replace(IO.LinuxDirectorySeparatorChar, IO.WindowsDirectorySeparatorChar)
                rootPath + relativePath |> FilePath.tryCreate)
            |> Option.flatten

    [<ProtoContract; CLIMutable; NoComparison>]
    type ContentFile =
        {
            [<ProtoMember(1)>]
            QualityId: int
            [<ProtoMember(2)>]
            FileName: string
            [<ProtoMember(3)>]
            SourcePath: string
            [<ProtoMember(4)>]
            MediaPropertiesV2: MediaPropertiesV2 array
            [<ProtoMember(5)>]
            MediaPropertiesV1: MediaPropertiesV1 array
            [<ProtoMember(6)>]
            SourcePathV2: FilePath array
        }

        interface IProtoBufSerializable
        static member FromDomain(file: MediaSetTypes.ContentFile) =
            let mediaPropertiesV1, mediaPropertiesV2 =
                match file.MediaProperties with
                | Unspecified -> null, null
                | MediaPropertiesV1 props -> [| MediaPropertiesV1.FromDomain props |], null
                | MediaPropertiesV2 props -> null, [| MediaPropertiesV2.FromDomain props |]
            {
                QualityId = file.QualityId.Value
                FileName = file.FileName.Value
                SourcePath = file.SourcePath |> Option.map FilePath.value |> Option.defaultValue null
                MediaPropertiesV2 = mediaPropertiesV2
                MediaPropertiesV1 = mediaPropertiesV1
                SourcePathV2 = null
            }
        member this.ToDomain() =
            let mediaProperties =
                let mediaPropertiesV1 =
                    this.MediaPropertiesV1 |> Option.ofArrayOrNull |> Option.map (fun (props: MediaPropertiesV1) -> props.ToDomain())
                let mediaPropertiesV2 =
                    this.MediaPropertiesV2 |> Option.ofArrayOrNull |> Option.map (fun (props: MediaPropertiesV2) -> props.ToDomain())
                match mediaPropertiesV2 with
                | Some props -> MediaPropertiesV2 props
                | None ->
                    match mediaPropertiesV1 with
                    | Some props -> MediaPropertiesV1 props
                    | None -> Unspecified
            {
                MediaSetTypes.QualityId = QualityId.create this.QualityId
                FileName = FileName.tryParse this.FileName |> Option.defaultValue (FileName String.Empty)
                SourcePath =
                    match Option.ofArrayOrNull this.SourcePathV2 with
                    | Some sourcePath -> sourcePath.ToDomain()
                    | None -> FilePath.tryCreate this.SourcePath
                MediaProperties = mediaProperties
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignFile =
        {
            [<ProtoMember(1)>]
            PartId: string
            [<ProtoMember(2)>]
            File: ContentFile
            [<ProtoMember(3)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(partId: PartId option, file: MediaSetTypes.ContentFile, timestamp: DateTimeOffset) =
            let partId = partId |> Option.map _.Value |> Option.defaultValue null
            {
                PartId = partId
                File = ContentFile.FromDomain file
                Timestamp = timestamp
            }
        static member FromDomain(partId: PartId option, file: MediaSetTypes.ContentFile) =
            AssignFile.FromDomainWithTimestamp(partId, file, DateTimeOffset.Now)
        member this.ToDomain() =
            let partId = PartId.ofString this.PartId
            let file = this.File.ToDomain()
            partId, file

    [<ProtoContract; CLIMutable; NoComparison>]
    type SubtitlesFile =
        {
            [<ProtoMember(1)>]
            LanguageCode: string
            [<ProtoMember(2)>]
            LegacyName: string
            [<ProtoMember(3)>]
            FileName: string
            [<ProtoMember(4)>]
            SourcePath: string
            [<ProtoMember(5)>]
            Version: int
            [<ProtoMember(6)>]
            Name: string
            [<ProtoMember(7)>]
            Roles: string array
        }

        interface IProtoBufSerializable
        static member FromDomain(sub: MediaSetTypes.SubtitlesFile) =
            match sub with
            | MediaSetTypes.SubtitlesFile.V1 sub ->
                {
                    LanguageCode = sub.LanguageCode.Value
                    LegacyName = sub.Name.Value
                    FileName = sub.FileName.Value
                    SourcePath = sub.SourcePath.Value
                    Version = sub.Version
                    Name = null
                    Roles = null
                }
            | MediaSetTypes.SubtitlesFile.V2 sub ->
                {
                    LanguageCode = sub.LanguageCode
                    LegacyName = null
                    FileName = sub.FileName.Value
                    SourcePath = sub.SourcePath.Value
                    Version = sub.Version
                    Name = sub.Name
                    Roles = sub.Roles |> List.toArrayOrNull
                }
        member this.ToDomain() : MediaSetTypes.SubtitlesFile =
            if String.isNotNullOrEmpty this.LegacyName then
                SubtitlesFile.V1
                    {
                        LanguageCode = Alphanumeric this.LanguageCode
                        Name = Alphanumeric this.LegacyName
                        FileName = FileName.tryParse this.FileName |> Option.defaultValue (FileName String.Empty)
                        SourcePath =
                            if this.SourcePath.Contains("://") then
                                SubtitlesLocation.AbsoluteUrl(AbsoluteUrl this.SourcePath)
                            else
                                SubtitlesLocation.FilePath(this.SourcePath |> Option.ofObj |> Option.bind FilePath.tryCreate)
                        Version = this.Version
                    }
            else
                SubtitlesFile.V2
                    {
                        LanguageCode = this.LanguageCode
                        Name = this.Name
                        Roles = this.Roles |> List.ofArrayOrNull
                        FileName = FileName.tryParse this.FileName |> Option.defaultValue (FileName String.Empty)
                        SourcePath =
                            if this.SourcePath.Contains("://") then
                                SubtitlesLocation.AbsoluteUrl(AbsoluteUrl this.SourcePath)
                            else
                                SubtitlesLocation.FilePath(this.SourcePath |> Option.ofObj |> Option.bind FilePath.tryCreate)
                        Version = this.Version
                    }

    [<ProtoContract; CLIMutable; NoComparison>]
    type SubtitlesLink =
        {
            [<ProtoMember(1)>]
            LanguageCode: string
            [<ProtoMember(2)>]
            Name: string
            [<ProtoMember(3)>]
            Format: int
            [<ProtoMember(4)>]
            SourcePath: string
            [<ProtoMember(5)>]
            Version: int
        }

        interface IProtoBufSerializable
        static member FromDomain(sub: MediaSetTypes.SubtitlesLink) =
            {
                LanguageCode = sub.LanguageCode.Value
                Name = sub.Name.Value
                Format = sub.Format |> convertFromSubtitlesLinksFormat
                SourcePath = sub.SourcePath.Value
                Version = sub.Version
            }
        member this.ToDomain() : MediaSetTypes.SubtitlesLink =
            {
                LanguageCode = Alphanumeric this.LanguageCode
                Name = Alphanumeric this.Name
                Format = this.Format |> convertToSubtitlesLinksFormat
                SourcePath = AbsoluteUrl this.SourcePath
                Version = this.Version
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignSubtitlesFile =
        {
            [<ProtoMember(1)>]
            SubtitlesFile: SubtitlesFile
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(sub: MediaSetTypes.SubtitlesFile, timestamp: DateTimeOffset) =
            {
                SubtitlesFile = SubtitlesFile.FromDomain sub
                Timestamp = timestamp
            }
        static member FromDomain(sub: MediaSetTypes.SubtitlesFile) =
            AssignSubtitlesFile.FromDomainWithTimestamp(sub, DateTimeOffset.Now)
        member this.ToDomain() = this.SubtitlesFile.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignSubtitlesFiles =
        {
            [<ProtoMember(1)>]
            SubtitlesFiles: SubtitlesFile array
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(subs: MediaSetTypes.SubtitlesFile list, timestamp: DateTimeOffset) =
            {
                SubtitlesFiles = subs |> List.map SubtitlesFile.FromDomain |> List.toArrayOrNull
                Timestamp = timestamp
            }
        static member FromDomain(subs: MediaSetTypes.SubtitlesFile list) =
            AssignSubtitlesFiles.FromDomainWithTimestamp(subs, DateTimeOffset.Now)
        member this.ToDomain() =
            this.SubtitlesFiles |> List.ofArrayOrNull |> List.map _.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignSubtitlesLinks =
        {
            [<ProtoMember(1)>]
            SubtitlesLinks: SubtitlesLink array
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp

        static member FromDomainWithTimestamp(subs: MediaSetTypes.SubtitlesLink list, timestamp: DateTimeOffset) =
            {
                SubtitlesLinks = subs |> List.map SubtitlesLink.FromDomain |> List.toArrayOrNull
                Timestamp = timestamp
            }
        static member FromDomain(subs: MediaSetTypes.SubtitlesLink list) =
            AssignSubtitlesLinks.FromDomainWithTimestamp(subs, DateTimeOffset.Now)
        member this.ToDomain() =
            this.SubtitlesLinks |> List.ofArrayOrNull |> List.map _.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type RemovePart =
        {
            [<ProtoMember(1)>]
            PartId: string
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(partId: PartId, timestamp: DateTimeOffset) =
            {
                PartId = partId.Value
                Timestamp = timestamp
            }
        static member FromDomain(partId: PartId) =
            RemovePart.FromDomainWithTimestamp(partId, DateTimeOffset.Now)
        member this.ToDomain() = PartId.create this.PartId

    [<ProtoContract; CLIMutable; NoComparison>]
    type RevokePart =
        {
            [<ProtoMember(1)>]
            PartId: string
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(partId: PartId, timestamp: DateTimeOffset) =
            {
                PartId = partId.Value
                Timestamp = timestamp
            }
        static member FromDomain(partId: PartId) =
            RevokePart.FromDomainWithTimestamp(partId, DateTimeOffset.Now)
        member this.ToDomain() = PartId.create this.PartId

    [<ProtoContract; CLIMutable; NoComparison>]
    type RestorePart =
        {
            [<ProtoMember(1)>]
            PartId: string
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(partId: PartId, timestamp: DateTimeOffset) =
            {
                PartId = partId.Value
                Timestamp = timestamp
            }
        static member FromDomain(partId: PartId) =
            RestorePart.FromDomainWithTimestamp(partId, DateTimeOffset.Now)
        member this.ToDomain() = PartId.create this.PartId

    [<ProtoContract; CLIMutable; NoComparison>]
    type RemoveFile =
        {
            [<ProtoMember(1)>]
            PartId: string
            [<ProtoMember(2)>]
            QualityId: int
            [<ProtoMember(3)>]
            FileName: string
            [<ProtoMember(4)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(partId: PartId option, file: MediaSetTypes.ContentFile, timestamp: DateTimeOffset) =
            let partId = partId |> Option.map _.Value |> Option.defaultValue null
            {
                PartId = partId
                QualityId = file.QualityId.Value
                FileName = file.FileName.Value
                Timestamp = timestamp
            }
        static member FromDomain(partId: PartId option, file: MediaSetTypes.ContentFile) =
            RemoveFile.FromDomainWithTimestamp(partId, file, DateTimeOffset.Now)
        member this.ToDomain() =
            let partId = PartId.ofString this.PartId
            let file =
                {
                    MediaSetTypes.QualityId = QualityId.create this.QualityId
                    FileName = FileName.tryParse this.FileName |> Option.defaultValue (FileName String.Empty)
                    SourcePath = None
                    MediaProperties = Unspecified
                }
            partId, file

    [<ProtoContract; CLIMutable; NoComparison>]
    type RemoveSubtitlesFile =
        {
            [<ProtoMember(1)>]
            LanguageCode: string
            [<ProtoMember(2)>]
            Name: string
            [<ProtoMember(3)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(languageCode: Alphanumeric, name: Alphanumeric, timestamp: DateTimeOffset) =
            {
                LanguageCode = languageCode.Value
                Name = name.Value
                Timestamp = timestamp
            }
        static member FromDomain(languageCode: Alphanumeric, kind: Alphanumeric) =
            RemoveSubtitlesFile.FromDomainWithTimestamp(languageCode, kind, DateTimeOffset.Now)
        member this.ToDomain() =
            Alphanumeric this.LanguageCode, Alphanumeric this.Name

    [<ProtoContract; CLIMutable; NoComparison>]
    [<ProtoReserved(2, "PartNumber")>]
    [<ProtoReserved(4, "SubtitlesLinks")>]
    type ContentChunk =
        {
            [<ProtoMember(1)>] // Can be removed/reserved after journal migration
            PartId: string
            [<ProtoMember(6)>]
            PartIds: string array
            [<ProtoMember(3)>]
            Files: ContentFile array
            [<ProtoMember(5)>]
            Subtitles: SubtitlesFile array
            [<ProtoMember(7)>]
            SubtitlesLinks: SubtitlesLink array
        } // Last protobuf index: 7

        interface IProtoBufSerializable
        static member FromDomain partIds (chunk: MediaSetTypes.ContentChunk) =
            {
                PartId = null // Deprecated in favour of PartIds
                PartIds = partIds |> List.toArrayOrNull
                Files =
                    if List.isEmpty chunk.Files then
                        null
                    else
                        chunk.Files |> List.map ContentFile.FromDomain |> List.toArray
                Subtitles =
                    if List.isEmpty chunk.Subtitles then
                        null
                    else
                        chunk.Subtitles |> List.map SubtitlesFile.FromDomain |> List.toArray
                SubtitlesLinks =
                    if List.isEmpty chunk.SubtitlesLinks then
                        null
                    else
                        chunk.SubtitlesLinks |> List.map SubtitlesLink.FromDomain |> List.toArray
            }
        member this.ToDomain() : MediaSetTypes.ContentChunk =
            {
                Files = this.Files |> List.ofArrayOrNull |> List.map _.ToDomain()
                Subtitles = this.Subtitles |> List.ofArrayOrNull |> List.map _.ToDomain()
                SubtitlesLinks = this.SubtitlesLinks |> List.ofArrayOrNull |> List.map _.ToDomain()
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type MediaType =
        {
            [<ProtoMember(1)>]
            MediaType: int
        }

        interface IProtoBufSerializable
        static member FromDomain(mediaType: CoreTypes.MediaType option) =
            match mediaType with
            | Some mediaType ->
                {
                    MediaType = convertFromMediaType mediaType
                }
            | None -> { MediaType = UnspecifiedValue }
        member this.ToDomain() =
            match this.MediaType with
            | UnspecifiedValue -> None
            | mediaType -> convertToMediaType mediaType |> Some

    [<ProtoContract; CLIMutable; NoComparison>]
    type DesiredState =
        {
            [<ProtoMember(1)>]
            MediaMode_Legacy: MediaMode_Legacy
            [<ProtoMember(2)>]
            AccessRestrictions: AccessRestrictions
            [<ProtoMember(3)>]
            Content: ContentChunk array
            [<ProtoMember(4)>]
            MediaType: int
            [<ProtoMember(5)>]
            RevokedParts: string array
        }

        interface IProtoBufSerializable
        static member FromDomain(desiredState: DesiredMediaSetState) =
            let chunks =
                match desiredState.Content with
                | ContentSet.Empty -> null
                | ContentSet.NoParts chunk -> [| ContentChunk.FromDomain [] chunk |]
                | ContentSet.Parts(partIds, chunk) -> [| ContentChunk.FromDomain (partIds |> List.map _.Value) chunk |]
            {
                MediaMode_Legacy = MediaMode_Legacy.Zero
                AccessRestrictions = AccessRestrictions.FromDomain desiredState.GeoRestriction
                Content = chunks
                MediaType = desiredState.MediaType |> convertFromMediaType
                RevokedParts = desiredState.RevokedParts |> List.map (_.Value) |> List.toArrayOrNull
            }
        member this.ToDomain() =
            let content =
                if this.Content = null then
                    ContentSet.Empty
                else
                    // We have to handle a legacy case that uses PartId (see Legacy case comment below)
                    // The legacy case is triggered when we read journal entries for older single-part programs that used PartId to store its part
                    // Newer programs use PartIds field instead.
                    // To eliminate the legacy case and be able to remove PartId field we need to migrate the journal
                    // and copy ContentChunk.PartId values to PartIds array
                    match this.Content with
                    | [| chunk |] when String.isNullOrEmpty chunk.PartId && chunk.PartIds = null -> chunk.ToDomain() |> ContentSet.NoParts
                    | [| chunk |] when chunk.PartIds <> null ->
                        (chunk.PartIds |> List.ofArrayOrNull |> List.map PartId.create, chunk.ToDomain()) |> ContentSet.Parts
                    | [| chunk |] -> (chunk.PartId |> PartId.create |> List.singleton, chunk.ToDomain()) |> ContentSet.Parts // Legacy case
                    | _ -> // The case with multiple parts is handled earlier as deprecated event and should not occur
                        ContentSet.Empty
            // For older programs that do no have MediaProperties we need to interpret
            // MediaMode_Legacy as Mixdown and assign it as MediaPropertiesV1 on files
            let contentWithMixdownFallback =
                match MediaMode_Legacy.Mixdown(this.MediaMode_Legacy) with
                | Some mixdown -> ContentSet.replaceUnspecifiedMixdowns content mixdown
                | None -> content
            {
                MediaType =
                    if this.MediaType <> UnspecifiedValue then
                        convertToMediaType this.MediaType
                    else
                        this.MediaMode_Legacy.ToDomain()
                GeoRestriction = this.AccessRestrictions.ToDomain()
                Content = contentWithMixdownFallback
                RevokedParts = this.RevokedParts |> List.ofArrayOrNull |> List.map PartId.create
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignDesiredState =
        {
            [<ProtoMember(1)>]
            DesiredState: DesiredState
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(desiredState: DesiredMediaSetState, timestamp: DateTimeOffset) =
            {
                DesiredState = DesiredState.FromDomain desiredState
                Timestamp = timestamp
            }
        static member FromDomain(desiredState: DesiredMediaSetState) =
            AssignDesiredState.FromDomainWithTimestamp(desiredState, DateTimeOffset.Now)
        member this.ToDomain() = this.DesiredState.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type MergeDesiredState =
        {
            [<ProtoMember(1)>]
            DesiredState: DesiredState
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(desiredState: DesiredMediaSetState, timestamp: DateTimeOffset) =
            {
                DesiredState = DesiredState.FromDomain desiredState
                Timestamp = timestamp
            }
        static member FromDomain(desiredState: DesiredMediaSetState) =
            MergeDesiredState.FromDomainWithTimestamp(desiredState, DateTimeOffset.Now)
        member this.ToDomain() = this.DesiredState.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type ClearDesiredState =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(timestamp: DateTimeOffset) = { Timestamp = timestamp }
        static member FromDomain() =
            ClearDesiredState.FromDomainWithTimestamp(DateTimeOffset.Now)

    [<ProtoContract; CLIMutable; NoComparison>]
    [<ProtoReserved(1, "ResultCode (int64)")>]
    type RemoteResult =
        {
            [<ProtoMember(2)>]
            ResultMessage: string
            [<ProtoMember(3)>]
            ResultCode: int
        }

        interface IProtoBufSerializable
        static member FromDomain(result: MediaSetTypes.RemoteResult) =
            {
                ResultCode =
                    match result with
                    | RemoteResult.Ok() -> 0
                    | RemoteResult.Error(code, _) -> code
                ResultMessage =
                    match result with
                    | RemoteResult.Ok() -> null
                    | RemoteResult.Error(_, message) -> message
            }
        member this.ToDomain() =
            RemoteResult.create this.ResultCode this.ResultMessage

    [<ProtoContract; CLIMutable; NoComparison>]
    type ReceivedRemoteFileState =
        {
            [<ProtoMember(1)>]
            Origin: int
            [<ProtoMember(2)>]
            PartId: string
            [<ProtoMember(3)>]
            QualityId: int
            [<ProtoMember(4)>]
            State: int
            [<ProtoMember(5)>]
            Result: RemoteResult
            [<ProtoMember(6)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(origin: Origin, fileRef: FileRef, remoteState: MediaSetTypes.RemoteState, result: MediaSetTypes.RemoteResult) =
            {
                Origin = origin |> convertFromOrigin
                PartId = fileRef.PartId |> Option.map _.Value |> Option.defaultValue null
                QualityId = fileRef.QualityId.Value
                State = RemoteState.FromDomain remoteState
                Result = RemoteResult.FromDomain result
                Timestamp =
                    if remoteState.Timestamp = DateTimeOffset.MinValue then
                        DateTimeOffset.Now
                    else
                        remoteState.Timestamp
            }
        member this.ToDomain() =
            let origin = this.Origin |> convertToOrigin
            let partId = PartId.ofString this.PartId
            let qualityId = QualityId.create this.QualityId
            let remoteState =
                {
                    State = this.State |> convertToDistributionState
                    Timestamp = this.Timestamp
                }
            let remoteResult = this.Result.ToDomain()
            origin,
            {
                PartId = partId
                QualityId = qualityId
            },
            remoteState,
            remoteResult

    [<ProtoContract; CLIMutable; NoComparison>]
    type ReceivedRemoteSubtitlesFileState =
        {
            [<ProtoMember(1)>]
            Origin: int
            [<ProtoMember(2)>]
            LanguageCode: string
            [<ProtoMember(3)>]
            Name: string
            [<ProtoMember(4)>]
            State: int
            [<ProtoMember(5)>]
            Result: RemoteResult
            [<ProtoMember(6)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(origin: Origin, subRef: SubtitlesRef, remoteState: MediaSetTypes.RemoteState, result: MediaSetTypes.RemoteResult) =
            {
                Origin = origin |> convertFromOrigin
                LanguageCode = subRef.LanguageCode.Value
                Name = subRef.Name.Value
                State = RemoteState.FromDomain remoteState
                Result = RemoteResult.FromDomain result
                Timestamp =
                    if remoteState.Timestamp = DateTimeOffset.MinValue then
                        DateTimeOffset.Now
                    else
                        remoteState.Timestamp
            }
        member this.ToDomain() =
            let origin = this.Origin |> convertToOrigin
            let languageCode = Alphanumeric this.LanguageCode
            let kind = Alphanumeric this.Name
            let remoteState =
                {
                    State = this.State |> convertToDistributionState
                    Timestamp = this.Timestamp
                }
            let remoteResult = this.Result.ToDomain()
            origin,
            {
                LanguageCode = languageCode
                Name = kind
            },
            remoteState,
            remoteResult

    [<ProtoContract; CLIMutable; NoComparison>]
    type ReceivedRemoteSmilState =
        {
            [<ProtoMember(1)>]
            Origin: int
            [<ProtoMember(5)>]
            Version: int
            [<ProtoMember(2)>]
            State: int
            [<ProtoMember(3)>]
            Result: RemoteResult
            [<ProtoMember(4)>]
            Timestamp: DateTimeOffset
        } // Last protobuf index: 5

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(origin: Origin, version, remoteState: MediaSetTypes.RemoteState, result: MediaSetTypes.RemoteResult) =
            {
                Origin = origin |> convertFromOrigin
                Version = version
                State = RemoteState.FromDomain remoteState
                Result = RemoteResult.FromDomain result
                Timestamp =
                    if remoteState.Timestamp = DateTimeOffset.MinValue then
                        DateTimeOffset.Now
                    else
                        remoteState.Timestamp
            }
        member this.ToDomain() =
            let origin = this.Origin |> convertToOrigin
            let remoteState =
                {
                    State = this.State |> convertToDistributionState
                    Timestamp = this.Timestamp
                }
            let remoteResult = this.Result.ToDomain()
            origin, this.Version, remoteState, remoteResult

    [<ProtoContract; CLIMutable; NoComparison>]
    type ClearRemoteFile =
        {
            [<ProtoMember(1)>]
            Origin: int
            [<ProtoMember(2)>]
            PartId: string
            [<ProtoMember(3)>]
            QualityId: int
            [<ProtoMember(4)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(origin: Origin, fileRef: FileRef, timestamp: DateTimeOffset) =
            {
                Origin = origin |> convertFromOrigin
                PartId = fileRef.PartId |> Option.map _.Value |> Option.defaultValue null
                QualityId = fileRef.QualityId.Value
                Timestamp = timestamp
            }
        static member FromDomain(origin: Origin, fileRef: FileRef) =
            ClearRemoteFile.FromDomainWithTimestamp(origin, fileRef, DateTimeOffset.Now)
        member this.ToDomain() =
            let origin = this.Origin |> convertToOrigin
            let partId = PartId.ofString this.PartId
            let qualityId = QualityId.create this.QualityId
            origin,
            {
                PartId = partId
                QualityId = qualityId
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type GlobalConnectFile =
        {
            [<ProtoMember(1)>]
            SourcePath: string
            [<ProtoMember(2)>]
            RemotePath: string
            [<ProtoMember(3)>]
            TranscodingVersion: int
        }

        interface IProtoBufSerializable
        static member FromDomain(file: MediaSetTypes.GlobalConnectFile) =
            {
                SourcePath = file.SourcePath |> Option.map FilePath.value |> Option.defaultValue null
                RemotePath = file.RemotePath.Value
                TranscodingVersion = file.TranscodingVersion
            }
        member this.ToDomain() =
            {
                MediaSetTypes.GlobalConnectFile.SourcePath = FilePath.tryCreate this.SourcePath
                RemotePath = RelativeUrl this.RemotePath
                TranscodingVersion = this.TranscodingVersion
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignGlobalConnectFile =
        {
            [<ProtoMember(1)>]
            PartId: string
            [<ProtoMember(2)>]
            QualityId: int
            [<ProtoMember(3)>]
            File: GlobalConnectFile
            [<ProtoMember(4)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(fileRef: FileRef, file: MediaSetTypes.GlobalConnectFile, timestamp: DateTimeOffset) =
            {
                PartId = fileRef.PartId |> Option.map _.Value |> Option.defaultValue null
                QualityId = fileRef.QualityId.Value
                File = GlobalConnectFile.FromDomain file
                Timestamp = timestamp
            }
        static member FromDomain(fileRef: FileRef, file: MediaSetTypes.GlobalConnectFile) =
            AssignGlobalConnectFile.FromDomainWithTimestamp(fileRef, file, DateTimeOffset.Now)
        member this.ToDomain() =
            let partId = PartId.ofString this.PartId
            let qualityId = QualityId.create this.QualityId
            let file = this.File.ToDomain()
            {
                PartId = partId
                QualityId = qualityId
            },
            file

    [<ProtoContract; CLIMutable; NoComparison>]
    type GlobalConnectFileState =
        {
            [<ProtoMember(1)>]
            PartId: string
            [<ProtoMember(2)>]
            QualityId: int
            [<ProtoMember(3)>]
            File: GlobalConnectFile
            [<ProtoMember(4)>]
            State: int
            [<ProtoMember(5)>]
            LastResult: RemoteResult
            [<ProtoMember(6)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializable
        static member FromDomain(fileRef: FileRef, file: MediaSetTypes.GlobalConnectFileState) =
            {
                PartId = fileRef.PartId |> Option.map _.Value |> Option.defaultValue null
                QualityId = fileRef.QualityId.Value
                File = GlobalConnectFile.FromDomain file.File
                State = file.RemoteState.State |> convertFromDistributionState
                LastResult = RemoteResult.FromDomain file.LastResult
                Timestamp = file.RemoteState.Timestamp
            }
        member this.ToDomain() =
            let fileRef =
                {
                    PartId = PartId.ofString this.PartId
                    QualityId = QualityId.create this.QualityId
                }
            let fileState =
                {
                    MediaSetTypes.GlobalConnectFileState.File = this.File.ToDomain()
                    RemoteState =
                        {
                            State = this.State |> convertToDistributionState
                            Timestamp = this.Timestamp
                        }
                    LastResult = this.LastResult.ToDomain()
                }
            fileRef, fileState

    [<ProtoContract; CLIMutable; NoComparison>]
    type GlobalConnectSubtitles =
        {
            [<ProtoMember(1)>]
            Subtitles: SubtitlesFile
            [<ProtoMember(2)>]
            RemotePath: string
        }

        interface IProtoBufSerializable
        static member FromDomain(sub: MediaSetTypes.GlobalConnectSubtitles) =
            {
                Subtitles = SubtitlesFile.FromDomain sub.Subtitles
                RemotePath = sub.RemotePath.Value
            }
        member this.ToDomain() =
            {
                MediaSetTypes.GlobalConnectSubtitles.Subtitles = this.Subtitles.ToDomain()
                RemotePath = RelativeUrl this.RemotePath
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignGlobalConnectSubtitles =
        {
            [<ProtoMember(1)>]
            Subtitles: GlobalConnectSubtitles
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(sub: MediaSetTypes.GlobalConnectSubtitles, timestamp: DateTimeOffset) =
            {
                Subtitles = GlobalConnectSubtitles.FromDomain sub
                Timestamp = timestamp
            }
        static member FromDomain(sub: MediaSetTypes.GlobalConnectSubtitles) =
            AssignGlobalConnectSubtitles.FromDomainWithTimestamp(sub, DateTimeOffset.Now)
        member this.ToDomain() = this.Subtitles.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type GlobalConnectSubtitlesState =
        {
            [<ProtoMember(1)>]
            Subtitles: GlobalConnectSubtitles
            [<ProtoMember(2)>]
            State: int
            [<ProtoMember(3)>]
            LastResult: RemoteResult
            [<ProtoMember(4)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializable
        static member FromDomain(sub: MediaSetTypes.GlobalConnectSubtitlesState) =
            {
                Subtitles = GlobalConnectSubtitles.FromDomain sub.Subtitles
                State = sub.RemoteState.State |> convertFromDistributionState
                LastResult = RemoteResult.FromDomain sub.LastResult
                Timestamp = sub.RemoteState.Timestamp
            }
        member this.ToDomain() =
            let subRef =
                {
                    LanguageCode = Alphanumeric this.Subtitles.Subtitles.LanguageCode
                    Name =
                        if String.isNullOrEmpty this.Subtitles.Subtitles.LegacyName then
                            this.Subtitles.Subtitles.Name
                        else
                            this.Subtitles.Subtitles.LegacyName
                        |> Alphanumeric
                }
            let subState =
                {
                    MediaSetTypes.GlobalConnectSubtitlesState.Subtitles = this.Subtitles.ToDomain()
                    RemoteState =
                        {
                            State = this.State |> convertToDistributionState
                            Timestamp = this.Timestamp
                        }
                    LastResult = this.LastResult.ToDomain()
                }
            subRef, subState

    [<ProtoContract; CLIMutable; NoComparison>]
    type GlobalConnectSmil =
        {
            [<ProtoMember(1)>]
            FileName: string
            [<ProtoMember(2)>]
            Content: string
            [<ProtoMember(3)>]
            RemotePath: string
            [<ProtoMember(4)>]
            Version: int
        }

        interface IProtoBufSerializable
        static member FromDomain(smil: MediaSetTypes.GlobalConnectSmil) =
            {
                FileName = smil.FileName.Value
                Content =
                    if smil.Content = SmilTypes.SmilDocument.Zero then
                        null
                    else
                        smil.Content.Serialize()
                RemotePath = smil.RemotePath.Value
                Version = smil.Version
            }
        member this.ToDomain() =
            {
                MediaSetTypes.GlobalConnectSmil.FileName = FileName this.FileName
                Content =
                    if String.isNullOrEmpty this.Content then
                        SmilTypes.SmilDocument.Zero
                    else
                        SmilTypes.SmilDocument.Deserialize this.Content
                RemotePath = RelativeUrl this.RemotePath
                Version = this.Version
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignGlobalConnectSmil =
        {
            [<ProtoMember(1)>]
            SmilFile: GlobalConnectSmil
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(smil: MediaSetTypes.GlobalConnectSmil, timestamp: DateTimeOffset) =
            {
                SmilFile = GlobalConnectSmil.FromDomain(smil)
                Timestamp = timestamp
            }
        static member FromDomain(smil: MediaSetTypes.GlobalConnectSmil) =
            AssignGlobalConnectSmil.FromDomainWithTimestamp(smil, DateTimeOffset.Now)
        member this.ToDomain() = this.SmilFile.ToDomain()

    [<ProtoContract; CLIMutable; NoComparison>]
    type GlobalConnectSmilState =
        {
            [<ProtoMember(1)>]
            Smil: GlobalConnectSmil
            [<ProtoMember(2)>]
            State: int
            [<ProtoMember(3)>]
            LastResult: RemoteResult
            [<ProtoMember(4)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializable
        static member FromDomain(smil: MediaSetTypes.GlobalConnectSmilState) =
            {
                Smil = GlobalConnectSmil.FromDomain smil.Smil
                State = smil.RemoteState.State |> convertFromDistributionState
                LastResult = RemoteResult.FromDomain smil.LastResult
                Timestamp = smil.RemoteState.Timestamp
            }
        member this.ToDomain() =
            let smil = this.Smil.ToDomain()
            {
                MediaSetTypes.GlobalConnectSmilState.Smil = smil
                RemoteState =
                    {
                        State = this.State |> convertToDistributionState
                        Timestamp = this.Timestamp
                    }
                LastResult = this.LastResult.ToDomain()
            }


    [<ProtoContract; CLIMutable; NoComparison>]
    type ClearCurrentState =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(timestamp: DateTimeOffset) = { Timestamp = timestamp }
        static member FromDomain() =
            ClearCurrentState.FromDomainWithTimestamp(DateTimeOffset.Now)

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignClientContentId =
        {
            [<ProtoMember(1)>]
            ClientContentId: string
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(clientContentId, timestamp: DateTimeOffset) =
            {
                ClientContentId = clientContentId |> Option.defaultValue null
                Timestamp = timestamp
            }
        static member FromDomain clientContentId =
            AssignClientContentId.FromDomainWithTimestamp(clientContentId, DateTimeOffset.Now)
        member this.ToDomain() = this.ClientContentId |> Option.ofObj

    [<ProtoContract; CLIMutable; NoComparison>]
    type SetSchemaVersion =
        {
            [<ProtoMember(1)>]
            Version: int
            [<ProtoMember(2)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(version: int, timestamp: DateTimeOffset) =
            {
                Version = version
                Timestamp = timestamp
            }
        static member FromDomain(version) =
            SetSchemaVersion.FromDomainWithTimestamp(version, DateTimeOffset.Now)
        member this.ToDomain() = this.Version

    [<ProtoContract; CLIMutable; NoComparison>]
    [<ProtoReserved(1, "AkamaiStorage")>]
    [<ProtoReserved(2, "AkamaiFiles")>]
    [<ProtoReserved(3, "AssignNepStorage")>]
    [<ProtoReserved(4, "NepFiles")>]
    [<ProtoReserved(5, "NepSubtitles")>]
    [<ProtoReserved(6, "NepThumbnails")>]
    [<ProtoReserved(7, "AkamaiSmil")>]
    [<ProtoReserved(8, "GlobalConnectStorage")>]
    type CurrentState =
        {
            [<ProtoMember(9)>]
            GlobalConnectFiles: GlobalConnectFileState array
            [<ProtoMember(11)>]
            GlobalConnectSubtitles: GlobalConnectSubtitlesState array
            [<ProtoMember(10)>]
            GlobalConnectSmil: GlobalConnectSmilState
            [<ProtoMember(12)>]
            GlobalConnectVersion: int
        } // Last protobuf index: 12

        interface IProtoBufSerializable
        static member FromDomain(state: MediaSetTypes.CurrentMediaSetState) =
            {
                GlobalConnectFiles = state.GlobalConnect.Files |> Map.toList |> List.map GlobalConnectFileState.FromDomain |> List.toArrayOrNull
                GlobalConnectSubtitles =
                    state.GlobalConnect.Subtitles
                    |> Map.values
                    |> Seq.toList
                    |> List.map GlobalConnectSubtitlesState.FromDomain
                    |> List.toArrayOrNull
                GlobalConnectSmil = GlobalConnectSmilState.FromDomain state.GlobalConnect.Smil
                GlobalConnectVersion = state.GlobalConnect.Version
            }
        member this.ToDomain() : MediaSetTypes.CurrentMediaSetState =
            {
                GlobalConnect =
                    let smil =
                        try
                            this.GlobalConnectSmil.ToDomain()
                        with :? NullReferenceException ->
                            // Fallback for mediasets created prior GlobalConnect era
                            GlobalConnectSmilState.Zero
                    {
                        Files = this.GlobalConnectFiles |> List.ofArrayOrNull |> List.map _.ToDomain() |> Map.ofList
                        Subtitles = this.GlobalConnectSubtitles |> List.ofArrayOrNull |> List.map _.ToDomain() |> Map.ofList
                        Smil = smil
                        Version = this.GlobalConnectVersion
                    }
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    [<ProtoReserved(3, "Initialization")>]
    [<ProtoReserved(6, "MediaSetVersion")>]
    type MediaSetState =
        {
            [<ProtoMember(1)>]
            Desired: DesiredState
            [<ProtoMember(2)>]
            Current: CurrentState
            [<ProtoMember(4)>]
            ClientContentId: string
            [<ProtoMember(5)>]
            LastSequenceNr: int64
            [<ProtoMember(7)>]
            SchemaVersion: int
        }

        interface IProtoBufSerializable
        static member FromDomain(state: MediaSetTypes.MediaSetState) =
            {
                Desired = DesiredState.FromDomain state.Desired
                Current = CurrentState.FromDomain state.Current
                ClientContentId = state.ClientContentId |> Option.defaultValue null
                LastSequenceNr = state.LastSequenceNr
                SchemaVersion = state.SchemaVersion
            }
        member this.ToDomain() =
            {
                MediaSetTypes.MediaSetState.Desired = this.Desired.ToDomain()
                Current = this.Current.ToDomain()
                ClientContentId = Option.ofObj this.ClientContentId
                LastSequenceNr = this.LastSequenceNr
                SchemaVersion = this.SchemaVersion
            }

    module PartId =
        let fromDomain (partId: MediaSetTypes.PartId option) =
            match partId with
            | Some partId -> partId.Value
            | None -> null

        let toDomain partId = PartId.ofString partId

    type FileRef = { PartId: string; QualityId: int }

    module FileRef =
        let fromDomain (fileRef: MediaSetTypes.FileRef) =
            {
                PartId = PartId.fromDomain fileRef.PartId
                QualityId = fileRef.QualityId.Value
            }

        let toDomain (fileRef: FileRef) : MediaSetTypes.FileRef =
            {
                PartId = PartId.toDomain fileRef.PartId
                QualityId = QualityId fileRef.QualityId
            }

    type SubtitlesRef = { LanguageCode: string; Name: string }

    module SubtitlesRef =
        let fromDomain (subtitlesFileRef: MediaSetTypes.SubtitlesRef) =
            {
                LanguageCode = subtitlesFileRef.LanguageCode.Value
                Name = subtitlesFileRef.Name.Value
            }

        let toDomain (subtitlesFileRef: SubtitlesRef) : MediaSetTypes.SubtitlesRef =
            {
                LanguageCode = Alphanumeric subtitlesFileRef.LanguageCode
                Name = Alphanumeric subtitlesFileRef.Name
            }

    type GlobalConnectCommand =
        | UploadFile of FileRef
        | MoveFile of FileRef
        | DeleteFile of FileRef
        | UploadSubtitles of SubtitlesRef
        | MoveSubtitles of SubtitlesRef
        | DeleteSubtitles of SubtitlesRef
        | UploadSmil of int
        | MoveSmil of int
        | DeleteSmil of int
        | CleanupStorage of string list

    module GlobalConnectCommand =
        let fromDomain (cmd: GlobalConnectCommands.GlobalConnectCommand) =
            match cmd with
            | GlobalConnectCommands.GlobalConnectCommand.UploadFile(UploadFileCommand fileRef) -> GlobalConnectCommand.UploadFile(FileRef.fromDomain fileRef)
            | GlobalConnectCommands.GlobalConnectCommand.MoveFile(MoveFileCommand fileRef) -> GlobalConnectCommand.MoveFile(FileRef.fromDomain fileRef)
            | GlobalConnectCommands.GlobalConnectCommand.DeleteFile(DeleteFileCommand fileRef) -> GlobalConnectCommand.DeleteFile(FileRef.fromDomain fileRef)
            | GlobalConnectCommands.GlobalConnectCommand.UploadSubtitles(UploadSubtitlesCommand subRef) ->
                GlobalConnectCommand.UploadSubtitles(SubtitlesRef.fromDomain subRef)
            | GlobalConnectCommands.GlobalConnectCommand.MoveSubtitles(MoveSubtitlesCommand subRef) ->
                GlobalConnectCommand.MoveSubtitles(SubtitlesRef.fromDomain subRef)
            | GlobalConnectCommands.GlobalConnectCommand.DeleteSubtitles(DeleteSubtitlesCommand subRef) ->
                GlobalConnectCommand.DeleteSubtitles(SubtitlesRef.fromDomain subRef)
            | GlobalConnectCommands.GlobalConnectCommand.UploadSmil(UploadSmilCommand version) -> GlobalConnectCommand.UploadSmil(version)
            | GlobalConnectCommands.GlobalConnectCommand.MoveSmil(MoveSmilCommand version) -> GlobalConnectCommand.MoveSmil(version)
            | GlobalConnectCommands.GlobalConnectCommand.DeleteSmil(DeleteSmilCommand version) -> GlobalConnectCommand.DeleteSmil(version)
            | GlobalConnectCommands.GlobalConnectCommand.CleanupStorage(CleanupStorageCommand inactiveFiles) ->
                GlobalConnectCommand.CleanupStorage(inactiveFiles)

        let toDomain (cmd: GlobalConnectCommand) : GlobalConnectCommands.GlobalConnectCommand =
            match cmd with
            | GlobalConnectCommand.UploadFile fileRef -> GlobalConnectCommands.GlobalConnectCommand.UploadFile(UploadFileCommand(FileRef.toDomain fileRef))
            | GlobalConnectCommand.MoveFile fileRef -> GlobalConnectCommands.GlobalConnectCommand.MoveFile(MoveFileCommand(FileRef.toDomain fileRef))
            | GlobalConnectCommand.DeleteFile fileRef -> GlobalConnectCommands.GlobalConnectCommand.DeleteFile(DeleteFileCommand(FileRef.toDomain fileRef))
            | GlobalConnectCommand.UploadSubtitles subRef ->
                GlobalConnectCommands.GlobalConnectCommand.UploadSubtitles(UploadSubtitlesCommand(SubtitlesRef.toDomain subRef))
            | GlobalConnectCommand.MoveSubtitles subRef ->
                GlobalConnectCommands.GlobalConnectCommand.MoveSubtitles(MoveSubtitlesCommand(SubtitlesRef.toDomain subRef))
            | GlobalConnectCommand.DeleteSubtitles subRef ->
                GlobalConnectCommands.GlobalConnectCommand.DeleteSubtitles(DeleteSubtitlesCommand(SubtitlesRef.toDomain subRef))
            | GlobalConnectCommand.UploadSmil version -> GlobalConnectCommands.GlobalConnectCommand.UploadSmil(UploadSmilCommand(version))
            | GlobalConnectCommand.MoveSmil version -> GlobalConnectCommands.GlobalConnectCommand.MoveSmil(MoveSmilCommand(version))
            | GlobalConnectCommand.DeleteSmil version -> GlobalConnectCommands.GlobalConnectCommand.DeleteSmil(DeleteSmilCommand(version))
            | GlobalConnectCommand.CleanupStorage inactiveFiles ->
                GlobalConnectCommands.GlobalConnectCommand.CleanupStorage(CleanupStorageCommand(inactiveFiles))

    type RemainingActions =
        {
            GlobalConnect: GlobalConnectCommand list
        }

        static member Zero =
            {
                GlobalConnect = List.empty
            }

    module RemainingActions =
        let fromDomain (actions: MediaSetState.RemainingActions) =
            {
                GlobalConnect = actions.GlobalConnect |> List.map GlobalConnectCommand.fromDomain
            }

        let toDomain (actions: RemainingActions) : MediaSetState.RemainingActions =
            {
                GlobalConnect = actions.GlobalConnect |> List.map GlobalConnectCommand.toDomain
            }
