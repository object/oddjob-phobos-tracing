namespace Nrk.Oddjob.Potion

[<RequireQualifiedAccess>]
module PotionPersistence =

    open System
    open System.IO
    open FSharpx.Collections
    open ProtoBuf
    open Akka.Actor
    open Akka.Util

    open Nrk.Oddjob.Core
    open PotionTypes

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

    let private immutableOriginMapping =
        // The following mappings are immutable: new values can be added, but once a numeric value is mapped it can not be changed
        // Value 1 is deprecated Origin.Akamai
        // Value 2 is deprecated Origin.Nep
        [ (3, Origin.GlobalConnect) ]

    let deprecatedOrigins = [ 1; 2 ] // Akamai, NEP

    let convertFromGeoRestriction value =
        value |> convertOrFail (tryConvertFromDomainValue immutableGeoRestrictionMapping)

    let convertToGeoRestriction value =
        value |> convertOrFail (tryConvertToDomainValue immutableGeoRestrictionMapping)

    let convertFromMediaType value =
        value |> convertOrFail (tryConvertFromDomainValue immutableMediaTypeMapping)

    let convertToMediaType value =
        value |> convertOrFail (tryConvertToDomainValue immutableMediaTypeMapping)

    let private tryConvertFromMediaType value =
        value |> convertOrDefault (tryConvertFromDomainValue immutableMediaTypeMapping)

    let private tryConvertToMediaType value =
        value |> convertOrNone (tryConvertToDomainValue immutableMediaTypeMapping)

    let convertFromOrigin value =
        value |> convertOrFail (tryConvertFromDomainValue immutableOriginMapping)

    let convertToOrigin value =
        value |> convertOrFail (tryConvertToDomainValue immutableOriginMapping)

    [<AllowNullLiteral>]
    type IProtoBufSerializable = interface end

    [<AllowNullLiteral>]
    type IProtoBufSerializableEvent =
        inherit IProtoBufSerializable
        abstract member Timestamp: DateTimeOffset

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignedGeoBlock =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
            [<ProtoMember(2)>]
            Source: string
            [<ProtoMember(3)>]
            CorrelationId: string
            [<ProtoMember(4)>]
            GroupId: string
            [<ProtoMember(5)>]
            GeoBlock: int
            [<ProtoMember(6)>]
            ForwardedFrom: string
            [<ProtoMember(7)>]
            Version: int
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(cmd: SetGeoBlockCommand, forwardedFrom, timestamp) =
            {
                Timestamp = timestamp
                Source = cmd.Source |> Option.defaultValue null
                CorrelationId = cmd.CorrelationId
                GroupId = cmd.GroupId
                GeoBlock = convertFromGeoRestriction cmd.GeoBlock
                ForwardedFrom = Option.toObj forwardedFrom
                Version = cmd.Version |> Option.defaultValue 0
            }
        member this.ToDomain() =
            {
                GeoBlock = convertToGeoRestriction this.GeoBlock
                GroupId = this.GroupId
                CorrelationId = this.CorrelationId
                Version = if this.Version = 0 then None else Some this.Version
                Source = Option.ofObj this.Source
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignedFile =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
            [<ProtoMember(2)>]
            Source: string
            [<ProtoMember(3)>]
            CorrelationId: string
            [<ProtoMember(4)>]
            GroupId: string
            [<ProtoMember(5)>]
            FileId: string
            [<ProtoMember(6)>]
            QualityId: int
            [<ProtoMember(7)>]
            MediaType: int
            [<ProtoMember(8)>]
            FilePath: string
            [<ProtoMember(9)>]
            ForwardedFrom: string
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(cmd: AddFileCommand, forwardedFrom, timestamp) =
            {
                Timestamp = timestamp
                Source = cmd.Source |> Option.defaultValue null
                CorrelationId = cmd.CorrelationId
                GroupId = cmd.GroupId
                FileId = cmd.FileId
                QualityId = cmd.QualityId
                MediaType = convertFromMediaType cmd.MediaType
                FilePath = cmd.FilePath
                ForwardedFrom = Option.toObj forwardedFrom
            }
        member this.ToDomain() =
            {
                MediaType = convertToMediaType this.MediaType
                GroupId = this.GroupId
                FileId = this.FileId
                FilePath = this.FilePath
                QualityId = this.QualityId
                CorrelationId = this.CorrelationId
                Source = Option.ofObj this.Source
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type SubtitlesInfo =
        {
            [<ProtoMember(1)>]
            Url: string
            [<ProtoMember(2)>]
            Duration: int
            [<ProtoMember(3)>]
            Language: string
        }

        interface IProtoBufSerializable
        static member FromDomain(sub: SubtitleInfo) =
            {
                Url = sub.Url
                Duration = sub.Duration
                Language = sub.Language
            }
        member this.ToDomain() : SubtitleInfo =
            {
                Url = this.Url
                Duration = this.Duration
                Language = this.Language
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignedSubtitles =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
            [<ProtoMember(2)>]
            Source: string
            [<ProtoMember(3)>]
            CorrelationId: string
            [<ProtoMember(4)>]
            GroupId: string
            [<ProtoMember(5)>]
            Subtitles: SubtitlesInfo array
            [<ProtoMember(6)>]
            ForwardedFrom: string
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(cmd: AddSubtitleCommand, forwardedFrom, timestamp) =
            {
                Timestamp = timestamp
                Source = cmd.Source |> Option.defaultValue null
                CorrelationId = cmd.CorrelationId
                GroupId = cmd.GroupId
                Subtitles = cmd.Subtitles |> List.map SubtitlesInfo.FromDomain |> List.toArrayOrNull
                ForwardedFrom = Option.toObj forwardedFrom
            }
        member this.ToDomain() : AddSubtitleCommand =
            {
                GroupId = this.GroupId
                CorrelationId = this.CorrelationId
                Subtitles = this.Subtitles |> Array.map _.ToDomain() |> Array.toList
                Source = Option.ofObj this.Source
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type DeletedGroup =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
            [<ProtoMember(2)>]
            Source: string
            [<ProtoMember(3)>]
            CorrelationId: string
            [<ProtoMember(4)>]
            GroupId: string
            [<ProtoMember(5)>]
            ForwardedFrom: string
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(cmd: DeleteGroupCommand, forwardedFrom, timestamp) =
            {
                Timestamp = timestamp
                Source = cmd.Source |> Option.defaultValue null
                CorrelationId = cmd.CorrelationId
                GroupId = cmd.GroupId
                ForwardedFrom = Option.toObj forwardedFrom
            }
        member this.ToDomain() =
            {
                GroupId = this.GroupId
                CorrelationId = this.CorrelationId
                Source = Option.ofObj this.Source
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    [<ProtoReserved(5, "Languages")>]
    type DeletedSubtitles =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
            [<ProtoMember(2)>]
            Source: string
            [<ProtoMember(3)>]
            CorrelationId: string
            [<ProtoMember(4)>]
            GroupId: string
            [<ProtoMember(7)>]
            Language: string
            [<ProtoMember(6)>]
            ForwardedFrom: string
        } // Last protobuf index: 7

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(cmd: DeleteSubtitleCommand, forwardedFrom, timestamp) =
            {
                Timestamp = timestamp
                Source = cmd.Source |> Option.defaultValue null
                CorrelationId = cmd.CorrelationId
                GroupId = cmd.GroupId
                Language = cmd.Language
                ForwardedFrom = Option.toObj forwardedFrom
            }
        member this.ToDomain() : DeleteSubtitleCommand =
            {
                GroupId = this.GroupId
                CorrelationId = this.CorrelationId
                Language = this.Language
                Source = Option.ofObj this.Source
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type PotionFile =
        {
            [<ProtoMember(1)>]
            FileId: string
            [<ProtoMember(2)>]
            MediaSetFileId: string
            [<ProtoMember(3)>]
            FilePath: string
            [<ProtoMember(4)>]
            QualityId: int
            [<ProtoMember(5)>]
            Source: string
        }

        interface IProtoBufSerializable
        static member FromDomain(file: PotionTypes.PotionFile) =
            {
                FileId = file.FileId
                MediaSetFileId = file.MediaSetFileId
                FilePath = file.FilePath
                QualityId = file.QualityId
                Source = file.Source |> Option.defaultValue null
            }
        member this.ToDomain() : PotionTypes.PotionFile =
            {
                FileId = this.FileId
                MediaSetFileId = this.MediaSetFileId
                FilePath = this.FilePath
                QualityId = this.QualityId
                Source = this.Source |> Option.ofObj
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type PotionSubtitles =
        {
            [<ProtoMember(1)>]
            Url: string
            [<ProtoMember(2)>]
            Duration: int
            [<ProtoMember(3)>]
            Language: string
            [<ProtoMember(4)>]
            Source: string
        }

        interface IProtoBufSerializable
        static member FromDomain(file: PotionTypes.PotionSubtitles) =
            {
                Url = file.Url
                Duration = file.Duration
                Language = file.Language
                Source = file.Source |> Option.defaultValue null
            }
        member this.ToDomain() : PotionTypes.PotionSubtitles =
            {
                Url = this.Url
                Duration = this.Duration
                Language = this.Language
                Source = this.Source |> Option.ofObj
            }

    // This event occurs only under migration of old Potion clips from ReferenceMap table that is now removed.
    // The migrated clip was processed before Potion command persistence layer had been implemented.
    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignedGroupId =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
            [<ProtoMember(2)>]
            GroupId: string
            [<ProtoMember(3)>]
            MediaSetId: string
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(groupId, mediaSetId: MediaSetId, timestamp) =
            {
                Timestamp = timestamp
                GroupId = groupId
                MediaSetId = mediaSetId.Value
            }
        static member FromDomain(groupId, mediaSetId) =
            AssignedGroupId.FromDomainWithTimestamp(groupId, mediaSetId, DateTimeOffset.Now)
        member this.ToDomain() =
            let groupId =
                if this.GroupId.Contains "/" then
                    this.GroupId
                else
                    PotionExternalGroupIdPrefix + this.GroupId
            groupId, MediaSetId.parse this.MediaSetId

    // This event occurs only under migration of old Potion clips from ReferenceMap table that is now removed.
    // The migrated clip was processed before Potion command persistence layer had been implemented.
    [<ProtoContract; CLIMutable; NoComparison>]
    type AssignedFileId =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
            [<ProtoMember(2)>]
            FileId: string
            [<ProtoMember(3)>]
            MediaSetFileId: string
            [<ProtoMember(4)>]
            QualityId: int
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomainWithTimestamp(fileId, mediaSetFileId, qualityId, timestamp) =
            {
                Timestamp = timestamp
                FileId = fileId
                MediaSetFileId = mediaSetFileId
                QualityId = qualityId
            }
        static member FromDomain(fileId, mediaSetFileId, qualityId) =
            AssignedFileId.FromDomainWithTimestamp(fileId, mediaSetFileId, qualityId, DateTimeOffset.Now)
        member this.ToDomain() =
            let fileId =
                if this.FileId.Contains "/" then
                    this.FileId
                else
                    PotionExternalFileIdPrefix + this.FileId
            fileId, this.MediaSetFileId, this.QualityId

    // This event occurs only under migration of old Potion clips from ReferenceMap table that is now removed.
    // The migrated clip was processed before Potion command persistence layer had been implemented.
    [<ProtoContract; CLIMutable; NoComparison>]
    type CompletedMigration =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp

    [<ProtoContract; CLIMutable; NoComparison>]
    type PendingCommand =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
            [<ProtoMember(2)>]
            CommandId: string
            [<ProtoMember(3)>]
            Command: string
            [<ProtoMember(4)>]
            Origin: int
            [<ProtoMember(5)>]
            ForwardedFrom: string
            [<ProtoMember(6)>]
            Priority: byte
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(command: PotionTypes.PendingCommand) =
            {
                Timestamp = command.Timestamp
                CommandId = command.CommandId
                Command = command.Command |> Serialization.Newtonsoft.serializeObject SerializationSettings.PascalCase
                Origin = command.Origin |> convertFromOrigin
                ForwardedFrom = Option.toObj command.ForwardedFrom
                Priority = command.Priority
            }
        member this.ToDomain() : PotionTypes.PendingCommand =
            {
                CommandId = this.CommandId
                Command = this.Command |> Serialization.Newtonsoft.deserializeObject<PotionCommand> SerializationSettings.PascalCase
                Origin = this.Origin |> convertToOrigin
                Timestamp = this.Timestamp
                ForwardedFrom = Option.ofObj this.ForwardedFrom
                Priority = this.Priority
                RetryCount = 0
            }

    [<ProtoContract; CLIMutable; NoComparison>]
    type CompletedCommand =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
            [<ProtoMember(2)>]
            CommandId: string
            [<ProtoMember(3)>]
            Origin: int
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(command: PotionTypes.PendingCommand, timestamp) =
            {
                Timestamp = timestamp
                CommandId = command.CommandId
                Origin = convertFromOrigin command.Origin
            }
        member this.ToDomain() =
            this.CommandId, this.Origin, this.Timestamp

    [<ProtoContract; CLIMutable; NoComparison>]
    type CancelledCommand =
        {
            [<ProtoMember(1)>]
            Timestamp: DateTimeOffset
            [<ProtoMember(2)>]
            CommandId: string
            [<ProtoMember(3)>]
            Origin: int
        }

        interface IProtoBufSerializableEvent with
            member this.Timestamp = this.Timestamp
        static member FromDomain(command: PotionTypes.PendingCommand, timestamp) =
            {
                Timestamp = timestamp
                CommandId = command.CommandId
                Origin = convertFromOrigin command.Origin
            }
        member this.ToDomain() =
            this.CommandId, this.Origin, this.Timestamp

    [<ProtoContract; CLIMutable; NoComparison>]
    type PotionSetState =
        {
            [<ProtoMember(1)>]
            GroupId: string
            [<ProtoMember(2)>]
            MediaSetId: string
            [<ProtoMember(3)>]
            GeoBlock: int
            [<ProtoMember(4)>]
            MediaType: int
            [<ProtoMember(5)>]
            Files: PotionFile array
            [<ProtoMember(6)>]
            Subtitles: PotionSubtitles array
            [<ProtoMember(9)>]
            LastVersion: int
            [<ProtoMember(10)>]
            CreationTime: DateTimeOffset
            [<ProtoMember(7)>]
            PendingCommands: PendingCommand array
            // Temporary field, to be removed after clip migration
            [<ProtoMember(8)>]
            HasCommands: bool
        } // Last protobuf index: 9

        interface IProtoBufSerializable
        static member FromDomain(state: PotionTypes.PotionSetState) =
            let groupId, mediaSetId, geoBlock, mediaType =
                match state.Group with
                | Some group -> group.GroupId, group.MediaSetId.Value, convertFromGeoRestriction group.GeoBlock, tryConvertFromMediaType group.MediaType
                | None -> null, null, 0, 0
            {
                GroupId = groupId
                MediaSetId = mediaSetId
                GeoBlock = geoBlock
                MediaType = mediaType
                Files = state.Files |> Map.values |> Seq.map PotionFile.FromDomain |> Seq.toArray
                Subtitles = state.Subtitles |> Map.values |> Seq.map PotionSubtitles.FromDomain |> Seq.toArray
                LastVersion = state.LastVersion |> Option.defaultValue 0
                CreationTime = state.CreationTime
                PendingCommands = state.PendingCommands |> Seq.map PendingCommand.FromDomain |> Seq.toArray
                HasCommands = state.HasCommands || state.ClientCommands |> Seq.isNotEmpty
            }
        member this.ToDomain() : PotionTypes.PotionSetState =
            let group =
                if String.IsNullOrEmpty this.GroupId then
                    None
                else
                    Some
                        {
                            GroupId = this.GroupId
                            MediaSetId = MediaSetId.parse this.MediaSetId
                            GeoBlock = convertToGeoRestriction this.GeoBlock
                            MediaType = tryConvertToMediaType this.MediaType
                            CorrectGeoBlock = convertToGeoRestriction this.GeoBlock // UNDO: this is a temporary field to be removed once geoblocks are fixed 17.01.2024
                        }
            {
                Group = group
                Files = this.Files |> List.ofArrayOrNull |> List.map (fun x -> x.QualityId, x.ToDomain()) |> Map.ofList
                Subtitles = this.Subtitles |> List.ofArrayOrNull |> List.map (fun x -> x.Language, x.ToDomain()) |> Map.ofList
                LastVersion = if this.LastVersion = 0 then None else Some this.LastVersion
                CreationTime = this.CreationTime
                ClientCommands = List.empty
                PendingCommands = this.PendingCommands |> List.ofArrayOrNull |> List.map _.ToDomain()
                HasCommands = this.HasCommands
                ShouldNotRetryGeoBlockCommand = false
                ShouldSkipNotifications = false
            }

    type ProtobufSerializer(system: ExtendedActorSystem) =
        inherit Akka.Serialization.SerializerWithStringManifest(system)

        static member SupportedTypes =
            [
                typedefof<AssignedGeoBlock>
                typedefof<AssignedFile>
                typedefof<AssignedSubtitles>
                typedefof<DeletedGroup>
                typedefof<DeletedSubtitles>
                typedefof<AssignedGroupId>
                typedefof<AssignedFileId>
                typedefof<CompletedMigration>
                typedefof<PendingCommand>
                typedefof<CompletedCommand>
                typedefof<CancelledCommand>
                typedefof<PotionSetState>
            ]

        override this.ToBinary(o: obj) =
            use stream = new MemoryStream()
            Serializer.Serialize(stream, o)
            stream.ToArray()

        override this.FromBinary(bytes: byte[], manifest: string) =

            let typ =
                ProtobufSerializer.SupportedTypes
                |> List.tryFind (fun t -> manifest = t.Name || manifest = t.FullName || manifest = t.TypeQualifiedName())
                |> Option.defaultWith (fun () -> notSupported $"Serializer doesn't support manifest %s{manifest}")

            use stream = new MemoryStream(bytes)
            Serializer.Deserialize(typ, stream)

        override this.Manifest(o: obj) =
            if ProtobufSerializer.SupportedTypes |> List.contains (o.GetType()) then
                o.GetType().Name
            else
                notSupported $"Serializer doesn't support type %s{o.GetType().FullName}"

        override this.Identifier = 130
