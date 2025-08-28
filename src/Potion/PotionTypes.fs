namespace Nrk.Oddjob.Potion

open Nrk.Oddjob.Core.Dto.Events

module PotionTypes =
    open System
    open Nrk.Oddjob.Core

    [<Literal>]
    let RequestSourcePotion = "potion"

    [<Literal>]
    let PotionExternalGroupIdPrefix = "http://id.nrk.no/2016/mdb/publicationMediaObject/"

    [<Literal>]
    let PotionExternalFileIdPrefix = "http://id.nrk.no/2016/mdb/essence/"

    let getShortGroupId (groupId: string) =
        groupId |> String.split '/' |> Array.last

    type SubtitleInfo =
        {
            Url: string
            Duration: int
            Language: string
        }

    type AddFileCommand =
        {
            MediaType: MediaType
            GroupId: string
            FileId: string
            FilePath: string
            QualityId: int
            CorrelationId: string
            Source: string option
        }

    type AddSubtitleCommand =
        {
            GroupId: string
            CorrelationId: string
            Subtitles: SubtitleInfo list
            Source: string option
        }

    type SetGeoBlockCommand =
        {
            GeoBlock: GeoRestriction
            GroupId: string
            CorrelationId: string
            Version: int option
            Source: string option
        }

    type DeleteGroupCommand =
        {
            GroupId: string
            CorrelationId: string
            Source: string option
        }

    type DeleteSubtitleCommand =
        {
            GroupId: string
            CorrelationId: string
            Language: string
            Source: string option
        }

    type PotionCommand =
        | AddFile of AddFileCommand
        | AddSubtitle of AddSubtitleCommand
        | SetGeoBlock of SetGeoBlockCommand
        | DeleteGroup of DeleteGroupCommand
        | DeleteSubtitle of DeleteSubtitleCommand

        member this.GetGroupId() =
            match this with
            | AddFile x -> x.GroupId
            | AddSubtitle x -> x.GroupId
            | SetGeoBlock x -> x.GroupId
            | DeleteGroup x -> x.GroupId
            | DeleteSubtitle x -> x.GroupId
        member this.GetShortGroupId() = this.GetGroupId() |> getShortGroupId
        member this.GetFileId() =
            match this with
            | AddFile x -> Some x.FileId
            | _ -> None
        member this.GetQualityId() =
            match this with
            | AddFile x -> Some x.QualityId
            | _ -> None
        member this.GetCorrelationId() =
            match this with
            | AddFile x -> x.CorrelationId
            | AddSubtitle x -> x.CorrelationId
            | SetGeoBlock x -> x.CorrelationId
            | DeleteGroup x -> x.CorrelationId
            | DeleteSubtitle x -> x.CorrelationId
        member this.GetSource() =
            match this with
            | AddFile x -> x.Source
            | AddSubtitle x -> x.Source
            | SetGeoBlock x -> x.Source
            | DeleteGroup x -> x.Source
            | DeleteSubtitle x -> x.Source
            |> Option.bind (fun x -> if String.IsNullOrEmpty x then None else Some <| x.ToLower())
            |> Option.defaultValue RequestSourcePotion

    type GetPotionSetState = GetPotionSetState of string

    type EventStoreMessage =
        {
            Id: string
            Description: string
            Content: string
            Created: DateTime
        }

    type PotionGroup =
        {
            GroupId: string
            MediaSetId: MediaSetId
            GeoBlock: GeoRestriction
            MediaType: MediaType option
            CorrectGeoBlock: GeoRestriction // UNDO: this is a temporary field to be removed once geoblocks are fixed 17.01.2024
        }

    module PotionGroup =

        let createMediaSetId (groupId: string) =
            {
                ContentId = ContentId.create <| normalizeGuid (Guid(groupId |> String.split '/' |> Array.last))
                ClientId = Alphanumeric PotionClientId
            }

        let createMediaSetFileId qualityId mediaSetId =
            {
                PartId = None
                QualityId = QualityId.create qualityId
            }
            |> FileRef.toFileName mediaSetId
            |> fun x -> sprintf "%s/%s/%s" mediaSetId.ClientId.Value mediaSetId.ContentId.Value x.Value

        let assignGroupId groupId mediaSetId (group: PotionGroup option) =
            match group with
            | Some group when group.GroupId <> groupId -> invalidOp $"GroupId can not be changed (attempt to change from %s{group.GroupId} to %s{groupId}"
            | Some group -> Some { group with MediaSetId = mediaSetId }
            | None ->
                Some
                    {
                        GroupId = groupId
                        MediaSetId = mediaSetId
                        MediaType = None
                        GeoBlock = GeoRestriction.Unspecified
                        CorrectGeoBlock = GeoRestriction.Unspecified // UNDO: this is a temporary field to be removed once geoblocks are fixed 17.01.2024
                    }

        let updateGroup groupId (geoBlock: GeoRestriction option) mediaType isInternalCommand (group: PotionGroup option) =
            let assignMediaType mediaType currentMediaType =
                match mediaType, currentMediaType with
                | None, _ -> currentMediaType
                | Some _, None -> mediaType
                | Some mt, Some cmt when mt = cmt -> currentMediaType
                | Some mt, Some cmt -> invalidOp $"Media type can not be changed (attempt to change from %A{cmt} to %A{mt}"
            match group with
            | Some stateGroup ->
                // UNDO: this is a temporary field to be removed once geoblocks are fixed 17.01.2024
                let correctGeoBlock =
                    match geoBlock with
                    | Some geoBlock ->
                        if isInternalCommand then
                            if stateGroup.GeoBlock = GeoRestriction.Unspecified then
                                geoBlock
                            else
                                stateGroup.CorrectGeoBlock
                        else
                            geoBlock
                    | None -> stateGroup.CorrectGeoBlock
                Some
                    {
                        GroupId = groupId
                        MediaSetId = stateGroup.MediaSetId
                        MediaType = assignMediaType mediaType stateGroup.MediaType
                        GeoBlock = geoBlock |> Option.defaultValue stateGroup.GeoBlock
                        CorrectGeoBlock = correctGeoBlock // UNDO: this is a temporary field to be removed once geoblocks are fixed 17.01.2024
                    }
            | None ->
                Some
                    {
                        GroupId = groupId
                        MediaSetId = createMediaSetId groupId
                        MediaType = mediaType
                        GeoBlock = geoBlock |> Option.defaultValue GeoRestriction.Unspecified
                        CorrectGeoBlock = geoBlock |> Option.defaultValue GeoRestriction.Unspecified // UNDO: this is a temporary field to be removed once geoblocks are fixed 17.01.2024
                    }

        let updateGeoBlock groupId geoBlock mediaType correlationId (group: PotionGroup option) =
            updateGroup groupId geoBlock mediaType correlationId group

    type PotionFile =
        {
            FileId: string
            MediaSetFileId: string
            FilePath: string
            QualityId: int
            Source: string option
        }

    module PotionFiles =
        let assignFileId fileId mediaSetFileId qualityId files =
            let file =
                {
                    FileId = fileId
                    MediaSetFileId = mediaSetFileId
                    FilePath = null
                    QualityId = qualityId
                    Source = None
                }
            files |> Map.add qualityId file

        let assignMediaSetFileId mediaSetId files =
            files
            |> Map.map (fun _ (file: PotionFile) ->
                let mediaSetFileId = PotionGroup.createMediaSetFileId file.QualityId mediaSetId
                { file with
                    MediaSetFileId = mediaSetFileId
                })

        let addFile (cmd: AddFileCommand) (group: PotionGroup option) files =
            let mediaSetId =
                match group with
                | Some group -> group.MediaSetId
                | None -> PotionGroup.createMediaSetId cmd.GroupId
            let mediaSetFileId = PotionGroup.createMediaSetFileId cmd.QualityId mediaSetId
            let file =
                {
                    FileId = cmd.FileId
                    MediaSetFileId = mediaSetFileId
                    FilePath = cmd.FilePath
                    QualityId = cmd.QualityId
                    Source = cmd.Source
                }
            files |> Map.add cmd.QualityId file

    type PotionSubtitles =
        {
            Url: string
            Duration: int
            Language: string
            Source: string option
        }

    module PotionSubtitles =
        let addSubtitles (cmd: AddSubtitleCommand) subs =
            cmd.Subtitles
            |> List.fold
                (fun acc sub ->
                    acc
                    |> Map.add
                        sub.Language
                        {
                            Url = sub.Url
                            Duration = sub.Duration
                            Language = sub.Language
                            Source = cmd.Source
                        })
                subs

        let removeSubtitles (cmd: DeleteSubtitleCommand) subs = subs |> Map.remove cmd.Language

    type PendingCommand =
        {
            CommandId: string
            Command: PotionCommand
            Origin: Origin
            Timestamp: DateTimeOffset
            ForwardedFrom: string option
            Priority: byte
            RetryCount: int
        }

    module PendingCommand =
        let create commandId cmd origin timestamp priority forwardedFrom =
            {
                CommandId = commandId
                Command = cmd
                Origin = origin
                Timestamp = timestamp
                ForwardedFrom = forwardedFrom
                Priority = priority
                RetryCount = 0
            }
        let isTimeToRetry (retryIntervals: TimeSpan seq) (now: DateTimeOffset) (cmd: PendingCommand) =
            if cmd.RetryCount < Seq.length retryIntervals then
                let retryInterval =
                    retryIntervals |> Seq.truncate (cmd.RetryCount + 1) |> Seq.map _.TotalMinutes |> Seq.sum |> TimeSpan.FromMinutes
                now - cmd.Timestamp > retryInterval
            else
                false

    module PendingCommands =
        let addCommand (command: PendingCommand) (pendingCommands: PendingCommand list) =
            let pendingCommands =
                match command.Command with
                | AddFile cmd ->
                    pendingCommands
                    |> List.choose (fun cmd' ->
                        match cmd'.Command with
                        | AddFile cmd'' when cmd''.QualityId = cmd.QualityId ->
                            if cmd''.CorrelationId <> cmd.CorrelationId || cmd'.Origin = command.Origin then
                                None
                            else
                                Some cmd'
                        | _ -> Some cmd')
                | AddSubtitle cmd ->
                    pendingCommands
                    |> List.choose (fun cmd' ->
                        match cmd'.Command with
                        | AddSubtitle cmd'' ->
                            if cmd''.CorrelationId <> cmd.CorrelationId || cmd'.Origin = command.Origin then
                                None
                            else
                                Some cmd'
                        | _ -> Some cmd')
                | SetGeoBlock cmd ->
                    pendingCommands
                    |> List.choose (fun cmd' ->
                        match cmd'.Command with
                        | SetGeoBlock cmd'' ->
                            if cmd''.CorrelationId <> cmd.CorrelationId || cmd'.Origin = command.Origin then
                                None
                            else
                                Some cmd'
                        | _ -> Some cmd')
                | DeleteGroup cmd ->
                    pendingCommands
                    |> List.choose (fun cmd' ->
                        match cmd'.Command with
                        | DeleteGroup cmd'' ->
                            if cmd''.CorrelationId <> cmd.CorrelationId || cmd'.Origin = command.Origin then
                                None
                            else
                                Some cmd'
                        | _ -> None)
                | DeleteSubtitle cmd ->
                    pendingCommands
                    |> List.choose (fun cmd' ->
                        match cmd'.Command with
                        | DeleteSubtitle cmd'' ->
                            if cmd''.CorrelationId <> cmd.CorrelationId || cmd'.Origin = command.Origin then
                                None
                            else
                                Some cmd'
                        | _ -> Some cmd')
            command :: pendingCommands

        let removeCommand commandId pendingCommands =
            pendingCommands |> List.filter (fun x -> not (x.CommandId = commandId))

        let removeCommandForOrigin commandId origin pendingCommands =
            pendingCommands |> List.filter (fun x -> not (x.CommandId = commandId && x.Origin = origin))

        let getMatchingCommands correlationId fileId origin (pendingCommands: PendingCommand list) =
            pendingCommands
            |> List.filter (fun x -> x.Command.GetCorrelationId() = correlationId && x.Command.GetFileId() = fileId && x.Origin = origin)

        let bumpRetryCount commandId (pendingCommands: PendingCommand list) =
            match pendingCommands |> List.tryFind (fun x -> x.CommandId = commandId) with
            | Some cmd ->
                { cmd with
                    RetryCount = cmd.RetryCount + 1
                }
                :: (pendingCommands |> List.filter (fun x -> x.CommandId <> commandId))
            | None -> pendingCommands

    type PotionSetState =
        {
            Group: PotionGroup option
            Files: Map<int, PotionFile>
            Subtitles: Map<string, PotionSubtitles>
            LastVersion: int option
            CreationTime: DateTimeOffset
            ClientCommands: PotionCommand list
            PendingCommands: PendingCommand list
            ShouldNotRetryGeoBlockCommand: bool
            ShouldSkipNotifications: bool
            // Temporary field, to be removed after clip migration
            HasCommands: bool
        }

        static member Zero =
            {
                Group = None
                Files = Map.empty
                Subtitles = Map.empty
                LastVersion = None
                CreationTime = DateTimeOffset.MinValue
                ClientCommands = List.empty
                PendingCommands = List.empty
                ShouldNotRetryGeoBlockCommand = false
                ShouldSkipNotifications = false
                HasCommands = false
            }

    type PotionBump = { ContentId: string }

    type PotionShardExtractor(numOfShards) =
        inherit Akka.Cluster.Sharding.HashCodeMessageExtractor(numOfShards)
        override this.EntityId(message: obj) : string =
            match message with
            | :? Akka.Cluster.Sharding.ShardRegion.StartEntity as se -> se.EntityId
            | :? QueueMessage<PotionCommand> as message -> message.Payload.GetShortGroupId()
            | :? GetPotionSetState as message ->
                match message with
                | GetPotionSetState groupId -> groupId
            | :? Dto.Events.OddjobEventDto as message ->
                match message with
                | Dto.Events.OddjobEventDto.CommandStatus status when String.isNotNullOrEmpty status.ClientGroupId ->
                    status.ClientGroupId |> String.split '/' |> Array.last
                | _ -> // Won't work with old Potion clips where groupId and mediaSetId are unrelated
                    match MediaSetId.tryParse message.MediaSetId with
                    | Some mediaSetId -> Guid(mediaSetId.ContentId.Value).ToString()
                    | None -> invalidArg "message" $"Failed to parse MediaSetId from a message [%A{message}] of type [%s{message.GetType().FullName}]"
            | :? PotionBump as message -> message.ContentId
            | _ -> invalidArg "message" $"Unable to extract EntityId from a message [%A{message}] of type [%s{message.GetType().FullName}]"

    [<RequireQualifiedAccess>]
    module CorrelationId =
        [<Literal>]
        let InternalSetGeoBlock = "internal_setgeoblock"
        [<Literal>]
        let InternalRepair = "internal_repair"
        [<Literal>]
        let InternalDeprecated = "__internal"

        let isInternal correlationId =
            match correlationId with
            | InternalRepair
            | InternalSetGeoBlock -> true
            | InternalDeprecated -> true
            | _ -> false

    [<RequireQualifiedAccess>]
    module MessagePriority =
        [<Literal>]
        let Normal = 6uy
        [<Literal>]
        let Low = 3uy
