namespace Nrk.Oddjob.Core

open System.Text.RegularExpressions
open Nrk.Oddjob.Core.SmilTypes

[<AutoOpen>]
module MediaSetTypes =

    open System
    open FSharpx.Collections

    /// Non-empty, normalized (lower case, without hyphens) string representing resource set id, created from value from external system (PS) or internal mapping
    /// This is an internal value that can be used to create an actor name
    type ContentId =
        | ContentId of string

        member this.Value =
            this
            |> function
                | ContentId s -> s

    module ContentId =
        let create contentId =
            if String.isNullOrEmpty contentId then
                invalidArg "contentId" "Null or empty contentId"
            else
                ContentId contentId

    type MediaSetId =
        {
            ClientId: Alphanumeric
            ContentId: ContentId
        }

        member this.Value = sprintf "%s~%s" this.ClientId.Value this.ContentId.Value

    module MediaSetId =
        let create (clientId: Alphanumeric) (contentId: ContentId) =
            // Make sure strings are lowercased
            {
                ClientId = clientId.Value |> String.toLower |> Alphanumeric
                ContentId = contentId.Value |> String.toLower |> ContentId
            }

        let parse (serialized: string) =
            let arr = serialized.Split([| '/'; '~' |])
            {
                ClientId = Alphanumeric arr[0]
                ContentId = ContentId arr[1]
            }

        let tryParse (serialized: string) =
            let arr = serialized.Split([| '/'; '~' |])
            match arr with
            | [| clientId; contentId |] ->
                Some
                    {
                        ClientId = Alphanumeric clientId
                        ContentId = ContentId contentId
                    }
            | _ -> None

    [<RequireQualifiedAccess>]
    type DistributionState =
        | None
        | Requested_Deprecated
        | Initiated
        | Ingesting_Deprecated
        | Segmenting_Deprecated
        | Completed
        | Deleted
        | Cancelled
        | Failed
        | Rejected

        member this.IsFinal() =
            match this with
            | Completed
            | Deleted
            | Cancelled
            | Failed
            | Rejected -> true
            | _ -> false
        member this.IsPending() =
            match this with
            | None
            | Requested_Deprecated
            | Initiated
            | Ingesting_Deprecated
            | Segmenting_Deprecated -> true
            | _ -> false

    type RemoteState =
        {
            State: DistributionState
            Timestamp: DateTimeOffset
        }

        static member None =
            {
                State = DistributionState.None
                Timestamp = DateTimeOffset.MinValue
            }

    type RemoteResult = Result<unit, int * string>

    module RemoteResult =
        let create errorCode errorMessage =
            if errorCode = 0 && String.IsNullOrEmpty errorMessage then
                RemoteResult.Ok()
            else
                RemoteResult.Error(errorCode, errorMessage)

    type QualityId =
        | QualityId of int

        member this.Value =
            this
            |> function
                | QualityId s -> s

    module QualityId =
        let create qualityId =
            if qualityId < 0 then
                invalidArg "qualityId" "qualityId must be a non-negative integer"
            else
                QualityId qualityId

    [<RequireQualifiedAccess>]
    type MediaSetStatus =
        | Pending
        | Completed
        | Rejected
        | Expired

    module MediaSetStatus =
        let fromInt num =
            match num with
            | 0 -> MediaSetStatus.Pending
            | 1 -> MediaSetStatus.Completed
            | 2 -> MediaSetStatus.Rejected
            | 3 -> MediaSetStatus.Expired
            | _ -> invalidArg "num" <| sprintf "Invalid status %d" num

        let toInt status =
            match status with
            | MediaSetStatus.Pending -> 0
            | MediaSetStatus.Completed -> 1
            | MediaSetStatus.Rejected -> 2
            | MediaSetStatus.Expired -> 3

        let fromRemoteState remoteState =
            match remoteState with
            | DistributionState.Completed -> MediaSetStatus.Completed
            | DistributionState.Rejected -> MediaSetStatus.Rejected
            | _ -> MediaSetStatus.Pending

    type FileName =
        | FileName of string

        member this.Value =
            this
            |> function
                | FileName s -> s

        member this.IsEmpty() = String.IsNullOrEmpty this.Value

    module FileName =
        let parse path =
            let fileName = Utils.IO.getFileName path
            if String.isNullOrEmpty fileName then
                invalidArg "fileName" "fileName must not be empty"
            else
                FileName <| fileName

        let tryParse path =
            try
                parse path |> Some
            with _ ->
                None

        let create baseName qualityId extension =
            if String.isNullOrEmpty baseName then
                invalidArg "baseName" "baseName must not be empty"
            if qualityId < 0 then
                invalidArg "qualityId" "qualityId must be a non-negative integer"
            if String.isNullOrEmpty extension || not (extension.StartsWith ".") then
                invalidArg "extension" "extension must start with '.'"
            else
                FileName <| sprintf "%s_%d%s" (String.toLower baseName) qualityId extension

    type PartId =
        private
        | PartId of string

        member this.Value =
            this
            |> function
                | PartId s -> s
        static member TryParse(serialized: string) =
            match serialized with
            | ""
            | null -> None
            | x -> Some <| PartId x

    module PartId =
        let create partId =
            if String.isNullOrEmpty partId then
                invalidArg "partId" "Null or empty partId"
            else
                partId.Replace("-", "").ToLower() |> PartId

        let ofString str =
            if String.isNullOrEmpty str then
                None
            else
                str |> create |> Some

    type VideoProperties =
        {
            Codec: string
            DynamicRangeProfile: string
            DisplayAspectRatio: string
            Width: int
            Height: int
            FrameRate: decimal
        }

        static member Zero =
            {
                Codec = ""
                DynamicRangeProfile = ""
                DisplayAspectRatio = ""
                Width = 0
                Height = 0
                FrameRate = 0m
            }

    type Mixdown =
        | Stereo
        | Surround

    module Mixdown =
        let parse string =
            if string = "5.1" then Surround else Stereo

    type AudioProperties =
        {
            Mixdown: Mixdown
        }

        static member Zero = { Mixdown = Stereo }

    type MediaPropertiesV1 = Mixdown

    type MediaPropertiesV2 =
        {
            BitRate: int
            Duration: string
            Video: VideoProperties
            Audio: AudioProperties
            TranscodingVersion: int
        }

        static member Zero =
            {
                BitRate = 0
                Duration = ""
                Video = VideoProperties.Zero
                Audio = AudioProperties.Zero
                TranscodingVersion = 0
            }

    type MediaProperties =
        | MediaPropertiesV1 of MediaPropertiesV1
        | MediaPropertiesV2 of MediaPropertiesV2
        | Unspecified

    module MediaProperties =
        let transcodingVersion mediaProperties =
            match mediaProperties with
            | MediaPropertiesV2 p -> p.TranscodingVersion
            | _ -> 0

    type ContentFile =
        {
            QualityId: QualityId
            FileName: FileName
            SourcePath: FilePath option
            MediaProperties: MediaProperties
        }

        static member Zero =
            {
                QualityId = QualityId.create 0
                FileName = FileName ""
                SourcePath = None
                MediaProperties = MediaPropertiesV2 MediaPropertiesV2.Zero
            }

    module ContentFile =
        let transcodingVersion file =
            match file.MediaProperties with
            | MediaPropertiesV2 props -> props.TranscodingVersion
            | MediaPropertiesV1 _
            | Unspecified -> 0

    type SubtitlesTrackRef =
        {
            LanguageCode: Alphanumeric
            Name: Alphanumeric
        }

    [<RequireQualifiedAccess>]
    type SubtitlesLocation =
        | FilePath of FilePath option
        | AbsoluteUrl of AbsoluteUrl

        member this.Value =
            match this with
            | FilePath(Some filePath) -> FilePath.value filePath
            | FilePath None -> ""
            | AbsoluteUrl url -> url.Value

    module SubtitlesLocation =
        let fromFilePath filePath =
            FilePath.tryCreate filePath |> SubtitlesLocation.FilePath

        let fromAbsoluteUrl url =
            AbsoluteUrl url |> SubtitlesLocation.AbsoluteUrl

    type SubtitlesFileV1 =
        {
            LanguageCode: Alphanumeric
            Name: Alphanumeric
            FileName: FileName
            SourcePath: SubtitlesLocation
            Version: int
        }

        static member Zero =
            {
                LanguageCode = Alphanumeric ""
                Name = Alphanumeric ""
                FileName = FileName ""
                SourcePath = SubtitlesLocation.FilePath None
                Version = 0
            }

    type SubtitlesFileV2 =
        {
            LanguageCode: string
            Name: string
            Roles: string list
            FileName: FileName
            SourcePath: SubtitlesLocation
            Version: int
        }

        static member Zero =
            {
                LanguageCode = ""
                Name = ""
                Roles = List.empty
                FileName = FileName ""
                SourcePath = SubtitlesLocation.FilePath None
                Version = 0
            }

    [<RequireQualifiedAccess>]
    type SubtitlesFile =
        | V1 of SubtitlesFileV1
        | V2 of SubtitlesFileV2

        static member Zero = V2 SubtitlesFileV2.Zero
        member this.LanguageCode =
            match this with
            | V1 sub -> sub.LanguageCode.Value
            | V2 sub -> sub.LanguageCode
        member this.Name =
            match this with
            | V1 sub -> sub.Name.Value
            | V2 sub -> sub.Name
        member this.FileName =
            match this with
            | V1 sub -> sub.FileName
            | V2 sub -> sub.FileName
        member this.SourcePath =
            match this with
            | V1 sub -> sub.SourcePath
            | V2 sub -> sub.SourcePath
        member this.Version =
            match this with
            | V1 sub -> sub.Version
            | V2 sub -> sub.Version
        member this.TrackRef =
            match this with
            | SubtitlesFile.V1 sub ->
                {
                    LanguageCode = sub.LanguageCode
                    Name = sub.Name
                }
            | SubtitlesFile.V2 sub ->
                {
                    LanguageCode = Alphanumeric sub.LanguageCode
                    Name = Alphanumeric sub.Name
                }

    type SubtitlesLinkFormat =
        | M3U8
        | TTML
        | WebVTT

    type SubtitlesLink =
        {
            LanguageCode: Alphanumeric
            Name: Alphanumeric
            Format: SubtitlesLinkFormat
            SourcePath: AbsoluteUrl
            Version: int
        }

    type ContentChunk =
        {
            Files: ContentFile list
            Subtitles: SubtitlesFile list
            SubtitlesLinks: SubtitlesLink list
        }

        static member Zero =
            {
                Files = List.empty
                Subtitles = List.empty
                SubtitlesLinks = List.empty
            }

    type FileRef =
        {
            PartId: PartId option
            QualityId: QualityId
        }

    type SubtitlesRef = SubtitlesTrackRef

    [<RequireQualifiedAccess>]
    type ResourceRef =
        | File of FileRef
        | Subtitles of SubtitlesRef
        | Smil

        member this.PartId =
            match this with
            | File r -> r.PartId
            | Subtitles _ -> None
            | Smil -> None

    module FileRef =
        let fromResourceRef res =
            match res with
            | ResourceRef.File file -> file
            | _ -> invalidOp $"Unable to convert {res} to FileRef"

        let toFileName (mediaSetId: MediaSetId) (fileRef: FileRef) =
            let fileExtension = ".mp4"
            let baseName =
                match fileRef.PartId with
                | Some partId -> partId.Value
                | None ->
                    let (ContentId contentId) = mediaSetId.ContentId
                    contentId.Substring(0, Math.Min(8, contentId.Length))
            FileName.create baseName fileRef.QualityId.Value fileExtension

        let getDirectoryPath (mediaSetId: MediaSetId) (mediaSetVersion: int) (fileRef: FileRef) =
            let (ContentId contentId) = mediaSetId.ContentId
            match mediaSetVersion, fileRef.PartId with
            | 0, Some partId -> $"%s{contentId}~%s{partId.Value}"
            | 0, None -> $"%s{contentId}"
            | version, Some partId -> $"%s{contentId}~%d{version}~%s{partId.Value}"
            | version, None -> $"%s{contentId}~%d{version}"
            |> RelativeUrl

        let replaceLegacyQualityId (fileRef: FileRef) =
            match Legacy.qualityBitrates |> Map.tryFind fileRef.QualityId.Value with
            | Some bitrate ->
                { fileRef with
                    QualityId = QualityId.create bitrate
                }
            | None -> fileRef

    module SubtitlesRef =
        let fromResourceRef res =
            match res with
            | ResourceRef.Subtitles subs -> subs
            | _ -> invalidOp $"Unable to convert {res} to SubtitlesRef"

    type ThumbnailResolution = { Width: int; Height: int }

    module ContentChunk =
        let addOrReplaceFile (file: ContentFile) (chunk: ContentChunk) =
            { chunk with
                Files =
                    file :: (chunk.Files |> List.filter (fun x -> not (x.QualityId = file.QualityId || x.FileName = file.FileName)))
                    |> List.sortBy _.QualityId
            }

        let addOrReplaceSubtitles (sub: SubtitlesFile) (chunk: ContentChunk) =
            { chunk with
                Subtitles = sub :: (chunk.Subtitles |> List.filter (fun x -> x.TrackRef <> sub.TrackRef)) |> List.sortBy _.TrackRef
            }

        let removeFile (file: ContentFile) (chunk: ContentChunk) =
            { chunk with
                Files = chunk.Files |> List.filter (fun x -> x.QualityId <> file.QualityId)
            }

        let removeSubtitlesFile (trackRef: SubtitlesTrackRef) (chunk: ContentChunk) =
            { chunk with
                Subtitles = chunk.Subtitles |> List.filter (fun x -> x.TrackRef <> trackRef)
            }

        let removeSubtitlesFiles (chunk: ContentChunk) = { chunk with Subtitles = List.empty }

        let removeSubtitlesLinks (chunk: ContentChunk) =
            { chunk with
                SubtitlesLinks = List.empty
            }

        let merge (newChunk: ContentChunk) (originalChunk: ContentChunk) =
            let rec mergeItems items mergeFn chunk =
                match items with
                | [] -> chunk
                | item :: items -> chunk |> mergeFn item |> mergeItems items mergeFn
            originalChunk |> mergeItems newChunk.Files addOrReplaceFile |> mergeItems newChunk.Subtitles addOrReplaceSubtitles

        let containsFile (file: ContentFile) (chunk: ContentChunk) =
            chunk.Files |> List.exists (fun x -> x.QualityId = file.QualityId)

        let containsQuality (qualityId: QualityId) (chunk: ContentChunk) =
            chunk.Files |> List.exists (fun x -> x.QualityId = qualityId)

    type ContentPart =
        {
            PartNumber: int
            Content: ContentChunk
        }

        static member Zero =
            {
                PartNumber = 0
                Content = ContentChunk.Zero
            }

    module ContentPart =
        let merge (newPart: ContentPart) (originalPart: ContentPart) =
            if originalPart = ContentPart.Zero then
                newPart
            else
                { originalPart with
                    Content = originalPart.Content |> ContentChunk.merge newPart.Content
                }

    type ContentSet =
        | Empty
        | NoParts of ContentChunk
        | Parts of PartId list * ContentChunk

    module ContentSet =
        // assignPart is only used in unit tests to build MediaSet state with parts before tests publish subtitles jobs.
        // Publishing subtitles require MediaSet to contain files so they must be created up front using assignPart and assignFile methods.
        let assignPart partId partNumber (content: ContentSet) =
            match content with
            | Empty -> Parts(partId |> List.singleton, ContentChunk.Zero)
            | Parts([ partId' ], _) when partId = partId' -> content
            | Parts(partIds, chunk) ->
                if partIds |> Seq.notContains partId then
                    (partId :: partIds |> List.distinct, chunk) |> Parts
                else
                    content
            | NoParts _ -> invalidOp "Unable to add part to content set with no parts"

        let assignFile partId (file: ContentFile) (content: ContentSet) =
            let newChunk =
                {
                    Files = [ file ]
                    Subtitles = List.empty
                    SubtitlesLinks = List.empty
                }
            match content, partId with
            | Empty, None -> NoParts newChunk
            | NoParts chunk, None -> chunk |> ContentChunk.addOrReplaceFile file |> NoParts
            | Parts(partIds, chunk), _ -> (partIds, chunk |> ContentChunk.addOrReplaceFile file) |> Parts
            | _ -> content // Do not update content on deprecated file assignment events (multipart)

        let assignSubtitlesFile (sub: SubtitlesFile) (content: ContentSet) =
            let newChunk =
                {
                    Files = List.empty
                    Subtitles = [ sub ]
                    SubtitlesLinks = List.empty
                }
            match content with
            | Empty -> NoParts newChunk
            | NoParts chunk -> chunk |> ContentChunk.addOrReplaceSubtitles sub |> NoParts
            | Parts(partIds, chunk) -> (partIds, chunk |> ContentChunk.addOrReplaceSubtitles sub) |> Parts

        let assignSubtitlesFiles (subs: SubtitlesFile list) (content: ContentSet) =
            // Clear current subtitles files
            let content =
                match content with
                | NoParts chunk -> { chunk with Subtitles = List.empty } |> NoParts
                | Parts(partIds, chunk) -> (partIds, { chunk with Subtitles = List.empty }) |> Parts
                | _ -> content
            // Add new subtitles files
            subs |> List.fold (fun acc elt -> acc |> assignSubtitlesFile elt) content

        let updateChunckSubtitleLinks (chunk: ContentChunk) (newLinks: SubtitlesLink list) =
            let oldVersion = chunk.SubtitlesLinks |> List.map _.Version |> List.tryMaxBy id
            let newVersion = newLinks |> List.map _.Version |> List.tryMaxBy id

            match oldVersion, newVersion with
            | Some ov, Some nv when ov = 0 || nv > ov -> { chunk with SubtitlesLinks = newLinks } // Note: always update if version was 0, e.g. granitt subs
            | None, Some _ -> { chunk with SubtitlesLinks = newLinks }
            | Some _, Some _
            | Some _, None
            | None, None -> chunk

        let assignSubtitlesLinks (links: SubtitlesLink list) (content: ContentSet) =
            let newChunk =
                {
                    Files = List.empty
                    Subtitles = List.empty
                    SubtitlesLinks = links
                }
            match content with
            | Empty -> NoParts newChunk
            | NoParts chunk -> NoParts(updateChunckSubtitleLinks chunk links)
            | Parts(partIds, chunk) -> (partIds, (updateChunckSubtitleLinks chunk links)) |> Parts

        let removePart partId (content: ContentSet) =
            match content with
            | Empty -> content
            | Parts([ partId' ], _) when partId' = partId -> Empty
            | Parts _ -> content
            | NoParts _ -> invalidOp "Unable to remove part from NoPart"

        let removeFile partId (file: ContentFile) (content: ContentSet) =
            match content, partId with
            | Empty, _ -> content
            | NoParts chunk, None -> chunk |> ContentChunk.removeFile file |> NoParts
            | Parts(partIds, chunk), _ -> (partIds, chunk |> ContentChunk.removeFile file) |> Parts
            | _ -> content // Do not update content on deprecated file assignment events (multipart)

        let removeSubtitlesFile trackRef (content: ContentSet) =
            match content with
            | Empty -> content
            | NoParts chunk -> chunk |> ContentChunk.removeSubtitlesFile trackRef |> NoParts
            | Parts(partIds, chunk) -> (partIds, chunk |> ContentChunk.removeSubtitlesFile trackRef) |> Parts

        let removeSubtitlesFiles (content: ContentSet) =
            match content with
            | Empty -> content
            | NoParts chunk -> chunk |> ContentChunk.removeSubtitlesFiles |> NoParts
            | Parts(partIds, chunk) -> (partIds, chunk |> ContentChunk.removeSubtitlesFiles) |> Parts

        let removeSubtitlesLinks (content: ContentSet) =
            match content with
            | Empty -> content
            | NoParts chunk -> chunk |> ContentChunk.removeSubtitlesLinks |> NoParts
            | Parts(partIds, chunk) -> (partIds, chunk |> ContentChunk.removeSubtitlesLinks) |> Parts

        let getFiles (content: ContentSet) =
            match content with
            | NoParts chunk ->
                match chunk.Files with
                | [] -> Map.empty
                | files ->
                    files
                    |> List.map (fun file ->
                        {
                            PartId = None
                            QualityId = file.QualityId
                        },
                        file)
                    |> Map.ofList
            | Parts([ partId ], chunk) ->
                match chunk.Files with
                | [] -> Map.empty
                | files ->
                    files
                    |> List.map (fun file ->
                        {
                            PartId = Some partId
                            QualityId = file.QualityId
                        },
                        file)
                    |> Map.ofList
            | Parts(_, chunk) ->
                match chunk.Files with
                | [] -> Map.empty
                | files ->
                    files
                    |> List.map (fun file ->
                        {
                            PartId = None
                            QualityId = file.QualityId
                        },
                        file)
                    |> Map.ofList
            | _ -> Map.empty

        let maxMixdown content =
            let mixdowns =
                content
                |> getFiles
                |> Map.values
                |> Seq.map (fun contentFile ->
                    match contentFile.MediaProperties with
                    | Unspecified -> Stereo
                    | MediaPropertiesV1 mixdown -> mixdown
                    | MediaPropertiesV2 properties -> properties.Audio.Mixdown)
            if Seq.isEmpty mixdowns then
                Stereo
            else
                Seq.maxBy (fun mixdown -> mixdown = Surround) mixdowns

        // For any file without MediaProperties = Unspecified, set MediaPropertiesV1 with the specified mixdown
        let replaceUnspecifiedMixdowns contentSet mixdown =
            let updateFile file mixdown =
                match file.MediaProperties with
                | Unspecified ->
                    { file with
                        MediaProperties = MediaPropertiesV1 mixdown
                    }
                | MediaPropertiesV1 _
                | MediaPropertiesV2 _ -> file
            let updateFiles files mixdown =
                files |> List.map (fun (file: ContentFile) -> updateFile file mixdown)
            match contentSet with
            | Empty -> Empty
            | NoParts chunk ->
                NoParts
                    { chunk with
                        Files = updateFiles chunk.Files mixdown
                    }
            | Parts(partIds, chunk) ->
                (partIds,
                 { chunk with
                     Files = updateFiles chunk.Files mixdown
                 })
                |> Parts

        let tryGetFile fileRef (content: ContentSet) = getFiles content |> Map.tryFind fileRef

        let getSubtitles (content: ContentSet) =
            match content with
            | Empty -> List.empty
            | NoParts chunk -> chunk.Subtitles
            | Parts(_, chunk) -> chunk.Subtitles

        let tryGetSubtitles (subRef: SubtitlesRef) (content: ContentSet) =
            getSubtitles content |> List.tryFind (fun x -> x.TrackRef = subRef)

        let containsPart partId (content: ContentSet) =
            match content with
            | Parts(partIds, _) when partIds |> Seq.contains partId -> true
            | _ -> false

        let containsFileRef fileRef (content: ContentSet) =
            content |> getFiles |> Map.containsKey fileRef

        let containsSubtitlesRef (subRef: SubtitlesRef) (content: ContentSet) =
            content |> getSubtitles |> List.exists (fun x -> x.TrackRef = subRef)

        let merge (newContent: ContentSet) (originalContent: ContentSet) =
            match originalContent, newContent with
            | Empty, _ -> newContent
            | NoParts originalChunk, NoParts newChunk -> originalChunk |> ContentChunk.merge newChunk |> NoParts
            | Parts(originalPartIds, originalChunk), Parts(newPartIds, newChunk) ->
                ([ originalPartIds; newPartIds ] |> List.concat |> List.distinct, originalChunk |> ContentChunk.merge newChunk) |> Parts
            | _ -> invalidOp $"Unable to merge contents of different types, original content: {originalContent}, new content: {newContent}"

    // Desired state

    type DesiredMediaSetState =
        {
            GeoRestriction: GeoRestriction
            Content: ContentSet
            MediaType: MediaType
            RevokedParts: PartId list
        }

        static member Zero =
            {
                GeoRestriction = GeoRestriction.Unspecified
                Content = ContentSet.Empty
                MediaType = MediaType.Default
                RevokedParts = []
            }
        member this.IsEmpty() =
            this.GeoRestriction = GeoRestriction.Unspecified && this.Content = ContentSet.Empty

    // GlobalConnect storage types

    let private normalizeGlobalConnectFileName (fileName: string) =
        let result = fileName.ToLower().Replace("æ", "ae").Replace("ø", "oe").Replace("å", "aa")
        Regex(@"[^a-z0-9_\-.]").Replace(result, String.Empty)

    type GlobalConnectFile =
        {
            SourcePath: FilePath option
            RemotePath: RelativeUrl
            TranscodingVersion: int
        }

        static member Zero =
            {
                SourcePath = None
                RemotePath = RelativeUrl ""
                TranscodingVersion = 0
            }

    module GlobalConnectFile =
        let fromContentFile remotePathBase (file: ContentFile) =
            let fileName = file.FileName.Value |> normalizeGlobalConnectFileName
            let remotePath = $"{remotePathBase}/{fileName}"
            {
                SourcePath = file.SourcePath
                RemotePath = RelativeUrl remotePath
                TranscodingVersion = ContentFile.transcodingVersion file
            }

    type GlobalConnectSubtitles =
        {
            Subtitles: SubtitlesFile
            RemotePath: RelativeUrl
        }

        static member Zero =
            {
                Subtitles = SubtitlesFile.Zero
                RemotePath = RelativeUrl ""
            }

    module GlobalConnectSubtitles =
        let fromSubtitlesFile remotePathBase (file: SubtitlesFile) =
            let fileName = file.FileName.Value |> normalizeGlobalConnectFileName
            let remotePath = $"{remotePathBase}/{fileName}"
            {
                Subtitles =
                    match file with
                    | SubtitlesFile.V1 file ->
                        SubtitlesFile.V1
                            { file with
                                FileName = FileName fileName
                            }
                    | SubtitlesFile.V2 file ->
                        SubtitlesFile.V2
                            { file with
                                FileName = FileName fileName
                            }
                RemotePath = RelativeUrl remotePath
            }

    type GlobalConnectSmil =
        {
            FileName: FileName
            Content: SmilTypes.SmilDocument
            RemotePath: RelativeUrl
            Version: int
        }

        static member Zero =
            {
                FileName = FileName ""
                Content = SmilDocument.Zero
                RemotePath = RelativeUrl ""
                Version = 0
            }

        member this.IsEmpty() =
            this.FileName.IsEmpty() && this.Content = SmilDocument.Zero && this.RemotePath.IsEmpty() && this.Version = 0

    type GlobalConnectFileState =
        {
            File: GlobalConnectFile
            RemoteState: RemoteState
            LastResult: RemoteResult
        }

        static member Zero =
            {
                File = GlobalConnectFile.Zero
                RemoteState = RemoteState.None
                LastResult = Result.Ok()
            }

    type GlobalConnectSubtitlesState =
        {
            Subtitles: GlobalConnectSubtitles
            RemoteState: RemoteState
            LastResult: RemoteResult
        }

        static member Zero =
            {
                Subtitles = GlobalConnectSubtitles.Zero
                RemoteState = RemoteState.None
                LastResult = RemoteResult.Ok()
            }

    type GlobalConnectSmilState =
        {
            Smil: GlobalConnectSmil
            RemoteState: RemoteState
            LastResult: RemoteResult
        }

        static member Zero =
            {
                Smil = GlobalConnectSmil.Zero
                RemoteState = RemoteState.None
                LastResult = Result.Ok()
            }

    [<RequireQualifiedAccess>]
    type GlobalConnectResource =
        | File of GlobalConnectFile
        | Subtitles of GlobalConnectSubtitles
        | Smil of GlobalConnectSmil

        member this.RemotePath =
            match this with
            | File x -> x.RemotePath
            | Subtitles x -> x.RemotePath
            | Smil x -> x.RemotePath

    [<RequireQualifiedAccess>]
    type GlobalConnectResourceState =
        | File of GlobalConnectFileState
        | Subtitles of GlobalConnectSubtitlesState
        | Smil of GlobalConnectSmilState

        member this.Resource =
            match this with
            | File x -> GlobalConnectResource.File x.File
            | Subtitles x -> GlobalConnectResource.Subtitles x.Subtitles
            | Smil x -> GlobalConnectResource.Smil x.Smil
        member this.RemoteState =
            match this with
            | File x -> x.RemoteState
            | Subtitles x -> x.RemoteState
            | Smil x -> x.RemoteState

    module GlobalConnectResourceState =
        let updateResource resource state =
            match state, resource with
            | GlobalConnectResourceState.File x, GlobalConnectResource.File y -> GlobalConnectResourceState.File { x with File = y }
            | GlobalConnectResourceState.Subtitles x, GlobalConnectResource.Subtitles y -> GlobalConnectResourceState.Subtitles { x with Subtitles = y }
            | GlobalConnectResourceState.Smil x, GlobalConnectResource.Smil y -> GlobalConnectResourceState.Smil { x with Smil = y }
            | _ -> invalidOp <| $"Unable to update %A{state} with %A{resource}"

        let updateRemoteState remoteState state =
            match state with
            | GlobalConnectResourceState.File x -> GlobalConnectResourceState.File { x with RemoteState = remoteState }
            | GlobalConnectResourceState.Subtitles x -> GlobalConnectResourceState.Subtitles { x with RemoteState = remoteState }
            | GlobalConnectResourceState.Smil x -> GlobalConnectResourceState.Smil { x with RemoteState = remoteState }

    // MediaSetState

    type GlobalConnectFilesStates = Map<FileRef, GlobalConnectFileState>

    type GlobalConnectSubtitlesStates = Map<SubtitlesRef, GlobalConnectSubtitlesState>

    type CurrentGlobalConnectState =
        {
            Files: GlobalConnectFilesStates
            Subtitles: GlobalConnectSubtitlesStates
            Smil: GlobalConnectSmilState
            Version: int
        }

        static member Zero =
            {
                Files = Map.empty
                Subtitles = Map.empty
                Smil = GlobalConnectSmilState.Zero
                Version = 0
            }

        member this.IsEmpty() =
            { this with Version = 0 } = CurrentGlobalConnectState.Zero

    module CurrentGlobalConnectState =
        let getCompletedFiles (state: CurrentGlobalConnectState) =
            state.Files |> Map.filter (fun _ file -> file.RemoteState.State = DistributionState.Completed)

        let getCompletedSubtitles (state: CurrentGlobalConnectState) =
            state.Subtitles |> Map.filter (fun _ sub -> sub.RemoteState.State = DistributionState.Completed)

        let isPlayable (state: CurrentGlobalConnectState) =
            state |> getCompletedFiles |> Map.isNotEmpty && state.Smil.RemoteState.State = DistributionState.Completed

        let hasPendingFiles (state: CurrentGlobalConnectState) =
            state.Files |> Map.exists (fun _ file -> file.RemoteState.State.IsPending())

        let hasPendingSubtitles (state: CurrentGlobalConnectState) =
            state.Subtitles |> Map.exists (fun _ sub -> sub.RemoteState.State.IsPending())

        let hasPendingSmil (state: CurrentGlobalConnectState) =
            not (state.Smil.Smil.IsEmpty()) && state.Smil.RemoteState.State.IsPending()

        let hasPendingResources state =
            hasPendingFiles state || hasPendingSubtitles state || hasPendingSmil state

    [<RequireQualifiedAccess>]
    module GlobalConnectAccessRestrictions =
        [<Literal>]
        let World = "world"

        [<Literal>]
        let Norway = "no"

        [<Literal>]
        let Nrk = "nrk"

        let fromGeoRestriction gr =
            match gr with
            | GeoRestriction.World -> World
            | GeoRestriction.Norway -> Norway
            | GeoRestriction.NRK -> Nrk
            | GeoRestriction.Unspecified -> Nrk // Use default

        let toGeoRestriction gr =
            match gr with
            | World -> GeoRestriction.World
            | Norway -> GeoRestriction.Norway
            | Nrk -> GeoRestriction.NRK
            | _ -> GeoRestriction.Unspecified

        let parse (path: RelativeUrl) =
            let segments = path.Value.Split([| '/' |])
            toGeoRestriction segments[1]

        let apply (gr: GeoRestriction) (path: RelativeUrl) =
            let gr = fromGeoRestriction gr
            let segments = path.Value.Split([| '/' |])
            let segments = Array.concat [| [| segments[0] |]; [| gr; "open" |]; segments[3..] |]
            String.Join("/", segments) |> RelativeUrl

    [<RequireQualifiedAccess>]
    module GlobalConnectPathBase =
        let fromGeoRestriction (mediaSetId: MediaSetId) (geoRestriction: GeoRestriction) =
            let geoSegment = GlobalConnectAccessRestrictions.fromGeoRestriction geoRestriction
            let contentId = mediaSetId.ContentId.Value.Replace("æ", "ae").Replace("ø", "oe").Replace("å", "aa")
            let clientId = mediaSetId.ClientId.Value
            $"assets/{geoSegment}/open/{clientId}/{contentId.Substring(0, 4)}/{contentId}"

    module GlobalConnectSmil =

        let generateSmilFileName (content: SmilDocument) version =
            $"{content.GetFileNameBase()}-{version}.smil"

        let private createTextStreamElement src languageCode subtitleName subtitleAccessibility isDefault =
            TextStream
                {
                    Src = src
                    Language = languageCode
                    SystemLanguage = languageCode
                    SubtitleName = subtitleName
                    SubtitleAccessibility = subtitleAccessibility
                    SubtitleDefault = if isDefault then "true" else null
                }

        let private createSubtitleV1TextStreamElements (subtitlesFiles: SubtitlesFileV1 list) : SmilElement list =
            subtitlesFiles
            |> List.filter (fun s -> List.exists (fun sc -> List.contains s.LanguageCode.Value (fst sc)) SubtitlesRules.V1.subtitlesConfig) //Removes unsupported languages
            |> List.groupBy (fun s -> List.find (fun sc -> List.contains s.LanguageCode.Value (fst sc)) SubtitlesRules.V1.subtitlesConfig) //Groups subtitles by appropriate ruleset
            |> List.sortBy (fun (rules, _) -> List.findIndex ((=) rules) SubtitlesRules.V1.subtitlesConfig) //Sort language groups by order in config
            |> List.map (fun (rules: string list * SubtitlesRules.V1.SubtitleRuleSet list, subs) ->
                let isSingle = List.isSingleton subs
                subs
                |> List.choose (fun sub ->
                    let function' = sub.Name.Value
                    maybe {
                        let! appropriateRule =
                            List.tryFind
                                (fun (rule: SubtitlesRules.V1.SubtitleRuleSet) ->
                                    rule.IsSingle = isSingle && (List.contains function' rule.Functions || List.contains "*" rule.Functions))
                                (snd rules)
                        let gcPath = sub.FileName.Value |> normalizeGlobalConnectFileName
                        let externalLanguageCode = appropriateRule.LanguageCode
                        let externalLabel = appropriateRule.Label
                        let isTranscription = appropriateRule.SubtitleAccessibility
                        let isDefault = appropriateRule.IsDefaultTrack
                        let sortOrder = List.findIndex ((=) appropriateRule) (snd rules) //Find order to sort internally in language group
                        return (sortOrder, createTextStreamElement gcPath externalLanguageCode externalLabel isTranscription isDefault)
                    })
                |> List.sortBy fst
                |> List.map snd)
            |> List.collect id

        let private createSubtitleV2TextStreamElements (subtitlesFiles: SubtitlesFileV2 list) : SmilElement list =
            let filteredSubs =
                subtitlesFiles
                |> List.filter (fun s -> List.exists (fun sc -> List.contains s.LanguageCode (fst sc)) SubtitlesRules.V2.subtitlesConfig) //Removes unsupported languages

            SubtitlesRules.V2.subtitlesConfig
            |> List.fold
                (fun (subsToUse: (SubtitlesFileV2 * SubtitlesRules.V2.SubtitleRuleSet) list) (languages, rule) ->
                    match filteredSubs |> List.tryFind (fun s -> List.contains s.LanguageCode languages && s.Name = rule.Name) with
                    | Some sub ->
                        //We found a subtitle track matching our rule and languagecode, only add it if its roles are unique of its language
                        let isUnique =
                            subsToUse
                            |> List.filter (fun (_, r) -> r.LanguageCode = rule.LanguageCode)
                            |> List.exists (fun (s, _) -> s.Roles |> List.sort |> (=) (sub.Roles |> List.sort))
                            |> not

                        if isUnique then
                            let subsToUse =
                                if rule.IsDefaultTrack then //must override previous default tracks, can only be one per language
                                    subsToUse
                                    |> List.map (fun (s, r) ->
                                        if rule.LanguageCode = r.LanguageCode then
                                            (s, { r with IsDefaultTrack = false })
                                        else
                                            (s, r))
                                else
                                    subsToUse
                            subsToUse @ [ (sub, rule) ]
                        else
                            subsToUse
                    | None -> subsToUse)
                List.empty
            |> List.map (fun (sub, rule) ->
                let gcPath = sub.FileName.Value |> normalizeGlobalConnectFileName
                let subtitleAccessibility =
                    match rule.SubtitleAccessibilityRoleRule with
                    | Some(role, an) when sub.Roles |> List.contains role -> an
                    | _ -> null
                createTextStreamElement gcPath rule.LanguageCode rule.Label subtitleAccessibility rule.IsDefaultTrack)


        let private createSubtitleTextStreamElements (subtitlesFiles: SubtitlesFile list) : SmilElement list =
            match subtitlesFiles |> List.tryHead with
            | Some(SubtitlesFile.V1 _) ->
                subtitlesFiles
                |> List.choose (function
                    | SubtitlesFile.V1 s -> Some s
                    | _ -> None)
                |> createSubtitleV1TextStreamElements
            | Some(SubtitlesFile.V2 _) ->
                subtitlesFiles
                |> List.choose (function
                    | SubtitlesFile.V2 s -> Some s
                    | _ -> None)
                |> createSubtitleV2TextStreamElements
            | None -> List.empty

        let private createSmilDocument mediaType (mediaFiles: FileName list) (subtitlesFiles: SubtitlesFile list) =
            let files =
                mediaFiles
                |> List.map (fun fileName ->
                    let fileName = fileName.Value |> normalizeGlobalConnectFileName
                    match mediaType with
                    | MediaType.Audio -> Audio { Src = fileName }
                    | MediaType.Video -> Video { Src = fileName })

            let subtitles = createSubtitleTextStreamElements subtitlesFiles

            let content = [ files; subtitles ] |> List.concat
            let document =
                {
                    SmilDocument.Content =
                        match content with
                        | [] -> List.empty
                        | content -> [ { Switch = content } ]
                }
            document

        let fromCurrentState (desiredState: DesiredMediaSetState) (currentState: CurrentGlobalConnectState) =
            let mediaFiles =
                currentState.Files
                |> Map.filter (fun _ file -> GlobalConnectAccessRestrictions.parse file.File.RemotePath = desiredState.GeoRestriction)
                |> Map.filter (fun fileRef _ -> desiredState.Content |> ContentSet.getFiles |> Map.keys |> Seq.contains fileRef)
                |> Map.filter (fun _ file -> file.RemoteState.State = DistributionState.Completed)
                |> Map.values
                |> Seq.map _.File.RemotePath.Value
                |> Seq.map (fun path -> path.Split [| '/' |] |> Seq.last |> FileName)
                |> Seq.toList
            let subtitlesFiles =
                currentState.Subtitles
                |> Map.filter (fun _ file -> GlobalConnectAccessRestrictions.parse file.Subtitles.RemotePath = desiredState.GeoRestriction)
                |> Map.filter (fun subRef _ -> desiredState.Content |> ContentSet.getSubtitles |> Seq.map _.TrackRef |> Seq.contains subRef)
                |> Map.filter (fun _ file -> file.RemoteState.State = DistributionState.Completed)
                |> Map.values
                |> Seq.map _.Subtitles.Subtitles
                |> Seq.toList
            createSmilDocument desiredState.MediaType mediaFiles subtitlesFiles

    [<RequireQualifiedAccess>]
    type OriginState = GlobalConnect of CurrentGlobalConnectState

    type CurrentMediaSetState =
        {
            GlobalConnect: CurrentGlobalConnectState
        }

        static member Zero =
            {
                GlobalConnect = CurrentGlobalConnectState.Zero
            }

        member this.IsEmpty() = this.GlobalConnect.IsEmpty()

    module CurrentMediaSetState =
        let isPlayable (state: CurrentMediaSetState) origins =
            List.contains Origin.GlobalConnect origins && CurrentGlobalConnectState.isPlayable state.GlobalConnect

        let hasPendingFiles (state: CurrentMediaSetState) origins =
            List.contains Origin.GlobalConnect origins && CurrentGlobalConnectState.hasPendingFiles state.GlobalConnect

        let hasPendingSubtitles (state: CurrentMediaSetState) origins =
            List.contains Origin.GlobalConnect origins && CurrentGlobalConnectState.hasPendingSubtitles state.GlobalConnect

        let hasPendingSmil (state: CurrentMediaSetState) origins =
            List.contains Origin.GlobalConnect origins && CurrentGlobalConnectState.hasPendingSmil state.GlobalConnect

        let tryGetRemoteFileState origin fileRef (state: CurrentMediaSetState) =
            match origin with
            | Origin.GlobalConnect -> state.GlobalConnect.Files |> Map.tryFind fileRef |> Option.map _.RemoteState

        let tryGetRemoteSubtitlesState subRef (state: CurrentMediaSetState) =
            state.GlobalConnect.Subtitles |> Map.tryFind subRef |> Option.map _.RemoteState

        let getLastResourceCompletionTime (state: CurrentMediaSetState) origins =
            [
                if List.contains Origin.GlobalConnect origins then
                    state.GlobalConnect
                    |> CurrentGlobalConnectState.getCompletedFiles
                    |> Map.map (fun _ file -> file.RemoteState.Timestamp)
                    |> Map.values
                    |> Seq.toList
                    state.GlobalConnect
                    |> CurrentGlobalConnectState.getCompletedSubtitles
                    |> Map.map (fun _ file -> file.RemoteState.Timestamp)
                    |> Map.values
                    |> Seq.toList
                    if state.GlobalConnect.Smil.RemoteState.State = DistributionState.Completed then
                        [ state.GlobalConnect.Smil.RemoteState.Timestamp ]
                else
                    List.empty
            ]
            |> List.concat
            |> List.tryMaxBy id
            |> Option.defaultValue DateTimeOffset.MinValue

        let getLastUpdateTime (state: CurrentMediaSetState) origins =
            [ getLastResourceCompletionTime state origins ] |> List.max

        let hasPendingResources (state: CurrentMediaSetState) =
            CurrentGlobalConnectState.hasPendingResources state.GlobalConnect

    // Schema versions
    // 1: With Akamai, NEP and GlobalConnect origins
    // 2: With NEP and GlobalConnect origins
    // 3: With GlobalConnect origin, no mediaAccess

    [<Literal>]
    let CurrentSchemaVersion = 3

    type MediaSetState =
        {
            Desired: DesiredMediaSetState
            Current: CurrentMediaSetState
            ClientContentId: string option
            LastSequenceNr: int64
            SchemaVersion: int
        }

        static member Zero =
            {
                Desired = DesiredMediaSetState.Zero
                Current = CurrentMediaSetState.Zero
                ClientContentId = None
                LastSequenceNr = 0L
                SchemaVersion = 0
            }
        member this.IsEmpty() =
            this.Desired.IsEmpty() && this.Current = CurrentMediaSetState.Zero

    module MediaSetState =
        let purgeUnusedResources (state: MediaSetState) =
            let globalConnectFiles =
                state.Current.GlobalConnect.Files
                |> Map.filter (fun fileRef file ->
                    file.RemoteState.State = DistributionState.Completed || state.Desired.Content |> ContentSet.containsFileRef fileRef)
            let globalConnectSubtitles =
                state.Current.GlobalConnect.Subtitles
                |> Map.filter (fun subRef sub ->
                    sub.RemoteState.State = DistributionState.Completed || state.Desired.Content |> ContentSet.containsSubtitlesRef subRef)
            { state with
                Current.GlobalConnect.Files = globalConnectFiles
                Current.GlobalConnect.Subtitles = globalConnectSubtitles
            }

    [<RequireQualifiedAccess>]
    type MediaSetValidation =
        /// Validate only local persisted state
        | LocalState
        /// Validate media at CDN
        | RemoteStorage
        /// Cleanup storage
        | CleanupStorage
        /// Validate mediaset playability
        | Playback

    module MediaSetValidation =
        let Local = [ MediaSetValidation.LocalState ]

        let LocalAndRemote = [ MediaSetValidation.LocalState; MediaSetValidation.RemoteStorage ]

        let LocalAndRemoteWithCleanup =
            [
                MediaSetValidation.LocalState
                MediaSetValidation.RemoteStorage
                MediaSetValidation.CleanupStorage
            ]
