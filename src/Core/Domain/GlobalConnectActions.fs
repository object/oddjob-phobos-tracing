namespace Nrk.Oddjob.Core

open System
open System.Net
open Akka.Event
open FSharpx.Collections
open FsHttp

type GlobalConnectActions = GlobalConnectCommand list

module GlobalConnectActions =

    let private isFileCompleted (ctx: GlobalConnectActionContext) (globalConnectFiles: GlobalConnectFilesStates) fileRef =
        match globalConnectFiles |> Map.tryFind fileRef with
        | Some fileState ->
            if fileState.RemoteState.State <> DistributionState.Completed then
                ctx.Logger.Debug $"File {fileRef} has state {fileState.RemoteState.State}"
                false
            else
                true
        | None -> false

    let private isFileRejected (globalConnectFiles: GlobalConnectFilesStates) fileRef =
        match globalConnectFiles |> Map.tryFind fileRef with
        | Some fileState -> fileState.RemoteState.State = DistributionState.Rejected
        | None -> false

    let private isFileVersionNewer (ctx: GlobalConnectActionContext) (contentFile: ContentFile) (fileState: GlobalConnectFileState) =
        if ContentFile.transcodingVersion contentFile > fileState.File.TranscodingVersion then
            ctx.Logger.Debug
                $"File {contentFile.FileName} has newer version {ContentFile.transcodingVersion contentFile} (was {fileState.File.TranscodingVersion})"
            true
        else
            false

    let private isFileContentChanged (ctx: GlobalConnectActionContext) (contentFile: ContentFile) (fileState: GlobalConnectFileState) =
        try
            maybe {
                let! validSourcePath = contentFile.SourcePath
                let! validMapping = FilePath.tryMap ctx.Environment.PathMappings validSourcePath
                return ctx.Environment.LocalFileSystem.GetLastModifiedTime validMapping > fileState.RemoteState.Timestamp
            }
            |> Option.defaultValue false
        with _ ->
            false

    let private isFileModified (ctx: GlobalConnectActionContext) (globalConnectFiles: GlobalConnectFilesStates) fileRef (contentFile: ContentFile) =
        match globalConnectFiles |> Map.tryFind fileRef with
        | Some fileState when Option.isSome contentFile.SourcePath ->
            isFileVersionNewer ctx contentFile fileState || isFileContentChanged ctx contentFile fileState
        | _ -> false

    let private isFilePresentAtSource (ctx: GlobalConnectActionContext) (contentFile: ContentFile) =
        try
            match contentFile.SourcePath with
            | Some filePath -> ctx.Environment.LocalFileSystem.FileExists(FilePath.value filePath)
            | _ -> false
        with _ ->
            false

    let private isFilePresentAtOrigin (ctx: GlobalConnectActionContext) (globalConnectFiles: GlobalConnectFilesStates) fileRef =
        try
            match globalConnectFiles |> Map.tryFind fileRef with
            | Some fileState -> ctx.Environment.RemoteFileSystemResolver().FileExists fileState.File.RemotePath.Value
            | _ -> false
        with _ ->
            false

    let private isSubtitlesCompleted (ctx: GlobalConnectActionContext) (globalConnectSubtitles: GlobalConnectSubtitlesStates) subRef =
        match globalConnectSubtitles |> Map.tryFind subRef with
        | Some subState ->
            if subState.RemoteState.State <> DistributionState.Completed then
                ctx.Logger.Debug $"Subtitles {subRef} has state {subState.RemoteState.State}"
                false
            else
                true
        | None -> false

    let private isSubtitlesRejected (globalConnectSubtitles: GlobalConnectSubtitlesStates) subRef =
        match globalConnectSubtitles |> Map.tryFind subRef with
        | Some subState -> subState.RemoteState.State = DistributionState.Rejected
        | None -> false

    let private isSubtitlesContentChanged (ctx: GlobalConnectActionContext) (subtitlesFile: SubtitlesFile) (subState: GlobalConnectSubtitlesState) =
        try
            match subtitlesFile.SourcePath with
            | SubtitlesLocation.FilePath filePath ->
                if subtitlesFile.Version = 0 then
                    maybe {
                        let! validSourcePath = filePath
                        let! validMapping = FilePath.tryMap ctx.Environment.PathMappings validSourcePath
                        return ctx.Environment.LocalFileSystem.GetLastModifiedTime validMapping > subState.RemoteState.Timestamp
                    }
                    |> Option.defaultValue false
                else
                    subtitlesFile.FileName <> subState.Subtitles.Subtitles.FileName
            | SubtitlesLocation.AbsoluteUrl url ->
                match Http.lastModifiedTime url.Value with
                | Some time -> DateTimeOffset time > subState.RemoteState.Timestamp
                | None -> true // Assume changed content if no headers are present
        with _ ->
            false

    let private isSubtitlesModified (ctx: GlobalConnectActionContext) (globalConnectSubtitles: GlobalConnectSubtitlesStates) (subtitlesFile: SubtitlesFile) =
        match globalConnectSubtitles |> Map.tryFind subtitlesFile.TrackRef with
        | Some subState -> isSubtitlesContentChanged ctx subtitlesFile subState
        | _ -> false

    let private isSubtitlesPresentAtSource (ctx: GlobalConnectActionContext) (subtitlesFile: SubtitlesFile) =
        try
            match subtitlesFile.SourcePath with
            | SubtitlesLocation.FilePath(Some filePath) -> ctx.Environment.LocalFileSystem.FileExists(FilePath.value filePath)
            | SubtitlesLocation.FilePath(None) -> false
            | SubtitlesLocation.AbsoluteUrl url ->
                match http { HEAD url.Value } |> Request.send |> _.statusCode with
                | HttpStatusCode.OK -> true
                | _ -> false
        with _ ->
            false

    let private isSubtitlesPresentAtOrigin (ctx: GlobalConnectActionContext) (globalConnectSubtitles: GlobalConnectSubtitlesStates) subRef =
        try
            match globalConnectSubtitles |> Map.tryFind subRef with
            | Some subState -> ctx.Environment.RemoteFileSystemResolver().FileExists subState.Subtitles.RemotePath.Value
            | _ -> false
        with _ ->
            false

    let private isSmilPresentAtOrigin (fileSystem: IFileSystemInfo) (globalConnectSmil: GlobalConnectSmilState) =
        try
            fileSystem.FileExists globalConnectSmil.Smil.RemotePath.Value
        with _ ->
            false

    let private isAccessRestrictionsChanged (desiredState: DesiredMediaSetState) remotePath =
        if desiredState.IsEmpty() then
            false
        else
            let ar = GlobalConnectAccessRestrictions.parse remotePath
            ar <> desiredState.GeoRestriction

    let private isSubjectToCleanup (ctx: GlobalConnectActionContext) (remotePath: RelativeUrl) =
        try
            if ctx.Validation.ValidationMode |> List.contains MediaSetValidation.CleanupStorage then
                let fileSystem = ctx.Environment.RemoteFileSystemResolver()
                if fileSystem.FileExists remotePath.Value then
                    let lastModified = fileSystem.GetLastModifiedTime remotePath.Value
                    lastModified < DateTimeOffset.Now - ctx.Validation.StorageCleanupDelay
                else
                    false
            else
                false
        with _ ->
            false

    let buildUploadFileActions (ctx: GlobalConnectActionContext) globalConnectState (desiredState: DesiredMediaSetState) =
        let shouldUploadFile fileRef contentFile =
            ctx.Validation.OverwriteMode = OverwriteMode.Always
            || not (isFileCompleted ctx globalConnectState.Files fileRef)
            || isFileModified ctx globalConnectState.Files fileRef contentFile
            || ctx.Validation.ValidationMode |> List.contains MediaSetValidation.RemoteStorage
               && not (isFilePresentAtOrigin ctx globalConnectState.Files fileRef)

        desiredState.Content
        |> ContentSet.getFiles
        |> Map.filter (fun _ file -> Option.isSome file.SourcePath)
        |> Map.filter (fun fileRef file ->
            // We always generate upload action for files that were not previously attempted to upload
            // For files that already have state Rejected no further attempts made if they don't exist at source
            (isFilePresentAtSource ctx file || not <| isFileRejected globalConnectState.Files fileRef)
            && shouldUploadFile fileRef file)
        |> Map.map (fun fileRef _ -> GlobalConnectCommand.UploadFile(UploadFileCommand fileRef))
        |> Map.values
        |> Seq.toList

    let buildMoveFileActions (ctx: GlobalConnectActionContext) globalConnectState (desiredState: DesiredMediaSetState) =
        let shouldMoveFile fileRef (globalConnectFile: GlobalConnectFile) =
            match desiredState.Content |> ContentSet.tryGetFile fileRef with
            | Some contentFile ->
                isFileCompleted ctx globalConnectState.Files fileRef
                && not (isFileModified ctx globalConnectState.Files fileRef contentFile)
                && isAccessRestrictionsChanged desiredState globalConnectFile.RemotePath
            | _ -> false

        let filterFiles (globalConnectFiles: GlobalConnectFilesStates) =
            if ctx.Validation.ValidationMode |> List.contains MediaSetValidation.RemoteStorage then
                globalConnectFiles
                |> Map.filter (fun fileRef file -> isFilePresentAtOrigin ctx globalConnectFiles fileRef && shouldMoveFile fileRef file.File)
            else
                globalConnectFiles |> Map.filter (fun fileRef file -> shouldMoveFile fileRef file.File)

        globalConnectState.Files
        |> filterFiles
        |> Map.map (fun fileRef _ -> GlobalConnectCommand.MoveFile(MoveFileCommand fileRef))
        |> Map.values
        |> Seq.toList

    let buildDeleteFileActions ctx globalConnectState (desiredState: DesiredMediaSetState) =
        let shouldDeleteFile fileRef (file: GlobalConnectFile) =
            match desiredState.Content |> ContentSet.tryGetFile fileRef with
            | Some _ -> false
            | None ->
                isFileCompleted ctx globalConnectState.Files fileRef
                && (desiredState.IsEmpty()
                    || isAccessRestrictionsChanged desiredState file.RemotePath
                    || isSubjectToCleanup ctx file.RemotePath)

        let filterFiles (globalConnectFiles: GlobalConnectFilesStates) =
            globalConnectFiles |> Map.filter (fun fileRef file -> shouldDeleteFile fileRef file.File)

        globalConnectState.Files
        |> filterFiles
        |> Map.map (fun fileRef _ -> GlobalConnectCommand.DeleteFile(DeleteFileCommand fileRef))
        |> Map.values
        |> Seq.toList

    let buildUploadSubtitlesActions (ctx: GlobalConnectActionContext) globalConnectState (desiredState: DesiredMediaSetState) =
        let shouldUploadSubtitles (sub: SubtitlesFile) =
            ctx.Validation.OverwriteMode = OverwriteMode.Always
            || not (isSubtitlesCompleted ctx globalConnectState.Subtitles sub.TrackRef)
            || isSubtitlesModified ctx globalConnectState.Subtitles sub
            || ctx.Validation.ValidationMode |> List.contains MediaSetValidation.RemoteStorage
               && not (isSubtitlesPresentAtOrigin ctx globalConnectState.Subtitles sub.TrackRef)

        desiredState.Content
        |> ContentSet.getSubtitles
        |> List.filter (fun sub ->
            // We always generate upload action for files that were not previously attempted to upload
            // For files that already have state Rejected no further attempts made if they don't exist at source
            (isSubtitlesPresentAtSource ctx sub || not <| isSubtitlesRejected globalConnectState.Subtitles sub.TrackRef)
            && shouldUploadSubtitles sub)
        |> List.map _.TrackRef
        |> List.map (fun subRef -> GlobalConnectCommand.UploadSubtitles(UploadSubtitlesCommand subRef))

    let buildMoveSubtitlesActions (ctx: GlobalConnectActionContext) globalConnectState (desiredState: DesiredMediaSetState) =
        let shouldMoveSubtitles subRef (globalConnectSubtitles: GlobalConnectSubtitles) =
            match desiredState.Content |> ContentSet.tryGetSubtitles subRef with
            | Some subtitlesFile ->
                isSubtitlesCompleted ctx globalConnectState.Subtitles subRef
                && not (isSubtitlesModified ctx globalConnectState.Subtitles subtitlesFile)
                && isAccessRestrictionsChanged desiredState globalConnectSubtitles.RemotePath
            | _ -> false

        let filterSubtitles (globalConnectSubtitles: GlobalConnectSubtitlesStates) =
            if ctx.Validation.ValidationMode |> List.contains MediaSetValidation.RemoteStorage then
                globalConnectSubtitles
                |> Map.filter (fun subRef sub -> isSubtitlesPresentAtOrigin ctx globalConnectSubtitles subRef && shouldMoveSubtitles subRef sub.Subtitles)
            else
                globalConnectSubtitles |> Map.filter (fun subRef sub -> shouldMoveSubtitles subRef sub.Subtitles)


        globalConnectState.Subtitles
        |> filterSubtitles
        |> Map.map (fun subRef _ -> GlobalConnectCommand.MoveSubtitles(MoveSubtitlesCommand subRef))
        |> Map.values
        |> Seq.toList

    let buildDeleteSubtitlesActions ctx globalConnectState (desiredState: DesiredMediaSetState) =
        let shouldDeleteSubtitles subRef (sub: GlobalConnectSubtitles) =
            match desiredState.Content |> ContentSet.tryGetSubtitles subRef with
            | Some _ -> false
            | None ->
                isSubtitlesCompleted ctx globalConnectState.Subtitles subRef
                && (desiredState.IsEmpty()
                    || ContentSet.getSubtitles desiredState.Content |> Seq.isEmpty
                    || isAccessRestrictionsChanged desiredState sub.RemotePath)

        let filterSubtitles (globalConnectSubtitles: GlobalConnectSubtitlesStates) =
            globalConnectSubtitles |> Map.filter (fun subRef sub -> shouldDeleteSubtitles subRef sub.Subtitles)

        globalConnectState.Subtitles
        |> filterSubtitles
        |> Map.map (fun subRef _ -> GlobalConnectCommand.DeleteSubtitles(DeleteSubtitlesCommand subRef))
        |> Map.values
        |> Seq.toList

    let buildSmilActions (ctx: GlobalConnectActionContext) globalConnectState (desiredState: DesiredMediaSetState) =

        let fileSystemInfo = ctx.Environment.RemoteFileSystemResolver()
        let smilVersion = globalConnectState.Smil.Smil.Version

        let isSmilMissing () =
            globalConnectState.Smil.RemoteState.State <> DistributionState.Completed
            || ctx.Validation.ValidationMode |> List.contains MediaSetValidation.RemoteStorage
               && not (isSmilPresentAtOrigin fileSystemInfo globalConnectState.Smil)

        let getActionsForEmptyContent () =
            if not (globalConnectState.Smil.Smil.IsEmpty()) then
                [ GlobalConnectCommand.DeleteSmil <| DeleteSmilCommand smilVersion ]
            else
                []

        let getActionsForMissingSmil () =
            [ GlobalConnectCommand.UploadSmil <| UploadSmilCommand smilVersion ]

        let getActionsForUpdatedContent () =
            if
                isAccessRestrictionsChanged desiredState globalConnectState.Smil.Smil.RemotePath
                || isSubjectToCleanup ctx globalConnectState.Smil.Smil.RemotePath
            then
                [ GlobalConnectCommand.MoveSmil <| MoveSmilCommand smilVersion ]
            else
                [ GlobalConnectCommand.UploadSmil <| UploadSmilCommand smilVersion ]

        match desiredState.Content with
        | ContentSet.Empty -> getActionsForEmptyContent ()
        | _ ->
            let isOriginEmpty =
                CurrentGlobalConnectState.getCompletedFiles globalConnectState |> Seq.isEmpty
                && CurrentGlobalConnectState.getCompletedFiles globalConnectState |> Seq.isEmpty
            if isOriginEmpty then
                getActionsForEmptyContent ()
            else if isSmilMissing () then
                getActionsForMissingSmil ()
            else
                let smil = GlobalConnectSmil.fromCurrentState desiredState globalConnectState
                if globalConnectState.Smil.Smil.Content <> smil then
                    getActionsForUpdatedContent ()
                else
                    []

    let private filterActions actions actionsToBeFiltered =
        actionsToBeFiltered
        |> List.filter (fun x ->
            not (
                actions
                |> List.exists (fun y ->
                    match x, y with
                    | GlobalConnectFileCommand ref1, GlobalConnectFileCommand ref2 when ref1 = ref2 -> true
                    | _ -> false)
            ))

    let buildFileActions ctx (globalConnectState: CurrentGlobalConnectState) (desiredState: DesiredMediaSetState) =
        let uploadActions, moveActions =
            if desiredState.GeoRestriction = GeoRestriction.Unspecified then
                List.empty, List.empty
            else
                buildUploadFileActions ctx globalConnectState desiredState, buildMoveFileActions ctx globalConnectState desiredState
        let moveActions = moveActions |> filterActions uploadActions
        let deleteActions = buildDeleteFileActions ctx globalConnectState desiredState
        [ uploadActions; moveActions; deleteActions ] |> List.concat

    let buildSubtitlesActions ctx (globalConnectState: CurrentGlobalConnectState) (desiredState: DesiredMediaSetState) =
        let uploadActions, moveActions =
            if desiredState.GeoRestriction = GeoRestriction.Unspecified then
                List.empty, List.empty
            else
                buildUploadSubtitlesActions ctx globalConnectState desiredState, buildMoveSubtitlesActions ctx globalConnectState desiredState
        let moveActions = moveActions |> filterActions uploadActions
        let deleteActions = buildDeleteSubtitlesActions ctx globalConnectState desiredState
        [ uploadActions; moveActions; deleteActions ] |> List.concat

    let getActiveFiles (globalConnectState: CurrentGlobalConnectState) (desiredState: DesiredMediaSetState) mediaSetId =
        let remotePathBase = GlobalConnectPathBase.fromGeoRestriction mediaSetId desiredState.GeoRestriction
        let expectedFiles =
            desiredState.Content
            |> ContentSet.getFiles
            |> Map.values
            |> Seq.map (GlobalConnectFile.fromContentFile remotePathBase)
            |> Seq.map _.RemotePath
            |> Seq.toList
        let expectedSubtitles =
            desiredState.Content
            |> ContentSet.getSubtitles
            |> List.map (GlobalConnectSubtitles.fromSubtitlesFile remotePathBase)
            |> List.map _.RemotePath
        let expectedSmil = RelativeUrl $"{remotePathBase}/{globalConnectState.Smil.Smil.FileName}"
        List.concat
            [
                expectedFiles
                expectedSubtitles
                [ expectedSmil ]
                [ globalConnectState.Smil.Smil.RemotePath ]
                globalConnectState.Files |> Map.values |> Seq.map _.File.RemotePath |> Seq.toList
                globalConnectState.Subtitles |> Map.values |> Seq.map _.Subtitles.RemotePath |> Seq.toList
            ]
        |> List.map _.Value
        |> List.distinct

    let getInactiveFiles (ctx: GlobalConnectActionContext) mediaSetId activeFiles =
        let fileSystemInfo = ctx.Environment.RemoteFileSystemResolver()
        let geoRestrictions = [ GeoRestriction.NRK; GeoRestriction.Norway; GeoRestriction.World ]
        geoRestrictions
        |> List.map (GlobalConnectPathBase.fromGeoRestriction mediaSetId)
        |> List.map (fun x -> fileSystemInfo.GetDirectoryContent x |> Seq.toList)
        |> List.concat
        |> List.filter (fun file -> not (activeFiles |> List.contains file))

    let buildStorageActions ctx (globalConnectState: CurrentGlobalConnectState) (desiredState: DesiredMediaSetState) mediaSetId =
        let activeFiles = getActiveFiles globalConnectState desiredState mediaSetId
        match getInactiveFiles ctx mediaSetId activeFiles with
        | [] -> []
        | files -> [ GlobalConnectCommand.CleanupStorage <| CleanupStorageCommand files ]

    let forMediaSet (ctx: GlobalConnectActionContext) mediaSetId (globalConnectState: CurrentGlobalConnectState) (desiredState: DesiredMediaSetState) =
        let fileActions = buildFileActions ctx globalConnectState desiredState
        let subtitlesActions = buildSubtitlesActions ctx globalConnectState desiredState
        let smilActions = buildSmilActions ctx globalConnectState desiredState
        let storageActions =
            // Never cleanup storage while there are other pending non-deletion actions
            let onlyDeletions =
                [ fileActions; subtitlesActions; smilActions ]
                |> List.concat
                |> List.forall (fun x -> x.IsDeleteFile || x.IsDeleteSubtitles || x.IsDeleteSmil)
            if onlyDeletions && ctx.Validation.ValidationMode |> List.contains MediaSetValidation.CleanupStorage then
                buildStorageActions ctx globalConnectState desiredState mediaSetId
            else
                List.empty
        List.concat [ fileActions; subtitlesActions; smilActions; storageActions ]

    let forFile ctx _mediaSetId (globalConnectState: CurrentGlobalConnectState) fileRef globalConnectFile (desiredState: DesiredMediaSetState) =
        let globalConnectFiles = [ fileRef, globalConnectFile ] |> Map.ofList
        buildFileActions
            ctx
            { globalConnectState with
                Files = globalConnectFiles
            }
            desiredState
        |> List.filter (fun action ->
            match action with
            | GlobalConnectFileCommand fileRef' -> fileRef = fileRef'
            | _ -> false)

    let forSubtitles ctx _mediaSetId (globalConnectState: CurrentGlobalConnectState) subRef globalConnectSubtitles (desiredState: DesiredMediaSetState) =
        let globalConnectSubtitles = [ subRef, globalConnectSubtitles ] |> Map.ofList
        buildSubtitlesActions
            ctx
            { globalConnectState with
                Subtitles = globalConnectSubtitles
            }
            desiredState
        |> List.filter (fun action ->
            match action with
            | GlobalConnectSubtitlesCommand subRef' -> subRef = subRef'
            | _ -> false)

    let forSmil ctx _mediaSetId (globalConnectState: CurrentGlobalConnectState) globalConnectSmil (desiredState: DesiredMediaSetState) =
        let globalConnectState =
            { globalConnectState with
                Smil = globalConnectSmil
            }
        buildSmilActions ctx globalConnectState desiredState

    let evaluateCompletionStatus (state: MediaSetState) (actions: GlobalConnectCommand list) =
        if Seq.isNotEmpty actions then
            MediaSetStatus.Pending
        else
            seq {
                for x in state.Current.GlobalConnect.Files |> Map.values |> Seq.map _.RemoteState do
                    yield x
                for x in state.Current.GlobalConnect.Subtitles |> Map.values |> Seq.map _.RemoteState do
                    yield x
                if not (state.Current.GlobalConnect.Smil.Smil.IsEmpty()) then
                    yield state.Current.GlobalConnect.Smil.RemoteState
            }
            |> List.ofSeq
            |> List.fold
                (fun acc remoteState ->
                    match acc, (remoteState.State |> MediaSetStatus.fromRemoteState) with
                    | MediaSetStatus.Completed, remoteState -> remoteState
                    | MediaSetStatus.Rejected, _ -> MediaSetStatus.Rejected
                    | _, MediaSetStatus.Rejected -> MediaSetStatus.Rejected
                    | _ -> MediaSetStatus.Pending)
                MediaSetStatus.Completed
