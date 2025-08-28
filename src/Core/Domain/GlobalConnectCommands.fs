namespace Nrk.Oddjob.Core

[<AutoOpen>]
module GlobalConnectCommands =

    type UploadFileCommand = UploadFileCommand of FileRef

    type MoveFileCommand = MoveFileCommand of FileRef

    type DeleteFileCommand = DeleteFileCommand of FileRef

    type UploadSubtitlesCommand = UploadSubtitlesCommand of SubtitlesRef

    type MoveSubtitlesCommand = MoveSubtitlesCommand of SubtitlesRef

    type DeleteSubtitlesCommand = DeleteSubtitlesCommand of SubtitlesRef

    type UploadSmilCommand = UploadSmilCommand of version: int

    type MoveSmilCommand = MoveSmilCommand of version: int

    type DeleteSmilCommand = DeleteSmilCommand of version: int

    type CleanupStorageCommand = CleanupStorageCommand of inactiveFiles: string list

    [<RequireQualifiedAccess>]
    type GlobalConnectCommand =
        | UploadFile of UploadFileCommand
        | MoveFile of MoveFileCommand
        | DeleteFile of DeleteFileCommand
        | UploadSubtitles of UploadSubtitlesCommand
        | MoveSubtitles of MoveSubtitlesCommand
        | DeleteSubtitles of DeleteSubtitlesCommand
        | UploadSmil of UploadSmilCommand
        | MoveSmil of MoveSmilCommand
        | DeleteSmil of DeleteSmilCommand
        | CleanupStorage of CleanupStorageCommand

        member this.CommandName =
            match this with
            | GlobalConnectCommand.UploadFile _ -> "UploadGlobalConnectFile"
            | GlobalConnectCommand.MoveFile _ -> "MoveGlobalConnectFile"
            | GlobalConnectCommand.DeleteFile _ -> "DeleteGlobalConnectFile"
            | GlobalConnectCommand.UploadSubtitles _ -> "UploadGlobalConnectSubtitles"
            | GlobalConnectCommand.MoveSubtitles _ -> "MoveGlobalConnectSubtitles"
            | GlobalConnectCommand.DeleteSubtitles _ -> "DeleteGlobalConnectSubtitles"
            | GlobalConnectCommand.UploadSmil _ -> "UploadGlobalConnectSmil"
            | GlobalConnectCommand.MoveSmil _ -> "MoveGlobalConnectSmil"
            | GlobalConnectCommand.DeleteSmil _ -> "DeleteGlobalConnectSmil"
            | GlobalConnectCommand.CleanupStorage _ -> "CleanupGlobalConnectStorage"

    let (|GlobalConnectFileCommand|_|) cmd =
        match cmd with
        | GlobalConnectCommand.UploadFile(UploadFileCommand fileRef) -> Some fileRef
        | GlobalConnectCommand.MoveFile(MoveFileCommand fileRef) -> Some fileRef
        | GlobalConnectCommand.DeleteFile(DeleteFileCommand fileRef) -> Some fileRef
        | _ -> None

    let (|GlobalConnectSubtitlesCommand|_|) cmd =
        match cmd with
        | GlobalConnectCommand.UploadSubtitles(UploadSubtitlesCommand subRef) -> Some subRef
        | GlobalConnectCommand.MoveSubtitles(MoveSubtitlesCommand subRef) -> Some subRef
        | GlobalConnectCommand.DeleteSubtitles(DeleteSubtitlesCommand subRef) -> Some subRef
        | _ -> None

    let (|GlobalConnectSmilCommand|_|) cmd =
        match cmd with
        | GlobalConnectCommand.UploadSmil(UploadSmilCommand version) -> Some version
        | GlobalConnectCommand.MoveSmil(MoveSmilCommand version) -> Some version
        | GlobalConnectCommand.DeleteSmil(DeleteSmilCommand version) -> Some version
        | _ -> None

    let (|GlobalConnectResourceCommand|_|) cmd =
        match cmd with
        | GlobalConnectCommand.UploadFile(UploadFileCommand fileRef) -> Some <| ResourceRef.File fileRef
        | GlobalConnectCommand.MoveFile(MoveFileCommand fileRef) -> Some <| ResourceRef.File fileRef
        | GlobalConnectCommand.DeleteFile(DeleteFileCommand fileRef) -> Some <| ResourceRef.File fileRef
        | GlobalConnectCommand.UploadSubtitles(UploadSubtitlesCommand subRef) -> Some <| ResourceRef.Subtitles subRef
        | GlobalConnectCommand.MoveSubtitles(MoveSubtitlesCommand subRef) -> Some <| ResourceRef.Subtitles subRef
        | GlobalConnectCommand.DeleteSubtitles(DeleteSubtitlesCommand subRef) -> Some <| ResourceRef.Subtitles subRef
        | GlobalConnectCommand.UploadSmil(UploadSmilCommand _) -> Some <| ResourceRef.Smil
        | GlobalConnectCommand.MoveSmil(MoveSmilCommand _) -> Some <| ResourceRef.Smil
        | GlobalConnectCommand.DeleteSmil(DeleteSmilCommand _) -> Some <| ResourceRef.Smil
        | _ -> None

    let (|GlobalConnectStorageCommand|_|) (cmd: GlobalConnectCommand) = cmd.IsCleanupStorage
