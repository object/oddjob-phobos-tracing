namespace Nrk.Oddjob.Ps

module PsFileArchive =

    open System
    open System.IO
    open Akka.Event

    open Nrk.Oddjob.Core
    open PsTypes

    let private getArchiveFilePath rootPath progId =
        let piProgIdStr = PiProgId.value progId
        Path.Combine(rootPath, piProgIdStr.Substring(0, 6), piProgIdStr.Substring(6, 2), piProgIdStr)

    type PrepareArchiveConfig =
        | MoveFromDropToDestinationFolder of dropFolder: string * destinationRoot: string
        // Downloading audio files
        | CopyToDestinationFolder of destinationRoot: string
        | UseFromArchive of sourceRoots: string

    module PsPrepareArchive =

        let getDestinationFilePath destinationRoot piProgId fileName =
            let destinationDirectory = getArchiveFilePath destinationRoot piProgId
            Path.Combine(destinationDirectory, fileName)

        let getTempFilePath filepath = $"%s{filepath}.temp"

        let archiveOrLocateTranscodedFile
            dropFolder
            destinationRoot
            piProgId
            (fileSystem: IFileSystemOperations)
            (log: ILoggingAdapter)
            (file: PsTranscodedFile)
            =
            let destinationFilePath = getDestinationFilePath destinationRoot piProgId file.FileName
            let dropFolderFile = Path.Combine(dropFolder, file.FileName)
            if fileSystem.FileExists dropFolderFile then
                log.Debug $"Found file %s{file.FileName} in the drop folder (moving to archive)"
                let destinationDirectory = getArchiveFilePath destinationRoot piProgId
                fileSystem.CreateDirectory destinationDirectory
                let tempFilePath = getTempFilePath destinationFilePath
                if fileSystem.FileExists tempFilePath then
                    fileSystem.DeleteFile tempFilePath
                fileSystem.CopyFile dropFolderFile tempFilePath true
                fileSystem.DeleteFile destinationFilePath
                fileSystem.MoveFile tempFilePath destinationFilePath
                fileSystem.DeleteFile dropFolderFile
            else if fileSystem.FileExists destinationFilePath then
                log.Debug $"File %s{file.FileName} is already moved to archive"
            else
                log.Debug $"File %s{file.FileName} is not found in either drop folder or archive"
            { file with
                FilePath = destinationFilePath
            }

        let prepareVideoFileFromDropFolderOrArchive
            dropFolder
            destinationRoot
            piProgId
            (fileSystem: IFileSystemOperations)
            (log: ILoggingAdapter)
            (file: PsTranscodedFile)
            =
            archiveOrLocateTranscodedFile dropFolder destinationRoot piProgId fileSystem log file

        let prepareVideoFileFromArchive destinationRoot piProgId (fileSystem: IFileSystemOperations) (log: ILoggingAdapter) (file: PsTranscodedFile) =
            let destinationFilePath = getDestinationFilePath destinationRoot piProgId file.FileName
            if fileSystem.FileExists destinationFilePath then
                log.Debug $"File %s{file.FileName} is found in archive)"
            else
                log.Debug $"File %s{file.FileName} is not found in archive"
            { file with
                FilePath = destinationFilePath
            }

        let prepareAudioFileFromExternalSourceOrArchive
            destinationRoot
            piProgId
            (fileSystem: IFileSystemOperations)
            (log: ILoggingAdapter)
            (file: PsTranscodedFile)
            =
            let destinationFilePath = getDestinationFilePath destinationRoot piProgId file.FileName
            if String.isNotNullOrEmpty file.FilePath && file.FilePath.StartsWith "http" then
                let destinationDirectory = getArchiveFilePath destinationRoot piProgId
                let sourceFileTime = Http.lastModifiedTime file.FilePath |> Option.defaultValue DateTime.Now
                let isSourceFileNewer =
                    if fileSystem.FileExists destinationFilePath then
                        let archiveFileTime = IO.getLastModifiedTime destinationFilePath
                        sourceFileTime.ToUniversalTime() > archiveFileTime.DateTime
                    else
                        true

                if isSourceFileNewer then
                    log.Debug $"File %s{file.FileName} will be downloaded"
                    fileSystem.CreateDirectory destinationDirectory
                    let tempFilePath = getTempFilePath destinationFilePath
                    if fileSystem.FileExists tempFilePath then
                        fileSystem.DeleteFile tempFilePath
                    fileSystem.DownloadFile file.FilePath tempFilePath
                    fileSystem.DeleteFile destinationFilePath
                    fileSystem.MoveFile tempFilePath destinationFilePath
                    fileSystem.SetFileCreationTime destinationFilePath sourceFileTime
                    fileSystem.SetFileLastWriteTime destinationFilePath sourceFileTime
                    fileSystem.DeleteFile tempFilePath
                else
                    log.Debug $"File %s{file.FileName} already exists"
            else
                log.Debug $"File %s{file.FileName} is already downloaded"
            { file with
                FilePath = destinationFilePath
            }

        let prepareAudioFileFromArchive destinationRoot piProgId (fileSystem: IFileSystemOperations) (log: ILoggingAdapter) (file: PsTranscodedFile) =
            let destinationFilePath = getDestinationFilePath destinationRoot piProgId file.FileName
            if fileSystem.FileExists destinationFilePath then
                log.Debug $"File %s{file.FileName} is found in archive)"
            else
                log.Debug $"File %s{file.FileName} is not found in archive"
            { file with
                FilePath = destinationFilePath
            }

        let prepareArchiveForVideo config progId archiveFiles fileSystem log =
            match config with
            | MoveFromDropToDestinationFolder(dropFolder, destinationRoot) ->
                archiveFiles |> List.map (prepareVideoFileFromDropFolderOrArchive dropFolder destinationRoot progId fileSystem log)
            | CopyToDestinationFolder archiveRoot
            | UseFromArchive archiveRoot -> archiveFiles |> List.map (prepareVideoFileFromArchive archiveRoot progId fileSystem log)

        let prepareArchiveForAudio config progId archiveFiles fileSystem log =
            match config with
            | MoveFromDropToDestinationFolder(_, destinationRoot)
            | CopyToDestinationFolder destinationRoot ->
                archiveFiles |> List.map (prepareAudioFileFromExternalSourceOrArchive destinationRoot progId fileSystem log)
            | UseFromArchive archiveRoot -> archiveFiles |> List.map (prepareAudioFileFromArchive archiveRoot progId fileSystem log)
