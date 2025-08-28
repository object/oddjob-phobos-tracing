namespace Nrk.Oddjob.WebApi

module GlobalConnect =

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.S3.S3Types
    open Nrk.Oddjob.Upload.GlobalConnect

    let tryGetFileInfo (fileInfo: IFileSystemInfo) path : GlobalConnectRemoteFile option =
        if fileInfo.FileExists path then
            Some
                {
                    Path = path
                    Length = fileInfo.GetFileLength path
                    Modified = (fileInfo.GetLastModifiedTime path).ToLocalTime()
                    Content = fileInfo.GetFileContent path
                }
        else
            None

    let getRemoteFiles (state: MediaSetState) (s3Api: IS3Api) =
        try
            let fileSystemInfo = GlobalConnectBootstrapperUtils.resolveRemoteFileSystem s3Api
            PartUtils.getPartsInfo state
            |> Array.collect _.GlobalConnect.Files
            |> Array.map _.File.RemotePath
            |> Array.map String.toOption
            |> Array.choose id
            |> Array.map (tryGetFileInfo fileSystemInfo)
            |> Array.choose id
        with exn ->
            Array.empty

    let getRemoteSubtitles (state: MediaSetState) (s3Api: IS3Api) =
        try
            let fileSystemInfo = GlobalConnectBootstrapperUtils.resolveRemoteFileSystem s3Api
            PartUtils.getPartsInfo state
            |> Array.collect _.GlobalConnect.Subtitles
            |> Array.map _.File.RemotePath
            |> Array.map String.toOption
            |> Array.choose id
            |> Array.map (tryGetFileInfo fileSystemInfo)
            |> Array.choose id
        with exn ->
            Array.empty

    let getRemoteSmil (state: MediaSetState) (s3Api: IS3Api) =
        try
            let fileSystemInfo = GlobalConnectBootstrapperUtils.resolveRemoteFileSystem s3Api
            PartUtils.getPartsInfo state
            |> Array.map _.GlobalConnect.Smil
            |> Array.tryHead
            |> Option.map _.Smil.RemotePath
            |> Option.bind String.toOption
            |> Option.bind (tryGetFileInfo fileSystemInfo)
        with exn ->
            None

    let getMediaSetInfo state s3Api : Async<GlobalConnectInfo> =
        async {
            return
                {
                    Remote =
                        {
                            Files = getRemoteFiles state s3Api
                            Subtitles = getRemoteSubtitles state s3Api
                            Smil = getRemoteSmil state s3Api
                        }
                }
        }

    let getStatusSummary (globalConnect: GlobalConnectInfo) =
        $"%d{globalConnect.Remote.Files |> Array.length} media files, %d{globalConnect.Remote.Subtitles |> Array.length} subtitles files"

    let getGlobalConnectFilesResult (globalConnect: GlobalConnectInfo) filesResult =
        match globalConnect.Remote.Files with
        | [||] when filesResult = DistributionStatus.Ok -> DistributionStatus.Error "No GlobalConnect media files"
        | _ -> DistributionStatus.Ok

    let getGlobalConnectSubtitlesResult (globalConnect: GlobalConnectInfo) (subtitles: array<_>) =
        let subtitles = subtitles |> Array.sort
        match globalConnect.Remote.Subtitles with
        | [||] when Seq.isNotEmpty subtitles -> DistributionStatus.Error "No GlobalConnect subtitles files"
        | _ -> DistributionStatus.Ok
