namespace Nrk.Oddjob.Core

open System
open System.IO
open System.Net
open FsHttp

[<AutoOpen>]
module FileSystem =

    type IFileSystemInfo =
        abstract member FileExists: string -> bool
        abstract member DirectoryExists: string -> bool
        abstract member GetFileLength: string -> uint64
        abstract member GetLastModifiedTime: string -> DateTimeOffset
        abstract member GetFileContent: string -> string
        abstract member GetDirectoryContent: string -> string list

    type IFileSystemOperations =
        inherit IFileSystemInfo
        abstract member CopyFile: string -> string -> bool -> unit
        abstract member MoveFile: string -> string -> unit
        abstract member DeleteFile: string -> unit
        abstract member DownloadFile: string -> string -> unit
        abstract member CreateDirectory: string -> unit
        abstract member SetFileCreationTime: string -> DateTime -> unit
        abstract member SetFileLastWriteTime: string -> DateTime -> unit

    type LocalFileSystemInfo() =
        interface IFileSystemInfo with
            member _.FileExists path = File.Exists path
            member _.DirectoryExists path = Directory.Exists path
            member _.GetFileLength path = IO.getFileLengthSafely path
            member _.GetLastModifiedTime path = IO.getLastModifiedTime path
            member _.GetFileContent path = IO.getFileContent path
            member _.GetDirectoryContent path = IO.getDirectoryContent path

    type LocalFileSystemOperations() =
        interface IFileSystemOperations with
            member _.FileExists path = File.Exists path
            member _.DirectoryExists path = Directory.Exists path
            member _.GetFileLength path = IO.getFileLengthSafely path
            member _.GetLastModifiedTime path = IO.getLastModifiedTime path
            member _.GetFileContent path = IO.getFileContent path
            member _.GetDirectoryContent path = IO.getDirectoryContent path
            member _.CopyFile sourcePath destPath overwrite = File.Copy(sourcePath, destPath, true)
            member _.MoveFile sourcePath destPath = File.Move(sourcePath, destPath)
            member _.DeleteFile path = File.Delete(path)
            member _.DownloadFile sourcePath destPath =
                async {
                    let! response = http { GET sourcePath } |> Request.sendAsync
                    if response.statusCode = HttpStatusCode.OK then
                        use fs = new FileStream(destPath, FileMode.CreateNew)
                        return! response.ToStream().CopyToAsync(fs) |> Async.AwaitTask
                    else
                        failwithf $"Failed to download file %s{sourcePath} ({response.statusCode})"
                }
                |> Async.RunSynchronously
            member _.CreateDirectory path =
                Directory.CreateDirectory(path) |> ignore
            member _.SetFileCreationTime path time = File.SetCreationTime(path, time)
            member _.SetFileLastWriteTime path time = File.SetLastWriteTime(path, time)

    type EmptyFileSystemInfo() =
        interface IFileSystemInfo with
            member _.FileExists _ = false
            member _.DirectoryExists _ = false
            member _.GetFileLength _ = 0UL
            member _.GetLastModifiedTime _ = DateTimeOffset.MinValue
            member _.GetFileContent _ = ""
            member _.GetDirectoryContent _ = List.empty

    type EmptyFileSystemOperations() =
        interface IFileSystemOperations with
            member _.FileExists _ = false
            member _.DirectoryExists _ = false
            member _.GetFileLength _ = 0UL
            member _.GetLastModifiedTime _ = DateTimeOffset.MinValue
            member _.GetFileContent _ = ""
            member _.GetDirectoryContent _ = List.empty
            member _.CopyFile _ _ _ = ()
            member _.MoveFile _ _ = ()
            member _.DeleteFile _ = ()
            member _.DownloadFile sourcePath destPath = ()
            member _.CreateDirectory _ = ()
            member _.SetFileCreationTime _ _ = ()
            member _.SetFileLastWriteTime _ _ = ()
