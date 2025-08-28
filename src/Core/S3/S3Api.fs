namespace Nrk.Oddjob.Core.S3

module S3Api =

    open System
    open System.Net

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config

    open Amazon.S3
    open Amazon.S3.Model
    open Amazon.S3.Transfer

    open S3Types

    type S3Api(s3Settings: OddjobConfig.S3, accessKeys: Map<string, string>) =
        let amazonConfig = AmazonS3Config(ServiceURL = accessKeys["Endpoint"], ForcePathStyle = true)
        let amazonClient = new AmazonS3Client(accessKeys["AccessKey"], accessKeys["SecretAccessKey"], amazonConfig)
        let config = TransferUtilityConfig()
        do config.ConcurrentServiceRequests <- s3Settings.ConcurrentServiceRequests
        let fileTransferUtility = new TransferUtility(amazonClient)

        interface IS3Api with
            member this.ListObjectsAsync storageKey =
                let prefix =
                    if storageKey.EndsWith "/" then
                        storageKey
                    else
                        storageKey + "/"
                ListObjectsV2Request(BucketName = s3Settings.BucketName, Prefix = prefix) |> amazonClient.ListObjectsV2Async
            member this.GetObjectAsync storageKey =
                GetObjectRequest(BucketName = s3Settings.BucketName, Key = storageKey) |> amazonClient.GetObjectAsync
            member this.GetObjectMetadataAsync storageKey =
                GetObjectMetadataRequest(BucketName = s3Settings.BucketName, Key = storageKey) |> amazonClient.GetObjectMetadataAsync
            member this.UploadObjectAsync storageKey filePath =
                TransferUtilityUploadRequest(BucketName = s3Settings.BucketName, Key = storageKey, FilePath = filePath)
                |> fileTransferUtility.UploadAsync
            member this.PutObjectAsync storageKey contentBody =
                PutObjectRequest(BucketName = s3Settings.BucketName, Key = storageKey, ContentBody = contentBody)
                |> amazonClient.PutObjectAsync
            member this.CopyObjectAsync sourceStorageKey destinationStorageKey =
                CopyObjectRequest(
                    SourceBucket = s3Settings.BucketName,
                    DestinationBucket = s3Settings.BucketName,
                    SourceKey = sourceStorageKey,
                    DestinationKey = destinationStorageKey
                )
                |> amazonClient.CopyObjectAsync
            member this.DeleteObjectAsync storageKey =
                DeleteObjectRequest(BucketName = s3Settings.BucketName, Key = storageKey) |> amazonClient.DeleteObjectAsync

        interface IS3ManagementApi with
            member this.PutBucketAsync bucketName =
                PutBucketRequest(BucketName = s3Settings.BucketName, UseClientRegion = true) |> amazonClient.PutBucketAsync
            member this.DeleteBucketAsync bucketName =
                DeleteBucketRequest(BucketName = s3Settings.BucketName) |> amazonClient.DeleteBucketAsync

        interface IDisposable with
            member _.Dispose() =
                fileTransferUtility.Dispose()
                amazonClient.Dispose()

    type S3Client(s3Api: IS3Api, fileSystem: IFileSystemInfo) =

        let createException (statusCode: HttpStatusCode) =
            Http.HttpRequestException("", null, statusCode) :> exn

        interface IS3Client with

            member _.UploadFile storageKey filePath =
                try
                    task {
                        if not (fileSystem.FileExists filePath) then
                            System.IO.FileNotFoundException filePath |> raise
                        do! s3Api.UploadObjectAsync storageKey filePath
                        return Ok()
                    }
                    |> Async.AwaitTask
                    |> Async.RunSynchronously
                with exn ->
                    Error exn

            member _.UploadContent storageKey contentBody =
                try
                    task {
                        let! res = s3Api.PutObjectAsync storageKey contentBody
                        return
                            if res.HttpStatusCode.IsSuccess then
                                Ok()
                            else
                                Error(createException res.HttpStatusCode)
                    }
                    |> Async.AwaitTask
                    |> Async.RunSynchronously
                with exn ->
                    Error exn

            member _.DeleteFile(storageKey: string) : S3ClientResult =
                try
                    task {
                        let! res = s3Api.DeleteObjectAsync storageKey
                        return
                            if res.HttpStatusCode.IsSuccess then
                                Ok()
                            else
                                Error(createException res.HttpStatusCode)
                    }
                    |> Async.AwaitTask
                    |> Async.RunSynchronously
                with exn ->
                    Error exn

            member _.MoveFile (sourceStorageKey: string) (destinationStorageKey: string) : S3ClientResult =
                try
                    task {
                        let! res = s3Api.CopyObjectAsync sourceStorageKey destinationStorageKey
                        if res.HttpStatusCode.IsSuccess then
                            let! deleteRes = s3Api.DeleteObjectAsync sourceStorageKey
                            return
                                if deleteRes.HttpStatusCode.IsSuccess then
                                    Ok()
                                else
                                    Error(createException res.HttpStatusCode)
                        else
                            return Error(createException res.HttpStatusCode)
                    }
                    |> Async.AwaitTask
                    |> Async.RunSynchronously
                with exn ->
                    Error exn

    type S3FileSystemInfo(s3Api: IS3Api) =

        let getFileOrDirectoryInfo path =

            let isNotFoundException (exn: Exception) =
                let exn = exn :?> AmazonS3Exception
                exn <> null && (exn.StatusCode = HttpStatusCode.NotFound || exn.ErrorCode = "NoSuchKey")

            try
                task {
                    let! response = s3Api.GetObjectMetadataAsync path
                    return
                        if response.HttpStatusCode.IsSuccess then
                            Ok response
                        else
                            Error()
                }
                |> Async.AwaitTask
                |> Async.RunSynchronously
            with
            | :? AggregateException as exn when isNotFoundException exn.InnerException -> Error()
            | :? AmazonS3Exception as exn when isNotFoundException exn.InnerException -> Error()

        let getFileContent path =
            task {
                use! response = s3Api.GetObjectAsync path
                return
                    if response.HttpStatusCode.IsSuccess then
                        use reader = new System.IO.StreamReader(response.ResponseStream)
                        reader.ReadToEnd()
                    else
                        "Failed to retrieve file content"
            }
            |> Async.AwaitTask
            |> Async.RunSynchronously

        let getDirectoryContent (path: string) =
            task {
                let! response = s3Api.ListObjectsAsync path
                return
                    if response.HttpStatusCode.IsSuccess then
                        response.S3Objects |> Seq.map _.Key |> Seq.toList
                    else
                        List.empty
            }
            |> Async.AwaitTask
            |> Async.RunSynchronously

        interface IFileSystemInfo with
            member _.FileExists path = (getFileOrDirectoryInfo path).IsOk

            member _.DirectoryExists path = (getFileOrDirectoryInfo path).IsOk

            member _.GetFileLength path =
                match getFileOrDirectoryInfo path with
                | Result.Ok info -> info.ContentLength |> uint64
                | Result.Error _ -> 0UL

            member _.GetLastModifiedTime path =
                match getFileOrDirectoryInfo path with
                | Result.Ok info -> DateTimeOffset info.LastModified
                | Result.Error _ -> DateTimeOffset.MinValue

            member _.GetFileContent path =
                match getFileOrDirectoryInfo path with
                | Result.Ok _ ->
                    let extPos = path.LastIndexOf "."
                    let ext = if extPos >= 0 then path.Substring(extPos) else ""
                    if [ ".smil"; ".txt" ] |> List.contains ext then
                        getFileContent path
                    else
                        "<binary>"
                | Result.Error error -> $"Error: {error}"

            member _.GetDirectoryContent path = getDirectoryContent path
