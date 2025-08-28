namespace Nrk.Oddjob.Upload.GlobalConnect

module GlobalConnectBootstrapperUtils =

    open System

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Core.S3.S3Types
    open Nrk.Oddjob.Core.S3.S3Api
    open Nrk.Oddjob.Upload.MediaSetController
    open Nrk.Oddjob.Upload.GlobalConnect
    open Nrk.Oddjob.Upload.GlobalConnect.GlobalConnectUploadActor

    let resolveRemoteFileSystem (s3Api: IS3Api) =
        S3FileSystemInfo(s3Api) :> IFileSystemInfo

    let createGlobalConnectEnvironment (oddjobConfig: OddjobConfig) s3Api =
        {
            LocalFileSystem = LocalFileSystemInfo()
            PathMappings = oddjobConfig.PathMappings
            RemoteFilePathResolver = id
            RemoteFileSystemResolver = fun () -> resolveRemoteFileSystem s3Api
        }

    type GlobalConnectUploaderPropsFactory(oddjobConfig: OddjobConfig, s3api, s3Queue) =

        interface IOriginSpecificUploadActorPropsFactory with
            member _.GetUploadActorProps mediaSetId mediaSetController clientRef clientContentId =
                let uploaderProps =
                    {
                        MediaSetId = mediaSetId
                        MediaSetController = mediaSetController
                        ClientRef = clientRef
                        ClientContentId = clientContentId
                        BucketName = oddjobConfig.S3.BucketName
                        S3Queue = s3Queue
                        FailingFilesRetryInterval =
                            oddjobConfig.Limits.FailingFilesRetryIntervalInMinutes |> Seq.map float |> Seq.map TimeSpan.FromMinutes |> Seq.toArray
                        PublishConfirmationTimeout = TimeSpan.ToOption TimeSpan.FromSeconds oddjobConfig.Limits.PublishConfirmationTimeoutInSeconds
                        ActionEnvironment = createGlobalConnectEnvironment oddjobConfig s3api
                        StorageCleanupDelay = TimeSpan.FromHours(float oddjobConfig.Limits.OriginStorageCleanupIntervalInHours)
                        SkipProcessingForSources = oddjobConfig.Upload.SkipProcessingForSources |> List.ofArrayOrNull
                        MinimumMessagePriority = oddjobConfig.Upload.MinimumMessagePriority
                    }
                propsNamed "upload-globalconnect" (globalConnectUploadActor uploaderProps)
