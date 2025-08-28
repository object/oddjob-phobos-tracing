namespace Nrk.Oddjob.Core.S3

module S3Types =

    open System
    open System.Threading.Tasks
    open Amazon.S3.Model

    open Nrk.Oddjob.Core

    type S3UploadFileCommand =
        {
            SourcePath: FilePath option
            RemotePath: RelativeUrl
            JobContext: FileJobContext option
        }

    [<StructuredFormatDisplay("RemotePath={RemotePath}; JobContext={JobContext}")>]
    type S3UploadTextCommand =
        {
            ContentBody: string
            RemotePath: RelativeUrl
            JobContext: FileJobContext option
        }

    type S3UploadFileFromUrlCommand =
        {
            SourceUrl: AbsoluteUrl
            RemotePath: RelativeUrl
            JobContext: FileJobContext option
        }

    type S3MoveFileCommand =
        {
            SourcePath: RelativeUrl
            DestinationPath: RelativeUrl
            JobContext: FileJobContext option
        }

    [<StructuredFormatDisplay("SourcePath={SourcePath}; DestinationPath={DestinationPath}; JobContext={JobContext}")>]
    type S3UpdateTextCommand =
        {
            SourcePath: RelativeUrl
            DestinationPath: RelativeUrl
            ContentBody: string
            JobContext: FileJobContext option
        }

    type S3DeleteFileCommand =
        {
            RemotePath: RelativeUrl
            JobContext: FileJobContext option
        }

    type S3DeleteFilesCommand =
        {
            RemotePaths: RelativeUrl list
            JobContext: FileJobContext option
        }

    [<RequireQualifiedAccess>]
    type S3Command =
        | UploadFile of S3UploadFileCommand
        | UploadFileFromUrl of S3UploadFileFromUrlCommand
        | UploadText of S3UploadTextCommand
        | MoveFile of S3MoveFileCommand
        | UpdateText of S3UpdateTextCommand
        | DeleteFile of S3DeleteFileCommand
        | DeleteFiles of S3DeleteFilesCommand

        member this.GetCommandTarget() =
            match this with
            | UploadFile cmd -> cmd.RemotePath.Value
            | UploadFileFromUrl cmd -> cmd.RemotePath.Value
            | UploadText cmd -> cmd.RemotePath.Value
            | MoveFile cmd -> cmd.DestinationPath.Value
            | UpdateText cmd -> cmd.DestinationPath.Value
            | DeleteFile cmd -> cmd.RemotePath.Value
            | DeleteFiles cmd -> String.Join(",", cmd.RemotePaths)
        // EntityId is used by cluster sharding to compute the shard where the message will be routed
        member this.GetEntityId numberOfIngesters =
            let summary = this.GetCommandTarget()
            $"{Math.Abs(Akka.Util.MurmurHash.StringHash summary) % numberOfIngesters}"

    type S3ClientResult = Result<unit, exn>

    type IS3Client =
        abstract member UploadFile: storageKey: string -> filePath: string -> S3ClientResult
        abstract member MoveFile: sourceStorageKey: string -> destinationStorageKey: string -> S3ClientResult
        abstract member DeleteFile: storageKey: string -> S3ClientResult
        abstract member UploadContent: storageKey: string -> contentBody: string -> S3ClientResult

    [<RequireQualifiedAccess>]
    type GlobalConnectIngesterStatus =
        | EnteredIdle
        | UploadFileReceived
        | MoveFileReceived
        | DeleteFileReceived
        | CommandCompleted
        | CommandFailed
        | CommandRejected

    module GlobalConnectIngesterStatus =
        let toInt s =
            match s with
            | GlobalConnectIngesterStatus.EnteredIdle -> 0
            | GlobalConnectIngesterStatus.UploadFileReceived -> 1
            | GlobalConnectIngesterStatus.MoveFileReceived -> 2
            | GlobalConnectIngesterStatus.DeleteFileReceived -> 3
            | GlobalConnectIngesterStatus.CommandCompleted -> 4
            | GlobalConnectIngesterStatus.CommandFailed -> 5
            | GlobalConnectIngesterStatus.CommandRejected -> 6

        let fromInt n =
            match n with
            | 0 -> GlobalConnectIngesterStatus.EnteredIdle
            | 1 -> GlobalConnectIngesterStatus.UploadFileReceived
            | 2 -> GlobalConnectIngesterStatus.MoveFileReceived
            | 3 -> GlobalConnectIngesterStatus.DeleteFileReceived
            | 4 -> GlobalConnectIngesterStatus.CommandCompleted
            | 5 -> GlobalConnectIngesterStatus.CommandFailed
            | 6 -> GlobalConnectIngesterStatus.CommandRejected
            | _ -> invalidOp <| $"Unable to convert %d{n} to S3IngesterStatus"

    [<CustomComparison; CustomEquality>]
    type S3MessageContext =
        {
            Message: Message<S3Command>
            Sender: Akkling.ActorRefs.IActorRef<Events.FileJobOriginEvent>
        }

        member this.GetSummary() =
            $"%s{getUnionCaseName this.Message.Payload} %s{this.Message.Payload.GetCommandTarget()}"

        member this.JobContext getValue =
            let getValue ctx = ctx |> Option.map getValue
            match this.Message.Payload with
            | S3Command.UploadFile command -> command.JobContext |> getValue
            | S3Command.UploadFileFromUrl command -> command.JobContext |> getValue
            | S3Command.UploadText command -> command.JobContext |> getValue
            | S3Command.MoveFile command -> command.JobContext |> getValue
            | S3Command.UpdateText command -> command.JobContext |> getValue
            | S3Command.DeleteFile command -> command.JobContext |> getValue
            | S3Command.DeleteFiles command -> command.JobContext |> getValue

        member this.Priority = this.JobContext _.Job.Header.Priority
        member this.Timestamp = this.JobContext _.Job.Header.Timestamp
        member this.TargetPath =
            match this.Message.Payload with
            | S3Command.UploadFile command -> Some command.RemotePath.Value
            | S3Command.UploadFileFromUrl command -> Some command.RemotePath.Value
            | S3Command.UploadText command -> Some command.RemotePath.Value
            | S3Command.MoveFile command -> Some command.DestinationPath.Value
            | S3Command.UpdateText command -> Some command.DestinationPath.Value
            | S3Command.DeleteFile command -> Some command.RemotePath.Value
            | S3Command.DeleteFiles _ -> None

        interface IComparable with
            member x.CompareTo y' =
                let tryGetFileExtension (path: string option) =
                    match path with
                    | Some path ->
                        let ndx = path.LastIndexOf '.'
                        if ndx >= 0 then path.Substring ndx |> Some else None
                    | None -> None
                let y = y' :?> S3MessageContext
                if
                    (x.Message.Payload.IsDeleteFile || x.Message.Payload.IsDeleteFiles)
                    && not (y.Message.Payload.IsDeleteFile || y.Message.Payload.IsDeleteFiles)
                then
                    -1
                else if
                    (y.Message.Payload.IsDeleteFile || y.Message.Payload.IsDeleteFiles)
                    && not (x.Message.Payload.IsDeleteFile || x.Message.Payload.IsDeleteFiles)
                then
                    1
                else if tryGetFileExtension y.TargetPath = Some ".smil" && tryGetFileExtension x.TargetPath <> Some ".smil" then
                    1
                else if tryGetFileExtension y.TargetPath <> Some ".smil" && tryGetFileExtension x.TargetPath = Some ".smil" then
                    -1
                else if y.Priority > x.Priority then
                    1
                else if y.Priority < x.Priority then
                    -1
                else if y.Timestamp < x.Timestamp then
                    1
                else if y.Timestamp > x.Timestamp then
                    -1
                else
                    0

        override x.Equals y' =
            if y' :? S3MessageContext then
                let y = y' :?> S3MessageContext
                not (x > y) && not (x < y)
            else
                false
        override x.GetHashCode() = 0

    [<RequireQualifiedAccess>]
    type S3Event =
        | Initiated
        | Completed
        | Error of int * string
        | Rejected of int * string

    type IS3Api =
        abstract member ListObjectsAsync: storageKey: string -> Task<ListObjectsV2Response>
        abstract member GetObjectAsync: storageKey: string -> Task<GetObjectResponse>
        abstract member GetObjectMetadataAsync: storageKey: string -> Task<GetObjectMetadataResponse>
        abstract member UploadObjectAsync: storageKey: string -> filePath: string -> Task
        abstract member PutObjectAsync: storageKey: string -> contentBody: string -> Task<PutObjectResponse>
        abstract member CopyObjectAsync: sourceStorageKey: string -> destinationStorageKey: string -> Task<CopyObjectResponse>
        abstract member DeleteObjectAsync: storageKey: string -> Task<DeleteObjectResponse>

    type IS3ManagementApi =
        abstract member PutBucketAsync: bucketName: string -> Task<PutBucketResponse>
        abstract member DeleteBucketAsync: bucketName: string -> Task<DeleteBucketResponse>
