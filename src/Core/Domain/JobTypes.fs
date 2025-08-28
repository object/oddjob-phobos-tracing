namespace Nrk.Oddjob.Core

[<AutoOpen>]
module JobTypes =

    open System

    [<RequireQualifiedAccess>]
    type JobStatus =
        | Assigned
        | Queued
        /// Request is already fulfilled, skipped
        | Unchanged
        // operational
        | Initiated
        | Completed
        | Cancelled
        | Failed
        | Rejected

        member this.IsFinal() =
            match this with
            | Completed
            | Cancelled
            | Failed
            | Rejected -> true
            | _ -> false

    [<RequireQualifiedAccess>]
    type OverwriteMode =
        | IfNewer
        | Always

    /// Common information available in all Job messages not related to business logic of messages themselves
    type JobHeader =
        {
            RequestSource: string
            ForwardedFrom: string option
            Priority: int
            Timestamp: DateTimeOffset
            OverwriteMode: OverwriteMode
        }

        static member Zero =
            {
                RequestSource = String.Empty
                ForwardedFrom = None
                Priority = 0
                Timestamp = DateTimeOffset.Now
                OverwriteMode = OverwriteMode.IfNewer
            }
        static member Internal =
            { JobHeader.Zero with
                RequestSource = OddjobSystem.ToLower()
            }

    type PublishMediaSetJob =
        {
            Header: JobHeader
            MediaSetId: MediaSetId
            GeoRestriction: GeoRestriction
            Content: ContentSet
            MediaType: MediaType
        }

    type PublishFileJob =
        {
            Header: JobHeader
            MediaSetId: MediaSetId
            PartId: PartId option
            ContentFile: ContentFile
        }

    type PublishSubtitlesJob =
        {
            Header: JobHeader
            MediaSetId: MediaSetId
            PartId: PartId option
            SubtitlesFiles: SubtitlesFile list
        }

    type PublishSubtitlesLinksJob =
        {
            Header: JobHeader
            MediaSetId: MediaSetId
            SubtitlesLinks: SubtitlesLink list
        }

    type SetGeoRestrictionJob =
        {
            Header: JobHeader
            MediaSetId: MediaSetId
            GeoRestriction: GeoRestriction
        }

    type GlobalConnectCommandJob =
        {
            Header: JobHeader
            MediaSetId: MediaSetId
            Command: GlobalConnectCommand
        }

    type ClearMediaSetJob =
        {
            Header: JobHeader
            MediaSetId: MediaSetId
        }

    type RepairMediaSetJob =
        {
            Header: JobHeader
            MediaSetId: MediaSetId
        }

    type DeleteSubtitlesJob =
        {
            Header: JobHeader
            MediaSetId: MediaSetId
            PartId: PartId option
            LanguageCode: Alphanumeric
            Name: Alphanumeric
        }

    type ActivatePartsJob =
        {
            Header: JobHeader
            MediaSetId: MediaSetId
            Parts: (PartId * int) list
        }

    type DeactivatePartsJob =
        {
            Header: JobHeader
            MediaSetId: MediaSetId
            Parts: PartId list
        }

    [<RequireQualifiedAccess>]
    type MediaSetJob =
        | PublishMediaSet of PublishMediaSetJob
        | PublishFile of PublishFileJob
        | PublishSubtitles of PublishSubtitlesJob
        | PublishSubtitlesLinks of PublishSubtitlesLinksJob
        | DeleteSubtitles of DeleteSubtitlesJob
        | SetGeoRestriction of SetGeoRestrictionJob
        | ClearMediaSet of ClearMediaSetJob
        | RepairMediaSet of RepairMediaSetJob
        | ActivateParts of ActivatePartsJob
        | DeactivateParts of DeactivatePartsJob

        member this.CommandName = getUnionCaseName this

        member this.MediaSetId =
            match this with
            | PublishMediaSet job -> job.MediaSetId
            | PublishFile job -> job.MediaSetId
            | PublishSubtitles job -> job.MediaSetId
            | PublishSubtitlesLinks job -> job.MediaSetId
            | DeleteSubtitles job -> job.MediaSetId
            | SetGeoRestriction job -> job.MediaSetId
            | ClearMediaSet job -> job.MediaSetId
            | RepairMediaSet job -> job.MediaSetId
            | ActivateParts job -> job.MediaSetId
            | DeactivateParts job -> job.MediaSetId

        member this.Header =
            match this with
            | PublishMediaSet job -> job.Header
            | PublishFile job -> job.Header
            | PublishSubtitles job -> job.Header
            | PublishSubtitlesLinks job -> job.Header
            | DeleteSubtitles job -> job.Header
            | SetGeoRestriction job -> job.Header
            | ClearMediaSet job -> job.Header
            | RepairMediaSet job -> job.Header
            | ActivateParts job -> job.Header
            | DeactivateParts job -> job.Header

    type MediaSetJobContext =
        {
            Header: JobHeader
            MediaSetId: MediaSetId
            Job: MediaSetJob
            ResourceRef: ResourceRef option
        }

    module MediaSetJobContext =
        let fromMediaSetJob (job: MediaSetJob) =
            {
                Header = job.Header
                MediaSetId = job.MediaSetId
                Job = job
                ResourceRef =
                    match job with
                    | MediaSetJob.PublishFile job' ->
                        ResourceRef.File
                            {
                                PartId = job'.PartId
                                QualityId = job'.ContentFile.QualityId
                            }
                        |> Some
                    | _ -> None
            }

    type OriginStateUpdateContext =
        {
            Header: JobHeader
            MediaSetId: MediaSetId
            ResourceRef: ResourceRef
            ResourceState: DistributionState
        }

    module OriginStateUpdateContext =
        let create mediaSetId resourceRef resourceState =
            {
                Header = JobHeader.Internal
                MediaSetId = mediaSetId
                ResourceRef = resourceRef
                ResourceState = resourceState
            }

    [<RequireQualifiedAccess>]
    type ExecutionContext =
        | MediaSetJob of MediaSetJobContext
        | OriginStateUpdate of OriginStateUpdateContext

        member this.Header =
            match this with
            | MediaSetJob ctx -> ctx.Header
            | OriginStateUpdate ctx -> ctx.Header
        member this.MediaSetId =
            match this with
            | MediaSetJob ctx -> ctx.MediaSetId
            | OriginStateUpdate ctx -> ctx.MediaSetId
        member this.CommandName =
            match this with
            | MediaSetJob ctx -> ctx.Job.CommandName
            | OriginStateUpdate _ -> getUnionCaseName this
        member this.ResourceRef =
            match this with
            | MediaSetJob ctx -> ctx.ResourceRef
            | OriginStateUpdate ctx -> Some ctx.ResourceRef

    [<RequireQualifiedAccess>]
    type OriginJob =
        | GlobalConnectCommand of GlobalConnectCommandJob

        member this.CommandName =
            match this with
            | GlobalConnectCommand job -> job.Command.CommandName

        member this.MediaSetId =
            match this with
            | GlobalConnectCommand job -> job.MediaSetId

        member this.Header =
            match this with
            | GlobalConnectCommand job -> job.Header

    let (|FileJob|_|) job =
        match job with
        | OriginJob.GlobalConnectCommand job' ->
            match job'.Command with
            | GlobalConnectFileCommand fileRef -> Some fileRef
            | _ -> None

    let (|SubtitlesFileJob|_|) job =
        match job with
        | OriginJob.GlobalConnectCommand job' ->
            match job'.Command with
            | GlobalConnectSubtitlesCommand subRef -> Some subRef
            | _ -> None

    let (|UploadJob|_|) job =
        let (OriginJob.GlobalConnectCommand job) = job
        job.Command.IsUploadFile || job.Command.IsUploadSubtitles || job.Command.IsUploadSmil

    let (|DeleteJob|_|) job =
        let (OriginJob.GlobalConnectCommand job) = job
        job.Command.IsDeleteFile || job.Command.IsDeleteSubtitles || job.Command.IsDeleteSmil

    let (|StorageJob|_|) job =
        let (OriginJob.GlobalConnectCommand job) = job
        job.Command.IsCleanupStorage

    /// Information related to Job. Used in internal events - SftpEvent and AtvEvent
    type FileJobContext =
        {
            RequestId: string
            Job: OriginJob
            DesiredGeoRestriction: GeoRestriction
            FileGeoRestriction: GeoRestriction
            ClientGroupId: string option // TODO: consider removing (is in JobHeader, used in cluster sharding?)
        }

    module FileJobContext =
        let create desiredGeoRestriction fileGeoRestriction clientGroupId job =
            {
                RequestId = sprintf "%O" (Guid.NewGuid())
                Job = job
                DesiredGeoRestriction = desiredGeoRestriction
                FileGeoRestriction = fileGeoRestriction
                ClientGroupId = clientGroupId
            }
