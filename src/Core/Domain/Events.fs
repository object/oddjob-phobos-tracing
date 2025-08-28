namespace Nrk.Oddjob.Core.Events

open System
open Nrk.Oddjob.Core

type FileJobOriginEvent =
    {
        JobContext: FileJobContext
        JobStatus: JobStatus
        Origin: Origin
        Result: Result<unit, int * string>
        Timestamp: DateTimeOffset
        Ack: MessageAck option
    }

    member this.GetSummary() =
        $"%s{getUnionCaseName this.JobStatus} %s{this.JobContext.Job.CommandName} %s{this.JobContext.Job.MediaSetId.Value}"
    member this.GetErrorMessage() =
        match this.Result with
        | Result.Ok() -> None
        | Result.Error(errorCode, errorMessage) ->
            if errorCode <> 0 then
                $"%s{errorMessage} (%%d{errorCode})"
            else
                $"%s{errorMessage}"
            |> Some

type PublishedGlobalConnectFileEvent =
    {
        FileRef: FileRef
        LocalPath: FilePath option
        RemotePath: RelativeUrl
        Timestamp: DateTimeOffset
    }

module PublishedGlobalConnectFileEvent =
    let fromCommand (UploadFileCommand fileRef) (file: GlobalConnectFile) timestamp =
        {
            PublishedGlobalConnectFileEvent.FileRef = fileRef
            LocalPath = file.SourcePath
            RemotePath = file.RemotePath
            Timestamp = timestamp
        }

type MovedGlobalConnectFileEvent =
    {
        FileRef: FileRef
        SourcePath: RelativeUrl
        DestinationPath: RelativeUrl
        Timestamp: DateTimeOffset
    }

module MovedGlobalConnectFileEvent =
    let fromCommand (MoveFileCommand fileRef) (file: GlobalConnectFile) accessRestrictions timestamp =
        {
            MovedGlobalConnectFileEvent.FileRef = fileRef
            SourcePath = file.RemotePath
            DestinationPath = GlobalConnectAccessRestrictions.apply accessRestrictions file.RemotePath
            Timestamp = timestamp
        }

type DeletedGlobalConnectFileEvent =
    {
        FileRef: FileRef
        Timestamp: DateTimeOffset
    }

module DeletedGlobalConnectFileEvent =
    let fromCommand (DeleteFileCommand fileRef) timestamp =
        {
            DeletedGlobalConnectFileEvent.FileRef = fileRef
            Timestamp = timestamp
        }

type RejectedGlobalConnectFileEvent =
    {
        FileRef: FileRef
        Reason: string
        Timestamp: DateTimeOffset
    }

type PublishedGlobalConnectSubtitlesEvent =
    {
        SubtitlesRef: SubtitlesRef
        LocalPath: SubtitlesLocation
        RemotePath: RelativeUrl
        Timestamp: DateTimeOffset
    }

module PublishedGlobalConnectSubtitlesEvent =
    let fromCommand (UploadSubtitlesCommand subRef) (sub: GlobalConnectSubtitles) timestamp =
        {
            PublishedGlobalConnectSubtitlesEvent.SubtitlesRef = subRef
            LocalPath = sub.Subtitles.SourcePath
            RemotePath = sub.RemotePath
            Timestamp = timestamp
        }

type MovedGlobalConnectSubtitlesEvent =
    {
        SubtitlesRef: SubtitlesRef
        SourcePath: RelativeUrl
        DestinationPath: RelativeUrl
        Timestamp: DateTimeOffset
    }

module MovedGlobalConnectSubtitlesEvent =
    let fromCommand (MoveSubtitlesCommand subRef) (file: GlobalConnectSubtitles) accessRestrictions timestamp =
        {
            MovedGlobalConnectSubtitlesEvent.SubtitlesRef = subRef
            SourcePath = file.RemotePath
            DestinationPath = GlobalConnectAccessRestrictions.apply accessRestrictions file.RemotePath
            Timestamp = timestamp
        }

type DeletedGlobalConnectSubtitlesEvent =
    {
        SubtitlesRef: SubtitlesRef
        Timestamp: DateTimeOffset
    }

module DeletedGlobalConnectSubtitlesEvent =
    let fromCommand (DeleteSubtitlesCommand subRef) timestamp =
        {
            DeletedGlobalConnectSubtitlesEvent.SubtitlesRef = subRef
            Timestamp = timestamp
        }

type RejectedGlobalConnectSubtitlesEvent =
    {
        SubtitlesRef: SubtitlesRef
        Reason: string
        Timestamp: DateTimeOffset
    }

type PublishedGlobalConnectSmilEvent =
    {
        Version: int
        RemotePath: RelativeUrl
        Timestamp: DateTimeOffset
    }

module PublishedGlobalConnectSmilEvent =
    let fromCommand (smil: GlobalConnectSmil) timestamp =
        {
            PublishedGlobalConnectSmilEvent.Version = smil.Version
            RemotePath = smil.RemotePath
            Timestamp = timestamp
        }

type MovedGlobalConnectSmilEvent =
    {
        OldVersion: int
        NewVersion: int
        RemotePath: RelativeUrl
        Timestamp: DateTimeOffset
    }

module MovedGlobalConnectSmilEvent =
    let fromCommand (MoveSmilCommand version) (smil: GlobalConnectSmil) accessRestrictions timestamp =
        {
            MovedGlobalConnectSmilEvent.OldVersion = version
            NewVersion = smil.Version
            RemotePath = GlobalConnectAccessRestrictions.apply accessRestrictions smil.RemotePath
            Timestamp = timestamp
        }

type DeletedGlobalConnectSmilEvent =
    {
        Version: int
        Timestamp: DateTimeOffset
    }

module DeletedGlobalConnectSmilEvent =
    let fromCommand (DeleteSmilCommand version) timestamp =
        {
            DeletedGlobalConnectSmilEvent.Version = version
            Timestamp = timestamp
        }

[<RequireQualifiedAccess>]
type OddjobEvent =
    | PublishedGlobalConnectFile of PublishedGlobalConnectFileEvent
    | MovedGlobalConnectFile of MovedGlobalConnectFileEvent
    | DeletedGlobalConnectFile of DeletedGlobalConnectFileEvent
    | RejectedGlobalConnectFile of RejectedGlobalConnectFileEvent
    | PublishedGlobalConnectSubtitles of PublishedGlobalConnectSubtitlesEvent
    | MovedGlobalConnectSubtitles of MovedGlobalConnectSubtitlesEvent
    | DeletedGlobalConnectSubtitles of DeletedGlobalConnectSubtitlesEvent
    | RejectedGlobalConnectSubtitles of RejectedGlobalConnectSubtitlesEvent
    | PublishedGlobalConnectSmil of PublishedGlobalConnectSmilEvent
    | MovedGlobalConnectSmil of MovedGlobalConnectSmilEvent
    | DeletedGlobalConnectSmil of DeletedGlobalConnectSmilEvent

    member this.Timestamp =
        match this with
        | PublishedGlobalConnectFile e -> e.Timestamp
        | MovedGlobalConnectFile e -> e.Timestamp
        | DeletedGlobalConnectFile e -> e.Timestamp
        | RejectedGlobalConnectFile e -> e.Timestamp
        | PublishedGlobalConnectSubtitles e -> e.Timestamp
        | MovedGlobalConnectSubtitles e -> e.Timestamp
        | DeletedGlobalConnectSubtitles e -> e.Timestamp
        | RejectedGlobalConnectSubtitles e -> e.Timestamp
        | PublishedGlobalConnectSmil e -> e.Timestamp
        | MovedGlobalConnectSmil e -> e.Timestamp
        | DeletedGlobalConnectSmil e -> e.Timestamp

[<RequireQualifiedAccess>]
type MediaResource =
    | GlobalConnectFile of FileRef * GlobalConnectFile
    | GlobalConnectSubtitles of SubtitlesRef * GlobalConnectSubtitles
    | GlobalConnectSmil of GlobalConnectSmil

[<RequireQualifiedAccess>]
type OddjobCommandEvent =
    {
        Timestamp: DateTimeOffset
        Origin: Origin
        Status: JobStatus
        ErrorMessage: string option
        CommandName: string
        RequestSource: string
        ForwardedFrom: string option
        Resource: MediaResource option
        ClientId: string
    }

module OddjobCommandEvent =
    let fromOriginJob origin commandStatus errorMessage (resource: MediaResource option) timestamp (job: OriginJob) : OddjobCommandEvent =
        {
            Timestamp = timestamp
            Origin = origin
            Status = commandStatus
            CommandName = job.CommandName
            ErrorMessage = errorMessage
            RequestSource = job.Header.RequestSource
            ForwardedFrom = job.Header.ForwardedFrom
            ClientId = job.MediaSetId.ClientId.Value
            Resource = resource
        }

    let fromExecutionContext origin commandStatus errorMessage (resource: MediaResource option) timestamp (ctx: ExecutionContext) : OddjobCommandEvent =
        {
            Timestamp = timestamp
            Origin = origin
            Status = commandStatus
            CommandName = ctx.CommandName
            ErrorMessage = errorMessage
            RequestSource = ctx.Header.RequestSource
            ForwardedFrom = ctx.Header.ForwardedFrom
            ClientId = ctx.MediaSetId.ClientId.Value
            Resource = resource
        }

    let fromOriginEvent origin resource (originEvent: FileJobOriginEvent) =
        fromOriginJob origin originEvent.JobStatus (originEvent.GetErrorMessage()) resource originEvent.Timestamp

[<RequireQualifiedAccess>]
type PlayabilityEventPriority =
    | Normal
    | High

type NonPlayableMediaSet =
    {
        MediaSetId: MediaSetId
        Timestamp: DateTimeOffset
        Priority: PlayabilityEventPriority
    }

type PlayableOnDemandMediaSet =
    {
        MediaSetId: MediaSetId
        Timestamp: DateTimeOffset
        Priority: PlayabilityEventPriority
    }

type MediaSetPlayabilityEvent =
    | MediaSetPlayableOnDemand of PlayableOnDemandMediaSet
    | MediaSetNotPlayableOnDemand of NonPlayableMediaSet

    member this.MediaSetId =
        match this with
        | MediaSetPlayableOnDemand msg -> msg.MediaSetId
        | MediaSetNotPlayableOnDemand msg -> msg.MediaSetId

module MediaSetPlayabilityEvent =
    let createOnDemand mediaSetId mediaSetState origins timestamp priority =
        if not (mediaSetState.Desired.IsEmpty()) && CurrentMediaSetState.isPlayable mediaSetState.Current origins then
            MediaSetPlayableOnDemand
                {
                    MediaSetId = mediaSetId
                    Timestamp = timestamp
                    Priority = priority
                }
        else
            MediaSetNotPlayableOnDemand
                {
                    MediaSetId = mediaSetId
                    Timestamp = timestamp
                    Priority = priority
                }

    let create mediaSetId mediaSetState origins timestamp priority =
        createOnDemand mediaSetId mediaSetState origins timestamp priority
