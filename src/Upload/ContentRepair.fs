namespace Nrk.Oddjob.Upload

module ContentRepair =

    open System
    open Akka.Actor
    open Akkling

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Core.Granitt

    [<NoEquality; NoComparison>]
    type ContentRepairActorProps =
        {
            MediaSetController: IActorRef<obj>
            GetGranittAccess: IActorContext -> IActorRef<Granitt.GranittCommand>
            FileSystem: IFileSystemInfo
            DestinationRoot: string
            ArchiveRoot: string
            AlternativeArchiveRoot: string
            SubtitlesSourceRootWindows: string
            SubtitlesSourceRootLinux: string
            PathMappings: OddjobConfig.PathMapping list
            ActionEnvironment: ActionEnvironment
        }

    type ContentValidationCommand =
        {
            MediaSetId: MediaSetId
            MediaSetState: MediaSetState
            RequestSource: string
            Priority: int
            Ack: MessageAck option
        }

    type ContentValidationResult =
        | ContentIsValidated of ContentValidationCommand
        | ContentIsRepaired of ContentValidationCommand

    let contentRepairActor (aprops: ContentRepairActorProps) (mailbox: Actor<_>) =

        let repairGeoRestriction geoRestriction =
            logDebug mailbox "Repairing GeoRestriction"
            geoRestriction
            |> Option.iter (fun geoRestriction -> aprops.MediaSetController <! box (MediaSetCommand.SetGeoRestriction geoRestriction))

        let repairMediaType mediaType =
            logDebug mailbox "Repairing MediaType"
            aprops.MediaSetController <! box (MediaSetCommand.SetMediaType mediaType)

        let repairSourceFiles (state: MediaSetState) =
            let files =
                state.Desired.Content
                |> ContentSet.getFiles
                |> Map.map (fun _ file ->
                    let oldPath = FilePath.value file.SourcePath.Value
                    let newPath = System.Text.RegularExpressions.Regex.Replace(oldPath, @"^\\\\madata(\d)+\\(\d)+\\", aprops.DestinationRoot)
                    logDebug mailbox $"Repairing SourcePath from {oldPath} to {newPath}"
                    { file with
                        SourcePath = FilePath.tryCreate newPath
                    })
            let content =
                files
                |> Map.fold (fun content fileRef file -> content |> ContentSet.assignFile fileRef.PartId file) state.Desired.Content
            aprops.MediaSetController <! box (MediaSetCommand.AssignDesiredState({ state.Desired with Content = content }))

        let repairGlobalConnectFile (state: MediaSetState) (fileRef: FileRef) =
            logDebug mailbox $"Repairing GlobalConnect file %A{fileRef}"
            match state.Current.GlobalConnect.Files |> Map.tryFind fileRef with
            | Some remoteFile ->
                state.Desired.Content
                |> ContentSet.tryGetFile fileRef
                |> Option.iter (fun file ->
                    let globalConnectFile: GlobalConnectFile =
                        {
                            SourcePath = file.SourcePath
                            RemotePath = remoteFile.File.RemotePath
                            TranscodingVersion = file.MediaProperties |> MediaProperties.transcodingVersion
                        }
                    let remoteState: RemoteState =
                        {
                            State = DistributionState.Completed
                            Timestamp = remoteFile.RemoteState.Timestamp
                        }
                    let remoteResult = RemoteResult.Ok()
                    aprops.MediaSetController <! box (MediaSetCommand.AssignGlobalConnectFile(fileRef, globalConnectFile))
                    aprops.MediaSetController
                    <! box (MediaSetEvent.ReceivedRemoteFileState(Origin.GlobalConnect, fileRef, remoteState, remoteResult)))
            | None -> ()

        let repairContent (cmd: ContentValidationCommand) contentActions (details: Granitt.GranittContent) =
            contentActions
            |> List.iter (fun action ->
                match action with
                | ContentCommand.RepairMediaType -> repairMediaType details.MediaType
                | ContentCommand.RepairSourceFiles -> repairSourceFiles cmd.MediaSetState
                | ContentCommand.RepairGlobalConnectFile(RepairGlobalConnectFileCommand fileRef) -> repairGlobalConnectFile cmd.MediaSetState fileRef)
            aprops.MediaSetController <! Message.create (ShardMessages.MediaSetShardMessage.UpdateMediaSetStateCache cmd.MediaSetId)

        let rec idle () =
            logDebug mailbox "idle"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? ContentValidationCommand as cmd ->
                        if cmd.MediaSetId.ClientId = Alphanumeric PsClientId then
                            let granitt = aprops.GetGranittAccess mailbox.UntypedContext
                            granitt <! Granitt.GranittCommand.GetGranittDetails cmd.MediaSetId
                            awaiting_granitt cmd
                        else
                            evaluating_actions
                                cmd
                                {
                                    Granitt.GranittContent.MediaType = MediaType.Default
                                }
                            ignored ()
                    | _ -> ignored ()
            }
        and awaiting_granitt cmd =
            logDebug mailbox "awaiting_granitt"
            actor {
                let! message = mailbox.Receive()
                return!
                    match message with
                    | :? GranittResult<Granitt.GranittContent> as result ->
                        match result with
                        | GranittResult.Ok details -> evaluating_actions cmd details
                        | GranittResult.Error exn ->
                            logErrorWithExn mailbox exn "Failed to repair content"
                            terminating ()
                        ignored ()
                    | _ -> ignored ()
            }
        and evaluating_actions cmd details =
            logDebug mailbox "evaluating_actions"
            let validationSettings =
                {
                    ValidationMode = MediaSetValidation.LocalAndRemote
                    OverwriteMode = OverwriteMode.IfNewer
                    StorageCleanupDelay = TimeSpan.MinValue
                }
            let contentEnvironment =
                { aprops.ActionEnvironment.Content with
                    GetGranittMediaType = fun _ -> details.MediaType
                }
            let actionEnvironment =
                { aprops.ActionEnvironment with
                    Content = contentEnvironment
                }
            let logger = getLoggerForMailbox mailbox
            let actions =
                cmd.MediaSetState
                |> MediaSetState.getRemainingActions logger [ ActionSelection.Content ] actionEnvironment validationSettings cmd.MediaSetId
            match actions.Content with
            | [] ->
                aprops.MediaSetController <! box (ContentIsValidated cmd)
                terminating ()
            | actions -> repairing_content cmd details actions
        and repairing_content cmd details actions =
            logDebug mailbox $"repairing_content (%A{actions})"
            repairContent (cmd: ContentValidationCommand) actions details
            aprops.MediaSetController <! box (ContentIsRepaired cmd)
            terminating ()
        and terminating () =
            (retype mailbox.Self) <! PoisonPill.Instance

        idle ()
