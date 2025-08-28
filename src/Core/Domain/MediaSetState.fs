namespace Nrk.Oddjob.Core

[<AutoOpen>]
module MediaSetState =

    module DesiredMediaSetState =
        let update msg state : DesiredMediaSetState =
            match msg with
            | MediaSetEvent.SetMediaType mt ->
                { state with
                    DesiredMediaSetState.MediaType = mt
                }

            | MediaSetEvent.SetGeoRestriction gr ->
                if gr = GeoRestriction.Unspecified then
                    state
                else
                    { state with GeoRestriction = gr }

            | MediaSetEvent.AssignedPart(partId, partNumber) ->
                { state with
                    Content = state.Content |> ContentSet.assignPart partId partNumber
                }

            | MediaSetEvent.AssignedFile(partId, file) ->
                let content = state.Content |> ContentSet.assignFile partId file
                { state with Content = content }

            | MediaSetEvent.AssignedSubtitlesFile subtitles ->
                // Version check can be removed when the legacy SubtitleEngine is no more
                let newVersion = subtitles.Version
                let oldVersion =
                    match state.Content |> ContentSet.getSubtitles with
                    | [] -> 0
                    | subs -> subs |> List.maxBy _.Version |> _.Version
                if oldVersion = 0 || newVersion > oldVersion then
                    let content = state.Content |> ContentSet.assignSubtitlesFile subtitles
                    { state with Content = content }
                else
                    state

            | MediaSetEvent.AssignedSubtitlesFiles subtitles ->
                if Seq.isNotEmpty subtitles then
                    // Version check can be removed when the legacy SubtitleEngine is no more
                    let newVersion = subtitles |> Seq.head |> _.Version
                    let oldVersion =
                        match state.Content |> ContentSet.getSubtitles with
                        | [] -> 0
                        | subs -> subs |> List.maxBy _.Version |> _.Version
                    if oldVersion = 0 || newVersion > oldVersion then
                        let content = state.Content |> ContentSet.assignSubtitlesFiles subtitles
                        { state with Content = content }
                    else
                        state
                else
                    state
            | MediaSetEvent.AssignedSubtitlesLinks links ->
                { state with
                    Content = state.Content |> ContentSet.assignSubtitlesLinks links
                }
            | MediaSetEvent.RemovedPart partId ->
                let content = state.Content |> ContentSet.removePart partId
                { state with Content = content }

            | MediaSetEvent.RevokedPart partId ->
                let content = state.Content |> ContentSet.removePart partId
                let revokedParts = partId :: state.RevokedParts |> List.distinct
                { state with
                    Content = content
                    RevokedParts = revokedParts
                }

            | MediaSetEvent.RestoredPart partId ->
                let revokedParts = state.RevokedParts |> List.filter (fun x -> x <> partId)
                { state with
                    RevokedParts = revokedParts
                }

            | MediaSetEvent.RemovedFile(partId, file) ->
                let content = state.Content |> ContentSet.removeFile partId file
                { state with Content = content }

            | MediaSetEvent.RemovedSubtitlesFile(lang, kind) ->
                let trackRef = { LanguageCode = lang; Name = kind }
                let content = state.Content |> ContentSet.removeSubtitlesFile trackRef
                { state with Content = content }

            | MediaSetEvent.AssignedDesiredState desiredState ->
                let gr =
                    if desiredState.GeoRestriction <> GeoRestriction.Unspecified then
                        desiredState.GeoRestriction
                    else
                        state.GeoRestriction
                { desiredState with
                    GeoRestriction = gr
                }

            | MediaSetEvent.MergedDesiredState desiredState ->
                let gr =
                    if desiredState.GeoRestriction <> GeoRestriction.Unspecified then
                        desiredState.GeoRestriction
                    else
                        state.GeoRestriction
                let content = state.Content |> ContentSet.merge desiredState.Content
                { state with
                    GeoRestriction = gr
                    Content = content
                }

            | MediaSetEvent.ClearedDesiredState ->
                { DesiredMediaSetState.Zero with
                    MediaType = state.MediaType
                }

            | _ -> state

    module CurrentMediaSetState =

        [<RequireQualifiedAccess>]
        type private StateAction =
            | Update
            | Delete

        let update msg (desiredState: DesiredMediaSetState) (state: CurrentMediaSetState) =

            let contentContainsResourceRef resourceRef =
                match resourceRef with
                | ResourceRef.File fileRef -> desiredState.Content |> ContentSet.containsFileRef fileRef
                | ResourceRef.Subtitles subRef -> desiredState.Content |> ContentSet.containsSubtitlesRef subRef
                | ResourceRef.Smil -> true

            let getUpdateAction resourceRef (remoteState: RemoteState) =
                match remoteState.State with
                | DistributionState.Initiated
                | DistributionState.Completed
                | DistributionState.Failed -> StateAction.Update
                | DistributionState.Deleted -> StateAction.Delete
                | _ ->
                    if contentContainsResourceRef resourceRef then
                        StateAction.Update
                    else
                        StateAction.Delete

            match msg with
            | MediaSetEvent.ReceivedRemoteFileState(origin, fileRef, remoteState, remoteResult) ->
                match origin with
                | Origin.GlobalConnect ->
                    let files =
                        match getUpdateAction (ResourceRef.File fileRef) remoteState with
                        | StateAction.Update ->
                            let file =
                                match state.GlobalConnect.Files |> Map.tryFind fileRef with
                                | Some x -> x
                                | None -> GlobalConnectFileState.Zero
                            state.GlobalConnect.Files
                            |> Map.add
                                fileRef
                                { file with
                                    RemoteState = remoteState
                                    LastResult = remoteResult
                                }
                        | StateAction.Delete -> state.GlobalConnect.Files |> Map.remove fileRef
                    { state with
                        GlobalConnect.Files = files
                    }

            | MediaSetEvent.ReceivedRemoteSubtitlesFileState(origin, subRef, remoteState, remoteResult) ->
                match origin with
                | Origin.GlobalConnect ->
                    let subs =
                        match getUpdateAction (ResourceRef.Subtitles subRef) remoteState with
                        | StateAction.Update ->
                            let sub =
                                match state.GlobalConnect.Subtitles |> Map.tryFind subRef with
                                | Some x -> x
                                | None -> GlobalConnectSubtitlesState.Zero
                            state.GlobalConnect.Subtitles
                            |> Map.add
                                subRef
                                { sub with
                                    RemoteState = remoteState
                                    LastResult = remoteResult
                                }
                        | StateAction.Delete -> state.GlobalConnect.Subtitles |> Map.remove subRef
                    { state with
                        GlobalConnect.Subtitles = subs
                    }

            | MediaSetEvent.ReceivedRemoteSmilState(origin, version, smilState, smilResult) ->
                match origin with
                | Origin.GlobalConnect when state.GlobalConnect.Smil.Smil.Version = version ->
                    let smil =
                        if smilState.State = DistributionState.Deleted then
                            GlobalConnectSmilState.Zero
                        else
                            {
                                Smil = state.GlobalConnect.Smil.Smil
                                RemoteState = smilState
                                LastResult = smilResult
                            }
                    { state with GlobalConnect.Smil = smil }
                | Origin.GlobalConnect -> state

            | MediaSetEvent.ClearedRemoteFile(origin, _) ->
                match origin with
                | Origin.GlobalConnect -> notImpl "GlobalConnect ClearedRemoteFile"

            | MediaSetEvent.AssignedGlobalConnectFile(fileRef, file) ->
                let fileState =
                    match state.GlobalConnect.Files |> Map.tryFind fileRef with
                    | Some x ->
                        { x with
                            File =
                                { file with
                                    SourcePath = file.SourcePath |> Option.orElse x.File.SourcePath
                                }
                        }
                    | None ->
                        {
                            File = file
                            RemoteState = RemoteState.None
                            LastResult = Result.Ok()
                        }
                { state with
                    GlobalConnect.Files = state.GlobalConnect.Files |> Map.add fileRef fileState
                }

            | MediaSetEvent.AssignedGlobalConnectSubtitles subs ->
                let subState =
                    match state.GlobalConnect.Subtitles |> Map.tryFind subs.Subtitles.TrackRef with
                    | Some x -> { x with Subtitles = subs }
                    | None ->
                        {
                            Subtitles = subs
                            RemoteState = RemoteState.None
                            LastResult = RemoteResult.Ok()
                        }
                { state with
                    GlobalConnect.Subtitles = state.GlobalConnect.Subtitles |> Map.add subs.Subtitles.TrackRef subState
                }

            | MediaSetEvent.AssignedGlobalConnectSmil smil ->
                let smilState: GlobalConnectSmilState =
                    {
                        Smil = smil
                        RemoteState = RemoteState.None
                        LastResult = Result.Ok()
                    }
                { state with
                    GlobalConnect.Smil = smilState
                    GlobalConnect.Version = smil.Version
                }

            | MediaSetEvent.ClearedCurrentState -> CurrentMediaSetState.Zero

            | _ -> state

    [<RequireQualifiedAccess>]
    type RemainingActions =
        {
            GlobalConnect: GlobalConnectActions
        }

        static member Zero = { GlobalConnect = List.empty }

    [<RequireQualifiedAccess>]
    type ActionSelection =
        | GlobalConnect
        | Content

    module ActionSelection =
        let all = [ ActionSelection.GlobalConnect; ActionSelection.Content ]

    [<RequireQualifiedAccess>]
    module MediaSetState =
        let update msg state =
            match msg with
            | MediaSetEvent.AssignedClientContentId contentId ->
                { state with
                    ClientContentId = contentId
                }
            | MediaSetEvent.SetSchemaVersion version -> { state with SchemaVersion = version }
            // Desired state update
            | MediaSetEvent.SetMediaType _
            | MediaSetEvent.SetGeoRestriction _
            | MediaSetEvent.AssignedPart _
            | MediaSetEvent.AssignedFile _
            | MediaSetEvent.AssignedSubtitlesFile _
            | MediaSetEvent.AssignedSubtitlesFiles _
            | MediaSetEvent.AssignedSubtitlesLinks _
            | MediaSetEvent.RemovedPart _
            | MediaSetEvent.RevokedPart _
            | MediaSetEvent.RestoredPart _
            | MediaSetEvent.RemovedFile _
            | MediaSetEvent.RemovedSubtitlesFile _
            | MediaSetEvent.AssignedDesiredState _
            | MediaSetEvent.MergedDesiredState _
            | MediaSetEvent.ClearedDesiredState ->
                { state with
                    Desired = state.Desired |> DesiredMediaSetState.update msg
                }
            // Current state update
            | MediaSetEvent.ReceivedRemoteFileState _
            | MediaSetEvent.ReceivedRemoteSubtitlesFileState _
            | MediaSetEvent.ReceivedRemoteSmilState _
            | MediaSetEvent.ClearedRemoteFile _
            | MediaSetEvent.AssignedGlobalConnectFile _
            | MediaSetEvent.AssignedGlobalConnectSubtitles _
            | MediaSetEvent.AssignedGlobalConnectSmil _
            | MediaSetEvent.ClearedCurrentState ->
                { state with
                    Current = state.Current |> CurrentMediaSetState.update msg state.Desired
                }
            | MediaSetEvent.Deprecated -> state

        let getRemainingActions
            logger
            actionSelection
            (actionEnvironment: ActionEnvironment)
            validationSettings
            mediaSetId
            (set: MediaSetState)
            : RemainingActions =
            let globalConnectActionContext = GlobalConnectActionContext.create logger validationSettings actionEnvironment.GlobalConnect
            let globalConnectActions =
                if actionSelection |> List.contains ActionSelection.GlobalConnect then
                    set.Desired |> GlobalConnectActions.forMediaSet globalConnectActionContext mediaSetId set.Current.GlobalConnect
                else
                    []
            { GlobalConnect = globalConnectActions }
