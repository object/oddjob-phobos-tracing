namespace Nrk.Oddjob.WebApi

open Nrk.Oddjob.Upload.GlobalConnect

module MediaSetUtils =

    open System
    open Akka.Event
    open Akkling
    open Giraffe
    open Microsoft.AspNetCore.Http

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Core.MediaSetStateCache
    open Nrk.Oddjob.Upload

    open EventJournalUtils
    open WebAppUtils

    let private applyRightsResult rightsResult result =
        match result, rightsResult with
        | _, DistributionStatus.Ok -> result
        | DistributionStatus.Error msg, _ -> DistributionStatus.Warning msg
        | _, _ -> result

    let getRepairActions (oddjobConfig: OddjobConfig) actionEnvironment state mediaSetId =
        let validationSettings =
            {
                ValidationMode = MediaSetValidation.LocalAndRemote
                OverwriteMode = OverwriteMode.IfNewer
                StorageCleanupDelay = TimeSpan.FromHours oddjobConfig.Limits.OriginStorageCleanupIntervalInHours
            }
        let actionSelection =
            ActionSelection.all
            |> List.filter (fun x ->
                if OddjobConfig.hasGlobalConnect oddjobConfig.Origins then
                    true
                else
                    x <> ActionSelection.GlobalConnect)
        state
        |> MediaSetState.getRemainingActions NoLogger.Instance actionSelection actionEnvironment validationSettings mediaSetId
        |> Dto.MediaSet.RemainingActions.fromDomain

    let getMediaSetState mediaSetStateActor (reader: IStateReader) mediaSetId =
        async {
            let mediaSetId = NormalizedMediaSetId.create mediaSetId
            let! (state: Dto.MediaSet.MediaSetState) = mediaSetStateActor <? MediaSetStateCacheCommand.GetState mediaSetId
            let state = state.ToDomain()
            if state = MediaSetState.Zero || state.LastSequenceNr = 0L then
                let! state = reader.MediaSetState mediaSetId
                mediaSetStateActor
                <! MediaSetStateCacheCommand.SaveState
                    {
                        MediaSetId = mediaSetId
                        State = Dto.MediaSet.MediaSetState.FromDomain state
                        Timestamp = DateTime.Now
                    }
                return state
            else
                return state
        }

    let getMediaSetStateWithFilter mediaSetStateActor oddjobConfig (reader: IStateReader) mediaSetId createActionEnvironment includeActions =
        async {
            let! state = getMediaSetState mediaSetStateActor reader mediaSetId
            return
                {
                    PersistenceId = mediaSetId
                    State = Dto.MediaSet.MediaSetState.FromDomain state
                    RepairActions =
                        if includeActions then
                            Some(getRepairActions oddjobConfig createActionEnvironment state <| MediaSetId.parse mediaSetId)
                        else
                            None
                }
        }

    let toResponseWithMediaSetStateFilterTask<'T> (f: bool -> Async<'T>) =
        fun _next (ctx: HttpContext) ->
            task {
                let includeActions = parseQueryParam (ctx.Request.Query.Item "includeActions") TryParser.parseBool false
                let! result = f includeActions
                ctx.SetHttpHeader(AccessControlHeader, "*")
                return! ctx.NegotiateWithAsync(rules, unacceptableHandler, result)
            }

    let getGeneralSummary
        (oddjobConfig: OddjobConfig)
        (parts: PartInfo array)
        (ps: PsInfo option)
        (potion: PotionInfo option)
        (globalConnect: GlobalConnectInfo)
        actionEnvironment
        (state: MediaSetState)
        mediaSetId
        =
        let psSubtitles = ps |> Option.map _.Subtitles |> Option.defaultValue Array.empty
        let rightsResult =
            ps
            |> Option.map (fun ps -> PsUtils.getRightsResult ps.Rights |> PsUtils.rightsStatusToDistributionStatus)
            |> Option.defaultValue DistributionStatus.Ok
        let filesResult = PartUtils.getFilesResult parts |> applyRightsResult rightsResult
        let subtitlesResult = PartUtils.getSubtitlesResult parts psSubtitles |> applyRightsResult rightsResult
        let geoblockResult = PartUtils.getGeorestrictionResult parts
        let globalConnectFilesResult =
            GlobalConnect.getGlobalConnectFilesResult globalConnect filesResult |> applyRightsResult rightsResult
        let globalConnectSubtitlesResult =
            ps
            |> Option.map (fun _ -> GlobalConnect.getGlobalConnectSubtitlesResult globalConnect psSubtitles |> applyRightsResult rightsResult)
            |> Option.defaultValue DistributionStatus.Ok
        let archiveResult =
            ps |> Option.map (fun ps -> PsUtils.getArchiveResult ps parts) |> Option.defaultValue DistributionStatus.Ok
        let globalConnectResults =
            if OddjobConfig.hasGlobalConnect oddjobConfig.Origins then
                [ globalConnectFilesResult; globalConnectSubtitlesResult ]
            else
                []
        let results =
            List.concat
                [
                    globalConnectResults
                    [ filesResult; subtitlesResult; geoblockResult; rightsResult; archiveResult ]
                ]

        let errors =
            [
                results
                |> List.choose (fun r ->
                    match r with
                    | DistributionStatus.Error err -> Some err
                    | _ -> None)
                results
                |> List.choose (fun r ->
                    match r with
                    | DistributionStatus.Errors err -> Some(err |> Array.toList)
                    | _ -> None)
                |> List.concat
            ]
            |> List.concat
            |> List.toArray
        let warnings =
            results
            |> List.choose (fun r ->
                match r with
                | DistributionStatus.Warning text -> Some text
                | _ -> None)
            |> List.toArray

        let aggregatedStatus =
            if Seq.isNotEmpty errors then
                "Errors"
            else if Seq.isNotEmpty warnings then
                "Warnings"
            else if results |> Seq.tryFind (fun x -> x = DistributionStatus.InProgress) |> Option.isSome then
                "Upload in progress"
            else if results |> Seq.tryFind (fun x -> x = DistributionStatus.NotStarted) |> Option.isSome then
                "Upload not started"
            else
                "OK"
        {
            Status = aggregatedStatus
            LastUpdateTime = (CurrentMediaSetState.getLastUpdateTime state.Current (Origins.active oddjobConfig)).ToString("u")
            Format = state.Desired.MediaType.ToString()
            Mixdown = ContentSet.maxMixdown state.Desired.Content |> sprintf "%A"
            Files = PartUtils.getStatusSummary parts
            GlobalConnect = GlobalConnect.getStatusSummary globalConnect
            Ps = ps |> Option.map PsUtils.getStatusSummary
            Potion = potion |> Option.map (PotionUtils.getStatusSummary oddjobConfig)
            Errors = errors
            Warnings = warnings
            RepairActions = getRepairActions oddjobConfig actionEnvironment state mediaSetId
        }

    let createActionEnvironment (oddjobConfig: OddjobConfig) (connectionStrings: ConnectionStrings) s3Api log =
        let globalConnectEnvironment = GlobalConnectBootstrapperUtils.createGlobalConnectEnvironment oddjobConfig s3Api
        let contentEnvironment =
            Granitt.createContentEnvironment connectionStrings.GranittMySql log globalConnectEnvironment.RemoteFileSystemResolver
        {
            GlobalConnect = globalConnectEnvironment
            Content = contentEnvironment
        }

    let getAggregatedSummary
        oddjobConfig
        (connectionStrings: ConnectionStrings)
        s3Client
        clientId
        (persistenceId: string)
        (log: ILoggingAdapter)
        mediaSetStateActor
        (reader: IStateReader)
        =
        async {
            let persistenceId =
                match String.toLower clientId with
                | PotionClientId -> PotionUtils.evaluatePersistenceId reader persistenceId
                | _ -> persistenceId
            // ContentId is not normalized for actor paths
            let mediaSetId = sprintf "%s~%s" clientId persistenceId |> String.toLower
            let actionEnvironment = createActionEnvironment oddjobConfig connectionStrings s3Client log

            let! state = getMediaSetState mediaSetStateActor reader mediaSetId
            try
                let parts = PartUtils.getPartsInfo state
                let! ps =
                    async {
                        match clientId with
                        | "ps" ->
                            let! ps = PsUtils.getMediaSetInfo connectionStrings.CatalogUsageRights reader persistenceId log
                            return Some ps
                        | _ -> return None
                    }
                let potion =
                    match clientId with
                    | "potion" -> PotionUtils.getMediaSetInfo oddjobConfig reader persistenceId |> Some
                    | _ -> None
                let! globalConnect = GlobalConnect.getMediaSetInfo state s3Client
                return
                    {
                        Summary = getGeneralSummary oddjobConfig parts ps potion globalConnect actionEnvironment state <| MediaSetId.parse mediaSetId
                        MediaSetId = mediaSetId
                        Ps = ps
                        Potion = potion
                        GlobalConnect = Some globalConnect
                        Parts = parts
                    }
            with exn ->
                log.Error(exn, "Could not get aggregated summary for client:{0}, id:{1}", clientId, persistenceId)
                return
                    {
                        Summary =
                            {
                                Status = exn.Message
                                LastUpdateTime = null
                                Format = null
                                Mixdown = null
                                Files = null
                                GlobalConnect = null
                                Ps = None
                                Potion = None
                                Errors = [||]
                                Warnings = [||]
                                RepairActions = { GlobalConnect = []; Content = [] }
                            }
                        MediaSetId = mediaSetId
                        Ps = None
                        Potion = None
                        GlobalConnect = None
                        Parts = null
                    }
        }
