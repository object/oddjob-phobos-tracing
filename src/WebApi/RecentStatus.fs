namespace Nrk.Oddjob.WebApi

open System
open Akka.Event
open Akkling
open Giraffe
open Microsoft.AspNetCore.Http

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Config
open Nrk.Oddjob.Upload.MediaSetStatusPersistence

open WebAppUtils

module RecentStatus =

    let getRecentStatus recentStatusActor cmd =
        async {
            let! results = recentStatusActor <? StatusQueryCommand cmd
            return! results
        }

    let getRemainingActions mediaSetId status (oddjobConfig: OddjobConfig) mediaSetStateActor reader actionEnvironment =
        let actionSelection =
            ActionSelection.all
            |> List.filter (fun x ->
                if OddjobConfig.hasGlobalConnect oddjobConfig.Origins then
                    true
                else
                    x <> ActionSelection.GlobalConnect)
        match status with
        | MediaSetStatus.Completed -> RemainingActions.Zero
        | _ ->
            match MediaSetId.tryParse mediaSetId with
            | Some mediaSetId ->
                let state = MediaSetUtils.getMediaSetState mediaSetStateActor reader mediaSetId.Value |> Async.RunSynchronously
                let validationSettings =
                    {
                        ValidationMode = MediaSetValidation.Local
                        OverwriteMode = OverwriteMode.IfNewer
                        StorageCleanupDelay = TimeSpan.FromTicks oddjobConfig.Limits.OriginStorageCleanupIntervalInHours
                    }
                state
                |> MediaSetState.getRemainingActions NoLogger.Instance actionSelection actionEnvironment validationSettings mediaSetId
            | _ -> RemainingActions.Zero
        |> Dto.MediaSet.RemainingActions.fromDomain

    let toResponseWithMediaSetStateFilterTask<'T>
        oddjobConfig
        mediaSetStateActor
        reader
        actionEnvironment
        (f: StatusQueryCommand -> Async<MediaSetStatusResult>)
        =
        fun _next (ctx: HttpContext) ->

            let tryParseMediaSetStatus s =
                match tryParseUnionCaseName<MediaSetStatus> true s with
                | Some r -> true, Some r
                | None -> false, None

            task {
                let count = parseQueryParam (ctx.Request.Query.Item "count") TryParser.parseInt 100
                let countOnly = parseQueryParam (ctx.Request.Query.Item "countOnly") TryParser.parseBool false
                let skipCompleted = parseQueryParam (ctx.Request.Query.Item "skipCompleted") TryParser.parseBool false
                let status = parseQueryParam (ctx.Request.Query.Item "status") (TryParser.tryParseWith tryParseMediaSetStatus) None
                let includeActions = parseQueryParam (ctx.Request.Query.Item "includeActions") TryParser.parseBool false
                let takeLast =
                    parseQueryParam (ctx.Request.Query.Item "takeLast") TryParser.parseTimeIntervalWithSpecification TimeSpan.MinValue
                let skipLast =
                    parseQueryParam (ctx.Request.Query.Item "skipLast") TryParser.parseTimeIntervalWithSpecification TimeSpan.MinValue
                let now = DateTime.Now
                let fromLastActivationTime =
                    if takeLast > TimeSpan.MinValue then
                        Some(now - takeLast)
                    else
                        None
                let toLastActivationTime =
                    if skipLast > TimeSpan.MinValue then
                        Some(now - skipLast)
                    else
                        Some DateTime.Now
                let countSpec = if countOnly then CountOnly else MaxCount count
                let orderSpec =
                    if countSpec = CountOnly then
                        Unspecified
                    else
                        ActivationDesc

                let cmd =
                    let interval = LastActivationAndUpdateTime(fromLastActivationTime, toLastActivationTime)
                    if skipCompleted then
                        QueryNotHavingStatus(MediaSetStatus.Completed, interval, countSpec, orderSpec)
                    else
                        match status with
                        | Some status -> QueryHavingStatus(status, interval, countSpec, orderSpec)
                        | None -> QueryAll(interval, countSpec, orderSpec)

                let! result = f cmd

                let result =
                    match result with
                    | ResultRows result ->
                        result
                        |> Seq.map (fun x ->
                            {
                                MediaSetId = x.MediaSetId
                                Status = MediaSetStatus.fromInt x.Status
                                ActivationTime = x.LastActivationTime
                                UpdateTime = x.LastUpdateTime
                                RetryCount = x.RetryCount
                                Priority = x.Priority
                                RemainingActions =
                                    if includeActions then
                                        Some(
                                            getRemainingActions
                                                x.MediaSetId
                                                (MediaSetStatus.fromInt x.Status)
                                                oddjobConfig
                                                mediaSetStateActor
                                                reader
                                                actionEnvironment
                                        )
                                    else
                                        None
                            })
                        |> box
                    | ResultCount result ->
                        result
                        |> fun x ->
                            {
                                Count = x
                                Query =
                                    ctx.Request.Query.Keys
                                    |> Seq.map (fun k -> k, ctx.Request.Query.Item k)
                                    |> Seq.map (fun (x, y) -> x, y.ToArray() |> Seq.head)
                                    |> Map.ofSeq
                            }
                        |> box

                ctx.SetHttpHeader(AccessControlHeader, "*")
                return! ctx.NegotiateWithAsync(rules, unacceptableHandler, result)
            }
