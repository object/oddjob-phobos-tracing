namespace Nrk.Oddjob.WebApi

open System
open Akkling
open Akka.Cluster.Tools.PublishSubscribe

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.S3.S3Types
open Nrk.Oddjob.Ingesters.GlobalConnect.IngesterMetrics

module IngesterStatus =

    type IdleStatus = { Since: DateTimeOffset }

    type IngesterEvent = { Name: string; Time: DateTimeOffset }

    type JobStatus =
        {
            Started: DateTimeOffset
            BytesSent: uint64
            ProcessingTime: TimeSpan option
            Events: IngesterEvent list
        }

        static member Empty =
            {
                Started = DateTimeOffset.Now
                BytesSent = 0UL
                ProcessingTime = None
                Events = []
            }

    [<RequireQualifiedAccess>]
    type IngesterState =
        | Idle of IdleStatus
        | Working of JobStatus

    type TotalStats =
        {
            Jobs: int
            BytesSent: uint64
            ProcessingTime: TimeSpan
            Outcomes: Map<string, int>
        }

        static member Empty =
            {
                Jobs = 0
                BytesSent = 0UL
                ProcessingTime = TimeSpan.FromSeconds 0.
                Outcomes = Map.empty
            }

    type CurrentIngesterState =
        {
            Status: IngesterState
            QueueLength: int
            QueueItems: string array
        }

    module CurrentIngesterState =
        let setStatus status (state: CurrentIngesterState) = { state with Status = status }

        let setQueueLength queueLength (state: CurrentIngesterState) =
            { state with QueueLength = queueLength }

        let setQueueItems queueItems (state: CurrentIngesterState) = { state with QueueItems = queueItems }

    type IngesterStats =
        {
            Current: CurrentIngesterState
            LastJob: JobStatus option
            Past: TotalStats
        }

        static member Empty =
            {
                Current =
                    {
                        Status = IngesterState.Idle { Since = DateTimeOffset.Now }
                        QueueLength = 0
                        QueueItems = Array.empty
                    }
                LastJob = None
                Past = TotalStats.Empty
            }

    module IngesterStats =
        let setIdle (stats: IngesterStats) =
            let status = IngesterState.Idle { Since = DateTimeOffset.Now }
            { stats with
                Current = stats.Current |> CurrentIngesterState.setStatus status
            }

        let setWorking stats =
            match stats.Current.Status with
            | IngesterState.Idle _ ->
                let status = IngesterState.Working JobStatus.Empty
                { stats with
                    Current = stats.Current |> CurrentIngesterState.setStatus status
                }
            | IngesterState.Working _ -> stats

        let updateEvents topic status (events: IngesterEvent list) =
            let eventName =
                match topic with
                | PubSub.Topic.GlobalConnectIngesterStatus -> GlobalConnectIngesterStatus.fromInt status |> getUnionCaseName
                | _ -> notSupported <| $"Topic %s{topic} is not supported"
            {
                Name = eventName
                Time = DateTimeOffset.Now
            }
            :: (events |> List.filter (fun x -> x.Name <> eventName))
            |> List.sortBy _.Time

        let private shouldSkipStatus topic status =
            match topic with
            | PubSub.Topic.GlobalConnectIngesterStatus -> GlobalConnectIngesterStatus.fromInt status = GlobalConnectIngesterStatus.EnteredIdle
            | _ -> false

        let updateStatus topic status (stats: IngesterStats) =
            if shouldSkipStatus topic status then
                stats
            else
                let status =
                    match stats.Current.Status with
                    | IngesterState.Idle _ -> IngesterState.Working JobStatus.Empty
                    | IngesterState.Working job ->
                        IngesterState.Working
                            {
                                Started = job.Started
                                BytesSent = job.BytesSent
                                ProcessingTime = None
                                Events = job.Events |> updateEvents topic status
                            }
                { stats with
                    Current = stats.Current |> CurrentIngesterState.setStatus status
                }

        let updateQueueLength queueLength (stats: IngesterStats) =
            { stats with
                Current = stats.Current |> CurrentIngesterState.setQueueLength queueLength
            }

        let updateQueueItems queueItems (stats: IngesterStats) =
            { stats with
                Current = stats.Current |> CurrentIngesterState.setQueueItems queueItems
            }

        let updateProgress bytes stats =
            let status =
                match stats.Current.Status with
                | IngesterState.Idle _ -> stats.Current.Status
                | IngesterState.Working job ->
                    IngesterState.Working
                        {
                            Started = job.Started
                            BytesSent = job.BytesSent + bytes
                            ProcessingTime = job.ProcessingTime
                            Events = job.Events
                        }
            { stats with
                Current = stats.Current |> CurrentIngesterState.setStatus status
            }

        let updateTotals totalBytes totalTime stats =
            let outcomes, lastJob =
                match stats.Current.Status with
                | IngesterState.Idle _ -> stats.Past.Outcomes, None
                | IngesterState.Working work ->
                    let lastJob =
                        Some
                            { work with
                                BytesSent = totalBytes
                                ProcessingTime = Some totalTime
                            }
                    match work.Events |> Seq.tryLast with
                    | Some event ->
                        let count = stats.Past.Outcomes |> Map.tryFind event.Name |> Option.defaultValue 0
                        stats.Past.Outcomes |> Map.add event.Name (count + 1), lastJob
                    | None -> stats.Past.Outcomes, lastJob
            let totals =
                {
                    Jobs = stats.Past.Jobs + 1
                    BytesSent = stats.Past.BytesSent + totalBytes
                    ProcessingTime = stats.Past.ProcessingTime + totalTime
                    Outcomes = outcomes
                }
            {
                Current =
                    {
                        Status = IngesterState.Idle { Since = DateTimeOffset.Now }
                        QueueLength = stats.Current.QueueLength
                        QueueItems = stats.Current.QueueItems
                    }
                LastJob = lastJob
                Past = totals
            }

    type IngesterStatusCollection = Map<string, IngesterStats>

    type CurrentIngestersState =
        {
            PendingJobs: int
            IdleIngesters: int
            WorkingIngesters: int
            QueueLength: int
        }

        static member Empty =
            {
                PendingJobs = 0
                WorkingIngesters = 0
                IdleIngesters = 0
                QueueLength = 0
            }

    type IngestersState =
        {
            RunningSince: DateTimeOffset
            Current: CurrentIngestersState
            Past: TotalStats
            Ingesters: IngesterStatusCollection
        }

        static member Empty =
            {
                RunningSince = DateTimeOffset.Now
                Current = CurrentIngestersState.Empty
                Past = TotalStats.Empty
                Ingesters = Map.empty
            }

    module IngestersState =
        let updateCurrent state =
            let ingesters = state.Ingesters |> Map.toList |> List.map snd
            let idleCount =
                ingesters
                |> List.choose (fun x ->
                    match x.Current.Status with
                    | IngesterState.Idle _ -> Some x
                    | _ -> None)
                |> List.length
            let workingCount =
                ingesters
                |> List.choose (fun x ->
                    match x.Current.Status with
                    | IngesterState.Working _ -> Some x
                    | _ -> None)
                |> List.length
            let queueLength = ingesters |> List.sumBy _.Current.QueueLength
            { state with
                Current =
                    {
                        PendingJobs = workingCount + queueLength
                        IdleIngesters = idleCount
                        WorkingIngesters = workingCount
                        QueueLength = queueLength
                    }
            }

        let updatePast (state: IngestersState) =
            let ingesters = state.Ingesters |> Map.toList |> List.map snd
            let outcomes =
                ingesters
                |> List.fold
                    (fun acc x ->
                        x.Past.Outcomes
                        |> Map.fold
                            (fun acc' key count ->
                                let count' = acc' |> Map.tryFind key |> Option.defaultValue 0
                                acc' |> Map.add key (count + count'))
                            acc)
                    Map.empty
            let past: TotalStats =
                state.Ingesters
                |> Map.toList
                |> List.map snd
                |> List.fold
                    (fun acc ingester ->
                        {
                            TotalStats.Jobs = acc.Jobs + ingester.Past.Jobs
                            BytesSent = acc.BytesSent + ingester.Past.BytesSent
                            ProcessingTime = acc.ProcessingTime + ingester.Past.ProcessingTime
                            Outcomes = outcomes
                        })
                    TotalStats.Empty
            { state with Past = past }

        let updateIngesterStats ingesterId stats state =
            { state with
                Ingesters = state.Ingesters |> Map.add ingesterId stats
            }
            |> updateCurrent
            |> updatePast

    type GetIngesters = | GetIngesters

    let statusMonitor topic (mailbox: Actor<_>) =
        let getStats ingesters ingesterId =
            match ingesters |> Map.tryFind ingesterId with
            | Some stats -> stats
            | None -> IngesterStats.Empty

        let handleIngesterMessage (state: IngestersState) msg =
            match msg with
            | IngesterStatusMessage.Idle ingesterId ->
                let stats = getStats state.Ingesters ingesterId |> IngesterStats.setIdle
                state |> IngestersState.updateIngesterStats ingesterId stats
            | IngesterStatusMessage.Busy ingesterId ->
                let stats = getStats state.Ingesters ingesterId |> IngesterStats.setWorking
                state |> IngestersState.updateIngesterStats ingesterId stats
            | IngesterStatusMessage.Status(ingesterId, status) ->
                let stats = getStats state.Ingesters ingesterId |> IngesterStats.updateStatus topic status
                state |> IngestersState.updateIngesterStats ingesterId stats
            | IngesterStatusMessage.Progress(ingesterId, bytes) ->
                let stats = getStats state.Ingesters ingesterId |> IngesterStats.updateProgress bytes
                state |> IngestersState.updateIngesterStats ingesterId stats
            | IngesterStatusMessage.Final(ingesterId, totalBytes, totalTime) ->
                let stats = getStats state.Ingesters ingesterId |> IngesterStats.updateTotals totalBytes totalTime
                state |> IngestersState.updateIngesterStats ingesterId stats
            | IngesterStatusMessage.QueueLength(ingesterId, queueLength) ->
                let stats = getStats state.Ingesters ingesterId |> IngesterStats.updateQueueLength queueLength
                state |> IngestersState.updateIngesterStats ingesterId stats
            | IngesterStatusMessage.QueueItems(ingesterId, queueItems) ->
                let stats = getStats state.Ingesters ingesterId |> IngesterStats.updateQueueItems queueItems
                state |> IngestersState.updateIngesterStats ingesterId stats

        let mdr = typed (DistributedPubSub.Get(mailbox.System).Mediator)
        mdr <! Subscribe(topic, mailbox.UntypedContext.Self)

        let rec loop (state: IngestersState) =
            actor {
                let! (msg: obj) = mailbox.Receive()
                return!
                    match msg with
                    | :? IngesterStatusMessage as msg ->
                        let state = handleIngesterMessage state msg
                        loop state

                    | :? GetIngesters ->
                        mailbox.Sender() <! state
                        ignored ()

                    | _ -> unhandled ()
            }
        loop IngestersState.Empty

    let getIngesterStatus statusAccumulator : Async<IngestersState> =
        async {
            let! ingesters = statusAccumulator <? GetIngesters
            return ingesters
        }

    let spawnIngesterMonitor system topic =
        spawn system (makeActorName [ topic; "Monitor" ]) <| propsNamed "webapi-ingester-monitor" (statusMonitor topic)
