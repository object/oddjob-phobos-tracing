namespace Nrk.Oddjob.Upload

module MediaSetMonitor =

    open Akkling
    open System

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.ShardMessages

    open UploadTypes
    open MediaSetStatusPersistence

    let updatedStatus retryCount maxRetries =
        if retryCount < maxRetries then
            MediaSetStatus.Pending
        else
            MediaSetStatus.Expired

    let updatedData status mediaSetId =
        SetStatusOnUpdate
            {
                MediaSetId = mediaSetId
                Status = MediaSetStatus.toInt status
                Timestamp = DateTime.Now
            }

    module Actors =

        type MonitorMessage = CheckMediaSetStatuses

        let monitor akkaConnectionString (config: Config.OddjobConfig) (mailbox: Actor<MonitorMessage>) =

            let queryRate = TimeSpan.FromMinutes(float config.Limits.MediaSetMonitorQueriesPerMinute)
            let repairRateInMs = (60 * 1000 |> float) / (config.Limits.MediaSetMonitorRepairsPerMinute |> float) |> int
            let recentWindow = TimeSpan.FromMinutes(float config.Limits.MediaSetMonitorRecentWindowInMinutes)
            let maxRetries = config.Limits.MediaSetMonitorMaxRetries
            let minPriority = config.Upload.MinimumMessagePriority

            let mediaSetController =
                UploadShardExtractor(config.Upload.NumberOfShards) |> ClusterShards.getUploadMediator mailbox.System
            let statusPersistenceActor =
                spawn
                    mailbox
                    (makeActorName [ "MediaSet State Persistence" ])
                    (propsNamed "upload-mediaset-status-persistence" <| mediaSetStatusPersistenceActor akkaConnectionString)

            let queryPersistence =
                fun cmd ->
                    statusPersistenceActor <? StatusQueryCommand cmd
                    |> Async.RunSynchronously // Wait for answer
                    |> Async.RunSynchronously // Run DB query
            let updatePersistence = fun cmd -> statusPersistenceActor <! StatusUpdateCommand cmd

            let repair id priority =
                match MediaSetId.tryParse id with
                | Some mediaSetId ->
                    logDebug mailbox $"Repairing media set %s{mediaSetId.ContentId.Value}"
                    mediaSetController <! Message.create (MediaSetShardMessage.RepairMediaSet(mediaSetId, priority, "MediaSetMonitor"))
                | None -> logError mailbox $"Invalid media set ID %s{id}"

            let processStatuses statuses =
                match statuses with
                | ResultRows statuses ->
                    statuses
                    |> Seq.iter (fun status ->
                        let newStatus = updatedStatus status.RetryCount maxRetries
                        let newData = updatedData newStatus status.MediaSetId
                        updatePersistence newData
                        if newStatus = MediaSetStatus.Pending && status.Priority >= minPriority then
                            repair status.MediaSetId status.Priority
                            Async.Sleep repairRateInMs |> Async.RunSynchronously // Prevent flooding MSC with repair messages
                    )
                | ResultCount _ -> ()

            let rec loop () =
                actor {
                    let! msg = mailbox.Receive()
                    return!
                        match msg with
                        | CheckMediaSetStatuses ->
                            let interval = LastActivationAndUpdateTime(None, DateTime.Now - recentWindow |> Some)
                            QueryHavingStatus(MediaSetStatus.Pending, interval, NoCount, PriorityDescActivationAsc)
                            |> queryPersistence
                            |> processStatuses
                            ignored ()
                }
            mailbox.ScheduleRepeatedly TimeSpan.Zero queryRate mailbox.Self CheckMediaSetStatuses |> ignore
            loop ()
