namespace Nrk.Oddjob.Core

module SchedulerActors =

    open System
    open Akkling

    open Nrk.Oddjob.Core.Queues

    let triggerInterval = TimeSpan.FromMinutes 10.
    let triggerInitialDelay = TimeSpan.FromSeconds 10.
    let batchSize = 100

    type PublishContext =
        {
            Ids: int64 array
            References: string list
        }

    type TriggerScheduledMessagesCommand() = class end

    type UpdateDbResult = Result<PublishContext, exn * PublishContext>

    [<CLIMutable>]
    type GetScheduledMessages =
        {
            ScheduledMessageId: int64
            MessageReference: string
            RecipientCategory: string
            RecipientPath: string
            Message: string
            MessageVersion: int
            TriggerTime: DateTime
            Triggered: bool
            Cancelled: bool
        }

    /// Queries the DB for scheduled messages periodically and sends them to correct actors
    /// N.B. scheduled messages always have priority of 1, because we want them to be distinguished
    /// from default priority, but at the same time they don't seem that important.
    let triggerScheduledQueueMessagesActor akkaConnectionString (exchanges: IExchangesApi) (mailbox: Actor<_>) =

        let rec publishMessages messages targetPublisher =
            match messages with
            | [] -> ()
            | _ ->
                let batch, messages =
                    if List.length messages <= batchSize then
                        messages, []
                    else
                        List.truncate batchSize messages, List.skip batchSize messages
                if List.isNotEmpty batch then
                    logDebug mailbox $"Publishing batch of %d{List.length batch} scheduled messages"
                    let ids, references, messages = batch |> List.unzip3
                    let ctx =
                        {
                            Ids = ids |> Array.ofList
                            References = references
                        }
                    targetPublisher <! PublishMany(messages, ctx, retype mailbox.Self)
                publishMessages messages targetPublisher

        let rec fillCache (exchangesCache: Map<ExchangeCategory, IActorRef<QueuePublisherCommand>>) exchangeCategories =
            match exchangeCategories with
            | [] -> exchangesCache
            | hd :: tl ->
                if not (exchangesCache.ContainsKey hd) then
                    let publisher = exchanges.GetPublisher(mailbox.System, hd)
                    fillCache (exchangesCache.Add(hd, publisher)) tl
                else
                    fillCache exchangesCache tl

        mailbox.System.Scheduler.ScheduleTellRepeatedly(triggerInitialDelay, triggerInterval, mailbox.Self, box <| TriggerScheduledMessagesCommand())

        let rec loop (exchangesCache: Map<ExchangeCategory, IActorRef<QueuePublisherCommand>>, db, lastTriggered) =

            let scheduledMessagesSql =
                """SELECT
                     ScheduledMessageId,
                     MessageReference, 
                     RecipientCategory,
                     RecipientPath,
                     Message,
                     MessageVersion,
                     TriggerTime,
                     Triggered,
                     Cancelled 
                   FROM Dbo.ScheduledMessages WHERE TriggerTime<=CURRENT_TIMESTAMP AND Triggered=0 AND Cancelled=0"""

            let getUpdateMessagesSql (ids: int64 array) =
                let sql = """UPDATE Dbo.ScheduledMessages SET Triggered = 1 WHERE ScheduledMessageId IN @Ids"""
                let param = Map [ "Ids", ids ]
                sql, param

            actor {
                let! msg = mailbox.Receive()
                return!
                    match msg with
                    | :? TriggerScheduledMessagesCommand ->
                        logDebug mailbox $"Triggered {triggerInterval.TotalSeconds} seconds after last execution"
                        let now = DateTimeOffset.UtcNow
                        if now - lastTriggered < triggerInterval then
                            logDebug mailbox "Skipped triggering scheduled messages"
                            ignored ()
                        else
                            let lastTriggered = now
                            let db = db |> Option.defaultWith (fun () -> new Microsoft.Data.SqlClient.SqlConnection(akkaConnectionString))
                            let messages = db.QueryMap<DateTime, GetScheduledMessages>(scheduledMessagesSql, Map.empty) |> Seq.toList
                            let grouped = messages |> List.groupBy (_.RecipientCategory)

                            let exchangeCategories = grouped |> List.map fst |> List.choose ExchangeCategory.TryFromString

                            let exchangesCache = fillCache exchangesCache exchangeCategories

                            for exchange, rows in grouped do
                                let parsed = ExchangeCategory.TryFromString exchange
                                match parsed with
                                | Some exchange ->
                                    let publisher =
                                        exchangesCache.TryFind exchange |> Option.getOrFail "At this point it should already be inserted by fillCache"
                                    let messages =
                                        rows |> List.map (fun row -> row.ScheduledMessageId, row.MessageReference, OutgoingMessage.create row.Message)
                                    if List.isNotEmpty messages then
                                        publishMessages messages publisher
                                    else
                                        logInfo
                                            mailbox
                                            $"Triggering scheduled messages for [%A{exchange}] skipped because there are no messages to be scheduled"
                                | None -> logError mailbox $"Could not parse recipient category [%s{exchange}]"
                            loop (exchangesCache, Some db, lastTriggered)
                    | :? QueuePublishResult as publishResult ->
                        match publishResult with
                        | Ok ctx ->
                            let db = db |> Option.defaultWith (fun () -> new Microsoft.Data.SqlClient.SqlConnection(akkaConnectionString))
                            let ctx = ctx :?> PublishContext
                            logDebug mailbox $"Updating %d{Array.length ctx.Ids} published scheduled messages"
                            let sql, param = getUpdateMessagesSql ctx.Ids
                            db.ExecuteMapAsync(sql, param)
                            |> Async.Ignore
                            |> Async.pipeTo (retype mailbox.Self) (Result.map (fun () -> ctx) >> Result.mapError (fun exn -> exn, ctx))
                            loop (exchangesCache, Some db, lastTriggered)
                        | Error(exn, ctx) ->
                            let ctx = ctx :?> PublishContext
                            logErrorWithExn
                                mailbox
                                exn
                                $"Triggering scheduled messages with IDs %A{ctx.Ids} (refs %A{ctx.References}) failed - error during publishing to queue"
                            db |> Option.iter (_.Dispose())
                            loop (exchangesCache, None, lastTriggered)
                    | :? UpdateDbResult as updateResult ->
                        match updateResult with
                        | Result.Ok ctx -> logInfo mailbox $"Triggering scheduled messages with IDs %A{ctx.Ids} (refs %A{ctx.References}) succeeded"
                        | Result.Error(exn, ctx) ->
                            logErrorWithExn
                                mailbox
                                exn
                                $"Triggering scheduled messages with IDs %A{ctx.Ids} (refs %A{ctx.References}) failed - error during updating database"
                        db |> Option.iter (_.Dispose())
                        loop (exchangesCache, None, lastTriggered)
                    | _ -> unhandled ()
            }
        loop (Map.empty, None, DateTimeOffset.MinValue)

    [<NoComparison>]
    type ScheduleMessage<'Msg> =
        {
            Message: 'Msg
            Id: string
            TriggerDateTime: DateTime
            Ack: MessageAck option
        }

    [<NoComparison>]
    type ScheduleRmqMessageCommand =
        {
            RmqMessage: obj
            /// When another message is scheduled with the same id, previous messages with this id will be cancelled
            Id: string
            Version: int
            Ack: MessageAck option
            TargetCategory: ExchangeCategory
            TriggerDateTime: DateTime
        }

    [<NoComparison>]
    type ScheduleMessageResult =
        {
            Command: ScheduleRmqMessageCommand
            Result: Result<Unit, exn>
        }

    /// Schedules message to be triggered in the future. New message cancels all previous messages with the same id.
    let scheduleQueueMessagesActor akkaConnectionString (mailbox: Actor<_>) =

        let getScheduledMessagesSql (cmd: ScheduleRmqMessageCommand) =
            let sql =
                """UPDATE Dbo.ScheduledMessages SET Cancelled=1 WHERE MessageReference=@MessageReference AND Triggered=0 AND Cancelled=0;
                         INSERT INTO Dbo.ScheduledMessages (Cancelled, Message, MessageReference, MessageVersion, RecipientCategory, RecipientPath, TriggerTime, Triggered)
                         VALUES (@Cancelled, @Message, @MessageReference, @MessageVersion, @RecipientCategory, @RecipientPath, @TriggerTime, @Triggered)"""
            let param =
                Map
                    [
                        "Cancelled", box 0
                        "Message", box <| Serialization.Newtonsoft.serializeObject SerializationSettings.PascalCase cmd.RmqMessage
                        "MessageReference", box cmd.Id
                        "MessageVersion", box cmd.Version
                        "RecipientCategory", box <| cmd.TargetCategory.ToString()
                        "RecipientPath", box "NOT IN USE"
                        "TriggerTime", box cmd.TriggerDateTime
                        "Triggered", box 0
                    ]
            sql, param

        let rec loop initiating =
            if not initiating then
                mailbox.UnstashAll()
            actor {
                let! (msg: obj) = mailbox.Receive()
                return!
                    match msg with
                    | :? ScheduleRmqMessageCommand as cmd ->
                        logDebug mailbox $"Requested scheduling message [%A{cmd}]"
                        let db = new Microsoft.Data.SqlClient.SqlConnection(akkaConnectionString)
                        let sql, param = getScheduledMessagesSql cmd
                        db.ExecuteMapAsync(sql, param)
                        |> Async.Ignore
                        |> Async.pipeTo (retype mailbox.Self) (fun result -> { Command = cmd; Result = result })
                        awaitingResult db
                    | _ -> unhandled ()
            }
        and awaitingResult db =
            actor {
                let! (msg: obj) = mailbox.Receive()
                return!
                    match msg with
                    | :? ScheduleRmqMessageCommand ->
                        mailbox.Stash()
                        ignored ()
                    | :? ScheduleMessageResult as result ->
                        let cmd = result.Command
                        match result.Result with
                        | Result.Ok() ->
                            logInfo mailbox $"Scheduled message [{cmd}]"
                            sendAck mailbox cmd.Ack $"Scheduled message for %s{cmd.Id} on {cmd.TriggerDateTime}"
                        | Result.Error exn ->
                            logErrorWithExn mailbox exn $"Could not schedule message for %s{cmd.Id}"
                            sendNack mailbox cmd.Ack $"Could not schedule message for %s{cmd.Id}"
                        db.Dispose()
                        loop false
                    | _ -> unhandled ()
            }
        loop true
