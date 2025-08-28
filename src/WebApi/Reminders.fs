namespace Nrk.Oddjob.WebApi

open System
open System.Diagnostics.Metrics
open System.Threading
open System.Threading.Tasks
open Giraffe
open Microsoft.AspNetCore.Http

open Microsoft.Extensions.Options
open Microsoft.Extensions.Hosting
open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Config
open WebAppUtils

module ReminderGroups =

    [<CLIMutable; NoComparison>]
    type ReminderTriggerDto =
        {
            Trigger_Name: string
            Start_Time: Nullable<int64>
            End_Time: Nullable<int64>
            Repeat_Count: int
            Repeat_Interval: int
            Times_Triggered: int
            Next_Fire_Time: Nullable<int64>
            Prev_Fire_Time: Nullable<int64>
            Trigger_State: string
        }

    let getReminderCountSql groupName =
        let sql =
            """SELECT COUNT(*) FROM Dbo.QRTZ_SIMPLE_TRIGGERS t 
                WHERE t.TRIGGER_GROUP=@GroupName"""
        let param = Map [ "GroupName", groupName ]
        sql, param

    let getReminderItemsSql groupName orderBy =
        let orderBy =
            if orderBy = "triggerCount" then
                "st.TIMES_TRIGGERED DESC"
            else
                "t.NEXT_FIRE_TIME"
        let sql =
            """SELECT st.TRIGGER_NAME, t.START_TIME, t.END_TIME, st.REPEAT_COUNT, st.REPEAT_INTERVAL, st.TIMES_TRIGGERED, t.NEXT_FIRE_TIME, t.PREV_FIRE_TIME, t.TRIGGER_STATE 
                FROM Dbo.QRTZ_SIMPLE_TRIGGERS st
                JOIN Dbo.QRTZ_TRIGGERS t 
                ON st.TRIGGER_NAME=t.TRIGGER_NAME and st.TRIGGER_GROUP=t.TRIGGER_GROUP
                WHERE t.TRIGGER_GROUP=@GroupName
                ORDER BY """
            + orderBy
        let param = Map [ "GroupName", groupName ]
        sql, param

    let getGroupResults akkaConnectionString groupName takeCount skipCount orderBy countOnly =
        use db = new Microsoft.Data.SqlClient.SqlConnection(akkaConnectionString)
        db.Open()
        if countOnly then
            let sql, param = getReminderCountSql groupName
            db.QueryMap(sql, param) |> Seq.map ReminderGroupResult.Count
        else
            let sql, param = getReminderItemsSql groupName orderBy
            db.QueryMap(sql, param)
            |> Seq.map (fun (x: ReminderTriggerDto) ->
                {
                    Name = x.Trigger_Name
                    StartTimeUtc = Option.ofNullable x.Start_Time |> Option.map DateTime
                    EndTimeUtc = Option.ofNullable x.End_Time |> Option.map DateTime
                    RepeatCount = x.Repeat_Count
                    RepeatInterval = x.Repeat_Interval
                    TimesTriggered = x.Times_Triggered
                    NextFireTimeUtc = Option.ofNullable x.Next_Fire_Time |> Option.map DateTime
                    PrevFireTimeUtc = Option.ofNullable x.Prev_Fire_Time |> Option.map DateTime
                    State = x.Trigger_State
                }
                |> ReminderGroupResult.Trigger)
            |> Seq.skip skipCount
            |> Seq.truncate takeCount
        |> Seq.toList

    let allGroups =
        [|
            Reminders.ClearMediaSet
            Reminders.StorageCleanup
            Reminders.PotionBump
            Reminders.PlayabilityBump
        |]

    let getReminderGroups akkaConnectionString groups takeCount skipCount orderBy countOnly : Async<seq<ReminderGroup>> =
        let selectedGroups =
            if Seq.isEmpty groups then
                allGroups
            else
                allGroups |> Array.filter (fun x -> groups |> List.map String.toLower |> List.contains (String.toLower x))
            |> Seq.map (fun groupName -> (groupName, getGroupResults akkaConnectionString groupName takeCount skipCount orderBy countOnly))
        let groupResults = selectedGroups |> Seq.map snd |> Seq.toArray
        let results =
            Seq.zip selectedGroups groupResults
            |> Seq.map (fun (group, result) ->
                {
                    GroupName = fst group
                    Items = result |> Seq.toList
                })
        async { return results }

    let tryParseGroupFilter s =
        if String.IsNullOrEmpty s then
            false, None
        else
            let groupFilter =
                s
                |> String.split ','
                |> Array.map String.toLower
                |> Array.map (fun x ->
                    match allGroups |> Array.tryFind (fun y -> x = String.toLower y) with
                    | Some y -> [| String.toLower y |]
                    | None -> [||])
                |> Array.concat
                |> Array.distinct
                |> Array.toList
            true, Some groupFilter

    let toResponseWithRemindersFilterTask<'T> akkaConnectionString =
        fun _next (ctx: HttpContext) ->
            task {
                let groups =
                    parseQueryParam (ctx.Request.Query.Item "include") (TryParser.tryParseWith tryParseGroupFilter) None
                    |> Option.defaultValue List.empty
                let takeLast = parseQueryParam (ctx.Request.Query.Item "take") TryParser.parseInt 10
                let skipLast = parseQueryParam (ctx.Request.Query.Item "skip") TryParser.parseInt 0
                let orderBy = parseQueryParam (ctx.Request.Query.Item "orderBy") Some "triggerTime"
                let countOnly = parseQueryParam (ctx.Request.Query.Item "countOnly") TryParser.parseBool false

                let! result = getReminderGroups akkaConnectionString groups takeLast skipLast orderBy countOnly

                ctx.SetHttpHeader(AccessControlHeader, "*")
                return! ctx.NegotiateWithAsync(rules, unacceptableHandler, result)
            }

    type ReminderMetrics(meterFactory: IMeterFactory, connectionStrings: IOptionsSnapshot<ConnectionStrings>) =
        let meter = meterFactory.Create("ReminderMeter")
        let akkaConnectionString = connectionStrings.Value.Akka

        let getReminderCount reminderName =
            let takeLast = 0
            let skipLast = 0
            let orderBy = "triggerTime"
            let countOnly = true

            getGroupResults akkaConnectionString reminderName takeLast skipLast orderBy countOnly
            |> List.choose (function
                | Count n -> Some n
                | _ -> None)
            |> List.tryHead
            |> Option.defaultValue 0

        interface IHostedService with
            member this.StartAsync(cancellationToken: CancellationToken) : Task =
                allGroups
                |> Array.iter (fun reminderName ->
                    meter.CreateObservableGauge<int>(
                        $"reminder-{reminderName}-count",
                        Func<int>(fun () -> getReminderCount reminderName),
                        "{reminder}",
                        $"The number of pending reminders for {reminderName}"
                    )
                    |> ignore)

                Task.CompletedTask

            member this.StopAsync(cancellationToken: CancellationToken) : Task = Task.CompletedTask
