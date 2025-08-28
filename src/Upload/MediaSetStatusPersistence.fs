namespace Nrk.Oddjob.Upload

module MediaSetStatusPersistence =

    open System
    open System.Collections.Generic
    open Akkling

    open Nrk.Oddjob.Core

    [<CLIMutable>]
    type GetMediaSetStatus =
        {
            MediaSetId: string
            Status: int
            LastActivationTime: DateTime
            LastUpdateTime: DateTime
            RetryCount: int
            Priority: int
        }

    type MediaSetStatusResult =
        | ResultRows of seq<GetMediaSetStatus>
        | ResultCount of int

    type MediaSetStatusData =
        {
            MediaSetId: string
            Status: int
            Timestamp: DateTime
        }

    type StatusUpdateCommand =
        | SetStatusOnActivation of MediaSetStatusData * int
        | SetStatusOnUpdate of MediaSetStatusData
        | SetStatusOnRepair of MediaSetStatusData * int

        member this.Data =
            match this with
            | SetStatusOnActivation(data, _) -> data
            | SetStatusOnUpdate data -> data
            | SetStatusOnRepair(data, _) -> data

    type MediaSetStatusQueryData = { SinceTime: DateTime }

    type TimeInterval =
        | LastActivationTime of DateTime option * DateTime option
        | LastUpdateTime of DateTime option * DateTime option
        | LastActivationAndUpdateTime of DateTime option * DateTime option

    type CountSpec =
        | NoCount
        | MaxCount of int
        | CountOnly

    type OrderSpec =
        | Unspecified
        | ActivationDesc // Recent first, used in "recent" endpoint
        | PriorityDescActivationAsc // High priority first, used by MediaSetMonitor

    type StatusQueryCommand =
        | QueryAll of interval: TimeInterval * count: CountSpec * order: OrderSpec
        | QueryHavingStatus of MediaSetStatus * interval: TimeInterval * count: CountSpec * order: OrderSpec
        | QueryNotHavingStatus of MediaSetStatus * interval: TimeInterval * count: CountSpec * order: OrderSpec

        member this.Interval =
            match this with
            | QueryAll(interval, _, _) -> interval
            | QueryHavingStatus(_, interval, _, _) -> interval
            | QueryNotHavingStatus(_, interval, _, _) -> interval
        member this.CountSpec =
            match this with
            | QueryAll(_, countSpec, _) -> countSpec
            | QueryHavingStatus(_, _, countSpec, _) -> countSpec
            | QueryNotHavingStatus(_, _, countSpec, _) -> countSpec
        member this.OrderSpec =
            match this with
            | QueryAll(_, _, orderSpec) -> orderSpec
            | QueryHavingStatus(_, _, _, orderSpec) -> orderSpec
            | QueryNotHavingStatus(_, _, _, orderSpec) -> orderSpec

    type MediaSetStatusCommand =
        | StatusUpdateCommand of StatusUpdateCommand
        | StatusQueryCommand of StatusQueryCommand

    let mediaSetStatusPersistenceActor akkaConnectionString (mailbox: Actor<_>) =

        let getStatusSql (cmd: StatusQueryCommand) =
            let sql =
                """SELECT """
                + match cmd.CountSpec with
                  | NoCount -> " "
                  | MaxCount maxCount -> sprintf "TOP %d " maxCount
                  | CountOnly -> "COUNT "
                + match cmd.CountSpec with
                  | NoCount
                  | MaxCount _ ->
                      """MediaSetId,
                       Status, 
                       LastActivationTime,
                       LastUpdateTime,
                       RetryCount,
                       Priority """
                  | CountOnly -> """(*) """
                + """FROM Dbo.MediaSetStatuses """
                + match cmd with
                  | QueryAll _ -> """WHERE 1=1 """
                  | QueryHavingStatus _ -> """WHERE Status = @Status """
                  | QueryNotHavingStatus _ -> """WHERE Status <> @Status """
                + match cmd.Interval with
                  | LastActivationTime(from, to') ->
                      (from |> Option.map (fun _ -> "AND LastActivationTime >= @FromLastActivationTimeStamp ") |> Option.defaultValue "")
                      + (to' |> Option.map (fun _ -> "AND LastActivationTime < @ToLastActivationTimeStamp ") |> Option.defaultValue "")
                  | LastUpdateTime(from, to') ->
                      (from |> Option.map (fun _ -> "AND LastUpdateTime >= @FromLastUpdateTimeStamp ") |> Option.defaultValue "")
                      + (to' |> Option.map (fun _ -> "AND LastUpdateTime < @ToLastUpdateTimeStamp ") |> Option.defaultValue "")
                  | LastActivationAndUpdateTime(from, to') ->
                      (from |> Option.map (fun _ -> "AND LastActivationTime >= @FromLastActivationTimeStamp ") |> Option.defaultValue "")
                      + (to' |> Option.map (fun _ -> "AND LastActivationTime < @ToLastActivationTimeStamp ") |> Option.defaultValue "")
                      + (from |> Option.map (fun _ -> "AND LastUpdateTime >= @FromLastUpdateTimeStamp ") |> Option.defaultValue "")
                      + (to' |> Option.map (fun _ -> "AND LastUpdateTime < @ToLastUpdateTimeStamp ") |> Option.defaultValue "")
                + match cmd.CountSpec with
                  | NoCount
                  | MaxCount _ ->
                      match cmd.OrderSpec with
                      | Unspecified -> ""
                      | ActivationDesc -> """ORDER BY LastActivationTime DESC """
                      | PriorityDescActivationAsc -> """ORDER BY Priority DESC, LastActivationTime ASC """
                  | CountOnly -> ""
            let statusParam =
                match cmd with
                | QueryAll _ -> List.empty
                | QueryHavingStatus(status, _, _, _) -> [ ("Status", MediaSetStatus.toInt status |> box) ]
                | QueryNotHavingStatus(status, _, _, _) -> [ ("Status", MediaSetStatus.toInt status |> box) ]
            let timeParams =
                match cmd.Interval with
                | LastActivationTime(from, to') ->
                    (from |> Option.map (fun ts -> [ ("FromLastActivationTimeStamp", box ts) ]) |> Option.defaultValue List.empty)
                    |> List.append (to' |> Option.map (fun ts -> [ ("ToLastActivationTimeStamp", box ts) ]) |> Option.defaultValue List.empty)
                | LastUpdateTime(from, to') ->
                    (from |> Option.map (fun ts -> [ ("FromLastUpdateTimeStamp", box ts) ]) |> Option.defaultValue List.empty)
                    |> List.append (to' |> Option.map (fun ts -> [ ("ToLastUpdateTimeStamp", box ts) ]) |> Option.defaultValue List.empty)
                | LastActivationAndUpdateTime(from, to') ->
                    (from |> Option.map (fun ts -> [ ("FromLastActivationTimeStamp", box ts) ]) |> Option.defaultValue List.empty)
                    |> List.append (to' |> Option.map (fun ts -> [ ("ToLastActivationTimeStamp", box ts) ]) |> Option.defaultValue List.empty)
                    |> List.append (from |> Option.map (fun ts -> [ ("FromLastUpdateTimeStamp", box ts) ]) |> Option.defaultValue List.empty)
                    |> List.append (to' |> Option.map (fun ts -> [ ("ToLastUpdateTimeStamp", box ts) ]) |> Option.defaultValue List.empty)
            sql, [ statusParam; timeParams ] |> List.concat |> Map.ofList

        let getUpsertStatusSql (cmd: StatusUpdateCommand) =
            let sql =
                match cmd with
                | SetStatusOnActivation _ ->
                    """UPDATE Dbo.MediaSetStatuses SET Status=@Status, LastActivationTime=@Timestamp, LastUpdateTime=@Timestamp, RetryCount=0, Priority=@Priority WHERE MediaSetId=@MediaSetId;
                       IF @@ROWCOUNT=0 INSERT INTO Dbo.MediaSetStatuses (MediaSetId, Status, LastActivationTime, LastUpdateTime, RetryCount, Priority) VALUES (@MediaSetId, @Status, @Timestamp, @Timestamp, 0, @Priority)"""
                | SetStatusOnUpdate _ -> """UPDATE Dbo.MediaSetStatuses SET Status=@Status, LastUpdateTime=@Timestamp WHERE MediaSetId=@MediaSetId"""
                | SetStatusOnRepair _ ->
                    """UPDATE Dbo.MediaSetStatuses SET Status=@Status, LastUpdateTime=@Timestamp, RetryCount=RetryCount+1, Priority=@Priority WHERE MediaSetId=@MediaSetId;
                       IF @@ROWCOUNT=0 INSERT INTO Dbo.MediaSetStatuses (MediaSetId, Status, LastActivationTime, LastUpdateTime, RetryCount, Priority) VALUES (@MediaSetId, @Status, @Timestamp, @Timestamp, 0, @Priority)"""
            let param =
                let map =
                    Map
                        [
                            "MediaSetId", box cmd.Data.MediaSetId
                            "Timestamp", box cmd.Data.Timestamp
                            "Status", box cmd.Data.Status
                        ]
                match cmd with
                | SetStatusOnActivation(_, priority) -> Map.add "Priority" (box priority) map
                | SetStatusOnUpdate _ -> map
                | SetStatusOnRepair(_, priority) -> Map.add "Priority" (box priority) map
            sql, param

        let rec loop () =
            actor {
                let! msg = mailbox.Receive()
                use db = new Microsoft.Data.SqlClient.SqlConnection(akkaConnectionString)
                match msg with
                | StatusUpdateCommand cmd ->
                    let sql, param = getUpsertStatusSql cmd
                    db.ExecuteMap(sql, param) |> ignore
                | StatusQueryCommand cmd ->
                    let sql, param = getStatusSql cmd
                    let result =
                        match cmd.CountSpec with
                        | CountOnly ->
                            let result: int = db.QueryMap(sql, param) |> Seq.head
                            async { return ResultCount result }
                        | _ ->
                            let result: IEnumerable<GetMediaSetStatus> = db.QueryMap(sql, param)
                            async { return ResultRows result }
                    mailbox.Sender() <! result
                return! ignored ()
            }
        loop ()
