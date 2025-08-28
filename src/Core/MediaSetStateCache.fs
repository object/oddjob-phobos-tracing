namespace Nrk.Oddjob.Core

module MediaSetStateCache =

    open System
    open Akkling

    open Nrk.Oddjob.Core

    type MediaSetStateData =
        {
            MediaSetId: string
            State: Dto.MediaSet.MediaSetState
            Timestamp: DateTime
        }

    type MediaSetStateCacheCommand =
        | SaveState of MediaSetStateData
        | GetState of mediaSetId: string

    [<CLIMutable>]
    type MediaSetStateResult =
        {
            State: byte[]
            Timestamp: DateTimeOffset
            SequenceNr: int64
        }

    let mediaSetStateCacheActor akkaConnectionString (mailbox: Actor<_>) =

        let getStateSql mediaSetId persistenceId =
            let sql =
                """SELECT TOP 1 ms.State, ms.Timestamp, ej.SequenceNr FROM Dbo.MediaSetStates ms
                         JOIN Dbo.EventJournal ej ON PersistenceID=@PersistenceId
                         WHERE ms.MediaSetId=@MediaSetId
                         ORDER BY ej.SequenceNr DESC"""
            let param = Map [ "MediaSetId", mediaSetId; "PersistenceId", persistenceId ]
            sql, param

        let getUpsertStateSql (cmd: MediaSetStateData) persistenceId =
            let sql =
                """SELECT TOP 1 SequenceNr FROM Dbo.EventJournal WHERE PersistenceID=@PersistenceId AND SequenceNr>@SequenceNr;
                   IF @@ROWCOUNT=0 BEGIN
                         UPDATE Dbo.MediaSetStates SET State=@State, Timestamp=@Timestamp WHERE MediaSetId=@MediaSetId;
                         IF @@ROWCOUNT=0 INSERT INTO Dbo.MediaSetStates (MediaSetId, State, Timestamp) VALUES (@MediaSetId, @State, @Timestamp);
                   END"""
            let state = Dto.Serialization.ProtobufSerializer(mailbox.System :?> Akka.Actor.ExtendedActorSystem).ToBinary cmd.State
            let param =
                Map
                    [
                        "MediaSetId", box cmd.MediaSetId
                        "PersistenceId", box persistenceId
                        "Timestamp", box cmd.Timestamp
                        "State", box state
                        "SequenceNr", box cmd.State.LastSequenceNr
                    ]
            sql, param

        let rec loop () =
            actor {
                let! msg = mailbox.Receive()
                use db = new Microsoft.Data.SqlClient.SqlConnection(akkaConnectionString)
                match msg with
                | SaveState cmd ->
                    try
                        let persistenceId = $"msc:{normalizeActorNameSegment cmd.MediaSetId}"
                        let sql, param = getUpsertStateSql cmd persistenceId
                        let message = $"cache update for {cmd.MediaSetId} (SequenceNr = {cmd.State.LastSequenceNr}, Timestamp = {cmd.Timestamp})"
                        let result =
                            if db.ExecuteMap(sql, param) > 0 then
                                "Accepted"
                            else
                                "Rejected"
                        logDebug mailbox $"{result} {message}"
                    with exn ->
                        logErrorWithExn mailbox exn $"Failed to update state cache for {cmd.MediaSetId}"
                | GetState mediaSetId ->
                    try
                        let persistenceId = $"msc:{normalizeActorNameSegment mediaSetId}"
                        let sql, param = getStateSql mediaSetId persistenceId
                        let ser = Dto.Serialization.ProtobufSerializer(mailbox.System :?> Akka.Actor.ExtendedActorSystem)
                        let result =
                            db.QueryMap<string, MediaSetStateResult>(sql, param)
                            |> Seq.tryHead
                            |> Option.map (fun result ->
                                let state = ser.FromBinary(result.State, typedefof<Dto.MediaSet.MediaSetState>.Name) :?> Dto.MediaSet.MediaSetState
                                if state.LastSequenceNr < result.SequenceNr then
                                    { state with LastSequenceNr = 0L }
                                else
                                    state)
                            |> Option.defaultValue (Dto.MediaSet.MediaSetState.FromDomain MediaSetState.Zero)
                        mailbox.Sender() <! result
                    with exn ->
                        logErrorWithExn mailbox exn $"Failed to retrieve state cache for {mediaSetId}"
                        mailbox.Sender() <! Dto.MediaSet.MediaSetState.FromDomain MediaSetState.Zero
                return! ignored ()
            }
        loop ()
