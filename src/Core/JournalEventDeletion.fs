namespace Nrk.Oddjob.Core

module JournalEventDeletion =

    open System
    open Akka.Actor
    open Akkling

    [<NoEquality; NoComparison>]
    type JournalEventDeletionActorProps =
        {
            AkkaConnectionString: string
            PersistenceId: string
            BatchSize: int
        }

    type JournalEventDeletionCommand = DeleteEvents of int64 list

    type JournalEventDeletionResponse = DeletedEvents of int64 list

    let journalEventDeletionActor (aprops: JournalEventDeletionActorProps) (mailbox: Actor<_>) =

        let getDeleteSql (events: int64 list) =
            let sequenceNumbers = String.Join(',', events)
            let sql =
                """DELETE FROM Dbo.EventJournal
                   WHERE PersistenceId=@PersistenceId AND SequenceNr IN (SELECT CONVERT(bigint, value) FROM STRING_SPLIT(@SequenceNumbers, ','))"""
            let param =
                Map
                    [
                        "PersistenceId", box aprops.PersistenceId
                        "SequenceNumbers", box sequenceNumbers
                    ]
            sql, param

        let deleteEvents (db: Microsoft.Data.SqlClient.SqlConnection) events =
            let sql, param = getDeleteSql events
            let _rowsAffected = db.ExecuteMap(sql, param)
            ()

        let rec idle () =
            logDebug mailbox "idle"
            actor {
                let! (message: obj) = mailbox.Receive()
                let db = new Microsoft.Data.SqlClient.SqlConnection(aprops.AkkaConnectionString)
                return!
                    match message with
                    | :? JournalEventDeletionCommand as cmd ->
                        match cmd with
                        | DeleteEvents events ->
                            let sender = mailbox.Sender()
                            processing db sender events
                    | _ -> ignored ()
            }
        and processing db sender remainingEvents =
            logDebug mailbox $"processing {remainingEvents.Length} events"
            match remainingEvents with
            | [] ->
                db.Dispose()
                terminating ()
                ignored ()
            | _ ->
                let eventsToDelete, remainingEvents =
                    if remainingEvents.Length > aprops.BatchSize then
                        remainingEvents |> List.splitAt aprops.BatchSize
                    else
                        remainingEvents, []
                deleteEvents db eventsToDelete
                sender <! DeletedEvents eventsToDelete
                processing db sender remainingEvents
        and terminating () =
            (retype mailbox.Self) <! PoisonPill.Instance

        idle ()
