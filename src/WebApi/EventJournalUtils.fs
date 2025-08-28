namespace Nrk.Oddjob.WebApi

module EventJournalUtils =

    open System
    open Akka.Streams
    open Akka.Persistence.Query
    open Akkling

    open Nrk.Oddjob
    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Potion.PotionTypes
    open Nrk.Oddjob.Upload.UploadTypes

    [<Literal>]
    let PersistenceQueryJournalPluginId = "akka.persistence.query.journal.sql"

    [<Literal>]
    let MaxResultCount = 100

    [<NoEquality; NoComparison>]
    type JournalEvent =
        {
            PersistenceId: string
            SequenceNr: int64
            Operation: string
            State: obj
        }

    [<NoEquality; NoComparison>]
    type MediaSetPersistenceState =
        {
            PersistenceId: string
            State: Dto.MediaSet.MediaSetState
            RepairActions: Dto.MediaSet.RemainingActions option
        }

    [<NoEquality; NoComparison>]
    type PersistenceJournalState = { PersistenceId: string; State: obj }

    let private normalizePersistenceOrMediaSetId (str: string) =
        let items = str.ToLower().Replace("/", "~").Split('~')
        if items.Length > 1 then
            let items = Array.concat [ items[0..0]; items[1 .. items.Length - 2]; [| items[items.Length - 1] |] ]
            String.Join("~", items).ToLower()
        else
            items[0]

    type NormalizedPersistenceId =
        private
        | NormalizedPersistenceId of string

        override this.ToString() =
            let (NormalizedPersistenceId str) = this in str

    [<RequireQualifiedAccess>]
    module NormalizedPersistenceId =
        let replaceNationalCharacters (text: string) =
            text.Replace("æ", "ae").Replace("ø", "oe").Replace("å", "aa").Replace("Æ", "AE").Replace("Ø", "OE").Replace("Å", "AA")

        let create str =
            if String.isNullOrEmpty str then
                invalidArg "str" "PersistenceId cannot be empty"
            else
                Uri.UnescapeDataString(str) |> replaceNationalCharacters |> normalizePersistenceOrMediaSetId
            |> NormalizedPersistenceId

        let value (NormalizedPersistenceId str) = str

    module NormalizedMediaSetId =
        let create str =
            if String.isNullOrEmpty str then
                invalidArg "str" "MediaSetId cannot be empty"
            else
                Uri.UnescapeDataString(str) |> normalizePersistenceOrMediaSetId

    type IStateReader =
        abstract member MediaSetState: entityId: string -> Async<MediaSetState>
        abstract member PotionSetState: entityId: string -> Async<PotionSetState>

    module Queries =

        let mapJournalEvent (eventType: string) (evt: EventEnvelope) =
            let operation, state =
                // MediaSet
                if Core.Dto.Serialization.ProtobufSerializer.SupportedTypes |> List.contains (evt.Event.GetType()) then
                    evt.Event.GetType().Name, evt |> box
                // MediaSet (deprecated)
                else if evt.Event :? Core.Dto.MediaSet.DeprecatedEvent then
                    evt.Event.GetType().Name, null
                // Potion
                else if Potion.PotionPersistence.ProtobufSerializer.SupportedTypes |> List.contains (evt.Event.GetType()) then
                    evt.Event.GetType().Name, evt |> box
                else
                    String.Empty, evt
            {
                PersistenceId = evt.PersistenceId.Substring(eventType.Length + 1)
                SequenceNr = evt.SequenceNr
                Operation = operation
                State = state
            }

        let getJournalEventsById system (materializer: ActorMaterializer) eventType eventId fromSequenceNr toSequenceNr =
            let journal =
                PersistenceQuery.Get(system).ReadJournalFor<Akka.Persistence.Sql.Query.SqlReadJournal>(PersistenceQueryJournalPluginId)
            let addEvent (acc: seq<JournalEvent>) (evt: EventEnvelope) =
                acc |> Seq.append [ mapJournalEvent eventType evt ]
            let qualifiedPersistenceId = sprintf "%s:%s" eventType eventId
            let normalizedPersistenceId = qualifiedPersistenceId |> NormalizedPersistenceId.create
            journal
                .CurrentEventsByPersistenceId(
                    NormalizedPersistenceId.value normalizedPersistenceId,
                    int64 fromSequenceNr,
                    if int64 toSequenceNr > 0L then
                        int64 toSequenceNr
                    else
                        Int64.MaxValue
                )
                .RunAggregate(Seq.empty, System.Func<seq<JournalEvent>, EventEnvelope, seq<JournalEvent>> addEvent, materializer)
            |> Async.AwaitTask

    module Sharding =

        open Nrk.Oddjob.Core.ShardMessages
        open Nrk.Oddjob.Core.Config

        type UploadReader(system, oddjobConfig) =
            let uploadMediator = UploadShardExtractor(oddjobConfig.Upload.NumberOfShards) |> ClusterShards.getUploadMediator system

            member _.GetPersistentState entityId =
                match MediaSetId.tryParse entityId with
                | Some mediaSetId ->
                    async {
                        let! (state: Dto.MediaSet.MediaSetState) = uploadMediator <? Message.create (MediaSetShardMessage.GetMediaSetState mediaSetId)
                        return state.ToDomain()
                    }
                | _ -> Async.fromResult MediaSetState.Zero

        type PotionSetReader(system, oddjobConfig) =
            let potionMediator = PotionShardExtractor(oddjobConfig.Potion.NumberOfShards) |> ClusterShards.getPotionMediator system

            member _.GetPersistentState entityId =
                async {
                    let normalizedPersistenceId = NormalizedPersistenceId.create entityId
                    let (NormalizedPersistenceId persistentId) = normalizedPersistenceId
                    let! (state: Potion.PotionPersistence.PotionSetState) = potionMediator <? GetPotionSetState persistentId
                    return state.ToDomain()
                }

        let createStateReader (system: Akka.Actor.ActorSystem) (oddjobConfig: OddjobConfig) =

            let uploadReader = UploadReader(system, oddjobConfig)
            let potionSetReader = PotionSetReader(system, oddjobConfig)

            { new IStateReader with
                member _.MediaSetState entityId =
                    uploadReader.GetPersistentState entityId

                member _.PotionSetState entityId =
                    potionSetReader.GetPersistentState entityId
            }

    let getUploadJournalState (reader: IStateReader) entityId getRepairActions =
        let persistenceId = NormalizedPersistenceId.create entityId
        let state = reader.MediaSetState entityId
        state
        |> Async.map (fun state ->
            {
                PersistenceId = NormalizedPersistenceId.value persistenceId
                State = Dto.MediaSet.MediaSetState.FromDomain state
                RepairActions = Some(getRepairActions state (MediaSetId.parse entityId))
            })

    let getPotionSetJournalState (reader: IStateReader) entityId =
        let persistenceId = NormalizedPersistenceId.create entityId
        let state = reader.PotionSetState entityId
        state
        |> Async.map (fun state ->
            {
                PersistenceId = NormalizedPersistenceId.value persistenceId
                State = Potion.PotionPersistence.PotionSetState.FromDomain state
            })

    let getFullEventId eventType eventId connectionString =
        if eventType = "potion" && String.length eventId = 8 then // Special treatment for partially specified Potion group Ids
            let commandText =
                $"SELECT TOP 1 PersistenceID FROM [dbo].[EventJournal] WHERE PersistenceID LIKE '%s{eventType}:%s{eventId}%%' ORDER BY Ordering DESC"
            use connection = new Microsoft.Data.SqlClient.SqlConnection(connectionString)
            connection.Open()
            let command = connection.CreateCommand()
            command.CommandText <- commandText
            let result = command.ExecuteScalar()
            if result = null then
                eventId
            else
                (result :?> string) |> String.split ':' |> Seq.last
        else
            eventId
