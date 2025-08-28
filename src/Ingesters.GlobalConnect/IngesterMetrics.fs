namespace Nrk.Oddjob.Ingesters.GlobalConnect

module IngesterMetrics =

    open System
    open Akka.Actor
    open Akka.Cluster.Tools.PublishSubscribe
    open Akkling

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.S3.S3Types
    open Nrk.Oddjob.Core.PubSub

    [<RequireQualifiedAccess>]
    type IngesterStatusMessage =
        | Idle of ingesterId: string
        | Busy of ingesterId: string
        | Status of ingesterId: string * status: int
        | Progress of ingesterId: string * bytesWritten: uint64
        | Final of ingesterId: string * totalBytes: uint64 * totalTime: TimeSpan
        | QueueLength of ingesterId: string * queueLength: int
        | QueueItems of ingesterId: string * items: string array

        interface Phobos.Actor.Common.INeverTrace

    type IIngesterInstrumentation =
        abstract member WriteIdle: ingesterId: string -> unit
        abstract member WriteBusy: ingesterId: string -> unit
        abstract member WriteStatus: ingesterId: string -> status: int -> unit
        abstract member WriteProgress: ingesterId: string -> bytes: uint64 -> unit
        abstract member WriteFinal: ingesterId: string -> bytes: uint64 -> elapsed: TimeSpan -> unit
        abstract member WriteQueueLength: ingesterId: string -> queueLength: int -> unit
        abstract member WriteQueueItems: ingesterId: string -> queueItems: string array -> unit

    type NullIngesterInstrumentation() =
        interface IIngesterInstrumentation with
            member this.WriteIdle _ = ()
            member this.WriteBusy _ = ()
            member this.WriteStatus _ _ = ()
            member this.WriteProgress _ _ = ()
            member this.WriteFinal _ _ _ = ()
            member this.WriteQueueLength _ _ = ()
            member this.WriteQueueItems _ _ = ()

    type IPublishCommandStatus =
        abstract member CommandInitiated: ingesterId: string -> S3MessageContext -> unit
        abstract member CommandCompleted: ingesterId: string -> S3MessageContext -> unit
        abstract member CommandFailed: ingesterId: string -> S3MessageContext -> string -> unit
        abstract member CommandRejected: ingesterId: string -> S3MessageContext -> string -> unit

    type NullCommandStatus() =
        interface IPublishCommandStatus with
            member this.CommandInitiated _ _ = ()
            member this.CommandCompleted _ _ = ()
            member this.CommandFailed _ _ _ = ()
            member this.CommandRejected _ _ _ = ()

    type IPublishQueueContentChange =
        abstract member ItemAdded: item: S3MessageContext -> unit
        abstract member ItemRemoved: item: S3MessageContext -> unit

    type NullQueueContentChange() =
        interface IPublishQueueContentChange with
            member this.ItemAdded _ = ()
            member this.ItemRemoved _ = ()

    type PubSubIngesterInstrumentation(system) =
        let topic = Topic.GlobalConnectIngesterStatus
        interface IIngesterInstrumentation with
            member this.WriteIdle ingesterId =
                IngesterStatusMessage.Idle ingesterId |> PubSub.publish system topic
            member this.WriteBusy ingesterId =
                IngesterStatusMessage.Busy ingesterId |> PubSub.publish system topic
            member this.WriteStatus ingesterId status =
                IngesterStatusMessage.Status(ingesterId, status) |> PubSub.publish system topic
            member this.WriteProgress ingesterId bytes =
                IngesterStatusMessage.Progress(ingesterId, bytes) |> PubSub.publish system topic
            member this.WriteFinal ingesterId totalBytes totalTime =
                IngesterStatusMessage.Final(ingesterId, totalBytes, totalTime) |> PubSub.publish system topic
            member this.WriteQueueLength ingesterId queueLength =
                IngesterStatusMessage.QueueLength(ingesterId, queueLength) |> PubSub.publish system topic
            member this.WriteQueueItems ingesterId queueItems =
                IngesterStatusMessage.QueueItems(ingesterId, queueItems) |> PubSub.publish system topic

    type PubSubCommandStatus(system) =
        let topic = Topic.GlobalConnectCommandStatus
        interface IPublishCommandStatus with
            member this.CommandInitiated ingesterId cmd =
                IngesterCommandInitiated(ingesterId, S3CommandItem.fromDomain cmd) |> PubSub.publish system topic
            member this.CommandCompleted ingesterId cmd =
                IngesterCommandCompleted(ingesterId, S3CommandItem.fromDomain cmd) |> PubSub.publish system topic
            member this.CommandFailed ingesterId cmd error =
                IngesterCommandFailed(ingesterId, S3CommandItem.fromDomain cmd, error) |> PubSub.publish system topic
            member this.CommandRejected ingesterId cmd error =
                IngesterCommandRejected(ingesterId, S3CommandItem.fromDomain cmd, error) |> PubSub.publish system topic

    type PubSubQueueContentChange(system) =
        let topic = Topic.GlobalConnectQueue
        interface IPublishQueueContentChange with
            member this.ItemAdded item =
                PriorityQueueItemAdded(S3CommandItem.fromDomain item) |> PubSub.publish system topic
            member this.ItemRemoved item =
                PriorityQueueItemRemoved(S3CommandItem.fromDomain item) |> PubSub.publish system topic

    type MediatorPublisher(ingesterInstrumentation: IIngesterInstrumentation list) as actor =
        inherit ActorBase()

        let mdr = typed (DistributedPubSub.Get(ActorBase.Context.System).Mediator)
        do mdr <! Subscribe(Topic.GlobalConnectIngesterStatus, actor.Self)

        override actor.Receive(msg: obj) =
            match msg with
            | SubscribeAck _ -> true
            | UnsubscribeAck _ -> true
            | :? IngesterStatusMessage as msg ->
                match msg with
                | IngesterStatusMessage.Idle ingesterId ->
                    ingesterInstrumentation |> List.iter (fun ingesterInstrumentation -> ingesterInstrumentation.WriteIdle ingesterId)
                | IngesterStatusMessage.Busy ingesterId ->
                    ingesterInstrumentation |> List.iter (fun ingesterInstrumentation -> ingesterInstrumentation.WriteBusy ingesterId)
                | IngesterStatusMessage.Status(ingesterId, status) ->
                    ingesterInstrumentation
                    |> List.iter (fun ingesterInstrumentation -> ingesterInstrumentation.WriteStatus ingesterId status)
                | IngesterStatusMessage.Progress(ingesterId, bytes) ->
                    ingesterInstrumentation
                    |> List.iter (fun ingesterInstrumentation -> ingesterInstrumentation.WriteProgress ingesterId bytes)
                | IngesterStatusMessage.Final(ingesterId, totalBytes, totalTime) ->
                    ingesterInstrumentation
                    |> List.iter (fun ingesterInstrumentation -> ingesterInstrumentation.WriteFinal ingesterId totalBytes totalTime)
                | IngesterStatusMessage.QueueLength(ingesterId, queueLength) ->
                    ingesterInstrumentation
                    |> List.iter (fun ingesterInstrumentation -> ingesterInstrumentation.WriteQueueLength ingesterId queueLength)
                | IngesterStatusMessage.QueueItems(ingesterId, queueItems) ->
                    ingesterInstrumentation
                    |> List.iter (fun ingesterInstrumentation -> ingesterInstrumentation.WriteQueueItems ingesterId queueItems)
                true
            | _ ->
                actor.Unhandled msg
                false
        static member Props(instrumentation: IIngesterInstrumentation list) =
            Props.Create<MediatorPublisher>(instrumentation)

    module InfluxDb =
        open InfluxDB.Collector

        let configureInstrumentation connectionString =
            maybe {
                let settings = Config.parseConnectionString connectionString
                let! serverName = settings |> Map.tryFind "Server"
                let! databaseName = settings |> Map.tryFind "Database"
                let! username = settings |> Map.tryFind "Username"
                let! password = settings |> Map.tryFind "Password"
                return
                    CollectorConfiguration()
                        .Batch.AtInterval(TimeSpan.FromSeconds(2.))
                        .WriteTo.InfluxDB(serverName, databaseName, username, password)
                        .CreateCollector()
            }

        type NoOpMetricsCollector() =
            inherit MetricsCollector()
            override this.Emit _ = ()

        type Instrumentation(ingesterCategory, collector: MetricsCollector) =

            let getTags ingesterId =
                Map.ofList [ "client", ingesterCategory; "id", ingesterId ]

            interface IIngesterInstrumentation with
                member this.WriteIdle ingesterId =
                    let fields = Map.ofList [ "is_busy", box 0 ]
                    collector.Write("ingestor_busy", fields, getTags ingesterId)
                member this.WriteBusy ingesterId =
                    let fields = Map.ofList [ "is_busy", box 1 ]
                    collector.Write("ingestor_busy", fields, getTags ingesterId)
                member this.WriteStatus ingesterId status =
                    let fields = Map.ofList [ "state", box status ]
                    collector.Write("ingestor_state_detailed", fields, getTags ingesterId)
                member this.WriteProgress ingesterId bytes =
                    let fields = Map.ofList [ "bytes", box bytes ]
                    collector.Write("ingestor_upload_progress", fields, getTags ingesterId)
                member this.WriteFinal ingesterId totalBytes totalTime =
                    let fields = Map.ofList [ "totalBytes", box totalBytes; "totalTime", box totalTime.TotalMilliseconds ]
                    collector.Write("ingestor_final", fields, getTags ingesterId)
                member this.WriteQueueLength _ _ = () // Not written to InfluxDB
                member this.WriteQueueItems _ _ = () // Not written to InfluxDB

    module OpenTelemetry =

        open System.Diagnostics.Metrics
        open System.Collections.Generic


        type OpenTelemetryIngesterInstrumentation(meterFactory: IMeterFactory, ingesterCategory: string) =


            let meter = meterFactory.Create("IngesterMeter")
            let ingesterBusyGauge = meter.CreateGauge<int>("ingester-busy", "Boolean", "Ingester status idle/busy")
            let ingesterUploadedCounter = meter.CreateCounter<int>("ingester-uploaded", "Bytes", "Bytes uploaded to origin")
            let ingesterSpeedGauge = meter.CreateGauge<float>("ingester-speed", "By/s", "Transfer rate in bytes per second")

            let getTag name value = KeyValuePair(name, value :> obj)

            interface IIngesterInstrumentation with
                member this.WriteBusy(ingesterId) =
                    ingesterBusyGauge.Record(1, getTag "client" ingesterCategory, getTag "id" ingesterId)
                member this.WriteIdle(ingesterId) =
                    ingesterBusyGauge.Record(0, getTag "client" ingesterCategory, getTag "id" ingesterId)
                member this.WriteFinal ingesterId bytes elapsed =
                    ingesterUploadedCounter.Add(int bytes, getTag "client" ingesterCategory, getTag "id" ingesterId)
                    ingesterSpeedGauge.Record(((double bytes) / elapsed.TotalSeconds), getTag "client" ingesterCategory, getTag "id" ingesterId)
                member this.WriteProgress ingesterId bytes = ()
                member this.WriteQueueItems ingesterId queueItems = ()
                member this.WriteQueueLength ingesterId queueLength = ()
                member this.WriteStatus ingesterId status = ()
