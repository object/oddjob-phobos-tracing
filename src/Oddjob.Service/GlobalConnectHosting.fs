module Nrk.Oddjob.Service.GlobalConnectHosting

open System
open System.Diagnostics.Metrics
open Akka.Actor
open Akka.Cluster.Hosting
open Akka.DependencyInjection
open Akka.Hosting
open Akkling
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Config
open Nrk.Oddjob.Core.S3.S3Types
open Nrk.Oddjob.Core.S3.S3Api
open Nrk.Oddjob.Ingesters.GlobalConnect
open Nrk.Oddjob.Ingesters.GlobalConnect.Bootstrapper

open ActorsMetadata

let configureS3Services (configRoot: IConfigurationRoot) (runtimeSettings: RuntimeSettings) (services: IServiceCollection) =

    let s3AccessKeys = configRoot.GetConnectionString "AmazonS3" |> parseConnectionString
    let s3Api = new S3Api(runtimeSettings.OddjobConfig.S3, s3AccessKeys)
    let s3Client = S3Client(s3Api, LocalFileSystemInfo()) :> IS3Client

    services.AddSingleton<IS3Api>(s3Api) |> ignore
    services.AddSingleton<IS3Client>(s3Client) |> ignore

let configureGlobalConnectMetrics (services: IServiceCollection) =
    services.AddSingleton<IngesterMetrics.IIngesterInstrumentation, IngesterMetrics.PubSubIngesterInstrumentation>(fun sp ->
        IngesterMetrics.PubSubIngesterInstrumentation(sp.GetService<ActorSystem>()))
    |> ignore
    services.AddSingleton<IngesterMetrics.IPublishCommandStatus, IngesterMetrics.PubSubCommandStatus>(fun sp ->
        IngesterMetrics.PubSubCommandStatus(sp.GetService<ActorSystem>()))
    |> ignore
    services.AddSingleton<IngesterMetrics.IPublishQueueContentChange, IngesterMetrics.PubSubQueueContentChange>(fun sp ->
        IngesterMetrics.PubSubQueueContentChange(sp.GetService<ActorSystem>()))
    |> ignore

let getInstrumentationProps (configRoot: IConfigurationRoot) (system: ActorSystem) (registry: IReadOnlyActorRegistry) (resolver: IDependencyResolver) =
    let metricsCollector =
        configRoot.GetConnectionString "InfluxDBIngesters"
        |> IngesterMetrics.InfluxDb.configureInstrumentation
        |> Option.defaultValue (new IngesterMetrics.InfluxDb.NoOpMetricsCollector() :> _)
    let influxDbInstrumentation = IngesterMetrics.InfluxDb.Instrumentation("globalConnect", metricsCollector)
    let meterFactory = resolver.GetService<IMeterFactory>()
    let otelInstrumentation = IngesterMetrics.OpenTelemetry.OpenTelemetryIngesterInstrumentation(meterFactory, "globalconnect")
    IngesterMetrics.MediatorPublisher.Props([ influxDbInstrumentation; otelInstrumentation ])

let getPriorityQueueProps (system: ActorSystem) (registry: IReadOnlyActorRegistry) (resolver: IDependencyResolver) =
    let priorityQueue = registry.Get<GlobalConnectPriorityQueueProxyMarker>() |> typed
    let pathMappings = resolver.GetService<OddjobConfig.PathMapping list>()
    let startS3Pool = fun () -> startS3Pool system pathMappings priorityQueue
    let instrumentation = resolver.GetService<IngesterMetrics.IIngesterInstrumentation>()
    let changePublisher = resolver.GetService<IngesterMetrics.IPublishQueueContentChange>()
    (propsNamed "globalconnect-priority-queue" (getGlobalConnectPriorityQueueProps instrumentation changePublisher "GlobalConnectIngesterQueue" startS3Pool))
        .ToProps()

type AkkaConfigurationBuilder with

    member this.WithGlobalConnect(configRoot: IConfigurationRoot, settings: RoleSettings) : AkkaConfigurationBuilder =
        this
        |> fun builder ->
            builder
                .AddHocon(
                    """
                    akka {
                      actor {
                        deployment {
                          "/s3_ingesters" {
                            router = broadcast-pool
                            cluster {
                              enabled = on
                              max-total-nr-of-instances = 50
                              max-nr-of-instances-per-node = 10
                              allow-local-routees = on
                              use-role = GlobalConnect
                            }
                          }
                        }
                      }
                    }
                    """,
                    HoconAddMode.Prepend
                )
                .WithSingletonProxy<GlobalConnectPriorityQueueProxyMarker>(
                    makeActorName [ "GlobalConnect Priority Queue" ],
                    spawnGlobalConnectPriorityQueueProxy
                )
                .WithSingleton<GlobalConnectPriorityQueueMarker>(
                    makeActorName [ "GlobalConnect Priority Queue" ],
                    getPriorityQueueProps,
                    ClusterSingletonOptions(Role = "GlobalConnect")
                )
                .WithSingletonOnCondition<GlobalConnectInstrumentationMarker>(
                    makeActorName [ "GlobalConnect InfluxDB Mediator" ],
                    getInstrumentationProps configRoot,
                    ClusterSingletonOptions(Role = "GlobalConnect"),
                    fun () -> configRoot.GetConnectionString "InfluxDBIngesters" <> null
                )
