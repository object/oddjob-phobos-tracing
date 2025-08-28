module Nrk.Oddjob.Service.UploadHosting

open System
open Akka.Actor
open Akka.Cluster.Sharding
open Akka.Cluster.Hosting
open Akka.DependencyInjection
open Akka.Hosting
open Akkling
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.Options

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Config
open Nrk.Oddjob.Core.S3.S3Types
open Nrk.Oddjob.Upload
open Nrk.Oddjob.Upload.UploadTypes
open Nrk.Oddjob.Upload.UploadBootstrapperUtils
open Nrk.Oddjob.Upload.MediaSetController
open Nrk.Oddjob.Upload.MediaSetPublisher
open Nrk.Oddjob.Upload.MediaSetStatusPersistence
open Nrk.Oddjob.Upload.GlobalConnect.GlobalConnectBootstrapperUtils
open Nrk.Oddjob.Ingesters.GlobalConnect.Bootstrapper
open Nrk.Oddjob.Potion.PotionTypes

open ActorsMetadata

let createActionEnvironment oddjobConfig (connectionStrings: ConnectionStrings) s3Client log : ActionEnvironment =
    let globalConnectEnvironment = createGlobalConnectEnvironment oddjobConfig s3Client
    {
        GlobalConnect = globalConnectEnvironment
    }

let uploadShardEntityPropsFactory (system: ActorSystem) (registry: IReadOnlyActorRegistry) (resolver: IDependencyResolver) =
    let scope = resolver.CreateScope().Resolver
    let oddjobConfig = scope.GetService<IOptionsSnapshot<OddjobConfig.Settings>>().Value |> makeOddjobConfig
    let connectionStrings = scope.GetService<IOptionsSnapshot<ConnectionStrings>>().Value
    let priorityQueue = registry.Get<GlobalConnectPriorityQueueProxyMarker>() |> typed
    let s3Queue = priorityQueue
    let amazon = scope.GetService<IS3Api>()

    let actorFactories =
        seq {
            if OddjobConfig.hasGlobalConnect oddjobConfig.Origins then
                yield (Origin.GlobalConnect, GlobalConnectUploaderPropsFactory(oddjobConfig, amazon, s3Queue) :> IOriginSpecificUploadActorPropsFactory)
        }
        |> Map.ofSeq
    let uploadActorProps = createUploadActorProps actorFactories
    let statusPersistenceActor = registry.Get<MediaSetStatusPersistanceMarker>() |> typed
    let stateCacheActor = registry.Get<MediaSetStateCacheMarker>() |> typed

    let mediaSetStatusPublisher = PubSubMediaSetStatus(system) :> IPublishMediaSetStatus
    let mediaSetResourceStatePublisher = PubSubMediaSetResourceState(system) :> IPublishMediaSetResourceState
    let actionEnvironment = createActionEnvironment oddjobConfig connectionStrings amazon system.Log
    let uploadShardExtractor = UploadShardExtractor(oddjobConfig.Upload.NumberOfShards)
    let uploadMediator = uploadShardExtractor |> ClusterShards.getUploadMediator system
    let potionShardExtractor = PotionShardExtractor(oddjobConfig.Potion.NumberOfShards)
    let potionMediator = potionShardExtractor |> ClusterShards.getPotionMediator system
    let externalGroupIdResolver groupId = PotionExternalGroupIdPrefix + groupId
    let startCompletionReminder mediaSetId = None

    Func<string, Props>(fun entityId ->
        let clientRef = getUploadClientRef externalGroupIdResolver potionMediator entityId
        let aprops =
            createMediaSetControllerProps
                oddjobConfig
                connectionStrings.Akka
                uploadActorProps
                actionEnvironment
                clientRef
                startCompletionReminder
                statusPersistenceActor
                stateCacheActor
                mediaSetStatusPublisher
                mediaSetResourceStatePublisher
                (registry.Get<UploadSchedulerMarker>())
                (registry.Get<PsSchedulerMarker>())
                (scope.GetService<ActivitySourceContext>())
        { propsNamed "upload-shard" (uploadShardActor aprops uploadMediator) with
            SupervisionStrategy = Some <| getOneForOneRestartSupervisorStrategy system.Log
        }
        |> _.ToProps())

type AkkaConfigurationBuilder with

    member private this.withUpload(configRoot: IConfigurationRoot, settings: RoleSettings, proxyOnly) : AkkaConfigurationBuilder =
        let uploadShardExtractor = UploadShardExtractor(configRoot.GetValue<int>("OddjobSettings:Upload:NumberOfShards"))
        this
            .WithQuartzScheduler<UploadSchedulerMarker>(Reminders.UploadRole, settings, proxyOnly)
            .WithSingletonActor<MediaSetStateCacheMarker, MediaSetStateCache.MediaSetStateCacheCommand>(
                (makeActorName [ "MediaSet State Cache" ]),
                propsNamed "mediaset-state-cache" <| MediaSetStateCache.mediaSetStateCacheActor settings.ConnectionStrings.Akka
            )
            .WithShardProxy<UploadProxyMarker>(ClusterShards.UploadMediaSetRegion, ClusterShards.UploadRole, uploadShardExtractor)
        |> fun builder ->
            if proxyOnly then
                builder
            else
                builder
                    .AddHocon(
                        """
                        akka {
                          actor {
                            deployment {
                              "/upload_granitt" {
                                router = round-robin-pool
                                nr-of-instances = 5
                              }
                            }
                          }
                        }
                        """,
                        HoconAddMode.Prepend
                    )
                    .WithSingletonActor<MediaSetStatusPersistanceMarker, MediaSetStatusCommand>(
                        (makeActorName [ "MediaSet Status Persistence" ]),
                        propsNamed "upload-mediaset-status-persistence" <| mediaSetStatusPersistenceActor settings.ConnectionStrings.Akka
                    )
                    .WithSingletonProxy<GlobalConnectPriorityQueueProxyMarker>(
                        makeActorName [ "GlobalConnect Priority Queue" ],
                        spawnGlobalConnectPriorityQueueProxy
                    )
                    .WithShardRegion<UploadShardMarker>(
                        ClusterShards.UploadMediaSetRegion,
                        uploadShardEntityPropsFactory,
                        uploadShardExtractor,
                        ShardOptions(
                            Role = ClusterShards.UploadRole,
                            PassivateIdleEntityAfter = TimeSpan.FromMinutes(5.),
                            StateStoreMode = StateStoreMode.DData
                        )
                    )

    member this.WithUpload(configRoot: IConfigurationRoot, settings: RoleSettings) =
        this.withUpload (configRoot, settings, false)

    member this.WithUploadProxy(configRoot: IConfigurationRoot, settings: RoleSettings) =
        this.withUpload (configRoot, settings, true)
