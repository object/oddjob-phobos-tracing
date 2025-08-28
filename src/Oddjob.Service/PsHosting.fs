module Nrk.Oddjob.Service.PsHosting

open System
open Akka.Actor
open Akka.Cluster.Sharding
open Akka.Cluster.Hosting
open Akka.DependencyInjection
open Akka.Hosting
open Akkling
open Azure.Messaging.ServiceBus
open Microsoft.Extensions.Azure
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.Options

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Config
open Nrk.Oddjob.Core.Queues
open Nrk.Oddjob.Ps.PsDto
open Nrk.Oddjob.Ps.PsBootstrapperUtils
open Nrk.Oddjob.Ps.PsShardMessages
open Nrk.Oddjob.Upload.UploadTypes
open Nrk.Oddjob.Service.ActorsMetadata

let psShardEntityPropsFactory
    featureFlags
    (configRoot: IConfigurationRoot)
    (system: ActorSystem)
    (registry: IReadOnlyActorRegistry)
    (resolver: IDependencyResolver)
    =
    let scope = resolver.CreateScope().Resolver
    let oddjobConfig = scope.GetService<IOptionsSnapshot<OddjobConfig.Settings>>().Value |> makeOddjobConfig
    let connectionStrings = scope.GetService<IOptionsSnapshot<ConnectionStrings>>().Value
    let clientFactory = scope.GetService<IAzureClientFactory<ServiceBusClient>>()

    let granittRouter = createPsGranittRouter connectionStrings.GranittMySql system
    let serviceBusRouter = createPsServiceBusRouter oddjobConfig system clientFactory
    let mediaSetStateCache = registry.Get<MediaSetStateCacheMarker>() |> typed
    let queueScheduler =
        spawn
            system
            (makeActorName [ "Queue Scheduler" ])
            (propsNamed "ps-queue-scheduler" <| SchedulerActors.scheduleQueueMessagesActor connectionStrings.Akka)
        |> retype
    let psMediator = registry.Get<PsProxyMarker>() |> typed
    let environment = configRoot.GetSection("AppSettings").GetValue "environment"
    let tekstebankenConfig = configRoot.GetSection("Tekstebanken").Get<Tekstebanken.Config>()

    Func<string, Props>(fun entityId ->
        createPsMediaSetProps
            oddjobConfig
            connectionStrings
            environment
            tekstebankenConfig
            system
            (scope.GetService<IRabbitMqApi>())
            mediaSetStateCache
            granittRouter
            serviceBusRouter
            (PsMediatorRef.Proxy(retype psMediator))
            psMediator
            (registry.Get<UploadProxyMarker>() |> typed)
            (registry.Get<PsSchedulerMarker>())
            (registry.Get<UploadSchedulerMarker>())
            queueScheduler
            (fun (keyValue: string) (defaultValue: bool) -> getFeatureFlag featureFlags keyValue defaultValue)
            (scope.GetService<ActivitySourceContext>())
        |> _.ToProps())

type AkkaConfigurationBuilder with

    member private this.withPs(configRoot: IConfigurationRoot, settings: RoleSettings, proxyOnly) : AkkaConfigurationBuilder =
        let psShardExtractor = PsShardExtractor(configRoot.GetValue<int>("OddjobSettings:Ps:NumberOfShards"))
        let uploadShardExtractor = UploadShardExtractor(configRoot.GetValue<int>("OddjobSettings:Upload:NumberOfShards"))
        this
            .WithQuartzScheduler<PsSchedulerMarker>(Reminders.PsRole, settings, proxyOnly)
            .WithShardProxy<PsProxyMarker>(ClusterShards.PsMediaSetRegion, ClusterShards.PsRole, psShardExtractor)
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
                              "/ps_granitt" {
                                router = round-robin-pool
                                nr-of-instances = 5
                              }
                              "/ps_azure" {
                                router = round-robin-pool
                                nr-of-instances = 5
                              }
                            }
                          }
                        }
                        """,
                        HoconAddMode.Prepend
                    )
                    .WithShardRegion<PsShardMarker>(
                        ClusterShards.PsMediaSetRegion,
                        psShardEntityPropsFactory settings.FeatureFlags configRoot,
                        psShardExtractor,
                        ShardOptions(Role = ClusterShards.PsRole, PassivateIdleEntityAfter = TimeSpan.FromMinutes(5.), StateStoreMode = StateStoreMode.DData)
                    )
                    .WithRabbitMqSubscriber<PsShardMarker, PsProgramSubscriberMarker, PsShardMessage, PsChangeJobDto>(
                        startPsProgramSubscriber,
                        QueueCategory.PsProgramsWatch
                    )
                    .WithRabbitMqSubscriber<PsShardMarker, PsTranscodingSubscriberMarker, PsShardMessage, PsTranscodingFinishedDto>(
                        startPsTranscodingSubscriber,
                        QueueCategory.PsTranscodingWatch
                    )
                    .WithRabbitMqSubscriber<PsShardMarker, PsSubtitlesSubscriberMarker, PsShardMessage, PsTvSubtitlesDto>(
                        startPsSubtitlesSubscriber,
                        QueueCategory.PsSubtitlesWatch
                    )
                    .WithRabbitMqSubscriber<PsShardMarker, PsUsageRightsLegacySubscriberMarker, PsShardMessage, PsChangeJobDto>(
                        startPsUsageRightsLegacySubscriber,
                        QueueCategory.PsUsageRightsPsWatch
                    )
                    .WithRabbitMqSubscriber<PsShardMarker, PsProgramStatusSubscriberMarker, PsShardMessage, PsProgramStatusEventDto>(
                        startPsProgramStatusSubscriber,
                        QueueCategory.PsProgramStatusWatch
                    )
                    .WithServiceBusSubscriber<PsShardMarker, PsRadioTranscodingSubscriberMarker, PsShardMessage, PsRadioChangeDto>(
                        (fun (config: OddjobConfig) -> config.Ps.RadioTranscodingListener),
                        startPsRadioTranscodingSubscriber,
                        startPsRadioTranscodingAdapter
                    )
                    .WithServiceBusSubscriber<PsShardMarker, PsUsageRightsSubscriberMarker, PsShardMessage, PsUsageRightsChangeDto>(
                        (fun (config: OddjobConfig) -> config.Ps.UsageRightsListener),
                        startPsUsageRightsSubscriber,
                        startPsUsageRightsAdapter
                    )

    member this.WithPs(configRoot: IConfigurationRoot, settings: RoleSettings) =
        this.withPs (configRoot, settings, false)

    member this.WithPsProxy(configRoot: IConfigurationRoot, settings: RoleSettings) =
        this.withPs (configRoot, settings, true)
