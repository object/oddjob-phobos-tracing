module Nrk.Oddjob.Service.PotionHosting

open System
open Akkling
open Akka.Actor
open Akka.Cluster.Sharding
open Akka.Cluster.Hosting
open Akka.DependencyInjection
open Akka.Hosting
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.Options

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Config
open Nrk.Oddjob.Core.Queues
open Nrk.Oddjob.Potion.PotionTypes
open Nrk.Oddjob.Potion.PotionDto
open Nrk.Oddjob.Potion.PotionWorkerActors
open Nrk.Oddjob.Potion.PotionBootstrapperUtils

open ActorsMetadata

let potionShardEntityPropsFactory featureFlags (system: ActorSystem) (registry: IReadOnlyActorRegistry) (resolver: IDependencyResolver) =
    let scope = resolver.CreateScope().Resolver
    let oddjobConfig = scope.GetService<IOptionsSnapshot<OddjobConfig.Settings>>().Value |> makeOddjobConfig
    let connectionStrings = scope.GetService<IOptionsSnapshot<ConnectionStrings>>().Value

    Func<string, Props>(fun entityId ->
        let aprops =
            createPotionHandlerProps
                oddjobConfig
                connectionStrings
                system
                (scope.GetService<IRabbitMqApi>())
                (registry.Get<PotionProxyMarker>() |> typed)
                (registry.Get<UploadProxyMarker>() |> typed)
                (registry.Get<PotionSchedulerMarker>())
                (registry.Get<UploadSchedulerMarker>())
                (fun (keyValue: string) (defaultValue: bool) -> getFeatureFlag featureFlags keyValue defaultValue)
                (fun (keyValue: string) (defaultValue: string) -> getFeatureFlag featureFlags keyValue defaultValue)
        { propsNamed "potion-shard" (potionShardActor aprops) with
            SupervisionStrategy = Some <| getOneForOneRestartSupervisorStrategy system.Log
        }
        |> _.ToProps())

type AkkaConfigurationBuilder with

    member private this.getShardExtractor(configRoot: IConfigurationRoot) =
        PotionShardExtractor(configRoot.GetValue<int>("OddjobSettings:Potion:NumberOfShards"))

    member private this.withPotion(configRoot: IConfigurationRoot, settings, proxyOnly) : AkkaConfigurationBuilder =
        let potionShardExtractor = PotionShardExtractor(configRoot.GetValue<int>("OddjobSettings:Potion:NumberOfShards"))
        this
            .WithQuartzScheduler<PotionSchedulerMarker>(Reminders.PotionRole, settings, proxyOnly)
            .WithShardProxy<PotionProxyMarker>(ClusterShards.PotionSetRegion, ClusterShards.PotionRole, potionShardExtractor)
        |> fun builder ->
            if proxyOnly then
                builder
            else
                builder
                    .WithShardRegion<PotionShardMarker>(
                        ClusterShards.PotionSetRegion,
                        potionShardEntityPropsFactory settings.FeatureFlags,
                        potionShardExtractor,
                        ShardOptions(
                            Role = ClusterShards.PotionRole,
                            PassivateIdleEntityAfter = TimeSpan.FromMinutes(5.),
                            StateStoreMode = StateStoreMode.DData
                        )
                    )
                    .WithRabbitMqSubscriber<PotionShardMarker, PotionCommandSubscriberMarker, PotionCommand, PotionCommandDto>(
                        startPotionWatchSubscriber,
                        QueueCategory.PotionWatch
                    )

    member this.WithPotion(configRoot: IConfigurationRoot, settings) =
        this.withPotion (configRoot, settings, false)

    member this.WithPotionProxy(configRoot: IConfigurationRoot, settings) =
        this.withPotion (configRoot, settings, true)
