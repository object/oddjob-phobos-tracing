module Nrk.Oddjob.Service.WebApiHosting

open Akkling
open Akka.Hosting
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.Options

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Config
open Nrk.Oddjob.Service.ActorsMetadata
open Nrk.Oddjob.WebApi
open Nrk.Oddjob.Upload.MediaSetStatusPersistence

type AkkaConfigurationBuilder with

    member this.WithWebApi(configRoot: IConfigurationRoot, settings: RoleSettings) : AkkaConfigurationBuilder =
        this
        |> fun builder ->
            builder
                .AddHocon(
                    """
                    akka {
                      actor {
                        deployment {
                          "/webapi_upload" {
                            router = round-robin-pool
                            nr-of-instances = 10
                          }
                          "/webapi_ps_mediaset" {
                            router = round-robin-pool
                            nr-of-instances = 10
                          }
                        }
                      }
                    }
                    """,
                    HoconAddMode.Prepend
                )
                .WithSingletonActor<MediaSetStatusPersistanceMarker, MediaSetStatusCommand>(
                    (makeActorName [ "MediaSet Status Persistence" ]),
                    propsNamed "webapi-mediaset-status-persistence" <| mediaSetStatusPersistenceActor settings.ConnectionStrings.Akka
                )
                .WithActors(fun system registry resolver ->
                    let scope = resolver.CreateScope().Resolver
                    let oddjobConfig = scope.GetService<IOptionsSnapshot<OddjobConfig.Settings>>().Value |> makeOddjobConfig
                    let queuesConfig =
                        makeQueuesConfig
                            (scope.GetService<IOptionsSnapshot<QueuesConfig.Connections>>().Value)
                            (scope.GetService<IOptionsSnapshot<QueuesConfig.Settings>>().Value)
                    let schedulers =
                        [
                            Reminders.PsRole, registry.Get<PsSchedulerMarker>()
                            Reminders.PotionRole, registry.Get<PotionSchedulerMarker>()
                            Reminders.UploadRole, registry.Get<UploadSchedulerMarker>()
                        ]
                        |> List.map (fun (role, schedulerActor) ->
                            let scheduler = QuartzBootstrapper.getQuartzScheduler settings.ConnectionStrings.Akka role settings.QuartzSettings
                            (role, (scheduler, schedulerActor)))
                        |> Map.ofList
                    let aprops =
                        {
                            ConnectionStrings = settings.ConnectionStrings
                            Schedulers = schedulers
                            UploadMediator = typed <| registry.Get<UploadProxyMarker>()
                            PsMediator = typed <| registry.Get<PsProxyMarker>()
                            PotionMediator = typed <| registry.Get<PotionProxyMarker>()
                            MediaSetStateCache = typed <| registry.Get<MediaSetStateCacheMarker>()
                        }
                    WebApp.startApp
                        aprops
                        system
                        oddjobConfig
                        queuesConfig
                        (registry.Get<MediaSetStatusPersistanceMarker>() |> typed)
                        (scope.GetService<ActivitySourceContext>())
                    |> ignore
                    SocketApp.startApp system oddjobConfig |> ignore)
