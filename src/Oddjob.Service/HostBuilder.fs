namespace Nrk.Oddjob.Service

module HostBuilder =

    open System
    open System.Collections.Generic
    open System.Reflection
    open Akkling
    open Akka.Cluster.Hosting
    open Akka.Cluster.Tools.PublishSubscribe
    open Akka.Discovery.Azure
    open Akka.Hosting
    open Akka.Logger.Serilog
    open Akka.Management
    open Akka.Management.Cluster.Bootstrap
    open Akka.Persistence.Hosting
    open Akka.Persistence.Sql.Config
    open Akka.Persistence.Sql.Hosting
    open Akka.Remote.Hosting
    open Microsoft.Extensions.Azure
    open Microsoft.Extensions.Configuration
    open Microsoft.Extensions.DependencyInjection
    open Microsoft.Extensions.Hosting
    open OpenTelemetry
    open OpenTelemetry.Metrics
    open OpenTelemetry.Trace
    open OpenTelemetry.Resources
    open Petabridge.Cmd.Host
    open Petabridge.Cmd.Cluster
    open Phobos.Hosting
    open Phobos.Actor

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Core.Queues

    open ActorsMetadata
    open GlobalConnectHosting

    [<Literal>]
    let ApplicationName = "Oddjob.Service"

    [<Literal>]
    let AkkaSystemName = "Oddjob"

    [<Literal>]
    let DiscoveryServiceName = "oddjob"

    let getDomainQualifiedHostName () =
        System.Net.Dns.GetHostEntry("").HostName

    let configureRuntimeSettings (configRoot: IConfigurationRoot) (appSettings: IConfigurationSection) (services: IServiceCollection) =
        let clusterRoles =
            let roles = appSettings.GetValue<string>("ClusterRoles")
            if String.isNullOrEmpty roles then
                ClusterRole.All
            else
                roles
                |> String.split ','
                |> Seq.map parseUnionCaseName<ClusterRole>
                |> Seq.toList
                |> function
                    | [] -> ClusterRole.All
                    | roles -> roles
        let featureFlagSource = appSettings.GetValue<string>("FeatureFlagSource")

        let queueConnectionsConfiguration = services.BindSection<QueuesConfig.Connections>(configRoot, "QueuesConnections")
        let queueSettingsConfiguration = services.BindSection<QueuesConfig.Settings>(configRoot, "QueuesSettings")
        let oddjobSettingsConfiguration = services.BindSection<OddjobConfig.Settings>(configRoot, "OddjobSettings")
        let featureFlagsConfiguration = services.BindSection<IDictionary<string, obj>>(configRoot, "FeatureFlags")
        let _ = services.BindSection<ConnectionStrings>(configRoot, "ConnectionStrings")
        let featureFlags =
            if featureFlagSource = "ConfigCat" then
                ConfigCat.Client.ConfigCatClient.Get(
                    configRoot.GetConnectionString "ConfigCatClient",
                    fun options -> options.DataGovernance <- ConfigCat.Client.DataGovernance.EuOnly
                )
                |> FeatureFlags.ConfigCat
            else
                featureFlagsConfiguration.Get() |> FeatureFlags.AppSettings

        let queuesConfig = makeQueuesConfig (queueConnectionsConfiguration.Get()) (queueSettingsConfiguration.Get())
        let oddjobConfig = oddjobSettingsConfiguration.Get() |> makeOddjobConfig
        let featureFlagsConfig = featureFlags

        {
            Environment = appSettings.GetValue<string>("Environment")
            ClusterRoles = clusterRoles
            FeatureFlags = featureFlagsConfig
            OddjobConfig = oddjobConfig
            QueuesConfig = queuesConfig
            AkkaRemotingPort = appSettings.GetValue<int>("AkkaRemotingPort", 1970)
            AkkaManagementPort = appSettings.GetValue<int>("AkkaManagementPort", 8558)
            PetabridgeCmdPort = appSettings.GetValue<int>("PetabridgeCmdPort", 9110)
        }

    let configureLoggers (runtimeSettings: RuntimeSettings) (builder: LoggerConfigBuilder) =
        let nonProdEnvironment = runtimeSettings.Environment <> "Prod"
        builder.LogLevel <- Akka.Event.LogLevel.DebugLevel
        builder.LogConfigOnStart <- true
        builder.DebugOptions <-
            DebugOptions(
                Receive = nonProdEnvironment,
                AutoReceive = nonProdEnvironment,
                LifeCycle = nonProdEnvironment,
                EventStream = nonProdEnvironment,
                Unhandled = true
            )
        builder.ClearLoggers().AddLogger<SerilogLogger>() |> ignore

    let configureServices (configRoot: IConfigurationRoot) (runtimeSettings: RuntimeSettings) (services: IServiceCollection) =

        let activityContext = ActivitySourceContext.Create(getAssemblyName ())
        let rmqApi = QueueApiImpl.OddjobRabbitMqApi(runtimeSettings.OddjobConfig, runtimeSettings.QueuesConfig, activityContext)
        services.AddSingleton<IRabbitMqApi>(rmqApi) |> ignore

        services.AddSingleton<OddjobConfig.PathMapping list>(runtimeSettings.OddjobConfig.PathMappings) |> ignore
        services.AddSingleton<ActivitySourceContext>(activityContext) |> ignore

        if (runtimeSettings.ClusterRoles |> List.contains ClusterRole.GlobalConnect) then
            configureS3Services configRoot runtimeSettings services
            configureGlobalConnectMetrics services

    let parseInstrumentationConfigFeatures (runtimeSettings: RuntimeSettings) =
        match getFeatureFlag runtimeSettings.FeatureFlags ConfigKey.OtelInstrumentation "None" with
        | "All" ->
            [
                InstrumentationFeature.AspNet
                InstrumentationFeature.Aws
                InstrumentationFeature.GrpcClient
                InstrumentationFeature.HttpClient
                InstrumentationFeature.Process
                InstrumentationFeature.Quartz
                InstrumentationFeature.RabbitMQ
                InstrumentationFeature.Runtime
                InstrumentationFeature.ServiceBus
                InstrumentationFeature.SqlClient
            ]
        | "None"
        | null -> []
        | text -> text.Split(",") |> Seq.map (fun item -> item |> tryParseUnionCaseName true) |> Seq.choose id |> Seq.toList

    let configureMetrics (configRoot: IConfigurationRoot) (runtimeSettings: RuntimeSettings) resource enabledFeatures (builder: OpenTelemetryBuilder) =
        let metricsConnectionString = configRoot.GetConnectionString "OtelCollector"
        if String.isNotNullOrEmpty metricsConnectionString then
            let connectionDetails = metricsConnectionString |> parseConnectionString
            let uri = connectionDetails["Uri"]
            builder.WithMetrics(fun builder ->
                builder
                    .SetResourceBuilder(resource)
                    .AddPhobosInstrumentation()
                    .ConfigureInstrumentation(enabledFeatures)
                    .AddMeter("IngesterMeter")
                    .AddMeter("ReminderMeter")
                    .AddOtlpExporter(fun opts -> opts.Endpoint <- Uri(uri))
                |> ignore)
        else
            builder

    let configureTracing (configRoot: IConfigurationRoot) (runtimeSettings: RuntimeSettings) resource enabledFeatures (builder: OpenTelemetryBuilder) =
        let tracesConnectionString = configRoot.GetConnectionString "OtelCollector"
        if String.isNotNullOrEmpty tracesConnectionString then
            let connectionDetails = tracesConnectionString |> parseConnectionString
            let uri = connectionDetails["Uri"]
            builder.WithTracing(fun builder ->
                builder
                    .SetResourceBuilder(resource)
                    .AddPhobosInstrumentation()
                    .ConfigureInstrumentation(enabledFeatures)
                    .AddOtlpExporter(fun opts -> opts.Endpoint <- Uri(uri))
                |> ignore)
        else
            builder

    let configureTracing_ (configRoot: IConfigurationRoot) (runtimeSettings: RuntimeSettings) resource _ (builder: OpenTelemetryBuilder) =
        let tracesConnectionString = configRoot.GetConnectionString "OtelCollector"
        if String.isNotNullOrEmpty tracesConnectionString then
            let connectionDetails = tracesConnectionString |> parseConnectionString
            let uri = connectionDetails["Uri"]
            // RabbitMQ instrumentation requires composite text map propagator
            let propagators: seq<OpenTelemetry.Context.Propagation.TextMapPropagator> =
                [
                    OpenTelemetry.Context.Propagation.TraceContextPropagator()
                    OpenTelemetry.Context.Propagation.BaggagePropagator()
                ]
            Sdk.SetDefaultTextMapPropagator(OpenTelemetry.Context.Propagation.CompositeTextMapPropagator(propagators))
            builder.WithTracing(fun builder ->
                builder
                    .SetResourceBuilder(resource)
                    .AddPhobosInstrumentation()
                    .AddAWSInstrumentation()
                    .AddRabbitMQInstrumentation()
                    .AddAspNetCoreInstrumentation()
                    .AddHttpClientInstrumentation()
                    .AddOtlpExporter(fun opts -> opts.Endpoint <- Uri(uri))
                |> ignore)
        else
            builder

    let configureOpenTelemetry (configRoot: IConfigurationRoot) runtimeSettings (services: IServiceCollection) =
        let enabledFeatures = parseInstrumentationConfigFeatures runtimeSettings
        let resource =
            ResourceBuilder.CreateDefault().AddService(Assembly.GetEntryAssembly().GetName().Name, $"{System.Net.Dns.GetHostName()}")
        services.AddOpenTelemetry()
        |> configureMetrics configRoot runtimeSettings resource enabledFeatures
        |> configureTracing configRoot runtimeSettings resource enabledFeatures
        |> ignore

    let configurePhobos (runtimeSettings: RuntimeSettings) (builder: Phobos.Actor.Configuration.PhobosConfigBuilder) =
        builder
            .WithMetrics(fun builder ->
                if getFeatureFlag runtimeSettings.FeatureFlags ConfigKey.DisablePhobosMetrics false then
                    builder.DisableAllMetrics()
                else
                    builder.ConfigurePhobos(MetricsSettings.enableAll)
                |> ignore)
            .WithTracing(fun builder ->
                if getFeatureFlag runtimeSettings.FeatureFlags ConfigKey.DisablePhobosTracing false then
                    builder.DisableAllTracing()
                else
                    builder.ConfigurePhobos(TraceSettings.withManualCreationAndFilter <| TraceFilter())
                |> ignore)
        |> ignore

    let configureAkkaManagement (runtimeSettings: RuntimeSettings) (setup: AkkaManagementSetup) =
        setup.Http.HostName <- getDomainQualifiedHostName ()
        setup.Http.Port <- runtimeSettings.AkkaManagementPort
        setup.Http.BindHostName <- "0.0.0.0"
        setup.Http.BindPort <- runtimeSettings.AkkaManagementPort

    let configureClusterBoostrapOptions (runtimeSettings: RuntimeSettings) (opts: ClusterBootstrapOptions) =
        opts.ContactPoint.ProbeInterval <- TimeSpan.FromSeconds 5.
        opts.ContactPointDiscovery.ServiceName <- DiscoveryServiceName
        opts.ContactPointDiscovery.PortName <- "management"
        opts.ContactPointDiscovery.RequiredContactPointsNr <- getFeatureFlag<int> runtimeSettings.FeatureFlags ConfigKey.RequiredContactPointNumber 2
        opts.ContactPointDiscovery.StableMargin <- TimeSpan.FromSeconds 10.
        opts.ContactPointDiscovery.Interval <- TimeSpan.FromSeconds 5.

    let rec applyRoles roles (rolesConfigurator: Map<_, _>) builder =
        match roles with
        | [] -> builder
        | role :: roles -> rolesConfigurator[role]builder |> applyRoles roles rolesConfigurator

    type AkkaConfigurationBuilder with

        member this.WithAllSerializers() =
            this.AddHocon(
                """
                akka {
                  actor {
                    serializers {
                      hyperion = "Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion"
                      protobuf-mediaset = "Nrk.Oddjob.Core.Dto.Serialization+ProtobufSerializer, Core"
                    }
                    serialization-bindings {
                      "System.Object" = hyperion
                      "Nrk.Oddjob.Core.Dto.IProtoBufSerializable, Core" = protobuf-mediaset
                    }
                    serialization-identifiers {
                      "Akka.Serialization.HyperionSerializer, Akka.Serialization.Hyperion" = -5
                      "Akka.Serialization.NewtonSoftJsonSerializer, Akka" = 1
                      "Nrk.Oddjob.Core.Dto.Serialization+ProtobufSerializer, Core" = 126
                    }
                  }
                }
                """,
                HoconAddMode.Prepend
            )

        member this.WithServiceRoles(roles, configRoot: IConfigurationRoot, featureFlags) =
            let quartzSettings = configRoot.GetRequiredSection("Quartz").GetValue<string>
            let connectionStrings = configRoot.GetSection("ConnectionStrings").Get()
            let settings =
                {
                    ConnectionStrings = connectionStrings
                    QuartzSettings = quartzSettings
                    FeatureFlags = featureFlags
                }
            let rolesConfigurator: Map<ClusterRole, AkkaConfigurationBuilder -> AkkaConfigurationBuilder> =
                Map.ofSeq
                    [
                        (ClusterRole.GlobalConnect, _.WithGlobalConnect(configRoot, settings))
                    ]
            this |> applyRoles roles rolesConfigurator

    type IHostBuilder with
        member this.ConfigureCluster(configRoot: IConfigurationRoot) =

            this.ConfigureServices(fun hostContext services ->

                let appSettings = configRoot.GetSection("AppSettings")
                let runtimeSettings = configureRuntimeSettings configRoot appSettings services

                let applicationName =
                    if runtimeSettings.ClusterRoles = ClusterRole.All then
                        ApplicationName
                    else
                        let roles = String.Join(".", runtimeSettings.ClusterRoles)
                        $"{ApplicationName}.{roles}"

                ProtoBufUtils.initProtoBuf ()

                configureLogging configRoot (String.replace "." "-" applicationName) runtimeSettings.Environment runtimeSettings.FeatureFlags
                configureServices configRoot runtimeSettings services
                configureOpenTelemetry configRoot runtimeSettings services

                services.AddAkka(
                    AkkaSystemName,
                    fun akkaBuilder provider ->
                        akkaBuilder
                            .WithRemoting(getDomainQualifiedHostName (), runtimeSettings.AkkaRemotingPort)
                            .WithActorAskTimeout(TimeSpan.FromSeconds 30.)
                            .ConfigureLoggers(configureLoggers runtimeSettings)
                            .AddPetabridgeCmd(
                                PetabridgeCmdOptions(Port = runtimeSettings.PetabridgeCmdPort),
                                fun cmd ->
                                    cmd.RegisterCommandPalette(ClusterCommands.Instance) |> ignore
                                    cmd.RegisterCommandPalette(ShardCommand.ShardCommandPaletteHandler()) |> ignore
                            )
                            .WithAkkaManagement(configureAkkaManagement runtimeSettings)
                            .WithAzureDiscovery(fun options ->
                                options.TableName <- "akkadiscovery"
                                options.ConnectionString <- configRoot.GetConnectionString("AkkaManagementTableStorage")
                                options.ServiceName <- DiscoveryServiceName
                                options.HostName <- getDomainQualifiedHostName ()
                                options.Port <- runtimeSettings.AkkaManagementPort)
                            (* TODO: Can be used for local testing, should be put behind a flag
                            .WithConfigDiscovery(fun options ->
                                  let service = Service()
                                  service.Name <- AkkaSystemName
                                  service.Endpoints <- [| $"localhost:{AkkaManagementDefaultPort}" |]
                                  options.Services.Add(service))
                             *)
                            .WithSqlPersistence(
                                configRoot.GetConnectionString "Akka",
                                "LinqToDB.ProviderName.SqlServer2019",
                                PersistenceMode.Both,
                                "dbo",
                                null,
                                false,
                                "sql",
                                true,
                                DatabaseMapping.SqlServer,
                                TagMode.TagTable,
                                false,
                                false
                            )
                            .WithClustering(
                                ClusterOptions(
                                    Roles = (runtimeSettings.ClusterRoles |> Seq.map getUnionCaseName |> Seq.toArray),
                                    // FailureDetector should match values set in HOCON file for legacy cluster nodes
                                    FailureDetector =
                                        PhiAccrualFailureDetectorOptions(
                                            HeartbeatInterval = TimeSpan.FromSeconds 5.,
                                            AcceptableHeartbeatPause = TimeSpan.FromSeconds 30.,
                                            ExpectedResponseAfter = TimeSpan.FromSeconds 10.,
                                            Threshold = 24
                                        )
                                )
                            )
                            .AddHocon("akka.cluster.downing-provider-class=\"\"", HoconAddMode.Prepend) // There is no way to turn SBR off using Akka.Hocon
                            .WithExtensions(typedefof<DistributedPubSubExtensionProvider>)
                            .WithClusterBootstrap(configureClusterBoostrapOptions runtimeSettings, autoStart = true)
                            .WithActors(fun system registry ->
                                QueueUtils.Config.declareQueuesAndExchanges system.Log runtimeSettings.OddjobConfig runtimeSettings.QueuesConfig
                                // If the ActorSystem gets quarantined by the cluster, we want to restart the system
                                spawnAnonymous
                                    system
                                    (propsNamed
                                        "akka-restarter"
                                        (actorOf2 (fun mailbox (msg: Akka.Remote.ThisActorSystemQuarantinedEvent) ->
                                            let msg =
                                                $"Detected quarantined event, starting system restart procedure. Local address %A{msg.LocalAddress}, remote address %A{msg.RemoteAddress}"
                                            logWarning mailbox msg
                                            // If the system has been quarantined we want it to be restarted. This should be handled by the orchestrator (Windows Services or Kubernetes)
                                            Environment.Exit(1)
                                            stop ())))
                                |> fun actor ->
                                    match system.EventStream.Subscribe(untyped actor, typeof<Akka.Remote.ThisActorSystemQuarantinedEvent>) with
                                    | false -> failwith "Could not subscribe to cluster quarantined event"
                                    | true -> registry.Register<SystemQuarantinedMarker>(untyped actor))
                            .WithPhobos(AkkaRunMode.AkkaCluster, configurePhobos runtimeSettings)
                            .WithAllSerializers()
                            .WithServiceRoles(runtimeSettings.ClusterRoles, configRoot, runtimeSettings.FeatureFlags)
                        |> ignore
                )
                |> ignore)
