[<AutoOpen>]
module Nrk.Oddjob.Service.Extensions

open System
open Akka.Actor
open Akka.DependencyInjection
open Akka.Hosting
open Akka.Cluster
open Akka.Cluster.Hosting
open Akkling
open Azure.Messaging.ServiceBus
open Microsoft.Extensions.Azure
open Microsoft.Extensions.Configuration
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Options
open OpenTelemetry
open OpenTelemetry.Metrics
open OpenTelemetry.Trace
open Phobos.Actor.Configuration

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Config
open Nrk.Oddjob.Core.Queues

type AkkaConfigurationBuilder with

    member this.WithShardProxy<'TKey>(typeName, roleName, extractor) : AkkaConfigurationBuilder =
        this.WithActors(fun system registry ->
            if registry.TryGet<'TKey>() |> fst |> not then
                let shardRegionProxy = Akka.Cluster.Sharding.ClusterSharding.Get(system).StartProxy(typeName, roleName, extractor)
                registry.Register<'TKey>(shardRegionProxy))

    member this.WithQuartzScheduler<'TKey>(roleName, settings: RoleSettings, proxyOnly) =
        this.WithActors(fun system registry ->
            if registry.TryGet<'TKey>() |> fst |> not then
                let getScheduler =
                    if proxyOnly then
                        QuartzBootstrapper.getRoleScheduler
                    else
                        QuartzBootstrapper.startRoleScheduler
                let scheduler = getScheduler system settings.ConnectionStrings.Akka roleName settings.QuartzSettings
                registry.Register<'TKey>(snd scheduler))

    member this.WithSingletonActor<'TKey, 'TMessage>(actorName, actorProps: Props<'TMessage>) =
        this.WithActors(fun system registry ->
            if registry.TryGet<'TKey>() |> fst |> not then
                let actor = spawn system actorName actorProps
                registry.Register<'TKey>(untyped actor))

    member this.WithSingleton<'TKey>(actorName: string, props: Props, options: ClusterSingletonOptions) =
        this.WithSingleton<'TKey>(actorName, "singleton", props, options, false)

    member this.WithSingleton<'TKey>
        (actorName: string, propsFactory: ActorSystem -> IActorRegistry -> IDependencyResolver -> Props, options: ClusterSingletonOptions)
        =
        this.WithSingleton<'TKey>(actorName, "singleton", propsFactory, options, false)

    member this.WithSingletonOnCondition<'TKey>
        (actorName: string, propsFactory: ActorSystem -> IActorRegistry -> IDependencyResolver -> Props, options: ClusterSingletonOptions, predicate)
        =
        if predicate () then
            this.WithSingleton<'TKey>(actorName, "singleton", propsFactory, options, false)
        else
            this

    member this.WithSingletonProxy<'TKey>(actorName, spawnProxy) =
        this.WithActors(fun system registry ->
            if registry.TryGet<'TKey>() |> fst |> not then
                registry.Register<'TKey>(spawnProxy system actorName |> untyped))

    member this.WithRabbitMqSubscriber<'TShardMarker, 'TSubscriberMarker, 'TShardMessage, 'TQueueMessage>
        (spawnSubscriber: _ -> _ -> IActorRef<QueueMessage<'TShardMessage>> -> _ -> IActorRef<QueueMessage<'TQueueMessage>>, queueCategory)
        =
        this.WithActors(fun system registry resolver ->
            let scope = resolver.CreateScope().Resolver
            let shard = registry.Get<'TShardMarker>() |> typed
            let subscriber = spawnSubscriber system resolver shard (scope.GetService<ActivitySourceContext>())
            registry.Register<'TSubscriberMarker>(untyped subscriber)
            let rabbitMqApi = resolver.GetService<IRabbitMqApi>()
            Cluster.Get(system).RegisterOnMemberUp(fun _ -> rabbitMqApi.AttachConsumer(system, queueCategory, subscriber)))

type IServiceCollection with

    member this.BindSection<'TKey when 'TKey: not struct>(configRoot: IConfigurationRoot, sectionName) =
        let section = configRoot.GetSection(sectionName)
        this.AddOptions<'TKey>().Bind(section) |> ignore
        section

type MetricsConfigBuilder with
    member this.ConfigurePhobos(settings: MetricsSettings) =
        this
            .SetMonitorUserActors(settings.MonitorUserActors)
            .SetMonitorSystemActors(settings.MonitorSystemActors)
            .SetMonitorEventStream(settings.MonitorEventStream)
            .SetMonitorMailboxDepth(settings.MonitorMailboxDepth)

type MeterProviderBuilder with
    member this.ConfigureInstrumentation(settings: InstrumentationFeature seq) =
        let builder = this
        let builder =
            if settings |> Seq.contains InstrumentationFeature.AspNet then
                builder.AddAspNetCoreInstrumentation()
            else
                builder
        let builder =
            if settings |> Seq.contains InstrumentationFeature.Aws then
                builder.AddAWSInstrumentation()
            else
                builder
        let builder =
            if settings |> Seq.contains InstrumentationFeature.HttpClient then
                builder.AddHttpClientInstrumentation()
            else
                builder
        let builder =
            if settings |> Seq.contains InstrumentationFeature.Process then
                builder.AddProcessInstrumentation()
            else
                builder
        let builder =
            if settings |> Seq.contains InstrumentationFeature.Runtime then
                builder.AddRuntimeInstrumentation()
            else
                builder
        let builder =
            if settings |> Seq.contains InstrumentationFeature.SqlClient then
                builder.AddSqlClientInstrumentation()
            else
                builder
        builder

type TracingConfigBuilder with
    member this.ConfigurePhobos(settings: TraceSettings) =
        this
            .VerboseTracing(settings.VerboseTracingAllowed)
            .SetCreateTraceUponReceive(settings.CreateTraceUponReceive)
            .IncludeMessagesAlreadyInTrace(settings.ShouldIncludeMessagesAlreadyInTrace)
            .SetAppendLogsToTrace(settings.AppendLogsToTrace)
            .SetTraceAsk(settings.TraceAsk)
            .SetTraceAkkaPersistence(settings.TraceAkkaPersistence)
            .SetTraceActorLifecycle(settings.AllowLifecycleTracing)
            .SetTraceUserActors(settings.TraceUserActors)
            .SetTraceSystemActors(settings.TraceSystemActors)
            .SetTraceFilter(TraceFilter())

type TracerProviderBuilder with
    member this.ConfigureInstrumentation(settings: InstrumentationFeature seq) =
        // RabbitMQ instrumentation requires composite text map propagator
        let propagators: seq<OpenTelemetry.Context.Propagation.TextMapPropagator> =
            [
                OpenTelemetry.Context.Propagation.TraceContextPropagator()
                OpenTelemetry.Context.Propagation.BaggagePropagator()
            ]
        Sdk.SetDefaultTextMapPropagator(OpenTelemetry.Context.Propagation.CompositeTextMapPropagator(propagators))
        let builder = this
        let builder =
            if settings |> Seq.contains InstrumentationFeature.AspNet then
                builder.AddAspNetCoreInstrumentation()
            else
                builder
        let builder =
            if settings |> Seq.contains InstrumentationFeature.Aws then
                builder.AddAWSInstrumentation()
            else
                builder
        let builder =
            if settings |> Seq.contains InstrumentationFeature.HttpClient then
                builder.AddHttpClientInstrumentation()
            else
                builder
        let builder =
            if settings |> Seq.contains InstrumentationFeature.GrpcClient then
                builder.AddGrpcClientInstrumentation()
            else
                builder
        let builder =
            if settings |> Seq.contains InstrumentationFeature.Quartz then
                builder.AddQuartzInstrumentation()
            else
                builder
        let builder =
            if settings |> Seq.contains InstrumentationFeature.RabbitMQ then
                builder.AddRabbitMQInstrumentation()
            else
                builder
        let builder =
            if settings |> Seq.contains InstrumentationFeature.ServiceBus then
                builder.AddSource("Azure.Messaging.ServiceBus")
            else
                builder
        let builder =
            if settings |> Seq.contains InstrumentationFeature.SqlClient then
                builder.AddSqlClientInstrumentation()
            else
                builder
        builder
