namespace Nrk.Oddjob.WebApi

open System
open Akka.Actor
open Giraffe
open Giraffe.EndpointRouting
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Hosting
open Microsoft.AspNetCore.Builder
open Microsoft.Extensions.DependencyInjection
open Microsoft.Extensions.Logging

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Config
open WebAppUtils

module WebApp =

    let toResponseWithSequenceFilterTask<'T> (f: int -> int -> Async<seq<'T>>) =
        fun _next (ctx: HttpContext) ->
            task {
                let fromSequenceNr = parseQueryParam (ctx.Request.Query.Item "from") TryParser.parseInt 0
                let toSequenceNr = parseQueryParam (ctx.Request.Query.Item "to") TryParser.parseInt 0
                let! result = f fromSequenceNr toSequenceNr
                ctx.SetHttpHeader(AccessControlHeader, "*")
                return! ctx.NegotiateWithAsync(rules, unacceptableHandler, (result |> Seq.toList))
            }

    let private configureApp config (appBuilder: IApplicationBuilder) =
        appBuilder.UseRouting().UseSwagger().UseSwaggerUI().UseGiraffe(Endpoints.buildEndpoints config).UseGiraffe(text "Not Found" |> RequestErrors.notFound)

    let private configureServices (services: IServiceCollection) =
        services.AddRouting().AddGiraffe().AddEndpointsApiExplorer().AddSwaggerGen().AddSingleton<Json.ISerializer>(SystemTextJson.Serializer())
        |> ignore

    let private configureLogging system (builder: ILoggingBuilder) =
        let provider = new GiraffeUtils.AkkaLoggerProvider(system, LogLevel.Debug)
        builder.AddProvider(provider).AddConsole().AddDebug() |> ignore

    let startApp aprops (system: ActorSystem) oddjobConfig queuesConfig statusPersistenceActor activityContext =
        let appLocation = System.Reflection.Assembly.GetEntryAssembly().Location
        let contentRoot = appLocation.Substring(0, appLocation.LastIndexOf("\\") + 1)
        let url = sprintf "http://%s:%d" "0.0.0.0" oddjobConfig.WebApi.PortNumber
        let config = aprops, contentRoot, system, oddjobConfig, queuesConfig, statusPersistenceActor, activityContext
        let server =
            WebHostBuilder()
                .UseKestrel()
                .UseContentRoot(contentRoot)
                .Configure(configureApp config)
                .ConfigureServices(configureServices)
                .ConfigureLogging(configureLogging system)
                .UseUrls(url)
                .Build()
        server.Start()
        server
