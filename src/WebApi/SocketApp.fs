namespace Nrk.Oddjob.WebApi

module SocketApp =

    open Akka.Actor
    open Giraffe
    open Microsoft.AspNetCore.Hosting
    open Microsoft.AspNetCore.Builder
    open Microsoft.Extensions.DependencyInjection
    open Microsoft.Extensions.Logging

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config

    open SocketMessages

    module SocketServer =

        open Elmish
        open Elmish.Bridge

        type ConnectionState =
            | Connected
            | Disconnected

        let serverHub =
            ServerHub<ConnectionState, ServerMsg, ServerEvent>().RegisterServer(ClientCommand).RegisterServer(ServerMsg.ServerEvent)

        let update clientDispatch msg state =
            match msg with
            | ClientCommand msg ->
                match msg with
                | Connect -> Connected, Cmd.none
            | ServerMsg.ServerEvent msg ->
                msg |> clientDispatch
                state, Cmd.none
            | Closed -> Disconnected, Cmd.none

        let init _ () = Disconnected, Cmd.none

        let socketServer: HttpHandler =
            Bridge.mkServer "/socket" init update
            |> Bridge.register ClientCommand
            |> Bridge.register ServerEvent
            |> Bridge.whenDown Closed
            |> Bridge.withServerHub serverHub
            |> Bridge.run Giraffe.server

    let webApp config =
        let system, _ = config
        PubSubSubscriber.spawnPubSubSubscriber system SocketServer.serverHub |> ignore

        choose [ SocketServer.socketServer ]

    let private configureApp config (appBuilder: IApplicationBuilder) =
        appBuilder.UseWebSockets().UseGiraffeErrorHandler(GiraffeUtils.errorHandler).UseGiraffe(webApp config)

    let private configureServices (services: IServiceCollection) =
        services.AddGiraffe().AddSingleton<Json.ISerializer>(Thoth.Json.Giraffe.ThothSerializer()) |> ignore

    let private configureLogging system (builder: ILoggingBuilder) =
        let provider = new GiraffeUtils.AkkaLoggerProvider(system, LogLevel.Debug)
        builder.AddProvider(provider).AddConsole().AddDebug() |> ignore

    let startApp (system: ActorSystem) oddjobConfig =
        let url = sprintf "http://%s:%d" "0.0.0.0" oddjobConfig.WebApi.SocketPortNumber
        let config = system, oddjobConfig
        let server =
            WebHostBuilder()
                .UseKestrel()
                .Configure(System.Action<IApplicationBuilder>(configureApp config))
                .ConfigureServices(configureServices)
                .ConfigureLogging(configureLogging system)
                .UseUrls(url)
                .Build()
        server.Start()
        server
