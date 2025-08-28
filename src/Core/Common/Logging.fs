namespace Nrk.Oddjob.Core

[<AutoOpen>]
module Logging =
    open System
    open Akka.Actor
    open Akka.Event
    open Akkling
    open Microsoft.Extensions.Configuration
    open Serilog
    open Nrk.Oddjob.Core.Config

    let private logToConsole = true

    let inline getLoggerForContext (context: IActorContext) =
        context.GetLogger(DefaultLogMessageFormatter.Instance)

    let inline getLoggerForMailbox (mailbox: Actor<_>) =
        mailbox.UntypedContext.GetLogger(DefaultLogMessageFormatter.Instance)

    let logErrorWithExn mailbox (exn: Exception) msg =
        logError mailbox $"{msg}{Environment.NewLine}{Environment.NewLine}{exn}"

    let configureConsole (config: LoggerConfiguration) =
        match logToConsole with
        | false -> config
        | true -> config.WriteTo.Console().MinimumLevel.Debug()

    let configureOpenTelemetry (applicationName: string) (configRoot: IConfigurationRoot) (config: LoggerConfiguration) =
        let otelLogsConnectionstring = configRoot.GetConnectionString "OtelCollector" |> parseConnectionString
        if String.IsNullOrEmpty otelLogsConnectionstring["Uri"] then
            config
        else
            config.WriteTo.OpenTelemetry(fun opts ->
                opts.Endpoint <- otelLogsConnectionstring["Uri"]
                opts.ResourceAttributes <- [ ("app", applicationName :> obj) ] |> dict)

    let configureLogging configRoot (applicationName: string) (environment: string) featureFlags =
        Quartz.Logging.LogProvider.IsDisabled <- getFeatureFlag featureFlags ConfigKey.EnableQuartzLogging false |> not
        let config = LoggerConfiguration() |> configureConsole |> configureOpenTelemetry applicationName configRoot
        let logger =
            config.Enrich
                .FromLogContext()
                .MinimumLevel.Debug()
                .Enrich.WithProperty("MachineName", Environment.MachineName)
                .Enrich.WithProperty("ApplicationName", applicationName)
                .CreateLogger()
        Log.Logger <- logger

    let configureAppStartupLogging (applicationName: string) =
        // This is/should later overwritten by configureLogging above, and is here to ensure logging is available from application start up until the actor system is created
        Log.Logger <-
            LoggerConfiguration()
                .MinimumLevel.Debug()
                .Enrich.FromLogContext()
                .Enrich.WithProperty("MachineName", Environment.MachineName)
                .Enrich.WithProperty("ApplicationName", applicationName)
                .WriteTo.Console()
                .WriteTo.Async(fun a ->
                    a.File(
                        $"{applicationName}-log.txt",
                        rollingInterval = RollingInterval.Day,
                        retainedFileCountLimit = 7,
                        fileSizeLimitBytes = 10000000L,
                        shared = false
                    )
                    |> ignore)
                .CreateLogger()
        Log.Logger.Information("Starting up")

        // This will stay in place to catch any unhandled exceptions even after configureLogging has been called
        AppDomain.CurrentDomain.UnhandledException.AddHandler(fun _ e ->
            Log.Logger.Fatal(e.ExceptionObject :?> Exception, "Unhandled exception")
            Log.CloseAndFlush())
