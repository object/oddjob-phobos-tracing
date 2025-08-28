namespace Nrk.Oddjob.Service

module Main =

    open Microsoft.Extensions.Configuration
    open Microsoft.Extensions.Hosting
    open Microsoft.Extensions.Logging
    open Serilog

    open Nrk.Oddjob.Core
    open HostBuilder

    [<EntryPoint>]
    let main args =

        configureAppStartupLogging ApplicationName

        let suffixBuilder = ConfigurationBuilder().AddJsonFile("config/appSettings/appsettings.json").Build()
        let suffix =
            suffixBuilder.Item "AppSettings:Environment"
            |> function
                | "Test"
                | null -> ""
                | env -> $".{env}"

        let configRoot =
            ConfigurationBuilder()
                .AddJsonFile("config/appSettings/appsettings.json")
                .AddJsonFile($"config/appSettings/OddjobSettings{suffix}.json")
                .AddJsonFile($"config/appSettings/QueuesSettings{suffix}.json")
                .AddJsonFile("config/appSettings/QueuesConnections.json")
                .AddEnvironmentVariables(prefix = "Oddjob_")
                .AddCommandLine(args)
                .Build()

        let builder =
            Host
                .CreateDefaultBuilder(args)
                .ConfigureLogging(fun loggingBuilder -> loggingBuilder.AddEventLog() |> ignore)
                .UseSerilog(Log.Logger)
                .UseWindowsService()
                .ConfigureCluster(configRoot)

        builder.Build()
        |> fun app ->
            app.Run()
            0
