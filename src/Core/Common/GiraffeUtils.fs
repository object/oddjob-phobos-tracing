namespace Nrk.Oddjob.Core

module GiraffeUtils =
    open System
    open Giraffe
    open Akka
    open Akka.Actor
    open Akka.Event
    open Microsoft.Extensions.Logging

    type AkkaLoggerAdapter(system: ActorSystem, minLevel) =

        interface ILogger with
            member _.BeginScope<'TState>(_state: 'TState) = null
            member _.IsEnabled(level) = level >= minLevel
            member this.Log<'TState>(level, _, state: 'TState, exn, fn) =
                if (this :> ILogger).IsEnabled(level) then
                    let msg = fn.Invoke(state, exn)
                    if isNull exn then
                        let akkaLevel =
                            match level with
                            | LogLevel.Debug
                            | LogLevel.Trace -> Event.LogLevel.DebugLevel
                            | LogLevel.Information -> Event.LogLevel.InfoLevel
                            | LogLevel.Warning -> Event.LogLevel.WarningLevel
                            | _ -> Event.LogLevel.ErrorLevel
                        system.Log.Log(akkaLevel, msg)
                    else
                        system.Log.Error(exn, msg)

    type AkkaLoggerProvider(system: ActorSystem, minLevel) =
        interface ILoggerProvider with
            member _.CreateLogger _ =
                AkkaLoggerAdapter(system, minLevel) :> ILogger
            member _.Dispose() = ()

    let errorHandler (ex: Exception) (logger: ILogger) =
        logger.LogError(EventId(), ex, "An unhandled exception has occurred while executing the request.")
        clearResponse >=> ServerErrors.INTERNAL_ERROR ex.Message
