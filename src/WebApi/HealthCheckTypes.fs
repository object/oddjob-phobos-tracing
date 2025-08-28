namespace Nrk.Oddjob.WebApi

module rec HealthCheckTypes =

    open System
    open Akka.Event
    open Nrk.Oddjob.Core

    type ComponentName = string
    type CheckerInterval = TimeSpan option

    /// Any message related to reason of ok/failure. This message is currently dropped and not displayed.
    type HealthCheckStatusMessage = string

    type HealthCheckResult =
        | Ok of ComponentName * HealthMetrics list
        | Warning of ComponentName * HealthCheckStatusMessage * HealthMetrics list
        | Error of ComponentName * HealthCheckStatusMessage * HealthMetrics list

    [<NoEquality; NoComparison>]
    type CheckerSpec =
        {
            Name: ComponentName
            Check: ILoggingAdapter -> HealthCheckResult
            CheckInterval: TimeSpan option
            CheckIntervalOffset: TimeSpan Option
        }

    [<NoEquality; NoComparison>]
    type GroupSpec =
        {
            Name: ComponentName
            SubComponents: ComponentSpec list
            CalculateStatus: HealthCheckResult list -> HealthCheckResult
        }

    [<NoEquality; NoComparison>]
    type ComponentSpec =
        | CheckerSpec of CheckerSpec
        | GroupSpec of GroupSpec

        static member WithChecker(name, check, checkInterval, checkIntervalOffset) =
            CheckerSpec
                {
                    Name = name
                    Check = check
                    CheckInterval = checkInterval
                    CheckIntervalOffset = checkIntervalOffset
                }
        static member WithComponents(name, components, calculateStatus) =
            GroupSpec
                {
                    Name = name
                    SubComponents = components
                    CalculateStatus = calculateStatus
                }
        member this.Name =
            match this with
            | CheckerSpec ch -> ch.Name
            | GroupSpec g -> g.Name

    type ComponentStatus =
        {
            Name: ComponentName
            Updated: DateTimeOffset
            Status: HealthCheckResult
            StatusMessage: string
            Components: Map<ComponentName, ComponentStatus>
        }

        static member Initial name =
            {
                Name = name
                Updated = DateTimeOffset.MinValue
                Status = HealthCheckResult.Ok(name, [])
                StatusMessage = null
                Components = Map.empty
            }

    module Strategies =
        let singleFailureFailsAll name (childrenStatuses: HealthCheckResult list) : HealthCheckResult =
            childrenStatuses
            |> List.fold
                (fun acc elt ->
                    let appendComponentMessage accText childName msg =
                        accText + Environment.NewLine + $"{childName}: {msg}"

                    match elt, acc with
                    | HealthCheckResult.Ok _, _ -> acc
                    | HealthCheckResult.Warning(_, msg, _), HealthCheckResult.Ok _ ->
                        HealthCheckResult.Warning(name, appendComponentMessage "One or more child components failed" name msg, [])
                    | HealthCheckResult.Error(_, msg, _), HealthCheckResult.Ok _ ->
                        HealthCheckResult.Error(name, appendComponentMessage "One or more child components failed" name msg, [])
                    | HealthCheckResult.Warning(childName, msg, _), HealthCheckResult.Warning(_, msgs, _) ->
                        HealthCheckResult.Warning(name, appendComponentMessage msgs childName msg, [])
                    | HealthCheckResult.Warning(childName, msg, _), HealthCheckResult.Error(_, msgs, _) ->
                        HealthCheckResult.Error(name, appendComponentMessage msgs childName msg, [])
                    | HealthCheckResult.Error(childName, msg, _), HealthCheckResult.Warning(_, msgs, _)
                    | HealthCheckResult.Error(childName, msg, _), HealthCheckResult.Error(_, msgs, _) ->
                        HealthCheckResult.Error(name, appendComponentMessage msgs childName msg, []))
                (HealthCheckResult.Ok(name, []))

        let percentageOfFailuresFailsAll name (maxPercentageOfFailures: int) (childrenStatuses: HealthCheckResult list) =
            if maxPercentageOfFailures < 0 || maxPercentageOfFailures > 100 then
                invalidArg "minPercentageForSuccess" $"Incorrect percentage value %d{maxPercentageOfFailures}"
            let failuresCount = childrenStatuses |> List.where _.IsError |> List.length
            let failuresPercentage = int <| (float failuresCount / float childrenStatuses.Length) * 100.
            if failuresPercentage > maxPercentageOfFailures then
                HealthCheckResult.Error(name, sprintf "Failed more than %d%% child components" maxPercentageOfFailures, [])
            else
                HealthCheckResult.Ok(name, [])
