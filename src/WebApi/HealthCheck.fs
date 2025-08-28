namespace Nrk.Oddjob.WebApi

module HealthCheck =

    open System
    open Akka.Actor
    open Akka.Cluster.Tools.Singleton
    open Akkling
    open Azure.Messaging.ServiceBus.Administration


    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.WebApi.HealthCheckTypes

    let createRootComponentSpecification system (oddjobConfig: OddjobConfig) queuesConfig (connectionStrings: ConnectionStrings) =
        let S3ComponentName = "S3"
        let GranittComponentName = "Granitt"
        let ArchiveComponentName = "Archive"
        let QueuesComponentName = "Queues"
        let ClusterComponentName = "Cluster"
        let EventStoreComponentName = "EventStore"
        let RemindersComponentName = "Reminders"
        let RootComponentName = "Oddjob"

        let components = oddjobConfig.HealthCheck.Components

        let isEnabled (componentName: ComponentName) : bool =
            components
            // Look for component entry in the config. Only enable component if parent is enabled.
            |> Seq.tryFind (fun x -> x.name = componentName)
            |> Option.map _.enabled
            |> Option.defaultValue false

        let getCheckInterval (parentName: ComponentName) (componentName: ComponentName) : CheckerInterval =
            components
            // Look for component entry in the config. If component is not found, fall back to component parent's entry.
            |> Seq.tryFind (fun x -> x.name = componentName || x.name = parentName)
            |> Option.map (fun x -> x.intervalSeconds |> float |> TimeSpan.FromSeconds)

        {
            GroupSpec.Name = RootComponentName
            SubComponents =
                seq {
                    if isEnabled RootComponentName then
                        if isEnabled GranittComponentName then
                            yield
                                ComponentSpec.WithChecker(
                                    GranittComponentName,
                                    PsUtils.getGranittHealth connectionStrings.GranittMySql,
                                    getCheckInterval RootComponentName GranittComponentName,
                                    None
                                )
                        if isEnabled ArchiveComponentName then
                            yield PsUtils.getArchiveComponentSpec ArchiveComponentName (getCheckInterval ArchiveComponentName) oddjobConfig isEnabled
                        if isEnabled QueuesComponentName then
                            yield QueueUtils.getQueueComponentSpec QueuesComponentName oddjobConfig queuesConfig connectionStrings getCheckInterval isEnabled
                        if isEnabled ClusterComponentName then
                            yield ClusterState.createClusterStateComponent ClusterComponentName (getCheckInterval RootComponentName ClusterComponentName) system
                        if isEnabled EventStoreComponentName then
                            yield
                                ComponentSpec.WithChecker(
                                    EventStoreComponentName,
                                    eventStoreSettings connectionStrings.EventStore |> PotionUtils.getEventStoreHealth,
                                    getCheckInterval RootComponentName EventStoreComponentName,
                                    None
                                )
                        if isEnabled RemindersComponentName then
                            let interval = getCheckInterval RootComponentName RemindersComponentName
                            yield
                                ComponentSpec.WithComponents(
                                    RemindersComponentName,
                                    ReminderUtils.getRemindersHealth connectionStrings.Akka interval oddjobConfig.HealthCheck.TooManyReminders,
                                    Strategies.singleFailureFailsAll RemindersComponentName
                                )
                        if isEnabled S3ComponentName then
                            yield
                                ComponentSpec.WithChecker(
                                    S3ComponentName,
                                    S3Health.getS3Health connectionStrings.AmazonS3 oddjobConfig,
                                    getCheckInterval RootComponentName S3ComponentName,
                                    None
                                )
                }
                |> Seq.toList
            CalculateStatus = Strategies.singleFailureFailsAll RootComponentName
        }

    module rec Actors =
        type QueryState = QueryState
        type UpdateSubcomponentState = UpdateSubcomponentState of newStatus: ComponentStatus
        type EvaluateState = EvaluateState

        let private updateState calculateStatus (current: ComponentStatus) (msg: UpdateSubcomponentState) =
            let (UpdateSubcomponentState subStatus) = msg
            let newComponents = current.Components |> Map.add subStatus.Name subStatus
            let status = calculateStatus (newComponents |> Map.toList |> List.map (fun (_, c) -> c.Status))
            let newState =
                { current with
                    ComponentStatus.Updated = DateTimeOffset.Now
                    Status = status
                    StatusMessage =
                        match status with
                        | HealthCheckResult.Ok _ -> null
                        | HealthCheckResult.Warning(_, error, _)
                        | HealthCheckResult.Error(_, error, _) -> error
                    Components = newComponents
                }
            newState

        let private spawnChildren (parentSpec: GroupSpec) (parent: Actor<obj>) =
            for childSpec in parentSpec.SubComponents do
                match childSpec with
                | CheckerSpec ch ->
                    spawn parent (makeActorName [ ch.Name ]) <| propsNamed "webapi-health-checker" (checkerActor ch (retype parent.Self))
                    |> ignore
                | GroupSpec g ->
                    spawn parent (makeActorName [ g.Name ]) <| propsNamed "webapi-health-group" (groupActor g (retype parent.Self))
                    |> ignore

        let rootActor (spec: GroupSpec) (mailbox: Actor<_>) =
            let rec mapInternalStatusToPublic (status: ComponentStatus) : HealthStatus =
                {
                    Name = status.Name
                    Updated = status.Updated
                    Status =
                        match status.Status with
                        | HealthCheckResult.Ok _ -> HealthCheckStatus.Ok
                        | HealthCheckResult.Warning _ -> HealthCheckStatus.Warning
                        | HealthCheckResult.Error _ -> HealthCheckStatus.Fail
                    StatusMessage = status.StatusMessage
                    Metrics =
                        match status.Status with
                        | HealthCheckResult.Ok(_, m) -> m
                        | HealthCheckResult.Warning(_, _, m) -> m
                        | HealthCheckResult.Error(_, _, m) -> m
                    Components = status.Components |> Map.toList |> List.map (snd >> mapInternalStatusToPublic)
                }

            let rec loop status =
                actor {
                    let! (msg: obj) = mailbox.Receive()
                    return!
                        match msg with
                        | LifecycleEvent e ->
                            match e with
                            | PreStart -> spawnChildren spec mailbox
                            | _ -> ()
                            ignored ()
                        | :? UpdateSubcomponentState as msg ->
                            let status = updateState spec.CalculateStatus status msg
                            loop status
                        | :? QueryState ->
                            let statusToDisplay = mapInternalStatusToPublic status
                            mailbox.Sender() <! statusToDisplay
                            ignored ()

                        | _ -> unhandled ()
                }
            logDebug mailbox $"Creating health check of components: [%s{String.Join(',', spec.SubComponents |> Seq.map (_.Name))}]"
            loop (ComponentStatus.Initial spec.Name)

        let groupActor (spec: GroupSpec) (subscriber: IActorRef<UpdateSubcomponentState>) (mailbox: Actor<_>) =
            let rec loop status =
                actor {
                    let! (msg: obj) = mailbox.Receive()
                    return!
                        match msg with
                        | LifecycleEvent e ->
                            match e with
                            | PreStart ->
                                subscriber <! UpdateSubcomponentState status
                                spawnChildren spec mailbox
                            | _ -> ()
                            ignored ()
                        | :? UpdateSubcomponentState as msg ->
                            let status = updateState spec.CalculateStatus status msg
                            subscriber <! UpdateSubcomponentState status
                            loop status
                        | _ -> unhandled ()
                }
            loop (ComponentStatus.Initial spec.Name)

        let checkerActor (spec: CheckerSpec) (subscriber: IActorRef<UpdateSubcomponentState>) (mailbox: Actor<_>) =
            let rec loop status =
                actor {
                    let! (msg: obj) = mailbox.Receive()
                    return!
                        match msg with
                        | LifecycleEvent e ->
                            match e with
                            | PreStart ->
                                spec.CheckInterval
                                |> Option.iter (fun checkInterval ->
                                    mailbox.System.Scheduler.ScheduleTellRepeatedly(
                                        spec.CheckIntervalOffset |> Option.defaultValue TimeSpan.Zero,
                                        checkInterval,
                                        (retype mailbox.Self),
                                        EvaluateState
                                    ))
                                subscriber <! UpdateSubcomponentState status
                            | _ -> ()
                            ignored ()
                        | :? EvaluateState ->
                            logDebug mailbox "Check started"
                            let startTime = DateTimeOffset.Now
                            let result =
                                try
                                    spec.Check(getLoggerForMailbox mailbox)
                                with exn ->
                                    let error = "Failed to execute component checker"
                                    logErrorWithExn mailbox exn error
                                    HealthCheckResult.Error("Component Checker", error, [])
                            let elapsed = DateTimeOffset.Now - startTime
                            logDebug mailbox $"Check completed (elapsed %f{elapsed.TotalSeconds} seconds)"
                            let status =
                                { status with
                                    ComponentStatus.Updated = DateTimeOffset.Now
                                    Status = result
                                    StatusMessage =
                                        match result with
                                        | HealthCheckResult.Ok _ -> null
                                        | HealthCheckResult.Warning(_, error, _)
                                        | HealthCheckResult.Error(_, error, _) -> error
                                }

                            match result with
                            | HealthCheckResult.Ok _ -> ()
                            | HealthCheckResult.Warning(componentName, statusMessage, healthMetrics)
                            | HealthCheckResult.Error(componentName, statusMessage, healthMetrics) ->
                                let hmStrings = healthMetrics |> List.map (fun hm -> $"Name: {hm.Name} Value: {hm.Value} ")
                                let hmString = String.concat ", " hmStrings
                                logError
                                    mailbox
                                    $"Health check failed. Component name: {componentName} Status message: {statusMessage} HealthMetrics: {hmString}"

                            subscriber <! UpdateSubcomponentState status
                            loop status
                        | _ -> unhandled ()
                }
            loop (ComponentStatus.Initial spec.Name)

    let spawnHealthChecker (system: ActorSystem) oddjobConfig queuesConfig connectionStrings : IActorRef<Actors.QueryState> =
        let componentsSpec = createRootComponentSpecification system oddjobConfig queuesConfig connectionStrings
        let actorNameBase = "Health"
        let actorName = makeActorName [ actorNameBase ]
        let healthCheckerManager =
            system.ActorOf(
                ClusterSingletonManager.Props(
                    (propsNamed "webapi-health-check" (Actors.rootActor componentsSpec)).ToProps(),
                    ClusterSingletonManagerSettings.Create(system).WithRole("WebApi")
                ),
                actorName
            )
        typed
        <| system.ActorOf(
            ClusterSingletonProxy.Props(healthCheckerManager.Path.ToStringWithoutAddress(), ClusterSingletonProxySettings.Create(system).WithRole("WebApi")),
            $"{actorName}-proxy"
        )
