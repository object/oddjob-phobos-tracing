namespace Nrk.Oddjob.Core

module Reminders =

    open System
    open Akka.Quartz.Actor.Commands
    open Akkling
    open Quartz

    [<Literal>]
    let ClearMediaSet = "ClearMediaSet"

    [<Literal>]
    let StorageCleanup = "StorageCleanup"

    [<Literal>]
    let PotionBump = "PotionBump"

    [<Literal>]
    let PlayabilityBump = "PlayabilityBump"

    [<Literal>]
    let UploadRole = "Upload"

    [<Literal>]
    let PsRole = "Ps"

    [<Literal>]
    let PotionRole = "Potion"

    type ReminderCreated = Akka.Quartz.Actor.Events.JobCreated
    type ReminderRemoved = Akka.Quartz.Actor.Events.JobRemoved
    type ReminderNotRemoved = Akka.Quartz.Actor.Events.RemoveJobFail

    let (|LifetimeEvent|_|) (event: obj) =
        match event with
        | :? ReminderCreated -> Some event
        | :? ReminderRemoved -> Some event
        | :? ReminderNotRemoved -> Some event
        | _ -> None

    let createTriggerForSingleReminder groupName taskId triggerTime =
        TriggerBuilder.Create().WithIdentity(taskId, groupName).ForJob(taskId, groupName).StartAt(triggerTime).Build()

    let createTriggerForRepeatingReminder groupName taskId triggerTime interval =
        TriggerBuilder
            .Create()
            .WithIdentity(taskId, groupName)
            .ForJob(taskId, groupName)
            .StartAt(triggerTime)
            .WithSimpleSchedule(fun s -> s.RepeatForever().WithInterval(interval) |> ignore)
            .Build()

    let createOrUpdateReminder scheduler (trigger: ITrigger) targetPath message =
        typed scheduler <! RemoveJob(trigger.JobKey, trigger.Key)
        Async.Sleep(int 100) |> Async.RunSynchronously
        typed scheduler <! CreatePersistentJob(targetPath, message, trigger)

    let scheduleReminderTask scheduler groupName message targetPath triggerTime logger =
        let taskId = string (Guid.NewGuid())
        logger $"Scheduling one-time reminder {groupName}.{taskId} at {triggerTime}"
        let trigger = createTriggerForSingleReminder groupName taskId triggerTime
        typed scheduler <! CreatePersistentJob(targetPath, message, trigger)

    let rescheduleReminderTask scheduler groupName taskId message targetPath triggerTime logger =
        logger $"Rescheduling one-time reminder {groupName}.{taskId} at {triggerTime}"
        let trigger = createTriggerForSingleReminder groupName taskId triggerTime
        createOrUpdateReminder scheduler trigger targetPath message

    let scheduleRepeatingReminderTask scheduler groupName taskId message targetPath triggerTime interval logger =
        logger $"Scheduling repeating reminder {groupName}.{taskId} at {triggerTime} with interval {interval}"
        let trigger = createTriggerForRepeatingReminder groupName taskId triggerTime interval
        typed scheduler <! CreatePersistentJob(targetPath, message, trigger)

    let rescheduleRepeatingReminderTask scheduler groupName taskId message targetPath triggerTime interval logger =
        logger $"Rescheduling repeating reminder {groupName}.{taskId} at {triggerTime} with interval {interval}"
        let trigger = createTriggerForRepeatingReminder groupName taskId triggerTime interval
        createOrUpdateReminder scheduler trigger targetPath message

    let cancelReminderTask scheduler groupName taskId logger =
        logger $"Cancelling repeating reminder {groupName}.{taskId}"
        let jobKey = JobKey(taskId, groupName)
        let triggerKey = TriggerKey(taskId, groupName)
        typed scheduler <! RemoveJob(jobKey, triggerKey)

module QuartzBootstrapper =

    open System.Collections.Specialized
    open Akka.Actor
    open Quartz.Impl
    open Nrk.Oddjob.Core

    let private getSchedulerProperties connectionString instanceName quartzSettings =
        let properties = NameValueCollection()
        [
            "quartz.scheduler.instanceId"
            "quartz.serializer.type"
            "quartz.jobStore.driverDelegateType"
            "quartz.jobStore.dataSource"
            "quartz.jobStore.clustered"
            "quartz.jobStore.type"
            "quartz.dataSource.default.provider"
        ]
        |> Seq.iter (fun key -> properties.Add(key, quartzSettings key))
        properties.Add("quartz.scheduler.instanceName", instanceName)
        properties.Add("quartz.dataSource.default.connectionString", connectionString)
        properties

    let getQuartzScheduler connectionString roleName quartzSettings =
        let properties =
            if String.isNullOrEmpty roleName then
                getSchedulerProperties connectionString "Oddjob" quartzSettings
            else
                getSchedulerProperties connectionString $"Oddjob-{roleName}" quartzSettings
        let sf = StdSchedulerFactory(properties)
        sf.GetScheduler() |> Async.AwaitTask |> Async.RunSynchronously

    let private getScheduler (actorSystem: ActorSystem) connectionString roleName shouldStart quartzSettings =
        let scheduler = getQuartzScheduler connectionString roleName quartzSettings
        let actorName =
            if String.isNullOrEmpty roleName then
                [ "Quartz"; "Scheduler"; "Common" ]
            else
                [ "Quartz"; "Scheduler"; roleName ]
            |> makeActorName
        if shouldStart then
            scheduler.Start() |> Async.AwaitTask |> Async.RunSynchronously
        let schedulerActor =
            actorSystem.ActorOf(Props.Create(fun () -> Akka.Quartz.Actor.QuartzPersistentActor(scheduler)), actorName)
        (scheduler, schedulerActor)

    let startCommonScheduler (actorSystem: ActorSystem) connectionString quartzSettings =
        getScheduler (actorSystem: ActorSystem) connectionString null true quartzSettings

    let startRoleScheduler (actorSystem: ActorSystem) connectionString roleName quartzSettings =
        getScheduler (actorSystem: ActorSystem) connectionString roleName true quartzSettings

    let getCommonScheduler (actorSystem: ActorSystem) connectionString quartzSettings =
        getScheduler (actorSystem: ActorSystem) connectionString null false quartzSettings

    let getRoleScheduler (actorSystem: ActorSystem) connectionString roleName quartzSettings =
        getScheduler (actorSystem: ActorSystem) connectionString roleName false quartzSettings
