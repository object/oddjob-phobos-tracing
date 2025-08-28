namespace Nrk.Oddjob.WebApi

module ReminderUtils =

    open PublicTypes
    open HealthCheckTypes
    open ReminderGroups

    let private getReminderHealth akkaConnectionString groupName tooManyReminders _ =
        let reminderCount =
            match getGroupResults akkaConnectionString groupName 0 0 null true |> Seq.tryHead with
            | Some(ReminderGroupResult.Count count) -> count
            | _ -> -1

        let metrics =
            [
                {
                    Name = "Count"
                    Value = int64 reminderCount
                }
            ]
        if reminderCount < 0 then
            HealthCheckResult.Error(groupName, "Failure to retrieve reminders", metrics)
        else if reminderCount < tooManyReminders then
            HealthCheckResult.Ok($"Reminder count is {reminderCount}", metrics)
        else
            HealthCheckResult.Warning(groupName, $"Reminder count is {reminderCount} >= {tooManyReminders}", metrics)

    let getRemindersHealth akkaConnectionString interval tooManyReminders =
        allGroups
        |> Seq.toList
        |> List.map (fun groupName -> ComponentSpec.WithChecker(groupName, getReminderHealth akkaConnectionString groupName tooManyReminders, interval, None))
