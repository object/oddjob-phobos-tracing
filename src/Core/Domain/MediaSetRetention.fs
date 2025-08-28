namespace Nrk.Oddjob.Core

module MediaSetRetention =

    open System

    type RetentionJob = { TriggerTime: DateTimeOffset }

    let tryGetRetentionPeriod (requestSource: string) (forwardedFrom: string option) (retentionPeriods: Map<string, TimeSpan>) =
        match retentionPeriods |> Map.tryFind "*" with
        | Some x -> Some x
        | None ->
            [ Some requestSource; forwardedFrom ]
            |> List.choose (Option.bind (fun source -> Map.tryFind source retentionPeriods))
            |> List.tryHead

    let createClearJob (mediaSetId: MediaSetId) =
        MediaSetJob.ClearMediaSet
            {
                Header = JobHeader.Internal
                MediaSetId = mediaSetId
            }

    let createRetentionJob cleanupInterval =
        let now = DateTimeOffset.Now
        let triggerTime = now.AddSafely(cleanupInterval)
        { TriggerTime = triggerTime }
