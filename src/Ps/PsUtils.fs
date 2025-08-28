namespace Nrk.Oddjob.Ps

open System
open FsHttp

open Nrk.Oddjob.Core
open Nrk.Oddjob.Ps.PsTypes

module Utils =

    let createFileName (filename: string) bitRate =
        let name = IO.getFileNameWithoutExtension filename
        let basePart = name |> String.splitByLast '_' |> Option.map fst |> Option.defaultValue name
        let ext = System.IO.Path.GetExtension(filename)
        FileName.create basePart bitRate ext

    let getFilesFromTranscodedFiles transcodedFiles =
        match transcodedFiles with
        | PsTranscodedFiles.Video files ->
            files
            |> List.map (fun f ->
                {
                    Path = f.FilePath
                    Length = IO.getFileLengthSafely f.FilePath
                    Modified = IO.getLastModifiedTime f.FilePath
                })
        | PsTranscodedFiles.LegacyVideo files ->
            files
            |> List.map (fun f ->
                {
                    Path = f.FilePath
                    Length = IO.getFileLengthSafely f.FilePath
                    Modified = IO.getLastModifiedTime f.FilePath
                })
        | PsTranscodedFiles.Audio files ->
            files
            |> List.map (fun f ->
                {
                    Path = f.FilePath
                    Length = IO.getFileLengthSafely f.FilePath
                    Modified = IO.getLastModifiedTime f.FilePath
                })
        | PsTranscodedFiles.LegacyAudio files ->
            files
            |> List.map (fun f ->
                {
                    Path = f.FilePath
                    Length = IO.getFileLengthSafely f.FilePath
                    Modified = IO.getLastModifiedTime f.FilePath
                })

    let startPlayabilityReminder psScheduler programId recipientPath interval =
        // For repeatedly waking up Playback actor until there are no pending events
        let bumpMessage: obj =
            {
                PsDto.PlayabilityBump.ProgramId = programId
            }
        let interval = interval
        let triggerTime = DateTimeOffset.Now.AddSafely(interval)
        Reminders.rescheduleRepeatingReminderTask psScheduler Reminders.PlayabilityBump programId bumpMessage recipientPath triggerTime interval

module Rights =

    open Nrk.Oddjob.Core.Granitt

    type RightsInfo =
        {
            Region: string
            PublishStart: DateTime
            PublishEnd: DateTime option
        }

    type UsageRightsService(baseUrl, apiKey) =
        member this.GetUsageRights piProgId =
            let piProgId = PiProgId.value piProgId |> String.toUpper
            let url = $"{baseUrl}/{piProgId}"
            http {
                GET url
                Accept "application/vnd.nrk.usagerights.2+json"
                header "x-api-key" apiKey
            }
            |> Request.send
            |> Response.toResult
            |> Result.map (fun response ->
                response.ToJson().GetProperty("usageRights").EnumerateArray()
                |> Seq.map (fun x ->
                    {
                        Region = x.GetProperty("region").GetString()
                        PublishStart = x.GetProperty("from").GetDateTime()
                        PublishEnd =
                            match x.TryGetProperty("to") with
                            | true, x -> x.GetDateTime() |> Some
                            | false, _ -> None
                    })
                |> Seq.toArray)

    type RightsUploadDetails =
        {
            Geolocation: GranittGeorestriction
            /// Includes advanced publishing
            PublishStart: DateTime
            /// Does not include advanced publishing
            FirstPublishStart: DateTime
            ScheduleCleanup: DateTime option
        }

    type DeleteOrUploadProgram =
        | Delete
        | Upload of RightsUploadDetails

    type RightsAction =
        {
            DeleteOrUpload: DeleteOrUploadProgram option
            ScheduleUpload: DateTime option
        }

    type InvalidRights = string

    type ProgramRightsAnalyzer(advancedPublishing: TimeSpan, delayedCleanup: TimeSpan, now: unit -> DateTime) =
        do
            if advancedPublishing.Ticks < 0L then
                invalidArg "advancedPublishing" "cannot be negative"

            if delayedCleanup.Ticks < 0L then
                invalidArg "delayedCleanup" "cannot be negative"

        let rnd = Random()

        let tryFindOverlapping (programRights: PsProgramRights list) =
            let isOverlapping (left: PsProgramRights, right: PsProgramRights) =
                match left.RightsPeriod, right.RightsPeriod with
                | RightsPeriod.Unlimited _, RightsPeriod.Unlimited _ -> true
                | RightsPeriod.Unlimited s, RightsPeriod.Limited(_, e) -> e > s
                | RightsPeriod.Limited(_, e), RightsPeriod.Unlimited s -> e > s
                | RightsPeriod.Limited(s1, e1), RightsPeriod.Limited(s2, e2) -> (s1 < e2 && e1 > s2) || (s2 < e1 && s1 < e2)
            let generatePairs lst =
                let rec impl curr agg =
                    match curr with
                    | [] -> agg
                    | x :: xs ->
                        let streak =
                            [
                                for elem in xs do
                                    yield x, elem
                            ]
                        impl xs (streak @ agg)
                impl lst []
            generatePairs programRights |> List.where isOverlapping

        let containsTime now (before, after) =
            RightsPeriod.contains now before.RightsPeriod || RightsPeriod.contains now after.RightsPeriod

        let createBroadcastingRights (rights: PsProgramRights) =
            PsBroadcastingRights.create
                (DateTimeOffset(RightsPeriod.getPublishStart rights.RightsPeriod))
                (DateTimeOffset(RightsPeriod.getPublishEnd rights.RightsPeriod))
                (GranittGeorestriction.toDomain rights.Geolocation)

        member this.GetCurrentRights(programRights: PsProgramRights list) : PsBroadcastingRights =
            if List.isEmpty programRights then
                PsBroadcastingRights.Unspecified
            else
                let now = now ()
                match tryFindOverlapping programRights with
                | overlapping when overlapping |> List.exists (containsTime now) -> PsBroadcastingRights.Unspecified
                | _ ->
                    // The algorithm here requires rights to be sorted
                    let programRights = programRights |> List.sortBy (fun pr -> RightsPeriod.getPublishStart pr.RightsPeriod)

                    let rights =
                        programRights
                        |> List.pairwise
                        |> List.map (fun (_, next) ->
                            { next with
                                RightsPeriod = next.RightsPeriod
                            })
                        // Append adjusted first element - we know it exists because we checked it at the beginning of the function
                        |> List.append [ List.head programRights ]

                    rights // Try current rights first
                    |> List.tryFind (fun x -> RightsPeriod.contains now x.RightsPeriod)
                    |> Option.map createBroadcastingRights
                    |> Option.defaultValue (
                        rights // Then try future rights
                        |> List.tryFind (fun x -> RightsPeriod.getPublishStart x.RightsPeriod > now)
                        |> Option.map createBroadcastingRights
                        |> Option.defaultValue PsBroadcastingRights.Unspecified
                    )

        member this.GetUploadAction(programRights: PsProgramRights list) : Result<RightsAction, InvalidRights> =
            if List.isEmpty programRights then
                Ok
                    {
                        DeleteOrUpload = Some Delete
                        ScheduleUpload = None
                    }
            else
                let now = now ()
                match tryFindOverlapping programRights with
                | overlapping when overlapping |> List.exists (containsTime now) ->
                    Error $"Found overlapping rights that are relevant to current time (%A{now}): %A{overlapping}"
                | _ ->
                    // The algorithm here requires rights to be sorted
                    let programRights = programRights |> List.sortBy (fun pr -> RightsPeriod.getPublishStart pr.RightsPeriod)

                    let rights =
                        programRights
                        |> List.pairwise
                        |> List.map (fun (prev, next) ->
                            let spanBetweenRights = RightsPeriod.getPublishStart next.RightsPeriod - RightsPeriod.getPublishEnd prev.RightsPeriod
                            let adjustedAdvancedPublishing =
                                if advancedPublishing >= spanBetweenRights then
                                    TimeSpan(int64 (0.9 * float spanBetweenRights.Ticks))
                                else
                                    advancedPublishing
                            { next with
                                RightsPeriod = next.RightsPeriod |> RightsPeriod.movePublishStart -adjustedAdvancedPublishing
                            })
                        // Append adjusted first element - we know it exists because we checked it at the beginning of the function
                        |> List.append
                            [
                                let hd = List.head programRights in

                                yield
                                    { hd with
                                        RightsPeriod = hd.RightsPeriod |> RightsPeriod.movePublishStart -advancedPublishing
                                    }
                            ]

                    let periods = rights |> List.map (_.RightsPeriod)

                    let current = rights |> List.tryFind (fun pr -> RightsPeriod.contains now pr.RightsPeriod)
                    let past = periods |> List.tryFindBack (fun period -> RightsPeriod.getPublishEnd period < now)
                    let future = periods |> List.tryFind (fun period -> RightsPeriod.getPublishStart period > now)

                    let deleteOrUpload =
                        match past, current with
                        | None, None -> None
                        | Some _, None -> Some Delete
                        | _, Some current ->
                            // We know the list has at least one element - namely, `current`
                            let info =
                                {
                                    Geolocation = current.Geolocation
                                    ScheduleCleanup =
                                        RightsPeriod.tryGetPublishEnd current.RightsPeriod
                                        |> Option.map (_.AddSafely(delayedCleanup + TimeSpan(rnd.Next(3, 12), 0, 0)))
                                    PublishStart = RightsPeriod.getPublishStart current.RightsPeriod
                                    FirstPublishStart = RightsPeriod.getPublishStart (programRights |> List.head).RightsPeriod
                                }
                            Some(Upload info)
                    let schedule = future |> Option.map RightsPeriod.getPublishStart

                    Ok
                        {
                            DeleteOrUpload = deleteOrUpload
                            ScheduleUpload = schedule
                        }

module Retention =
    open System.IO
    open System.Text.RegularExpressions
    open Nrk.Oddjob.Core.Config

    type RetentionExpiration =
        | Expire of DateTime
        | NeverExpire
        | NonApplicable

    type RetentionExceptions =
        {
            Programs: string array
            ForcedSources: Map<string, TimeSpan>
        }

        static member Zero =
            {
                Programs = Array.empty
                ForcedSources = Map.empty
            }

    module RetentionExceptions =
        // Retention policy can have exceptions from rule loaded from an exception file.
        // The exception file uses YAML format and has the following structure:
        // Programs:
        //  - program1
        //  - program2
        //  - ...
        // Programs can be specified using exact PI program ID (e.g. NNFA53030117) or a string with wild character (e.g. DKSF43000*)
        let fromConfig (oddjobConfig: OddjobConfig) =
            let retentionPeriod = oddjobConfig.Ps.RetentionPeriod
            let programs =
                if File.Exists retentionPeriod.Except then
                    let exceptionsConfig = loadExpirationExceptionsConfiguration retentionPeriod.Except
                    exceptionsConfig.Programs
                else
                    Array.empty
            let forcedSources =
                retentionPeriod.Sources
                |> List.ofArrayOrNull
                |> Seq.map (fun x -> x.source, TimeSpan.FromDays(float x.days))
                |> Map.ofSeq
            {
                Programs = programs
                ForcedSources = forcedSources
            }

        let calculateExpiration (exceptions: RetentionExceptions) piProgId source forwardedFrom =

            let isInProgramExceptionList () =
                exceptions.Programs
                |> Seq.where String.isNotNullOrEmpty
                |> Seq.map (_.Replace("*", ".*").Replace("?", "."))
                |> Seq.exists (fun pattern -> Regex.Match(PiProgId.value piProgId, pattern, RegexOptions.IgnoreCase).Success)

            if isInProgramExceptionList () then
                RetentionExpiration.NeverExpire
            else
                MediaSetRetention.tryGetRetentionPeriod source forwardedFrom exceptions.ForcedSources
                |> Option.map (fun expiration -> RetentionExpiration.Expire(DateTime.Now.Add expiration))
                |> Option.defaultValue RetentionExpiration.NonApplicable

module Priority =
    open System.IO
    open Nrk.Oddjob.Core.Config

    type MessageSource = string
    type Priority = byte

    type PublishedStatus =
        | ActiveAfter of DateTime
        | ActiveSince of DateTime
        | Inactive

    type RightsRange = int * int

    type Rights =
        | ActiveAfter of RightsRange option
        | ActiveSince of RightsRange option
        | Any

        static member AllowedRights = [ "activeAfter"; "activeSince"; "*" ]
        static member Parse (rights: string) (range: string) =
            let parseRange (str: string) =
                match str with
                | "*" -> None
                | _ -> str.Split "-" |> fun x -> Some(Int32.Parse x[0], Int32.Parse x[1])
            match rights with
            | "activeAfter" -> ActiveAfter(parseRange range)
            | "activeSince" -> ActiveSince(parseRange range)
            | "*" -> Any
            | x -> invalidArg "str" (sprintf "%s in not valid Rights" x)

    [<NoComparison; NoEquality>]
    type PriorityConfig =
        {
            Exceptions: HighPriorityYaml option
            ExceptionsPriority: Priority
            ExceptionsCutoff: TimeSpan
            DefaultPriority: Priority
            Sources: (MessageSource * Rights * Priority) list
        }

        static member Create(oddjobConfig: OddjobConfig) =
            let config = oddjobConfig.Ps.MessagePriorities
            let priorities = config.Sources
            let defaultPriority: Priority =
                match priorities |> Seq.tryFind (fun p -> p.source = "*" && p.rights = "*") with
                | Some p -> byte p.priority
                | None -> failwith "Missing default priority in configuration"
            if priorities |> Seq.exists (fun p -> String.IsNullOrWhiteSpace p.source) then
                failwith "Empty source is not allowed"
            if priorities |> Seq.map (_.priority) |> Seq.append [ config.ExceptionsPriority ] |> Seq.exists (fun p -> p < 0 || p > 10) then
                failwith "Allowed range for priorities is [0..10]"
            priorities
            |> Seq.tryFind (fun p -> Rights.AllowedRights |> List.contains p.rights |> not)
            |> Option.iter (fun invalid -> failwithf "Invalid rights entry found: %s. Allowed values are [%A]" invalid.rights Rights.AllowedRights)
            if config.ExceptionsCutoffInDays < 0 then
                failwith "ExceptionsCutoffInDays cannot be negative"
            let prioritiesExceptions =
                if File.Exists config.Exceptions then
                    Some(loadHighPriorityConfiguration config.Exceptions)
                else
                    None
            {
                Exceptions = prioritiesExceptions
                ExceptionsPriority = byte config.ExceptionsPriority
                ExceptionsCutoff = TimeSpan.FromDays(float config.ExceptionsCutoffInDays)
                Sources = priorities |> Seq.map (fun x -> x.source, Rights.Parse x.rights x.hours, byte x.priority) |> List.ofSeq
                DefaultPriority = defaultPriority
            }

    type CalculatePriority = MessageSource * PublishedStatus * PsPublishingPriority option -> Priority

    type PriorityCalculator(config: PriorityConfig) =

        [<Literal>]
        let LowPriority = 1uy
        [<Literal>]
        let HighPriority = 4uy

        member _.CalculatePriority(source: MessageSource, published: PublishedStatus, priority: PsPublishingPriority option) : Priority =
            let inRange range =
                match published, range with
                | PublishedStatus.ActiveSince time, Rights.ActiveSince(Some(fromHour, toHour)) ->
                    let hours = (DateTime.Now - time).TotalHours
                    hours >= fromHour && hours <= toHour
                | PublishedStatus.ActiveAfter time, Rights.ActiveAfter(Some(fromHour, toHour)) ->
                    let hours = (time - DateTime.Now).TotalHours
                    hours >= fromHour && hours <= toHour
                | PublishedStatus.ActiveSince _, Rights.ActiveSince None -> true
                | PublishedStatus.ActiveAfter _, Rights.ActiveAfter None -> true
                | _ -> false

            match priority with
            | Some PsPublishingPriority.Low -> LowPriority
            | Some PsPublishingPriority.High -> HighPriority
            | Some PsPublishingPriority.Medium
            | None ->
                config.Sources
                |> List.tryFind (fun (cfgSrc, cfgRights, _) -> cfgSrc = source && (cfgRights = Rights.Any || inRange cfgRights))
                |> Option.map (fun (_, _, cfgPriority) -> cfgPriority)
                |> Option.defaultValue config.DefaultPriority
