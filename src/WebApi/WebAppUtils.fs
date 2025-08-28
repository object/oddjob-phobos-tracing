namespace Nrk.Oddjob.WebApi

module WebAppUtils =

    open System
    open System.Net
    open Akka.Actor
    open Akka.Routing
    open Akkling
    open Giraffe
    open Microsoft.AspNetCore.Http
    open Microsoft.Extensions.Primitives

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Core.Queues
    open Nrk.Oddjob.Core.ShardMessages
    open Nrk.Oddjob.Core.MediaSetStateCache
    open Nrk.Oddjob.Potion.PotionTypes
    open Nrk.Oddjob.Ps.PsDto
    open Nrk.Oddjob.Ps.PsShardMessages

    [<Literal>]
    let RepairJobPriority = 5

    let rules =
        dict
            [
                "*/*", json
                "text/html", json
                "application/json", json
                "application/xml", xml
                "text/xml", xml
                "text/plain", (fun x -> x.ToString() |> text)
            ]

    let unacceptableHandler = setStatusCode 406 >=> text "Request cannot be satisfied by the web server."

    let tryParseQueryParam (q: StringValues) f =
        q.ToArray() |> Array.tryHead |> Option.bind f

    let parseQueryParam (q: StringValues) f d =
        tryParseQueryParam q f |> Option.defaultValue d

    let toResponse v =
        setHttpHeader AccessControlHeader "*" >=> negotiateWith rules unacceptableHandler v

    let toResponseFromResult<'T> (result: Result<'T, int * string>) =
        fun _next (ctx: HttpContext) ->
            task {
                match result with
                | Result.Ok v ->
                    ctx.SetHttpHeader(AccessControlHeader, "*")
                    return! ctx.NegotiateWithAsync(rules, unacceptableHandler, v)
                | Result.Error(statusCode, reasonPhrase) ->
                    ctx.SetStatusCode(statusCode)
                    return! ctx.NegotiateWithAsync(rules, unacceptableHandler, reasonPhrase)
            }

    let toResponseTask<'T> (f: Async<'T>) =
        fun _next (ctx: HttpContext) ->
            task {
                let! result = f
                ctx.SetHttpHeader(AccessControlHeader, "*")
                return! ctx.NegotiateWithAsync(rules, unacceptableHandler, result)
            }

    let toResponseTaskWithStatusCode<'T> (httpStatusCodeAsync: Async<HttpStatusCode>) (f: Async<'T>) =
        fun _next (ctx: HttpContext) ->
            task {
                let! result = f
                ctx.SetHttpHeader(AccessControlHeader, "*")
                let! statusCode = httpStatusCodeAsync
                ctx.SetStatusCode(int statusCode)
                return! ctx.NegotiateWithAsync(rules, unacceptableHandler, result)
            }

    let toResponseWithSequenceFilterTask<'T> (f: int -> int -> Async<seq<'T>>) =
        fun _next (ctx: HttpContext) ->
            task {
                let fromSequenceNr = parseQueryParam (ctx.Request.Query.Item "from") TryParser.parseInt 0
                let toSequenceNr = parseQueryParam (ctx.Request.Query.Item "to") TryParser.parseInt 0
                let! result = f fromSequenceNr toSequenceNr
                ctx.SetHttpHeader(AccessControlHeader, "*")
                return! ctx.NegotiateWithAsync(rules, unacceptableHandler, (result |> Seq.toList))
            }

    let getStatusEndpoint (request: HttpRequest) clientId contentId =
        let port =
            if request.Host.Port.HasValue then
                $":%d{request.Host.Port.Value}"
            else
                String.Empty
        $"%s{request.Scheme}://%s{request.Host.Host}%s{port}/status/%s{clientId}/%s{contentId}"

    let createProxyRouter routerName shardProxy config (system: ActorSystem) =
        let routerConfig =
            FromConfig.Instance.WithSupervisorStrategy(getOneForOneRestartSupervisorStrategy system.Log) :> RouterConfig
        let proxyProps =
            actorOf2 (fun mailbox msg ->
                logDebug mailbox $"{msg}"
                shardProxy <! msg
                Async.Sleep config.Limits.WebApiRouterIntervalBetweenRequestsInMilliseconds |> Async.RunSynchronously
                ignored ())
        spawn
            system
            routerName
            { propsNamed "webapi-proxy-router" proxyProps with
                Router = Some routerConfig
            }

    let repairMediaSet uploadMediator (clientId, contentId) =
        fun _next (ctx: HttpContext) ->
            task {
                let priority = parseQueryParam (ctx.Request.Query.Item "priority") TryParser.parseInt RepairJobPriority
                let mediaSetId = MediaSetId.create (Alphanumeric clientId) (ContentId contentId)
                uploadMediator <! Message.create (MediaSetShardMessage.RepairMediaSet(mediaSetId, priority, "WebAPI"))
                let statusEndpoint = getStatusEndpoint ctx.Request clientId contentId
                return! ctx.NegotiateWithAsync(rules, unacceptableHandler, { Status = statusEndpoint })
            }

    let migrateMediaSet uploadMediator (clientId, contentId) =
        fun _next (ctx: HttpContext) ->
            task {
                let mediaSetId = MediaSetId.create (Alphanumeric clientId) (ContentId contentId)
                uploadMediator <! Message.create (MediaSetShardMessage.MigrateMediaSet mediaSetId)
                let statusEndpoint = getStatusEndpoint ctx.Request clientId contentId
                return! ctx.NegotiateWithAsync(rules, unacceptableHandler, { Status = statusEndpoint })
            }

    let activateMediaSet uploadMediator (clientId, contentId) =
        fun _next (ctx: HttpContext) ->
            task {
                let mediaSetId = MediaSetId.create (Alphanumeric clientId) (ContentId contentId)
                uploadMediator <! Message.create (MediaSetShardMessage.ActivateMediaSet mediaSetId)
                let statusEndpoint = getStatusEndpoint ctx.Request clientId contentId
                return! ctx.NegotiateWithAsync(rules, unacceptableHandler, { Status = statusEndpoint })
            }

    let deactivateMediaSet uploadMediator (clientId, contentId) =
        fun _next (ctx: HttpContext) ->
            task {
                let mediaSetId = MediaSetId.create (Alphanumeric clientId) (ContentId contentId)
                uploadMediator <! Message.create (MediaSetShardMessage.DeactivateMediaSet mediaSetId)
                let statusEndpoint = getStatusEndpoint ctx.Request clientId contentId
                return! ctx.NegotiateWithAsync(rules, unacceptableHandler, { Status = statusEndpoint })
            }

    let sendPsPlaybackEvents contentId mediaSetStateActor uploadProxy psProxy origins =
        fun _next (ctx: HttpContext) ->
            task {
                let mediaSetId = MediaSetId.create (Alphanumeric PsClientId) (ContentId contentId)
                let! (state: Dto.MediaSet.MediaSetState) = mediaSetStateActor <? MediaSetStateCacheCommand.GetState mediaSetId.Value
                let state = state.ToDomain()
                let msg =
                    Events.MediaSetPlayabilityEvent.create mediaSetId state origins DateTimeOffset.Now Events.PlayabilityEventPriority.High
                    |> Dto.Events.MediaSetPlayabilityEventDto.fromDomain
                psProxy <! PsShardMessage.PlayabilityEvent msg
                let statusEndpoint = getStatusEndpoint ctx.Request PsClientId contentId
                return! ctx.NegotiateWithAsync(rules, unacceptableHandler, { Status = statusEndpoint })
            }

    let sendMessageToPsQueue config exchangeCategory (message: PsChangeJobDto) =
        fun _next (ctx: HttpContext) ->
            task {
                let priority = parseQueryParam (ctx.Request.Query.Item "priority") TryParser.parseByte 0uy
                let source = parseQueryParam (ctx.Request.Query.Item "source") Some message.Source
                let message = { message with Source = source }
                match! QueueUtils.sendToQueue config exchangeCategory message priority with
                | Result.Ok() ->
                    let statusEndpoint = getStatusEndpoint ctx.Request PsClientId message.ProgrammeId
                    return! ctx.NegotiateWithAsync(rules, unacceptableHandler, { Status = statusEndpoint })
                | Result.Error exn ->
                    ctx.SetStatusCode(int HttpStatusCode.InternalServerError)
                    return! ctx.NegotiateWithAsync(rules, unacceptableHandler, exn.Message)
            }

    let sendMessageToPotionQueue config (message: PotionCommand) =
        fun next (ctx: HttpContext) ->
            task {
                let priority = parseQueryParam (ctx.Request.Query.Item "priority") TryParser.parseByte 0uy
                match! QueueUtils.sendToQueue config ExchangeCategory.PotionWatch message priority with
                | Result.Ok() -> return! Successful.NO_CONTENT next ctx
                | Result.Error exn ->
                    ctx.SetStatusCode(int HttpStatusCode.InternalServerError)
                    return! ctx.NegotiateWithAsync(rules, unacceptableHandler, exn.Message)
            }

    let clearMediaSet clientId contentId uploadProxy _config =
        fun _next (ctx: HttpContext) ->
            task {
                let job =
                    MediaSetJob.ClearMediaSet
                        {
                            Header =
                                { JobHeader.Zero with
                                    RequestSource = "WebAPI"
                                }
                            MediaSetId = MediaSetId.create (Alphanumeric clientId) (ContentId contentId)
                        }
                uploadProxy <! Message.create (MediaSetShardMessage.MediaSetJob job)
                let statusEndpoint = getStatusEndpoint ctx.Request clientId contentId
                return! ctx.NegotiateWithAsync(rules, unacceptableHandler, { Status = statusEndpoint })
            }

    let private createReminderMessage groupName message : obj =
        match groupName, message with
        | Reminders.ClearMediaSet, ReminderMessage.ClearMediaSet mediaSetId -> MediaSetShardMessage.ClearMediaSetReminder mediaSetId
        | Reminders.StorageCleanup, ReminderMessage.StorageCleanup mediaSetId ->
            MediaSetShardMessage.RepairMediaSet(MediaSetId.parse mediaSetId, 0, "storage-cleanup")
        | Reminders.PlayabilityBump, ReminderMessage.PlayabilityBump programId ->
            {
                PlayabilityBump.ProgramId = programId
            }
        | Reminders.PotionBump, ReminderMessage.PotionBump contentId -> { PotionBump.ContentId = contentId }
        | _ -> notSupported groupName

    let private getReminders (request: HttpRequest) =
        let port =
            if request.Host.Port.HasValue then
                $":%d{request.Host.Port.Value}"
            else
                String.Empty
        $"%s{request.Scheme}://%s{request.Host.Host}%s{port}/reminders"

    let schedulerReminder schedulers proxyPaths (reminderInfo: CreateReminderInfo) =
        fun _next (ctx: HttpContext) ->
            task {
                let reminders =
                    [
                        (Reminders.ClearMediaSet, Reminders.UploadRole)
                        (Reminders.StorageCleanup, Reminders.UploadRole)
                        (Reminders.PlayabilityBump, Reminders.PsRole)
                        (Reminders.PotionBump, Reminders.PotionRole)
                    ]
                let result =
                    reminders
                    |> List.tryFind (fun (groupName, _) -> String.Equals(groupName, reminderInfo.GroupName, StringComparison.OrdinalIgnoreCase))
                    |> Option.map (fun (groupName, role) ->
                        let message = createReminderMessage groupName reminderInfo.Message
                        let scheduler: IActorRef = schedulers |> Map.find role
                        let targetPath: ActorPath = proxyPaths |> Map.find role
                        let trigger = Reminders.createTriggerForSingleReminder groupName reminderInfo.TaskId reminderInfo.StartTime
                        Reminders.createOrUpdateReminder scheduler trigger targetPath message
                        getReminders ctx.Request |> Result.Ok)
                    |> Option.defaultWith (fun () -> $"Invalid reminder group name {reminderInfo.GroupName}" |> Result.Error)
                match result with
                | Result.Ok reminders -> return! ctx.NegotiateWithAsync(rules, unacceptableHandler, reminders)
                | Result.Error message ->
                    ctx.SetStatusCode(int HttpStatusCode.BadRequest)
                    return! ctx.NegotiateWithAsync(rules, unacceptableHandler, message)
            }
