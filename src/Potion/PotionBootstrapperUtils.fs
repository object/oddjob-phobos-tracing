namespace Nrk.Oddjob.Potion

module PotionBootstrapperUtils =

    open System
    open Akka.Actor
    open Akka.DependencyInjection
    open Akkling
    open FsHttp

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Core.Queues

    open PotionDto
    open PotionTypes
    open PotionUtils
    open PotionWorkerActors

    let saveToEventStore (eventStoreSettings: EventStore) (message: PotionCommand) =
        let url = sprintf "%s/potion?code=%s" eventStoreSettings.PublishUrl eventStoreSettings.AuthorizationKey
        let eventStoreGroupId = message.GetShortGroupId()
        let eventStoreMessage =
            {
                Id = eventStoreGroupId
                Description = getUnionCaseName message
                Content = Serialization.Newtonsoft.serializeObject SerializationSettings.CamelCase message
                Created = DateTime.SpecifyKind(DateTime.UtcNow, DateTimeKind.Utc)
            }
        let jsonString = Serialization.Newtonsoft.serializeObject SerializationSettings.PascalCase eventStoreMessage
        let response =
            http {
                POST url
                body
                json jsonString
            }
            |> Request.send
        if int response.statusCode >= 300 then
            failwithf $"Failed to save command to event store ({response.statusCode})"

    let createPotionHandlerProps
        (oddjobConfig: OddjobConfig)
        (connectionStrings: ConnectionStrings)
        system
        (rmqApi: IRabbitMqApi)
        potionMediator
        uploadMediator
        potionScheduler
        uploadScheduler
        fetchBoolConfigValue
        fetchStringConfigValue
        =

        let priorityCalc = Priority.PriorityCalculator oddjobConfig.Potion.MessagePriorities
        let skipNotificationForSources =
            match fetchStringConfigValue ConfigKey.SkipNotificationForSources "" with
            | "" -> Array.empty
            | text -> text.Split ','
        {
            MediaSetController = uploadMediator
            ClearMediaSetReminderInterval = TimeSpan.FromHours(float oddjobConfig.Limits.ClearMediaSetReminderIntervalInHours)
            NotificationPublisher = rmqApi.GetPublisher(system, ExchangeCategory.OddjobEvents)
            RetentionPeriods =
                oddjobConfig.Potion.RetentionPeriod.Sources
                |> List.ofArrayOrNull
                |> Seq.map (fun x -> x.source, TimeSpan.FromDays(float x.days))
                |> Map.ofSeq
            CalculateJobPriority = priorityCalc.CalculatePriority
            PendingCommandsRetryInterval =
                oddjobConfig.Potion.PendingCommandsRetryIntervalInMinutes |> Seq.map float |> Seq.map TimeSpan.FromMinutes |> Seq.toList
            PotionMediator = potionMediator
            CompletionReminderInterval = TimeSpan.FromMinutes(float oddjobConfig.Limits.PotionCompletionReminderIntervalInMinutes)
            CompletionExpiryInterval = TimeSpan.FromMinutes(float oddjobConfig.Limits.PotionCompletionExpiryIntervalInHours)
            ReminderAckTimeout = TimeSpan.ToOption TimeSpan.FromSeconds oddjobConfig.Limits.ReminderAckTimeoutInSeconds
            IdleStateTimeout = Some <| TimeSpan.FromSeconds 5.
            PotionScheduler = potionScheduler
            UploadScheduler = uploadScheduler
            ExternalRequestTimeout = TimeSpan.ToOption TimeSpan.FromSeconds oddjobConfig.Limits.ExternalRequestTimeoutInSeconds
            SaveToEventStore = eventStoreSettings connectionStrings.EventStore |> saveToEventStore
            CreateCommandId = fun _ -> Guid.NewGuid().ToString()
            Origins = Origins.active oddjobConfig
            SkipNotificationForSources = [ oddjobConfig.Potion.SkipNotificationForSources; skipNotificationForSources ] |> Seq.concat |> Seq.toList
            EnableDefaultGeoBlock = fun () -> fetchBoolConfigValue ConfigKey.EnableDefaultPotionGeoBlock false
            CorrectGeoBlockOnActivation = fun () -> fetchBoolConfigValue ConfigKey.CorrectPotionGeoBlockOnActivation false
        }

    let startPotionWatchSubscriber
        (system: ActorSystem)
        (resolver: IDependencyResolver)
        potionMediator
        activityContext
        : IActorRef<QueueMessage<PotionCommandDto>> =
        let getTraceTags (command: PotionCommand) =
            match command.GetFileId(), command.GetQualityId() with
            | Some fileId, Some qualityId ->
                [
                    ("oddjob.potion.groupId", command.GetShortGroupId())
                    ("oddjob.potion.fileId", fileId)
                    ("oddjob.potion.correlationId", command.GetCorrelationId())
                    ("oddjob.potion.qualityId", $"{qualityId}")
                    ("oddjob.potion.messageSource", command.GetSource())
                    ("oddjob.mediaset.qualityId", $"{qualityId}")
                ]
            | _, _ ->
                [
                    ("oddjob.potion.groupId", command.GetShortGroupId())
                    ("oddjob.potion.correlationId", command.GetCorrelationId())
                    ("oddjob.potion.messageSource", command.GetSource())
                ]

        let healthResponsePublisher = resolver.GetService<IRabbitMqApi>().GetPublisher(system, ExchangeCategory.OddjobEvents)

        let potionHealthCheckActor =
            spawn system (makeActorName [ "Potion Health" ])
            <| propsNamed
                "potion-queue-health-check"
                (actorOf2 (fun mailbox (msg: QueueMessage<HealthCheckCommand>) ->
                    logDebug mailbox $"Received health check command with requestId: {msg.Payload.RequestId}"
                    let response =
                        {
                            RequestId = msg.Payload.RequestId
                            Response = "PONG!"
                        }
                        |> Serialization.Newtonsoft.serializeObject SerializationSettings.PascalCase
                        |> OutgoingMessage.create
                    healthResponsePublisher <! PublishOne(response, null, typed ActorRefs.Nobody)
                    sendAck mailbox msg.Ack $"Responded {msg.Payload.RequestId} with PONG!"
                    ignored ()))

        spawn system (makeActorName [ "Potion Command" ])
        <| propsNamed
            "potion-queue-command"
            (actorOf2 (fun mailbox msg ->
                match msg.Payload with
                | PotionCommandDto cmd ->
                    use activity = activityContext.ActivitySource.StartActivity("Potion.Command")
                    activity |> setActivityTags (getTraceTags cmd)
                    let queueMsg = QueueMessage.createWithAck cmd msg.Ack
                    potionMediator <! queueMsg
                | PotionHealthCheckDto cmd ->
                    let queueMsg = QueueMessage.createWithAck cmd msg.Ack
                    retype potionHealthCheckActor <! queueMsg
                | _ -> ()
                |> ignored))
