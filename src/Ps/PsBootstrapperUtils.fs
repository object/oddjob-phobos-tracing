namespace Nrk.Oddjob.Ps

open Nrk.Oddjob.Ps.PsDto

module PsBootstrapperUtils =

    open System
    open System.Net
    open Akka.Actor
    open Akka.Routing
    open Akkling
    open Azure.Messaging.ServiceBus
    open Azure.Storage.Blobs
    open FsHttp
    open Microsoft.Extensions.Azure

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Granitt
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Core.SchedulerActors
    open Nrk.Oddjob.Core.Queues

    open PsTypes
    open PsFilesHandlerActor
    open PsTranscodingActors
    open PsTvSubtitleFilesActor
    open PsProgramStatusActors
    open PsGranittActors
    open PsPlaybackEvents
    open PsPlaybackActors
    open PsRadioActors
    open PsFileArchive
    open ServiceBusPublisher
    open PsShardMessages
    open PsShardActors
    open Retention

    let saveToEventStore (eventStoreSettings: EventStore) collectionName piProgId carrierId description message =
        let url = sprintf "%s/%s?code=%s" eventStoreSettings.PublishUrl collectionName eventStoreSettings.AuthorizationKey
        let eventStoreMessage =
            {
                Program = PiProgId.value piProgId
                Carrier = carrierId
                Description = description
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
            failwithf $"Failed to save message to event store ({response.statusCode})"

    let createPsGranittRouter granittConnectionString (system: ActorSystem) =
        let routerConfig =
            FromConfig.Instance.WithSupervisorStrategy(getOneForOneRestartSupervisorStrategy system.Log) :> RouterConfig
        let granittActorProps =
            psGranittActor
                {
                    GetGranittContext = fun () -> getGranittContext granittConnectionString system.Log
                }
        let granittRouterProps =
            { propsNamed "ps-granitt" granittActorProps with
                Router = Some routerConfig
            }
        spawn system (makeActorName [ "PS Granitt" ]) granittRouterProps

    let createPsServiceBusRouter
        (oddjobConfig: OddjobConfig)
        (system: ActorSystem)
        (clientFactory: IAzureClientFactory<ServiceBusClient>)
        : IActorRef<ServiceBusMessage> =
        let routerConfig =
            FromConfig.Instance.WithSupervisorStrategy(getOneForOneRestartSupervisorStrategy system.Log) :> RouterConfig
        let serviceBusActorProps =
            let configureEventSink (serviceBus: OddjobConfig.PsServiceBusPublisher) =
                let client =
                    if String.isNullOrEmpty serviceBus.ServiceBusName then
                        null
                    else
                        clientFactory.CreateClient serviceBus.ServiceBusName
                {
                    Client = client
                    Topic = serviceBus.Topic
                }
            serviceBusPublisherActor
                {
                    EventSinks =
                        [
                            (PlaybackEventSink, configureEventSink oddjobConfig.Ps.PlaybackEvents)
                            (DistributionStatusEventSink, configureEventSink oddjobConfig.Ps.DistributionStatusEvents)
                        ]
                        |> Map.ofList
                }
        let serviceBusRouterProps =
            { propsNamed "ps-servicebus-publisher" serviceBusActorProps with
                Router = Some routerConfig
            }
        spawn system (makeActorName [ "PS Azure" ]) serviceBusRouterProps |> retype

    let getArchiveConfig (oddjobConfig: OddjobConfig) =
        let dropFolder =
            if oddjobConfig.Ps.MediaFiles.UseDropFolder then
                Some oddjobConfig.Ps.MediaFiles.DropFolder
            else
                None
        let destinationRoot =
            if oddjobConfig.Ps.MediaFiles.UseDestinationRoot then
                Some oddjobConfig.Ps.MediaFiles.DestinationRoot
            else
                None
        match (dropFolder, destinationRoot) with
        | Some dropFolder, Some destinationRoot -> MoveFromDropToDestinationFolder(dropFolder, destinationRoot)
        | None, Some destinationRoot -> CopyToDestinationFolder destinationRoot
        | _ -> UseFromArchive oddjobConfig.Ps.MediaFiles.ArchiveRoot

    let private createPsTranscodingHandlerProps
        (oddjobConfig: OddjobConfig)
        (connectionStrings: ConnectionStrings)
        granittRouter
        psMediator
        fetchBoolConfigValue
        activityContext
        =
        {
            GetGranittAccess = fun _ -> granittRouter
            GetPsMediator = fun () -> retype psMediator
            SaveToEventStore = saveToEventStore (eventStoreSettings connectionStrings.EventStore) "transcoding"
            DisableGranittUpdate = fun () -> fetchBoolConfigValue ConfigKey.DisableGranittUpdate false
            ReminderIntervalInMinutes = oddjobConfig.Limits.TranscodingReminderIntervalInMinutes
            ReminderTimeoutInDays = oddjobConfig.Limits.TranscodingReminderTimeoutInDays
            ReminderAckTimeout = TimeSpan.ToOption TimeSpan.FromSeconds oddjobConfig.Limits.ReminderAckTimeoutInSeconds
            ArchiveConfig = getArchiveConfig oddjobConfig
            ArchiveTimeouts = oddjobConfig.Ps.MediaFiles.ArchiveTimeouts
            FileSystem = LocalFileSystemOperations()
            ExternalRequestTimeout = TimeSpan.ToOption TimeSpan.FromSeconds oddjobConfig.Limits.ExternalRequestTimeoutInSeconds
            ActivityContext = activityContext
        }

    let private createPsFilesHandlerProps
        (oddjobConfig: OddjobConfig)
        system
        (rmqApi: IRabbitMqApi)
        granittRouter
        psMediator
        uploadMediaSetProxy
        psScheduler
        uploadScheduler
        queueScheduler
        =

        let reschedulePsChangeMessage (msg: ScheduleMessage<PsChangeJob>) =
            let cmd =
                {
                    RmqMessage = PsChangeJobDto.fromPsJob msg.Message
                    Id = msg.Id
                    Version = 1
                    Ack = msg.Ack
                    TargetCategory = ExchangeCategory.PsProgramsWatch
                    TriggerDateTime = msg.TriggerDateTime
                }
            queueScheduler <! cmd

        let prioCalc = Priority.PriorityCalculator(Priority.PriorityConfig.Create oddjobConfig)
        let rightsAnalyzer =
            Rights.ProgramRightsAnalyzer(
                TimeSpan.FromHours(float oddjobConfig.Ps.UploadProgramBeforePublishTimeInHours),
                TimeSpan.FromHours(float oddjobConfig.Ps.RemoveProgramAfterExpirationTimeInHours),
                fun () -> DateTime.Now
            )

        let fileSystem: IFileSystemOperations = LocalFileSystemOperations()
        let archiveFileLocator =
            fun (path: string) ->
                if String.IsNullOrEmpty oddjobConfig.Ps.MediaFiles.AlternativeArchiveRoot then
                    path
                else
                    let altPath = path.Replace(oddjobConfig.Ps.MediaFiles.ArchiveRoot, oddjobConfig.Ps.MediaFiles.AlternativeArchiveRoot)
                    if fileSystem.FileExists altPath then altPath else path
        {
            SubtitlesConfig =
                {
                    BaseUrl = Uri(oddjobConfig.Subtitles.BaseURL)
                    BaseUrlTvSubtitles = Uri(oddjobConfig.Subtitles.BaseUrlTvSubtitles)
                    SourceRootWindows = oddjobConfig.Subtitles.SourceRootWindows
                    SourceRootLinux = oddjobConfig.Subtitles.SourceRootLinux
                    DestinationRootWindows = oddjobConfig.Subtitles.DestinationRootWindows
                    DestinationRootLinux = oddjobConfig.Subtitles.DestinationRootLinux
                }
            GetGranittAccess = fun _ -> granittRouter
            MediaSetController = uploadMediaSetProxy
            ReschedulePsChangeMessage = reschedulePsChangeMessage
            ForceOperationForSources = oddjobConfig.Ps.ForceOperationForSources |> Seq.toList
            MissingFilesRetryInterval = oddjobConfig.Limits.MissingFilesRetryIntervalInMinutes |> Seq.toArray
            EnableFileArchivePreparation = String.isNotNullOrEmpty oddjobConfig.Ps.MediaFiles.DestinationRoot
            CalculatePriority = prioCalc.CalculatePriority
            RightsAnalyzer = rightsAnalyzer
            RetentionExceptions = RetentionExceptions.fromConfig oddjobConfig
            QueuePublisher = rmqApi.GetPublisher(system, ExchangeCategory.PsProgramsWatch)
            GetPsMediator = fun () -> psMediator
            ClearMediaSetReminderInterval = TimeSpan.FromHours(float oddjobConfig.Limits.ClearMediaSetReminderIntervalInHours)
            PlayabilityReminderInterval = TimeSpan.FromMinutes(float oddjobConfig.Limits.PlaybackCompletionReminderIntervalInMinutes)
            ReminderAckTimeout = TimeSpan.FromSeconds(float oddjobConfig.Limits.ReminderAckTimeoutInSeconds)
            PsScheduler = psScheduler
            UploadScheduler = uploadScheduler
            ArchiveConfig = getArchiveConfig oddjobConfig
            ArchiveFileLocator = archiveFileLocator
            PathMappings = oddjobConfig.PathMappings
            FileSystem = fileSystem
            DropFolder = oddjobConfig.Ps.MediaFiles.DropFolder
            ExternalRequestTimeout = TimeSpan.ToOption TimeSpan.FromSeconds oddjobConfig.Limits.ExternalRequestTimeoutInSeconds
        }

    let private createPsTvSubtitleFilesHandlerProps
        (oddjobConfig: OddjobConfig)
        (connectionStrings: ConnectionStrings)
        environment
        tekstebankenConfig
        psMediator
        psScheduler
        : PsTvSubtitleFilesProps =
        let blobClient: BlobServiceClient = BlobServiceClient(connectionStrings.SideloadingAzureStorage)
        let containerClient: BlobContainerClient = blobClient.GetBlobContainerClient("subtitles-v1")

        {
            Environment = environment
            SubtitlesConfig =
                {
                    BaseUrl = Uri(oddjobConfig.Subtitles.BaseURL)
                    BaseUrlTvSubtitles = Uri(oddjobConfig.Subtitles.BaseUrlTvSubtitles)
                    SourceRootWindows = oddjobConfig.Subtitles.SourceRootWindows
                    SourceRootLinux = oddjobConfig.Subtitles.SourceRootLinux
                    DestinationRootWindows = oddjobConfig.Subtitles.DestinationRootWindows
                    DestinationRootLinux = oddjobConfig.Subtitles.DestinationRootLinux
                }
            TekstebankenConfig = tekstebankenConfig
            PsMediator = psMediator
            SaveToEventStore = saveToEventStore (eventStoreSettings connectionStrings.EventStore) "tiltsubtitles"
            SideloadingContainerClient = containerClient
            PlayabilityReminderInterval = TimeSpan.FromMinutes(float oddjobConfig.Limits.PotionCompletionReminderIntervalInMinutes)
            ReminderAckTimeout = TimeSpan.FromSeconds(float oddjobConfig.Limits.ReminderAckTimeoutInSeconds)
            ArchivalTimeout = TimeSpan.FromSeconds 10.
            PsScheduler = psScheduler
            FileSystem = LocalFileSystemInfo()
        }

    let private createPsPlayabilityHandlerProps
        (oddjobConfig: OddjobConfig)
        (connectionStrings: ConnectionStrings)
        psMediator
        mediaSetStateCache
        mediaSetController
        serviceBusRouter
        psScheduler
        : PsPlayabilityNotificationProps =
        {
            AkkaConnectionString = connectionStrings.Akka
            Origins = Origins.active oddjobConfig
            GetPsMediator = fun () -> psMediator
            GetServiceBusPublisher = fun _ -> serviceBusRouter
            PlaybackEventConsumerCount =
                if String.IsNullOrEmpty oddjobConfig.Ps.PlaybackEvents.ServiceBusName then
                    0
                else
                    1
            DistributionStatusEventConsumerCount =
                if String.IsNullOrEmpty oddjobConfig.Ps.DistributionStatusEvents.ServiceBusName then
                    0
                else
                    1
            GetEventCreator = fun parent -> PlaybackEventCreator(parent, Origins.active oddjobConfig) :> IPlaybackEventCreator
            MediaSetStateCache = mediaSetStateCache
            MediaSetController = mediaSetController
            IdleStateTimeout = TimeSpan.ToOption TimeSpan.FromSeconds oddjobConfig.Limits.PlaybackIdleStateTimeoutIntervalInSeconds
            ExternalRequestTimeout = TimeSpan.ToOption TimeSpan.FromSeconds oddjobConfig.Limits.ExternalRequestTimeoutInSeconds
            AskTimeout = TimeSpan.ToOption TimeSpan.FromSeconds oddjobConfig.Limits.AskTimeoutInSeconds
            CompletionReminderInterval = TimeSpan.FromMinutes(float oddjobConfig.Limits.PlaybackCompletionReminderIntervalInMinutes)
            CompletionExpiryInterval = TimeSpan.FromHours(float oddjobConfig.Limits.PlaybackCompletionExpiryIntervalInHours)
            EventsRetentionInterval = TimeSpan.FromDays(float oddjobConfig.Limits.PlaybackEventsRetentionPeriodInDays)
            PsScheduler = psScheduler
        }

    let private createPsProgramStatusHandlerProps (oddjobConfig: OddjobConfig) (connectionStrings: ConnectionStrings) uploadMediator psMediator psScheduler =
        {
            SubtitlesConfig =
                {
                    BaseUrl = Uri(oddjobConfig.Subtitles.BaseURL)
                    BaseUrlTvSubtitles = Uri(oddjobConfig.Subtitles.BaseUrlTvSubtitles)
                    SourceRootWindows = oddjobConfig.Subtitles.SourceRootWindows
                    SourceRootLinux = oddjobConfig.Subtitles.SourceRootLinux
                    DestinationRootWindows = oddjobConfig.Subtitles.DestinationRootWindows
                    DestinationRootLinux = oddjobConfig.Subtitles.DestinationRootLinux
                }
            MediaSetController = uploadMediator
            PsMediator = psMediator
            SaveToEventStore = saveToEventStore (eventStoreSettings connectionStrings.EventStore) "prfprogramstatus"
            PlayabilityReminderInterval = TimeSpan.FromMinutes(float oddjobConfig.Limits.PotionCompletionReminderIntervalInMinutes)
            ReminderAckTimeout = TimeSpan.FromSeconds(float oddjobConfig.Limits.ReminderAckTimeoutInSeconds)
            PsScheduler = psScheduler
        }

    let private createPsRadioHandlerProps (oddjobConfig: OddjobConfig) (connectionStrings: ConnectionStrings) psMediator activityContext =

        {
            GetPsMediator = fun () -> retype psMediator
            SaveToEventStore = saveToEventStore (eventStoreSettings connectionStrings.EventStore) "radiotranscoding"
            ArchiveConfig = getArchiveConfig oddjobConfig
            FileSystem = LocalFileSystemOperations()
            DetailsAckTimeout = Some <| TimeSpan.FromMinutes 1.
            ActivityContext = activityContext
        }

    let createPsMediaSetProps
        oddjobConfig
        connectionStrings
        environment
        tekstebankenConfig
        system
        rmqApi
        mediaSetStateCache
        granittRouter
        serviceBusRouter
        mediaSetRef
        psMediator
        uploadMediator
        psScheduler
        uploadScheduler
        queueScheduler
        fetchBoolConfigValue
        activityContext
        =

        let aprops =
            {
                FilesHandlerProps =
                    createPsFilesHandlerProps oddjobConfig system rmqApi granittRouter psMediator uploadMediator psScheduler uploadScheduler queueScheduler
                TvSubtitleFilesHandlerProps =
                    createPsTvSubtitleFilesHandlerProps oddjobConfig connectionStrings environment tekstebankenConfig psMediator psScheduler
                PlaybackHandlerProps =
                    createPsPlayabilityHandlerProps oddjobConfig connectionStrings psMediator mediaSetStateCache uploadMediator serviceBusRouter psScheduler
                ProgramStatusHandlerProps = createPsProgramStatusHandlerProps oddjobConfig connectionStrings uploadMediator (psMediator |> retype) psScheduler
                TranscodingHandlerProps =
                    createPsTranscodingHandlerProps oddjobConfig connectionStrings granittRouter psMediator fetchBoolConfigValue activityContext
                RadioHandlerProps = createPsRadioHandlerProps oddjobConfig connectionStrings psMediator activityContext
                MediaSetRef = mediaSetRef
                ActivityContext = activityContext
            }
        { propsNamed "ps-mediaset-shard" (psMediaSetShardActor aprops) with
            SupervisionStrategy = Some <| getOneForOneRestartSupervisorStrategy system.Log
        }

    /// Creates actor reacting to Ps queue changes related to programs which is fetching jobs to Oddjob.Upload
    let startPsProgramSubscriber (system: ActorSystem) _resolver psMediaSet activityContext : IActorRef<QueueMessage<PsChangeJobDto>> =
        spawn system (makeActorName [ "PS Files" ])
        <| propsNamed
            "ps-queue-files"
            (actorOf2 (fun mailbox msg ->
                if String.isNullOrEmpty msg.Payload.ProgrammeId then
                    sendReject mailbox msg.Ack "Message has empty program ID"
                else
                    try
                        let job = msg |> QueueMessage.transform (PsChangeJobDto.toPsJob >> PsShardMessage.FilesJob)
                        use activity = activityContext.ActivitySource.StartActivity("PS.Files")
                        activity |> setActivityTags [ "ProgramId", msg.Payload.ProgrammeId ]
                        psMediaSet <! job
                    with
                    | :? Newtonsoft.Json.JsonReaderException as exn -> sendReject mailbox msg.Ack $"Error parsing Files queue message: {exn}"
                    | :? ArgumentNullException as exn -> sendReject mailbox msg.Ack $"Error parsing Files queue message: {exn}"
                |> ignored))

    /// Creates actor reacting to Ps queue transcoding messages and passes them to PS
    let startPsTranscodingSubscriber (system: ActorSystem) _resolver psMediaSet activityContext : IActorRef<QueueMessage<PsTranscodingFinishedDto>> =
        spawn system (makeActorName [ "PS Transcoding" ])
        <| propsNamed
            "ps-queue-transcoding"
            (actorOf2 (fun mailbox msg ->
                let (TranscodingFinished dto) = msg.Payload
                if Seq.isEmpty dto.Carriers then
                    sendReject mailbox msg.Ack "Message has empty carriers"
                else
                    try
                        let job = msg |> QueueMessage.transform (PsTranscodingFinishedDto.toPsJob >> PsShardMessage.VideoTranscodingJob)
                        use activity = activityContext.ActivitySource.StartActivity("PS.Transcoding")
                        activity |> setActivityTags [ "CarrierIds", String.Join(",", dto.Carriers |> List.map _.CarrierId) ]
                        psMediaSet <! job
                    with
                    | :? Newtonsoft.Json.JsonReaderException as exn -> sendReject mailbox msg.Ack $"Error parsing Transcoding queue message: {exn}"
                    | :? ArgumentNullException as exn -> sendReject mailbox msg.Ack $"Error parsing Transcoding queue message: {exn}"
                |> ignored))

    /// Creates actor reacting to Ps queue changes related to subs which is fetching jobs to Oddjob.Upload
    let startPsSubtitlesSubscriber (system: ActorSystem) _resolver psMediaSet activityContext : IActorRef<QueueMessage<PsTvSubtitlesDto>> =
        spawn system (makeActorName [ "PS TV Subtitles" ])
        <| propsNamed
            "ps-queue-tv-subtitles"
            (actorOf2 (fun mailbox msg ->
                try
                    let job = msg |> QueueMessage.transform (PsTvSubtitlesDto.toPsJob >> PsShardMessage.SubtitleFilesJob)
                    use activity = activityContext.ActivitySource.StartActivity("PS.TvSubtitles")
                    activity |> setActivityTags [ "ProgramId", msg.Payload.ProgramId ]
                    psMediaSet <! job
                with
                | :? Newtonsoft.Json.JsonReaderException as exn -> sendReject mailbox msg.Ack $"Error parsing Subtitles queue message: {exn}"
                | :? ArgumentNullException as exn -> sendReject mailbox msg.Ack $"Error parsing Subtitles queue message: {exn}"
                |> ignored))

    /// Creates actor reacting to Ps usage rights queue changes related to programs which is fetching jobs to Oddjob.Upload
    let startPsUsageRightsLegacySubscriber (system: ActorSystem) _resolver psMediaSet activityContext : IActorRef<QueueMessage<PsChangeJobDto>> =
        spawn system (makeActorName [ "PS Usage Rights Legacy" ])
        <| propsNamed
            "ps-queue-usage-rights-legacy"
            (actorOf2 (fun mailbox msg ->
                if String.isNullOrEmpty msg.Payload.ProgrammeId then
                    sendReject mailbox msg.Ack "Message has empty program ID"
                else
                    try
                        let job = msg |> QueueMessage.transform (PsChangeJobDto.toPsJob >> PsShardMessage.FilesJob)
                        use activity = activityContext.ActivitySource.StartActivity("PS.UsageRights")
                        activity |> setActivityTags [ "ProgramId", msg.Payload.ProgrammeId ]
                        psMediaSet <! job
                    with
                    | :? Newtonsoft.Json.JsonReaderException as exn -> sendReject mailbox msg.Ack $"Error processing UsageRights queue message: {exn}"
                    | :? ArgumentNullException as exn -> sendReject mailbox msg.Ack $"Error processing UsageRights queue message: {exn}"
                |> ignored))

    /// Creates actor reacting to Program status changes from PRF
    let startPsProgramStatusSubscriber (system: ActorSystem) _resolver psMediaSet activityContext : IActorRef<QueueMessage<PsProgramStatusEventDto>> =
        spawn system (makeActorName [ "PS Program Status" ])
        <| propsNamed
            "ps-queue-program-status"
            (actorOf2 (fun mailbox msg ->
                if String.isNullOrEmpty msg.Payload.ProgramId then
                    sendReject mailbox msg.Ack "Message has empty program ID"
                else
                    try
                        let job = msg |> QueueMessage.transform (PsProgramStatusEventDto.toDomain >> PsShardMessage.ProgramStatusEvent)
                        use activity = activityContext.ActivitySource.StartActivity("PS.ProgramStatus")
                        activity |> setActivityTags [ "ProgramId", msg.Payload.ProgramId ]
                        psMediaSet <! job
                    with
                    | :? Newtonsoft.Json.JsonReaderException as exn -> sendReject mailbox msg.Ack $"Error processing ProgramStatus queue message: {exn}"
                    | :? ArgumentNullException as exn -> sendReject mailbox msg.Ack $"Error processing ProgramStatus queue message: {exn}"
                |> ignored))

    /// Creates actor reacting to Radio transcoding messages
    let startPsRadioTranscodingSubscriber
        (system: ActorSystem)
        (oddjobConfig: Config.OddjobConfig)
        psMediaSet
        activityContext
        : IActorRef<QueueMessage<PsRadioChangeDto>> =
        spawn system (makeActorName [ "PS Radio Transcoding" ])
        <| propsNamed
            "ps-queue-radio-transcoding"
            (actorOf2 (fun mailbox msg ->
                if msg.Payload.Type <> "RADIO_EPISODE" then
                    sendAck mailbox msg.Ack "Skipping unrelated Type"
                else if String.isNullOrEmpty msg.Payload.EventType then
                    sendReject mailbox msg.Ack "Message has empty change type"
                else if msg.Payload.EventType <> "AUDIO_UPDATED" && msg.Payload.EventType <> "RESEND" then
                    sendAck mailbox msg.Ack "Skipping non-audio change type"
                else if String.isNullOrEmpty msg.Payload.ProgId then
                    sendReject mailbox msg.Ack "Message has empty program ID"
                else if String.isNullOrEmpty msg.Payload.Uri then
                    sendReject mailbox msg.Ack "Message does not contain URI"
                else
                    try
                        let response = http { GET msg.Payload.Uri } |> Request.send
                        if response.statusCode = HttpStatusCode.OK then
                            let message = Serialization.Newtonsoft.deserializeObject SerializationSettings.PascalCase (response.ToText())
                            let filesSelector = oddjobConfig.Radio.FilesSelector |> Seq.map (fun x -> x.label, x.bitRate) |> Map.ofSeq
                            let job =
                                QueueMessage.createWithAck message msg.Ack
                                |> QueueMessage.transform (PsRadioMediaDto.toPsJob filesSelector >> PsShardMessage.AudioTranscodingJob)
                            use activity = activityContext.ActivitySource.StartActivity("PS.Radio")
                            activity |> setActivityTags [ "ProgramId", msg.Payload.ProgId ]
                            psMediaSet <! job
                        else if int response.statusCode = 410 then
                            sendAck mailbox msg.Ack "Skipping message with 410 HTTP status code (Gone)"
                        else
                            sendNack mailbox msg.Ack $"Error {response.statusCode} retrieving change message from {msg.Payload.Uri}: {response.reasonPhrase}"
                    with
                    | :? Newtonsoft.Json.JsonReaderException as exn -> sendReject mailbox msg.Ack $"Error processing Radio queue message: {exn}"
                    | :? ArgumentNullException as exn -> sendReject mailbox msg.Ack $"Error processing Radio queue message: {exn}"
                |> ignored))

    let startPsRadioTranscodingAdapter system subscriber =
        ServiceBusAdapter.MessageProcessor.Forward subscriber
        |> ServiceBusAdapter.serviceBusAdapterActor
        |> propsNamed "ps-radio-transcoding-adapter"
        |> spawn system (makeActorName [ "PS Radio Transcoding Adapter" ])

    /// Creates actor reacting to Catalog UsageRights change messages
    let startPsUsageRightsSubscriber
        (system: ActorSystem)
        (oddjobConfig: Config.OddjobConfig)
        psMediaSet
        activityContext
        : IActorRef<QueueMessage<PsUsageRightsChangeDto>> =
        spawn system (makeActorName [ "PS Usage Rights" ])
        <| propsNamed
            "ps-queue-usage-rights"
            (actorOf2 (fun mailbox msg -> sendAck mailbox msg.Ack "Skipping (TODO: handled usage rights change - see tracking issue #3205)" |> ignored))

    let startPsUsageRightsAdapter system subscriber =
        ServiceBusAdapter.MessageProcessor.Forward subscriber
        |> ServiceBusAdapter.serviceBusAdapterActor
        |> propsNamed "ps-usage-rights-adapter"
        |> spawn system (makeActorName [ "PS Usage Rights Adapter" ])
