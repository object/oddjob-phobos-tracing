namespace Nrk.Oddjob.Core

module Config =

    open System
    open System.Collections.Generic
    open System.IO
    open System.Threading
    open System.Threading.Tasks
    open Microsoft.Extensions.Configuration

    let parseConnectionString (connectionString: string) =
        connectionString.Split(';')
        |> Array.choose (fun item ->
            let pos = item.IndexOf '='
            if pos > 0 then
                Some(item.Substring(0, pos), (if pos = item.Length then "" else item.Substring(pos + 1)))
            else
                None)
        |> Map.ofArray

    [<RequireQualifiedAccess>]
    module OddjobConfig =

        open System.Runtime.InteropServices

        [<CLIMutable>]
        type Features =
            {
                RecreateIncompatibleQueues: bool
                UseQuorumQueues: bool
            }

        [<CLIMutable>]
        type Limits =
            {
                DefaultHttpConnectionLimit: int
                QueuePrefetchCount: int
                QueueRetryCount: int
                QueueMessageExpiryTimeInMinutes: int
                QueueMessageExpiryCheckEveryMinutes: int
                MissingFilesRetryIntervalInMinutes: int array
                FailingFilesRetryIntervalInMinutes: int array
                RequestProgramIdRetryIntervalInMinutes: int array
                MediaSetMonitorQueriesPerMinute: int
                MediaSetMonitorRepairsPerMinute: int
                MediaSetMonitorRecentWindowInMinutes: int
                MediaSetMonitorMaxRetries: int
                MediaSetPassivationTimeoutOnActivationInMinutes: int
                MediaSetPassivationTimeoutOnCommandsInMinutes: int
                OriginStorageCleanupIntervalInHours: int
                PlaybackIdleStateTimeoutIntervalInSeconds: int
                PublishConfirmationTimeoutInSeconds: int
                AskTimeoutInSeconds: int
                ReminderAckTimeoutInSeconds: int
                ExternalRequestTimeoutInSeconds: int
                TranscodingReminderIntervalInMinutes: int
                TranscodingReminderTimeoutInDays: int
                PotionCompletionReminderIntervalInMinutes: int
                PotionCompletionExpiryIntervalInHours: int
                PlaybackCompletionReminderIntervalInMinutes: int
                PlaybackCompletionExpiryIntervalInHours: int
                PlaybackEventsRetentionPeriodInDays: int
                ClearMediaSetReminderIntervalInHours: int
                WebApiRouterIntervalBetweenRequestsInMilliseconds: int
            }

        [<CLIMutable>]
        type Origin = { name: string; enable: bool }

        [<CLIMutable>]
        type RetentionPeriodSource = { source: string; days: int }

        [<CLIMutable>]
        type RetentionPeriodDto =
            {
                Sources: RetentionPeriodSource array
                ExceptWindows: string
                ExceptLinux: string
            }

            member this.Except =
                if RuntimeInformation.IsOSPlatform(OSPlatform.Windows) then
                    this.ExceptWindows
                else
                    this.ExceptLinux

        [<CLIMutable>]
        type RetentionPeriod =
            {
                Sources: RetentionPeriodSource array
                Except: string
            }

        [<CLIMutable>]
        type PotionLink = { Url: string; ForwardedFrom: string }

        [<CLIMutable>]
        type PotionMessagePriority = { source: string; priority: int }

        [<CLIMutable>]
        type Potion =
            {
                NumberOfShards: int
                RetentionPeriod: RetentionPeriod
                SkipNotificationForSources: string array
                Mdb: PotionLink
                Publication: PotionLink
                MessagePriorities: PotionMessagePriority array
                PendingCommandsRetryIntervalInMinutes: int array
            }

        [<CLIMutable>]
        type PsMessagePriority =
            {
                source: string
                rights: string
                hours: string
                priority: int
            }

        [<CLIMutable>]
        type PathMapping = { From: string; To: string }

        [<CLIMutable>]
        type PsArchiveTimeout = { size: int; timeout: int }

        [<CLIMutable>]
        type PsMediaFiles =
            {
                DestinationRoot: string
                UseDestinationRoot: bool
                DropFolder: string
                UseDropFolder: bool
                CheckTranscodingLocation: bool
                ArchiveRoot: string
                AlternativeArchiveRoot: string
                ArchiveTimeouts: PsArchiveTimeout array
            }

        [<CLIMutable>]
        type PsMessagePrioritiesDto =
            {
                Sources: PsMessagePriority array
                ExceptionsWindows: string
                ExceptionsLinux: string
                ExceptionsPriority: int
                ExceptionsCutoffInDays: int
            }

            member this.Exceptions =
                if RuntimeInformation.IsOSPlatform(OSPlatform.Windows) then
                    this.ExceptionsWindows
                else
                    this.ExceptionsLinux

        [<CLIMutable>]
        type PsMessagePriorities =
            {
                Sources: PsMessagePriority array
                Exceptions: string
                ExceptionsPriority: int
                ExceptionsCutoffInDays: int
            }

        [<CLIMutable>]
        type PsServiceBusListener =
            {
                QueueName: string
                ServiceBusName: string
            }

        [<CLIMutable>]
        type PsServiceBusPublisher =
            {
                Topic: string
                ServiceBusName: string
            }

        [<CLIMutable>]
        type Ps =
            {
                NumberOfShards: int
                RetentionPeriod: RetentionPeriodDto
                ForceOperationForSources: string array
                MediaFiles: PsMediaFiles
                UploadProgramBeforePublishTimeInHours: int
                RemoveProgramAfterExpirationTimeInHours: int
                MessagePriorities: PsMessagePrioritiesDto
                RadioTranscodingListener: PsServiceBusListener
                UsageRightsListener: PsServiceBusListener
                PlaybackEvents: PsServiceBusPublisher
                DistributionStatusEvents: PsServiceBusPublisher
            }

        let hasGlobalConnect origins =
            origins |> Seq.exists (fun x -> x.name = "GlobalConnect" && x.enable)

        let makePathMappings (pathMappings: string array array) =
            pathMappings
            |> Array.map (fun fromTo ->
                if Array.length fromTo = 2 then
                    {
                        From = Array.item 0 fromTo
                        To = Array.item 1 fromTo
                    }
                else
                    failwithf "Invalid PathMappings")
            |> List.ofArray

        [<CLIMutable>]
        type Upload =
            {
                NumberOfShards: int
                SkipProcessingForSources: string array
                MinimumMessagePriority: int
            }

        [<CLIMutable>]
        type Subtitles =
            {
                BaseURL: string
                BaseUrlTvSubtitles: string
                SourceRootWindows: string
                SourceRootLinux: string
                DestinationRootWindows: string
                DestinationRootLinux: string
            }

        [<CLIMutable>]
        type RadioFilesSelector = { label: string; bitRate: int }

        [<CLIMutable>]
        type Radio =
            {
                FilesSelector: RadioFilesSelector array
            }

        [<CLIMutable>]
        type S3 =
            {
                BucketName: string
                NumberOfIngesters: int
                ConcurrentServiceRequests: int
            }

        [<CLIMutable>]
        type WebApi =
            {
                PortNumber: int
                SocketPortNumber: int
                EnableDeveloperEndpoints: bool
            }

        [<CLIMutable>]
        type HealthCheckComponent =
            {
                name: string
                intervalSeconds: int
                enabled: bool
            }

        [<CLIMutable>]
        type HealthCheck =
            {
                Components: HealthCheckComponent array
                TooManyReminders: int
            }

        [<CLIMutable>]
        type Instrumentation = { EnablePhobos: bool }

        [<CLIMutable>]
        type Settings =
            {
                Features: Features
                Limits: Limits
                Origins: Origin array
                PathMappings: string array array
                Potion: Potion
                Ps: Ps
                Upload: Upload
                Subtitles: Subtitles
                Radio: Radio
                S3: S3
                WebApi: WebApi
                HealthCheck: HealthCheck
                Instrumentation: Instrumentation
            }

    type OddjobConfig =
        {
            Features: OddjobConfig.Features
            Limits: OddjobConfig.Limits
            Origins: OddjobConfig.Origin array
            PathMappings: OddjobConfig.PathMapping list
            Potion: OddjobConfig.Potion
            Ps: OddjobConfig.Ps
            Upload: OddjobConfig.Upload
            Subtitles: OddjobConfig.Subtitles
            Radio: OddjobConfig.Radio
            S3: OddjobConfig.S3
            WebApi: OddjobConfig.WebApi
            HealthCheck: OddjobConfig.HealthCheck
            Instrumentation: OddjobConfig.Instrumentation
        }

    let makeOddjobConfig (settings: OddjobConfig.Settings) =
        {
            Features = settings.Features
            Limits = settings.Limits
            Origins = settings.Origins
            PathMappings = OddjobConfig.makePathMappings settings.PathMappings
            Potion = settings.Potion
            Ps = settings.Ps
            Upload = settings.Upload
            Subtitles = settings.Subtitles
            Radio = settings.Radio
            S3 = settings.S3
            WebApi = settings.WebApi
            HealthCheck = settings.HealthCheck
            Instrumentation = settings.Instrumentation
        }

    [<CLIMutable>]
    type EventStore =
        {
            PublishUrl: string
            QueryUrl: string
            AuthorizationKey: string
        }

    let eventStoreSettings eventStoreConnectionString : EventStore =
        let config = eventStoreConnectionString |> parseConnectionString
        {
            PublishUrl = config["PublishUrl"]
            QueryUrl = config["QueryUrl"]
            AuthorizationKey = config["AuthorizationKey"]
        }

    [<RequireQualifiedAccess>]
    module QueuesConfig =
        [<CLIMutable>]
        type QueueBinding =
            {
                exchangeName: string
                routingKey: string
            }

        [<CLIMutable>]
        type QueueHosts = { Amqp: string; Http: string }

        [<CLIMutable>]
        type QueueServer =
            {
                Name: string
                Hosts: QueueHosts
                Port: int
                ManagementPort: int
                VirtualHost: string
                Username: string
                Password: string
            }

        [<CLIMutable>]
        type Exchange =
            {
                Name: string
                Server: string
                Type: string
                BindTo: QueueBinding array
                Siblings: string array
            }

        [<CLIMutable>]
        type QueueFeatures =
            {
                DLX: string
                TTL: int
                MaxPriority: int
            }

        [<CLIMutable>]
        type Queue =
            {
                Category: string
                Server: string
                QueueName: string
                BindFrom: QueueBinding array
                Features: QueueFeatures
            }

        [<CLIMutable>]
        type Connections = { QueueServers: QueueServer array }

        [<CLIMutable>]
        type Settings =
            {
                Exchanges: Exchange array
                Queues: Queue array
            }

    [<CLIMutable>]
    type QueuesConfig =
        {
            QueueServers: QueuesConfig.QueueServer array
            Exchanges: QueuesConfig.Exchange array
            Queues: QueuesConfig.Queue array
        }

    let makeQueuesConfig (connections: QueuesConfig.Connections) (settings: QueuesConfig.Settings) =
        {
            QueueServers = connections.QueueServers
            Exchanges = settings.Exchanges
            Queues = settings.Queues
        }

    [<CLIMutable>]
    type ExpirationExceptionsYaml = { Programs: string array }

    [<CLIMutable>]
    type HighPriorityYaml = { Programs: string array }

    module Tekstebanken =
        type ApiKeyMapping =
            {
                Url: string
                ApiKey: string
                EnvShortname: string
            }

        [<CLIMutable>]
        type Config =
            {
                ApiKeyMappings: ApiKeyMapping array
                Format: string
            }

    let private loadYamlConfiguration<'T> (configFile: string) =
        use reader = new StreamReader(configFile)
        let serializer = SharpYaml.Serialization.Serializer()
        serializer.Deserialize<'T>(reader)

    let loadExpirationExceptionsConfiguration (configFile: string) =
        loadYamlConfiguration<ExpirationExceptionsYaml> configFile

    let loadHighPriorityConfiguration (configFile: string) =
        loadYamlConfiguration<HighPriorityYaml> configFile

    [<RequireQualifiedAccess>]
    type FeatureFlags =
        | AppSettings of IDictionary<string, obj>
        | ConfigCat of ConfigCat.Client.IConfigCatClient

    [<CLIMutable>]
    type ConnectionStrings =
        {
            Akka: string
            AkkaManagementTableStorage: string
            AmazonS3: string
            AzureNrk: string
            CatalogUsageRights: string
            EventStore: string
            OtelCollector: string
            GranittMySql: string
            InfluxDBIngesters: string
            PsServiceBus: string
            RadioServiceBus: string
            SideloadingAzureStorage: string
        }

    let inline getFeatureFlag<'a> featureFlagsSource keyName (defaultValue: 'a) : 'a =
        match featureFlagsSource with
        | FeatureFlags.AppSettings settings ->
            if settings.ContainsKey keyName then
                Convert.ChangeType(settings[keyName], typedefof<'a>) |> unbox
            else
                Unchecked.defaultof<'a>
        | FeatureFlags.ConfigCat configCatClient -> configCatClient.GetValue(keyName, defaultValue)

    [<RequireQualifiedAccess>]
    module ConfigKey =
        [<Literal>]
        let EnableDefaultPotionGeoBlock = "enableDefaultPotionGeoBlock"
        [<Literal>]
        let CorrectPotionGeoBlockOnActivation = "correctPotionGeoBlockOnActivation"
        [<Literal>]
        let SkipNotificationForSources = "skipNotificationForSources"
        [<Literal>]
        let RequiredContactPointNumber = "requiredContactPointNumber"
        [<Literal>]
        let EnableQuartzLogging = "enableQuartzLogging"
        [<Literal>]
        let DisableGranittUpdate = "disableGranittUpdate"
        [<Literal>]
        let DisablePhobosMetrics = "disablePhobosMetrics"
        [<Literal>]
        let DisablePhobosTracing = "disablePhobosTracing"
        [<Literal>]
        let OtelInstrumentation = "otelInstrumentation"
