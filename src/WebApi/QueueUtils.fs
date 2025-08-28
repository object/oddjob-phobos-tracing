namespace Nrk.Oddjob.WebApi

module QueueUtils =

    open System
    open System.Text
    open Akka.Event
    open Azure.Messaging.ServiceBus.Administration
    open RabbitMQ.Client

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Core.Queues
    open Nrk.Oddjob.Ps.PsDto
    open HealthCheckTypes

    let private getConnection (queuesConfig: QueuesConfig) =
        let server = queuesConfig.QueueServers |> Seq.head
        let connectionFactory =
            ConnectionFactory(HostName = server.Hosts.Amqp, VirtualHost = server.VirtualHost, UserName = server.Username, Password = server.Password)
        connectionFactory.CreateConnectionAsync()

    let private createMessageProperties headers priority =
        let msgProperties = BasicProperties()
        msgProperties.Persistent <- true
        headers |> Option.iter (fun headers -> msgProperties.Headers <- headers)
        msgProperties.Priority <- priority
        msgProperties

    let publishToExchange (queuesConfig: QueuesConfig) (category: ExchangeCategory) (message: string) priority =
        let exchange = queuesConfig.Queues |> Seq.find (fun q -> q.Category = category.ToString()) |> (_.QueueName)
        task {
            use! connection = getConnection queuesConfig
            use! model = connection.CreateChannelAsync()
            let msgProperties = createMessageProperties None priority
            do! model.BasicPublishAsync(exchange, "", true, msgProperties, ReadOnlyMemory(Encoding.UTF8.GetBytes(message)))
        }

    let getRabbitMqQueueHealth (serverConfig: QueuesConfig.QueueServer) (queue: QueuesConfig.Queue) (logger: ILoggingAdapter) =
        let tenMinutes = 600
        let errorLevelTotalMessageCount = 5000
        let errorLevelUndeliveredMessageCount = 100

        try
            match QueueUtils.Metrics.getQueueStats serverConfig queue.QueueName tenMinutes logger with
            | Result.Ok stats ->
                let metrics =
                    [
                        {
                            Name = "ConsumerCount"
                            Value = int64 stats.ConsumerCount
                        }
                        {
                            Name = "MessageCount"
                            Value = int64 stats.MessageCount
                        }
                        {
                            Name = "MessagesHandledLastTenMin"
                            Value = int64 stats.MessageStats[QueueUtils.Metrics.StatsType.DeliverGet].WithinInterval
                        }
                    ]
                if stats.ConsumerCount = 0 then
                    HealthCheckResult.Error(queue.QueueName, "No consumers", metrics)
                else if stats.MessageCount > errorLevelTotalMessageCount then
                    HealthCheckResult.Error(queue.QueueName, "Number of messages", metrics)
                else if
                    stats.MessageCount > errorLevelUndeliveredMessageCount
                    && stats.MessageStats[QueueUtils.Metrics.StatsType.DeliverGet].WithinInterval <= 0
                then
                    HealthCheckResult.Error(queue.QueueName, "Messages are not delivered", metrics)
                else
                    HealthCheckResult.Ok(HealthCheckStatus.Ok, metrics)
            | Result.Error err -> HealthCheckResult.Warning(queue.QueueName, err, List.empty)

        with exn ->
            let statsUrl = QueueUtils.Metrics.getStatsUrl serverConfig queue.QueueName tenMinutes
            let errorMessage = $"Error retrieving stats from queue %s{queue.QueueName} (url: %s{statsUrl})"
            logger.Error(exn.GetBaseException(), errorMessage)
            HealthCheckResult.Error(queue.QueueName, errorMessage, [])

    let toQueueItems (queuesConfig: QueuesConfig) categoryNames =
        let queues = queuesConfig.Queues |> Seq.toList
        categoryNames |> List.map (fun name -> (queues |> List.find (fun queue -> queue.Category = name)))

    let getRabbitMqHealth (queuesConfig: QueuesConfig) checkInterval =
        let serverConfig = queuesConfig.QueueServers |> Seq.head

        [
            QueueCategory.PsProgramsWatch
            QueueCategory.PsSubtitlesWatch
            QueueCategory.PsTranscodingWatch
            QueueCategory.PotionWatch
        ]
        |> List.map (_.ToString())
        |> toQueueItems queuesConfig
        |> List.mapi (fun i queue ->
            let name = queue.Category
            ComponentSpec.WithChecker(name, getRabbitMqQueueHealth serverConfig queue, checkInterval name, TimeSpan.FromSeconds(float i) |> Some))

    let getServiceBusQueueHealth (managementClient: ServiceBusAdministrationClient) queueName (_: ILoggingAdapter) =
        let properties = managementClient.GetQueueRuntimePropertiesAsync(queueName) |> Async.AwaitTask |> Async.RunSynchronously
        if properties.HasValue then
            let metrics =
                [
                    {
                        Name = "ScheduledMessageCount"
                        Value = int64 properties.Value.ScheduledMessageCount
                    }
                    {
                        Name = "MessageCount"
                        Value = int64 properties.Value.TotalMessageCount
                    }
                    {
                        Name = "DeadLetterMessageCount"
                        Value = int64 properties.Value.DeadLetterMessageCount
                    }
                    {
                        Name = "SizeInBytes"
                        Value = int64 properties.Value.SizeInBytes
                    }
                ]

            match properties.Value.DeadLetterMessageCount with
            | 0L -> HealthCheckResult.Ok("ServiceBusQueueHealth", metrics)
            | num -> HealthCheckResult.Warning("ServiceBusQueueHealth", $"{num} messages on {queueName} dead letter queue", metrics)
        else
            HealthCheckResult.Warning("ServiceBusQueueHealth", $"Error retrieving stats from queue {queueName}", [])

    let getServiceBusHealth (oddjobConfig: OddjobConfig) connectionStrings checkInterval =
        let managementClient = ServiceBusAdministrationClient(connectionStrings.RadioServiceBus)
        [
            (ServiceBusAdministrationClient(connectionStrings.PsServiceBus), oddjobConfig.Ps.UsageRightsListener.QueueName)
            (ServiceBusAdministrationClient(connectionStrings.RadioServiceBus), oddjobConfig.Ps.RadioTranscodingListener.QueueName)
        ]
        |> List.mapi (fun i (managementClient, queueName) ->
            ComponentSpec.WithChecker(
                queueName,
                getServiceBusQueueHealth managementClient queueName,
                checkInterval queueName,
                TimeSpan.FromSeconds(float i) |> Some
            ))

    let getQueueComponentSpec name (oddjobConfig: OddjobConfig) queuesConfig connectionStrings checkInterval isEnabled =

        let rabbitMqName = "RabbitMQ"
        let serviceBusName = "ServiceBus"

        ComponentSpec.WithComponents(
            name,
            [
                if isEnabled rabbitMqName then
                    yield
                        ComponentSpec.WithComponents(
                            rabbitMqName,
                            getRabbitMqHealth queuesConfig (checkInterval rabbitMqName),
                            Strategies.singleFailureFailsAll name
                        )
                if isEnabled serviceBusName then
                    yield
                        ComponentSpec.WithComponents(
                            serviceBusName,
                            getServiceBusHealth oddjobConfig connectionStrings (checkInterval serviceBusName),
                            Strategies.singleFailureFailsAll name
                        )
            ],
            Strategies.singleFailureFailsAll name
        )

    let getPsChangeJobDto programId =
        {
            ProgrammeId = programId
            Source = "oddjob web api"
            TimeStamp = DateTime.Now
            RetryCount = None
            PublishingPriority = None
        }

    let sendToQueue<'msg> config exchangeCategory (message: 'msg) priority =
        task {
            try
                let msg = Serialization.SystemTextJson.serializeObject SerializationOptions.PascalCase message
                do! publishToExchange config exchangeCategory msg priority
                return Result.Ok()
            with exn ->
                return Result.Error exn
        }
