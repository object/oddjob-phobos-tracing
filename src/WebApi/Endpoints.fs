namespace Nrk.Oddjob.WebApi

open System
open System.Net
open Akka.Actor
open Akkling
open Giraffe
open Giraffe.EndpointRouting
open Giraffe.OpenApi

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.S3.S3Api
open Nrk.Oddjob.Upload.MediaSetStatusPersistence
open Nrk.Oddjob.Core.Config
open Nrk.Oddjob.Core.Queues
open Nrk.Oddjob.Potion.PotionTypes
open Nrk.Oddjob.Ps.PsDto

open EventJournalUtils
open WebAppUtils
open RecentStatus
open IngesterStatus
open EndpointsMetadata

type WebAppProps =
    {
        ConnectionStrings: ConnectionStrings
        Schedulers: Map<string, Quartz.IScheduler * IActorRef>
        UploadMediator: IActorRef<obj>
        PsMediator: IActorRef<obj>
        PotionMediator: IActorRef<obj>
        MediaSetStateCache: IActorRef<MediaSetStateCache.MediaSetStateCacheCommand>
    }

module Endpoints =

    let buildEndpoints config =

        let (aprops: WebAppProps,
             _: string,
             system: ActorSystem,
             oddjobConfig: OddjobConfig,
             queuesConfig: QueuesConfig,
             statusPersistenceActor: IActorRef<MediaSetStatusCommand>,
             _: ActivitySourceContext) =
            config

        let s3AccessKeys = aprops.ConnectionStrings.AmazonS3 |> parseConnectionString
        let s3Api = new S3Api(oddjobConfig.S3, s3AccessKeys)

        let materializer = Akka.Streams.ActorMaterializer.Create(system)
        let actionEnvironment = MediaSetUtils.createActionEnvironment oddjobConfig aprops.ConnectionStrings s3Api system.Log
        let reader = Sharding.createStateReader system oddjobConfig
        let globalConnectIngestersMonitor = spawnIngesterMonitor system PubSub.Topic.GlobalConnectIngesterStatus |> retype

        let uploadMediator =
            retype <| createProxyRouter (makeActorName [ "WebApi Upload" ]) aprops.UploadMediator oddjobConfig system
        let psMediator = retype <| createProxyRouter (makeActorName [ "WebApi PS MediaSet" ]) aprops.PsMediator oddjobConfig system
        let proxyPaths =
            [
                (Reminders.PsRole, retype aprops.PsMediator)
                (Reminders.PotionRole, retype aprops.PotionMediator)
                (Reminders.UploadRole, retype aprops.UploadMediator)
            ]
            |> Map.ofList
            |> Map.map (fun _ x -> x.Path)
        let healthChecker = HealthCheck.spawnHealthChecker system oddjobConfig queuesConfig aprops.ConnectionStrings |> Some

        [
            GET
            <| [
                route "/" <| redirectTo false "/swagger"
                route "/index.html" <| redirectTo false "/swagger"

                route "/health"
                <| warbler (fun _ ->
                    match healthChecker with
                    | Some healthChecker ->
                        let status: Async<HealthStatus> = healthChecker <? HealthCheck.Actors.QueryState
                        let httpStatusCodeAsync =
                            async {
                                let! health = status
                                return
                                    match health.Status with
                                    | "OK"
                                    | "WARNING" -> HttpStatusCode.OK
                                    | _ -> HttpStatusCode.InternalServerError
                            }
                        status |> toResponseTaskWithStatusCode httpStatusCodeAsync
                    | None -> Successful.OK "Not supported on this port number")
                |> withOpenApi Operation.GetHealth

                route "/recent"
                <| warbler (fun _ ->
                    let result = getRecentStatus statusPersistenceActor
                    result |> toResponseWithMediaSetStateFilterTask oddjobConfig aprops.MediaSetStateCache reader actionEnvironment)
                |> withOpenApi Operation.GetRecent

                route "/ingesters/globalconnect"
                <| warbler (fun _ ->
                    let result = getIngesterStatus globalConnectIngestersMonitor
                    result |> toResponseTask)
                |> withOpenApi Operation.GetGlobalConnectIngesters

                routef "/status/%s:clientId/%s:contentId" (fun (clientId, contentId) ->
                    //use activity = Instrumentation.startActivity activitySource "status" [ ("contentId", contentId) ]
                    MediaSetUtils.getAggregatedSummary
                        oddjobConfig
                        aprops.ConnectionStrings
                        s3Api
                        clientId
                        contentId
                        system.Log
                        aprops.MediaSetStateCache
                        reader
                    |> toResponseTask)
                |> withOpenApi Operation.GetMediaSetStatus

                routef "/status/%s:mediaSetId" (fun mediaSetId ->
                    //use activity = Instrumentation.startActivity activitySource "status" [ ("mediaSetId", mediaSetId) ]
                    match MediaSetId.tryParse (String.toLower mediaSetId) with
                    | Some mediaSetId ->
                        MediaSetUtils.getAggregatedSummary
                            oddjobConfig
                            aprops.ConnectionStrings
                            s3Api
                            mediaSetId.ClientId.Value
                            mediaSetId.ContentId.Value
                            system.Log
                            aprops.MediaSetStateCache
                            reader
                        |> toResponseTask
                    | None -> RequestErrors.notFound (text "Not Found"))

                routef "/mediasets/%s:clientId/%s:contentId" (fun (clientId, contentId) ->
                    let actionEnvironment = MediaSetUtils.createActionEnvironment oddjobConfig aprops.ConnectionStrings s3Api system.Log
                    let mediaSetId = sprintf "%s~%s" clientId contentId |> String.toLower
                    MediaSetUtils.getMediaSetStateWithFilter aprops.MediaSetStateCache oddjobConfig reader mediaSetId actionEnvironment
                    |> MediaSetUtils.toResponseWithMediaSetStateFilterTask)
                |> withOpenApi Operation.GetMediaSetState

                routef "/mediasets/%s:mediaSetId" (fun mediaSetId ->
                    let actionEnvironment = MediaSetUtils.createActionEnvironment oddjobConfig aprops.ConnectionStrings s3Api system.Log
                    match MediaSetId.tryParse (String.toLower mediaSetId) with
                    | Some mediaSetId ->
                        MediaSetUtils.getMediaSetStateWithFilter aprops.MediaSetStateCache oddjobConfig reader mediaSetId.Value actionEnvironment
                        |> MediaSetUtils.toResponseWithMediaSetStateFilterTask
                    | None -> RequestErrors.notFound (text "Not Found"))

                routef "/events/%s:eventType/%s:eventId" (fun (eventType, eventId) ->
                    let eventId = getFullEventId eventType eventId aprops.ConnectionStrings.Akka
                    Queries.getJournalEventsById system materializer eventType (eventId.Replace("/", "~"))
                    |> toResponseWithSequenceFilterTask)
                |> withOpenApi Operation.GetEvents

                routef "/events/%s:eventType/%s:eventId/state" (fun (eventType, eventId) ->
                    let actionEnvironment = MediaSetUtils.createActionEnvironment oddjobConfig aprops.ConnectionStrings s3Api system.Log
                    let eventId = getFullEventId eventType eventId aprops.ConnectionStrings.Akka
                    let notSupportedResponse =
                        async {
                            return
                                {
                                    PersistenceId = eventId
                                    State = null
                                }
                        }
                        |> toResponseTask
                    match eventType, eventId with
                    | "msc", _ -> getUploadJournalState reader eventId <| MediaSetUtils.getRepairActions oddjobConfig actionEnvironment |> toResponseTask
                    | "psf", _ -> getPsFilesJournalState reader eventId |> toResponseTask
                    | "psp", _ -> getPsPlayabilityJournalState reader eventId |> toResponseTask
                    | "potion", _ -> getPotionSetJournalState reader eventId |> toResponseTask
                    | _ -> notSupportedResponse)
                |> withOpenApi Operation.GetEventsState

                routef "/activate/%s:clientId/%s:contentId" (activateMediaSet uploadMediator) |> withOpenApi Operation.ActivateMediaSet

                routef "/deactivate/%s:clientId/%s:contentId" (deactivateMediaSet uploadMediator)
                |> withOpenApi Operation.DeactivateMediaSet

                routef "/ps/rights/%s:contentId" (fun contentId ->
                    let mediaSetId = MediaSetId.create (Alphanumeric PsClientId) (ContentId.create contentId)
                    let filesState = reader.PsFilesState mediaSetId.Value |> Async.RunSynchronously
                    contentId
                    |> PiProgId.create
                    |> PsUtils.getRightsInfo aprops.ConnectionStrings.CatalogUsageRights filesState
                    |> toResponseFromResult)
                |> withOpenApi Operation.GetPsRights

                routef "/ps/files/%s:contentId" (fun contentId -> contentId |> PiProgId.create |> PsUtils.getFilesInfo reader system.Log |> toResponse)
                |> withOpenApi Operation.GetPsFiles

                routef "/ps/transcoding/%s:contentId" (fun contentId ->
                    let mediaSetId = MediaSetId.create (Alphanumeric PsClientId) (ContentId.create contentId)
                    let filesState = reader.PsFilesState mediaSetId.Value |> Async.RunSynchronously
                    contentId |> PiProgId.create |> PsUtils.getTranscodingInfo filesState |> toResponse)
                |> withOpenApi Operation.GetPsTranscoding

                routef "/ps/archive/%s:contentId" (fun contentId ->
                    let mediaSetId = MediaSetId.create (Alphanumeric PsClientId) (ContentId.create contentId)
                    let filesState = reader.PsFilesState mediaSetId.Value |> Async.RunSynchronously
                    contentId |> PiProgId.create |> PsUtils.getArchiveFiles filesState system.Log |> toResponse)
                |> withOpenApi Operation.GetPsArchive

                route "/reminders" <| warbler (fun _ -> ReminderGroups.toResponseWithRemindersFilterTask aprops.ConnectionStrings.Akka)
                |> withOpenApi Operation.GetReminders
            ]

            PUT
            <| [
                routef "/repair/%s:clientId/%s:contentId" (repairMediaSet uploadMediator) |> withOpenApi Operation.RepairMediaSet

                routef "/migrate/%s:clientId/%s:contentId" (migrateMediaSet uploadMediator) |> withOpenApi Operation.MigrateMediaSet

                routef "/playback/ps/%s:contentId" (fun contentId ->
                    sendPsPlaybackEvents contentId aprops.MediaSetStateCache uploadMediator (retype psMediator) (Origins.active oddjobConfig))
                |> withOpenApi Operation.ResyncPlayback

                routef "/ps/rights/%s:contentId" (fun contentId ->
                    if oddjobConfig.WebApi.EnableDeveloperEndpoints then
                        bindModel<RightsInfo> None (fun rights -> PsUtils.updateRights (retype psMediator) contentId rights |> toResponse)
                    else
                        RequestErrors.METHOD_NOT_ALLOWED "Unavailable in current environment")
                |> withOpenApi Operation.UpdatePsRights

                routef "/ps/files/%s:contentId" (fun contentId ->
                    bindModel<PsTranscodingDto> None (fun files ->
                        let mediaSetId = MediaSetId.create (Alphanumeric PsClientId) (ContentId.create contentId)
                        let filesState = reader.PsFilesState mediaSetId.Value |> Async.RunSynchronously
                        match files with
                        | PsTranscodingDto.Video files ->
                            PsUtils.updateVideoFiles (retype psMediator) contentId filesState files reader system.Log |> toResponse
                        | PsTranscodingDto.Audio files ->
                            PsUtils.updateAudioFiles (retype psMediator) contentId filesState files reader system.Log |> toResponse))
                |> withOpenApi Operation.UpdatePsFiles
            ]

            POST
            <| [
                routef "/queues/ps/programs/%s:contentId" (fun contentId ->
                    sendMessageToPsQueue queuesConfig ExchangeCategory.PsProgramsWatch (QueueUtils.getPsChangeJobDto contentId))
                |> withOpenApi Operation.SendPsProgramToQueue

                route "/queues/ps/programs"
                <| bindModel<PsChangeJobDto> None (sendMessageToPsQueue queuesConfig ExchangeCategory.PsProgramsWatch)
                |> withOpenApi Operation.SendPsMessageToQueue

                route "/queues/potion" <| bindModel<PotionCommand> None (sendMessageToPotionQueue queuesConfig)
                |> withOpenApi Operation.SendPotionMessageToQueue

                route "/reminders"
                <| bindModel<CreateReminderInfo> None (schedulerReminder (aprops.Schedulers |> Map.map (fun _ -> snd)) proxyPaths)
                |> withOpenApi Operation.CreateReminder
            ]

            DELETE
            <| [
                routef "/clear/%s:clientId/%s:contentId" (fun (clientId, contentId) ->
                    if oddjobConfig.WebApi.EnableDeveloperEndpoints then
                        clearMediaSet clientId contentId uploadMediator oddjobConfig
                    else
                        RequestErrors.METHOD_NOT_ALLOWED "Unavailable in current environment")
                |> withOpenApi Operation.ClearMediaSet
            ]
        ]
