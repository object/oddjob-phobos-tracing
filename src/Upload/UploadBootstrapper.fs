namespace Nrk.Oddjob.Upload

module UploadBootstrapperUtils =

    open System
    open Akka.Actor
    open Akka.Routing
    open Akkling

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Upload.MediaSetController

    open UploadTypes
    open MediaSetStatusPersistence

    let createUploadActorProps actorFactories =
        { new IUploadActorPropsFactory with
            member _.GetUploadActorProps origin mediaSetId mediaSetController clientRef clientContentId =
                actorFactories
                |> Map.tryFind origin
                |> Option.map (fun (factory: IOriginSpecificUploadActorPropsFactory) ->
                    factory.GetUploadActorProps mediaSetId mediaSetController clientRef clientContentId)
        }

    let getUploadClientRef externalGroupIdResolver potionMediator entityId =
        ClientRef.Potion
            {
                PotionMediator = retype potionMediator
                ExternalGroupIdResolver = externalGroupIdResolver
            }

    let startPlayabilityCompletionReminder scheduler recipientPath interval mediaSetId logger =
        let bumpMessage: obj =
            {
                Dto.Events.MediaSetPlayabilityBump.ProgramId = mediaSetId.ContentId.Value
            }
        let triggerTime = DateTimeOffset.Now.AddSafely(interval)
        Reminders.rescheduleRepeatingReminderTask
            scheduler
            Reminders.PlayabilityBump
            mediaSetId.ContentId.Value
            bumpMessage
            recipientPath
            triggerTime
            interval
            logger

    let createMediaSetControllerProps
        (oddjobConfig: OddjobConfig)
        akkaConnectionString
        uploadActorProps
        actionEnvironment
        clientRef
        startCompletionReminder
        statusPersistenceActor
        stateCacheActor
        mediaSetStatusPublisher
        mediaSetResourceStatePublisher
        uploadScheduler
        psScheduler
        activityContext
        =
        {
            AkkaConnectionString = akkaConnectionString
            UploadActorPropsFactory = uploadActorProps
            Origins = Origins.active oddjobConfig
            ActionEnvironment = actionEnvironment
            PersistMediaSetStatus = fun cmd -> statusPersistenceActor <! StatusUpdateCommand cmd
            PersistMediaSetState = fun cmd -> stateCacheActor <! cmd
            MediaSetStatusPublisher = mediaSetStatusPublisher
            MediaSetResourceStatePublisher = mediaSetResourceStatePublisher
            ClientRef = clientRef
            StartCompletionReminder = startCompletionReminder
            UploadScheduler = uploadScheduler
            PsScheduler = psScheduler
            FileSystem = LocalFileSystemInfo()
            DestinationRoot = oddjobConfig.Ps.MediaFiles.DestinationRoot
            ArchiveRoot = oddjobConfig.Ps.MediaFiles.ArchiveRoot
            AlternativeArchiveRoot = oddjobConfig.Ps.MediaFiles.AlternativeArchiveRoot
            SubtitlesSourceRootWindows = oddjobConfig.Subtitles.SourceRootWindows
            SubtitlesSourceRootLinux = oddjobConfig.Subtitles.SourceRootLinux
            PassivationTimeoutOnActivation = TimeSpan.ToOption TimeSpan.FromMinutes oddjobConfig.Limits.MediaSetPassivationTimeoutOnActivationInMinutes
            PassivationTimeoutOnCommands = TimeSpan.ToOption TimeSpan.FromMinutes oddjobConfig.Limits.MediaSetPassivationTimeoutOnCommandsInMinutes
            ReminderAckTimeout = TimeSpan.ToOption TimeSpan.FromSeconds oddjobConfig.Limits.ReminderAckTimeoutInSeconds
            PathMappings = oddjobConfig.PathMappings
            StorageCleanupDelay = TimeSpan.FromHours oddjobConfig.Limits.OriginStorageCleanupIntervalInHours
            ActivityContext = activityContext
        }

    let uploadShardActor mediaSetControllerProps uploadMediator (mailbox: Actor<_>) =
        let actorName = $"msc:%s{mailbox.Self.Path.Name}"
        let mediaSetController =
            spawn mailbox actorName (propsPersistNamed "upload-mediaset-controller" (mediaSetControllerActor mediaSetControllerProps uploadMediator))
        let rec loop () =
            actor {
                let! (message: obj) = mailbox.Receive()
                retype mediaSetController <<! message
                ignored ()
            }
        loop ()
