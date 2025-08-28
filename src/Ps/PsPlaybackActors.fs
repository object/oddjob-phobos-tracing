namespace Nrk.Oddjob.Ps

module PsPlaybackActors =

    open System
    open FSharpx.Collections
    open Akka.Actor
    open Akka.Persistence
    open Akkling
    open Akkling.Persistence

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.JournalEventDeletion
    open Nrk.Oddjob.Core.Dto.Events

    open PsTypes
    open PsPlaybackEvents
    open PsShardMessages

    module ServiceBusPublisher =

        open Azure.Messaging.ServiceBus

        [<Literal>]
        let private HeaderStreamingType = "streamingType"

        [<Literal>]
        let private HeaderMessageType = "messageType"

        [<Literal>]
        let private HeaderSourceMedium = "sourceMedium"

        [<Literal>]
        let private HeaderProgramScope = "programScope"

        [<Literal>]
        let private HeaderPriority = "priority"

        [<NoComparison>]
        type ServiceBusEventConfig =
            {
                Client: ServiceBusClient
                Topic: string
            }

        [<NoEquality; NoComparison>]
        type ServiceBusPublisherProps =
            {
                EventSinks: Map<EventSinkType, ServiceBusEventConfig>
            }

        let serviceBusPublisherActor (aprops: ServiceBusPublisherProps) (mailbox: Actor<_>) =

            let getProgramId mediaSetId =
                (MediaSetId.parse mediaSetId).ContentId.Value.ToUpper()

            let publishMessageAsync (topicClient: ServiceBusSender) (message: PsPlaybackEvents.ServiceBusMessage) =
                let headers = message.Headers
                let subject =
                    $"%s{headers.SourceMedium}_%s{headers.StreamingType}_%s{headers.MessageType}_%s{getProgramId message.MediaSetId}"
                async {
                    logInfo mailbox $"Publishing %s{headers.SourceMedium} %s{headers.StreamingType} %s{headers.MessageType} for %s{message.MediaSetId}"
                    let clientMessage = ServiceBusMessage()
                    clientMessage.Body <- BinaryData(message.Payload)
                    clientMessage.Subject <- subject
                    clientMessage.ApplicationProperties.Add(HeaderMessageType, headers.MessageType)
                    if headers.SourceMedium <> null then
                        clientMessage.ApplicationProperties.Add(HeaderSourceMedium, headers.SourceMedium)
                    if headers.StreamingType <> null then
                        clientMessage.ApplicationProperties.Add(HeaderStreamingType, headers.StreamingType)
                    if headers.ProgramScope <> null then
                        clientMessage.ApplicationProperties.Add(HeaderProgramScope, headers.ProgramScope)
                    clientMessage.ApplicationProperties.Add(HeaderPriority, headers.Priority)
                    let! result = topicClient.SendMessageAsync clientMessage |> Async.AwaitTask |> Async.Catch
                    return
                        match result with
                        | Choice1Of2() -> Result.Ok()
                        | Choice2Of2 exn ->
                            logErrorWithExn
                                mailbox
                                exn
                                $"Topic: {topicClient.EntityPath}, MediaSetId: {message.MediaSetId}, Payload size: {message.Payload.Length}, Headers: {message.Headers}"
                            Result.Error exn
                }

            let getTraceTags eventSink message =
                [
                    ("oddjob.ps.playback.topic", eventSink.Topic)
                    ("oddjob.ps.playback.sourceMedium", message.Headers.SourceMedium)
                    ("oddjob.ps.playback.streamingType", message.Headers.StreamingType)
                    ("oddjob.ps.playback.messageType", message.Headers.MessageType)
                    ("oddjob.ps.playback.programScope", message.Headers.ProgramScope)
                    ("oddjob.ps.playback.priority", message.Headers.Priority)
                    ("oddjob.ps.programId", getProgramId message.MediaSetId)
                    ("oddjob.mediaset.mediaSetId", message.MediaSetId)
                ]

            let publishMessage message sender =
                try
                    let eventSink = aprops.EventSinks[message.EventSinkType]
                    if eventSink.Client <> null then
                        let topicClient = eventSink.Client.CreateSender eventSink.Topic
                        use _ = createTraceSpan mailbox "serviceBusPublisherActor.publishMessage" (getTraceTags eventSink message)
                        let result =
                            async {
                                let! response = publishMessageAsync topicClient message
                                do! topicClient.CloseAsync() |> Async.AwaitTask
                                return response
                            }
                            |> Async.RunSynchronously
                        sender <! result
                with ex ->
                    logErrorWithExn mailbox ex "Failed to send messages to service bus"

            let rec idle () =
                actor {
                    let! (message: obj) = mailbox.Receive()
                    return!
                        match message with
                        | :? PsPlaybackEvents.ServiceBusMessage as message' ->
                            logDebug mailbox $"Received service bus message (%X{message'.GetHashCode()})"
                            publishMessage message' (mailbox.Sender())
                            ignored ()
                        | LifecycleEvent _ -> ignored ()
                        | _ ->
                            logDebug mailbox $"Unexpected message [%s{message.GetType().Name}]"
                            unhandled ()
                }

            idle ()

    type CancelEvent = CancelEvent of eventId: string
    type private Continue = Continue

    [<Literal>]
    let DeletionBatchSize = 25

    let toPiProgId (mediaSetId: string) =
        let (ContentId contentId) = (MediaSetId.parse mediaSetId).ContentId
        PiProgId.create contentId

    let toMediaSetId programId =
        (MediaSetId.create (Alphanumeric PsClientId) (PiProgId.value programId |> ContentId)).Value

    let (|MessageWithProgId|_|) (msg: obj) =
        match msg with
        | :? MediaSetPlayabilityEventDto as message' -> Some(toPiProgId message'.MediaSetId)
        | :? PsDto.PlayabilityBump as message' -> Some(PiProgId.create message'.ProgramId)
        | :? PsDto.TranscodingBump as message' -> Some(PiProgId.create message'.ProgramId)
        | _ -> None

    type ActionOnIdleTimeout =
        | DoNothing
        | PublishNewEvent

    type PendingPlayabilityMessage =
        | NewPlayabilityEvent of MediaSetPlayabilityEventDto
        | OldPlayabilityEvent of MediaSetPlayabilityEventDto * eventId: string

    type PublishPlayabilityEvent = PublishPlayabilityEvent of PendingPlayabilityMessage

    type private StateSource =
        | FromCache
        | FromController

    [<NoEquality; NoComparison>]
    type PsPlayabilityNotificationProps =
        {
            AkkaConnectionString: string
            Origins: Origin list
            GetPsMediator: unit -> IActorRef<PsShardMessage>
            GetServiceBusPublisher: IActorContext -> IActorRef<ServiceBusMessage>
            PlaybackEventConsumerCount: int
            DistributionStatusEventConsumerCount: int
            GetEventCreator: Actor<obj> -> IPlaybackEventCreator
            MediaSetStateCache: IActorRef<MediaSetStateCache.MediaSetStateCacheCommand>
            MediaSetController: IActorRef<Message<ShardMessages.MediaSetShardMessage>> // UNDO: remove and use MediaSetStateCache when playback error is fixed
            IdleStateTimeout: TimeSpan option
            ExternalRequestTimeout: TimeSpan option
            AskTimeout: TimeSpan option
            CompletionReminderInterval: TimeSpan
            CompletionExpiryInterval: TimeSpan
            EventsRetentionInterval: TimeSpan
            PsScheduler: IActorRef
        }

    let psPlayabilityNotificationActor (aprops: PsPlayabilityNotificationProps) (mailbox: Eventsourced<_>) =

        let serviceBusPublisher = aprops.GetServiceBusPublisher mailbox.UntypedContext
        let eventCreator = aprops.GetEventCreator(mailbox :> Actor<_>)

        let stopReminderOnCompletionOrExpiry (state: PsPlayabilityHandlerState) =
            let now = DateTimeOffset.Now
            if aprops.CompletionReminderInterval > TimeSpan.MinValue && aprops.CompletionExpiryInterval > TimeSpan.MinValue then
                state.ProgramId
                |> Option.iter (fun programId ->
                    state.PendingEvent
                    |> Option.map _.Timestamp
                    |> function
                        | Some timestamp when now - timestamp < aprops.CompletionExpiryInterval -> ()
                        | _ -> Reminders.cancelReminderTask aprops.PsScheduler Reminders.PlayabilityBump (PiProgId.value programId) (logDebug mailbox))

        let applyPersistentEvent (state: PsPlayabilityHandlerState) (event: PsPersistence.IProtoBufSerializableEvent) =
            let updatePersistedEvents eventId state =
                { state with
                    PersistedEvents =
                        {
                            EventId = eventId
                            Timestamp = event.Timestamp
                            SequenceNr = mailbox.LastSequenceNr()
                        }
                        :: state.PersistedEvents
                }
            try
                match event with
                | :? PsPersistence.AssignedProgramId as event ->
                    let piProgId = event.ToDomain()
                    { state with ProgramId = Some piProgId }
                | :? PsPersistence.AssignedMediaType as event ->
                    let mediaType = event.ToDomain()
                    { state with
                        MediaType = Some mediaType
                    }
                | :? PsPersistence.PendingEvent as event ->
                    try
                        let event = event.ToPlayabilityEvent()
                        { state with PendingEvent = Some event }
                    with _ -> // Discard playability events that are no longer supported
                        state
                    |> updatePersistedEvents event.EventId
                | :? PsPersistence.CompletedEvent as event ->
                    { state with
                        PendingEvent =
                            match state.PendingEvent with
                            | Some ev when ev.EventId = event.EventId -> None
                            | _ -> state.PendingEvent
                    }
                    |> updatePersistedEvents event.EventId
                | :? PsPersistence.CancelledEvent as event ->
                    { state with
                        PendingEvent =
                            match state.PendingEvent with
                            | Some ev when ev.EventId = event.EventId -> None
                            | _ -> state.PendingEvent
                    }
                    |> updatePersistedEvents event.EventId
                | _ -> invalidOp <| $"Unexpected event %A{event}"
            with exn ->
                logErrorWithExn mailbox exn $"Failed to recover actor state, last sequenceNr: {mailbox.LastSequenceNr()}"
                reraise ()

        let persistEvent event =
            logDebug mailbox $"Persisting %s{event.GetType().Name}"
            Persist event

        let createPlaybackEvent mediaSetState psFilesState event =
            try
                eventCreator.CreatePlayabilityEvent event mediaSetState psFilesState
            with exn ->
                logErrorWithExn mailbox exn $"Failed to create playback event for [{event}]"
                ResultWithRetry.Error()

        let tryCreateDistributionStatusEvent mediaSetState event =
            eventCreator.TryCreateOnDemandStatusEvent event mediaSetState

        let publishPlaybackEvent event mediaSetId =
            use _ =
                createTraceSpan mailbox "psPlayabilityNotificationActor.publishPlaybackEvent" [ ("oddjob.ps.programId", getProgramId mediaSetId) ]
            match event with
            | ResultWithRetry.Ok evt ->
                serviceBusPublisher <! evt
                logDebug mailbox $"Playability event is published (%X{evt.GetHashCode()})"
                ResultWithRetry.Ok()
            | ResultWithRetry.Retry evt ->
                serviceBusPublisher <! evt
                logDebug mailbox $"Playability event is published but will be retried (%X{evt.GetHashCode()})"
                ResultWithRetry.Retry()
            | ResultWithRetry.Error _ -> ResultWithRetry.Error()

        let publishDistributionStatusEvent event =
            serviceBusPublisher <! event
            logDebug mailbox $"Distribution status event is published (%X{event.GetHashCode()})"

        let cancelEvent (evt: PendingPlayabilityEvent) =
            (retype mailbox.Self) <! CancelEvent evt.EventId

        let createPlayabilityEvent mediaSetId mediaSetState =
            logDebug mailbox "Generating new playability event"
            Events.MediaSetPlayabilityEvent.create
                (MediaSetId.parse mediaSetId)
                mediaSetState
                aprops.Origins
                DateTimeOffset.Now
                Events.PlayabilityEventPriority.Normal
            |> MediaSetPlayabilityEventDto.fromDomain

        let createPendingEvent (event: MediaSetPlayabilityEventDto) eventId : PendingPlayabilityEvent =
            {
                EventId = eventId
                Event = event
                Timestamp = DateTimeOffset.Now
            }

        let getOutdatedEvents (state: PsPlayabilityHandlerState) =
            let now = DateTimeOffset.Now
            state.PersistedEvents
            |> List.filter (fun x -> x.Timestamp - now > aprops.EventsRetentionInterval)
            |> List.map _.SequenceNr

        let logPendingResource origins (currentState: CurrentMediaSetState) =
            if origins |> List.contains Origin.GlobalConnect then
                let pendingGlobalConnect =
                    let pendingGlobalConnectFiles =
                        currentState.GlobalConnect.Files
                        |> Map.filter (fun _ file -> file.RemoteState.State.IsPending())
                        |> Map.keys
                        |> Seq.map (fun fileRef -> $"File-{fileRef.QualityId.Value}")
                    let pendingGlobalConnectSubtitles =
                        currentState.GlobalConnect.Subtitles
                        |> Map.filter (fun _ sub -> sub.RemoteState.State.IsPending())
                        |> Map.keys
                        |> Seq.map (fun subRef -> $"Sub-{subRef.LanguageCode.Value}-{subRef.Name.Value}")
                    let pendingGlobalConnectSmil =
                        if currentState.GlobalConnect.Smil.RemoteState.State.IsPending() then
                            [ $"Smil-{currentState.GlobalConnect.Smil.Smil.Version}" ]
                        else
                            []
                    [
                        pendingGlobalConnectFiles
                        pendingGlobalConnectSubtitles
                        pendingGlobalConnectSmil
                    ]
                    |> Seq.concat
                    |> String.concat ","
                if String.isNotNullOrEmpty pendingGlobalConnect then
                    logDebug mailbox $"Pending at GlobalConnect: [{pendingGlobalConnect}]"

        let rec init state =
            if state = PsPlayabilityHandlerState.Zero then
                logDebug mailbox "init"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? PsPersistence.IProtoBufSerializableEvent as event ->
                        let state = applyPersistentEvent state event
                        init state
                    | :? RecoveryCompleted ->
                        logDebug mailbox "Persistence recovery completed"
                        match state.ProgramId with
                        | Some programId ->
                            state.PendingEvent |> Option.iter cancelEvent
                            let mediaSetId = (toMediaSetId programId)
                            match getOutdatedEvents state with
                            | [] -> idle state mediaSetId PublishNewEvent None None
                            | events -> purge_events state mediaSetId events
                        | None -> ensure_program_id state
                    | LifecycleEvent _ -> ignored ()
                    | PersistentLifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }
        and ensure_program_id state =
            logDebug mailbox "ensure_program_id"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | MessageWithProgId progId ->
                        mailbox.Stash()
                        PsPersistence.AssignedProgramId.FromDomain progId |> box |> persistEvent :> Effect<_>
                    | :? PsPersistence.IProtoBufSerializableEvent as event ->
                        match event with
                        | :? PsPersistence.AssignedProgramId as event ->
                            let programId = event.ToDomain()
                            idle state (toMediaSetId programId) DoNothing None None
                        | _ ->
                            logError mailbox "Unexpected persistence event, still awaiting ProgramId"
                            ignored ()
                    | LifecycleEvent _ -> ignored ()
                    | PersistentLifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }
        and purge_events state mediaSetId eventsToPurge =
            logDebug mailbox "start_purging_events"
            let deletionProps: JournalEventDeletionActorProps =
                {
                    AkkaConnectionString = aprops.AkkaConnectionString
                    PersistenceId = $"psp:{normalizeActorNameSegment mediaSetId}"
                    BatchSize = DeletionBatchSize
                }
            let purgeActor =
                getOrSpawnChildActor
                    mailbox.UntypedContext
                    (makeActorName [ "Purge" ])
                    (propsNamed "ps-purge-events" <| journalEventDeletionActor deletionProps)
            purgeActor <! DeleteEvents eventsToPurge
            purging_events state mediaSetId eventsToPurge.Length
        and purging_events state mediaSetId remainingCount =
            logDebug mailbox "purging_events"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? JournalEventDeletionResponse as msg ->
                        match msg with
                        | DeletedEvents events ->
                            let remainingEvents = remainingCount - events.Length
                            if remainingEvents > 0 then
                                purging_events state mediaSetId remainingEvents
                            else
                                logDebug mailbox "Purging completed"
                                idle state mediaSetId PublishNewEvent None None
                    | :? PsShardMessage as msg ->
                        match msg with
                        | PsShardMessage.GetPlayabilityHandlerState _ ->
                            mailbox.Sender() <! PsPersistence.PlayabilityHandlerState.FromDomain state
                            ignored ()
                        | _ ->
                            mailbox.Stash()
                            ignored ()
                    | LifecycleEvent _ -> ignored ()
                    | _ ->
                        mailbox.Stash()
                        ignored ()
            }
        and idle state mediaSetId actionOnTimeout traceContext traceSpan =
            logDebug mailbox $"idle (actionOnTimeout={actionOnTimeout})"
            mailbox.UnstashAll()
            match actionOnTimeout with
            | DoNothing -> ()
            | PublishNewEvent -> mailbox.SetReceiveTimeout aprops.IdleStateTimeout
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? MediaSetPlayabilityEventDto ->
                        logDebug mailbox "Received playback event"
                        let traceContext = traceContext |> Option.defaultWith (fun () -> getTraceContext mailbox)
                        let traceSpan =
                            traceSpan
                            |> Option.defaultWith (fun () ->
                                createTraceSpanForContext
                                    mailbox
                                    traceContext
                                    "psPlayabilityNotificationActor.AggregateEvents"
                                    [ ("oddjob.ps.programId", getProgramId mediaSetId) ])
                        if Option.isSome aprops.IdleStateTimeout then
                            // Hold back playability events while they keep coming
                            idle state mediaSetId PublishNewEvent (Some traceContext) (Some traceSpan)
                        else
                            // Publish event without delay
                            retype mailbox.Self <! Continue
                            aprops.GetPsMediator() <! PsShardMessage.GetFilesState(MediaSetId.parse mediaSetId)
                            awaiting_psfiles_state state mediaSetId (Some traceContext) (Some traceSpan)
                    | :? CancelEvent as pending_event ->
                        let (CancelEvent eventId) = pending_event
                        logDebug mailbox $"Cancel event {eventId}"
                        PsPersistence.CancelledEvent.FromDomain(eventId, DateTimeOffset.Now) |> box |> persistEvent :> Effect<_>
                    | :? PsPersistence.CancelledEvent as event ->
                        let state = applyPersistentEvent state event
                        idle state mediaSetId PublishNewEvent traceContext traceSpan
                    | :? PsShardMessage as msg ->
                        match msg with
                        | PsShardMessage.GetPlayabilityHandlerState _ ->
                            mailbox.Sender() <! PsPersistence.PlayabilityHandlerState.FromDomain state
                            ignored ()
                        | _ -> unhandled ()
                    | :? PsDto.PlayabilityBump ->
                        logDebug mailbox "Received PlayabilityBump reminder"
                        state.PendingEvent |> Option.iter cancelEvent
                        stopReminderOnCompletionOrExpiry state
                        idle state mediaSetId PublishNewEvent traceContext traceSpan
                    | Reminders.LifetimeEvent e ->
                        logDebug mailbox $"Reminder lifetime event {e}"
                        ignored ()
                    | :? ReceiveTimeout ->
                        retype mailbox.Self <! Continue
                        aprops.GetPsMediator() <! PsShardMessage.GetFilesState(MediaSetId.parse mediaSetId)
                        awaiting_psfiles_state state mediaSetId traceContext traceSpan
                    | LifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }
        and awaiting_psfiles_state state mediaSetId traceContext traceSpan =
            logDebug mailbox "awaiting_psfiles_state"
            mailbox.SetReceiveTimeout aprops.ExternalRequestTimeout
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? PsPersistence.FilesState as result ->
                        let psFilesState = result.ToDomain()
                        mailbox.SetReceiveTimeout None
                        retype mailbox.Self <! Continue
                        aprops.MediaSetStateCache <! MediaSetStateCache.MediaSetStateCacheCommand.GetState mediaSetId
                        awaiting_mediaset_state state mediaSetId psFilesState traceContext traceSpan FromCache
                    | :? MediaSetPlayabilityEventDto
                    | :? CancelEvent ->
                        mailbox.Stash()
                        ignored ()
                    | :? ReceiveTimeout ->
                        let message = "Timeout awaiting PS Files state"
                        logError mailbox message
                        mailbox.SetReceiveTimeout None
                        raise <| TimeoutException message
                    | :? PsDto.PlayabilityBump ->
                        logDebug mailbox "Received PlayabilityBump reminder"
                        ignored ()
                    | LifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }
        and awaiting_mediaset_state state mediaSetId psFilesState traceContext traceSpan stateSource =
            mailbox.SetReceiveTimeout aprops.AskTimeout
            logDebug mailbox $"awaiting_mediaset_state ({getUnionCaseName stateSource})"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? Dto.MediaSet.MediaSetState as mediaSetState ->
                        mailbox.SetReceiveTimeout None
                        let mediaSetState = mediaSetState.ToDomain()
                        if stateSource = FromCache && (mediaSetState = MediaSetState.Zero || mediaSetState.LastSequenceNr = 0L) then
                            // Cache is either not present or not up-to-date, ask MediaSetController instead
                            mailbox.SetReceiveTimeout aprops.AskTimeout
                            aprops.MediaSetController
                            <! Message.create (ShardMessages.MediaSetShardMessage.GetMediaSetState <| MediaSetId.parse mediaSetId)
                            awaiting_mediaset_state state mediaSetId psFilesState traceContext traceSpan FromController
                        else
                            if stateSource = FromController then
                                // Update state cache with the value received from controller
                                aprops.MediaSetStateCache
                                <! MediaSetStateCache.MediaSetStateCacheCommand.SaveState
                                    {
                                        MediaSetId = mediaSetId
                                        State = Dto.MediaSet.MediaSetState.FromDomain mediaSetState
                                        Timestamp = DateTime.Now
                                    }
                            let isGlobalConnectPending =
                                aprops.Origins |> List.contains Origin.GlobalConnect
                                && CurrentGlobalConnectState.hasPendingResources mediaSetState.Current.GlobalConnect
                            if isGlobalConnectPending then
                                logPendingResource aprops.Origins mediaSetState.Current
                            if isGlobalConnectPending && Option.isSome aprops.IdleStateTimeout then
                                idle state mediaSetId PublishNewEvent traceContext traceSpan
                            else
                                let event = NewPlayabilityEvent(createPlayabilityEvent mediaSetId mediaSetState)
                                before_publishing state mediaSetId mediaSetState psFilesState event traceContext traceSpan
                    | :? MediaSetPlayabilityEventDto
                    | :? CancelEvent ->
                        mailbox.Stash()
                        ignored ()
                    | :? ReceiveTimeout ->
                        let message = "Timeout awaiting mediaset state"
                        logError mailbox message
                        mailbox.SetReceiveTimeout None
                        raise <| TimeoutException message
                    | :? PsDto.PlayabilityBump ->
                        logDebug mailbox "Received PlayabilityBump reminder"
                        ignored ()
                    | :? Continue
                    | LifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }
        and before_publishing state mediaSetId mediaSetState psFilesState event traceContext traceSpan =
            logDebug mailbox "before_publishing"
            (retype mailbox.Self) <! PublishPlayabilityEvent event
            publishing_event state mediaSetId mediaSetState psFilesState traceContext traceSpan
        and publishing_event state mediaSetId mediaSetState psFilesState traceContext traceSpan =
            logDebug mailbox "publishing_event"
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? PublishPlayabilityEvent as message ->
                        let (PublishPlayabilityEvent msg) = message
                        match msg with
                        | OldPlayabilityEvent(event, eventId) ->
                            // Resending an old event, it's already persisted
                            create_and_publish_event state mediaSetId mediaSetState psFilesState event eventId traceContext traceSpan
                        | NewPlayabilityEvent event ->
                            // Persist a new event before sending
                            let pendingEvent = createPendingEvent event (Guid.NewGuid().ToString())
                            PsPersistence.PendingEvent.FromPlayabilityEvent pendingEvent |> box |> persistEvent :> Effect<_>
                    | :? PsPersistence.PendingEvent as event ->
                        let state = applyPersistentEvent state event
                        create_and_publish_event
                            state
                            mediaSetId
                            mediaSetState
                            psFilesState
                            (event.ToPlayabilityEvent().Event)
                            event.EventId
                            traceContext
                            traceSpan
                    | :? MediaSetPlayabilityEventDto
                    | :? CancelEvent ->
                        mailbox.Stash()
                        ignored ()
                    | :? PsDto.PlayabilityBump ->
                        logDebug mailbox "Received PlayabilityBump reminder"
                        ignored ()
                    | :? Reminders.ReminderRemoved
                    | :? Reminders.ReminderNotRemoved -> ignored ()
                    | LifecycleEvent _ -> ignored ()
                    | PersistentLifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }
        and create_and_publish_event state mediaSetId mediaSetState psFilesState event eventId traceContext traceSpan =
            let serviceBusPlaybackMessage = createPlaybackEvent mediaSetState psFilesState event
            let serviceBusDistributionStatusMessage = tryCreateDistributionStatusEvent mediaSetState event
            let totalEventConsumerCount = aprops.PlaybackEventConsumerCount + aprops.DistributionStatusEventConsumerCount
            let playbackPublishResult = publishPlaybackEvent serviceBusPlaybackMessage mediaSetId
            let distributionStatusPublishResult = serviceBusDistributionStatusMessage |> Option.map publishDistributionStatusEvent
            // Playback and DistributionStatus events are handled differently:
            // Playback event is created as ResultWithRetry because some origin may not be ready but we still need to send event with what we have and try again later
            // DistributionStatus even is created as Option because it is generated only for OnDemand state changes, not for LiveToVod
            // Combined together these cases produce in total 6 cases: 3 (ResultWithRetry) x 2 (Option)
            match playbackPublishResult, distributionStatusPublishResult with
            | _, None when aprops.PlaybackEventConsumerCount = 0 -> idle state mediaSetId DoNothing None (endTraceSpan traceSpan)
            | ResultWithRetry.Ok(), None -> awaiting_publisher state mediaSetId eventId false aprops.PlaybackEventConsumerCount 0 0 traceContext traceSpan
            | ResultWithRetry.Retry(), None -> awaiting_publisher state mediaSetId eventId true aprops.PlaybackEventConsumerCount 0 0 traceContext traceSpan
            | ResultWithRetry.Error(), None -> idle state mediaSetId DoNothing None (endTraceSpan traceSpan)
            | ResultWithRetry.Ok(), Some() -> awaiting_publisher state mediaSetId eventId false totalEventConsumerCount 0 0 traceContext traceSpan
            | ResultWithRetry.Retry(), Some() -> awaiting_publisher state mediaSetId eventId true totalEventConsumerCount 0 0 traceContext traceSpan
            | ResultWithRetry.Error(), Some() ->
                awaiting_publisher state mediaSetId eventId true aprops.DistributionStatusEventConsumerCount 0 0 traceContext traceSpan
        and awaiting_publisher state mediaSetId eventId shouldRetry eventConsumerCount completionCount errorCount traceContext traceSpan =
            logDebug mailbox $"awaiting_publisher ({eventConsumerCount - completionCount - errorCount} pending events)"
            mailbox.SetReceiveTimeout aprops.ExternalRequestTimeout
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? Result<unit, exn> as result ->
                        mailbox.SetReceiveTimeout None
                        match result with
                        | Result.Ok() ->
                            let completionCount = completionCount + 1
                            logInfo mailbox $"Event {completionCount} is sent to service bus"
                            if completionCount = eventConsumerCount && not shouldRetry then
                                PsPersistence.CompletedEvent.FromDomain(eventId, DateTimeOffset.Now) |> box |> persistEvent :> Effect<_>
                            else if completionCount + errorCount = eventConsumerCount then
                                idle state mediaSetId DoNothing None (endTraceSpan traceSpan)
                            else
                                awaiting_publisher state mediaSetId eventId shouldRetry eventConsumerCount completionCount errorCount traceContext traceSpan
                        | Result.Error exn ->
                            logErrorWithExn mailbox exn "Failed to publish event to service bus"
                            let errorCount = errorCount + 1
                            if completionCount + errorCount = eventConsumerCount then
                                idle state mediaSetId DoNothing None (endTraceSpan traceSpan)
                            else
                                awaiting_publisher state mediaSetId eventId shouldRetry eventConsumerCount completionCount errorCount traceContext traceSpan
                    | :? PsPersistence.CompletedEvent as event ->
                        let state = applyPersistentEvent state event
                        idle state mediaSetId DoNothing None (endTraceSpan traceSpan)
                    | :? MediaSetPlayabilityEventDto
                    | :? CancelEvent ->
                        mailbox.Stash()
                        ignored ()
                    | :? ReceiveTimeout ->
                        let message = "Timeout awaiting result from service bus"
                        logError mailbox message
                        mailbox.SetReceiveTimeout None
                        idle state mediaSetId DoNothing None (endTraceSpan traceSpan)
                    | :? PsDto.PlayabilityBump ->
                        logDebug mailbox "Received PlayabilityBump reminder"
                        ignored ()
                    | LifecycleEvent _ -> ignored ()
                    | PersistentLifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }

        init PsPlayabilityHandlerState.Zero
