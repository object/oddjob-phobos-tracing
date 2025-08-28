namespace Nrk.Oddjob.Upload

module UploadEventPublisher =

    open Akkling

    open Nrk.Oddjob.Core.Dto.Events

    type PublishEventResult = Result<unit, exn>

    type PublicEvent =
        | ResourceStateEvent of OddjobEventDto
        | CommandStatusEvent of OddjobEventDto

        member this.ClientId =
            match this with
            | ResourceStateEvent event -> event.ClientId
            | CommandStatusEvent event -> event.ClientId
        member this.Payload =
            match this with
            | ResourceStateEvent event -> box event
            | CommandStatusEvent event -> box event

    type PublisherMessage =
        /// Returns PublishEventResult - use with Ask
        | PublishEvent of PublicEvent
        /// Fire and forget - might not deliver event at all - use with Tell
        | PublishEventWithoutConfirmation of PublicEvent

    let publishEventsWithoutConfirmation (eventPublisher: ICanTell<PublisherMessage>) events =
        events |> List.iter (fun e -> eventPublisher <! PublishEventWithoutConfirmation e)

    let publishEvents (eventPublisher: ICanTell<PublisherMessage>) events =
        events
        |> List.map (fun e -> eventPublisher.Ask(PublishEvent e, None))
        |> Async.Parallel
        |> Async.RunSynchronously
        |> Array.toList
        |> List.fold
            (fun acc elt ->
                match acc, elt with
                | PublishEventResult.Ok _, PublishEventResult.Ok _ -> acc
                | PublishEventResult.Ok _, err -> err
                | err, _ -> err)
            (PublishEventResult.Ok())

    [<NoEquality; NoComparison>]
    type UploadEventsPublisherProps =
        {
            ClientPublisher: IActorRef<OddjobEventDto>
        }

    type private PublisherCommand =
        | PublishAndConfirm of PublicEvent * string
        | PublishOnly of PublicEvent * string

        member this.Unwrap() =
            match this with
            | PublishAndConfirm(x, y) -> x, y
            | PublishOnly(x, y) -> x, y

    let uploadEventsPublisherActor (aprops: UploadEventsPublisherProps) (mailbox: Actor<_>) =

        let publishToClientShard message =
            logDebug mailbox $"Publishing to client shard {aprops.ClientPublisher}"
            match message with
            | ResourceStateEvent msg -> aprops.ClientPublisher <! msg
            | CommandStatusEvent msg -> aprops.ClientPublisher <! msg

        let publishMessage (command: PublisherCommand) =
            let message, summary = command.Unwrap()
            publishToClientShard message
            match command with
            | PublishAndConfirm _ ->
                logDebug mailbox $"Publishing with confirmation [{summary}]"
                mailbox.Sender() <! PublishEventResult.Ok()
            | PublishOnly _ -> logDebug mailbox $"Publishing without confirmation [{summary}]"

        let handlePublishMessage message =
            match message with
            | PublishEvent(ResourceStateEvent event) -> PublishAndConfirm(ResourceStateEvent event, event.GetSummary())
            | PublishEventWithoutConfirmation(ResourceStateEvent event) -> PublishOnly(ResourceStateEvent event, event.GetSummary())
            | PublishEvent(CommandStatusEvent event) -> PublishAndConfirm(CommandStatusEvent event, event.GetSummary())
            | PublishEventWithoutConfirmation(CommandStatusEvent event) -> PublishOnly(CommandStatusEvent event, event.GetSummary())
            |> publishMessage

        let rec loop () =
            actor {
                let! (message: obj) = mailbox.Receive()
                return!
                    match message with
                    | :? PublisherMessage as message ->
                        handlePublishMessage message
                        ignored ()
                    | LifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }

        loop ()
