namespace Nrk.Oddjob.Core.HelperActors

module PriorityQueueActor =

    open Akkling
    open FSharpx.Collections

    type PriorityQueueCommand<'a when 'a: comparison> =
        | Enqueue of 'a
        | TryDequeue

    type PriorityQueueEvent<'a when 'a: comparison> =
        | ItemAdded of 'a
        | ItemRemoved of 'a

    type QueueWorkerEvent = | QueueNonEmpty

    type PriorityQueueActorProps<'a when 'a: comparison> =
        {
            QueueIsDescending: bool
            OnDequeue: 'a -> IPriorityQueue<'a> -> Actor<obj> -> unit
            OnEnqueue: 'a -> IPriorityQueue<'a> -> Actor<obj> -> unit
            StartWorkerPool: unit -> ICanTell<QueueWorkerEvent>
        }

    let priorityQueueActor<'a when 'a: comparison> (aprops: PriorityQueueActorProps<'a>) (mailbox: Actor<_>) =

        logDebug mailbox "Starting worker pool"
        let workerPool = aprops.StartWorkerPool()

        let rec idle (queue: IPriorityQueue<'a>) =
            logDebug mailbox $"idle (count: {queue.Count})"
            actor {
                let! (message: obj) = mailbox.Receive()
                let sender = mailbox.Sender()
                return!
                    match message with
                    | :? PriorityQueueCommand<'a> as message ->
                        match message with
                        | Enqueue item ->
                            logDebug mailbox $"Enqueue from {sender.Path}"
                            let newQueue = queue |> PriorityQueue.insert item
                            aprops.OnEnqueue item newQueue mailbox
                            if queue.IsEmpty then
                                workerPool <! QueueNonEmpty
                            idle newQueue
                        | TryDequeue ->
                            logDebug mailbox $"TryDequeue from {sender.Path}"
                            let newQueue =
                                match queue |> PriorityQueue.tryPop with
                                | None -> queue
                                | Some(item, newQueue) ->
                                    aprops.OnDequeue item newQueue mailbox
                                    sender <! item
                                    newQueue
                            idle newQueue
                    | LifecycleEvent _ -> ignored ()
                    | _ -> unhandled ()
            }

        idle (PriorityQueue.empty aprops.QueueIsDescending)
