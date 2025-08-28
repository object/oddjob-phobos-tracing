namespace Nrk.Oddjob.Service

type TraceFilter() =
    interface Phobos.Actor.Configuration.ITraceFilter with
        override _.ShouldTraceMessage(message, alreadyInTrace) =

            let isOneOfTypes (message: obj) (typeNames: string seq) =
                typeNames |> Seq.exists (fun typeName -> message.GetType().FullName.StartsWith typeName)

            match message with
            | :? Nrk.Oddjob.Core.CommonTypes.AsyncResponse
            | :? Nrk.Oddjob.Core.MessageTypes.AcknowledgementCommand
            | :? Nrk.Oddjob.Core.Queues.QueueActors.FactoryCommand
            | :? Nrk.Oddjob.Core.Queues.QueueApi.ActorCommands.QueueConsumerCommand
            | :? Nrk.Oddjob.Core.HelperActors.PriorityQueueActor.QueueWorkerEvent
            | :? Nrk.Oddjob.Core.HelperActors.PriorityQueueActor.PriorityQueueCommand<Nrk.Oddjob.Core.S3.S3Types.S3MessageContext>
            | :? Nrk.Oddjob.Core.PubSub.PubSubMessages.IngesterCommandStatus
            | :? Nrk.Oddjob.Core.PubSub.PubSubMessages.PriorityQueueEvent
            | :? Nrk.Oddjob.Core.MediaSetStateCache.MediaSetStateCacheCommand
            | :? Nrk.Oddjob.Upload.MediaSetStatusPersistence.MediaSetStatusCommand
            | :? Nrk.Oddjob.Core.PubSub.PubSubMessages.MediaSetStatusUpdate
            | :? Nrk.Oddjob.Core.PubSub.PubSubMessages.MediaSetRemoteResourceUpdate -> false
            | :? Nrk.Oddjob.Core.Dto.Events.OddjobEventDto as msg when msg.IsCommandStatus ->
                match msg with
                | Nrk.Oddjob.Core.Dto.Events.OddjobEventDto.CommandStatus statusEvent -> statusEvent.Status.IsFinal()
                | _ -> false
            | :? Nrk.Oddjob.Core.Dto.Events.CommandStatusEvent as msg -> msg.Status.IsFinal()
            | _ ->
                // Private types can only be accessible by type name
                if
                    isOneOfTypes
                        message
                        [
                            "Akka.Cluster.Sharding.ShardCoordinator"
                            "Nrk.Oddjob.Core.Queues.QueueActors+InternalAcknowledgmentCommand"
                            "Nrk.Oddjob.Ingesters.GlobalConnect.S3Actors+CompletionStatus"
                        ]
                then
                    false
                else
                    alreadyInTrace
