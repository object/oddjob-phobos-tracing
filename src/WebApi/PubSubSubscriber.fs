namespace Nrk.Oddjob.WebApi

open Akkling
open Akka.Cluster.Tools.PublishSubscribe
open Elmish.Bridge

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.PubSub
open SocketMessages

module PubSubSubscriber =

    let subscriber (serverHub: ServerHub<_, _, _>) (mailbox: Actor<_>) =
        let publish msg =
            try
                serverHub.BroadcastClient msg
            with exn ->
                logErrorWithExn mailbox exn $"Failed to publish message %A{msg}"

        let mdr = typed (DistributedPubSub.Get(mailbox.System).Mediator)
        mdr <! Subscribe(Topic.MediaSetStatus, mailbox.UntypedContext.Self)
        mdr <! Subscribe(Topic.MediaSetResourceState, mailbox.UntypedContext.Self)
        mdr <! Subscribe(Topic.GlobalConnectQueue, mailbox.UntypedContext.Self)
        mdr <! Subscribe(Topic.GlobalConnectCommandStatus, mailbox.UntypedContext.Self)

        let rec loop () =
            actor {
                let! (msg: obj) = mailbox.Receive()
                return!
                    match msg with
                    | :? MediaSetStatusUpdate as msg ->
                        msg |> MediaSetStatusEvent |> publish
                        loop ()
                    | :? MediaSetRemoteResourceUpdate as msg ->
                        msg |> MediaSetResourceEvent |> publish
                        loop ()
                    | :? PriorityQueueEvent as msg ->
                        msg |> PriorityQueueEvent |> publish
                        loop ()
                    | :? IngesterCommandStatus as msg ->
                        msg |> IngesterCommandEvent |> publish
                        loop ()
                    | _ -> unhandled ()
            }
        loop ()

    let spawnPubSubSubscriber system serverHub =
        spawn system (makeActorName [ "PubSub"; "Subscriber" ]) <| propsNamed "webapi-pubsub-subscriber" (subscriber serverHub)
