namespace Nrk.Oddjob.Upload

open Akka.Actor
open Akka.Cluster.Tools.PublishSubscribe
open Akkling
open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.PubSub

module MediaSetPublisher =

    type IPublishMediaSetStatus =
        abstract member UpdateMediaSetStatus: statusUpdate: MediaSetStatusUpdate -> unit

    type IPublishMediaSetResourceState =
        abstract member ReceivedRemoteResourceState: MediaSetRemoteResourceUpdate -> unit

    type NullMediaSetStatusPublisher() =
        interface IPublishMediaSetStatus with
            member this.UpdateMediaSetStatus _ = ()

    type NullMediaSetResourceStatePublisher() =
        interface IPublishMediaSetResourceState with
            member this.ReceivedRemoteResourceState _ = ()

    type PubSubMediaSetStatus(system) =
        let topic = Topic.MediaSetStatus
        interface IPublishMediaSetStatus with
            member this.UpdateMediaSetStatus msg = PubSub.publish system topic msg

    type PubSubMediaSetResourceState(system) =
        let topic = Topic.MediaSetResourceState
        interface IPublishMediaSetResourceState with
            member this.ReceivedRemoteResourceState msg = PubSub.publish system topic msg

    type MediatorPublisher() as actor =
        inherit ActorBase()

        let mdr = typed (DistributedPubSub.Get(ActorBase.Context.System).Mediator)
        do mdr <! Subscribe(Topic.MediaSetStatus, actor.Self)
        do mdr <! Subscribe(Topic.MediaSetResourceState, actor.Self)
        override actor.Receive(msg: obj) =
            match msg with
            | SubscribeAck _ -> true
            | UnsubscribeAck _ -> true
            | :? MediaSetStatusUpdate
            | :? MediaSetRemoteResourceUpdate -> true
            | _ ->
                actor.Unhandled msg
                false
        static member Props() = Props.Create<MediatorPublisher>()
