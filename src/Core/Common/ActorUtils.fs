namespace Nrk.Oddjob.Core

[<AutoOpen>]
module ActorUtils =

    open System
    open Akka.Actor
    open Akka.Event
    open Akka.Persistence
    open Akkling
    open Akkling.Persistence

    let normalizeActorNameSegment (name: string) =
        name
            .ToLower()
            .Replace(' ', '_')
            .Replace('\\', '~')
            .Replace('/', '~')
            .Replace('|', '*')
            .Replace('#', '*')
            .Replace('[', '*')
            .Replace(']', '*')
            .Replace("æ", "ae")
            .Replace("ø", "oe")
            .Replace("å", "aa")

    let makeActorName (nameSegments: string list) =
        String.Join(".", nameSegments |> List.map normalizeActorNameSegment)

    let getOneForOneRestartSupervisorStrategy (log: ILoggingAdapter) =
        Strategy.OneForOne(fun exn ->
            log.Warning("Invoking one-for-one restart supervisor strategy")
            match exn with
            | :? ActorInitializationException as exn ->
                log.Error("Stopping actor after initialization failure ({0})", exn)
                Directive.Stop
            | _ ->
                log.Error("Restarting actor after failure ({0})", exn)
                Directive.Restart)

    let inline propsNamed actorTypeName receive =
        props receive |> withActorTypeName actorTypeName

    let inline propsPersistNamed actorTypeName receive =
        propsPersist receive |> withActorTypeName actorTypeName

    let getOrSpawnChildActor (parentContext: IActorContext) actorName actorProps =
        let actor = parentContext.Child(actorName)
        if actor.IsNobody() then
            spawn parentContext actorName <| actorProps |> retype
        else
            typed actor

    let (|SubscribeAck|_|) (msg: obj) : Akka.Cluster.Tools.PublishSubscribe.SubscribeAck option =
        match msg with
        | :? Akka.Cluster.Tools.PublishSubscribe.SubscribeAck as e -> Some e
        | _ -> None

    let (|UnsubscribeAck|_|) (msg: obj) : Akka.Cluster.Tools.PublishSubscribe.UnsubscribeAck option =
        match msg with
        | :? Akka.Cluster.Tools.PublishSubscribe.UnsubscribeAck as e -> Some e
        | _ -> None

    [<RequireQualifiedAccess>]
    type ActorLifecycleCommand = | KeepAlive

    type TakeSnapshotCommand = TakeSnapshotCommand of lastSequenceNr: int64

    type DeleteSnapshotsCommand = DeleteSnapshotsCommand of maxSequenceNr: int64

    [<Literal>]
    let SnapshotFrequency = 100

    let (|SnapshotCommand|_|) (message: obj) =
        match message with
        | :? TakeSnapshotCommand
        | :? DeleteSnapshotsCommand -> Some message
        | _ -> None

    let (|SnapshotEvent|_|) (message: obj) =
        match message with
        | :? SaveSnapshotSuccess
        | :? SaveSnapshotFailure
        | :? DeleteSnapshotsSuccess
        | :? DeleteSnapshotsFailure -> Some message
        | _ -> None
