module Nrk.Oddjob.WebApi.ClusterState

open System
open Akkling
open Akka.Cluster
open Akka.Event

open Nrk.Oddjob.Core
open HealthCheckTypes

type private Reachability =
    | Reachable
    | Unreachable

type private QueryClusterState = QueryClusterState

type private ClusterState = Map<Member, Reachability>

let clusterStateListener (mailbox: Actor<_>) =
    let cluster = Cluster.Get(mailbox.System)

    let rec loop (state: ClusterState) =
        actor {
            let! (msg: obj) = mailbox.Receive()
            return!
                match msg with
                | LifecycleEvent ev ->
                    match ev with
                    | PreStart ->
                        cluster.Subscribe(
                            mailbox.UntypedContext.Self,
                            ClusterEvent.InitialStateAsSnapshot,
                            [|
                                typedefof<ClusterEvent.IMemberEvent>
                                typedefof<ClusterEvent.UnreachableMember>
                                typedefof<ClusterEvent.ReachableMember>
                            |]
                        )
                    | PostStop -> cluster.Unsubscribe(mailbox.UntypedContext.Self)
                    | _ -> ()
                    ignored ()
                | :? ClusterEvent.ReachableMember as ev -> loop <| Map.add ev.Member Reachable state
                | :? ClusterEvent.UnreachableMember as ev -> loop <| Map.add ev.Member Unreachable state
                | :? ClusterEvent.CurrentClusterState as s ->
                    let state =
                        s.Members
                        |> Seq.map (fun mem ->
                            if s.Unreachable.Contains mem then
                                mem, Unreachable
                            else
                                mem, Reachable)
                        |> Map.ofSeq
                    loop state
                | :? ClusterEvent.IMemberEvent as ev -> loop <| Map.add ev.Member Reachable state
                | :? QueryClusterState ->
                    mailbox.Sender() <! state
                    ignored ()
                | _ -> unhandled ()
        }
    loop Map.empty

let createClusterStateComponent componentName checkInterval system =
    let stateListener = spawnAnonymous system (propsNamed "cluster-state-listener" clusterStateListener) |> retype
    let queryState (logger: ILoggingAdapter) =
        let state: ClusterState = stateListener <? QueryClusterState |> Async.RunSynchronously

        let numberOfUnreachableMembers =
            state |> Map.filter (fun _ status -> status = Reachability.Unreachable) |> Map.count |> int64

        let metrics: HealthMetrics list =
            [
                {
                    Name = "Unreachable"
                    Value = numberOfUnreachableMembers
                }
                {
                    Name = "Reachable"
                    Value = state |> Map.filter (fun _ status -> status = Reachability.Reachable) |> Map.count |> int64
                }
                {
                    Name = "Total"
                    Value = state |> Map.count |> int64
                }
            ]

        if numberOfUnreachableMembers = 0 then
            logger.Info "Cluster state is healthy"
            HealthCheckResult.Ok(componentName, metrics)
        else
            let message =
                state
                |> Map.toList
                |> List.filter (fun (_, status) -> status = Reachability.Unreachable)
                |> List.map (fun (mem, status) -> (sprintf "%s:%s - %s" mem.Address.Host (mem.Roles |> String.concat ",") (string status)))
                |> String.concat Environment.NewLine

            logger.Error $"Cluster state is unhealthy. Unreachable nodes: {message}"

            HealthCheckResult.Error(componentName, message, metrics)

    ComponentSpec.WithChecker(componentName, queryState, checkInterval, None)
