namespace Nrk.Oddjob.Ingesters.GlobalConnect

module Bootstrapper =

    open Akka.Actor
    open Akka.Routing
    open Akka.Cluster.Tools.Singleton
    open Akkling
    open FsHttp

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.S3.S3Types
    open Nrk.Oddjob.Core.HelperActors.PriorityQueueActor
    open S3Actors

    let getS3Props pathMappings priorityQueue =
        let httpGet url =
            http { GET url } |> Request.send |> Response.toResult |> Result.map _.ToText()
        {
            ServiceResolver = None
            FileSystem = LocalFileSystemInfo()
            HttpGet = httpGet
            PathMappings = pathMappings
            PriorityQueue = priorityQueue
        }

    let getGlobalConnectPriorityQueueProps instrumentation contentChangePublisher (queueName: string) startS3Pool =
        let enqueue, dequeue = getOnEnqueueDequeueForCommonQueue instrumentation contentChangePublisher queueName
        {
            QueueIsDescending = false
            OnDequeue = dequeue
            OnEnqueue = enqueue
            StartWorkerPool = startS3Pool
        }
        |> priorityQueueActor<S3MessageContext>

    let spawnGlobalConnectPriorityQueueProxy (system: ActorSystem) actorName =
        system.ActorOf(
            ClusterSingletonProxy.Props($"user/{actorName}", ClusterSingletonProxySettings.Create(system).WithRole("GlobalConnect").WithBufferSize(1000)),
            $"{actorName}-proxy"
        )
        |> typed

    let startS3Pool (system: ActorSystem) oddjobConfig priorityQueue : ICanTell<_> =
        let routerConfig =
            FromConfig.Instance.WithSupervisorStrategy(getOneForOneRestartSupervisorStrategy system.Log) :> RouterConfig
        let s3ActorProps = s3Actor (getS3Props oddjobConfig priorityQueue)
        let s3IngesterRouterProps =
            { propsNamed "s3-router" s3ActorProps with
                Router = Some routerConfig
            }
        retype (spawn system (makeActorName [ "S3 Ingesters" ]) s3IngesterRouterProps)
