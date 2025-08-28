[<AutoOpen>]
module Nrk.Oddjob.Service.CommonTypes

open Nrk.Oddjob.Core.Config

type RoleSettings =
    {
        ConnectionStrings: ConnectionStrings
        QuartzSettings: string -> string
        FeatureFlags: FeatureFlags
    }

[<RequireQualifiedAccess>]
type ClusterRole =
    | GlobalConnect

    static member All = [ GlobalConnect ]

type RuntimeSettings =
    {
        Environment: string
        ClusterRoles: ClusterRole list
        FeatureFlags: FeatureFlags
        OddjobConfig: OddjobConfig
        QueuesConfig: QueuesConfig
        AkkaRemotingPort: int
        AkkaManagementPort: int
        PetabridgeCmdPort: int
    }

type MetricsSettings =
    {
        MonitorSystemActors: bool
        MonitorUserActors: bool
        MonitorMailboxDepth: bool
        MonitorEventStream: bool
    }

module MetricsSettings =
    let enableAll =
        {
            MonitorSystemActors = true
            MonitorUserActors = true
            MonitorMailboxDepth = true
            MonitorEventStream = true
        }

type TraceSettings =
    {
        TraceSystemActors: bool
        TraceUserActors: bool
        ShouldIncludeMessagesAlreadyInTrace: bool
        VerboseTracingAllowed: bool
        AppendLogsToTrace: bool
        AllowLifecycleTracing: bool
        TraceAkkaPersistence: bool
        CreateTraceUponReceive: bool
        LogMessageEvents: bool
        TraceAsk: bool
        TraceFilter: TraceFilter
    }

module TraceSettings =
    let enableAll =
        {
            TraceSystemActors = true
            TraceUserActors = true
            ShouldIncludeMessagesAlreadyInTrace = true
            VerboseTracingAllowed = true
            AppendLogsToTrace = true
            AllowLifecycleTracing = true
            TraceAkkaPersistence = true
            CreateTraceUponReceive = true
            LogMessageEvents = true
            TraceAsk = true
            TraceFilter = null
        }
    let withAutomaticCreation =
        { enableAll with
            TraceSystemActors = false
            VerboseTracingAllowed = false
            AllowLifecycleTracing = false
            TraceAkkaPersistence = false
            TraceAsk = false
        }
    let withManualCreation =
        { enableAll with
            TraceSystemActors = false
            VerboseTracingAllowed = false
            AllowLifecycleTracing = false
            TraceAkkaPersistence = false
            CreateTraceUponReceive = false
            TraceAsk = false
        }
    let withManualCreationAndFilter traceFilter =
        { enableAll with
            TraceSystemActors = false
            VerboseTracingAllowed = false
            AllowLifecycleTracing = false
            TraceAkkaPersistence = false
            CreateTraceUponReceive = false
            TraceAsk = false
            TraceFilter = traceFilter
        }

[<RequireQualifiedAccess>]
type InstrumentationFeature =
    | AspNet
    | Aws
    | GrpcClient
    | HttpClient
    | Process
    | Quartz
    | RabbitMQ
    | Runtime
    | ServiceBus
    | SqlClient
