namespace Nrk.Oddjob.Core.Queues

module QueueResources =

    open FSharp.Data

    type QueueStats = JsonProvider<"Queues/Resources/QueueStats.json", SampleIsList=false, EmbeddedResource="Core, QueueStats.json">
