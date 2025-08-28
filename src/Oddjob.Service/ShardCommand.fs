module Nrk.Oddjob.Service.ShardCommand

open System
open Akka.Actor
open Akka.Cluster.Sharding
open Akkling
open Petabridge.Cmd
open Petabridge.Cmd.Host
open Nrk.Oddjob.Core

let ShowShardsCommand =
    CommandDefinitionBuilder()
        .WithName("show")
        .WithDescription("Shows shards in a cluster")
        .WithArgument(fun builder ->
            builder
                .WithName("region")
                .WithDescription("Name of the shard region from which shards should be shown")
                .IsMandatory(true)
                .WithSwitch("-r")
                .WithSwitch("-R")
            |> ignore)
        .Build()

let CommandPalette = CommandPalette("shard", [ ShowShardsCommand ])

type ShardCommandHandlerActor() as this =
    inherit CommandHandlerActor(CommandPalette)

    let system = ReceiveActor.Context.System

    do this.Process(ShowShardsCommand.Name, (fun cmd -> this.ProcessShow cmd))

    member this.ProcessShow(command) =
        let sender = typed this.Sender
        try
            let regionName =
                command.Arguments
                |> Seq.tryFind (fun a -> ShowShardsCommand.ArgumentsByName["region"].Switch.Contains(fst a))
                |> Option.map snd
                |> Option.getOrFail "Region information not provided"

            let region = ClusterSharding.Get(system).ShardRegion(regionName)

            let (stats: ClusterShardingStats) = (typed <| region) <<? GetClusterShardingStats(TimeSpan.FromSeconds 10.)
            let response =
                [
                    if stats.Regions |> Seq.isEmpty then
                        yield "No shard regions found"
                    else
                        for region in stats.Regions do
                            yield sprintf "Address %A:" region.Key
                            if region.Value.Stats |> Seq.isEmpty then
                                yield "\tNo shards found"
                            else
                                for kv in region.Value.Stats do
                                    yield sprintf "\tShard '%s' has %d entities on it" kv.Key kv.Value
                ]
                |> String.concat Environment.NewLine
            sender <! CommandResponse(response)
        with exn ->
            sender <! CommandResponse(sprintf "Error when executing 'shard show' [%A]" exn)


type ShardCommandPaletteHandler() =
    inherit CommandPaletteHandler(CommandPalette)

    override this.HandlerProps = Props.Create<ShardCommandHandlerActor>()
