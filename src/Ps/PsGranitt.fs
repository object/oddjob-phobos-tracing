namespace Nrk.Oddjob.Ps

open System
open System.Transactions
open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Granitt
open PsTypes

module GranittTypes =

    type GranittCarrierId = private GranittCarrierId of string

    [<RequireQualifiedAccess>]
    module GranittCarrierId =
        let ofCarrierId carrierId =
            (CarrierId.value carrierId).ToUpper() |> GranittCarrierId

        let ofString (str: string) = str.ToUpper() |> GranittCarrierId
        let value (GranittCarrierId carrierId) = carrierId

module GranittUtils =
    let computeDuration fpDuration fpInPoint fpOutPoint =
        // Duration: frames
        // InPoint: seconds since midnight
        // OutPoint: seconds since midnight
        match (defaultArg fpDuration 0m) with
        | 0m ->
            let outPoint = (defaultArg fpOutPoint 0m)
            let inPoint = (defaultArg fpInPoint 0m)
            if outPoint >= inPoint then
                (outPoint - inPoint) * 1000m // milliseconds
            else
                let numberOfSecondsIn24Hours = 86400m
                ((outPoint + numberOfSecondsIn24Hours) - inPoint) * 1000m // milliseconds
        | x -> x * 40m // convert frames to milliseconds

module DapperTypes =
    [<CLIMutable>]
    type GetProgramRightsResult =
        {
            PublishStart: DateTime
            PublishEnd: DateTime
            HighSecurity: decimal
            Description: string
        }

    [<CLIMutable; NoComparison>]
    type GetProgramFilesetParts =
        {
            FilesetPartId: int
            CarrierId: string
            PartNumber: Nullable<int>
            Deactivated: int
            Priority: int
            InPoint: Nullable<int>
            OutPoint: Nullable<int>
            Duration: Nullable<int>
            AudioDefinition: string
        }

    [<CLIMutable; NoComparison>]
    type GetProgramSubtitles =
        {
            LanguageCode: string
            Description: string
            FilePath: string
        }

module GranittCommon =

    let tryGetSingle (ctx: DbContext) sql param =
        ctx.Connection.QueryMap(sql, param) |> Seq.tryHead

    let executeStatement (ctx: DbContext) sql param programmeId changeLogInsert doFail =
        use ts = new TransactionScope()
        let affectedRows = ctx.Connection.ExecuteMap(sql, param)
        if affectedRows = 0 then
            Option.iter failwith doFail
        else
            changeLogInsert ctx.Connection programmeId
        ts.Complete()

    let getProgramRights (ctx: DbContext) sql param =
        let result: DapperTypes.GetProgramRightsResult list = ctx.Connection.QueryMap(sql, param) |> List.ofSeq
        result
        |> List.choose (fun res ->
            Option.map2
                (fun rightsPeriod georestriction ->
                    {
                        Geolocation = georestriction
                        RightsPeriod = rightsPeriod
                    })
                (RightsPeriod.tryCreate res.PublishStart res.PublishEnd)
                (GranittGeorestriction.tryCreate res.Description))

    let getProgramFilesetParts (ctx: DbContext) sql param =
        ctx.Connection.QueryMap(sql, param)
        |> List.ofSeq
        |> List.map (fun (res: DapperTypes.GetProgramFilesetParts) ->
            {
                CarrierId = res.CarrierId
                PartNumber = res.PartNumber.GetValueOrDefault()
                DurationMs =
                    GranittUtils.computeDuration
                        (Option.ofNullable res.Duration |> Option.map decimal)
                        (Option.ofNullable res.InPoint |> Option.map decimal)
                        (Option.ofNullable res.OutPoint |> Option.map decimal)
                AudioDefinition =
                    if res.AudioDefinition = "5.1-lyd" then
                        Mixdown.Surround
                    else
                        Mixdown.Stereo
            })

    let getProgramSubtitles (ctx: DbContext) sql param =
        ctx.Connection.QueryMap(sql, param)
        |> List.ofSeq
        |> List.map (fun (res: DapperTypes.GetProgramSubtitles) ->
            {
                LanguageCode = res.LanguageCode
                LanguageType = res.Description
                SideloadingUri =
                    match res.FilePath with
                    | null -> ""
                    | x -> x
            })

    let tryGetProgrammeId (ctx: DbContext) sql param =
        let result: Nullable<decimal> = ctx.Connection.QuerySingleOrDefaultMap(sql, param)
        result |> Option.ofNullable
