namespace Nrk.Oddjob.WebApi

module PsGranitt =

    open System

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Granitt
    open Nrk.Oddjob.Ps
    open Nrk.Oddjob.Ps.PsTypes

    [<CLIMutable; NoComparison>]
    type GetRights =
        {
            RightsId: decimal
            ProgrammeId: decimal
            PublishStart: DateTime
            PublishEnd: DateTime
            GeolocationId: decimal
            HighSecurity: decimal
            NoHD: decimal
            Created: DateTime
            Changed: DateTime
        }

    [<CLIMutable; NoComparison>]
    type GetDebugProgramRights =
        {
            PiProgId: string
            Geolocation: string
            PublishStart: DateTime
            PublishEnd: DateTime
            HighSecurity: decimal
        }

    module MySql =
        let getDebugProgramRights (ctx: MySqlContext) (piProgId: PiProgId) =
            let sql =
                """
                 SELECT
                     p.PI_PROG_ID AS PiProgId,
                     g.DESCRIPTION AS Geolocation,
                     r.PUBLISH_START AS PublishStart,
                     r.PUBLISH_END AS PublishEnd,
                     r.HIGH_SECURITY AS HighSecurity
                 FROM PROGRAMMES p
                 JOIN RIGHTS r ON r.PROGRAMME_ID = p.PROGRAMME_ID
                 JOIN GEOLOCATIONS g ON g.GEOLOCATION_ID = r.GEOLOCATION_ID
                 WHERE p.PI_PROG_ID = @PiProgId
                 ORDER BY r.PUBLISH_START
                 """
            let param = Map [ "PiProgId", PiProgId.value piProgId ]
            ctx.Connection.QueryMap(sql, param)
            |> Seq.toList
            |> List.map (fun (x: GetDebugProgramRights) ->
                {
                    DebugPsProgramRights.PiProgId = PiProgId.create x.PiProgId
                    Geolocation = x.Geolocation
                    RawPublishStart = x.PublishStart
                    RawPublishEnd = x.PublishEnd
                    PublishStart = RightsPeriod.tryCreate x.PublishStart x.PublishEnd |> Option.map RightsPeriod.getPublishStart
                    PublishEnd = RightsPeriod.tryCreate x.PublishStart x.PublishEnd |> Option.map RightsPeriod.getPublishEnd
                    HighSecurity = x.HighSecurity > 0m
                })

        let tryGetGeolocationId (ctx: MySqlContext) (geolocationDescription: string) =
            let sql = """SELECT GEOLOCATION_ID FROM GEOLOCATIONS WHERE DESCRIPTION = @Description"""
            let param = Map [ "Description", geolocationDescription ]
            ctx.Connection.QueryMap(sql, param) |> Seq.tryHead |> Option.map decimal

        let getProgramMedium (ctx: MySqlContext) (piProgId: PiProgId) =
            let sql =
                """
                SELECT MEDIUM
                FROM PROGRAMMES
                WHERE PI_PROG_ID = @PiProgId
                """
            let param = Map [ "PiProgId", PiProgId.value piProgId ]
            ctx.Connection.QueryMap(sql, param) |> Seq.tryHead
