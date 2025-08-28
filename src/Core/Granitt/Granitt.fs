module Nrk.Oddjob.Core.Granitt

open System
open System.Data.Common
open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Config

[<AutoOpen>]
module Types =

    type GranittResult<'a> = Result<'a, exn>

    type GranittFileId = GranittFileId of decimal

    [<RequireQualifiedAccess>]
    module GranittFileId =
        let value (GranittFileId id) = id

    type GranittProgrammeId = GranittProgrammeId of decimal

    [<RequireQualifiedAccess>]
    module GranittProgrammeId =
        let value (GranittProgrammeId id) = id

    /// Georestriction that can be expressed in Granitt
    type GranittGeorestriction =
        | Norway
        | World

    module GranittGeorestriction =
        let geoWorld = "world"
        let geoNorway = "norway"

        let tryCreate (str: string) =
            match str with
            | "world" -> Some GranittGeorestriction.World
            | "no"
            | "norway"
            | "nrk" -> Some GranittGeorestriction.Norway
            | _ -> None

        let create (str: string) =
            match str with
            | "world" -> GranittGeorestriction.World
            | "no"
            | "norway"
            | "nrk" -> GranittGeorestriction.Norway
            | _ -> failwithf "Could not create Georestriction from %s" str

        /// Converts to a string version used in Geolocations table
        let toGeolocationsString (g: GranittGeorestriction) =
            match g with
            | Norway -> geoNorway
            | World -> geoWorld

        let toDomain (g: GranittGeorestriction) =
            match g with
            | Norway -> GeoRestriction.Norway
            | World -> GeoRestriction.World

    type MySqlContext =
        {
            Connection: MySqlConnector.MySqlConnection
            Log: Akka.Event.ILoggingAdapter
        }

    type DbContext =
        {
            Connection: DbConnection
            Log: Akka.Event.ILoggingAdapter
        }

    module DbContext =
        let fromMySql (ctx: MySqlContext) =
            {
                Connection = ctx.Connection
                Log = ctx.Log
            }

        let toMySql (ctx: DbContext) : MySqlContext =
            {
                Connection = ctx.Connection :?> MySqlConnector.MySqlConnection
                Log = ctx.Log
            }

    let private getMySqlContext granittConnectionString log : MySqlContext =
        let conn = new MySqlConnector.MySqlConnection(granittConnectionString)
        conn.Open()
        {
            MySqlContext.Connection = conn
            Log = log
        }

    type GranittContext = Disposable<MySqlContext>

    let getGranittContext config log =
        let ctx = getMySqlContext config log
        new Disposable<_>(ctx, (fun () -> ctx.Connection.Dispose()))

    let getGranittResult fn =
        try
            fn () |> GranittResult.Ok
        with exn ->
            GranittResult.Error exn

module ChangeLog =
    let private execute (connection: DbConnection) sql param =
        let affectedRows = connection.ExecuteMap(sql, param)
        if affectedRows = 0 then
            failwithf "Could not update change log"

    module MySql =
        let insert connection programmeId =
            let sql =
                """INSERT INTO CHANGE_LOG(OBJECT_TYPE, OBJECT_ID, SOURCE, LOG_TIME) 
                         VALUES ('program', @ObjectId, 'dm', CURRENT_TIMESTAMP())"""
            let param = Map [ "ObjectId", GranittProgrammeId.value programmeId ]
            execute connection sql param
