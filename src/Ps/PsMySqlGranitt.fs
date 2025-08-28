module Nrk.Oddjob.Ps.MySqlGranitt

open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Granitt
open GranittTypes

let getProgramRights (ctx: MySqlContext) piProgId =
    let sql =
        """
        SELECT
          r.PUBLISH_START AS PublishStart,
          r.PUBLISH_END AS PublishEnd,
          r.HIGH_SECURITY AS HighSecurity,
          g.DESCRIPTION AS Description
        FROM PROGRAMMES p
        JOIN RIGHTS r ON p.PROGRAMME_ID = r.PROGRAMME_ID
        JOIN GEOLOCATIONS g ON r.GEOLOCATION_ID = g.GEOLOCATION_ID
        WHERE p.PI_PROG_ID = @PiProgId
        ORDER BY r.PUBLISH_START
        """
    let param = Map [ "PiProgId", PiProgId.value piProgId ]
    GranittCommon.getProgramRights (DbContext.fromMySql ctx) sql param

let tryGetProgrammeIdFromPiProgId (ctx: MySqlContext) piProgId =
    let sql = "SELECT PROGRAMME_ID AS ProgrammeId FROM PROGRAMMES WHERE PI_PROG_ID = @PiProgId"
    let param = Map [ "PiProgId", PiProgId.value piProgId ]
    GranittCommon.tryGetProgrammeId (DbContext.fromMySql ctx) sql param |> Option.map (decimal >> GranittProgrammeId)

let tryGetProgrammeIdFromCarrierId (ctx: MySqlContext) carrierId =
    let sql = "SELECT PROGRAMME_ID AS ProgrammeId FROM FILESETS_PARTS WHERE CARRIER_ID = @CarrierId"
    let param = Map [ "CarrierId", CarrierId.value carrierId ]
    GranittCommon.tryGetProgrammeId (DbContext.fromMySql ctx) sql param |> Option.map (decimal >> GranittProgrammeId)

let setTranscodingStatus (ctx: MySqlContext) carrierId programmeId =
    let sql =
        """
        UPDATE FILESETS_PARTS fp
        SET fp.TRANSCODING_STATUS = 100, fp.CHANGED = SYSDATE()
        WHERE CARRIER_ID = @CarrierId
        """
    let param = Map [ "CarrierId", box (GranittCarrierId.value carrierId) ]
    GranittCommon.executeStatement (DbContext.fromMySql ctx) sql param programmeId ChangeLog.MySql.insert None

let setTranscodingStatusAndDuration (ctx: MySqlContext) (carrierId, duration) programmeId =
    let sql =
        """
        UPDATE FILESETS_PARTS fp
        SET fp.TRANSCODING_STATUS = 100, fp.DURATION = @Duration, fp.CHANGED = SYSDATE()
        WHERE CARRIER_ID = @CarrierId
        """
    let param =
        Map
            [
                "CarrierId", box (GranittCarrierId.value carrierId)
                "Duration", box duration
            ]
    GranittCommon.executeStatement (DbContext.fromMySql ctx) sql param programmeId ChangeLog.MySql.insert None
