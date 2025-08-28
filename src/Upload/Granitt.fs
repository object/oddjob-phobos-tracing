namespace Nrk.Oddjob.Upload

module Granitt =

    open System
    open Akkling

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Granitt

    module GranittCommon =

        let getMediaType (ctx: DbContext) sql param =
            ctx.Connection.QueryMap(sql, param)
            |> Seq.tryHead
            |> Option.map (fun x ->
                match x with
                | "tv" -> MediaType.Video
                | "radio" -> MediaType.Audio
                | medium -> failwithf $"Invalid MEDIUM {medium} in Granitt")
            |> Option.defaultWith (fun () -> failwithf "No MEDIUM found in Granitt")

    module MySqlGranitt =

        let getMediaType (ctx: MySqlContext) piProgId =
            let sql =
                """
                SELECT
                  MEDIUM
                FROM PROGRAMMES
                WHERE PI_PROG_ID = @PiProgId
                """
            let param = Map [ "PiProgId", PiProgId.value piProgId ]
            GranittCommon.getMediaType (DbContext.fromMySql ctx) sql param

    let getMediaType (ctx: MySqlContext) (mediaSetId: MediaSetId) =
        MySqlGranitt.getMediaType ctx <| PiProgId.create mediaSetId.ContentId.Value

    let resolveGranittMediaType (getGranitt: unit -> GranittContext) (mediaSetId: MediaSetId) =
        using (getGranitt ()) <| fun granitt -> getMediaType granitt.Value mediaSetId

    let createContentEnvironment granittCfg log globalConnectFileSystemResolver =
        {
            GetGranittMediaType = resolveGranittMediaType <| fun () -> getGranittContext granittCfg log
            GlobalConnectFileSystemResolver = globalConnectFileSystemResolver
        }

    type GranittCommand = GetGranittDetails of MediaSetId

    type GranittContent = { MediaType: MediaType }

    [<NoEquality; NoComparison>]
    type UploadGranittActorProps =
        {
            GetGranittContext: unit -> GranittContext
        }

    let uploadGranittActor (aprops: UploadGranittActorProps) (mailbox: Actor<_>) =

        let getGranittDetails mediaSetId =
            use ctx = aprops.GetGranittContext()
            {
                MediaType = getMediaType ctx.Value mediaSetId
            }

        let rec loop () =
            actor {
                let! message = mailbox.Receive()
                match message with
                | GetGranittDetails mediaSetId -> mailbox.Sender() <! getGranittResult (fun () -> getGranittDetails mediaSetId)
                return! ignored ()
            }

        loop ()
