namespace Nrk.Oddjob.WebApi

module PotionUtils =

    open System
    open System.Net
    open Akka.Event
    open FsHttp

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Config
    open Nrk.Oddjob.Potion.PotionTypes

    open PublicTypes
    open HealthCheckTypes
    open EventJournalUtils

    let private getQualifiedExternalGroupId (externalGroupId: string) =
        if externalGroupId.StartsWith PotionExternalGroupIdPrefix then
            externalGroupId
        else
            PotionExternalGroupIdPrefix + externalGroupId

    let private tryGetPotionPublication (mdbUrl: string) (potionUrl: string) (externalGroupId: string) =
        if String.IsNullOrWhiteSpace mdbUrl || String.IsNullOrWhiteSpace potionUrl then
            invalidOp "Tried to call tryGetPotionPublication without providing mdbUrl or potionUrl"
        let extractResId (url: string) = url.Split '/' |> Seq.last
        let mediaObjectId = extractResId externalGroupId
        let publicationMediaObjectPath = sprintf "api/publicationMediaObject/%s" mediaObjectId
        try
            let response = http { GET $"{mdbUrl}{publicationMediaObjectPath}" } |> Request.send
            if response.statusCode = HttpStatusCode.OK then
                response.ToJson().TryGetProperty("publicationEvent")
                |> Option.ofObj
                |> Option.map (fun (_, publicationEvent) ->
                    publicationEvent.GetProperty("links").EnumerateArray() |> Seq.head |> _.GetProperty("href").GetString())
                |> Option.bind (fun publicationEventUrl ->
                    let response = http { GET publicationEventUrl } |> Request.send
                    if response.statusCode = HttpStatusCode.OK then
                        response.ToJson().GetProperty("publishes").GetProperty("links").EnumerateArray()
                        |> Seq.head
                        |> _.GetProperty("href").GetString()
                        |> fun masterEditorialObjectUrl ->
                            let publicationEventId = extractResId publicationEventUrl
                            let masterEditorialObjectId = extractResId masterEditorialObjectUrl
                            Some
                                {
                                    PublicationMediaObjectUrl = Uri(Uri mdbUrl, publicationMediaObjectPath).AbsoluteUri
                                    PublicationEventUrl = publicationEventUrl
                                    MasterEditorialObjectUrl = Uri(Uri mdbUrl, $"api/masterEO/{masterEditorialObjectId}").AbsoluteUri
                                    PublicationUrl = Uri(Uri potionUrl, $"program/{masterEditorialObjectId}/pub/{publicationEventId}").AbsoluteUri
                                }
                    else
                        None)
            else
                None
        with _ ->
            None

    let private getPotionPublication (oddjobConfig: OddjobConfig) externalGroupId =
        if String.IsNullOrEmpty externalGroupId then
            PotionPublication.Empty
        else
            tryGetPotionPublication oddjobConfig.Potion.Mdb.Url oddjobConfig.Potion.Publication.Url externalGroupId
            |> Option.orElseWith (fun () ->
                tryGetPotionPublication oddjobConfig.Potion.Mdb.ForwardedFrom oddjobConfig.Potion.Publication.ForwardedFrom externalGroupId)
            |> Option.defaultValue PotionPublication.Empty

    let getMediaSetInfo oddjobConfig (reader: IStateReader) (contentId: string) =
        let externalGroupId, mediaSetId =
            if contentId.Contains("-") then
                let state = reader.PotionSetState contentId |> Async.RunSynchronously
                match state.Group with
                | Some group -> group.GroupId, group.MediaSetId.Value
                | None -> getQualifiedExternalGroupId contentId, ""
            else
                let mediaSetId = sprintf "potion~%s" contentId
                let state = reader.MediaSetState mediaSetId |> Async.RunSynchronously
                let externalGroupId =
                    match state.ClientContentId with
                    | Some contentId -> $"http://id.nrk.no/2016/mdb/publicationMediaObject/{contentId}"
                    | None -> ""
                externalGroupId, mediaSetId
        let mapping =
            {
                InternalGroupId = mediaSetId
                ExternalGroupId = externalGroupId
            }
        let publication = getPotionPublication oddjobConfig externalGroupId
        {
            Mapping = mapping
            Publication = publication
        }

    let getStatusSummary oddjobConfig (potion: PotionInfo) =
        let potionPublication = getPotionPublication oddjobConfig potion.Mapping.ExternalGroupId
        {
            PotionStatusSummary.PotionGroupId = potion.Mapping.ExternalGroupId
            PublicationUrl = potionPublication.PublicationUrl
        }

    let evaluatePersistenceId (reader: IStateReader) (entityId: string) =
        // In case this is an external Potion group ID, try resolving internal one
        if entityId.Contains("-") then
            let state = reader.PotionSetState entityId |> Async.RunSynchronously
            match state.Group with
            | Some group -> group.MediaSetId.ContentId.Value
            | None -> entityId
        else
            entityId

    let getEventStoreHealth (eventStoreSettings: EventStore) (logger: ILoggingAdapter) =
        let url = sprintf "%s/potion/count?code=%s" eventStoreSettings.PublishUrl eventStoreSettings.AuthorizationKey
        let response = http { GET url } |> Request.send
        match int response.statusCode with
        | statusCode when (int statusCode) < 300 -> HealthCheckResult.Ok("", [])
        | statusCode ->
            let message = $"Error accessing Event Store (error={statusCode}, {response.reasonPhrase})"
            logger.Error message
            HealthCheckResult.Error("Event Store", message, [])
