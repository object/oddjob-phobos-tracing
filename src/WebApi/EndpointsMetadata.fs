namespace Nrk.Oddjob.WebApi

open Giraffe.EndpointRouting
open Giraffe.OpenApi
open Microsoft.AspNetCore.Http
open Microsoft.AspNetCore.Builder
open Microsoft.OpenApi.Any
open Microsoft.OpenApi.Models

open Nrk.Oddjob.Potion.PotionTypes
open Nrk.Oddjob.Ps.PsTypes
open Nrk.Oddjob.Ps.PsDto

open EventJournalUtils
open IngesterStatus
open SwaggerTypes

module EndpointsMetadata =

    module Operation =
        [<Literal>]
        let GetHealth = "getHealth"
        [<Literal>]
        let GetRecent = "getRecent"
        [<Literal>]
        let GetGlobalConnectIngesters = "getGlobalConnectIngesters"
        [<Literal>]
        let GetMediaSetStatus = "getMediaSetStatus"
        [<Literal>]
        let GetMediaSetState = "getMediaSetState"
        [<Literal>]
        let GetEvents = "getEvents"
        [<Literal>]
        let GetEventsState = "getEventsState"
        [<Literal>]
        let ActivateMediaSet = "activateMediaSet"
        [<Literal>]
        let DeactivateMediaSet = "deactivateMediaSet"
        [<Literal>]
        let GetPsRights = "getPsRights"
        [<Literal>]
        let GetPsFiles = "getPsFiles"
        [<Literal>]
        let GetPsTranscoding = "getPsTranscoding"
        [<Literal>]
        let GetPsArchive = "getPsArchive"
        [<Literal>]
        let GetReminders = "getReminders"
        [<Literal>]
        let RepairMediaSet = "repairMediaSet"
        [<Literal>]
        let MigrateMediaSet = "migrateMediaSet"
        [<Literal>]
        let ResyncPlayback = "resyncPlayback"
        [<Literal>]
        let UpdatePsRights = "updatePsRights"
        [<Literal>]
        let UpdatePsFiles = "updatePsFiles"
        [<Literal>]
        let SendPsProgramToQueue = "sendPsProgramToQueue"
        [<Literal>]
        let SendPsMessageToQueue = "sendPsMessageToQueue"
        [<Literal>]
        let SendPotionMessageToQueue = "sendPotionMessageToQueue"
        [<Literal>]
        let CreateReminder = "createReminder"
        [<Literal>]
        let ClearMediaSet = "clearMediaSet"

    let withOpenApi operationName (endpoint: Routers.Endpoint) =
        match operationName with
        | Operation.GetHealth ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Returns health status of Oddjob services and integrations")
            |> configureEndpoint _.WithTags([| "status" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<HealthStatus>)
                            ResponseBody(statusCode = 500, contentTypes = [| "application/json" |], responseType = typeof<HealthStatus>)
                        |]
                )
            )

        | Operation.GetRecent ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Obtains information about recently processed mediasets, with timestamps and status")
            |> configureEndpoint _.WithTags([| "status" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<MediaSetStatusInfo array>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "status",
                                        In = ParameterLocation.Query,
                                        Description = "Only retrieve mediasets with the specified [status]",
                                        Required = false,
                                        Schema =
                                            OpenApiSchema(
                                                Enum =
                                                    [|
                                                        OpenApiString("pending")
                                                        OpenApiString("rejected")
                                                        OpenApiString("completed")
                                                        OpenApiString("expired")
                                                    |]
                                            )
                                    )
                                    OpenApiParameter(
                                        Name = "skipCompleted",
                                        In = ParameterLocation.Query,
                                        Description = "Hide completed mediasets",
                                        Required = false,
                                        Schema = OpenApiSchema(Type = "boolean")
                                    )
                                    OpenApiParameter(
                                        Name = "takeLast",
                                        In = ParameterLocation.Query,
                                        Description =
                                            "Retrieve mediasets that are activated within the [takeLast] interval (notation examples: 1m, 2h, 5s, 1d)",
                                        Required = false,
                                        Schema = OpenApiSchema(Type = "interval")
                                    )
                                    OpenApiParameter(
                                        Name = "skipLast",
                                        In = ParameterLocation.Query,
                                        Description = "Skip mediasets that are activated after the [skipLast] interval (notation examples: 1m, 2h, 5s, 1d)",
                                        Required = false,
                                        Schema = OpenApiSchema(Type = "interval")
                                    )
                                    OpenApiParameter(
                                        Name = "count",
                                        In = ParameterLocation.Query,
                                        Description = "Return the [count] most recent entries (after applying other parameters)",
                                        Required = false,
                                        Schema = OpenApiSchema(Default = OpenApiInteger(100), Type = "integer")
                                    )
                                    OpenApiParameter(
                                        Name = "countOnly",
                                        In = ParameterLocation.Query,
                                        Description = "When specified return only the count of matching entries, without the actual data",
                                        Required = false,
                                        Schema = OpenApiSchema(Type = "boolean")
                                    )
                                    OpenApiParameter(
                                        Name = "includeActions",
                                        In = ParameterLocation.Query,
                                        Description = "Show outstanding actions (if any)",
                                        Required = false,
                                        Schema = OpenApiSchema(Type = "boolean")
                                    )
                                |]
                            o)
                )
            )

        | Operation.GetGlobalConnectIngesters ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Returns status of GlobalConnect ingesters")
            |> configureEndpoint _.WithTags([| "globalconnect" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<IngestersState>)
                        |]
                )
            )

        | Operation.GetMediaSetStatus ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Returns a summary of the distribution status of a program or clip")
            |> configureEndpoint _.WithTags([| "status"; "mediaset"; "ps"; "potion" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<MediaSetSummary>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "clientId",
                                        In = ParameterLocation.Path,
                                        Description = "Client ID ('ps' or 'potion')",
                                        Required = true,
                                        Schema = OpenApiSchema(Enum = [| OpenApiString("ps"); OpenApiString("potion") |])
                                    )
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program/clip ID (without client ID prefix)",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.GetMediaSetState ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Returnes a snapshot of a specified mediaset state (more efficient than mediaset status endpoint)")
            |> configureEndpoint _.WithTags([| "status"; "mediaset"; "ps"; "potion" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<MediaSetPersistenceState>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "clientId",
                                        In = ParameterLocation.Path,
                                        Description = "Client ID ('ps' or 'potion')",
                                        Required = true,
                                        Schema = OpenApiSchema(Enum = [| OpenApiString("ps"); OpenApiString("potion") |])
                                    )
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program/clip ID (without client ID prefix)",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                    OpenApiParameter(
                                        Name = "includeActions",
                                        In = ParameterLocation.Query,
                                        Description = "Show outstanding actions (if any)",
                                        Required = false,
                                        Schema = OpenApiSchema(Type = "boolean")
                                    )
                                |]
                            o)
                )
            )

        | Operation.GetEvents ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Returns persistent actor event history")
            |> configureEndpoint _.WithTags([| "events"; "ps"; "potion" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<JournalEvent>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "eventType",
                                        In = ParameterLocation.Path,
                                        Description =
                                            "Event type (supported types: 'msc' for MediaSet Controller, 'psf' for PS Files Handler, 'psp' for PS Playback Handler, 'potion' for Potion Handler)",
                                        Required = true,
                                        Schema =
                                            OpenApiSchema(
                                                Enum =
                                                    [|
                                                        OpenApiString("msc")
                                                        OpenApiString("psf")
                                                        OpenApiString("psp")
                                                        OpenApiString("potion")
                                                    |]
                                            )
                                    )
                                    OpenApiParameter(
                                        Name = "eventId",
                                        In = ParameterLocation.Path,
                                        Description = "Event ID",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.GetEventsState ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Returns persistent actor state built from its events")
            |> configureEndpoint _.WithTags([| "events"; "ps"; "potion" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<PersistenceJournalState>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "eventType",
                                        In = ParameterLocation.Path,
                                        Description =
                                            "Event type (supported types: 'msc' for MediaSet Controller, 'psf' for PS Files Handler, 'psp' for PS Playback Handler, 'potion' for Potion Handler)",
                                        Required = true,
                                        Schema =
                                            OpenApiSchema(
                                                Enum =
                                                    [|
                                                        OpenApiString("msc")
                                                        OpenApiString("psf")
                                                        OpenApiString("psp")
                                                        OpenApiString("potion")
                                                    |]
                                            )
                                    )
                                    OpenApiParameter(
                                        Name = "eventId",
                                        In = ParameterLocation.Path,
                                        Description = "Event ID",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.ActivateMediaSet ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Activates the specified mediaset")
            |> configureEndpoint _.WithTags([| "mediaset" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<ProgramStatus>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "clientId",
                                        In = ParameterLocation.Path,
                                        Description = "Client ID ('ps' or 'potion')",
                                        Required = true,
                                        Schema = OpenApiSchema(Enum = [| OpenApiString("ps"); OpenApiString("potion") |])
                                    )
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program/clip ID (without client ID prefix)",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.DeactivateMediaSet ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Deactivates the specified mediaset")
            |> configureEndpoint _.WithTags([| "mediaset" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<ProgramStatus>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "clientId",
                                        In = ParameterLocation.Path,
                                        Description = "Client ('ps' or 'potion')",
                                        Required = true,
                                        Schema = OpenApiSchema(Enum = [| OpenApiString("ps"); OpenApiString("potion") |])
                                    )
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program/clip ID (without client ID prefix)",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.GetPsRights ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Returns information about PS program rights")
            |> configureEndpoint _.WithTags([| "rights"; "ps" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<RightsInfo array>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program ID",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.GetPsFiles ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Returns information about PS program files")
            |> configureEndpoint _.WithTags([| "files"; "ps" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<PsProgramFiles array>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program ID",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.GetPsTranscoding ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Returns information about PS program transcoding")
            |> configureEndpoint _.WithTags([| "files"; "ps" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<TranscodingInfo>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program ID",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.GetPsArchive ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Returns information about PS program archive")
            |> configureEndpoint _.WithTags([| "files"; "ps" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<ArchivePart array>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program ID",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.GetReminders ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Returns information about pending reminders")
            |> configureEndpoint _.WithTags([| "status"; "reminders" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<ReminderGroup array>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "include",
                                        In = ParameterLocation.Query,
                                        Description = "Only include reminders from a specific group",
                                        Required = false,
                                        Schema =
                                            OpenApiSchema(
                                                Enum =
                                                    [|
                                                        OpenApiString("clearmediaset")
                                                        OpenApiString("storagecleanup")
                                                        OpenApiString("potionbump")
                                                        OpenApiString("playabilitybump")
                                                    |]
                                            )
                                    )
                                    OpenApiParameter(
                                        Name = "take",
                                        In = ParameterLocation.Query,
                                        Description = "Return [take] first entries",
                                        Required = false,
                                        Schema = OpenApiSchema(Default = OpenApiInteger(10), Type = "integer")
                                    )
                                    OpenApiParameter(
                                        Name = "skip",
                                        In = ParameterLocation.Query,
                                        Description = "Skip first [skip] entries",
                                        Required = false,
                                        Schema = OpenApiSchema(Default = OpenApiInteger(0), Type = "integer")
                                    )
                                    OpenApiParameter(
                                        Name = "orderBy",
                                        In = ParameterLocation.Query,
                                        Description = "Order results by [orderBy] criteria",
                                        Required = false,
                                        Schema =
                                            OpenApiSchema(
                                                Default = OpenApiString("triggerTime"),
                                                Enum = [| OpenApiString("triggerTime"); OpenApiString("triggerCount") |]
                                            )
                                    )
                                    OpenApiParameter(
                                        Name = "countOnly",
                                        In = ParameterLocation.Query,
                                        Description = "When specified return only the count of matching entries, without the actual data",
                                        Required = false,
                                        Schema = OpenApiSchema(Type = "boolean")
                                    )
                                |]
                            o)
                )
            )

        | Operation.RepairMediaSet ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Repairs the specified mediaset")
            |> configureEndpoint _.WithTags([| "mediaset"; "ps"; "potion" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<ProgramStatus>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "clientId",
                                        In = ParameterLocation.Path,
                                        Description = "Client ID ('ps' or 'potion')",
                                        Required = true,
                                        Schema = OpenApiSchema(Enum = [| OpenApiString("ps"); OpenApiString("potion") |])
                                    )
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program/clip ID (without client ID prefix)",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.MigrateMediaSet ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Migrates the specified mediaset")
            |> configureEndpoint _.WithTags([| "mediaset"; "ps"; "potion" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<ProgramStatus>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "clientId",
                                        In = ParameterLocation.Path,
                                        Description = "Client ID ('ps' or 'potion')",
                                        Required = true,
                                        Schema = OpenApiSchema(Enum = [| OpenApiString("ps"); OpenApiString("potion") |])
                                    )
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program/clip ID (without client ID prefix)",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.ResyncPlayback ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Recync playback events for PS program")
            |> configureEndpoint _.WithTags([| "ps" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<ProgramStatus>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program ID",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.UpdatePsRights ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Updates rights information for PS program")
            |> configureEndpoint _.WithTags([| "rights"; "ps" |])
            |> addOpenApi (
                OpenApiConfig(
                    requestBody = RequestBody(requestType = typedefof<RightsInfo>, isOptional = false),
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<ProgramStatus>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program ID",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.UpdatePsFiles ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Updates files information for PS program")
            |> configureEndpoint _.WithTags([| "files"; "ps" |])
            |> addOpenApi (
                OpenApiConfig(
                    requestBody = RequestBody(requestType = typedefof<PsTranscodingDto>, isOptional = false),
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<ProgramStatus>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program ID",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.SendPsProgramToQueue ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Sends change message for PS programs to PS Change queue")
            |> configureEndpoint _.WithTags([| "queues"; "ps" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<ProgramStatus>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program ID",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                    OpenApiParameter(
                                        Name = "priority",
                                        In = ParameterLocation.Query,
                                        Description = "Set queue message priority",
                                        Required = false,
                                        Schema = OpenApiSchema(Default = OpenApiInteger(0), Type = "integer")
                                    )
                                    OpenApiParameter(
                                        Name = "source",
                                        In = ParameterLocation.Query,
                                        Description = "Set queue message source",
                                        Required = false,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.SendPsMessageToQueue ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Sends change message for PS programs to PS Change queue")
            |> configureEndpoint _.WithTags([| "queues"; "ps" |])
            |> addOpenApi (
                OpenApiConfig(
                    requestBody = RequestBody(requestType = typedefof<PsChangeJobDto>, isOptional = false),
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<ProgramStatus>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "priority",
                                        In = ParameterLocation.Query,
                                        Description = "Set queue message priority",
                                        Required = false,
                                        Schema = OpenApiSchema(Default = OpenApiInteger(0), Type = "integer")
                                    )
                                    OpenApiParameter(
                                        Name = "source",
                                        In = ParameterLocation.Query,
                                        Description = "Set queue message source",
                                        Required = false,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.SendPotionMessageToQueue ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Sends message for Potion clip to Potion queue")
            |> configureEndpoint _.WithTags([| "queues"; "potion" |])
            |> addOpenApi (
                OpenApiConfig(
                    requestBody = RequestBody(requestType = typedefof<PotionCommand>, isOptional = false),
                    responseBodies = [| ResponseBody(statusCode = 200) |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "priority",
                                        In = ParameterLocation.Query,
                                        Description = "Set queue message priority",
                                        Required = false,
                                        Schema = OpenApiSchema(Default = OpenApiInteger(0), Type = "integer")
                                    )
                                    OpenApiParameter(
                                        Name = "source",
                                        In = ParameterLocation.Query,
                                        Description = "Set queue message source",
                                        Required = false,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | Operation.CreateReminder ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Schedule reminder job")
            |> configureEndpoint _.WithTags([| "reminders" |])
            |> addOpenApi (
                OpenApiConfig(
                    requestBody = RequestBody(requestType = typedefof<CreateReminderInfo>, isOptional = false),
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<string>)
                        |]
                )
            )

        | Operation.ClearMediaSet ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithDescription("Clears (unpublishes) the specified mediaset")
            |> configureEndpoint _.WithTags([| "mediaset"; "ps"; "potion" |])
            |> addOpenApi (
                OpenApiConfig(
                    responseBodies =
                        [|
                            ResponseBody(statusCode = 200, contentTypes = [| "application/json" |], responseType = typeof<ProgramStatus>)
                        |],
                    configureOperation =
                        (fun o ->
                            o.Parameters <-
                                [|
                                    OpenApiParameter(
                                        Name = "clientId",
                                        In = ParameterLocation.Path,
                                        Description = "Client ('ps' or 'potion')",
                                        Required = true,
                                        Schema = OpenApiSchema(Enum = [| OpenApiString("ps"); OpenApiString("potion") |])
                                    )
                                    OpenApiParameter(
                                        Name = "contentId",
                                        In = ParameterLocation.Path,
                                        Description = "Program/clip ID (without client ID prefix)",
                                        Required = true,
                                        Schema = OpenApiSchema(Type = "string")
                                    )
                                |]
                            o)
                )
            )

        | _ ->
            endpoint
            |> configureEndpoint _.WithName(operationName)
            |> configureEndpoint _.WithTags([| "unsupported" |])
            |> addOpenApi (OpenApiConfig())
