namespace Nrk.Oddjob.Potion

module PotionUtils =

    open System
    open Nrk.Oddjob.Core
    open PotionTypes

    type Priority = byte

    module Priority =

        type MessageSource = string

        type PriorityCalculator(priorities: Config.OddjobConfig.PotionMessagePriority seq) =

            let validateConfiguration () =
                if priorities |> Seq.notExists (fun p -> p.source = "*") then
                    failwith "Missing default priority in configuration"
                if priorities |> Seq.exists (fun p -> String.IsNullOrWhiteSpace p.source) then
                    failwith "Empty source is not allowed"
                if priorities |> Seq.exists (fun p -> p.priority < 0 || p.priority > 10) then
                    failwith "Allowed range for priorities is [0..10]"

            do validateConfiguration ()

            let defaultPriority = (priorities |> Seq.find (fun p -> p.source = "*")).priority |> byte

            member _.CalculatePriority(source: MessageSource) : Priority =
                let result =
                    priorities
                    |> Seq.tryFind (fun p -> p.source = source)
                    |> Option.map (fun p -> byte p.priority)
                    |> Option.defaultValue defaultPriority
                result

    let getHeader (requestSource: string) forwardedFrom priority =
        let requestSource, forwardedFromSource =
            match requestSource.Split([| '|' |]) with
            | [| requestSource; forwardedFrom |] -> (requestSource, Some forwardedFrom)
            | _ -> (requestSource, None)
        {
            RequestSource = requestSource
            ForwardedFrom =
                forwardedFrom
                |> function
                    | Some _ -> forwardedFrom
                    | None -> forwardedFromSource
            Priority = priority
            Timestamp = DateTimeOffset.Now
            OverwriteMode = OverwriteMode.IfNewer
        }

    let createInternalGroupId () =
        let mediaSetId =
            {
                ContentId = ContentId.create <| normalizeGuid (Guid.NewGuid())
                ClientId = Alphanumeric PotionClientId
            }
        mediaSetId.Value

    let createInternalFileId (mediaSetId: MediaSetId) qualityId =
        {
            PartId = None
            QualityId = QualityId.create qualityId
        }
        |> FileRef.toFileName mediaSetId
        |> fun x -> sprintf "%s/%s/%s" mediaSetId.ClientId.Value mediaSetId.ContentId.Value x.Value

    let createInternalSubtitlesId (mediaSetId: MediaSetId) =
        sprintf "%s/%s/subtitles" mediaSetId.ClientId.Value mediaSetId.ContentId.Value

    let createPublishFileCommand (cmd: AddFileCommand) mediaSetId requestSource forwardedFrom priority : PublishFileJob =
        let fileRef =
            {
                PartId = None
                QualityId = QualityId.create cmd.QualityId
            }
        {
            Header = getHeader requestSource forwardedFrom priority
            MediaSetId =
                {
                    ClientId = mediaSetId.ClientId
                    ContentId = ContentId mediaSetId.ContentId.Value
                }
            PartId = None
            ContentFile =
                {
                    QualityId = QualityId.create cmd.QualityId
                    FileName = fileRef |> FileRef.toFileName mediaSetId
                    SourcePath = FilePath.tryCreate cmd.FilePath
                    MediaProperties = Unspecified
                }
        }

    let createPublishSubtitlesCommand (cmd: AddSubtitleCommand) mediaSetId requestSource forwardedFrom priority : PublishSubtitlesJob =
        {
            Header = getHeader requestSource forwardedFrom priority
            MediaSetId =
                {
                    ClientId = mediaSetId.ClientId
                    ContentId = ContentId mediaSetId.ContentId.Value
                }
            PartId = None
            SubtitlesFiles =
                cmd.Subtitles
                |> List.map (fun sub ->
                    SubtitlesFile.V1
                        {
                            LanguageCode = Alphanumeric sub.Language
                            Name = Alphanumeric "nor"
                            FileName = sub.Url |> String.split '/' |> Seq.last |> FileName
                            SourcePath = sub.Url |> SubtitlesLocation.fromAbsoluteUrl
                            Version = 0
                        })
        }

    let createDeleteSubtitlesCommand (cmd: DeleteSubtitleCommand) mediaSetId requestSource forwardedFrom priority : DeleteSubtitlesJob =
        {
            Header = getHeader requestSource forwardedFrom priority
            MediaSetId = mediaSetId
            PartId = None
            LanguageCode = Alphanumeric cmd.Language
            Name = Alphanumeric "nor"
        }

    let createJob (state: PotionSetState) (cmd: PotionCommand) (priority: Priority) forwardedFrom =
        let mediaSetId =
            match state.Group with
            | Some group -> group.MediaSetId
            | None -> invalidOp "State doesn't have MediaSetId"
        let requestSource = cmd.GetSource()
        let priority = int priority
        match cmd with
        | AddFile cmd -> createPublishFileCommand cmd mediaSetId requestSource forwardedFrom priority |> MediaSetJob.PublishFile
        | AddSubtitle cmd -> createPublishSubtitlesCommand cmd mediaSetId requestSource forwardedFrom priority |> MediaSetJob.PublishSubtitles
        | SetGeoBlock cmd ->
            MediaSetJob.SetGeoRestriction
                {
                    Header = getHeader requestSource forwardedFrom priority
                    MediaSetId = mediaSetId
                    GeoRestriction = cmd.GeoBlock
                }
        | DeleteGroup _ ->
            MediaSetJob.ClearMediaSet
                {
                    Header = getHeader requestSource forwardedFrom priority
                    MediaSetId = mediaSetId
                }
        | DeleteSubtitle cmd -> createDeleteSubtitlesCommand cmd mediaSetId requestSource forwardedFrom priority |> MediaSetJob.DeleteSubtitles
