namespace Nrk.Oddjob.Core

type MediaSetActions = MediaSetCommand list

[<RequireQualifiedAccess>]
module MediaSetActions =
    let fromMediaSetJob job (desiredState: DesiredMediaSetState) =
        match job with
        | MediaSetJob.PublishMediaSet j ->
            let containsRevoked =
                match j.Content with
                | Parts(partIds, _) -> desiredState.RevokedParts |> Seq.intersect partIds |> Seq.isNotEmpty
                | _ -> false

            if containsRevoked then
                [ MediaSetCommand.ClearDesiredState ]
            else
                [
                    MediaSetCommand.AssignDesiredState
                        {
                            GeoRestriction = j.GeoRestriction
                            Content = j.Content
                            MediaType = j.MediaType
                            RevokedParts = desiredState.RevokedParts
                        }
                ]
        | MediaSetJob.PublishFile j ->
            match j.PartId with
            | Some partId ->
                if desiredState.RevokedParts |> List.contains partId then
                    []
                else
                    [
                        MediaSetCommand.AssignPart(partId, 0)
                        MediaSetCommand.AssignFile(j.PartId, j.ContentFile)
                    ]
            | None -> [ MediaSetCommand.AssignFile(j.PartId, j.ContentFile) ]
        | MediaSetJob.PublishSubtitles j ->
            match j.SubtitlesFiles with
            | [] -> []
            | _ ->
                match desiredState.Content, j.PartId with
                | Empty, _ -> false
                | NoParts _, None -> true // Potion clips
                | NoParts _, Some _ -> false // Invalid combination
                | Parts _, None -> true // Single-part or stitched
                | Parts([ partId ], _), Some partId' -> partId = partId' // Single-part that receives update on that part
                | Parts _, _ -> false // No matching parts
                |> function
                    | true -> // Content set has matching part
                        let subtitlesCommands =
                            if j.SubtitlesFiles.IsEmpty then //TODO: Can be removed
                                match desiredState.Content with
                                | Empty -> []
                                | NoParts chunk when chunk.Subtitles.IsEmpty -> []
                                | Parts(_, chunk) when chunk.Subtitles.IsEmpty -> []
                                | _ -> [ MediaSetCommand.AssignSubtitlesFiles j.SubtitlesFiles ]
                            else
                                [ MediaSetCommand.AssignSubtitlesFiles j.SubtitlesFiles ]
                        match j.PartId with
                        | Some partId when desiredState.RevokedParts |> List.contains partId -> []
                        | _ -> subtitlesCommands
                    | false -> [] // Content set has no matching part
        | MediaSetJob.PublishSubtitlesLinks j -> [ MediaSetCommand.AssignSubtitlesLinks j.SubtitlesLinks ]
        | MediaSetJob.DeleteSubtitles j -> [ MediaSetCommand.RemoveSubtitlesFile(j.LanguageCode, j.Name) ]
        | MediaSetJob.SetGeoRestriction j -> [ MediaSetCommand.SetGeoRestriction j.GeoRestriction ]
        | MediaSetJob.ClearMediaSet _ -> [ MediaSetCommand.ClearDesiredState ]
        | MediaSetJob.ActivateParts j ->
            [
                j.Parts |> List.map (fun p -> MediaSetCommand.RestorePart(fst p))
                j.Parts |> List.map (fun p -> MediaSetCommand.AssignPart(fst p, snd p))
            ]
            |> List.concat
        | MediaSetJob.DeactivateParts j ->
            let revokePartsComamnds = j.Parts |> List.map MediaSetCommand.RevokePart
            match desiredState.Content with
            | Parts(partIds, _) when Seq.intersect partIds j.Parts |> Seq.isNotEmpty -> MediaSetCommand.ClearDesiredState :: revokePartsComamnds
            | _ -> revokePartsComamnds
        | _ -> []

    let rec filter desiredState forceOverwrite (actions: MediaSetActions) =
        match actions, desiredState with
        | [ MediaSetCommand.AssignDesiredState jobDesiredState ], desiredState when desiredState = jobDesiredState && not forceOverwrite -> []

        | [ MediaSetCommand.ClearDesiredState ], desiredState when desiredState.IsEmpty() && not forceOverwrite -> []

        | [ MediaSetCommand.ClearDesiredState ], _ -> actions

        | [ MediaSetCommand.SetGeoRestriction geoRestriction ], desiredState when geoRestriction <> desiredState.GeoRestriction -> actions

        | [ MediaSetCommand.AssignDesiredState jobDesiredState ], desiredState ->
            let gr =
                if jobDesiredState.GeoRestriction <> GeoRestriction.Unspecified then
                    jobDesiredState.GeoRestriction
                else
                    desiredState.GeoRestriction
            [
                MediaSetCommand.AssignDesiredState
                    { jobDesiredState with
                        GeoRestriction = gr
                    }
            ]

        | action :: actions, _ ->
            let newDesiredState = desiredState |> DesiredMediaSetState.update (MediaSetEvent.fromCommand action)
            if newDesiredState <> desiredState then
                action :: filter newDesiredState forceOverwrite actions
            else
                match action with
                | MediaSetCommand.AssignSubtitlesFile _
                | MediaSetCommand.AssignSubtitlesFiles _ -> action :: filter newDesiredState forceOverwrite actions // Always proceed on published subtitles files, the content may differ
                | _ -> filter desiredState forceOverwrite actions

        | _, desiredState when desiredState.IsEmpty() -> actions

        | _ -> []
