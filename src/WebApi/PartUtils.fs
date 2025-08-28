namespace Nrk.Oddjob.WebApi

module PartUtils =

    open Nrk.Oddjob.Core

    let private prfQualityIds = [ 180UL; 270UL; 360UL; 540UL; 720UL ]

    let getPartInfo (state: MediaSetState) (partIds: PartId list) partNumber : PartInfo =
        {
            PartIds = partIds |> List.map _.Value |> List.toArray
            PartNumber = uint64 partNumber
            GlobalConnect =
                {
                    Files =
                        state.Current.GlobalConnect.Files
                        |> Map.toList
                        |> List.filter (fun (x, y) ->
                            match x.PartId with
                            | Some partId -> partIds |> List.contains partId
                            | None -> List.isEmpty partIds || partIds.Length > 1
                            && (y.RemoteState.State <> DistributionState.Deleted || state.Desired.Content |> ContentSet.containsFileRef x))
                        |> List.map (fun (x, y) ->
                            {
                                GlobalConnectFileStateInfo.QualityId = x.QualityId.Value
                                File =
                                    {
                                        GlobalConnectFileInfo.SourcePath =
                                            y.File.SourcePath |> Option.map FilePath.value |> Option.defaultValue System.String.Empty
                                        RemotePath = y.File.RemotePath.Value
                                    }
                                RemoteState = y.RemoteState
                                LastResult = RemoteResourceResult.Convert(y.LastResult)
                            })
                        |> List.toArray
                    Subtitles =
                        state.Current.GlobalConnect.Subtitles
                        |> Map.toList
                        |> List.filter (fun (x, y) ->
                            (y.RemoteState.State <> DistributionState.Deleted || state.Desired.Content |> ContentSet.containsSubtitlesRef x))
                        |> List.map (fun (x, y) ->
                            {
                                GlobalConnectSubtitlesStateInfo.LanguageCode = x.LanguageCode.Value
                                Name = x.Name.Value
                                File =
                                    {
                                        GlobalConnectSubtitlesInfo.SourcePath = y.Subtitles.Subtitles.SourcePath.Value
                                        RemotePath = y.Subtitles.RemotePath.Value
                                    }
                                RemoteState = y.RemoteState
                                LastResult = RemoteResourceResult.Convert(y.LastResult)
                            })
                        |> List.toArray
                    Smil =
                        state.Current.GlobalConnect.Smil
                        |> fun x ->
                            {
                                GlobalConnectSmilStateInfo.Smil =
                                    {
                                        Content = x.Smil.Content.Serialize()
                                        RemotePath = x.Smil.RemotePath.Value
                                    }
                                RemoteState = x.RemoteState
                                LastResult = RemoteResourceResult.Convert(x.LastResult)
                            }
                }
        }

    let getPartsInfo state =
        match state.Desired.Content with
        | ContentSet.Empty -> [||]
        | ContentSet.NoParts _ -> [| getPartInfo state [] 1 |]
        | ContentSet.Parts(partIds, _) -> [| getPartInfo state partIds 1 |]

    let getRemoteStatesResult remoteStates resourceType =
        match remoteStates with
        | [||] -> DistributionStatus.Error $"No {resourceType} available"
        | _ ->
            let countAll = remoteStates |> Array.length
            let getFilesCountByState (state: DistributionState) =
                remoteStates |> Array.filter (fun x -> x.State = state) |> Array.length
            if getFilesCountByState DistributionState.Completed = countAll then
                DistributionStatus.Ok
            else if getFilesCountByState DistributionState.Deleted = countAll then
                DistributionStatus.NotStarted
            else if
                ([
                    DistributionState.Failed
                    DistributionState.Rejected
                    DistributionState.Cancelled
                 ]
                 |> List.map getFilesCountByState
                 |> List.sum) > 0
            then
                DistributionStatus.Error $"Upload of {resourceType} failed"
            else
                DistributionStatus.InProgress

    let getFilesResult (parts: PartInfo array) =
        match parts with
        | [||] -> DistributionStatus.Error "No active parts"
        | _ ->
            let filesStates = parts |> Array.map PartInfo.fileStates |> Array.concat
            getRemoteStatesResult filesStates "media files"

    let getSubtitlesResult (parts: PartInfo array) subtitles =
        match parts, subtitles with
        | [||], _ -> DistributionStatus.Error "No active parts"
        | _, [||] -> DistributionStatus.Ok
        | _, _ ->
            let subtitlesStates = parts |> Array.map PartInfo.subtitlesStates |> Array.concat
            getRemoteStatesResult subtitlesStates "subtitles files"

    let getGeorestrictionResult (_parts: PartInfo array) =
        // TODO
        DistributionStatus.Ok

    let getStatusSummary (parts: PartInfo array) =
        let filesStates = parts |> Array.map PartInfo.fileStates |> Array.concat
        let subtitlesStates = parts |> Array.map PartInfo.subtitlesStates |> Array.concat
        sprintf
            "%d active part(s), %d completed of total %d media files, %d completed of total %d subtitles files"
            (parts |> Array.length)
            (filesStates |> Array.filter (fun x -> x.State = DistributionState.Completed) |> Array.length)
            (filesStates |> Array.length)
            (subtitlesStates |> Array.filter (fun x -> x.State = DistributionState.Completed) |> Array.length)
            (subtitlesStates |> Array.length)
