namespace Nrk.Oddjob.Ps

module PsGranittActors =

    open Akkling

    open Nrk.Oddjob.Core
    open Nrk.Oddjob.Core.Granitt

    open PsTypes
    open GranittTypes

    [<NoEquality; NoComparison>]
    type PsGranittActorProps =
        {
            GetGranittContext: unit -> GranittContext
        }

    type SubtitlesDetails =
        {
            FilesetParts: PsProgramFilesetParts list
            ProgramSubtitles: PsSubtitles list
        }

    type PsPublishJobInfo = PsPublishJobInfo of PsProgramFilesetParts list * PsSubtitles list

    type TranscodingStatusUpdated = TranscodingStatusUpdated

    type GranittCommand =
        | UpdateForTranscodingJob of PsVideoTranscodingJob
        | GetProgramRights of PiProgId

    let psGranittActor (aprops: PsGranittActorProps) (mailbox: Actor<_>) =

        let updateForTranscodingJob (job: PsVideoTranscodingJob) =
            use ctx = aprops.GetGranittContext()
            match job.CarrierIds |> List.tryHead with
            | Some carrierId ->
                match MySqlGranitt.tryGetProgrammeIdFromCarrierId ctx.Value (CarrierId.create carrierId) with
                | Some programmeId ->
                    job.Carriers
                    |> Seq.iter (fun carrier ->
                        if String.isNotNullOrEmpty carrier.Duration then
                            let duration = int64 (System.Xml.XmlConvert.ToTimeSpan(carrier.Duration).TotalSeconds) * int64 25 // Granitt stores duration in frames, 25 fps
                            MySqlGranitt.setTranscodingStatusAndDuration ctx.Value (GranittCarrierId.ofString carrier.CarrierId, duration) programmeId
                        else
                            MySqlGranitt.setTranscodingStatus ctx.Value (GranittCarrierId.ofString carrier.CarrierId) programmeId)
                    logDebug mailbox $"Updated Granitt for transcoding job on carrier %A{carrierId}"
                    TranscodingStatusUpdated
                | None -> failwithf $"Programme ID for carrier ID [%A{carrierId}] not found"
            | None -> failwithf "Empty CarrierIds list"

        let getProgramRights piProgId =
            use ctx = aprops.GetGranittContext()
            MySqlGranitt.getProgramRights ctx.Value piProgId

        let rec loop () =
            actor {
                let! message = mailbox.Receive()
                match message with
                | UpdateForTranscodingJob job -> mailbox.Sender() <! getGranittResult (fun () -> updateForTranscodingJob job)
                | GetProgramRights piProgId -> mailbox.Sender() <! getGranittResult (fun () -> getProgramRights piProgId)
                return! ignored ()
            }

        loop ()
