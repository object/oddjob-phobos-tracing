namespace Nrk.Oddjob.Upload.GlobalConnect

open Nrk.Oddjob.Core
open Nrk.Oddjob.Upload.UploadTypes

module GlobalConnectUtils =

    type ExternalState =
        {
            Desired: DesiredMediaSetState
            Origin: CurrentGlobalConnectState
        }

    let createGlobalConnectState (job: ApplyState) =
        let originState =
            match job.OriginState with
            | OriginState.GlobalConnect state -> state
        {
            Desired = job.DesiredState
            Origin = originState
        }
