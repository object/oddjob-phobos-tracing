namespace Nrk.Oddjob.Core

[<AutoOpen>]
module CoreTypes =

    [<Literal>]
    let OddjobSystem = "Oddjob"

    type ProgressCallback = uint64 -> unit

    [<RequireQualifiedAccess>]
    type Origin = | GlobalConnect

    module Origins =
        open Config

        let active (oddjobConfig: OddjobConfig) =
            seq {
                if OddjobConfig.hasGlobalConnect oddjobConfig.Origins then
                    Origin.GlobalConnect
            }
            |> Seq.toList

    [<RequireQualifiedAccess>]
    type GeoRestriction =
        | Unspecified
        | World
        | Norway
        | NRK

        static member Default = NRK

    // TODO: Migrate events and remove (AudioMode is replaced by Mixdown)
    [<RequireQualifiedAccess>]
    type AudioMode =
        | Default
        | Audio51

    [<RequireQualifiedAccess>]
    type MediaType =
        | Video
        | Audio

        static member Default = Video

    [<RequireQualifiedAccess>]
    type StreamingType =
        //| Unspecified
        | OnDemand
        | Live

    /// Please, try to use NrkClient instead
    [<Literal>]
    let PsClientId = "ps"
    /// Please, try to use NrkClient instead
    [<Literal>]
    let PotionClientId = "potion"

    [<RequireQualifiedAccess>]
    type NrkClient =
        | Potion
        | Ps

        override this.ToString() =
            match this with
            | Potion -> PotionClientId
            | Ps -> PsClientId

    type CarrierId =
        private
        | CarrierId of string

        override this.ToString() = let (CarrierId str) = this in str

    [<RequireQualifiedAccess>]
    module CarrierId =
        let tryCreate str =
            if String.isNullOrEmpty str then
                None
            else
                Some(CarrierId str)

        let create str =
            if String.isNullOrEmpty str then
                invalidArg "str" "CarrierId cannot be empty"
            else
                CarrierId str

        let value (CarrierId str) = str

    type PiProgId =
        | PiProgId of string

        override this.ToString() = let (PiProgId str) = this in str

    module PiProgId =
        let tryCreate str =
            if String.isNullOrEmpty str then
                None
            else
                Some(PiProgId(str.ToUpper()))

        let create str =
            if String.isNullOrEmpty str then
                invalidArg "str" "PiProgId cannot be empty"
            else
                PiProgId(str.ToUpper())

        let value (PiProgId str) = str

    [<RequireQualifiedAccess>]
    module Legacy =
        let qualityBitrates =
            Map
                [
                    (141, 141)
                    (180, 141)
                    (316, 316)
                    (270, 316)
                    (563, 563)
                    (360, 563)
                    (1266, 1266)
                    (540, 1266)
                    (2250, 2250)
                    (720, 2250)
                ]

        let qualityResolutions =
            Map
                [
                    (141, 180)
                    (180, 180)
                    (316, 270)
                    (270, 270)
                    (563, 360)
                    (360, 360)
                    (1266, 540)
                    (540, 540)
                    (2250, 720)
                    (720, 720)
                ]

    [<Literal>]
    let ThePerpetualEndOfTime = 2100

    type HealthCheckCommand = { RequestId: string }

    type HealthCheckResponse = { RequestId: string; Response: string }
