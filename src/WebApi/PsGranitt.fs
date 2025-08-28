namespace Nrk.Oddjob.WebApi

module PsGranitt =

    open System

    [<CLIMutable; NoComparison>]
    type GetRights =
        {
            RightsId: decimal
            ProgrammeId: decimal
            PublishStart: DateTime
            PublishEnd: DateTime
            GeolocationId: decimal
            HighSecurity: decimal
            NoHD: decimal
            Created: DateTime
            Changed: DateTime
        }

    [<CLIMutable; NoComparison>]
    type GetDebugProgramRights =
        {
            PiProgId: string
            Geolocation: string
            PublishStart: DateTime
            PublishEnd: DateTime
            HighSecurity: decimal
        }
