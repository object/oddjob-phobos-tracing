namespace Nrk.Oddjob.Core

[<AutoOpen>]
module CommonTypes =

    type Alphanumeric =
        | Alphanumeric of string

        member this.Value =
            this
            |> function
                | Alphanumeric s -> s
        member this.IsEmpty() = String.isNullOrEmpty this.Value

    type Url =
        | Url of string

        member this.Value =
            this
            |> function
                | Url s -> s
        member this.IsEmpty() = String.isNullOrEmpty this.Value

    type AbsoluteUrl =
        | AbsoluteUrl of string

        member this.Value =
            this
            |> function
                | AbsoluteUrl s -> s
        member this.IsEmpty() = String.isNullOrEmpty this.Value

    type RelativeUrl =
        | RelativeUrl of string

        member this.Value =
            this
            |> function
                | RelativeUrl s -> s
        member this.IsEmpty() = String.isNullOrEmpty this.Value

    type TransferBuffer = byte array

    type AsyncResponse = | Success
