namespace Nrk.Oddjob.Core.Manifests

[<RequireQualifiedAccess>]
type MediaManifestStream =
    | Hls of HlsManifestStream
    | Dash of DashManifestStream

    member this.Bandwidth =
        match this with
        | Hls stream -> stream.Bandwidth
        | Dash stream -> stream.Bandwidth
    member this.Resolution =
        match this with
        | Hls stream -> stream.Resolution
        | Dash stream -> stream.Resolution
    member this.FrameRate =
        match this with
        | Hls stream -> stream.FrameRate
        | Dash stream -> stream.FrameRate
    member this.Codecs =
        match this with
        | Hls stream -> stream.Codecs
        | Dash stream -> stream.Codecs

[<RequireQualifiedAccess>]
type MediaManifest =
    | Hls of HlsManifest
    | Dash of DashManifest

    member this.Streams =
        match this with
        | Hls manifest -> manifest.Streams |> List.map MediaManifestStream.Hls
        | Dash manifest -> manifest.Streams |> List.map MediaManifestStream.Dash
