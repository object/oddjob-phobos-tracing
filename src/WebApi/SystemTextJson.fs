namespace Nrk.Oddjob.WebApi

open Nrk.Oddjob.Core

[<RequireQualifiedAccess>]
module SystemTextJson =

    open System
    open System.Text
    open Microsoft.IO
    open System.Text.Json
    open Giraffe

    let internal recyclableMemoryStreamManager = Lazy<RecyclableMemoryStreamManager>()

    type Serializer(options: JsonSerializerOptions, _rmsManager: RecyclableMemoryStreamManager) =

        new() = Serializer(SerializationOptions.CamelCase, recyclableMemoryStreamManager.Value)
        new(options: JsonSerializerOptions) = Serializer(options, recyclableMemoryStreamManager.Value)

        interface Json.ISerializer with
            member _.Deserialize<'T>(string: string) =
                JsonSerializer.Deserialize<'T>(string, options)
            member _.Deserialize<'T>(bytes: byte[]) =
                JsonSerializer.Deserialize<'T>(ReadOnlySpan bytes, options)
            member _.DeserializeAsync<'T>(stream) =
                JsonSerializer.DeserializeAsync<'T>(stream, options).AsTask()
            member _.SerializeToBytes<'T>(value: 'T) =
                JsonSerializer.SerializeToUtf8Bytes<'T>(value, options)
            member _.SerializeToStreamAsync<'T> (value: 'T) stream =
                JsonSerializer.SerializeAsync<'T>(stream, value, options)
            member _.SerializeToString<'T>(value: 'T) =
                JsonSerializer.Serialize<'T>(value, options)
