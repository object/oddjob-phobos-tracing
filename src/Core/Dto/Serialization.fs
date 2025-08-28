namespace Nrk.Oddjob.Core.Dto

[<RequireQualifiedAccess>]
module Serialization =

    open System
    open System.IO
    open ProtoBuf
    open Akka.Util
    open Nrk.Oddjob.Core

    type ProtobufSerializer(system: Akka.Actor.ExtendedActorSystem) =

        inherit Akka.Serialization.SerializerWithStringManifest(system)

        static member SupportedTypes =
            [
                typedefof<MediaSet.AssignClientContentId>
                typedefof<MediaSet.SetSchemaVersion>
                typedefof<MediaSet.SetMediaTypeFromMediaMode>
                typedefof<MediaSet.SetMediaType>
                typedefof<MediaSet.SetGeoRestriction>
                typedefof<MediaSet.SetAccessRestrictions>
                typedefof<MediaSet.AssignPart>
                typedefof<MediaSet.AssignFile>
                typedefof<MediaSet.AssignSubtitlesFile>
                typedefof<MediaSet.AssignSubtitlesFiles>
                typedefof<MediaSet.AssignSubtitlesLinks>
                typedefof<MediaSet.RemovePart>
                typedefof<MediaSet.RevokePart>
                typedefof<MediaSet.RestorePart>
                typedefof<MediaSet.RemoveFile>
                typedefof<MediaSet.RemoveSubtitlesFile>
                typedefof<MediaSet.AssignDesiredState>
                typedefof<MediaSet.MergeDesiredState>
                typedefof<MediaSet.ClearDesiredState>
                typedefof<MediaSet.ReceivedRemoteFileState>
                typedefof<MediaSet.ReceivedRemoteSubtitlesFileState>
                typedefof<MediaSet.ReceivedRemoteSmilState>
                typedefof<MediaSet.ClearRemoteFile>
                typedefof<MediaSet.AssignGlobalConnectFile>
                typedefof<MediaSet.AssignGlobalConnectSubtitles>
                typedefof<MediaSet.AssignGlobalConnectSmil>
                typedefof<MediaSet.ClearCurrentState>
                typedefof<MediaSet.MediaSetState>
            ]

        static member private RenamedManifests =
            [
                ("SetMediaMode", typedefof<MediaSet.SetMediaTypeFromMediaMode>)
                ("ReceivedRemoteSubtitlesState", typedefof<MediaSet.ReceivedRemoteSubtitlesFileState>)
            ]

        override this.ToBinary(o: obj) =
            use stream = new MemoryStream()
            Serializer.Serialize(stream, o)
            stream.ToArray()

        override this.FromBinary(bytes: byte[], manifest: string) =

            let typ =
                ProtobufSerializer.SupportedTypes
                |> List.tryFind (fun t -> manifest = t.Name || manifest = t.FullName || manifest = t.TypeQualifiedName())
                |> Option.defaultWith (fun () ->
                    ProtobufSerializer.RenamedManifests
                    |> List.tryFind (fun (m, _) -> m = manifest)
                    |> Option.map snd
                    |> Option.defaultWith (fun () ->
                        if
                            manifest.EndsWith("_Deprecated", StringComparison.InvariantCultureIgnoreCase)
                            || manifest.Contains("Akamai")
                            || manifest.Contains("Nep")
                        then
                            typedefof<MediaSet.DeprecatedEvent>
                        else
                            let errorMessage = $"Serializer doesn't support manifest %s{manifest}"
                            system.Log.Log(Akka.Event.LogLevel.ErrorLevel, null, errorMessage)
                            notSupported errorMessage))

            use stream = new MemoryStream(bytes)
            Serializer.Deserialize(typ, stream)

        override this.Manifest(o: obj) =
            if ProtobufSerializer.SupportedTypes |> List.contains (o.GetType()) then
                o.GetType().Name
            else
                let errorMessage = $"Serializer doesn't support type %s{o.GetType().FullName}"
                system.Log.Log(Akka.Event.LogLevel.ErrorLevel, null, errorMessage)
                notSupported errorMessage
        override this.Identifier = 126
