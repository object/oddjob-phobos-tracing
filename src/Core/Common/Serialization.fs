namespace Nrk.Oddjob.Core

module SerializationSettings =
    open Microsoft.FSharpLu.Json
    open Newtonsoft.Json

    type ActorRefConverter(system: Akka.Actor.ActorSystem) =
        inherit JsonConverter()

        static member IsActorRef(objectType: System.Type) =
            objectType = typedefof<Akka.Actor.IActorRef> || typedefof<Akka.Actor.IActorRef>.IsAssignableFrom(objectType)

        static member IsGenericActorRef(objectType: System.Type) =
            typedefof<Akkling.ActorRefs.IActorRef<_>>.IsAssignableFrom(objectType)
            || objectType.IsGenericType && objectType.GetGenericTypeDefinition() = typedefof<Akkling.ActorRefs.IActorRef<_>>
            || objectType.GetInterfaces()
               |> Seq.exists (fun x -> x.IsGenericType && x.GetGenericTypeDefinition() = typedefof<Akkling.ActorRefs.IActorRef<_>>)

        override this.WriteJson(writer, value, _serializer) =
            let actorRef =
                if value :? Akka.Actor.IActorRef then
                    value :?> Akka.Actor.IActorRef
                else
                    value.GetType().GetProperty("Underlying").GetValue(value) :?> Akka.Actor.IActorRef
            if actorRef = null then
                failwithf "Unable to serialize value of type %A" (value.GetType())
            let str = Akka.Serialization.Serialization.SerializedActorPath actorRef
            writer.WriteValue str

        override this.ReadJson(reader, objectType, _existingValue, _serializer) =
            let actorRef = (system :?> Akka.Actor.ExtendedActorSystem).Provider.ResolveActorRef(reader.Value.ToString())
            if ActorRefConverter.IsGenericActorRef objectType then
                let generic = typedefof<Akkling.ActorRefs.TypedActorRef<_>>
                let typeArgs = objectType.GetGenericArguments()
                let ctor = generic.MakeGenericType(typeArgs).GetConstructors() |> Seq.head
                let typedActorRef = ctor.Invoke([| box actorRef |])
                box typedActorRef
            else
                box actorRef

        override this.CanConvert(objectType) =
            ActorRefConverter.IsActorRef objectType || ActorRefConverter.IsGenericActorRef objectType

    /// Based on https://github.com/Microsoft/fsharplu/blob/master/FSharpLu.Json/Compact.fs
    type Settings =
        static member pascalCase =
            let s =
                JsonSerializerSettings(NullValueHandling = NullValueHandling.Ignore, MissingMemberHandling = MissingMemberHandling.Ignore)
            s.Converters.Add(CompactUnionJsonConverter())
            s

        static member camelCase =
            let s =
                JsonSerializerSettings(
                    NullValueHandling = NullValueHandling.Ignore,
                    MissingMemberHandling = MissingMemberHandling.Ignore,
                    ContractResolver = Serialization.CamelCasePropertyNamesContractResolver()
                )
            s.Converters.Add(CompactUnionJsonConverter())
            s

        static member snakeCase =
            let s =
                JsonSerializerSettings(
                    NullValueHandling = NullValueHandling.Ignore,
                    MissingMemberHandling = MissingMemberHandling.Ignore,
                    ContractResolver = Serialization.DefaultContractResolver(NamingStrategy = Serialization.SnakeCaseNamingStrategy())
                )
            s.Converters.Add(CompactUnionJsonConverter())
            s

    type SettingsWithActorRef(system: Akka.Actor.ActorSystem) =
        member _.PascalCase =
            let s =
                JsonSerializerSettings(NullValueHandling = NullValueHandling.Ignore, MissingMemberHandling = MissingMemberHandling.Ignore)
            s.Converters.Add(CompactUnionJsonConverter())
            s.Converters.Add(ActorRefConverter(system))
            s

    let PascalCaseWithActorRef system = SettingsWithActorRef(system).PascalCase
    let PascalCase = Settings.pascalCase
    let CamelCase = Settings.camelCase
    let SnakeCase = Settings.snakeCase

module SerializationOptions =
    open System.Text.Json.Serialization
    open System.Text.Json

    type PascalCaseNamingPolicy() =
        inherit JsonNamingPolicy()
        override this.ConvertName(name) =
            let upperCaseFirstLetter (token: string) =
                token.Substring(0, 1).ToUpper() + token.Substring(1)
            if name = name.ToLower() then // Support names with camel casing
                name.Split '_' |> Seq.map upperCaseFirstLetter |> Seq.concat |> System.String.Concat
            else
                upperCaseFirstLetter name

    let getOptionsWithNamingPolicy namingPolicy =
        let options =
            JsonFSharpOptions()
                .WithUnionExternalTag()
                .WithUnionUnwrapFieldlessTags()
                .WithUnwrapOption()
                .WithUnionUnwrapSingleFieldCases()
                .WithUnionAllowUnorderedTag()
                .WithUnionTagNamingPolicy(namingPolicy)
                .WithAllowNullFields()
                .WithSkippableOptionFields()
                .ToJsonSerializerOptions()
        options.PropertyNameCaseInsensitive <- true
        options.PropertyNamingPolicy <- namingPolicy
        options.UnmappedMemberHandling <- JsonUnmappedMemberHandling.Skip
        options.DefaultIgnoreCondition <- JsonIgnoreCondition.WhenWritingNull
        options.Converters.Add(JsonStringEnumConverter(JsonNamingPolicy.CamelCase))
        options

    let CamelCase = getOptionsWithNamingPolicy JsonNamingPolicy.CamelCase
    let PascalCase = getOptionsWithNamingPolicy <| PascalCaseNamingPolicy()
    let SnakeCase = getOptionsWithNamingPolicy JsonNamingPolicy.SnakeCaseLower

module Serialization =

    module Newtonsoft =
        open Newtonsoft.Json

        let inline private serialize<'t> (settings: JsonSerializerSettings) x =
            JsonConvert.SerializeObject(x, Formatting.Indented, settings)

        let inline private deserialize<'t> (settings: JsonSerializerSettings) json : 't =
            JsonConvert.DeserializeObject<'t>(json, settings)

        let inline serializeObject settings (o: obj) = serialize settings o

        let inline deserializeObject<'a> settings (s: string) : 'a = deserialize<'a> settings s

    module SystemTextJson =
        open System.Text.Json

        let inline private serialize<'t> (options: JsonSerializerOptions) x = JsonSerializer.Serialize(x, options)

        let inline private deserialize<'t> (options: JsonSerializerOptions) (json: string) : 't =
            JsonSerializer.Deserialize<'t>(json, options)

        let inline serializeObject options (o: obj) = serialize options o

        let inline deserializeObject<'a> options (s: string) : 'a = deserialize<'a> options s
