namespace Nrk.Oddjob.Core.Dto

open System

[<AllowNullLiteral>]
type IProtoBufSerializable = interface end

[<AllowNullLiteral>]
type IProtoBufSerializableEvent =
    inherit IProtoBufSerializable
    abstract member Timestamp: DateTimeOffset
