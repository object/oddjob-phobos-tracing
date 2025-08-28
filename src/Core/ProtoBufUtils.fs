namespace Nrk.Oddjob.Core

module ProtoBufUtils =
    open ProtoBuf
    open System

    [<ProtoContract>]
    type DateTimeOffsetSurrogate() =
        [<ProtoMember(1)>]
        member val DateTimeString = "" with get, set
        static member public op_Implicit(value: DateTimeOffset) : DateTimeOffsetSurrogate =
            DateTimeOffsetSurrogate(DateTimeString = value.ToString("o"))
        static member public op_Implicit(value: DateTimeOffsetSurrogate) : DateTimeOffset =
            DateTimeOffset.Parse(value.DateTimeString)

    let initProtoBuf () =
        ProtoBuf.Meta.RuntimeTypeModel.Default.Add(typedefof<DateTimeOffset>, false).SetSurrogate(typedefof<DateTimeOffsetSurrogate>)
