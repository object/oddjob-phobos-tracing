namespace Nrk.Oddjob.Core

[<AutoOpen>]
module Instrumentation =

    open System
    open Akkling
    open System.Diagnostics
    open OpenTelemetry.Context.Propagation
    open Phobos.Actor

    type ActivitySourceContext =
        {
            ActivitySource: ActivitySource
            ContextPropagator: TextMapPropagator
        }

        static member Create(sourceName) =
            {
                ActivitySource = new ActivitySource(sourceName)
                ContextPropagator = Propagators.DefaultTextMapPropagator
            }

    let rec private getTypeName' (t: Type) =
        if t.IsGenericType then
            let name = t.GetGenericTypeDefinition().Name
            let index = name.IndexOf('`')
            let name = if index = -1 then name else name.Substring(0, index)
            $"{name}<{String.Join(',', t.GetGenericArguments() |> Seq.map getTypeName')}>"
        else
            t.Name

    let private getTypeName o =
        if o = null then "Null" else o.GetType() |> getTypeName'

    let getTraceContext (mailbox: Actor<_>) =
        let instrumentation = mailbox.UntypedContext.GetInstrumentation()
        if instrumentation <> null then
            if instrumentation.UsableContext.HasValue then
                instrumentation.UsableContext.Value
            else
                Unchecked.defaultof<OpenTelemetry.Trace.SpanContext>
        else
            Unchecked.defaultof<OpenTelemetry.Trace.SpanContext>

    let createTraceSpanForContext (mailbox: Actor<_>) (spanContext: OpenTelemetry.Trace.SpanContext) spanName tags =
        let instrumentation = mailbox.UntypedContext.GetInstrumentation()
        if instrumentation <> null then
            let span = instrumentation.Tracer.StartActiveSpan(spanName, OpenTelemetry.Trace.SpanKind.Consumer, &spanContext)
            tags |> Seq.iter (fun (k, v: string) -> span.SetAttribute(k, v) |> ignore)
            span
        else
            null

    let createTraceSpan (mailbox: Actor<_>) spanName tags =
        let instrumentation = mailbox.UntypedContext.GetInstrumentation()
        if instrumentation <> null then
            createTraceSpanForContext mailbox (getTraceContext mailbox) spanName tags
        else
            null

    let createTraceSpanForMessage (mailbox: Actor<_>) baseName msg tags =
        let msg = box msg
        let spanName = $"{baseName}:{getTypeName msg}"
        createTraceSpan mailbox spanName tags

    let createTraceSpanOnConditionForContext (mailbox: Actor<_>) (spanContext: OpenTelemetry.Trace.SpanContext) spanName tags predicate =
        if predicate () then
            createTraceSpanForContext mailbox spanContext spanName tags
        else
            null

    let createTraceSpanOnCondition (mailbox: Actor<_>) spanName tags predicate =
        if predicate () then
            createTraceSpan mailbox spanName tags
        else
            null

    let updateTraceSpan (mailbox: Actor<_>) spanName tags =
        let instrumentation = mailbox.UntypedContext.GetInstrumentation()
        if instrumentation <> null && instrumentation.ActiveSpan <> null then
            instrumentation.ActiveSpan.UpdateName(spanName) |> ignore
            tags |> Seq.iter (fun (k, v: string) -> instrumentation.ActiveSpan.SetAttribute(k, v) |> ignore)

    let updateTraceSpanForMessage (mailbox: Actor<_>) baseName msg tags =
        let msg = box msg
        let spanName = $"{baseName}:{getTypeName msg}"
        updateTraceSpan mailbox spanName tags

    let endTraceSpan (traceSpan: OpenTelemetry.Trace.TelemetrySpan option) =
        traceSpan |> Option.iter _.Dispose()
        None

    let getActivityContext (activity: Activity) =
        if activity <> null then
            activity.Context
        else if Activity.Current <> null then
            Activity.Current.Context
        else
            Unchecked.defaultof<ActivityContext>

    let setTraceBaggage (baggage: (string * string) seq) =
        OpenTelemetry.Baggage.Current.SetBaggage(dict baggage) |> ignore

    let getTraceBaggage () =
        OpenTelemetry.Baggage.Current.GetBaggage()

    let addToTraceBaggage key value =
        getTraceBaggage () |> Seq.map (fun x -> x.Key, x.Value) |> Set.ofSeq |> Set.add (key, value) |> setTraceBaggage

    let setActivityTags tags (activity: Activity) =
        if activity <> null then
            tags |> Seq.iter (fun (key, value) -> activity.SetTag(key, value) |> ignore)

    let withActorTypeName actorTypeName (props: Props<'T>) =
        let settings = Phobos.Actor.Configuration.PhobosActorSettings(true, true).WithActorTypeName(actorTypeName)
        props.ToProps().WithInstrumentation(settings) |> Props<'T>.From
