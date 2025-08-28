namespace Nrk.Oddjob.Core

open System
open System.Net
open System.Threading.Tasks
open Akkling
open FSharpx.Collections
open FsHttp

[<RequireQualifiedAccess>]
module Result =
    let map f result =
        match result with
        | Result.Ok x -> Result.Ok(f x)
        | Result.Error err -> Result.Error err

    let mapError f result =
        match result with
        | Result.Ok x -> Result.Ok x
        | Result.Error err -> Result.Error(f err)

    let bind f result =
        match result with
        | Result.Ok x ->
            match f x with
            | Result.Ok y -> Result.Ok y
            | Result.Error err -> Result.Error err
        | Result.Error err -> Result.Error err

    let getOrFail f result =
        match result with
        | Result.Ok x -> x
        | Result.Error err -> failwithf "%s" (f err)

    let getOrRaise result =
        match result with
        | Result.Ok x -> x
        | Result.Error err -> raise err

    let ofOption onMissing =
        function
        | Some x -> Result.Ok x
        | None -> Result.Error onMissing

    let ofChoice =
        function
        | Choice1Of2 x -> Result.Ok x
        | Choice2Of2 x -> Result.Error x

    let attempt f x =
        try
            Result.Ok(f x)
        with ex ->
            Result.Error ex

module Async =
    let AwaitTaskVoid: (Task -> Async<unit>) = Async.AwaitIAsyncResult >> Async.Ignore

    let map f workflow =
        async {
            let! res = workflow
            return f res
        }

    let cast<'t> workflow =
        async {
            let! res = workflow
            return box res :?> 't
        }

    let fromResult result = async { return result }

    /// Pipes result of async computation to recipient actor, wrapping exception in a Result and mapping it
    /// See https://github.com/Horusiath/Akkling/issues/88
    let pipeTo<'a, 'b> (recipient: ICanTell<'b>) (map: Result<'a, exn> -> 'b) (computation: Async<'a>) : unit =

        async {
            let! res = Async.Catch computation
            match res with
            | Choice1Of2 x -> recipient.Tell(map (Result.Ok x), Akka.Actor.ActorRefs.NoSender)
            | Choice2Of2 e -> recipient.Tell(map (Result.Error e), Akka.Actor.ActorRefs.NoSender)
        }
        |> Async.Start

[<AutoOpen>]
module Akkling =
    /// Like (<?), but synchronous.
    /// For the love of $DEITY, try not to use it. This is a blocking call.
    let inline (<<?) (tell: #ICanTell<'Message>) (msg: 'Message) : 'Response =
        tell.Ask<'Response>(msg, None) |> Async.RunSynchronously

module Option =
    /// Unwraps underlying value if Some, otherwise throws exception with provided `message`
    let getOrFail message option =
        match option with
        | Some x -> x
        | None -> failwith message

    let ofResult result =
        match result with
        | Ok x -> Some x
        | Error _ -> None

    let toArrayOrNull value =
        value |> Option.map (fun x -> [| x |]) |> Option.defaultValue null

    let ofArrayOrNull (xs: 'a array) =
        if xs = null then
            None
        else if xs.Length = 1 then
            Some xs[0]
        else
            failwithf $"%A{typeof<'a>} array must either be null or contain a single element, found %d{xs.Length}"

[<AutoOpen>]
module OptionCE =

    type OptionalBuilder() =
        member _.Return(x) = Some x

        member _.Bind(x, f) = Option.bind f x

        member _.ReturnFrom(x) = x

        member _.Zero() = None

    let maybe = OptionalBuilder()

module Function =
    let flip f a b = f b a
    let curry f a b = f (a, b)
    let uncurry f (a, b) = f a b

module String =
    /// Splits string by provided char, and then joins all but last result by same char
    /// E.g. _ -> foo_bar_baz_bah -> Some (foo_bar_baz, bah)
    let splitByLast (ch: char) (str: string) =
        match str.LastIndexOf(ch) with
        | -1 -> None
        | pos -> Some(str.Substring(0, pos), str.Substring(pos + 1))

    let toLower (str: string) = str.ToLower()

    let toUpper (str: string) = str.ToUpper()

    let split (char: char) (str: string) = str.Split(char)

    /// Replaces the given pattern in the given text with the replacement
    let inline replace (oldValue: string) newValue (text: string) = text.Replace(oldValue, newValue)
    let inline trimStart (char: char) (text: string) = text.TrimStart char

    let inline isNotNullOrEmpty value =
        System.String.IsNullOrEmpty(value) |> not

    let inline isNullOrEmpty value = System.String.IsNullOrEmpty(value)

    let inline toOption (str: string) =
        if isNullOrEmpty str then None else Some str

module List =
    let inline isNotEmpty source = List.isEmpty source |> not
    let inline isSingleton source = Seq.length source = 1

    /// Replaces all occurences satisfying the predicate with the result of applied replacement function call.
    let replace predicate replaceFn source =
        let rec replace' curr acc =
            match curr with
            | [] -> acc
            | x :: xs ->
                let newElem = if predicate x then replaceFn x else x
                replace' xs (newElem :: acc)
        replace' source [] |> List.rev

    /// Replaces all occurences satisfying the predicate with the result of applied replacement function call. On no match an 'addElt' is added.
    let addOrReplace predicate addElt replaceFn source =
        let rec addOrReplace' curr acc hasReplaced =
            match curr with
            | [] -> if hasReplaced then acc else addElt :: acc
            | x :: xs ->
                let elem, replaced = if predicate x then replaceFn x, true else x, hasReplaced
                addOrReplace' xs (elem :: acc) replaced
        addOrReplace' source [] false |> List.rev

    /// Searches for occurrences satisfying the predicate. If found, returns source intact. If not, 'addElt' is added to the source and returned.
    let retainOrAdd predicate addElt source =
        match source |> List.tryFind predicate with
        | Some _ -> source
        | None -> addElt :: source

    let tryMinBy (projection: 'T -> 'U) (list: 'T list) =
        match list with
        | [] -> None
        | list -> Some(List.minBy projection list)

    let tryMaxBy (projection: 'T -> 'U) (list: 'T list) =
        match list with
        | [] -> None
        | list -> Some(List.maxBy projection list)

    let toArrayOrNull xs =
        match xs with
        | [] -> null
        | xs -> xs |> List.toArray

    let ofArrayOrNull xs =
        if xs = null then List.empty else xs |> Array.toList

type These<'a, 'b> =
    | A of 'a
    | B of 'b
    | Both of 'a * 'b

module These =
    let tryA t =
        match t with
        | A a -> Some a
        | Both(a, _) -> Some a
        | B _ -> None

    let tryB t =
        match t with
        | A _ -> None
        | Both(_, b) -> Some b
        | B b -> Some b

    let tryBoth t =
        match t with
        | A _ -> None
        | Both(a, b) -> Some(a, b)
        | B _ -> None

    let from (t, u) =
        match t, u with
        | Some t, Some u -> Both(t, u)
        | Some t, None -> A t
        | None, Some u -> B u
        | None, None -> invalidOp "These has to contain at least one value"

module Map =
    let inline isNotEmpty source = Map.isEmpty source |> not

    let where = Map.filter

    let join (p: Map<'a, 'b>) (q: Map<'a, 'c>) : Map<'a, These<'b, 'c>> =
        Map.keys p
        |> Seq.append (Map.keys q)
        |> Seq.distinct
        |> Seq.map (fun key -> key, These.from (Map.tryFind key p, Map.tryFind key q))
        |> Map.ofSeq

    /// Does not throw if key not found
    let tryUpdate (key: 'k) (updater: 't -> 't) (table: Map<'k, 't>) : Map<'k, 't> =
        let elem = table |> Map.tryFind key
        match elem with
        | Some elem -> table |> Map.add key (updater elem)
        | None -> table

    let inline notContainsKey elt source = Map.containsKey elt source |> not

module Set =
    let inline notContains elt source = Set.contains elt source |> not

    let inline isNotEmpty source = Set.isEmpty source |> not

module Seq =
    let inline isNotEmpty source = Seq.isEmpty source |> not
    let inline notExists cond source = Seq.exists cond source |> not
    let inline notContains elt source = Seq.contains elt source |> not
    let inline isSingleton source = Seq.length source = 1

    let inline intersect ys xs =
        Set.intersect (Set.ofSeq ys) (Set.ofSeq xs) |> Set.toSeq

module Choice =
    let map f =
        function
        | Choice1Of2 v -> Choice1Of2(f v)
        | Choice2Of2 msg -> Choice2Of2 msg

    let mapSnd f =
        function
        | Choice1Of2 v -> Choice1Of2 v
        | Choice2Of2 v -> Choice2Of2(f v)

[<AutoOpen>]
module TypeExtensions =
    open System

    type DateTimeOffset with

        /// Just like AddDays, but saturates on min/max values instead of raising exception
        member x.AddDaysSafely(days: float) =
            if Math.Sign days > 0 then
                if x < DateTimeOffset.MaxValue.AddDays(-days) then
                    x.AddDays days
                else
                    DateTimeOffset.MaxValue
            else if x > DateTimeOffset.MinValue.AddDays(-days) then
                x.AddDays days
            else
                DateTimeOffset.MinValue

        /// Just like Add, but saturates on min/max values instead of raising exception
        member x.AddSafely(ts: TimeSpan) =
            if Math.Sign ts.Ticks > 0 then
                if x < DateTimeOffset.MaxValue.Add(-ts) then
                    x.Add ts
                else
                    DateTimeOffset.MaxValue
            else if x > DateTimeOffset.MinValue.Add(-ts) then
                x.Add ts
            else
                DateTimeOffset.MinValue

    type DateTime with

        /// Just like AddDays, but saturates on min/max values instead of raising exception
        member x.AddDaysSafely(days: float) =
            if Math.Sign days > 0 then
                if x < DateTime.MaxValue.AddDays(-days) then
                    x.AddDays days
                else
                    DateTime.MaxValue
            else if x > DateTime.MinValue.AddDays(-days) then
                x.AddDays days
            else
                DateTime.MinValue

        /// Just like Add, but saturates on min/max values instead of raising exception
        member x.AddSafely(ts: TimeSpan) =
            if Math.Sign ts.Ticks > 0 then
                if x < DateTime.MaxValue.Add(-ts) then
                    x.Add ts
                else
                    DateTime.MaxValue
            else if x > DateTime.MinValue.Add(-ts) then
                x.Add ts
            else
                DateTime.MinValue

    type TimeSpan with

        static member ToOption createTimeSpan (interval: int) =
            if interval > 0 then
                Some(createTimeSpan (float interval))
            else
                None

    type HttpStatusCode with

        member x.IsSuccess = int x >= 200 && int x < 300

// http://www.fssnip.net/2y/title/Functional-wrappers-for-TryParse-APIs
// see also
// http://stackoverflow.com/questions/4949941/convert-string-to-system-datetime-in-f
module TryParser =
    open System

    let private tryParseTimeInterval input =
        if String.IsNullOrEmpty input then
            None
        else
            match input.Substring(0, input.Length - 1) |> Int32.TryParse with
            | true, n ->
                match input[input.Length - 1] with
                | 'd' -> TimeSpan.FromDays(double n) |> Some
                | 'h' -> TimeSpan.FromHours(double n) |> Some
                | 'm' -> TimeSpan.FromMinutes(double n) |> Some
                | 's' -> TimeSpan.FromSeconds(double n) |> Some
                | _ -> None
            | _ -> None

    // convenient, functional TryParse wrappers returning option<'a>
    let tryParseWith tryParseFunc =
        tryParseFunc
        >> function
            | true, v -> Some v
            | false, _ -> None

    let parseDate: string -> DateTime option = (tryParseWith DateTime.TryParse)
    let parseInt: string -> int option = (tryParseWith Int32.TryParse)
    let parseSingle: string -> float32 option = (tryParseWith Single.TryParse)
    let parseDouble: string -> float option = (tryParseWith Double.TryParse)
    let parseByte: string -> byte option = (tryParseWith Byte.TryParse)
    let parseBool: string -> bool option = (tryParseWith Boolean.TryParse)
    let parseTimeSpan: string -> TimeSpan option = (tryParseWith TimeSpan.TryParse)

    let parseTimeIntervalWithSpecification: string -> TimeSpan option = tryParseTimeInterval

    // active patterns for try-parsing strings
    let (|Date|_|) = parseDate
    let (|TimeSpan|_|) = parseDate
    let (|Int|_|) = parseInt
    let (|Single|_|) = parseSingle
    let (|Double|_|) = parseDouble

module RegularExpressions =
    open System.Text.RegularExpressions

    /// http://www.fssnip.net/29/title/Regular-expression-active-pattern
    let (|Regex|_|) pattern input =
        let m = Regex.Match(input, pattern)
        if m.Success then
            Some(List.tail [ for g in m.Groups -> g.Value ])
        else
            None

[<AutoOpen>]
module ExceptionExtensions =
    open System
    // Exception helpers, extend nullArg, invalidArg and invalidOp.
    let inline notImpl text = raise <| NotImplementedException(text)
    let inline notSupported text = raise <| NotSupportedException(text)
    let inline badFormat text = raise <| FormatException(text)

[<AutoOpen>]
module ResultCE =

    let private ofOption error =
        function
        | Some s -> Ok s
        | None -> Error error

    type ResultBuilder() =
        member _.Return(x) = Ok x

        member _.ReturnFrom(m: Result<_, _>) = m

        member _.Bind(m, f) = Result.bind f m
        member _.Bind((m, error): Option<'T> * 'E, f) = m |> ofOption error |> Result.bind f

        member _.Zero() = None

        member _.Combine(m, f) = Result.bind f m

        member _.Delay(f: unit -> _) = f

        member _.Run(f) = f ()

        member __.TryWith(m, h) =
            try
                __.ReturnFrom(m)
            with e ->
                h e

        member __.TryFinally(m, compensation) =
            try
                __.ReturnFrom(m)
            finally
                compensation ()

        member __.Using(res: #System.IDisposable, body) =
            __.TryFinally(
                body res,
                fun () ->
                    match res with
                    | null -> ()
                    | disp -> disp.Dispose()
            )

        member __.While(guard, f) =
            if not (guard ()) then
                Ok()
            else
                do f () |> ignore
                __.While(guard, f)

        member __.For(sequence: seq<_>, body) =
            __.Using(sequence.GetEnumerator(), (fun enum -> __.While(enum.MoveNext, __.Delay(fun () -> body enum.Current))))

    let result = ResultBuilder()
