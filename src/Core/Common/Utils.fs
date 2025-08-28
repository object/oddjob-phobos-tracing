namespace Nrk.Oddjob.Core

open System
open System.IO
open System.Net

[<AutoOpen>]
module Utils =

    open Microsoft.FSharp.Reflection
    open Newtonsoft.Json.Linq
    open Akkling

    let getUnionCaseNames<'a> () =
        FSharpType.GetUnionCases(typeof<'a>) |> Array.map _.Name

    let getUnionCaseName (x: 'a) =
        match FSharpValue.GetUnionFields(x, typeof<'a>, true) with
        | case, _ -> case.Name

    let tryParseUnionCaseName<'a> ignoreCase (s: string) =
        let stringComparison =
            if ignoreCase then
                StringComparison.InvariantCultureIgnoreCase
            else
                StringComparison.InvariantCulture
        match FSharpType.GetUnionCases typeof<'a> |> Array.filter (fun case -> String.Equals(case.Name, s, stringComparison)) with
        | [| case |] -> Some(FSharpValue.MakeUnion(case, [||], true) :?> 'a)
        | _ -> None

    let parseUnionCaseName<'a> (s: string) =
        tryParseUnionCaseName<'a> false s |> Option.getOrFail (sprintf "Unable to parse %s as %A" s <| typeof<'a>)

    let normalizeGuid (guid: Guid) = guid.ToString("N").ToLower()

    let rec parseKeyValueString (text: string) (itemSeparator: char) (stringDelimiter: char) acc =
        let rec findNextKeyValuePos text itemSeparator stringDelimiter currentPos quoted =
            match text with
            | "" -> currentPos
            | text when currentPos >= text.Length -> -1
            | text ->
                match text[currentPos] with
                | x when x = itemSeparator && not quoted -> currentPos + 1
                | x when x = itemSeparator && quoted -> findNextKeyValuePos text itemSeparator stringDelimiter (currentPos + 1) quoted
                | x when x = stringDelimiter && quoted -> currentPos + 2
                | x when x = stringDelimiter && not quoted -> findNextKeyValuePos text itemSeparator stringDelimiter (currentPos + 1) true
                | _ -> findNextKeyValuePos text itemSeparator stringDelimiter (currentPos + 1) quoted

        let extractKeyValue text (itemSeparator: char) (stringDelimiter: char) =
            let valuePos = findNextKeyValuePos text itemSeparator stringDelimiter 0 false
            if valuePos <= 0 || valuePos >= text.Length then
                (text, "")
            else
                (text.Substring(0, valuePos - 1), text.Substring(valuePos))

        let kv, rest = extractKeyValue text itemSeparator stringDelimiter
        match (kv, rest) with
        | "", "" -> acc
        | item, text ->
            let pos = item.IndexOf('=')
            let key, value = item.Substring(0, pos), item.Substring(pos + 1, item.Length - pos - 1).Trim(stringDelimiter)
            (key, value) :: parseKeyValueString text itemSeparator stringDelimiter acc

    /// http://www.fssnip.net/29/title/Regular-expression-active-pattern
    let (|Regex|_|) pattern input =
        let m = System.Text.RegularExpressions.Regex.Match(input, pattern)
        if m.Success then
            Some(List.tail [ for g in m.Groups -> g.Value ])
        else
            None

    let inline convertOrFail tryConvert value =
        value |> tryConvert |> Option.getOrFail $"Unsupported value %A{value}"

    let inline convertOrDefault tryConvert (value: 'a option) : 'b =
        match value with
        | Some value -> value |> convertOrFail tryConvert
        | None -> Unchecked.defaultof<'b>

    let inline convertOrNone tryConvert value = value |> tryConvert

    let inline tryConvertFromDomainValue mapping domainValue =
        mapping |> List.tryFind (fun (_, y) -> y = domainValue) |> Option.map fst

    let inline tryConvertToDomainValue mapping dtoValue =
        mapping |> List.tryFind (fun (x, _) -> x = dtoValue) |> Option.map snd

    let getAssemblyName () =
        Reflection.Assembly.GetEntryAssembly().GetName().Name

    module IO =
        let getFileLengthSafely path =
            try
                FileInfo(path).Length |> uint64
            with _ ->
                0UL

        let getLastModifiedTime path =
            let fi = FileInfo(path)
            fi.Refresh()
            if fi.CreationTimeUtc > fi.LastWriteTimeUtc then
                fi.CreationTimeUtc
            else
                fi.LastWriteTimeUtc
            |> DateTimeOffset

        let getCreationTime path =
            let fi = FileInfo(path)
            fi.Refresh()
            fi.CreationTimeUtc |> DateTimeOffset

        let getFileContent (path: string) =
            let ext = Path.GetExtension path
            if ext = ".smil" || ext = ".txt" then
                File.ReadAllText path
            else
                "<binary>"

        let getDirectoryContent (path: string) =
            [| Directory.GetDirectories(path); Directory.GetFiles(path) |] |> Array.concat |> Array.toList

        [<Literal>]
        let WindowsDirectorySeparatorChar = '\\'

        [<Literal>]
        let LinuxDirectorySeparatorChar = '/'

        [<Literal>]
        let ServerPreface = @"\\"

        let getFileName (path: string) =
            // For getting file name of Windows path on Linux host
            if not (OperatingSystem.IsWindows()) then
                if path.StartsWith(ServerPreface) then
                    path.Split(WindowsDirectorySeparatorChar) |> Array.last
                else if path.Length > 3 && Char.IsLetter path[0] && path.Substring(1).StartsWith ":\\" then
                    path.Split(WindowsDirectorySeparatorChar) |> Array.last
                else
                    IO.Path.GetFileName(path)
            else
                IO.Path.GetFileName(path)

        let getFileNameWithoutExtension (path: string) =
            IO.Path.GetFileNameWithoutExtension(getFileName path)

    module Base64 =
        let encode (plain: string) =
            System.Text.Encoding.UTF8.GetBytes plain |> Convert.ToBase64String

        let decode (base64: string) =
            Convert.FromBase64String base64 |> System.Text.Encoding.UTF8.GetString

    type Disposable<'t>(wrapped: 't, disp: unit -> unit) =
        static member Create<'a>(wrapped, [<ParamArray>] arr: IDisposable array) =
            new Disposable<'a>(
                wrapped,
                fun () ->
                    for a in arr do
                        a.Dispose()
            )
        member this.Value = wrapped
        interface IDisposable with
            member this.Dispose() = disp ()

    let getLocalHostIP () =
        if Environment.GetEnvironmentVariable("DOTNET_RUNNING_IN_CONTAINER") = "true" then
            let host = Dns.GetHostEntry(Dns.GetHostName())
            let address =
                host.AddressList
                |> Array.filter (fun x -> not (IPAddress.IsLoopback(x)))
                |> Array.find (fun x -> x.AddressFamily = System.Net.Sockets.AddressFamily.InterNetwork)
            address.ToString()
        else
            let internalAddresses =
                [
                    ("10.0.0.0", "10.255.255.255")
                    ("172.16.0.0", "172.31.255.255")
                    ("192.168.0.0", "192.168.255.255")
                ]
            let inRange (address: IPAddress) (range: string * string) =
                address.ToString().Split('.') |> Seq.head = ((fst range).Split('.') |> Seq.head)
            let inRanges (address: IPAddress) (ranges: (string * string) list) = ranges |> List.exists (inRange address)
            let host = Dns.GetHostEntry(Dns.GetHostName())
            let localHost =
                host.AddressList
                |> Array.filter (fun x -> x.AddressFamily = Sockets.AddressFamily.InterNetwork)
                |> Array.filter (fun x -> not (inRanges x internalAddresses))
                |> Seq.tryHead
            match localHost with
            | Some localHost -> localHost.ToString()
            | None -> failwith "Could not get local host ip address"
