namespace Nrk.Oddjob.Core

open Nrk.Oddjob.Core.Config

type FilePath = FilePath of string

[<RequireQualifiedAccess>]
module FilePath =

    open System.IO
    open System.Runtime.InteropServices

    let tryCreate path =
        if String.isNotNullOrEmpty path then
            path |> FilePath |> Some
        else
            None

    let private tryFindMapping (mappings: OddjobConfig.PathMapping list) (absolutePath: string) =
        mappings |> List.tryFind (fun mapping -> absolutePath.Contains(mapping.From))

    let private tryFindReverseMapping (mappings: OddjobConfig.PathMapping list) (absolutePath: string) =
        mappings |> List.tryFind (fun mapping -> absolutePath.Contains(mapping.To))

    let private mapRelativePath (mappedRoot: string) (absolutePath: string) =
        let relativePath = absolutePath.Split mappedRoot |> Array.last
        relativePath.Replace(IO.WindowsDirectorySeparatorChar, IO.LinuxDirectorySeparatorChar)

    let private reverseMapRelativePath (mappedRoot: string) (absolutePath: string) =
        let relativePath = absolutePath.Split mappedRoot |> Array.last
        relativePath.Replace(IO.LinuxDirectorySeparatorChar, IO.WindowsDirectorySeparatorChar)

    let tryMap (mappings: OddjobConfig.PathMapping list) (FilePath path) =
        if RuntimeInformation.IsOSPlatform(OSPlatform.Linux) && path.StartsWith @"\\" then
            tryFindMapping mappings path |> Option.map (fun mapping -> Path.Combine(mapping.To, mapRelativePath mapping.From path))
        else
            Some path

    let private combineWindowsPathOnLinuxHost (mapping: OddjobConfig.PathMapping) (relativePath: string) =
        let separator = $"%c{IO.WindowsDirectorySeparatorChar}"
        let rootPath = mapping.From.TrimEnd(IO.WindowsDirectorySeparatorChar)
        let relativePath = relativePath.TrimStart(IO.WindowsDirectorySeparatorChar)
        rootPath + separator + relativePath

    let tryReverseMap (mappings: OddjobConfig.PathMapping list) (FilePath path) =
        if RuntimeInformation.IsOSPlatform(OSPlatform.Linux) then
            tryFindReverseMapping mappings path
            |> Option.map (fun mapping ->
                let relativePath = reverseMapRelativePath mapping.To path
                combineWindowsPathOnLinuxHost mapping relativePath)
        else
            Some path

    let value (FilePath path) = path

    let mapSourceRoots (mappings: OddjobConfig.PathMapping list) (sourceRoots: string list) =
        sourceRoots |> List.map tryCreate |> List.choose id |> List.map (tryMap mappings) |> List.choose id

    let reverseMapSourceRoots (mappings: OddjobConfig.PathMapping list) (sourceRoots: string list) =
        sourceRoots |> List.map tryCreate |> List.choose id |> List.map (tryReverseMap mappings) |> List.choose id
