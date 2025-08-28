namespace Nrk.Oddjob.Core

open FSharpx.Collections

[<AutoOpen>]
module ContentCommands =

    type RepairGlobalConnectFileCommand = RepairGlobalConnectFileCommand of FileRef

    [<RequireQualifiedAccess>]
    type ContentCommand =
        | RepairMediaType
        | RepairSourceFiles
        | RepairGlobalConnectFile of RepairGlobalConnectFileCommand

open ContentCommands

type ContentActions = ContentCommand list

module ContentActions =

    let private buildRepairMediaTypeActions (ctx: ContentActionContext) (desiredState: DesiredMediaSetState) mediaSetId granittMediaType =

        if
            mediaSetId.ClientId = Alphanumeric PsClientId
            && ctx.Validation.ValidationMode |> List.contains MediaSetValidation.RemoteStorage
            && desiredState.MediaType <> granittMediaType
        then
            [ ContentCommand.RepairMediaType ]
        else
            List.empty

    let private buildRepairSourceFilesPathActions (desiredState: DesiredMediaSetState) mediaSetId =

        let hasOldPath (path: string) =
            System.Text.RegularExpressions.Regex.Match(path, @"^\\\\madata(\d)+\\").Success

        if mediaSetId.ClientId = Alphanumeric PsClientId && desiredState.Content <> ContentSet.Empty then
            desiredState.Content
            |> ContentSet.getFiles
            |> Map.values
            |> Seq.filter (fun file -> file.SourcePath |> Option.map (FilePath.value >> hasOldPath) |> Option.defaultValue false)
            |> Seq.isNotEmpty
            |> function
                | true -> [ ContentCommand.RepairSourceFiles ]
                | false -> List.empty
        else
            List.empty

    let private buildRepairGlobalConnectFileActions (ctx: ContentActionContext) (desiredState: DesiredMediaSetState) globalConnectFiles =

        let isFilePresentAtOrigin (fileSystem: IFileSystemInfo) (globalConnectFiles: GlobalConnectFilesStates) fileRef =
            try
                match globalConnectFiles |> Map.tryFind fileRef with
                | Some fileState -> fileSystem.FileExists fileState.File.RemotePath.Value
                | _ -> false
            with _ ->
                false

        let shouldRepairFile fileRef (originFiles: GlobalConnectFilesStates) =
            let fileSystemInfo = ctx.Environment.GlobalConnectFileSystemResolver()
            match originFiles |> Map.tryFind fileRef with
            | Some file -> file.RemoteState.State <> DistributionState.Completed && (isFilePresentAtOrigin fileSystemInfo originFiles fileRef)
            | None -> false

        if ctx.Validation.ValidationMode |> List.contains MediaSetValidation.RemoteStorage then
            desiredState.Content
            |> ContentSet.getFiles
            |> Map.keys
            |> Seq.filter (fun fileRef -> shouldRepairFile fileRef globalConnectFiles)
            |> Seq.map (fun fileRef -> ContentCommand.RepairGlobalConnectFile(RepairGlobalConnectFileCommand fileRef))
            |> Seq.toList
        else
            List.empty

    let forMediaSet (ctx: ContentActionContext) mediaSetId globalConnectFiles (desiredState: DesiredMediaSetState) =
        let contentActions =
            if mediaSetId.ClientId = Alphanumeric PsClientId then
                let mediaType = ctx.Environment.GetGranittMediaType mediaSetId

                let mediaTypeActions = buildRepairMediaTypeActions ctx desiredState mediaSetId mediaType
                let sourceFilesActions = buildRepairSourceFilesPathActions desiredState mediaSetId
                List.concat [ mediaTypeActions; sourceFilesActions ]
            else
                []
        let globalConnectFileActions = buildRepairGlobalConnectFileActions ctx desiredState globalConnectFiles
        List.concat [ contentActions; globalConnectFileActions ]
