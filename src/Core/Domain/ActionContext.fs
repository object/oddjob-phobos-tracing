namespace Nrk.Oddjob.Core

open Akka.Event
open Nrk.Oddjob.Core.Config

[<AutoOpen>]
module ActionContext =

    open System

    type ValidationSettings =
        {
            ValidationMode: MediaSetValidation list
            OverwriteMode: OverwriteMode
            StorageCleanupDelay: TimeSpan
        }

    type GlobalConnectEnvironment =
        {
            LocalFileSystem: IFileSystemInfo
            PathMappings: OddjobConfig.PathMapping list
            RemoteFilePathResolver: string -> string
            RemoteFileSystemResolver: unit -> IFileSystemInfo
        }

    type GlobalConnectActionContext =
        {
            Validation: ValidationSettings
            Environment: GlobalConnectEnvironment
            Logger: ILoggingAdapter
        }

    type ActionEnvironment =
        {
            GlobalConnect: GlobalConnectEnvironment
        }

    module GlobalConnectActionContext =
        let create logger validationSettings globalConnectEnvironment =
            {
                GlobalConnectActionContext.Validation = validationSettings
                Environment = globalConnectEnvironment
                Logger = logger
            }
