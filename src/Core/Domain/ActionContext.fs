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

    type ContentEnvironment =
        {
            GetGranittMediaType: MediaSetId -> MediaType
            GlobalConnectFileSystemResolver: unit -> IFileSystemInfo
        }

    type ContentActionContext =
        {
            Validation: ValidationSettings
            Environment: ContentEnvironment
            Logger: ILoggingAdapter
        }

    type ActionEnvironment =
        {
            GlobalConnect: GlobalConnectEnvironment
            Content: ContentEnvironment
        }

    module GlobalConnectActionContext =
        let create logger validationSettings globalConnectEnvironment =
            {
                GlobalConnectActionContext.Validation = validationSettings
                Environment = globalConnectEnvironment
                Logger = logger
            }

    module ContentActionContext =
        let create logger validationSettings contentEnvironment =
            {
                ContentActionContext.Validation = validationSettings
                Environment = contentEnvironment
                Logger = logger
            }
