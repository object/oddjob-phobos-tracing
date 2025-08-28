namespace Nrk.Oddjob.WebApi

open Akka.Event
open Nrk.Oddjob.Core
open Nrk.Oddjob.Core.Config
open Nrk.Oddjob.Core.S3.S3Api
open Nrk.Oddjob.Core.S3.S3Types
open HealthCheckTypes

module S3Health =

    let getS3Health amazonS3ConnectionString (oddjobConfig: OddjobConfig) (_: ILoggingAdapter) =
        let s3AccessKeys = amazonS3ConnectionString |> parseConnectionString
        let s3Api = new S3Api(oddjobConfig.S3, s3AccessKeys)
        let s3Client = S3Client(s3Api, LocalFileSystemInfo()) :> IS3Client

        let s3HealthPath = "health/health.txt"
        let result = s3Client.UploadContent s3HealthPath "It Works!"
        match result with
        | Result.Ok _ ->
            let deleteResult = s3Client.DeleteFile s3HealthPath
            match deleteResult with
            | Result.Ok _ -> HealthCheckResult.Ok("", [])
            | Result.Error errorValue -> HealthCheckResult.Error("S3Health", errorValue.Message, [])
        | Result.Error errorValue -> HealthCheckResult.Error("S3Health", errorValue.Message, [])
