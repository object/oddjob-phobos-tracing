namespace Nrk.Oddjob.Core

open System
open System.Net
open FsHttp

[<RequireQualifiedAccess>]
module Http =
    let lastModifiedTime url =
        let lastModifiedHeader = "Last-Modified"
        let response = http { HEAD url } |> Request.send
        if response.statusCode = HttpStatusCode.OK then
            let headers = response.originalHttpResponseMessage.Content.Headers
            if headers.Contains lastModifiedHeader then
                headers.GetValues lastModifiedHeader |> Seq.head |> DateTime.Parse |> Some
            else
                None
        else
            None

    let BasicAuthorization (usr, pwd) =
        "Basic " + Convert.ToBase64String(System.Text.ASCIIEncoding.ASCII.GetBytes($"{usr}:{pwd}"))

[<RequireQualifiedAccess>]
module Response =
    let toResult (response: Response) =
        if response.statusCode = HttpStatusCode.OK then
            Result.Ok response
        else
            Result.Error(int response.statusCode, response.reasonPhrase)
