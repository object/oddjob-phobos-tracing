namespace Nrk.Oddjob.Core

open System
open System.Data.Common
open System.Dynamic
open System.Collections.Generic
open Dapper

[<AutoOpen>]
module DapperExtensions =

    let inline private createExpando (param: Map<string, 'a>) =
        let expando = ExpandoObject()
        let expandoDictionary = expando :> IDictionary<string, obj>
        for paramValue in param do
            expandoDictionary.Add(paramValue.Key, paramValue.Value :> obj)
        expando

    // Based on https://gist.github.com/vbfox/1e9f42f6dcdd9efd6660
    type DbConnection with

        member this.QueryMap
            (sql: string, param: Map<string, _>, ?transaction: Data.IDbTransaction, ?buffered: bool, ?commandTimeout: int, ?commandType: Data.CommandType)
            : IEnumerable<'result> =

            let expando = createExpando param
            let transaction = Option.toObj transaction
            let buffered = buffered |> Option.defaultValue true
            let commandTimeout = Option.toNullable commandTimeout
            let commandType = Option.toNullable commandType
            this.Query<'result>(sql, expando, transaction, buffered, commandTimeout, commandType)

        member this.QueryMapAsync
            (sql: string, param: Map<string, _>, ?transaction: Data.IDbTransaction, ?buffered: bool, ?commandTimeout: int, ?commandType: Data.CommandType)
            : Async<IEnumerable<'result>> =

            let expando = createExpando param
            let transaction = Option.toObj transaction
            let commandTimeout = Option.toNullable commandTimeout
            let commandType = Option.toNullable commandType
            this.QueryAsync<'result>(sql, expando, transaction, commandTimeout, commandType) |> Async.AwaitTask

        member this.QuerySingleOrDefaultMap
            (sql: string, param: Map<string, _>, ?transaction: Data.IDbTransaction, ?commandTimeout: int, ?commandType: Data.CommandType)
            : 'result =
            let expando = createExpando param
            let transaction = Option.toObj transaction
            let commandTimeout = Option.toNullable commandTimeout
            let commandType = Option.toNullable commandType
            this.QuerySingleOrDefault<'result>(sql, expando, transaction, commandTimeout, commandType)

        member this.QueryFirstOrDefaultMap
            (sql: string, param: Map<string, _>, ?transaction: Data.IDbTransaction, ?commandTimeout: int, ?commandType: Data.CommandType)
            : 'result =
            let expando = createExpando param
            let transaction = Option.toObj transaction
            let commandTimeout = Option.toNullable commandTimeout
            let commandType = Option.toNullable commandType
            this.QueryFirstOrDefault<'result>(sql, expando, transaction, commandTimeout, commandType)

        member this.ExecuteMap
            (sql: string, param: Map<string, _>, ?transaction: Data.IDbTransaction, ?commandTimeout: int, ?commandType: Data.CommandType)
            : int =

            let expando = createExpando param
            let transaction = Option.toObj transaction
            let commandTimeout = Option.toNullable commandTimeout
            let commandType = Option.toNullable commandType
            this.Execute(sql, expando, transaction, commandTimeout, commandType)

        member this.ExecuteMapAsync
            (sql: string, param: Map<string, _>, ?transaction: Data.IDbTransaction, ?commandTimeout: int, ?commandType: Data.CommandType)
            : Async<int> =

            let expando = createExpando param
            let transaction = Option.toObj transaction
            let commandTimeout = Option.toNullable commandTimeout
            let commandType = Option.toNullable commandType
            this.ExecuteAsync(sql, expando, transaction, commandTimeout, commandType) |> Async.AwaitTask

        member this.ExecuteReader
            (sql: string, param: Map<string, _>, ?transaction: Data.IDbTransaction, ?commandTimeout: int, ?commandType: Data.CommandType)
            : Data.IDataReader =

            let expando = createExpando param
            let transaction = Option.toObj transaction
            let commandTimeout = Option.toNullable commandTimeout
            let commandType = Option.toNullable commandType
            this.ExecuteReader(sql, expando, transaction, commandTimeout, commandType)

        member this.ExecuteReaderAsync
            (sql: string, param: Map<string, _>, ?transaction: Data.IDbTransaction, ?commandTimeout: int, ?commandType: Data.CommandType)
            =

            let expando = createExpando param
            let transaction = Option.toObj transaction
            let commandTimeout = Option.toNullable commandTimeout
            let commandType = Option.toNullable commandType
            this.ExecuteReaderAsync(sql, expando, transaction, commandTimeout, commandType) |> Async.AwaitTask
