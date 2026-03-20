namespace Polars.FSharp

open System
open System.Collections.Generic
open Polars.NET.Core

/// <summary>
/// Config for Polars.FSharp
/// </summary>
[<RequireQualifiedAccess>] 
module Config =

    /// Inject Environment var to Rust
    let set (key: string) (value: string) =
        PolarsWrapper.SetEnvVar(key, value)

    /// Inject Environment vars to Rust from KeyValuePair sequence
    let setFromKvp (variables: seq<KeyValuePair<string, string>>) =
        ArgumentNullException.ThrowIfNull(variables)
        for kvp in variables do
            PolarsWrapper.SetEnvVar(kvp.Key, kvp.Value)

    /// Inject Environment vars to Rust from a sequence of tuples
    let setMany (variables: seq<string * string>) =
        ArgumentNullException.ThrowIfNull(variables)
        for key, value in variables do
            PolarsWrapper.SetEnvVar(key, value)