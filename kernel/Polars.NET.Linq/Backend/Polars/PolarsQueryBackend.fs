namespace Polars.NET.Linq.Backend.Polars

open Polars.NET.Core.Wrappers
open Polars.NET.Linq.IR

/// Polars implementation of the backend services consumed by the LINQ frontend during IR construction.
[<Sealed>]
type internal PolarsQueryBackend private () =
    static member val Instance: IQueryBackend = PolarsQueryBackend() :> IQueryBackend

    interface IQueryBackend

    interface IBackendCapabilities with
        member _.IsRegexPatternSupported(pattern: string) = PolarsWrapper.RegexIsValid pattern
        // The check runs as a managed callback, which DisallowUdfPushdown rules out.
        member _.SupportsAssertions = not Polars.NET.Core.Config.CoreConfig.DisallowUdfPushdown

    interface ISourceBinder with
        member _.TryBindExternalSource(value: obj) = PolarsObjectConverter.tryBindExternalSource value
