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
        // A row-indexed query read twice is how a correlated EXISTS is decorrelated: one selective outer query, whose
        // filters do the work. Polars finds this repeat itself and still pushes the filters into the scans, which it
        // does not do beneath an explicit cache.
        member _.SharesRepeatedSubplanItself(subplan: LogicalPlan) =
            match subplan with
            | LogicalPlan.WithRowIndex _ -> true
            | _ -> false

    interface ISourceBinder with
        member _.TryBindExternalSource(value: obj) = PolarsObjectConverter.tryBindExternalSource value
