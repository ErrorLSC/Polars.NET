namespace Polars.NET.Linq.Backend.Polars

open Polars.NET.Core.Wrappers
open Polars.NET.Core.Exceptions
open Kaminari.Samidare.IR

/// Polars implementation of the backend services consumed by the LINQ frontend during IR construction.
[<Sealed>]
type internal PolarsQueryBackend private () =
    static member val Instance: IQueryBackend = PolarsQueryBackend() :> IQueryBackend

    interface IQueryBackend with
        member _.CreatePushdownFailure(stage: string, expressionText: string, unsupportedFeature: string) =
            PolarsPushdownException(stage, expressionText, unsupportedFeature) :> exn

    interface IBackendCapabilities with
        member _.IsRegexPatternSupported(pattern: string) = PolarsWrapper.RegexIsValid pattern
        // The check runs as a managed callback, which DisallowUdfPushdown rules out.
        member _.SupportsAssertions = not Kaminari.Samidare.Config.SamidareConfig.DisallowUdfPushdown
        // A row-indexed query read twice is how a correlated EXISTS is decorrelated: one selective outer query, whose
        // filters do the work. Polars finds this repeat itself and still pushes the filters into the scans, which it
        // does not do beneath an explicit cache.
        member _.SharesRepeatedSubplanItself(subplan: LogicalPlan) =
            match subplan with
            | LogicalPlan.WithRowIndex _ -> true
            | _ -> false
        member _.IsEngineAssembly(assembly: System.Reflection.Assembly) =
            assembly = typeof<PolarsPushdownException>.Assembly
            || assembly = typeof<PolarsQueryBackend>.Assembly
            || (let name = assembly.GetName().Name in name = "Polars.CSharp" || name = "Polars.FSharp")

    interface ISourceBinder with
        member _.TryBindExternalSource(value: obj) = PolarsObjectConverter.tryBindExternalSource value
