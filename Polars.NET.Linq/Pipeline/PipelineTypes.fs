namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open Polars.NET.Linq.IR

/// Row selection strategy for scalar single-row operations
type internal RowFetchKind =
    | First
    | Last
    | Single

/// Internal interface allowing direct zero-reflection plan querying between Provider and Query instances.
/// Returns the compiled, optimized query, whether client-side predicates / fallback apply, and the client projection.
type internal IInternalPlanSource =
    abstract member GetLogicalPlanInfo: unit -> CompiledLogicalQuery * bool * (Type * Func<obj, obj>) option

/// Internal normalized context capturing all GroupBy variants (C# 4-args, F# pipeline, Having, Sorts)
type internal GroupFoldContext =
    {
        KeyLambda: LambdaExpression
        ElemLambdaOpt: LambdaExpression option
        HavingPreds: LambdaExpression list
        SortStages: (LambdaExpression * bool * bool) list
        /// Bodies of the transparent `let`s bound before the first having filter. A let is evaluated where it
        /// appears, so one of these that depends on other groups (a window, a rank) sees the unfiltered groups.
        PreFilterLets: Expression list
    }

/// Strongly-typed IGrouping implementation for in-memory GroupJoin materialization.
type internal MaterializedGrouping<'TKey, 'TElement>(key: 'TKey, elements: 'TElement array) =
    // 1. Explicit base interface implementations resolving FS0363 interface diamond collision
    interface IEnumerable<'TElement> with
        member _.GetEnumerator() : IEnumerator<'TElement> =
            (elements :> IEnumerable<'TElement>).GetEnumerator()

    interface IEnumerable with
        member _.GetEnumerator() : IEnumerator = elements.GetEnumerator()

    // 2. IGrouping specific implementation
    interface IGrouping<'TKey, 'TElement> with
        member _.Key = key

    // 3. Collection interfaces for O(1) counting and fast BCL compatibility
    interface IReadOnlyCollection<'TElement> with
        member _.Count = elements.Length

    interface ICollection with
        member _.Count = elements.Length
        member _.IsSynchronized = false
        member _.SyncRoot = elements.SyncRoot
        member _.CopyTo(array, index) = elements.CopyTo(array, index)

/// Represents the diagnostic inspection result of a Polars LINQ query pipeline.
type PlanInspection =
    {
        NativePlan: string
        LogicalPlan: string option
        HasClientPredicates: bool
        ClientPredicateExpressions: LambdaExpression list
        HasClientProjection: bool
        IsFullFallback: bool
        /// Under full fallback, the LINQ method at which LINQ-to-Objects takes over from a native prefix;
        /// None when no prefix of the pipeline can run natively.
        FallbackBoundary: string option
        FallbackReason: string option
        HasUdf: bool
        UdfExpressions: string list
    }

    /// Formats the inspection result into a human-readable diagnostic report.
    member this.FormatReport() : string =
        let sb = System.Text.StringBuilder()

        if this.IsFullFallback then
            sb.AppendLine "[Polars.NET Query Plan - Full Fallback to In-Memory LINQ]"
            |> ignore

            sb.AppendLine "---------------------------------------------------------"
            |> ignore

            sb.AppendLine "Warning: The query contains unsupported expressions or transparent identifiers."
            |> ignore

            match this.FallbackReason with
            | Some reason ->
                sb.AppendLine(sprintf "   Diagnostic reason: %s" reason) |> ignore
            | None -> ()

            match this.FallbackBoundary with
            | Some boundary ->
                sb.AppendLine(sprintf "   The pipeline runs natively up to '%s'; from there on it is executed in managed memory via LINQ-to-Objects." boundary)
                |> ignore
            | None ->
                sb.AppendLine "   The entire pipeline will be executed in managed memory via LINQ-to-Objects."
                |> ignore
        elif not this.HasClientPredicates && not this.HasClientProjection then
            if this.HasUdf then
                sb.AppendLine "[Polars.NET Query Plan - 100% Native Pushdown (with C# UDF Callback)]"
                |> ignore

                sb.AppendLine "---------------------------------------------------------------------"
                |> ignore

                sb.AppendLine "Note: Native plan delegates execution to managed C# UDF callback via Arrow batch interop."
                |> ignore

                if not this.UdfExpressions.IsEmpty then
                    sb.AppendLine " Pushed-down C# UDF Callbacks:" |> ignore
                    for udf in this.UdfExpressions do
                        sb.AppendFormat("     • {0}\n", udf) |> ignore

                sb.AppendLine this.NativePlan |> ignore
            else
                sb.AppendLine "[Polars.NET Query Plan - 100% Native Pushdown]" |> ignore
                sb.AppendLine "---------------------------------------------" |> ignore
                sb.AppendLine this.NativePlan |> ignore
        else
            sb.AppendLine "[Polars.NET Query Plan - Hybrid Execution (Partial Pushdown)]"
            |> ignore

            sb.AppendLine "------------------------------------------------------------"
            |> ignore

            sb.AppendLine "Phase 1: Native Polars Pushdown Plan:" |> ignore
            sb.AppendLine this.NativePlan |> ignore

            if this.HasUdf then
                sb.AppendLine " Native C# UDF Callbacks (Executed inside Polars):" |> ignore
                for udf in this.UdfExpressions do
                    sb.AppendFormat("     • {0}\n", udf) |> ignore

            sb.AppendLine "Phase 2: Managed Client Fallback:" |> ignore

            if this.HasClientPredicates then
                sb.AppendLine " Client-side Predicates (Evaluated in .NET memory):" |> ignore

                for pred in this.ClientPredicateExpressions do
                    sb.AppendFormat("     • {0}\n", pred.ToString()) |> ignore

            if this.HasClientProjection then
                sb.AppendLine " Client-side Projection: Expression could not be translated natively."
                |> ignore
                match this.FallbackReason with
                | Some r -> sb.AppendLine(sprintf "   Reason: %s" r) |> ignore
                | None -> ()

        sb.ToString().TrimEnd()

/// Public interface implemented by PolarsQueryable to expose query plan inspection.
type IPolarsExplainable =
    abstract member InspectPlan: optimized: bool -> PlanInspection
    abstract member Explain: optimized: bool -> string
    abstract member ExplainLogicalPlan: unit -> string
    abstract member GetLogicalPlan: unit -> LogicalPlan

[<RequireQualifiedAccess>]
type internal GroupingKind =
    | Rollup
    | Cube
    | GroupingSets of Expression


