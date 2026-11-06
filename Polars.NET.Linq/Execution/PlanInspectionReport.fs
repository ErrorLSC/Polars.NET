namespace Polars.NET.Linq

open Polars.NET.Linq.IR

/// Assembling the PlanInspection report of a compiled query.
module internal PlanInspectionReport =

    /// The report for a query whose pipeline is `pipeline`, as `compiled` for materialization, whose native plan
    /// explains as `nativePlan`. `isGrouping` tells whether the query yields groups; `fallbackBoundary` is the
    /// method at which LINQ-to-Objects takes over, for a query that needs the full fallback.
    let build
        (pipeline: CompiledPipeline)
        (compiled: CompiledLogicalQuery)
        (nativePlan: string)
        (isGrouping: bool)
        (fallbackBoundary: string option)
        : PlanInspection =
        let stages = pipeline.Stages

        let chunkSizeOpt = stages |> List.tryPick (function LinqStage.Chunk s -> Some s | _ -> None)
        let shouldShuffle = stages |> List.exists (function LinqStage.Shuffle -> true | _ -> false)
        let mutable postOps = []

        if shouldShuffle then
            postOps <-
                "[Eager Post-Processing]: Shuffle(fraction = 1.0, with_replacement = false)"
                :: postOps

        if isGrouping then
            postOps <-
                "[Client Post-Processing]: Materialize to IEnumerable<IGrouping<TKey, TElement>>"
                :: postOps

        match chunkSizeOpt with
        | Some size ->
            postOps <-
                sprintf "[Client Post-Processing]: Chunk(size = %d) -> Materialize as TSource[] batches" size
                :: postOps
        | None -> ()

        let planWithPostOps =
            if postOps.IsEmpty then
                nativePlan
            else
                nativePlan + "\n" + String.concat "\n" (List.rev postOps)

        let detectedUdfs = pipeline.Udfs
        let fallbackReason = pipeline.FallbackDiagnostic
        let isFullFallback = compiled.Residual = ClientResidual.LinqToObjects

        {
            NativePlan = planWithPostOps
            LogicalPlan = Some(LogicalPlanOps.formatPlan compiled.Plan)
            HasClientPredicates = not (ClientResidualOps.filters compiled.Residual).IsEmpty
            ClientPredicateExpressions = ClientResidualOps.filters compiled.Residual |> List.map snd
            HasClientProjection =
                (match compiled.Residual with
                 | ClientResidual.RowOps ops -> ops |> List.exists (function ClientRowOp.Map _ -> true | _ -> false)
                 | _ -> false)
                || chunkSizeOpt.IsSome || shouldShuffle || isGrouping
            IsFullFallback = isFullFallback
            FallbackBoundary = fallbackBoundary
            FallbackReason = fallbackReason
            HasUdf = not detectedUdfs.IsEmpty
            UdfExpressions = detectedUdfs
        }
