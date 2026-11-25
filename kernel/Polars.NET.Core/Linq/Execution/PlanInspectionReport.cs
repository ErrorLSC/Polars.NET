using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Microsoft.FSharp.Collections;

namespace Polars.NET.Core.Linq.Execution;

/// <summary>Assembling the PlanInspection report of a compiled query.</summary>
internal static class PlanInspectionReport
{
    /// <summary>
    /// The report for a query whose pipeline is <paramref name="pipeline"/>, as <paramref name="compiled"/> for
    /// materialization, whose native plan explains as <paramref name="nativePlan"/>. <paramref name="isGrouping"/> tells
    /// whether the query yields groups; <paramref name="fallbackBoundary"/> is the method at which LINQ-to-Objects takes
    /// over, for a query that needs the full fallback.
    /// </summary>
    public static PlanInspection Build(CompiledPipeline pipeline, CompiledLogicalQuery compiled, string nativePlan, bool isGrouping, string? fallbackBoundary)
    {
        var stages = pipeline.Stages;
        int? chunkSize = stages.OfType<LinqStage.Chunk>().Select(c => (int?)c.size).FirstOrDefault();
        bool shouldShuffle = stages.Any(s => s.IsShuffle);
        List<string> postOps = [];
        if (shouldShuffle) postOps.Add("[Eager Post-Processing]: Shuffle(fraction = 1.0, with_replacement = false)");
        if (isGrouping) postOps.Add("[Client Post-Processing]: Materialize to IEnumerable<IGrouping<TKey, TElement>>");
        if (chunkSize is { } size) postOps.Add($"[Client Post-Processing]: Chunk(size = {size}) -> Materialize as TSource[] batches");
        string planWithPostOps = postOps.Count == 0 ? nativePlan : nativePlan + "\n" + string.Join("\n", postOps);

        var filters = ClientResidualOps.filters(compiled.Residual);
        bool hasClientProjection =
            (compiled.Residual is ClientResidual.RowOps rowOps && rowOps.Ops.Any(op => op is ClientRowOp.Map))
            || chunkSize is not null || shouldShuffle || isGrouping;
        return new PlanInspection(
            planWithPostOps,
            IrOps.Format(compiled.Plan),
            !filters.IsEmpty,
            ListModule.OfSeq(filters.Select(f => f.Item2)),
            hasClientProjection,
            compiled.Residual.Equals(ClientResidual.LinqToObjects),
            fallbackBoundary.ToOption(),
            pipeline.FallbackDiagnostic,
            !pipeline.Udfs.IsEmpty,
            pipeline.Udfs);
    }
}
