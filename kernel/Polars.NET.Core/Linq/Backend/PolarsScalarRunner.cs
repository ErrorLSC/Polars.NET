using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Linq.Backend;

/// <summary>
/// Polars interpreter for planned scalar terminals: lowers and collects LogicalPlans, then hands the collected
/// DataFrames to the IDataFrameMaterializer.
/// </summary>
internal static class PolarsScalarRunner
{
    private static DataFrameHandle Collect(SourceBinding[] sources, LogicalPlan plan)
    {
        // A scalar plan wraps an optimized pipeline plan in its terminal (a row count, an aggregate), which may free the
        // pipeline's rows from their order. The terminal's expressions resolve against this backend's capabilities (a
        // checked sum asserts).
        PolarsRuntimeConfig.EnsureImported();
        var prepared = PlanOptimizer.PrepareScalarPlan(PolarsQueryBackend.Instance, sources, plan);
        return PolarsWrapper.LazyCollect(PolarsPhysicalLowerer.LowerPlan(sources, prepared), PlEngine.Auto);
    }

    private static ScalarOutcome Value(object? value) => ScalarOutcome.NewValue(value);

    private static ScalarOutcome RunRowsOp(IDataFrameMaterializer mat, SourceBinding[] sources, DataFrameHandle df, RowsOp op)
    {
        switch (op)
        {
            case RowsOp.Count count:
                return Value(Convert.ToInt64(MaterializerInvokers.GetMaterializerCountInvoker(count.sourceType).Invoke(mat, df, count.predicate)));
            case RowsOp.BoolPredicate p:
                return Value(MaterializerInvokers.GetMaterializerBoolPredicateInvoker(p.methodName, p.sourceType).Invoke(mat, df, p.predicate));
            case RowsOp.Any any:
                return Value(MaterializerInvokers.GetMaterializerAnyInvoker(any.sourceType).Invoke(mat, df, any.predicate));
            case RowsOp.RowFetch fetch:
                return fetch.isOrDefault
                    ? Value(MaterializerInvokers.GetMaterializerRowFetchOrDefaultInvoker(fetch.kindName + "OrDefault", fetch.targetType).Invoke(mat, df, fetch.predicate, fetch.defaultValue))
                    : Value(MaterializerInvokers.GetMaterializerRowFetchInvoker(fetch.kindName, fetch.targetType).Invoke(mat, df, fetch.predicate));
            case RowsOp.NumericFold fold:
            {
                long height = PolarsWrapper.DataFrameHeight(df);
                if (height == 0L) return ScalarOutcome.NoRows;
                var (seed, folder, finish) = QueryCompiler.CreateNumericFolder(fold.sourceType, fold.accumType, fold.methodName, fold.selector.OrNull());
                var raw = MaterializerInvokers.GetMaterializerAggregateInvoker(fold.sourceType, fold.accumType).Invoke(mat, df, seed, folder);
                return Value(finish(raw!, height));
            }
            case RowsOp.ElementAt elementAt:
                return PolarsWrapper.DataFrameHeight(df) == 0L
                    ? ScalarOutcome.NoRows
                    : Value(MaterializerInvokers.GetMaterializerElementAtInvoker(elementAt.targetType).Invoke(mat, df, elementAt.index));
            case RowsOp.Contains contains:
                return Value(MaterializerInvokers.GetMaterializerContainsInvoker(contains.elemType).Invoke(mat, df, contains.item, contains.comparer));
            case RowsOp.FirstRow first:
                return PolarsWrapper.DataFrameHeight(df) == 0L
                    ? ScalarOutcome.NoRows
                    : Value(MaterializerInvokers.GetMaterializerFirstInvoker(first.elemType).Invoke(mat, df, null));
            case RowsOp.ExtremumBy extremum:
                return Value(MaterializerInvokers.GetMaterializerExtremumByInvoker(extremum.methodName, extremum.sourceType, extremum.keyType)
                    .Invoke(mat, df, extremum.keySelector, extremum.comparer));
            case RowsOp.SequenceEqualPlan other:
                return Value(MaterializerInvokers.GetMaterializerSeqEqualDfInvoker(other.elemType).Invoke(mat, df, Collect(sources, other.other), other.comparer));
            case RowsOp.SequenceEqualSeq other:
                return Value(MaterializerInvokers.GetMaterializerSeqEqualSeqInvoker(other.elemType).Invoke(mat, df, other.other, other.comparer));
            default:
                throw new ArgumentOutOfRangeException(nameof(op));
        }
    }

    /// <summary>Executes a planned scalar terminal against Polars and returns its raw outcome.</summary>
    public static ScalarOutcome Run(IDataFrameMaterializer mat, SourceBinding[] sources, ScalarTerminal terminal)
    {
        switch (terminal)
        {
            case ScalarTerminal.RowCount t: return Value(mat.MaterializeScalar<long>(Collect(sources, t.plan)));
            case ScalarTerminal.ScalarValue t: return Value(mat.MaterializeScalar<object>(Collect(sources, t.plan)));
            case ScalarTerminal.HasRows t: return Value(PolarsWrapper.DataFrameHeight(Collect(sources, t.plan)) > 0L);
            case ScalarTerminal.FramesEqual t:
            {
                var left = Collect(sources, t.left);
                var right = Collect(sources, t.right);
                return PolarsWrapper.DataFrameHeight(left) != PolarsWrapper.DataFrameHeight(right)
                    ? Value(false)
                    : Value(PolarsWrapper.DataFrameEquals(left, right, nullEqual: true));
            }
            case ScalarTerminal.Rows t: return RunRowsOp(mat, sources, Collect(sources, t.plan), t.op);
            case ScalarTerminal.Deferred t: return Value(t.compute.Invoke(null!));
            case ScalarTerminal.Constant t: return Value(t.value);
            default: throw new ArgumentOutOfRangeException(nameof(terminal));
        }
    }

    /// <summary>Executes a planned scalar query and applies its managed post-processing.</summary>
    public static T Execute<T>(IDataFrameMaterializer mat, PlannedScalar<T> planned) =>
        planned.Query.Finish.Invoke(Run(mat, planned.Sources, planned.Query.Terminal));
}
