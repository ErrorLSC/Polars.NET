namespace Polars.NET.Linq.Backend.Polars

open Polars.NET.Core
open Polars.NET.Core.Wrappers
open Polars.NET.Core.Enums
open Polars.NET.Linq
open Polars.NET.Linq.IR

/// Polars interpreter for planned scalar terminals: lowers and collects LogicalPlans, then hands
/// the collected DataFrames to the IDataFrameMaterializer.
module internal PolarsScalarRunner =

    let private collect (plan: LogicalPlan) : DataFrameHandle =
        PolarsWrapper.LazyCollect(PolarsPhysicalLowerer.lowerPlan plan, PlEngine.Auto)

    let private runRowsOp (mat: IDataFrameMaterializer) (df: DataFrameHandle) (op: RowsOp) : ScalarOutcome =
        match op with
        | RowsOp.Count(sourceType, pred) ->
            let invoker = MaterializerInvokers.GetMaterializerCountInvoker sourceType
            ScalarOutcome.Value(box (System.Convert.ToInt64(invoker.Invoke(mat, df, pred))))

        | RowsOp.BoolPredicate(methodName, sourceType, pred) ->
            let invoker = MaterializerInvokers.GetMaterializerBoolPredicateInvoker(methodName, sourceType)
            ScalarOutcome.Value(box (invoker.Invoke(mat, df, pred)))

        | RowsOp.Any(sourceType, pred) ->
            let invoker = MaterializerInvokers.GetMaterializerAnyInvoker sourceType
            ScalarOutcome.Value(box (invoker.Invoke(mat, df, pred)))

        | RowsOp.RowFetch(kindName, isOrDefault, targetType, pred, defaultValue) ->
            if isOrDefault then
                let invoker = MaterializerInvokers.GetMaterializerRowFetchOrDefaultInvoker(kindName + "OrDefault", targetType)
                ScalarOutcome.Value(invoker.Invoke(mat, df, pred, defaultValue))
            else
                let invoker = MaterializerInvokers.GetMaterializerRowFetchInvoker(kindName, targetType)
                ScalarOutcome.Value(invoker.Invoke(mat, df, pred))

        | RowsOp.NumericFold(sourceType, accumType, methodName, selectorOpt) ->
            let height = PolarsWrapper.DataFrameHeight df
            if height = 0L then
                ScalarOutcome.NoRows
            else
                let seed, folder, finisher = LinqReflectionCache.CreateNumericFolder(sourceType, accumType, methodName, selectorOpt)
                let invoker = MaterializerInvokers.GetMaterializerAggregateInvoker(sourceType, accumType)
                let rawAcc = invoker.Invoke(mat, df, seed, folder)
                ScalarOutcome.Value(finisher rawAcc height)

        | RowsOp.ElementAt(targetType, index) ->
            if PolarsWrapper.DataFrameHeight df = 0L then
                ScalarOutcome.NoRows
            else
                let invoker = MaterializerInvokers.GetMaterializerElementAtInvoker targetType
                ScalarOutcome.Value(invoker.Invoke(mat, df, index))

        | RowsOp.Contains(elemType, item, comparer) ->
            let invoker = MaterializerInvokers.GetMaterializerContainsInvoker elemType
            ScalarOutcome.Value(box (invoker.Invoke(mat, df, item, comparer)))

        | RowsOp.FirstRow elemType ->
            if PolarsWrapper.DataFrameHeight df = 0L then
                ScalarOutcome.NoRows
            else
                let invoker = MaterializerInvokers.GetMaterializerFirstInvoker elemType
                ScalarOutcome.Value(invoker.Invoke(mat, df, null))

        | RowsOp.ExtremumBy(methodName, sourceType, keyType, keySelector, comparer) ->
            let invoker = MaterializerInvokers.GetMaterializerExtremumByInvoker(methodName, sourceType, keyType)
            ScalarOutcome.Value(invoker.Invoke(mat, df, keySelector, comparer))

        | RowsOp.SequenceEqualPlan(other, elemType, comparer) ->
            let otherDf = collect other
            let invoker = MaterializerInvokers.GetMaterializerSeqEqualDfInvoker elemType
            ScalarOutcome.Value(box (invoker.Invoke(mat, df, otherDf, comparer)))

        | RowsOp.SequenceEqualSeq(other, elemType, comparer) ->
            let invoker = MaterializerInvokers.GetMaterializerSeqEqualSeqInvoker elemType
            ScalarOutcome.Value(box (invoker.Invoke(mat, df, other, comparer)))

    /// Executes a planned scalar terminal against Polars and returns its raw outcome.
    let run (mat: IDataFrameMaterializer) (terminal: ScalarTerminal) : ScalarOutcome =
        match terminal with
        | ScalarTerminal.RowCount plan -> ScalarOutcome.Value(box (mat.MaterializeScalar<int64>(collect plan)))
        | ScalarTerminal.ScalarValue plan -> ScalarOutcome.Value(mat.MaterializeScalar<obj>(collect plan))
        | ScalarTerminal.HasRows plan -> ScalarOutcome.Value(box (PolarsWrapper.DataFrameHeight(collect plan) > 0L))
        | ScalarTerminal.FramesEqual(left, right) ->
            let leftDf = collect left
            let rightDf = collect right
            if PolarsWrapper.DataFrameHeight leftDf <> PolarsWrapper.DataFrameHeight rightDf then
                ScalarOutcome.Value(box false)
            else
                ScalarOutcome.Value(box (PolarsWrapper.DataFrameEquals(leftDf, rightDf, nullEqual = true)))
        | ScalarTerminal.Rows(plan, op) -> runRowsOp mat (collect plan) op
        | ScalarTerminal.Deferred compute -> ScalarOutcome.Value(compute ())
        | ScalarTerminal.Constant value -> ScalarOutcome.Value value

    /// Executes a planned scalar query and applies its managed post-processing.
    let execute<'T> (mat: IDataFrameMaterializer) (query: ScalarQuery<'T>) : 'T =
        query.Finish(run mat query.Terminal)
