// The LINQ provider builds generic code at run time (groups and rows of element types known only then): its public
// entry points carry RequiresDynamicCode / RequiresUnreferencedCode, so the analyzer's warnings inside it are silenced
// here.
#pragma warning disable IL2026, IL2060, IL2067, IL2070, IL2072, IL2075, IL2080, IL2087, IL2090, IL2091, IL3050, IL3051

using System.Collections;
using System.Linq.Expressions;
using System.Runtime.CompilerServices;
using Kaminari.Samidare;
using Kaminari.Samidare.IR;
using Microsoft.FSharp.Collections;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Linq.Backend;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Linq.Execution;

/// <summary>
/// Turning a collected native plan into the query's elements: packing join scopes before lowering, then materializing
/// rows, groups, chunks or AggregateBy pairs and applying the client residual.
/// </summary>
internal static class RowMaterialization
{
    /// <summary>Applies eager row shuffling (sample_frac = 1.0, without replacement) to a DataFrame.</summary>
    public static DataFrameHandle ApplyShuffle(DataFrameHandle df) =>
        PolarsWrapper.SampleFrac(df, PolarsWrapper.SeriesNew("", [1.0]), false, true, null);

    /// <summary>Collects the native plan, shuffling the rows when the pipeline asks for it.</summary>
    public static DataFrameHandle Collect(LazyFrameHandle nativeLf, FSharpList<LinqStage> stages)
    {
        var collected = PolarsWrapper.LazyCollect(nativeLf, PlEngine.Auto);
        return stages.Any(s => s.IsShuffle) ? ApplyShuffle(collected) : collected;
    }

    /// <summary>
    /// The type the rows of the native plan are materialized as, for a query of elements <paramref name="targetType"/>:
    /// the grouped element when the query yields groups, the source of AggregateBy, the element of Chunk's arrays,
    /// otherwise the query's element type.
    /// </summary>
    public static Type MaterializedRowType(Type targetType, FSharpList<LinqStage> stages)
    {
        bool isGrouping = QueryCompiler.IsGroupingType(targetType);
        foreach (var stage in stages)
        {
            switch (stage)
            {
                case LinqStage.GroupByKey k when isGrouping: return k.Item.Parameters[0].Type;
                case LinqStage.GroupByWithResult k when isGrouping: return k.Item1.Parameters[0].Type;
                case LinqStage.GroupByWithElementAndResult k when isGrouping: return k.keySelector.Parameters[0].Type;
                case LinqStage.AggregateBy a: return a.keySelector.Parameters[0].Type;
                case LinqStage.Chunk when targetType.IsArray: return targetType.GetElementType()!;
            }
        }
        return targetType;
    }

    /// <summary>
    /// Packs the entities of the join scopes still in effect over the plan's rows into struct columns, so the rows
    /// materialize as their element type; escalates to LINQ-to-Objects when an entity has no columns.
    /// </summary>
    public static CompiledLogicalQuery ForMaterialization(Type targetType, FSharpList<LinqStage> stages, CompiledLogicalQuery compiled)
    {
        if (compiled.RowShape.IsEmpty) return compiled;
        return QueryCompiler.PackRow(compiled.RowShape, compiled.Plan, MaterializedRowType(targetType, stages)) switch
        {
            RowPacking.Packed packed => new CompiledLogicalQuery(packed.Item, compiled.Sources, compiled.Residual, RowShape.Empty),
            { IsUnresolvable: true } => new CompiledLogicalQuery(compiled.Plan, compiled.Sources, ClientResidual.LinqToObjects, compiled.RowShape),
            _ => compiled,
        };
    }

    /// <summary>Materializes flat native rows and applies the residual row operations in pipeline order.</summary>
    public static IEnumerable<T> MaterializeFlatRows<T>(IDataFrameMaterializer materializer, DataFrameHandle df, ClientResidual residual)
    {
        if (residual is ClientResidual.RowOps rowOps)
        {
            var rows = MaterializerInvokers.GetMaterializerInvoker(ClientResidualOps.rowType(rowOps.Ops)).Invoke(materializer, df);
            return ClientResidualOps.apply(rowOps.Ops, rows).Cast<T>();
        }
        return materializer.Materialize<T>(df);
    }

    /// <summary>Materializes a collected frame as the query's elements: groups, chunks, AggregateBy pairs, or rows.</summary>
    public static IEnumerable<T> Materialize<T>(IDataFrameMaterializer materializer, FSharpList<LinqStage> stages, ClientResidual residual, DataFrameHandle df)
    {
        var targetType = typeof(T);
        bool isGrouping = QueryCompiler.IsGroupingType(targetType);
        if (residual is ClientResidual.RowOps && !isGrouping) return MaterializeFlatRows<T>(materializer, df, residual);

        // Case 1: GroupBy & GroupJoin materialization (produces IEnumerable<IGrouping<TKey, TElement>>)
        if (isGrouping) return MaterializeGroups<T>(materializer, stages, residual, df);

        // Case 2: Chunk slicing via stack-based cursor
        if (targetType.IsArray && stages.OfType<LinqStage.Chunk>().FirstOrDefault() is { } chunk)
            return MaterializerInvokers.GetMaterializerChunkInvoker(targetType.GetElementType()!).Invoke(materializer, df, chunk.size).Cast<T>();

        // Case 3: AggregateBy (produces IEnumerable<KeyValuePair<TKey, TAccum>>)
        if (stages.OfType<LinqStage.AggregateBy>().FirstOrDefault() is { } aggregateBy)
        {
            var genericArgs = targetType.GetGenericArguments();
            var (keyType, accumType) = (genericArgs[0], genericArgs[1]);
            var sourceElemType = aggregateBy.keySelector.Parameters[0].Type;
            var compiledKey = aggregateBy.keySelector.Compile();
            var compiledFunc = aggregateBy.func.Compile();
            IEnumerable result;
            if (aggregateBy.seedFactoryOpt.OrNull() is { } seedLambda)
            {
                result = MaterializerInvokers.GetMaterializerAggByFactoryInvoker(sourceElemType, keyType, accumType)
                    .Invoke(materializer, df, compiledKey, seedLambda.Compile(), compiledFunc, null!);
            }
            else
            {
                if (!QueryCompiler.TryEvaluate(aggregateBy.seedExpr, out var seed))
                    throw new InvalidOperationException("Failed to evaluate static seed for AggregateBy.");
                result = MaterializerInvokers.GetMaterializerAggBySeedInvoker(sourceElemType, keyType, accumType)
                    .Invoke(materializer, df, compiledKey, seed, compiledFunc, null!);
            }
            return result.Cast<T>();
        }

        // Case 4: Flat row materialization (standard records, anonymous types, DTOs)
        if (PolarsWrapper.DataFrameHeight(df) == 0L && stages.OfType<LinqStage.DefaultIfEmpty>().FirstOrDefault() is { } defaultIfEmpty)
        {
            T defaultItem = default!;
            if (defaultIfEmpty.defaultValExprOpt.OrNull() is { } defaultExpr && QueryCompiler.TryEvaluate(defaultExpr, out var value) && value is not null)
                defaultItem = (T)value;
            return [defaultItem];
        }
        return MaterializeFlatRows<T>(materializer, df, residual);
    }

    /// <summary>
    /// The order of group sort keys: F#'s generic comparison, strings ordinally (the engine's order, and the order a
    /// pushed-down sort gives), not the current culture's.
    /// </summary>
    private static readonly IComparer<IComparable> GroupKeyOrder =
        Comparer<IComparable>.Create(static (a, b) => Microsoft.FSharp.Core.LanguagePrimitives.GenericComparison(a, b));

    private static IEnumerable<T> MaterializeGroups<T>(IDataFrameMaterializer materializer, FSharpList<LinqStage> stages, ClientResidual residual, DataFrameHandle df)
    {
        var targetType = typeof(T);
        var genericArgs = targetType.GetGenericArguments();
        var (keyType, elemType) = (genericArgs[0], genericArgs[1]);

        IEnumerable<T> ApplyGroupSort(IEnumerable<T> groups)
        {
            if (stages.OfType<LinqStage.Sort>().FirstOrDefault() is not { } sort) return groups;
            var param = Expression.Parameter(typeof(T), "g");
            var boxed = Expression.Convert(Expression.Invoke(sort.Item1, param), typeof(IComparable));
            var keySelector = Expression.Lambda<Func<T, IComparable>>(boxed, param).Compile();
            return sort.isDescending ? groups.OrderByDescending(keySelector, GroupKeyOrder) : groups.OrderBy(keySelector, GroupKeyOrder);
        }

        (LambdaExpression Key, LambdaExpression? Element)? groupBy = null;
        foreach (var stage in stages)
        {
            groupBy = stage switch
            {
                LinqStage.GroupByKey k => (k.Item, null),
                LinqStage.GroupByWithResult r when r.Item2.Parameters.Count < 2 => (r.Item1, r.Item2),
                LinqStage.GroupByWithElementAndResult e => (e.keySelector, e.elemSelector),
                _ => null,
            };
            if (groupBy is not null) break;
        }

        if (groupBy is { } info)
        {
            var compiledKeySelector = info.Key.Compile();
            IEnumerable<T> grouped;
            if (info.Element is { } elemLambda)
            {
                var sourceType = elemLambda.Parameters[0].Type;
                var rawRows = MaterializerInvokers.GetMaterializerInvoker(sourceType).Invoke(materializer, df);
                var groupByMethod = LinqReflectionCache.GetEnumerableMethod("GroupBy", 3, [sourceType, keyType, elemType]);
                grouped = (IEnumerable<T>)groupByMethod.Invoke(null, [rawRows, compiledKeySelector, elemLambda.Compile()])!;
            }
            else
            {
                var rawRows = MaterializerInvokers.GetMaterializerInvoker(elemType).Invoke(materializer, df);
                var groupByMethod = LinqReflectionCache.GetEnumerableMethod("GroupBy", 2, [elemType, keyType]);
                grouped = (IEnumerable<T>)groupByMethod.Invoke(null, [rawRows, compiledKeySelector])!;
            }
            // Client HAVING predicates are residual filters over the groups.
            var filtered = residual is ClientResidual.RowOps rowOps ? ClientResidualOps.apply(rowOps.Ops, grouped).Cast<T>() : grouped;
            return ApplyGroupSort(filtered);
        }

        if (!stages.Any(s => s is LinqStage.GroupJoin)) throw new InvalidOperationException("Could not locate keySelector for GroupBy pipeline.");

        var innerNullableType = elemType.IsValueType ? typeof(Nullable<>).MakeGenericType(elemType) : elemType;
        var pairType = typeof(ValueTuple<,>).MakeGenericType(keyType, innerNullableType);
        var rawPairs = MaterializerInvokers.GetMaterializerInvoker(pairType).Invoke(materializer, df);
        List<object> keyOrder = [];
        var groupMap = new Dictionary<object, List<object>>();
        foreach (ITuple tuple in rawPairs)
        {
            object outer = tuple[0]!;
            object? inner = tuple[1];
            if (!groupMap.TryGetValue(outer, out var list))
            {
                list = [];
                groupMap[outer] = list;
                keyOrder.Add(outer);
            }
            if (inner is not null) list.Add(inner);
        }
        var ctor = typeof(MaterializedGrouping<,>).MakeGenericType(keyType, elemType).GetConstructors()[0];
        var toArrayMethod = LinqReflectionCache.GetEnumerableMethod("ToArray", 1, [elemType]);
        var castMethod = LinqReflectionCache.GetEnumerableMethod("Cast", 1, [elemType]);
        List<T> result = [];
        foreach (var key in keyOrder)
        {
            var typedArray = toArrayMethod.Invoke(null, [castMethod.Invoke(null, [groupMap[key]])]);
            result.Add((T)ctor.Invoke([key, typedArray]));
        }
        return ApplyGroupSort(result);
    }
}
