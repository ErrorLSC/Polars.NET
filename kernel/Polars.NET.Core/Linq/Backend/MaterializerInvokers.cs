// The LINQ provider builds generic code at run time (materializer calls for element types known only then): its public
// entry points carry RequiresDynamicCode / RequiresUnreferencedCode, so the analyzer's warnings inside it are silenced
// here.
#pragma warning disable IL2026, IL2060, IL2067, IL2070, IL2072, IL2075, IL2080, IL2087, IL2090, IL2091, IL3050, IL3051

using System.Collections;
using System.Collections.Concurrent;
using System.Linq.Expressions;
using System.Reflection;
using Polars.NET.Core.Helpers;

namespace Polars.NET.Core.Linq.Backend;

/// <summary>
/// Compiled, cached invokers for IDataFrameMaterializer operations over native DataFrameHandles, plus the
/// DataFrameBuilder.FromRows entry point used to materialize in-memory rows.
/// </summary>
internal static class MaterializerInvokers
{
    private static readonly MethodInfo[] MaterializerMethods = typeof(IDataFrameMaterializer).GetMethods();

    private static readonly ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable>> MaterializeCache = new();
    private static readonly ConcurrentDictionary<(Type, Type, Type), Func<IDataFrameMaterializer, DataFrameHandle, Delegate, object, Delegate, object, IEnumerable>> AggBySeedCache = new();
    private static readonly ConcurrentDictionary<(Type, Type, Type), Func<IDataFrameMaterializer, DataFrameHandle, Delegate, Delegate, Delegate, object, IEnumerable>> AggByFactoryCache = new();
    private static readonly ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, int, IEnumerable>> ChunkCache = new();
    private static readonly ConcurrentDictionary<(string, Type), Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, bool>> BoolPredicateCache = new();
    private static readonly ConcurrentDictionary<(string, Type), Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, object?>> RowFetchCache = new();
    private static readonly ConcurrentDictionary<(string, Type), Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, object?, object?>> RowFetchOrDefaultCache = new();
    private static readonly ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, long>> CountCache = new();
    private static readonly ConcurrentDictionary<(string, Type), Func<IDataFrameMaterializer, DataFrameHandle, long, object?>> ElementAtCache = new();
    private static readonly ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, object?, object?, bool>> ContainsCache = new();
    private static readonly ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, DataFrameHandle, object?, bool>> SeqEqualDfCache = new();
    private static readonly ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable, object?, bool>> SeqEqualSeqCache = new();
    private static readonly ConcurrentDictionary<(string, Type, Type), Func<IDataFrameMaterializer, DataFrameHandle, Delegate, object?, object?>> ExtremumByCache = new();
    private static readonly ConcurrentDictionary<(Type, Type), Func<IDataFrameMaterializer, DataFrameHandle, object?, Delegate, object?>> AggregateCache = new();

    /// <summary>Cached MethodInfo for DataFrameBuilder.FromRows.</summary>
    public static readonly MethodInfo FromRowsMethod = typeof(DataFrameBuilder).GetMethod("FromRows", BindingFlags.Public | BindingFlags.Static)!;

    private static MethodInfo FindMaterializerMethod(string name, int paramCount, params Type[] typeArgs) =>
        MaterializerMethods
            .First(m => m.Name == name
                && m.IsGenericMethodDefinition
                && m.GetGenericArguments().Length == typeArgs.Length
                && m.GetParameters().Length == paramCount)
            .MakeGenericMethod(typeArgs);

    private static ConditionalExpression NullableCast(ParameterExpression param, Type targetType) =>
        Expression.Condition(
            Expression.Equal(param, Expression.Constant(null, param.Type)),
            Expression.Constant(null, targetType),
            Expression.Convert(param, targetType));

    private static Type FuncOf(params Type[] types) => types.Length switch
    {
        2 => typeof(Func<,>).MakeGenericType(types),
        3 => typeof(Func<,,>).MakeGenericType(types),
        _ => throw new ArgumentOutOfRangeException(nameof(types)),
    };

    private static ParameterExpression Mat() => Expression.Parameter(typeof(IDataFrameMaterializer), "mat");

    private static ParameterExpression Handle(string name = "handle") => Expression.Parameter(typeof(DataFrameHandle), name);

    private static ParameterExpression Param<T>(string name) => Expression.Parameter(typeof(T), name);

    /// <summary>Resolves a strongly-typed, zero-reflection materializer invoker for the specified element type.</summary>
    public static Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable> GetMaterializerInvoker(Type elemType) =>
        MaterializeCache.GetOrAdd(elemType, static t =>
        {
            var (mat, handle) = (Mat(), Handle());
            var call = Expression.Call(mat, FindMaterializerMethod("Materialize", 1, t), handle);
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable>>(Expression.Convert(call, typeof(IEnumerable)), mat, handle).Compile();
        });

    /// <summary>Resolves compiled invoker for IDataFrameMaterializer.AggregateBy with static seed.</summary>
    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate, object, Delegate, object, IEnumerable> GetMaterializerAggBySeedInvoker(Type srcType, Type keyType, Type accumType) =>
        AggBySeedCache.GetOrAdd((srcType, keyType, accumType), static key =>
        {
            var (tSrc, tKey, tAcc) = key;
            var method = MaterializerMethods
                .First(m => m.Name == "AggregateBy" && m.GetGenericArguments().Length == 3 && m.GetParameters()[2].ParameterType == m.GetGenericArguments()[2])
                .MakeGenericMethod(tSrc, tKey, tAcc);
            var (mat, handle) = (Mat(), Handle());
            var (keySel, seed, func, comp) = (Param<Delegate>("keySel"), Param<object>("seed"), Param<Delegate>("func"), Param<object>("comp"));
            var call = Expression.Call(mat, method, handle,
                Expression.Convert(keySel, FuncOf(tSrc, tKey)),
                Expression.Convert(seed, tAcc),
                Expression.Convert(func, FuncOf(tAcc, tSrc, tAcc)),
                Expression.Convert(comp, typeof(IEqualityComparer<>).MakeGenericType(tKey)));
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, object, Delegate, object, IEnumerable>>(
                Expression.Convert(call, typeof(IEnumerable)), mat, handle, keySel, seed, func, comp).Compile();
        });

    /// <summary>Resolves compiled invoker for IDataFrameMaterializer.AggregateBy with seedFactory.</summary>
    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate, Delegate, Delegate, object, IEnumerable> GetMaterializerAggByFactoryInvoker(Type srcType, Type keyType, Type accumType) =>
        AggByFactoryCache.GetOrAdd((srcType, keyType, accumType), static key =>
        {
            var (tSrc, tKey, tAcc) = key;
            var method = MaterializerMethods
                .First(m => m.Name == "AggregateBy"
                    && m.GetGenericArguments().Length == 3
                    && m.GetParameters()[2].ParameterType.IsGenericType
                    && m.GetParameters()[2].ParameterType.GetGenericTypeDefinition() == typeof(Func<,>))
                .MakeGenericMethod(tSrc, tKey, tAcc);
            var (mat, handle) = (Mat(), Handle());
            var (keySel, seedFactory, func, comp) = (Param<Delegate>("keySel"), Param<Delegate>("seedFactory"), Param<Delegate>("func"), Param<object>("comp"));
            var call = Expression.Call(mat, method, handle,
                Expression.Convert(keySel, FuncOf(tSrc, tKey)),
                Expression.Convert(seedFactory, FuncOf(tKey, tAcc)),
                Expression.Convert(func, FuncOf(tAcc, tSrc, tAcc)),
                Expression.Convert(comp, typeof(IEqualityComparer<>).MakeGenericType(tKey)));
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, Delegate, Delegate, object, IEnumerable>>(
                Expression.Convert(call, typeof(IEnumerable)), mat, handle, keySel, seedFactory, func, comp).Compile();
        });

    /// <summary>Resolves a compiled invoker for IDataFrameMaterializer.Chunk&lt;T&gt;.</summary>
    public static Func<IDataFrameMaterializer, DataFrameHandle, int, IEnumerable> GetMaterializerChunkInvoker(Type elemType) =>
        ChunkCache.GetOrAdd(elemType, static t =>
        {
            var (mat, handle, size) = (Mat(), Handle(), Param<int>("chunkSize"));
            var call = Expression.Call(mat, FindMaterializerMethod("Chunk", 2, t), handle, size);
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, int, IEnumerable>>(Expression.Convert(call, typeof(IEnumerable)), mat, handle, size).Compile();
        });

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, bool> GetMaterializerBoolPredicateInvoker(string methodName, Type elemType) =>
        BoolPredicateCache.GetOrAdd((methodName, elemType), static key =>
        {
            var (name, t) = key;
            var (mat, handle, pred) = (Mat(), Handle(), Param<Delegate>("pred"));
            var call = Expression.Call(mat, FindMaterializerMethod(name, 2, t), handle, NullableCast(pred, FuncOf(t, typeof(bool))));
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, bool>>(call, mat, handle, pred).Compile();
        });

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, bool> GetMaterializerAnyInvoker(Type elemType) => GetMaterializerBoolPredicateInvoker("Any", elemType);

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, bool> GetMaterializerAllInvoker(Type elemType) => GetMaterializerBoolPredicateInvoker("All", elemType);

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, object?> GetMaterializerRowFetchInvoker(string methodName, Type elemType) =>
        RowFetchCache.GetOrAdd((methodName, elemType), static key =>
        {
            var (name, t) = key;
            var (mat, handle, pred) = (Mat(), Handle(), Param<Delegate>("pred"));
            var call = Expression.Call(mat, FindMaterializerMethod(name, 2, t), handle, NullableCast(pred, FuncOf(t, typeof(bool))));
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, object?>>(Expression.Convert(call, typeof(object)), mat, handle, pred).Compile();
        });

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, object?> GetMaterializerFirstInvoker(Type elemType) => GetMaterializerRowFetchInvoker("First", elemType);

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, object?> GetMaterializerLastInvoker(Type elemType) => GetMaterializerRowFetchInvoker("Last", elemType);

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, object?> GetMaterializerSingleInvoker(Type elemType) => GetMaterializerRowFetchInvoker("Single", elemType);

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, object?, object?> GetMaterializerRowFetchOrDefaultInvoker(string methodName, Type elemType) =>
        RowFetchOrDefaultCache.GetOrAdd((methodName, elemType), static key =>
        {
            var (name, t) = key;
            var (mat, handle, pred, defaultValue) = (Mat(), Handle(), Param<Delegate>("pred"), Param<object>("defaultVal"));
            var call = Expression.Call(mat, FindMaterializerMethod(name, 3, t), handle, NullableCast(pred, FuncOf(t, typeof(bool))), Expression.Convert(defaultValue, t));
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, object?, object?>>(
                Expression.Convert(call, typeof(object)), mat, handle, pred, defaultValue).Compile();
        });

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, object?, object?> GetMaterializerFirstOrDefaultInvoker(Type elemType) => GetMaterializerRowFetchOrDefaultInvoker("FirstOrDefault", elemType);

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, object?, object?> GetMaterializerLastOrDefaultInvoker(Type elemType) => GetMaterializerRowFetchOrDefaultInvoker("LastOrDefault", elemType);

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, object?, object?> GetMaterializerSingleOrDefaultInvoker(Type elemType) => GetMaterializerRowFetchOrDefaultInvoker("SingleOrDefault", elemType);

    /// <summary>Resolves a compiled invoker for IDataFrameMaterializer.Count&lt;TSource&gt;(handle, predicate).</summary>
    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, long> GetMaterializerCountInvoker(Type elemType) =>
        CountCache.GetOrAdd(elemType, static t =>
        {
            var (mat, handle, pred) = (Mat(), Handle(), Param<Delegate>("pred"));
            var call = Expression.Call(mat, FindMaterializerMethod("Count", 2, t), handle, NullableCast(pred, FuncOf(t, typeof(bool))));
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate?, long>>(call, mat, handle, pred).Compile();
        });

    public static Func<IDataFrameMaterializer, DataFrameHandle, long, object?> GetMaterializerElementAtByNameInvoker(string methodName, Type elemType) =>
        ElementAtCache.GetOrAdd((methodName, elemType), static key =>
        {
            var (name, t) = key;
            var (mat, handle, index) = (Mat(), Handle(), Param<long>("index"));
            var call = Expression.Call(mat, FindMaterializerMethod(name, 2, t), handle, index);
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, long, object?>>(Expression.Convert(call, typeof(object)), mat, handle, index).Compile();
        });

    public static Func<IDataFrameMaterializer, DataFrameHandle, long, object?> GetMaterializerElementAtInvoker(Type elemType) => GetMaterializerElementAtByNameInvoker("ElementAt", elemType);

    public static Func<IDataFrameMaterializer, DataFrameHandle, long, object?> GetMaterializerElementAtOrDefaultInvoker(Type elemType) => GetMaterializerElementAtByNameInvoker("ElementAtOrDefault", elemType);

    /// <summary>Resolves a compiled invoker for IDataFrameMaterializer.Contains&lt;TSource&gt;(handle, item, comparer).</summary>
    public static Func<IDataFrameMaterializer, DataFrameHandle, object?, object?, bool> GetMaterializerContainsInvoker(Type elemType) =>
        ContainsCache.GetOrAdd(elemType, static t =>
        {
            var (mat, handle, item, comparer) = (Mat(), Handle(), Param<object>("item"), Param<object>("comparer"));
            var call = Expression.Call(mat, FindMaterializerMethod("Contains", 3, t), handle,
                Expression.Convert(item, t), NullableCast(comparer, typeof(IEqualityComparer<>).MakeGenericType(t)));
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, object?, object?, bool>>(call, mat, handle, item, comparer).Compile();
        });

    /// <summary>Invoker for SequenceEqual(dfHandle1, dfHandle2, comparer).</summary>
    public static Func<IDataFrameMaterializer, DataFrameHandle, DataFrameHandle, object?, bool> GetMaterializerSeqEqualDfInvoker(Type elemType) =>
        SeqEqualDfCache.GetOrAdd(elemType, static t =>
        {
            var method = MaterializerMethods
                .First(m => m.Name == "SequenceEqual" && m.IsGenericMethodDefinition && m.GetParameters().Length == 3 && m.GetParameters()[1].ParameterType == typeof(DataFrameHandle))
                .MakeGenericMethod(t);
            var (mat, h1, h2, comp) = (Mat(), Handle("h1"), Handle("h2"), Param<object>("comp"));
            var call = Expression.Call(mat, method, h1, h2, NullableCast(comp, typeof(IEqualityComparer<>).MakeGenericType(t)));
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, DataFrameHandle, object?, bool>>(call, mat, h1, h2, comp).Compile();
        });

    /// <summary>Invoker for SequenceEqual(dfHandle, enumerable, comparer).</summary>
    public static Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable, object?, bool> GetMaterializerSeqEqualSeqInvoker(Type elemType) =>
        SeqEqualSeqCache.GetOrAdd(elemType, static t =>
        {
            var method = MaterializerMethods
                .First(m =>
                {
                    if (m.Name != "SequenceEqual" || !m.IsGenericMethodDefinition) return false;
                    var ps = m.GetParameters();
                    return ps.Length == 3 && ps[1].ParameterType.IsGenericType && ps[1].ParameterType.GetGenericTypeDefinition() == typeof(IEnumerable<>);
                })
                .MakeGenericMethod(t);
            var (mat, handle, seq, comp) = (Mat(), Handle(), Param<IEnumerable>("seq"), Param<object>("comp"));
            var call = Expression.Call(mat, method, handle,
                Expression.Convert(seq, typeof(IEnumerable<>).MakeGenericType(t)), NullableCast(comp, typeof(IEqualityComparer<>).MakeGenericType(t)));
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable, object?, bool>>(call, mat, handle, seq, comp).Compile();
        });

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate, object?, object?> GetMaterializerExtremumByInvoker(string methodName, Type sourceType, Type keyType) =>
        ExtremumByCache.GetOrAdd((methodName, sourceType, keyType), static key =>
        {
            var (name, tSrc, tKey) = key;
            var (mat, handle, keySelector, comparer) = (Mat(), Handle(), Param<Delegate>("keySelector"), Param<object>("comparer"));
            var call = Expression.Call(mat, FindMaterializerMethod(name, 3, tSrc, tKey), handle,
                Expression.Convert(keySelector, FuncOf(tSrc, tKey)), NullableCast(comparer, typeof(IComparer<>).MakeGenericType(tKey)));
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, object?, object?>>(
                Expression.Convert(call, typeof(object)), mat, handle, keySelector, comparer).Compile();
        });

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate, object?, object?> GetMaterializerMinByInvoker(Type sourceType, Type keyType) => GetMaterializerExtremumByInvoker("MinBy", sourceType, keyType);

    public static Func<IDataFrameMaterializer, DataFrameHandle, Delegate, object?, object?> GetMaterializerMaxByInvoker(Type sourceType, Type keyType) => GetMaterializerExtremumByInvoker("MaxBy", sourceType, keyType);

    /// <summary>Resolves compiled invoker for IDataFrameMaterializer.Aggregate&lt;TSource, TAccum&gt;(handle, seed, folder).</summary>
    public static Func<IDataFrameMaterializer, DataFrameHandle, object?, Delegate, object?> GetMaterializerAggregateInvoker(Type sourceType, Type accumType) =>
        AggregateCache.GetOrAdd((sourceType, accumType), static key =>
        {
            var (tSrc, tAcc) = key;
            var (mat, handle, seed, folder) = (Mat(), Handle(), Param<object>("seed"), Param<Delegate>("folder"));
            var call = Expression.Call(mat, FindMaterializerMethod("Aggregate", 3, tSrc, tAcc), handle,
                Expression.Convert(seed, tAcc), Expression.Convert(folder, FuncOf(tAcc, tSrc, tAcc)));
            return Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, object?, Delegate, object?>>(
                Expression.Convert(call, typeof(object)), mat, handle, seed, folder).Compile();
        });
}
