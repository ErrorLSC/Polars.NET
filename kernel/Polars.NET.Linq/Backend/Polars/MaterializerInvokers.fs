namespace Polars.NET.Linq.Backend.Polars

open System
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core
open Polars.NET.Core.Helpers

/// Compiled, cached invokers for IDataFrameMaterializer operations over native DataFrameHandles,
/// plus the DataFrameBuilder.FromRows entry point used to materialize in-memory rows.
[<AbstractClass; Sealed>]
type internal MaterializerInvokers private () =
    static let fromRowsMethod =
        typeof<DataFrameBuilder>.GetMethod("FromRows", BindingFlags.Public ||| BindingFlags.Static)

    static let materializerMethods = typeof<IDataFrameMaterializer>.GetMethods()

    static let materializerDelegateCache =
        ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable>>()

    static let matAggBySeedCache =
        ConcurrentDictionary<
            Type * Type * Type,
            Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, Delegate, obj, IEnumerable>
         >()

    static let matAggByFactoryCache =
        ConcurrentDictionary<
            Type * Type * Type,
            Func<IDataFrameMaterializer, DataFrameHandle, Delegate, Delegate, Delegate, obj, IEnumerable>
         >()

    static let chunkDelegateCache =
        ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, int, IEnumerable>>()

    // Unified Cache: (methodName, elemType) -> Func<IDataFrameMaterializer, DataFrameHandle, Delegate, bool> for Any / All
    static let matPredBoolCache =
        ConcurrentDictionary<string * Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate, bool>>()

    // Unified Cache: (methodName, elemType) -> Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj> for First / Last / Single
    static let matRowFetchCache =
        ConcurrentDictionary<string * Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj>>()

    // Unified Cache: (methodName, elemType) -> Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj> for FirstOrDefault / LastOrDefault / SingleOrDefault
    static let matRowFetchOrDefaultCache =
        ConcurrentDictionary<string * Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj>>()

    static let matCountCache =
        ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate, int64>>()

    // Unified Cache: (methodName, elemType) -> Func<IDataFrameMaterializer, DataFrameHandle, int64, obj> for ElementAt / ElementAtOrDefault
    static let matElementAtCache =
        ConcurrentDictionary<string * Type, Func<IDataFrameMaterializer, DataFrameHandle, int64, obj>>()

    static let matContainsCache =
        ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, obj, obj, bool>>()

    static let matSeqEqualDfCache =
        ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, DataFrameHandle, obj, bool>>()

    static let matSeqEqualSeqCache =
        ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable, obj, bool>>()

    // Unified Cache: (methodName, sourceType, keyType) -> Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj> for MinBy / MaxBy
    static let matExtremumByCache =
        ConcurrentDictionary<string * Type * Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj>>()

    static let matAggregateCache =
        ConcurrentDictionary<Type * Type, Func<IDataFrameMaterializer, DataFrameHandle, obj, Delegate, obj>>()

    static let findMaterializerMethod (name: string) (paramCount: int) (typeArgs: Type array) =
        materializerMethods
        |> Array.find (fun m ->
            m.Name = name
            && m.IsGenericMethodDefinition
            && m.GetGenericArguments().Length = typeArgs.Length
            && m.GetParameters().Length = paramCount)
        |> fun m -> m.MakeGenericMethod(typeArgs)

    static let nullableCast (param: ParameterExpression) (targetType: Type) =
        Expression.Condition(
            Expression.Equal(param, Expression.Constant(null, param.Type)),
            Expression.Constant(null, targetType),
            Expression.Convert(param, targetType)
        )

    /// Cached MethodInfo for DataFrameBuilder.FromRows
    static member FromRowsMethod = fromRowsMethod

    /// Resolves a strongly-typed, zero-reflection materializer invoker for the specified element type
    static member GetMaterializerInvoker(elemType: Type) : Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable> =
        materializerDelegateCache.GetOrAdd(
            elemType,
            fun t ->
                let methodInfo = findMaterializerMethod "Materialize" 1 [| t |]
                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
                let callExpr = Expression.Call(matParam, methodInfo, handleParam)
                let castResult = Expression.Convert(callExpr, typeof<IEnumerable>)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable>>(
                        castResult,
                        matParam,
                        handleParam
                    )
                    .Compile()
        )

    /// Resolves compiled invoker for IDataFrameMaterializer.AggregateBy with static seed
    static member GetMaterializerAggBySeedInvoker(srcType: Type, keyType: Type, accumType: Type) =
        matAggBySeedCache.GetOrAdd(
            (srcType, keyType, accumType),
            fun (tSrc, tKey, tAcc) ->
                let methodInfo =
                    materializerMethods
                    |> Array.find (fun m ->
                        m.Name = "AggregateBy"
                        && m.GetGenericArguments().Length = 3
                        && m.GetParameters().[2].ParameterType = m.GetGenericArguments().[2])
                    |> fun m -> m.MakeGenericMethod(tSrc, tKey, tAcc)

                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
                let keySelParam = Expression.Parameter(typeof<Delegate>, "keySel")
                let seedParam = Expression.Parameter(typeof<obj>, "seed")
                let funcParam = Expression.Parameter(typeof<Delegate>, "func")
                let compParam = Expression.Parameter(typeof<obj>, "comp")

                let castKeySel = Expression.Convert(keySelParam, typedefof<Func<_, _>>.MakeGenericType(tSrc, tKey))
                let castSeed = Expression.Convert(seedParam, tAcc)
                let castFunc = Expression.Convert(funcParam, typedefof<Func<_, _, _>>.MakeGenericType(tAcc, tSrc, tAcc))
                let castComp = Expression.Convert(compParam, typedefof<IEqualityComparer<_>>.MakeGenericType(tKey))

                let callExpr =
                    Expression.Call(matParam, methodInfo, handleParam, castKeySel, castSeed, castFunc, castComp)

                let castResult = Expression.Convert(callExpr, typeof<IEnumerable>)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, Delegate, obj, IEnumerable>>(
                        castResult,
                        matParam,
                        handleParam,
                        keySelParam,
                        seedParam,
                        funcParam,
                        compParam
                    )
                    .Compile()
        )

    /// Resolves compiled invoker for IDataFrameMaterializer.AggregateBy with seedFactory
    static member GetMaterializerAggByFactoryInvoker(srcType: Type, keyType: Type, accumType: Type) =
        matAggByFactoryCache.GetOrAdd(
            (srcType, keyType, accumType),
            fun (tSrc, tKey, tAcc) ->
                let methodInfo =
                    materializerMethods
                    |> Array.find (fun m ->
                        m.Name = "AggregateBy"
                        && m.GetGenericArguments().Length = 3
                        && m.GetParameters().[2].ParameterType.IsGenericType
                        && m.GetParameters().[2].ParameterType.GetGenericTypeDefinition() = typedefof<Func<_, _>>)
                    |> fun m -> m.MakeGenericMethod(tSrc, tKey, tAcc)

                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
                let keySelParam = Expression.Parameter(typeof<Delegate>, "keySel")
                let seedFactoryParam = Expression.Parameter(typeof<Delegate>, "seedFactory")
                let funcParam = Expression.Parameter(typeof<Delegate>, "func")
                let compParam = Expression.Parameter(typeof<obj>, "comp")

                let castKeySel = Expression.Convert(keySelParam, typedefof<Func<_, _>>.MakeGenericType(tSrc, tKey))
                let castSeedFactory = Expression.Convert(seedFactoryParam, typedefof<Func<_, _>>.MakeGenericType(tKey, tAcc))
                let castFunc = Expression.Convert(funcParam, typedefof<Func<_, _, _>>.MakeGenericType(tAcc, tSrc, tAcc))
                let castComp = Expression.Convert(compParam, typedefof<IEqualityComparer<_>>.MakeGenericType(tKey))

                let callExpr =
                    Expression.Call(matParam, methodInfo, handleParam, castKeySel, castSeedFactory, castFunc, castComp)

                let castResult = Expression.Convert(callExpr, typeof<IEnumerable>)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, Delegate, Delegate, obj, IEnumerable>>(
                        castResult,
                        matParam,
                        handleParam,
                        keySelParam,
                        seedFactoryParam,
                        funcParam,
                        compParam
                    )
                    .Compile()
        )

    /// Resolves a compiled invoker for IDataFrameMaterializer.Chunk<T>
    static member GetMaterializerChunkInvoker
        (elemType: Type)
        : Func<IDataFrameMaterializer, DataFrameHandle, int, IEnumerable> =
        chunkDelegateCache.GetOrAdd(
            elemType,
            fun t ->
                let methodInfo = findMaterializerMethod "Chunk" 2 [| t |]
                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
                let sizeParam = Expression.Parameter(typeof<int>, "chunkSize")

                let callExpr = Expression.Call(matParam, methodInfo, handleParam, sizeParam)
                let castResult = Expression.Convert(callExpr, typeof<IEnumerable>)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, int, IEnumerable>>(
                        castResult,
                        matParam,
                        handleParam,
                        sizeParam
                    )
                    .Compile()
        )

    static member GetMaterializerBoolPredicateInvoker
        (methodName: string, elemType: Type)
        : Func<IDataFrameMaterializer, DataFrameHandle, Delegate, bool> =
        matPredBoolCache.GetOrAdd(
            (methodName, elemType),
            fun (mName, t) ->
                let methodInfo = findMaterializerMethod mName 2 [| t |]
                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
                let predParam = Expression.Parameter(typeof<Delegate>, "pred")
                let funcType = typedefof<Func<_, _>>.MakeGenericType(t, typeof<bool>)
                let callExpr = Expression.Call(matParam, methodInfo, handleParam, nullableCast predParam funcType)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, bool>>(
                        callExpr,
                        matParam,
                        handleParam,
                        predParam
                    )
                    .Compile()
        )

    static member GetMaterializerAnyInvoker(elemType: Type) =
        MaterializerInvokers.GetMaterializerBoolPredicateInvoker("Any", elemType)

    static member GetMaterializerAllInvoker(elemType: Type) =
        MaterializerInvokers.GetMaterializerBoolPredicateInvoker("All", elemType)

    static member GetMaterializerRowFetchInvoker
        (methodName: string, elemType: Type)
        : Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj> =
        matRowFetchCache.GetOrAdd(
            (methodName, elemType),
            fun (mName, t) ->
                let methodInfo = findMaterializerMethod mName 2 [| t |]
                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
                let predParam = Expression.Parameter(typeof<Delegate>, "pred")
                let funcType = typedefof<Func<_, _>>.MakeGenericType(t, typeof<bool>)
                let callExpr = Expression.Call(matParam, methodInfo, handleParam, nullableCast predParam funcType)
                let boxResult = Expression.Convert(callExpr, typeof<obj>)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj>>(
                        boxResult,
                        matParam,
                        handleParam,
                        predParam
                    )
                    .Compile()
        )

    static member GetMaterializerFirstInvoker(elemType: Type) =
        MaterializerInvokers.GetMaterializerRowFetchInvoker("First", elemType)

    static member GetMaterializerLastInvoker(elemType: Type) =
        MaterializerInvokers.GetMaterializerRowFetchInvoker("Last", elemType)

    static member GetMaterializerSingleInvoker(elemType: Type) =
        MaterializerInvokers.GetMaterializerRowFetchInvoker("Single", elemType)

    static member GetMaterializerRowFetchOrDefaultInvoker
        (methodName: string, elemType: Type)
        : Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj> =
        matRowFetchOrDefaultCache.GetOrAdd(
            (methodName, elemType),
            fun (mName, t) ->
                let methodInfo = findMaterializerMethod mName 3 [| t |]
                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
                let predParam = Expression.Parameter(typeof<Delegate>, "pred")
                let defValParam = Expression.Parameter(typeof<obj>, "defaultVal")
                let funcType = typedefof<Func<_, _>>.MakeGenericType(t, typeof<bool>)
                let castDefVal = Expression.Convert(defValParam, t)

                let callExpr =
                    Expression.Call(matParam, methodInfo, handleParam, nullableCast predParam funcType, castDefVal)

                let boxResult = Expression.Convert(callExpr, typeof<obj>)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj>>(
                        boxResult,
                        matParam,
                        handleParam,
                        predParam,
                        defValParam
                    )
                    .Compile()
        )

    static member GetMaterializerFirstOrDefaultInvoker(elemType: Type) =
        MaterializerInvokers.GetMaterializerRowFetchOrDefaultInvoker("FirstOrDefault", elemType)

    static member GetMaterializerLastOrDefaultInvoker(elemType: Type) =
        MaterializerInvokers.GetMaterializerRowFetchOrDefaultInvoker("LastOrDefault", elemType)

    static member GetMaterializerSingleOrDefaultInvoker(elemType: Type) =
        MaterializerInvokers.GetMaterializerRowFetchOrDefaultInvoker("SingleOrDefault", elemType)

    /// Resolves a compiled invoker for IDataFrameMaterializer.Count<TSource>(handle, predicate) -> int64
    static member GetMaterializerCountInvoker
        (elemType: Type)
        : Func<IDataFrameMaterializer, DataFrameHandle, Delegate, int64> =
        matCountCache.GetOrAdd(
            elemType,
            fun t ->
                let methodInfo = findMaterializerMethod "Count" 2 [| t |]
                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
                let predParam = Expression.Parameter(typeof<Delegate>, "pred")
                let funcType = typedefof<Func<_, _>>.MakeGenericType(t, typeof<bool>)
                let callExpr = Expression.Call(matParam, methodInfo, handleParam, nullableCast predParam funcType)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, int64>>(
                        callExpr,
                        matParam,
                        handleParam,
                        predParam
                    )
                    .Compile()
        )

    static member GetMaterializerElementAtByNameInvoker
        (methodName: string, elemType: Type)
        : Func<IDataFrameMaterializer, DataFrameHandle, int64, obj> =
        matElementAtCache.GetOrAdd(
            (methodName, elemType),
            fun (mName, t) ->
                let methodInfo = findMaterializerMethod mName 2 [| t |]
                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
                let indexParam = Expression.Parameter(typeof<int64>, "index")

                let callExpr = Expression.Call(matParam, methodInfo, handleParam, indexParam)
                let boxResult = Expression.Convert(callExpr, typeof<obj>)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, int64, obj>>(
                        boxResult,
                        matParam,
                        handleParam,
                        indexParam
                    )
                    .Compile()
        )

    static member GetMaterializerElementAtInvoker(elemType: Type) =
        MaterializerInvokers.GetMaterializerElementAtByNameInvoker("ElementAt", elemType)

    static member GetMaterializerElementAtOrDefaultInvoker(elemType: Type) =
        MaterializerInvokers.GetMaterializerElementAtByNameInvoker("ElementAtOrDefault", elemType)

    /// Resolves a compiled invoker for IDataFrameMaterializer.Contains<TSource>(handle, item, comparer)
    static member GetMaterializerContainsInvoker
        (elemType: Type)
        : Func<IDataFrameMaterializer, DataFrameHandle, obj, obj, bool> =
        matContainsCache.GetOrAdd(
            elemType,
            fun t ->
                let methodInfo = findMaterializerMethod "Contains" 3 [| t |]
                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
                let itemParam = Expression.Parameter(typeof<obj>, "item")
                let compParam = Expression.Parameter(typeof<obj>, "comparer")

                let castItem = Expression.Convert(itemParam, t)
                let compType = typedefof<IEqualityComparer<_>>.MakeGenericType(t)
                let callExpr = Expression.Call(matParam, methodInfo, handleParam, castItem, nullableCast compParam compType)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, obj, obj, bool>>(
                        callExpr,
                        matParam,
                        handleParam,
                        itemParam,
                        compParam
                    )
                    .Compile()
        )

    /// Invoker for SequenceEqual(dfHandle1, dfHandle2, comparer)
    static member GetMaterializerSeqEqualDfInvoker
        (elemType: Type)
        : Func<IDataFrameMaterializer, DataFrameHandle, DataFrameHandle, obj, bool> =
        matSeqEqualDfCache.GetOrAdd(
            elemType,
            fun t ->
                let methodInfo =
                    materializerMethods
                    |> Array.find (fun m ->
                        m.Name = "SequenceEqual"
                        && m.IsGenericMethodDefinition
                        && m.GetParameters().Length = 3
                        && m.GetParameters().[1].ParameterType = typeof<DataFrameHandle>)
                    |> fun m -> m.MakeGenericMethod(t)

                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let h1Param = Expression.Parameter(typeof<DataFrameHandle>, "h1")
                let h2Param = Expression.Parameter(typeof<DataFrameHandle>, "h2")
                let compParam = Expression.Parameter(typeof<obj>, "comp")
                let compType = typedefof<IEqualityComparer<_>>.MakeGenericType(t)

                let callExpr = Expression.Call(matParam, methodInfo, h1Param, h2Param, nullableCast compParam compType)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, DataFrameHandle, obj, bool>>(
                        callExpr,
                        matParam,
                        h1Param,
                        h2Param,
                        compParam
                    )
                    .Compile()
        )

    /// Invoker for SequenceEqual(dfHandle, enumerable, comparer)
    static member GetMaterializerSeqEqualSeqInvoker
        (elemType: Type)
        : Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable, obj, bool> =
        matSeqEqualSeqCache.GetOrAdd(
            elemType,
            fun t ->
                let methodInfo =
                    materializerMethods
                    |> Array.find (fun m ->
                        if m.Name <> "SequenceEqual" || not m.IsGenericMethodDefinition then
                            false
                        else
                            let ps = m.GetParameters()
                            ps.Length = 3
                            && ps.[1].ParameterType.IsGenericType
                            && ps.[1].ParameterType.GetGenericTypeDefinition() = typedefof<IEnumerable<_>>)
                    |> fun m -> m.MakeGenericMethod(t)

                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let hParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
                let seqParam = Expression.Parameter(typeof<IEnumerable>, "seq")
                let compParam = Expression.Parameter(typeof<obj>, "comp")

                let castSeq = Expression.Convert(seqParam, typedefof<IEnumerable<_>>.MakeGenericType(t))
                let compType = typedefof<IEqualityComparer<_>>.MakeGenericType(t)

                let callExpr = Expression.Call(matParam, methodInfo, hParam, castSeq, nullableCast compParam compType)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable, obj, bool>>(
                        callExpr,
                        matParam,
                        hParam,
                        seqParam,
                        compParam
                    )
                    .Compile()
        )

    static member GetMaterializerExtremumByInvoker
        (methodName: string, sourceType: Type, keyType: Type)
        : Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj> =
        matExtremumByCache.GetOrAdd(
            (methodName, sourceType, keyType),
            fun (mName, tSrc, tKey) ->
                let methodInfo = findMaterializerMethod mName 3 [| tSrc; tKey |]
                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let hParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
                let keySelParam = Expression.Parameter(typeof<Delegate>, "keySelector")
                let compParam = Expression.Parameter(typeof<obj>, "comparer")

                let castKeySel = Expression.Convert(keySelParam, typedefof<Func<_, _>>.MakeGenericType(tSrc, tKey))
                let compType = typedefof<IComparer<_>>.MakeGenericType(tKey)

                let callExpr = Expression.Call(matParam, methodInfo, hParam, castKeySel, nullableCast compParam compType)
                let boxResult = Expression.Convert(callExpr, typeof<obj>)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj>>(
                        boxResult,
                        matParam,
                        hParam,
                        keySelParam,
                        compParam
                    )
                    .Compile()
        )

    static member GetMaterializerMinByInvoker(sourceType: Type, keyType: Type) =
        MaterializerInvokers.GetMaterializerExtremumByInvoker("MinBy", sourceType, keyType)

    static member GetMaterializerMaxByInvoker(sourceType: Type, keyType: Type) =
        MaterializerInvokers.GetMaterializerExtremumByInvoker("MaxBy", sourceType, keyType)

    /// Resolves compiled invoker for IDataFrameMaterializer.Aggregate<TSource, TAccum>(handle, seed, folder)
    static member GetMaterializerAggregateInvoker
        (sourceType: Type, accumType: Type)
        : Func<IDataFrameMaterializer, DataFrameHandle, obj, Delegate, obj> =
        matAggregateCache.GetOrAdd(
            (sourceType, accumType),
            fun (tSrc, tAcc) ->
                let methodInfo = findMaterializerMethod "Aggregate" 3 [| tSrc; tAcc |]
                let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
                let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
                let seedParam = Expression.Parameter(typeof<obj>, "seed")
                let folderParam = Expression.Parameter(typeof<Delegate>, "folder")

                let castSeed = Expression.Convert(seedParam, tAcc)
                let castFolder = Expression.Convert(folderParam, typedefof<Func<_, _, _>>.MakeGenericType(tAcc, tSrc, tAcc))

                let callExpr = Expression.Call(matParam, methodInfo, handleParam, castSeed, castFolder)
                let boxResult = Expression.Convert(callExpr, typeof<obj>)

                Expression
                    .Lambda<Func<IDataFrameMaterializer, DataFrameHandle, obj, Delegate, obj>>(
                        boxResult,
                        matParam,
                        handleParam,
                        seedParam,
                        folderParam
                    )
                    .Compile()
        )
