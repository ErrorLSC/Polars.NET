namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core
open Polars.NET.Core.Helpers

/// Global reflection cache for LINQ methods to eliminate runtime lookup overhead.
[<AbstractClass; Sealed>]
type internal LinqReflectionCache private () =
    static let enumerableMethods =
        typeof<Enumerable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)

    static let materializerMethods = typeof<IDataFrameMaterializer>.GetMethods()

    static let materializerDelegateCache =
        ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable>>()

    static let enumerableGenericCache =
        ConcurrentDictionary<string * int * string, MethodInfo>()

    static let aggOverload1Cache =
        ConcurrentDictionary<Type, Func<IEnumerable, Delegate, obj>>()

    static let aggOverload2Cache =
        ConcurrentDictionary<Type * Type, Func<IEnumerable, obj, Delegate, obj>>()

    static let aggOverload3Cache =
        ConcurrentDictionary<Type * Type * Type, Func<IEnumerable, obj, Delegate, Delegate, obj>>()

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

    static let composedPredicateCache =
        ConcurrentDictionary<Type, Func<Func<obj, obj>, Delegate, Type, Delegate>>()

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

    /// Resolves and caches Enumerable methods for in-memory fallbacks
    static member GetEnumerableMethod(name: string, paramCount: int, typeArgs: Type array) : MethodInfo =
        let key =
            sprintf "%s_%d_%s" name paramCount (String.Join(",", typeArgs |> Array.map (fun t -> t.FullName)))

        enumerableGenericCache.GetOrAdd(
            (name, paramCount, key),
            fun _ ->
                let methodDef =
                    enumerableMethods
                    |> Array.find (fun m ->
                        m.Name = name
                        && m.GetParameters().Length = paramCount
                        && m.GetGenericArguments().Length = typeArgs.Length)

                methodDef.MakeGenericMethod(typeArgs)
        )

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

    /// Resolves a compiled invoker for Enumerable.Aggregate(source, func)
    static member GetAggregateInvoker(elemType: Type) : Func<IEnumerable, Delegate, obj> =
        aggOverload1Cache.GetOrAdd(
            elemType,
            fun t ->
                let methodInfo = LinqReflectionCache.GetEnumerableMethod("Aggregate", 2, [| t |])
                let srcParam = Expression.Parameter(typeof<IEnumerable>, "src")
                let delParam = Expression.Parameter(typeof<Delegate>, "del")

                let castSrc = Expression.Convert(srcParam, typedefof<seq<_>>.MakeGenericType(t))
                let funcType = typedefof<Func<_, _, _>>.MakeGenericType(t, t, t)
                let castDel = Expression.Convert(delParam, funcType)

                let callExpr = Expression.Call(null, methodInfo, castSrc, castDel)
                let boxResult = Expression.Convert(callExpr, typeof<obj>)

                Expression.Lambda<Func<IEnumerable, Delegate, obj>>(boxResult, srcParam, delParam).Compile()
        )

    /// Resolves a compiled invoker for Enumerable.Aggregate(source, seed, func)
    static member GetAggregateWithSeedInvoker(elemType: Type, accumType: Type) : Func<IEnumerable, obj, Delegate, obj> =
        aggOverload2Cache.GetOrAdd(
            (elemType, accumType),
            fun (tElem, tAcc) ->
                let methodInfo = LinqReflectionCache.GetEnumerableMethod("Aggregate", 3, [| tElem; tAcc |])
                let srcParam = Expression.Parameter(typeof<IEnumerable>, "src")
                let seedParam = Expression.Parameter(typeof<obj>, "seed")
                let delParam = Expression.Parameter(typeof<Delegate>, "del")

                let castSrc = Expression.Convert(srcParam, typedefof<seq<_>>.MakeGenericType(tElem))
                let castSeed = Expression.Convert(seedParam, tAcc)
                let funcType = typedefof<Func<_, _, _>>.MakeGenericType(tAcc, tElem, tAcc)
                let castDel = Expression.Convert(delParam, funcType)

                let callExpr = Expression.Call(null, methodInfo, castSrc, castSeed, castDel)
                let boxResult = Expression.Convert(callExpr, typeof<obj>)

                Expression
                    .Lambda<Func<IEnumerable, obj, Delegate, obj>>(boxResult, srcParam, seedParam, delParam)
                    .Compile()
        )

    /// Resolves a compiled invoker for Enumerable.Aggregate(source, seed, func, resultSelector)
    static member GetAggregateWithResultSelectorInvoker
        (elemType: Type, accumType: Type, resultType: Type)
        : Func<IEnumerable, obj, Delegate, Delegate, obj> =
        aggOverload3Cache.GetOrAdd(
            (elemType, accumType, resultType),
            fun (tElem, tAcc, tRes) ->
                let methodInfo = LinqReflectionCache.GetEnumerableMethod("Aggregate", 4, [| tElem; tAcc; tRes |])
                let srcParam = Expression.Parameter(typeof<IEnumerable>, "src")
                let seedParam = Expression.Parameter(typeof<obj>, "seed")
                let funcParam = Expression.Parameter(typeof<Delegate>, "func")
                let resSelParam = Expression.Parameter(typeof<Delegate>, "resSel")

                let castSrc = Expression.Convert(srcParam, typedefof<seq<_>>.MakeGenericType(tElem))
                let castSeed = Expression.Convert(seedParam, tAcc)
                let funcType = typedefof<Func<_, _, _>>.MakeGenericType(tAcc, tElem, tAcc)
                let castFunc = Expression.Convert(funcParam, funcType)
                let resSelType = typedefof<Func<_, _>>.MakeGenericType(tAcc, tRes)
                let castResSel = Expression.Convert(resSelParam, resSelType)

                let callExpr = Expression.Call(null, methodInfo, castSrc, castSeed, castFunc, castResSel)
                let boxResult = Expression.Convert(callExpr, typeof<obj>)

                Expression
                    .Lambda<Func<IEnumerable, obj, Delegate, Delegate, obj>>(
                        boxResult,
                        srcParam,
                        seedParam,
                        funcParam,
                        resSelParam
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

    static member private GetMaterializerBoolPredicateInvoker
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
        LinqReflectionCache.GetMaterializerBoolPredicateInvoker("Any", elemType)

    static member GetMaterializerAllInvoker(elemType: Type) =
        LinqReflectionCache.GetMaterializerBoolPredicateInvoker("All", elemType)

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
        LinqReflectionCache.GetMaterializerRowFetchInvoker("First", elemType)

    static member GetMaterializerLastInvoker(elemType: Type) =
        LinqReflectionCache.GetMaterializerRowFetchInvoker("Last", elemType)

    static member GetMaterializerSingleInvoker(elemType: Type) =
        LinqReflectionCache.GetMaterializerRowFetchInvoker("Single", elemType)

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
        LinqReflectionCache.GetMaterializerRowFetchOrDefaultInvoker("FirstOrDefault", elemType)

    static member GetMaterializerLastOrDefaultInvoker(elemType: Type) =
        LinqReflectionCache.GetMaterializerRowFetchOrDefaultInvoker("LastOrDefault", elemType)

    static member GetMaterializerSingleOrDefaultInvoker(elemType: Type) =
        LinqReflectionCache.GetMaterializerRowFetchOrDefaultInvoker("SingleOrDefault", elemType)

    /// Composes a client projection (Func<obj, obj>) and a compiled predicate (Delegate)
    /// into a strongly-typed Func<TSource, bool> that can be passed directly to IDataFrameMaterializer.
    static member CreateComposedPredicate
        (sourceType: Type, projFunc: Func<obj, obj>, predDelegate: Delegate, predArgType: Type)
        : Delegate =
        let factory =
            composedPredicateCache.GetOrAdd(
                sourceType,
                Func<Type, Func<Func<obj, obj>, Delegate, Type, Delegate>>(fun tSrc ->
                    let projParam = Expression.Parameter(typeof<Func<obj, obj>>, "proj")
                    let predParam = Expression.Parameter(typeof<Delegate>, "pred")
                    let argTypeParam = Expression.Parameter(typeof<Type>, "predArgType")

                    let srcParam = Expression.Parameter(tSrc, "src")
                    let boxedSrc = Expression.Convert(srcParam, typeof<obj>)
                    let projInvoke = Expression.Invoke(projParam, boxedSrc)

                    let targetPredType = typedefof<Func<_, _>>.MakeGenericType(predArgType, typeof<bool>)
                    let typedPred = Expression.Convert(predParam, targetPredType)
                    let castProjected = Expression.Convert(projInvoke, predArgType)
                    let predCall = Expression.Invoke(typedPred, castProjected)

                    let innerLambdaType = typedefof<Func<_, _>>.MakeGenericType(tSrc, typeof<bool>)
                    let innerLambda = Expression.Lambda(innerLambdaType, predCall, srcParam)

                    Expression
                        .Lambda<Func<Func<obj, obj>, Delegate, Type, Delegate>>(
                            Expression.Convert(innerLambda, typeof<Delegate>),
                            projParam,
                            predParam,
                            argTypeParam
                        )
                        .Compile())
            )

        factory.Invoke(projFunc, predDelegate, predArgType)

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

    static member private GetMaterializerElementAtByNameInvoker
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
        LinqReflectionCache.GetMaterializerElementAtByNameInvoker("ElementAt", elemType)

    static member GetMaterializerElementAtOrDefaultInvoker(elemType: Type) =
        LinqReflectionCache.GetMaterializerElementAtByNameInvoker("ElementAtOrDefault", elemType)

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

    static member private GetMaterializerExtremumByInvoker
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
        LinqReflectionCache.GetMaterializerExtremumByInvoker("MinBy", sourceType, keyType)

    static member GetMaterializerMaxByInvoker(sourceType: Type, keyType: Type) =
        LinqReflectionCache.GetMaterializerExtremumByInvoker("MaxBy", sourceType, keyType)

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

    /// Creates a strongly-typed Func<TAccum, TSource, TAccum> folder for numeric Sum/Min/Max/Average
    static member CreateNumericFolder
        (sourceType: Type, accumType: Type, opName: string, selectorOpt: Delegate option)
        : obj * Delegate * (obj -> int64 -> obj) =
        let accParam = Expression.Parameter(accumType, "acc")
        let srcParam = Expression.Parameter(sourceType, "src")

        let valExpr =
            match selectorOpt with
            | Some sel ->
                let selType =
                    typedefof<Func<_, _>>.MakeGenericType(sourceType, sel.GetType().GetGenericArguments().[1])

                Expression.Convert(Expression.Invoke(Expression.Constant(sel, selType), srcParam), accumType)
            | None -> Expression.Convert(srcParam, accumType)

        match opName with
        | "Sum" ->
            let body = Expression.Add(accParam, valExpr)
            let lambdaType = typedefof<Func<_, _, _>>.MakeGenericType(accumType, sourceType, accumType)
            let folder = Expression.Lambda(lambdaType, body, accParam, srcParam).Compile()
            let seed = Activator.CreateInstance(accumType)
            seed, folder, (fun (acc: obj) (_: int64) -> acc)

        | "Average" ->
            let body = Expression.Add(accParam, Expression.Convert(valExpr, typeof<double>))
            let lambdaType = typedefof<Func<_, _, _>>.MakeGenericType(typeof<double>, sourceType, typeof<double>)
            let folder = Expression.Lambda(lambdaType, body, accParam, srcParam).Compile()
            let seed = box 0.0

            let finisher (acc: obj) (count: int64) =
                if count = 0L then
                    invalidOp "Sequence contains no elements."

                box ((acc :?> double) / float count)

            seed, folder, finisher

        | "Min"
        | "Max" ->
            let isMin = (opName = "Min")
            let cmp = if isMin then Expression.LessThan(valExpr, accParam) else Expression.GreaterThan(valExpr, accParam)
            let cond = Expression.Condition(cmp, valExpr, accParam)
            let lambdaType = typedefof<Func<_, _, _>>.MakeGenericType(accumType, sourceType, accumType)
            let folder = Expression.Lambda(lambdaType, cond, accParam, srcParam).Compile()
            let seedField = accumType.GetField(if isMin then "MaxValue" else "MinValue")

            let seed =
                if isNull seedField then
                    Activator.CreateInstance(accumType)
                else
                    seedField.GetValue(null)

            let finisher (acc: obj) (count: int64) =
                if count = 0L then
                    let isNullable =
                        not (accumType.IsValueType && isNull (Nullable.GetUnderlyingType accumType))

                    if isNullable then
                        null
                    else
                        invalidOp "Sequence contains no elements."
                else
                    acc

            seed, folder, finisher

        | _ -> failwithf "Unsupported aggregation operator: %s" opName

    /// Creates the standard "empty/null" representation for a type,
    /// properly handling Reference Types, Nullable<T>, FSharpOption<'T>, and FSharpValueOption<'T>.
    static member GetEmptyOptionalValue(t: Type) : obj =
        if not (PolarsTypeHelper.AcceptsNull t) then
            null
        else
            let struct (_, kind) = PolarsTypeHelper.GetOptionalInfo t

            match kind with
            | OptionalKind.FSharpOption -> null
            | OptionalKind.FSharpValueOption -> Activator.CreateInstance(t)
            | OptionalKind.Nullable
            | _ -> null
