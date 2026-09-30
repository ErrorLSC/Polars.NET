namespace Polars.NET.Linq.Provider

open System
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core

/// Global reflection cache for LINQ methods to eliminate runtime lookup overhead.
[<AbstractClass; Sealed>]
type internal LinqReflectionCache private () =
    // Static method definition pools
    static let queryableMethods = typeof<Queryable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
    static let enumerableMethods = typeof<Enumerable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)

    // Pre-resolved Queryable definitions as static fields
    static let whereMethodDef =
        queryableMethods
        |> Array.find (fun m -> 
            m.Name = "Where" && 
            m.GetParameters().Length = 2 && 
            m.GetParameters().[1].ParameterType.GetGenericTypeDefinition() = typedefof<Expression<Func<_, _>>>)

    static let countMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "Count" && m.GetParameters().Length = 1)
    static let longCountMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "LongCount" && m.GetParameters().Length = 1)
    static let anyMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "Any" && m.GetParameters().Length = 1)
    static let singleMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "Single" && m.GetParameters().Length = 1)
    static let singleOrDefaultMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "SingleOrDefault" && m.GetParameters().Length = 1)
    static let firstMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "First" && m.GetParameters().Length = 1)
    static let firstOrDefaultMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "FirstOrDefault" && m.GetParameters().Length = 1)
    static let lastMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "Last" && m.GetParameters().Length = 1)
    static let lastOrDefaultMethodDef = queryableMethods |> Array.find (fun m -> m.Name = "LastOrDefault" && m.GetParameters().Length = 1)

    // Static thread-safe caches for constructed generic methods and compiled expression delegates
    static let materializerDelegateCache = ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable>>()
    static let queryableGenericCache = ConcurrentDictionary<string * Type, MethodInfo>()
    static let enumerableGenericCache = ConcurrentDictionary<string * int * string, MethodInfo>()
    static let aggOverload1Cache = ConcurrentDictionary<Type, Func<IEnumerable, Delegate, obj>>()
    static let aggOverload2Cache = ConcurrentDictionary<Type * Type, Func<IEnumerable, obj, Delegate, obj>>()
    static let aggOverload3Cache = ConcurrentDictionary<Type * Type * Type, Func<IEnumerable, obj, Delegate, Delegate, obj>>()
    static let matAggBySeedCache = ConcurrentDictionary<Type * Type * Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, Delegate, obj, IEnumerable>>()
    static let matAggByFactoryCache = ConcurrentDictionary<Type * Type * Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate, Delegate, Delegate, obj, IEnumerable>>()
    static let chunkDelegateCache = ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, int, IEnumerable>>()

    // Cache: elemType -> Func<IDataFrameMaterializer, DataFrameHandle, Delegate, bool> for Any(predicate)
    static let matAnyCache =
        ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate, bool>>()

    // Cache: elemType -> Func<IDataFrameMaterializer, DataFrameHandle, Delegate, bool> for All(predicate)
    static let matAllCache =
        ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate, bool>>()

    // Cache: (TIn, TOut) -> Func<Delegate, obj, obj> for fast typed projection invoke: (proj, entity) -> result
    static let projectInvokerCache =
        ConcurrentDictionary<Type * Type, Func<Delegate, obj, obj>>()

    // Cache: (TMid) -> Func<Delegate, obj, bool> for fast typed predicate invoke: (pred, entity) -> bool
    static let predicateInvokerCache =
        ConcurrentDictionary<Type, Func<Delegate, obj, bool>>()

    // Cache: elemType -> Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj> for First(predicate)
    static let matFirstCache =
        ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj>>()

    // Cache: elemType -> Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj> for FirstOrDefault(predicate, defaultValue)
    static let matFirstOrDefaultCache =
        ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj>>()
    // Cache: elemType -> Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj> for Last(predicate)
    static let matLastCache =
        ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj>>()

    // Cache: elemType -> Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj> for LastOrDefault(predicate, defaultValue)
    static let matLastOrDefaultCache =
        ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj>>()

    /// Cached MethodInfo for Queryable.Where definition
    static member val QueryableWhereDef = whereMethodDef with get

    static member GetQueryableWhere(elemType: Type) =
        queryableGenericCache.GetOrAdd(("Where", elemType), fun _ -> whereMethodDef.MakeGenericMethod(elemType))

    /// Resolves and caches 1-argument Queryable scalar methods (Count, Any, First, etc.)
    static member GetQueryableScalar(methodName: string, elemType: Type) : MethodInfo =
        queryableGenericCache.GetOrAdd((methodName, elemType), fun _ ->
            let mDef =
                match methodName with
                | "Count" -> countMethodDef
                | "LongCount" -> longCountMethodDef
                | "Any" -> anyMethodDef
                | "Single" -> singleMethodDef
                | "SingleOrDefault" -> singleOrDefaultMethodDef
                | "First" -> firstMethodDef
                | "FirstOrDefault" -> firstOrDefaultMethodDef
                | "Last" -> lastMethodDef
                | "LastOrDefault" -> lastOrDefaultMethodDef
                | other -> 
                    queryableMethods 
                    |> Array.find (fun m -> m.Name = other && m.GetParameters().Length = 1)
            mDef.MakeGenericMethod(elemType)
        )

    /// Resolves and caches Enumerable methods for in-memory fallbacks
    static member GetEnumerableMethod(name: string, paramCount: int, typeArgs: Type array) : MethodInfo =
        let key = sprintf "%s_%d_%s" name paramCount (String.Join(",", typeArgs |> Array.map (fun t -> t.FullName)))
        enumerableGenericCache.GetOrAdd((name, paramCount, key), fun _ ->
            let methodDef =
                enumerableMethods
                |> Array.find (fun m ->
                    m.Name = name &&
                    m.GetParameters().Length = paramCount &&
                    m.GetGenericArguments().Length = typeArgs.Length)
            methodDef.MakeGenericMethod(typeArgs)
        )

    /// Resolves a strongly-typed, zero-reflection materializer invoker for the specified element type
    static member GetMaterializerInvoker (elemType: Type) : Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable> =
        materializerDelegateCache.GetOrAdd(elemType, fun t ->
            let methodInfo =
                typeof<IDataFrameMaterializer>.GetMethods()
                |> Array.find (fun m -> m.Name = "Materialize" && m.IsGenericMethodDefinition && m.GetParameters().Length = 1)
                |> fun m -> m.MakeGenericMethod(t)

            let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
            let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
            let callExpr = Expression.Call(matParam, methodInfo, handleParam)
            let castResult = Expression.Convert(callExpr, typeof<IEnumerable>)

            Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable>>(castResult, matParam, handleParam).Compile()
        )

    /// Resolves a compiled invoker for Enumerable.Aggregate(source, func)
    static member GetAggregateInvoker(elemType: Type) : Func<IEnumerable, Delegate, obj> =
        aggOverload1Cache.GetOrAdd(elemType, fun t ->
            let methodInfo =
                typeof<Enumerable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                |> Array.find (fun m -> m.Name = "Aggregate" && m.GetParameters().Length = 2)
                |> fun m -> m.MakeGenericMethod(t)

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
        aggOverload2Cache.GetOrAdd((elemType, accumType), fun (tElem, tAcc) ->
            let methodInfo =
                typeof<Enumerable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                |> Array.find (fun m -> m.Name = "Aggregate" && m.GetParameters().Length = 3)
                |> fun m -> m.MakeGenericMethod(tElem, tAcc)

            let srcParam = Expression.Parameter(typeof<IEnumerable>, "src")
            let seedParam = Expression.Parameter(typeof<obj>, "seed")
            let delParam = Expression.Parameter(typeof<Delegate>, "del")

            let castSrc = Expression.Convert(srcParam, typedefof<seq<_>>.MakeGenericType(tElem))
            let castSeed = Expression.Convert(seedParam, tAcc)
            let funcType = typedefof<Func<_, _, _>>.MakeGenericType(tAcc, tElem, tAcc)
            let castDel = Expression.Convert(delParam, funcType)

            let callExpr = Expression.Call(null, methodInfo, castSrc, castSeed, castDel)
            let boxResult = Expression.Convert(callExpr, typeof<obj>)

            Expression.Lambda<Func<IEnumerable, obj, Delegate, obj>>(boxResult, srcParam, seedParam, delParam).Compile()
        )

    /// Resolves a compiled invoker for Enumerable.Aggregate(source, seed, func, resultSelector)
    static member GetAggregateWithResultSelectorInvoker(elemType: Type, accumType: Type, resultType: Type) : Func<IEnumerable, obj, Delegate, Delegate, obj> =
        aggOverload3Cache.GetOrAdd((elemType, accumType, resultType), fun (tElem, tAcc, tRes) ->
            let methodInfo =
                typeof<Enumerable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
                |> Array.find (fun m -> m.Name = "Aggregate" && m.GetParameters().Length = 4)
                |> fun m -> m.MakeGenericMethod(tElem, tAcc, tRes)

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

            Expression.Lambda<Func<IEnumerable, obj, Delegate, Delegate, obj>>(boxResult, srcParam, seedParam, funcParam, resSelParam).Compile()
        )

    /// Resolves compiled invoker for IDataFrameMaterializer.AggregateBy with static seed
    static member GetMaterializerAggBySeedInvoker(srcType: Type, keyType: Type, accumType: Type) =
        matAggBySeedCache.GetOrAdd((srcType, keyType, accumType), fun (tSrc, tKey, tAcc) ->
            let methodInfo =
                typeof<IDataFrameMaterializer>.GetMethods()
                |> Array.find (fun m ->
                    m.Name = "AggregateBy" &&
                    m.GetGenericArguments().Length = 3 &&
                    m.GetParameters().[2].ParameterType = m.GetGenericArguments().[2])
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

            let callExpr = Expression.Call(matParam, methodInfo, handleParam, castKeySel, castSeed, castFunc, castComp)
            let castResult = Expression.Convert(callExpr, typeof<IEnumerable>)

            Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, Delegate, obj, IEnumerable>>(
                castResult, matParam, handleParam, keySelParam, seedParam, funcParam, compParam
            ).Compile()
        )

    /// Resolves compiled invoker for IDataFrameMaterializer.AggregateBy with seedFactory
    static member GetMaterializerAggByFactoryInvoker(srcType: Type, keyType: Type, accumType: Type) =
        matAggByFactoryCache.GetOrAdd((srcType, keyType, accumType), fun (tSrc, tKey, tAcc) ->
            let methodInfo =
                typeof<IDataFrameMaterializer>.GetMethods()
                |> Array.find (fun m ->
                    m.Name = "AggregateBy" &&
                    m.GetGenericArguments().Length = 3 &&
                    m.GetParameters().[2].ParameterType.IsGenericType &&
                    m.GetParameters().[2].ParameterType.GetGenericTypeDefinition() = typedefof<Func<_, _>>)
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

            let callExpr = Expression.Call(matParam, methodInfo, handleParam, castKeySel, castSeedFactory, castFunc, castComp)
            let castResult = Expression.Convert(callExpr, typeof<IEnumerable>)

            Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, Delegate, Delegate, obj, IEnumerable>>(
                castResult, matParam, handleParam, keySelParam, seedFactoryParam, funcParam, compParam
            ).Compile()
        )

    /// Resolves a compiled invoker for IDataFrameMaterializer.Chunk<T>
    static member GetMaterializerChunkInvoker(elemType: Type) : Func<IDataFrameMaterializer, DataFrameHandle, int, IEnumerable> =
        chunkDelegateCache.GetOrAdd(elemType, fun t ->
            let methodInfo =
                typeof<IDataFrameMaterializer>.GetMethods()
                |> Array.find (fun m -> m.Name = "Chunk" && m.IsGenericMethodDefinition && m.GetParameters().Length = 2)
                |> fun m -> m.MakeGenericMethod(t)

            let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
            let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
            let sizeParam = Expression.Parameter(typeof<int>, "chunkSize")

            let callExpr = Expression.Call(matParam, methodInfo, handleParam, sizeParam)
            let castResult = Expression.Convert(callExpr, typeof<IEnumerable>)

            Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, int, IEnumerable>>(
                castResult, matParam, handleParam, sizeParam
            ).Compile()
        )

    /// Resolves a compiled invoker for IDataFrameMaterializer.Any<TSource>(handle, predicate)
    static member GetMaterializerAnyInvoker(elemType: Type) : Func<IDataFrameMaterializer, DataFrameHandle, Delegate, bool> =
        matAnyCache.GetOrAdd(elemType, fun t ->
            let methodInfo =
                typeof<IDataFrameMaterializer>.GetMethods()
                |> Array.find (fun m -> m.Name = "Any" && m.IsGenericMethodDefinition && m.GetParameters().Length = 2)
                |> fun m -> m.MakeGenericMethod(t)

            let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
            let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
            let predParam = Expression.Parameter(typeof<Delegate>, "pred")

            let funcType = typedefof<Func<_, _>>.MakeGenericType(t, typeof<bool>)
            let castPred = Expression.Condition(
                Expression.Equal(predParam, Expression.Constant(null, typeof<Delegate>)),
                Expression.Constant(null, funcType),
                Expression.Convert(predParam, funcType)
            )

            let callExpr = Expression.Call(matParam, methodInfo, handleParam, castPred)

            Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, bool>>(
                callExpr, matParam, handleParam, predParam
            ).Compile()
        )

    /// Resolves a compiled invoker for IDataFrameMaterializer.All<TSource>(handle, predicate)
    static member GetMaterializerAllInvoker(elemType: Type) : Func<IDataFrameMaterializer, DataFrameHandle, Delegate, bool> =
        matAllCache.GetOrAdd(elemType, fun t ->
            let methodInfo =
                typeof<IDataFrameMaterializer>.GetMethods()
                |> Array.find (fun m -> m.Name = "All" && m.IsGenericMethodDefinition && m.GetParameters().Length = 2)
                |> fun m -> m.MakeGenericMethod(t)

            let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
            let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
            let predParam = Expression.Parameter(typeof<Delegate>, "pred")

            let funcType = typedefof<Func<_, _>>.MakeGenericType(t, typeof<bool>)
            let castPred = Expression.Convert(predParam, funcType)

            let callExpr = Expression.Call(matParam, methodInfo, handleParam, castPred)

            Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, bool>>(
                callExpr, matParam, handleParam, predParam
            ).Compile()
        )

    /// Resolves a compiled invoker for IDataFrameMaterializer.First<TSource>(handle, predicate)
    static member GetMaterializerFirstInvoker(elemType: Type) : Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj> =
        matFirstCache.GetOrAdd(elemType, fun t ->
            let methodInfo =
                typeof<IDataFrameMaterializer>.GetMethods()
                |> Array.find (fun m -> m.Name = "First" && m.IsGenericMethodDefinition && m.GetParameters().Length = 2)
                |> fun m -> m.MakeGenericMethod(t)

            let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
            let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
            let predParam = Expression.Parameter(typeof<Delegate>, "pred")

            let funcType = typedefof<Func<_, _>>.MakeGenericType(t, typeof<bool>)
            let castPred = Expression.Condition(
                Expression.Equal(predParam, Expression.Constant(null, typeof<Delegate>)),
                Expression.Constant(null, funcType),
                Expression.Convert(predParam, funcType)
            )

            let callExpr = Expression.Call(matParam, methodInfo, handleParam, castPred)
            let boxResult = Expression.Convert(callExpr, typeof<obj>)

            Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj>>(
                boxResult, matParam, handleParam, predParam
            ).Compile()
        )

    /// Resolves a compiled invoker for IDataFrameMaterializer.FirstOrDefault<TSource>(handle, predicate, defaultValue)
    static member GetMaterializerFirstOrDefaultInvoker(elemType: Type) : Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj> =
        matFirstOrDefaultCache.GetOrAdd(elemType, fun t ->
            let methodInfo =
                typeof<IDataFrameMaterializer>.GetMethods()
                |> Array.find (fun m -> m.Name = "FirstOrDefault" && m.IsGenericMethodDefinition && m.GetParameters().Length = 3)
                |> fun m -> m.MakeGenericMethod(t)

            let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
            let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
            let predParam = Expression.Parameter(typeof<Delegate>, "pred")
            let defValParam = Expression.Parameter(typeof<obj>, "defaultVal")

            let funcType = typedefof<Func<_, _>>.MakeGenericType(t, typeof<bool>)
            let castPred = Expression.Condition(
                Expression.Equal(predParam, Expression.Constant(null, typeof<Delegate>)),
                Expression.Constant(null, funcType),
                Expression.Convert(predParam, funcType)
            )
            let castDefVal = Expression.Convert(defValParam, t)

            let callExpr = Expression.Call(matParam, methodInfo, handleParam, castPred, castDefVal)
            let boxResult = Expression.Convert(callExpr, typeof<obj>)

            Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj>>(
                boxResult, matParam, handleParam, predParam, defValParam
            ).Compile()
        )

    /// Invokes a client projection delegate Func<TIn, TOut> with zero reflection overhead (strongly-typed invoke).
    static member InvokeClientProjection(projDelegate: Delegate, entity: obj, inType: Type, outType: Type) : obj =
        let invoker =
            projectInvokerCache.GetOrAdd((inType, outType), fun (tIn, tOut) ->
                let delParam = Expression.Parameter(typeof<Delegate>, "del")
                let argParam = Expression.Parameter(typeof<obj>, "arg")

                let funcType = typedefof<Func<_, _>>.MakeGenericType(tIn, tOut)
                let castDel = Expression.Convert(delParam, funcType)
                let castArg = Expression.Convert(argParam, tIn)

                let callExpr = Expression.Invoke(castDel, castArg)
                let boxResult = Expression.Convert(callExpr, typeof<obj>)

                Expression.Lambda<Func<Delegate, obj, obj>>(boxResult, delParam, argParam).Compile()
            )
        invoker.Invoke(projDelegate, entity)

    /// Invokes a client predicate delegate Func<T, bool> with zero reflection overhead.
    static member InvokeClientPredicate(predDelegate: Delegate, entity: obj, argType: Type) : bool =
        let invoker =
            predicateInvokerCache.GetOrAdd(argType, fun t ->
                let delParam = Expression.Parameter(typeof<Delegate>, "del")
                let argParam = Expression.Parameter(typeof<obj>, "arg")

                let funcType = typedefof<Func<_, _>>.MakeGenericType(t, typeof<bool>)
                let castDel = Expression.Convert(delParam, funcType)
                let castArg = Expression.Convert(argParam, t)

                let callExpr = Expression.Invoke(castDel, castArg)

                Expression.Lambda<Func<Delegate, obj, bool>>(callExpr, delParam, argParam).Compile()
            )
        invoker.Invoke(predDelegate, entity)
    /// Resolves a compiled invoker for IDataFrameMaterializer.Last<TSource>(handle, predicate)
    static member GetMaterializerLastInvoker(elemType: Type) : Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj> =
        matLastCache.GetOrAdd(elemType, fun t ->
            let methodInfo =
                typeof<IDataFrameMaterializer>.GetMethods()
                |> Array.find (fun m -> m.Name = "Last" && m.IsGenericMethodDefinition && m.GetParameters().Length = 2)
                |> fun m -> m.MakeGenericMethod(t)

            let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
            let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
            let predParam = Expression.Parameter(typeof<Delegate>, "pred")

            let funcType = typedefof<Func<_, _>>.MakeGenericType(t, typeof<bool>)
            let castPred = Expression.Condition(
                Expression.Equal(predParam, Expression.Constant(null, typeof<Delegate>)),
                Expression.Constant(null, funcType),
                Expression.Convert(predParam, funcType)
            )

            let callExpr = Expression.Call(matParam, methodInfo, handleParam, castPred)
            let boxResult = Expression.Convert(callExpr, typeof<obj>)

            Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj>>(
                boxResult, matParam, handleParam, predParam
            ).Compile()
        )

    /// Resolves a compiled invoker for IDataFrameMaterializer.LastOrDefault<TSource>(handle, predicate, defaultValue)
    static member GetMaterializerLastOrDefaultInvoker(elemType: Type) : Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj> =
        matLastOrDefaultCache.GetOrAdd(elemType, fun t ->
            let methodInfo =
                typeof<IDataFrameMaterializer>.GetMethods()
                |> Array.find (fun m -> m.Name = "LastOrDefault" && m.IsGenericMethodDefinition && m.GetParameters().Length = 3)
                |> fun m -> m.MakeGenericMethod(t)

            let matParam = Expression.Parameter(typeof<IDataFrameMaterializer>, "mat")
            let handleParam = Expression.Parameter(typeof<DataFrameHandle>, "handle")
            let predParam = Expression.Parameter(typeof<Delegate>, "pred")
            let defValParam = Expression.Parameter(typeof<obj>, "defaultVal")

            let funcType = typedefof<Func<_, _>>.MakeGenericType(t, typeof<bool>)
            let castPred = Expression.Condition(
                Expression.Equal(predParam, Expression.Constant(null, typeof<Delegate>)),
                Expression.Constant(null, funcType),
                Expression.Convert(predParam, funcType)
            )
            let castDefVal = Expression.Convert(defValParam, t)

            let callExpr = Expression.Call(matParam, methodInfo, handleParam, castPred, castDefVal)
            let boxResult = Expression.Convert(callExpr, typeof<obj>)

            Expression.Lambda<Func<IDataFrameMaterializer, DataFrameHandle, Delegate, obj, obj>>(
                boxResult, matParam, handleParam, predParam, defValParam
            ).Compile()
        )