namespace Polars.NET.Linq.Provider

open System
open System.Collections
open System.Collections.Concurrent
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core

/// Global reflection cache for LINQ methods to eliminate runtime method lookup overhead
type internal LinqReflectionCache private () =
    static let queryableMethods = typeof<Queryable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)
    static let enumerableMethods = typeof<Enumerable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)

    /// Thread-safe cache holding compiled delegate invokers for IDataFrameMaterializer.Materialize<T>
    static let materializerDelegateCache = ConcurrentDictionary<Type, Func<IDataFrameMaterializer, DataFrameHandle, IEnumerable>>()

    // Pre-resolved Queryable definitions
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

    // Cache for constructed generic methods
    static let queryableGenericCache = ConcurrentDictionary<string * Type, MethodInfo>()
    static let enumerableGenericCache = ConcurrentDictionary<string * int * string, MethodInfo>()

    // Cache: elemType -> Func<IEnumerable, Delegate, obj> for Aggregate(source, func)
    static let aggOverload1Cache = 
        ConcurrentDictionary<Type, Func<IEnumerable, Delegate, obj>>()

    // Cache: (elemType, accumType) -> Func<IEnumerable, obj, Delegate, obj> for Aggregate(source, seed, func)
    static let aggOverload2Cache = 
        ConcurrentDictionary<Type * Type, Func<IEnumerable, obj, Delegate, obj>>()

    // Cache: (elemType, accumType, resultType) -> Func<IEnumerable, obj, Delegate, Delegate, obj> for Aggregate(source, seed, func, resultSelector)
    static let aggOverload3Cache = 
        ConcurrentDictionary<Type * Type * Type, Func<IEnumerable, obj, Delegate, Delegate, obj>>()

    static member QueryableWhereDef = whereMethodDef
    
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