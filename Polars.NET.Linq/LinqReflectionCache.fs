namespace Polars.NET.Linq

open System
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open System.Linq
open System.Linq.Expressions
open System.Reflection
open Polars.NET.Core.Helpers

/// Global reflection cache for LINQ / Enumerable / Queryable methods and managed delegate factories.
[<AbstractClass; Sealed>]
type internal LinqReflectionCache private () =
    static let enumerableMethods =
        typeof<Enumerable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)

    static let queryableMethods =
        typeof<Queryable>.GetMethods(BindingFlags.Public ||| BindingFlags.Static)

    static let queryProviderExecuteGenericMethod =
        typeof<IQueryProvider>.GetMethods()
        |> Array.find (fun m -> m.Name = "Execute" && m.IsGenericMethod)

    static let queryProviderExecuteCache =
        ConcurrentDictionary<Type, MethodInfo>()

    static let enumerableGenericCache =
        ConcurrentDictionary<string * int * string, MethodInfo>()

    // Separate from enumerableGenericCache: Enumerable and Queryable share method names and arities
    // (e.g. Count, Where), so a shared key space would return the wrong declaring type.
    static let queryableGenericCache =
        ConcurrentDictionary<string * int * string, MethodInfo>()

    static let aggOverload1Cache =
        ConcurrentDictionary<Type, Func<IEnumerable, Delegate, obj>>()

    static let aggOverload2Cache =
        ConcurrentDictionary<Type * Type, Func<IEnumerable, obj, Delegate, obj>>()

    static let aggOverload3Cache =
        ConcurrentDictionary<Type * Type * Type, Func<IEnumerable, obj, Delegate, Delegate, obj>>()

    static let composedPredicateCache =
        ConcurrentDictionary<Type, Func<Func<obj, obj>, Delegate, Type, Delegate>>()

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

    /// Resolves and caches Queryable methods for in-memory fallbacks
    static member GetQueryableMethod(name: string, paramCount: int, typeArgs: Type array) : MethodInfo =
        let key =
            sprintf "%s_%d_%s" name paramCount (String.Join(",", typeArgs |> Array.map (fun t -> t.FullName)))

        queryableGenericCache.GetOrAdd(
            (name, paramCount, key),
            fun _ ->
                let methodDef =
                    queryableMethods
                    |> Array.find (fun m ->
                        m.Name = name
                        && m.GetParameters().Length = paramCount
                        && m.GetGenericArguments().Length = typeArgs.Length)

                methodDef.MakeGenericMethod(typeArgs)
        )

    /// Resolves and caches IQueryProvider.Execute<TResult> MethodInfo
    static member GetQueryProviderExecuteMethod(resultType: Type) : MethodInfo =
        queryProviderExecuteCache.GetOrAdd(resultType, fun t -> queryProviderExecuteGenericMethod.MakeGenericMethod t)

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
