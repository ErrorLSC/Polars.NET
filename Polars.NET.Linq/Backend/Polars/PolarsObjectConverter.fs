namespace Polars.NET.Linq.Backend.Polars

open Polars.NET.Core
open Polars.NET.Core.Helpers
open Polars.NET.Linq
open Polars.NET.Linq.IR
open System
open System.Collections
open System.Linq
open System.Linq.Expressions
open System.Reflection

/// Dedicated converter for translating in-memory .NET collections, arrays, and objects
/// into native Polars Series, DataFrame, and LazyFrame handles.
module internal PolarsObjectConverter =

    /// Attempts to convert an in-memory object (array or IEnumerable) into a native SeriesHandle.
    let tryCreateSeriesHandle (name: string) (objVal: obj) : SeriesHandle option =
        match objVal with
        | null -> None
        | :? Array as arr ->
            try
                let h = SeriesFactory.Create(name, arr)
                if not (isNull h || h.IsInvalid) then Some h else None
            with _ ->
                None
        | :? IEnumerable as seq when seq.GetType() <> typeof<string> ->
            try
                let elemType =
                    match PolarsTypeHelper.TryGetEnumerableElementType(seq.GetType()) with
                    | null -> typeof<obj>
                    | t -> t

                let createGeneric =
                    typeof<SeriesFactory>
                        .GetMethod("CreateGenericType", BindingFlags.Public ||| BindingFlags.Static)
                        .MakeGenericMethod(elemType)

                let h = createGeneric.Invoke(null, [| box name; seq |]) :?> SeriesHandle
                if not (isNull h || h.IsInvalid) then Some h else None
            with _ ->
                None
        | _ -> None

    /// Attempts to extract or convert an in-memory object into a DataFrameHandle.
    let tryCreateDataFrameHandle (objVal: obj) : DataFrameHandle option =
        match objVal with
        | null -> None
        | :? DataFrameHandle as df -> Some df
        | value ->
            let valueType = value.GetType()
            let handleProp =
                valueType.GetProperty("Handle", BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
            if not (isNull handleProp) then
                match handleProp.GetValue value with
                | :? DataFrameHandle as df -> Some df
                | _ -> None
            elif valueType <> typeof<string> then
                match PolarsTypeHelper.TryGetEnumerableElementType valueType with
                | null -> None
                | elemType ->
                    try
                        let fromRows = LinqReflectionCache.FromRowsMethod.MakeGenericMethod elemType
                        let dfHandle = fromRows.Invoke(null, [| value |]) :?> DataFrameHandle
                        if not (isNull dfHandle || dfHandle.IsInvalid) then Some dfHandle else None
                    with _ ->
                        None
            else
                None

    /// Creates a 1-row DataFrame / LazyFrame from a single element instance.
    let fromSingleRow (elem: obj) (elemType: Type) : LazyFrameHandle option =
        if isNull elem then None
        else
            try
                let fromRows = LinqReflectionCache.FromRowsMethod.MakeGenericMethod elemType
                let typedArray = Array.CreateInstance(elemType, 1)
                typedArray.SetValue(elem, 0)
                let singleDf = fromRows.Invoke(null, [| box typedArray |]) :?> DataFrameHandle
                Some(PolarsWrapper.DataFrameToLazy singleDf)
            with _ ->
                None

    /// Creates a 1-row LogicalPlan.Scan from a single element instance.
    let fromSingleRowPlan (elem: obj) (elemType: Type) : LogicalPlan option =
        fromSingleRow elem elemType |> Option.map PolarsPhysicalLowerer.scanLazyFrame

    /// Attempts to extract or compile an in-memory object / queryable / handle into a LazyFrameHandle.
    let tryCreateLazyFrameHandle (objVal: obj) : LazyFrameHandle option =
        match objVal with
        | null -> None
        | :? IPolarsPlanSource as plan -> Some(plan.GetCompiledLazyFrameHandle())
        | :? LazyFrameHandle as lf -> Some(PolarsWrapper.LazyClone lf)
        | :? DataFrameHandle as df -> Some(PolarsWrapper.DataFrameToLazy df)
        | value ->
            let valueType = value.GetType()
            let handleProp =
                valueType.GetProperty("Handle", BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
            if not (isNull handleProp) then
                match handleProp.GetValue value with
                | :? LazyFrameHandle as lf -> Some(PolarsWrapper.LazyClone lf)
                | :? DataFrameHandle as df -> Some(PolarsWrapper.DataFrameToLazy df)
                | _ -> None
            else
                tryCreateDataFrameHandle value
                |> Option.map PolarsWrapper.DataFrameToLazy

    /// Uniformly extracts a LazyFrameHandle from an Expression.
    let resolveToLazyFrameHandle (expr: Expression) : LazyFrameHandle option =
        let cleanExpr = ExpressionPatterns.unwrap expr
        let rawObjOpt =
            match cleanExpr with
            | :? ConstantExpression as ce -> Some ce.Value
            | other ->
                match tryEvaluate other with
                | Some v -> Some v
                | None ->
                    try
                        let lambda = Expression.Lambda<Func<obj>>(Expression.Convert(other, typeof<obj>)).Compile()
                        Some(lambda.Invoke())
                    with _ ->
                        None
        match rawObjOpt with
        | None | Some null -> None
        | Some v -> tryCreateLazyFrameHandle v

    /// Compiles any sub-expression into an isolated LazyFrameHandle by finding the root queryable.
    let compileSubtreeToLazyFrame (expr: Expression) : LazyFrameHandle option =
        match expr with
        | :? ConstantExpression as c when not (isNull c.Value) ->
            match c.Value with
            | :? IPolarsPlanSource as planSource -> Some(planSource.GetCompiledLazyFrameHandle())
            | _ -> None
        | _ ->
            let rec findRootQueryable (e: Expression) : IQueryable option =
                match e with
                | :? ConstantExpression as c when not (isNull c.Value) ->
                    match c.Value with
                    | :? IQueryable as q -> Some q
                    | _ -> None
                | :? MethodCallExpression as mc -> mc.Arguments |> Seq.tryPick findRootQueryable
                | _ -> None

            match findRootQueryable expr with
            | Some rootQuery ->
                try
                    let subQuery = rootQuery.Provider.CreateQuery expr
                    match subQuery with
                    | :? IPolarsPlanSource as ps -> Some(ps.GetCompiledLazyFrameHandle())
                    | _ -> None
                with _ ->
                    None
            | None ->
                match tryEvaluate expr with
                | Some(:? IPolarsPlanSource as ps) -> Some(ps.GetCompiledLazyFrameHandle())
                | _ -> None

    /// Resolves an inner sequence expression to a LazyFrameHandle by trying queryable subtree compilation,
    /// handle unwrapping, and in-memory IEnumerable conversion.
    let resolveSequenceLazyFrame (expr: Expression) : LazyFrameHandle option =
        compileSubtreeToLazyFrame expr
        |> Option.orElseWith (fun () -> resolveToLazyFrameHandle expr)

    /// Resolves an inner sequence expression to a LogicalPlan (grafting pure LogicalPlan subtrees when possible).
    let resolveSequencePlan (expr: Expression) : LogicalPlan option =
        let tryFromExplainable (objVal: obj) : LogicalPlan option =
            match objVal with
            | :? IPolarsInternalExplainable as exp -> exp.TryGetPureLogicalPlan()
            | _ -> None

        let rec findRootQueryable (e: Expression) : IQueryable option =
            match e with
            | :? ConstantExpression as c when not (isNull c.Value) ->
                match c.Value with
                | :? IQueryable as q -> Some q
                | _ -> None
            | :? MethodCallExpression as mc -> mc.Arguments |> Seq.tryPick findRootQueryable
            | _ -> None

        let purePlanOpt =
            match expr with
            | :? ConstantExpression as c when not (isNull c.Value) ->
                tryFromExplainable c.Value
            | _ ->
                match findRootQueryable expr with
                | Some rootQuery ->
                    try
                        let subQuery = rootQuery.Provider.CreateQuery expr
                        tryFromExplainable subQuery
                    with _ ->
                        None
                | None ->
                    match tryEvaluate expr with
                    | Some v when not (isNull v) -> tryFromExplainable v
                    | _ -> None

        purePlanOpt
        |> Option.orElseWith (fun () ->
            resolveSequenceLazyFrame expr
            |> Option.map PolarsPhysicalLowerer.scanLazyFrame)

