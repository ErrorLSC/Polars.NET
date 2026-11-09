namespace Polars.NET.Linq.Backend.Polars

open Polars.NET.Core
open Polars.NET.Core.Wrappers
open Kaminari.Samidare.IR
open System
open System.Linq.Expressions
open System.Reflection
open Kaminari.Sazanami.Helpers
open Kaminari.Samidare

/// Dedicated converter for translating in-memory .NET collections, arrays, and objects
/// into native Polars Series, DataFrame, and LazyFrame handles.
module internal PolarsObjectConverter =

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
                match ClrTypeHelper.TryGetEnumerableElementType valueType with
                | null -> None
                | elemType ->
                    try
                        let fromRows = MaterializerInvokers.FromRowsMethod.MakeGenericMethod elemType
                        let dfHandle = fromRows.Invoke(null, [| value |]) :?> DataFrameHandle
                        if not (isNull dfHandle || dfHandle.IsInvalid) then Some dfHandle else None
                    with _ ->
                        None
            else
                None

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
                match PartialEvaluator.tryEvaluate other with
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

    /// Binds a backend-native runtime object (compiled queryable, LazyFrame/DataFrame handle, or a wrapper
    /// exposing a Handle property) as an opaque table source. In-memory collections return None.
    let tryBindExternalSource (value: obj) : ILogicalTableSource option =
        let lfOpt =
            match value with
            | null -> None
            | :? IPolarsPlanSource as plan -> Some(plan.GetCompiledLazyFrameHandle())
            | :? LazyFrameHandle as lf -> Some(PolarsWrapper.LazyClone lf)
            | :? DataFrameHandle as df -> Some(PolarsWrapper.DataFrameToLazy df)
            | v ->
                let handleProp =
                    v.GetType().GetProperty("Handle", BindingFlags.Public ||| BindingFlags.NonPublic ||| BindingFlags.Instance)
                if isNull handleProp then
                    None
                else
                    match handleProp.GetValue v with
                    | :? LazyFrameHandle as lf -> Some(PolarsWrapper.LazyClone lf)
                    | :? DataFrameHandle as df -> Some(PolarsWrapper.DataFrameToLazy df)
                    | _ -> None
        lfOpt |> Option.map PolarsPhysicalLowerer.tableSourceOfLazyFrame
