namespace Polars.NET.Linq.Provider

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Core.Helpers

type internal SortSpec = {
    Expr: ExprHandle
    Descending: bool
    NullsLast: bool
}
/// Internal representation of supported operations in the LINQ query pipeline
[<RequireQualifiedAccess>]
type internal QueryOp =
    | Filter of ExprHandle
    | Limit of uint32
    | Sort of SortSpec list
    | SelectPassthrough

/// Strongly-typed IQueryProvider backed by Polars.NET.Core
type PolarsQueryProvider(initialLazyFrame: LazyFrameHandle, materializer: IDataFrameMaterializer) =
    // Clone handle to prevent native pipeline destruction
    let clonedLf = PolarsWrapper.LazyClone initialLazyFrame

    member _.LazyFrame = clonedLf
    member _.Materializer = materializer

    interface IQueryProvider with
        member _.CreateQuery(expression: Expression) : IQueryable =
            let elemType =
                match PolarsTypeHelper.TryGetEnumerableElementType(expression.Type) with
                | null -> typedefof<obj>
                | t -> t

            let queryType = typedefof<PolarsQuery<_>>.MakeGenericType(elemType)
            Activator.CreateInstance(queryType, [| box clonedLf; box materializer; box expression |]) :?> IQueryable

        member this.CreateQuery<'TElement>(expression: Expression) : IQueryable<'TElement> =
            PolarsQuery<'TElement>(clonedLf, materializer, expression) :> IQueryable<'TElement>

        member _.Execute(expression: Expression) : obj =
            failwith "Scalar execution is not supported yet"

        member _.Execute<'TResult>(expression: Expression) : 'TResult =
            failwith "Scalar execution is not supported yet"

and PolarsQuery<'T>(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer, expression: Expression) =
    let lfCloned = PolarsWrapper.LazyClone lazyFrameHandle
    let provider = PolarsQueryProvider(lfCloned, materializer)

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer) =
        PolarsQuery<'T>(lazyFrameHandle, materializer, Expression.Constant(null, typeof<IQueryable<'T>>))

    static member private ResolveMaterializer(mat: IDataFrameMaterializer) =
        if not (isNull mat) then mat
        elif not (isNull DataFrameMaterializerRegistry.Default) then DataFrameMaterializerRegistry.Default
        else failwith "No DataFrameMaterializer registered. Ensure upper API layers configured a materializer."

    /// Applies a list of QueryOp to a target LazyFrameHandle in forward order.
    /// Single Source of Truth for translating QueryOp to Polars Native LazyFrame transformations.
    static member private ApplyNativeOps (sourceLf: LazyFrameHandle) (ops: QueryOp list) : LazyFrameHandle =
        ops
        |> List.fold (fun currentLf op ->
            match op with
            | QueryOp.Filter exprHandle -> PolarsWrapper.LazyFilter(currentLf, exprHandle)
            | QueryOp.Limit count       -> PolarsWrapper.LazySlice(currentLf,0, count)
            | QueryOp.Sort specs ->
                let exprs = specs |> List.map (fun s -> s.Expr) |> List.toArray
                let descending = specs |> List.map (fun s -> s.Descending) |> List.toArray
                let nullsLast = specs |> List.map (fun s -> s.NullsLast) |> List.toArray

                // Native call directly consuming arrays
                PolarsWrapper.LazyFrameSort(currentLf, exprs, descending, nullsLast, maintainOrder = false)
            | QueryOp.SelectPassthrough -> currentLf
        ) (PolarsWrapper.LazyClone sourceLf)

    /// Parses the LINQ expression tree into native ops and any trailing client predicates
    member private this.CompilePipeline() : QueryOp list * (Func<'T, bool> list) =
        
        // Merges contiguous Sort operations by peeking at the accumulator's head
        let addSort (spec: SortSpec) (ops: QueryOp list) =
            match ops with
            | QueryOp.Sort specs :: tail -> QueryOp.Sort (spec :: specs) :: tail
            | _ -> QueryOp.Sort [spec] :: ops

        let rec analyze (expr: Expression) (opsAcc: QueryOp list) (clientAcc: Func<'T, bool> list) =
            match expr with
            | MethodCall(methodInfo, null, [ sourceExpr; StripQuotes (Lambda([ param ], body)) ]) ->
                match methodInfo.Name with
                | "Where" ->
                    match ExprTranslator.tryTranslate param.Name body with
                    | Some exprHandle when clientAcc.IsEmpty ->
                        analyze sourceExpr (QueryOp.Filter exprHandle :: opsAcc) clientAcc
                    | _ ->
                        let compiled = Expression.Lambda<Func<'T, bool>>(body, param).Compile()
                        analyze sourceExpr opsAcc (compiled :: clientAcc)

                // OrderBy and ThenBy map directly to descending = false
                | "OrderBy" | "ThenBy" ->
                    match ExprTranslator.tryTranslate param.Name body with
                    | Some exprHandle when clientAcc.IsEmpty ->
                        let spec = { Expr = exprHandle; Descending = false; NullsLast = false }
                        analyze sourceExpr (addSort spec opsAcc) clientAcc
                    | _ ->
                        analyze sourceExpr opsAcc clientAcc

                // OrderByDescending and ThenByDescending map directly to descending = true
                | "OrderByDescending" | "ThenByDescending" ->
                    match ExprTranslator.tryTranslate param.Name body with
                    | Some exprHandle when clientAcc.IsEmpty ->
                        let spec = { Expr = exprHandle; Descending = true; NullsLast = false }
                        analyze sourceExpr (addSort spec opsAcc) clientAcc
                    | _ ->
                        analyze sourceExpr opsAcc clientAcc

                | "Select" ->
                    analyze sourceExpr (QueryOp.SelectPassthrough :: opsAcc) clientAcc

                | unsupported ->
                    failwithf "LINQ operator '%s' is not implemented yet" unsupported

            | MethodCall(methodInfo, null, [ sourceExpr; countExpr ]) when methodInfo.Name = "Take" && clientAcc.IsEmpty ->
                match tryEvaluate countExpr with
                | Some value ->
                    let count = Convert.ToUInt32(value)
                    analyze sourceExpr (QueryOp.Limit count :: opsAcc) clientAcc
                | None ->
                    failwithf "Could not evaluate argument for Take: %A" countExpr

            | _ ->
                (opsAcc, clientAcc)

        analyze expression [] []

    /// Evaluates the complete pipeline, executing native plan first, then client predicates in memory
    member private this.ExecuteQuery() : IEnumerable<'T> =
        let ops, clientPredicates = this.CompilePipeline()
        let mat = PolarsQuery<'T>.ResolveMaterializer(materializer)

        // 1. Single point of native plan execution
        let nativeLf = PolarsQuery<'T>.ApplyNativeOps lfCloned ops
        let dfHandle = PolarsWrapper.LazyCollect(nativeLf,PlEngine.Auto,true)
        let rows = mat.Materialize<'T>(dfHandle)

        // 2. Client-side tail evaluation (if any)
        if clientPredicates.IsEmpty then
            rows
        else
            clientPredicates
            |> List.fold (fun (acc: IEnumerable<'T>) pred -> acc.Where(pred.Invoke)) rows

    /// Compiles pipeline to LazyFrameHandle. If client predicates exist, materializes and transposes back.
    member this.CompileToLazyFrameHandle() : LazyFrameHandle =
        let ops, clientPredicates = this.CompilePipeline()

        if clientPredicates.IsEmpty then
            // Pure native fast-path
            PolarsQuery<'T>.ApplyNativeOps lfCloned ops
        else
            // Fallback path: execute query with client filters, then transpose back to LazyFrameHandle
            let filteredRows = this.ExecuteQuery()
            let newDfHandle = DataFrameBuilder.FromRows<'T>(filteredRows)
            PolarsWrapper.DataFrameToLazy(newDfHandle)

    /// Compiles the query and materializes directly into a native DataFrameHandle
    member this.CompileToDataFrameHandle() : DataFrameHandle =
        let ops, clientPredicates = this.CompilePipeline()

        if clientPredicates.IsEmpty then
            // Pure native fast-path: collect straight from native plan
            let nativeLf = PolarsQuery<'T>.ApplyNativeOps lfCloned ops
            PolarsWrapper.LazyCollect(nativeLf,PlEngine.Auto,true)
        else
            // Fallback path: execute query with client filters, then transpose directly to DataFrameHandle
            let filteredRows = this.ExecuteQuery()
            DataFrameBuilder.FromRows<'T>(filteredRows)
        
    interface IQueryable<'T> with
        member this.GetEnumerator() = this.ExecuteQuery().GetEnumerator()
        member this.GetEnumerator() = (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator
        member _.ElementType = typeof<'T>
        member _.Expression = expression
        member _.Provider = provider :> IQueryProvider

    interface IOrderedQueryable<'T>
