namespace Polars.NET.Linq.Provider

open System
open System.Collections
open System.Collections.Generic
open System.Linq
open System.Linq.Expressions
open Polars.NET.Core
open Polars.NET.Core.Helpers

/// Internal representation of supported operations in the LINQ query pipeline
[<RequireQualifiedAccess>]
type internal QueryOp =
    | Filter of ExprHandle
    | Limit of uint32
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
    // Always clone the lazyframe handle to protect against consumption
    let lfCloned = PolarsWrapper.LazyClone lazyFrameHandle
    let provider = PolarsQueryProvider(lfCloned, materializer)

    new(lazyFrameHandle: LazyFrameHandle, materializer: IDataFrameMaterializer) =
        PolarsQuery<'T>(lazyFrameHandle, materializer, Expression.Constant(null, typeof<IQueryable<'T>>))

    static member private ResolveMaterializer(mat: IDataFrameMaterializer) =
        if not (isNull mat) then mat
        elif not (isNull DataFrameMaterializerRegistry.Default) then DataFrameMaterializerRegistry.Default
        else failwith "No DataFrameMaterializer registered. Ensure Polars.CSharp or Polars.FSharp has configured a materializer."

    member private this.ExecuteQuery() : IEnumerable<'T> =
        let mat = PolarsQuery<'T>.ResolveMaterializer(materializer)

        let rec collectOps (expr: Expression) (acc: QueryOp list) : QueryOp list =
            match expr with
            // Matches Queryable.Where / Queryable.Select extension methods (Object is null)
            | MethodCall(methodInfo, null, [ sourceExpr; StripQuotes (Lambda([ param ], body)) ]) ->
                match methodInfo.Name with
                | "Where" ->
                    let filterExprHandle = ExprTranslator.translate param.Name body
                    collectOps sourceExpr (QueryOp.Filter filterExprHandle :: acc)
                | "Select" ->
                    collectOps sourceExpr (QueryOp.SelectPassthrough :: acc)
                | unsupported ->
                    failwithf "LINQ operator '%s' is not implemented yet" unsupported

            // Matches Queryable.Take(source, count)
            | MethodCall(methodInfo, null, [ sourceExpr; countExpr ]) when methodInfo.Name = "Take" ->
                match tryEvaluate countExpr with
                | Some value ->
                    let count = Convert.ToUInt32(value)
                    collectOps sourceExpr (QueryOp.Limit count :: acc)
                | None ->
                    failwithf "Could not evaluate argument for Take: %A" countExpr

            | _ -> acc

        let pipelineOps = collectOps expression []

        let finalLfHandle =
            pipelineOps
            |> List.fold (fun currentLf op ->
                match op with
                | QueryOp.Filter exprHandle ->
                    PolarsWrapper.LazyFilter(currentLf, exprHandle)
                | QueryOp.Limit count ->
                    PolarsWrapper.LazySlice(currentLf, 0, count)
                | QueryOp.SelectPassthrough ->
                    currentLf
            ) lfCloned

        // Materialize native DataFrameHandle
        let dfHandle: DataFrameHandle = PolarsWrapper.LazyCollect(finalLfHandle,PlEngine.Auto,true)

        // Delegate materialization to the registered materializer strategy
        mat.Materialize<'T>(dfHandle)

    interface IQueryable<'T> with
        member this.GetEnumerator() = this.ExecuteQuery().GetEnumerator()
        member this.GetEnumerator() = (this :> IEnumerable<'T>).GetEnumerator() :> IEnumerator
        member _.ElementType = typeof<'T>
        member _.Expression = expression
        member _.Provider = provider :> IQueryProvider
