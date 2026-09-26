namespace Polars.FSharp.Query

open Polars.FSharp
open System.Linq
open System.Linq.Expressions
open Polars.NET.Linq.Provider
open System
open System.Reflection
open Polars.NET.Core

[<AutoOpen>]
module LinqExtension =
    /// Inspects an IQueryable and safely extracts its underlying Polars LazyFrameHandle
    let private tryExtractLazyFrame (q: IQueryable) : LazyFrameHandle option =
        match q with
        | null -> None
        // 1. Direct plan source
        | :? IPolarsPlanSource as ps ->
            Some (ps.GetCompiledLazyFrameHandle())
        // 2. Query originated from Polars Provider
        | _ when not (isNull q.Provider) && (q.Provider :? PolarsQueryProvider) ->
            // Use the standard IQueryProvider interface to create the plan query
            let provider = q.Provider
            let polarsQ = provider.CreateQuery q.Expression
            match polarsQ with
            | :? IPolarsPlanSource as ps -> Some (ps.GetCompiledLazyFrameHandle())
            | _ -> None
        // 3. Fallback: Expression tree evaluation through PolarsQuery resolver
        | _ ->
            PolarsQuery<obj>.ResolveToLazyFrameHandle q.Expression
            
    type LazyFrame with
        /// <summary>
        /// Convert LazyFrame into an IQueryable LINQ query provider.
        /// Operations will be translated to Polars Expressions and compiled into execution plans.
        /// </summary>
        member this.AsQueryable<'T>():IQueryable<'T> =
            new PolarsQuery<'T>(this.Handle,new FSharpRowCursorMaterializer())
        /// Directly creates an IQueryable with type inferred from an existing sample sequence
        member this.AsQueryable(sample: 'T seq) : IQueryable<'T> =
            this.AsQueryable<'T>()

    type DataFrame with
        /// <summary>
        /// Convert DataFrame into an IQueryable LINQ query provider.
        /// Operations will be translated to Polars Expressions and compiled into execution plans.
        /// </summary>
        member this.AsQueryable<'T>():IQueryable<'T> =
            this.Lazy().AsQueryable<'T>()
        /// Directly creates an IQueryable with type inferred from an existing sample sequence
        member this.AsQueryable(sample: 'T seq) : IQueryable<'T> =
            this.AsQueryable<'T>()

    type IQueryable with
        /// <summary>
        /// Compiles the LINQ query pipeline and converts it back to a Polars LazyFrame.
        /// </summary>
        member this.ToLazyFrame(): LazyFrame =
            match tryExtractLazyFrame this with
            | Some lfHandle -> new LazyFrame(lfHandle)
            | None -> raise (NotSupportedException "ToLazyFrame can only be invoked on queries originating from Polars.NET.")

        /// <summary>
        /// Compiles the LINQ query pipeline and converts it back to a Polars DataFrame.
        /// </summary>
        member this.ToDataFrame(): DataFrame =
            match tryExtractLazyFrame this with
            | Some lfHandle ->
                let dfHandle = PolarsWrapper.LazyCollect(lfHandle, PlEngine.Auto, true)
                new DataFrame(dfHandle)
            | None -> raise (NotSupportedException "ToDataFrame can only be invoked on queries originating from Polars.NET.")