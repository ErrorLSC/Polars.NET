namespace Polars.FSharp.Query

open Polars.FSharp
open System.Linq
open Polars.NET.Linq
open System
open Polars.NET.Core
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open Polars.NET.Core.Helpers
open System.Runtime.ExceptionServices

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

    type IAsyncEnumerable<'T> with
        /// <summary>
        /// Asynchronously drains an IAsyncEnumerable sequence of objects directly into a native Polars DataFrame.
        /// </summary>
        /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
        /// <returns>A task yielding the resulting DataFrame.</returns>
        member this.ToDataFrameAsync(?cancellationToken: CancellationToken) : Task<DataFrame> =
            let ct = defaultArg cancellationToken CancellationToken.None
            ct.ThrowIfCancellationRequested()

            task {
                ct.ThrowIfCancellationRequested()
                let results = ResizeArray<'T>()
                let enumerator = this.GetAsyncEnumerator ct
                let mutable ediOpt = None

                try
                    let mutable hasNext = true
                    while hasNext do
                        let! moved = enumerator.MoveNextAsync()
                        if moved then
                            results.Add enumerator.Current
                        else
                            hasNext <- false
                with ex ->
                    ediOpt <- Some (ExceptionDispatchInfo.Capture ex)

                let vt = enumerator.DisposeAsync()
                if not vt.IsCompletedSuccessfully then
                    do! vt.AsTask()

                match ediOpt with
                | Some edi ->
                    edi.Throw()
                    return DataFrame.create()
                | None ->
                    let dfHandle = DataFrameBuilder.FromRows<'T> results
                    return new DataFrame(dfHandle)
            }
    type IQueryable<'T> with
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
                let dfHandle = PolarsWrapper.LazyCollect(lfHandle, PlEngine.Auto)
                new DataFrame(dfHandle)
            | None -> raise (NotSupportedException "ToDataFrame can only be invoked on queries originating from Polars.NET.")
        /// <summary>
        /// Asynchronously compiles and executes the query, materializing the result directly into a native Polars DataFrame.
        /// When native pushdown is possible, this bypasses F# object hydration entirely for zero-allocation performance.
        /// </summary>
        /// <param name="engine">Execution engine selection.</param>
        /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
        /// <returns>A task yielding the resulting DataFrame.</returns>
        member this.ToDataFrameAsync
            (
                ?engine: Engine,
                ?cancellationToken: CancellationToken
            ) : Task<DataFrame> =

            let eng = defaultArg engine Engine.Auto
            let ct = defaultArg cancellationToken CancellationToken.None
            ct.ThrowIfCancellationRequested()

            // 1. Fast-path: Directly extract compiled plan without hydrating intermediate objects
            match box this with
            | :? IPolarsPlanSource as planSource ->
                task {
                    ct.ThrowIfCancellationRequested()
                    let lfHandle = planSource.GetCompiledLazyFrameHandle()
                    use lazyDf = new LazyFrame(lfHandle)
                    return! lazyDf.CollectAsync(engine = eng, cancellationToken = ct)
                }

            // 2. Fallback path A: Stream asynchronously if the source supports IAsyncEnumerable
            | :? IAsyncEnumerable<'T> as asyncStream ->
                asyncStream.ToDataFrameAsync(cancellationToken = ct)

            // 3. Fallback path B: Synchronous in-memory LINQ offloaded to thread pool
            | _ ->
                Task.Run((fun () ->
                    ct.ThrowIfCancellationRequested()
                    let rows = this |> Seq.toList
                    let dfHandle = DataFrameBuilder.FromRows<'T>(rows)
                    new DataFrame(dfHandle)
                ), ct)

        /// <summary>
        /// Synchronously compiles and executes the query, returning the result directly as a native Polars DataFrame.
        /// Bypasses object hydration when native pushdown is possible.
        /// </summary>
        /// <param name="engine">Execution engine selection.</param>
        /// <returns>The resulting DataFrame.</returns>
        member this.ToDataFrame(?engine: Engine) : DataFrame =
            let eng = defaultArg engine Engine.Auto
            match box this with
            | :? IPolarsPlanSource as planSource ->
                let lfHandle = planSource.GetCompiledLazyFrameHandle()
                use lazyDf = new LazyFrame(lfHandle)
                lazyDf.Collect(engine = eng)
            | _ ->
                let rows = this |> Seq.toList
                let dfHandle = DataFrameBuilder.FromRows<'T>(rows)
                new DataFrame(dfHandle)