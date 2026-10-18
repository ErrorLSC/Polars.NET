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
open System.Reflection

[<AutoOpen>]
module LinqExtension =

    [<TailCall>]
    let rec private getFieldAnywhere (t: Type) (name: string) : FieldInfo =
        if isNull t || t = typeof<obj> then
            null
        else
            let f =
                t.GetField(name, BindingFlags.NonPublic ||| BindingFlags.Public ||| BindingFlags.Instance)

            if not (isNull f) then
                f
            else
                getFieldAnywhere t.BaseType name

    /// Inspects an object or IQueryable and safely extracts its underlying Polars IPolarsExplainable
    [<TailCall>]
    let rec private tryExtractExplainable (obj: obj) : IPolarsExplainable option =
        match obj with
        | null -> None
        | :? IPolarsExplainable as exp -> Some exp
        | _ ->
            let t = obj.GetType()
            let fSource = getFieldAnywhere t "_source"

            if not (isNull fSource) then
                tryExtractExplainable (fSource.GetValue obj)
            else
                let fEnumerable = getFieldAnywhere t "_enumerable"

                if not (isNull fEnumerable) then
                    tryExtractExplainable (fEnumerable.GetValue obj)
                else
                    match obj with
                    | :? IQueryable as q ->
                        match q.Expression with
                        | :? System.Linq.Expressions.ConstantExpression as ce when
                            not (isNull ce.Value || Object.ReferenceEquals(ce.Value, obj))
                            ->
                            tryExtractExplainable ce.Value
                        | _ ->
                            if not (isNull q.Provider) && (q.Provider :? PolarsQueryProvider) then
                                try
                                    let polarsQ = q.Provider.CreateQuery q.Expression

                                    match polarsQ with
                                    | :? IPolarsExplainable as exp -> Some exp
                                    | _ -> None
                                with _ ->
                                    None
                            else
                                None
                    | _ -> None

    /// Inspects an IQueryable and safely extracts its underlying Polars LazyFrameHandle
    let rec private tryExtractLazyFrame (q: IQueryable) : LazyFrameHandle option =
        match q with
        | null -> None
        // 1. Direct plan source
        | :? IPolarsPlanSource as ps -> Some(ps.GetCompiledLazyFrameHandle())
        // 2. Query originated from Polars Provider
        | _ when not (isNull q.Provider) && (q.Provider :? PolarsQueryProvider) ->
            // Use the standard IQueryProvider interface to create the plan query
            let provider = q.Provider
            let polarsQ = provider.CreateQuery q.Expression

            match polarsQ with
            | :? IPolarsPlanSource as ps -> Some(ps.GetCompiledLazyFrameHandle())
            | _ -> None
        // 3. Fallback: Expression tree evaluation through PolarsQuery resolver
        | _ ->
            match SubqueryDecorrelator.resolveToLazyFrameHandle q.Expression with
            | Some h -> Some h
            | None ->
                match tryExtractExplainable (box q) with
                | Some(:? IPolarsPlanSource as ps) -> Some(ps.GetCompiledLazyFrameHandle())
                | _ -> None

    type LazyFrame with
        /// <summary>
        /// Convert LazyFrame into an IQueryable LINQ query provider.
        /// Operations will be translated to Polars Expressions and compiled into execution plans.
        /// </summary>
        member this.AsQueryable<'T>() : IQueryable<'T> =
            PolarsQuery<'T>(this.Handle, FSharpRowCursorMaterializer())

        /// Directly creates an IQueryable with type inferred from an existing sample sequence
        member this.AsQueryable(sample: 'T seq) : IQueryable<'T> = this.AsQueryable<'T>()

    type DataFrame with
        /// <summary>
        /// Convert DataFrame into an IQueryable LINQ query provider.
        /// Operations will be translated to Polars Expressions and compiled into execution plans.
        /// </summary>
        member this.AsQueryable<'T>() : IQueryable<'T> = this.Lazy().AsQueryable<'T>()
        /// Directly creates an IQueryable with type inferred from an existing sample sequence
        member this.AsQueryable(sample: 'T seq) : IQueryable<'T> = this.AsQueryable<'T>()

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
                    ediOpt <- Some(ExceptionDispatchInfo.Capture ex)

                let vt = enumerator.DisposeAsync()

                if not vt.IsCompletedSuccessfully then
                    do! vt.AsTask()

                match ediOpt with
                | Some edi ->
                    edi.Throw()
                    return DataFrame.create ()
                | None ->
                    let dfHandle = DataFrameBuilder.FromRows<'T> results
                    return new DataFrame(dfHandle)
            }

    type IQueryable<'T> with
        /// <summary>
        /// Compiles the LINQ query pipeline and converts it back to a Polars LazyFrame.
        /// </summary>
        member this.ToLazyFrame() : LazyFrame =
            match tryExtractLazyFrame this with
            | Some lfHandle -> new LazyFrame(lfHandle)
            | None ->
                raise (NotSupportedException "ToLazyFrame can only be invoked on queries originating from Polars.NET.")

        /// <summary>
        /// Compiles the LINQ query pipeline and converts it back to a Polars DataFrame.
        /// </summary>
        member this.ToDataFrame() : DataFrame = this.ToDataFrame(Engine.Auto)

        /// <summary>
        /// Asynchronously compiles and executes the query, materializing the result directly into a native Polars DataFrame.
        /// When native pushdown is possible, this bypasses F# object hydration entirely for zero-allocation performance.
        /// </summary>
        /// <param name="engine">Execution engine selection.</param>
        /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
        /// <returns>A task yielding the resulting DataFrame.</returns>
        member this.ToDataFrameAsync(?engine: Engine, ?cancellationToken: CancellationToken) : Task<DataFrame> =

            let eng = defaultArg engine Engine.Auto
            let ct = defaultArg cancellationToken CancellationToken.None
            ct.ThrowIfCancellationRequested()

            // 1. Fast-path: Directly extract compiled plan without hydrating intermediate objects
            match tryExtractLazyFrame this with
            | Some lfHandle ->
                task {
                    ct.ThrowIfCancellationRequested()
                    use lazyDf = new LazyFrame(lfHandle)
                    return! lazyDf.CollectAsync(engine = eng, cancellationToken = ct)
                }

            // 2. Fallback path A: Stream asynchronously if the source supports IAsyncEnumerable
            | None ->
                match box this with
                | :? IAsyncEnumerable<'T> as asyncStream -> asyncStream.ToDataFrameAsync(cancellationToken = ct)

                // 3. Fallback path B: Synchronous in-memory LINQ offloaded to thread pool
                | _ ->
                    Task.Run(
                        (fun () ->
                            ct.ThrowIfCancellationRequested()
                            let rows = this |> Seq.toList
                            let dfHandle = DataFrameBuilder.FromRows<'T>(rows)
                            new DataFrame(dfHandle)),
                        ct
                    )

        /// <summary>
        /// Synchronously compiles and executes the query, returning the result directly as a native Polars DataFrame.
        /// Bypasses object hydration when native pushdown is possible.
        /// </summary>
        /// <param name="engine">Execution engine selection.</param>
        /// <returns>The resulting DataFrame.</returns>
        member this.ToDataFrame(?engine: Engine) : DataFrame =
            let eng = defaultArg engine Engine.Auto

            match tryExtractLazyFrame this with
            | Some lfHandle ->
                use lazyDf = new LazyFrame(lfHandle)
                lazyDf.Collect(engine = eng)
            | None ->
                let rows = this |> Seq.toList
                let dfHandle = DataFrameBuilder.FromRows<'T> rows
                new DataFrame(dfHandle)

        /// <summary>
        /// Returns the Polars native query execution plan, including warnings if parts of the query
        /// fallback to managed client-side execution.
        /// </summary>
        /// <param name="optimized">Whether to display the Polars optimized logical plan or the raw plan.</param>
        member this.Explain(?optimized: bool) : string =
            let optimized = defaultArg optimized true

            match tryExtractExplainable (box this) with
            | Some explainable -> explainable.Explain optimized
            | None -> "[Polars.FSharp] Query source is not a native PolarsQueryable. In-memory execution only."

        /// <summary>
        /// Gets or sets the default plan inspector action used when none is explicitly specified.
        /// Defaults to <see cref="Console.WriteLine(string)"/>.
        /// </summary>
        member this.DefaultInspector = printfn "%s"

        /// <summary>
        /// Inspects the query plan using the default inspector (<see cref="DefaultInspector"/>)
        /// </summary>
        /// <param name="optimized">Whether to display the optimized plan. Defaults to true.</param>
        member this.InspectPlan(?optimized: bool) : string = this.Explain(?optimized = optimized)

        /// <summary>
        /// Inspects the query plan at the current pipeline stage and invokes a custom inspector action,
        /// </summary>
        /// <param name="inspector">Action receiving the diagnostic plan string.</param>
        /// <param name="optimized">Whether to display the optimized plan. Defaults to true.</param>
        member this.InspectPlan(?inspector: string -> unit, ?optimized: bool) =
            let optimized = defaultArg optimized true
            let inspector = defaultArg inspector this.DefaultInspector

            inspector (this.Explain optimized)
            this
