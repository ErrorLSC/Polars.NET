namespace Polars.FSharp

open System.Threading
open System.Threading.Tasks
open System.Collections.Generic

/// <summary>
/// High-performance asynchronous row enumerator with cooperative micro-batching.
/// Synchronously yields within the batch to achieve near-zero task allocation,
/// and yields control at batch boundaries to honor cooperative cancellation.
/// </summary>
type private DataFrameAsyncRowEnumerator<'T>(df: DataFrame, chunkSize: int, ct: CancellationToken) =
    let colNames = FSharpRowMapper<'T>.ColumnNames
    let cols = colNames |> Array.map (fun name -> df.[name])
    let height = df.Height
    let mutable index = -1L
    let mutable current = Unchecked.defaultof<'T>

    interface IAsyncEnumerator<'T> with
        member _.Current = current

        member _.DisposeAsync() = 
            ValueTask.CompletedTask

        member _.MoveNextAsync() : ValueTask<bool> =
            let next = index + 1L
            if next < height then
                index <- next
                // Micro-batch boundary: yield execution slot and inspect cancellation
                if index > 0L && index % int64 chunkSize = 0L then
                    ct.ThrowIfCancellationRequested()
                    ValueTask<bool>(
                        task {
                            do! Task.Yield()
                            current <- FSharpRowMapper<'T>.Hydrate(cols, index)
                            return true
                        }
                    )
                else
                    // Fast path: synchronous, zero-heap-allocation iteration inside batch
                    current <- FSharpRowMapper<'T>.Hydrate(cols, index)
                    ValueTask<bool>(true)
            else
                current <- Unchecked.defaultof<'T>
                ValueTask<bool>(false)

/// <summary>
/// Asynchronously materializes and streams rows from a LazyFrame after background plan execution.
/// </summary>
type private LazyFrameAsyncRowEnumerator<'T>(lazyFrame: LazyFrame, chunkSize: int, engine: Engine, ct: CancellationToken) =
    let mutable innerEnumerator: IAsyncEnumerator<'T> option = None
    let mutable collectedDf: DataFrame option = None

    interface IAsyncEnumerator<'T> with
        member _.Current =
            match innerEnumerator with
            | Some it -> it.Current
            | None -> Unchecked.defaultof<'T>

        member _.DisposeAsync() =
            match collectedDf with
            | Some df -> df.Dispose()
            | None -> ()
            ValueTask.CompletedTask

        member _.MoveNextAsync() : ValueTask<bool> =
            match innerEnumerator with
            | Some it -> it.MoveNextAsync()
            | None ->
                ValueTask<bool>(
                    task {
                        ct.ThrowIfCancellationRequested()
                        // 1. Offload query execution to engine asynchronously
                        let! df = lazyFrame.CollectAsync(engine,ct)
                        collectedDf <- Some df

                        // 2. Wrap collected DataFrame into micro-batch async stream
                        let it = new DataFrameAsyncRowEnumerator<'T>(df, chunkSize, ct) :> IAsyncEnumerator<'T>
                        innerEnumerator <- Some it
                        return! it.MoveNextAsync()
                    }
                )

[<AutoOpen>]
module DataFrameAsyncExtensions =

    type DataFrame with
        /// <summary>
        /// Returns an IAsyncEnumerable streaming strongly-typed F# Record or struct rows.
        /// </summary>
        /// <param name="chunkSize">Number of rows hydrated synchronously per cooperative slice. Default is 10,000.</param>
        /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
        member this.RowsAsync<'T>(?chunkSize: int, ?cancellationToken: CancellationToken) : IAsyncEnumerable<'T> =
            let cs = defaultArg chunkSize 10_000
            let ct = defaultArg cancellationToken CancellationToken.None

            { new IAsyncEnumerable<'T> with
                member _.GetAsyncEnumerator(enumCt) =
                    // Prefer local enumerator token if explicitly passed via WithCancellation
                    let effectiveToken = if enumCt.CanBeCanceled then enumCt else ct
                    new DataFrameAsyncRowEnumerator<'T>(this, cs, effectiveToken) :> IAsyncEnumerator<'T> }

    type LazyFrame with
        /// <summary>
        /// Executes the LazyFrame execution plan asynchronously and returns an IAsyncEnumerable
        /// </summary>
        /// <param name="chunkSize">Number of rows hydrated synchronously per cooperative slice. Default is 10,000.</param>
        /// <param name="engine">Execution engine option (Auto, InProcess, Streaming). Default is Auto.</param>
        /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
        member this.RowsAsync<'T>(?chunkSize: int, ?engine: Engine, ?cancellationToken: CancellationToken) : IAsyncEnumerable<'T> =
            let cs = defaultArg chunkSize 10_000
            let eng = defaultArg engine Engine.Auto
            let ct = defaultArg cancellationToken CancellationToken.None

            { new IAsyncEnumerable<'T> with
                member _.GetAsyncEnumerator(enumCt) =
                    let effectiveToken = if enumCt.CanBeCanceled then enumCt else ct
                    new LazyFrameAsyncRowEnumerator<'T>(this, cs, eng, effectiveToken) :> IAsyncEnumerator<'T> }

/// <summary>
/// Functional combinators for consuming IAsyncEnumerable row streams within F# pipelines.
/// </summary>
[<RequireQualifiedAccess>]
module RowsAsync =

    /// <summary>
    /// Applies a synchronous action to each row of the async stream.
    /// </summary>
    let inline iter ([<InlineIfLambda>] action: 'T -> unit) (stream: IAsyncEnumerable<'T>) : Task<unit> =
        task {
            let! ct = Task.FromResult(CancellationToken.None)
            use it = stream.GetAsyncEnumerator(ct)
            while! it.MoveNextAsync() do
                action it.Current
        }

    /// <summary>
    /// Applies an asynchronous task-returning action to each row of the async stream.
    /// </summary>
    let inline iterTask ([<InlineIfLambda>] action: 'T -> Task<unit>) (stream: IAsyncEnumerable<'T>) : Task<unit> =
        task {
            let! ct = Task.FromResult(CancellationToken.None)
            use it = stream.GetAsyncEnumerator(ct)
            while! it.MoveNextAsync() do
                do! action it.Current
        }

    /// <summary>
    /// Folds over the rows of the async stream using the accumulator function.
    /// </summary>
    let inline fold ([<InlineIfLambda>] folder: 'State -> 'T -> 'State) (initial: 'State) (stream: IAsyncEnumerable<'T>) : Task<'State> =
        task {
            let mutable state = initial
            let! ct = Task.FromResult(CancellationToken.None)
            use it = stream.GetAsyncEnumerator(ct)
            while! it.MoveNextAsync() do
                state <- folder state it.Current
            return state
        }

    /// <summary>
    /// Materializes all elements of the async stream into an F# immutable list.
    /// </summary>
    let toList (stream: IAsyncEnumerable<'T>) : Task<'T list> =
        task {
            let buffer = ResizeArray<'T>()
            let! ct = Task.FromResult(CancellationToken.None)
            use it = stream.GetAsyncEnumerator(ct)
            while! it.MoveNextAsync() do
                buffer.Add it.Current
            return buffer |> Seq.toList
        }

    /// <summary>
    /// Materializes all elements of the async stream into an array.
    /// </summary>
    let toArray (stream: IAsyncEnumerable<'T>) : Task<'T array> =
        task {
            let buffer = ResizeArray<'T>()
            let! ct = Task.FromResult(CancellationToken.None)
            use it = stream.GetAsyncEnumerator(ct)
            while! it.MoveNextAsync() do
                buffer.Add it.Current
            return buffer.ToArray()
        }