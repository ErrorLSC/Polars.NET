namespace Polars.FSharp

open System
open System.Collections.Generic
open System.Runtime.ExceptionServices
open System.Threading
open System.Threading.Tasks
open Polars.NET.Core

// =========================================================================
// 1. Dedicated Async Enumerator Implementations (Zero Closure Allocations)
// =========================================================================

/// <summary>
/// Streams native DataFrame batches directly from Polars streaming engine.
/// </summary>
[<Sealed>]
type internal DataFrameBatchAsyncEnumerator
    (
        nativeStream: IAsyncEnumerable<DataFrameHandle>,
        ct: CancellationToken,
        linkedCtsOpt: CancellationTokenSource option
    ) =

    let nativeEnumerator = nativeStream.GetAsyncEnumerator(ct)
    let mutable currentBatch: DataFrame = DataFrame.create()
    let mutable isDisposed = false

    interface IAsyncEnumerator<DataFrame> with
        member _.Current = currentBatch

        member _.MoveNextAsync() : ValueTask<bool> =
            ValueTask<bool>(task {
                ct.ThrowIfCancellationRequested()
                let! hasNext = nativeEnumerator.MoveNextAsync()
                if hasNext then
                    currentBatch <- new DataFrame(nativeEnumerator.Current)
                    return true
                else
                    currentBatch <- DataFrame.create()
                    return false
            })

        member _.DisposeAsync() : ValueTask =
            if not isDisposed then
                isDisposed <- true
                linkedCtsOpt |> Option.iter (fun cts -> cts.Dispose())
            nativeEnumerator.DisposeAsync()

/// <summary>
/// Dual fast-path streaming enumerator for LazyFrame rows.
/// Employs tight synchronous loop within batches, and fetches batches asynchronously.
/// </summary>
[<Sealed>]
type internal RowAsyncEnumerator<'T>
    (
        batchesStream: IAsyncEnumerable<DataFrame>,
        ct: CancellationToken,
        linkedCtsOpt: CancellationTokenSource option
    ) =

    let batchEnumerator = batchesStream.GetAsyncEnumerator(ct)
    let mutable currentBatchDf: DataFrame = DataFrame.create()
    let mutable rowEnumerator: RowEnumerator<'T> = Unchecked.defaultof<_>
    let mutable hasActiveBatch = false
    let mutable currentItem = Unchecked.defaultof<'T>
    let mutable isDisposed = false

    interface IAsyncEnumerator<'T> with
        member _.Current = currentItem

        member _.MoveNextAsync() : ValueTask<bool> =
            ct.ThrowIfCancellationRequested()

            // 1. Fast-path: Synchronous row iteration within active batch without Task allocation
            if hasActiveBatch && rowEnumerator.MoveNext() then
                currentItem <- rowEnumerator.Current
                ValueTask<bool>(true)
            else
                // 2. Slow-path: Active batch exhausted, asynchronously pull next batch from stream
                ValueTask<bool>(task {
                    ct.ThrowIfCancellationRequested()
                    let mutable advanced = false
                    let mutable doneStreaming = false

                    while not advanced && not doneStreaming do
                        if hasActiveBatch && rowEnumerator.MoveNext() then
                            currentItem <- rowEnumerator.Current
                            advanced <- true
                        else
                            // Immediately dispose previous batch's unmanaged memory
                            if hasActiveBatch then
                                currentBatchDf.Dispose()
                                currentBatchDf <- DataFrame.create()
                                hasActiveBatch <- false

                            let! hasNextBatch = batchEnumerator.MoveNextAsync()
                            if hasNextBatch then
                                currentBatchDf <- batchEnumerator.Current
                                if currentBatchDf.Height > 0L then
                                    rowEnumerator <- currentBatchDf.Rows<'T>()
                                    hasActiveBatch <- true
                            else
                                doneStreaming <- true

                    return advanced
                })

        member _.DisposeAsync() : ValueTask =
            if not isDisposed then
                isDisposed <- true
                if hasActiveBatch then
                    currentBatchDf.Dispose()
                    currentBatchDf <- DataFrame.create()
                    hasActiveBatch <- false
                linkedCtsOpt |> Option.iter (fun cts -> cts.Dispose())
            batchEnumerator.DisposeAsync()

/// <summary>
/// Micro-batched async enumerator for in-memory DataFrame rows with cooperative thread yields.
/// </summary>
[<Sealed>]
type internal DataFrameRowAsyncEnumerator<'T>
    (
        df: DataFrame,
        chunkSize: int,
        ct: CancellationToken,
        linkedCtsOpt: CancellationTokenSource option
    ) =

    let totalRows = df.Height
    let mutable enumerator = df.Rows<'T>()
    let mutable currentItem = Unchecked.defaultof<'T>
    let mutable processedInSlice = 0
    let mutable isDisposed = false

    interface IAsyncEnumerator<'T> with
        member _.Current = currentItem

        member _.MoveNextAsync() : ValueTask<bool> =
            ct.ThrowIfCancellationRequested()

            if totalRows = 0L then
                ValueTask<bool>(false)
            elif processedInSlice >= chunkSize then
                // Cooperative yield to avoid thread-pool starvation
                processedInSlice <- 0
                ValueTask<bool>(task {
                    do! Task.Yield()
                    ct.ThrowIfCancellationRequested()
                    if enumerator.MoveNext() then
                        currentItem <- enumerator.Current
                        processedInSlice <- processedInSlice + 1
                        return true
                    else
                        return false
                })
            else
                if enumerator.MoveNext() then
                    currentItem <- enumerator.Current
                    processedInSlice <- processedInSlice + 1
                    ValueTask<bool>(true)
                else
                    ValueTask<bool>(false)

        member _.DisposeAsync() : ValueTask =
            if not isDisposed then
                isDisposed <- true
                linkedCtsOpt |> Option.iter (fun cts -> cts.Dispose())
            ValueTask()

// =========================================================================
// 2. Extension Methods for LazyFrame and DataFrame
// =========================================================================

[<AutoOpen>]
module AsyncExtensions =

    type LazyFrame with
        /// <summary>
        /// Streams materialized DataFrame batches asynchronously using the native Polars streaming batch engine.
        /// Native memory consumption is strictly bounded by O(batch) size.
        /// </summary>
        /// <param name="chunkSize">Maximum row count per native batch (0 uses Polars default batch size).</param>
        /// <param name="engine">Execution engine selection.</param>
        /// <param name="maintainOrder">Whether to enforce deterministic row ordering across batches.</param>
        /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
        /// <returns>An asynchronous stream of DataFrame batches.</returns>
        member this.CollectBatchesAsync
            (
                ?chunkSize: int64,
                ?engine: Engine,
                ?maintainOrder: bool,
                ?cancellationToken: CancellationToken
            ) : IAsyncEnumerable<DataFrame> =

            let chunk = defaultArg chunkSize 0L
            let eng = defaultArg engine Engine.Auto
            let order = defaultArg maintainOrder false
            let ct = defaultArg cancellationToken CancellationToken.None

            ct.ThrowIfCancellationRequested()

            if chunk < 0L then
                invalidArg (nameof chunkSize) "Chunk size must be non-negative."

            { new IAsyncEnumerable<DataFrame> with
                member _.GetAsyncEnumerator(enumeratorCt: CancellationToken) =
                    let linkedCts =
                        if enumeratorCt = CancellationToken.None || enumeratorCt = ct then None
                        else Some (CancellationTokenSource.CreateLinkedTokenSource(ct, enumeratorCt))

                    let effectiveCt =
                        match linkedCts with
                        | Some cts -> cts.Token
                        | None -> if enumeratorCt <> CancellationToken.None then enumeratorCt else ct

                    effectiveCt.ThrowIfCancellationRequested()

                    let clonedHandle = PolarsWrapper.LazyClone this.Handle
                    let nativeStream =
                        PolarsWrapper.LazyCollectBatchesStreamAsync(
                            clonedHandle,
                            eng.ToNative(),
                            order,
                            chunk,
                            effectiveCt
                        )

                    new DataFrameBatchAsyncEnumerator(nativeStream, effectiveCt, linkedCts) :> IAsyncEnumerator<DataFrame>
            }

        /// <summary>
        /// Executes the LazyFrame via native streaming batch engine,
        /// yielding strongly typed F# Records/Tuples/DTOs with O(batch) memory overhead and pull-based backpressure.
        /// </summary>
        /// <typeparam name="'T">Target F# Record, Tuple, or DTO type.</typeparam>
        /// <param name="chunkSize">Maximum row count per native batch (0 uses Polars default batch size).</param>
        /// <param name="engine">Execution engine selection.</param>
        /// <param name="maintainOrder">Whether to enforce deterministic row ordering across batches.</param>
        /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
        /// <returns>An asynchronous stream of hydrated F# row items.</returns>
        member this.ToAsyncEnumerable<'T>
            (
                ?chunkSize: int64,
                ?engine: Engine,
                ?maintainOrder: bool,
                ?cancellationToken: CancellationToken
            ) : IAsyncEnumerable<'T> =

            let ct = defaultArg cancellationToken CancellationToken.None
            ct.ThrowIfCancellationRequested()

            let batchesStream =
                this.CollectBatchesAsync(
                    ?chunkSize = chunkSize,
                    ?engine = engine,
                    ?maintainOrder = maintainOrder,
                    ?cancellationToken = cancellationToken
                )

            { new IAsyncEnumerable<'T> with
                member _.GetAsyncEnumerator(enumeratorCt: CancellationToken) =
                    let linkedCts =
                        if enumeratorCt = CancellationToken.None || enumeratorCt = ct then None
                        else Some (CancellationTokenSource.CreateLinkedTokenSource(ct, enumeratorCt))

                    let effectiveCt =
                        match linkedCts with
                        | Some cts -> cts.Token
                        | None -> if enumeratorCt <> CancellationToken.None then enumeratorCt else ct

                    effectiveCt.ThrowIfCancellationRequested()
                    new RowAsyncEnumerator<'T>(batchesStream, effectiveCt, linkedCts) :> IAsyncEnumerator<'T>
            }

        /// <summary>
        /// Streams LazyFrame rows asynchronously as strongly-typed F# Records/Tuples/DTOs.
        /// Serves as the asynchronous counterpart to DataFrame.Rows&lt;'T&gt;().
        /// </summary>
        member inline this.RowsAsync<'T>
            (
                ?chunkSize: int64,
                ?engine: Engine,
                ?maintainOrder: bool,
                ?cancellationToken: CancellationToken
            ) : IAsyncEnumerable<'T> =
            this.ToAsyncEnumerable<'T>(
                ?chunkSize = chunkSize,
                ?engine = engine,
                ?maintainOrder = maintainOrder,
                ?cancellationToken = cancellationToken
            )

    type DataFrame with
        /// <summary>
        /// Streams in-memory DataFrame rows asynchronously as strongly-typed F# Records/Tuples/DTOs.
        /// Uses cooperative task slicing to prevent thread pool starvation during large scans.
        /// </summary>
        member this.ToAsyncEnumerable<'T>
            (
                ?chunkSize: int,
                ?cancellationToken: CancellationToken
            ) : IAsyncEnumerable<'T> =

            let chunk = defaultArg chunkSize 10_000
            let ct = defaultArg cancellationToken CancellationToken.None

            ct.ThrowIfCancellationRequested()

            if chunk <= 0 then
                invalidArg (nameof chunkSize) "Chunk size must be greater than zero."

            { new IAsyncEnumerable<'T> with
                member _.GetAsyncEnumerator(enumeratorCt: CancellationToken) =
                    let linkedCts =
                        if enumeratorCt = CancellationToken.None || enumeratorCt = ct then None
                        else Some (CancellationTokenSource.CreateLinkedTokenSource(ct, enumeratorCt))

                    let effectiveCt =
                        match linkedCts with
                        | Some cts -> cts.Token
                        | None -> if enumeratorCt <> CancellationToken.None then enumeratorCt else ct

                    effectiveCt.ThrowIfCancellationRequested()
                    new DataFrameRowAsyncEnumerator<'T>(this, chunk, effectiveCt, linkedCts) :> IAsyncEnumerator<'T>
            }

        /// <summary>
        /// Streams in-memory DataFrame rows asynchronously as strongly-typed F# Records/Tuples/DTOs.
        /// Serves as the asynchronous counterpart to DataFrame.Rows&lt;'T&gt;().
        /// </summary>
        member inline this.RowsAsync<'T>
            (
                ?chunkSize: int,
                ?cancellationToken: CancellationToken
            ) : IAsyncEnumerable<'T> =
            this.ToAsyncEnumerable<'T>(?chunkSize = chunkSize, ?cancellationToken = cancellationToken)

// =========================================================================
// 3. RowsAsync Functional Pipeline Module
// =========================================================================

[<RequireQualifiedAccess>]
module RowsAsync =
    open Polars.NET.Core.Helpers

    /// <summary>
    /// Internal consumption helper ensuring exception safety and non-allocating asynchronous disposal.
    /// </summary>
    let private consumeStream (stream: IAsyncEnumerable<'T>) (onNext: 'T -> unit) : Task<unit> = task {
        let enumerator = stream.GetAsyncEnumerator()
        let mutable ediOpt = None
        try
            let mutable hasNext = true
            while hasNext do
                let! moved = enumerator.MoveNextAsync()
                if moved then
                    onNext enumerator.Current
                else
                    hasNext <- false
        with ex ->
            ediOpt <- Some (ExceptionDispatchInfo.Capture(ex))

        // Safely dispose async enumerator outside try block
        let vt = enumerator.DisposeAsync()
        if not vt.IsCompletedSuccessfully then
            do! vt.AsTask()

        match ediOpt with
        | Some edi -> edi.Throw()
        | None -> ()
    }

    /// <summary>
    /// Materializes all items of the async stream into an F# immutable list.
    /// </summary>
    let toList (stream: IAsyncEnumerable<'T>) : Task<'T list> = task {
        let results = ResizeArray<'T>()
        do! consumeStream stream results.Add
        return results |> Seq.toList
    }

    /// <summary>
    /// Materializes all items of the async stream into a strongly-typed array.
    /// </summary>
    let toArray (stream: IAsyncEnumerable<'T>) : Task<'T array> = task {
        let results = ResizeArray<'T>()
        do! consumeStream stream results.Add
        return results.ToArray()
    }

    /// <summary>
    /// Materializes all items of the async stream into a ResizeArray.
    /// </summary>
    let toResizeArray (stream: IAsyncEnumerable<'T>) : Task<ResizeArray<'T>> = task {
        let results = ResizeArray<'T>()
        do! consumeStream stream results.Add
        return results
    }

    /// <summary>
    /// Returns the first element of the async stream and halts engine computation immediately.
    /// Raises InvalidOperationException if the stream contains no elements.
    /// </summary>
    let head (stream: IAsyncEnumerable<'T>) : Task<'T> = task {
        let enumerator = stream.GetAsyncEnumerator()
        let mutable ediOpt = None
        let mutable result = Unchecked.defaultof<'T>
        let mutable found = false

        try
            let! moved = enumerator.MoveNextAsync()
            if moved then
                result <- enumerator.Current
                found <- true
            else
                invalidOp "The async stream contains no elements."
        with ex ->
            ediOpt <- Some (ExceptionDispatchInfo.Capture(ex))

        let vt = enumerator.DisposeAsync()
        if not vt.IsCompletedSuccessfully then
            do! vt.AsTask()

        match ediOpt with
        | Some edi ->
            edi.Throw()
            return Unchecked.defaultof<'T>
        | None ->
            return result
    }

    /// <summary>
    /// Returns the first element of the stream as an option and halts engine computation immediately.
    /// </summary>
    let tryHead (stream: IAsyncEnumerable<'T>) : Task<'T option> = task {
        let enumerator = stream.GetAsyncEnumerator()
        let mutable ediOpt = None
        let mutable result = None

        try
            let! moved = enumerator.MoveNextAsync()
            if moved then
                result <- Some enumerator.Current
        with ex ->
            ediOpt <- Some (ExceptionDispatchInfo.Capture(ex))

        let vt = enumerator.DisposeAsync()
        if not vt.IsCompletedSuccessfully then
            do! vt.AsTask()

        match ediOpt with
        | Some edi ->
            edi.Throw()
            return None
        | None ->
            return result
    }

    /// <summary>
    /// Returns the first element of the stream as a ValueOption without heap allocation.
    /// </summary>
    let tryHeadValue (stream: IAsyncEnumerable<'T>) : Task<'T voption> = task {
        let enumerator = stream.GetAsyncEnumerator()
        let mutable ediOpt = None
        let mutable result = ValueNone

        try
            let! moved = enumerator.MoveNextAsync()
            if moved then
                result <- ValueSome enumerator.Current
        with ex ->
            ediOpt <- Some (ExceptionDispatchInfo.Capture(ex))

        let vt = enumerator.DisposeAsync()
        if not vt.IsCompletedSuccessfully then
            do! vt.AsTask()

        match ediOpt with
        | Some edi ->
            edi.Throw()
            return ValueNone
        | None ->
            return result
    }

    /// <summary>
    /// Counts total number of rows in the async stream.
    /// </summary>
    let length (stream: IAsyncEnumerable<'T>) : Task<int64> = task {
        let mutable count = 0L
        do! consumeStream stream (fun _ -> count <- count + 1L)
        return count
    }

    /// <summary>
    /// Iterates over the async stream, executing an action on each item.
    /// </summary>
    let iter (action: 'T -> unit) (stream: IAsyncEnumerable<'T>) : Task<unit> =
        consumeStream stream action

    /// <summary>
    /// Iterates over the async stream with zero-based 64-bit row index, executing an action on each item.
    /// </summary>
    let iteri ( action: int64 -> 'T -> unit) (stream: IAsyncEnumerable<'T>) : Task<unit> = task {
        let mutable idx = 0L
        do! consumeStream stream (fun item ->
            action idx item
            idx <- idx + 1L
        )
    }

    /// <summary>
    /// Folds over the async stream, accumulating state on each element.
    /// </summary>
    let fold ( folder: 'State -> 'T -> 'State) (initial: 'State) (stream: IAsyncEnumerable<'T>) : Task<'State> = task {
        let mutable state = initial
        do! consumeStream stream (fun item -> state <- folder state item)
        return state
    }

    /// <summary>
    /// Filters the async stream, yielding only elements that satisfy the predicate.
    /// </summary>
    let filter ( predicate: 'T -> bool) (stream: IAsyncEnumerable<'T>) : IAsyncEnumerable<'T> =
        { new IAsyncEnumerable<'T> with
            member _.GetAsyncEnumerator(ct: CancellationToken) =
                let inner = stream.GetAsyncEnumerator(ct)
                let mutable currentVal = Unchecked.defaultof<'T>
                { new IAsyncEnumerator<'T> with
                    member _.Current = currentVal
                    member _.MoveNextAsync() =
                        ValueTask<bool>(task {
                            ct.ThrowIfCancellationRequested()
                            let mutable found = false
                            let mutable hasNext = true
                            while not found && hasNext do
                                let! moved = inner.MoveNextAsync()
                                if moved then
                                    if predicate inner.Current then
                                        currentVal <- inner.Current
                                        found <- true
                                else
                                    hasNext <- false
                            return found
                        })
                    member _.DisposeAsync() = inner.DisposeAsync()
                }
        }

    /// <summary>
    /// Projects each element of the async stream using the mapping function.
    /// </summary>
    let map (mapping: 'T -> 'U) (stream: IAsyncEnumerable<'T>) : IAsyncEnumerable<'U> =
        { new IAsyncEnumerable<'U> with
            member _.GetAsyncEnumerator(ct: CancellationToken) =
                let inner = stream.GetAsyncEnumerator(ct)
                let mutable currentVal = Unchecked.defaultof<'U>
                { new IAsyncEnumerator<'U> with
                    member _.Current = currentVal
                    member _.MoveNextAsync() =
                        ValueTask<bool>(task {
                            ct.ThrowIfCancellationRequested()
                            let! moved = inner.MoveNextAsync()
                            if moved then
                                currentVal <- mapping inner.Current
                                return true
                            else
                                return false
                        })
                    member _.DisposeAsync() = inner.DisposeAsync()
                }
        }
    /// <summary>
    /// Materializes all items of the async stream directly into a native Polars DataFrame.
    /// </summary>
    let toDataFrame (stream: IAsyncEnumerable<'T>) : Task<DataFrame> = task {
        let results = ResizeArray<'T>()
        do! consumeStream stream results.Add
        let dfHandle = DataFrameBuilder.FromRows<'T>(results)
        return new DataFrame(dfHandle)
    }