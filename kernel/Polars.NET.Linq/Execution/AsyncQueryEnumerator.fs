namespace Polars.NET.Linq

open System.Collections.Generic
open System.Threading
open System.Threading.Tasks
open Polars.NET.Core
open Polars.NET.Core.Wrappers
open Polars.NET.Core.Enums
open Kaminari.Samidare

/// Asynchronous enumeration of a query's elements.
module internal AsyncQueryEnumerator =

    /// Runs the whole query on the first MoveNextAsync and then enumerates its elements.
    let ofDeferredRows<'T> (cancellationToken: CancellationToken) (run: unit -> IEnumerable<'T>) : IAsyncEnumerator<'T> =
        let enumeratorRef = ref (None: IEnumerator<'T> option)

        { new IAsyncEnumerator<'T> with
            member _.Current =
                match enumeratorRef.Value with
                | Some e -> e.Current
                | None -> invalidOp "Enumeration has not started."

            member _.MoveNextAsync() : ValueTask<bool> =
                cancellationToken.ThrowIfCancellationRequested()

                match enumeratorRef.Value with
                | Some e -> ValueTask<bool>(e.MoveNext())
                | None ->
                    ValueTask<bool>(
                        task {
                            cancellationToken.ThrowIfCancellationRequested()
                            let rows = run ()
                            let e = rows.GetEnumerator()
                            enumeratorRef.Value <- Some e
                            return e.MoveNext()
                        }
                    )

            member _.DisposeAsync() : ValueTask =
                match enumeratorRef.Value with
                | Some e -> e.Dispose()
                | None -> ()

                ValueTask()
        }

    /// Streams the native plan's batches and enumerates each batch's elements, materialized with
    /// `materializeBatch`, before collecting the next one.
    let ofBatchStream<'T>
        (cancellationToken: CancellationToken)
        (nativeLf: LazyFrameHandle)
        (materializeBatch: DataFrameHandle -> IEnumerable<'T>)
        : IAsyncEnumerator<'T> =
        let batchStream =
            PolarsWrapper.LazyCollectBatchesStreamAsync(
                nativeLf,
                PlEngine.Auto,
                maintainOrder = false,
                chunkSize = 0L,
                cancellationToken = cancellationToken
            )

        let batchEnumerator = batchStream.GetAsyncEnumerator cancellationToken
        let mutable currentBatchHandle: DataFrameHandle = null
        let mutable currentBatchRows: IEnumerator<'T> = null
        let mutable hasActiveRows = false
        let mutable isDisposed = false

        { new IAsyncEnumerator<'T> with
            member _.Current =
                if hasActiveRows && not (isNull currentBatchRows) then
                    currentBatchRows.Current
                else
                    invalidOp "Enumeration has not started or is completed."

            member _.MoveNextAsync() : ValueTask<bool> =
                cancellationToken.ThrowIfCancellationRequested()

                if hasActiveRows && currentBatchRows.MoveNext() then
                    ValueTask<bool>(true)
                else
                    ValueTask<bool>(
                        task {
                            cancellationToken.ThrowIfCancellationRequested()
                            let mutable advanced = false
                            let mutable doneStreaming = false

                            while not (advanced || doneStreaming) do
                                if hasActiveRows && currentBatchRows.MoveNext() then
                                    advanced <- true
                                else
                                    if not (isNull currentBatchHandle) then
                                        currentBatchHandle.Dispose()
                                        currentBatchHandle <- null
                                        currentBatchRows <- null
                                        hasActiveRows <- false

                                    let! hasNextBatch = batchEnumerator.MoveNextAsync()

                                    if hasNextBatch then
                                        let dfHandle = batchEnumerator.Current
                                        let h = PolarsWrapper.DataFrameHeight dfHandle

                                        if h > 0L then
                                            currentBatchHandle <- dfHandle
                                            let rows = materializeBatch dfHandle
                                            currentBatchRows <- rows.GetEnumerator()
                                            hasActiveRows <- true
                                        else
                                            dfHandle.Dispose()
                                    else
                                        doneStreaming <- true

                            return advanced
                        }
                    )

            member _.DisposeAsync() : ValueTask =
                ValueTask(
                    task {
                        if not isDisposed then
                            isDisposed <- true

                            if not (isNull currentBatchHandle) then
                                currentBatchHandle.Dispose()
                                currentBatchHandle <- null
                                currentBatchRows <- null
                                hasActiveRows <- false

                            do! batchEnumerator.DisposeAsync()
                    }
                )
        }
