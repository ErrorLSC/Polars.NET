namespace Polars.FSharp

open System.Collections.Generic
open System.Data.Common
open System.Threading
open System.Threading.Tasks
open Apache.Arrow
open Polars.NET.Core.Data
open Polars.NET.Core.Arrow

/// <summary>
/// Streams native DataFrame batches directly from DbDataReader cursor via Arrow FFI.
/// Eliminates closure allocations and enforces deterministic RecordBatch disposal.
/// </summary>
[<Sealed>]
type internal DbBatchAsyncEnumerator
    (
        batchStream: IAsyncEnumerable<RecordBatch>,
        ct: CancellationToken,
        linkedCtsOpt: CancellationTokenSource option
    ) =

    let batchEnumerator = batchStream.GetAsyncEnumerator(ct)
    let mutable currentBatch: DataFrame = DataFrame.create()
    let mutable isDisposed = false

    interface IAsyncEnumerator<DataFrame> with
        member _.Current = currentBatch

        member _.MoveNextAsync() : ValueTask<bool> =
            ValueTask<bool>(task {
                ct.ThrowIfCancellationRequested()
                let mutable advanced = false
                let mutable isEof = false

                while not advanced && not isEof do
                    let! hasNext = batchEnumerator.MoveNextAsync()
                    if hasNext then
                        use batch = batchEnumerator.Current
                        if not (isNull batch) && batch.Length > 0 then
                            let handle = ArrowFfiBridge.ImportDataFrame(batch)
                            currentBatch <- new DataFrame(handle)
                            advanced <- true
                    else
                        currentBatch <- DataFrame.create()
                        isEof <- true

                return advanced
            })

        member _.DisposeAsync() : ValueTask =
            if not isDisposed then
                isDisposed = true |> ignore
                linkedCtsOpt |> Option.iter (fun cts -> cts.Dispose())
                batchEnumerator.DisposeAsync()
            else
                ValueTask()

[<AutoOpen>]
module DatabaseAsyncExtensions =

    type DbDataReader with
        /// <summary>
        /// Asynchronously streams database cursor records into chunks of native Polars DataFrames.
        /// </summary>
        /// <param name="batchSize">Maximum number of rows per Arrow/DataFrame batch.</param>
        /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
        /// <returns>An asynchronous stream of materialized DataFrame chunks.</returns>
        member this.ReadDatabaseBatchesAsync
            (
                ?batchSize: int,
                ?cancellationToken: CancellationToken
            ) : IAsyncEnumerable<DataFrame> =

            let batch = defaultArg batchSize 50_000
            let ct = defaultArg cancellationToken CancellationToken.None

            ct.ThrowIfCancellationRequested()

            if batch <= 0 then
                invalidArg (nameof batchSize) "Batch size must be greater than zero."

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

                    let arrowBatchesStream = DbToArrowStream.ToArrowBatchesAsync(this, batch, effectiveCt)
                    new DbBatchAsyncEnumerator(arrowBatchesStream, effectiveCt, linkedCts) :> IAsyncEnumerator<DataFrame>
            }

[<RequireQualifiedAccess>]
module DbDataReader =

    /// <summary>
    /// Asynchronously streams database cursor records into chunks of native Polars DataFrames.
    /// </summary>
    let inline readDatabaseBatchesAsync
        (batchSize: int)
        (reader: DbDataReader) : IAsyncEnumerable<DataFrame> =
        reader.ReadDatabaseBatchesAsync(batchSize = batchSize)