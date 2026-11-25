using Polars.NET.Core.Enums;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Linq.Execution;

/// <summary>Asynchronous enumeration of a query's elements.</summary>
internal static class AsyncQueryEnumerator
{
    /// <summary>Runs the whole query on the first MoveNextAsync and then enumerates its elements.</summary>
    public static IAsyncEnumerator<T> OfDeferredRows<T>(CancellationToken cancellationToken, Func<IEnumerable<T>> run) =>
        new DeferredRows<T>(cancellationToken, run);

    /// <summary>
    /// Streams the native plan's batches and enumerates each batch's elements, materialized with
    /// <paramref name="materializeBatch"/>, before collecting the next one.
    /// </summary>
    public static IAsyncEnumerator<T> OfBatchStream<T>(CancellationToken cancellationToken, LazyFrameHandle nativeLf, Func<DataFrameHandle, IEnumerable<T>> materializeBatch) =>
        new BatchStream<T>(cancellationToken, nativeLf, materializeBatch);

    private sealed class DeferredRows<T>(CancellationToken cancellationToken, Func<IEnumerable<T>> run) : IAsyncEnumerator<T>
    {
        private IEnumerator<T>? _rows;

        public T Current => _rows is { } rows ? rows.Current : throw new InvalidOperationException("Enumeration has not started.");

        public ValueTask<bool> MoveNextAsync()
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_rows is { } rows) return ValueTask.FromResult(rows.MoveNext());
            // The query runs on the caller's thread (its compilation scope is the thread's); a failure is the task's.
            try
            {
                cancellationToken.ThrowIfCancellationRequested();
                var started = run().GetEnumerator();
                _rows = started;
                return ValueTask.FromResult(started.MoveNext());
            }
            catch (OperationCanceledException e)
            {
                return ValueTask.FromCanceled<bool>(e.CancellationToken);
            }
            catch (Exception e)
            {
                return ValueTask.FromException<bool>(e);
            }
        }

        public ValueTask DisposeAsync()
        {
            _rows?.Dispose();
            return ValueTask.CompletedTask;
        }
    }

    private sealed class BatchStream<T>(CancellationToken cancellationToken, LazyFrameHandle nativeLf, Func<DataFrameHandle, IEnumerable<T>> materializeBatch)
        : IAsyncEnumerator<T>
    {
        private readonly IAsyncEnumerator<DataFrameHandle> _batches =
            PolarsWrapper.LazyCollectBatchesStreamAsync(nativeLf, PlEngine.Auto, maintainOrder: false, chunkSize: 0L, cancellationToken: cancellationToken)
                .GetAsyncEnumerator(cancellationToken);
        private DataFrameHandle? _currentBatch;
        private IEnumerator<T>? _currentRows;
        private bool _disposed;

        public T Current => _currentRows is { } rows ? rows.Current : throw new InvalidOperationException("Enumeration has not started or is completed.");

        public ValueTask<bool> MoveNextAsync()
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_currentRows is { } rows && rows.MoveNext()) return ValueTask.FromResult(true);
            return Advance();
        }

        private void ReleaseBatch()
        {
            _currentBatch?.Dispose();
            _currentBatch = null;
            _currentRows = null;
        }

        private async ValueTask<bool> Advance()
        {
            cancellationToken.ThrowIfCancellationRequested();
            while (true)
            {
                if (_currentRows is { } rows && rows.MoveNext()) return true;
                ReleaseBatch();
                if (!await _batches.MoveNextAsync()) return false;
                var batch = _batches.Current;
                if (PolarsWrapper.DataFrameHeight(batch) > 0L)
                {
                    _currentBatch = batch;
                    _currentRows = materializeBatch(batch).GetEnumerator();
                }
                else batch.Dispose();
            }
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;
            _disposed = true;
            ReleaseBatch();
            await _batches.DisposeAsync();
        }
    }
}
