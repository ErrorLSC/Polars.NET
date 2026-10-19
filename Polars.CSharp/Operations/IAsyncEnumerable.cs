#pragma warning disable CS1591
using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Apache.Arrow;
using Apache.Arrow.Ipc;
using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Provides high-performance, asynchronous streaming extension methods for DataFrame and LazyFrame.
/// Bridges native Polars streaming batch execution with .NET IAsyncEnumerable protocols.
/// </summary>
public static class AsyncExtensions
{
    /// <summary>
    /// Exposes in-memory DataFrame rows as a cooperative, cancellation-aware IAsyncEnumerable.
    /// </summary>
    /// <typeparam name="T">The target strongly typed record or struct.</typeparam>
    /// <param name="df">The source DataFrame.</param>
    /// <param name="chunkSize">Number of rows hydrated synchronously per cooperative slice.</param>
    /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
    /// <returns>An asynchronous stream of hydrated row objects.</returns>
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(
        this DataFrame df,
        int chunkSize = 10_000,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(df);

        if (chunkSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be greater than zero.");
        }

        long totalRows = df.Height;
        if (totalRows == 0L)
        {
            yield break;
        }

        var mapper = RowMapper<T>.GetOrCreate(df);
        long offset = 0L;

        while (offset < totalRows)
        {
            cancellationToken.ThrowIfCancellationRequested();

            long currentChunkLimit = Math.Min(offset + chunkSize, totalRows);

            // Fast-path: Tight, synchronous, zero-allocation loop within current batch
            for (long i = offset; i < currentChunkLimit; i++)
            {
                yield return mapper(df, i);
            }

            offset = currentChunkLimit;

            // Yield execution slot to prevent starving thread pool during massive iterations
            if (offset < totalRows)
            {
                await Task.Yield();
            }
        }
    }

    /// <summary>
    /// Streams strongly typed rows asynchronously from an Apache Arrow C Stream interface.
    /// </summary>
    /// <typeparam name="T">The target strongly typed record or struct.</typeparam>
    /// <param name="stream">The source Arrow stream.</param>
    /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
    /// <returns>An asynchronous stream of hydrated row objects.</returns>
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(
        this IArrowArrayStream stream,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        Func<DataFrame, long, T>? mapper = null;

        while (!cancellationToken.IsCancellationRequested)
        {
            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);

            if (batch is null)
            {
                break;
            }

            if (batch.Length == 0)
            {
                continue;
            }

            // Zero-copy bridge: Import managed RecordBatch into Polars DataFrame
            using var chunkDf = DataFrame.FromArrow(batch);

            // Cache row mapper once as Schema is invariant across batches within the same stream
            mapper ??= RowMapper<T>.GetOrCreate(chunkDf);

            long height = chunkDf.Height;

            for (long i = 0L; i < height; i++)
            {
                yield return mapper(chunkDf, i);
            }
        }
    }

    /// <summary>
    /// Executes the LazyFrame via native streaming batch engine,
    /// </summary>
    /// <typeparam name="T">The target strongly typed record or struct.</typeparam>
    /// <param name="lazyDf">The source LazyFrame.</param>
    /// <param name="chunkSize">Maximum row count per native batch (0 uses Polars default batch size).</param>
    /// <param name="engine">Execution engine selection.</param>
    /// <param name="maintainOrder">Whether to enforce deterministic row ordering across batches.</param>
    /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
    /// <returns>An asynchronous stream of hydrated row objects.</returns>
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(
        this LazyFrame lazyDf,
        long chunkSize = 0,
        Engine engine = Engine.Auto,
        bool maintainOrder = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lazyDf);

        if (chunkSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be non-negative.");
        }

        cancellationToken.ThrowIfCancellationRequested();

        Func<DataFrame, long, T>? mapper = null;

        await foreach (var chunkDf in lazyDf.ToBatchAsyncEnumerable(chunkSize, engine, maintainOrder, cancellationToken)
                                            .ConfigureAwait(false))
        {
            using (chunkDf)
            {
                if (chunkDf.Height == 0)
                {
                    continue;
                }

                mapper ??= RowMapper<T>.GetOrCreate(chunkDf);
                long height = chunkDf.Height;

                for (long i = 0L; i < height; i++)
                {
                    yield return mapper(chunkDf, i);
                }
            }
        }
    }

    /// <summary>
    /// Streams materialized DataFrame batches asynchronously from a LazyFrame.
    /// Each batch is emitted as soon as computed by the native streaming engine.
    /// </summary>
    /// <param name="lazyDf">The source LazyFrame.</param>
    /// <param name="chunkSize">Maximum row count per native batch (0 uses Polars default batch size).</param>
    /// <param name="engine">Execution engine selection.</param>
    /// <param name="maintainOrder">Whether to enforce deterministic row ordering across batches.</param>
    /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
    /// <returns>An asynchronous stream of DataFrame batches.</returns>
    public static async IAsyncEnumerable<DataFrame> ToBatchAsyncEnumerable(
        this LazyFrame lazyDf,
        long chunkSize = 0,
        Engine engine = Engine.Auto,
        bool maintainOrder = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lazyDf);

        if (chunkSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be non-negative.");
        }

        // Fast-path: Throw immediately if token is already cancelled before cloning handle
        cancellationToken.ThrowIfCancellationRequested();

        var clonedHandle = PolarsWrapper.LazyClone(lazyDf.Handle);

        await foreach (var dfHandle in PolarsWrapper.LazyCollectBatchesStreamAsync(
            clonedHandle,
            (PlEngine)engine,
            maintainOrder,
            chunkSize,
            cancellationToken).ConfigureAwait(false))
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return new DataFrame(dfHandle);
        }
    }

    /// <summary>
    /// Executes the LazyFrame and hydrates rows via a bounded channel pipeline,
    /// </summary>
    /// <typeparam name="T">The target strongly typed record or struct.</typeparam>
    /// <param name="lazyDf">The source LazyFrame.</param>
    /// <param name="boundedCapacity">Maximum capacity of the intermediate buffer channel.</param>
    /// <param name="chunkSize">Maximum row count per native batch (0 uses Polars default batch size).</param>
    /// <param name="engine">Execution engine selection.</param>
    /// <param name="maintainOrder">Whether to enforce deterministic row ordering across batches.</param>
    /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
    /// <returns>An asynchronous stream of hydrated row objects with backpressure.</returns>
    public static async IAsyncEnumerable<T> ToAsyncEnumerableWithBackpressure<T>(
        this LazyFrame lazyDf,
        int boundedCapacity = 20_000,
        long chunkSize = 0,
        Engine engine = Engine.Auto,
        bool maintainOrder = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lazyDf);

        if (boundedCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(boundedCapacity), "Bounded capacity must be greater than zero.");
        }

        if (chunkSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be non-negative.");
        }

        // Link tokens to ensure producer task aborts immediately if consumer breaks early or throws
        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var token = linkedCts.Token;

        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(boundedCapacity)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        var producerTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var item in lazyDf.ToAsyncEnumerable<T>(chunkSize, engine, maintainOrder, token)
                                                 .ConfigureAwait(false))
                {
                    await channel.Writer.WriteAsync(item, token).ConfigureAwait(false);
                }

                channel.Writer.Complete();
            }
            catch (Exception ex)
            {
                channel.Writer.Complete(ex);
            }
        }, token);

        try
        {
            while (await channel.Reader.WaitToReadAsync(token).ConfigureAwait(false))
            {
                while (channel.Reader.TryRead(out var item))
                {
                    yield return item;
                }
            }
        }
        finally
        {
            // Early break by consumer cancels producer, closing the native channel and stopping Rust execution
            linkedCts.Cancel();

            try
            {
                await producerTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                // Expected when cancellation was triggered by early termination
            }
        }
    }

    /// <summary>
    /// Streams DataFrame batches via a bounded channel pipeline,
    /// overlapping batch execution and consumption while bounding in-flight batch count.
    /// </summary>
    /// <param name="lazyDf">The source LazyFrame.</param>
    /// <param name="boundedCapacity">Maximum number of in-flight DataFrame batches buffered.</param>
    /// <param name="chunkSize">Maximum row count per native batch (0 uses Polars default batch size).</param>
    /// <param name="engine">Execution engine selection.</param>
    /// <param name="maintainOrder">Whether to enforce deterministic row ordering across batches.</param>
    /// <param name="cancellationToken">Optional cooperative cancellation token.</param>
    /// <returns>An asynchronous stream of DataFrame batches with backpressure.</returns>
    public static async IAsyncEnumerable<DataFrame> ToBatchAsyncEnumerableWithBackpressure(
        this LazyFrame lazyDf,
        int boundedCapacity = 16,
        long chunkSize = 0,
        Engine engine = Engine.Auto,
        bool maintainOrder = false,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lazyDf);

        if (boundedCapacity <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(boundedCapacity), "Bounded capacity must be greater than zero.");
        }

        if (chunkSize < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be non-negative.");
        }

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var token = linkedCts.Token;

        var channel = Channel.CreateBounded<DataFrame>(new BoundedChannelOptions(boundedCapacity)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        var producerTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var batch in lazyDf.ToBatchAsyncEnumerable(chunkSize, engine, maintainOrder, token)
                                                  .ConfigureAwait(false))
                {
                    await channel.Writer.WriteAsync(batch, token).ConfigureAwait(false);
                }

                channel.Writer.Complete();
            }
            catch (Exception ex)
            {
                channel.Writer.Complete(ex);
            }
        }, token);

        try
        {
            while (await channel.Reader.WaitToReadAsync(token).ConfigureAwait(false))
            {
                while (channel.Reader.TryRead(out var batch))
                {
                    yield return batch;
                }
            }
        }
        finally
        {
            linkedCts.Cancel();

            // Prevent unmanaged memory leaks by disposing any batches left buffered in channel
            while (channel.Reader.TryRead(out var unreadBatch))
            {
                unreadBatch.Dispose();
            }

            try
            {
                await producerTask.ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
            }
        }
    }
}