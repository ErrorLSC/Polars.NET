#pragma warning disable CS1591
using System.Runtime.CompilerServices;
using Apache.Arrow.Ipc;
using Apache.Arrow;
using System.Threading.Channels;

namespace Polars.CSharp;

public static class AsyncExtensions
{
    /// <summary>
    /// Exposes DataFrame rows as a high-performance, cancellation-aware IAsyncEnumerable.
    /// Employs chunked micro-batching to eliminate per-row task state machine allocations.
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
            // Cooperative cancellation check between micro-batches
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

    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(
        this IArrowArrayStream stream,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(stream);

        Func<DataFrame, long, T>? mapper = null;

        // Pull RecordBatches asynchronously one by one from the unmanaged C Stream
        while (!cancellationToken.IsCancellationRequested)
        {
            // Official Apache.Arrow API returns ValueTask<RecordBatch?>
            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync(cancellationToken).ConfigureAwait(false);

            // Null indicates EOF (End of Stream)
            if (batch is null)
            {
                break;
            }

            if (batch.Length == 0)
            {
                continue;
            }

            // 1. Zero-copy bridge: Import managed RecordBatch into native Polars DataFrame
            using var chunkDf = DataFrame.FromArrow(batch);

            // 2. Cache row mapper once as Schema is invariant across batches within the same stream
            mapper ??= RowMapper<T>.GetOrCreate(chunkDf);

            long height = chunkDf.Height;

            // 3. Fast tight-loop row hydration within current batch
            for (long i = 0L; i < height; i++)
            {
                yield return mapper(chunkDf, i);
            }
        }
    }
    /// <summary>
    /// Executes the LazyFrame asynchronously using native CollectAsync,
    /// streaming strongly-typed rows with O(1) memory overhead and pull-based backpressure.
    /// </summary>
    public static async IAsyncEnumerable<T> ToAsyncEnumerable<T>(
        this LazyFrame lazyDf,
        int chunkSize = 10_000,
        Engine engine = Engine.Auto,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lazyDf);

        // 1. Native asynchronous execution with cancellation and engine dispatch
        using var df = await lazyDf.CollectAsync(engine, cancellationToken)
                                   .ConfigureAwait(false);

        // 2. Stream rows via zero-allocation micro-batch iterator
        await foreach (var row in df.ToAsyncEnumerable<T>(chunkSize, cancellationToken)
                                    .ConfigureAwait(false))
        {
            yield return row;
        }
    }

    /// <summary>
    /// Executes the LazyFrame and hydrates rows via a bounded channel pipeline,
    /// overlapping hydration and consumption while enforcing hard memory backpressure limits.
    /// </summary>
    public static async IAsyncEnumerable<T> ToAsyncEnumerableWithBackpressure<T>(
        this LazyFrame lazyDf,
        int boundedCapacity = 20_000,
        int chunkSize = 10_000,
        Engine engine = Engine.Auto,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(lazyDf);

        // 1. Configure bounded channel with wait-based backpressure
        var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(boundedCapacity)
        {
            SingleWriter = true,
            SingleReader = true,
            FullMode = BoundedChannelFullMode.Wait
        });

        // 2. Producer task: executes LazyFrame and feeds bounded channel
        var producerTask = Task.Run(async () =>
        {
            try
            {
                using var df = await lazyDf.CollectAsync(engine, cancellationToken)
                                           .ConfigureAwait(false);

                await foreach (var item in df.ToAsyncEnumerable<T>(chunkSize, cancellationToken)
                                            .ConfigureAwait(false))
                {
                    await channel.Writer.WriteAsync(item, cancellationToken).ConfigureAwait(false);
                }

                channel.Writer.Complete();
            }
            catch (Exception ex)
            {
                channel.Writer.Complete(ex);
            }
        }, cancellationToken);

        // 3. Consumer stream: reads from channel as items arrive
        while (await channel.Reader.WaitToReadAsync(cancellationToken).ConfigureAwait(false))
        {
            while (channel.Reader.TryRead(out var item))
            {
                yield return item;
            }
        }

        // Ensure producer exceptions are observed
        await producerTask.ConfigureAwait(false);
    }

}