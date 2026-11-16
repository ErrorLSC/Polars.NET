using Kaminari.Sazanami.Helpers;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// The threads and memory several readers share when a dataset drives them unit by unit: one scheduler running at
/// most <c>parallelism</c> column decodes at once, one pool of scratch buffers, and the bytes of row groups that may be
/// in flight ahead of the consumer. Owned by the dataset's read; the readers neither complete nor dispose it.
/// </summary>
internal sealed class ReadBudget(int parallelism, long bytes, int buffers, bool writesPastCaches) : IDisposable
{
    /// <summary>The threads decoding at once.</summary>
    public int Threads { get; } = Math.Max(1, parallelism);

    /// <summary>The scheduler of the column decodes.</summary>
    public ConcurrentExclusiveSchedulerPair Schedulers { get; } = new(TaskScheduler.Default, Math.Max(1, parallelism));

    /// <summary>The scratch buffers (chunks fetched, pages decompressed) kept between units.</summary>
    public NativeBufferPool Buffers { get; } = new(buffers);

    /// <summary>The uncompressed bytes of row groups started and not yet taken, beyond the head one.</summary>
    public long Bytes => bytes;

    /// <summary>Whether long strings are written past the caches (threads competing for memory bandwidth).</summary>
    public bool WritesPastCaches => writesPastCaches;

    public void Dispose()
    {
        Schedulers.Complete();
        Buffers.Dispose();
    }
}
