using Kaminari.Seseragi.Files;
using System.Diagnostics;
using Apache.Arrow;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// The requests a dataset read sends to a store whose requests wait on the network: bounded
/// (<see cref="DatasetOptions.MaxRequestsInFlight"/>) apart from the files open, so that a slow request does not
/// stop the files after it; not bounded from local files and memory.
/// </summary>
public sealed class ParquetDatasetRequestTests
{
    private static byte[] File(int first, int rows, int rowGroupRows)
    {
        var id = new Int64Array.Builder().AppendRange(Enumerable.Range(first, rows).Select(i => (long)i)).Build();
        var x = new Int64Array.Builder().AppendRange(Enumerable.Range(first, rows).Select(i => (long)(i * 7 % 1000))).Build();
        var schema = new Apache.Arrow.Schema([new Field("id", id.Data.DataType, true), new Field("x", x.Data.DataType, true)], null);
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions { RowGroupRows = rowGroupRows }))
        {
            writer.Write(new RecordBatch(schema, [id, x], rows));
            writer.Complete();
        }
        return stream.ToArray();
    }

    private static List<long> Ids(IEnumerable<RecordBatch> batches)
    {
        List<long> ids = [];
        foreach (RecordBatch batch in batches)
        {
            using (batch)
            {
                var column = (Int64Array)batch.Column("id");
                for (int i = 0; i < batch.Length; i++) ids.Add(column.GetValue(i)!.Value);
            }
        }
        return ids;
    }

    private static MemoryFileStore Files(int files, int rows, int rowGroupRows)
    {
        var store = new MemoryFileStore();
        for (int f = 0; f < files; f++) store.Add($"d/f{f:D3}.parquet", File(f * rows, rows, rowGroupRows));
        return store;
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task ASlowFile_DoesNotStopTheFilesAfterIt(int threads)
    {
        // The second file's open waits (the first is opened by the dataset for its schema, and read first).
        var store = new RemoteStore(Files(60, 10, 10)) { Holding = "d/f001.parquet" };
        using var dataset = await ParquetDataset.OpenAsync("d/", new ParquetDatasetOptions { Store = store, MaxRequestsInFlight = 4 });
        using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads });
        int before = store.Opened;
        Task<List<long>> read = Task.Run(() => Ids(reader.ReadBatches()));
        // The other files open meanwhile, four requests at a time.
        var watch = Stopwatch.StartNew();
        while (store.Opened - before < 59 && watch.Elapsed < TimeSpan.FromSeconds(20)) await Task.Delay(10);
        Assert.Equal(59, store.Opened - before);
        Assert.False(read.IsCompleted);
        store.Release();
        Assert.Equal([.. Enumerable.Range(0, 600).Select(i => (long)i)], await read);
        Assert.InRange(store.PeakRequests, 1, 4);
    }

    [Theory]
    [InlineData(1, 8)]
    [InlineData(3, 8)]
    [InlineData(3, 1)]
    public void Requests_StayWithinTheBound(int requests, int threads)
    {
        // Files of several row groups: opens, then a fetch per row group.
        var store = new RemoteStore(Files(24, 400, 100)) { Delay = 2 };
        using var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = store, MaxRequestsInFlight = requests });
        using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads });
        Assert.Equal([.. Enumerable.Range(0, 24 * 400).Select(i => (long)i)], Ids(reader.ReadBatches()));
        Assert.InRange(store.PeakRequests, 1, requests);
        Assert.InRange(reader.PeakRequests, 1, requests);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void Opens_AreLetIn_InFileOrder(int threads)
    {
        // One place: the files open one after another, in the order they are read (the thread pool may start the
        // later opens first).
        var store = new RemoteStore(Files(80, 10, 10));
        using var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = store, MaxRequestsInFlight = 1 });
        store.Opens.Clear();
        using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads });
        Assert.Equal(800, Ids(reader.ReadBatches()).Count);
        Assert.Equal([.. Enumerable.Range(0, 80).Select(f => $"d/f{f:D3}.parquet")], store.Opens);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public void UnitsFetchedAhead_DecodeWithinTheThreads(int threads)
    {
        // Fetches that waited resume on the thread pool: their units still decode on the reader's threads only.
        var store = new RemoteStore(Files(16, 20_000, 2_000)) { Delay = 2 };
        using var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = store });
        using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads });
        Assert.Equal(16 * 20_000, Ids(reader.ReadBatches()).Count);
        Assert.InRange(reader.PeakDecoding, 1, threads);
    }

    [Fact]
    public void FilesInFlight_BoundTheRequestsToo()
    {
        var store = new RemoteStore(Files(24, 400, 100)) { Delay = 2 };
        using var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = store, MaxFilesInFlight = 2 });
        using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 8 });
        Assert.Equal(24 * 400, Ids(reader.ReadBatches()).Count);
        Assert.InRange(store.PeakRequests, 1, 2);
    }

    [Fact]
    public void ByDefault_RemoteRequestsAreBounded_LocalOnesNot()
    {
        var memory = Files(200, 10, 10);
        var remote = new RemoteStore(memory) { Delay = 1 };
        using (var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = remote }))
        using (var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 16 }))
        {
            Assert.Equal(2000, Ids(reader.ReadBatches()).Count);
            Assert.InRange(reader.PeakRequests, 1, ParquetDatasetReader.RemoteRequests);
            Assert.InRange(remote.PeakRequests, 1, ParquetDatasetReader.RemoteRequests);
        }
        using (var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = memory }))
        using (var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 16 }))
        {
            Assert.Equal(2000, Ids(reader.ReadBatches()).Count);
            Assert.Equal(0, reader.PeakRequests);
        }
        Assert.Throws<ArgumentOutOfRangeException>(() => ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = memory, MaxRequestsInFlight = 0 }));
    }

    [Theory]
    [InlineData(1, false)]
    [InlineData(1, true)]
    [InlineData(8, false)]
    [InlineData(8, true)]
    public async Task StoppingEarly_WithRequestsWaiting_Returns(int threads, bool useAsync)
    {
        // Two places, one taken by an open that hangs until cancelled; the other serves the files before it (a place is
        // not taken back: requests all hanging would stop the read), the opens after them wait.
        var store = new RemoteStore(Files(40, 10, 10)) { Hanging = "d/f005.parquet" };
        using var dataset = await ParquetDataset.OpenAsync("d/", new ParquetDatasetOptions { Store = store, MaxRequestsInFlight = 2 });
        Task<int> read = Task.Run(async () =>
        {
            using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads });
            int rows = 0;
            if (useAsync)
            {
                await foreach (RecordBatch batch in reader.ReadBatchesAsync())
                {
                    using (batch) rows += batch.Length;
                    if (rows >= 30) break;
                }
            }
            else
            {
                foreach (RecordBatch batch in reader.ReadBatches())
                {
                    using (batch) rows += batch.Length;
                    if (rows >= 30) break;
                }
            }
            return rows;
        });
        Assert.Same(read, await Task.WhenAny(read, Task.Delay(TimeSpan.FromSeconds(30))));
        Assert.Equal(30, await read);
    }

    /// <summary>
    /// A store neither local nor in memory (its requests bounded as remote ones), over files in memory: counts the
    /// requests at once (opens and reads), can delay each, hold one file's open until released, or hang one until
    /// cancelled.
    /// </summary>
    internal sealed class RemoteStore(MemoryFileStore inner) : IFileStore
    {
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Lock _opensLock = new();
        private int _inside, _peak, _opened;

        public int Delay { get; init; }

        public string? Holding { get; init; }

        public string? Hanging { get; init; }

        public int PeakRequests => Volatile.Read(ref _peak);

        public int Opened => Volatile.Read(ref _opened);

        public void Release() => _released.TrySetResult();

        /// <summary>The files opened, in the order their opens reached the store.</summary>
        public List<string> Opens { get; } = [];

        public bool CanList => true;

        public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, recursive, cancellationToken);

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public async ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default)
        {
            lock (_opensLock) Opens.Add(file.Path);
            IRandomAccess source = await Request(file.Path, () => inner.OpenAsync(file, cancellationToken), cancellationToken);
            Interlocked.Increment(ref _opened);
            return new Source(source, this);
        }

        public async ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default)
        {
            lock (_opensLock) Opens.Add(path);
            IRandomAccess? source = await Request(path, () => inner.OpenAsync(path, cancellationToken), cancellationToken);
            if (source is null) return null;
            Interlocked.Increment(ref _opened);
            return new Source(source, this);
        }

        private async ValueTask<T> Request<T>(string? path, Func<ValueTask<T>> send, CancellationToken cancellationToken)
        {
            int inside = Interlocked.Increment(ref _inside);
            int peak;
            while (inside > (peak = Volatile.Read(ref _peak)) && Interlocked.CompareExchange(ref _peak, inside, peak) != peak) { }
            try
            {
                if (path is not null && path == Holding) await _released.Task.WaitAsync(cancellationToken);
                if (path is not null && path == Hanging) await Task.Delay(Timeout.Infinite, cancellationToken);
                if (Delay > 0) await Task.Delay(Delay, cancellationToken);
                return await send();
            }
            finally
            {
                Interlocked.Decrement(ref _inside);
            }
        }

        private sealed class Source(IRandomAccess inner, RemoteStore owner) : IRandomAccess, IDisposable
        {
            public long Length => inner.Length;

            public int ReadAt(long offset, Span<byte> destination)
            {
                int inside = Interlocked.Increment(ref owner._inside);
                int peak;
                while (inside > (peak = Volatile.Read(ref owner._peak)) && Interlocked.CompareExchange(ref owner._peak, inside, peak) != peak) { }
                try
                {
                    if (owner.Delay > 0) Thread.Sleep(owner.Delay);
                    return inner.ReadAt(offset, destination);
                }
                finally
                {
                    Interlocked.Decrement(ref owner._inside);
                }
            }

            public async ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken) =>
                await owner.Request(null, () => inner.ReadAtAsync(offset, destination, cancellationToken), cancellationToken);

            public void Dispose() => (inner as IDisposable)?.Dispose();
        }
    }
}
