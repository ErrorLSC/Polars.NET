using Kaminari.Seseragi.Files;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Datasets over a store that fails, hangs or stalls the way a network does: a failure surfaces in order (after the
/// rows of the files before it) and names the file; a hung read ends with the caller's cancellation or when the
/// consumer stops; a store answering late reads the same rows; every source opened is disposed.
/// </summary>
public sealed class ParquetDatasetFaultTests
{
    private const int Files = 8, RowsPerFile = 20_000, RowGroupRows = 5_000;

    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

    private static MemoryFileStore Data()
    {
        var store = new MemoryFileStore();
        for (int f = 0; f < Files; f++)
        {
            int first = f * RowsPerFile;
            var ids = new Int64Array.Builder().AppendRange(Enumerable.Range(first, RowsPerFile).Select(i => (long)i)).Build();
            var s = new StringArray.Builder().AppendRange(Enumerable.Range(first, RowsPerFile).Select(i => $"value {i} {i * 7919 % 104729}")).Build();
            var schema = new Apache.Arrow.Schema([new Field("id", Int64Type.Default, true), new Field("s", StringType.Default, true)], null);
            using var stream = new MemoryStream();
            using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions { RowGroupRows = RowGroupRows }))
            {
                writer.Write(new RecordBatch(schema, [ids, s], RowsPerFile));
                writer.Complete();
            }
            byte[] bytes = stream.ToArray();
            Assert.True(bytes.Length > ParquetMetadata.TailGuess); // data read apart from the footer
            store.Add($"d/f{f}.parquet", bytes);
        }
        return store;
    }

    private static string PathOf(int file) => $"d/f{file}.parquet";

    /// <summary>The ids read until the read ends (the exception it ends with, or null).</summary>
    private static (List<long> Ids, Exception? Error) Drain(IEnumerable<RecordBatch> batches)
    {
        List<long> ids = [];
        try
        {
            foreach (RecordBatch batch in batches)
            {
                using (batch) ids.AddRange(((Int64Array)batch.Column(0)).Values.ToArray());
            }
            return (ids, null);
        }
        catch (Exception e)
        {
            return (ids, e);
        }
    }

    private static async Task<(List<long> Ids, Exception? Error)> DrainAsync(IAsyncEnumerable<RecordBatch> batches)
    {
        List<long> ids = [];
        try
        {
            await foreach (RecordBatch batch in batches)
            {
                using (batch) ids.AddRange(((Int64Array)batch.Column(0)).Values.ToArray());
            }
            return (ids, null);
        }
        catch (Exception e)
        {
            return (ids, e);
        }
    }

    private static async Task<(List<long> Ids, Exception? Error)> Read(FaultStore store, int threads, bool async, CancellationToken ct = default)
    {
        using var dataset = await ParquetDataset.OpenAsync("d/", new ParquetDatasetOptions { Store = store }, ct);
        using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads });
        return async
            ? await DrainAsync(reader.ReadBatchesAsync(ct)).WaitAsync(Limit)
            : await Task.Run(() => Drain(reader.ReadBatches())).WaitAsync(Limit);
    }

    private static void AssertPrefix(List<long> ids, int files)
    {
        Assert.Equal(Enumerable.Range(0, ids.Count).Select(i => (long)i), ids); // in order, nothing skipped
        Assert.True(ids.Count >= files * RowsPerFile, $"{ids.Count} rows before the failure; the {files} files before it hold {files * RowsPerFile}");
    }

    public static TheoryData<int, bool> Modes => new() { { 1, false }, { 1, true }, { 4, false }, { 4, true }, { 16, true } };

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task AFailingOpen_SurfacesInOrder_NamingTheFile(int threads, bool async)
    {
        var store = new FaultStore(Data()) { FailOpen = path => path == PathOf(3) };
        var (ids, error) = await Read(store, threads, async);
        Assert.IsAssignableFrom<IOException>(error);
        Assert.Contains(PathOf(3), error!.Message);
        AssertPrefix(ids, 3);
        Assert.True(ids.Count <= 3 * RowsPerFile);
        await store.Settled();
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task AFailingFooterRead_SurfacesInOrder_NamingTheFile(int threads, bool async)
    {
        var store = new FaultStore(Data()) { FailRead = (path, tail) => path == PathOf(4) && tail };
        var (ids, error) = await Read(store, threads, async);
        Assert.IsAssignableFrom<IOException>(error);
        Assert.Contains(PathOf(4), error!.Message);
        AssertPrefix(ids, 4);
        Assert.True(ids.Count <= 4 * RowsPerFile);
        await store.Settled();
    }

    [Theory]
    [MemberData(nameof(Modes))]
    public async Task AFailingDataRead_SurfacesInOrder_NamingTheFile(int threads, bool async)
    {
        var store = new FaultStore(Data()) { FailRead = (path, tail) => path == PathOf(5) && !tail };
        var (ids, error) = await Read(store, threads, async);
        Assert.IsAssignableFrom<IOException>(error);
        Assert.Contains(PathOf(5), error!.Message);
        AssertPrefix(ids, 5);
        Assert.True(ids.Count < 6 * RowsPerFile);
        await store.Settled();
    }

    /// <summary>A read failing while a file is planned (its page index, read to prune pages for a filter) names the file.</summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task AFailingIndexRead_NamesTheFile(int threads, bool async)
    {
        var store = new FaultStore(Data()) { FailRead = (path, tail) => path == PathOf(5) && !tail };
        long wanted = 5L * RowsPerFile + 1234;
        var filter = Kaminari.Samidare.IR.ScalarExpr.NewBinary(Kaminari.Samidare.IR.BinaryOp.Eq, Kaminari.Samidare.IR.ScalarExpr.NewColumn("id"),
            Kaminari.Samidare.IR.ScalarExpr.NewLiteral(Kaminari.Samidare.IR.LiteralValue.NewInt64(wanted)));
        using var dataset = await ParquetDataset.OpenAsync("d/", new ParquetDatasetOptions { Store = store });
        using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads, Filter = filter, FilterRows = true });
        var (ids, error) = async ? await DrainAsync(reader.ReadBatchesAsync()).WaitAsync(Limit) : await Task.Run(() => Drain(reader.ReadBatches())).WaitAsync(Limit);
        Assert.IsAssignableFrom<IOException>(error);
        Assert.Contains(PathOf(5), error!.Message);
        Assert.Empty(ids);
        await store.Settled();
    }

    /// <summary>A read that never answers ends with the caller's cancellation, and leaves nothing open.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(16)]
    public async Task AHungRead_EndsWithTheCancellation(int threads)
    {
        var store = new FaultStore(Data()) { Hang = (path, tail) => path == PathOf(2) && !tail };
        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        var (ids, error) = await Read(store, threads, async: true, cts.Token);
        Assert.IsAssignableFrom<OperationCanceledException>(error);
        AssertPrefix(ids, 0);
        Assert.True(ids.Count <= 2 * RowsPerFile);
        await store.Settled();
    }

    /// <summary>A consumer that stops reading while a later file's read hangs is not held up, and leaves nothing open.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    [InlineData(16)]
    public async Task AConsumerStopping_IsNotHeldByAHungRead(int threads)
    {
        var store = new FaultStore(Data()) { Hang = (path, _) => path == PathOf(1) };
        await Task.Run(async () =>
        {
            using var dataset = await ParquetDataset.OpenAsync("d/", new ParquetDatasetOptions { Store = store });
            await using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads });
            await foreach (RecordBatch batch in reader.ReadBatchesAsync())
            {
                batch.Dispose();
                break; // a batch of the first file
            }
        }).WaitAsync(Limit);
        await store.Settled();
    }

    /// <summary>Answers coming late and out of order: the same rows, in order, at every degree of parallelism.</summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task AStoreAnsweringLate_ReadsTheRowsInOrder(int threads, bool async)
    {
        var store = new FaultStore(Data()) { MaxDelayMs = 4 };
        var (ids, error) = await Read(store, threads, async);
        Assert.Null(error);
        Assert.Equal(Enumerable.Range(0, Files * RowsPerFile).Select(i => (long)i), ids);
        await store.Settled();
    }

    /// <summary>
    /// A store over memory whose sources are not local (as a network's), failing, hanging or answering late as told,
    /// counting the sources opened and disposed.
    /// </summary>
    private sealed class FaultStore(MemoryFileStore inner) : IFileStore
    {
        private int _opened, _disposed;

        public Func<string, bool>? FailOpen { get; init; }

        /// <summary>Whether a read of a file fails (with whether it reads the file's tail: its footer).</summary>
        public Func<string, bool, bool>? FailRead { get; init; }

        /// <summary>Whether a read of a file never answers (an asynchronous one until cancelled).</summary>
        public Func<string, bool, bool>? Hang { get; init; }

        public int MaxDelayMs { get; init; }

        /// <summary>Waits for opens still running to finish and their sources to be disposed; asserts none is left.</summary>
        public async Task Settled()
        {
            for (int i = 0; i < 1000 && Volatile.Read(ref _opened) != Volatile.Read(ref _disposed); i++) await Task.Delay(10);
            Assert.Equal(Volatile.Read(ref _opened), Volatile.Read(ref _disposed));
        }

        public bool CanList => true;

        public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, recursive, cancellationToken);

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public async ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default)
        {
            await Delay(cancellationToken);
            if (FailOpen?.Invoke(file.Path) == true) throw new IOException("connection refused");
            return Wrap(file.Path, await inner.OpenAsync(file, cancellationToken));
        }

        public async ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default)
        {
            await Delay(cancellationToken);
            if (FailOpen?.Invoke(path) == true) throw new IOException("connection refused");
            return await inner.OpenAsync(path, cancellationToken) is { } source ? Wrap(path, source) : null;
        }

        private Task Delay(CancellationToken ct) => MaxDelayMs > 0 ? Task.Delay(Random.Shared.Next(MaxDelayMs + 1), ct) : Task.CompletedTask;

        private Source Wrap(string path, IRandomAccess source)
        {
            Interlocked.Increment(ref _opened);
            return new Source(this, path, source);
        }

        private sealed class Source(FaultStore owner, string path, IRandomAccess inner) : IRandomAccess, IDisposable
        {
            private int _done;

            public long Length => inner.Length;

            private bool Tail(long offset, int length) => offset + length == Length;

            public int ReadAt(long offset, Span<byte> destination)
            {
                bool tail = Tail(offset, destination.Length);
                if (owner.FailRead?.Invoke(path, tail) == true) throw new IOException("connection reset");
                if (owner.Hang?.Invoke(path, tail) == true) Thread.Sleep(Timeout.Infinite); // not used: sync reads cannot be cancelled
                if (owner.MaxDelayMs > 0) Thread.Sleep(Random.Shared.Next(owner.MaxDelayMs + 1));
                return inner.ReadAt(offset, destination);
            }

            public async ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
            {
                bool tail = Tail(offset, destination.Length);
                await owner.Delay(cancellationToken);
                if (owner.FailRead?.Invoke(path, tail) == true) throw new IOException("connection reset");
                if (owner.Hang?.Invoke(path, tail) == true) await Task.Delay(Timeout.Infinite, cancellationToken);
                return await inner.ReadAtAsync(offset, destination, cancellationToken);
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _done, 1) == 1) return;
                Interlocked.Increment(ref owner._disposed);
            }
        }
    }
}
