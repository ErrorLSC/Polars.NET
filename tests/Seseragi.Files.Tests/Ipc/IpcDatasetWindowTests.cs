using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// <see cref="IpcDatasetReader"/>'s window: the batches of many files read side by side come as read one after another
/// (whatever the threads, the store, sync or async, rows numbered, filtered, cut); a read stopped early or failing frees
/// every file it opened; a failure surfaces after the rows before it; requests stay within their bound; typed rows.
/// </summary>
public sealed class IpcDatasetWindowTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-ipc-window-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private string At(string relative) => Path.Combine(_root, relative);

    /// <summary>
    /// Partitioned files (p = file % 3) of 'id' (each row's number across the files, in the order a dataset reads them:
    /// by path) and 'name': some of one batch, some of several, one empty.
    /// </summary>
    private string Files(string name, int files, IIpcCompression? codec = null)
    {
        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, true)], null);
        long row = 0;
        foreach (int f in Enumerable.Range(0, files).OrderBy(f => $"p={f % 3}/f{f:D3}.arrow", StringComparer.Ordinal))
        {
            int rows = f == 5 ? 0 : f % 4 == 0 ? 2500 : 37;
            string path = At($"{name}/p={f % 3}/f{f:D3}.arrow");
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            using IpcWriter writer = IpcWriter.Create(path, schema, new IpcWriteOptions { Compression = codec ?? NoCompression.Instance });
            for (int at = 0; at < rows; at += 1000)
            {
                int n = Math.Min(1000, rows - at);
                using var batch = new RecordBatch(schema,
                [
                    new Int64Array.Builder().AppendRange(Enumerable.Range(0, n).Select(i => row + at + i)).Build(),
                    new StringArray.Builder().AppendRange(Enumerable.Range(0, n).Select(i => $"n{row + at + i}")).Build(),
                ], n);
                writer.Write(batch);
            }
            writer.Complete();
            row += rows;
        }
        return At(name);
    }

    private static List<string> Rows(IEnumerable<RecordBatch> batches)
    {
        List<string> rows = [];
        foreach (RecordBatch batch in batches)
        {
            using (batch)
            {
                for (int i = 0; i < batch.Length; i++)
                    rows.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(c => batch.Column(c) switch
                    {
                        var a when a.IsNull(i) => "null",
                        Int64Array l => l.GetValue(i)!.Value.ToString(),
                        StringArray s => s.GetString(i),
                        StringViewArray v => v.GetString(i),
                        var a => a.ToString()!,
                    })));
            }
        }
        return rows;
    }

    private static readonly ScalarExpr Filter = ScalarExpr.NewBinary(BinaryOp.And,
        ScalarExpr.NewBinary(BinaryOp.NotEq, ScalarExpr.NewColumn("p"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(1))),
        ScalarExpr.NewBinary(BinaryOp.Lt, ScalarExpr.NewColumn("id"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(9000))));

    public static TheoryData<string> Reads => ["plain", "indexed", "filtered", "indexed filtered", "indexed filtered cut", "partitions only"];

    private static IpcReadOptions Options(string read) => read switch
    {
        "plain" => new(),
        "indexed" => new() { RowIndexColumn = "row" },
        "filtered" => new() { Filter = Filter, FilterRows = true },
        "indexed filtered" => new() { RowIndexColumn = "row", Filter = Filter, FilterRows = true }, // files of p=1 only counted
        "indexed filtered cut" => new() { RowIndexColumn = "row", Filter = Filter, FilterRows = true, MaxRows = 3000 },
        _ => new() { Columns = ["p"], Filter = Filter, FilterRows = true },
    };

    [Theory]
    [MemberData(nameof(Reads))]
    public async Task ManyFiles_ReadSideBySide_AsOneAfterAnother(string read)
    {
        string root = Files("many", 40, Lz4FrameCompression.Instance);
        using var dataset = IpcDataset.Open(root);
        IpcReadOptions options = Options(read);
        List<string> expected;
        using (IpcDatasetReader reader = dataset.OpenReader(options with { MaxDegreeOfParallelism = 1 }))
            expected = Rows(reader.ReadBatches());
        Assert.NotEmpty(expected);
        if (read.Contains("indexed")) Assert.All(expected, r => Assert.Equal(r.Split('|')[0], r.Split('|')[1])); // the number is the row's 'id'
        if (read == "partitions only") Assert.Equal(expected.Count, Rows(dataset.OpenReader(options with { Columns = ["id"] }).ReadBatches()).Count);

        foreach (IpcReadOptions way in (IpcReadOptions[])[options with { MaxDegreeOfParallelism = 2 }, options with { MaxDegreeOfParallelism = 16 },
                     options with { MaxDegreeOfParallelism = 16, MemoryMap = IpcMemoryMap.Off, InFlightBytes = 1 }])
        {
            using IpcDatasetReader reader = dataset.OpenReader(way);
            Assert.Equal(expected, Rows(reader.ReadBatches()));
            List<RecordBatch> batches = [];
            await foreach (RecordBatch batch in reader.ReadBatchesAsync()) batches.Add(batch);
            Assert.Equal(expected, Rows(batches));
        }

        // A store whose requests wait on the network: read through the window even on one thread.
        var store = new Counting(new LocalFileStore());
        using var remote = IpcDataset.Open(root + "/", new IpcDatasetOptions { Store = store, MaxRequestsInFlight = 3 });
        foreach (int threads in (int[])[1, 8])
        {
            using IpcDatasetReader reader = remote.OpenReader(options with { MaxDegreeOfParallelism = threads });
            Assert.Equal(expected, Rows(reader.ReadBatches()));
            Assert.InRange(reader.PeakRequests, 1, 3);
            Assert.InRange(store.PeakReads, 1, 3);
        }
        await store.Settled();
        Assert.Equal(store.Opened, store.Disposed);
    }

    /// <summary>A read stopped after a few batches frees every file it opened ahead, decoded batches included.</summary>
    [Fact]
    public async Task AReadStoppedEarly_FreesEveryFileOpenedAhead()
    {
        string root = Files("stop", 60);
        var store = new Counting(new LocalFileStore());
        using var dataset = IpcDataset.Open(root + "/", new IpcDatasetOptions { Store = store });
        foreach (string read in (string[])["plain", "indexed"])
        {
            using IpcDatasetReader reader = dataset.OpenReader(Options(read) with { MaxDegreeOfParallelism = 8 });
            int taken = 0;
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                batch.Dispose();
                if (++taken == 3) break;
            }
            Assert.True(store.Opened > 3, $"{store.Opened} files opened ahead");
            await store.Settled();
            Assert.Equal(store.Opened, store.Disposed);
        }
    }

    /// <summary>A file that fails to open surfaces after the rows of the files before it; every file opened is freed.</summary>
    [Fact]
    public async Task AFailure_SurfacesAfterTheRowsBeforeIt()
    {
        string root = Files("fail", 12);
        string damaged = Directory.GetFiles(root, "f007.arrow", SearchOption.AllDirectories).Single();
        var store = new Counting(new LocalFileStore());
        using var dataset = IpcDataset.Open(root + "/", new IpcDatasetOptions { Store = store });
        File.WriteAllBytes(damaged, [.. "ARROW1\0\0"u8, .. Enumerable.Repeat((byte)3, 100), .. "ARROW1"u8]);
        foreach (int threads in (int[])[1, 8])
        {
            List<string> rows = [];
            var error = Assert.Throws<IpcFormatException>(() =>
            {
                using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads, Columns = ["id"] });
                foreach (RecordBatch batch in reader.ReadBatches()) rows.AddRange(Rows([batch]));
            });
            Assert.StartsWith(damaged, error.Message);
            // Every row of the files before it, and none after.
            using var before = IpcDataset.Open(dataset.Files.TakeWhile(f => f.Path != damaged).Select(f => new IpcDatasetFile(f.Path)));
            Assert.Equal(Rows(before.OpenReader(new IpcReadOptions { Columns = ["id"], MaxDegreeOfParallelism = 1 }).ReadBatches()), rows);
            await store.Settled();
            Assert.Equal(store.Opened, store.Disposed);
        }
    }

    public sealed record Row(long Id, string? Name, long P, string File);

    [Fact]
    public async Task TypedRows_MapColumnsPartitionsAndTheFilePath()
    {
        string root = Files("typed", 9);
        long last = 0;
        using (var all = IpcDataset.Open(root))
            last = Rows(all.OpenReader(new IpcReadOptions { Columns = ["id"] }).ReadBatches()).Select(long.Parse).Max();
        List<Row> rows = IpcDataset.ReadList<Row>(root, r => r.P == 2 && r.Id < last - 10, new IpcDatasetOptions { FilePathColumn = "file" },
            new IpcReadOptions { MaxDegreeOfParallelism = 4 });
        Assert.NotEmpty(rows);
        Assert.All(rows, r =>
        {
            Assert.Equal(2, r.P);
            Assert.True(r.Id < last - 10);
            Assert.Equal($"n{r.Id}", r.Name);
            Assert.Contains("p=2", r.File);
        });
        Assert.Equal(rows, await IpcDataset.ReadListAsync<Row>(root, r => r.P == 2 && r.Id < last - 10, new IpcDatasetOptions { FilePathColumn = "file" }));
        using var dataset = IpcDataset.Open(root, new IpcDatasetOptions { FilePathColumn = "file" });
        using IpcDatasetReader<Row> reader = dataset.OpenReader<Row>(r => r.P == 2);
        Assert.Equal(dataset.Files.Select(f => f.Path.Contains("p=2")), reader.Inner.FilesRead);
        Assert.Contains(false, reader.Inner.FilesRead);
        // A predicate on the file path decides the files read too.
        string one = dataset.Files[^1].Path;
        using IpcDatasetReader<Row> byPath = dataset.OpenReader<Row>(r => r.File == one);
        Assert.Equal(dataset.Files.Select(f => f.Path == one), byPath.Inner.FilesRead);
        Assert.True(byPath.FiltersExactly);
        Assert.All(byPath.ReadRows(), r => Assert.Equal(one, r.File));
    }

    /// <summary>A store that counts the sources opened and disposed and the most reads at once.</summary>
    private sealed class Counting(IFileStore inner) : IFileStore
    {
        private int _opened, _disposed, _reads, _peak;

        public int Opened => Volatile.Read(ref _opened);

        public int Disposed => Volatile.Read(ref _disposed);

        public int PeakReads => Volatile.Read(ref _peak);

        /// <summary>Waits for the opens still running to finish and be disposed.</summary>
        public async Task Settled()
        {
            for (int i = 0; i < 500 && Opened != Disposed; i++) await Task.Delay(10);
        }

        public bool CanList => inner.CanList;

        public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, recursive, cancellationToken);

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public async ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) =>
            Wrap(await inner.OpenAsync(file, cancellationToken));

        public async ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) =>
            await inner.OpenAsync(path, cancellationToken) is { } source ? Wrap(source) : null;

        private Source Wrap(IRandomAccess source)
        {
            Interlocked.Increment(ref _opened);
            return new Source(this, source);
        }

        private sealed class Source(Counting owner, IRandomAccess inner) : IRandomAccess, IDisposable
        {
            private int _done;

            public long Length => inner.Length;

            public int ReadAt(long offset, Span<byte> destination)
            {
                Enter();
                try
                {
                    return inner.ReadAt(offset, destination);
                }
                finally
                {
                    Interlocked.Decrement(ref owner._reads);
                }
            }

            public async ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
            {
                Enter();
                try
                {
                    await Task.Delay(1, cancellationToken); // a request that waits: others may run meanwhile
                    return await inner.ReadAtAsync(offset, destination, cancellationToken);
                }
                finally
                {
                    Interlocked.Decrement(ref owner._reads);
                }
            }

            private void Enter()
            {
                int now = Interlocked.Increment(ref owner._reads);
                for (int peak; now > (peak = Volatile.Read(ref owner._peak));) Interlocked.CompareExchange(ref owner._peak, now, peak);
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _done, 1) == 1) return;
                (inner as IDisposable)?.Dispose();
                Interlocked.Increment(ref owner._disposed);
            }
        }
    }
}
