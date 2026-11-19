using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using FaultStore = Seseragi.Files.Tests.Parquet.ParquetDatasetFaultTests.FaultStore;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// IPC datasets over a store that fails, hangs or stalls the way a network does, and over local files that change
/// under the read: a failure surfaces in order (after the rows of the files before it) and names the file, whether
/// it hits a file's opening, its footer, a dictionary, a record batch, or the footer of a file left out but counted
/// for the row index; a hung read ends with the caller's cancellation or when the consumer stops; a store answering
/// late reads the same rows; a file replaced under its kept footer fails; every source opened is disposed.
/// </summary>
public sealed class IpcDatasetFaultTests : IDisposable
{
    private const int Files = 8, RowsPerFile = 20_000, BatchRows = 5_000;

    private static readonly TimeSpan Limit = TimeSpan.FromSeconds(30);

    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-ipc-faults-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    /// <summary>
    /// File <paramref name="f"/>: <c>id</c> (each row's number across the files), <c>s</c>, and with
    /// <paramref name="dictionary"/> a dictionary column <c>d</c> (one dictionary per file), in batches of
    /// <see cref="BatchRows"/>; LZ4 for odd files.
    /// </summary>
    private static byte[] File(int f, bool dictionary, int rows = RowsPerFile, long first = -1, bool stream = false)
    {
        first = first < 0 ? (long)f * RowsPerFile : first;
        var dictionaryType = new DictionaryType(Int32Type.Default, StringType.Default, false);
        List<Field> fields = [new Field("id", Int64Type.Default, true), new Field("s", StringType.Default, true)];
        if (dictionary) fields.Add(new Field("d", dictionaryType, true));
        var schema = new Schema(fields, null);
        using var output = new MemoryStream();
        using (IpcWriter writer = IpcWriter.Create(output, schema, new IpcWriteOptions
        {
            Compression = f % 2 == 1 ? Lz4FrameCompression.Instance : NoCompression.Instance,
            Format = stream ? IpcFormat.Stream : IpcFormat.File,
        }))
        {
            for (int at = 0; at < rows; at += BatchRows)
            {
                // The same dictionary for every batch (one per file), in buffers of its own (disposing a batch disposes them).
                StringArray entries = new StringArray.Builder().AppendRange(Enumerable.Range(0, 50).Select(i => $"category {f} {i}")).Build();
                int n = Math.Min(BatchRows, rows - at);
                List<IArrowArray> columns =
                [
                    new Int64Array.Builder().AppendRange(Enumerable.Range(0, n).Select(i => first + at + i)).Build(),
                    new StringArray.Builder().AppendRange(Enumerable.Range(0, n).Select(i => $"value {first + at + i} {(first + at + i) * 7919 % 104729}")).Build(),
                ];
                if (dictionary)
                    columns.Add(new DictionaryArray(dictionaryType, new Int32Array.Builder().AppendRange(Enumerable.Range(0, n).Select(i => (at + i) % 50)).Build(), entries));
                using var batch = new RecordBatch(schema, columns, n);
                writer.Write(batch);
            }
            writer.Complete();
        }
        byte[] bytes = output.ToArray();
        Assert.True(bytes.Length > IpcFile.TailGuess); // batches read apart from the footer
        return bytes;
    }

    /// <summary>The files, under <c>d/</c> (or, <paramref name="hive"/>, one partition each: <c>h/p={f}/</c>).</summary>
    private static MemoryFileStore Data(bool dictionary = false, bool hive = false, bool stream = false)
    {
        var store = new MemoryFileStore();
        for (int f = 0; f < Files; f++) store.Add(PathOf(f, hive), File(f, dictionary, stream: stream && f % 2 == 0)); // streams beside files
        return store;
    }

    private static string PathOf(int file, bool hive = false) => hive ? $"h/p={file}/f{file}.arrow" : $"d/f{file}.arrow";

    /// <summary>The ids read until the read ends (the exception it ends with, or null).</summary>
    private static (List<long> Ids, Exception? Error) Drain(IEnumerable<RecordBatch> batches)
    {
        List<long> ids = [];
        try
        {
            foreach (RecordBatch batch in batches)
            {
                using (batch) ids.AddRange(((Int64Array)batch.Column("id")).Values.ToArray());
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
                using (batch) ids.AddRange(((Int64Array)batch.Column("id")).Values.ToArray());
            }
            return (ids, null);
        }
        catch (Exception e)
        {
            return (ids, e);
        }
    }

    private static async Task<(List<long> Ids, Exception? Error)> Read(IFileStore store, int threads, bool async, IpcDatasetOptions? options = null,
        IpcReadOptions? read = null, string root = "d/", CancellationToken ct = default)
    {
        using var dataset = await IpcDataset.OpenAsync(root, (options ?? new()) with { Store = store }, ct);
        using IpcDatasetReader reader = dataset.OpenReader((read ?? new()) with { MaxDegreeOfParallelism = threads });
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
    public async Task AFailingBatchRead_SurfacesInOrder_NamingTheFile(int threads, bool async)
    {
        var store = new FaultStore(Data()) { FailRead = (path, tail) => path == PathOf(5) && !tail };
        var (ids, error) = await Read(store, threads, async);
        Assert.IsAssignableFrom<IOException>(error);
        Assert.Contains(PathOf(5), error!.Message);
        AssertPrefix(ids, 5);
        Assert.True(ids.Count < 6 * RowsPerFile);
        await store.Settled();
    }

    /// <summary>
    /// A file's dictionary read failing (its first read past the footer: dictionaries come before its batches) names
    /// the file, after every row of the files before it and none of its own; with dictionaries unified too.
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task AFailingDictionaryRead_SurfacesBeforeTheFilesRows(int threads, bool async)
    {
        foreach (bool unify in (bool[])[false, true])
        {
            int reads = 0;
            var store = new FaultStore(Data(dictionary: true)) { FailRead = (path, tail) => path == PathOf(5) && !tail && Interlocked.Increment(ref reads) == 1 };
            var (ids, error) = await Read(store, threads, async, new IpcDatasetOptions { UnifyDictionaries = unify });
            Assert.IsAssignableFrom<IOException>(error);
            Assert.Contains(PathOf(5), error!.Message);
            Assert.Equal(Enumerable.Range(0, 5 * RowsPerFile).Select(i => (long)i), ids);
            await store.Settled();
        }
    }

    /// <summary>
    /// A file left out by its partition is opened only when rows are numbered (its rows counted from its footer): a
    /// failure reading that footer then names it, after the rows before it; without the row index it is never opened.
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task AFileLeftOut_FailsOnlyWhenRowsAreNumbered(int threads, bool async)
    {
        ScalarExpr notThree = ScalarExpr.NewBinary(BinaryOp.NotEq, ScalarExpr.NewColumn("p"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(3)));
        var options = new IpcReadOptions { Filter = notThree, FilterRows = true };

        var store = new FaultStore(Data(hive: true)) { FailRead = (path, tail) => path == PathOf(3, hive: true) && tail };
        var (ids, error) = await Read(store, threads, async, read: options, root: "h/");
        Assert.Null(error);
        Assert.Equal(Enumerable.Range(0, Files * RowsPerFile).Where(i => i / RowsPerFile != 3).Select(i => (long)i), ids);
        Assert.DoesNotContain(PathOf(3, hive: true), store.OpenedPaths);
        await store.Settled();

        var counting = new FaultStore(Data(hive: true)) { FailRead = (path, tail) => path == PathOf(3, hive: true) && tail };
        (ids, error) = await Read(counting, threads, async, read: options with { RowIndexColumn = "n" }, root: "h/");
        Assert.IsAssignableFrom<IOException>(error);
        Assert.Contains(PathOf(3, hive: true), error!.Message);
        AssertPrefix(ids, 3);
        Assert.True(ids.Count <= 3 * RowsPerFile);
        await counting.Settled();
    }

    /// <summary>
    /// A stream file (beside files) whose every read fails — opening it, telling its format, scanning it — or whose
    /// batch reads fail after its scan: named, after every row of the files before it, in order.
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task AFailingStreamFile_SurfacesInOrder_NamingTheFile(int threads, bool async)
    {
        foreach (string how in (string[])["any", "after scan"])
        {
            int reads = 0;
            var store = new FaultStore(Data(dictionary: true, stream: true))
            {
                // File 4 is a stream: its first reads (its tail, its start, its scan) open it; those after read batches.
                FailRead = (path, _) => path == PathOf(4) && (how == "any" || Interlocked.Increment(ref reads) > 3),
            };
            var (ids, error) = await Read(store, threads, async, new IpcDatasetOptions { UnifyDictionaries = threads > 1 });
            Assert.IsAssignableFrom<IOException>(error);
            Assert.Contains(PathOf(4), error!.Message);
            AssertPrefix(ids, 4);
            Assert.True(ids.Count < 5 * RowsPerFile, $"{how}: {ids.Count} rows");
            await store.Settled();
        }
    }

    /// <summary>
    /// A stream file on a store whose scan fails past its first stretch (1 MiB: the scan goes on in the background while
    /// its batches are read): the rows of the batches found before are read, in order, then the error, naming the file.
    /// </summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task AStreamWhoseScanFailsMidway_SurfacesInOrder_NamingTheFile(int threads, bool async)
    {
        var memory = new MemoryFileStore();
        memory.Add("big/f0.arrow", File(0, dictionary: false, rows: 400_000, first: 0, stream: true));
        memory.Add("big/f1.arrow", File(1, dictionary: false, first: 400_000));
        var store = new FaultStore(memory) { FailAt = (path, offset, length) => path == "big/f0.arrow" && offset > 0 && length == 1 << 20 };
        var (ids, error) = await Read(store, threads, async, root: "big/");
        Assert.IsAssignableFrom<IOException>(error);
        Assert.Contains("big/f0.arrow", error!.Message);
        Assert.Equal(Enumerable.Range(0, ids.Count).Select(i => (long)i), ids);
        Assert.InRange(ids.Count, BatchRows, 400_000 - BatchRows);
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
        var (ids, error) = await Read(store, threads, async: true, ct: cts.Token);
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
        var store = new FaultStore(Data(dictionary: true)) { Hang = (path, _) => path == PathOf(1) };
        await Task.Run(async () =>
        {
            using var dataset = await IpcDataset.OpenAsync("d/", new IpcDatasetOptions { Store = store, UnifyDictionaries = true });
            await using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads });
            await foreach (RecordBatch batch in reader.ReadBatchesAsync())
            {
                batch.Dispose();
                break; // a batch of the first file
            }
        }).WaitAsync(Limit);
        await store.Settled();
    }

    /// <summary>Answers coming late and out of order: the same rows, in order, numbered alike, at every degree of parallelism.</summary>
    [Theory]
    [MemberData(nameof(Modes))]
    public async Task AStoreAnsweringLate_ReadsTheRowsInOrder(int threads, bool async)
    {
        var store = new FaultStore(Data(dictionary: true)) { MaxDelayMs = 4 };
        var (ids, error) = await Read(store, threads, async, new IpcDatasetOptions { UnifyDictionaries = true });
        Assert.Null(error);
        Assert.Equal(Enumerable.Range(0, Files * RowsPerFile).Select(i => (long)i), ids);
        await store.Settled();

        // Numbered: the row index is the row's id.
        using var dataset = await IpcDataset.OpenAsync("d/", new IpcDatasetOptions { Store = store });
        using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads, RowIndexColumn = "n", Columns = ["id"] });
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch) Assert.Equal(((Int64Array)batch.Column("n")).Values.ToArray(), ((Int64Array)batch.Column("id")).Values.ToArray());
        }
        await store.Settled();
    }

    /// <summary>
    /// A file replaced on the store after its footer was kept (a store that tells no version): the next read finds the
    /// kept footer's blocks no longer fit the file (a body of another length, a block past the end) and fails naming
    /// the file, after the rows before it; the old footer is never read over the new bytes. (A replacement whose every
    /// block kept its place and length would be read through the old footer: its batches are the new file's.)
    /// </summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task AFileReplacedUnderItsKeptFooter_IsNotReadThroughIt(int threads)
    {
        MemoryFileStore memory = Data();
        var store = new FaultStore(memory);
        using var dataset = await IpcDataset.OpenAsync("d/", new IpcDatasetOptions { Store = store });
        using (IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads }))
            Assert.Equal(Files * RowsPerFile, Drain(reader.ReadBatches()).Ids.Count);
        Assert.Equal(Files, dataset.Footers.Count);

        foreach (var (rows, first) in (ValueTuple<int, long>[])[(RowsPerFile, 1_000_000), (RowsPerFile / 2 + 7, 2_000_000)]) // other values; fewer rows too
        {
            memory.Remove(PathOf(2));
            memory.Add(PathOf(2), File(2, dictionary: false, rows, first));
            using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads });
            var (ids, error) = Drain(reader.ReadBatches());
            List<long> before = [.. Enumerable.Range(0, 2 * RowsPerFile).Select(i => (long)i)];
            Assert.IsType<IpcFormatException>(error);
            Assert.StartsWith(PathOf(2), error.Message);
            Assert.Equal(before, ids);
        }
        await store.Settled();
    }

    // ---- local files: the one-thread path reads file after file on the calling thread ------------------------------

    /// <summary>Local files that disappear, shrink or turn to junk after the dataset listed them: named, after the rows before them.</summary>
    [Theory]
    [InlineData("deleted", 1)]
    [InlineData("deleted", 4)]
    [InlineData("truncated", 1)]
    [InlineData("truncated", 4)]
    [InlineData("junk", 1)]
    [InlineData("junk", 4)]
    public async Task ALocalFileChangedAfterListing_IsNamed(string how, int threads)
    {
        string root = Path.Combine(_root, how + threads);
        Directory.CreateDirectory(root);
        for (int f = 0; f < 4; f++) System.IO.File.WriteAllBytes(Path.Combine(root, $"f{f}.arrow"), File(f, dictionary: f == 2));
        foreach (bool async in (bool[])[false, true])
        {
            using var dataset = IpcDataset.Open(root);
            string victim = Path.Combine(root, "f2.arrow");
            byte[] kept = System.IO.File.ReadAllBytes(victim);
            switch (how)
            {
                case "deleted": System.IO.File.Delete(victim); break;
                case "truncated": System.IO.File.WriteAllBytes(victim, kept.AsSpan(0, kept.Length / 2).ToArray()); break;
                default: System.IO.File.WriteAllBytes(victim, [.. "ARROW1\0\0"u8, .. Enumerable.Repeat((byte)5, 200), .. "ARROW1"u8]); break;
            }
            try
            {
                using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { MaxDegreeOfParallelism = threads });
                var (ids, error) = async ? await DrainAsync(reader.ReadBatchesAsync()) : Drain(reader.ReadBatches());
                Assert.True(error is IpcFormatException or IOException, error?.ToString() ?? "no error");
                Assert.Contains(victim, error!.Message);
                Assert.Equal(Enumerable.Range(0, 2 * RowsPerFile).Select(i => (long)i), ids);
            }
            finally
            {
                System.IO.File.WriteAllBytes(victim, kept);
            }
        }
    }
}
