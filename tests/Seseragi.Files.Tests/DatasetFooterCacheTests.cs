using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests;

/// <summary>
/// Footers shared between scans (<see cref="DatasetOptions.FooterCache"/>): a scan of files another scan opened reads
/// no footer (the first file's, for the schema, neither); a file rewritten since (another size, another time written)
/// is read again; files given with their sizes (a table's log) match by path and size; small files and local ones are
/// not kept; the capacity holds (least recent first out); a disposed cache keeps nothing; Parquet and IPC; scans side
/// by side.
/// </summary>
public sealed class DatasetFooterCacheTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-footer-cache-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private string At(string relative) => Path.Combine(_root, relative);

    private static readonly Apache.Arrow.Schema Schema = new([new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, true)], null);

    private static RecordBatch Batch(long first, int rows) => new(Schema,
        [new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => first + i)).Build(),
         new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => $"n{first + i}")).Build()], rows);

    /// <summary>A Parquet file of <paramref name="rows"/> rows from <paramref name="first"/>, row groups of 1000 rows (its footer read apart from its tail when large).</summary>
    private string WriteParquet(string relative, long first, int rows)
    {
        string path = At(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        using (var writer = ParquetWriter.Create(path, Schema, new ParquetWriteOptions { RowGroupRows = 1000, Compression = NoCompression.Instance }))
        {
            writer.Write(Batch(first, rows));
            writer.Complete();
        }
        return path;
    }

    private string WriteIpc(string relative, long first, int rows)
    {
        string path = At(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        using (IpcWriter writer = IpcWriter.Create(path, Schema))
        {
            for (int at = 0; at < rows; at += 500)
            {
                using RecordBatch batch = Batch(first + at, Math.Min(500, rows - at));
                writer.Write(batch);
            }
            writer.Complete();
        }
        return path;
    }

    private const int Large = 8000;

    private static List<string> Read(ParquetDataset dataset)
    {
        using var reader = dataset.OpenReader();
        return ArrowRender.Rows(reader.ReadBatches());
    }

    private static List<string> Expected(params (long First, int Rows)[] files) =>
        [.. files.SelectMany(f => Enumerable.Range(0, f.Rows).Select(i => $"{f.First + i} | \"n{f.First + i}\""))];

    private ParquetDataset Open(string glob, IFileStore store, FooterCache? cache) =>
        ParquetDataset.Open(At(glob), new ParquetDatasetOptions { Store = store, FooterCache = cache });

    [Fact]
    public void AScanOfFilesAnotherScanOpened_ReadsNoFooter()
    {
        for (int i = 0; i < 3; i++) WriteParquet($"p/f{i}.parquet", i * Large, Large);
        var store = new Counting(new LocalFileStore()); // its sources are a store's, not local
        using var cache = new FooterCache();
        using (ParquetDataset first = Open("p/*.parquet", store, cache)) Assert.Equal(Expected((0, 3 * Large)), Read(first));
        Assert.Equal(3, store.TailReads);
        Assert.Equal(3, cache.Count);
        using (ParquetDataset second = Open("p/*.parquet", store, cache)) Assert.Equal(Expected((0, 3 * Large)), Read(second));
        Assert.Equal(3, store.TailReads); // none: the schema's footer neither
        Assert.Equal(3, cache.Hits);      // the schema's (the first file's, then kept by the dataset), the two others'
        // Without a cache each scan reads them.
        using (ParquetDataset alone = Open("p/*.parquet", store, null)) Assert.Equal(Expected((0, 3 * Large)), Read(alone));
        Assert.Equal(6, store.TailReads);
    }

    [Fact]
    public void AFileRewrittenSince_IsReadAgain()
    {
        WriteParquet("r/a.parquet", 0, Large);
        string b = WriteParquet("r/b.parquet", 10_000, Large); // five digits each, as the values written again
        long size = new FileInfo(b).Length;
        var store = new Counting(new LocalFileStore());
        using var cache = new FooterCache();
        using (ParquetDataset first = Open("r/*.parquet", store, cache)) Read(first);
        // The same size, written at another time: its footer is not taken.
        DateTime written = File.GetLastWriteTimeUtc(b);
        // Other values of the same size (the first of a few starts that encode alike).
        long other = (long[])[20_000, 30_000, 40_000, 50_000, 60_000, 70_000] is var starts
            ? starts.First(start => new FileInfo(WriteParquet("r-other/b.parquet", start, Large)).Length == size) : 0;
        File.Copy(WriteParquet("r-other/b.parquet", other, Large), b, overwrite: true);
        Assert.Equal(size, new FileInfo(b).Length);
        File.SetLastWriteTimeUtc(b, written.AddSeconds(5));
        int before = store.TailReads;
        using (ParquetDataset second = Open("r/*.parquet", store, cache)) Assert.Equal(Expected((0, Large), (other, Large)), Read(second));
        Assert.Equal(before + 1, store.TailReads); // b's alone
        // Another size (written at the time kept): read again too.
        WriteParquet("r/b.parquet", 0, Large + 500);
        File.SetLastWriteTimeUtc(b, written.AddSeconds(5));
        using (ParquetDataset third = Open("r/*.parquet", store, cache)) Assert.Equal(Expected((0, Large), (0, Large + 500)), Read(third));
        Assert.Equal(before + 2, store.TailReads);
    }

    [Fact]
    public void FilesGivenWithTheirSizes_MatchByPathAndSize()
    {
        string a = WriteParquet("g/a.parquet", 0, Large), b = WriteParquet("g/b.parquet", Large, Large);
        var store = new Counting(new LocalFileStore());
        using var cache = new FooterCache();
        ParquetDatasetFile[] files = [new(a, new FileInfo(a).Length), new(b, new FileInfo(b).Length)];
        using (var first = ParquetDataset.Open(files, new ParquetDatasetOptions { Store = store, FooterCache = cache })) Read(first);
        int before = store.TailReads;
        using (var second = ParquetDataset.Open(files, new ParquetDatasetOptions { Store = store, FooterCache = cache }))
            Assert.Equal(Expected((0, 2 * Large)), Read(second));
        Assert.Equal(before, store.TailReads);
    }

    [Fact]
    public void SmallAndLocalFiles_AreNotKept()
    {
        WriteParquet("s/a.parquet", 0, 100); // comes whole in the first read of its tail
        using var cache = new FooterCache();
        using (ParquetDataset small = Open("s/*.parquet", new Counting(new LocalFileStore()), cache)) Read(small);
        WriteParquet("l/a.parquet", 0, Large);
        using (var local = ParquetDataset.Open(At("l/*.parquet"), new ParquetDatasetOptions { FooterCache = cache })) Read(local);
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public void TheCapacityHolds_TheLeastRecentGoesFirst_ADisposedCacheKeepsNothing()
    {
        for (int i = 0; i < 3; i++) WriteParquet($"c/f{i}.parquet", i * Large, Large);
        var store = new Counting(new LocalFileStore());
        long one;
        using (var probe = new FooterCache())
        {
            using (ParquetDataset scan = Open("c/f[0].parquet", store, probe)) Read(scan);
            one = probe.Bytes;
        }
        using var cache = new FooterCache(2 * one + one / 2); // two footers
        // One file after another (in order: the least recent is f0, then f1).
        for (int i = 0; i < 3; i++)
        {
            using ParquetDataset scan = Open($"c/f[{i}].parquet", store, cache);
            Read(scan);
        }
        Assert.Equal(2, cache.Count);
        Assert.True(cache.Bytes <= cache.Capacity);
        int before = store.TailReads;
        // f1 and f2 kept (f0 out first); f1 used again, then f0 read: f2 is now the least recent, out.
        using (ParquetDataset older = Open("c/f[1].parquet", store, cache)) Read(older); // kept (listed: its size and version known)
        Assert.Equal(before, store.TailReads);
        using (ParquetDataset firstAgain = Open("c/f[0].parquet", store, cache)) Read(firstAgain); // out: read again, kept
        Assert.Equal(before + 1, store.TailReads);
        using (ParquetDataset recent = Open("c/f[1].parquet", store, cache)) Read(recent);
        Assert.Equal(before + 1, store.TailReads);
        using (ParquetDataset last = Open("c/f[2].parquet", store, cache)) Read(last);
        Assert.Equal(before + 2, store.TailReads);

        cache.Dispose();
        Assert.Equal(0, cache.Count);
        using (ParquetDataset after = Open("c/*.parquet", store, cache)) Assert.Equal(Expected((0, 3 * Large)), Read(after));
        Assert.Equal(0, cache.Count);
    }

    [Fact]
    public async Task ScansSideBySide_ShareTheFooters()
    {
        for (int i = 0; i < 4; i++) WriteParquet($"m/f{i}.parquet", i * Large, Large);
        var store = new Counting(new LocalFileStore());
        using var cache = new FooterCache();
        List<string>[] results = await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            using ParquetDataset scan = Open("m/*.parquet", store, cache);
            return Read(scan);
        })));
        Assert.All(results, rows => Assert.Equal(Expected((0, 4 * Large)), rows));
        Assert.Equal(4, cache.Count);
        int before = store.TailReads;
        using (ParquetDataset again = Open("m/*.parquet", store, cache)) Read(again);
        Assert.Equal(before, store.TailReads);
    }

    [Fact]
    public void IpcFiles_ShareTheirFootersToo()
    {
        for (int i = 0; i < 2; i++) WriteIpc($"i/f{i}.arrow", i * Large, Large);
        var store = new Counting(new LocalFileStore());
        using var cache = new FooterCache();
        List<string> Scan()
        {
            using var dataset = IpcDataset.Open(At("i/*.arrow"), new IpcDatasetOptions { Store = store, FooterCache = cache });
            using IpcDatasetReader reader = dataset.OpenReader();
            return ArrowRender.Rows(reader.ReadBatches());
        }
        Assert.Equal(Expected((0, 2 * Large)), Scan());
        Assert.Equal(2, cache.Count);
        int before = store.TailReads;
        Assert.Equal(Expected((0, 2 * Large)), Scan());
        Assert.Equal(before, store.TailReads);
    }

    [Fact]
    public void AFileNamedAlone_IsTakenAtTheVersionItsStoreTells_AndOnlyThen()
    {
        string a = WriteParquet("v/a.parquet", 0, Large);
        var store = new Counting(new LocalFileStore()) { TellsVersions = true };
        using var cache = new FooterCache();
        using (var first = ParquetDataset.Open(a, new ParquetDatasetOptions { Store = store, FooterCache = cache })) Read(first);
        int before = store.TailReads;
        using (var second = ParquetDataset.Open(a, new ParquetDatasetOptions { Store = store, FooterCache = cache })) Assert.Equal(Expected((0, Large)), Read(second));
        Assert.Equal(before, store.TailReads);
        // Written again (the same size, another time): another version, read again.
        DateTime written = File.GetLastWriteTimeUtc(a);
        WriteParquet("v/a.parquet", Large, Large);
        File.SetLastWriteTimeUtc(a, written.AddSeconds(5));
        using (var third = ParquetDataset.Open(a, new ParquetDatasetOptions { Store = store, FooterCache = cache })) Assert.Equal(Expected((Large, Large)), Read(third));
        Assert.Equal(before + 1, store.TailReads);
        // Its sources telling no version now: a file named alone never takes a footer (it is read).
        store.TellsVersions = false;
        using (var fourth = ParquetDataset.Open(a, new ParquetDatasetOptions { Store = store, FooterCache = cache })) Read(fourth);
        Assert.Equal(before + 2, store.TailReads);
    }

    /// <summary>
    /// A store over another whose sources are a store's (not local): counts the reads that end at a file's end (its
    /// tail); with <see cref="TellsVersions"/> its sources tell the file's size and when it was written.
    /// </summary>
    private sealed class Counting(IFileStore inner) : IFileStore
    {
        private int _tails;

        public bool TellsVersions { get; set; }

        public int TailReads => Volatile.Read(ref _tails);

        public bool CanList => inner.CanList;

        public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, recursive, cancellationToken);

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public async ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) =>
            Wrap(file.Path, await inner.OpenAsync(file, cancellationToken));

        public async ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) =>
            await inner.OpenAsync(path, cancellationToken) is { } source ? Wrap(path, source) : null;

        private IRandomAccess Wrap(string path, IRandomAccess source) => TellsVersions
            ? new Telling(this, source, new StoredFile(path, source.Length, new DateTimeOffset(File.GetLastWriteTimeUtc(path))))
            : new Source(this, source);

        private sealed class Telling(Counting owner, IRandomAccess inner, StoredFile stored) : IRandomAccess, IDisposable, IStoredSource
        {
            private readonly Source _source = new(owner, inner);

            public StoredFile Stored => stored;

            public long Length => inner.Length;

            public int ReadAt(long offset, Span<byte> destination) => _source.ReadAt(offset, destination);

            public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken) =>
                _source.ReadAtAsync(offset, destination, cancellationToken);

            public void Dispose() => _source.Dispose();
        }

        private sealed class Source(Counting owner, IRandomAccess inner) : IRandomAccess, IDisposable
        {
            public long Length => inner.Length;

            public int ReadAt(long offset, Span<byte> destination)
            {
                if (offset + destination.Length == Length && destination.Length > 8) Interlocked.Increment(ref owner._tails);
                return inner.ReadAt(offset, destination);
            }

            public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
            {
                if (offset + destination.Length == Length && destination.Length > 8) Interlocked.Increment(ref owner._tails);
                return inner.ReadAtAsync(offset, destination, cancellationToken);
            }

            public void Dispose() => (inner as IDisposable)?.Dispose();
        }
    }
}
