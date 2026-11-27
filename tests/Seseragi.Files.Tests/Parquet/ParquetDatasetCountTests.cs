using System.Buffers.Binary;
using System.Collections.Concurrent;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetDatasetReader.CountRows"/>: the rows a read delivers, against a read of the same options; from
/// the footers alone (no byte before a file's footer read; a limit stops the footers read) unless a filter over the
/// files' columns leaves rows out.
/// </summary>
public sealed class ParquetDatasetCountTests
{
    private static byte[] File(long first, int rows, int rowGroupRows)
    {
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        for (int i = 0; i < rows; i++)
        {
            ids.Append(first + i);
            names.Append($"name {first + i} {new string('x', (int)((first + i) % 17))}");
        }
        var schema = new Apache.Arrow.Schema([new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, true)], null);
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions { RowGroupRows = rowGroupRows }))
        {
            writer.Write(new RecordBatch(schema, [ids.Build(), names.Build()], rows));
            writer.Complete();
        }
        return stream.ToArray();
    }

    /// <summary>Files under <c>d/k=…/</c>: 3 partitions, file sizes and row groups of every kind (an empty file too).</summary>
    private static (MemoryFileStore Store, Dictionary<string, long> FooterStarts) Store()
    {
        var store = new MemoryFileStore();
        Dictionary<string, long> starts = [];
        (string Path, long First, int Rows, int Group)[] files =
        [
            ("d/k=0/a.parquet", 0, 15_000, 4000),
            ("d/k=0/b.parquet", 15_000, 0, 100),
            ("d/k=1/c.parquet", 15_000, 700, 300),
            ("d/k=2/e.parquet", 15_700, 10, 1000),
            ("d/k=2/f.parquet", 15_710, 20_000, 5120),
        ];
        foreach (var (path, first, rows, group) in files)
        {
            byte[] bytes = File(first, rows, group);
            store.Add(path, bytes);
            // The footer starts its length (the 4 bytes before "PAR1") before the tail; opening a file reads the
            // footer's guess (its last 64 KiB) at once.
            long footer = bytes.Length - 8 - BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(bytes.Length - 8));
            starts[path] = Math.Min(footer, bytes.Length - ParquetMetadata.TailGuess);
        }
        return (store, starts);
    }

    private static long Read(ParquetDataset dataset, ParquetReadOptions options)
    {
        using var reader = dataset.OpenReader(options);
        long rows = 0;
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch) rows += batch.Length;
        }
        return rows;
    }

    private static ScalarExpr Compare(BinaryOp op, string column, long value) =>
        ScalarExpr.NewBinary(op, ScalarExpr.NewColumn(column), ScalarExpr.NewLiteral(LiteralValue.NewInt64(value)));

    public static TheoryData<string> MetadataCases => ["all", "head 0", "head 5", "head 1600", "head past the end", "row index", "partition k = 1", "partition k <> 1 + head 1505", "no partition"];

    private static ParquetReadOptions MetadataOptions(string name) => name switch
    {
        "all" => new(),
        "head 0" => new() { MaxRows = 0 },
        "head 5" => new() { MaxRows = 5 },
        "head 1600" => new() { MaxRows = 1600 },
        "head past the end" => new() { MaxRows = 1_000_000 },
        "row index" => new() { RowIndexColumn = "n", Columns = ["name"] },
        "partition k = 1" => new() { Filter = Compare(BinaryOp.Eq, "k", 1), FilterRows = true },
        "partition k <> 1 + head 1505" => new() { Filter = Compare(BinaryOp.NotEq, "k", 1), FilterRows = true, MaxRows = 1505 },
        "no partition" => new() { Filter = Compare(BinaryOp.Eq, "k", 9), FilterRows = true },
        _ => throw new ArgumentOutOfRangeException(nameof(name)),
    };

    [Theory]
    [MemberData(nameof(MetadataCases))]
    public async Task WithoutARowFilter_TheCountComesFromTheFooters(string name)
    {
        var (inner, starts) = Store();
        foreach (int threads in (int[])[1, 4])
        {
            ParquetReadOptions options = MetadataOptions(name) with { MaxDegreeOfParallelism = threads };
            long expected;
            using (var plain = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = inner })) expected = Read(plain, options);

            var store = new TailStore(inner);
            using var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = store });
            using var reader = dataset.OpenReader(options);
            Assert.Equal(expected, reader.CountRows());
            Assert.Equal(expected, await reader.CountRowsAsync());
            // The large files' data lies before the bytes their footers' reads reach.
            Assert.True(starts["d/k=0/a.parquet"] > 0 && starts["d/k=2/f.parquet"] > 0);
            Assert.All(store.LowestRead, read => Assert.True(read.Value >= starts[read.Key], $"{name}: {read.Key} read at {read.Value}, before its footer at {starts[read.Key]}"));
        }
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task WithARowFilter_TheRowsAreReadAndCounted(bool filterRows)
    {
        var (store, _) = Store();
        using var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = store });
        ScalarExpr[] filters =
        [
            Compare(BinaryOp.GtEq, "id", 1600),
            Compare(BinaryOp.Lt, "id", 0),
            ScalarExpr.NewBinary(BinaryOp.And, Compare(BinaryOp.Eq, "k", 2), Compare(BinaryOp.Lt, "id", 15_800)),
            ScalarExpr.NewBinary(BinaryOp.Or, Compare(BinaryOp.Eq, "k", 0), Compare(BinaryOp.Gt, "id", 30_000)),
        ];
        foreach (ScalarExpr filter in filters)
        {
            foreach (long? head in (long?[])[null, 7])
            {
                var options = new ParquetReadOptions { Filter = filter, FilterRows = filterRows, MaxRows = head, MaxDegreeOfParallelism = 4 };
                long expected = Read(dataset, options);
                using var reader = dataset.OpenReader(options);
                Assert.Equal(expected, reader.CountRows());
                Assert.Equal(expected, await reader.CountRowsAsync());
            }
        }
        // Rows are filtered: fewer than the files hold.
        using (var filtered = dataset.OpenReader(new ParquetReadOptions { Filter = filters[0], FilterRows = true })) Assert.Equal(34_110, filtered.CountRows());
    }

    [Fact]
    public async Task RowsSelected_AreCounted_AndASelectionPastTheFile_FailsAsTheReadDoes()
    {
        var (store, _) = Store();
        StoredFile a = await store.StatAsync("d/k=0/a.parquet") ?? throw new FileNotFoundException("a");
        StoredFile c = await store.StatAsync("d/k=1/c.parquet") ?? throw new FileNotFoundException("c");
        using (var dataset = ParquetDataset.Open(
        [
            new ParquetDatasetFile(a.Path, a.Size, Rows: RowRanges.Of([(3, 10), (1400, 100)])),
            new ParquetDatasetFile(c.Path, c.Size),
        ], new ParquetDatasetOptions { Store = store }))
        {
            using var reader = dataset.OpenReader();
            Assert.Equal(810, Read(dataset, new ParquetReadOptions()));
            Assert.Equal(810, reader.CountRows());
        }
        using var past = ParquetDataset.Open([new ParquetDatasetFile(c.Path, c.Size, Rows: RowRanges.Of([(690, 20)]))], new ParquetDatasetOptions { Store = store });
        using var pastReader = past.OpenReader();
        Assert.Throws<ArgumentOutOfRangeException>(() => Read(past, new ParquetReadOptions()));
        var counted = Assert.Throws<ArgumentOutOfRangeException>(() => pastReader.CountRows());
        Assert.Contains(c.Path, counted.Message);
        Assert.Contains("the file has 700 rows", counted.Message);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public async Task ALimit_ReadsTheFootersOfTheFilesItReachesOnly(int threads)
    {
        var inner = new MemoryFileStore();
        for (int f = 0; f < 200; f++) inner.Add($"m/f{f:D3}.parquet", File(f * 10, 10, 10));
        var store = new TailStore(inner);
        using var dataset = ParquetDataset.Open("m/", new ParquetDatasetOptions { Store = store });
        foreach (long head in (long[])[0, 15, 165])
        {
            store.LowestRead.Clear();
            using var reader = dataset.OpenReader(new ParquetReadOptions { MaxRows = head, MaxDegreeOfParallelism = threads });
            Assert.Equal(head, reader.CountRows());
            Assert.Equal(head, await reader.CountRowsAsync());
            // A window of footers past the file reaching the limit at most (the first file's footer is kept).
            Assert.InRange(store.LowestRead.Count, 0, (int)(head / 10) + 16);
        }
        using var whole = dataset.OpenReader();
        Assert.Equal(2000, whole.CountRows());
    }

    /// <summary>A store recording, per file, the lowest offset read.</summary>
    private sealed class TailStore(MemoryFileStore inner) : IFileStore
    {
        public ConcurrentDictionary<string, long> LowestRead { get; } = [];

        public bool CanList => true;

        public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, recursive, cancellationToken);

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public async ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) =>
            new Source(await inner.OpenAsync(file, cancellationToken), file.Path, this);

        public async ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) =>
            await inner.OpenAsync(path, cancellationToken) is { } source ? new Source(source, path, this) : null;

        private void Read(string path, long offset) => LowestRead.AddOrUpdate(path, offset, (_, lowest) => Math.Min(lowest, offset));

        private sealed class Source(IRandomAccess inner, string path, TailStore owner) : IRandomAccess, IDisposable
        {
            public long Length => inner.Length;

            public int ReadAt(long offset, Span<byte> destination)
            {
                owner.Read(path, offset);
                return inner.ReadAt(offset, destination);
            }

            public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
            {
                owner.Read(path, offset);
                return inner.ReadAtAsync(offset, destination, cancellationToken);
            }

            public void Dispose() => (inner as IDisposable)?.Dispose();
        }
    }
}
