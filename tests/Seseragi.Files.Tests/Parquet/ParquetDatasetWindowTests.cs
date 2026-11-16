using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetDatasetReader"/> reading several files' row groups side by side: the same rows as file after
/// file on one thread (order, row numbers, limits, filters, conversions, missing columns, partitions), failures in
/// order, the in-flight budget, files opened ahead; and the typed rows of a dataset (<see cref="ParquetDatasetReader{T}"/>).
/// </summary>
public sealed class ParquetDatasetWindowTests
{
    private static byte[] File(int first, int rows, int rowGroupRows, bool narrow, bool withS, bool doubles = false)
    {
        IArrowArray x = doubles ? new DoubleArray.Builder().AppendRange(Enumerable.Range(first, rows).Select(i => i * 7 % 1000 + 0.5)).Build()
            : narrow ? new Int32Array.Builder().AppendRange(Enumerable.Range(first, rows).Select(i => i * 7 % 1000)).Build()
            : new Int64Array.Builder().AppendRange(Enumerable.Range(first, rows).Select(i => (long)(i * 7 % 1000))).Build();
        List<(string, IArrowArray)> columns = [("id", new Int64Array.Builder().AppendRange(Enumerable.Range(first, rows).Select(i => (long)i)).Build()), ("x", x)];
        if (withS) columns.Add(("s", new StringArray.Builder().AppendRange(Enumerable.Range(first, rows).Select(i => i % 5 == 0 ? null : $"value number {i}")).Build()));
        var schema = new Apache.Arrow.Schema([.. columns.Select(c => new Field(c.Item1, c.Item2.Data.DataType, true))], null);
        using var stream = new MemoryStream();
        using (var writer = ParquetWriter.Create(stream, schema, new ParquetWriteOptions { RowGroupRows = rowGroupRows }))
        {
            writer.Write(new RecordBatch(schema, [.. columns.Select(c => c.Item2)], rows));
            writer.Complete();
        }
        return stream.ToArray();
    }

    private static List<string> Rows(IEnumerable<RecordBatch> batches)
    {
        List<string> rows = [];
        foreach (RecordBatch batch in batches)
        {
            using (batch)
            {
                for (int i = 0; i < batch.Length; i++)
                    rows.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(c => Render(batch.Column(c), i))));
            }
        }
        return rows;
    }

    private static async Task<List<string>> RowsAsync(IAsyncEnumerable<RecordBatch> batches)
    {
        List<RecordBatch> all = [];
        await foreach (RecordBatch batch in batches) all.Add(batch);
        return Rows(all);
    }

    private static string Render(IArrowArray array, int i) => array switch
    {
        StringArray s => s.GetString(i) ?? "null",
        StringViewArray v => v.GetString(i) ?? "null",
        _ => CanonicalRender.Value(array, i),
    };

    private static ScalarExpr Compare(BinaryOp op, string column, LiteralValue value) =>
        ScalarExpr.NewBinary(op, ScalarExpr.NewColumn(column), ScalarExpr.NewLiteral(value));

    [Fact]
    public async Task ManyThreads_ReadTheRowsOneThreadReads()
    {
        var random = new Random(2026_11_16);
        for (int round = 0; round < 40; round++)
        {
            var store = new MemoryFileStore();
            int files = random.Next(1, 26), id = 0;
            string[] keys = ["a", "b", "__HIVE_DEFAULT_PARTITION__"];
            // In path order (the dataset's): the first file has the wide column, the others may be narrower (or, with
            // lossy casts, the first holds doubles and the others integers: filters over them apply after converting).
            bool lossy = random.Next(3) == 0;
            string[] paths = [.. Enumerable.Range(0, files).Select(f => $"d/k={keys[random.Next(3)]}/f{f:D2}.parquet").Order(StringComparer.Ordinal)];
            for (int f = 0; f < files; f++)
            {
                int rows = random.Next(1, 300);
                store.Add(paths[f], File(id, rows, random.Next(1, 120), narrow: f > 0 && random.Next(2) == 0, withS: random.Next(3) != 0, doubles: lossy && f == 0));
                id += rows;
            }
            var options = new ParquetDatasetOptions
            {
                Store = store, AllowMissingColumns = true, AllowLossyCasts = lossy, FilePathColumn = random.Next(2) == 0 ? "file" : null,
                MaxFilesInFlight = random.Next(3) == 0 ? random.Next(1, 5) : null,
            };
            ScalarExpr? filter = random.Next(5) switch
            {
                0 => Compare(BinaryOp.Gt, "x", lossy ? LiteralValue.NewFloat64(random.Next(1000)) : LiteralValue.NewInt64(random.Next(1000))),
                1 => Compare(BinaryOp.Eq, "k", LiteralValue.NewString("b")),
                2 => ScalarExpr.NewBinary(BinaryOp.Or, Compare(BinaryOp.Lt, "x", lossy ? LiteralValue.NewFloat64(100) : LiteralValue.NewInt64(100)),
                    Compare(BinaryOp.Eq, "k", LiteralValue.NewString("a"))),
                3 => ScalarExpr.NewUnary(UnaryOp.IsNull, ScalarExpr.NewColumn("s")),
                _ => null,
            };
            ParquetReadOptions read = new()
            {
                Filter = filter,
                FilterRows = random.Next(3) != 0,
                MaxRows = random.Next(3) == 0 ? random.Next(0, id + 10) : null,
                RowIndexColumn = random.Next(2) == 0 ? "n" : null,
                BatchSize = random.Next(2) == 0 ? 37 : 65_536,
            };
            using var dataset = ParquetDataset.Open("d/", options);
            List<string> expected;
            using (var reader = dataset.OpenReader(read with { MaxDegreeOfParallelism = 1 })) expected = Rows(reader.ReadBatches());
            foreach (int threads in (int[])[2, 8])
            {
                using var reader = dataset.OpenReader(read with { MaxDegreeOfParallelism = threads });
                Assert.Equal(expected, Rows(reader.ReadBatches()));
                Assert.Equal(expected, await RowsAsync(reader.ReadBatchesAsync()));
            }
        }
    }

    [Fact]
    public void AFailingFile_SurfacesAfterTheRowsBeforeIt()
    {
        var store = new MemoryFileStore();
        for (int f = 0; f < 8; f++) store.Add($"d/f{f}.parquet", f == 3 ? [.. "PAR1"u8, .. new byte[64], .. "PAR1"u8] : File(f * 10, 10, 4, false, true));
        using var dataset = ParquetDataset.Open(["d/f0.parquet", "d/f1.parquet", "d/f2.parquet", "d/f3.parquet", "d/f4.parquet"], new ParquetDatasetOptions { Store = store });
        foreach (int threads in (int[])[1, 8])
        {
            using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads, Columns = ["id"] });
            List<string> rows = [];
            var error = Assert.Throws<ParquetFormatException>(() =>
            {
                foreach (RecordBatch batch in reader.ReadBatches())
                {
                    using (batch) rows.AddRange(Enumerable.Range(0, batch.Length).Select(i => Render(batch.Column(0), i)));
                }
            });
            Assert.Contains("d/f3.parquet", error.Message);
            Assert.Equal([.. Enumerable.Range(0, 30).Select(i => i.ToString())], rows);
        }
    }

    [Fact]
    public void RowGroups_InFlight_StayWithinTheBudget_TheOneWaitedForAside()
    {
        var store = new MemoryFileStore();
        for (int f = 0; f < 12; f++) store.Add($"d/f{f:D2}.parquet", File(f * 2000, 2000, 500, false, true));
        using var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = store });
        using (var tight = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 8, InFlightBytes = 1 }))
        {
            Assert.Equal(24_000, Rows(tight.ReadBatches()).Count);
            Assert.Equal(1, tight.PeakInFlight.Units); // only the unit waited for runs, over the budget
        }
        using (var roomy = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 8 }))
        {
            Assert.Equal(24_000, Rows(roomy.ReadBatches()).Count);
            Assert.InRange(roomy.PeakInFlight.Units, 2, 8 * ParquetDatasetReader.UnitsAheadPerThread + 1);
        }
        using var bounded = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 8, InFlightBytes = 40_000 });
        Assert.Equal(24_000, Rows(bounded.ReadBatches()).Count);
        Assert.True(bounded.PeakInFlight.Bytes <= 40_000 || bounded.PeakInFlight.Units == 1, $"{bounded.PeakInFlight}");
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public void FilesInFlight_AsAsked(int files)
    {
        var store = new MemoryFileStore();
        for (int f = 0; f < 20; f++) store.Add($"d/f{f:D2}.parquet", File(f * 5, 5, 5, false, false));
        var counting = new PeakStore(store);
        using var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = counting, MaxFilesInFlight = files });
        foreach (int threads in (int[])[1, 8])
        {
            counting.Reset();
            using var reader = dataset.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = threads, Columns = ["id"] });
            Assert.Equal([.. Enumerable.Range(0, 100).Select(i => i.ToString())], Rows(reader.ReadBatches()));
            Assert.InRange(counting.Peak, 1, files);
            if (threads > 1) Assert.InRange(reader.PeakInFlight.Files, 1, files);
        }
        Assert.Throws<ArgumentOutOfRangeException>(() => ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = store, MaxFilesInFlight = 0 }));
    }

    // ---- typed rows -----------------------------------------------------------------------------------------------

    private sealed record Row(long Id, long X, string? K, string? S);

    private sealed record Located(long Id, string File);

    private static (MemoryFileStore Store, List<Row> Rows) Partitioned()
    {
        var store = new MemoryFileStore();
        List<Row> rows = [];
        string[] keys = ["a", "b", "c"];
        for (int f = 0; f < 9; f++)
        {
            string key = keys[f % 3];
            store.Add($"d/k={key}/f{f}.parquet", File(f * 50, 50, 20, narrow: f > 0 && f % 2 == 1, withS: true));
            rows.AddRange(Enumerable.Range(f * 50, 50).Select(i => new Row(i, i * 7 % 1000, key, i % 5 == 0 ? null : $"value number {i}")));
        }
        // Read in path order: by key, then file.
        return (store, [.. rows.OrderBy(r => r.K, StringComparer.Ordinal).ThenBy(r => r.Id)]);
    }

    [Fact]
    public async Task TypedRows_MapColumnsAndPartitions_AndPushThePredicateDown()
    {
        var (store, all) = Partitioned();
        using var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = store });
        foreach (int threads in (int[])[1, 8])
        {
            var options = new ParquetReadOptions { MaxDegreeOfParallelism = threads };
            using (var reader = dataset.OpenReader<Row>(null, options))
                Assert.Equal(all, reader.ReadRows());
            // A conjunct over the partition decides which files are read.
            using (var reader = dataset.OpenReader<Row>(r => r.K == "b" && r.X > 500, options))
            {
                Assert.True(reader.FiltersExactly);
                Assert.Equal(all.Where(r => r.K == "b" && r.X > 500), reader.ReadRows());
                Assert.Equal(3, reader.Inner.FilesRead.Count(read => read));
            }
            string prefix = "value number 1";
            using (var reader = dataset.OpenReader<Row>(r => r.S != null && r.S.StartsWith(prefix) || r.Id < 3, options))
            {
                List<Row> got = [];
                await foreach (Row row in reader.ReadRowsAsync()) got.Add(row);
                Assert.Equal(all.Where(r => r.S != null && r.S.StartsWith(prefix) || r.Id < 3), got);
            }
        }
        Assert.Equal(all.Where(r => r.Id >= 100).Take(7), ParquetDataset.ReadList<Row>("d/", r => r.Id >= 100,
            new ParquetDatasetOptions { Store = store }, new ParquetReadOptions { MaxRows = 7 }));
        Assert.Equal(all.Where(r => r.K == "c"), await ParquetDataset.ReadListAsync<Row>("d/", r => r.K == "c", new ParquetDatasetOptions { Store = store }));
    }

    [Fact]
    public void TypedRows_MapTheFilePath_AndNameAMissingMember()
    {
        var (store, _) = Partitioned();
        using var dataset = ParquetDataset.Open("d/", new ParquetDatasetOptions { Store = store, FilePathColumn = "file" });
        using (var reader = dataset.OpenReader<Located>(r => r.Id == 120))
            Assert.Equal([new Located(120, "d/k=c/f2.parquet")], reader.ReadRows());
        Assert.Contains("'Missing'", Assert.Throws<ArgumentException>(() => dataset.OpenReader<NoSuchColumn>()).Message);
    }

    private sealed record NoSuchColumn(long Id, long Missing);

    [Fact]
    public async Task ReadersOfOneFile_ShareItsMapping_SideBySide()
    {
        var store = new MemoryFileStore();
        store.Add("one.parquet", File(0, 5000, 300, false, true));
        using var file = ParquetFile.Open((await store.OpenAsync("one.parquet"))!);
        List<string> Read(int columns)
        {
            using ParquetReader reader = file.OpenReader(new ParquetReadOptions { MaxDegreeOfParallelism = 4, Columns = columns == 0 ? null : ["s"], ViewTypes = columns == 0 });
            // Each reader maps the columns as its own options say (the mapping is kept per type shape).
            Assert.IsType(columns == 0 ? typeof(StringViewType) : typeof(StringType), reader.Schema.GetFieldByName("s").DataType);
            return Rows(reader.ReadBatches());
        }
        List<string> all = Read(0), strings = Read(1);
        for (int round = 0; round < 10; round++)
        {
            Task<List<string>>[] reads = [.. Enumerable.Range(0, 6).Select(i => Task.Run(() => Read(i % 2)))];
            await Task.WhenAll(reads);
            for (int i = 0; i < reads.Length; i++) Assert.Equal(i % 2 == 0 ? all : strings, reads[i].Result);
        }
    }

    /// <summary>A store counting the files open at once (opened, not yet disposed).</summary>
    private sealed class PeakStore(IFileStore inner) : IFileStore
    {
        private int _open, _peak;

        public int Peak => Volatile.Read(ref _peak);

        public void Reset() => Volatile.Write(ref _peak, Volatile.Read(ref _open));

        public bool CanList => inner.CanList;

        public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, recursive, cancellationToken);

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public async ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) =>
            Track(await inner.OpenAsync(file, cancellationToken));

        public async ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) =>
            await inner.OpenAsync(path, cancellationToken) is { } source ? Track(source) : null;

        private Tracked Track(IRandomAccess source)
        {
            int open = Interlocked.Increment(ref _open);
            int peak;
            while (open > (peak = Volatile.Read(ref _peak)) && Interlocked.CompareExchange(ref _peak, open, peak) != peak) { }
            return new Tracked(source, this);
        }

        private sealed class Tracked(IRandomAccess inner, PeakStore owner) : IRandomAccess, IDisposable
        {
            private int _disposed;

            public long Length => inner.Length;

            public int ReadAt(long offset, Span<byte> destination) => inner.ReadAt(offset, destination);

            public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken) =>
                inner.ReadAtAsync(offset, destination, cancellationToken);

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _disposed, 1) != 0) return;
                Interlocked.Decrement(ref owner._open);
                (inner as IDisposable)?.Dispose();
            }
        }
    }
}
