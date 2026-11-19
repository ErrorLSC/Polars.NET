using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Ipc;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// <see cref="IpcDataset"/>: paths, directories and globs expanded as for Parquet datasets, files read one after another
/// into one schema (the first file's, united by name, or given), Hive partitions (pruning files), rows numbered across
/// files (files left out counted), a file path column, limits across files, errors naming the file, footers kept on
/// stores, and one file read as <see cref="IpcFile"/> reads it.
/// </summary>
public sealed class IpcDatasetTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-ipc-dataset-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private string At(string relative) => Path.Combine(_root, relative);

    /// <summary>
    /// Writes a file of an Int64 column <c>id</c> (from <paramref name="first"/>, <paramref name="rows"/> rows) and a
    /// string column <c>name</c> (plain or as views), in batches of <paramref name="batchRows"/>.
    /// </summary>
    private string Write(string relative, long first, int rows, int batchRows = 1000, bool views = false, IIpcCompression? codec = null, bool extra = false)
    {
        string path = At(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        List<Field> fields = [new Field("id", Int64Type.Default, true), new Field("name", views ? StringViewType.Default : StringType.Default, true)];
        if (extra) fields.Add(new Field("extra", Int32Type.Default, true));
        var schema = new Schema(fields, null);
        using IpcWriter writer = IpcWriter.Create(path, schema, new IpcWriteOptions { Compression = codec ?? NoCompression.Instance });
        for (int at = 0; at < rows || at == 0 && rows == 0; at += batchRows)
        {
            int n = Math.Min(batchRows, rows - at);
            List<IArrowArray> arrays = [Ids(first + at, n), Names(first + at, n, views)];
            if (extra) arrays.Add(new Int32Array.Builder().AppendRange(Enumerable.Range(0, n).Select(i => (int)(first + at + i) * 2)).Build());
            using var batch = new RecordBatch(schema, arrays, n);
            writer.Write(batch);
            if (rows == 0) break;
        }
        writer.Complete();
        return path;
    }

    private static Int64Array Ids(long first, int rows) => new Int64Array.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => first + i)).Build();

    private static IArrowArray Names(long first, int rows, bool views) => views
        ? new StringViewArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => $"n{first + i}")).Build()
        : new StringArray.Builder().AppendRange(Enumerable.Range(0, rows).Select(i => $"n{first + i}")).Build();

    /// <summary>Every row rendered, columns joined by '|'.</summary>
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

    private static string Render(IArrowArray array, int i) => array switch
    {
        _ when array.IsNull(i) => "null",
        StringArray s => s.GetString(i),
        StringViewArray v => v.GetString(i),
        _ => CanonicalRender.Value(array, i),
    };

    private static List<string> Read(IpcDataset dataset, IpcReadOptions? options = null)
    {
        using IpcDatasetReader reader = dataset.OpenReader(options);
        return Rows(reader.ReadBatches());
    }

    private static List<string> Expected(params (long First, int Rows)[] files) =>
        [.. files.SelectMany(f => Enumerable.Range(0, f.Rows).Select(i => $"{f.First + i}|n{f.First + i}"))];

    private List<string> Relative(IpcDataset dataset) => [.. dataset.Files.Select(f => Path.GetRelativePath(_root, f.Path).Replace('\\', '/'))];

    private static readonly IpcReadOptions[] Ways =
    [
        new() { MaxDegreeOfParallelism = 1 }, new() { MaxDegreeOfParallelism = 4 }, new() { MaxDegreeOfParallelism = 4, MemoryMap = IpcMemoryMap.Off },
    ];

    [Fact]
    public void ADirectory_IsReadWhole_InPathOrder_HiddenFilesIncluded_EmptyFilesLeftOut()
    {
        Write("t/b.arrow", 100, 3);
        Write("t/a.arrow", 0, 2);
        Write("t/sub/c.arrow", 200, 1);
        Write("t/.hidden.arrow", 300, 1);
        Write("t/_meta.arrow", 400, 1);
        File.WriteAllBytes(At("t/empty.arrow"), []);
        using var dataset = IpcDataset.Open(At("t"));
        Assert.Equal([".hidden.arrow", "_meta.arrow", "a.arrow", "b.arrow", "sub/c.arrow"], [.. Relative(dataset).Select(p => p["t/".Length..])]);
        foreach (IpcReadOptions way in Ways) Assert.Equal(Expected((300, 1), (400, 1), (0, 2), (100, 3), (200, 1)), Read(dataset, way));
        using var ignoring = IpcDataset.Open(At("t"), new IpcDatasetOptions { IgnorePrefixes = [".", "_"] });
        Assert.Equal(Expected((0, 2), (100, 3), (200, 1)), Read(ignoring));
    }

    [Fact]
    public async Task GlobsAndSeveralInputs_KeepTheirOrder_ReadSyncOrAsync()
    {
        Write("g/x/f1.arrow", 0, 1500, batchRows: 400, codec: Lz4FrameCompression.Instance);
        Write("g/y/f0.arrow", 5000, 10);
        Write("g/y/f2.arrows.txt", 9000, 1);
        using var dataset = IpcDataset.Open([At("g/y/*.arrow"), At("g/x/*.arrow")]);
        Assert.Equal(["g/y/f0.arrow", "g/x/f1.arrow"], Relative(dataset));
        List<string> expected = Expected((5000, 10), (0, 1500));
        Assert.Equal(expected, Read(dataset));
        using IpcDatasetReader reader = dataset.OpenReader();
        List<RecordBatch> batches = [];
        await foreach (RecordBatch batch in reader.ReadBatchesAsync()) batches.Add(batch);
        Assert.Equal(expected, Rows(batches));
    }

    /// <summary>One corpus file as a dataset: the values <see cref="IpcFile"/> reads (columns made nullable).</summary>
    [Theory]
    [MemberData(nameof(IpcOwnershipTests.Files), MemberType = typeof(IpcOwnershipTests))]
    public void OneFile_ReadsAsTheFileReads(string name)
    {
        if (!IpcValueTests.IsFile(name)) return; // streams are I4b's
        string path = Path.Combine(IpcValueTests.Root, name);
        List<string> expected;
        bool twice;
        using (IpcFile file = IpcFile.Open(path))
        using (IpcReader reader = file.OpenReader())
        {
            expected = Rows(reader.ReadBatches());
            twice = file.Schema.FieldsList.Select(f => f.Name).Distinct().Count() < file.Schema.FieldsList.Count;
        }
        if (twice)
        {
            // Columns sharing a name: a dataset refuses them (as Polars does), naming the file.
            Assert.StartsWith(path, Assert.Throws<IpcFormatException>(() => IpcDataset.Open(path)).Message);
            return;
        }
        using var dataset = IpcDataset.Open(path);
        Assert.All(dataset.Schema.FieldsList, f => Assert.True(f.IsNullable));
        Assert.Equal(expected, Read(dataset));
    }

    [Fact]
    public void HivePartitions_AreColumns_AndAFilterOnThemPrunesFiles()
    {
        Write("h/year=2023/m=1/f.arrow", 0, 3);
        Write("h/year=2023/m=2/f.arrow", 10, 2);
        Write("h/year=2024/m=1/f.arrow", 20, 4);
        using var dataset = IpcDataset.Open(At("h"));
        Assert.Equal(["id", "name", "year", "m"], dataset.Schema.FieldsList.Select(f => f.Name));
        Assert.IsType<Int64Type>(dataset.PartitionSchema!.GetFieldByName("year").DataType);
        Assert.Contains("0|n0|2023|1", Read(dataset));

        ScalarExpr year2023 = ScalarExpr.NewBinary(BinaryOp.Eq, ScalarExpr.NewColumn("year"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(2023)));
        using IpcDatasetReader reader = dataset.OpenReader(new IpcReadOptions { Filter = year2023, FilterRows = true, Columns = ["id", "m"] });
        Assert.Equal([true, true, false], reader.FilesRead);
        Assert.True(reader.FiltersExactly);
        Assert.Equal(["0|1", "1|1", "2|1", "10|2", "11|2"], Rows(reader.ReadBatches()));

        // A conjunct over both a partition and a column: each file's value put in, and the comparison between columns
        // lowers over the dataset's types, so the dataset promises exact rows too.
        ScalarExpr mixed = ScalarExpr.NewBinary(BinaryOp.Gt, ScalarExpr.NewColumn("id"), ScalarExpr.NewColumn("m"));
        using IpcDatasetReader both = dataset.OpenReader(new IpcReadOptions { Filter = mixed, FilterRows = true, Columns = ["id"] });
        Assert.True(both.FiltersExactly);
        Assert.Equal(["2", "10", "11", "20", "21", "22", "23"], Rows(both.ReadBatches()));
        // Arithmetic over a partition value and a column: id > m * 10 (m put in per file, folded).
        ScalarExpr scaled = ScalarExpr.NewBinary(BinaryOp.Gt, ScalarExpr.NewColumn("id"),
            ScalarExpr.NewBinary(BinaryOp.Multiply, ScalarExpr.NewColumn("m"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(10))));
        using IpcDatasetReader times = dataset.OpenReader(new IpcReadOptions { Filter = scaled, FilterRows = true, Columns = ["id"] });
        Assert.True(times.FiltersExactly);
        Assert.Equal(["20", "21", "22", "23"], Rows(times.ReadBatches()));
        // A partition alone in arithmetic decides whole files: m * 2 = 4 reads m=2 only.
        ScalarExpr doubled = ScalarExpr.NewBinary(BinaryOp.Eq,
            ScalarExpr.NewBinary(BinaryOp.Multiply, ScalarExpr.NewColumn("m"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(2))), ScalarExpr.NewLiteral(LiteralValue.NewInt64(4)));
        using IpcDatasetReader pruned = dataset.OpenReader(new IpcReadOptions { Filter = doubled, FilterRows = true, Columns = ["id"] });
        Assert.Equal([false, true, false], pruned.FilesRead);
        Assert.Equal(["10", "11"], Rows(pruned.ReadBatches()));
    }

    /// <summary>Arithmetic over a column a file stores narrower than the dataset is the dataset's (no Int32 wrap).</summary>
    [Fact]
    public void Arithmetic_OverWidenedColumns_IsTheDatasets()
    {
        void One(string relative, IArrowArray values)
        {
            string path = At(relative);
            Directory.CreateDirectory(Path.GetDirectoryName(path)!);
            var schema = new Schema([new Field("a", values.Data.DataType, true)], null);
            using IpcWriter writer = IpcWriter.Create(path, schema);
            using (var batch = new RecordBatch(schema, [values], values.Length)) writer.Write(batch);
            writer.Complete();
        }
        One("w/a.arrow", new Int64Array.Builder().Append(1).Append(-5).Build());
        One("w/b.arrow", new Int32Array.Builder().Append(int.MaxValue).Append(-1).Build());
        using var dataset = IpcDataset.Open(At("w"), new IpcDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName });
        Assert.IsType<Int64Type>(dataset.Schema.GetFieldByName("a").DataType);
        ScalarExpr sum = ScalarExpr.NewBinary(BinaryOp.Gt, ScalarExpr.NewBinary(BinaryOp.Add, ScalarExpr.NewColumn("a"), ScalarExpr.NewColumn("a")),
            ScalarExpr.NewLiteral(LiteralValue.NewInt64(0)));
        foreach (IpcReadOptions way in Ways)
        {
            using IpcDatasetReader reader = dataset.OpenReader(way with { Filter = sum, FilterRows = true });
            Assert.True(reader.FiltersExactly);
            Assert.Equal(["1", "2147483647"], Rows(reader.ReadBatches()));
        }
    }

    /// <summary>Rows are numbered across files before any filter, files left out counted (as Polars's scan_ipc numbers them).</summary>
    [Fact]
    public void TheRowIndex_NumbersRowsAcrossFiles_FilesLeftOutCounted()
    {
        Write("r/p=1/f.arrow", 10, 3);
        Write("r/p=2/f.arrow", 20, 3, batchRows: 2);
        Write("r/p=3/f.arrow", 30, 3);
        using var dataset = IpcDataset.Open(At("r"));
        foreach (IpcReadOptions way in Ways)
        {
            Assert.Equal(["0|10", "1|11", "2|12", "3|20", "4|21", "5|22", "6|30", "7|31", "8|32"],
                Read(dataset, way with { RowIndexColumn = "idx", Columns = ["id"] }));
            ScalarExpr p2 = ScalarExpr.NewBinary(BinaryOp.Eq, ScalarExpr.NewColumn("p"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(2)));
            Assert.Equal(["3|20", "4|21", "5|22"], Read(dataset, way with { RowIndexColumn = "idx", Columns = ["id"], Filter = p2, FilterRows = true }));
            ScalarExpr id21 = ScalarExpr.NewBinary(BinaryOp.Eq, ScalarExpr.NewColumn("id"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(21)));
            Assert.Equal(["4|21"], Read(dataset, way with { RowIndexColumn = "idx", Columns = ["id"], Filter = id21, FilterRows = true }));
            Assert.Equal(["0|10", "1|11", "2|12", "3|20"], Read(dataset, way with { RowIndexColumn = "idx", Columns = ["id"], MaxRows = 4 }));
        }
    }

    /// <summary>Without a row index, files left out by their partitions are never opened (a damaged one does not fail the read).</summary>
    [Fact]
    public void FilesLeftOut_AreNotOpened_UnlessRowsAreNumbered()
    {
        Write("x/p=1/f.arrow", 10, 3);
        Write("x/p=2/f.arrow", 20, 3);
        using var dataset = IpcDataset.Open(At("x"));
        File.WriteAllBytes(At("x/p=1/f.arrow"), [.. Enumerable.Repeat((byte)7, 64)]);
        ScalarExpr p2 = ScalarExpr.NewBinary(BinaryOp.Eq, ScalarExpr.NewColumn("p"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(2)));
        Assert.Equal(["20", "21", "22"], Read(dataset, new IpcReadOptions { Columns = ["id"], Filter = p2, FilterRows = true }));
        var error = Assert.Throws<IpcFormatException>(() => Read(dataset, new IpcReadOptions { Columns = ["id"], Filter = p2, FilterRows = true, RowIndexColumn = "i" }));
        Assert.StartsWith(At("x/p=1/f.arrow"), error.Message);
    }

    [Fact]
    public void MaxRows_AndTheFilePath_SpanFiles()
    {
        Write("m/a.arrow", 0, 3);
        Write("m/b.arrow", 100, 3);
        using var dataset = IpcDataset.Open(At("m"), new IpcDatasetOptions { FilePathColumn = "file" });
        Assert.Equal("file", dataset.Schema.FieldsList[^1].Name);
        foreach (IpcReadOptions way in Ways)
        {
            List<string> rows = Read(dataset, way with { MaxRows = 4 });
            Assert.Equal([$"0|n0|{At("m/a.arrow")}", $"1|n1|{At("m/a.arrow")}", $"2|n2|{At("m/a.arrow")}", $"100|n100|{At("m/b.arrow")}"], rows);
        }
    }

    [Fact]
    public void Schemas_AreTheFirstFiles_UnitedByName_OrGiven()
    {
        Write("s/a.arrow", 0, 2);
        Write("s/b.arrow", 10, 2, extra: true);
        Write("s/c.arrow", 20, 2, views: true);

        // The first file's: 'extra' not read; views read as plain strings (the first file's layout).
        using var first = IpcDataset.Open(At("s"));
        Assert.Equal(["id", "name"], first.Schema.FieldsList.Select(f => f.Name));
        Assert.IsType<StringType>(first.Schema.GetFieldByName("name").DataType);
        Assert.Equal(Expected((0, 2), (10, 2), (20, 2)), Read(first));

        // United: 'extra' read, nulls where a file lacks it; strings in the first layout met.
        using var united = IpcDataset.Open(At("s"), new IpcDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName });
        Assert.Equal(["id", "name", "extra"], united.Schema.FieldsList.Select(f => f.Name));
        Assert.IsType<StringType>(united.Schema.GetFieldByName("name").DataType);
        Assert.Equal(["0|n0|null", "1|n1|null", "10|n10|20", "11|n11|22", "20|n20|null", "21|n21|null"], Read(united));
        using var viewsFirst = IpcDataset.Open([At("s/c.arrow"), At("s/a.arrow")], new IpcDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName });
        Assert.IsType<StringViewType>(viewsFirst.Schema.GetFieldByName("name").DataType);
        Assert.Equal(Expected((20, 2), (0, 2)), Read(viewsFirst));

        // Given: columns and types as said (a missing one an error naming the file, or nulls when allowed).
        var given = new Schema([new Field("name", StringViewType.Default, true), new Field("extra", Int64Type.Default, true)], null);
        var error = Assert.Throws<IpcFormatException>(() => Read(IpcDataset.Open(At("s"), new IpcDatasetOptions { Schema = given })));
        Assert.Contains(At("s/a.arrow"), error.Message);
        Assert.Contains("AllowMissingColumns", error.Message);
        using var allowed = IpcDataset.Open(At("s"), new IpcDatasetOptions { Schema = given, AllowMissingColumns = true });
        Assert.Equal(["n0|null", "n1|null", "n10|20", "n11|22", "n20|null", "n21|null"], Read(allowed));
    }

    [Fact]
    public void AWiderTypeInALaterFile_IsRefusedByName_UnlessUnited()
    {
        // The first file's 'id' 32-bit; a later one's 64-bit.
        string first = At("w/a.arrow");
        Directory.CreateDirectory(Path.GetDirectoryName(first)!);
        var narrow = new Schema([new Field("id", Int32Type.Default, true), new Field("name", StringType.Default, true)], null);
        using (IpcWriter writer = IpcWriter.Create(first, narrow))
        {
            using var batch = new RecordBatch(narrow, [new Int32Array.Builder().Append(1).Append(2).Build(), Names(1, 2, false)], 2);
            writer.Write(batch);
        }
        string later = Write("w/b.arrow", 5_000_000_000, 1);
        var error = Assert.Throws<IpcFormatException>(() => Read(IpcDataset.Open(At("w"))));
        Assert.StartsWith(later, error.Message);
        Assert.Contains("UnionByName", error.Message);
        using var united = IpcDataset.Open(At("w"), new IpcDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName });
        Assert.IsType<Int64Type>(united.Schema.GetFieldByName("id").DataType);
        Assert.Equal(["1|n1", "2|n2", "5000000000|n5000000000"], Read(united));
    }

    [Fact]
    public void ADamagedFile_IsNamed()
    {
        Write("d/a.arrow", 0, 2);
        File.WriteAllBytes(At("d/b.arrow"), [.. "ARROW1\0\0"u8, .. Enumerable.Repeat((byte)1, 40), .. "ARROW1"u8]);
        var error = Assert.Throws<IpcFormatException>(() => Read(IpcDataset.Open(At("d"))));
        Assert.StartsWith(At("d/b.arrow"), error.Message);
    }

    [Fact]
    public void AMemoryStore_IsListedAndGlobbed()
    {
        var memory = new MemoryFileStore();
        memory.Add("m/p=1/a.arrow", File.ReadAllBytes(Write("mem/a.arrow", 0, 2)));
        memory.Add("m/p=2/b.arrow", File.ReadAllBytes(Write("mem/b.arrow", 10, 2)));
        using var dataset = IpcDataset.Open("m/", new IpcDatasetOptions { Store = memory });
        Assert.Equal(["0|n0|1", "1|n1|1", "10|n10|2", "11|n11|2"], Read(dataset));
        using var globbed = IpcDataset.Open("m/**/b.arrow", new IpcDatasetOptions { Store = memory });
        Assert.Equal(Expected((10, 2)), Read(globbed));
    }

    /// <summary>On a store, a file opens with one read of its tail (the footer in it), and a large file's footer is kept between reads.</summary>
    [Fact]
    public void OnAStore_FilesOpenWithOneTailRead_AndLargeFilesFootersAreKept()
    {
        for (int i = 0; i < 3; i++) Write($"big/f{i}.arrow", i * 100_000L, 100_000, batchRows: 20_000); // over 64 KiB each
        var store = new Counting(new LocalFileStore());
        using var dataset = IpcDataset.Open(At("big/*.arrow"), new IpcDatasetOptions { Store = store });
        Assert.Equal(1, store.TailReads); // the first file's, for the schema
        Assert.Equal(300_000, Read(dataset, new IpcReadOptions { Columns = ["id"] }).Count);
        Assert.Equal(3, store.TailReads); // the first file's footer kept from the opening
        Assert.Equal(3, dataset.Footers.Count);
        Assert.Equal(300_000, Read(dataset, new IpcReadOptions { Columns = ["id"] }).Count);
        Assert.Equal(3, store.TailReads);

        // A footer larger than the tail read: read apart, still kept.
        string wide = At("wide/f.arrow");
        Directory.CreateDirectory(Path.GetDirectoryName(wide)!);
        var schema = new Schema([.. Enumerable.Range(0, 2000).Select(c => new Field($"column number {c}", Int32Type.Default, true))], null);
        using (IpcWriter writer = IpcWriter.Create(wide, schema))
        {
            using var batch = new RecordBatch(schema, [.. Enumerable.Range(0, 2000).Select(c => (IArrowArray)new Int32Array.Builder().Append(c).Build())], 1);
            writer.Write(batch);
        }
        var wideStore = new Counting(new LocalFileStore());
        using var wideSet = IpcDataset.Open(wide, new IpcDatasetOptions { Store = wideStore });
        Assert.Equal(2000, wideSet.Schema.FieldsList.Count);
        Assert.Equal(["0|1|2"], Read(wideSet, new IpcReadOptions { Columns = ["column number 0", "column number 1", "column number 2"] }));
        Assert.Equal(1, wideSet.Footers.Count);
    }

    [Fact]
    public void LocalFilesAndMemory_KeepNoFooter()
    {
        for (int i = 0; i < 2; i++) Write($"loc/f{i}.arrow", i * 100_000L, 100_000);
        using var local = IpcDataset.Open(At("loc"));
        Assert.Equal(200_000, Read(local, new IpcReadOptions { Columns = ["id"] }).Count);
        Assert.Equal(0, local.Footers.Count);
    }

    /// <summary>A store that counts the reads ending at a file's end (tail reads) and the sources opened and disposed.</summary>
    private sealed class Counting(IFileStore inner) : IFileStore
    {
        private int _tails;

        public int TailReads => Volatile.Read(ref _tails);

        public bool CanList => inner.CanList;

        public IAsyncEnumerable<StoredFile> ListAsync(string prefix, bool recursive, CancellationToken cancellationToken = default) =>
            inner.ListAsync(prefix, recursive, cancellationToken);

        public ValueTask<StoredFile?> StatAsync(string path, CancellationToken cancellationToken = default) => inner.StatAsync(path, cancellationToken);

        public async ValueTask<IRandomAccess> OpenAsync(StoredFile file, CancellationToken cancellationToken = default) =>
            new Source(this, await inner.OpenAsync(file, cancellationToken));

        public async ValueTask<IRandomAccess?> OpenAsync(string path, CancellationToken cancellationToken = default) =>
            await inner.OpenAsync(path, cancellationToken) is { } source ? new Source(this, source) : null;

        private sealed class Source(Counting owner, IRandomAccess inner) : IRandomAccess, IDisposable
        {
            public long Length => inner.Length;

            public int ReadAt(long offset, Span<byte> destination)
            {
                if (offset + destination.Length == Length && destination.Length > 10) Interlocked.Increment(ref owner._tails);
                return inner.ReadAt(offset, destination);
            }

            public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
            {
                if (offset + destination.Length == Length && destination.Length > 10) Interlocked.Increment(ref owner._tails);
                return inner.ReadAtAsync(offset, destination, cancellationToken);
            }

            public void Dispose() => (inner as IDisposable)?.Dispose();
        }
    }
}
