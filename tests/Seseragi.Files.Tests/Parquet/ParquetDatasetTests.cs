using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetDataset"/>: paths, directories and globs expanded as Polars expands them, files read one after
/// another into one schema (the first file's), rows numbered across files, a file path column, limits across files.
/// </summary>
public sealed class ParquetDatasetTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-dataset-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private string At(string relative) => Path.Combine(_root, relative);

    /// <summary>Writes a file of an Int64 column <c>id</c> (from <paramref name="first"/>, <paramref name="rows"/> rows) and a string column <c>name</c>.</summary>
    private string Write(string relative, long first, int rows, int rowGroupRows = 1000, bool requiredId = false)
    {
        string path = At(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var schema = new Apache.Arrow.Schema([new Field("id", Int64Type.Default, !requiredId), new Field("name", StringType.Default, true)], null);
        using var writer = ParquetWriter.Create(path, schema, new ParquetWriteOptions { RowGroupRows = rowGroupRows });
        writer.Write(new RecordBatch(schema, [Ids(first, rows), Names(first, rows)], rows));
        writer.Complete();
        return path;
    }

    private static Int64Array Ids(long first, int rows)
    {
        var builder = new Int64Array.Builder();
        for (int i = 0; i < rows; i++) builder.Append(first + i);
        return builder.Build();
    }

    private static StringArray Names(long first, int rows)
    {
        var builder = new StringArray.Builder();
        for (int i = 0; i < rows; i++) builder.Append($"n{first + i}");
        return builder.Build();
    }

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
        StringArray s => s.GetString(i),
        StringViewArray v => v.GetString(i),
        _ => CanonicalRender.Value(array, i),
    };

    private static List<string> Read(ParquetDataset dataset, ParquetReadOptions? options = null)
    {
        using var reader = dataset.OpenReader(options);
        return Rows(reader.ReadBatches());
    }

    private static List<string> Expected(params (long First, int Rows)[] files) =>
        [.. files.SelectMany(f => Enumerable.Range(0, f.Rows).Select(i => $"{f.First + i}|n{f.First + i}"))];

    private List<string> Relative(ParquetDataset dataset) => [.. dataset.Files.Select(f => Path.GetRelativePath(_root, f.Path).Replace('\\', '/'))];

    [Fact]
    public void ADirectory_IsReadWhole_InPathOrder_HiddenFilesIncluded_EmptyFilesLeftOut()
    {
        Write("t/b.parquet", 100, 3);
        Write("t/a.parquet", 0, 2);
        Write("t/sub/c.parquet", 200, 1);
        Write("t/.hidden.parquet", 300, 1);
        Write("t/_meta.parquet", 400, 1);
        File.WriteAllBytes(At("t/empty.parquet"), []);
        using var dataset = ParquetDataset.Open(At("t"));
        Assert.Equal([".hidden.parquet", "_meta.parquet", "a.parquet", "b.parquet", "sub/c.parquet"], [.. Relative(dataset).Select(p => p["t/".Length..])]);
        Assert.Equal(Expected((300, 1), (400, 1), (0, 2), (100, 3), (200, 1)), Read(dataset));
        // A trailing separator names the directory too.
        using var slash = ParquetDataset.Open(At("t") + "/");
        Assert.Equal(dataset.Files.Count, slash.Files.Count);
    }

    [Fact]
    public void ADirectoryAlone_MustHoldOneExtension()
    {
        Write("t/a.parquet", 0, 1);
        File.WriteAllText(At("t/notes.txt"), "x");
        var error = Assert.Throws<InvalidOperationException>(() => ParquetDataset.Open(At("t")));
        Assert.Contains("different extensions", error.Message);
        // A glob says which files to read.
        using var dataset = ParquetDataset.Open(At("t/*.parquet"));
        Assert.Single(dataset.Files);
    }

    [Fact]
    public void IgnorePrefixes_LeaveFilesOut()
    {
        Write("t/a.parquet", 0, 1);
        Write("t/_tmp.parquet", 10, 1);
        Write("t/.x.parquet", 20, 1);
        var options = new ParquetDatasetOptions { IgnorePrefixes = ["_", "."] };
        using var dataset = ParquetDataset.Open(At("t"), options);
        Assert.Equal(["t/a.parquet"], Relative(dataset));
        using var globbed = ParquetDataset.Open(At("t/*.parquet"), options);
        Assert.Equal(["t/a.parquet"], Relative(globbed));
        // Files named are left out too (as Polars does).
        using var named = ParquetDataset.Open([At("t/a.parquet"), At("t/_tmp.parquet")], options);
        Assert.Equal(["t/a.parquet"], Relative(named));
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open(At("t"), new ParquetDatasetOptions { IgnorePrefixes = [""] }));
    }

    [Fact]
    public void Globs_MatchWithinSegments_AndAcrossThemWithDoubleStars()
    {
        Write("g/y=1/m=1/a.parquet", 0, 1);
        Write("g/y=1/m=2/b.parquet", 10, 1);
        Write("g/y=2/m=1/c.parquet", 20, 1);
        Write("g/y=2/top.parquet", 30, 1);
        Write("g/part-1.parquet", 40, 1);
        Write("g/part-12.parquet", 50, 1);
        Assert.Equal(["g/y=1/m=1/a.parquet", "g/y=1/m=2/b.parquet", "g/y=2/m=1/c.parquet", "g/y=2/top.parquet"],
            Relative(ParquetDataset.Open(At("g/y=*/**/*.parquet"))));
        Assert.Equal(["g/y=2/top.parquet"], Relative(ParquetDataset.Open(At("g/y=*/*.parquet"))));
        Assert.Equal(["g/y=1/m=1/a.parquet", "g/y=2/m=1/c.parquet"], Relative(ParquetDataset.Open(At("g/y=[0-9]/m=1/*"))));
        Assert.Equal(["g/part-1.parquet"], Relative(ParquetDataset.Open(At("g/part-?.parquet"))));
        Assert.Equal(6, ParquetDataset.Open(At("g/**/*.parquet")).Files.Count);
        Assert.Throws<FileNotFoundException>(() => ParquetDataset.Open(At("g/*.csv")));
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open(At("g/a**/x")));
    }

    [Fact]
    public void SeveralInputs_KeepTheirOrder_EachSorted()
    {
        Write("s/b/2.parquet", 20, 1);
        Write("s/b/1.parquet", 10, 1);
        Write("s/a.parquet", 0, 1);
        using var dataset = ParquetDataset.Open([At("s/b"), At("s/a.parquet")]);
        Assert.Equal(["s/b/1.parquet", "s/b/2.parquet", "s/a.parquet"], Relative(dataset));
        Assert.Equal(Expected((10, 1), (20, 1), (0, 1)), Read(dataset));
    }

    [Fact]
    public async Task TheRowIndex_NumbersRowsAcrossFiles_FirstColumn()
    {
        Write("r/a.parquet", 0, 1500, rowGroupRows: 400);
        Write("r/b.parquet", 1500, 700, rowGroupRows: 300);
        Write("r/c.parquet", 2200, 10);
        using var dataset = ParquetDataset.Open(At("r"));
        var options = new ParquetReadOptions { RowIndexColumn = "n", BatchSize = 256 };
        using var reader = dataset.OpenReader(options);
        Assert.Equal(["n", "id", "name"], reader.Schema.FieldsList.Select(f => f.Name));
        List<string> rows = Rows(reader.ReadBatches());
        Assert.Equal(2210, rows.Count);
        Assert.All(rows, r => Assert.Equal(r.Split('|')[0], r.Split('|')[1]));
        Assert.Equal(rows, Rows(await ToListAsync(reader.ReadBatchesAsync())));
        // Rows a filter prunes keep their numbers.
        ScalarExpr filter = ScalarExpr.NewBinary(BinaryOp.GtEq, ScalarExpr.NewColumn("id"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(1600)));
        List<string> filtered = Read(dataset, options with { Filter = filter, FilterRows = true });
        Assert.Equal(610, filtered.Count);
        Assert.All(filtered, r => Assert.Equal(r.Split('|')[0], r.Split('|')[1]));
    }

    [Fact]
    public void MaxRows_CountsAcrossFiles()
    {
        Write("m/a.parquet", 0, 5);
        Write("m/b.parquet", 5, 5);
        Write("m/c.parquet", 10, 5);
        using var dataset = ParquetDataset.Open(At("m"));
        foreach (long max in (long[])[0, 3, 5, 7, 15, 100])
            Assert.Equal(Expected((0, 15)).Take((int)max), Read(dataset, new ParquetReadOptions { MaxRows = max }));
    }

    [Fact]
    public void TheFilePathColumn_HoldsEachRowsFile()
    {
        string a = Write("p/a.parquet", 0, 2);
        string b = Write("p/b.parquet", 2, 30);
        using var dataset = ParquetDataset.Open(At("p"), new ParquetDatasetOptions { FilePathColumn = "file" });
        Assert.Equal(["id", "name", "file"], dataset.Schema.FieldsList.Select(f => f.Name));
        foreach (bool views in (bool[])[true, false])
        {
            using var reader = dataset.OpenReader(new ParquetReadOptions { ViewTypes = views, RowIndexColumn = "n" });
            Assert.Equal(views ? ArrowTypeId.StringView : ArrowTypeId.String, reader.Schema.GetFieldByName("file").DataType.TypeId);
            List<string> rows = Rows(reader.ReadBatches());
            Assert.Equal([.. Enumerable.Repeat(a, 2), .. Enumerable.Repeat(b, 30)], rows.Select(r => r.Split('|')[3]));
        }
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open(At("p"), new ParquetDatasetOptions { FilePathColumn = "id" }));
        using var collide = ParquetDataset.Open(At("p"), new ParquetDatasetOptions { FilePathColumn = "n" });
        Assert.Throws<ArgumentException>(() => collide.OpenReader(new ParquetReadOptions { RowIndexColumn = "n" }));
    }

    /// <summary>Files may order the columns differently, hold more of them, or declare them otherwise nullable.</summary>
    [Fact]
    public void LaterFiles_AreReadInTheFirstFilesSchema()
    {
        Write("o/1.parquet", 0, 3, requiredId: true);
        // Columns in another order, one more, and 'id' nullable.
        var schema = new Apache.Arrow.Schema([new Field("extra", Int32Type.Default, true), new Field("name", StringType.Default, true), new Field("id", Int64Type.Default, true)], null);
        using (var writer = ParquetWriter.Create(At("o/2.parquet"), schema))
        {
            writer.Write(new RecordBatch(schema, [new Int32Array.Builder().AppendRange([7, 7]).Build(), Names(3, 2), Ids(3, 2)], 2));
            writer.Complete();
        }
        using var dataset = ParquetDataset.Open(At("o"));
        Assert.Equal(["id", "name"], dataset.Schema.FieldsList.Select(f => f.Name));
        Assert.All(dataset.Schema.FieldsList, f => Assert.True(f.IsNullable));
        Assert.Equal(Expected((0, 5)), Read(dataset));
        Assert.Equal(["n0", "n1", "n2", "n3", "n4"], Read(dataset, new ParquetReadOptions { Columns = ["name"] }));
    }

    [Fact]
    public void ALaterFileLackingAColumn_OrTypedOtherwise_IsRefusedByName()
    {
        Write("x/1.parquet", 0, 2);
        var narrow = new Apache.Arrow.Schema([new Field("id", Int64Type.Default, true)], null);
        string missing = At("x/2.parquet");
        using (var writer = ParquetWriter.Create(missing, narrow))
        {
            writer.Write(new RecordBatch(narrow, [Ids(2, 1)], 1));
            writer.Complete();
        }
        using (var dataset = ParquetDataset.Open(At("x")))
        {
            var error = Assert.Throws<ParquetFormatException>(() => Read(dataset));
            Assert.Contains(missing, error.Message);
            Assert.Contains("'name'", error.Message);
            Assert.Equal(["0", "1", "2"], Read(dataset, new ParquetReadOptions { Columns = ["id"] }).Select(r => r.Split('|')[0]));
        }
        File.Delete(missing);
        var retyped = new Apache.Arrow.Schema([new Field("id", Int32Type.Default, true), new Field("name", StringType.Default, true)], null);
        string other = At("x/3.parquet");
        using (var writer = ParquetWriter.Create(other, retyped))
        {
            writer.Write(new RecordBatch(retyped, [new Int32Array.Builder().Append(9).Build(), Names(9, 1)], 1));
            writer.Complete();
        }
        using (var dataset = ParquetDataset.Open(At("x")))
        {
            var error = Assert.Throws<ParquetFormatException>(() => Read(dataset));
            Assert.Contains(other, error.Message);
            Assert.Contains("'id'", error.Message);
        }
    }

    [Fact]
    public void ADamagedFile_IsNamed()
    {
        Write("d/1.parquet", 0, 2);
        File.WriteAllBytes(At("d/2.parquet"), [.. "PAR1"u8, .. new byte[40], .. "PAR1"u8]);
        using var dataset = ParquetDataset.Open(At("d"));
        var error = Assert.Throws<ParquetFormatException>(() => Read(dataset));
        Assert.StartsWith(At("d/2.parquet"), error.Message);
    }

    [Fact]
    public void GivenFiles_AreReadWithoutListing_TheirRowsSelected()
    {
        string a = Write("f/a.parquet", 0, 10);
        string b = Write("f/b.parquet", 10, 10);
        using ParquetFile footer = ParquetFile.Open(b);
        using var dataset = ParquetDataset.Open(
        [
            new ParquetDatasetFile(b, new FileInfo(b).Length, RowRanges.Of([(0, 2)]), footer.Metadata),
            new ParquetDatasetFile(a, Rows: RowRanges.Of([(8, 2)])),
        ]);
        Assert.Equal([.. Expected((10, 2)), .. Expected((8, 2))], Read(dataset));
        // Row numbers are the files' own, after the previous files' rows.
        Assert.Equal(["0", "1", "18", "19"], Read(dataset, new ParquetReadOptions { RowIndexColumn = "n" }).Select(r => r.Split('|')[0]));
    }

    [Fact]
    public void OptionsNumberingPerFile_AreRefused()
    {
        Write("o/a.parquet", 0, 1);
        using var dataset = ParquetDataset.Open(At("o"));
        Assert.Throws<ArgumentException>(() => dataset.OpenReader(new ParquetReadOptions { RowGroups = [0] }));
        Assert.Throws<ArgumentException>(() => dataset.OpenReader(new ParquetReadOptions { Rows = RowRanges.Of([(0, 1)]) }));
        Assert.Throws<ArgumentException>(() => dataset.OpenReader(new ParquetReadOptions { Columns = ["nope"] }));
    }

    [Fact]
    public void Urls_NeedAStore_AndNothingFound_IsAnError()
    {
        Assert.Throws<ArgumentException>(() => ParquetDataset.Open("s3://bucket/data/*.parquet"));
        Assert.Throws<FileNotFoundException>(() => ParquetDataset.Open(At("nowhere/*.parquet")));
        Assert.Throws<FileNotFoundException>(() => ParquetDataset.Open(At("nowhere.parquet")));
        Assert.Throws<FileNotFoundException>(() => ParquetDataset.Open(At("nowhere/x.parquet")));
    }

    [Fact]
    public async Task AMemoryStore_IsListedAndGlobbedAsALocalOne()
    {
        string a = Write("mem/a.parquet", 0, 3);
        string b = Write("mem/sub/b.parquet", 3, 2);
        var store = new MemoryFileStore();
        store.Add("data/a.parquet", File.ReadAllBytes(a));
        store.Add("data/sub/b.parquet", File.ReadAllBytes(b));
        store.Add("data/sub/notes.txt", "x"u8.ToArray());
        var options = new ParquetDatasetOptions { Store = store, FilePathColumn = "file" };
        await using ParquetDataset dataset = await ParquetDataset.OpenAsync("data/**/*.parquet", options);
        Assert.Equal(["data/a.parquet", "data/sub/b.parquet"], dataset.Files.Select(f => f.Path));
        using var reader = dataset.OpenReader();
        List<string> rows = Rows(await ToListAsync(reader.ReadBatchesAsync()));
        Assert.Equal([.. Expected((0, 3)).Select(r => r + "|data/a.parquet"), .. Expected((3, 2)).Select(r => r + "|data/sub/b.parquet")], rows);
        // A directory of a store that tells directories is read whole.
        Assert.Throws<InvalidOperationException>(() => ParquetDataset.Open("data", options));
        Assert.Equal(2, ParquetDataset.Open("data/sub/b.parquet", options).Files.Concat(ParquetDataset.Open("data/a.parquet", options).Files).Count());
    }

    [Fact]
    public void ManyFiles_AreReadInOrder()
    {
        for (int i = 0; i < 40; i++) Write($"many/f{i:D3}.parquet", i * 3, 3);
        using var dataset = ParquetDataset.Open(At("many/*.parquet"));
        Assert.Equal(Expected((0, 120)), Read(dataset, new ParquetReadOptions { MaxDegreeOfParallelism = 4 }));
        // Stopping early leaves no file open (they are deleted afterwards by Dispose).
        Assert.Equal(4, Read(dataset, new ParquetReadOptions { MaxRows = 4 }).Count);
    }

    /// <summary>Files opened ahead and not read (the read stopped early) are disposed; a failure ahead is not reported.</summary>
    [Fact]
    public async Task FilesOpenedAhead_AreDisposed_WhenTheReadStopsEarly()
    {
        for (int i = 0; i < 30; i++) Write($"ahead/f{i:D2}.parquet", i * 3, 3);
        var store = new Counting(new LocalFileStore());
        using var dataset = ParquetDataset.Open(At("ahead/*.parquet"), new ParquetDatasetOptions { Store = store });
        Assert.Equal(3, Read(dataset, new ParquetReadOptions { MaxRows = 3 }).Count);
        await store.Settled();
        Assert.True(store.Opened > 2, $"{store.Opened} opened");
        Assert.Equal(store.Opened, store.Disposed);
        using (var reader = dataset.OpenReader(new ParquetReadOptions { MaxRows = 4 }))
        {
            await foreach (RecordBatch batch in reader.ReadBatchesAsync()) batch.Dispose();
        }
        await store.Settled();
        Assert.Equal(store.Opened, store.Disposed);
    }

    /// <summary>A footer read once (or given) is not read again: later reads open the files without reading their tails.</summary>
    [Fact]
    public void Footers_AreReadOnce()
    {
        for (int i = 0; i < 5; i++) Write($"once/f{i}.parquet", i * 100, 100);
        var store = new Counting(new LocalFileStore());
        using var dataset = ParquetDataset.Open(At("once/*.parquet"), new ParquetDatasetOptions { Store = store });
        Assert.Equal(1, store.TailReads); // the first file's, for the schema
        Assert.Equal(500, Read(dataset).Count);
        Assert.Equal(5, store.TailReads);
        Assert.Equal(500, Read(dataset).Count);
        Assert.Equal(5, store.TailReads);
        // A footer given is never read.
        string path = At("once/f3.parquet");
        using ParquetFile file = ParquetFile.Open(path);
        var given = new Counting(new LocalFileStore());
        using var known = ParquetDataset.Open([new ParquetDatasetFile(At("once/f0.parquet")), new ParquetDatasetFile(path, Metadata: file.Metadata)],
            new ParquetDatasetOptions { Store = given });
        Assert.Equal([.. Expected((0, 100)), .. Expected((300, 100))], Read(known));
        Assert.Equal(1, given.TailReads); // the first file's only
    }

    /// <summary>An HTTP(S) URL is never a glob: its '?' starts a query, its brackets are characters.</summary>
    [Fact]
    public void HttpUrls_AreNotGlobs()
    {
        string a = Write("http/a.parquet", 0, 2);
        var store = new MemoryFileStore();
        const string url = "https://host/data/a.parquet?sig=[a]&x=*";
        store.Add(url, File.ReadAllBytes(a));
        using var dataset = ParquetDataset.Open(url, new ParquetDatasetOptions { Store = store });
        Assert.Equal([url], dataset.Files.Select(f => f.Path));
        Assert.Equal(Expected((0, 2)), Read(dataset));
    }

    /// <summary>A store that counts the sources it opens, those disposed, and the reads of a file's last bytes (footers).</summary>
    private sealed class Counting(IFileStore inner) : IFileStore
    {
        private int _opened, _disposed, _tails;

        public int Opened => Volatile.Read(ref _opened);

        public int Disposed => Volatile.Read(ref _disposed);

        public int TailReads => Volatile.Read(ref _tails);

        /// <summary>Waits for the opens still running ahead to finish and be disposed (they continue after the read stops).</summary>
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
                if (offset + destination.Length == Length && destination.Length > 8) Interlocked.Increment(ref owner._tails);
                return inner.ReadAt(offset, destination);
            }

            public ValueTask<int> ReadAtAsync(long offset, Memory<byte> destination, CancellationToken cancellationToken)
            {
                if (offset + destination.Length == Length && destination.Length > 8) Interlocked.Increment(ref owner._tails);
                return inner.ReadAtAsync(offset, destination, cancellationToken);
            }

            public void Dispose()
            {
                if (Interlocked.Exchange(ref _done, 1) == 1) return;
                (inner as IDisposable)?.Dispose();
                Interlocked.Increment(ref owner._disposed);
            }
        }
    }

    private static async Task<List<RecordBatch>> ToListAsync(IAsyncEnumerable<RecordBatch> batches)
    {
        List<RecordBatch> list = [];
        await foreach (RecordBatch batch in batches) list.Add(batch);
        return list;
    }
}
