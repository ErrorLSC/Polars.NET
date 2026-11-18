using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.Ipc;
using Seseragi.Files.Tests.Parquet;
using static Seseragi.Files.Tests.Parquet.ParquetPruningTests;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// <see cref="IpcReadOptions.FilterRows"/> against the IR evaluator, over every corpus file: the rows delivered are
/// exactly those random filters keep (a superset when <see cref="IpcReader.FiltersExactly"/> is false), with the values
/// the full read gives at those rows, every column type filtered, on one thread and many, with row limits.
/// </summary>
public class IpcFilterTests
{
    private static string Path(string name) => System.IO.Path.Combine(IpcValueTests.Root, name);

    /// <summary>The batches of a corpus file, as a file or a stream.</summary>
    private static IEnumerable<RecordBatch> Batches(string name, IpcReadOptions options)
    {
        if (IpcValueTests.IsFile(name))
        {
            using IpcFile file = IpcFile.Open(Path(name));
            using IpcReader reader = file.OpenReader(options);
            foreach (RecordBatch batch in reader.ReadBatches()) yield return batch;
            yield break;
        }
        using IpcStreamReader stream = IpcStreamReader.Open(File.OpenRead(Path(name)), options);
        foreach (RecordBatch batch in stream.ReadBatches()) yield return batch;
    }

    /// <summary>Each row's canonical values, joined.</summary>
    private static List<string> Rows(IEnumerable<RecordBatch> batches)
    {
        List<string> rows = [];
        foreach (RecordBatch batch in batches)
        {
            using (batch)
            {
                for (int i = 0; i < batch.Length; i++)
                    rows.Add(string.Join(" | ", Enumerable.Range(0, batch.ColumnCount).Select(c => CanonicalRender.Value(batch.Column(c), i))));
            }
        }
        return rows;
    }

    /// <summary>A value as the evaluator sees it; <see cref="IrEvaluator.Missing"/> where the CLR types cannot hold it (gold files' extremes).</summary>
    private static object? Value(IArrowArray array, int i)
    {
        try
        {
            return ParquetPruningTests.Value(array, i);
        }
        catch (Exception e) when (e is OverflowException or ArgumentOutOfRangeException or NotSupportedException)
        {
            return IrEvaluator.Missing.Instance;
        }
    }

    private static bool Filterable(IArrowType type) => type switch
    {
        ListViewType or LargeListViewType or UnionType or RunEndEncodedType => false,
        DictionaryType d => Filterable(d.ValueType),
        NestedType n => n.Fields.All(f => Filterable(f.DataType)),
        _ => true,
    };

    public static TheoryData<string> Readable => IpcParallelTests.Readable;

    [Theory]
    [MemberData(nameof(Readable))]
    public void RandomFilters_KeepTheRowsTheEvaluatorKeeps(string name)
    {
        Schema schema = IpcValueTests.Read(name, new IpcReadOptions { MaxRows = 0 }).Schema;
        string[] names = [.. schema.FieldsList.Select(f => f.Name)];
        if (names.Distinct().Count() != names.Length || names.Length == 0) return; // columns selected by name
        string[] columns = [.. schema.FieldsList.Where(f => Filterable(f.DataType)).Select(f => f.Name)];
        if (columns.Length == 0) return;

        // The rows as the evaluator sees them, batch by batch, and as rendered.
        var options = new IpcReadOptions { Columns = columns, MaxDegreeOfParallelism = 1 };
        List<Dictionary<string, object?>>[] groups = [.. Batches(name, options).Select(batch =>
        {
            using (batch)
            {
                return Enumerable.Range(0, batch.Length).Select(i => Enumerable.Range(0, batch.ColumnCount)
                    .ToDictionary(c => batch.Schema.GetFieldByIndex(c).Name, c => Value(batch.Column(c), i))).ToList();
            }
        })];
        var rows = FileRows.Of(Path(name), groups, schema.FieldsList.Where(f => columns.Contains(f.Name)).ToDictionary(f => f.Name, f => f.DataType));
        List<string> all = Rows(Batches(name, options));
        ScalarExpr[] leaves = [.. columns.Select(Col).Where(c => rows.Literals(c, most: 64).Length > 0)];
        if (leaves.Length == 0 || all.Count == 0) return;

        var random = new Random(name.Aggregate(17, (h, c) => h * 31 + c));
        int compared = 0;
        for (int n = 0; n < 24; n++)
        {
            ScalarExpr filter = RandomFilter(random, rows, leaves, depth: 2);
            var (truth, unknown) = rows.Truth(filter);
            if (unknown.Count > 0) continue;
            long? limit = random.Next(4) == 0 ? random.Next(truth.Count + 2) : null;
            var read = options with
            {
                Filter = filter,
                FilterRows = true,
                MaxRows = limit,
                MaxDegreeOfParallelism = random.Next(2) == 0 ? 1 : 4,
                ParallelDecompressFrom = 0,
            };
            bool exact;
            using (IpcFile? file = IpcValueTests.IsFile(name) ? IpcFile.Open(Path(name)) : null)
            {
                if (file is null)
                {
                    using IpcStreamReader stream = IpcStreamReader.Open(File.OpenRead(Path(name)), read);
                    exact = stream.FiltersExactly;
                }
                else
                {
                    using IpcReader reader = file.OpenReader(read);
                    exact = reader.FiltersExactly;
                }
            }
            List<string> delivered = Rows(Batches(name, read));
            List<string> expected = [.. truth.Order().Select(i => all[(int)i])];
            if (exact)
            {
                Assert.Equal(limit is { } l ? expected.Take((int)l) : expected, delivered);
                compared++;
            }
            else if (limit is null)
            {
                // A superset, in order: every row kept is delivered.
                int at = 0;
                foreach (string row in delivered)
                {
                    if (at < expected.Count && row == expected[at]) at++;
                }
                Assert.Equal(expected.Count, at);
            }
        }
        Assert.True(compared > 0, "no filter was compared exactly");
    }

    [Fact]
    public void WithoutFilterRows_TheFilterDoesNothing()
    {
        ScalarExpr none = Bin(BinaryOp.Lt, Col("i64"), Lit(LiteralValue.NewInt64(long.MinValue)));
        using IpcFile file = IpcFile.Open(Path("pyarrow/primitives.arrow"));
        using IpcReader hint = file.OpenReader(new IpcReadOptions { Filter = none });
        Assert.False(hint.FiltersExactly);
        Assert.Equal(file.OpenReader().ReadBatches().Sum(b => b.Length), hint.ReadBatches().Sum(b => b.Length));
        using IpcReader filtered = file.OpenReader(new IpcReadOptions { Filter = none, FilterRows = true });
        Assert.True(filtered.FiltersExactly);
        Assert.Empty(filtered.ReadBatches()); // batches keeping no row are passed over
        using RecordBatch empty = filtered.ReadBatch(0); // read on their own, they are empty
        Assert.Equal(0, empty.Length);
        Assert.Equal(file.Schema.FieldsList.Count, empty.ColumnCount);
    }

    /// <summary>A filter's column is read though not selected: from ranges (no mapping), from the mapping, from a stream that cannot seek.</summary>
    [Theory]
    [InlineData(IpcMemoryMap.Off, false)]
    [InlineData(IpcMemoryMap.On, false)]
    [InlineData(IpcMemoryMap.Off, true)]
    public void AFilterOnAColumnNotSelected_ReadsItButDoesNotReturnIt(IpcMemoryMap map, bool stream)
    {
        // many_batches rewritten with a gap of more than 1 MiB between the filter's column and the one selected.
        string path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), $"ipc-filter-{Guid.NewGuid():N}.arrow{(stream ? "s" : "")}");
        try
        {
            var schema = new Schema([new Field("k", Int64Type.Default, true), new Field("pad", BinaryType.Default, false), new Field("v", Int32Type.Default, false)], null);
            const int Rows = 4000;
            var k = new Int64Array.Builder();
            var pad = new BinaryArray.Builder();
            var v = new Int32Array.Builder();
            for (int i = 0; i < Rows; i++)
            {
                if (i % 3 == 0) k.AppendNull(); else k.Append(i);
                pad.Append(new byte[400]);
                v.Append(i);
            }
            using (RecordBatch batch = new(schema, [k.Build(), pad.Build(), v.Build()], Rows))
                IpcWriter.Write(path, schema, [batch], new IpcWriteOptions { Format = stream ? IpcFormat.Stream : IpcFormat.File });
            var options = new IpcReadOptions { Columns = ["v"], Filter = ScalarExpr.NewUnary(UnaryOp.IsNotNull, Col("k")), FilterRows = true, MemoryMap = map };
            List<int> read = [];
            IEnumerable<RecordBatch> batches;
            IDisposable owner;
            if (stream)
            {
                var reader = IpcStreamReader.Open(new IpcValueTests.NonSeekable(File.OpenRead(path)), options);
                Assert.Equal(["v"], reader.Schema.FieldsList.Select(f => f.Name));
                (batches, owner) = (reader.ReadBatches(), reader);
            }
            else
            {
                var file = IpcFile.Open(path);
                var reader = file.OpenReader(options);
                Assert.Equal(["v"], reader.Schema.FieldsList.Select(f => f.Name));
                (batches, owner) = (reader.ReadBatches(), new Both(reader, file));
            }
            using (owner)
            {
                foreach (RecordBatch batch in batches)
                {
                    using (batch) read.AddRange(((Int32Array)batch.Column(0)).Values.ToArray());
                }
            }
            Assert.Equal(Enumerable.Range(0, Rows).Where(i => i % 3 != 0), read);
        }
        finally
        {
            File.Delete(path);
        }
    }

    private sealed class Both(IDisposable a, IDisposable b) : IDisposable
    {
        public void Dispose()
        {
            a.Dispose();
            b.Dispose();
        }
    }

    /// <summary>Batches where the filter keeps nothing are passed over by the stream reader too, on one thread and many.</summary>
    [Theory]
    [InlineData(1)]
    [InlineData(4)]
    public void AStreamReader_PassesOverBatchesKeepingNoRow(int parallelism)
    {
        ScalarExpr none = Bin(BinaryOp.Lt, Col("i64"), Lit(LiteralValue.NewInt64(long.MinValue)));
        using IpcStreamReader reader = IpcStreamReader.Open(new MemoryStream(IpcParallelTests.ManyBatchesStream()),
            new IpcReadOptions { Filter = none, FilterRows = true, MaxDegreeOfParallelism = parallelism });
        Assert.Null(reader.ReadNext());
    }

    [Fact]
    public void ColumnsThatCannotBeFiltered_AreRefusedOnOpen()
    {
        using IpcFile file = IpcFile.Open(Path("pyarrow/ree.arrow"));
        string ree = file.Schema.FieldsList.First(f => f.DataType is RunEndEncodedType).Name;
        ScalarExpr always = Lit(LiteralValue.NewBool(true));
        var e = Assert.Throws<NotSupportedException>(() => file.OpenReader(new IpcReadOptions { Columns = [ree], Filter = always, FilterRows = true }));
        Assert.Contains(ree, e.Message);
        using IpcReader hint = file.OpenReader(new IpcReadOptions { Columns = [ree], Filter = always }); // without FilterRows: fine
    }

    /// <summary>Filtered batches hold memory of their own: they outlive the reader and the file, mapped or not (strings, dictionaries).</summary>
    [Theory]
    [InlineData(IpcMemoryMap.On, "pyarrow/strings.arrow", "utf8")]
    [InlineData(IpcMemoryMap.Off, "pyarrow/strings.arrow", "utf8")]
    [InlineData(IpcMemoryMap.On, "pyarrow/dictionary.arrow", "dict_int8")]
    [InlineData(IpcMemoryMap.Off, "pyarrow/dictionary.arrow", "dict_int8")]
    public void FilteredBatches_OutliveTheirFile(IpcMemoryMap map, string name, string column)
    {
        ScalarExpr notNull = ScalarExpr.NewUnary(UnaryOp.IsNotNull, Col(column));
        var options = new IpcReadOptions { Filter = notNull, FilterRows = true, MemoryMap = map };
        var (_, expected) = IpcValueTests.Read(name, options);
        var (_, all) = IpcValueTests.Read(name, new IpcReadOptions());
        Assert.True(expected.Sum(b => b[0].Count) < all.Sum(b => b[0].Count), "the filter keeps every row");
        List<RecordBatch> batches;
        using (IpcFile file = IpcFile.Open(Path(name)))
        using (IpcReader reader = file.OpenReader(options))
        {
            batches = [.. reader.ReadBatches()];
        }
        GC.Collect();
        Assert.Equal(expected.SelectMany(b => b.SelectMany(c => c)),
            batches.SelectMany(b => Enumerable.Range(0, b.ColumnCount).SelectMany(c => Enumerable.Range(0, b.Length).Select(i => CanonicalRender.Value(b.Column(c), i)))));
        foreach (RecordBatch batch in batches) batch.Dispose();
    }
}
