using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files.Parquet;
using static Seseragi.Files.Tests.Parquet.ParquetPruningTests;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetReadOptions.FilterRows"/> (late materialization) against the IR evaluator: the rows delivered are
/// exactly those the filter keeps (a superset when <see cref="ParquetReader.FiltersExactly"/> is false), with the
/// values the full read gives at those rows, in every way of reading: dense and sparse, streamed, sliced and fetched,
/// on one thread and many, with small batches, row selections, row limits and projections.
/// </summary>
[Collection(RowLevelCorpus.Name)]
public class ParquetFilterRowsTests
{
    private const string RowColumn = "__row";

    private sealed record Config(LateMaterialization Mode, ChunkReading Reading, int Parallelism, int BatchSize, RowRanges? Rows, long? MaxRows,
        string[]? Columns, bool Async)
    {
        public ParquetReadOptions Options(ScalarExpr? filter) => new()
        {
            Filter = filter,
            FilterRows = filter is not null,
            LateMaterialization = Mode,
            Reading = Reading,
            MaxDegreeOfParallelism = Parallelism,
            BatchSize = BatchSize,
            Rows = Rows,
            MaxRows = filter is null ? null : MaxRows,
            Columns = Columns,
            RowIndexColumn = RowColumn,
        };
    }

    private static Config RandomConfig(Random random, long rows, string[] flat)
    {
        RowRanges? selection = null;
        if (random.Next(4) == 0 && rows > 0)
        {
            long start = random.NextInt64(rows);
            selection = RowRanges.Of([(start, random.NextInt64(1, rows - start + 1)), (random.NextInt64(rows), 1)]);
        }
        string[]? columns = random.Next(2) == 0 || flat.Length == 0 ? null : [.. flat.Where(_ => random.Next(2) == 0).DefaultIfEmpty(flat[0])];
        return new Config(
            (LateMaterialization)random.Next(3),
            random.Next(3) switch { 0 => ChunkReading.Stream, 1 => ChunkReading.Slice, _ => ChunkReading.Fetch },
            random.Next(2) == 0 ? 1 : 4,
            random.Next(3) switch { 0 => 65_536, 1 => 7, _ => 100 },
            selection,
            random.Next(5) == 0 ? random.NextInt64(rows + 1) : null,
            columns,
            random.Next(3) == 0);
    }

    /// <summary>The rows a reader delivers: each row's number and rendered values, and what the reader reported.</summary>
    private sealed record Delivery(List<long> Rows, List<string[]> Values, bool Exact, int Dense, int Sparse);

    private static Delivery Read(string path, ParquetReadOptions options, bool async = false)
    {
        using var file = ParquetFile.Open(path);
        using var reader = file.OpenReader(options);
        List<long> rows = [];
        List<string[]> values = [];
        foreach (RecordBatch batch in async ? reader.ReadBatchesAsync().ToBlockingEnumerable() : reader.ReadBatches())
        {
            using (batch)
            {
                if (options.FilterRows) Assert.True(batch.Length > 0, "an empty batch delivered");
                var index = (Int64Array)batch.Column(RowColumn);
                for (int i = 0; i < batch.Length; i++)
                {
                    rows.Add(index.GetValue(i)!.Value);
                    values.Add([.. Enumerable.Range(0, batch.ColumnCount - 1).Select(c => CanonicalRender.Value(batch.Column(c), i))]);
                }
            }
        }
        return new Delivery(rows, values, reader.FiltersExactly, reader.DenseUnits, reader.SparseUnits);
    }

    /// <summary>The columns and struct fields filters may test, with their literals.</summary>
    private static ScalarExpr[] Testable(FileRows rows)
    {
        List<ScalarExpr> columns = [];
        foreach (string name in rows.TopLevel)
        {
            ScalarExpr column = Col(name);
            if (rows.Literals(column, perRowGroup: 3, most: 1).Length > 0) columns.Add(column);
            foreach (string field in StructFields(rows, name)) columns.Add(Field(column, field));
        }
        return [.. columns.Where(c => rows.Literals(c, perRowGroup: 3, most: 1).Length > 0)];

        static IEnumerable<string> StructFields(FileRows rows, string name) =>
            rows.AllRows.Select(r => r.GetValueOrDefault(name)).OfType<IReadOnlyDictionary<string, object?>>().FirstOrDefault()?.Keys ?? [];
    }

    /// <summary>Checks a filtered read against the full read and the evaluator; returns whether it was exact.</summary>
    private static bool Check(FileRows rows, ScalarExpr filter, Config config, string where, ref int dense, ref int sparse)
    {
        Delivery full = Read(rows.Path, config.Options(null));
        Dictionary<long, string[]> byRow = full.Rows.Zip(full.Values).ToDictionary(p => p.First, p => p.Second);
        Delivery all = Read(rows.Path, config.Options(filter) with { MaxRows = null }, config.Async);
        dense += all.Dense;
        sparse += all.Sparse;
        where = $"{where}: {Show(filter)} ({config with { Rows = null }}, rows {config.Rows})";

        // In file order, each row once, from the selection, with the full read's values.
        Assert.True(all.Rows.Zip(all.Rows.Skip(1)).All(p => p.First < p.Second), $"{where}: rows out of order");
        for (int i = 0; i < all.Rows.Count; i++)
        {
            Assert.True(byRow.TryGetValue(all.Rows[i], out string[]? want), $"{where}: row {all.Rows[i]} is not one the full read gives");
            Assert.True(want.SequenceEqual(all.Values[i]), $"{where}: row {all.Rows[i]} differs");
        }

        var (kept, unknown) = rows.Truth(filter);
        HashSet<long> delivered = [.. all.Rows];
        foreach (long row in kept.Where(r => config.Rows?.Contains(r) ?? true))
            Assert.True(delivered.Contains(row), $"{where}: row {row} passes the filter but was not delivered");
        if (all.Exact)
        {
            long[] extra = [.. delivered.Where(r => !kept.Contains(r) && !unknown.Contains(r))];
            Assert.True(extra.Length == 0, $"{where}: rows {string.Join(",", extra.Take(10))} do not pass the filter");
        }

        // A row limit takes the first rows delivered.
        if (config.MaxRows is long most)
        {
            Delivery limited = Read(rows.Path, config.Options(filter), config.Async);
            Assert.Equal(all.Rows.Take((int)Math.Min(most, all.Rows.Count)), limited.Rows);
        }
        return all.Exact;
    }

    public static TheoryData<string> Fixtures =>
        ["types.parquet", "indexed_v1.parquet", "indexed_v2.parquet", "flat_v1_pages.parquet", "flat_v2_pages.parquet", "nested_v2_pages.parquet", "bloom.parquet"];

    [Theory]
    [MemberData(nameof(Fixtures))]
    public void RandomFilters_DeliverTheRowsTheyKeep(string name)
    {
        FileRows rows = FileRows.Read(Fixture(name))!;
        ScalarExpr[] columns = Testable(rows);
        Assert.NotEmpty(columns);
        string[] flat = FlatColumns(Fixture(name));
        long count = rows.AllRows.LongCount();
        var random = new Random(name.Length * 104_729);
        int exact = 0, dense = 0, sparse = 0, rounds = name == "types.parquet" ? 160 : 60;
        for (int round = 0; round < rounds; round++)
        {
            ScalarExpr filter = RandomFilter(random, rows, columns, depth: 2);
            if (Check(rows, filter, RandomConfig(random, count, flat), $"{name} round {round}", ref dense, ref sparse)) exact++;
        }
        Assert.True(exact > rounds * 9 / 10, $"only {exact} of {rounds} filters were evaluated exactly");
        Assert.True(dense > 0 && sparse > 0, $"{dense} row groups read densely, {sparse} sparsely");
    }

    /// <summary>The top-level columns that are neither lists nor structs nor maps.</summary>
    private static string[] FlatColumns(string path)
    {
        using var file = ParquetFile.Open(path);
        using var reader = file.OpenReader(new ParquetReadOptions());
        return [.. reader.Schema.FieldsList.Where(f => f.DataType is not NestedType).Select(f => f.Name)];
    }

    private static readonly Lazy<System.Text.Json.JsonDocument> Values =
        new(() => System.Text.Json.JsonDocument.Parse(File.ReadAllBytes(Path.Combine(ParquetFixtures.Root, "values.json"))));

    public static TheoryData<string> CorpusFiles => [.. Values.Value.RootElement.GetProperty("files").EnumerateObject().Select(p => p.Name)];

    [Theory]
    [MemberData(nameof(CorpusFiles))]
    public void Corpus_FilteredRows_AreTheFullReadsRowsTheFilterKeeps(string name)
    {
        string path = ParquetFixtures.FullPath(name);
        FileRows? rows;
        try
        {
            rows = FileRows.Read(path, maxRows: 20_000);
        }
        catch (Exception)
        {
            return; // refused files are covered by ParquetValueTests
        }
        if (rows is null) return; // too large to evaluate row by row
        ScalarExpr[] columns = Testable(rows);
        if (columns.Length == 0) return;
        string[] flat = FlatColumns(path);
        long count = rows.AllRows.LongCount();
        var random = new Random(StringComparer.Ordinal.GetHashCode(name) & 0x7FFFFFFF);
        int dense = 0, sparse = 0;
        for (int round = 0; round < 4; round++)
            Check(rows, RandomFilter(random, rows, columns, depth: 1), RandomConfig(random, count, flat), $"{name} round {round}", ref dense, ref sparse);
    }

    // ------------------------------------------------------------------
    // Particular cases
    // ------------------------------------------------------------------

    [Fact]
    public void FiltersExactly_TellsWhetherEveryConjunctIsEvaluated()
    {
        using var file = ParquetFile.Open(Fixture("indexed_v1.parquet"));
        ScalarExpr key = Bin(BinaryOp.Lt, Col("key"), Lit(LiteralValue.NewInt64(100)));
        ScalarExpr columns = Bin(BinaryOp.Lt, Col("key"), Col("nn")); // between columns: not evaluated here
        using (var reader = file.OpenReader(new ParquetReadOptions { Filter = key, FilterRows = true })) Assert.True(reader.FiltersExactly);
        using (var reader = file.OpenReader(new ParquetReadOptions { Filter = Bin(BinaryOp.And, key, columns), FilterRows = true }))
        {
            Assert.False(reader.FiltersExactly);
            Assert.Equal(100, reader.ReadBatches().Sum(Rows)); // the conjunct evaluated still filters
        }
        using (var reader = file.OpenReader(new ParquetReadOptions { Filter = columns, FilterRows = true }))
        {
            Assert.False(reader.FiltersExactly);
            Assert.Equal(3000, reader.ReadBatches().Sum(Rows)); // nothing to evaluate: every row
        }
        using (var reader = file.OpenReader(new ParquetReadOptions { Filter = key })) Assert.False(reader.FiltersExactly); // rows not filtered
        using (var reader = file.OpenReader(new ParquetReadOptions { FilterRows = true })) Assert.True(reader.FiltersExactly); // no filter
        using (var reader = file.OpenReader(new ParquetReadOptions { Filter = Lit(LiteralValue.NewBool(false)), FilterRows = true }))
            Assert.Empty(reader.ReadBatches());
        using (var reader = file.OpenReader(new ParquetReadOptions { Filter = Lit(LiteralValue.NewBool(true)), FilterRows = true }))
            Assert.Equal(3000, reader.ReadBatches().Sum(Rows));

        static int Rows(RecordBatch batch)
        {
            using (batch) return batch.Length;
        }
    }

    [Fact]
    public void AFilterOnUnselectedColumns_StillFilters()
    {
        using var file = ParquetFile.Open(Fixture("indexed_v1.parquet"));
        ScalarExpr filter = Bin(BinaryOp.And, Bin(BinaryOp.Eq, Col("cat"), Lit(LiteralValue.NewString("c1"))),
            Bin(BinaryOp.Lt, Field(Col("st"), "a"), Lit(LiteralValue.NewInt64(500))));
        FileRows rows = FileRows.Read(Fixture("indexed_v1.parquet"))!;
        HashSet<long> kept = rows.Truth(filter).True;
        Assert.NotEmpty(kept);
        using var reader = file.OpenReader(new ParquetReadOptions { Filter = filter, FilterRows = true, Columns = ["f"], RowIndexColumn = RowColumn });
        Assert.True(reader.FiltersExactly);
        List<long> delivered = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch)
            {
                Assert.Equal(["f", RowColumn], batch.Schema.FieldsList.Select(f => f.Name));
                var index = (Int64Array)batch.Column(1);
                for (int i = 0; i < batch.Length; i++) delivered.Add(index.GetValue(i)!.Value);
            }
        }
        Assert.Equal(kept.Order(), delivered);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void AFieldOfANullStruct_IsNull_EvenWhenItIsRequired(bool sparse)
    {
        // s is optional, s.a required: where s is null the reader gives s.a a placeholder value, which the filter must
        // read as null (the IR reads a field of a null struct as null).
        ParquetNode[] schema =
        [
            new ParquetLeaf("id", Repetition.Required, PhysicalType.Int64),
            new ParquetStruct("s", Repetition.Optional, [new ParquetLeaf("a", Repetition.Required, PhysicalType.Int64),
                new ParquetLeaf("b", Repetition.Optional, PhysicalType.Int64)]),
        ];
        IReadOnlyDictionary<string, object?> Row(long i) => new Dictionary<string, object?>
        {
            ["id"] = i,
            ["s"] = i % 3 == 0 ? null : new Dictionary<string, object?> { ["a"] = i - 100, ["b"] = i % 5 == 0 ? null : i },
        };
        byte[] bytes = MinimalParquetWriter.Write(schema, [.. Enumerable.Range(0, 200).Select(i => Row(i))]);
        ScalarExpr a = Field(Col("s"), "a");
        (ScalarExpr Filter, Func<long, bool> Keeps)[] cases =
        [
            (ScalarExpr.NewUnary(UnaryOp.IsNull, a), i => i % 3 == 0),
            (Not(Bin(BinaryOp.Lt, a, Lit(LiteralValue.NewInt64(0)))), i => i % 3 != 0 && i >= 100),
            // s.a is 0 at row 100 only; the placeholders of null structs (rows 0, 3, 6, …) are 0 too, and must not match.
            (Bin(BinaryOp.EqMissing, a, Lit(LiteralValue.NewInt64(0))), i => i == 100),
        ];
        foreach (var (filter, keeps) in cases)
        {
            using var file = ParquetFile.Open(new MemoryStream(bytes));
            using var reader = file.OpenReader(new ParquetReadOptions
            {
                Filter = filter, FilterRows = true, Columns = ["id"], BatchSize = 32,
                LateMaterialization = sparse ? LateMaterialization.Sparse : LateMaterialization.Dense,
            });
            Assert.True(reader.FiltersExactly);
            List<long> ids = [];
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch)
                {
                    var id = (Int64Array)batch.Column(0);
                    for (int i = 0; i < batch.Length; i++) ids.Add(id.GetValue(i)!.Value);
                }
            }
            Assert.True(Enumerable.Range(0, 200).Select(i => (long)i).Where(keeps).SequenceEqual(ids), $"{Show(filter)}: {string.Join(",", ids.Take(20))}");
        }
    }

    [Fact]
    public void RowsTheIndexCannotPrune_AreStillReadSparselyByThePageIndex()
    {
        // A filter statistics cannot decide (a three-way coalesce): every row group is read, the filter's column whole.
        // Only row 1234 is kept: the other columns are read for page 4 of group 1 alone (by the offset index).
        byte[] bytes = File.ReadAllBytes(Fixture("indexed_v1.parquet"));
        var source = new RecordingSource(bytes);
        using var file = ParquetFile.Open(source);
        ScalarExpr hit = Bin(BinaryOp.Eq, Col("key"), Lit(LiteralValue.NewInt64(1234)));
        ScalarExpr filter = ScalarExpr.NewCoalesce(Microsoft.FSharp.Collections.ListModule.OfSeq<ScalarExpr>([hit, hit, Lit(LiteralValue.NewBool(false))]));
        using var reader = file.OpenReader(new ParquetReadOptions
        {
            Filter = filter, FilterRows = true, Reading = ChunkReading.Fetch, FetchGap = 0, MaxDegreeOfParallelism = 1,
            LateMaterialization = LateMaterialization.Sparse, Columns = ["key", "cat", "f"],
        });
        Assert.Equal([0, 1, 2], reader.RowGroupsRead);
        Assert.Equal([null, null, null], reader.RowsRead);
        source.Reads.Clear();
        Assert.Equal(1, reader.ReadBatches().Sum(b => { using (b) return b.Length; }));
        List<(long Offset, int Length)> reads = [.. source.Reads];
        file.PageIndexes.LoadAsync([.. Enumerable.Range(0, 3).SelectMany(g => Enumerable.Range(0, 7).Select(l => (g, l)))], [], false, default)
            .AsTask().GetAwaiter().GetResult();
        List<(long Start, long End)> forbidden = [];
        for (int g = 0; g < 3; g++)
        {
            for (int leaf = 1; leaf < 7; leaf++) // key (leaf 0) is read whole
            {
                ChunkPageIndex index = file.PageIndexes.Get(g, leaf, withColumns: false)!;
                for (int page = 0; page < index.PageCount; page++)
                {
                    if (g == 1 && page == 4 && leaf is 1 or 3) continue;
                    forbidden.Add((index.Offsets[page], index.Offsets[page] + index.Sizes[page]));
                }
            }
        }
        foreach (var (offset, length) in reads)
        {
            var touched = forbidden.FirstOrDefault(f => offset < f.End && offset + length > f.Start);
            Assert.True(touched == default, $"read [{offset}, +{length}) touches page [{touched.Start}, {touched.End})");
        }
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void FetchedReads_TakeOnlyWhatEachPhaseNeeds(bool sparse)
    {
        LateMaterialization mode = sparse ? LateMaterialization.Sparse : LateMaterialization.Dense;
        // key = 1234 keeps one row of group 1 (page 24 of key); the other columns are read for that row only when
        // sparse (page 24 of each), for the filter's rows when dense (the pages the page index left).
        byte[] bytes = File.ReadAllBytes(Fixture("indexed_v1.parquet"));
        var source = new RecordingSource(bytes);
        using var file = ParquetFile.Open(source);
        ScalarExpr filter = Bin(BinaryOp.Eq, Col("key"), Lit(LiteralValue.NewInt64(1234)));
        using var reader = file.OpenReader(new ParquetReadOptions
        {
            Filter = filter, FilterRows = true, Reading = ChunkReading.Fetch, FetchGap = 0, MaxDegreeOfParallelism = 1, LateMaterialization = mode,
            Columns = ["key", "cat", "f"],
        });
        source.Reads.Clear();
        int rowsRead = reader.ReadBatches().Sum(b => { using (b) return b.Length; });
        Assert.Equal(1, rowsRead);
        Assert.Equal(mode == LateMaterialization.Dense ? 1 : 0, reader.DenseUnits);
        Assert.Equal(mode == LateMaterialization.Sparse ? 1 : 0, reader.SparseUnits);
        // Data pages other than page 4 of group 1 (rows 1200..1249) of key, cat and f are never read, nor any page of
        // the columns neither selected nor tested (planning reads only footers and indexes).
        List<(long Offset, int Length)> reads = [.. source.Reads];
        file.PageIndexes.LoadAsync([.. Enumerable.Range(0, 3).SelectMany(g => Enumerable.Range(0, 7).Select(l => (g, l)))], [], false, default)
            .AsTask().GetAwaiter().GetResult();
        List<(long Start, long End)> forbidden = [];
        for (int g = 0; g < 3; g++)
        {
            for (int leaf = 0; leaf < 7; leaf++)
            {
                ChunkPageIndex index = file.PageIndexes.Get(g, leaf, withColumns: false)
                                       ?? throw new InvalidOperationException($"no offset index for group {g} leaf {leaf}");
                for (int page = 0; page < index.PageCount; page++)
                {
                    if (g == 1 && page == 4 && leaf is 0 or 1 or 3) continue;
                    forbidden.Add((index.Offsets[page], index.Offsets[page] + index.Sizes[page]));
                }
            }
        }
        Assert.NotEmpty(reads);
        foreach (var (offset, length) in reads)
        {
            var hit = forbidden.FirstOrDefault(f => offset < f.End && offset + length > f.Start);
            Assert.True(hit == default, $"read [{offset}, +{length}) touches page [{hit.Start}, {hit.End})");
        }
    }
}

/// <summary>
/// The classes that evaluate whole corpus files row by row hold several GiB of rows at their peak: they run one after
/// the other, not side by side (the suite runs under a memory cap).
/// </summary>
[CollectionDefinition(Name)]
public sealed class RowLevelCorpus
{
    public const string Name = "Row-level corpus evaluation";
}
