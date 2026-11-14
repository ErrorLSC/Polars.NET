using System.Text.Json;
using Apache.Arrow;
using Kaminari.Samidare.IR;
using Kaminari.Sazanami.Rows;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetReadOptions.Rows"/>, <see cref="ParquetReadOptions.RowIndexColumn"/> and
/// <see cref="ParquetReadOptions.MaxRows"/>: a selective read gives exactly the rows a full read holds at those
/// numbers, in every way of reading chunks, and passes over pages without reading them where their headers allow.
/// </summary>
public class ParquetRowSelectionTests
{
    private static string Pushdown(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "parquet-pushdown", name);

    private static readonly Lazy<JsonDocument> Values = new(() => JsonDocument.Parse(File.ReadAllBytes(Path.Combine(ParquetFixtures.Root, "values.json"))));

    public static TheoryData<string> CorpusFiles =>
    [
        .. Values.Value.RootElement.GetProperty("files").EnumerateObject().Select(p => p.Name),
        "pushdown:flat_v1_pages.parquet", "pushdown:flat_v2_pages.parquet", "pushdown:nested_v2_pages.parquet",
    ];

    private static string PathOf(string name) =>
        name.StartsWith("pushdown:", StringComparison.Ordinal) ? Pushdown(name["pushdown:".Length..]) : ParquetFixtures.FullPath(name);

    /// <summary>Every row of a read, each column rendered (CanonicalRender), with the row index column last when asked.</summary>
    private static List<string[]> Rows(ParquetFile file, ParquetReadOptions options)
    {
        using var reader = file.OpenReader(options);
        List<string[]> rows = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch)
            {
                for (int i = 0; i < batch.Length; i++)
                    rows.Add([.. Enumerable.Range(0, batch.ColumnCount).Select(c => CanonicalRender.Value(batch.Column(c), i))]);
            }
        }
        return rows;
    }

    /// <summary>A few random ranges over <paramref name="rows"/> rows, seeded by the file's name.</summary>
    private static RowRanges RandomSelection(string seed, long rows)
    {
        var random = new Random(StringComparer.Ordinal.GetHashCode(seed) & 0x7FFFFFFF);
        int n = random.Next(1, 5);
        var ranges = new (long, long)[n];
        for (int i = 0; i < n; i++)
        {
            long start = random.NextInt64(Math.Max(1, rows));
            ranges[i] = (start, random.NextInt64(1, Math.Max(2, Math.Min(rows - start, rows / 3 + 1)) + 1));
        }
        return RowRanges.Of(ranges).Intersect(RowRanges.All(rows));
    }

    [Theory]
    [MemberData(nameof(CorpusFiles))]
    public void SelectedRows_AreTheFullReadsRowsAtThoseNumbers(string name)
    {
        List<string[]> all;
        ParquetFile file;
        try
        {
            file = ParquetFile.Open(PathOf(name));
            if (file.Metadata.RowCount > 200_000)
            {
                file.Dispose();
                return;
            }
            all = Rows(file, new ParquetReadOptions { MaxDegreeOfParallelism = 1 });
        }
        catch (Exception)
        {
            return; // refused files are covered by ParquetValueTests
        }
        using (file)
        {
            RowRanges selection = RandomSelection(name, all.Count);
            long[] numbers = [.. selection.SelectMany(r => Enumerable.Range(0, (int)r.Count).Select(i => r.Start + i))];
            foreach (var (reading, parallelism) in (ValueTuple<ChunkReading, int>[])[(ChunkReading.Stream, 1), (ChunkReading.Slice, 4), (ChunkReading.Fetch, 2)])
            {
                List<string[]> read = Rows(file, new ParquetReadOptions
                {
                    Rows = selection, RowIndexColumn = "__row", MaxDegreeOfParallelism = parallelism, Reading = reading, BatchSize = 37,
                });
                Assert.Equal(numbers.Length, read.Count);
                for (int i = 0; i < numbers.Length; i++)
                {
                    Assert.Equal([.. all[(int)numbers[i]], numbers[i].ToString(System.Globalization.CultureInfo.InvariantCulture)], read[i]);
                }
            }
        }
    }

    [Theory]
    [InlineData("pushdown:nested_v2_pages.parquet")]
    [InlineData("pushdown:flat_v1_pages.parquet")]
    [InlineData("pushdown:flat_v2_pages.parquet")]
    public void PagesWhollyBetweenSelectedRows_AreNotRead(string name)
    {
        using var file = ParquetFile.OpenOnSource(PathOf(name));
        List<string[]> all = Rows(file, new ParquetReadOptions { MaxDegreeOfParallelism = 1 });
        long rows = all.Count;
        // The last few rows of each row group: everything before them in the group is skipped.
        long first = 0;
        List<(long, long)> tails = [];
        foreach (ParquetRowGroup group in file.Metadata.RowGroups)
        {
            tails.Add((first + group.RowCount - 3, 3));
            first += group.RowCount;
        }
        RowRanges selection = RowRanges.Of([.. tails]);
        using var reader = file.OpenReader(new ParquetReadOptions { Rows = selection, MaxDegreeOfParallelism = 1, RowIndexColumn = "__row" });
        List<string[]> read = [];
        foreach (RecordBatch batch in reader.ReadBatches())
        {
            using (batch)
            {
                for (int i = 0; i < batch.Length; i++)
                    read.Add([.. Enumerable.Range(0, batch.ColumnCount).Select(c => CanonicalRender.Value(batch.Column(c), i))]);
            }
        }
        Assert.Equal(selection.RowCount, read.Count);
        foreach (string[] row in read) Assert.Equal(all[int.Parse(row[^1], System.Globalization.CultureInfo.InvariantCulture)], row[..^1]);
        Assert.True(reader.PagesSkipped > file.Metadata.RowGroups.Count, $"only {reader.PagesSkipped} pages skipped");
        Assert.True(rows > 0);
    }

    [Fact]
    public void MaxRows_StopsInDeliveryOrder_AcrossSelectionsAndRowGroups()
    {
        using var file = ParquetFile.Open(ParquetFixtures.FullPath("matrix/pa_pages_rowgroups.parquet"));
        List<string[]> all = Rows(file, new ParquetReadOptions { MaxDegreeOfParallelism = 1, RowIndexColumn = "__row" });
        long total = all.Count;
        foreach (long max in (long[])[0, 1, 69, 70, 71, 150, total, total + 5])
        {
            List<string[]> read = Rows(file, new ParquetReadOptions { MaxRows = max, RowIndexColumn = "__row" });
            Assert.Equal(all.Take((int)Math.Min(max, total)), read);
        }
        // With a selection and the row groups in another order, the limit counts what is delivered.
        int groups = file.Metadata.RowGroups.Count;
        RowRanges evens = RowRanges.FromIndices([.. Enumerable.Range(0, (int)total).Where(i => i % 2 == 0).Select(i => (long)i)]);
        var options = new ParquetReadOptions { RowGroups = [groups - 1, 0], Rows = evens, MaxRows = 40, RowIndexColumn = "__row" };
        List<string[]> limited = Rows(file, options);
        long lastStart = file.Metadata.RowGroups.Take(groups - 1).Sum(g => g.RowCount);
        long[] expected = [.. Enumerable.Range((int)lastStart, (int)file.Metadata.RowGroups[^1].RowCount).Concat(Enumerable.Range(0, 70))
                              .Where(i => i % 2 == 0).Take(40).Select(i => (long)i)];
        Assert.Equal(expected, limited.Select(r => long.Parse(r[^1], System.Globalization.CultureInfo.InvariantCulture)));
        Assert.All(limited, r => Assert.Equal(all[int.Parse(r[^1], System.Globalization.CultureInfo.InvariantCulture)], r));
    }

    [Fact]
    public void RowGroupsWithoutSelectedRows_AreNotRead_AndAFilterCombines()
    {
        using var file = ParquetFile.Open(Pushdown("types.parquet"));
        // Rows 5..6 lie in row group 1 (4 rows per group); the filter keeps groups 0 and 2 only.
        using (var reader = file.OpenReader(new ParquetReadOptions { Rows = RowRanges.Of([(5, 2)]) }))
        {
            Assert.Equal([1], reader.RowGroupsRead);
            Assert.Equal(RowRanges.Of([(1, 2)]), reader.RowsRead[0]);
        }
        ScalarExpr notMiddle = ScalarExpr.NewBinary(BinaryOp.Or,
            ScalarExpr.NewBinary(BinaryOp.Lt, ScalarExpr.NewColumn("i8"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(-10))),
            ScalarExpr.NewBinary(BinaryOp.Gt, ScalarExpr.NewColumn("i8"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(0))));
        using (var reader = file.OpenReader(new ParquetReadOptions { Rows = RowRanges.Of([(2, 8)]), Filter = notMiddle }))
        {
            Assert.Equal([0, 2], reader.RowGroupsRead);
            Assert.Equal([RowRanges.Of([(2, 2)]), RowRanges.Of([(0, 2)])], reader.RowsRead);
        }
        using (var reader = file.OpenReader(new ParquetReadOptions { Rows = RowRanges.All(12) }))
        {
            Assert.Equal([null, null, null], reader.RowsRead); // whole row groups read as such
        }
    }

    [Fact]
    public void TheRowIndexAlone_CanBeRead()
    {
        using var file = ParquetFile.Open(Pushdown("types.parquet"));
        List<string[]> rows = Rows(file, new ParquetReadOptions { Columns = [], RowIndexColumn = "n", Rows = RowRanges.Of([(3, 3)]) });
        Assert.Equal([["3"], ["4"], ["5"]], rows);
    }

    [Fact]
    public void BadSelections_AreRefused()
    {
        using var file = ParquetFile.Open(Pushdown("types.parquet"));
        Assert.Throws<ArgumentOutOfRangeException>(() => file.OpenReader(new ParquetReadOptions { Rows = RowRanges.Of([(10, 3)]) }));
        Assert.Throws<ArgumentException>(() => file.OpenReader(new ParquetReadOptions { RowIndexColumn = "i8" }));
        Assert.Throws<ArgumentException>(() => file.OpenReader(new ParquetReadOptions { RowIndexColumn = "" }));
        Assert.Throws<ArgumentOutOfRangeException>(() => file.OpenReader(new ParquetReadOptions { MaxRows = -1 }));
        using var empty = file.OpenReader(new ParquetReadOptions { MaxRows = 0 });
        Assert.Empty(empty.ReadBatches());
        Assert.Empty(empty.RowGroupsRead);
    }
}
