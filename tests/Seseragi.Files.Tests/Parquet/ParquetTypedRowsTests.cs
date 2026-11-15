using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// <see cref="ParquetReader{T}"/> pushes its predicate into the read: row groups and pages it cannot match are not
/// read (its rows are checked against Polars in Polars.CSharp.Tests, its client evaluation policy in Polars.Config.Tests).
/// </summary>
public class ParquetTypedRowsTests
{
    private static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "parquet-pushdown", name);

    [Fact]
    public void APredicate_SkipsTheRowGroupsAndPagesItCannotMatch()
    {
        using var file = ParquetFile.Open(Fixture("indexed_v1.parquet"));
        using (ParquetReader<Row> reader = file.OpenReader<Row>(r => r.Key < 100))
        {
            Assert.True(reader.FiltersExactly);
            Assert.Equal(Enumerable.Range(0, 100).Select(i => (long)i), reader.ReadRows().Select(r => r.Key));
            Assert.Equal([0], reader.Inner.RowGroupsRead);
            Assert.True(reader.Inner.PagesRead < 20, $"{reader.Inner.PagesRead} pages read");
        }
        long[] keys = [5, 2500, 9999];
        using (ParquetReader<Row> reader = file.OpenReader<Row>(r => keys.Contains(r.Key)))
        {
            Assert.Equal([5L, 2500L], reader.ReadRows().Select(r => r.Key));
            Assert.Equal([0, 2], reader.Inner.RowGroupsRead);
        }
    }

    [Fact]
    public void OnlyTheMembersColumns_AreRead()
    {
        using var file = ParquetFile.Open(Fixture("indexed_v1.parquet"));
        using ParquetReader<Row> reader = file.OpenReader<Row>();
        Assert.Equal(["key", "cat"], reader.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(3000, reader.ReadRows().Count());
    }

    [Fact]
    public void FiltersExactly_SaysWhetherTheOptionsFilterIsEvaluatedToo()
    {
        using var file = ParquetFile.Open(Fixture("indexed_v1.parquet"));
        ScalarExpr betweenColumns = ScalarExpr.NewBinary(BinaryOp.Lt, ScalarExpr.NewColumn("key"), ScalarExpr.NewColumn("nn")); // not evaluated by the reader
        ScalarExpr evaluated = ScalarExpr.NewBinary(BinaryOp.Lt, ScalarExpr.NewColumn("key"), ScalarExpr.NewLiteral(LiteralValue.NewInt64(50)));
        foreach (var (filter, rowsFiltered, exact) in (ValueTuple<ScalarExpr, bool, bool>[])[(betweenColumns, true, false), (betweenColumns, false, false), (evaluated, false, true)])
        {
            var options = new ParquetReadOptions { Filter = filter, FilterRows = rowsFiltered };
            using (ParquetReader<Row> reader = file.OpenReader<Row>(r => r.Key < 100, options)) Assert.Equal(exact, reader.FiltersExactly);
            using (ParquetReader<Row> reader = file.OpenReader<Row>(null, options)) Assert.Equal(exact && rowsFiltered, reader.FiltersExactly);
        }
        // A filter given as a hint only is applied with a predicate: its rows are filtered too.
        using (ParquetReader<Row> reader = file.OpenReader<Row>(r => r.Key < 100, new ParquetReadOptions { Filter = evaluated }))
            Assert.Equal(50, reader.ReadRows().Count());
    }

    [Fact]
    public void NestedMembers_ReadAsLists_AndRecords()
    {
        using var file = ParquetFile.Open(Fixture("indexed_v1.parquet"));
        using ParquetReader<WithNested> reader = file.OpenReader<WithNested>(r => r.Key < 5);
        WithNested[] rows = [.. reader.ReadRows()];
        Assert.Equal([0L, 1, 2, 3, 4], rows.Select(r => r.Key));
        Assert.Equal([[], [0], [0, 1], [0, 1, 2], []], rows.Select(r => r.Li!.ToArray()));
        Assert.Equal([new Inner(0, "s0000"), new Inner(1, "s0001"), new Inner(2, "s0002"), new Inner(3, "s0003"), new Inner(4, "s0004")], rows.Select(r => r.St));
    }

    public sealed record Row(long Key, string? Cat);

    public sealed record WithNested(long Key, List<int>? Li, Inner? St);

    public sealed record Inner(long A, string? S);
}
