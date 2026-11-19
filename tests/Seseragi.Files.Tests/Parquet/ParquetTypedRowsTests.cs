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

    /// <summary>
    /// Arithmetic in a predicate is evaluated by the reader (exactly: no client evaluation, which the default Strict
    /// policy would refuse).
    /// </summary>
    [Fact]
    public void ArithmeticPredicates_ArePushedExactly()
    {
        using var file = ParquetFile.Open(Fixture("indexed_v1.parquet"));
        using (ParquetReader<Row> reader = file.OpenReader<Row>(r => r.Key * 2 < 200))
        {
            Assert.True(reader.FiltersExactly);
            Assert.Equal(Enumerable.Range(0, 100).Select(i => (long)i), reader.ReadRows().Select(r => r.Key));
        }
        using (ParquetReader<Row> reader = file.OpenReader<Row>(r => r.Key % 1000 == 0 && -r.Key > -2500))
        {
            Assert.True(reader.FiltersExactly);
            Assert.Equal([0L, 1000L, 2000L], reader.ReadRows().Select(r => r.Key));
        }
    }

    /// <summary>
    /// Arithmetic over a member is computed in the member's type, not the column's: a long member over an Int32 column
    /// does not wrap at int.MaxValue (as LINQ to Objects on the rows).
    /// </summary>
    [Fact]
    public void Arithmetic_IsInTheMembersType()
    {
        string path = Path.Combine(Path.GetTempPath(), $"typed-wide-{Guid.NewGuid():N}.parquet");
        try
        {
            var schema = new Apache.Arrow.Schema([new Apache.Arrow.Field("A", Apache.Arrow.Types.Int32Type.Default, false)], null);
            using (ParquetWriter writer = ParquetWriter.Create(path, schema))
            {
                using var batch = new Apache.Arrow.RecordBatch(schema, [new Apache.Arrow.Int32Array.Builder().Append(int.MaxValue).Append(-1).Append(3).Build()], 3);
                writer.Write(batch);
            }
            System.Linq.Expressions.Expression<Func<Wide, bool>> where = r => r.A + r.A > 0;
            List<Wide> all = ParquetFile.ReadList<Wide>(path);
            using var file = ParquetFile.Open(path);
            using ParquetReader<Wide> reader = file.OpenReader(where);
            Assert.True(reader.FiltersExactly);
            Assert.Equal(all.Where(where.Compile()), reader.ReadRows());
            Assert.Equal([int.MaxValue, 3L], reader.ReadRows().Select(r => r.A));
            // A nullable member: computed in long too.
            using (ParquetReader<WideNullable> nullable = file.OpenReader<WideNullable>(r => r.A + r.A > 0))
            {
                Assert.True(nullable.FiltersExactly);
                Assert.Equal([int.MaxValue, 3L], nullable.ReadRows().Select(r => r.A!.Value));
            }
            // A decimal member: decimal arithmetic is not the reader's, so the predicate is not pushed (Strict refuses).
            Assert.Contains("could not be pushed down", Assert.Throws<NotSupportedException>(() => file.OpenReader<Precise>(r => r.A + r.A > 0)).Message);
        }
        finally
        {
            File.Delete(path);
        }
    }

    /// <summary>A double member over a Float32 column: the product is the double's (0.1f squared in Float64).</summary>
    [Fact]
    public void FloatingArithmetic_IsInTheMembersType()
    {
        string path = Path.Combine(Path.GetTempPath(), $"typed-float-{Guid.NewGuid():N}.parquet");
        try
        {
            var schema = new Apache.Arrow.Schema([new Apache.Arrow.Field("X", Apache.Arrow.Types.FloatType.Default, false)], null);
            using (ParquetWriter writer = ParquetWriter.Create(path, schema))
            {
                using var batch = new Apache.Arrow.RecordBatch(schema, [new Apache.Arrow.FloatArray.Builder().Append(0.1f).Append(3f).Build()], 2);
                writer.Write(batch);
            }
            double wide = (double)0.1f * (double)0.1f;
            System.Linq.Expressions.Expression<Func<Real, bool>> where = r => r.X * r.X == wide;
            using var file = ParquetFile.Open(path);
            using ParquetReader<Real> reader = file.OpenReader(where);
            Assert.True(reader.FiltersExactly);
            Assert.Equal(ParquetFile.ReadList<Real>(path).Where(where.Compile()), reader.ReadRows());
            Assert.Single(reader.ReadRows());
        }
        finally
        {
            File.Delete(path);
        }
    }

    public sealed record Wide(long A);

    public sealed record WideNullable(long? A);

    public sealed record Precise(decimal A);

    public sealed record Real(double X);

    /// <summary>A row index column put first does not shift the members: they bind by name.</summary>
    [Fact]
    public void ARowIndexColumn_DoesNotShiftTheMembers()
    {
        using var file = ParquetFile.Open(Fixture("indexed_v1.parquet"));
        List<Row> plain = file.OpenReader<Row>().ReadList();
        using ParquetReader<Row> reader = file.OpenReader<Row>(options: new ParquetReadOptions { RowIndexColumn = "n" });
        Assert.Equal(["n", "key", "cat"], reader.Schema.FieldsList.Select(f => f.Name));
        Assert.Equal(plain, reader.ReadList());
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
        ScalarExpr betweenColumns = ScalarExpr.NewBinary(BinaryOp.Lt, ScalarExpr.NewBinary(BinaryOp.Power, ScalarExpr.NewColumn("key"), ScalarExpr.NewColumn("nn")),
            ScalarExpr.NewColumn("nn")); // a power: not evaluated by the reader
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

    /// <summary>
    /// Batches turned into rows on several threads give the rows one thread gives, in order: strings, dictionary
    /// strings, nulls, NaN, lists and records; with a predicate and a row limit; sync and async.
    /// </summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ParallelHydration_GivesTheRowsOfOneThread(bool async)
    {
        using var file = ParquetFile.Open(Fixture("indexed_v1.parquet"));
        foreach (var (where, maxRows, count) in (ValueTuple<System.Linq.Expressions.Expression<Func<Full, bool>>?, long?, int>[])
            [(null, null, 3000), (r => (r.Key < 700 || r.Key >= 2100) && r.Cat != "c3", null, 1371), (r => r.Cat != "c2", 1500, 1500),
             (r => r.Cat == "c2", null, 429)])
        {
            List<List<string>> read = [];
            foreach (int parallelism in (int[])[1, 8])
            {
                var options = new ParquetReadOptions { BatchSize = 64, MaxDegreeOfParallelism = parallelism, MaxRows = maxRows };
                using ParquetReader<Full> reader = file.OpenReader(where, options);
                List<Full> rows = [];
                if (async)
                    await foreach (Full row in reader.ReadRowsAsync()) rows.Add(row);
                else
                    rows.AddRange(reader.ReadRows());
                read.Add([.. rows.Select(Text)]);
                // A list in one call: the batches' rows copied once (kept in their pooled arrays, or copied out of them).
                List<Full> list = async
                    ? await ParquetFile.ReadListAsync(Fixture("indexed_v1.parquet"), where, options)
                    : ParquetFile.ReadList(Fixture("indexed_v1.parquet"), where, options);
                read.Add([.. list.Select(Text)]);
            }
            Assert.All(read, r => Assert.Equal(read[0], r));
            Assert.Equal(count, read[0].Count);
        }

        static string Text(Full r) => $"{r.Key}|{r.Cat}|{r.Nn}|{BitConverter.DoubleToInt64Bits(r.F)}|{string.Join(",", r.Li!)}|{r.St}";
    }

    /// <summary>A row that cannot be made fails the read after the rows before it, on several threads as on one.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ParallelHydration_FailsAfterTheRowsBeforeTheFailure(bool async)
    {
        using var file = ParquetFile.Open(Fixture("indexed_v1.parquet"));
        foreach (int parallelism in (int[])[1, 8])
        {
            using ParquetReader<NotNull> reader = file.OpenReader<NotNull>(null, new ParquetReadOptions { BatchSize = 64, MaxDegreeOfParallelism = parallelism });
            List<long> keys = [];
            Exception error = await Assert.ThrowsAnyAsync<Exception>(async () =>
            {
                if (async)
                    await foreach (NotNull row in reader.ReadRowsAsync()) keys.Add(row.Key);
                else
                    foreach (NotNull row in reader.ReadRows()) keys.Add(row.Key);
            });
            Assert.Contains("Nn", error.Message);
            Assert.Equal(Enumerable.Range(0, 1000).Select(i => (long)i), keys); // nn is null from row 1000 on
        }
    }

    public sealed record Row(long Key, string? Cat);

    public sealed record Full(long Key, string? Cat, long? Nn, double F, List<int>? Li, Inner? St);

    public sealed record NotNull(long Key, long Nn);

    public sealed record WithNested(long Key, List<int>? Li, Inner? St);

    public sealed record Inner(long A, string? S);
}
