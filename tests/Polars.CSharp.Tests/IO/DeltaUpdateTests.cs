using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>UPDATE on Delta tables through the public API: Delta.Update, DataFrame.UpdateDelta and LazyFrame.UpdateDelta.</summary>
public class DeltaUpdateTests
{
    private static string NewPath(string name) =>
        Path.Combine(Path.GetTempPath(), $"delta_update_{name}_{Guid.NewGuid():N}");

    private static void WriteProducts(string path)
    {
        using var df = DataFrame.FromColumns(new
        {
            Id = new[] { 1L, 2L, 3L },
            Name = new[] { "pen", "ink", "pad" },
            Price = new[] { 10.0, 20.0, 30.0 },
        });
        df.WriteDelta(path, mode: DeltaSaveMode.Overwrite, mkdir: true);
    }

    /// <summary>The operation of the newest commit in the table's history.</summary>
    private static string LastOperation(string path)
    {
        using var history = Delta.History(path);
        long[] versions = history.Column("version").ToArray<long>();
        string[] operations = history.Column("operation").ToArray<string>();
        return operations[System.Array.IndexOf(versions, versions.Max())];
    }

    [Fact]
    public void Update_WithPredicate_ChangesMatchingRowsOnly_AndCommitsUpdate()
    {
        string path = NewPath("predicate");
        try
        {
            WriteProducts(path);

            Delta.Update(path, Col("Id") == 2L, new Dictionary<string, Expr> { ["Price"] = Col("Price") * 2.0 });

            using var read = DataFrame.ReadDelta(path).Sort("Id");
            Assert.Equal([10.0, 40.0, 30.0], read.Column("Price").ToArray<double>());
            Assert.Equal(["pen", "ink", "pad"], read.Column("Name").ToArray<string>());
            Assert.Equal("UPDATE", LastOperation(path));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }

    [Fact]
    public void Update_WithoutPredicate_UpdatesEveryRow()
    {
        string path = NewPath("all_rows");
        try
        {
            WriteProducts(path);

            Delta.Update(path, null, new Dictionary<string, Expr> { ["Price"] = Col("Price") + 1.0 });

            using var read = DataFrame.ReadDelta(path).Sort("Id");
            Assert.Equal([11.0, 21.0, 31.0], read.Column("Price").ToArray<double>());
            Assert.Equal("UPDATE", LastOperation(path));
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }

    [Fact]
    public void Update_OfIdentityColumn_Throws()
    {
        string path = NewPath("identity");
        try
        {
            Delta.CreateTable(path,
                new PolarsSchema().Add("id", DataType.Int64).Add("v", DataType.String),
                identityColumns: [new DeltaIdentityColumn("id")]);
            using (var rows = DataFrame.FromColumns(new { v = new[] { "a", "b" } }))
                rows.WriteDelta(path, mode: DeltaSaveMode.Append);

            var ex = Assert.ThrowsAny<Exception>(() =>
                Delta.Update(path, Col("v") == "a", new Dictionary<string, Expr> { ["id"] = Lit(5L) }));
            Assert.Contains("UPDATE on IDENTITY column", ex.Message);
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }

    [Fact]
    public void DataFrameUpdateDelta_UpdatesMatchedRowsByKey_AndInsertsNothing()
    {
        string path = NewPath("frame_key");
        try
        {
            WriteProducts(path);

            using (var source = DataFrame.FromColumns(new
            {
                Id = new[] { 2L, 9L },
                Price = new[] { 200.0, 900.0 },
            }))
            {
                source.UpdateDelta(path, ["Id"]);
            }

            using var read = DataFrame.ReadDelta(path).Sort("Id");
            Assert.Equal(3L, read.Height);
            Assert.Equal([1L, 2L, 3L], read.Column("Id").ToArray<long>());
            Assert.Equal([10.0, 200.0, 30.0], read.Column("Price").ToArray<double>());
            Assert.Equal(["pen", "ink", "pad"], read.Column("Name").ToArray<string>());
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }

    [Fact]
    public void LazyFrameUpdateDelta_WithCondition_UpdatesOnlyMatchedRowsSatisfyingIt()
    {
        string path = NewPath("lazy_condition");
        try
        {
            using (var table = DataFrame.FromColumns(new
            {
                Id = new[] { 1L, 2L, 3L },
                Price = new[] { 10.0, 20.0, 30.0 },
                Active = new[] { true, false, true },
            }))
            {
                table.WriteDelta(path, mode: DeltaSaveMode.Overwrite, mkdir: true);
            }

            using (var source = DataFrame.FromColumns(new
            {
                Id = new[] { 1L, 2L, 3L, 4L },
                Price = new[] { 100.0, 200.0, 300.0, 400.0 },
            }))
            using (var lazy = source.Lazy())
            {
                // The condition names target columns: only rows flagged Active take the new price.
                lazy.UpdateDelta(path, ["Id"], condition: Col("Active") == true);
            }

            using var read = DataFrame.ReadDelta(path).Sort("Id");
            Assert.Equal(3L, read.Height);
            Assert.Equal([100.0, 20.0, 300.0], read.Column("Price").ToArray<double>());
            Assert.Equal([true, false, true], read.Column("Active").ToArray<bool>());
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }

    [Fact]
    public void UpdateDelta_WithAConditionOverBothSides_UpdatesOnlyNewerSourceRows()
    {
        string path = NewPath("context");
        try
        {
            WriteProducts(path);
            using var source = DataFrame.FromColumns(new
            {
                Id = new[] { 1L, 2L, 9L },
                Name = new[] { "PEN", "INK", "new" },
                Price = new[] { 5.0, 25.0, 1.0 },
            });

            // Only where the source price is higher than the table's: Id 2. Id 9 matches nothing and is not inserted.
            source.UpdateDelta(path, ["Id"], m => m.Source("Price") > m.Target("Price"));

            using var read = DataFrame.ReadDelta(path).Sort("Id");
            Assert.Equal([1L, 2L, 3L], read.Column("Id").ToArray<long>());
            Assert.Equal(["pen", "INK", "pad"], read.Column("Name").ToArray<string>());
            Assert.Equal([10.0, 25.0, 30.0], read.Column("Price").ToArray<double>());
        }
        finally
        {
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }
}
