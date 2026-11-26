using Polars.CSharp;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Wrappers;
using static Polars.CSharp.Polars;
using Delta = Polars.CSharp.Delta;

namespace Polars.Integration.Tests;

/// <summary>
/// Generated columns Spark declared (GENERATED ALWAYS AS, one of them a partition column): our appends, updates and
/// merges compute them as Spark does, Spark reads the values it would have written, and writes after us.
/// </summary>
public class SparkGeneratedColumnTests
{
    private sealed record Row(int id, string? s, DateTimeOffset ts);

    private static void InTable(Action<string> body)
    {
        string table = Path.Combine(Path.GetTempPath(), $"seseragi_generated_{Guid.NewGuid():N}");
        try
        {
            body(table);
        }
        finally
        {
            if (Directory.Exists(table)) Directory.Delete(table, true);
        }
    }

    /// <summary>Spark's rows: id, s, u, n, d (as days), a null as nothing.</summary>
    private static string SparkRows(string table) =>
        string.Join(" ", LocalSpark.Run("read", table)[0].RootElement.GetProperty("rows").EnumerateArray()
            .Select(r => $"{r.GetProperty("id")}|{r.GetProperty("s")}|{r.GetProperty("u")}|{r.GetProperty("n")}|{r.GetProperty("d")}"));

    private static readonly DateTimeOffset Then = new(2022, 3, 4, 5, 6, 7, TimeSpan.Zero);

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Generated_columns_Spark_declared_are_computed_on_our_writes()
    {
        InTable(table =>
        {
            LocalSpark.Run("sql", table, """
                CREATE TABLE {table} (id INT, s STRING, ts TIMESTAMP, u STRING GENERATED ALWAYS AS (upper(s)),
                    n INT GENERATED ALWAYS AS (length(s) * 2), d DATE GENERATED ALWAYS AS (CAST(ts AS DATE))) USING delta PARTITIONED BY (d)
                """);
            LocalSpark.Run("sql", table, "INSERT INTO {table} (id, s, ts) VALUES (1, 'ab', TIMESTAMP '2022-03-04 05:06:07')");

            using (DataFrame rows = DataFrame.From([new Row(2, "cd", Then.AddDays(1)), new Row(3, null, Then)]))
                rows.WriteDelta(table, mode: DeltaSaveMode.Append);
            Delta.Update(table, Col("id") == 1, new Dictionary<string, Expr> { ["s"] = Lit("qqq") });
            using (DataFrame source = DataFrame.From([new Row(2, "zz", Then), new Row(4, "new", Then.AddDays(2))]))
                DeltaKernelMerge.Merge(table, PolarsWrapper.LazyClone(source.Lazy().Handle), ["id"], []);

            // 2022-03-04 is day 19055.
            Assert.Equal("1|qqq|QQQ|6|19055 2|zz|ZZ|4|19055 3||||19055 4|new|NEW|6|19057", SparkRows(table));
            Assert.True(Directory.Exists(Path.Combine(table, "d=2022-03-06")));

            LocalSpark.Run("sql", table, "INSERT INTO {table} (id, s, ts) VALUES (5, 'five', TIMESTAMP '2022-03-04 00:00:00')");
            using DataFrame read = DataFrame.ReadDelta(table).Sort("id");
            Assert.Equal<string?[]>(["QQQ", "ZZ", null, "NEW", "FIVE"], read.Column("u").ToArray<string?>());
        });
    }
}
