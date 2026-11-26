using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Polars.CSharp;
using Polars.NET.Core.Exceptions;
using static Polars.CSharp.Polars;
using Delta = Polars.CSharp.Delta;

namespace Polars.Integration.Tests;

/// <summary>
/// CHECK constraints Spark added (ALTER TABLE ADD CONSTRAINT, stored as Spark renders them): rows meeting them are
/// written and Spark reads them; a row violating one fails the write with Spark's message, and Spark sees nothing new.
/// </summary>
public class SparkConstraintTests
{
    private sealed record Row(int a, string? s, DateTimeOffset ts, Inner st);
    private sealed record Inner(int x);

    private static void InTable(Action<string> body)
    {
        string table = Path.Combine(Path.GetTempPath(), $"seseragi_constraint_{Guid.NewGuid():N}");
        try
        {
            body(table);
        }
        finally
        {
            if (Directory.Exists(table)) Directory.Delete(table, true);
        }
    }

    private static string SparkRows(string table) =>
        string.Join(" ", LocalSpark.Run("read", table)[0].RootElement.GetProperty("rows").EnumerateArray().Select(r => r.GetRawText()));

    private static DeltaConstraintViolationException Violation(Action write) =>
        Assert.IsType<DeltaConstraintViolationException>(Assert.Throws<PolarsException>(write).InnerException);

    private static void Append(string table, params Row[] rows)
    {
        using DataFrame df = DataFrame.From(rows);
        df.WriteDelta(table, mode: DeltaSaveMode.Append);
    }

    private static readonly DateTimeOffset Then = new(2021, 3, 5, 4, 0, 0, TimeSpan.Zero);

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Constraints_Spark_added_are_enforced_on_appends_and_updates()
    {
        InTable(table =>
        {
            LocalSpark.Run("sql", table, "CREATE TABLE {table} (a INT, s STRING, ts TIMESTAMP, st STRUCT<x: INT>) USING delta");
            LocalSpark.Run("sql", table, "ALTER TABLE {table} ADD CONSTRAINT positive CHECK (a > 0 AND st.x > 0)");
            LocalSpark.Run("sql", table, """ALTER TABLE {table} ADD CONSTRAINT word CHECK (s <> "bad" OR s IS NULL)""");
            LocalSpark.Run("sql", table, "ALTER TABLE {table} ADD CONSTRAINT recent CHECK (ts > TIMESTAMP '2000-01-01 00:00:00')");

            Append(table, new Row(1, "ok", Then, new Inner(1)), new Row(2, null, Then, new Inner(2)));
            string rows = SparkRows(table);
            Assert.Contains("\"a\": 1,", rows);
            Assert.Contains("\"a\": 2,", rows);

            var e = Violation(() => Append(table, new Row(3, "ok", Then, new Inner(3)), new Row(-1, "ok", Then, new Inner(1))));
            Assert.Equal("positive", e.Constraint);
            Assert.Equal("[DELTA_VIOLATE_CONSTRAINT_WITH_VALUES] CHECK constraint positive (a > 0 AND st . x > 0) violated by row with values:\n - a : -1\n - st.x : 1", e.Message);
            Assert.Equal("word", Violation(() => Append(table, new Row(4, "bad", Then, new Inner(1)))).Constraint);
            var old = Violation(() => Append(table, new Row(5, "ok", new DateTimeOffset(1999, 1, 1, 0, 0, 0, TimeSpan.Zero), new Inner(1))));
            Assert.Equal([KeyValuePair.Create("ts", "915148800000000")], old.Values);
            Violation(() => Delta.Update(table, Col("a") == 2, new Dictionary<string, Expr> { ["a"] = Lit(-2) }));
            Assert.Equal(rows, SparkRows(table));

            // Spark writes to the table after us, under the same constraints.
            LocalSpark.Run("sql", table, "INSERT INTO {table} VALUES (6, 'six', TIMESTAMP '2021-01-01 00:00:00', named_struct('x', 6))");
            Append(table, new Row(7, "seven", Then, new Inner(7)));
            using DataFrame read = DataFrame.ReadDelta(table).Sort("a");
            Assert.Equal([1, 2, 6, 7], read.Column("a").ToArray<int>());
        });
    }
}
