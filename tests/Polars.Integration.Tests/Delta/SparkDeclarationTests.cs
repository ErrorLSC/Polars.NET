using Polars.CSharp;
using Polars.NET.Core.Exceptions;
using Delta = Polars.CSharp.Delta;

namespace Polars.Integration.Tests;

/// <summary>
/// The table's SQL expressions declared through the public API, read by Spark as its own: constraints we add and
/// defaults and generated columns we declare are enforced and filled by Spark's writes; constraints and defaults
/// Spark declared are dropped by us.
/// </summary>
public class SparkDeclarationTests
{
    private sealed record Row(int id, int? a, string? s, int? g);
    private sealed record IdOnly(int id);

    private static void InTable(Action<string> body)
    {
        string table = Path.Combine(Path.GetTempPath(), $"seseragi_declare_{Guid.NewGuid():N}");
        try
        {
            body(table);
        }
        finally
        {
            if (Directory.Exists(table)) Directory.Delete(table, true);
        }
    }

    private static string Rows(string table) => string.Join(" ",
        LocalSpark.Run("query", table, "SELECT id, a, s, g FROM {table} ORDER BY id")[0].RootElement.GetProperty("rows").EnumerateArray()
            .Select(r => $"{r.GetProperty("id").GetRawText()}|{r.GetProperty("a").GetRawText()}|{r.GetProperty("s").GetRawText()}|{r.GetProperty("g").GetRawText()}"));

    private static void Append<T>(string table, params T[] rows)
    {
        using DataFrame df = DataFrame.From(rows);
        df.WriteDelta(table, mode: DeltaSaveMode.Append);
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void What_we_declare_Spark_enforces_and_fills()
    {
        InTable(table =>
        {
            Delta.CreateTable(table, PolarsSchema.From<Row>(),
                generatedColumns: [new DeltaGeneratedColumn("g", "a * 2")], columnDefaults: [new DeltaColumnDefault("s", "'n/a'")]);
            Append(table, new IdOnly(1));
            // Kept as written: Spark's own rendering would store 'it''s' as 'it' 's', which reads back as "its".
            Delta.AddConstraint(table, "Word", "s <> 'it''s'");
            Delta.SetColumnDefault(table, "a", "5");

            LocalSpark.Run("sql", table, "INSERT INTO {table} (id) VALUES (2)");
            LocalSpark.Run("sql", table, "INSERT INTO {table} (id, a, s) VALUES (3, 1, 'its')");
            Assert.Contains("DELTA_VIOLATE_CONSTRAINT_WITH_VALUES",
                Assert.ThrowsAny<Exception>(() => LocalSpark.Run("sql", table, "INSERT INTO {table} (id, s) VALUES (4, 'it''s')")).Message);
            Assert.Contains("CHECK constraint Generated Column (g <=> (a * 2))",
                Assert.ThrowsAny<Exception>(() => LocalSpark.Run("sql", table, "INSERT INTO {table} VALUES (5, 1, 'x', 9)")).Message);
            Assert.Equal("""1|null|"n/a"|null 2|5|"n/a"|10 3|1|"its"|2""", Rows(table));

            // And we write after Spark under the same declarations.
            Append(table, new IdOnly(6));
            Assert.EndsWith(""" 6|5|"n/a"|10""", Rows(table));
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void What_Spark_declared_we_drop()
    {
        InTable(table =>
        {
            LocalSpark.Run("sql", table, """
                CREATE TABLE {table} (id INT, a INT, s STRING, g INT) USING delta
                TBLPROPERTIES ('delta.feature.allowColumnDefaults' = 'supported')
                """);
            LocalSpark.Run("sql", table, "ALTER TABLE {table} ADD CONSTRAINT Positive CHECK (a > -5)");
            LocalSpark.Run("sql", table, "ALTER TABLE {table} ALTER COLUMN s SET DEFAULT 'x'");
            Assert.Equal("1 rows in " + table + " violate the new CHECK constraint (a > -5 AND a < 0).",
                Record(() =>
                {
                    Append(table, new Row(1, 3, "y", null));
                    Delta.AddConstraint(table, "neg", "a > -5 AND a < 0");
                }));

            Delta.DropConstraint(table, "POSITIVE");
            Delta.DropColumnDefault(table, "S");
            LocalSpark.Run("sql", table, "INSERT INTO {table} (id, a) VALUES (2, -9)");
            Assert.Equal("""1|3|"y"|null 2|-9|null|null""", Rows(table));
        });

        static string Record(Action action) => Assert.Throws<PolarsException>(action).Message;
    }
}
