using System.Text.Json;
using System.Text.RegularExpressions;
using Polars.CSharp;
using Delta = Polars.CSharp.Delta;

namespace Polars.Integration.Tests;

/// <summary>
/// RENAME COLUMN, DROP COLUMNS and ALTER COLUMN TYPE through the public API, against Spark doing the same: the same
/// commits, then each reads, writes and alters what the other left.
/// </summary>
public partial class SparkColumnDdlTests
{
    private static void InRoot(Action<string> body)
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_ddl_{Guid.NewGuid():N}");
        try
        {
            body(root);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    private static (string Schema, string Rows) Spark(string table)
    {
        JsonElement read = LocalSpark.Run("read", table)[0].RootElement;
        return (read.GetProperty("schema").GetString()!,
            string.Join(" ", read.GetProperty("rows").EnumerateArray().Select(r => r.GetRawText())));
    }

    [GeneratedRegex("col-[0-9a-f-]{36}")]
    private static partial Regex PhysicalName();

    /// <summary>
    /// The commit's operation and parameters, and the metadata it leaves, as canonical JSON (keys sorted, JSON held in
    /// strings expanded), physical names (random per table) masked.
    /// </summary>
    private static string Commit(string table, long version)
    {
        var parts = new List<string>();
        foreach (string line in File.ReadAllLines(Path.Combine(table, "_delta_log", $"{version:D20}.json")))
        {
            using JsonDocument doc = JsonDocument.Parse(line);
            if (doc.RootElement.TryGetProperty("commitInfo", out JsonElement info))
                parts.Add(info.GetProperty("operation").GetString() + " " + Canonical(info.GetProperty("operationParameters")));
            if (doc.RootElement.TryGetProperty("metaData", out JsonElement metadata))
            {
                parts.Add(Canonical(metadata.GetProperty("schemaString")));
                parts.Add(Canonical(metadata.GetProperty("partitionColumns")));
                parts.Add(Canonical(metadata.GetProperty("configuration")));
            }
        }
        return PhysicalName().Replace(string.Join("\n", parts), "col-*");
    }

    private static string Canonical(JsonElement element) => element.ValueKind switch
    {
        JsonValueKind.Object => "{" + string.Join(",", element.EnumerateObject().OrderBy(p => p.Name, StringComparer.Ordinal)
            .Select(p => Quoted(p.Name) + ":" + Canonical(p.Value))) + "}",
        JsonValueKind.Array => "[" + string.Join(",", element.EnumerateArray().Select(Canonical)) + "]",
        JsonValueKind.String when element.GetString() is { } text && (text.StartsWith('{') || text.StartsWith('[')) => Expanded(text),
        _ => element.GetRawText(),
    };

    private static string Expanded(string text)
    {
        try
        {
            using JsonDocument inner = JsonDocument.Parse(text);
            return Canonical(inner.RootElement);
        }
        catch (JsonException)
        {
            return Quoted(text);
        }
    }

    private static string Quoted(string text) => "\"" + JsonEncodedText.Encode(text) + "\"";

    private const string Create =
        "CREATE TABLE {table} (id INT, b STRING, s STRUCT<x: INT, y: STRING>, arr ARRAY<STRUCT<f: INT, g: INT>>, pc STRING) USING delta " +
        "PARTITIONED BY (pc) TBLPROPERTIES ('delta.columnMapping.mode'='{mapping}', 'delta.enableTypeWidening'='true', " +
        "'delta.dataSkippingStatsColumns'='id,b,s.x')";

    private const string Rows =
        "INSERT INTO {table} VALUES (1, 'one', named_struct('x', 1, 'y', 'a'), array(named_struct('f', 1, 'g', 2)), 'p'), " +
        "(2, 'two', named_struct('x', 2, 'y', 'b'), array(named_struct('f', 3, 'g', 4)), 'q')";

    [SparkTheory]
    [InlineData("name")]
    [InlineData("id")]
    [Trait("DeltaKernel", "Spark")]
    public void ColumnChanges_CommitAsSpark_AndBothGoOnWithTheTable(string mapping)
    {
        InRoot(root =>
        {
            string kernel = Path.Combine(root, "kernel"), spark = Path.Combine(root, "spark");
            string[] tables = [kernel, spark];
            foreach (string table in tables)
            {
                LocalSpark.Run("sql", table, Create.Replace("{mapping}", mapping, StringComparison.Ordinal));
                LocalSpark.Run("sql", table, Rows);
            }

            (Action Kernel, string Spark)[] changes =
            [
                (() => Delta.RenameColumn(kernel, "B", "b2"), "RENAME COLUMN b TO b2"),
                (() => Delta.RenameColumn(kernel, "s.x", "x2"), "RENAME COLUMN s.x TO x2"),
                (() => Delta.RenameColumn(kernel, "pc", "pc2"), "RENAME COLUMN pc TO pc2"),
                (() => Delta.RenameColumn(kernel, "arr.element.f", "f1"), "RENAME COLUMN arr.element.f TO f1"),
                (() => Delta.DropColumns(kernel, ["s.y", "arr.element.g"]), "DROP COLUMNS (s.y, arr.element.g)"),
                (() => Delta.ChangeColumnType(kernel, "id", DataType.Int64), "ALTER COLUMN id TYPE BIGINT"),
                (() => Delta.ChangeColumnType(kernel, "s.x2", DataType.Int64), "ALTER COLUMN s.x2 TYPE BIGINT"),
                (() => Delta.ChangeColumnType(kernel, "arr.element.f1", DataType.Float64), "ALTER COLUMN arr.element.f1 TYPE DOUBLE"),
            ];
            long version = 2;
            foreach (var (byKernel, bySpark) in changes)
            {
                byKernel();
                LocalSpark.Run("sql", spark, $"ALTER TABLE {{table}} {bySpark}");
                Assert.Equal(Commit(spark, version), Commit(kernel, version));
                version++;
            }
            Assert.Equal(Spark(spark), Spark(kernel));

            // The kernel reads the altered table as Spark does, and writes, updates and compacts it.
            Assert.Equal(Spark(kernel).Rows, Spark(spark).Rows);
            using (DataFrame read = DataFrame.ReadDelta(kernel).Sort("id"))
            {
                Assert.Equal(["id", "b2", "s", "arr", "pc2"], read.Columns);
                Assert.Equal(["one", "two"], read["b2"].ToArray<string>());
            }
            using (DataFrame again = DataFrame.ReadDelta(kernel).Filter(Polars.CSharp.Polars.Col("id") == 1)
                .WithColumns(Polars.CSharp.Polars.Lit(3L).Alias("id")))
                again.WriteDelta(kernel);
            LocalSpark.Run("sql", spark, "INSERT INTO {table} SELECT 3, b2, s, arr, pc2 FROM {table} WHERE id = 1");
            Delta.Update(kernel, Polars.CSharp.Polars.Col("id") == 2,
                new Dictionary<string, Expr> { ["b2"] = Polars.CSharp.Polars.Lit("updated") });
            LocalSpark.Run("sql", spark, "UPDATE {table} SET b2 = 'updated' WHERE id = 2");
            Delta.Optimize(kernel);
            Assert.Equal(Spark(spark), Spark(kernel));

            // Spark goes on altering and writing the table the kernel altered.
            LocalSpark.Run("sql", kernel, "ALTER TABLE {table} RENAME COLUMN b2 TO b3");
            LocalSpark.Run("sql", kernel, "ALTER TABLE {table} DROP COLUMN arr");
            LocalSpark.Run("sql", kernel, "INSERT INTO {table} VALUES (4, 'four', named_struct('x2', 4), 'r')");
            var (schema, rows) = Spark(kernel);
            Assert.Equal("struct<id:bigint,b3:string,s:struct<x2:bigint>,pc2:string>", schema);
            Assert.Contains("\"b3\": \"four\"", rows);
            Assert.Contains("\"b3\": \"updated\"", rows);
            using DataFrame afterSpark = DataFrame.ReadDelta(kernel).Sort("id");
            Assert.Equal(["id", "b3", "s", "pc2"], afterSpark.Columns);
            Assert.Equal(4, afterSpark.Height);
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void ADroppedColumnAddedAgain_IsANewColumn_ForBoth()
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "table");
            LocalSpark.Run("sql", table, Create.Replace("{mapping}", "name", StringComparison.Ordinal));
            LocalSpark.Run("sql", table, Rows);

            Delta.DropColumns(table, ["b"]);
            LocalSpark.Run("sql", table, "ALTER TABLE {table} ADD COLUMN b STRING");
            LocalSpark.Run("sql", table, "INSERT INTO {table} (id, b, pc) VALUES (3, 'new', 'p')");

            // The old values of b stay in the old files, read by neither.
            Assert.DoesNotContain("\"one\"", Spark(table).Rows);
            using DataFrame read = DataFrame.ReadDelta(table).Sort("id");
            Assert.Equal([null, null, "new"], read["b"].ToArray<string?>());
        });
    }

    // ------------------------------------------------------------------ change data feed across schema changes

    /// <summary>The changes the kernel reads for a range ("version:id:type" in order, then the columns), or the refusal.</summary>
    private static string KernelChanges(string table, long start, long end, bool allowSchemaChanges = false)
    {
        try
        {
            using DataFrame changes = Delta.ReadChangeDataFeed(table, start, end, allowSchemaChanges: allowSchemaChanges).Collect();
            long[] versions = changes["_commit_version"].ToArray<long>();
            long[] ids = changes["id"].Cast(DataType.Int64).ToArray<long>();
            string[] types = changes["_change_type"].ToArray<string>();
            string rows = string.Join(" ", Enumerable.Range(0, (int)changes.Height).Select(i => $"{versions[i]}:{ids[i]}:{types[i]}").Order(StringComparer.Ordinal));
            return rows + " | " + (changes.Height == 0 ? "" : string.Join(",", changes.Columns.Where(c => c != "_commit_timestamp")));
        }
        catch (Polars.NET.Core.Exceptions.PolarsException e) when (e.Message.Contains("schema", StringComparison.Ordinal))
        {
            return "refused";
        }
    }

    private static string SparkChanges(string table, long start, long end)
    {
        JsonElement result = LocalSpark.Run("read_changes_range", table, start.ToString(), end.ToString())[0].RootElement;
        if (result.TryGetProperty("refused", out _)) return "refused";
        JsonElement[] rows = [.. result.GetProperty("rows").EnumerateArray()];
        string described = string.Join(" ", rows.Select(r => $"{r.GetProperty("_commit_version").GetInt64()}:{r.GetProperty("id").GetInt64()}:{r.GetProperty("_change_type").GetString()}").Order(StringComparer.Ordinal));
        return described + " | " + (rows.Length == 0 ? "" : string.Join(",", rows[0].EnumerateObject().Select(p => p.Name)));
    }

    [SparkTheory]
    [InlineData(true)]
    [InlineData(false)]
    [Trait("DeltaKernel", "Spark")]
    public void ChangeFeed_RefusesTheRangesSparkRefuses_AndReadsTheOthersAsSpark(bool mapped)
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "table");
            LocalSpark.Run("sql", table,
                "CREATE TABLE {table} (id INT, b STRING, c STRING) USING delta TBLPROPERTIES (" +
                (mapped ? "'delta.columnMapping.mode'='name', " : "") + "'delta.enableChangeDataFeed'='true', 'delta.enableTypeWidening'='true')");
            string[] steps = mapped
                ?
                [
                    "INSERT INTO {table} VALUES (1, 'b', 'c')", "ALTER TABLE {table} ADD COLUMN d STRING", "INSERT INTO {table} VALUES (2, 'b', 'c', 'd')",
                    "ALTER TABLE {table} RENAME COLUMN b TO b2", "INSERT INTO {table} VALUES (3, 'b', 'c', 'd')",
                    "ALTER TABLE {table} DROP COLUMN c", "INSERT INTO {table} VALUES (4, 'b', 'd')",
                    "ALTER TABLE {table} ADD COLUMN c STRING", "INSERT INTO {table} VALUES (5, 'b', 'd', 'c')", // c again: a new column
                    "ALTER TABLE {table} ALTER COLUMN id TYPE BIGINT", "INSERT INTO {table} VALUES (6, 'b', 'd', 'c')",
                ]
                :
                [
                    "INSERT INTO {table} VALUES (1, 'b', 'c')", "ALTER TABLE {table} ADD COLUMN d STRING", "INSERT INTO {table} VALUES (2, 'b', 'c', 'd')",
                    "ALTER TABLE {table} ALTER COLUMN id TYPE BIGINT", "INSERT INTO {table} VALUES (3, 'b', 'c', 'd')",
                    "ALTER TABLE {table} SET TBLPROPERTIES ('x'='y')", "INSERT INTO {table} VALUES (4, 'b', 'c', 'd')",
                ];
            foreach (string step in steps) LocalSpark.Run("sql", table, step);

            int refused = 0;
            for (long start = 0; start <= steps.Length; start++)
            {
                for (long end = start; end <= steps.Length; end++)
                {
                    string spark = SparkChanges(table, start, end);
                    Assert.True(spark == KernelChanges(table, start, end), $"[{start}, {end}]: Spark {spark}, kernel {KernelChanges(table, start, end)}");
                    if (spark == "refused") refused++;
                }
            }
            Assert.True(refused > 10); // the check is exercised, not vacuous

            // Allowed explicitly, the whole feed reads in the read schema (end version's under column mapping).
            string[] inserts = mapped
                ? ["1:1:insert", "3:2:insert", "5:3:insert", "7:4:insert", "9:5:insert", "11:6:insert"]
                : ["1:1:insert", "3:2:insert", "5:3:insert", "7:4:insert"];
            string columns = mapped ? "id,b2,d,c,_change_type,_commit_version" : "id,b,c,d,_change_type,_commit_version";
            Assert.Equal(string.Join(" ", inserts.Order(StringComparer.Ordinal)) + " | " + columns,
                KernelChanges(table, 0, steps.Length, allowSchemaChanges: true));
        });
    }
}
