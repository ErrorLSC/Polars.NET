using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Skipping;
using Polars.CSharp;
using Polars.NET.Core.Delta;
using Polars.NET.Core;
using Polars.NET.Core.Wrappers;
using Delta = Polars.CSharp.Delta;

namespace Polars.Integration.Tests;

/// <summary>
/// The typed columns of the kernel's checkpoints (<c>add.stats_parsed</c>, <c>add.partitionValues_parsed</c>) against
/// the ones delta-spark writes for the same files, and tables read through them alone.
/// </summary>
public class SparkCheckpointStatisticsTests
{
    private static void InRoot(Action<string> body)
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_ckpt_{Guid.NewGuid():N}");
        try
        {
            body(root);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    private static string CheckpointOf(string table, long version) =>
        Path.Combine(table, "_delta_log", $"{version:D20}.checkpoint.parquet");

    /// <summary>The add column's schema and each file's typed statistics and partition values, as Spark reads the checkpoint.</summary>
    private static (string Schema, Dictionary<string, string> Adds) Adds(string checkpoint)
    {
        JsonElement result = LocalSpark.Run("checkpoint_adds", checkpoint)[0].RootElement;
        Dictionary<string, string> adds = result.GetProperty("adds").EnumerateObject().ToDictionary(p => p.Name, p => p.Value.GetRawText());
        return (result.GetProperty("schema").GetString()!, adds);
    }

    internal const string Columns =
        "i INT, l BIGINT, s STRING, d DATE, ts TIMESTAMP, tn TIMESTAMP_NTZ, dec DECIMAL(10,2), big DECIMAL(25,3), dbl DOUBLE, f FLOAT, " +
        "sh SMALLINT, by TINYINT, b BOOLEAN, bin BINARY, st STRUCT<x: INT, y: STRING>, arr ARRAY<INT>, pi INT, pd DATE, ps STRING";

    internal const string Rows =
        "(1, 10000000000, 'abc', DATE'2024-01-02', TIMESTAMP'2024-01-02 03:04:05.123456', TIMESTAMP_NTZ'2024-01-02 03:04:05.123456', " +
        "-1.25, 1234567890123456789012.345, 1.5, 2.5, -3, 4, true, X'0102', named_struct('x', 1, 'y', 'q'), array(1, 2), 7, DATE'2020-02-02', 'p1'), " +
        "(2, -5, 'a string longer than thirty-two characters, cut in the statistics', DATE'1969-12-31', TIMESTAMP'1960-06-01 00:00:00', " +
        "TIMESTAMP_NTZ'2100-01-01 23:59:59.999999', 99999999.99, -0.001, -0.0, -1e30, 32767, -128, false, X'', named_struct('x', -1, 'y', NULL), " +
        "array(), 7, DATE'2020-02-02', 'p1'), " +
        "(NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)";

    [SparkTheory]
    [InlineData("none", false)]
    [InlineData("name", false)]
    [InlineData("id", true)]
    [Trait("DeltaKernel", "Spark")]
    public void TypedStatistics_EqualSparks_ForTheSameFiles(string mapping, bool deletionVectors)
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "table");
            LocalSpark.Run("sql", table,
                $"CREATE TABLE {{table}} ({Columns}) USING delta PARTITIONED BY (pi, pd, ps) TBLPROPERTIES ('delta.columnMapping.mode'='{mapping}', " +
                $"'delta.checkpointInterval'='4', 'delta.enableDeletionVectors'='{(deletionVectors ? "true" : "false")}')"); // v0
            LocalSpark.Run("sql", table, $"INSERT INTO {{table}} VALUES {Rows}");                                           // v1
            LocalSpark.Run("sql", table, "INSERT INTO {table} (i, s, pi) VALUES (3, 'x', 8)");                               // v2

            // v3: the kernel writes files of its own (or, with deletion vectors, marks a row deleted: loose bounds).
            if (deletionVectors)
            {
                Delta.Delete(table, Polars.CSharp.Polars.Col("i") == 2);
            }
            else
            {
                using DataFrame again = DataFrame.ReadDelta(table).Filter(Polars.CSharp.Polars.Col("i").IsNotNull());
                again.WriteDelta(table);
            }
            // v4: a kernel commit at the checkpoint interval: the kernel writes the checkpoint.
            Delta.SetTableProperties(table, new() { ["note"] = "checkpointed by the kernel" });
            Assert.True(File.Exists(CheckpointOf(table, 4)));

            // v5: Spark checkpoints the same files.
            LocalSpark.Run("sql", table, "ALTER TABLE {table} SET TBLPROPERTIES ('delta.checkpointInterval'='1')");
            Assert.True(File.Exists(CheckpointOf(table, 5)));

            var (kernelSchema, kernelAdds) = Adds(CheckpointOf(table, 4));
            var (sparkSchema, sparkAdds) = Adds(CheckpointOf(table, 5));
            Assert.Equal(sparkSchema, kernelSchema);
            Assert.Equal(sparkAdds.Keys.Order(), kernelAdds.Keys.Order());
            foreach (var (path, spark) in sparkAdds)
            {
                using JsonDocument expected = JsonDocument.Parse(spark), actual = JsonDocument.Parse(kernelAdds[path]);
                Assert.True(JsonElement.DeepEquals(expected.RootElement, actual.RootElement), $"{path}\nSpark:  {spark}\nkernel: {kernelAdds[path]}");
            }
            Assert.Contains("stats_parsed:struct<numRecords:bigint,minValues:", kernelSchema);
            Assert.Contains(deletionVectors ? ",tightBounds:boolean>" : "nullCount:", kernelSchema);
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void TablesCheckpointedWithTypedStatisticsOnly_AreReadAndSkippedByBoth()
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "table");
            LocalSpark.Run("sql", table,
                "CREATE TABLE {table} (i INT, d DECIMAL(25,3), tn TIMESTAMP_NTZ, pc STRING) USING delta PARTITIONED BY (pc) " +
                "TBLPROPERTIES ('delta.columnMapping.mode'='name', 'delta.checkpointInterval'='3', 'delta.checkpoint.writeStatsAsJson'='false')"); // v0
            LocalSpark.Run("sql", table, "INSERT INTO {table} VALUES (1, 1.5, TIMESTAMP_NTZ'2024-01-01 00:00:00', 'a')");     // v1
            LocalSpark.Run("sql", table, "INSERT INTO {table} VALUES (100, 99.25, TIMESTAMP_NTZ'2030-01-01 00:00:00', 'b')"); // v2
            Delta.SetTableProperties(table, new() { ["note"] = "x" });                                                        // v3: kernel checkpoint
            Assert.True(File.Exists(Path.Combine(table, "_delta_log", $"{3:D20}.checkpoint.parquet")));
            Assert.DoesNotContain("stats:", Adds(Path.Combine(table, "_delta_log", $"{3:D20}.checkpoint.parquet")).Schema);

            // Spark reads the table from the kernel's checkpoint, where no JSON statistics are left.
            JsonElement filtered = LocalSpark.Run("query", table, "SELECT i FROM {table} WHERE d > 50 AND tn > TIMESTAMP_NTZ'2029-01-01 00:00:00'")[0].RootElement;
            Assert.Equal("""[{"i": 100}]""", filtered.GetProperty("rows").GetRawText());

            // So does the kernel: its reader takes the statistics from stats_parsed.
            int scanned = -1;
            using (LazyFrameHandle lf = DeltaKernelScan.ScanPath(table, null, null, Predicate.Gt(new ColumnPath("i"), 50), null, n => scanned = n))
            {
            }
            Assert.Equal(1, scanned);
            using (LazyFrameHandle lf = DeltaKernelScan.ScanPath(table, null, null,
                       Predicate.Lt(new ColumnPath("tn"), new DateTime(2025, 1, 1)), null, n => scanned = n))
            {
            }
            Assert.Equal(1, scanned);
            using DataFrame all = DataFrame.ReadDelta(table).Sort("i");
            Assert.Equal([1, 100], all["i"].ToArray<int>());
        });
    }
}
