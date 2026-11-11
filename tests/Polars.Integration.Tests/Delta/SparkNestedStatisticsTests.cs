using System.Text.Json;
using System.Text.Json.Nodes;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Skipping;
using Polars.NET.Core;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Wrappers;

namespace Polars.Integration.Tests;

/// <summary>Statistics of nested struct fields, as Spark writes them and reads them for data skipping.</summary>
public class SparkNestedStatisticsTests
{
    [SparkTheory]
    [InlineData("none")]
    [InlineData("name")]
    [Trait("DeltaKernel", "Spark")]
    public void TheKernelsStatisticsOfNestedFields_EqualSparks_AndSkipFiles(string mapping)
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_nstats_{Guid.NewGuid():N}");
        try
        {
            string table = Path.Combine(root, "t");
            LocalSpark.Run("sql", table,
                "CREATE TABLE {table} (id INT, s STRUCT<x: INT, y: STRING, z: STRUCT<d: DATE, dec: DECIMAL(10,2), flag: BOOLEAN>>, " +
                $"arr ARRAY<INT>) USING delta TBLPROPERTIES ('delta.columnMapping.mode'='{mapping}')");
            // Two files with disjoint ranges of s.x; null structs and null fields in both.
            LocalSpark.Run("sql", table,
                "INSERT INTO {table} SELECT /*+ REPARTITION(1) */ * FROM VALUES " +
                "(1, named_struct('x', 10, 'y', 'apple', 'z', named_struct('d', DATE'2024-01-05', 'dec', 1.25, 'flag', true)), array(1))," +
                "(2, NULL, NULL)," +
                "(3, named_struct('x', 20, 'y', NULL, 'z', NULL), array())" +
                " AS v(id, s, arr)");
            LocalSpark.Run("sql", table,
                "INSERT INTO {table} SELECT /*+ REPARTITION(1) */ * FROM VALUES " +
                "(4, named_struct('x', 500, 'y', 'zebra', 'z', named_struct('d', DATE'2025-06-01', 'dec', -3.5, 'flag', false)), array(4))," +
                "(5, named_struct('x', 600, 'y', 'kiwi', 'z', named_struct('d', NULL, 'dec', 9.99, 'flag', NULL)), NULL)" +
                " AS v(id, s, arr)");

            Snapshot bySpark = Snapshot.Load(new LocalFileSystem(table));
            Assert.Equal(2, bySpark.Files.Count);
            Dictionary<long, string> sparkStats = bySpark.Files.ToDictionary(f => f.ParsedStats!.NumRecords!.Value, f => f.Stats!);

            // The kernel rewrites both files (rows unchanged) and computes their statistics from the footers.
            // (one update per file: an update writes the rows of the files it rewrites together.)
            DeltaKernelUpdate.Update(table, PolarsWrapper.LtEq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(3)), [("id", PolarsWrapper.Col("id"))]);
            DeltaKernelUpdate.Update(table, PolarsWrapper.GtEq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(4)), [("id", PolarsWrapper.Col("id"))]);
            Snapshot byKernel = Snapshot.Load(new LocalFileSystem(table));
            Assert.Equal(2, byKernel.Files.Count);
            Assert.DoesNotContain(byKernel.Files, f => bySpark.Files.Any(s => s.Path == f.Path));
            foreach (var file in byKernel.Files)
            {
                JsonNode spark = JsonNode.Parse(sparkStats[file.ParsedStats!.NumRecords!.Value])!;
                JsonNode kernel = JsonNode.Parse(file.Stats!)!;
                // Spark also counts null arrays, which a footer cannot tell from empty ones or null elements.
                string arr = bySpark.Schema.Field("arr")!.PhysicalName;
                spark["nullCount"]!.AsObject().Remove(arr);
                Assert.True(JsonNode.DeepEquals(spark, kernel), $"Spark:  {spark.ToJsonString()}\nkernel: {kernel.ToJsonString()}");
            }

            // A filter on a nested field skips the file whose range of s.x does not hold a match.
            int scanned = -1;
            using (LazyFrameHandle lf = DeltaKernelScan.ScanPath(table, null, null,
                       Predicate.Gt(new ColumnPath("s", "x"), 100), null, count => scanned = count))
            {
            }
            Assert.Equal(1, scanned);
            using (LazyFrameHandle lf = DeltaKernelScan.ScanPath(table, null, null,
                       Predicate.Lt(new ColumnPath("s", "z", "d"), new DateOnly(2024, 6, 1)), null, count => scanned = count))
            {
            }
            Assert.Equal(1, scanned);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }
}
