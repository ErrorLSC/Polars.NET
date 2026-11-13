using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Engine;
using Seseragi.Lakehouse.Tests;

namespace Polars.Integration.Tests;

/// <summary>
/// Checkpoints delta-spark writes, in each of its forms, read by the kernel's managed Parquet reader: the table read
/// through them equals the table replayed from its commits.
/// </summary>
public class SparkCheckpointReadTests
{
    private const string Partitioned = "PARTITIONED BY (pi, pd, ps)";

    [SparkTheory]
    [InlineData(Partitioned, "")]                                                                                        // text statistics
    [InlineData(Partitioned, ", 'delta.checkpoint.writeStatsAsStruct'='true'")]                                          // text and struct
    [InlineData(Partitioned, ", 'delta.checkpoint.writeStatsAsJson'='false', 'delta.checkpoint.writeStatsAsStruct'='true'")] // struct only
    [InlineData(Partitioned, ", 'delta.checkpointPolicy'='v2'")]
    [InlineData(Partitioned, ", 'delta.checkpointPolicy'='v2', 'delta.checkpoint.writeStatsAsJson'='false', 'delta.checkpoint.writeStatsAsStruct'='true'")]
    [InlineData(Partitioned, ", 'delta.enableDeletionVectors'='true', 'delta.enableRowTracking'='true'")]
    [InlineData(Partitioned, ", 'delta.columnMapping.mode'='name', 'delta.checkpoint.writeStatsAsJson'='false', 'delta.checkpoint.writeStatsAsStruct'='true'")]
    [InlineData("CLUSTER BY (i)", ", 'delta.enableDeletionVectors'='true'")]                                              // domain metadata
    [Trait("DeltaKernel", "Spark")]
    public void SparkCheckpoints_EqualTheCommits(string layout, string properties)
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_ckread_{Guid.NewGuid():N}");
        try
        {
            string table = Path.Combine(root, "table");
            LocalSpark.Run("sql", table,
                $"CREATE TABLE {{table}} ({SparkCheckpointStatisticsTests.Columns}) USING delta {layout} " +
                $"TBLPROPERTIES ('delta.checkpointInterval'='2'{properties})");                              // v0
            LocalSpark.Run("sql", table, $"INSERT INTO {{table}} VALUES {SparkCheckpointStatisticsTests.Rows}"); // v1
            LocalSpark.Run("sql", table, "INSERT INTO {table} (i, s, pi) VALUES (3, 'x', 8)");                 // v2: checkpoint
            LocalSpark.Run("sql", table, "DELETE FROM {table} WHERE i = 2");                                   // v3
            LocalSpark.Run("sql", table, "INSERT INTO {table} (i, s, pi) VALUES (4, 'y', 9)");                 // v4: checkpoint
            LocalSpark.Run("sql", table, "UPDATE {table} SET s = 'z' WHERE i = 3");                            // v5

            Assert.Contains(Directory.GetFiles(Path.Combine(table, "_delta_log")), f => Path.GetFileName(f).StartsWith("00000000000000000004.checkpoint.", StringComparison.Ordinal));
            var fs = new LocalFileSystem(table);
            foreach (long? version in (long?[])[2, 3, 4, null])
                SnapshotAssert.Same(Snapshot.Load(fs, version, useCheckpoints: false), Snapshot.Load(fs, version));
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }
}
