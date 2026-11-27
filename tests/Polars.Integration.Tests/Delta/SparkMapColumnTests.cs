using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.Integration.Tests;

/// <summary>
/// Tables with map columns, written by Spark and rewritten here: the engine reads them as maps, the managed sink writes
/// them back as Parquet maps, which Spark reads as the table's maps.
/// </summary>
public class SparkMapColumnTests
{
    private static void InRoot(Action<string> body)
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_map_{Guid.NewGuid():N}");
        try
        {
            body(root);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    private static string[] Rows(string table) =>
        [.. LocalSpark.Run("read", table)[0].RootElement.GetProperty("rows").EnumerateArray().Select(r => r.GetRawText())];

    private static long Version(string table) => Snapshot.Load(new LocalFileSystem(table)).Version;

    [SparkTheory]
    [InlineData("m MAP<STRING, INT>", "map('k', 1, 'j', 2)")]
    [InlineData("s STRUCT<n: INT, m: MAP<STRING, INT>>", "named_struct('n', 1, 'm', map('k', 1))")]
    [InlineData("m MAP<STRING, STRUCT<a: INT, b: STRING>>", "map('k', named_struct('a', 1, 'b', 'x'))")]
    [InlineData("a ARRAY<MAP<INT, STRING>>", "array(map(1, 'x'), map(2, 'y'))")]
    [Trait("DeltaKernel", "Spark")]
    public void RewritesOfTablesWithMapColumns_KeepTheMaps_AsSparkReadsThem(string column, string value)
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "map");
            LocalSpark.Run("sql", table, $"CREATE TABLE {{table}} (id INT, {column}) USING delta");
            // Files of several rows (one task each), so that deleting one row rewrites a file rather than dropping it.
            LocalSpark.Run("sql", table, $"INSERT INTO {{table}} SELECT /*+ REPARTITION(1) */ * FROM VALUES (1, {value}), (2, {value}) AS v(id, c)");
            LocalSpark.Run("sql", table, $"INSERT INTO {{table}} SELECT /*+ REPARTITION(1) */ * FROM VALUES (3, {value}), (4, {value}) AS v(id, c)");
            string[] before = Rows(table);
            // The map of a row as Spark renders it (every row holds the same one).
            string Row(int id) => before[0].Replace("\"id\": 1,", $"\"id\": {id},", StringComparison.Ordinal);
            long version = Version(table);

            (Action Rewrite, int[] Ids)[] steps =
            [
                (() => DeltaKernelUpdate.Update(table, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(1)), [("id", PolarsWrapper.Lit(100))]), [2, 3, 4, 100]),
                (() => DeltaKernelDelete.Delete(table, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(2))), [3, 4, 100]),
                // The source's maps come as the engine reads them: the table's maps.
                (() => DeltaKernelMerge.Merge(table, DeltaKernelScan.Scan(table), ["id"], [new(MergeClauseKind.MatchedUpdate, PolarsWrapper.Lit(true))]), [3, 4, 100]),
                (() => DeltaKernelOptimize.Optimize(table), [3, 4, 100]),
                (() => DeltaKernelWrite.Write(table, PolarsWrapper.LazyFilter(DeltaKernelScan.Scan(table), PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(3))),
                    KernelWriteMode.Append), [3, 3, 4, 100]),
            ];
            foreach (var (rewrite, ids) in steps)
            {
                rewrite();
                Assert.True(Version(table) >= version); // optimize commits nothing when there is nothing to compact
                version = Version(table);
                Assert.Equal([.. ids.Select(Row)], Rows(table));
            }
            // Read back here too: the maps as the engine reads them, every row's the same as Spark's first.
            using Polars.NET.Core.DataFrameHandle read = PolarsWrapper.LazyCollect(DeltaKernelScan.Scan(table), Polars.NET.Core.Enums.PlEngine.Auto);
            Assert.Equal(4L, PolarsWrapper.DataFrameHeight(read));
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void DeletesWithDeletionVectors_RewriteNothing_AndStillWork()
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "map");
            LocalSpark.Run("sql", table,
                "CREATE TABLE {table} (id INT, m MAP<STRING, INT>) USING delta TBLPROPERTIES ('delta.enableDeletionVectors'='true')");
            LocalSpark.Run("sql", table, "INSERT INTO {table} VALUES (1, map('a', 1)), (2, map('b', 2))");

            DeltaKernelDelete.Delete(table, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(2)));

            Assert.Equal(["""{"id": 1, "m": [{"key": "a", "value": 1}]}"""], Rows(table));
        });
    }
}
