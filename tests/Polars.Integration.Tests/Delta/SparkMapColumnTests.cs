using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.Integration.Tests;

/// <summary>
/// Tables with map columns: the engine has no map type and would write maps as Parquet lists, which Spark then
/// refuses to read, so the kernel writes no data files of such tables (temporary, until the engine writes maps).
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

    private static string Rows(string table) =>
        string.Join(" ", LocalSpark.Run("read", table)[0].RootElement.GetProperty("rows").EnumerateArray().Select(r => r.GetRawText()));

    private static long Version(string table) => Snapshot.Load(new LocalFileSystem(table)).Version;

    [SparkTheory]
    [InlineData("m MAP<STRING, INT>", "map('k', 1)", "m")]
    [InlineData("s STRUCT<n: INT, m: MAP<STRING, INT>>", "named_struct('n', 1, 'm', map('k', 1))", "s")]
    [Trait("DeltaKernel", "Spark")]
    public void RewritesOfTablesWithMapColumns_AreRefused_AndLeaveTheTableAsItWas(string column, string value, string name)
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "map");
            LocalSpark.Run("sql", table, $"CREATE TABLE {{table}} (id INT, {column}) USING delta");
            // Files of several rows (one task each), so that deleting one row rewrites a file rather than dropping it.
            LocalSpark.Run("sql", table, $"INSERT INTO {{table}} SELECT /*+ REPARTITION(1) */ * FROM VALUES (1, {value}), (2, {value}) AS v(id, c)");
            LocalSpark.Run("sql", table, $"INSERT INTO {{table}} SELECT /*+ REPARTITION(1) */ * FROM VALUES (3, {value}), (4, {value}) AS v(id, c)");
            string before = Rows(table);
            long version = Version(table);

            Action[] rewrites =
            [
                () => DeltaKernelUpdate.Update(table, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(1)), [("id", PolarsWrapper.Lit(100))]),
                () => DeltaKernelDelete.Delete(table, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(2))),
                () => DeltaKernelMerge.Merge(table, DeltaKernelScan.Scan(table), ["id"],
                    [new(MergeClauseKind.MatchedUpdate, PolarsWrapper.Lit(true))]),
                () => DeltaKernelOptimize.Optimize(table),
            ];
            for (int i = 0; i < rewrites.Length; i++)
            {
                var error = Record.Exception(rewrites[i]);
                Assert.True(error is PolarsException, $"rewrite {i}: {error?.GetType().Name ?? "no exception"}");
                // A merge's source brings the map as a list of key/value structs: refused even before, as the wrong type.
                Assert.Contains(i == 2 ? "is map<string,integer> in the table" : $"map columns ({name})", error!.Message);
            }

            Assert.Equal(version, Version(table));
            Assert.Equal(before, Rows(table));
            // Nothing left behind: only the files Spark wrote.
            Assert.Equal(Snapshot.Load(new LocalFileSystem(table)).Files.Count, Directory.GetFiles(table, "*.parquet").Length);
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

            Assert.Equal("""{"id": 1, "m": [{"key": "a", "value": 1}]}""", Rows(table));
        });
    }
}
