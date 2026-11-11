using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Delta.Log;
using Kaminari.Seseragi.Lakehouse.Engine;
using Kaminari.Seseragi.Lakehouse.Skipping;
using Polars.CSharp;
using Polars.NET.Core;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Wrappers;

namespace Polars.Integration.Tests;

/// <summary>Liquid clustering between the kernel and Spark (delta-spark 4.1).</summary>
public class SparkClusteringTests
{
    private static void InRoot(Action<string> body)
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_cluster_{Guid.NewGuid():N}");
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

    private static Snapshot Load(string table) => Snapshot.Load(new LocalFileSystem(table));

    private static List<DeltaAction> Commit(string table, long version) =>
        ActionParser.ParseCommit(File.ReadAllBytes(Path.Combine(table, "_delta_log", $"{version:D20}.json")));

    /// <summary>An operation parameter of a commit's commitInfo.</summary>
    private static string Param(CommitInfoAction info, string name)
    {
        using JsonDocument doc = JsonDocument.Parse(info.Json);
        JsonElement root = doc.RootElement.TryGetProperty("commitInfo", out JsonElement inner) ? inner : doc.RootElement;
        JsonElement value = root.GetProperty("operationParameters").GetProperty(name);
        return value.ValueKind == JsonValueKind.String ? value.GetString()! : value.GetRawText();
    }

    private const string Insert =
        "INSERT INTO {table} SELECT id, CAST(id % 5 AS STRING), id % 7, named_struct('x', id) FROM RANGE({0}, {1})";

    [SparkTheory]
    [InlineData("none")]
    [InlineData("name")]
    [Trait("DeltaKernel", "Spark")]
    public void OnSparksClusteredTable_KernelWritesAndOptimizes_AndSparkFindsItClustered(string mapping)
    {
        InRoot(root =>
        {
            string byKernel = Path.Combine(root, "kernel"), bySpark = Path.Combine(root, "spark"), extra = Path.Combine(root, "extra");
            string[] tables = [byKernel, bySpark];
            foreach (string table in tables)
            {
                LocalSpark.Run("sql", table,
                    "CREATE TABLE {table} (id BIGINT, a STRING, b BIGINT, s STRUCT<x: BIGINT>) USING delta " +
                    $"TBLPROPERTIES ('delta.columnMapping.mode'='{mapping}') CLUSTER BY (a, s.x)");
                for (int i = 0; i < 3; i++) LocalSpark.Run("sql", table, Insert.Replace("{0}", $"{i * 10}").Replace("{1}", $"{i * 10 + 10}"));
            }
            LocalSpark.Run("sql", extra, "CREATE TABLE {table} (id BIGINT, a STRING, b BIGINT, s STRUCT<x: BIGINT>) USING delta");
            LocalSpark.Run("sql", extra, Insert.Replace("{0}", "100").Replace("{1}", "110"));

            // Appends and row-level operations on a clustered table write unclustered files, as Spark's do.
            DeltaKernelWrite.Write(byKernel, DeltaKernelScan.Scan(extra), KernelWriteMode.Append);
            LocalSpark.Run("sql", bySpark, $"INSERT INTO {{table}} SELECT * FROM delta.`{extra}`");
            DeltaKernelUpdate.Update(byKernel, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(1L)), [("b", PolarsWrapper.Lit(99L))]);
            LocalSpark.Run("sql", bySpark, "UPDATE {table} SET b = 99 WHERE id = 1");
            DeltaKernelDelete.Delete(byKernel, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(2L)));
            LocalSpark.Run("sql", bySpark, "DELETE FROM {table} WHERE id = 2");
            Assert.All(Load(byKernel).Files, f => Assert.Null(f.ClusteringProvider));

            // The kernel's OPTIMIZE clusters every file into one cube, recorded as Spark records it.
            long before = Load(byKernel).Version;
            DeltaOptimizeResult result = DeltaKernelOptimize.Optimize(byKernel);
            Assert.Equal(before + 1, result.Version);
            Snapshot optimized = Load(byKernel);
            Assert.All(optimized.Files, f =>
            {
                Assert.Equal("liquid", f.ClusteringProvider);
                Assert.Equal("""["a","s.x"]""", f.Tags![Clustering.CubeColumnsTag]);
            });
            Assert.Single(optimized.Files.Select(f => f.Tags![Clustering.CubeIdTag]).Distinct());
            CommitInfoAction info = Commit(byKernel, result.Version!.Value).OfType<CommitInfoAction>().Single();
            Assert.Equal("OPTIMIZE", info.Operation);
            Assert.Equal("""["a","s.x"]""", Param(info, "clusterBy"));
            Assert.Equal("false", Param(info, "isFull"));

            // Nothing new: neither the kernel nor Spark clusters again (Spark sees the kernel's files as clustered).
            Assert.Null(DeltaKernelOptimize.Optimize(byKernel).Version);
            LocalSpark.Run("sql", byKernel, "OPTIMIZE {table}");
            Assert.Equal(optimized.Version, Load(byKernel).Version);

            // New data is merged into the small cube, by the kernel or by Spark alike.
            LocalSpark.Run("sql", byKernel, Insert.Replace("{0}", "200").Replace("{1}", "205"));
            LocalSpark.Run("sql", bySpark, Insert.Replace("{0}", "200").Replace("{1}", "205"));
            Assert.NotNull(DeltaKernelOptimize.Optimize(byKernel).Version);
            Assert.All(Load(byKernel).Files, f => Assert.Equal("liquid", f.ClusteringProvider));
            LocalSpark.Run("sql", bySpark, "OPTIMIZE {table}");

            Assert.Equal(Rows(bySpark), Rows(byKernel));
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void KernelClusteredTables_AreClusteredForSpark_AndClusterByChangesTheColumns()
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "table");
            using (var df = DataFrame.FromColumns(new { id = new long[] { 1, 2, 3 }, a = new[] { "x", "y", "z" }, b = new long[] { 3, 2, 1 } }))
                DeltaKernelWrite.Write(table, df.Lazy().CloneHandle(), KernelWriteMode.ErrorIfExists, clusterBy: ["a"]);
            Snapshot created = Load(table);
            Assert.Contains(Clustering.Feature, created.Protocol.WriterFeatures!);
            Assert.Contains(TableFeatures.DomainMetadata, created.Protocol.WriterFeatures!);
            Assert.Equal("""{"clusteringColumns":[["a"]],"domainName":"delta.clustering"}""", created.DomainMetadata[Clustering.Domain].Configuration);

            // Spark clusters by the kernel's columns.
            LocalSpark.Run("sql", table, "INSERT INTO {table} VALUES (4, 'w', 0)");
            LocalSpark.Run("sql", table, "OPTIMIZE {table}");
            Assert.All(Load(table).Files, f => Assert.Equal("""["a"]""", f.Tags![Clustering.CubeColumnsTag]));

            // New columns: a plain OPTIMIZE leaves the old cube, FULL reclusters it (as Spark).
            Assert.NotNull(DeltaKernelAlter.ClusterBy(table, ["b"]));
            CommitInfoAction alter = Commit(table, Load(table).Version).OfType<CommitInfoAction>().Single();
            Assert.Equal(("CLUSTER BY", "a", "b"),
                (alter.Operation, Param(alter, "oldClusteringColumns"), Param(alter, "newClusteringColumns")));
            Assert.Null(DeltaKernelOptimize.Optimize(table).Version);
            Assert.NotNull(DeltaKernelOptimize.Optimize(table, full: true).Version);
            Assert.All(Load(table).Files, f => Assert.Equal("""["b"]""", f.Tags![Clustering.CubeColumnsTag]));
            LocalSpark.Run("sql", table, "OPTIMIZE {table} FULL");
            Assert.Equal("""[["b"]]""", JsonDocument.Parse(Load(table).DomainMetadata[Clustering.Domain].Configuration).RootElement.GetProperty("clusteringColumns").GetRawText());

            // NONE: the domain names no columns, and OPTIMIZE has nothing to cluster.
            Assert.NotNull(DeltaKernelAlter.ClusterBy(table, []));
            Assert.Equal("""{"clusteringColumns":[],"domainName":"delta.clustering"}""", Load(table).DomainMetadata[Clustering.Domain].Configuration);
            Assert.Equal("""{"id": 1, "a": "x", "b": 3} {"id": 2, "a": "y", "b": 2} {"id": 3, "a": "z", "b": 1} {"id": 4, "a": "w", "b": 0}""", Rows(table));
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void ClusteringRules_FollowSparks()
    {
        InRoot(root =>
        {
            using var df = DataFrame.FromColumns(new { id = new long[] { 1 }, a = new[] { "x" }, flag = new[] { true }, c = new long[] { 1 }, d = new long[] { 1 }, e = new long[] { 1 } });
            PolarsException Fails(string[]? partitionBy, string[] clusterBy, Dictionary<string, string>? properties = null) =>
                Assert.Throws<PolarsException>(() => DeltaKernelWrite.Write(
                    Path.Combine(root, Guid.NewGuid().ToString("N")), df.Lazy().CloneHandle(), KernelWriteMode.ErrorIfExists, partitionBy,
                    configuration: properties, clusterBy: clusterBy));
            Assert.Contains("cannot both be specified", Fails(["a"], ["id"]).Message);
            Assert.Contains("up to 4", Fails(null, ["id", "a", "c", "d", "e"]).Message);
            Assert.Contains("data skipping", Fails(null, ["flag"]).Message);
            Assert.Contains("statistics", Fails(null, ["a"], new() { ["delta.dataSkippingNumIndexedCols"] = "1" }).Message);

            // ALTER on a partitioned table is refused; on an unclustered one it turns clustering on.
            string partitioned = Path.Combine(root, "partitioned"), plain = Path.Combine(root, "plain");
            DeltaKernelWrite.Write(partitioned, df.Lazy().CloneHandle(), KernelWriteMode.ErrorIfExists, ["a"]);
            Assert.Contains("partitioned table", Assert.Throws<PolarsException>(() => DeltaKernelAlter.ClusterBy(partitioned, ["id"])).Message);
            DeltaKernelWrite.Write(plain, df.Lazy().CloneHandle(), KernelWriteMode.ErrorIfExists);
            Assert.Null(DeltaKernelAlter.ClusterBy(plain, [])); // NONE on an unclustered table: nothing to do
            Assert.NotNull(DeltaKernelAlter.ClusterBy(plain, ["id"]));
            Assert.Contains("ZORDER BY", Assert.Throws<PolarsException>(() => DeltaKernelOptimize.Optimize(plain, zOrderColumns: ["a"])).Message);
            LocalSpark.Run("sql", plain, "OPTIMIZE {table}");
            Assert.All(Load(plain).Files, f => Assert.Equal("""["id"]""", f.Tags![Clustering.CubeColumnsTag]));
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void ClusteredFiles_SkipFiles_AndKeepNearTheTargetSize_WithSkewedKeys()
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "big");
            const int n = 200_000;
            var rng = new Random(7);
            // a: uniform, k: skewed (90% one value).
            using (var df = DataFrame.FromColumns(new
                   {
                       id = Enumerable.Range(0, n).Select(i => (long)i).ToArray(),
                       a = Enumerable.Range(0, n).Select(_ => (long)rng.Next(1_000_000)).ToArray(),
                       k = Enumerable.Range(0, n).Select(i => i % 10 == 0 ? (long)i : 7L).ToArray(),
                   }))
            {
                // Written in insertion order: every file spans the whole range of a.
                DeltaKernelWrite.Write(table, df.Lazy().CloneHandle(), KernelWriteMode.ErrorIfExists, clusterBy: ["a", "k"],
                    options: KernelWriteOptions.Default with { MaxRowsPerFile = 20_000 });
            }
            int Scanned(Predicate filter)
            {
                int files = -1;
                using LazyFrameHandle lf = DeltaKernelScan.ScanPath(table, null, null, filter, null, count => files = count);
                return files;
            }
            Predicate narrow = Predicate.Lt(new ColumnPath("a"), 50_000L);
            int before = Scanned(narrow);
            Assert.Equal(Load(table).Files.Count, before);

            long size = Load(table).Files.Sum(f => f.Size);
            DeltaOptimizeResult result = DeltaKernelOptimize.Optimize(table, targetFileSize: size / 10);
            Assert.NotNull(result.Version);
            Snapshot clustered = Load(table);
            int after = Scanned(narrow);
            Assert.True(after < clustered.Files.Count / 2, $"{after} of {clustered.Files.Count} files scanned for a < 50000");
            // No file far above the target, despite the skewed second column.
            Assert.All(clustered.Files, f => Assert.True(f.Size < size / 10 * 3, $"{f.Path}: {f.Size} bytes, target {size / 10}"));
            Assert.Equal(n, clustered.Files.Sum(f => f.ParsedStats!.NumRecords));
        });
    }
}
