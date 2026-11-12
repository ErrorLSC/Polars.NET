using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.CSharp;
using Polars.NET.Core.Delta;
using Delta = Polars.CSharp.Delta;

namespace Polars.Integration.Tests;

/// <summary>Dropping table features (Spark's ALTER TABLE … DROP FEATURE) and tables Spark dropped features on.</summary>
public class SparkDropFeatureTests
{
    private static void InRoot(Action<string> body)
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_drop_{Guid.NewGuid():N}");
        try
        {
            body(root);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    private static string SparkRows(string table) =>
        string.Join(" ", LocalSpark.Run("read", table)[0].RootElement.GetProperty("rows").EnumerateArray().Select(r => r.GetRawText()));

    private static string[] LogNames(string table) =>
        [.. Directory.EnumerateFiles(Path.Combine(table, "_delta_log")).Select(Path.GetFileName).Order(StringComparer.Ordinal)!];

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void TablesSparkDroppedAFeatureOn_AreWrittenAndCleanedUpUnderCheckpointProtection()
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "table");
            LocalSpark.Run("sql", table,
                "CREATE TABLE {table} (id INT, v STRING) USING delta TBLPROPERTIES ('delta.enableDeletionVectors'='true', " +
                "'delta.feature.vacuumProtocolCheck'='supported', 'delta.checkpointInterval'='5')");
            LocalSpark.Run("sql", table, "INSERT INTO {table} VALUES (1, 'a'), (2, 'b'), (3, 'c')");
            LocalSpark.Run("sql", table, "DELETE FROM {table} WHERE id = 2");
            LocalSpark.Run("sql", table, "ALTER TABLE {table} DROP FEATURE deletionVectors");
            var fs = new LocalFileSystem(table);
            Snapshot dropped = Snapshot.Load(fs);
            long boundary = TableFeatures.CheckpointProtectionBoundary(dropped.Protocol, dropped.Metadata)
                ?? throw new InvalidOperationException("Spark left no checkpoint protection.");
            Assert.Equal(dropped.Version, boundary);

            // The kernel writes the table: checkpointProtection is a writer feature it supports.
            using (DataFrame more = DataFrame.ReadDelta(table).Filter(Polars.CSharp.Polars.Col("id") == 1)
                       .WithColumns(Polars.CSharp.Polars.Lit(4).Alias("id")))
                more.WriteDelta(table);
            Delta.Update(table, Polars.CSharp.Polars.Col("id") == 3, new Dictionary<string, Expr> { ["v"] = Polars.CSharp.Polars.Lit("c2") });
            Delta.Optimize(table);
            Delta.Vacuum(table, retentionHours: 168);
            Assert.Equal("""{"id": 1, "v": "a"} {"id": 3, "v": "c2"} {"id": 4, "v": "a"}""", SparkRows(table));

            // Log cleanup a month later: history before the boundary goes only all at once, once a checkpoint at or
            // after the boundary is past the retention.
            Checkpoints.Write(fs, Snapshot.Load(fs, parquet: new EngineParquetReader(table)));
            IReadOnlyList<string> deleted = LogCleanup.Run(fs, Snapshot.Load(fs, parquet: new EngineParquetReader(table)), DateTimeOffset.UtcNow.AddDays(31));
            Assert.NotEmpty(deleted);
            Assert.DoesNotContain(LogNames(table), n => n.EndsWith(".json", StringComparison.Ordinal) && long.Parse(n[..20]) < boundary);
            Assert.Equal("""{"id": 1, "v": "a"} {"id": 3, "v": "c2"} {"id": 4, "v": "a"}""", SparkRows(table));

            // Spark goes on with the table the kernel wrote: it drops another feature.
            LocalSpark.Run("sql", table, "ALTER TABLE {table} DROP FEATURE vacuumProtocolCheck");
            Snapshot after = Snapshot.Load(fs, parquet: new EngineParquetReader(table)); // the log now starts at a checkpoint
            Assert.Null(after.Protocol.ReaderFeatures);
            Assert.Equal(["appendOnly", "checkpointProtection", "invariants"], after.Protocol.WriterFeatures!.Order(StringComparer.Ordinal));
            using DataFrame read = DataFrame.ReadDelta(table).Sort("id");
            Assert.Equal([1, 3, 4], read["id"].ToArray<int>());
        });
    }

    /// <summary>
    /// The commits from a version on, as comparable text: operation and parameters, the kinds of actions, the
    /// protocol (feature lists as sets), the configuration (random names masked), and whether the version has a checkpoint.
    /// </summary>
    private static List<string> Commits(string table, long from)
    {
        List<string> commits = [];
        for (long v = from; File.Exists(Path.Combine(table, "_delta_log", $"{v:D20}.json")); v++)
        {
            List<string> parts = [];
            List<string> kinds = [];
            foreach (string line in File.ReadAllLines(Path.Combine(table, "_delta_log", $"{v:D20}.json")))
            {
                using JsonDocument doc = JsonDocument.Parse(line);
                JsonProperty action = doc.RootElement.EnumerateObject().First();
                switch (action.Name)
                {
                    case "commitInfo":
                        parts.Insert(0, action.Value.GetProperty("operation").GetString() + " " + action.Value.GetProperty("operationParameters").GetRawText());
                        break;
                    case "protocol":
                        JsonElement p = action.Value;
                        string Set(string name) => p.TryGetProperty(name, out JsonElement list)
                            ? string.Join(",", list.EnumerateArray().Select(e => e.GetString()).Order(StringComparer.Ordinal)) : "-";
                        parts.Add($"protocol {p.GetProperty("minReaderVersion")}/{p.GetProperty("minWriterVersion")} r[{Set("readerFeatures")}] w[{Set("writerFeatures")}]");
                        break;
                    case "metaData":
                        parts.Add("conf " + string.Join(",", action.Value.GetProperty("configuration").EnumerateObject()
                            .Select(c => $"{c.Name}={(c.Name.Contains("materialized", StringComparison.Ordinal) || c.Name.Contains("Enablement", StringComparison.Ordinal) ? "*" : c.Value.GetString())}")
                            .Order(StringComparer.Ordinal)));
                        parts.Add("schema " + action.Value.GetProperty("schemaString").GetString());
                        break;
                    case "domainMetadata":
                        kinds.Add($"domain {action.Value.GetProperty("domain").GetString()} removed={action.Value.GetProperty("removed").GetBoolean()}");
                        break;
                    default:
                        kinds.Add(action.Name);
                        break;
                }
            }
            bool checkpoint = File.Exists(Path.Combine(table, "_delta_log", $"{v:D20}.checkpoint.parquet"));
            // The operation first; the actions in any order (their order within a commit means nothing).
            commits.Add($"v{v}: {parts[0]} | {string.Join(" | ", parts.Skip(1).Order(StringComparer.Ordinal))} | " +
                $"{string.Join(",", kinds.Order(StringComparer.Ordinal))}{(checkpoint ? " +checkpoint" : "")}");
        }
        return commits;
    }

    [SparkTheory]
    [InlineData("vacuumProtocolCheck", "'delta.feature.vacuumProtocolCheck'='supported'")]
    [InlineData("v2Checkpoint", "'delta.checkpointPolicy'='v2'")]
    [InlineData("inCommitTimestamp", "'delta.enableInCommitTimestamps'='true'")]
    [InlineData("rowTracking", "'delta.enableRowTracking'='true'")]
    [InlineData("domainMetadata", "'delta.feature.domainMetadata'='supported'")]
    [InlineData("checkConstraints", "'delta.feature.checkConstraints'='supported'")]
    [Trait("DeltaKernel", "Spark")]
    public void DropFeature_CommitsAsSpark_AndSparkGoesOnWithTheTable(string feature, string properties)
    {
        InRoot(root =>
        {
            string kernel = Path.Combine(root, "kernel"), spark = Path.Combine(root, "spark");
            string[] tables = [kernel, spark];
            foreach (string table in tables)
            {
                LocalSpark.Run("sql", table, $"CREATE TABLE {{table}} (id INT, v STRING) USING delta TBLPROPERTIES ({properties})");
                LocalSpark.Run("sql", table, "INSERT INTO {table} VALUES (1, 'a'), (2, 'b')");
            }
            long from = 2;
            Delta.DropFeature(kernel, feature);
            LocalSpark.Run("sql", spark, $"ALTER TABLE {{table}} DROP FEATURE {feature}");

            List<string> bySpark = Commits(spark, from), byKernel = Commits(kernel, from);
            Assert.True(bySpark.SequenceEqual(byKernel), $"Spark:\n{string.Join("\n", bySpark)}\nkernel:\n{string.Join("\n", byKernel)}");

            // Spark reads and writes the table the kernel dropped the feature from.
            LocalSpark.Run("sql", kernel, "INSERT INTO {table} VALUES (3, 'c')");
            Assert.Equal("""{"id": 1, "v": "a"} {"id": 2, "v": "b"} {"id": 3, "v": "c"}""", SparkRows(kernel));
            // A second drop is refused, as Spark refuses it.
            Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(() => Delta.DropFeature(kernel, feature));
        });
    }

    /// <summary>The rows of a table with their row ids and row commit versions, as Spark reads them.</summary>
    private static string SparkRowIds(string table) =>
        string.Join(" ", LocalSpark.Run("row_ids", table)[0].RootElement.GetProperty("rows").EnumerateArray().Select(r => r.GetRawText()));

    [SparkTheory]
    // Deletion vectors under row tracking: a file with a vector, a file removed after its vector (whose vector file only
    // the removes refer to: the tombstones commit), a file without one.
    [InlineData("deletionVectors", "(id INT, v STRING) USING delta", "'delta.enableDeletionVectors'='true', 'delta.enableRowTracking'='true'",
        "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS x(id, v)",
        "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (4, 'd'), (5, 'e') AS x(id, v)",
        "INSERT INTO {table} VALUES (6, 'f')",
        "DELETE FROM {table} WHERE id = 2",
        "DELETE FROM {table} WHERE id = 4",
        "DELETE FROM {table} WHERE id = 5")]
    // Type widening: each file but the last holds older types; the one before it only an older short (16-bit int).
    [InlineData("typeWidening", "(id INT, s SHORT, i INT, f FLOAT, d DECIMAL(5,2), a ARRAY<SHORT>, st STRUCT<x: SHORT>) USING delta", "'delta.enableTypeWidening'='true'",
        "INSERT INTO {table} VALUES (1, 1, 1, 1.5, 1.25, array(1), named_struct('x', 1))",
        "ALTER TABLE {table} ALTER COLUMN d TYPE DECIMAL(7,2)",
        "INSERT INTO {table} VALUES (2, 2, 2, 2.5, 2.25, array(2), named_struct('x', 2))",
        "ALTER TABLE {table} ALTER COLUMN f TYPE DOUBLE",
        "INSERT INTO {table} VALUES (3, 3, 3, 3.5, 3.25, array(3), named_struct('x', 3))",
        "ALTER TABLE {table} ALTER COLUMN i TYPE BIGINT",
        "INSERT INTO {table} VALUES (4, 4, 4, 4.5, 4.25, array(4), named_struct('x', 4))",
        "ALTER TABLE {table} ALTER COLUMN st.x TYPE INT",
        "ALTER TABLE {table} ALTER COLUMN a.element TYPE INT",
        "INSERT INTO {table} VALUES (5, 5, 5, 5.5, 5.25, array(5), named_struct('x', 5))",
        "ALTER TABLE {table} ALTER COLUMN s TYPE INT",
        "INSERT INTO {table} VALUES (6, 70000, 5000000000, 6.5, 12345.25, array(70000), named_struct('x', 70000))")]
    // Column mapping: renamed and dropped columns (a file still holding the dropped one), nested fields.
    [InlineData("columnMapping", "(id INT, v STRING, st STRUCT<x: INT>, keep STRUCT<y: INT>) USING delta", "'delta.columnMapping.mode'='name'",
        "INSERT INTO {table} VALUES (1, 'a', named_struct('x', 1), named_struct('y', 1))",
        "ALTER TABLE {table} RENAME COLUMN v TO w",
        "ALTER TABLE {table} RENAME COLUMN keep.y TO z",
        "ALTER TABLE {table} DROP COLUMN st",
        "INSERT INTO {table} VALUES (2, 'b', named_struct('z', 2))")]
    // … partitioned by a renamed column (the partition directories and values take the logical name).
    [InlineData("columnMapping", "(id INT, p STRING) USING delta PARTITIONED BY (p)", "'delta.columnMapping.mode'='name'",
        "INSERT INTO {table} VALUES (1, 'x'), (2, 'y')",
        "ALTER TABLE {table} RENAME COLUMN p TO q")]
    // … in id mode.
    [InlineData("columnMapping", "(id INT, v STRING) USING delta", "'delta.columnMapping.mode'='id'",
        "INSERT INTO {table} VALUES (1, 'x')")]
    // … under row tracking with a deletion vector (its deleted rows are left out of the rewrite, the rest keep their ids).
    [InlineData("columnMapping", "(id INT, v STRING) USING delta",
        "'delta.columnMapping.mode'='name', 'delta.enableRowTracking'='true', 'delta.enableDeletionVectors'='true'",
        "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS x(id, v)",
        "INSERT INTO {table} VALUES (4, 'd')",
        "DELETE FROM {table} WHERE id = 2")]
    [Trait("DeltaKernel", "Spark")]
    public void DropFeature_RewritesDataAsSpark(string feature, string definition, string properties, params string[] setup)
    {
        InRoot(root =>
        {
            string kernel = Path.Combine(root, "kernel"), spark = Path.Combine(root, "spark");
            string[] tables = [kernel, spark];
            foreach (string table in tables)
            {
                LocalSpark.Run("sql", table, $"CREATE TABLE {{table}} {definition} TBLPROPERTIES ({properties})");
                foreach (string statement in setup) LocalSpark.Run("sql", table, statement);
            }
            if (feature == "typeWidening")
            {
                // The footers tell the older types apart: the first file holds a short and a decimal(5,2).
                var fs = new LocalFileSystem(kernel);
                AddAction first = Snapshot.Load(fs).Files.MinBy(f => f.ModificationTime)!;
                IReadOnlyList<Kaminari.Seseragi.Lakehouse.Parquet.ParquetSchemaElement> footer =
                    Kaminari.Seseragi.Lakehouse.Parquet.ParquetFooters.Read(fs, [first.DecodedPath])[0].Footer.Schema;
                Assert.Equal(16, footer.Single(e => e.Name == "s").IntBitWidth);
                Assert.Equal(5, footer.Single(e => e.Name == "d").DecimalPrecision);
            }
            long from = setup.Length + 1;
            bool tracked = properties.Contains("enableRowTracking", StringComparison.Ordinal);
            string Rows(string table) => tracked ? SparkRowIds(table) : SparkRows(table);
            string before = Rows(spark);
            Delta.DropFeature(kernel, feature);
            LocalSpark.Run("sql", spark, $"ALTER TABLE {{table}} DROP FEATURE {feature}");

            List<string> bySpark = Commits(spark, from), byKernel = Commits(kernel, from);
            Assert.True(bySpark.SequenceEqual(byKernel), $"Spark:\n{string.Join("\n", bySpark)}\nkernel:\n{string.Join("\n", byKernel)}");
            // The rows (with their row ids under row tracking) are the same as before, read by Spark from both tables.
            Assert.Equal(before, Rows(spark));
            Assert.Equal(before, Rows(kernel));
            Snapshot dropped = Snapshot.Load(new LocalFileSystem(kernel));
            if (feature != "columnMapping" || tracked) Assert.DoesNotContain(dropped.Files, f => f.DeletionVector is not null);
            if (feature == "columnMapping")
            {
                // The files hold the columns under their logical names, partitions under logical directories.
                var fs = new LocalFileSystem(kernel);
                foreach (var (_, footer) in Kaminari.Seseragi.Lakehouse.Parquet.ParquetFooters.Read(fs, [.. dropped.Files.Select(f => f.DecodedPath)]))
                    Assert.DoesNotContain(footer.Schema, e => e.Name.StartsWith("col-", StringComparison.Ordinal) || e.FieldId is not null);
                Assert.DoesNotContain(dropped.Files, f => f.Stats!.Contains("\"col-", StringComparison.Ordinal));
                Assert.All(dropped.Files, f => Assert.Equal(dropped.PartitionColumns.Select(c => $"{c}="), f.DecodedPath.Split('/')[..^1].Select(d => d[..(d.IndexOf('=') + 1)])));
            }
            // Spark writes the table on.
            LocalSpark.Run("sql", kernel, "INSERT INTO {table} (id) VALUES (7)");
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void DropColumnMapping_OnAClusteredTable_NamesTheClusteringColumnsByTheirLogicalNames()
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "table");
            LocalSpark.Run("sql", table, "CREATE TABLE {table} (id INT, v STRING) USING delta CLUSTER BY (v) TBLPROPERTIES ('delta.columnMapping.mode'='name')");
            LocalSpark.Run("sql", table, "INSERT INTO {table} VALUES (1, 'x'), (2, 'y')");
            LocalSpark.Run("sql", table, "OPTIMIZE {table}");
            LocalSpark.Run("sql", table, "ALTER TABLE {table} RENAME COLUMN v TO w");

            Delta.DropFeature(table, "columnMapping");

            // Spark leaves the domain naming the old physical column, after which its own OPTIMIZE fails; the kernel
            // names the column as the table now holds it, and both go on clustering the table.
            Snapshot dropped = Snapshot.Load(new LocalFileSystem(table), parquet: new EngineParquetReader(table));
            Assert.Equal([["w"]], Clustering.PhysicalColumns(dropped.DomainMetadata)!);
            LocalSpark.Run("sql", table, "INSERT INTO {table} VALUES (3, 'z')");
            LocalSpark.Run("sql", table, "OPTIMIZE {table}");
            Assert.Equal("""["w"]""", LocalSpark.Run("query", table, "DESCRIBE DETAIL {table}")[0].RootElement
                .GetProperty("rows")[0].GetProperty("clusteringColumns").GetRawText().Replace(" ", ""));
            Delta.Optimize(table);
            Assert.Equal("""{"id": 1, "w": "x"} {"id": 2, "w": "y"} {"id": 3, "w": "z"}""", SparkRows(table));
        });
    }

    [SparkTheory]
    // Names a data file cannot hold without column mapping (Spark refuses them too).
    [InlineData("(id INT, `a b` STRING) USING delta", "'delta.columnMapping.mode'='name'", "columnMapping", "INSERT INTO {table} VALUES (1, 'x')")]
    // A rewrite that would write map columns (which the engine cannot write yet as Spark reads them) is refused
    // before the first commit, not after the deletion vectors were turned off.
    [InlineData("(id INT, m MAP<STRING, INT>) USING delta", "'delta.enableDeletionVectors'='true'", "deletionVectors",
        "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (1, map('a', 1)), (2, map('b', 2)) AS x(id, m)",
        "DELETE FROM {table} WHERE id = 2")]
    [Trait("DeltaKernel", "Spark")]
    public void DropFeature_RefusesBeforeAnyCommit(string definition, string properties, string feature, params string[] setup)
    {
        InRoot(root =>
        {
            string table = Path.Combine(root, "table");
            LocalSpark.Run("sql", table, $"CREATE TABLE {{table}} {definition} TBLPROPERTIES ({properties})");
            foreach (string statement in setup) LocalSpark.Run("sql", table, statement);
            string[] before = LogNames(table);
            Assert.Throws<Polars.NET.Core.Exceptions.PolarsException>(() => Delta.DropFeature(table, feature));
            Assert.Equal(before, LogNames(table));
        });
    }
}
