using System.Text.RegularExpressions;
using System.Text.Json;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;
using Polars.CSharp;
using Polars.NET.Core.Delta;
using Delta = Polars.CSharp.Delta;

namespace Polars.Integration.Tests;

/// <summary>Dropping table features (Spark's ALTER TABLE … DROP FEATURE) and tables Spark dropped features on.</summary>
public partial class SparkDropFeatureTests
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
            Checkpoints.Write(fs, Snapshot.Load(fs));
            IReadOnlyList<string> deleted = LogCleanup.Run(fs, Snapshot.Load(fs), DateTimeOffset.UtcNow.AddDays(31));
            Assert.NotEmpty(deleted);
            Assert.DoesNotContain(LogNames(table), n => n.EndsWith(".json", StringComparison.Ordinal) && long.Parse(n[..20]) < boundary);
            Assert.Equal("""{"id": 1, "v": "a"} {"id": 3, "v": "c2"} {"id": 4, "v": "a"}""", SparkRows(table));

            // Spark goes on with the table the kernel wrote: it drops another feature.
            LocalSpark.Run("sql", table, "ALTER TABLE {table} DROP FEATURE vacuumProtocolCheck");
            Snapshot after = Snapshot.Load(fs); // the log now starts at a checkpoint
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
                        // Physical names under column mapping are random per table.
                        parts.Add("schema " + PhysicalName().Replace(action.Value.GetProperty("schemaString").GetString()!, "col-*"));
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
    // … with a dropped column: the purge also rewrites the file that still holds it (with and without a vector elsewhere).
    [InlineData("deletionVectors", "(id INT, v STRING, w STRING) USING delta", "'delta.enableDeletionVectors'='true', 'delta.columnMapping.mode'='name'",
        "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a', 'x'), (2, 'b', 'y') AS x(id, v, w)",
        "ALTER TABLE {table} DROP COLUMN v",
        "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (3, 'z'), (4, 'q') AS x(id, w)",
        "DELETE FROM {table} WHERE id = 3")]
    [InlineData("deletionVectors", "(id INT, v STRING, w STRING) USING delta", "'delta.enableDeletionVectors'='true', 'delta.columnMapping.mode'='name'",
        "INSERT INTO {table} VALUES (1, 'a', 'x')",
        "ALTER TABLE {table} DROP COLUMN v",
        "INSERT INTO {table} VALUES (2, 'y')")]
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
                IReadOnlyList<Kaminari.Seseragi.Files.Parquet.ParquetSchemaElement> footer =
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

    // ------------------------------------------------------------------ TRUNCATE HISTORY

    /// <summary>Makes every log file look 25 hours old (later versions a little younger), as the retention needs.</summary>
    private static void Age(string table)
    {
        DateTime old = DateTime.UtcNow.AddHours(-25);
        foreach (string file in Directory.EnumerateFiles(Path.Combine(table, "_delta_log")))
        {
            string name = Path.GetFileName(file);
            long version = name.Length >= 20 && long.TryParse(name[..20], out long v) ? v : 0;
            File.SetLastWriteTimeUtc(file, old.AddSeconds(version));
        }
    }

    /// <summary>The protocol's features and the configuration the last commit (the drop) set; the log may start at a checkpoint.</summary>
    private static (string[] Features, string[] Configuration) Dropped(string table)
    {
        string last = Directory.EnumerateFiles(Path.Combine(table, "_delta_log"), "*.json").Order(StringComparer.Ordinal).Last();
        string[] features = [], configuration = [];
        foreach (string line in File.ReadAllLines(last))
        {
            using JsonDocument doc = JsonDocument.Parse(line);
            if (doc.RootElement.TryGetProperty("protocol", out JsonElement p))
                features = [.. ((string[])["readerFeatures", "writerFeatures"]).SelectMany(k => p.TryGetProperty(k, out JsonElement l) ? l.EnumerateArray().Select(e => e.GetString()!) : [])];
            if (doc.RootElement.TryGetProperty("metaData", out JsonElement m))
                configuration = [.. m.GetProperty("configuration").EnumerateObject().Select(c => c.Name)];
        }
        return (features, configuration);
    }

    /// <summary>The commits and checkpoints left in the log (Spark's .crc files aside).</summary>
    private static string[] LogLeft(string table) =>
        [.. LogNames(table)
            .Where(n => !n.StartsWith('.') && !n.EndsWith(".crc", StringComparison.Ordinal)
                && (n.EndsWith(".json", StringComparison.Ordinal) || n.Contains(".checkpoint.", StringComparison.Ordinal)))
            .Select(n => CheckpointId().Replace(n, ".checkpoint.*."))]; // V2 checkpoints are named by a random id

    [GeneratedRegex(@"\.checkpoint\.[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}\.")]
    private static partial Regex CheckpointId();

    /// <summary>
    /// DROP FEATURE … TRUNCATE HISTORY run by the kernel on one table and by Spark on the other, after the same
    /// setup, run after run until it succeeds (the log aged between runs): each run fails or succeeds for both, with
    /// the same commits and the same log left.
    /// </summary>
    private static void TruncateBoth(string kernel, string spark, string feature, int expectedRuns, params string[] sparkErrors)
    {
        for (int run = 1; ; run++)
        {
            long from = LogNames(spark).Where(n => n.EndsWith(".json", StringComparison.Ordinal)).Select(n => long.Parse(n[..20])).Max() + 1;
            Exception? byKernel = Record.Exception(() => Delta.DropFeature(kernel, feature, truncateHistory: true));
            Exception? bySpark = Record.Exception(() => LocalSpark.Run("sql", spark, $"ALTER TABLE {{table}} DROP FEATURE {feature} TRUNCATE HISTORY"));
            Assert.True((byKernel is null) == (bySpark is null), $"run {run}: kernel {byKernel?.Message ?? "succeeded"}; Spark {bySpark?.Message ?? "succeeded"}");
            if (bySpark is not null)
            {
                Assert.Contains(sparkErrors[run - 1], bySpark.Message);
                // The kernel fails for the same reason.
                Assert.Contains(sparkErrors[run - 1] switch
                {
                    "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD" => "partially successful",
                    "DELTA_FEATURE_DROP_HISTORICAL_VERSIONS_EXIST" => "historical versions that use the feature",
                    "DELTA_FEATURE_DROP_CHECKPOINT_PROTECTION_WAIT_FOR_RETENTION_PERIOD" => "CheckpointProtection cannot be dropped",
                    var other => other,
                }, byKernel!.Message);
            }
            List<string> sparkCommits = Commits(spark, from), kernelCommits = Commits(kernel, from);
            Assert.True(sparkCommits.SequenceEqual(kernelCommits), $"run {run}\nSpark:\n{string.Join("\n", sparkCommits)}\nkernel:\n{string.Join("\n", kernelCommits)}");
            Assert.Equal(LogLeft(spark), LogLeft(kernel));
            if (byKernel is null)
            {
                Assert.Equal(expectedRuns, run);
                return;
            }
            Assert.True(run < expectedRuns, $"run {run} failed: {byKernel.Message}");
            Age(kernel);
            Age(spark);
        }
    }

    [SparkTheory]
    // Nothing to remove and no trace in history: dropped in one run.
    [InlineData("vacuumProtocolCheck", "(id INT) USING delta", "'delta.feature.vacuumProtocolCheck'='supported'", 1, "INSERT INTO {table} VALUES (1)")]
    // Deletion vectors: the first run removes them and asks to wait; a day later the history is cut and the feature goes.
    [InlineData("deletionVectors", "(id INT) USING delta", "'delta.enableDeletionVectors'='true'", 2,
        "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (1), (2), (3) AS x(id)", "DELETE FROM {table} WHERE id = 2")]
    // Already turned off and purged, but the young history still holds vectors: the first run says so, the second drops.
    [InlineData("deletionVectors", "(id INT) USING delta", "'delta.enableDeletionVectors'='true'", 2,
        "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (1), (2), (3) AS x(id)", "DELETE FROM {table} WHERE id = 2",
        "ALTER TABLE {table} UNSET TBLPROPERTIES ('delta.enableDeletionVectors')", "REORG TABLE {table} APPLY (PURGE)")]
    // Column mapping: the files are rewritten under logical names first.
    [InlineData("columnMapping", "(id INT, v STRING) USING delta", "'delta.columnMapping.mode'='name'", 2,
        "INSERT INTO {table} VALUES (1, 'a')", "ALTER TABLE {table} RENAME COLUMN v TO w")]
    // Type widening: the file in the older type is rewritten and the type changes leave the schema first.
    [InlineData("typeWidening", "(id INT, s SHORT) USING delta", "'delta.enableTypeWidening'='true'", 2,
        "INSERT INTO {table} VALUES (1, 1)", "ALTER TABLE {table} ALTER COLUMN s TYPE INT", "INSERT INTO {table} VALUES (2, 70000)")]
    // V2 checkpoints: the policy goes back to classic first (the history still holds v2 checkpoints and the policy).
    [InlineData("v2Checkpoint", "(id INT) USING delta", "'delta.checkpointPolicy'='v2', 'delta.checkpointInterval'='2'", 2,
        "INSERT INTO {table} VALUES (1)", "INSERT INTO {table} VALUES (2)", "INSERT INTO {table} VALUES (3)")]
    [Trait("DeltaKernel", "Spark")]
    public void TruncateHistory_DropsAReaderFeature_AsSpark(string feature, string definition, string properties, int runs, params string[] setup)
    {
        InRoot(root =>
        {
            string kernel = Path.Combine(root, "kernel"), spark = Path.Combine(root, "spark");
            foreach (string table in (string[])[kernel, spark])
            {
                LocalSpark.Run("sql", table, $"CREATE TABLE {{table}} {definition} TBLPROPERTIES ({properties})");
                foreach (string statement in setup) LocalSpark.Run("sql", table, statement);
            }
            string before = SparkRows(spark);
            bool tracesLeft = !setup.Any(s => s.Contains("REORG", StringComparison.Ordinal));
            TruncateBoth(kernel, spark, feature, runs,
                tracesLeft ? "DELTA_FEATURE_DROP_WAIT_FOR_RETENTION_PERIOD" : "DELTA_FEATURE_DROP_HISTORICAL_VERSIONS_EXIST");

            var (features, _) = Dropped(kernel);
            Assert.DoesNotContain(feature, features);
            Assert.DoesNotContain(TableFeatures.CheckpointProtection, features);
            Assert.Equal(before, SparkRows(spark));
            Assert.Equal(before, SparkRows(kernel));
            LocalSpark.Run("sql", kernel, "INSERT INTO {table} (id) VALUES (7)");
        });
    }

    [SparkTheory]
    // After a drop under checkpoint protection: the first run checkpoints and asks to wait; a day later the history
    // before the protected version is gone and the feature with it.
    [InlineData(2, "'delta.enableDeletionVectors'='true'",
        "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (1), (2), (3) AS x(id)", "DELETE FROM {table} WHERE id = 2",
        "ALTER TABLE {table} DROP FEATURE deletionVectors")]
    // Added by hand, nothing protected: the (absent) property is unset and the feature dropped in one run.
    [InlineData(1, "'delta.feature.checkpointProtection'='supported'", "INSERT INTO {table} VALUES (1)")]
    [Trait("DeltaKernel", "Spark")]
    public void TruncateHistory_DropsCheckpointProtection_AsSpark(int runs, string properties, params string[] setup)
    {
        InRoot(root =>
        {
            string kernel = Path.Combine(root, "kernel"), spark = Path.Combine(root, "spark");
            foreach (string table in (string[])[kernel, spark])
            {
                LocalSpark.Run("sql", table, $"CREATE TABLE {{table}} (id INT) USING delta TBLPROPERTIES ({properties})");
                foreach (string statement in setup) LocalSpark.Run("sql", table, statement);
            }
            string before = SparkRows(spark);
            TruncateBoth(kernel, spark, TableFeatures.CheckpointProtection, runs, "DELTA_FEATURE_DROP_CHECKPOINT_PROTECTION_WAIT_FOR_RETENTION_PERIOD");

            var (features, configuration) = Dropped(kernel);
            Assert.DoesNotContain(TableFeatures.CheckpointProtection, features);
            Assert.DoesNotContain(TableFeatures.RequireCheckpointProtectionBeforeVersionKey, configuration);
            Assert.Equal(before, SparkRows(kernel));
            LocalSpark.Run("sql", kernel, "INSERT INTO {table} VALUES (9)");
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void TruncateHistory_IsRefusedWhereSparkRefusesIt()
    {
        InRoot(root =>
        {
            string kernel = Path.Combine(root, "kernel"), spark = Path.Combine(root, "spark");
            foreach (string table in (string[])[kernel, spark])
                LocalSpark.Run("sql", table,
                    "CREATE TABLE {table} (id INT) USING delta TBLPROPERTIES ('delta.feature.domainMetadata'='supported', 'delta.feature.checkpointProtection'='supported')");

            Assert.Contains("does not require history truncation",
                Assert.ThrowsAny<Exception>(() => Delta.DropFeature(kernel, "domainMetadata", truncateHistory: true)).Message);
            Assert.Contains("DELTA_FEATURE_DROP_HISTORY_TRUNCATION_NOT_ALLOWED",
                Assert.ThrowsAny<Exception>(() => LocalSpark.Run("sql", spark, "ALTER TABLE {table} DROP FEATURE domainMetadata TRUNCATE HISTORY")).Message);
            Assert.Contains("truncating history",
                Assert.ThrowsAny<Exception>(() => Delta.DropFeature(kernel, "checkpointProtection")).Message);
            Assert.Contains("DELTA_FEATURE_CAN_ONLY_DROP_CHECKPOINT_PROTECTION_WITH_HISTORY_TRUNCATION",
                Assert.ThrowsAny<Exception>(() => LocalSpark.Run("sql", spark, "ALTER TABLE {table} DROP FEATURE checkpointProtection")).Message);
            Assert.Equal(LogNames(spark).Count(n => n.EndsWith(".json", StringComparison.Ordinal)), LogNames(kernel).Count(n => n.EndsWith(".json", StringComparison.Ordinal)));
        });
    }

    [GeneratedRegex("col-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")]
    private static partial Regex PhysicalName();

    /// <summary>The version each file of the log was added in, by path.</summary>
    private static Dictionary<string, long> AddedIn(string table)
    {
        Dictionary<string, long> added = [];
        for (long v = 0; File.Exists(Path.Combine(table, "_delta_log", $"{v:D20}.json")); v++)
            foreach (string line in File.ReadAllLines(Path.Combine(table, "_delta_log", $"{v:D20}.json")))
            {
                using JsonDocument doc = JsonDocument.Parse(line);
                if (doc.RootElement.TryGetProperty("add", out JsonElement add)) added[add.GetProperty("path").GetString()!] = v;
            }
        return added;
    }

    /// <summary>The files a commit removed, named by the version that added each (the same in both tables).</summary>
    private static long[] RemovedFilesAddedIn(string table, long version)
    {
        string commit = Path.Combine(table, "_delta_log", $"{version:D20}.json");
        if (!File.Exists(commit)) return [];
        Dictionary<string, long> added = AddedIn(table);
        return [.. File.ReadAllLines(commit)
            .Select(line => JsonDocument.Parse(line).RootElement)
            .Where(a => a.TryGetProperty("remove", out _))
            .Select(a => added[a.GetProperty("remove").GetProperty("path").GetString()!])
            .Order()];
    }

    [SparkTheory]
    // Dropped columns: top level, a struct field, a field of the structs in an array.
    [InlineData("(id INT, v STRING, w STRING)", "'delta.columnMapping.mode'='name'",
        "INSERT INTO {table} VALUES (1, 'a', 'x')", "ALTER TABLE {table} DROP COLUMN v", "INSERT INTO {table} VALUES (2, 'y')")]
    [InlineData("(id INT, st STRUCT<x: INT, y: INT>)", "'delta.columnMapping.mode'='name'",
        "INSERT INTO {table} VALUES (1, named_struct('x', 1, 'y', 1))", "ALTER TABLE {table} DROP COLUMN st.y",
        "INSERT INTO {table} VALUES (2, named_struct('x', 2))")]
    [InlineData("(id INT, a ARRAY<STRUCT<x: INT, y: INT>>)", "'delta.columnMapping.mode'='name'",
        "INSERT INTO {table} VALUES (1, array(named_struct('x', 1, 'y', 1)))", "ALTER TABLE {table} DROP COLUMN a.element.y",
        "INSERT INTO {table} VALUES (2, array(named_struct('x', 2)))")]
    // Files holding the materialized row tracking columns only: nothing to purge, no commit.
    [InlineData("(id INT, v STRING)", "'delta.enableRowTracking'='true', 'delta.enableDeletionVectors'='false'",
        "INSERT INTO {table} VALUES (1, 'a')", "INSERT INTO {table} VALUES (2, 'b')", "UPDATE {table} SET v = 'c' WHERE id = 2")]
    // Deletion vectors and a dropped column: a file with a vector, one holding the column, one with both, one clean;
    // in two partitions (packed per partition).
    [InlineData("(id INT, v STRING, p STRING) USING delta PARTITIONED BY (p)", "'delta.columnMapping.mode'='name', 'delta.enableDeletionVectors'='true'",
        "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a', 'p1'), (2, 'b', 'p1') AS x(id, v, p)",
        "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (3, 'c', 'p1'), (4, 'd', 'p2') AS x(id, v, p)",
        "DELETE FROM {table} WHERE id = 1",
        "ALTER TABLE {table} DROP COLUMN v",
        "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (5, 'p2'), (6, 'p2') AS x(id, p)",
        "DELETE FROM {table} WHERE id = 5",
        "INSERT INTO {table} VALUES (7, 'p1')")]
    [Trait("DeltaKernel", "Spark")]
    public void Purge_RewritesTheFilesSparkRewrites(string definition, string properties, params string[] setup)
    {
        InRoot(root =>
        {
            string kernel = Path.Combine(root, "kernel"), spark = Path.Combine(root, "spark");
            foreach (string table in (string[])[kernel, spark])
            {
                string create = definition.Contains("USING", StringComparison.Ordinal) ? definition : definition + " USING delta";
                LocalSpark.Run("sql", table, $"CREATE TABLE {{table}} {create} TBLPROPERTIES ({properties})");
                foreach (string statement in setup) LocalSpark.Run("sql", table, statement);
            }
            long from = setup.Length + 1;
            string before = SparkRows(spark);

            long purged = Delta.Purge(kernel);
            LocalSpark.Run("sql", spark, "REORG TABLE {table} APPLY (PURGE)");

            List<string> bySpark = Commits(spark, from), byKernel = Commits(kernel, from);
            Assert.True(bySpark.SequenceEqual(byKernel), $"Spark:\n{string.Join("\n", bySpark)}\nkernel:\n{string.Join("\n", byKernel)}");
            long[] removed = RemovedFilesAddedIn(spark, from);
            Assert.Equal(removed, RemovedFilesAddedIn(kernel, from));
            Assert.Equal(removed.Length, purged);
            Assert.Equal(before, SparkRows(spark));
            Assert.Equal(before, SparkRows(kernel));
            // No current file holds a dropped column or a deletion vector any more.
            var fs = new LocalFileSystem(kernel);
            Snapshot snapshot = Snapshot.Load(fs);
            string[] hidden = RowTracking.MaterializedColumns(snapshot.Metadata) is var (id, version) ? [id, version] : [];
            Assert.DoesNotContain(snapshot.Files, f => f.DeletionVector is not null);
            Assert.All(Kaminari.Seseragi.Lakehouse.Parquet.ParquetFooters.Read(fs, [.. snapshot.Files.Select(f => f.DecodedPath)]),
                f => Assert.False(FileTypes.HasExtraColumns(snapshot.Schema, hidden, f.Footer)));
            // Spark writes the table on.
            LocalSpark.Run("sql", kernel, "INSERT INTO {table} (id) VALUES (8)");
        });
    }

    [SparkTheory]
    [InlineData("p", "p1")]
    [InlineData("p", "p1", "q", "1")]
    [InlineData("p", null)]
    [InlineData("p", "zz")] // no file: no commit
    [Trait("DeltaKernel", "Spark")]
    public void Purge_OfSomePartitions_RewritesTheFilesSparkRewrites_AndRecordsItsWhere(params string?[] filter)
    {
        InRoot(root =>
        {
            string kernel = Path.Combine(root, "kernel"), spark = Path.Combine(root, "spark");
            foreach (string table in (string[])[kernel, spark])
            {
                LocalSpark.Run("sql", table,
                    "CREATE TABLE {table} (id INT, p STRING, q INT) USING delta PARTITIONED BY (p, q) TBLPROPERTIES ('delta.enableDeletionVectors'='true')");
                LocalSpark.Run("sql", table,
                    "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'p1', 1), (2, 'p1', 1), (3, 'p1', 2), (4, 'p1', 2), " +
                    "(5, 'p2', 1), (6, 'p2', 1), (7, NULL, 1), (8, NULL, 1) AS x(id, p, q)");
                LocalSpark.Run("sql", table, "DELETE FROM {table} WHERE id IN (1, 3, 5, 7)");
            }
            Dictionary<string, string?> filters = [];
            for (int i = 0; i < filter.Length; i += 2) filters[filter[i]!] = filter[i + 1];
            string where = string.Join(" AND ", filters.Select(f => f.Value is null ? $"{f.Key} IS NULL" : $"{f.Key} = '{f.Value}'"));
            const long From = 3;

            long purged = Delta.Purge(kernel, partitionFilters: filters);
            LocalSpark.Run("sql", spark, $"REORG TABLE {{table}} WHERE {where} APPLY (PURGE)");

            List<string> bySpark = Commits(spark, From), byKernel = Commits(kernel, From);
            Assert.True(bySpark.SequenceEqual(byKernel), $"Spark:\n{string.Join("\n", bySpark)}\nkernel:\n{string.Join("\n", byKernel)}");
            long[] removed = RemovedFilesAddedIn(spark, From);
            Assert.Equal(removed, RemovedFilesAddedIn(kernel, From));
            Assert.Equal(removed.Length, purged);
            Assert.Equal(SparkRows(spark), SparkRows(kernel));
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Purge_OfAClusteredTable_WritesANewCube_AsSpark()
    {
        InRoot(root =>
        {
            string kernel = Path.Combine(root, "kernel"), spark = Path.Combine(root, "spark");
            foreach (string table in (string[])[kernel, spark])
            {
                LocalSpark.Run("sql", table, "CREATE TABLE {table} (id INT, v STRING) USING delta CLUSTER BY (id) TBLPROPERTIES ('delta.enableDeletionVectors'='true')");
                LocalSpark.Run("sql", table, "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (1, 'a'), (2, 'b'), (3, 'c') AS x(id, v)");
                LocalSpark.Run("sql", table, "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (4, 'd'), (5, 'e') AS x(id, v)");
                LocalSpark.Run("sql", table, "OPTIMIZE {table}");
                LocalSpark.Run("sql", table, "INSERT INTO {table} SELECT /*+ COALESCE(1) */ * FROM VALUES (6, 'f'), (7, 'g') AS x(id, v)");
                LocalSpark.Run("sql", table, "DELETE FROM {table} WHERE id IN (2, 6)"); // a vector in the cube and in the new file
            }
            const long From = 6;

            Assert.Equal(2, Delta.Purge(kernel));
            LocalSpark.Run("sql", spark, "REORG TABLE {table} APPLY (PURGE)");

            List<string> bySpark = Commits(spark, From), byKernel = Commits(kernel, From);
            Assert.True(bySpark.SequenceEqual(byKernel), $"Spark:\n{string.Join("\n", bySpark)}\nkernel:\n{string.Join("\n", byKernel)}");
            // The rewritten rows are one new cube of the clustering columns, as Spark writes them.
            AddAction[] added = [.. File.ReadAllLines(Path.Combine(kernel, "_delta_log", $"{From:D20}.json"))
                .SelectMany(line => ActionParser.ParseCommit(System.Text.Encoding.UTF8.GetBytes(line))).OfType<AddAction>()];
            Assert.NotEmpty(added);
            Assert.All(added, a => Assert.Equal("liquid", a.ClusteringProvider));
            Assert.All(added, a => Assert.Equal("""["id"]""", a.Tags!["ZCUBE_ZORDER_BY"]));
            Assert.Single(added.Select(a => a.Tags!["ZCUBE_ID"]).Distinct());
            Assert.Equal(SparkRows(spark), SparkRows(kernel));
            // Spark's next OPTIMIZE treats both tables alike.
            foreach (string table in (string[])[kernel, spark]) LocalSpark.Run("sql", table, "OPTIMIZE {table}");
            Assert.Equal(Commits(spark, From + 1), Commits(kernel, From + 1));
        });
    }

    [SparkTheory]
    [InlineData(null, "p", "p1")]
    [InlineData(null, "p", "p1", "q", "1")]
    [InlineData(null, "q", "1", "p", "p1")] // recorded in the order given
    [InlineData(null, "p", null)]
    [InlineData("id", "p", "p1")]
    [Trait("DeltaKernel", "Spark")]
    public void Optimize_OfSomePartitions_CompactsAsSpark_AndRecordsItsWhere(string? zOrderBy, params string?[] filter)
    {
        InRoot(root =>
        {
            string kernel = Path.Combine(root, "kernel"), spark = Path.Combine(root, "spark");
            foreach (string table in (string[])[kernel, spark])
            {
                LocalSpark.Run("sql", table, "CREATE TABLE {table} (id INT, v INT, p STRING, q INT) USING delta PARTITIONED BY (p, q)");
                for (int i = 0; i < 3; i++) // three small files in each partition
                    LocalSpark.Run("sql", table, $"INSERT INTO {{table}} VALUES ({i}, {i}, 'p1', 1), ({i + 10}, {i}, 'p1', 2), ({i + 20}, {i}, NULL, 1)");
            }
            Dictionary<string, string> filters = [];
            for (int i = 0; i < filter.Length; i += 2) filters[filter[i]!] = filter[i + 1]!;
            string where = string.Join(" AND ", filters.Select(f => f.Value is null ? $"{f.Key} IS NULL" : $"{f.Key} = '{f.Value}'"));
            const long From = 4;

            Delta.Optimize(kernel, partitionFilters: filters, zOrderColumns: zOrderBy is null ? null : [zOrderBy]);
            LocalSpark.Run("sql", spark, $"OPTIMIZE {{table}} WHERE {where}{(zOrderBy is null ? "" : $" ZORDER BY ({zOrderBy})")}");

            List<string> bySpark = Commits(spark, From), byKernel = Commits(kernel, From);
            Assert.True(bySpark.SequenceEqual(byKernel), $"Spark:\n{string.Join("\n", bySpark)}\nkernel:\n{string.Join("\n", byKernel)}");
            Assert.Equal(RemovedFilesAddedIn(spark, From), RemovedFilesAddedIn(kernel, From));
            Assert.Equal(SparkRows(spark), SparkRows(kernel));
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Optimize_OfAColumnThatIsNoPartition_IsRefused_AsSpark()
    {
        InRoot(root =>
        {
            string kernel = Path.Combine(root, "kernel");
            LocalSpark.Run("sql", kernel, "CREATE TABLE {table} (id INT, p STRING) USING delta PARTITIONED BY (p)");
            Assert.Contains("non-partition column 'id'. Only the partition columns may be referenced: [p]",
                Assert.ThrowsAny<Exception>(() => Delta.Optimize(kernel, partitionFilters: new() { ["id"] = "1" })).Message);
        });
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Purge_OfAColumnThatIsNoPartition_IsRefused_AsSpark()
    {
        InRoot(root =>
        {
            string kernel = Path.Combine(root, "kernel"), spark = Path.Combine(root, "spark");
            foreach (string table in (string[])[kernel, spark])
                LocalSpark.Run("sql", table, "CREATE TABLE {table} (id INT, p STRING) USING delta PARTITIONED BY (p) TBLPROPERTIES ('delta.enableDeletionVectors'='true')");
            Assert.Contains("non-partition column 'id'",
                Assert.ThrowsAny<Exception>(() => Delta.Purge(kernel, partitionFilters: new Dictionary<string, string?> { ["id"] = "1" })).Message);
            Assert.Contains("DELTA_NON_PARTITION_COLUMN_REFERENCE",
                Assert.ThrowsAny<Exception>(() => LocalSpark.Run("sql", spark, "REORG TABLE {table} WHERE id = 1 APPLY (PURGE)")).Message);
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
            Snapshot dropped = Snapshot.Load(new LocalFileSystem(table));
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
