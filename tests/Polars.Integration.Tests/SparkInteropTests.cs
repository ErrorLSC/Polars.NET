using System.Diagnostics;
using System.Text.Json;
using Polars.CSharp;
using Polars.NET.Core;
using Polars.NET.Core.Delta;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Helpers;
using Polars.NET.Core.Wrappers;

namespace Polars.Integration.Tests;

/// <summary>
/// A local Spark with Delta Lake: the Python of a virtual environment holding pyspark and delta-spark, and a JDK it
/// runs on. Set SESERAGI_SPARK_PYTHON and SESERAGI_SPARK_JAVA_HOME, or keep the defaults
/// (~/.cache/seseragi/spark-venv/bin/python and ~/.local/jdk-21).
/// </summary>
public static class LocalSpark
{
    private static string Home => Environment.GetFolderPath(Environment.SpecialFolder.UserProfile);

    public static string Python =>
        Environment.GetEnvironmentVariable("SESERAGI_SPARK_PYTHON") ?? Path.Combine(Home, ".cache", "seseragi", "spark-venv", "bin", "python");

    public static string JavaHome =>
        Environment.GetEnvironmentVariable("SESERAGI_SPARK_JAVA_HOME") ?? Path.Combine(Home, ".local", "jdk-21");

    public static bool Available => File.Exists(Python) && File.Exists(Path.Combine(JavaHome, "bin", "java"));

    private static string Script => Path.Combine(AppContext.BaseDirectory, "Spark", "delta_interop.py");

    /// <summary>
    /// Runs one command of the interop script; returns its JSON result lines. Commands go to one Spark process kept
    /// for the whole test run (the JVM, the Spark context and Delta start once), one at a time. Set
    /// SESERAGI_SPARK_SERVER=0 to start a process per command instead.
    /// </summary>
    public static List<JsonDocument> Run(params string[] args)
    {
        if (Environment.GetEnvironmentVariable("SESERAGI_SPARK_SERVER") == "0") return RunOnce(args);
        lock (s_lock)
        {
            s_server ??= SparkServer.Start();
            try
            {
                return s_server.Run(args);
            }
            catch (SparkServer.ServerGoneException)
            {
                // The process died (not a failed command): start a new one for the next command.
                s_server.Dispose();
                s_server = null;
                throw;
            }
        }
    }

    private static readonly object s_lock = new();
    private static SparkServer? s_server;

    private static ProcessStartInfo StartInfo(params string[] args)
    {
        var start = new ProcessStartInfo(Python) { RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(Script);
        foreach (string arg in args) start.ArgumentList.Add(arg);
        start.Environment["JAVA_HOME"] = JavaHome;
        return start;
    }

    private static List<JsonDocument> RunOnce(string[] args)
    {
        using Process process = Process.Start(StartInfo(args))!;
        Task<string> stderr = process.StandardError.ReadToEndAsync();
        string stdout = process.StandardOutput.ReadToEnd();
        process.WaitForExit();
        if (process.ExitCode != 0)
            throw new InvalidOperationException($"Spark failed ({process.ExitCode}):\n{stderr.Result[^Math.Min(4000, stderr.Result.Length)..]}");
        return [.. stdout.Split('\n').Where(l => l.StartsWith('{')).Select(l => JsonDocument.Parse(l))];
    }

    /// <summary>The script in serve mode: a command per line in, its results and a done or error line out.</summary>
    private sealed class SparkServer : IDisposable
    {
        public sealed class ServerGoneException(string message) : InvalidOperationException(message);

        private readonly Process _process;
        private readonly System.Text.StringBuilder _stderr = new();

        private SparkServer(Process process)
        {
            _process = process;
            // Drain stderr all the time, or Spark blocks once the pipe fills; keep the tail for error messages.
            process.ErrorDataReceived += (_, e) =>
            {
                if (e.Data is null) return;
                lock (_stderr)
                {
                    _stderr.AppendLine(e.Data);
                    if (_stderr.Length > 64_000) _stderr.Remove(0, _stderr.Length - 32_000);
                }
            };
            process.BeginErrorReadLine();
        }

        public static SparkServer Start()
        {
            ProcessStartInfo start = StartInfo("serve");
            start.RedirectStandardInput = true;
            var server = new SparkServer(Process.Start(start)!);
            AppDomain.CurrentDomain.ProcessExit += (_, _) => server.Dispose();
            return server;
        }

        public List<JsonDocument> Run(string[] args)
        {
            _process.StandardInput.WriteLine(JsonSerializer.Serialize(args));
            _process.StandardInput.Flush();
            var results = new List<JsonDocument>();
            while (true)
            {
                string? line = _process.StandardOutput.ReadLine();
                if (line is null)
                    throw new ServerGoneException($"The Spark process ended unexpectedly:\n{Tail()}");
                if (!line.StartsWith('{')) continue;
                JsonDocument doc = JsonDocument.Parse(line);
                if (doc.RootElement.TryGetProperty("done", out _)) return results;
                if (doc.RootElement.TryGetProperty("error", out JsonElement error))
                    throw new InvalidOperationException(
                        $"Spark failed: {error.GetString()}\n{doc.RootElement.GetProperty("trace").GetString()}\n{Tail()}");
                results.Add(doc);
            }
        }

        private string Tail()
        {
            lock (_stderr) return _stderr.ToString()[^Math.Min(4000, _stderr.Length)..];
        }

        public void Dispose()
        {
            try
            {
                if (!_process.HasExited)
                {
                    _process.StandardInput.Close(); // the script ends at the end of its input
                    if (!_process.WaitForExit(15_000)) _process.Kill(entireProcessTree: true);
                }
            }
            catch (InvalidOperationException)
            {
                // already gone
            }
            _process.Dispose();
        }
    }
}

/// <summary>A fact that runs only where a local Spark is set up (see <see cref="LocalSpark"/>).</summary>
public sealed class SparkFactAttribute : FactAttribute
{
    public SparkFactAttribute()
    {
        if (!LocalSpark.Available)
            Skip = $"No local Spark: set SESERAGI_SPARK_PYTHON / SESERAGI_SPARK_JAVA_HOME (looked for {LocalSpark.Python} and {LocalSpark.JavaHome}).";
    }
}

/// <summary>
/// The managed Delta kernel against Spark, both ways: Spark reads tables the kernel writes, and the kernel reads
/// tables Spark writes with deletion vectors, the change data feed, checkpoints and column mapping.
/// </summary>
public class SparkInteropTests
{
    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Spark_ReadsTables_TheKernelWrites()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_spark_{Guid.NewGuid():N}");
        try
        {
            string typed = Path.Combine(root, "typed");
            Write(typed, Typed([1, 2, 3], "east"), KernelWriteMode.ErrorIfExists, ["region"]);
            Write(typed, Typed([4, 5], "west"), KernelWriteMode.Append);
            Write(typed, Typed([6, 7], "north"), KernelWriteMode.Overwrite);
            Write(typed, Typed([8], "a b/c:d=e%東京"), KernelWriteMode.Append);

            string ntz = Path.Combine(root, "ntz");
            using (var df = DataFrame.FromColumns(new { Id = new[] { 1, 2 }, At = new[] { new DateTime(2024, 5, 6, 7, 8, 9), new DateTime(2025, 1, 1) } }))
                Write(ntz, df, KernelWriteMode.ErrorIfExists);

            string empties = Path.Combine(root, "null_partitions");
            using (var df = DataFrame.FromColumns(new { Id = new[] { 1, 2, 3 }, Region = new string?[] { null, "", "x" } }))
                Write(empties, df, KernelWriteMode.ErrorIfExists, ["Region"]);

            // A table the kernel checkpointed, with the commits before its last checkpoint removed: Spark must read
            // the checkpoint (MAP, LIST and nested columns written by the kernel's own Parquet writer).
            string checkpointed = Path.Combine(root, "checkpointed");
            var properties = new Dictionary<string, string> { ["delta.checkpointInterval"] = "3" };
            using (var df = Typed([1, 2], "east"))
                DeltaKernelWrite.Write(checkpointed, PolarsWrapper.LazyClone(df.Lazy().Handle), KernelWriteMode.ErrorIfExists, ["region"], null, properties);
            for (int i = 3; i <= 9; i++) // versions 1 to 7; checkpoints at 3 and 6
                Write(checkpointed, Typed([i], i % 2 == 0 ? "east" : "a b/c"), i == 5 ? KernelWriteMode.Overwrite : KernelWriteMode.Append);
            Assert.True(File.Exists(Path.Combine(checkpointed, "_delta_log", $"{6:D20}.checkpoint.parquet")));
            for (int v = 0; v <= 6; v++) File.Delete(Path.Combine(checkpointed, "_delta_log", $"{v:D20}.json"));

            string[] tables = [typed, ntz, empties, checkpointed];
            List<JsonDocument> spark = LocalSpark.Run(["read", .. tables]);
            Assert.Equal(tables.Length, spark.Count);
            foreach (JsonDocument table in spark)
            {
                string path = table.RootElement.GetProperty("path").GetString()!;
                AssertSameRows(path, table.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(path)));
            }
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Kernel_ReadsTables_SparkWrites()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_spark_{Guid.NewGuid():N}");
        try
        {
            List<JsonDocument> spark = LocalSpark.Run("write", root);
            Assert.Equal(4, spark.Count);
            foreach (JsonDocument table in spark)
            {
                string path = table.RootElement.GetProperty("path").GetString()!;
                bool changes = table.RootElement.TryGetProperty("changes", out _);
                AssertSameRows(changes ? path + " (change feed)" : path, table.RootElement.GetProperty("rows"),
                    KernelRows(changes ? ChangeFeedWithoutTime(path) : DeltaKernelScan.Scan(path)));
            }

            // Spark did write the features the kernel had to handle.
            string log = string.Concat(Directory.GetFiles(Path.Combine(root, "dv_cdf", "_delta_log"), "*.json").Select(File.ReadAllText));
            Assert.Contains("\"deletionVector\":{", log);
            Assert.Contains("\"cdc\":{", log);
            Assert.NotEmpty(Directory.GetFiles(Path.Combine(root, "checkpointed", "_delta_log"), "*.checkpoint*.parquet"));
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Spark_AndTheKernel_AppendConcurrently_WithoutLosingACommit()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_spark_{Guid.NewGuid():N}");
        try
        {
            string table = Path.Combine(root, "shared");
            using (var seed = DataFrame.FromColumns(new { id = new[] { 0 }, src = new[] { "seed" } }))
                Write(table, seed, KernelWriteMode.ErrorIfExists);

            const int sparkCommits = 12, kernelCommits = 24;
            Task<List<JsonDocument>> spark = Task.Run(() => LocalSpark.Run("append", table, sparkCommits.ToString()));
            // Kernel appends from several threads while Spark appends from its own process.
            Parallel.For(0, kernelCommits, new ParallelOptions { MaxDegreeOfParallelism = 4 }, i =>
            {
                using var row = DataFrame.FromColumns(new { id = new[] { 1 + i }, src = new[] { "kernel" } });
                DeltaKernelWrite.Write(table, PolarsWrapper.LazyClone(row.Lazy().Handle), KernelWriteMode.Append);
            });
            spark.Wait();

            // Every commit landed, in one version each, and both readers see all rows.
            var log = Directory.GetFiles(Path.Combine(table, "_delta_log"), "*.json").Select(Path.GetFileName).Order().ToList();
            Assert.Equal(Enumerable.Range(0, 1 + sparkCommits + kernelCommits).Select(v => $"{v:D20}.json"), log);
            JsonElement kernelRows = KernelRows(DeltaKernelScan.Scan(table));
            Assert.Equal(1 + sparkCommits + kernelCommits, kernelRows.GetArrayLength());
            List<JsonDocument> sparkFinal = LocalSpark.Run("read", table);
            AssertSameRows(table, sparkFinal[0].RootElement.GetProperty("rows"), kernelRows);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void V2Checkpoints_WrittenByEither_AreReadByTheOther()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_spark_{Guid.NewGuid():N}");
        try
        {
            string table = Path.Combine(root, "v2");
            LocalSpark.Run("create_v2", table);
            string log = Path.Combine(table, "_delta_log");

            // The kernel appends; at version 2 it writes a V2 checkpoint (JSON, file actions inline).
            for (int i = 1; i <= 3; i++)
            {
                using var row = DataFrame.FromColumns(new { id = new[] { (long)i }, src = new[] { "kernel" } });
                DeltaKernelWrite.Write(table, PolarsWrapper.LazyClone(row.Lazy().Handle), KernelWriteMode.Append);
            }
            string ours = Assert.Single(Directory.GetFiles(log, $"{2:D20}.checkpoint.*.json"));

            // Without the commits before it, Spark has to start from that checkpoint.
            for (int v = 0; v <= 2; v++) File.Delete(Path.Combine(log, $"{v:D20}.json"));
            JsonDocument spark = LocalSpark.Run("read", table)[0];
            AssertSameRows(table, spark.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(table)));

            // Spark appends and checkpoints at version 4 its own way (sidecars); the kernel reads that.
            JsonDocument afterSpark = LocalSpark.Run("append", table, "1")[0];
            Assert.NotEmpty(Directory.GetFiles(log, $"{4:D20}.checkpoint.*"));
            for (int v = 3; v <= 4; v++) File.Delete(Path.Combine(log, $"{v:D20}.json"));
            File.Delete(ours);
            AssertSameRows(table, afterSpark.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(table)));
            Assert.Equal(4, afterSpark.RootElement.GetProperty("rows").GetArrayLength());
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Spark_ReadsDeletes_TheKernelCommits_AndDeletesOnTop()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_spark_{Guid.NewGuid():N}");
        try
        {
            static ExprHandle Id(Func<ExprHandle, ExprHandle, ExprHandle> op, int value) =>
                op(PolarsWrapper.Col("id"), PolarsWrapper.Lit(value));

            // Deletion vectors and the change data feed: partial deletes give vectors (one merged with an earlier
            // one), deleting the last live row of a file removes it.
            string vectors = Path.Combine(root, "dv_cdf");
            CreateTable(vectors, new() { ["delta.enableDeletionVectors"] = "true", ["delta.enableChangeDataFeed"] = "true" });
            DeltaKernelDelete.Delete(vectors, PolarsWrapper.Or(Id(PolarsWrapper.Eq, 2), Id(PolarsWrapper.GtEq, 7)));
            DeltaKernelDelete.Delete(vectors, Id(PolarsWrapper.Eq, 5));
            DeltaKernelDelete.Delete(vectors, Id(PolarsWrapper.Eq, 6)); // the last live row of its file

            // The change data feed without vectors: files are rewritten.
            string rewritten = Path.Combine(root, "rewrite_cdf");
            CreateTable(rewritten, new() { ["delta.enableChangeDataFeed"] = "true" });
            DeltaKernelDelete.Delete(rewritten, PolarsWrapper.Or(Id(PolarsWrapper.Eq, 3), Id(PolarsWrapper.Eq, 6)));

            List<JsonDocument> spark = LocalSpark.Run("read_changes", vectors, rewritten);
            Assert.Equal(4, spark.Count);
            foreach (JsonDocument table in spark)
            {
                string path = table.RootElement.GetProperty("path").GetString()!;
                bool changes = table.RootElement.TryGetProperty("changes", out _);
                AssertSameRows(changes ? path + " (change feed)" : path, table.RootElement.GetProperty("rows"),
                    KernelRows(changes ? ChangeFeedWithoutTime(path) : DeltaKernelScan.Scan(path)));
            }
            Assert.Equal(3, spark[0].RootElement.GetProperty("rows").GetArrayLength());
            Assert.Equal(8, spark[2].RootElement.GetProperty("rows").GetArrayLength());

            // Spark deletes on top of the kernel's vectors: it has to read and extend them.
            JsonDocument afterSpark = LocalSpark.Run("delete", vectors, "id IN (1, 3)")[0];
            AssertSameRows(vectors, afterSpark.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(vectors)));
            Assert.Equal(1, afterSpark.RootElement.GetProperty("rows").GetArrayLength());
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }

    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Spark_ReadsUpdates_TheKernelCommits_AndUpdatesOnTop()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_spark_{Guid.NewGuid():N}");
        try
        {
            static ExprHandle IdIs(int value) => PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(value));
            static ExprHandle Times(string column, double factor) => PolarsWrapper.Mul(PolarsWrapper.Col(column), PolarsWrapper.Lit(factor));

            // Deletion vectors and the change data feed: updated rows move to new files, one to another partition.
            string vectors = Path.Combine(root, "dv_cdf");
            CreateTable(vectors, new() { ["delta.enableDeletionVectors"] = "true", ["delta.enableChangeDataFeed"] = "true" });
            DeltaKernelUpdate.Update(vectors, PolarsWrapper.Or(IdIs(2), IdIs(7)), [("price", Times("price", 10)), ("note", PolarsWrapper.Lit("changed"))]);
            DeltaKernelUpdate.Update(vectors, IdIs(3), [("region", PolarsWrapper.Lit("north"))]);

            // The change data feed without vectors: files are rewritten with the rows in place.
            string rewritten = Path.Combine(root, "rewrite_cdf");
            CreateTable(rewritten, new() { ["delta.enableChangeDataFeed"] = "true" });
            DeltaKernelUpdate.Update(rewritten, PolarsWrapper.Or(IdIs(3), IdIs(6)), [("amount", PolarsWrapper.Lit(0.5m))]);

            List<JsonDocument> spark = LocalSpark.Run("read_changes", vectors, rewritten);
            Assert.Equal(4, spark.Count);
            foreach (JsonDocument table in spark)
            {
                string path = table.RootElement.GetProperty("path").GetString()!;
                bool changes = table.RootElement.TryGetProperty("changes", out _);
                AssertSameRows(changes ? path + " (change feed)" : path, table.RootElement.GetProperty("rows"),
                    KernelRows(changes ? ChangeFeedWithoutTime(path) : DeltaKernelScan.Scan(path)));
            }

            // Spark updates on top of the kernel's vectors.
            JsonDocument afterSpark = LocalSpark.Run("update", vectors, "price = -1", "id IN (1, 2, 8)")[0];
            AssertSameRows(vectors, afterSpark.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(vectors)));
            Assert.Equal(10, afterSpark.RootElement.GetProperty("rows").GetArrayLength());
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Spark_ReadsMerges_TheKernelCommits_AndMergesOnTop()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_spark_{Guid.NewGuid():N}");
        try
        {
            static ExprHandle Src(string column) => PolarsWrapper.Col(column + DeltaKernelMerge.SourceSuffix);
            MergeClause[] Clauses() =>
            [
                new(MergeClauseKind.MatchedDelete, PolarsWrapper.Lt(Src("price"), PolarsWrapper.Lit(0.0))),
                new(MergeClauseKind.MatchedUpdate, PolarsWrapper.Lit(true)),
                new(MergeClauseKind.NotMatchedInsert, PolarsWrapper.Lit(true)),
                new(MergeClauseKind.NotMatchedBySourceDelete, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(10))),
            ];

            // ids 2 and 7 updated (7 moves to another partition), 3 deleted, 10 deleted by source, 11 and 12 inserted.
            static DataFrame Source()
            {
                using DataFrame rows = Typed([2, 3, 7, 11, 12], "north");
                return rows.WithColumns(
                    Polars.CSharp.Polars.When(Polars.CSharp.Polars.Col("id") == 3).Then(Polars.CSharp.Polars.Lit(-1.0)).Otherwise(Polars.CSharp.Polars.Col("price")).Alias("price"),
                    Polars.CSharp.Polars.When(Polars.CSharp.Polars.Col("id") == 2).Then(Polars.CSharp.Polars.Lit("east")).Otherwise(Polars.CSharp.Polars.Col("region")).Alias("region"));
            }

            string vectors = Path.Combine(root, "dv_cdf");
            CreateTable(vectors, new() { ["delta.enableDeletionVectors"] = "true", ["delta.enableChangeDataFeed"] = "true" });
            using (DataFrame source = Source())
                DeltaKernelMerge.Merge(vectors, PolarsWrapper.LazyClone(source.Lazy().Handle), ["id"], Clauses());

            string rewritten = Path.Combine(root, "rewrite_cdf");
            CreateTable(rewritten, new() { ["delta.enableChangeDataFeed"] = "true" });
            using (DataFrame source = Source())
                DeltaKernelMerge.Merge(rewritten, PolarsWrapper.LazyClone(source.Lazy().Handle), ["id"], Clauses());

            List<JsonDocument> spark = LocalSpark.Run("read_changes", vectors, rewritten);
            Assert.Equal(4, spark.Count);
            foreach (JsonDocument table in spark)
            {
                string path = table.RootElement.GetProperty("path").GetString()!;
                bool changes = table.RootElement.TryGetProperty("changes", out _);
                AssertSameRows(changes ? path + " (change feed)" : path, table.RootElement.GetProperty("rows"),
                    KernelRows(changes ? ChangeFeedWithoutTime(path) : DeltaKernelScan.Scan(path)));
            }
            Assert.Equal(10, spark[0].RootElement.GetProperty("rows").GetArrayLength());

            // Spark merges on top of the kernel's vectors and files.
            JsonDocument afterSpark = LocalSpark.Run("sql", vectors,
                "MERGE INTO {table} t USING (SELECT * FROM VALUES (1, 99.0), (11, 0.5), (50, 1.0) AS s(id, price)) s ON t.id = s.id " +
                "WHEN MATCHED THEN UPDATE SET t.price = s.price WHEN NOT MATCHED THEN INSERT (id, region, price) VALUES (s.id, 'south', s.price)")[0];
            AssertSameRows(vectors, afterSpark.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(vectors)));
            Assert.Equal(11, afterSpark.RootElement.GetProperty("rows").GetArrayLength());
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Spark_ReadsOptimizedTables_AndTheKernelReadsSparksOptimize()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_spark_{Guid.NewGuid():N}");
        try
        {
            // Deletion vectors and small files, compacted; then Z-ordered.
            string table = Path.Combine(root, "optimized");
            CreateTable(table, new() { ["delta.enableDeletionVectors"] = "true", ["delta.enableChangeDataFeed"] = "true" });
            for (int i = 11; i <= 14; i++) Write(table, Typed([i], "west"), KernelWriteMode.Append);
            DeltaKernelDelete.Delete(table, PolarsWrapper.Or(
                PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(2)), PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(6))));
            var compacted = DeltaKernelOptimize.Optimize(table);
            Assert.True(compacted.DeletionVectorsRemoved >= 2);
            DeltaKernelOptimize.Optimize(table, zOrderColumns: ["price", "day"]);

            JsonDocument spark = LocalSpark.Run("read_changes", table)[0];
            AssertSameRows(table, spark.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(table)));
            Assert.Equal(12, spark.RootElement.GetProperty("rows").GetArrayLength());

            // Spark's own OPTIMIZE ZORDER BY on top; the kernel reads the result and the change feed stays the same.
            JsonDocument afterSpark = LocalSpark.Run("sql", table, "OPTIMIZE {table} ZORDER BY (id)")[0];
            AssertSameRows(table, afterSpark.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(table)));
            List<JsonDocument> changes = LocalSpark.Run("read_changes", table);
            AssertSameRows(table + " (change feed)", changes[1].RootElement.GetProperty("rows"), KernelRows(ChangeFeedWithoutTime(table)));
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Vacuum_ByEither_LeavesATableTheOtherReads()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_spark_{Guid.NewGuid():N}");
        try
        {
            string table = Path.Combine(root, "vacuumed");
            CreateTable(table, new() { ["delta.enableDeletionVectors"] = "true", ["delta.enableChangeDataFeed"] = "true" });
            DeltaKernelDelete.Delete(table, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(2)));
            DeltaKernelOptimize.Optimize(table);

            // Spark deletes with its own vectors and change files; the kernel vacuums everything Spark and the
            // kernel replaced, with no retention.
            LocalSpark.Run("sql", table, "DELETE FROM {table} WHERE id IN (5, 6)");
            var vacuumed = DeltaKernelVacuum.Vacuum(table, 0, enforceRetention: false, full: true);
            Assert.NotEmpty(vacuumed.Files);
            Assert.Contains(vacuumed.Files, f => f.StartsWith("deletion_vector_", StringComparison.Ordinal));

            JsonDocument spark = LocalSpark.Run("read", table)[0];
            AssertSameRows(table, spark.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(table)));
            Assert.Equal(7, spark.RootElement.GetProperty("rows").GetArrayLength());

            // Spark's own VACUUM on what the kernel wrote next.
            DeltaKernelUpdate.Update(table, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(7)), [("note", PolarsWrapper.Lit("spark vacuums this"))]);
            JsonDocument afterSpark = LocalSpark.Run("sql", table, "VACUUM {table} RETAIN 0 HOURS")[0];
            AssertSameRows(table, afterSpark.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(table)));
            Assert.Empty(DeltaKernelVacuum.Vacuum(table, 0, enforceRetention: false, dryRun: true, full: true).Files);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Restore_ByEither_AndTheKernelReadsSparksHistory()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_spark_{Guid.NewGuid():N}");
        try
        {
            string table = Path.Combine(root, "restored");
            CreateTable(table, new() { ["delta.enableDeletionVectors"] = "true", ["delta.enableChangeDataFeed"] = "true" }); // v0, v1
            DeltaKernelDelete.Delete(table, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(2)));             // v2
            LocalSpark.Run("sql", table, "UPDATE {table} SET price = 0 WHERE id > 7");                                    // v3

            // The kernel restores to v1 (before both deletes and updates); Spark reads the result and its change feed.
            Assert.Equal(4, DeltaKernelHistory.Restore(table, version: 1).Version);
            List<JsonDocument> spark = LocalSpark.Run("read_changes", table);
            AssertSameRows(table, spark[0].RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(table)));
            AssertSameRows(table + " (change feed)", spark[1].RootElement.GetProperty("rows"), KernelRows(ChangeFeedWithoutTime(table)));
            Assert.Equal(10, spark[0].RootElement.GetProperty("rows").GetArrayLength());

            // Spark restores to v2; the kernel reads it, and its history has Spark's operations as Spark wrote them.
            JsonDocument afterSpark = LocalSpark.Run("sql", table, "RESTORE TABLE {table} TO VERSION AS OF 2")[0];
            AssertSameRows(table, afterSpark.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(table)));
            var history = DeltaKernelHistory.Read(table);
            Assert.Equal([5L, 4, 3, 2, 1, 0], history.Select(e => e.Version));
            Assert.Equal(["RESTORE", "RESTORE", "UPDATE", "DELETE", "WRITE", "WRITE"], history.Select(e => e.CommitInfo!.Operation));
            Assert.Contains("Apache-Spark", history[0].CommitInfo!.Json);
            Assert.Contains(Kaminari.Seseragi.Lakehouse.Delta.Transaction.EngineInfo, history[1].CommitInfo!.Json);
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Spark_ReadsSchemasTheKernelEvolved_AndWritesOnTop()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_spark_{Guid.NewGuid():N}");
        try
        {
            static void Write(string path, DataFrame df, KernelWriteMode mode, Dictionary<string, string>? properties = null, bool evolve = false)
            {
                using (df)
                    DeltaKernelWrite.Write(path, PolarsWrapper.LazyClone(df.Lazy().Handle), mode, null, null, properties, evolve);
            }

            // Column mapping from the start, then a merge that adds a column.
            string mapped = Path.Combine(root, "mapped");
            Write(mapped, DataFrame.FromColumns(new { id = new[] { 1, 2 }, name = new[] { "a", "b" } }), KernelWriteMode.ErrorIfExists,
                new() { ["delta.columnMapping.mode"] = "name" });
            using (var source = DataFrame.FromColumns(new { id = new[] { 2, 3 }, name = new[] { "B", "c" }, score = new[] { 1.5, 2.5 } }))
                DeltaKernelMerge.Merge(mapped, PolarsWrapper.LazyClone(source.Lazy().Handle), ["id"], [], canEvolve: true);

            // Type widening: int widened to long by an evolving append.
            string widened = Path.Combine(root, "widened");
            Write(widened, DataFrame.FromColumns(new { id = new[] { 1, 2 } }), KernelWriteMode.ErrorIfExists, new() { ["delta.enableTypeWidening"] = "true" });
            Write(widened, DataFrame.FromColumns(new { id = new[] { 5_000_000_000L } }), KernelWriteMode.Append, evolve: true);

            // A schema replaced by an overwrite.
            string replaced = Path.Combine(root, "replaced");
            Write(replaced, DataFrame.FromColumns(new { id = new[] { 1 }, old = new[] { "x" } }), KernelWriteMode.ErrorIfExists);
            Write(replaced, DataFrame.FromColumns(new { id = new[] { 7L }, fresh = new[] { true } }), KernelWriteMode.Overwrite, evolve: true);

            List<JsonDocument> spark = LocalSpark.Run("read", mapped, widened, replaced);
            foreach (JsonDocument table in spark)
            {
                string path = table.RootElement.GetProperty("path").GetString()!;
                AssertSameRows(path, table.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(path)));
            }
            Assert.Contains("score", spark[0].RootElement.GetProperty("schema").GetString());
            Assert.Contains("id:bigint", spark[1].RootElement.GetProperty("schema").GetString());

            // Spark writes on top: the kernel reads what it wrote.
            foreach (var (path, statement) in new[]
                     {
                         (mapped, "INSERT INTO {table} (id, name, score) VALUES (9, 'z', 9.5)"),
                         (widened, "INSERT INTO {table} VALUES (6000000000)"),
                         (replaced, "INSERT INTO {table} VALUES (8, false)"),
                     })
            {
                JsonDocument after = LocalSpark.Run("sql", path, statement)[0];
                AssertSameRows(path, after.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(path)));
            }
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    [SparkFact]
    [Trait("DeltaKernel", "Spark")]
    public void Kernel_ReadsWidenedColumns_SkipsSafely_AndRewritesOldFiles()
    {
        string root = Path.Combine(Path.GetTempPath(), $"seseragi_spark_{Guid.NewGuid():N}");
        try
        {
            // Spark widens every kind of column after the first insert: the first file holds the older types.
            string table = Path.Combine(root, "widened");
            JsonDocument spark = LocalSpark.Run("create_widened", table)[0];
            AssertSameRows(table, spark.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(table)));
            Assert.Contains("\"typeWidening\"", string.Concat(Directory.GetFiles(Path.Combine(table, "_delta_log"), "*.json").Select(File.ReadAllText)));

            // The first file's largest f is the float 0.1, a little above the double 0.1: f > 0.1 must keep it.
            var kept = KernelRows(DeltaKernelScan.Scan(table,
                filter: Kaminari.Seseragi.Lakehouse.Skipping.Predicate.Gt(new Kaminari.Seseragi.Lakehouse.Skipping.ColumnPath("f"), 0.1)));
            Assert.Contains(kept.EnumerateArray(), row => row.GetProperty("id").GetInt64() == 1);

            // Deleting a row rewrites the old file in the table's current types; Spark reads the result.
            var result = DeltaKernelDelete.Delete(table, PolarsWrapper.Eq(PolarsWrapper.Col("id"), PolarsWrapper.Lit(2L)));
            Assert.Equal(1, result.RewrittenFiles);
            JsonDocument after = LocalSpark.Run("read", table)[0];
            AssertSameRows(table, after.RootElement.GetProperty("rows"), KernelRows(DeltaKernelScan.Scan(table)));
            Assert.Equal(2, after.RootElement.GetProperty("rows").GetArrayLength());
        }
        finally
        {
            if (Directory.Exists(root)) Directory.Delete(root, true);
        }
    }

    /// <summary>ids 1..4 east, 5..9 west (one file each), then 10 west (a file of one row).</summary>
    private static void CreateTable(string path, Dictionary<string, string> properties)
    {
        using DataFrame east = Typed([1, 2, 3, 4], "east"), west = Typed([5, 6, 7, 8, 9], "west");
        using DataFrame first = east.VStack(west);
        DeltaKernelWrite.Write(path, PolarsWrapper.LazyClone(first.Lazy().Handle), KernelWriteMode.ErrorIfExists, ["region"], null, properties);
        Write(path, Typed([10], "west"), KernelWriteMode.Append);
    }

    private static DataFrame Typed(int[] ids, string region) => DataFrame.FromColumns(new
    {
        id = ids,
        region = ids.Select(_ => region).ToArray(),
        price = ids.Select(i => i * 1.5).ToArray(),
        day = ids.Select(i => new DateOnly(2024, 1, i)).ToArray(),
        at = ids.Select(i => new DateTimeOffset(2024, 1, i, 8, 30, 0, 123, TimeSpan.Zero).AddTicks(4560)).ToArray(),
        amount = ids.Select(i => i + 0.25m).ToArray(),
        flag = ids.Select(i => i % 2 == 0).ToArray(),
        note = ids.Select(i => i == 2 ? null : new string('x', i * 20)).ToArray(),
    });

    private static void Write(string path, DataFrame df, KernelWriteMode mode, string[]? partitionBy = null)
    {
        using (df)
            DeltaKernelWrite.Write(path, PolarsWrapper.LazyClone(df.Lazy().Handle), mode, partitionBy);
    }

    private static LazyFrameHandle ChangeFeedWithoutTime(string path)
    {
        LazyFrameHandle lf = DeltaKernelScan.ReadChangeFeed(path, 0L);
        string[] keep = [.. PolarsSchemaExtractor.GetColumnNames(lf).Where(c => c != DeltaKernelScan.CommitTimestampColumn)];
        return PolarsWrapper.LazySelect(lf, [.. keep.Select(PolarsWrapper.Col)]);
    }

    /// <summary>Rows of a kernel frame in the canonical form of the interop script.</summary>
    private static JsonElement KernelRows(LazyFrameHandle lf)
    {
        var exprs = new List<ExprHandle>();
        foreach (var (name, type) in PolarsSchemaExtractor.GetFields(lf))
        {
            using (type)
            {
                PlDataType? target = PolarsWrapper.GetDataTypeKind(type) switch
                {
                    PlDataType.Date => PlDataType.Int32,
                    PlDataType.Datetime => PlDataType.Int64,
                    _ => null,
                };
                ExprHandle col = PolarsWrapper.Col(name);
                if (target is PlDataType t)
                {
                    using DataTypeHandle dt = PolarsWrapper.NewPrimitiveType((int)t);
                    col = PolarsWrapper.ExprCast(col, PolarsWrapper.DataTypeExprFromDataType(dt), strict: true, wrapNumerical: false);
                }
                exprs.Add(PolarsWrapper.Alias(col, name));
            }
        }
        using DataFrameHandle df = PolarsWrapper.LazyCollect(PolarsWrapper.LazySelect(lf, [.. exprs]), PlEngine.Auto);
        using NativeBuffer json = PolarsWrapper.WriteJsonNativeMemory(df, PlJsonFormat.Json);
        using JsonDocument doc = JsonDocument.Parse(json.Memory.ToArray());
        return doc.RootElement.Clone();
    }

    private static void AssertSameRows(string table, JsonElement spark, JsonElement kernel)
    {
        // Rows sort by id, then (in a change feed) version and change type.
        static string Key(JsonElement row)
        {
            string Part(string name) => row.TryGetProperty(name, out JsonElement v) ? v.ToString().PadLeft(12) : "";
            return Part(row.EnumerateObject().First().Name) + "|" + Part("_commit_version") + "|" + Part("_change_type");
        }
        var expected = spark.EnumerateArray().OrderBy(Key, StringComparer.Ordinal).ToList();
        var actual = kernel.EnumerateArray().OrderBy(Key, StringComparer.Ordinal).ToList();
        Assert.True(expected.Count == actual.Count, $"{table}: Spark has {expected.Count} rows, the kernel {actual.Count}");
        for (int i = 0; i < expected.Count; i++)
        {
            Assert.True(JsonEqual(expected[i], actual[i]),
                $"{table}, row {i}:\nSpark:  {expected[i].GetRawText()}\nkernel: {actual[i].GetRawText()}");
        }
    }

    /// <summary>Equal JSON values; numbers by value, object properties by name (Spark and the engine may order them alike or not).</summary>
    private static bool JsonEqual(JsonElement a, JsonElement b)
    {
        if (a.ValueKind == JsonValueKind.Number && b.ValueKind == JsonValueKind.Number) return a.GetDouble() == b.GetDouble();
        if (a.ValueKind != b.ValueKind) return false;
        switch (a.ValueKind)
        {
            case JsonValueKind.Object:
            {
                var bProps = b.EnumerateObject().ToDictionary(p => p.Name, p => p.Value);
                var aProps = a.EnumerateObject().ToList();
                return aProps.Count == bProps.Count && aProps.All(p => bProps.TryGetValue(p.Name, out JsonElement v) && JsonEqual(p.Value, v));
            }
            case JsonValueKind.Array:
                return a.GetArrayLength() == b.GetArrayLength() && a.EnumerateArray().Zip(b.EnumerateArray()).All(p => JsonEqual(p.First, p.Second));
            case JsonValueKind.String:
                return a.GetString() == b.GetString();
            default:
                return true; // null, true, false: equal kinds
        }
    }
}
