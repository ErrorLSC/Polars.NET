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

    /// <summary>Runs the interop script; returns its JSON result lines.</summary>
    public static List<JsonDocument> Run(params string[] args)
    {
        var start = new ProcessStartInfo(Python) { RedirectStandardOutput = true, RedirectStandardError = true };
        start.ArgumentList.Add(Script);
        foreach (string arg in args) start.ArgumentList.Add(arg);
        start.Environment["JAVA_HOME"] = JavaHome;
        using Process process = Process.Start(start)!;
        Task<string> stderr = process.StandardError.ReadToEndAsync();
        string stdout = process.StandardOutput.ReadToEnd();
        process.WaitForExit();
        if (process.ExitCode != 0)
            throw new InvalidOperationException($"Spark failed ({process.ExitCode}):\n{stderr.Result[^Math.Min(4000, stderr.Result.Length)..]}");
        return [.. stdout.Split('\n').Where(l => l.StartsWith('{')).Select(l => JsonDocument.Parse(l))];
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
