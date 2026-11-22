using System.Text;
using Kaminari.Seseragi.Files;
using Polars.NET.Core;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;
using Polars.NET.Core.Json;
using Polars.NET.Core.Wrappers;
using Xunit.Abstractions;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// The managed JSON entry points of Core (<see cref="ManagedJson"/>): every data type written byte for byte as the
/// engine's writer writes it, read back into the same frame; scans (projection, row limit, row index, file path, Hive
/// partitions, runs again), sinks to files, streams and pipes, partitioned sinks.
/// </summary>
public sealed class ManagedJsonTests(ITestOutputHelper output) : IDisposable
{
    private readonly string _dir = Directory.CreateTempSubdirectory("managed-json-").FullName;

    public void Dispose() => Directory.Delete(_dir, recursive: true);

    private string At(string name) => Path.Combine(_dir, name);

    private sealed record Inner(int A, string B);

    private sealed record Row(long Id, sbyte I8, short? I16, int I32, byte U8, ushort U16, uint U32, ulong U64, float F32, double F64, bool? B,
        string S, DateOnly D, DateTime Ts, TimeOnly T, TimeSpan Dur, decimal Dec, long[] L, Inner St, byte[] Bin);

    /// <summary>A frame of every type JSON writes (three rows, nulls and edge values among them).</summary>
    private static DataFrame Types()
    {
        using var rows = DataFrame.From<Row>(
        [
            new(1, -8, null, int.MinValue, 255, 65535, uint.MaxValue, ulong.MaxValue, 1.5f, double.NaN, true, "a", new DateOnly(2024, 2, 29),
                new DateTime(2024, 1, 2, 3, 4, 5, 678), new TimeOnly(23, 59, 59, 999), TimeSpan.FromMilliseconds(-1500), 12.345m, [1, 2], new(7, "x\n\"y\""), [1, 2, 255]),
            new(2, 0, 16, 0, 0, 0, 0, 0, float.PositiveInfinity, -0.0, false, "b", new DateOnly(1969, 12, 31), new DateTime(1950, 6, 15), new TimeOnly(0, 0),
                TimeSpan.Zero, -0.001m, null, null, null),
            new(3, 127, -16, int.MaxValue, 1, 1, 1, 1, -1e-7f, 1e300, null, null, new DateOnly(1, 1, 1), new DateTime(2262, 4, 11), new TimeOnly(12, 0, 0, 1),
                TimeSpan.FromDays(400), 0m, [3, 4], new(-1, null), []),
        ]);
        return rows.Lazy().WithColumns(
            Col("S").Cast(DataType.Categorical()).Alias("Cat"),
            Col("S").Cast(DataType.Enum(["a", "b", "c"])).Alias("En"),
            Col("Ts").Cast(DataType.Datetime(TimeUnit.Milliseconds, "UTC")).Alias("TsUtc"),
            Col("Ts").Cast(DataType.Datetime(TimeUnit.Nanoseconds)).Alias("TsNs"),
            Col("Ts").Cast(DataType.Datetime(TimeUnit.Microseconds, "Asia/Kolkata")).Alias("TsKolkata"),
            Col("L").Cast(DataType.Array(DataType.Int64, 2)).Alias("Arr"),
            Col("Id").Cast(DataType.Int128).Alias("I128"),
            Col("Dur").Cast(DataType.Duration(TimeUnit.Nanoseconds)).Alias("DurNs")).Collect();
    }

    /// <summary>
    /// Per column: the managed writer's bytes against the engine's (lines and documents, frame and sink), and the
    /// engine's lines read back by the managed reader with the frame's schema (the same frame) and by the engine's.
    /// Binary is left to the managed writer (the engine's panics): base64.
    /// </summary>
    [Fact]
    public void EveryType_WritesAsTheEngine_AndReadsBack()
    {
        using DataFrame all = Types();
        List<string> differences = [];
        foreach (string column in all.Columns)
        {
            using DataFrame one = all.Select(column);
            string type = one.Schema[column].ToString()!;
            string stage = "write";
            try
            {
                if (column != "Bin")
                {
                    foreach (PlJsonFormat format in (PlJsonFormat[])[PlJsonFormat.JsonLines, PlJsonFormat.Json])
                    {
                        byte[] native = PolarsWrapper.WriteJsonMemory(one.Handle, format);
                        byte[] managed = ManagedJson.WriteJsonToMemory(one.Handle, format);
                        if (!native.AsSpan().SequenceEqual(managed))
                            differences.Add($"{column} ({type}) write {format}:\n  native  {Encoding.UTF8.GetString(native)}\n  managed {Encoding.UTF8.GetString(managed)}");
                    }
                    byte[] sunkNative = PolarsWrapper.SinkJsonMemory(one.Lazy().CloneHandle(), PlExternalCompression.Uncompressed, -1, true, true);
                    byte[] sunk = ManagedJson.SinkJsonToMemory(one.Lazy().CloneHandle(), PlExternalCompression.Uncompressed, -1, true);
                    if (!sunkNative.AsSpan().SequenceEqual(sunk))
                        differences.Add($"{column} ({type}) sink:\n  native  {Encoding.UTF8.GetString(sunkNative)}\n  managed {Encoding.UTF8.GetString(sunk)}");
                }
                stage = "read";
                byte[] lines = ManagedJson.WriteJsonToMemory(one.Handle, PlJsonFormat.JsonLines);
                foreach (PlJsonFormat format in (PlJsonFormat[])[PlJsonFormat.JsonLines, PlJsonFormat.Json])
                {
                    byte[] bytes = format == PlJsonFormat.Json ? ManagedJson.WriteJsonToMemory(one.Handle, format) : lines;
                    using var back = new DataFrame(ManagedJson.ReadJson(bytes, null, one.Schema.Handle, null, 100, null, false, format));
                    // NaN and infinities are written as nulls (as the engine writes them): floats are compared with the engine's read only.
                    if (back.Schema[column].ToString() != type || column is not ("F32" or "F64") && !back.Equals(one))
                        differences.Add($"{column} ({type}) read {format}: {back.Schema[column]}\n{back}\nexpected\n{one}");
                    if (column == "Bin") continue;
                    DataFrame native;
                    try
                    {
                        native = new DataFrame(PolarsWrapper.ReadJson(bytes, null, one.Schema.Handle, 100, null, false, format));
                    }
                    catch (PolarsException e)
                    {
                        output.WriteLine($"{column} ({type}) {format}: the engine's reader refuses it: {e.Message}");
                        continue;
                    }
                    using (native)
                    {
                        if (!native.Equals(back) || native.Schema[column].ToString() != type)
                            differences.Add($"{column} ({type}) read {format} differs from the engine's: {native.Schema[column]}\n{native}\nmanaged\n{back}");
                    }
                }
            }
            catch (Exception e)
            {
                differences.Add($"{column} ({type}) {stage}: {e.GetType().Name}: {e.Message}");
            }
        }
        foreach (string difference in differences) output.WriteLine(difference);
        Assert.Empty(differences);
    }

    // ---------------------------------------------------------------- scans

    private static string Ndjson(int rows, int first = 0) =>
        string.Concat(Enumerable.Range(first, rows).Select(i => $$"""{"id":{{i}},"name":"n{{i}}","k":{{i % 3}}}""" + "\n"));

    private static LazyFrame ScanBytes(string text, SchemaHandle schema = null, SchemaHandle overrides = null, ulong? nRows = null,
        string rowIndex = null, uint offset = 0) =>
        new(ManagedJson.ScanNdjson(Encoding.UTF8.GetBytes(text).AsSpan(), schema, overrides, null, 100, nRows, false, rowIndex, offset));

    /// <summary>
    /// A scan reads what the query asks (columns, leading rows), numbers rows with a UInt32 index from the offset (also
    /// when the query keeps only some columns or rows), counts rows without reading columns, and runs again.
    /// </summary>
    [Fact]
    public void AScan_ReadsWhatTheQueryAsks_AndRunsAgain()
    {
        string text = Ndjson(1000);
        using LazyFrame lf = ScanBytes(text, rowIndex: "row", offset: 10);
        using (DataFrame all = lf.Collect())
        {
            Assert.Equal(["row", "id", "name", "k"], all.Columns);
            Assert.Equal("u32", all.Schema["row"].ToString());
            Assert.Equal(1000, all.Height);
            Assert.Equal(Enumerable.Range(10, 1000).Select(i => (uint)i), all.Column("row").ToArray<uint>());
        }
        using (DataFrame some = lf.Filter(Col("id") >= 500).Select("name", "row").Head(3).Collect())
        {
            Assert.Equal(["name", "row"], some.Columns);
            Assert.Equal([510u, 511u, 512u], some.Column("row").ToArray<uint>());
        }
        using (DataFrame count = lf.Select(Len()).Collect()) Assert.Equal(1000u, count.Column("len").ToArray<uint>()[0]);
        using (DataFrame again = lf.Select("id").Collect()) Assert.Equal(Enumerable.Range(0, 1000).Select(i => (long)i), again.Column("id").ToArray<long>());
        using LazyFrame limited = ScanBytes(text, nRows: 7);
        Assert.Equal(7, limited.Collect().Height);
    }

    /// <summary>A schema is the whole schema (nothing inferred, fields it lacks passed over); overrides type some inferred columns.</summary>
    [Fact]
    public void ASchema_IsTheWholeSchema_OverridesTypeInferredColumns()
    {
        string text = """{"a":1,"b":"x","c":{"d":[1,2]}}""" + "\n" + """{"a":2,"b":"y","c":null}""" + "\n";
        using var schema = PolarsSchema.From(new Dictionary<string, DataType> { ["a"] = DataType.Int32, ["b"] = DataType.Categorical() });
        using (DataFrame df = ScanBytes(text, schema: schema.Handle).Collect())
        {
            Assert.Equal(["a", "b"], df.Columns);
            Assert.Equal("i32", df.Schema["a"].ToString());
            Assert.Equal("cat", df.Schema["b"].ToString());
        }
        using var overrides = PolarsSchema.From(new Dictionary<string, DataType> { ["a"] = DataType.UInt8 });
        using (DataFrame inferred = ScanBytes(text, overrides: overrides.Handle).Collect())
        {
            Assert.Equal(["a", "b", "c"], inferred.Columns);
            Assert.Equal("u8", inferred.Schema["a"].ToString());
            Assert.Equal("struct[d: list[i64]]", inferred.Schema["c"].ToString().Replace("struct[1]", "struct"));
        }
        // Both: the overrides replace the schema's types; a 128-bit integer override (no field metadata to carry) is cast back.
        using var wide = PolarsSchema.From(new Dictionary<string, DataType> { ["a"] = DataType.Int128, ["b"] = DataType.String });
        using (DataFrame both = ScanBytes(text, schema: schema.Handle, overrides: wide.Handle).Collect())
        {
            Assert.Equal("i128", both.Schema["a"].ToString());
            Assert.Equal("str", both.Schema["b"].ToString());
            Assert.Equal(["x", "y"], both.Column("b").ToArray<string>());
        }
        using (DataFrame alone = ScanBytes(text, overrides: wide.Handle).Collect()) Assert.Equal("i128", alone.Schema["a"].ToString());
        using var unknown = PolarsSchema.From(new Dictionary<string, DataType> { ["z"] = DataType.Int8 });
        Assert.Throws<ArgumentException>(() => ScanBytes(text, schema: schema.Handle, overrides: unknown.Handle));
    }

    /// <summary>An enum given: values outside its categories are errors, nulls when errors are ignored.</summary>
    [Fact]
    public void AnEnumGiven_RefusesOtherValues_UnlessErrorsAreIgnored()
    {
        byte[] text = Encoding.UTF8.GetBytes("""{"e":"lo"}""" + "\n" + """{"e":"what"}""" + "\n");
        using var schema = PolarsSchema.From(new Dictionary<string, DataType> { ["e"] = DataType.Enum(["lo", "hi"]) });
        Assert.ThrowsAny<Exception>(() => new DataFrame(ManagedJson.ReadJson(text.AsSpan(), null, schema.Handle, null, 100, null, false, PlJsonFormat.JsonLines)));
        using var df = new DataFrame(ManagedJson.ReadJson(text.AsSpan(), null, schema.Handle, null, 100, null, true, PlJsonFormat.JsonLines));
        Assert.Equal("enum", df.Schema["e"].ToString());
        Assert.Equal(["lo", null], df.Column("e").ToArray<string>());
    }

    /// <summary>Files: a directory of files read as one, Hive partitions when asked, the file path column, a missing file named.</summary>
    [Fact]
    public void APathScan_ReadsDirectoriesAndGlobs_HiveWhenAsked()
    {
        Directory.CreateDirectory(At("t/k=1"));
        Directory.CreateDirectory(At("t/k=2"));
        File.WriteAllText(At("t/k=1/a.jsonl"), Ndjson(5));
        File.WriteAllText(At("t/k=2/b.jsonl"), Ndjson(5, 5));
        using (DataFrame df = new LazyFrame(ManagedJson.ScanNdjson(At("t"), null, null, null, 100, null, false, "row", 0, "file", false, null)).Collect())
        {
            Assert.Equal(["row", "id", "name", "k", "file"], df.Columns);
            Assert.Equal(10, df.Height);
            Assert.Equal(Enumerable.Range(0, 10).Select(i => (uint)i), df.Column("row").ToArray<uint>());
            Assert.EndsWith(Path.Combine("k=2", "b.jsonl"), df.Column("file").ToArray<string>()[9]);
        }
        // Hive: the directories' k (an integer) instead of the files' (the files' k column is shadowed by it).
        using (DataFrame hive = new LazyFrame(ManagedJson.ScanNdjson(At("t/**/*.jsonl"), null, null, null, 100, null, false, null, 0, null, true, null)).Collect())
        {
            Assert.Equal(10, hive.Height);
            Assert.Equal([1L, 1, 1, 1, 1, 2, 2, 2, 2, 2], hive.Column("k").ToArray<long>());
        }
        File.WriteAllText(At("one.ndjson"), Ndjson(3));
        Assert.Equal(3, new LazyFrame(ManagedJson.ScanNdjson(At("one.ndjson"), null, null, null, 100, null, false, null, 0, null, false, null)).Collect().Height);
        Assert.Throws<FileNotFoundException>(() => ManagedJson.ScanNdjson(At("none.jsonl"), null, null, null, 100, null, false, null, 0, null, false, null));
    }

    // ---------------------------------------------------------------- sinks

    /// <summary>A stream refusing synchronous writes (as ASP.NET Core's response bodies do).</summary>
    private sealed class AsyncOnly : MemoryStream
    {
        public override void Write(byte[] buffer, int offset, int count) => throw new InvalidOperationException("Synchronous operations are disallowed.");
        public override void Write(ReadOnlySpan<byte> buffer) => throw new InvalidOperationException("Synchronous operations are disallowed.");
        public override void Flush() => throw new InvalidOperationException("Synchronous operations are disallowed.");
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
        {
            await Task.Yield();
            byte[] copy = buffer.ToArray();
            base.Write(copy, 0, copy.Length);
        }
        public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    [Fact]
    public async Task Sinks_WriteWhatTheEngineWrites_ToFilesStreamsAndPipes()
    {
        using DataFrame df = DataFrame.From<Small>([.. Enumerable.Range(0, 5000).Select(i => new Small(i, $"s{i}", i % 7 == 0 ? null : i * 0.5))]);
        byte[] expected = PolarsWrapper.SinkJsonMemory(df.Lazy().CloneHandle(), PlExternalCompression.Uncompressed, -1, true, true);

        var stream = new AsyncOnly();
        await ManagedJson.SinkJsonAsync(df.Lazy().CloneHandle(), stream, PlExternalCompression.Uncompressed, -1, true);
        Assert.Equal(expected, stream.ToArray());

        var pipe = new System.IO.Pipelines.Pipe();
        Task<byte[]> reading = Task.Run(async () =>
        {
            using var all = new MemoryStream();
            await pipe.Reader.CopyToAsync(all);
            return all.ToArray();
        });
        await ManagedJson.SinkJsonAsync(df.Lazy().CloneHandle(), pipe.Writer, PlExternalCompression.Uncompressed, -1, true);
        await pipe.Writer.CompleteAsync();
        Assert.Equal(expected, await reading);

        using (NativeBuffer native = ManagedJson.SinkJsonNativeMemory(df.Lazy().CloneHandle(), PlExternalCompression.Uncompressed, -1, true))
            Assert.Equal(expected, native.Memory.ToArray());

        // Files: compressed ones read back whole; a directory made only when asked; the extension checked.
        foreach (var (compression, name) in (ReadOnlySpan<(PlExternalCompression, string)>)[(PlExternalCompression.Uncompressed, "a.jsonl"),
                     (PlExternalCompression.Gzip, "a.jsonl.gz"), (PlExternalCompression.ZSTD, "a.jsonl.zst")])
        {
            string path = At(Path.Combine("sub", name));
            ManagedJson.SinkJson(df.Lazy().CloneHandle(), path, compression, -1, true, true, PlSyncOnClose.None, true, null);
            using var back = new DataFrame(ManagedJson.ReadJson(path, null, null, null, 100, null, false, PlJsonFormat.JsonLines));
            Assert.True(back.Equals(df), name);
        }
        Assert.Throws<DirectoryNotFoundException>(() =>
            ManagedJson.SinkJson(df.Lazy().CloneHandle(), At("missing/a.jsonl"), PlExternalCompression.Uncompressed, -1, true, true, PlSyncOnClose.None, false, null));
        Assert.Throws<ArgumentException>(() =>
            ManagedJson.SinkJson(df.Lazy().CloneHandle(), At("a.json.gz"), PlExternalCompression.Uncompressed, -1, true, true, PlSyncOnClose.None, false, null));
        Assert.Throws<ArgumentException>(() =>
            ManagedJson.SinkJson(df.Lazy().CloneHandle(), At("a.json"), PlExternalCompression.Gzip, -1, true, true, PlSyncOnClose.None, false, null));
        ManagedJson.SinkJson(df.Lazy().CloneHandle(), At("a.json"), PlExternalCompression.Gzip, -1, false, true, PlSyncOnClose.None, false, null);
        ManagedJson.SinkJson(df.Lazy().CloneHandle(), At("plain"), PlExternalCompression.ZSTD, -1, true, true, PlSyncOnClose.None, false, null);

        // No rows: an empty file; an empty frame as a document is the engine's.
        ManagedJson.SinkJson(df.Lazy().Filter(Col("Id") < 0).CloneHandle(), At("empty.jsonl"), PlExternalCompression.Uncompressed, -1, true, true,
            PlSyncOnClose.None, false, null);
        Assert.Equal(0, new FileInfo(At("empty.jsonl")).Length);
        // Compressed, no rows: an empty member (or frame), as the engine writes (a file of no bytes is not compressed data).
        foreach (PlExternalCompression compression in (PlExternalCompression[])[PlExternalCompression.Gzip, PlExternalCompression.ZSTD])
        {
            byte[] native = PolarsWrapper.SinkJsonMemory(df.Lazy().Filter(Col("Id") < 0).CloneHandle(), compression, -1, true, true);
            byte[] managed = ManagedJson.SinkJsonToMemory(df.Lazy().Filter(Col("Id") < 0).CloneHandle(), compression, -1, true);
            Assert.NotEmpty(native);
            Assert.NotEmpty(managed);
            Assert.Empty(Decompressed(native, compression));
            Assert.Empty(Decompressed(managed, compression));
        }
        using DataFrame none = df.Lazy().Filter(Col("Id") < 0).Collect();
        Assert.Equal(PolarsWrapper.WriteJsonMemory(none.Handle, PlJsonFormat.Json), ManagedJson.WriteJsonToMemory(none.Handle, PlJsonFormat.Json));
    }

    private sealed record Small(long Id, string Name, double? Value);

    private static byte[] Decompressed(byte[] bytes, PlExternalCompression compression)
    {
        using Stream input = compression == PlExternalCompression.Gzip
            ? new System.IO.Compression.GZipStream(new MemoryStream(bytes), System.IO.Compression.CompressionMode.Decompress)
            : new ZstdSharp.DecompressionStream(new MemoryStream(bytes));
        using var output = new MemoryStream();
        input.CopyTo(output);
        return output.ToArray();
    }

    /// <summary>Partitions: Hive directories of the keys' values, our files' names, existing files refused, read back as one.</summary>
    [Fact]
    public void APartitionedSink_WritesHiveDirectories_ReadBackAsOne()
    {
        using DataFrame df = DataFrame.From<Keyed>([.. Enumerable.Range(0, 3000).Select(i => new Keyed(i, i % 3, $"n{i}"))]);
        string root = At("parts");
        ManagedJson.SinkJsonPartitioned(df.Lazy().CloneHandle(), root, ["K"], false, false, 400, 0, PlExternalCompression.Uncompressed, -1, true,
            PlSyncOnClose.None, Kaminari.Seseragi.Files.ExistingData.Error, null);
        string[] directories = [.. Directory.GetDirectories(root).Select(Path.GetFileName).Order()];
        Assert.Equal(["K=0", "K=1", "K=2"], directories);
        string[] files = Directory.GetFiles(Path.Combine(root, "K=1"));
        Assert.Equal(3, files.Length); // 1000 rows, 400 a file
        Assert.All(files, f => Assert.Matches(@"^part-\d{5}-.+\.jsonl$", Path.GetFileName(f)));
        Assert.DoesNotContain("\"K\"", File.ReadAllText(files[0])); // keys in the paths only
        using (DataFrame back = new LazyFrame(ManagedJson.ScanNdjson(root, null, null, null, 100, null, false, null, 0, null, true, null)).Collect())
        {
            Assert.Equal(3000, back.Height);
            using DataFrame sorted = back.Lazy().Sort("Id").Select("Id", "K", "Name").Collect();
            Assert.Equal(df.Column("Id").ToArray<long>(), sorted.Column("Id").ToArray<long>());
            Assert.Equal(df.Column("K").ToArray<long>(), sorted.Column("K").ToArray<long>());
        }
        Assert.ThrowsAny<Exception>(() => ManagedJson.SinkJsonPartitioned(df.Lazy().CloneHandle(), root, ["K"], false, false, 0, 0,
            PlExternalCompression.Uncompressed, -1, true, PlSyncOnClose.None, Kaminari.Seseragi.Files.ExistingData.Error, null));
        ManagedJson.SinkJsonPartitioned(df.Lazy().CloneHandle(), root, ["K"], true, true, 0, 0, PlExternalCompression.Gzip, -1, true,
            PlSyncOnClose.None, Kaminari.Seseragi.Files.ExistingData.Append, null);
        // Keys declared grouped while they are not: more files, each of its partition's rows only.
        string[] appended = Directory.GetFiles(Path.Combine(root, "K=1"));
        Assert.Equal(4, appended.Length);
        string added = appended.Single(f => f.EndsWith(".gz", StringComparison.Ordinal));
        using (var keys = new DataFrame(ManagedJson.ReadJson(added, null, null, null, 100, null, false, PlJsonFormat.JsonLines)))
            Assert.All(keys.Column("K").ToArray<long>(), k => Assert.Equal(1L, k));
        using var lf = df.Lazy();
        Assert.Equal(["K"], ManagedJson.SelectedColumns(lf.CloneHandle(), Selectors.ByName("K").Handle));
    }

    private sealed record Keyed(long Id, long K, string Name);

    // ---------------------------------------------------------------- the public API on it

    /// <summary>
    /// The public entry points: a read error keeps its type, overrides and Hive partitions on scans, partitioned writes
    /// refusing, adding to or replacing what is there.
    /// </summary>
    [Fact]
    public void ThePublicApi_TakesTheNewOptions()
    {
        byte[] bad = Encoding.UTF8.GetBytes("""{"a":1}""" + "\n" + """{"a":"x"}""" + "\n");
        var e = Assert.Throws<Kaminari.Seseragi.Files.Json.JsonValueException>(() =>
            DataFrame.ReadJson(bad, jsonFormat: JsonFormat.JsonLines, schema: new Dictionary<string, DataType> { ["a"] = DataType.Int64 }));
        Assert.Contains("Line 2", e.Message);
        using (DataFrame nulls = DataFrame.ReadJson(bad, jsonFormat: JsonFormat.JsonLines, ignoreErrors: true,
                   schemaOverrides: new Dictionary<string, DataType> { ["a"] = DataType.Int16 }))
        {
            Assert.Equal("i16", nulls.Schema["a"].ToString());
            Assert.Equal([(short?)1, null], nulls.Column("a").ToArray<short?>());
        }

        using DataFrame df = DataFrame.From<Keyed>([.. Enumerable.Range(0, 90).Select(i => new Keyed(i, i % 3, $"n{i}"))]);
        string root = At("api");
        df.WriteJsonPartitioned(root, "K", includeKeys: false);
        Assert.ThrowsAny<Exception>(() => df.WriteJsonPartitioned(root, "K"));
        df.WriteJsonPartitioned(root, "K", existingData: ExistingData.Append, compression: ExternalCompression.Gzip);
        using (DataFrame both = LazyFrame.ScanNdjson(Path.Combine(root, "**", "*"), hivePartitioning: true).Collect())
        {
            Assert.Equal(180, both.Height);
            Assert.Equal(["Id", "Name", "K"], both.Columns); // partition columns after the files' columns
        }
        using (DataFrame noHive = LazyFrame.ScanNdjson(root).Collect()) Assert.Equal(["Id", "Name"], noHive.Columns);
        df.Lazy().Filter(Col("K") == 1).SinkJsonPartitioned(root, "K", includeKeys: false, existingData: ExistingData.ReplacePartitions);
        using DataFrame replaced = LazyFrame.ScanNdjson(root, hivePartitioning: true).Collect();
        Assert.Equal(150, replaced.Height); // K=1 replaced (30 rows), K=0 and K=2 kept (60 each)
    }
}
