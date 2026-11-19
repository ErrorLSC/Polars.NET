using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Ipc;
using Seseragi.Files.Tests.Parquet;
using Case = Seseragi.Files.Tests.Parquet.ParquetDatasetInteropTests.Case;

namespace Seseragi.Files.Tests.Ipc;

/// <summary>
/// IPC datasets read as Polars reads them (<c>scan_ipc</c>): the files a directory, a glob or several inputs give, in
/// order, with the row index, the file path column, a row limit and Hive partitions (filters on them, rows numbered
/// past files left out); files of several codecs, layouts and writers, and files whose types differ, as pyarrow
/// (<c>pyarrow.dataset</c>) and Polars read them where the rules agree. Every read on one thread and several,
/// synchronously and not. Opt-in: SESERAGI_IPC_PYTHON (Polars, pyarrow).
/// </summary>
/// <remarks>
/// Intended differences, pinned here: layouts unite to the first file's; a first file's narrower dictionary indices
/// refuse a later file's unless dictionaries are unified (pyarrow casts the indices when they fit); Polars Enums of
/// other categories are refused (as Polars refuses them; pyarrow reads their values).
/// </remarks>
[Trait("Category", "Interop")]
public sealed class IpcDatasetInteropTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-ipc-dataset-interop-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private string At(string relative) => Path.Combine(_root, relative);

    private static string? Python => Environment.GetEnvironmentVariable("SESERAGI_IPC_PYTHON") is { Length: > 0 } python ? python : null;

    private static string Corpus(string relative) => Path.Combine(AppContext.BaseDirectory, "TestData", "ipc", relative);

    private static readonly IIpcCompression[] Codecs = [NoCompression.Instance, Lz4FrameCompression.Instance, new ZstandardCompression()];

    /// <summary>
    /// A file of <c>id</c> (Int64) and <c>name</c> (strings, every seventh null; views in some files), in batches of
    /// <paramref name="batchRows"/>, compressed by codec <paramref name="codec"/> (none, LZ4, Zstandard).
    /// </summary>
    private void Write(string relative, long first, int rows, int batchRows, int codec = 0, bool views = false)
    {
        string path = At(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("name", views ? StringViewType.Default : StringType.Default, true)], null);
        using IpcWriter writer = IpcWriter.Create(path, schema, new IpcWriteOptions { Compression = Codecs[codec % Codecs.Length] });
        for (int at = 0; at < rows; at += batchRows)
        {
            int n = Math.Min(batchRows, rows - at);
            var ids = new Int64Array.Builder();
            var plain = new StringArray.Builder();
            var view = new StringViewArray.Builder();
            for (int i = 0; i < n; i++)
            {
                long id = first + at + i;
                ids.Append(id);
                if ((at + i) % 7 == 3)
                {
                    plain.AppendNull();
                    view.AppendNull();
                }
                else
                {
                    string name = id % 2 == 0 ? $"n{id}" : $"n{id}, longer than twelve bytes"; // views: inline and out of line
                    plain.Append(name);
                    view.Append(name);
                }
            }
            IArrowArray names = views ? view.Build() : plain.Build();
            using var batch = new RecordBatch(schema, [ids.Build(), names], n);
            writer.Write(batch);
        }
        writer.Complete();
    }

    /// <summary>The ways every dataset is read: one thread, several, several without mapping files, several asynchronously.</summary>
    private static readonly (IpcReadOptions Options, bool Async)[] Ways =
    [
        (new() { MaxDegreeOfParallelism = 1 }, false), (new() { MaxDegreeOfParallelism = 4 }, false),
        (new() { MaxDegreeOfParallelism = 4, MemoryMap = IpcMemoryMap.Off }, false), (new() { MaxDegreeOfParallelism = 4 }, true),
    ];

    [Fact]
    public async Task Datasets_ReadAsPolarsReadsThem()
    {
        if (Python is not { } python) return;
        Write("t/b.arrow", 100, 30, 8, codec: 1);
        Write("t/a.arrow", 0, 5, 1000);
        Write("t/c.arrow", 200, 3, 1000, codec: 2, views: true);
        Write("t/sub/d.arrow", 300, 12, 5, codec: 2);
        Write("t/sub/deeper/e.arrow", 400, 4, 1000, codec: 1, views: true);
        Write("t/.hidden.arrow", 500, 2, 1000);
        Write("t/_meta.arrow", 600, 2, 1000);
        File.WriteAllBytes(At("t/sub/empty.arrow"), []);
        Dictionary<string, Case> cases = new()
        {
            ["dir"] = new([At("t")], null, null, null),
            ["dir_slash"] = new([At("t") + "/"], null, null, null),
            ["glob_all"] = new([At("t/**/*.arrow")], null, null, null),
            ["glob_top"] = new([At("t/*.arrow")], null, null, null),
            ["glob_one_char"] = new([At("t/?.arrow")], null, null, null),
            ["glob_class"] = new([At("t/[ab].arrow")], null, null, null),
            ["glob_mid"] = new([At("t/*/d.arrow")], null, null, null),
            ["list"] = new([At("t/sub"), At("t/a.arrow"), At("t/*.arrow")], null, null, null),
            ["index_paths"] = new([At("t")], "n", "file", null),
            ["index_paths_limit"] = new([At("t/**/*.arrow")], "n", "file", 41),
            ["limit_zero"] = new([At("t")], "n", null, 0),
            ["limit_in_batch"] = new([At("t/b.arrow"), At("t/sub/d.arrow")], "n", null, 13),
        };
        await Compare(python, cases);
    }

    /// <summary>Partitioned datasets: keys, types and values from paths, collisions, filters over partitions, row numbers past files left out.</summary>
    [Fact]
    public async Task HiveDatasets_ReadAsPolarsReadsThem()
    {
        if (Python is not { } python) return;
        int n = 0;
        foreach (string year in (string[])["2023", "2024", "2025"])
        {
            foreach (string month in (string[])["01", "12"]) Write($"h/year={year}/month={month}/p.arrow", n * 10, 3 + n % 4, 2, codec: n++);
        }
        Write("s/s=x/p.arrow", 0, 2, 1000);
        Write("s/s=/p.arrow", 10, 2, 1000);
        Write("s/s=__HIVE_DEFAULT_PARTITION__/p.arrow", 20, 2, 1000);
        Write("s/s=a%2Fb%20c/p.arrow", 30, 2, 1000);
        Write("d/d=2024-01-15/p.arrow", 0, 2, 1000);
        Write("d/d=2024-02-29/p.arrow", 10, 2, 1000);
        Write("f/k=1/p.arrow", 0, 1, 1000);
        Write("f/k=1.5/p.arrow", 10, 1, 1000);
        Write("f/k=-2/p.arrow", 20, 1, 1000);
        Write("b/k=True/p.arrow", 0, 1, 1000);
        Write("b/k=false/p.arrow", 10, 1, 1000);
        Write("t/t=2024-01-15 10%3A00%3A00/p.arrow", 0, 1, 1000);
        Write("t/t=2024-01-15T11%3A30%3A00.5/p.arrow", 10, 1, 1000);
        Write("u/t=2024-01-15T10%3A00%3A00Z/p.arrow", 0, 1, 1000);
        Write("u/t=2024-01-15T10%3A00%3A00%2B02%3A00/p.arrow", 10, 1, 1000);
        Write("i/k=99999999999999999999/p.arrow", 0, 1, 1000);
        Write("c/id=7/p.arrow", 0, 3, 1000);
        Dictionary<string, Case> cases = new()
        {
            ["dir_auto"] = new([At("h")], null, null, null, Hive: null),
            ["dir_off"] = new([At("h")], null, null, null, Hive: false),
            ["glob_on"] = new([At("h/**/*.arrow")], "n", "file", null, Hive: true),
            ["glob_auto"] = new([At("h/**/*.arrow")], null, null, null, Hive: null),
            ["filter_partition_index"] = new([At("h")], "n", null, null, Hive: null, Filter: ("year", 2024L)),
            ["filter_partition_limit"] = new([At("h")], "n", "file", 4, Hive: null, Filter: ("month", 12L)),
            ["filter_column_index"] = new([At("h")], "n", null, null, Hive: null, Filter: ("id", 21L)),
            ["strings"] = new([At("s")], null, null, null, Hive: null),
            ["strings_filter"] = new([At("s")], null, null, null, Hive: null, Filter: ("s", "a/b c")),
            ["dates"] = new([At("d")], null, null, null, Hive: null),
            ["dates_off"] = new([At("d")], null, null, null, Hive: null, ParseDates: false),
            ["floats"] = new([At("f")], null, null, null, Hive: null),
            ["booleans"] = new([At("b")], null, null, null, Hive: null),
            ["datetimes"] = new([At("t")], null, null, null, Hive: null),
            ["datetimes_utc"] = new([At("u")], null, null, null, Hive: null),
            ["int128"] = new([At("i")], null, null, null, Hive: null),
            ["collision"] = new([At("c")], "n", null, null, Hive: null),
        };
        await Compare(python, cases);
    }

    /// <summary>A dataset of files of other types, layouts or writers: the options, and the engine reading it alike (null: we refuse it).</summary>
    private sealed record TypeCase(string Name, string[] Inputs, IpcDatasetOptions Options, string? Oracle);

    /// <summary>
    /// Files of several codecs, layouts and writers (the corpus: pyarrow's LZ4, Zstandard and uncompressed files of one
    /// schema; Polars' frames at the newest and oldest levels, views and large layouts), and files whose types differ
    /// (TestData/ipc/datasets), as pyarrow and Polars read them where the rules agree; refused where ours refuse.
    /// </summary>
    [Fact]
    public async Task DatasetsOfMixedFiles_ReadAsOtherEnginesReadThem()
    {
        if (Python is not { } python) return;
        static string Mixed(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "ipc", "datasets", name);
        string[] codecs = [.. ((string[])["lz4", "zstd", "zstd_level19", "many_batches", "many_batches_lz4"]).Select(n => Corpus($"pyarrow/{n}.arrow"))];
        string[] frames = [.. ((string[])["frame_uncompressed", "frame_lz4", "frame_zstd", "frame_oldest_uncompressed", "frame_oldest_lz4", "frame_oldest_zstd"])
            .Select(n => Corpus($"polars/{n}.arrow"))];
        string[] oldestFirst = [Corpus("polars/frame_oldest_zstd.arrow"), Corpus("polars/frame_lz4.arrow")];
        string[] big = [Corpus("pyarrow/big_lz4.arrow"), Corpus("polars/big_lz4.arrow")];
        IpcDatasetOptions first = new(), unified = new() { UnifyDictionaries = true }, union = new() { SchemaMode = DatasetSchemaMode.UnionByName };
        IpcDatasetOptions unionUnified = union with { UnifyDictionaries = true };
        List<TypeCase> cases = [];
        foreach (var (name, inputs) in (ValueTuple<string, string[]>[])[("codecs", codecs), ("frames", frames), ("oldest_first", oldestFirst), ("big", big)])
        {
            foreach (string engine in (string[])["pyarrow", "polars"])
            {
                cases.Add(new(name, inputs, first, engine));
                cases.Add(new(name, inputs, unified, engine));
                cases.Add(new(name, inputs, union, engine));
            }
        }
        // "lists" is left to IpcDatasetTypeTests (each file as pyarrow reads it): pyarrow 26's dataset casts the list
        // view of c.arrow into list<int64> wrongly ([[], [10], [], None] for [[12, 13], [10, 11, 12], [], None]).
        foreach (string name in (string[])["strings", "dictplain", "enum_same", "enum_list"])
        {
            cases.Add(new(name, [Mixed(name)], first, "pyarrow"));
            cases.Add(new(name, [Mixed(name)], unionUnified, "pyarrow"));
        }
        cases.Add(new("nested", [Mixed("nested")], first, null)); // the first file's Int8 indices cannot take the second's Int16
        cases.Add(new("nested", [Mixed("nested")], unified, "pyarrow"));
        cases.Add(new("nested", [Mixed("nested")], unionUnified, "polars"));
        cases.Add(new("dicts", [Mixed("dicts")], first, null)); // the first file's Int8 indices cannot take the others' (pyarrow casts them)
        cases.Add(new("dicts", [Mixed("dicts")], unified, "pyarrow"));
        cases.Add(new("dicts", [Mixed("dicts")], unionUnified, "polars"));
        foreach (string name in (string[])["strings", "enum_same", "enum_list"]) cases.Add(new(name, [Mixed(name)], first, "polars"));
        foreach (string name in (string[])["enum_diff", "enum_plain", "enum_struct"])
        {
            cases.Add(new(name, [Mixed(name)], first, null)); // as Polars refuses them
            cases.Add(new(name, [Mixed(name)], unionUnified, null));
        }

        await CompareCases(python, cases, "mixed");
    }

    /// <summary>
    /// Datasets of streams (the corpus's, and the mixed-type datasets written again as streams by pyarrow), alone or
    /// beside files, as pyarrow and Polars read each file (neither reads datasets of streams): the rows concatenated.
    /// </summary>
    [Fact]
    public async Task DatasetsOfStreams_ReadAsEachFileReads()
    {
        if (Python is not { } python) return;
        static string Mixed(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "ipc", "datasets", name);
        string Streams(string name)
        {
            string target = At($"streams/{name}");
            if (!Directory.Exists(target))
                ParquetDatasetInteropTests.Python(python, Corpus("datasets/dataset_oracle.py"), "streams", Mixed(name), target).GetAwaiter().GetResult();
            return target;
        }
        IpcDatasetOptions first = new(), unified = new() { UnifyDictionaries = true }, union = new() { SchemaMode = DatasetSchemaMode.UnionByName };
        IpcDatasetOptions unionUnified = union with { UnifyDictionaries = true };
        string[] frames = [Corpus("polars/frame.arrows"), Corpus("polars/frame_zstd.arrows"), Corpus("polars/frame_oldest.arrows")];
        string[] mixed = [Corpus("polars/frame_zstd.arrow"), Corpus("polars/frame_zstd.arrows"), Corpus("polars/frame_oldest.arrows"), Corpus("polars/frame_lz4.arrow")];
        string[] codecs = [Corpus("pyarrow/lz4.arrows"), Corpus("pyarrow/zstd.arrows"), Corpus("pyarrow/lz4.arrow")];
        List<TypeCase> cases = [];
        foreach (var (name, inputs) in (ValueTuple<string, string[]>[])[("stream_frames", frames), ("stream_mixed", mixed), ("stream_codecs", codecs)])
        {
            foreach (IpcDatasetOptions options in (IpcDatasetOptions[])[first, unified, union])
            {
                cases.Add(new(name, inputs, options, "pyarrow_files"));
                cases.Add(new(name, inputs, options, "polars_files"));
            }
        }
        foreach (string name in (string[])["dictionary_replacement", "dictionary_delta", "dictionary"])
        {
            cases.Add(new($"stream_{name}", [Corpus($"pyarrow/{name}.arrows")], first, "pyarrow_files"));
            cases.Add(new($"stream_{name}", [Corpus($"pyarrow/{name}.arrows")], unified, "pyarrow_files"));
        }
        // The mixed-type datasets as streams: where the files' rules read them, each file as pyarrow reads it (lists too:
        // no dataset cast here), refused alike.
        foreach (string name in (string[])["strings", "lists", "dictplain", "enum_same", "enum_list"])
        {
            cases.Add(new($"streams_{name}", [Streams(name)], first, "pyarrow_files"));
            cases.Add(new($"streams_{name}", [Streams(name)], unionUnified, "pyarrow_files"));
        }
        cases.Add(new("streams_nested", [Streams("nested")], first, null));
        cases.Add(new("streams_nested", [Streams("nested")], unified, "pyarrow_files"));
        cases.Add(new("streams_dicts", [Streams("dicts")], unified, "pyarrow_files"));
        cases.Add(new("streams_enum_diff", [Streams("enum_diff")], first, null));
        await CompareCases(python, cases, "streams");
    }

    /// <summary>The cases read by the oracles (dataset_oracle.py) and by us on every way of reading; differences listed.</summary>
    private async Task CompareCases(string python, List<TypeCase> cases, string tag)
    {
        Dictionary<string, string[]> inputsByName = cases.GroupBy(c => c.Name).ToDictionary(g => g.Key, g => g.First().Inputs);
        string casesPath = At($"{tag}_cases.json"), outPath = At($"{tag}.json");
        using (var stream = File.Create(casesPath))
        using (var json = new Utf8JsonWriter(stream))
        {
            json.WriteStartObject();
            foreach (var (name, inputs) in inputsByName)
            {
                json.WriteStartObject(name);
                json.WriteStartArray("inputs");
                foreach (string input in inputs) json.WriteStringValue(input);
                json.WriteEndArray();
                json.WriteEndObject();
            }
            json.WriteEndObject();
        }
        await ParquetDatasetInteropTests.Python(python, Corpus("datasets/dataset_oracle.py"), casesPath, outPath);
        using JsonDocument oracle = JsonDocument.Parse(File.ReadAllText(outPath));

        List<string> differences = [];
        foreach (TypeCase c in cases)
        {
            string label = $"{c.Name} ({(c.Options.SchemaMode == DatasetSchemaMode.UnionByName ? "union" : "first")}{(c.Options.UnifyDictionaries ? ", unified" : "")})";
            JsonElement theirs = c.Oracle is null ? default : oracle.RootElement.GetProperty(c.Name).GetProperty(c.Oracle);
            if (c.Oracle is not null && theirs.TryGetProperty("error", out JsonElement failed))
            {
                differences.Add($"{label}: {c.Oracle} failed: {failed}");
                continue;
            }
            string[]? wanted = c.Oracle is null ? null : [.. theirs.GetProperty("rows").EnumerateArray().Select(e => e.GetString()!)];
            foreach (var (options, async) in Ways)
            {
                string way = $"{label}, DOP {options.MaxDegreeOfParallelism}{(async ? ", async" : "")}{(options.MemoryMap == IpcMemoryMap.Off ? ", unmapped" : "")}";
                List<string>? rows = null;
                string? error = null;
                try
                {
                    using var dataset = IpcDataset.Open(c.Inputs, c.Options);
                    rows = await Canonical(dataset, options, async);
                }
                catch (Exception e) when (e is IpcFormatException or NotSupportedException or OverflowException)
                {
                    error = e.Message;
                }
                if (wanted is null)
                {
                    if (error is null) differences.Add($"{way}: read {rows!.Count} rows, expected a refusal");
                }
                else if (error is not null) differences.Add($"{way}: refused ({error}), {c.Oracle} reads {wanted.Length} rows");
                else if (!rows!.SequenceEqual(wanted))
                {
                    List<string> read = rows!;
                    int at = Enumerable.Range(0, Math.Min(read.Count, wanted.Length)).FirstOrDefault(i => read[i] != wanted[i], Math.Min(read.Count, wanted.Length));
                    differences.Add($"{way}: {read.Count} rows, {c.Oracle} {wanted.Length}; first difference at {at}: "
                        + $"{(at < read.Count ? read[at] : "-")} vs {(at < wanted.Length ? wanted[at] : "-")}");
                }
            }
        }
        Assert.True(differences.Count == 0, string.Join(Environment.NewLine, differences));
    }

    /// <summary>Every row of a reading, its values in the canonical form (arrow_canonical.py) joined by '|'.</summary>
    private static async Task<List<string>> Canonical(IpcDataset dataset, IpcReadOptions options, bool async)
    {
        using IpcDatasetReader reader = dataset.OpenReader(options);
        List<string> rows = [];
        void Add(RecordBatch batch)
        {
            using (batch)
            {
                for (int i = 0; i < batch.Length; i++)
                    rows.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(c => CanonicalRender.Value(batch.Column(c), i))));
            }
        }
        if (async) await foreach (RecordBatch batch in reader.ReadBatchesAsync()) Add(batch);
        else foreach (RecordBatch batch in reader.ReadBatches()) Add(batch);
        return rows;
    }

    private async Task Compare(string python, Dictionary<string, Case> cases)
    {
        using JsonDocument polars = await ParquetDatasetInteropTests.ReadWithPolars(python, _root, cases, "ipc");
        List<string> differences = [];
        foreach (var (name, c) in cases)
        {
            JsonElement expected = polars.RootElement.GetProperty(name);
            Assert.False(expected.TryGetProperty("error", out JsonElement error), $"{name}: Polars failed: {error}");
            var options = new IpcDatasetOptions
            {
                FilePathColumn = c.FileColumn,
                Partitioning = c.Hive switch { null => HivePartitioning.Auto, true => HivePartitioning.On, false => HivePartitioning.Off },
                ParseHiveDates = c.ParseDates,
            };
            using var dataset = IpcDataset.Open(c.Inputs, options);
            ScalarExpr? filter = c.Filter is var (column, value)
                ? ScalarExpr.NewBinary(BinaryOp.Eq, ScalarExpr.NewColumn(column),
                    ScalarExpr.NewLiteral(value is long number ? LiteralValue.NewInt64(number) : LiteralValue.NewString((string)value)))
                : null;
            string[] columns = [.. expected.GetProperty("columns").EnumerateArray().Select(e => e.GetString()!)];
            string[] types = [.. expected.GetProperty("types").EnumerateArray().Select(e => e.GetString()!)];
            string[] theirs = [.. expected.GetProperty("rows").EnumerateArray().Select(e => e.GetString()!)];
            foreach (var (way, async) in Ways)
            {
                string label = $"{name} (DOP {way.MaxDegreeOfParallelism}{(async ? ", async" : "")}{(way.MemoryMap == IpcMemoryMap.Off ? ", unmapped" : "")})";
                using IpcDatasetReader reader = dataset.OpenReader(way with { RowIndexColumn = c.RowIndex, MaxRows = c.Rows, Filter = filter, FilterRows = filter is not null });
                if (!columns.SequenceEqual(reader.Schema.FieldsList.Select(f => f.Name)))
                    differences.Add($"{label}: columns {string.Join(",", reader.Schema.FieldsList.Select(f => f.Name))}, Polars {string.Join(",", columns)}");
                // Types as Polars names them (the row index aside: Polars numbers rows as UInt32, we as Int64).
                string[] ours = [.. reader.Schema.FieldsList.Select(f => f.Name == c.RowIndex ? "UInt32" : ParquetDatasetInteropTests.PolarsType(f.DataType))];
                if (!types.SequenceEqual(ours)) differences.Add($"{label}: types {string.Join(",", ours)}, Polars {string.Join(",", types)}");
                List<string> rows = [];
                void Add(RecordBatch batch)
                {
                    using (batch)
                    {
                        for (int i = 0; i < batch.Length; i++)
                            rows.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(k => ParquetDatasetInteropTests.Render(batch.Column(k), i))));
                    }
                }
                if (async) await foreach (RecordBatch batch in reader.ReadBatchesAsync()) Add(batch);
                else foreach (RecordBatch batch in reader.ReadBatches()) Add(batch);
                if (!rows.SequenceEqual(theirs))
                {
                    int at = Enumerable.Range(0, Math.Min(rows.Count, theirs.Length)).FirstOrDefault(i => rows[i] != theirs[i], Math.Min(rows.Count, theirs.Length));
                    differences.Add($"{label}: {rows.Count} rows, Polars {theirs.Length}; first difference at {at}: "
                        + $"{(at < rows.Count ? rows[at] : "-")} vs {(at < theirs.Length ? theirs[at] : "-")}");
                }
            }
        }
        Assert.True(differences.Count == 0, string.Join(Environment.NewLine, differences));
    }
}
