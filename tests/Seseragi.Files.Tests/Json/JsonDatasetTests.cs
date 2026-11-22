using System.IO.Compression;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Samidare.IR;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.IO;
using Kaminari.Seseragi.Files.Json;

namespace Seseragi.Files.Tests.Json;

/// <summary>
/// <see cref="JsonDataset"/>: paths, directories and globs expanded as for the other datasets, files read in order into
/// one schema (given, the first file's records', or every file's united), lines and documents, compressed files,
/// Hive partitions (pruning files), a row filter, a file path column, limits across files, errors naming the file and
/// the line, memory stores, typed rows; the same rows whatever the threads and chunks.
/// </summary>
public sealed class JsonDatasetTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-json-dataset-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private string At(string relative) => Path.Combine(_root, relative);

    private string Write(string relative, string text, string? compression = null)
    {
        string path = At(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        byte[] bytes = Encoding.UTF8.GetBytes(text);
        using FileStream file = File.Create(path);
        using Stream output = compression switch
        {
            "gzip" => new GZipStream(file, CompressionLevel.Fastest),
            "zstd" => new ZstdSharp.CompressionStream(file),
            _ => file,
        };
        output.Write(bytes);
        return path;
    }

    // Lines of records {"id": i, "name": "n{i}"} from first.
    private static string Lines(long first, int count) =>
        string.Concat(Enumerable.Range(0, count).Select(i => $$"""{"id":{{first + i}},"name":"n{{first + i}}"}""" + "\n"));

    private static List<string> Rows(IEnumerable<RecordBatch> batches)
    {
        List<string> rows = [];
        foreach (RecordBatch batch in batches)
            using (batch)
                rows.AddRange(ArrowRender.Rows(batch));
        return rows;
    }

    private static List<string> Read(JsonDataset dataset, JsonReadOptions? options = null)
    {
        using JsonDatasetReader reader = dataset.OpenReader(options);
        return Rows(reader.ReadBatches());
    }

    private static List<string> Expected(params (long First, int Count)[] runs) =>
        [.. runs.SelectMany(r => Enumerable.Range(0, r.Count).Select(i => $"{r.First + i} | \"n{r.First + i}\""))];

    private static ScalarExpr Col(string name) => ScalarExpr.NewColumn(name);

    private static ScalarExpr Lit(long value) => ScalarExpr.NewLiteral(LiteralValue.NewInt64(value));

    // ---------------------------------------------------------------- files

    [Fact]
    public async Task ADirectory_IsReadWhole_InPathOrder_WhateverTheThreadsAndChunks()
    {
        Write("d/b.jsonl", Lines(1000, 3000));
        Write("d/a.jsonl", Lines(0, 1000));
        Write("d/c/e.jsonl", Lines(4000, 10));
        Write("d/empty.jsonl", "");
        using JsonDataset dataset = JsonDataset.Open(At("d"));
        Assert.Equal(["id", "name"], dataset.Schema.FieldsList.Select(f => f.Name));
        List<string> expected = Expected((0, 1000), (1000, 3000), (4000, 10));
        foreach (int threads in (int[])[1, 8])
            foreach (int unit in (int[])[1 << 20, 1000])
            {
                var options = new JsonReadOptions { MaxDegreeOfParallelism = threads, UnitBytes = unit, BatchSize = 333 };
                Assert.Equal(expected, Read(dataset, options));
                using JsonDatasetReader reader = dataset.OpenReader(options);
                List<string> rows = [];
                await foreach (RecordBatch batch in reader.ReadBatchesAsync())
                    using (batch)
                        rows.AddRange(ArrowRender.Rows(batch));
                Assert.Equal(expected, rows);
            }
    }

    [Fact]
    public void GlobsAndLists_KeepTheirOrder()
    {
        Write("g/x1.jsonl", Lines(0, 5));
        Write("g/x2.jsonl", Lines(5, 5));
        Write("g/y.jsonl", Lines(10, 5));
        using (JsonDataset dataset = JsonDataset.Open(At("g/x*.jsonl"))) Assert.Equal(Expected((0, 10)), Read(dataset));
        using (JsonDataset dataset = JsonDataset.Open([At("g/y.jsonl"), At("g/x1.jsonl")])) Assert.Equal(Expected((10, 5), (0, 5)), Read(dataset));
        using (JsonDataset dataset = JsonDataset.Open([new JsonDatasetFile(At("g/x2.jsonl"))])) Assert.Equal(Expected((5, 5)), Read(dataset));
    }

    [Theory]
    [InlineData("gzip")]
    [InlineData("zstd")]
    public void CompressedFiles_AreReadInOrder_BesidePlainOnes(string codec)
    {
        Write("z/a.jsonl", Lines(0, 2000), codec);
        Write("z/b.jsonl", Lines(2000, 10));
        Write("z/c.jsonl", Lines(2010, 3000), codec);
        using JsonDataset dataset = JsonDataset.Open(At("z"));
        foreach (int threads in (int[])[1, 8])
            Assert.Equal(Expected((0, 5010)), Read(dataset, new JsonReadOptions { MaxDegreeOfParallelism = threads, UnitBytes = 4096 }));
    }

    // ---------------------------------------------------------------- columns

    [Fact]
    public void TheFirstFile_GivesTheColumns_AsDataframeEnginesInfer()
    {
        Write("f/1.jsonl", """
            {"a":1}
            {"a":2}
            """);
        Write("f/2.jsonl", """{"a":3,"b":"x"}""");
        Write("f/3.jsonl", """
            {"a":4}
            {"a":1.5}
            """);
        using JsonDataset firstTwo = JsonDataset.Open([At("f/1.jsonl"), At("f/2.jsonl")]);
        Assert.Equal(["a"], firstTwo.Schema.FieldsList.Select(f => f.Name)); // a later file's field is passed over
        Assert.Equal(["1", "2", "3"], Read(firstTwo));
        using JsonDataset all = JsonDataset.Open(At("f"));
        var e = Assert.Throws<JsonValueException>(() => Read(all));
        Assert.StartsWith($"{At("f/3.jsonl")}: Line 2: Column 'a'", e.Message);
        // An empty first file is passed over.
        Write("e/0.jsonl", "\n\n"); // no record (an empty file is left out of listings anyway)
        Write("e/1.jsonl", """{"a":1}""");
        using JsonDataset empty = JsonDataset.Open(At("e"));
        Assert.Equal(["a"], empty.Schema.FieldsList.Select(f => f.Name));
    }

    [Fact]
    public void UnitedByName_EveryFilesRecordsGiveTheColumns()
    {
        Write("u/1.jsonl", """{"a":1}""");
        Write("u/2.jsonl", """{"b":"x","a":2}""");
        Write("u/3.jsonl", """{"a":1.5,"c":[1]}""");
        using JsonDataset dataset = JsonDataset.Open(At("u"), new JsonDatasetOptions { SchemaMode = DatasetSchemaMode.UnionByName });
        // A file whose records hold more fields than seen so far gives their order (as records do within a file).
        Assert.Equal(["b:StringView", "a:Double", "c:List"], dataset.Schema.FieldsList.Select(f => $"{f.Name}:{f.DataType.TypeId}"));
        Assert.Equal(["null | 1 | null", "\"x\" | 2 | null", "null | 1.5 | [1]"], Read(dataset));
    }

    [Fact]
    public void AGivenSchema_ReadsNoFileToFindTheColumns_AndOverridesTypeInferredOnes()
    {
        Write("s/1.jsonl", """{"a":1,"b":"x"}""");
        var schema = new Schema([new Field("a", DoubleType.Default, false)], null);
        using (JsonDataset dataset = JsonDataset.Open(At("s"), new JsonDatasetOptions { Schema = schema }))
        {
            Assert.True(dataset.Schema.FieldsList[0].IsNullable);
            Assert.Equal(["1"], Read(dataset));
        }
        using (JsonDataset dataset = JsonDataset.Open(At("s"), new JsonDatasetOptions { SchemaOverrides = new Dictionary<string, IArrowType> { ["a"] = Int32Type.Default } }))
            Assert.Equal(Int32Type.Default.TypeId, dataset.Schema.GetFieldByName("a").DataType.TypeId);
        Assert.Throws<ArgumentException>(() => JsonDataset.Open(At("s"), new JsonDatasetOptions { AllowLossyCasts = true }));
        using JsonDataset plain = JsonDataset.Open(At("s"));
        Assert.Throws<ArgumentException>(() => plain.OpenReader(new JsonReadOptions { Schema = schema }));
    }

    [Fact]
    public void Documents_ReadAsOneFileDoes_ALaterFilesUnknownFieldNamed()
    {
        Write("doc/1.json", """[{"a":1},{"a":2}]""");
        Write("doc/2.json", """
            [
              {"a":3},
              {"a":4}
            ]
            """, "gzip");
        Write("doc/3.json", """{"a":5}""");
        var options = new JsonDatasetOptions { Format = JsonFormat.Document };
        using (JsonDataset dataset = JsonDataset.Open(At("doc"), options))
            foreach (int threads in (int[])[1, 8])
                Assert.Equal(["1", "2", "3", "4", "5"], Read(dataset, new JsonReadOptions { MaxDegreeOfParallelism = threads }));
        Write("doc/4.json", """[{"a":6},{"a":7,"z":0}]""");
        using JsonDataset strict = JsonDataset.Open(At("doc"), options);
        var e = Assert.Throws<JsonValueException>(() => Read(strict));
        Assert.StartsWith($"{At("doc/4.json")}: Line 1 (array index 1): ", e.Message);
        Assert.Equal(["1", "2", "3", "4", "5", "6", "7"], Read(strict, new JsonReadOptions { IgnoreErrors = true }));
    }

    // ---------------------------------------------------------------- partitions, filters, limits

    [Fact]
    public void HivePartitions_AreColumns_AndAFilterOnThemPrunesFiles()
    {
        Write("h/year=2023/part.jsonl", Lines(0, 10));
        Write("h/year=2024/part.jsonl", Lines(10, 10));
        Write("h/year=2025/part.jsonl", "not json at all"); // never opened
        using JsonDataset dataset = JsonDataset.Open(At("h"));
        Assert.Equal(["id", "name", "year"], dataset.Schema.FieldsList.Select(f => f.Name));
        var options = new JsonReadOptions { Filter = ScalarExpr.NewBinary(BinaryOp.Eq, Col("year"), Lit(2024)) };
        using JsonDatasetReader reader = dataset.OpenReader(options);
        Assert.Equal([false, true, false], reader.FilesRead);
        Assert.Equal([.. Expected((10, 10)).Select(r => r + " | 2024")], Rows(reader.ReadBatches()));
    }

    [Fact]
    public void ARowFilter_FiltersEveryFile_PartitionValuesInPlace_AndMaxRowsSpansFiles()
    {
        Write("r/k=1/a.jsonl", Lines(0, 3000));
        Write("r/k=2/a.jsonl", Lines(3000, 3000));
        using JsonDataset dataset = JsonDataset.Open(At("r"), new JsonDatasetOptions { FilePathColumn = "file" });
        // A conjunct with a partition column in arithmetic: each file filters with its value in place.
        ScalarExpr filter = ScalarExpr.NewBinary(BinaryOp.And,
            ScalarExpr.NewBinary(BinaryOp.GtEq, Col("id"), ScalarExpr.NewBinary(BinaryOp.Multiply, Col("k"), Lit(1000))),
            ScalarExpr.NewBinary(BinaryOp.Lt, Col("id"), Lit(5500)));
        foreach (int threads in (int[])[1, 8])
        {
            var options = new JsonReadOptions { Filter = filter, FilterRows = true, MaxDegreeOfParallelism = threads, UnitBytes = 2048, Columns = ["id", "k"] };
            using (JsonDatasetReader reader = dataset.OpenReader(options))
            {
                Assert.True(reader.FiltersExactly);
                List<string> rows = Rows(reader.ReadBatches());
                List<string> expected = [.. Enumerable.Range(1000, 2000).Select(i => $"{i} | 1"), .. Enumerable.Range(3000, 2500).Select(i => $"{i} | 2")];
                Assert.Equal(expected.Select(r => r + " | \"" + At("r/k=" + r[^1..] + "/a.jsonl") + "\""), rows);
            }
            using (JsonDatasetReader reader = dataset.OpenReader(options with { MaxRows = 2100 }))
                Assert.Equal(2100, Rows(reader.ReadBatches()).Count);
        }
    }

    [Fact]
    public void AnErrorNamesItsFileAndLine_UnlessARowLimitStopsBeforeIt()
    {
        Write("x/1.jsonl", Lines(0, 5));
        Write("x/2.jsonl", Lines(5, 2) + "{\"id\":\"seven\"}\n" + Lines(8, 2));
        using JsonDataset dataset = JsonDataset.Open(At("x"));
        foreach (int threads in (int[])[1, 8])
        {
            var options = new JsonReadOptions { MaxDegreeOfParallelism = threads };
            var e = Assert.Throws<JsonValueException>(() => Read(dataset, options));
            Assert.StartsWith($"{At("x/2.jsonl")}: Line 3: Column 'id'", e.Message);
            Assert.Equal(Expected((0, 7)), Read(dataset, options with { MaxRows = 7 }));
        }
    }

    [Fact]
    public void AMissingFile_IsNamed()
    {
        Write("m/1.jsonl", Lines(0, 1));
        using JsonDataset dataset = JsonDataset.Open([new JsonDatasetFile(At("m/1.jsonl")), new JsonDatasetFile(At("m/none.jsonl"))],
            new JsonDatasetOptions { Schema = new Schema([new Field("id", Int64Type.Default, true)], null) });
        var e = Assert.ThrowsAny<IOException>(() => Read(dataset));
        Assert.Contains("none.jsonl", e.Message);
    }

    // ---------------------------------------------------------------- stores and rows

    [Fact]
    public void AMemoryStore_IsListedAndGlobbed_AndReadInParallel()
    {
        var store = new MemoryFileStore();
        for (int f = 0; f < 50; f++) store.Add($"data/p{f:D2}.jsonl", Encoding.UTF8.GetBytes(Lines(f * 100, 100)));
        using JsonDataset dataset = JsonDataset.Open("data/*.jsonl", new JsonDatasetOptions { Store = store });
        Assert.Equal(50, dataset.Files.Count);
        foreach (int threads in (int[])[1, 8])
            Assert.Equal(Expected((0, 5000)), Read(dataset, new JsonReadOptions { MaxDegreeOfParallelism = threads }));
    }

    [Fact]
    public void TypedRows_PushTheirPredicate_ToPartitionsAndRows()
    {
        Write("t/year=2023/a.jsonl", Lines(0, 100));
        Write("t/year=2024/a.jsonl", Lines(100, 100));
        using JsonDataset dataset = JsonDataset.Open(At("t"));
        using JsonDatasetReader<Row> reader = dataset.OpenReader<Row>(r => r.Year == 2024 && r.Id < 150);
        Assert.True(reader.FiltersExactly);
        Assert.Equal([false, true], reader.Inner.FilesRead);
        Assert.Equal(Enumerable.Range(100, 50).Select(i => new Row(i, $"n{i}", 2024)), reader.ReadRows());
        Assert.Equal(200, JsonDataset.ReadList<Row>(At("t")).Count);
    }

    [Fact]
    public async Task OnARemoteStore_FilesReadInOrder_FailuresNameTheirFile_AfterTheRowsBefore_AndEverySourceIsClosed()
    {
        var memory = new MemoryFileStore();
        for (int f = 0; f < 20; f++) memory.Add($"s/p{f:D2}.jsonl", Encoding.UTF8.GetBytes(Lines(f * 200, 200)));
        var slow = new Parquet.ParquetDatasetFaultTests.FaultStore(memory) { MaxDelayMs = 3 };
        using (JsonDataset dataset = JsonDataset.Open("s/*.jsonl", new JsonDatasetOptions { Store = slow }))
        {
            foreach (int threads in (int[])[1, 8])
                Assert.Equal(Expected((0, 4000)), Read(dataset, new JsonReadOptions { MaxDegreeOfParallelism = threads, UnitBytes = 1024 }));
            Assert.Equal(Expected((0, 10)), Read(dataset, new JsonReadOptions { MaxRows = 10 }));
        }
        await slow.Settled();

        var failing = new Parquet.ParquetDatasetFaultTests.FaultStore(memory) { FailAt = (path, offset, _) => path.EndsWith("p07.jsonl") && offset > 0 };
        using (JsonDataset dataset = JsonDataset.Open("s/*.jsonl", new JsonDatasetOptions { Store = failing }))
        {
            using JsonDatasetReader reader = dataset.OpenReader(new JsonReadOptions { UnitBytes = 1024 });
            List<string> rows = [];
            var e = Assert.ThrowsAny<IOException>(() =>
            {
                foreach (RecordBatch batch in reader.ReadBatches())
                    using (batch)
                        rows.AddRange(ArrowRender.Rows(batch));
            });
            Assert.StartsWith("s/p07.jsonl: ", e.Message);
            Assert.True(rows.Count >= 1400 && rows.SequenceEqual(Expected((0, rows.Count))));
        }
        await failing.Settled();

        var refusing = new Parquet.ParquetDatasetFaultTests.FaultStore(memory) { FailOpen = path => path.EndsWith("p03.jsonl") };
        using (JsonDataset dataset = JsonDataset.Open("s/*.jsonl", new JsonDatasetOptions { Store = refusing, Schema = new Schema([new Field("id", Int64Type.Default, true)], null) }))
            Assert.StartsWith("s/p03.jsonl: ", Assert.ThrowsAny<IOException>(() => Read(dataset)).Message);
        await refusing.Settled();
    }

    public sealed record Row(long Id, string Name, long Year);
}
