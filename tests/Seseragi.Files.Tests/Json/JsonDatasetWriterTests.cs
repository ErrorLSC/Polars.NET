using System.IO.Compression;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Json;
using Seseragi.Files.Tests.Parquet;

namespace Seseragi.Files.Tests.Json;

/// <summary>
/// <see cref="JsonDatasetWriter"/> and <see cref="JsonDatasetWriter{T}"/>: lines or documents, compressed or not, Hive
/// partitions (nulls in their default directory), files cut by rows and size, read back by <see cref="JsonDataset"/>;
/// typed rows round trip.
/// </summary>
public sealed class JsonDatasetWriterTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-json-dataset-write-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private string At(string relative) => Path.Combine(_root, relative);

    private static RecordBatch Batch(int first, int count)
    {
        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("k", StringType.Default, true), new Field("v", DoubleType.Default, true)], null);
        var k = new StringArray.Builder();
        for (int i = 0; i < count; i++)
            if ((first + i) % 7 == 0) k.AppendNull();
            else k.Append($"p{(first + i) % 3}");
        return new RecordBatch(schema,
            [new Int64Array.Builder().AppendRange(Enumerable.Range(first, count).Select(i => (long)i)).Build(), k.Build(),
             new DoubleArray.Builder().AppendRange(Enumerable.Range(first, count).Select(i => i * 0.5)).Build()], count);
    }

    private static List<string> ReadBack(string path, JsonDatasetOptions? options = null, JsonReadOptions? read = null)
    {
        using JsonDataset dataset = JsonDataset.Open(path, options);
        using JsonDatasetReader reader = dataset.OpenReader(read);
        List<string> rows = [];
        foreach (RecordBatch batch in reader.ReadBatches())
            using (batch)
                rows.AddRange(ArrowRender.Rows(batch));
        return rows;
    }

    [Theory]
    [InlineData(JsonFormat.Lines, JsonCompression.None, ".jsonl")]
    [InlineData(JsonFormat.Document, JsonCompression.None, ".json")]
    [InlineData(JsonFormat.Lines, JsonCompression.Gzip, ".jsonl.gz")]
    [InlineData(JsonFormat.Document, JsonCompression.Zstd, ".json.zst")]
    public void AnUnpartitionedWrite_ReadsBack_FilesCutByRows(JsonFormat format, JsonCompression compression, string extension)
    {
        using RecordBatch a = Batch(0, 5000), b = Batch(5000, 2500);
        var options = new JsonDatasetWriteOptions { Writer = new JsonWriteOptions { Format = format, Compression = compression }, MaxRowsPerFile = 3000 };
        DatasetWriteResult<DatasetWrittenFile> result = JsonDatasetWriter.Write(At("u"), a.Schema, [a, b], options);
        Assert.Equal([3000L, 3000, 1500], result.Files.Select(f => f.Rows));
        Assert.All(result.Files, f => Assert.EndsWith(extension, f.Path));
        Assert.Equal(Enumerable.Range(0, 3).Select(i => $"part-{i:D5}-{result.WriteId}{extension}"), result.Files.Select(f => Path.GetFileName(f.Path)));
        List<string> expected = [.. ArrowRender.Rows(a), .. ArrowRender.Rows(b)];
        Assert.Equal(expected, ReadBack(At("u"), new JsonDatasetOptions { Format = format, Schema = a.Schema }));
        // A file holds the text JsonWriter writes.
        if (compression == JsonCompression.None && format == JsonFormat.Lines)
        {
            using RecordBatch first = a.Slice(0, 3000);
            Assert.Equal(JsonWriter.WriteString(a.Schema, [first]), File.ReadAllText(result.Files[0].Path));
        }
    }

    [Fact]
    public void AnUnpartitionedWrite_CutsFilesBySize()
    {
        using RecordBatch a = Batch(0, 20_000);
        var result = JsonDatasetWriter.Write(At("s"), a.Schema, [a], new JsonDatasetWriteOptions { ApproxBytesPerFile = 100_000 });
        Assert.True(result.Files.Count > 3);
        Assert.All(result.Files.SkipLast(1), f => Assert.InRange(f.Bytes, 90_000, 200_000));
        Assert.All(result.Files, f => Assert.Equal(new FileInfo(f.Path).Length, f.Bytes));
        Assert.Equal(ArrowRender.Rows(a), ReadBack(At("s"), new JsonDatasetOptions { Schema = a.Schema }));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void APartitionedWrite_PutsRowsInHiveDirectories_NullsInTheDefaultOne(bool include)
    {
        using RecordBatch a = Batch(0, 3000), b = Batch(3000, 3000);
        var options = new JsonDatasetWriteOptions { PartitionBy = ["k"], IncludePartitionColumns = include, MaxDegreeOfParallelism = 4 };
        DatasetWriteResult<DatasetWrittenFile> result = JsonDatasetWriter.Write(At("p"), a.Schema, [a, b], options);
        Assert.Equal(["k=__HIVE_DEFAULT_PARTITION__", "k=p0", "k=p1", "k=p2"], result.Files.Select(f => Path.GetFileName(Path.GetDirectoryName(f.Path)!)).Order());
        string firstLine = File.ReadLines(result.Files.First(f => f.Path.Contains("k=p1")).Path).First();
        Assert.Equal(include, firstLine.Contains("\"k\":"));
        // Read back (the partition column from the paths: k last), rows grouped by partition, in order within each.
        List<string> rows = ReadBack(At("p"), new JsonDatasetOptions { PartitionSchema = new Schema([new Field("k", StringType.Default, true)], null) });
        // A partition column the files hold too keeps its place (the path's value in it); else it comes last.
        List<string> expected = [.. ArrowRender.Rows(a).Concat(ArrowRender.Rows(b)).Select(r => r.Split(" | "))
            .Select(c => include ? string.Join(" | ", c) : $"{c[0]} | {c[2]} | {c[1]}")];
        Assert.Equal(expected.Order(), rows.Order());
    }

    // A file's text, decompressed.
    private static string Text(string path, JsonCompression compression)
    {
        using Stream file = File.OpenRead(path);
        using Stream text = compression switch
        {
            JsonCompression.Gzip => new GZipStream(file, CompressionMode.Decompress),
            JsonCompression.Zstd => new ZstdSharp.DecompressionStream(file),
            _ => file,
        };
        using var reader = new StreamReader(text, Encoding.UTF8);
        return reader.ReadToEnd();
    }

    /// <summary>
    /// A partition takes a few rows of each batch (runs encoded at once into its file's piece, pieces written as they
    /// fill): each file holds exactly the text JsonWriter writes for the partition's rows, in order, documents' commas
    /// and brackets across runs and pieces included, compressed files once decompressed.
    /// </summary>
    [Theory]
    [InlineData(JsonFormat.Lines, JsonCompression.None)]
    [InlineData(JsonFormat.Document, JsonCompression.None)]
    [InlineData(JsonFormat.Lines, JsonCompression.Gzip)]
    [InlineData(JsonFormat.Document, JsonCompression.Zstd)]
    public void ManySmallRunsOfAPartition_AreEncodedIntoItsFile_AsJsonWriterWritesThem(JsonFormat format, JsonCompression compression)
    {
        // 40k rows in batches of 2000: each partition takes about 500 rows of each batch; pieces of 16 KiB fill many times.
        RecordBatch[] batches = [.. Enumerable.Range(0, 20).Select(i => Batch(i * 2000, 2000))];
        try
        {
            var writer = new JsonWriteOptions { Format = format, Compression = compression, PieceBytes = 16 << 10 };
            DatasetWriteResult<DatasetWrittenFile> result = JsonDatasetWriter.Write(At("g"), batches[0].Schema, batches,
                new JsonDatasetWriteOptions { PartitionBy = ["k"], Writer = writer, MaxDegreeOfParallelism = 4 });
            Assert.Equal(4, result.Files.Count);
            Assert.Equal(40_000, result.Files.Sum(f => f.Rows));
            // Each row's object as JsonWriter writes it (the files leave the partition column out), by partition.
            var data = new Schema([batches[0].Schema.FieldsList[0], batches[0].Schema.FieldsList[2]], null);
            Dictionary<string, List<string>> objects = [];
            foreach (RecordBatch batch in batches)
            {
                var k = (StringArray)batch.Column(1);
                for (int i = 0; i < batch.Length; i++)
                {
                    var row = new RecordBatch(data, [ArrowArrayFactory.Slice(batch.Column(0), i, 1), ArrowArrayFactory.Slice(batch.Column(2), i, 1)], 1);
                    string key = k.IsNull(i) ? "__HIVE_DEFAULT_PARTITION__" : k.GetString(i);
                    if (!objects.TryGetValue(key, out List<string>? list)) objects[key] = list = [];
                    list.Add(JsonWriter.WriteString(data, [row]).TrimEnd('\n'));
                }
            }
            foreach (DatasetWrittenFile file in result.Files)
            {
                List<string> rows = objects[Path.GetFileName(Path.GetDirectoryName(file.Path)!)["k=".Length..]];
                string expected = format == JsonFormat.Document ? "[" + string.Join(",", rows) + "]" : string.Concat(rows.Select(r => r + "\n"));
                Assert.Equal(expected, Text(file.Path, compression));
                Assert.Equal(new FileInfo(file.Path).Length, file.Bytes);
            }
        }
        finally
        {
            foreach (RecordBatch batch in batches) batch.Dispose();
        }
    }

    /// <summary>
    /// Runs written at once (short ones encoded into the piece being filled) and runs written as they are (long ones,
    /// encoded on the writer's threads) keep their order: the file is the text JsonWriter writes for all the rows.
    /// </summary>
    [Theory]
    [InlineData(JsonFormat.Lines)]
    [InlineData(JsonFormat.Document)]
    public void ShortAndLongRuns_KeepTheirOrder(JsonFormat format)
    {
        RecordBatch[] batches = [Batch(0, 10), Batch(10, 70_000), Batch(70_010, 5), Batch(70_015, 100_000), Batch(170_015, 3)];
        try
        {
            var writer = new JsonWriteOptions { Format = format };
            DatasetWriteResult<DatasetWrittenFile> result = JsonDatasetWriter.Write(At("o"), batches[0].Schema, batches, new JsonDatasetWriteOptions { Writer = writer });
            Assert.Equal(JsonWriter.WriteString(batches[0].Schema, batches, writer), File.ReadAllText(Assert.Single(result.Files).Path));
        }
        finally
        {
            foreach (RecordBatch batch in batches) batch.Dispose();
        }
    }

    /// <summary>A value JSON cannot hold in a short run fails the write, naming its row, and leaves no file.</summary>
    [Fact]
    public void AValueJsonCannotHold_InAShortRun_FailsTheWrite()
    {
        var schema = new Schema([new Field("k", StringType.Default, true), new Field("s", StringType.Default, true)], null);
        var k = new StringArray.Builder();
        var s = new StringArray.Builder();
        for (int i = 0; i < 100; i++)
        {
            k.Append($"p{i % 4}");
            if (i == 57) s.Append([0xC3, 0x28]); // not UTF-8
            else s.Append($"v{i}");
        }
        using var batch = new RecordBatch(schema, [k.Build(), s.Build()], 100);
        var error = Assert.ThrowsAny<Exception>(() => JsonDatasetWriter.Write(At("f"), schema, [batch], new JsonDatasetWriteOptions { PartitionBy = ["k"] }));
        Assert.Contains("Row 14", error.ToString()); // row 57 is partition p1's 15th
        Assert.Empty(Directory.Exists(At("f")) ? Directory.GetFiles(At("f"), "*", SearchOption.AllDirectories) : []);
    }

    [Fact]
    public void ExistingFiles_AreRefusedByDefault_AndAnAbortLeavesNothing()
    {
        using RecordBatch a = Batch(0, 10);
        JsonDatasetWriter.Write(At("e"), a.Schema, [a]);
        Assert.Throws<IOException>(() => JsonDatasetWriter.Write(At("e"), a.Schema, [a]));
        using (JsonDatasetWriter writer = JsonDatasetWriter.Create(At("x"), a.Schema))
        {
            writer.Write(a);
            writer.Abort();
        }
        Assert.Empty(Directory.Exists(At("x")) ? Directory.GetFiles(At("x"), "*", SearchOption.AllDirectories) : []);
    }

    [Fact]
    public void TypedRows_RoundTrip_PartitionedByAnEnum()
    {
        Trade[] rows = ParquetWriterOfTTests.Trades(5000);
        var options = new JsonDatasetWriteOptions { PartitionBy = ["Side"] };
        DatasetWriteResult<DatasetWrittenFile> result = JsonDatasetWriter.WriteRows(At("t"), rows, options);
        Assert.Equal(3, result.Files.Count);
        // GUIDs are written as strings.
        Assert.Contains($"\"Ref\":\"{rows[0].Ref}\"", File.ReadAllText(result.Files.First(f => f.Path.Contains("Side=Buy")).Path));
        List<Trade> read = JsonDataset.ReadList<Trade>(At("t"));
        Assert.Equal(rows.Select(Show).Order(), read.Select(Show).Order());
        using JsonDataset dataset = JsonDataset.Open(At("t"));
        // A predicate on the enum partition compares its names (pushed: the policy here is Strict).
        using JsonDatasetReader<Trade> few = dataset.OpenReader<Trade>(t => t.Id < 100 && t.Fee > 10 && t.Side != Side.Hold);
        Assert.True(few.FiltersExactly);
        Assert.Equal(rows.Where(t => t.Id < 100 && t.Fee > 10 && t.Side != Side.Hold).Select(t => t.Id).Order(), few.ReadRows().Select(t => t.Id).Order());
    }

    // A trade as text, its lists included, its price without trailing zeros.
    private static string Show(Trade t) =>
        $"{t with { Price = t.Price / 1.000000000000000000000000000000m, Tags = null, Legs = null }}|{string.Join(",", t.Tags ?? ["<null>"])}|{string.Join(",", (t.Legs ?? []).Select(l => l?.ToString() ?? "<null>"))}";
}
