using System.IO.Compression;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Csv;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// <see cref="CsvDatasetWriter"/>: files compressed or not (<c>.csv</c>, <c>.csv.gz</c>, <c>.csv.zst</c>), each with its
/// own header (and byte order mark, encoding), Hive partitions (the partition columns kept in the files by default, nulls
/// in their default directory), files cut by rows, an empty write writing no file, read back by <see cref="CsvDataset"/>;
/// a partition's file is exactly the text <see cref="CsvWriter"/> writes for its rows.
/// </summary>
public sealed class CsvDatasetWriterTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-csv-dataset-write-").FullName;

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

    private static List<string> ReadBack(string path, CsvDatasetOptions? options = null)
    {
        using CsvDataset dataset = CsvDataset.Open(path, options);
        using CsvDatasetReader reader = dataset.OpenReader();
        List<string> rows = [];
        foreach (RecordBatch batch in reader.ReadBatches())
            using (batch)
                rows.AddRange(ArrowRender.Rows(batch));
        return rows;
    }

    // A file's bytes, decompressed.
    private static byte[] Bytes(string path, CsvCompression compression)
    {
        using Stream file = File.OpenRead(path);
        using Stream text = compression switch
        {
            CsvCompression.Gzip => new GZipStream(file, CompressionMode.Decompress),
            CsvCompression.Zstd => new ZstdSharp.DecompressionStream(file),
            _ => file,
        };
        using var copy = new MemoryStream();
        text.CopyTo(copy);
        return copy.ToArray();
    }

    [Theory]
    [InlineData(CsvCompression.None, ".csv")]
    [InlineData(CsvCompression.Gzip, ".csv.gz")]
    [InlineData(CsvCompression.Zstd, ".csv.zst")]
    public void AnUnpartitionedWrite_ReadsBack_FilesCutByRows_EachWithItsHeader(CsvCompression compression, string extension)
    {
        using RecordBatch a = Batch(0, 5000), b = Batch(5000, 2500);
        var options = new CsvDatasetWriteOptions { Writer = new CsvWriteOptions { Compression = compression }, MaxRowsPerFile = 3000 };
        DatasetWriteResult<DatasetWrittenFile> result = CsvDatasetWriter.Write(At("u"), a.Schema, [a, b], options);
        Assert.Equal([3000L, 3000, 1500], result.Files.Select(f => f.Rows));
        Assert.Equal(Enumerable.Range(0, 3).Select(i => $"part-{i:D5}-{result.WriteId}{extension}"), result.Files.Select(f => Path.GetFileName(f.Path)));
        Assert.All(result.Files, f => Assert.StartsWith("id,k,v\n", Encoding.UTF8.GetString(Bytes(f.Path, compression))));
        var schema = new Schema([new Field("id", Int64Type.Default, true), new Field("k", StringType.Default, true), new Field("v", DoubleType.Default, true)], null);
        Assert.Equal([.. ArrowRender.Rows(a), .. ArrowRender.Rows(b)], ReadBack(At("u"), new CsvDatasetOptions { Schema = schema }));
    }

    [Theory]
    [InlineData(null)]
    [InlineData(false)]
    public void APartitionedWrite_KeepsThePartitionColumnInTheFilesByDefault(bool? include)
    {
        using RecordBatch a = Batch(0, 3000), b = Batch(3000, 3000);
        var options = new CsvDatasetWriteOptions { PartitionBy = ["k"], MaxDegreeOfParallelism = 4 };
        if (include is { } given) options = options with { IncludePartitionColumns = given };
        DatasetWriteResult<DatasetWrittenFile> result = CsvDatasetWriter.Write(At("p"), a.Schema, [a, b], options);
        Assert.Equal(["k=__HIVE_DEFAULT_PARTITION__", "k=p0", "k=p1", "k=p2"], result.Files.Select(f => Path.GetFileName(Path.GetDirectoryName(f.Path)!)).Order());
        string header = File.ReadLines(result.Files.First(f => f.Path.Contains("k=p1")).Path).First();
        Assert.Equal(include ?? true ? "id,k,v" : "id,v", header);
        // Read back: the partition column from the paths (in its place when the files hold it too, else last).
        var schema = new Schema([new Field("id", Int64Type.Default, true), .. include ?? true ? [new Field("k", StringType.Default, true)] : (Field[])[],
            new Field("v", DoubleType.Default, true)], null);
        List<string> rows = ReadBack(At("p"), new CsvDatasetOptions { Schema = schema, PartitionSchema = new Schema([new Field("k", StringType.Default, true)], null) });
        List<string> expected = [.. ArrowRender.Rows(a).Concat(ArrowRender.Rows(b)).Select(r => r.Split(" | "))
            .Select(c => include ?? true ? string.Join(" | ", c) : $"{c[0]} | {c[2]} | {c[1]}")];
        Assert.Equal(expected.Order(), rows.Order());
    }

    [Fact]
    public void AnEmptyWrite_WritesNoFile()
    {
        using RecordBatch a = Batch(0, 0);
        DatasetWriteResult<DatasetWrittenFile> result = CsvDatasetWriter.Write(At("e"), a.Schema, [a], new CsvDatasetWriteOptions { PartitionBy = ["k"] });
        Assert.Empty(result.Files);
    }

    /// <summary>
    /// A partition takes a few rows of each batch (runs encoded at once into its file's piece, pieces written as they
    /// fill): each file holds exactly the bytes CsvWriter writes for the partition's rows, in order — its header, byte
    /// order mark and encoding included — compressed files once decompressed.
    /// </summary>
    [Theory]
    [InlineData(CsvCompression.None, null)]
    [InlineData(CsvCompression.Gzip, null)]
    [InlineData(CsvCompression.Zstd, 932)]
    [InlineData(CsvCompression.None, 1200)]
    public void ManySmallRunsOfAPartition_AreEncodedIntoItsFile_AsCsvWriterWritesThem(CsvCompression compression, int? codePage)
    {
        RecordBatch[] batches = [.. Enumerable.Range(0, 20).Select(i => Batch(i * 2000, 2000))];
        try
        {
            var writer = new CsvWriteOptions
            {
                Compression = compression, PieceBytes = 16 << 10, IncludeBom = codePage is 1200,
                Encoding = codePage is int page ? Kaminari.Sazanami.Text.TextEncodings.Resolve(page) : null,
            };
            DatasetWriteResult<DatasetWrittenFile> result = CsvDatasetWriter.Write(At("g"), batches[0].Schema, batches,
                new CsvDatasetWriteOptions { PartitionBy = ["k"], Writer = writer, MaxDegreeOfParallelism = 4 });
            Assert.Equal(4, result.Files.Count);
            Assert.Equal(40_000, result.Files.Sum(f => f.Rows));
            // Each partition's rows (the partition column kept), written by one CsvWriter.
            Dictionary<string, List<RecordBatch>> rows = [];
            foreach (RecordBatch batch in batches)
            {
                var k = (StringArray)batch.Column(1);
                for (int i = 0; i < batch.Length; i++)
                {
                    string key = k.IsNull(i) ? "__HIVE_DEFAULT_PARTITION__" : k.GetString(i);
                    if (!rows.TryGetValue(key, out List<RecordBatch>? list)) rows[key] = list = [];
                    list.Add(batch.Slice(i, 1));
                }
            }
            foreach (DatasetWrittenFile file in result.Files)
            {
                using var expected = new MemoryStream();
                using (CsvWriter one = CsvWriter.Create(expected, batches[0].Schema, writer with { Compression = CsvCompression.None, LeaveOpen = true, MaxDegreeOfParallelism = 1 }))
                {
                    foreach (RecordBatch row in rows[Path.GetFileName(Path.GetDirectoryName(file.Path)!)["k=".Length..]]) one.Write(row);
                    one.Complete();
                }
                Assert.Equal(expected.ToArray(), Bytes(file.Path, compression));
                Assert.Equal(new FileInfo(file.Path).Length, file.Bytes);
            }
        }
        finally
        {
            foreach (RecordBatch batch in batches) batch.Dispose();
        }
    }
}
