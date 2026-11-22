using Apache.Arrow;
using Kaminari.Seseragi.Files.Json;

namespace Seseragi.Files.Tests.Json;

/// <summary>
/// JSON datasets as Python Polars 2.1 reads and writes them (TestData/json/datasets, written by
/// make_dataset_fixtures.py): a list of files and a glob of them read as <c>pl.scan_ndjson(files,
/// include_file_paths=...)</c> reads them (types from the first file, its later fields dropped, missing ones null, a
/// gzip-compressed file), a value that does not parse failing the read, and the Hive partitions Polars writes
/// (<c>sink_ndjson(pl.PartitionBy(...))</c>: partition columns in the files and the paths, nulls as
/// <c>__HIVE_DEFAULT_PARTITION__</c>). Rows are compared as NDJSON lines, which our writer writes as Polars does.
/// </summary>
public class JsonDatasetPolarsOracleTests
{
    private static readonly string Root = Path.Combine(AppContext.BaseDirectory, "TestData", "json", "datasets");

    private static string At(string relative) => Path.Combine(Root, relative);

    private static string[] ListFiles => [.. ((string[])["a.jsonl", "b.jsonl", "c.jsonl", "d.jsonl.gz"]).Select(n => At(Path.Combine("list", n)))];

    // The dataset's rows as NDJSON lines (paths relative to the fixtures' root).
    private static List<string> Lines(JsonDataset dataset, int threads)
    {
        using JsonDatasetReader reader = dataset.OpenReader(new JsonReadOptions { MaxDegreeOfParallelism = threads, UnitBytes = threads == 1 ? 1 << 20 : 4096 });
        List<RecordBatch> batches = [.. reader.ReadBatches()];
        try
        {
            using var text = new StringWriter();
            JsonWriter.Write(text, reader.Schema, batches);
            return [.. text.ToString().Replace(Root + Path.DirectorySeparatorChar, "").Split('\n', StringSplitOptions.RemoveEmptyEntries)];
        }
        finally
        {
            batches.ForEach(b => b.Dispose());
        }
    }

    private static string[] Expected(string name) => File.ReadAllLines(At(name));

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public void AListOfFiles_IsReadAsPolarsScansIt(int threads)
    {
        using JsonDataset dataset = JsonDataset.Open(ListFiles, new JsonDatasetOptions { FilePathColumn = "file" });
        Assert.Equal(Expected("list.expected.ndjson"), Lines(dataset, threads));
    }

    [Fact]
    public void AGlobOfTheFiles_IsReadAsPolarsScansIt()
    {
        using JsonDataset dataset = JsonDataset.Open(At("list/*"), new JsonDatasetOptions { FilePathColumn = "file" });
        Assert.Equal(Expected("list.expected.ndjson"), Lines(dataset, 8));
    }

    [Fact]
    public void AValueThatDoesNotParse_FailsTheRead_NamingItsFile()
    {
        using JsonDataset dataset = JsonDataset.Open([ListFiles[0], At("bad.jsonl")]);
        Exception e = Assert.ThrowsAny<Exception>(() => Lines(dataset, 1));
        Assert.Contains("bad.jsonl", e.ToString());
    }

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public void HivePartitionsPolarsWrites_AreRead(int threads)
    {
        using JsonDataset dataset = JsonDataset.Open(At("part"));
        Assert.Equal(["k", "year"], dataset.PartitionSchema!.FieldsList.Select(f => f.Name));
        Assert.Equal(Expected("part.expected.ndjson").Order(StringComparer.Ordinal), Lines(dataset, threads).Order(StringComparer.Ordinal));
    }
}
