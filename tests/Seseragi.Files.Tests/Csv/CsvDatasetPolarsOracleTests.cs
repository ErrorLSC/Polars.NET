using Apache.Arrow;
using Kaminari.Seseragi.Files.Csv;
using Kaminari.Seseragi.Files.Json;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// CSV datasets as Python Polars 2.1 reads and writes them (TestData/csv/datasets, written by make_dataset_fixtures.py):
/// a list of files and a glob of them read as <c>pl.scan_csv(files, try_parse_dates=True, include_file_paths=...)</c>
/// reads them (the first files' kinds united: integers with floats, dates with datetimes; a gzip-compressed file;
/// quoted separators, quotes and line ends), a value that does not parse past the files inferred failing the read, and
/// the Hive partitions Polars writes (<c>sink_csv(pl.PartitionBy(...))</c>: partition columns in the files and the
/// paths, nulls as <c>__HIVE_DEFAULT_PARTITION__</c>). Rows are compared as NDJSON lines, which our JSON writer writes as
/// Polars does.
/// </summary>
public class CsvDatasetPolarsOracleTests
{
    private static readonly string Root = Path.Combine(AppContext.BaseDirectory, "TestData", "csv", "datasets");

    private static string At(string relative) => Path.Combine(Root, relative);

    private static string[] ListFiles => [.. ((string[])["a.csv", "b.csv", "c.csv", "d.csv.gz"]).Select(n => At(Path.Combine("list", n)))];

    // The dataset's rows as NDJSON lines (paths relative to the fixtures' root).
    private static List<string> Lines(CsvDataset dataset, int threads)
    {
        using CsvDatasetReader reader = dataset.OpenReader(new CsvReadOptions { MaxDegreeOfParallelism = threads, UnitBytes = threads == 1 ? 1 << 20 : 4096 });
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
        using CsvDataset dataset = CsvDataset.Open(ListFiles, new CsvDatasetOptions { FilePathColumn = "file" });
        Assert.Equal(Expected("list.expected.ndjson"), Lines(dataset, threads));
    }

    [Fact]
    public void AGlobOfTheFiles_IsReadAsPolarsScansIt()
    {
        using CsvDataset dataset = CsvDataset.Open(At("list/*"), new CsvDatasetOptions { FilePathColumn = "file" });
        Assert.Equal(Expected("list.expected.ndjson"), Lines(dataset, 8));
    }

    [Fact]
    public void AValueThatDoesNotParsePastTheFilesInferred_FailsTheRead_NamingItsFile()
    {
        using CsvDataset dataset = CsvDataset.Open([ListFiles[0], At("bad.csv")], new CsvDatasetOptions { InferSchemaFiles = 1 });
        var e = Assert.Throws<CsvValueException>(() => Lines(dataset, 1));
        Assert.Contains("bad.csv", e.Message);
        Assert.Equal("x", e.Column);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    public void HivePartitionsPolarsWrites_AreRead(int threads)
    {
        using CsvDataset dataset = CsvDataset.Open(At("part"));
        Assert.Equal(["k", "year"], dataset.PartitionSchema!.FieldsList.Select(f => f.Name));
        Assert.Equal(Expected("part.expected.ndjson").Order(StringComparer.Ordinal), Lines(dataset, threads).Order(StringComparer.Ordinal));
    }
}
