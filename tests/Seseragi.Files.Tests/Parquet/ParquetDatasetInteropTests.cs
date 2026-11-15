using System.Diagnostics;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Datasets read as Polars reads them (<c>scan_parquet</c>): the files a directory, a glob or several inputs give,
/// in order, with the row index, the file path column and a row limit. Opt-in: SESERAGI_PARQUET_PYTHON (Polars).
/// </summary>
[Trait("Category", "Interop")]
public sealed class ParquetDatasetInteropTests : IDisposable
{
    private readonly string _root = Directory.CreateTempSubdirectory("seseragi-dataset-interop-").FullName;

    public void Dispose() => Directory.Delete(_root, recursive: true);

    private string At(string relative) => Path.Combine(_root, relative);

    private void Write(string relative, long first, int rows, int rowGroupRows)
    {
        string path = At(relative);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);
        var schema = new Apache.Arrow.Schema([new Field("id", Int64Type.Default, true), new Field("name", StringType.Default, true)], null);
        var ids = new Int64Array.Builder();
        var names = new StringArray.Builder();
        for (int i = 0; i < rows; i++)
        {
            ids.Append(first + i);
            if (i % 7 == 3) names.AppendNull();
            else names.Append($"n{first + i}");
        }
        using var writer = ParquetWriter.Create(path, schema, new ParquetWriteOptions { RowGroupRows = rowGroupRows });
        writer.Write(new RecordBatch(schema, [ids.Build(), names.Build()], rows));
        writer.Complete();
    }

    private sealed record Case(string[] Inputs, string? RowIndex, string? FileColumn, long? Rows);

    [Fact]
    public async Task Datasets_ReadAsPolarsReadsThem()
    {
        string? python = Environment.GetEnvironmentVariable("SESERAGI_PARQUET_PYTHON");
        if (string.IsNullOrEmpty(python)) return;
        Write("t/b.parquet", 100, 30, 8);
        Write("t/a.parquet", 0, 5, 1000);
        Write("t/c.parquet", 200, 3, 1000);
        Write("t/sub/d.parquet", 300, 12, 5);
        Write("t/sub/deeper/e.parquet", 400, 4, 1000);
        Write("t/.hidden.parquet", 500, 2, 1000);
        Write("t/_meta.parquet", 600, 2, 1000);
        File.WriteAllBytes(At("t/sub/empty.parquet"), []);
        Dictionary<string, Case> cases = new()
        {
            ["dir"] = new([At("t")], null, null, null),
            ["dir_slash"] = new([At("t") + "/"], null, null, null),
            ["glob_all"] = new([At("t/**/*.parquet")], null, null, null),
            ["glob_top"] = new([At("t/*.parquet")], null, null, null),
            ["glob_one_char"] = new([At("t/?.parquet")], null, null, null),
            ["glob_class"] = new([At("t/[ab].parquet")], null, null, null),
            ["glob_mid"] = new([At("t/*/d.parquet")], null, null, null),
            ["list"] = new([At("t/sub"), At("t/a.parquet"), At("t/*.parquet")], null, null, null),
            ["index_paths"] = new([At("t")], "n", "file", null),
            ["index_paths_limit"] = new([At("t/**/*.parquet")], "n", "file", 41),
            ["limit_zero"] = new([At("t")], "n", null, 0),
        };
        string casesPath = At("cases.json"), outPath = At("polars.json");
        using (var stream = File.Create(casesPath))
        using (var json = new Utf8JsonWriter(stream))
        {
            json.WriteStartObject();
            foreach (var (name, c) in cases)
            {
                json.WriteStartObject(name);
                json.WriteStartArray("inputs");
                foreach (string input in c.Inputs) json.WriteStringValue(input);
                json.WriteEndArray();
                if (c.RowIndex is null) json.WriteNull("row_index"); else json.WriteString("row_index", c.RowIndex);
                if (c.FileColumn is null) json.WriteNull("file_column"); else json.WriteString("file_column", c.FileColumn);
                if (c.Rows is null) json.WriteNull("n_rows"); else json.WriteNumber("n_rows", c.Rows.Value);
                json.WriteEndObject();
            }
            json.WriteEndObject();
        }
        var psi = new ProcessStartInfo(python, ["-I", Path.Combine(AppContext.BaseDirectory, "TestData", "parquet", "dataset_polars.py"), casesPath, outPath])
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using (var process = Process.Start(psi)!)
        {
            Task<string> stderr = process.StandardError.ReadToEndAsync();
            await process.StandardOutput.ReadToEndAsync();
            await process.WaitForExitAsync();
            Assert.True(process.ExitCode == 0, await stderr);
        }

        using JsonDocument polars = JsonDocument.Parse(File.ReadAllText(outPath));
        List<string> differences = [];
        foreach (var (name, c) in cases)
        {
            JsonElement expected = polars.RootElement.GetProperty(name);
            Assert.False(expected.TryGetProperty("error", out JsonElement error), $"{name}: Polars failed: {error}");
            using var dataset = ParquetDataset.Open(c.Inputs, new ParquetDatasetOptions { FilePathColumn = c.FileColumn });
            using var reader = dataset.OpenReader(new ParquetReadOptions { RowIndexColumn = c.RowIndex, MaxRows = c.Rows });
            string[] columns = [.. expected.GetProperty("columns").EnumerateArray().Select(e => e.GetString()!)];
            if (!columns.SequenceEqual(reader.Schema.FieldsList.Select(f => f.Name)))
                differences.Add($"{name}: columns {string.Join(",", reader.Schema.FieldsList.Select(f => f.Name))}, Polars {string.Join(",", columns)}");
            List<string> ours = [];
            foreach (RecordBatch batch in reader.ReadBatches())
            {
                using (batch)
                {
                    for (int i = 0; i < batch.Length; i++)
                        ours.Add(string.Join('|', Enumerable.Range(0, batch.ColumnCount).Select(k => Render(batch.Column(k), i))));
                }
            }
            string[] theirs = [.. expected.GetProperty("rows").EnumerateArray().Select(e => e.GetString()!)];
            if (!ours.SequenceEqual(theirs))
            {
                int at = Enumerable.Range(0, Math.Min(ours.Count, theirs.Length)).FirstOrDefault(i => ours[i] != theirs[i], Math.Min(ours.Count, theirs.Length));
                differences.Add($"{name}: {ours.Count} rows, Polars {theirs.Length}; first difference at {at}: "
                    + $"{(at < ours.Count ? ours[at] : "-")} vs {(at < theirs.Length ? theirs[at] : "-")}");
            }
        }
        Assert.True(differences.Count == 0, string.Join(Environment.NewLine, differences));
    }

    private static string Render(IArrowArray array, int i) => array.IsNull(i) ? "" : array switch
    {
        Int64Array a => a.GetValue(i)!.Value.ToString(System.Globalization.CultureInfo.InvariantCulture),
        StringArray s => s.GetString(i),
        StringViewArray v => v.GetString(i),
        _ => throw new NotSupportedException(array.GetType().Name),
    };
}
