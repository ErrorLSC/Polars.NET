using System.Text;
using Apache.Arrow;
using Kaminari.Seseragi.Files.Ipc;
using Kaminari.Seseragi.Files.Json;

namespace Seseragi.Files.Tests.Json;

/// <summary>
/// Columns of every type Python Polars 2.1 writes to JSON, edge values and nulls included (TestData/json/written.arrow,
/// as Polars hands them to Arrow), written as Polars writes them: byte for byte its write_ndjson and write_json
/// (written.ndjson, written.json; make_write_fixtures.py).
/// </summary>
public class JsonWriterOracleTests
{
    private static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "json", name);

    internal static (Schema Schema, List<RecordBatch> Batches) ReadArrow(string name)
    {
        using IpcFile file = IpcFile.Open(Fixture(name));
        using IpcReader reader = file.OpenReader();
        return (reader.Schema, [.. reader.ReadBatches()]);
    }

    [Theory]
    [InlineData("written", JsonFormat.Lines, ".ndjson")]
    [InlineData("written", JsonFormat.Document, ".json")]
    [InlineData("written-empty", JsonFormat.Lines, ".ndjson")]
    [InlineData("written-empty", JsonFormat.Document, ".json")]
    public void TheBytesArePolars(string name, JsonFormat format, string extension)
    {
        var (schema, batches) = ReadArrow(name + ".arrow");
        try
        {
            byte[] expected = File.ReadAllBytes(Fixture(name + extension));
            using var output = new MemoryStream();
            JsonWriter.Write(output, schema, batches, new JsonWriteOptions { Format = format });
            AssertSameText(expected, output.ToArray());
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    // Equal bytes; else the first line that differs, shown.
    internal static void AssertSameText(byte[] expected, byte[] actual)
    {
        if (expected.AsSpan().SequenceEqual(actual)) return;
        string[] e = Encoding.UTF8.GetString(expected).Split('\n'), a = Encoding.UTF8.GetString(actual).Split('\n');
        int line = Enumerable.Range(0, Math.Min(e.Length, a.Length)).FirstOrDefault(i => e[i] != a[i], Math.Min(e.Length, a.Length));
        string el = line < e.Length ? e[line] : "<end>", al = line < a.Length ? a[line] : "<end>";
        int at = Enumerable.Range(0, Math.Min(el.Length, al.Length)).FirstOrDefault(i => el[i] != al[i], Math.Min(el.Length, al.Length));
        Assert.Fail($"Line {line + 1}, at {at}:\nexpected …{el[Math.Max(0, at - 80)..Math.Min(el.Length, at + 80)]}\nactual   …{al[Math.Max(0, at - 80)..Math.Min(al.Length, at + 80)]}");
    }
}
