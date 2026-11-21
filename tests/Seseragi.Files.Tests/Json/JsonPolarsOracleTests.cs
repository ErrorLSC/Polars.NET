using System.Numerics;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Json;

namespace Seseragi.Files.Tests.Json;

/// <summary>
/// A messy input (TestData/json/mixed.ndjson: every kind of value, missing fields, varying field order, escapes, blank
/// and CRLF lines) read as Python Polars 2.1 reads it: the same column types and, per column, the same nulls, sums,
/// string bytes and list items (mixed.expected.json, written by make_fixtures.py).
/// </summary>
public class JsonPolarsOracleTests
{
    private static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "json", name);

    [Theory]
    [InlineData(1, 1 << 20)]
    [InlineData(4, 4096)]
    public void PolarsReadsTheSameValues(int threads, int unit)
    {
        using var expected = JsonDocument.Parse(File.ReadAllBytes(Fixture("mixed.expected.json")));
        using var reader = JsonReader.Open(Fixture("mixed.ndjson"), new JsonReadOptions { InferSchemaLength = null, MaxDegreeOfParallelism = threads, UnitBytes = unit });
        var batches = reader.ReadBatches().ToList();
        try
        {
            JsonElement root = expected.RootElement;
            Assert.Equal(root.GetProperty("rows").GetInt32(), batches.Sum(b => b.Length));
            Assert.Equal([.. root.GetProperty("schema").EnumerateObject().Select(p => $"{p.Name}: {p.Value.GetString()}")],
                reader.Schema.FieldsList.Select(f => $"{f.Name}: {PolarsName(f)}"));
            for (int c = 0; c < reader.Schema.FieldsList.Count; c++)
            {
                Field field = reader.Schema.FieldsList[c];
                Check(field.Name, root.GetProperty("columns").GetProperty(field.Name), [.. batches.Select(b => b.Column(c))]);
            }
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    private static void Check(string path, JsonElement expected, IArrowArray[] chunks)
    {
        Assert.True(expected.GetProperty("nulls").GetInt32() == chunks.Sum(a => a.NullCount), $"{path}: nulls");
        IEnumerable<int> Valid(IArrowArray a) => Enumerable.Range(0, a.Length).Where(i => !a.IsNull(i));
        if (expected.TryGetProperty("sum", out JsonElement sum))
        {
            if (sum.ValueKind == JsonValueKind.String)
            {
                BigInteger total = 0;
                foreach (IArrowArray a in chunks)
                    foreach (int i in Valid(a))
                        total += a switch
                        {
                            Int64Array x => x.GetValue(i)!.Value,
                            Decimal128Array d => BigInteger.Parse(d.GetString(i)!),
                            _ => throw new InvalidOperationException(a.GetType().Name),
                        };
                Assert.True(sum.GetString() == total.ToString(), $"{path}: sum {total}");
            }
            else
            {
                double total = chunks.SelectMany(a => Valid(a).Select(i => ((DoubleArray)a).GetValue(i)!.Value)).Sum();
                Assert.True(Math.Abs(sum.GetDouble() - total) < 1e-6 * Math.Max(1, Math.Abs(total)), $"{path}: sum {total}");
            }
        }
        if (expected.TryGetProperty("bytes", out JsonElement bytes))
            Assert.True(bytes.GetInt64() == chunks.Sum(a => Valid(a).Sum(i => (long)Encoding.UTF8.GetByteCount(((StringViewArray)a).GetString(i)))), $"{path}: bytes");
        if (expected.TryGetProperty("true", out JsonElement trues))
            Assert.True(trues.GetInt32() == chunks.Sum(a => Valid(a).Count(i => ((BooleanArray)a).GetValue(i)!.Value)), $"{path}: true");
        if (expected.TryGetProperty("items", out JsonElement items))
            Assert.True(items.GetInt32() == chunks.Sum(a => Valid(a).Sum(i => ((ListArray)a).GetValueLength(i))), $"{path}: items");
        if (expected.TryGetProperty("fields", out JsonElement fields))
        {
            foreach (JsonProperty f in fields.EnumerateObject())
            {
                IArrowArray[] children = [.. chunks.Select(a => ((StructArray)a).Fields[((StructType)a.Data.DataType).GetFieldIndex(f.Name)])];
                // A field of a null struct is null too, as Polars counts it.
                var masked = children.Zip(chunks).Select(p => (p.First, p.Second)).ToList();
                int nulls = masked.Sum(p => Enumerable.Range(0, p.Second.Length).Count(i => p.Second.IsNull(i) || p.First.IsNull(i)));
                Assert.True(f.Value.GetProperty("nulls").GetInt32() == nulls, $"{path}.{f.Name}: nulls");
                var trimmed = JsonDocument.Parse(f.Value.GetRawText().Replace($"\"nulls\": {f.Value.GetProperty("nulls").GetInt32()}", "\"nulls\": -1"));
                CheckValues($"{path}.{f.Name}", f.Value, children);
            }
        }
    }

    // The aggregates other than nulls (a struct's children hold placeholders under null structs, not counted by Polars).
    private static void CheckValues(string path, JsonElement expected, IArrowArray[] chunks)
    {
        if (expected.TryGetProperty("fields", out _) || expected.TryGetProperty("sum", out _) || expected.TryGetProperty("bytes", out _))
        {
            var copy = new Dictionary<string, JsonElement>();
            foreach (JsonProperty p in expected.EnumerateObject()) if (p.Name != "nulls") copy[p.Name] = p.Value;
        }
        if (expected.TryGetProperty("sum", out JsonElement sum) && sum.ValueKind == JsonValueKind.Number)
        {
            double total = chunks.SelectMany(a => Enumerable.Range(0, a.Length).Where(i => !a.IsNull(i)).Select(i => ((DoubleArray)a).GetValue(i)!.Value)).Sum();
            Assert.True(Math.Abs(sum.GetDouble() - total) < 1e-6 * Math.Max(1, Math.Abs(total)), $"{path}: sum {total}");
        }
        if (expected.TryGetProperty("bytes", out JsonElement bytes))
            Assert.True(bytes.GetInt64() == chunks.Sum(a => Enumerable.Range(0, a.Length).Where(i => !a.IsNull(i)).Sum(i => (long)Encoding.UTF8.GetByteCount(((StringViewArray)a).GetString(i)))), $"{path}: bytes");
    }

    private static string PolarsName(Field f) => f.DataType switch
    {
        Int64Type => "Int64",
        DoubleType => "Float64",
        StringViewType => "String",
        BooleanType => "Boolean",
        NullType => "Null",
        Decimal128Type when f.Metadata?.GetValueOrDefault("kaminari:int128") == "i128" => "Int128",
        ListType l => $"List({PolarsName(l.ValueField)})",
        StructType s => "Struct({" + string.Join(", ", s.Fields.Select(c => $"'{c.Name}': {PolarsName(c)}")) + "})",
        _ => f.DataType.Name,
    };
}
