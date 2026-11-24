using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Data.Building;
using Kaminari.Seseragi.Files.Csv;
using Kaminari.Seseragi.Files.Ipc;
using static Seseragi.Files.Tests.Json.JsonWriterOracleTests;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// What Python Polars 2.1 writes (TestData/csv-write, made by make_write_fixtures.py): every frame written with each of
/// its option sets, byte for byte.
/// </summary>
public class CsvWriterOracleTests
{
    private static string Fixture(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", "csv-write", name);

    private static (Schema Schema, List<RecordBatch> Batches) ReadFrame(string frame)
    {
        using IpcFile file = IpcFile.Open(Fixture(frame + ".arrow"));
        using IpcReader reader = file.OpenReader();
        return (reader.Schema, [.. reader.ReadBatches()]);
    }

    // Polars' write_csv options as the writer's; null when the set has an option the writer does not take yet.
    private static CsvWriteOptions? Options(JsonElement set)
    {
        var options = new CsvWriteOptions();
        foreach (JsonProperty option in set.EnumerateObject())
        {
            CsvWriteOptions? next = option.Name switch
            {
                "include_header" => options with { IncludeHeader = option.Value.GetBoolean() },
                "include_bom" => options with { IncludeBom = option.Value.GetBoolean() },
                "separator" => options with { Separator = option.Value.GetString()![0] },
                "quote_char" => options with { Quote = option.Value.GetString()![0] },
                "quote_style" => options with
                {
                    QuoteStyle = option.Value.GetString() switch
                    {
                        "always" => CsvQuoteStyle.Always,
                        "non_numeric" => CsvQuoteStyle.NonNumeric,
                        "never" => CsvQuoteStyle.Never,
                        _ => CsvQuoteStyle.Necessary,
                    },
                },
                "null_value" => options with { NullValue = option.Value.GetString()! },
                "line_terminator" => options with { LineTerminator = option.Value.GetString()! },
                _ => null,
            };
            if (next is null) return null;
            options = next;
        }
        return options;
    }

    public static TheoryData<string, string> Files()
    {
        var data = new TheoryData<string, string>();
        foreach (string path in Directory.EnumerateFiles(Path.Combine(AppContext.BaseDirectory, "TestData", "csv-write"), "*.*.csv").Order())
        {
            string[] parts = Path.GetFileName(path).Split('.');
            data.Add(parts[0], parts[1]);
        }
        return data;
    }

    [Theory]
    [MemberData(nameof(Files))]
    public void TheBytesArePolars(string frame, string set)
    {
        using JsonDocument sets = JsonDocument.Parse(File.ReadAllText(Fixture("options.json")));
        if (Options(sets.RootElement.GetProperty(set)) is not { } options) return; // an option of a later step (W2)
        var (schema, batches) = ReadFrame(frame);
        try
        {
            byte[] expected = File.ReadAllBytes(Fixture($"{frame}.{set}.csv"));
            using var output = new MemoryStream();
            CsvWriter.Write(output, schema, batches, options);
            AssertSameText(expected, output.ToArray());
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    /// <summary>128-bit integers (decimal(38, 0) marked as integers) as Polars writes Int128 and UInt128.</summary>
    [Fact]
    public void Int128_AndUInt128_AreWrittenAsPolarsWritesThem()
    {
        Int128?[] signed = [Int128.MinValue, Int128.MaxValue, 0, null, -1];
        UInt128?[] unsigned = [0, UInt128.MaxValue, null, 1, UInt128.Parse("1000000000000000000000000000000")];
        var schema = new Schema([Wide("i128", WideIntegers.Signed), Wide("u128", WideIntegers.Unsigned)], null);
        using var batch = new RecordBatch(schema, [Decimals([.. signed.Select(v => v is { } x ? x : (Int128?)null)]), Decimals([.. unsigned.Select(v => v is { } x ? (Int128)x : (Int128?)null)])], 5);
        AssertSameText(File.ReadAllBytes(Fixture("int128.csv")), Encoding.UTF8.GetBytes(CsvWriter.WriteString(schema, [batch])));

        static Field Wide(string name, string kind) => new(name, WideIntegers.Type, true, [new(WideIntegers.MetadataKey, kind)]);
    }

    // A decimal(38, 0) array of 128-bit values (two's complement), nulls where null.
    private static Decimal128Array Decimals(Int128?[] values)
    {
        byte[] data = new byte[16 * values.Length];
        byte[] valid = new byte[(values.Length + 7) / 8];
        int nulls = 0;
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is { } v)
            {
                BitConverter.TryWriteBytes(data.AsSpan(16 * i), (ulong)(v & ulong.MaxValue));
                BitConverter.TryWriteBytes(data.AsSpan(16 * i + 8), (ulong)(v >> 64));
                valid[i / 8] |= (byte)(1 << (i % 8));
            }
            else nulls++;
        }
        return new Decimal128Array(new ArrayData(WideIntegers.Type, values.Length, nulls, 0, [new ArrowBuffer(valid), new ArrowBuffer(data)]));
    }
}
