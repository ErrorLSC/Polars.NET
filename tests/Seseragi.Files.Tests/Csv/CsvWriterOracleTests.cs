using System.Globalization;
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
                "float_scientific" => options with { FloatScientific = option.Value.GetBoolean() },
                "float_precision" => options with { FloatPrecision = option.Value.GetInt32() },
                "decimal_comma" => options with { DecimalComma = option.Value.GetBoolean() },
                "date_format" => options with { DateFormat = option.Value.GetString() },
                "time_format" => options with { TimeFormat = option.Value.GetString() },
                "datetime_format" => options with { DatetimeFormat = option.Value.GetString() },
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
        CsvWriteOptions options = Options(sets.RootElement.GetProperty(set)) ?? throw new InvalidOperationException($"Option set {set} has an option not mapped.");
        var (schema, batches) = ReadFrame(frame);
        try
        {
            byte[] expected = Deviations(File.ReadAllBytes(Fixture($"{frame}.{set}.csv")), schema, options);
            using var output = new MemoryStream();
            CsvWriter.Write(output, schema, batches, options);
            AssertSameText(expected, output.ToArray());
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    // Where the writer departs from Polars on purpose (C1 design rule 8), the expected text as the writer writes it:
    // (a) with the decimal comma and the separator ',', a null float or decimal is the null value, not "" (Polars
    // quotes the empty field); (b) with FloatScientific false, a float (and a half float) is written from its own
    // shortest digits, not from the double's ("0.1", not "0.10000000149011612").
    private static byte[] Deviations(byte[] polars, Schema schema, CsvWriteOptions options)
    {
        bool nulls = options.DecimalComma && options.Separator == ',' && options.QuoteStyle == CsvQuoteStyle.Necessary;
        bool floats = options.FloatScientific == false && options.FloatPrecision is null;
        if (!nulls && !floats) return polars;
        bool[] fractional = [.. schema.FieldsList.Select(f => f.DataType is FloatType or HalfFloatType or DoubleType or Decimal32Type or Decimal64Type or Decimal128Type)];
        bool[] narrow = [.. schema.FieldsList.Select(f => f.DataType is FloatType or HalfFloatType)];
        var text = new StringBuilder();
        string terminator = options.LineTerminator;
        List<List<string>> records = Records(Encoding.UTF8.GetString(polars), options.Separator, options.Quote, terminator);
        for (int r = 0; r < records.Count; r++)
        {
            List<string> fields = records[r];
            if (r > 0 || !options.IncludeHeader)
                for (int k = 0; k < fields.Count && k < fractional.Length; k++)
                {
                    if (nulls && fractional[k] && fields[k] == "\"\"") fields[k] = options.NullValue;
                    else if (floats && narrow[k] && fields[k] is { Length: > 0 } f && f is not ("NaN" or "inf" or "-inf"))
                        fields[k] = OwnDigits(f, options.DecimalComma);
                }
            text.Append(string.Join(options.Separator, fields)).Append(terminator);
        }
        return Encoding.UTF8.GetBytes(text.ToString());
    }

    // A float's own shortest digits (.NET's round-trip text of it; a value exactly halfway between two takes the
    // greater, as the engine's decimal notation does), every digit written out.
    private static string OwnDigits(string polars, bool comma)
    {
        float value = (float)double.Parse(comma ? polars.Replace(',', '.') : polars, CultureInfo.InvariantCulture);
        string r = value.ToString("R", CultureInfo.InvariantCulture);
        bool negative = r.StartsWith('-');
        if (negative) r = r[1..];
        int e = r.IndexOf('E');
        int exponent = e < 0 ? 0 : int.Parse(r[(e + 1)..], CultureInfo.InvariantCulture);
        string mantissa = e < 0 ? r : r[..e];
        int point = mantissa.IndexOf('.');
        string digits = mantissa.Replace(".", "").TrimStart('0');
        int whole = (point < 0 ? mantissa.Length : point) + exponent - (mantissa.Replace(".", "").Length - digits.Length); // digits before the point
        digits = digits.TrimEnd('0');
        if (digits.Length == 0) digits = "0";
        // Exactly halfway: |value| × 10^(digits − whole) × 2 is 2 × digits + 1.
        int bitsExponent = ((BitConverter.SingleToInt32Bits(value) >> 23) & 0xFF) is var b and > 0 ? b - 150 : -149;
        System.Numerics.BigInteger m = (BitConverter.SingleToInt32Bits(value) & 0x7FFFFF) | (((BitConverter.SingleToInt32Bits(value) >> 23) & 0xFF) > 0 ? 0x800000 : 0);
        int scale = digits.Length - whole;
        System.Numerics.BigInteger left = 2 * m, right = 2 * System.Numerics.BigInteger.Parse(digits) + 1;
        if (bitsExponent >= 0) left <<= bitsExponent; else right <<= -bitsExponent;
        if (scale >= 0) left *= System.Numerics.BigInteger.Pow(10, scale); else right *= System.Numerics.BigInteger.Pow(10, -scale);
        if (left == right)
        {
            string up = (System.Numerics.BigInteger.Parse(digits) + 1).ToString(CultureInfo.InvariantCulture);
            if (up.Length > digits.Length) whole++;
            digits = up.TrimEnd('0');
        }
        string positional = whole <= 0 ? "0." + new string('0', -whole) + digits
            : whole >= digits.Length ? digits + new string('0', whole - digits.Length)
            : digits[..whole] + "." + digits[whole..];
        positional = positional.TrimStart('0');
        if (positional.Length == 0 || positional[0] == '.') positional = "0" + positional;
        if (positional.Contains('.')) positional = positional.TrimEnd('0').TrimEnd('.');
        return (negative ? "-" : "") + (comma ? positional.Replace('.', ',') : positional);
    }

    // The raw fields (quotes kept) of each record of delimited text.
    private static List<List<string>> Records(string text, char separator, char quote, string terminator)
    {
        List<List<string>> records = [];
        List<string> fields = [];
        int start = 0, i = 0;
        bool quoted = false;
        while (i < text.Length)
        {
            if (quoted)
            {
                if (text[i] == quote) quoted = !(i + 1 < text.Length && text[i + 1] == quote) ? false : (i++ >= 0);
                i++;
                continue;
            }
            if (text[i] == quote) { quoted = true; i++; continue; }
            if (text[i] == separator) { fields.Add(text[start..i]); start = ++i; continue; }
            if (string.CompareOrdinal(text, i, terminator, 0, terminator.Length) == 0)
            {
                fields.Add(text[start..i]);
                records.Add(fields);
                fields = [];
                start = i += terminator.Length;
                continue;
            }
            i++;
        }
        if (start < text.Length || fields.Count > 0) { fields.Add(text[start..]); records.Add(fields); }
        return records;
    }

    public static TheoryData<string> FormatTargets() => ["date", "time", "naive", "zoned"];

    /// <summary>
    /// chrono format strings, one token (or a few) at a time, on dates, times, naive and zoned timestamps: the text
    /// Polars writes, or — where Polars refuses the format — a refusal when the writer is created.
    /// </summary>
    [Theory]
    [MemberData(nameof(FormatTargets))]
    public void ChronoFormats_WriteWhatPolarsWrites(string target)
    {
        using JsonDocument formats = JsonDocument.Parse(File.ReadAllText(Fixture("formats.json")));
        var (schema, batches) = ReadFrame(target == "zoned" ? "zoned" : "written");
        try
        {
            string[] columns = target switch { "date" => ["date"], "time" => ["time"], "naive" => ["dt_ms", "dt_us", "dt_ns"], _ => [.. schema.FieldsList.Select(f => f.Name)] };
            int[] at = [.. columns.Select(c => schema.GetFieldIndex(c))];
            var picked = new Schema([.. at.Select(i => schema.FieldsList[i])], null);
            List<RecordBatch> parts = [.. batches.Select(b => new RecordBatch(picked, [.. at.Select(b.Column)], b.Length))];
            List<string> failures = [];
            foreach (JsonProperty token in formats.RootElement.GetProperty(target).EnumerateObject())
            {
                string expected = token.Value.GetString()!;
                // Deviation (e): the century of a year past 9999 is its number (100); the engine's time library writes
                // two characters past '9' (":0").
                if (token.Name == "%C") expected = expected.Replace(":0", "100", StringComparison.Ordinal);
                var options = target switch
                {
                    "date" => new CsvWriteOptions { DateFormat = token.Name },
                    "time" => new CsvWriteOptions { TimeFormat = token.Name },
                    _ => new CsvWriteOptions { DatetimeFormat = token.Name },
                };
                string actual;
                try { actual = CsvWriter.WriteString(picked, parts, options); }
                catch (ArgumentException e) { actual = "ERROR: " + e.Message; }
                if (expected.StartsWith("ERROR", StringComparison.Ordinal) != actual.StartsWith("ERROR", StringComparison.Ordinal)
                    || !expected.StartsWith("ERROR", StringComparison.Ordinal) && expected != actual)
                    failures.Add($"{token.Name}:\n  expected {expected.ReplaceLineEndings("⏎")}\n  actual   {actual.ReplaceLineEndings("⏎")}");
            }
            Assert.True(failures.Count == 0, string.Join("\n", failures));
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
