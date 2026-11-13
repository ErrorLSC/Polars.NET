using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Excel;
using static Seseragi.Files.Tests.TestBatches;

namespace Seseragi.Files.Tests;

public class ExcelWriteTests
{
    private static long Micros(DateTime t) => (t.Ticks - DateTime.UnixEpoch.Ticks) / 10;

    private static int Days(int y, int m, int d) => new DateOnly(y, m, d).DayNumber - 719_162;

    private static IArrowArray Decimal(int precision, int scale, params decimal?[] values)
    {
        Int128?[] raw = [.. values.Select(v => v is { } x ? (Int128?)(Int128)(x * (decimal)Math.Pow(10, scale)) : null)];
        return Fixed(new Decimal128Type(precision, scale), raw);
    }

    private static IArrowArray BigDecimal(int precision, int scale, string unscaled) =>
        Fixed<Int128>(new Decimal128Type(precision, scale), [Int128.Parse(unscaled), 10, null]);

    internal static RecordBatch Wide() => Batch(
        ("b", Booleans(true, false, null), true),
        ("i8", Fixed<sbyte>(Int8Type.Default, [-128, 127, null]), true),
        ("u32", Fixed<uint>(UInt32Type.Default, [0, uint.MaxValue, 1]), true),
        ("i64", Fixed<long>(Int64Type.Default, [1L << 53, -5, null]), true),
        ("big", Fixed<long>(Int64Type.Default, [(1L << 53) + 1, 1, null]), true),
        ("u64", Fixed<ulong>(UInt64Type.Default, [ulong.MaxValue, 2, 3]), true),
        ("f32", Fixed<float>(FloatType.Default, [0.1f, -1.5f, float.NaN]), true),
        ("f64", Fixed<double>(DoubleType.Default, [1e300, double.PositiveInfinity, 2.5]), true),
        ("dec", Decimal(20, 2, 0.10m, 123.45m, null), true),
        ("bigdec", BigDecimal(20, 2, "1234567890123456789"), true),
        ("s", Strings(["  pad ", "a&b<c>\"q\" _x0041_ \u0001 \r\n\U0001F600", null]), true),
        ("dict", Dictionary(["x", "y"], [0, 1, 0]), true),
        ("d32", Fixed<int>(Date32Type.Default, [Days(1900, 1, 1), Days(2024, 2, 29), Days(9999, 12, 31)]), true),
        ("ts_ns", Fixed<long>(new TimestampType(TimeUnit.Nanosecond, (string?)null),
            [Micros(new DateTime(2024, 5, 6, 7, 8, 9).AddTicks(1_234_560)) * 1000, Micros(new DateTime(1900, 3, 1)) * 1000, null]), true),
        ("ts_tz", Fixed<long>(new TimestampType(TimeUnit.Microsecond, "Asia/Tokyo"), [Micros(new DateTime(2024, 1, 1)), null, null]), true),
        ("t64", Fixed<long>(new Time64Type(TimeUnit.Nanosecond), [0, 86_399_999_999_000, 3_723_000_001_000]), true),
        ("t32", Fixed<int>(new Time32Type(TimeUnit.Millisecond), [1000, null, 0]), true),
        ("dur", Fixed<long>(DurationType.Millisecond, [-1500, 108_900_000, 0]), true),
        ("n", new NullArray(3), true));

    private static IArrowArray Booleans(params bool?[] values)
    {
        var b = new BooleanArray.Builder();
        foreach (var v in values)
            if (v is { } x) b.Append(x);
            else b.AppendNull();
        return b.Build();
    }

    private static IArrowArray Dictionary(string[] values, int[] indices) =>
        new DictionaryArray(new DictionaryType(Int32Type.Default, StringType.Default, false),
            Fixed<int>(Int32Type.Default, [.. indices.Select(i => (int?)i)]), Strings(values));

    private static (Schema Schema, List<string> Rows) WriteAndRead(RecordBatch batch, ExcelWriteOptions? options = null, ExcelReadOptions? read = null)
    {
        var ms = new MemoryStream();
        ExcelWriter.Write(ms, batch.Schema, [batch], (options ?? new()) with { LeaveOpen = true });
        ms.Position = 0;
        using var reader = ExcelReader.Open(ms, read);
        return (reader.Schema, ArrowRender.Rows(reader.ReadBatches()));
    }

    private static List<string> Column(Schema schema, List<string> rows, string name)
    {
        int i = schema.GetFieldIndex(name);
        return [.. rows.Select(r => r.Split(" | ")[i])];
    }

    [Theory]
    [InlineData(true, false, false)]
    [InlineData(false, false, false)]
    [InlineData(true, true, true)]
    public void Wide_round_trip(bool sharedStrings, bool autofit, bool freeze)
    {
        var (schema, rows) = WriteAndRead(Wide(), new() { SharedStrings = sharedStrings, Autofit = autofit, FreezeHeader = freeze });
        Assert.Equal(3, rows.Count);
        Assert.Equal(["b", "i8", "u32", "i64", "big", "u64", "f32", "f64", "dec", "bigdec", "s", "dict", "d32", "ts_ns", "ts_tz", "t64", "t32", "dur", "n"],
            schema.FieldsList.Select(f => f.Name));
        string Type(string name) => schema.GetFieldByName(name).DataType.Name;

        Assert.Equal("bool", Type("b"));
        Assert.Equal(["true", "false", "null"], Column(schema, rows, "b"));
        Assert.Equal("int64", Type("i8"));
        Assert.Equal(["-128", "127", "null"], Column(schema, rows, "i8"));
        Assert.Equal(["0", "4294967295", "1"], Column(schema, rows, "u32"));
        Assert.Equal(["9007199254740992", "-5", "null"], Column(schema, rows, "i64"));
        // Beyond 2^53 the value is text, so the column mixes text and numbers: strings.
        Assert.Equal("utf8", Type("big"));
        Assert.Equal(["\"9007199254740993\"", "\"1\"", "null"], Column(schema, rows, "big"));
        Assert.Equal(["\"18446744073709551615\"", "\"2\"", "\"3\""], Column(schema, rows, "u64"));
        // Float32 as its shortest single-precision text; NaN and infinities are #NUM! errors, read as null.
        Assert.Equal("double", Type("f32"));
        Assert.Equal(["0.1", "-1.5", "null"], Column(schema, rows, "f32"));
        Assert.Equal(["1E+300", "null", "2.5"], Column(schema, rows, "f64"));
        Assert.Equal("double", Type("dec"));
        Assert.Equal(["0.1", "123.45", "null"], Column(schema, rows, "dec"));
        Assert.Equal(["\"12345678901234567.89\"", "\"0.1\"", "null"], Column(schema, rows, "bigdec"));
        Assert.Equal(["\"  pad \"", "\"a&b<c>\"q\" _x0041_ \u0001 \r\n\U0001F600\"", "null"], Column(schema, rows, "s"));
        Assert.Equal(["\"x\"", "\"y\"", "\"x\""], Column(schema, rows, "dict"));
        Assert.Equal("date32", Type("d32"));
        Assert.Equal([$"{Days(1900, 1, 1)}", $"{Days(2024, 2, 29)}", $"{Days(9999, 12, 31)}"], Column(schema, rows, "d32"));
        Assert.Equal("timestamp", Type("ts_ns"));
        Assert.Equal([$"{Micros(new DateTime(2024, 5, 6, 7, 8, 9).AddTicks(1_234_560))}", $"{Micros(new DateTime(1900, 3, 1))}", "null"], Column(schema, rows, "ts_ns"));
        // A zoned timestamp is written as the wall-clock time in its zone.
        Assert.Equal([$"{Micros(new DateTime(2024, 1, 1, 9, 0, 0))}", "null", "null"], Column(schema, rows, "ts_tz"));
        Assert.Equal("time64", Type("t64"));
        Assert.Equal(["0", "86399999999", "3723000001"], Column(schema, rows, "t64"));
        Assert.Equal(["1000000", "null", "0"], Column(schema, rows, "t32"));
        Assert.Equal("duration", Type("dur"));
        Assert.Equal(["-1500000", "108900000000", "0"], Column(schema, rows, "dur"));
        Assert.Equal("utf8", Type("n"));
        Assert.Equal(["null", "null", "null"], Column(schema, rows, "n"));
    }

    [Fact]
    public void Microseconds_round_trip_exactly()
    {
        // Every microsecond of a day, at dates up to 2079, reads back as written.
        var rng = new Random(42);
        long[] values = [.. Enumerable.Range(0, 20_000).Select(_ =>
            Micros(new DateTime(1900, 3, 1)) + (long)(rng.NextDouble() * (Micros(new DateTime(2079, 6, 1)) - Micros(new DateTime(1900, 3, 1)))))];
        var batch = Batch(("t", Fixed<long>(new TimestampType(TimeUnit.Microsecond, (string?)null), [.. values.Select(v => (long?)v)]), true));
        var (_, rows) = WriteAndRead(batch);
        Assert.Equal(values.Select(v => v.ToString()), rows);
    }

    [Fact]
    public void Header_can_be_left_out()
    {
        var batch = Batch(("a", Fixed<int>(Int32Type.Default, [1, 2]), true));
        var (schema, rows) = WriteAndRead(batch, new() { IncludeHeader = false }, new() { HasHeader = false });
        Assert.Equal("column_1", schema.FieldsList[0].Name);
        Assert.Equal(["1", "2"], rows);
    }

    [Fact]
    public void Many_batches_and_strings_stream()
    {
        const int rows = 50_000;
        var ms = new MemoryStream();
        var schema = new Schema([new Field("id", Int32Type.Default, true), new Field("s", StringType.Default, true)], null);
        using (var writer = ExcelWriter.Create(ms, schema, new() { LeaveOpen = true }))
        {
            for (int start = 0; start < rows; start += 10_000)
            {
                var ids = Enumerable.Range(start, 10_000).Select(i => (int?)i).ToArray();
                writer.Write(Batch(("id", Fixed<int>(Int32Type.Default, ids), true),
                    ("s", Strings([.. ids.Select(i => i % 2 == 0 ? $"value {i % 1000}" : $"unique {i}")]), true)));
            }
            writer.Complete();
        }
        ms.Position = 0;
        using var reader = ExcelReader.Open(ms, new() { BatchSize = 7_000 });
        var batches = reader.ReadBatches().ToList();
        Assert.Equal(rows, batches.Sum(b => b.Length));
        Assert.All(batches.SkipLast(1), b => Assert.Equal(7_000, b.Length));
        var last = batches[^1];
        Assert.Equal($"\"unique {rows - 1}\"", ArrowRender.Value(last.Column(1), last.Length - 1));
        Assert.Equal("\"value 998\"", ArrowRender.Value(last.Column(1), last.Length - 2));
        Assert.Equal($"{rows - 1}", ArrowRender.Value(last.Column(0), last.Length - 1));
    }

    [Fact]
    public async Task Async_write_and_read()
    {
        var ms = new MemoryStream();
        var batch = Batch(("a", Fixed<int>(Int32Type.Default, [1, null, 3]), true));
        await ExcelWriter.WriteAsync(ms, batch.Schema, One(batch), new() { LeaveOpen = true });
        ms.Position = 0;
        await using var reader = await ExcelReader.OpenAsync(ms, new() { DropEmptyRows = false });
        List<string> rows = [];
        await foreach (var b in reader.ReadBatchesAsync()) rows.AddRange(ArrowRender.Rows(b));
        Assert.Equal(["1", "null", "3"], rows);

        static async IAsyncEnumerable<RecordBatch> One(RecordBatch b)
        {
            await Task.Yield();
            yield return b;
        }
    }

    [Fact]
    public void Types_without_a_cell_form_are_refused()
    {
        var schema = new Schema([new Field("bin", BinaryType.Default, true)], null);
        var e = Assert.Throws<NotSupportedException>(() => ExcelWriter.Create(new MemoryStream(), schema));
        Assert.Contains("bin", e.Message);
    }

    [Theory]
    [InlineData("ns")]
    [InlineData("date")]
    [InlineData("string")]
    public void Values_excel_cannot_hold_are_refused(string what)
    {
        var batch = what switch
        {
            "ns" => Batch(("t", Fixed<long>(new TimestampType(TimeUnit.Nanosecond, (string?)null), [1_000_000_001]), true)),
            "date" => Batch(("d", Fixed<int>(Date32Type.Default, [Days(1899, 12, 31)]), true)),
            _ => Batch(("s", Strings([new string('x', 32_768)]), true)),
        };
        var e = Assert.Throws<NotSupportedException>(() => ExcelWriter.Write(new MemoryStream(), batch.Schema, [batch]));
        Assert.Contains("row 2", e.Message);
    }

    [Fact]
    public void Strings_of_the_limit_are_written()
    {
        string max = string.Concat(Enumerable.Repeat("\U0001F600", 16_383)) + "x"; // 32,767 UTF-16 units
        var (_, rows) = WriteAndRead(Batch(("s", Strings([max]), true)));
        Assert.Equal($"\"{max}\"", Assert.Single(rows));
    }

    [Fact]
    public void Too_many_rows_are_refused()
    {
        var batch = Batch(("n", new NullArray(ExcelCellReferenceMaxRows), true));
        var e = Assert.Throws<NotSupportedException>(() => ExcelWriter.Write(new MemoryStream(), batch.Schema, [batch]));
        Assert.Contains("1,048,576", e.Message);
    }

    private const int ExcelCellReferenceMaxRows = 1_048_576;

    [Theory]
    [InlineData("")]
    [InlineData("a/b")]
    [InlineData("'quoted'")]
    [InlineData("history")]
    [InlineData("this sheet name is far too long!")]
    public void Invalid_sheet_names_are_refused(string name)
    {
        var batch = Batch(("a", Fixed<int>(Int32Type.Default, [1]), true));
        Assert.Throws<ArgumentException>(() => ExcelWriter.Create(new MemoryStream(), batch.Schema, new() { SheetName = name }));
    }

    [Fact]
    public void Sheet_name_and_formats_reach_the_workbook()
    {
        var batch = Batch(("d", Fixed<int>(Date32Type.Default, [Days(2024, 1, 2)]), true));
        var ms = new MemoryStream();
        ExcelWriter.Write(ms, batch.Schema, [batch], new() { SheetName = "Data & more", DateFormat = "dd/mm/yyyy", LeaveOpen = true });
        ms.Position = 0;
        using var wb = ExcelWorkbook.Open(ms, leaveOpen: true);
        Assert.Equal("Data & more", Assert.Single(wb.Sheets).Name);
        using var reader = wb.OpenSheet();
        Assert.Equal("date32", reader.Schema.FieldsList[0].DataType.Name);
        Assert.Equal([$"{Days(2024, 1, 2)}"], ArrowRender.Rows(reader.ReadBatches()));
    }

    [Fact]
    public void Decimal_exactness_compares_values_not_text()
    {
        Assert.True(Kaminari.Seseragi.Files.Excel.Writing.DecimalEncoder.SameValue("1.50"u8, "1.5"u8));
        Assert.True(Kaminari.Seseragi.Files.Excel.Writing.DecimalEncoder.SameValue("1200"u8, "1.2E+3"u8));
        Assert.True(Kaminari.Seseragi.Files.Excel.Writing.DecimalEncoder.SameValue("-0.00"u8, "0"u8));
        Assert.False(Kaminari.Seseragi.Files.Excel.Writing.DecimalEncoder.SameValue("0.1000000000000000055"u8, "0.1"u8));
    }
}
