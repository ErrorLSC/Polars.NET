using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Csv;
using Kaminari.Seseragi.Files.Csv.Reading;
using Kaminari.Seseragi.Files.IO;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// The reader's fast paths against its general ones: records tokenized by masks (whole units, 64 bytes at a time) read
/// as the field-at-a-time loop reads them (units of a few bytes never reach the masks), numbers of plain fields decoded
/// in one go as value by value, and units sized by the table's width.
/// </summary>
public class CsvFastPathTests
{
    /// <summary>The schema and values read, or the error's type and message.</summary>
    private static string Outcome(string text, CsvReadOptions options)
    {
        try { return CsvBehaviourTests.Read(Encoding.UTF8.GetBytes(text), options); }
        catch (Exception e) when (e is CsvFormatException or CsvValueException) { return $"{e.GetType().Name}: {e.Message}"; }
    }

    private static string Repeat(string record, int count) => string.Concat(Enumerable.Repeat(record, count));

    // Records shorter than 64 bytes: in 3-byte units every unit holds one record, read field at a time. Failures lie past
    // the 100 rows read at open (whose failures the open raises, field at a time).
    public static TheoryData<string, string> Inputs() => new()
    {
        { "lf", "a,b,c\n" + Repeat("1,22,333\n", 40) + Repeat("x,,z\n", 30) },
        { "crlf", "a,b,c\r\n" + Repeat("1,22,333\r\n", 40) + "4,5,6" },
        { "cr only", "a,b,c\r" + Repeat("1,22,333\r", 40) + "4,5,6\r" },
        { "mixed ends", "a,b\n" + Repeat("1,2\r\n3,4\n5,6\r", 20) },
        { "blank lines", "a,b\n" + Repeat("1,2\n\n3,4\r\n\r\n", 20) },
        { "fewer fields", "a,b,c\n" + Repeat("1,2\n3\n4,5,6\n", 20) },
        { "quoted", "a,b,c\n" + Repeat("\"x,y\",\"q\"\"r\",\"line\nbreak\"\n\"\",z,\"\"\"\"\n", 15) },
        { "quoted last", "a,b\n" + Repeat("1,\"two, three\"\n4,\"5\"\r\n", 20) },
        { "too many fields", "a,b\n" + Repeat("1,2\n", 130) + "3,4,5\n" + Repeat("6,7\n", 10) },
        { "separator last", "a,b\n" + Repeat("1,2\n", 130) + "3,\n" + Repeat("6,7\n", 10) },
        { "quote inside", "a,b\n" + Repeat("1,2\n", 130) + "3,4\"5\n" + Repeat("6,7\n", 10) },
        { "text after quote", "a,b\n" + Repeat("1,2\n", 130) + "3,\"4\"5\n" + Repeat("6,7\n", 10) },
        { "unterminated", "a,b\n" + Repeat("1,2\n", 130) + "3,\"4\n" + Repeat("6,7\n", 10) },
        { "numbers", "i,f,s\n" + Repeat("-12,1.5,a\n+7, 2.25,b\n 8,-.5,c\n1234567890123456,1e3,d\n", 10) },
        { "no line end last", "a,b\n" + Repeat("10,20\n", 30) + "30,40" },
    };

    [Theory]
    [MemberData(nameof(Inputs))]
    public void MaskedRecords_ReadAsFieldAtATime(string name, string text)
    {
        CsvReadOptions[] all =
        [
            new(),
            new() { Columns = ["b"] }, // columns not kept
            new() { HasHeader = false },
        ];
        foreach (CsvReadOptions options in all)
        {
            if (name == "numbers" && options.Columns is not null) continue; // no column b
            string whole = Outcome(text, options), fields = Outcome(text, options with { UnitBytes = 3 });
            Assert.True(whole == fields, $"{name}: masked\n{whole}\nfield at a time\n{fields}");
        }
    }

    [Fact]
    public void MaskedRecords_OtherBytes()
    {
        string semicolons = "a;b\n" + Repeat("1;2\n\"x;y\";z\n", 30);
        Assert.Equal(Outcome(semicolons, new CsvReadOptions { Separator = ';', UnitBytes = 3 }), Outcome(semicolons, new CsvReadOptions { Separator = ';' }));
        string noQuotes = "a,b\n" + Repeat("1,\"2\n3,4\"\n", 30);
        Assert.Equal(Outcome(noQuotes, new CsvReadOptions { Quote = null, UnitBytes = 3 }), Outcome(noQuotes, new CsvReadOptions { Quote = null }));
        string custom = "a,b|" + Repeat("1,2\n3|4,\"5|6\"|", 30);
        Assert.Equal(Outcome(custom, new CsvReadOptions { LineEnd = '|', UnitBytes = 3 }), Outcome(custom, new CsvReadOptions { LineEnd = '|' }));
        Assert.Equal("a:str,b:i64 | \"1\\n2\",3 | \"4\\n5\",6", Outcome("a,b|" + "1\n2,3|4\n5,6|", new CsvReadOptions { LineEnd = '|' }));
    }

    [Fact]
    public void MaskedRecords_QuoteInsideAField_Fails()
    {
        // Read from the field's start as if quoted, x""" would close well before the separator. Its odd quotes run the
        // record's unit to the end of the input (the cut toggles on every quote) whatever the unit size: no field-at-a-time
        // twin to compare with.
        string text = "a,b,c\n" + Repeat("1,2,3\n", 130) + "3,x\"\"\",4\n" + Repeat("6,7,8\n", 40);
        Assert.StartsWith("CsvFormatException", Outcome(text, new CsvReadOptions()));
    }

    [Fact]
    public void MaskedRecords_LongFieldsAcrossWindows()
    {
        string a = new('a', 300), b = new('b', 70);
        string text = "x,y\n" + Repeat($"{a},{b}\n\"{b},{a}\",1\n", 5);
        Assert.Equal("x:str,y:str" + Repeat($" | \"{a}\",\"{b}\" | \"{b},{a}\",\"1\"", 5), Outcome(text, new CsvReadOptions()));
        // A record without a line end after a long field: the end of the input within 64 bytes.
        Assert.Equal($"x:str,y:i64 | \"{a}\",7", Outcome($"x,y\n{a},7", new CsvReadOptions()));
    }

    // ---- numbers of plain fields ----

    private static CsvReadOptions Typed(params (string Name, IArrowType Type)[] columns) =>
        new() { Schema = new Schema([.. columns.Select(c => new Field(c.Name, c.Type, true))], null) };

    [Fact]
    public void PlainNumbers_NullsAndEdges()
    {
        var ints = Typed(("a", Int64Type.Default));
        Assert.Equal("a:i64 | 7 | null | null | 5 | -3", Outcome("a\n 7\n\n\"\"\n+5\n-3", ints));
        Assert.Equal("a:i64 | null | 5", Outcome("a\n0\n5\n", ints with { NullValues = ["0"] }));
        Assert.StartsWith("CsvValueException", Outcome("a\n7 \n", ints));
        Assert.StartsWith("CsvValueException", Outcome("a\n1a345678901\n", ints));
        Assert.StartsWith("CsvValueException", Outcome("a\n123456789a1\n", ints));
        var doubles = Typed(("a", DoubleType.Default));
        Assert.Equal("a:f64 | 1.5 | null | 2", Outcome("a\n1.5\n\"\"\n 2\n", doubles));
        Assert.Equal("a:f64 | null | 2.5", Outcome("a\nNA\n2.5\n", doubles with { NullValues = ["NA"] }));
        Assert.Equal("a:f64 | 1234.5", Outcome("a\n\"1,234.5\"\n", doubles with { ThousandsSeparator = "," }));
        var floats = Typed(("a", FloatType.Default));
        Assert.Equal("a:f32 | 1.5 | null", Outcome("a\n1.5\n\n", floats));
        // Doubled quotes take the value-by-value path (and its error).
        Assert.StartsWith("CsvValueException", Outcome("a\n\"1\"\"2\"\n", ints));
    }

    [Theory]
    [InlineData("1", 1L)]
    [InlineData("12345678", 12345678L)]
    [InlineData("123456789", 123456789L)]
    [InlineData("9999999999999999", 9999999999999999L)]
    [InlineData("12345678901234567", 12345678901234567L)]
    [InlineData("123456789012345678", 123456789012345678L)]
    [InlineData("-9223372036854775808", long.MinValue)]
    [InlineData("0000000000000042", 42L)]
    [InlineData("-0000000000000042", -42L)]
    public void PlainIntegers_EveryLength(string text, long value)
    {
        // Mid-input (8 bytes readable after it) and last (the end of the input right after it).
        Assert.Equal($"a:i64,b:i64 | {value},1 | 1,{value}", Outcome($"a,b\n{text},1\n1,{text}", Typed(("a", Int64Type.Default), ("b", Int64Type.Default))));
    }

    [Fact]
    public void PlainNumbers_LastInTheInput_ReadNothingPastIt()
    {
        // The input ends at a guard page: a read past the last value faults.
        if (!OperatingSystem.IsLinux()) return;
        for (int digits = 1; digits <= 18; digits++)
        {
            string number = string.Concat(Enumerable.Range(1, digits).Select(d => (char)('0' + d % 10)));
            (string Text, IArrowType Type, string Shown)[] cases =
            [
                ($"a\n{number}", Int64Type.Default, $"a:i64 | {long.Parse(number)}"),
                ($"a\n-{number}", Int64Type.Default, $"a:i64 | -{long.Parse(number)}"),
                ($"a\n{number}.5", DoubleType.Default, $"a:f64 | {double.Parse(number + ".5", System.Globalization.CultureInfo.InvariantCulture).ToString("R", System.Globalization.CultureInfo.InvariantCulture)}"),
            ];
            foreach ((string text, IArrowType type, string shown) in cases)
            {
                using var block = Kaminari.Sazanami.Buffers.GuardedMemory.Block.Copy(Encoding.ASCII.GetBytes(text));
                using CsvReader reader = CsvReader.Open((ReadOnlyMemory<byte>)block.Memory, Typed(("a", type)));
                var batches = reader.ReadBatches().ToList();
                try { Assert.Equal(shown, CsvBehaviourTests.Render(reader.Schema, batches)); }
                finally { batches.ForEach(b => b.Dispose()); }
            }
        }
    }

    [Fact]
    public void PlainIntegers_Ranges()
    {
        Assert.StartsWith("CsvValueException", Outcome("a\n2147483648\n", Typed(("a", Int32Type.Default))));
        Assert.Equal("a:i32 | -2147483648", Outcome("a\n-2147483648\n", Typed(("a", Int32Type.Default))));
        Assert.StartsWith("CsvValueException", Outcome("a\n-1\n", Typed(("a", UInt8Type.Default))));
        Assert.Equal("a:u8 | 255", Outcome("a\n255\n", Typed(("a", UInt8Type.Default))));
    }

    // ---- unit sizes ----

    private static int UnitBytes(byte[] bytes, CsvReadOptions options) =>
        CsvInput.UnitBytesAsync(new MemoryRandomAccess(bytes), options, useAsync: false, CancellationToken.None).AsTask().GetAwaiter().GetResult();

    private static byte[] Table(int columns, int bytes)
    {
        var text = new byte[bytes];
        text.AsSpan().Fill((byte)'1');
        byte[] header = Encoding.ASCII.GetBytes(string.Join(",", Enumerable.Range(0, columns).Select(c => $"c{c}")) + "\n");
        header.CopyTo(text, 0);
        return text;
    }

    [Fact]
    public void Units_FollowTheTableWidth()
    {
        const int MiB = 1 << 20;
        byte[] wide = Table(200, 24 * MiB);
        Assert.Equal(6 * MiB, UnitBytes(wide, new CsvReadOptions { MaxDegreeOfParallelism = 1 }));  // a thread's quarter (200 × 64 KiB beyond it)
        Assert.Equal(2 * MiB, UnitBytes(wide, new CsvReadOptions { MaxDegreeOfParallelism = 3 }));
        Assert.Equal(MiB, UnitBytes(wide, new CsvReadOptions { MaxDegreeOfParallelism = 8 }));     // never below the default
        Assert.Equal(CsvInput.MaxUnitBytes, UnitBytes(Table(500, 64 * MiB), new CsvReadOptions { MaxDegreeOfParallelism = 1 }));
        Assert.Equal(48 * 64 << 10, UnitBytes(Table(48, 64 * MiB), new CsvReadOptions { MaxDegreeOfParallelism = 1 }));
        Assert.Equal(MiB, UnitBytes(Table(6, 64 * MiB), new CsvReadOptions { MaxDegreeOfParallelism = 1 }));
        Assert.Equal(MiB, UnitBytes(Table(200, 64 * MiB), new CsvReadOptions { MaxDegreeOfParallelism = 1, Separator = ';' })); // one column of ';'
        Assert.Equal(64, UnitBytes(wide, new CsvReadOptions { UnitBytes = 64 }));                   // set by a test: kept
        byte[] gzip = Table(200, 24 * MiB);
        gzip[0] = 0x1F;
        gzip[1] = 0x8B;
        Assert.Equal(TextUnits.DefaultUnitBytes, UnitBytes(gzip, new CsvReadOptions { MaxDegreeOfParallelism = 1 }));
    }
}
