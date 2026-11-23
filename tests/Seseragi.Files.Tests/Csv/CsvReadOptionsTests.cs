using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Csv;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// The managed CSV reader's own options and edges the probe table does not hold: thousands separators (rule 9), true
/// and false values, dictionaries across batches, errors past the inferred rows or past MaxRows, header edges.
/// </summary>
public class CsvReadOptionsTests
{
    private static string Read(string text, CsvReadOptions? options = null) =>
        CsvBehaviourTests.Read(Encoding.UTF8.GetBytes(text), options ?? new CsvReadOptions());

    private static Schema SchemaOf(params (string Name, IArrowType Type)[] columns) => new([.. columns.Select(c => new Field(c.Name, c.Type, true))], null);

    [Fact]
    public void ThousandsWithDecimalComma_InferredAsNumbers()
    {
        var options = new CsvReadOptions { Separator = ';', DecimalComma = true, ThousandsSeparator = "." };
        Assert.Equal("a:f64,b:i64 | 1234.56,1234567 | 7.5,12 | -1234.5,null",
            Read("a;b\n1.234,56;1.234.567\n7,5;12\n-1.234,5;\n", options));
    }

    [Theory]
    [InlineData(" ", "1 234,5")]
    [InlineData(" ", "1 234,5")]
    [InlineData(" ", "1 234,5")]
    public void ThousandsSpaces(string separator, string value) =>
        Assert.Equal("a:f64 | 1234.5 | 12000000.25",
            Read($"a\n{value}\n12{separator}000{separator}000,25\n", new CsvReadOptions { Separator = ';', DecimalComma = true, ThousandsSeparator = separator }));

    [Fact]
    public void ThousandsApostropheAndQuotedComma()
    {
        Assert.Equal("a:f64 | 1234.5", Read("a\n1'234.5\n", new CsvReadOptions { ThousandsSeparator = "'" }));
        Assert.Equal("a:f64,b:i64 | 1234.5,1 | 1000000,2", Read("a,b\n\"1,234.5\",1\n\"1,000,000\",2\n", new CsvReadOptions { ThousandsSeparator = "," }));
    }

    [Fact]
    public void ThousandsGroupsAreChecked()
    {
        var options = new CsvReadOptions { Separator = ';', DecimalComma = true, ThousandsSeparator = "." };
        // "1.5" and "12.34" are no grouped numbers: strings when inferred, refused when typed.
        Assert.Equal("a:str | \"1.5\" | \"12.34\" | \"1234.567\"", Read("a\n1.5\n12.34\n1234.567\n", options));
        Assert.Equal("a:str | \"1.2345\"", Read("a\n1.2345\n", options));
        Assert.Equal("a:str | \"1234.567\"", Read("a\n1234.567\n", options));
        Assert.Throws<CsvValueException>(() => Read("a\n1234.567\n", options with { Schema = SchemaOf(("a", Int64Type.Default)) }));
        var e = Assert.Throws<CsvValueException>(() => Read("a\n1.5\n", options with { Schema = SchemaOf(("a", DoubleType.Default)) }));
        Assert.Contains("'1.5' is not a floating-point number", e.Message);
        Assert.Equal("a:i64 | 1234567", Read("a\n1.234.567\n", options with { Schema = SchemaOf(("a", Int64Type.Default)) }));
        Assert.Throws<CsvValueException>(() => Read("a\n1.2345\n", options with { Schema = SchemaOf(("a", Int64Type.Default)) }));
        Assert.Throws<CsvValueException>(() => Read("a\n.123\n", options with { Schema = SchemaOf(("a", Int64Type.Default)) }));
    }

    [Fact]
    public void ThousandsDecimalsAndWideIntegers()
    {
        var options = new CsvReadOptions { Separator = ';', DecimalComma = true, ThousandsSeparator = "." };
        Assert.Equal("a:decimal[12,2] | 1234567.89", Read("a\n1.234.567,89\n", options with { Schema = SchemaOf(("a", new Decimal128Type(12, 2))) }));
    }

    [Theory]
    [InlineData(".", false)]
    [InlineData(",", true)]
    [InlineData("", false)]
    [InlineData("1", false)]
    public void ThousandsSeparatorRefusedWhenAmbiguous(string separator, bool decimalComma) =>
        Assert.Throws<ArgumentException>(() => Read("a\n1\n", new CsvReadOptions { ThousandsSeparator = separator, DecimalComma = decimalComma, Separator = ';' }));

    [Fact]
    public void TrueAndFalseValues()
    {
        var options = new CsvReadOptions { Separator = ';', TrueValues = ["VERDADEIRO", "SIM"], FalseValues = ["FALSO"] };
        Assert.Equal("a:bool,b:str | true,\"x\" | false,\"y\" | true,null | true,\"z\"", Read("a;b\nVERDADEIRO;x\nFALSO;y\nSIM;\ntrue;z\n", options));
        Assert.Equal("a:bool | false | true", Read("a\nFALSO\nVERDADEIRO\n", options with { Schema = SchemaOf(("a", BooleanType.Default)) }));
        Assert.Equal("a:bool | false | true", Read("a\nFALSO\nVERDADEIRO\n", options));
        var e = Assert.Throws<CsvValueException>(() => Read("a\nverdadeiro\n", options with { Schema = SchemaOf(("a", BooleanType.Default)) }));
        Assert.Contains("or a true or false value given", e.Message);
        // Without them the words are strings.
        Assert.Equal("a:str | \"VERDADEIRO\"", Read("a\nVERDADEIRO\n", new CsvReadOptions()));
    }

    [Fact]
    public void CategoricalAcrossBatches()
    {
        var text = new StringBuilder("x\n");
        for (int i = 0; i < 300; i++) text.Append(i % 7 == 0 ? "\n" : $"v{i % 5 + (i / 100) * 10}\n");
        var categorical = new CsvReadOptions { Schema = new Schema([new Field("x", new DictionaryType(Int32Type.Default, StringType.Default, false), true)], null), BatchSize = 37 };
        string strings = Read(text.ToString(), new CsvReadOptions { BatchSize = 37 });
        Assert.Equal(strings.Replace("x:str", "x:cat"), Read(text.ToString(), categorical));
        Assert.Equal(strings.Replace("x:str", "x:cat"), CsvBehaviourTests.Read(Encoding.UTF8.GetBytes(text.ToString()), categorical with { UnitBytes = 64 }, CsvBehaviourTests.CsvSource.Memory));
    }

    [Fact]
    public void NotUtf8PastTheInferredRows()
    {
        var text = new StringBuilder("a,b\n");
        for (int i = 0; i < 300; i++) text.Append($"{i},x\n");
        byte[] bytes = [.. Encoding.UTF8.GetBytes(text.ToString()), .. "7,Jos"u8, 0xE9, .. "\n8,y\n"u8];
        foreach (int unit in (int[])[1 << 20, 64, 5])
        {
            var e = Assert.Throws<CsvFormatException>(() => CsvBehaviourTests.Read(bytes, new CsvReadOptions { UnitBytes = unit }, CsvBehaviourTests.CsvSource.Memory));
            Assert.Equal("Row 301 (line 302): the text is not UTF-8 (give the input's encoding)", e.Message);
            Assert.Equal(301, e.Row);
            Assert.Equal(302, e.Line);
        }
    }

    [Fact]
    public void ErrorsPastMaxRowsAreNotRaised()
    {
        Assert.Equal("a:i64 | 1 | 2", Read("a\n1\n2\n3\nx\n", new CsvReadOptions { MaxRows = 2, Schema = SchemaOf(("a", Int64Type.Default)) }));
        // The rows the types are inferred from are read whatever MaxRows says (as natively: f1_n_rows_* cases).
        Assert.Throws<CsvFormatException>(() => Read("a\n1\n2\n3\n4,5,6\n", new CsvReadOptions { MaxRows = 2 }));
        var past = new StringBuilder("a\n");
        for (int i = 0; i < 200; i++) past.Append(i).Append('\n');
        Assert.Equal(2, Read(past + "4,5,6\n", new CsvReadOptions { MaxRows = 2 }).Split(" | ").Length - 1);
        Assert.Equal("a:i64", Read("a\n1\nx\n", new CsvReadOptions { MaxRows = 0, Schema = SchemaOf(("a", Int64Type.Default)) }));
    }

    [Fact]
    public void ErrorsPastMaxRowsInALaterUnitAreNotRaised()
    {
        foreach (int unit in (int[])[1, 2, 3, 5, 1 << 20])
            Assert.Equal("a:i64 | 1 | 2 | 3", CsvBehaviourTests.Read("a\n1\n2\n3\nx\n4\n"u8.ToArray(),
                new CsvReadOptions { MaxRows = 3, Schema = SchemaOf(("a", Int64Type.Default)), UnitBytes = unit }, CsvBehaviourTests.CsvSource.Memory));
    }

    [Fact]
    public void TheFirstErrorInRecordOrderWins_NotTheFirstColumns()
    {
        var e = Assert.Throws<CsvValueException>(() => Read("a,b\n1,2\nx,3\n4,y\n", new CsvReadOptions { Schema = SchemaOf(("a", Int64Type.Default), ("b", Int64Type.Default)) }));
        Assert.Equal("Row 2 (line 3): column 'a': 'x' is not an Int64 integer", e.Message);
        e = Assert.Throws<CsvValueException>(() => Read("a,b\n1,2\n3,y\nx,4\n", new CsvReadOptions { Schema = SchemaOf(("a", Int64Type.Default), ("b", Int64Type.Default)) }));
        Assert.Equal("Row 2 (line 3): column 'b': 'y' is not an Int64 integer", e.Message);
    }

    [Theory]
    [InlineData("9223372036854775807", typeof(long), "9223372036854775807")]
    [InlineData("-9223372036854775808", typeof(long), "-9223372036854775808")]
    [InlineData("9223372036854775808", typeof(long), null)]
    [InlineData("-9223372036854775809", typeof(long), null)]
    [InlineData("00000000000000000000042", typeof(long), "42")]
    [InlineData("18446744073709551615", typeof(ulong), "18446744073709551615")]
    [InlineData("18446744073709551616", typeof(ulong), null)]
    [InlineData("-1234567890123456789", typeof(ulong), null)]
    [InlineData("-0", typeof(ulong), null)]
    public void LongIntegers(string text, Type type, string? expected)
    {
        IArrowType arrow = type == typeof(long) ? Int64Type.Default : UInt64Type.Default;
        var options = new CsvReadOptions { Schema = SchemaOf(("a", arrow)) };
        string name = type == typeof(long) ? "i64" : "u64";
        if (expected is null) Assert.Throws<CsvValueException>(() => Read($"a\n{text}\n", options));
        else Assert.Equal($"a:{name} | {expected}", Read($"a\n{text}\n", options));
    }

    [Fact]
    public void TimeInMicroseconds_RefusesNanoseconds()
    {
        var options = new CsvReadOptions { Schema = SchemaOf(("t", new Time64Type(TimeUnit.Microsecond))) };
        Assert.Equal("t:time | 12:00:00.123456", Read("t\n12:00:00.123456\n", options));
        var e = Assert.Throws<CsvValueException>(() => Read("t\n12:00:00.1234567\n", options));
        Assert.Contains("more precise than the column's unit (μs)", e.Message);
    }

    [Fact]
    public void ErrorsNameRowLineColumnAndValue()
    {
        var e = Assert.Throws<CsvValueException>(() => Read("a,b\r\n1,\"multi\nline\"\r\n2,\"x\"\r\nz,y\r\n", new CsvReadOptions { Schema = SchemaOf(("a", Int64Type.Default), ("b", StringType.Default)) }));
        Assert.Equal("Row 3 (line 5): column 'a': 'z' is not an Int64 integer", e.Message);
        Assert.Equal(("a", 3L, 5L, "z"), (e.Column, e.Row, e.Line, e.Value));
    }

    [Fact]
    public void LinesCountLoneCarriageReturns()
    {
        foreach (int unit in (int[])[1 << 20, 3])
        {
            var e = Assert.Throws<CsvValueException>(() => CsvBehaviourTests.Read("a\r1\r\"x\ry\"\rz\r"u8.ToArray(),
                new CsvReadOptions { Schema = SchemaOf(("a", Int64Type.Default)), UnitBytes = unit }, CsvBehaviourTests.CsvSource.Memory));
            Assert.Equal("Row 2 (line 3): column 'a': 'x\ry' is not an Int64 integer", e.Message);
        }
    }

    /// <summary>Text whose first bytes pass the general zlib header rule ("H,", "x ", "(\r") is read as text, from every source.</summary>
    [Theory]
    [InlineData("H,1\nP,2\n")]
    [InlineData("x y,b\n1,2\n")]
    [InlineData("hj,b\n1,2\n")]
    public void TextThatLooksLikeAZlibHeader(string text)
    {
        byte[] bytes = Encoding.UTF8.GetBytes(text);
        string whole = CsvBehaviourTests.Read(bytes, new CsvReadOptions());
        foreach (CsvBehaviourTests.CsvSource source in Enum.GetValues<CsvBehaviourTests.CsvSource>())
            Assert.Equal(whole, CsvBehaviourTests.Read(bytes, new CsvReadOptions(), source));
    }

    [Fact]
    public void ZlibInputIsDecompressed()
    {
        using var compressed = new MemoryStream();
        using (var z = new System.IO.Compression.ZLibStream(compressed, System.IO.Compression.CompressionLevel.Optimal, leaveOpen: true))
            z.Write("a,b\n1,x\n"u8);
        Assert.Equal("a:i64,b:str | 1,\"x\"", CsvBehaviourTests.Read(compressed.ToArray(), new CsvReadOptions()));
    }

    /// <summary>
    /// A value past the inferred rows that the inferred type cannot hold names the rows the type came from and the fixes
    /// (no schema was given); one needing a finer datetime unit says so (native truncates it).
    /// </summary>
    [Fact]
    public void InferredTypesRefuseLaterValuesWithTheFixes()
    {
        var text = new StringBuilder("t,n\n");
        for (int i = 0; i < 150; i++) text.Append($"2023-03-28 15:10:{i % 60:D2}.123456,{i}\n");
        string late = text + "2023-03-28 15:10:24.045736300,7\n";
        var e = Assert.Throws<CsvValueException>(() => Read(late));
        Assert.Equal("Row 151 (line 152): column 't': '2023-03-28 15:10:24.045736300' is more precise than the column's unit (μs), inferred from the "
            + "first 100 rows: give the column a finer unit in SchemaOverrides, e.g. datetime[ns], or a larger InferSchemaLength", e.Message);
        // The fixes work.
        Assert.EndsWith("2023-03-28T15:10:24.0457363,7", Read(late, new CsvReadOptions { SchemaOverrides = new Dictionary<string, IArrowType> { ["t"] = new TimestampType(TimeUnit.Nanosecond, (string?)null) } }));
        Assert.StartsWith("t:datetime[ns]", Read(late, new CsvReadOptions { InferSchemaLength = null }));
        // Other types name the inferred rows too; a given type does not.
        e = Assert.Throws<CsvValueException>(() => Read(text + "2023-03-28 15:10:24,x\n"));
        Assert.EndsWith("'x' is not an Int64 integer (the column's type was inferred from the first 100 rows: give the column its type in SchemaOverrides, or a larger InferSchemaLength)", e.Message);
        e = Assert.Throws<CsvValueException>(() => Read(text + "2023-03-28 15:10:24,x\n", new CsvReadOptions { SchemaOverrides = new Dictionary<string, IArrowType> { ["n"] = Int64Type.Default } }));
        Assert.EndsWith("'x' is not an Int64 integer", e.Message);
    }

    [Fact]
    public void HeaderEdges()
    {
        Assert.Equal("a:i64,b:i64,:str | 1,2,null", Read("a,b,\n1,2,\n"));
        Assert.Equal("a:i64,a_duplicated_0:i64,a_duplicated_1:i64 | 1,2,3", Read("a,a,a\n1,2,3\n"));
        Assert.Equal("a:str,b:str", Read("\r\n\r\na,b"));
        // A separator last at the input's end: one more (empty) field, in a header and in a first record.
        Assert.Equal("a:str,b:str,:str", Read("a,b,"));
        Assert.Equal("column_1:i64,column_2:str | 1,null", Read("1,", new CsvReadOptions { HasHeader = false }));
    }

    [Fact]
    public void MissingIsNullOff_EmptyAndMissingAreEmptyStrings_BlankLinesNull()
    {
        var options = new CsvReadOptions { MissingIsNull = false, Schema = SchemaOf(("a", StringType.Default), ("b", StringType.Default), ("c", new DictionaryType(Int32Type.Default, StringType.Default, false))) };
        Assert.Equal("a:str,b:str,c:cat | \"1\",\"\",\"\" | null,null,null | \"\",\"x\",\"\"", Read("a,b,c\n1,\n\n,x,\n", options));
    }

    [Fact]
    public void ColumnsProjectedInFileOrder_RowIndexFirst()
    {
        var options = new CsvReadOptions { Columns = ["c", "a"], RowIndexColumn = "i", RowIndexOffset = 5 };
        Assert.Equal("i:i64,a:i64,c:str | 5,1,\"x\" | 6,3,\"y\"", Read("a,b,c\n1,2,x\n3,4,y\n", options));
        Assert.Throws<ArgumentException>(() => Read("a\n1\n", new CsvReadOptions { Columns = ["zz"] }));
    }

    [Fact]
    public void SkipsAndCommentPrefix_AreChecked()
    {
        Assert.Throws<ArgumentException>(() => Read("a\n1\n", new CsvReadOptions { CommentPrefix = "" }));
        Assert.Throws<ArgumentException>(() => Read("a\n1\n", new CsvReadOptions { CommentPrefix = "#\n" }));
        Assert.Throws<ArgumentException>(() => Read("a;1;", new CsvReadOptions { CommentPrefix = "#;", LineEnd = ';' }));
        Assert.Throws<ArgumentOutOfRangeException>(() => Read("a\n1\n", new CsvReadOptions { SkipLines = -1 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => Read("a\n1\n", new CsvReadOptions { SkipRows = -1 }));
        Assert.Throws<ArgumentOutOfRangeException>(() => Read("a\n1\n", new CsvReadOptions { SkipRowsAfterHeader = -1 }));
        Assert.Throws<ArgumentException>(() => Read("a\n1\n", new CsvReadOptions { SkipLines = 1, SkipRows = 1 }));
        // Any length, any characters (a prefix with non-ASCII ones is compared on the text).
        Assert.Equal("a:i64 | 1", Read("### x\na\n1\n### y\n", new CsvReadOptions { CommentPrefix = "###" }));
        Assert.Equal("a:i64 | 1", Read("§ x\na\n1\n§ y\n", new CsvReadOptions { CommentPrefix = "§" }));
    }
}
