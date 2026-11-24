using System.IO.Compression;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Csv;
using Kaminari.Seseragi.Files.Ipc;
using static Seseragi.Files.Tests.Json.JsonWriterOracleTests;

namespace Seseragi.Files.Tests.Csv;

public class CsvWriterTests
{
    private static readonly int Count = int.TryParse(Environment.GetEnvironmentVariable("CSV_WRITE_CASES"), out int n) ? n : 150;

    private static Field F(string name, IArrowType type) => new(name, type, true);

    private static (Schema Schema, List<RecordBatch> Batches) Written()
    {
        using IpcFile file = IpcFile.Open(Path.Combine(AppContext.BaseDirectory, "TestData", "csv-write", "written.arrow"));
        using IpcReader reader = file.OpenReader();
        return (reader.Schema, [.. reader.ReadBatches()]);
    }

    // ---------------------------------------------------------------- G3: bytes independent of how they are made

    [Fact]
    public void TheBytes_DoNotDependOnThreads_Pieces_TheWindow_Ownership_Batches_OrFlushes()
    {
        var (schema, whole) = Written();
        byte[] expected;
        try { expected = Encoding.UTF8.GetBytes(CsvWriter.WriteString(schema, whole)); }
        finally { whole.ForEach(b => b.Dispose()); }
        var random = new Random(20261124);
        for (int i = 0; i < Count; i++)
        {
            var (_, batches) = Written();
            try
            {
                int slice = random.Next(4) switch { 0 => 1, 1 => 3, 2 => 1000, _ => 0 };
                List<RecordBatch> parts = [.. batches.SelectMany(b => Slices(b, slice, random))];
                var options = new CsvWriteOptions
                {
                    MaxDegreeOfParallelism = random.Next(1, 9),
                    PieceBytes = random.Next(2) == 0 ? random.Next(1, 300) : random.Next(64, 1 << 20),
                    WindowPieces = random.Next(3) == 0 ? 0 : random.Next(1, 6),
                    InFlightBytes = random.Next(2) == 0 ? random.Next(1, 4000) : 256L << 20,
                    BatchOwnership = random.Next(2) == 0 ? BatchOwnership.Caller : BatchOwnership.Writer,
                    LeaveOpen = true,
                };
                using var output = new MemoryStream();
                using (CsvWriter writer = CsvWriter.Create(output, schema, options))
                    foreach (RecordBatch part in parts)
                    {
                        writer.Write(options.BatchOwnership == BatchOwnership.Writer ? Owned(part) : part);
                        if (random.Next(5) == 0) writer.Flush();
                    }
                AssertSameText(expected, output.ToArray());
            }
            finally { batches.ForEach(b => b.Dispose()); }
        }
    }

    // A batch cut into slices of `rows` (0: random lengths), rows at offsets in every array.
    private static IEnumerable<RecordBatch> Slices(RecordBatch batch, int rows, Random random)
    {
        for (int start = 0; start < batch.Length;)
        {
            int length = Math.Min(rows > 0 ? rows : random.Next(1, 8), batch.Length - start);
            yield return batch.Slice(start, length);
            start += length;
        }
    }

    // A batch of its own, its buffers copied to managed memory (the writer disposes what it owns).
    private static RecordBatch Owned(RecordBatch batch) => new(batch.Schema, [.. batch.Arrays.Select(a => ArrowArrayFactory.BuildArray(Copy(a.Data)))], batch.Length);

    private static ArrayData Copy(ArrayData data) => new(data.DataType, data.Length, data.NullCount, data.Offset,
        [.. data.Buffers.Select(b => new ArrowBuffer(b.Span.ToArray()))], [.. (data.Children ?? []).Select(Copy)], data.Dictionary is { } d ? Copy(d) : null);

    // ---------------------------------------------------------------- G2: what is written reads back

    /// <summary>
    /// Generated files read (any types the reader infers or is given), written with random options, read back with the
    /// first read's schema and the written options: the same rows.
    /// </summary>
    [Fact]
    public void WhatIsWritten_ReadsBack()
    {
        var random = new Random(20261125);
        int checkedCases = 0;
        for (int i = 0; checkedCases < Count && i < 20 * Count; i++)
        {
            var (bytes, readOptions) = CsvGenerator.Generate(random);
            List<RecordBatch> first;
            Schema schema;
            try
            {
                using CsvReader reader = CsvReader.Open(new MemoryStream(bytes), readOptions);
                schema = reader.Schema;
                first = [.. reader.ReadBatches()];
            }
            catch (Exception e) when (e is CsvFormatException or CsvValueException)
            {
                continue; // a planted error: nothing to write
            }
            try
            {
                CsvQuoteStyle style = random.Next(3) switch { 0 => CsvQuoteStyle.Always, 1 => CsvQuoteStyle.NonNumeric, _ => CsvQuoteStyle.Necessary };
                string nullValue = style == CsvQuoteStyle.Always || random.Next(3) == 0 ? "<null>" : "";
                bool comma = random.Next(3) == 0;
                string? group = random.Next(4) != 0 ? null : comma ? ((string[])[".", " ", "'"])[random.Next(3)] : ((string[])[" ", "'", ","])[random.Next(3)];
                var options = new CsvWriteOptions
                {
                    FloatScientific = random.Next(3) switch { 0 => true, 1 => false, _ => null },
                    DecimalComma = comma,
                    ThousandsSeparator = group,
                    Separator = random.Next(4) switch { 0 => ';', 1 => '\t', 2 => '|', _ => ',' },
                    Quote = random.Next(4) == 0 && group != "'" ? '\'' : '"',
                    QuoteStyle = style,
                    NullValue = nullValue,
                    LineTerminator = random.Next(2) == 0 ? "\n" : "\r\n",
                    MaxDegreeOfParallelism = random.Next(1, 5),
                    PieceBytes = random.Next(2) == 0 ? random.Next(1, 500) : 1 << 20,
                };
                using var output = new MemoryStream();
                CsvWriter.Write(output, schema, first, options);
                var back = new CsvReadOptions
                {
                    Schema = schema,
                    Separator = options.Separator,
                    Quote = options.Quote,
                    NullValues = nullValue.Length > 0 ? [nullValue] : null,
                    DecimalComma = comma,
                    ThousandsSeparator = group,
                };
                using CsvReader again = CsvReader.Open(new MemoryStream(output.ToArray()), back);
                List<RecordBatch> second = [.. again.ReadBatches()];
                try
                {
                    Assert.Equal(ArrowRender.Rows(first), ArrowRender.Rows(second));
                }
                catch (Exception e)
                {
                    throw new Xunit.Sdk.XunitException($"Case {i} ({options.QuoteStyle}, separator '{options.Separator}', quote '{options.Quote}', null '{nullValue}', "
                        + $"scientific {options.FloatScientific}, comma {comma}, group '{group}'):\n"
                        + Encoding.UTF8.GetString(output.ToArray())[..Math.Min(2000, (int)output.Length)] + "\n" + e.Message);
                }
                finally { second.ForEach(b => b.Dispose()); }
                checkedCases++;
            }
            finally { first.ForEach(b => b.Dispose()); }
        }
        Assert.Equal(Count, checkedCases);
    }

    // ---------------------------------------------------------------- outputs, compression, the header

    [Fact]
    public void Outputs_TextWriters_Strings_Files_AndBufferWriters_HoldTheSameText()
    {
        var (schema, batches) = Written();
        try
        {
            string text = CsvWriter.WriteString(schema, batches);
            using var stringWriter = new StringWriter();
            CsvWriter.Write(stringWriter, schema, batches);
            Assert.Equal(text, stringWriter.ToString());
            var buffer = new System.Buffers.ArrayBufferWriter<byte>();
            CsvWriter.Write(buffer, schema, batches);
            Assert.Equal(text, Encoding.UTF8.GetString(buffer.WrittenSpan));
            string path = Path.Combine(Path.GetTempPath(), $"csv-writer-{Guid.NewGuid():N}.csv");
            try
            {
                CsvWriter.Write(path, schema, batches);
                Assert.Equal(text, File.ReadAllText(path, new UTF8Encoding(false)));
            }
            finally { File.Delete(path); }
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    [Theory]
    [InlineData(CsvCompression.Gzip)]
    [InlineData(CsvCompression.Zstd)]
    public void CompressedOutput_DecompressesToTheText(CsvCompression compression)
    {
        var (schema, batches) = Written();
        try
        {
            string text = CsvWriter.WriteString(schema, batches);
            foreach (bool rows in (bool[])[true, false])
            {
                using var output = new MemoryStream();
                CsvWriter.Write(output, schema, rows ? batches : [], new CsvWriteOptions { Compression = compression, PieceBytes = 200, MaxDegreeOfParallelism = 3 });
                using Stream unpacked = compression == CsvCompression.Gzip
                    ? new GZipStream(new MemoryStream(output.ToArray()), CompressionMode.Decompress)
                    : new ZstdSharp.DecompressionStream(new MemoryStream(output.ToArray()));
                using var plain = new MemoryStream();
                unpacked.CopyTo(plain);
                string expected = rows ? text : text[..(text.IndexOf('\n', text.IndexOf("é\"", StringComparison.Ordinal)) + 1)];
                Assert.Equal(expected, Encoding.UTF8.GetString(plain.ToArray()));
            }
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    [Fact]
    public void NoRows_NoHeader_IsNothing_ButABom_IsWritten()
    {
        var schema = new Schema([F("a", Int64Type.Default)], null);
        Assert.Equal("a\n", CsvWriter.WriteString(schema, []));
        Assert.Equal("", CsvWriter.WriteString(schema, [], new CsvWriteOptions { IncludeHeader = false }));
        using var output = new MemoryStream();
        CsvWriter.Write(output, schema, [], new CsvWriteOptions { IncludeHeader = false, IncludeBom = true });
        Assert.Equal([0xEF, 0xBB, 0xBF], output.ToArray());
    }

    // ---------------------------------------------------------------- values the reference engine does not write

    [Fact]
    public void Durations_AreWrittenInIso8601()
    {
        var schema = new Schema([F("d", DurationType.Millisecond)], null);
        long[] values = [0, 1500, -90_000, 1, 86_400_000];
        using var batch = new RecordBatch(schema, [new DurationArray(new ArrayData(schema.FieldsList[0].DataType, values.Length, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(values.SelectMany(BitConverter.GetBytes).ToArray())]))], values.Length);
        Assert.Equal("d\nP0D\nPT1.5S\n-PT90S\nPT0.001S\nPT86400S\n", CsvWriter.WriteString(schema, [batch]));
    }

    // ---------------------------------------------------------------- numbers: deviations from the reference engine, groups

    private static RecordBatch Numbers(out Schema schema, double?[] doubles, float?[]? floats = null, long?[]? longs = null, (int Scale, long?[] Unscaled)? decimals = null)
    {
        List<Field> fields = [F("d", DoubleType.Default)];
        var first = new DoubleArray.Builder();
        foreach (double? v in doubles) { if (v is { } x) first.Append(x); else first.AppendNull(); }
        List<IArrowArray> arrays = [first.Build()];
        if (floats is not null)
        {
            fields.Add(F("f", FloatType.Default));
            var b = new FloatArray.Builder();
            foreach (float? v in floats) { if (v is { } x) b.Append(x); else b.AppendNull(); }
            arrays.Add(b.Build());
        }
        if (longs is not null)
        {
            fields.Add(F("i", Int64Type.Default));
            var b = new Int64Array.Builder();
            foreach (long? v in longs) { if (v is { } x) b.Append(x); else b.AppendNull(); }
            arrays.Add(b.Build());
        }
        if (decimals is { } dec)
        {
            var type = new Decimal128Type(18, dec.Scale);
            fields.Add(F("m", type));
            var b = new Decimal128Array.Builder(type);
            foreach (long? v in dec.Unscaled) { if (v is { } x) b.Append((decimal)x / (decimal)Math.Pow(10, dec.Scale)); else b.AppendNull(); }
            arrays.Add(b.Build());
        }
        schema = new Schema(fields, null);
        return new RecordBatch(schema, arrays, doubles.Length);
    }

    /// <summary>Rule 8 (a): with the decimal comma and the separator ',', a null float or decimal is the null value (the reference engine writes "").</summary>
    [Fact]
    public void DecimalComma_NullsAreTheNullValue()
    {
        using RecordBatch batch = Numbers(out Schema schema, [1.5, null], decimals: (2, [-150, null]));
        Assert.Equal("d,m\n\"1,5\",\"-1,50\"\n,\n", CsvWriter.WriteString(schema, [batch], new CsvWriteOptions { DecimalComma = true }));
        Assert.Equal("d,m\n\"1,5\",\"-1,50\"\nNA,NA\n", CsvWriter.WriteString(schema, [batch], new CsvWriteOptions { DecimalComma = true, NullValue = "NA" }));
    }

    /// <summary>Rule 8 (b): floats in decimal notation from their own shortest digits (the reference engine goes through the double).</summary>
    [Fact]
    public void Floats_InDecimalNotation_HaveTheirOwnDigits()
    {
        using RecordBatch batch = Numbers(out Schema schema, [0.1, 1e20, 464749.125], floats: [0.1f, 1e20f, 464749.125f]);
        Assert.Equal("d,f\n0.1,0.1\n100000000000000000000,100000000000000000000\n464749.125,464749.13\n",
            CsvWriter.WriteString(schema, [batch], new CsvWriteOptions { FloatScientific = false }));
    }

    /// <summary>Rule 8 (d): the decimal comma with scientific notation writes each value alone (the reference engine repeats earlier ones).</summary>
    [Fact]
    public void DecimalComma_WithScientificNotation_WritesEachValueAlone()
    {
        using RecordBatch batch = Numbers(out Schema schema, [0.1 + 0.2, 1.0 / 3, 1.5e20, -0.0, null]);
        Assert.Equal("d\n3,0000000000000004e-1\n3,333333333333333e-1\n1,5e20\n-0e0\n\n",
            CsvWriter.WriteString(schema, [batch], new CsvWriteOptions { DecimalComma = true, FloatScientific = true, Separator = ';' }));
    }

    [Theory]
    [InlineData(".", true, ';', "d;i;m\n-1.234.567,25;-1.234.567;1.234.567,89\n999,5;999;0,01\n1,5e+16;1.000;-1.000,00\nNaN;-100;\n")]
    [InlineData(" ", true, ';', "d;i;m\n-1 234 567,25;-1 234 567;1 234 567,89\n999,5;999;0,01\n1,5e+16;1 000;-1 000,00\nNaN;-100;\n")]
    [InlineData("\u00A0", false, ',', "d,i,m\n-1\u00A0234\u00A0567.25,-1\u00A0234\u00A0567,1\u00A0234\u00A0567.89\n999.5,999,0.01\n1.5e+16,1\u00A0000,-1\u00A0000.00\nNaN,-100,\n")]
    [InlineData("'", false, ',', "d,i,m\n-1'234'567.25,-1'234'567,1'234'567.89\n999.5,999,0.01\n1.5e+16,1'000,-1'000.00\nNaN,-100,\n")]
    [InlineData(",", false, ',', "d,i,m\n\"-1,234,567.25\",\"-1,234,567\",\"1,234,567.89\"\n\"999.5\",\"999\",\"0.01\"\n\"1.5e+16\",\"1,000\",\"-1,000.00\"\n\"NaN\",\"-100\",\n")]
    public void ThousandsSeparators_GroupTheIntegerDigits_AndQuoteWhenTheyAreTheSeparator(string group, bool comma, char separator, string expected)
    {
        using RecordBatch batch = Numbers(out Schema schema, [-1234567.25, 999.5, 1.5e16, double.NaN], longs: [-1234567, 999, 1000, -100],
            decimals: (2, [123456789, 1, -100000, null]));
        var options = new CsvWriteOptions { ThousandsSeparator = group, DecimalComma = comma, Separator = separator };
        string text = CsvWriter.WriteString(schema, [batch], options);
        Assert.Equal(expected, text);
        // It reads back with the same options.
        using CsvReader reader = CsvReader.Open(new MemoryStream(Encoding.UTF8.GetBytes(text)), new CsvReadOptions
        {
            Schema = schema, Separator = separator, DecimalComma = comma, ThousandsSeparator = group,
        });
        List<RecordBatch> back = [.. reader.ReadBatches()];
        try { Assert.Equal(ArrowRender.Rows([batch]), ArrowRender.Rows(back)); }
        finally { back.ForEach(b => b.Dispose()); }
    }

    /// <summary>Integers of every length grouped as .NET groups them (N0 with the same group text).</summary>
    [Fact]
    public void Groups_FallEveryThreeDigits_WhateverTheLength()
    {
        long[] values = [0, 1, -12, 123, -1234, 12345, 123456, -1234567, 12345678, 123456789, -1234567890, long.MaxValue, long.MinValue];
        var schema = new Schema([F("i", Int64Type.Default)], null);
        var builder = new Int64Array.Builder();
        foreach (long v in values) builder.Append(v);
        using var batch = new RecordBatch(schema, [builder.Build()], values.Length);
        foreach (string group in (string[])["'", "\u202F", "::"])
        {
            var format = new System.Globalization.NumberFormatInfo { NumberGroupSeparator = group, NegativeSign = "-" };
            string expected = "i\n" + string.Concat(values.Select(v => v.ToString("N0", format) + "\n"));
            Assert.Equal(expected, CsvWriter.WriteString(schema, [batch], new CsvWriteOptions { ThousandsSeparator = group }));
        }
    }

    [Fact]
    public void NumberOptionsOutOfRange_AreRefused()
    {
        var schema = new Schema([F("a", Int64Type.Default)], null);
        foreach (CsvWriteOptions options in (CsvWriteOptions[])[new() { FloatPrecision = -1 }, new() { ThousandsSeparator = "" },
            new() { ThousandsSeparator = "1" }, new() { ThousandsSeparator = "." }, new() { ThousandsSeparator = ",", DecimalComma = true },
            new() { ThousandsSeparator = "\n" }, new() { ThousandsSeparator = "'", Quote = '\'' }])
            Assert.ThrowsAny<ArgumentException>(() => CsvWriter.Create(new MemoryStream(), schema, options));
    }

    // ---------------------------------------------------------------- refusals

    [Fact]
    public void ColumnsCsvCannotHold_AreRefusedAtCreate_NamingTheColumn()
    {
        foreach ((IArrowType type, string words) in ((IArrowType, string)[])[
            (BinaryType.Default, "binary"), (new ListType(Int32Type.Default), "nested"), (new StructType([F("x", Int32Type.Default)]), "nested"),
            (new FixedSizeListType(F("i", Int8Type.Default), 2), "nested"), (new TimestampType(TimeUnit.Second, "Not/AZone"), "time zone")])
        {
            var e = Assert.Throws<NotSupportedException>(() => CsvWriter.Create(new MemoryStream(), new Schema([F("col", type)], null)));
            Assert.Contains("'col'", e.Message);
            Assert.Contains(words, e.Message);
        }
    }

    [Fact]
    public void OptionsOutOfRange_AreRefused()
    {
        var schema = new Schema([F("a", Int64Type.Default)], null);
        foreach (CsvWriteOptions options in (CsvWriteOptions[])[new() { Separator = 'é' }, new() { Separator = '\n' }, new() { Quote = ',' },
            new() { Quote = '\r' }, new() { MaxDegreeOfParallelism = 0 }, new() { CompressionLevel = 3 },
            new() { Compression = CsvCompression.Gzip, CompressionLevel = 10 }])
            Assert.ThrowsAny<ArgumentException>(() => CsvWriter.Create(new MemoryStream(), schema, options));
        Assert.Throws<ArgumentException>(() => CsvWriter.Create(new StringWriter(), schema, new CsvWriteOptions { Compression = CsvCompression.Zstd }));
    }

    [Fact]
    public void StringsThatAreNotUtf8_FailAtTheirRow_AndTheWriterCannotComplete()
    {
        var schema = new Schema([F("s", StringType.Default)], null);
        byte[] data = [.. "okfine"u8, 0xFF];
        var strings = new StringArray(new ArrayData(StringType.Default, 3, 0, 0,
            [ArrowBuffer.Empty, new ArrowBuffer(((int[])[0, 2, 6, 7]).SelectMany(BitConverter.GetBytes).ToArray()), new ArrowBuffer(data)]));
        using var batch = new RecordBatch(schema, [strings], 3);
        for (int i = 0; i < 100; i++)
        {
            using var output = new MemoryStream();
            var writer = CsvWriter.Create(output, schema, new CsvWriteOptions { PieceBytes = 1, MaxDegreeOfParallelism = 1 + i % 4 });
            var e = Assert.Throws<CsvWriteException>(() => writer.Write(batch));
            Assert.Equal("Row 2: Column 's': a string that is not UTF-8.", e.Message);
            Assert.Equal(("s", 2L), (e.Column, e.Row));
            Assert.Throws<InvalidOperationException>(() => writer.Complete());
            writer.Dispose();
        }
    }
}
