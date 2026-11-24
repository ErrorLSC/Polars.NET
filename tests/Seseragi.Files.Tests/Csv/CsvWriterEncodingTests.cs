using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files;
using Kaminari.Seseragi.Files.Csv;
using Kaminari.Seseragi.Files.Ipc;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// The writer's encodings (G5): the bytes are the UTF-8 text encoded as .NET encodes it with '?' for what the encoding
/// cannot write; a byte order mark only for Unicode encodings; a character a strict writer cannot write fails naming
/// its row, column and character (the first in order, on every path); what the options need of the encoding is checked
/// when the writer is created; written text reads back with the same encoding.
/// </summary>
public class CsvWriterEncodingTests
{
    private static Field F(string name, IArrowType type) => new(name, type, true);

    private static (Schema Schema, List<RecordBatch> Batches) Written()
    {
        using IpcFile file = IpcFile.Open(Path.Combine(AppContext.BaseDirectory, "TestData", "csv-write", "written.arrow"));
        using IpcReader reader = file.OpenReader();
        return (reader.Schema, [.. reader.ReadBatches()]);
    }

    [Theory]
    [InlineData(1200, true)]
    [InlineData(1201, true)]
    [InlineData(12000, true)]
    [InlineData(932, false)]
    [InlineData(936, false)]
    [InlineData(54936, false)]
    [InlineData(950, false)]
    [InlineData(949, false)]
    [InlineData(1252, false)]
    [InlineData(28591, false)]
    [InlineData(37, false)]
    public void TheBytes_AreTheTextEncoded_WithQuestionMarksForTheRest(int codePage, bool unicode)
    {
        var (schema, batches) = Written();
        try
        {
            Encoding encoding = TextEncodings.Resolve(codePage);
            string text = CsvWriter.WriteString(schema, batches);
            foreach (bool bom in unicode ? (bool[])[false, true] : [false])
            {
                using var output = new MemoryStream();
                CsvWriter.Write(output, schema, batches, new CsvWriteOptions
                {
                    Encoding = encoding, InvalidText = CsvInvalidText.Replace, IncludeBom = bom, PieceBytes = 300, MaxDegreeOfParallelism = 3,
                });
                byte[] expected = [.. bom ? TextEncoder.ByteOrderMark(codePage).ToArray() : [], .. TextEncoder.Clone(encoding, replace: true).GetBytes(text)];
                Assert.Equal(Convert.ToHexString(expected), Convert.ToHexString(output.ToArray()));
            }
        }
        finally { batches.ForEach(b => b.Dispose()); }
    }

    private static RecordBatch Strings(Schema schema, params string?[] values)
    {
        var builder = new StringArray.Builder();
        foreach (string? v in values) { if (v is null) builder.AppendNull(); else builder.Append(v); }
        return new RecordBatch(schema, [builder.Build()], values.Length);
    }

    /// <summary>
    /// Two rows the encoding cannot write, close enough to be encoded at once: the first in order fails the write, named
    /// by row, column and character — through pieces side by side, rows the caller encodes, and owned batches.
    /// </summary>
    [Fact]
    public void ACharacterTheEncodingCannotWrite_FailsAtItsRowAndColumn_TheFirstInOrder()
    {
        var schema = new Schema([F("a", StringType.Default)], null);
        string?[] values = [.. Enumerable.Range(0, 300).Select(i => i == 150 ? "café" : i == 151 ? "naïve" : $"行 {i}")];
        const string Expected = "Row 150: Column 'a': 'é' (U+00E9) cannot be written in shift_jis (932): set InvalidText to Replace (it becomes '?'), or write UTF-8.";
        for (int i = 0; i < 60; i++)
        {
            var options = new CsvWriteOptions
            {
                Encoding = TextEncodings.Resolve(932), PieceBytes = 1 + i % 50, MaxDegreeOfParallelism = 1 + i % 8, WindowPieces = 1 + i % 6,
                BatchOwnership = i % 3 == 2 ? BatchOwnership.Writer : BatchOwnership.Caller,
            };
            using var output = new MemoryStream();
            var writer = CsvWriter.Create(output, schema, options);
            var e = Assert.Throws<CsvWriteException>(() =>
            {
                if (i % 3 == 1)
                    for (int r = 0; r < values.Length; r += 2) writer.Write(Strings(schema, values[r], values[r + 1])); // small batches: rows the caller encodes
                else writer.Write(Strings(schema, values));
                writer.Complete();
            });
            Assert.Equal(Expected, e.Message);
            Assert.Equal(("a", 150L), (e.Column, e.Row));
            writer.Dispose();
        }
    }

    [Fact]
    public void WhatTheEncodingCannotWrite_InTheOptionsOrTheHeader_IsRefusedAtCreate()
    {
        Encoding sjis = TextEncodings.Resolve(932);
        var plain = new Schema([F("a", StringType.Default)], null);
        var e = Assert.Throws<ArgumentException>(() => CsvWriter.Create(new MemoryStream(), new Schema([F("ok", Int32Type.Default), F("prénom", StringType.Default)], null),
            new CsvWriteOptions { Encoding = sjis }));
        Assert.Contains("column 'prénom' has 'é' (U+00E9)", e.Message);
        Assert.Contains("null value", Assert.Throws<ArgumentException>(() => CsvWriter.Create(new MemoryStream(), plain, new CsvWriteOptions { Encoding = sjis, NullValue = "∅" })).Message);
        Assert.Contains("line terminator", Assert.Throws<ArgumentException>(() => CsvWriter.Create(new MemoryStream(), plain, new CsvWriteOptions { Encoding = sjis, LineTerminator = "é\n" })).Message);
        Assert.Contains("byte order mark", Assert.Throws<ArgumentException>(() => CsvWriter.Create(new MemoryStream(), plain, new CsvWriteOptions { Encoding = sjis, IncludeBom = true })).Message);
        Assert.Throws<ArgumentException>(() => CsvWriter.Create(new MemoryStream(), plain, new CsvWriteOptions { Encoding = sjis, InvalidText = CsvInvalidText.Replace, Separator = '?' }));
        Assert.Throws<ArgumentException>(() => CsvWriter.Create(new StringWriter(), plain, new CsvWriteOptions { Encoding = sjis }));
        Assert.Throws<NotSupportedException>(() => CsvWriter.Create(new MemoryStream(), plain, new CsvWriteOptions { Encoding = TextEncodings.Resolve(50220) }));
        // Replaced: a header name the encoding cannot write is '?'.
        Assert.Equal("pr?nom\n", sjis.GetString(Bytes(new Schema([F("prénom", StringType.Default)], null), [], new CsvWriteOptions { Encoding = sjis, InvalidText = CsvInvalidText.Replace })));
        // UTF-8 named as an encoding: as without one, text outputs too.
        Assert.Equal("a\n", CsvWriter.WriteString(plain, [], new CsvWriteOptions { Encoding = Encoding.UTF8 }));
    }

    private static byte[] Bytes(Schema schema, IEnumerable<RecordBatch> batches, CsvWriteOptions options)
    {
        using var output = new MemoryStream();
        CsvWriter.Write(output, schema, batches, options);
        return output.ToArray();
    }

    /// <summary>
    /// Replaced, never a look-alike: a fullwidth comma and quote (which .NET's default encoding of Windows-1252 writes as
    /// ',' and '"') become '?', and the records keep their shape.
    /// </summary>
    [Fact]
    public void Replaced_FullwidthCommasAndQuotes_KeepTheRecordsShape()
    {
        var schema = new Schema([F("a", StringType.Default), F("b", StringType.Default)], null);
        var a = new StringArray.Builder().Append("x，y").Append("＂q＂").Build();
        var b = new StringArray.Builder().Append("1").Append("2").Build();
        using var batch = new RecordBatch(schema, [a, b], 2);
        Encoding latin = TextEncodings.Resolve(1252);
        byte[] written = Bytes(schema, [batch], new CsvWriteOptions { Encoding = latin, InvalidText = CsvInvalidText.Replace });
        Assert.Equal("a,b\nx?y,1\n?q?,2\n", latin.GetString(written));
        using CsvReader reader = CsvReader.Open(new MemoryStream(written), new CsvReadOptions { Encoding = latin, Schema = schema });
        List<RecordBatch> back = [.. reader.ReadBatches()];
        try { Assert.Equal(["\"x?y\" | \"1\"", "\"?q?\" | \"2\""], ArrowRender.Rows(back)); }
        finally { back.ForEach(x => x.Dispose()); }
    }

    /// <summary>Generated files read, written in an encoding that holds every character, read back in it: the same rows.</summary>
    [Theory]
    [InlineData(1200)]
    [InlineData(1201)]
    [InlineData(54936)]
    [InlineData(12000)]
    [InlineData(936)]
    public void WhatIsWritten_ReadsBack_InTheEncoding(int codePage)
    {
        Encoding encoding = TextEncodings.Resolve(codePage);
        var random = new Random(codePage);
        int done = 0;
        for (int i = 0; done < 60 && i < 1000; i++)
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
                continue;
            }
            try
            {
                byte[] written = Bytes(schema, first, new CsvWriteOptions { Encoding = encoding, NullValue = "<null>", PieceBytes = 1 + random.Next(400), MaxDegreeOfParallelism = 1 + random.Next(4) });
                using CsvReader again = CsvReader.Open(new MemoryStream(written), new CsvReadOptions { Encoding = encoding, Schema = schema, NullValues = ["<null>"] });
                List<RecordBatch> second = [.. again.ReadBatches()];
                try { Assert.Equal(ArrowRender.Rows(first), ArrowRender.Rows(second)); }
                finally { second.ForEach(x => x.Dispose()); }
                done++;
            }
            finally { first.ForEach(x => x.Dispose()); }
        }
        Assert.Equal(60, done);
    }
}
