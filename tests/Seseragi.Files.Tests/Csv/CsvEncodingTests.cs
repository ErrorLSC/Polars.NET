using System.IO.Compression;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Text;
using Kaminari.Seseragi.Files.Csv;

namespace Seseragi.Files.Tests.Csv;

/// <summary>
/// Encodings (step f2): text in a code page or UTF-16 reads as the same text in UTF-8 does, through the bytes path (the
/// input cut and tokenized on its own bytes) and the transcode path (decoded into UTF-8 first) alike; byte order marks;
/// strict errors at their row and line; lossy reads.
/// </summary>
public class CsvEncodingTests
{
    private static string Utf8(string text, CsvReadOptions? options = null) => CsvBehaviourTests.Read(Encoding.UTF8.GetBytes(text), options ?? new CsvReadOptions());

    private static string Read(byte[] bytes, CsvReadOptions options) => CsvBehaviourTests.Read(bytes, options);

    private static Encoding Strict(int codePage)
    {
        var encoding = (Encoding)TextEncodings.Resolve(codePage).Clone();
        encoding.EncoderFallback = EncoderFallback.ExceptionFallback;
        return encoding;
    }

    // A CSV holding header names, quoted fields (separators, line ends, doubled quotes), nulls, numbers, dates and
    // words of the language.
    private static string Sample(string word, char separator = ',') =>
        $"id{separator}{word}{separator}value{separator}day{separator}flag\n" +
        $"1{separator}\"{word}{separator} \"\"q\"\"\n{word}\"{separator}1.5{separator}2024-01-02{separator}{word}\n" +
        $"2{separator}{word}{word}{separator}{separator}2024-03-02{separator}NA\n" +
        $"3{separator}{separator}-7{separator}{separator}{word}\n";

    public static TheoryData<int, string> Pages() => new()
    {
        { 1252, "Conceição €" },
        { 28591, "Ñandú ÿ" },
        { 932, "日本語ｶﾅ①" },
        { 936, "中文简体€" },
        { 949, "한국어" },
        { 950, "繁體中文" },
        { 54936, "中文𠀀" }, // GB18030: four-byte forms, decoded in order
        { 37, "EBCDIC Àé" }, // EBCDIC: no byte is ASCII
        { 1200, "UTF-16 日本 😀" },
        { 1201, "UTF-16BE 日本" },
    };

    [Theory]
    [MemberData(nameof(Pages))]
    public void TextInACodePage_ReadsAsTheSameTextInUtf8(int codePage, string word)
    {
        string text = Sample(word);
        var options = new CsvReadOptions { NullValues = ["NA"], TrueValues = [word], FalseValues = ["x"] };
        string expected = Utf8(text, options);
        byte[] bytes = Strict(codePage).GetBytes(text);
        foreach (bool force in (bool[])[false, true])
            foreach (int unit in (int[])[1 << 20, 7, 1])
                foreach (CsvBehaviourTests.CsvSource source in Enum.GetValues<CsvBehaviourTests.CsvSource>())
                    Assert.Equal(expected, CsvBehaviourTests.Read(bytes, options with { Encoding = TextEncodings.Resolve(codePage), ForceTranscode = force, UnitBytes = unit }, source));
    }

    [Fact]
    public void ADoubleBytePageWithAStructuralByteAbove0x40_IsDecodedFirst()
    {
        // A CP932 character whose second byte is '|' (0x7C): cut on the raw bytes, '|' would split it.
        Encoding cp932 = Strict(932);
        char trail = Enumerable.Range(0x3000, 0x6000).Select(c => (char)c)
            .First(c => !char.IsSurrogate(c) && Encodable(cp932, c) is [_, 0x7C]);
        string text = $"a|b\n1|{trail}{trail}\n2|x{trail}\n";
        var options = new CsvReadOptions { Separator = '|' };
        Assert.Equal(Utf8(text, options), Read(cp932.GetBytes(text), options with { Encoding = TextEncodings.Resolve(932) }));

        static byte[]? Encodable(Encoding e, char c)
        {
            try { return e.GetBytes([c]); }
            catch (EncoderFallbackException) { return null; }
        }
    }

    [Fact]
    public void ByteOrderMarks()
    {
        string text = "a,b\n1,日本\n";
        string expected = Utf8(text);
        byte[] le = [.. Encoding.Unicode.GetPreamble(), .. Encoding.Unicode.GetBytes(text)];
        byte[] be = [.. Encoding.BigEndianUnicode.GetPreamble(), .. Encoding.BigEndianUnicode.GetBytes(text)];
        byte[] utf8 = [.. Encoding.UTF8.GetPreamble(), .. Encoding.UTF8.GetBytes(text)];
        // No encoding (or UTF-8) given: the mark decides.
        foreach (byte[] marked in (byte[][])[le, be, utf8])
        {
            Assert.Equal(expected, Read(marked, new CsvReadOptions()));
            Assert.Equal(expected, Read(marked, new CsvReadOptions { Encoding = Encoding.UTF8 }));
        }
        // The encoding of the mark given: read; another one given: an error naming both.
        Assert.Equal(expected, Read(le, new CsvReadOptions { Encoding = Encoding.Unicode }));
        var e = Assert.Throws<CsvFormatException>(() => Read(le, new CsvReadOptions { Encoding = TextEncodings.Resolve(932) }));
        Assert.Contains("utf-16 byte order mark", e.Message);
        Assert.Contains("shift_jis", e.Message);
        Assert.Throws<CsvFormatException>(() => Read(utf8, new CsvReadOptions { Encoding = TextEncodings.Resolve(1252) }));
        // UTF-16 without a mark, given.
        Assert.Equal(expected, Read(Encoding.Unicode.GetBytes(text), new CsvReadOptions { Encoding = Encoding.Unicode }));
    }

    [Fact]
    public void CompressedText_ItsMarkFoundInside()
    {
        byte[] le = [.. Encoding.Unicode.GetPreamble(), .. Encoding.Unicode.GetBytes("a,b\n1,日本\n")];
        using var compressed = new MemoryStream();
        using (var gz = new GZipStream(compressed, CompressionLevel.Fastest, leaveOpen: true)) gz.Write(le);
        foreach (CsvBehaviourTests.CsvSource source in Enum.GetValues<CsvBehaviourTests.CsvSource>())
            Assert.Equal("a:i64,b:str | 1,\"日本\"", CsvBehaviourTests.Read(compressed.ToArray(), new CsvReadOptions(), source));
        byte[] cp932 = Strict(932).GetBytes("a,b\n1,日本\n");
        using var c2 = new MemoryStream();
        using (var gz = new GZipStream(c2, CompressionLevel.Fastest, leaveOpen: true)) gz.Write(cp932);
        Assert.Equal("a:i64,b:str | 1,\"日本\"", Read(c2.ToArray(), new CsvReadOptions { Encoding = TextEncodings.Resolve(932) }));
    }

    /// <summary>Strict reads fail the record holding the first invalid unit, at its row and line, on either path, in any units.</summary>
    [Theory]
    [InlineData(932, new byte[] { 0x81, 0x20 }, "shift_jis")] // a lead byte, then no trail
    [InlineData(936, new byte[] { 0x81, 0x0A }, "gb2312")]
    [InlineData(1200, new byte[] { 0x00, 0xD8 }, "utf-16")] // a lone high surrogate
    [InlineData(54936, new byte[] { 0x81, 0x30, 0x0A }, "gb18030")]
    public void StrictErrorsAtTheirRowAndLine(int codePage, byte[] invalid, string name)
    {
        Encoding encoding = Strict(codePage);
        byte[] head = encoding.GetBytes("a,b\n1,\"x\ny\"\n2,ok\n3,");
        byte[] tail = encoding.GetBytes("\n4,z\n");
        byte[] bytes = [.. head, .. invalid, .. tail];
        string expected = $"Row 3 (line 5): the text is not valid {name} (check the encoding, or read with InvalidText = Replace)";
        foreach (bool force in (bool[])[false, true])
            foreach (int unit in (int[])[1 << 20, 3])
            {
                var options = new CsvReadOptions { Encoding = TextEncodings.Resolve(codePage), ForceTranscode = force, UnitBytes = unit };
                var e = Assert.Throws<CsvFormatException>(() => Read(bytes, options));
                Assert.Equal(expected, e.Message);
                // Lossy: a replacement character, the rest read.
                string lossy = Read(bytes, options with { InvalidText = CsvInvalidText.Replace });
                Assert.Contains("3,\"�", lossy);
                Assert.EndsWith("4,\"z\"", lossy);
            }
    }

    [Fact]
    public void LossyLegacy_LeadByteAloneReplaced_TheByteAfterReadAgain()
    {
        // 0x81 then a newline: U+FFFD for 0x81, the newline still ends the record.
        byte[] bytes = [.. "a\nx"u8, 0x81, .. "\ny\n"u8];
        var options = new CsvReadOptions { Encoding = TextEncodings.Resolve(932), InvalidText = CsvInvalidText.Replace };
        Assert.Equal("a:str | \"x�\" | \"y\"", Read(bytes, options));
        Assert.Equal("a:str | \"x�\" | \"y\"", Read(bytes, options with { ForceTranscode = true }));
    }

    [Fact]
    public void LossyUtf8_RepairsInvalidSequences_ValidUnitsAsTheyAre()
    {
        byte[] bytes = [.. "a,b\n1,Jos"u8, 0xE9, .. "\n2,ok\n"u8];
        Assert.Equal("a:i64,b:str | 1,\"Jos�\" | 2,\"ok\"", Read(bytes, new CsvReadOptions { InvalidText = CsvInvalidText.Replace }));
        var e = Assert.Throws<CsvFormatException>(() => Read(bytes, new CsvReadOptions()));
        Assert.Equal("Row 1 (line 2): the text is not UTF-8 (give the input's encoding)", e.Message);
    }

    [Fact]
    public void ParsedText_IsUtf8WhateverTheOptionsSay()
    {
        using CsvReader reader = CsvReader.Parse("a\n日本\n", new CsvReadOptions { Encoding = TextEncodings.Resolve(932) });
        var batches = reader.ReadBatches().ToList();
        Assert.Equal("a:str | \"日本\"", CsvBehaviourTests.Render(reader.Schema, batches));
        batches.ForEach(b => b.Dispose());
    }

    [Fact]
    public void ValuesInACodePage_ErrorsShowTheirText()
    {
        Encoding cp932 = Strict(932);
        var e = Assert.Throws<CsvValueException>(() => Read(cp932.GetBytes("a\n1\n日本\n"),
            new CsvReadOptions { Encoding = TextEncodings.Resolve(932), Schema = new Schema([new Field("a", Int64Type.Default, true)], null) }));
        Assert.Equal("Row 2 (line 3): column 'a': '日本' is not an Int64 integer", e.Message);
    }

    /// <summary>
    /// Invalid text found as fields are decoded fails its record before a value refused in that record (in any column
    /// order), after a value refused in an earlier one; in a column not read it fails all the same.
    /// </summary>
    [Fact]
    public void InvalidTextAndRefusedValues_FirstInRecordOrder_WhateverColumnsAreRead()
    {
        Encoding cp932 = Strict(932);
        // Past the rows read at open (their input errors are raised there), so units decode the failures.
        string head = "a,b\n" + string.Concat(Enumerable.Range(0, 150).Select(i => $"{i},日本\n"));
        byte[] Bytes(string before, byte[] invalid, string after) => [.. cp932.GetBytes(head + before[4..]), .. invalid, .. cp932.GetBytes(after)];
        var schema = new Schema([new Field("a", Int64Type.Default, true), new Field("b", StringType.Default, true)], null);
        var options = new CsvReadOptions { Encoding = TextEncodings.Resolve(932), Schema = schema };
        const string Invalid = "the text is not valid shift_jis (check the encoding, or read with InvalidText = Replace)";
        foreach (int unit in (int[])[1 << 20, 5])
        {
            var o = options with { UnitBytes = unit };
            // Row 2: a refused value in 'a' and invalid text in 'b': the invalid text.
            var e = Assert.ThrowsAny<Exception>(() => Read(Bytes("a,b\n1,日本\nx,", [0x81, 0x20], "\n3,ok\n"), o));
            Assert.Equal($"Row 152 (line 153): {Invalid}", e.Message);
            // Invalid text in row 3, a refused value in row 2: the value.
            e = Assert.ThrowsAny<Exception>(() => Read(Bytes("a,b\n1,日本\nx,ok\n3,", [0x81, 0x20], "\n"), o));
            Assert.Equal("Row 152 (line 153): column 'a': 'x' is not an Int64 integer", e.Message);
            // Invalid text in a column not read.
            e = Assert.ThrowsAny<Exception>(() => Read(Bytes("a,b\n1,日本\n2,", [0x81, 0x20], "\n"), o with { Columns = ["a"] }));
            Assert.Equal($"Row 152 (line 153): {Invalid}", e.Message);
            Assert.IsType<CsvFormatException>(e);
        }
    }

    /// <summary>UTF-16 cut into blocks decoded side by side never splits a surrogate pair, whatever the block size and source.</summary>
    [Theory]
    [InlineData(1200)]
    [InlineData(1201)]
    public void Utf16Blocks_KeepSurrogatePairsWhole(int codePage)
    {
        var text = new StringBuilder("a,b\n");
        for (int i = 0; i < 40; i++) text.Append(i).Append(',').Append(new string('x', i % 7)).Append("😀😀𠀀").Append('\n');
        string expected = Utf8(text.ToString());
        byte[] bytes = Strict(codePage).GetBytes(text.ToString());
        for (int unit = 16; unit <= 40; unit++)
            foreach (CsvBehaviourTests.CsvSource source in (CsvBehaviourTests.CsvSource[])[CsvBehaviourTests.CsvSource.Memory, CsvBehaviourTests.CsvSource.NonSeekable])
                Assert.Equal(expected, CsvBehaviourTests.Read(bytes, new CsvReadOptions { Encoding = TextEncodings.Resolve(codePage), UnitBytes = unit }, source));
    }

    /// <summary>A byte a single-byte page leaves undefined: an error read strictly, U+FFFD read lossily, on either path.</summary>
    [Fact]
    public void SingleBytePage_UndefinedByte()
    {
        (int page, byte b) = LegacyText.AllowList.ToArray()
            .Select(p => (Page: p, Decoder: LegacyText.For(p, false)!))
            .Where(x => !x.Decoder.DoubleByte)
            .SelectMany(x => Enumerable.Range(0x80, 0x80).Select(v => (x.Page, Byte: (byte)v, x.Decoder)))
            .Where(x => x.Decoder.FirstInvalid([x.Byte]) == 0)
            .Select(x => (x.Page, x.Byte))
            .First();
        byte[] bytes = [.. "a\nok\nx"u8, b, .. "\ny\n"u8];
        foreach (bool force in (bool[])[false, true])
        {
            var options = new CsvReadOptions { Encoding = TextEncodings.Resolve(page), ForceTranscode = force };
            var e = Assert.Throws<CsvFormatException>(() => Read(bytes, options));
            Assert.StartsWith("Row 2 (line 3): the text is not valid", e.Message);
            Assert.Equal("a:str | \"ok\" | \"x�\" | \"y\"", Read(bytes, options with { InvalidText = CsvInvalidText.Replace }));
        }
    }
}
