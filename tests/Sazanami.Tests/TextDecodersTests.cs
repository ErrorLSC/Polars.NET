using System.Buffers;
using System.Text;
using Kaminari.Sazanami.Text;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="TextDecoder"/>s into UTF-8: the table code pages (<see cref="LegacyText"/>) equal to .NET's own decoding
/// on every single byte and pair and on random text, strict and lossy invalid units, streams cut anywhere, UTF-8,
/// UTF-16, the in-order decoder of other pages, and <see cref="TextEncodings"/>' byte order mark rules.
/// </summary>
public class TextDecodersTests
{
    private static readonly char[] Suspects = ['�', '?', '・'];

    private static byte[]? Decode(TextDecoder decoder, ReadOnlySpan<byte> source, out OperationStatus status, out int consumed)
    {
        byte[] destination = new byte[source.Length * 3];
        status = decoder.Decode(source, destination, true, out consumed, out int written);
        return status == OperationStatus.Done ? destination[..written] : null;
    }

    private static string Text(byte[]? utf8) => utf8 is null ? "<invalid>" : Encoding.UTF8.GetString(utf8);

    /// <summary>
    /// Every allow-listed page: ASCII as itself, and every single byte and pair either decoded exactly as .NET decodes it
    /// or refused because .NET can only replace it (a replacement character that does not encode back).
    /// </summary>
    [Fact]
    public void AllowListedPages_DecodeEveryUnitAsDotNet()
    {
        foreach (int codePage in LegacyText.AllowList.ToArray())
        {
            Encoding e = TextEncodings.Resolve(codePage);
            TextDecoder decoder = TextEncodings.Decoder(e, false);
            Assert.IsType<LegacyText>(decoder);
            Assert.Equal(TextLayout.AsciiCompatible, decoder.Layout);
            for (int a = 0; a < 256; a++)
                for (int b = -1; b < 256; b++)
                {
                    if (b >= 0 && a < 0x80) break; // pairs start with a byte ≥ 0x80
                    byte[] x = b < 0 ? [(byte)a] : [(byte)a, (byte)b];
                    string net = e.GetString(x);
                    byte[]? ours = Decode(decoder, x, out _, out _);
                    if (ours is not null) Assert.True(net == Text(ours), $"{codePage} {Convert.ToHexString(x)}: .NET {net}, ours {Text(ours)}");
                    else Assert.True(net.IndexOfAny(Suspects) >= 0 && !e.GetBytes(net).AsSpan().SequenceEqual(x), $"{codePage} {Convert.ToHexString(x)} refused, .NET decodes {net}");
                }
            for (int b = 0; b < 0x80; b++) Assert.Equal(((char)b).ToString(), e.GetString([(byte)b]));
        }
    }

    /// <summary>Random text of valid units and ASCII runs equals .NET's decoding, whole and cut into random pieces.</summary>
    [Theory]
    [InlineData(932)]
    [InlineData(936)]
    [InlineData(949)]
    [InlineData(950)]
    [InlineData(1252)]
    [InlineData(20932)]
    public void RandomText_MatchesDotNet_WholeAndInPieces(int codePage)
    {
        Encoding e = TextEncodings.Resolve(codePage);
        TextDecoder decoder = TextEncodings.Decoder(e, false);
        List<byte[]> units = [];
        for (int a = 0x80; a < 256; a++)
            for (int b = -1; b < 256; b++)
            {
                byte[] x = b < 0 ? [(byte)a] : [(byte)a, (byte)b];
                if (Decode(decoder, x, out _, out _) is not null && (b < 0 || Decode(decoder, [(byte)a], out _, out _) is null)) units.Add(x);
            }
        var r = new Random(codePage);
        for (int i = 0; i < 3000; i++)
        {
            byte[] x = RandomText(r, units);
            byte[] expected = Encoding.UTF8.GetBytes(e.GetString(x));
            Assert.Equal(expected, Decode(decoder, x, out _, out _));
            Assert.Equal(expected, Pieces(decoder, x, r));
        }
    }

    private static byte[] RandomText(Random r, List<byte[]> units)
    {
        List<byte> b = [];
        for (int n = r.Next(0, 40); n > 0; n--)
        {
            if (r.Next(3) == 0) for (int k = r.Next(0, r.Next(2) == 0 ? 4 : 60); k > 0; k--) b.Add((byte)r.Next(0x20, 0x7F));
            else b.AddRange(units[r.Next(units.Count)]);
        }
        return [.. b];
    }

    /// <summary>Decodes in random pieces, each not final but the last, carrying what a piece leaves.</summary>
    private static byte[] Pieces(TextDecoder decoder, byte[] x, Random r)
    {
        List<byte> output = [];
        byte[] buffer = new byte[x.Length * 3 + 3];
        int at = 0, carried = 0;
        while (true)
        {
            int end = Math.Min(x.Length, at + r.Next(1, 9));
            byte[] piece = x[(at - carried)..end];
            bool final = end == x.Length;
            OperationStatus status = decoder.Decode(piece, buffer, final, out int consumed, out int written);
            output.AddRange(buffer[..written]);
            Assert.True(status is OperationStatus.Done or OperationStatus.NeedMoreData, status.ToString());
            carried = piece.Length - consumed;
            at = end;
            if (final)
            {
                Assert.Equal(0, carried);
                return [.. output];
            }
        }
    }

    /// <summary>
    /// CP932 as Windows decodes it: the NEC-selected IBM rows (ED/EE) and the NEC row 13 duplicates that .NET's exception
    /// fallback would refuse, half-width katakana, and '\' as a backslash.
    /// </summary>
    [Theory]
    [InlineData("ED40", "纊")]
    [InlineData("EEEC", "黑")]
    [InlineData("8790", "≒")]
    [InlineData("B1", "ｱ")]
    [InlineData("DF", "ﾟ")]
    [InlineData("5C", "\\")]
    [InlineData("955C", "表")] // a trail byte 0x5C: no backslash inside a character
    [InlineData("82A082A2", "あい")]
    public void Cp932(string hex, string text) =>
        Assert.Equal(text, Text(Decode(TextEncodings.Decoder(TextEncodings.Resolve(932), false), Convert.FromHexString(hex), out _, out _)));

    /// <summary>
    /// Windows' mappings Python's codecs refuse are kept (RULE E1): C1 controls of CP1252's holes, the euro of CP936, private
    /// use rows.
    /// </summary>
    [Theory]
    [InlineData(1252, "81", "\u0081")]
    [InlineData(1252, "80", "€")]
    [InlineData(936, "80", "€")]
    [InlineData(950, "8140", "")]
    public void WindowsMappings_AreValid(int codePage, string hex, string text) =>
        Assert.Equal(text, Text(Decode(TextEncodings.Decoder(TextEncodings.Resolve(codePage), false), Convert.FromHexString(hex), out _, out _)));

    /// <summary>
    /// Strict stops at the invalid unit; lossy writes one U+FFFD for its lead byte and reads the next byte again, so a
    /// newline, a quote or a separator after a stray lead byte survives.
    /// </summary>
    [Theory]
    [InlineData(932, "41810A42", 1, "A�\nB")] // lead + LF: the LF stays
    [InlineData(932, "8122", 0, "�\"")]
    [InlineData(932, "81FF8140", 0, "\uFFFD\uF8F3\u3000")] // the lead replaced, 0xFF read again (a private use character, as Windows maps it)
    [InlineData(936, "41FF", 1, "A�")] // 0xFF is no GBK character … but .NET maps it: see below
    [InlineData(949, "C9", 0, "�")] // a lead byte last
    [InlineData(20127, "41C042", 1, "A�B")] // US-ASCII refuses every byte ≥ 0x80
    public void InvalidUnits(int codePage, string hex, int offset, string lossy)
    {
        Encoding e = TextEncodings.Resolve(codePage);
        byte[] x = Convert.FromHexString(hex);
        if (codePage == 936)
        {
            // .NET decodes 0xFF in CP936 to a private use character (Windows does): valid.
            Assert.NotNull(Decode(TextEncodings.Decoder(e, false), x, out _, out _));
            return;
        }
        Assert.Null(Decode(TextEncodings.Decoder(e, false), x, out OperationStatus status, out int consumed));
        Assert.Equal(OperationStatus.InvalidData, status);
        Assert.Equal(offset, consumed);
        Assert.Equal(lossy, Text(Decode(TextEncodings.Decoder(e, true), x, out _, out _)));
    }

    /// <summary>A lead byte last: more is needed unless the source is final, then it is invalid.</summary>
    [Fact]
    public void LeadByteLast()
    {
        TextDecoder decoder = TextEncodings.Decoder(TextEncodings.Resolve(932), false);
        byte[] destination = new byte[16];
        Assert.Equal(OperationStatus.NeedMoreData, decoder.Decode([0x41, 0x82], destination, false, out int consumed, out int written));
        Assert.Equal((1, 1), (consumed, written));
        Assert.Equal(OperationStatus.InvalidData, decoder.Decode([0x41, 0x82], destination, true, out consumed, out _));
        Assert.Equal(1, consumed);
        Assert.Equal(OperationStatus.Done, decoder.Decode([0x82, 0xA0], destination, true, out _, out written));
        Assert.Equal("あ", Encoding.UTF8.GetString(destination, 0, written));
    }

    /// <summary>Three bytes per source byte suffice, written exactly (no 4-byte write past the end); less is DestinationTooSmall.</summary>
    [Fact]
    public void Destination_ExactCapacity()
    {
        foreach ((int codePage, string hex, string text) in (ValueTuple<int, string, string>[])[(1252, "41E9", "Aé"), (1252, "80", "€"), (932, "82A0", "あ"), (932, "8140", "　")])
        {
            TextDecoder decoder = TextEncodings.Decoder(TextEncodings.Resolve(codePage), false);
            byte[] x = Convert.FromHexString(hex), expected = Encoding.UTF8.GetBytes(text);
            byte[] exact = new byte[expected.Length + 1];
            Assert.Equal(OperationStatus.Done, decoder.Decode(x, exact.AsSpan(0, expected.Length), true, out _, out int written));
            Assert.Equal(expected, exact[..written]);
            Assert.Equal(0, exact[^1]); // nothing past the destination
            Assert.Equal(OperationStatus.DestinationTooSmall, decoder.Decode(x, exact.AsSpan(0, expected.Length - 1), true, out _, out _));
        }
    }

    /// <summary>UTF-8: valid text as it is; strict stops at the first invalid sequence; lossy replaces as .NET does.</summary>
    [Fact]
    public void Utf8()
    {
        var strict = new Utf8TextDecoder(false);
        var lossy = new Utf8TextDecoder(true);
        var r = new Random(8);
        string[] pieces = ["a", "é", "日本", "😀", ",", "\n"];
        for (int i = 0; i < 20_000; i++)
        {
            byte[] x = Encoding.UTF8.GetBytes(string.Concat(Enumerable.Range(0, r.Next(0, 20)).Select(_ => pieces[r.Next(pieces.Length)])));
            if (x.Length > 0 && r.Next(2) == 0) x[r.Next(x.Length)] = (byte)r.Next(256);
            Assert.Equal(Encoding.UTF8.GetBytes(Encoding.UTF8.GetString(x)), Decode(lossy, x, out _, out _));
            byte[]? ours = Decode(strict, x, out OperationStatus status, out int consumed);
            Assert.Equal(System.Text.Unicode.Utf8.IsValid(x), ours is not null);
            if (ours is null)
            {
                Assert.Equal(OperationStatus.InvalidData, status);
                Assert.True(System.Text.Unicode.Utf8.IsValid(x.AsSpan(0, consumed)));
            }
            else Assert.Equal(x, Pieces(strict, x, r));
        }
        Assert.Equal(OperationStatus.NeedMoreData, strict.Decode([0x41, 0xE6, 0x97], new byte[8], false, out int c, out _));
        Assert.Equal(1, c);
    }

    /// <summary>UTF-16 both ways: surrogate pairs (also across the decoder's internal chunks), lone surrogates, an odd last byte.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Utf16(bool bigEndian)
    {
        Encoding e = bigEndian ? Encoding.BigEndianUnicode : Encoding.Unicode;
        var strict = new Utf16TextDecoder(bigEndian, false);
        var lossy = new Utf16TextDecoder(bigEndian, true);
        Assert.Equal(TextLayout.Utf16, strict.Layout);
        foreach (int at in (int[])[0, 1022, 1023, 1024, 2047])
        {
            string s = new string('a', at) + "😀" + new string('b', 1500);
            Assert.Equal(s, Text(Decode(strict, e.GetBytes(s), out _, out _)));
        }
        // An odd last byte: half a code unit — more is needed unless final, then invalid (lossy: U+FFFD).
        byte[] odd = bigEndian ? [0x00, 0x41, 0x00] : [0x41, 0x00, 0x42];
        byte[] destination = new byte[16];
        Assert.Equal(OperationStatus.NeedMoreData, strict.Decode(odd, destination, false, out int consumed, out int written));
        Assert.Equal((2, 1), (consumed, written));
        Assert.Equal(OperationStatus.InvalidData, strict.Decode(odd, destination, true, out consumed, out _));
        Assert.Equal(2, consumed);
        Assert.Equal("A\uFFFD", Text(Decode(lossy, odd, out _, out _)));

        var r = new Random(16);
        for (int i = 0; i < 2000; i++)
        {
            char[] chars = [.. Enumerable.Range(0, r.Next(0, 2500)).Select(_ => r.Next(10) switch { 0 => (char)r.Next(0xD800, 0xE000), 1 => '\uD83D', _ => (char)r.Next(0x20, 0x3000) })];
            byte[] x = new byte[chars.Length * 2 + (r.Next(8) == 0 ? 1 : 0)];
            for (int k = 0; k < chars.Length; k++) (x[2 * k], x[2 * k + 1]) = bigEndian ? ((byte)(chars[k] >> 8), (byte)chars[k]) : ((byte)chars[k], (byte)(chars[k] >> 8));
            if (x.Length % 2 == 1) x[^1] = 0x41;
            Assert.Equal(Encoding.UTF8.GetBytes(e.GetString(x)), Decode(lossy, x, out _, out _));
            bool valid = x.Length % 2 == 0 && !e.GetString(x).Contains('�');
            byte[]? ours = Decode(strict, x, out _, out _);
            Assert.Equal(valid, ours is not null);
            if (ours is not null) Assert.Equal(ours, Pieces(strict, x, r));
        }
    }

    /// <summary>Pages off the allow-list decode in order through .NET (GB18030's 4-byte forms, ISO-2022-JP's shifts, EBCDIC).</summary>
    [Theory]
    [InlineData(54936)]
    [InlineData(50220)]
    [InlineData(37)]
    public void OffListPages_InOrder(int codePage)
    {
        Encoding e = TextEncodings.Resolve(codePage);
        TextDecoder decoder = TextEncodings.Decoder(e, false);
        Assert.IsType<SequentialTextDecoder>(decoder);
        Assert.Equal(TextLayout.InOrder, decoder.Layout);
        var r = new Random(codePage);
        const string pool = "abcXYZ019 ,;\"\n日本語テキスト中文字符éàü€😀";
        for (int i = 0; i < 500; i++)
        {
            string s = new([.. Enumerable.Range(0, r.Next(0, 3000)).Select(_ => pool[r.Next(pool.Length)])]);
            byte[] x = e.GetBytes(e.GetString(e.GetBytes(s)));
            byte[] expected = Encoding.UTF8.GetBytes(e.GetString(x));
            Assert.Equal(expected, Decode(TextEncodings.Decoder(e, false), x, out _, out _));
            Assert.Equal(expected, Pieces(TextEncodings.Decoder(e, false), x, r));
        }
        // Strict: an invalid sequence stops it; lossy replaces it.
        if (codePage == 54936)
        {
            byte[] x = [0x41, 0x81, 0x30, 0x42];
            Assert.Null(Decode(TextEncodings.Decoder(e, false), x, out OperationStatus status, out _));
            Assert.Equal(OperationStatus.InvalidData, status);
            Assert.Contains('�', Text(Decode(TextEncodings.Decoder(e, true), x, out _, out _)));
        }
    }

    [Fact]
    public void Resolve_ByCodePageAndName()
    {
        Assert.Equal(932, TextEncodings.Resolve("shift_jis").CodePage);
        Assert.Equal(1252, TextEncodings.Resolve("windows-1252").CodePage);
        Assert.Equal(28591, TextEncodings.Resolve(28591).CodePage);
        Assert.IsType<Utf8TextDecoder>(TextEncodings.Decoder(TextEncodings.Resolve(65001), false));
        Assert.IsType<Utf16TextDecoder>(TextEncodings.Decoder(Encoding.BigEndianUnicode, false));
        Assert.IsType<SequentialTextDecoder>(TextEncodings.Decoder(TextEncodings.Resolve(10021), false)); // not ASCII-compatible
        Assert.Throws<NotSupportedException>(() => TextEncodings.Resolve(12345));
    }

    /// <summary>A byte order mark names the encoding unless another one is given; a given non-UTF-8 encoding meeting another's mark is an error.</summary>
    [Fact]
    public void ByteOrderMarks()
    {
        Assert.Equal(65001, TextEncodings.DetectBom([0xEF, 0xBB, 0xBF, 0x41], out int length)?.CodePage);
        Assert.Equal(3, length);
        Assert.Equal(1200, TextEncodings.DetectBom([0xFF, 0xFE, 0x00, 0x00], out length)?.CodePage); // not UTF-32
        Assert.Equal(1201, TextEncodings.DetectBom([0xFE, 0xFF], out length)?.CodePage);
        Assert.Null(TextEncodings.DetectBom([0xEF, 0xBB], out length));
        Assert.Equal(0, length);

        Assert.True(TextEncodings.TryChoose(null, [0x41], out Encoding chosen, out length));
        Assert.Equal((65001, 0), (chosen.CodePage, length));
        Assert.True(TextEncodings.TryChoose(null, [0xFF, 0xFE, 0x41, 0x00], out chosen, out length));
        Assert.Equal((1200, 2), (chosen.CodePage, length));
        Assert.True(TextEncodings.TryChoose(Encoding.UTF8, [0xFE, 0xFF, 0x00, 0x41], out chosen, out length)); // UTF-8 is the default: the mark wins
        Assert.Equal((1201, 2), (chosen.CodePage, length));
        Assert.True(TextEncodings.TryChoose(TextEncodings.Resolve(932), [0x82, 0xA0], out chosen, out length));
        Assert.Equal((932, 0), (chosen.CodePage, length));
        Assert.False(TextEncodings.TryChoose(TextEncodings.Resolve(932), [0xEF, 0xBB, 0xBF, 0x41], out _, out _));
        Assert.True(TextEncodings.TryChoose(Encoding.Unicode, [0xFF, 0xFE, 0x41, 0x00], out chosen, out length));
        Assert.Equal((1200, 2), (chosen.CodePage, length));
    }
}
