using System.Buffers;
using System.Text;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Text;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="TextEncoder"/>: the tables of the legacy code pages against .NET's encoding (with '?' for what a page
/// cannot write, never a look-alike), every BMP character; what a strict encoder writes reads back through the reader's
/// tables as the same character; UTF-16 and .NET-encoded pages; shift-state encodings refused.
/// </summary>
public class TextEncodersTests
{
    public static TheoryData<int> Pages() => [.. LegacyText.AllowList.ToArray()];

    [Theory]
    [MemberData(nameof(Pages))]
    public void Tables_WriteWhatNetWrites_AndStrictBytesReadBack(int codePage)
    {
        Encoding replacing = TextEncoder.Clone(TextEncodings.Resolve(codePage), replace: true);
        TextEncoder lossy = TextEncoder.For(TextEncodings.Resolve(codePage), replace: true)!, strict = TextEncoder.For(TextEncodings.Resolve(codePage), replace: false)!;
        TextDecoder decoder = TextEncodings.Decoder(TextEncodings.Resolve(codePage), replace: false);
        using var output = new NativeBufferWriter(64);
        Span<byte> utf8 = stackalloc byte[4];
        Span<byte> back = stackalloc byte[16];
        List<string> failures = [];
        for (int c = 0; c < 65536 && failures.Count < 10; c++)
        {
            if (char.IsSurrogate((char)c)) continue;
            int length = new Rune(c).EncodeToUtf8(utf8);
            output.ResetWrittenCount();
            Assert.True(lossy.TryEncode(utf8[..length], output, out _));
            byte[] expected = replacing.GetBytes([(char)c]);
            if (!output.WrittenSpan.SequenceEqual(expected))
                failures.Add($"U+{c:X4}: .NET {Convert.ToHexString(expected)}, table {Convert.ToHexString(output.WrittenSpan)}");
            output.ResetWrittenCount();
            if (!strict.TryEncode(utf8[..length], output, out _)) continue;
            // Written strictly: decodes (by the reader's tables) to the same character.
            OperationStatus status = decoder.Decode(output.WrittenSpan, back, true, out _, out int written);
            if (status != OperationStatus.Done || !back[..written].SequenceEqual(utf8[..length]))
                failures.Add($"U+{c:X4}: written {Convert.ToHexString(output.WrittenSpan)}, read back as {Convert.ToHexString(back[..written])} ({status})");
        }
        Assert.True(failures.Count == 0, string.Join("\n", failures));
    }

    [Theory]
    [InlineData(1200)]
    [InlineData(1201)]
    [InlineData(54936)]
    [InlineData(12000)]
    [InlineData(37)]
    [InlineData(932)]
    [InlineData(1252)]
    public void Text_IsWhatNetWrites_WithQuestionMarksForTheRest(int codePage)
    {
        string text = "plain, \"quoted\"\r\n日本語の文字 café ñ ü Ωμέγα кириллица 中文汉字 한국어 😀🎉 ，＂ ≒ ∥ ～ x" + new string('a', 9000) + "漢";
        Encoding encoding = TextEncodings.Resolve(codePage);
        TextEncoder encoder = TextEncoder.For(encoding, replace: true)!;
        using var output = new NativeBufferWriter(64);
        Assert.True(encoder.TryEncode(Encoding.UTF8.GetBytes(text), output, out _));
        Assert.Equal(Convert.ToHexString(TextEncoder.Clone(encoding, replace: true).GetBytes(text)), Convert.ToHexString(output.WrittenSpan));
    }

    [Theory]
    [InlineData(932, "日本 café", 10)]
    [InlineData(1252, "café ，", 6)]
    [InlineData(37, "abc 日", 4)]
    [InlineData(936, "中文 😀", 7)]
    public void Strict_StopsAtTheFirstCharacterTheEncodingCannotWrite(int codePage, string text, int utf8Offset)
    {
        TextEncoder encoder = TextEncoder.For(TextEncodings.Resolve(codePage), replace: false)!;
        using var output = new NativeBufferWriter(64);
        Assert.False(encoder.TryEncode(Encoding.UTF8.GetBytes(text), output, out int at));
        Assert.Equal(utf8Offset, at);
    }

    [Theory]
    [InlineData(932, false)]
    [InlineData(932, true)]
    [InlineData(1252, true)]
    [InlineData(1200, false)]
    [InlineData(1201, false)]
    [InlineData(54936, false)]
    [InlineData(54936, true)]
    [InlineData(12000, true)]
    [InlineData(37, true)]
    public void BytesThatAreNotUtf8_StopTheEncoding_WhereTheyStart_ReplacingOrNot(int codePage, bool replace)
    {
        TextEncoder encoder = TextEncoder.For(TextEncodings.Resolve(codePage), replace)!;
        // A lead byte without its continuation, a lone continuation byte, overlong forms, a surrogate, a cut-short end.
        foreach (byte[] bad in (byte[][])[[0xC3, 0x28], [0x80], [0xC0, 0xAF], [0xE0, 0x80, 0xAF], [0xED, 0xA0, 0x80], [0xE6, 0x97]])
        {
            using var output = new NativeBufferWriter(64);
            Assert.False(encoder.TryEncode([.. "abc, d"u8, .. bad, .. "z"u8], output, out int at));
            Assert.Equal(6, at);
        }
        // A character cut short at the end of the span, though the bytes after it in memory would complete it.
        foreach (byte[] whole in (byte[][])[[0xC3, 0xA9], [0xE6, 0x97, 0xA5], [0xF0, 0x9F, 0x98, 0x80]])
            for (int cut = 1; cut < whole.Length; cut++)
            {
                byte[] memory = [.. "abc, d"u8, .. whole];
                using var output = new NativeBufferWriter(64);
                Assert.False(encoder.TryEncode(memory.AsSpan(0, 6 + cut), output, out int at));
                Assert.Equal(6, at);
            }
    }

    [Theory]
    [InlineData(50220)]
    [InlineData(50222)]
    [InlineData(65000)]
    [InlineData(52936)]
    public void EncodingsWithShiftStates_AreRefused(int codePage) =>
        Assert.Throws<NotSupportedException>(() => TextEncoder.For(TextEncodings.Resolve(codePage), replace: true));

    [Fact]
    public void ByteOrderMarks_AreTheUnicodeEncodings()
    {
        Assert.Equal([0xEF, 0xBB, 0xBF], TextEncoder.ByteOrderMark(65001).ToArray());
        Assert.Equal([0xFF, 0xFE], TextEncoder.ByteOrderMark(1200).ToArray());
        Assert.Equal([0xFE, 0xFF], TextEncoder.ByteOrderMark(1201).ToArray());
        Assert.Equal([0xFF, 0xFE, 0, 0], TextEncoder.ByteOrderMark(12000).ToArray());
        Assert.True(TextEncoder.ByteOrderMark(932).IsEmpty);
        Assert.Null(TextEncoder.For(Encoding.UTF8, replace: false));
    }
}
