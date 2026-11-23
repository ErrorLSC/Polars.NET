using System.Text;

namespace Kaminari.Sazanami.Text;

/// <summary>
/// Text encodings by code page, name or <see cref="Encoding"/>: .NET's own encodings, the legacy code pages through
/// <see cref="CodePagesEncodingProvider"/> asked directly (never registered: that is process-wide), the byte order mark
/// rules, and the decoder that reads each into UTF-8.
/// </summary>
internal static class TextEncodings
{
    /// <summary>The encoding of a code page (932, 1252, 65001 …). Throws <see cref="NotSupportedException"/> for an unknown one.</summary>
    public static Encoding Resolve(int codePage) => CodePagesEncodingProvider.Instance.GetEncoding(codePage) ?? Encoding.GetEncoding(codePage);

    /// <summary>The encoding of a name ("shift_jis", "windows-1252", "utf-8" …). Throws <see cref="ArgumentException"/> for an unknown one.</summary>
    public static Encoding Resolve(string name) => CodePagesEncodingProvider.Instance.GetEncoding(name) ?? Encoding.GetEncoding(name);

    /// <summary>
    /// The encoding a byte order mark at the start of <paramref name="start"/> names (UTF-8, UTF-16 LE or BE; UTF-32 is
    /// not sniffed: FF FE 00 00 is UTF-16LE then a NUL), with the mark's <paramref name="length"/>; null without one.
    /// </summary>
    public static Encoding? DetectBom(ReadOnlySpan<byte> start, out int length)
    {
        (Encoding? encoding, length) = start switch
        {
            [0xEF, 0xBB, 0xBF, ..] => (Encoding.UTF8, 3),
            [0xFF, 0xFE, ..] => (Encoding.Unicode, 2),
            [0xFE, 0xFF, ..] => (Encoding.BigEndianUnicode, 2),
            _ => ((Encoding?)null, 0),
        };
        return encoding;
    }

    /// <summary>
    /// The encoding to read a source with: its byte order mark's when none is given or UTF-8 is (the default); the given
    /// one otherwise, its own mark skipped. False when a given non-UTF-8 encoding meets another encoding's mark.
    /// </summary>
    public static bool TryChoose(Encoding? given, ReadOnlySpan<byte> start, out Encoding encoding, out int bomLength)
    {
        Encoding? marked = DetectBom(start, out bomLength);
        encoding = given ?? marked ?? Encoding.UTF8;
        if (marked is null) return true;
        if (given is null || given.CodePage == Encoding.UTF8.CodePage || given.CodePage == marked.CodePage)
        {
            encoding = marked;
            return true;
        }
        bomLength = 0;
        return false;
    }

    /// <summary>The decoder of <paramref name="encoding"/> into UTF-8: strict, or <paramref name="replace"/> invalid units by U+FFFD.</summary>
    public static TextDecoder Decoder(Encoding encoding, bool replace) => encoding.CodePage switch
    {
        65001 => new Utf8TextDecoder(replace),
        1200 => new Utf16TextDecoder(bigEndian: false, replace),
        1201 => new Utf16TextDecoder(bigEndian: true, replace),
        int codePage => (TextDecoder?)LegacyText.For(codePage, replace) ?? new SequentialTextDecoder(encoding, replace),
    };
}
