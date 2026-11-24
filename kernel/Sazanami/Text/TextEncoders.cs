using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Unicode;
using Kaminari.Sazanami.Buffers;

namespace Kaminari.Sazanami.Text;

/// <summary>
/// Encodes UTF-8 text (whole characters, valid) into another encoding, the counterpart of <see cref="TextDecoder"/>.
/// Strict encoders stop at a character the encoding cannot write (one it would not read back as the same character);
/// lossy ones write '?' for it, as .NET does with '?' as the replacement ("??" past the BMP). Never a "best fit": .NET's
/// default maps a fullwidth comma to ',' on some pages, which would change a CSV record's shape. Encoders are stateless
/// and thread-safe: text cut at character boundaries and encoded piece by piece gives the bytes of the whole.
/// </summary>
internal abstract class TextEncoder(bool replace)
{
    /// <summary>A character the encoding cannot write becomes '?' instead of stopping the encoding.</summary>
    public bool Replace { get; } = replace;

    /// <summary>The most bytes <paramref name="utf8Length"/> bytes of UTF-8 encode to.</summary>
    public abstract int MaxBytes(int utf8Length);

    /// <summary>
    /// Appends <paramref name="utf8"/> encoded to <paramref name="output"/>; false when a character cannot be written
    /// (strict), <paramref name="failedAt"/> its UTF-8 offset (what was appended by then stays).
    /// </summary>
    public abstract bool TryEncode(ReadOnlySpan<byte> utf8, NativeBufferWriter output, out int failedAt);

    /// <summary>
    /// The encoder of <paramref name="encoding"/> (null for UTF-8: nothing to do): tables for the legacy code pages the
    /// decoder reads by tables, UTF-16 directly, .NET's encoder for the others.
    /// </summary>
    /// <exception cref="NotSupportedException">The encoding has shift states (ISO-2022, UTF-7, HZ): pieces cannot be encoded apart.</exception>
    public static TextEncoder? For(Encoding encoding, bool replace)
    {
        ArgumentNullException.ThrowIfNull(encoding);
        switch (encoding.CodePage)
        {
            case 65001: return null;
            case 1200: return new Utf16TextEncoder(bigEndian: false);
            case 1201: return new Utf16TextEncoder(bigEndian: true);
        }
        if (LegacyText.AllowList.Contains(encoding.CodePage) && TableTextEncoder.Tables(encoding.CodePage) is { } tables)
            return new TableTextEncoder(tables, replace);
        if (Stateful(encoding))
            throw new NotSupportedException($"The encoding {encoding.WebName} ({encoding.CodePage}) has shift states: write UTF-8, or a code page without them (Shift-JIS, EUC-JP, GBK …).");
        return new NetTextEncoder(encoding, replace);
    }

    /// <summary>
    /// The byte order mark of a Unicode encoding (UTF-8, UTF-16, UTF-32; by code page, not the encoding object's preamble,
    /// which may be off); empty for any other.
    /// </summary>
    public static ReadOnlySpan<byte> ByteOrderMark(int codePage) => codePage switch
    {
        65001 => [0xEF, 0xBB, 0xBF],
        1200 => [0xFF, 0xFE],
        1201 => [0xFE, 0xFF],
        12000 => [0xFF, 0xFE, 0x00, 0x00],
        12001 => [0x00, 0x00, 0xFE, 0xFF],
        _ => [],
    };

    // An encoding whose bytes for a text are not the bytes of its parts (a shift state carried between them).
    private static bool Stateful(Encoding encoding)
    {
        Encoding plain = Clone(encoding, replace: true);
        // Characters of two kinds side by side (a shift state stays on across them in the whole, not across the parts).
        string[] parts = ["a,", "アイ", "中文", "é", "x", "한", "b"];
        byte[] whole = plain.GetBytes(string.Concat(parts));
        byte[] pieces = [.. parts.SelectMany(plain.GetBytes)];
        return !whole.AsSpan().SequenceEqual(pieces);
    }

    /// <summary><paramref name="encoding"/> with '?' for what it cannot write (<paramref name="replace"/>), or an exception.</summary>
    public static Encoding Clone(Encoding encoding, bool replace)
    {
        var clone = (Encoding)encoding.Clone();
        clone.EncoderFallback = replace ? new EncoderReplacementFallback("?") : EncoderFallback.ExceptionFallback;
        return clone;
    }
}

/// <summary>UTF-16 (little or big endian): every character writable.</summary>
internal sealed class Utf16TextEncoder(bool bigEndian) : TextEncoder(replace: false)
{
    public override int MaxBytes(int utf8Length) => 2 * utf8Length;

    public override bool TryEncode(ReadOnlySpan<byte> utf8, NativeBufferWriter output, out int failedAt)
    {
        failedAt = -1;
        Span<char> chars = MemoryMarshal.Cast<byte, char>(output.GetSpan(MaxBytes(utf8.Length)));
        Utf8.ToUtf16(utf8, chars, out _, out int written);
        if (bigEndian)
        {
            Span<ushort> units = MemoryMarshal.Cast<char, ushort>(chars[..written]);
            BinaryPrimitives.ReverseEndianness(units, units);
        }
        output.Advance(2 * written);
        return true;
    }
}

/// <summary>
/// A legacy code page encoded by a table built from .NET's own encoding of it, once per page: each BMP character's one
/// or two bytes, and whether they read back as it. ASCII runs are copied with vector instructions; a character the page
/// has no bytes for (or none that read back as it, and every character past the BMP) is '?' or a stop.
/// </summary>
internal sealed class TableTextEncoder(uint[] table, bool replace) : TextEncoder(replace)
{
    private const uint RoundTrips = 1u << 31;
    private static readonly ConcurrentDictionary<int, uint[]?> Built = new();

    private readonly uint[] _table = table;

    public override int MaxBytes(int utf8Length) => utf8Length; // a character's bytes here are never more than its UTF-8's (a 4-byte one: "??")

    /// <summary>
    /// The table of a code page: per BMP character, its byte count (low byte), bytes (above), and
    /// <see cref="RoundTrips"/>; null when the page is not one of single and double bytes with ASCII as it is (then
    /// .NET's encoder writes it).
    /// </summary>
    public static uint[]? Tables(int codePage) => Built.GetOrAdd(codePage, static cp => Build(cp));

    private static uint[]? Build(int codePage)
    {
        Encoding page = TextEncodings.Resolve(codePage);
        Encoding replacing = Clone(page, replace: true);
        var table = new uint[65536];
        Span<char> one = stackalloc char[1];
        Span<char> back = stackalloc char[4];
        Span<byte> bytes = stackalloc byte[16];
        for (int c = 0; c < 65536; c++)
        {
            if (char.IsSurrogate((char)c)) continue;
            one[0] = (char)c;
            int length = replacing.GetBytes(one, bytes);
            if (length is < 1 or > 2) return null;
            if (c < 0x80 && (length != 1 || bytes[0] != c)) return null;
            uint entry = (uint)length | (uint)bytes[0] << 8 | (length == 2 ? (uint)bytes[1] << 16 : 0);
            bool readsBack = page.GetChars(bytes[..length], back) == 1 && back[0] == c && (c == '?' || !(length == 1 && bytes[0] == '?'));
            table[c] = entry | (readsBack ? RoundTrips : 0);
        }
        return table;
    }

    public override bool TryEncode(ReadOnlySpan<byte> utf8, NativeBufferWriter output, out int failedAt)
    {
        Span<byte> o = output.GetSpan(MaxBytes(utf8.Length));
        int at = 0, w = 0;
        uint[] table = _table;
        while (at < utf8.Length)
        {
            int run = utf8[at..].IndexOfAnyExceptInRange((byte)0, (byte)0x7F);
            if (run < 0) run = utf8.Length - at;
            utf8.Slice(at, run).CopyTo(o[w..]);
            at += run;
            w += run;
            if (at == utf8.Length) break;
            Rune.DecodeFromUtf8(utf8[at..], out Rune rune, out int consumed);
            uint entry = rune.Value < 0x10000 ? table[rune.Value] : 0;
            if ((entry & RoundTrips) == 0)
            {
                if (!Replace)
                {
                    output.Advance(w);
                    failedAt = at;
                    return false;
                }
                // Past the BMP (no page has these): '?' for each of its two UTF-16 units, as .NET writes it.
                if (rune.Value >= 0x10000) entry = 2 | (uint)'?' << 8 | (uint)'?' << 16;
            }
            o[w++] = (byte)(entry >> 8);
            if ((entry & 0xFF) == 2) o[w++] = (byte)(entry >> 16);
            at += consumed;
        }
        output.Advance(w);
        failedAt = -1;
        return true;
    }
}

/// <summary>Any other stateless encoding (GB18030, UTF-32, EBCDIC pages): .NET's encoder, the text a chunk of UTF-16 at a time.</summary>
internal sealed class NetTextEncoder(Encoding encoding, bool replace) : TextEncoder(replace)
{
    private const int Chunk = 4096;
    private readonly Encoding _encoding = Clone(encoding, replace);

    public override int MaxBytes(int utf8Length) => _encoding.GetMaxByteCount(Math.Min(utf8Length, Chunk));

    public override bool TryEncode(ReadOnlySpan<byte> utf8, NativeBufferWriter output, out int failedAt)
    {
        Span<char> chars = stackalloc char[Chunk];
        int at = 0;
        while (at < utf8.Length)
        {
            // Whole characters only: the conversion stops before a character that does not fit.
            Utf8.ToUtf16(utf8[at..], chars, out int read, out int written, replaceInvalidSequences: false, isFinalBlock: true);
            try
            {
                int bytes = _encoding.GetBytes(chars[..written], output.GetSpan(_encoding.GetMaxByteCount(written)));
                output.Advance(bytes);
            }
            catch (EncoderFallbackException e)
            {
                failedAt = at + Encoding.UTF8.GetByteCount(chars[..e.Index]);
                return false;
            }
            at += read;
        }
        failedAt = -1;
        return true;
    }
}
