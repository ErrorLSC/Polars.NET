using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
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
    /// (strict) or the bytes are not UTF-8 there (replacing or not), <paramref name="failedAt"/> its UTF-8 offset (what
    /// was appended by then stays).
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
        Span<char> chars = MemoryMarshal.Cast<byte, char>(output.GetSpan(MaxBytes(utf8.Length)));
        OperationStatus status = Utf8.ToUtf16(utf8, chars, out int read, out int written, replaceInvalidSequences: false);
        if (bigEndian)
        {
            Span<ushort> units = MemoryMarshal.Cast<char, ushort>(chars[..written]);
            BinaryPrimitives.ReverseEndianness(units, units);
        }
        output.Advance(2 * written);
        failedAt = status == OperationStatus.Done ? -1 : read;
        return status == OperationStatus.Done;
    }
}

/// <summary>
/// A legacy code page encoded by a table built from .NET's own encoding of it, once per page: each BMP character's one
/// or two bytes (16 bits each: 128 KiB, to stay in a core's cache), and whether they read back as it (one bit each).
/// ASCII runs are copied with vector instructions; a character the page has no bytes for (or none that read back as it,
/// and every character past the BMP) is '?' or a stop.
/// </summary>
internal sealed class TableTextEncoder(TableTextEncoder.PageTable table, bool replace) : TextEncoder(replace)
{
    private static readonly ConcurrentDictionary<int, PageTable?> Built = new();

    private readonly ushort[] _bytes = table.Bytes;
    private readonly ulong[] _readsBack = table.ReadsBack;

    /// <summary>
    /// A code page's table: per BMP character, its bytes (one: the low byte, the high one 0; two: the first in the high
    /// byte, never 0), and a bit per character set when they read back as it.
    /// </summary>
    internal sealed record PageTable(ushort[] Bytes, ulong[] ReadsBack);

    public override int MaxBytes(int utf8Length) => utf8Length; // a character's bytes here are never more than its UTF-8's (a 4-byte one: "??")

    /// <summary>
    /// The table of a code page; null when the page is not one of single and double bytes with ASCII as it is (then
    /// .NET's encoder writes it).
    /// </summary>
    public static PageTable? Tables(int codePage) => Built.GetOrAdd(codePage, static cp => Build(cp));

    private static PageTable? Build(int codePage)
    {
        Encoding page = TextEncodings.Resolve(codePage);
        Encoding replacing = Clone(page, replace: true);
        var table = new PageTable(new ushort[65536], new ulong[65536 / 64]);
        Span<char> one = stackalloc char[1];
        Span<char> back = stackalloc char[4];
        Span<byte> bytes = stackalloc byte[16];
        for (int c = 0; c < 65536; c++)
        {
            if (char.IsSurrogate((char)c)) continue;
            one[0] = (char)c;
            int length = replacing.GetBytes(one, bytes);
            if (length is < 1 or > 2 || length == 2 && bytes[0] == 0) return null;
            if (c < 0x80 && (length != 1 || bytes[0] != c)) return null;
            table.Bytes[c] = (ushort)(length == 2 ? bytes[0] << 8 | bytes[1] : bytes[0]);
            bool readsBack = page.GetChars(bytes[..length], back) == 1 && back[0] == c && (c == '?' || !(length == 1 && bytes[0] == '?'));
            if (readsBack) table.ReadsBack[c >> 6] |= 1UL << c;
        }
        return table;
    }

    public override bool TryEncode(ReadOnlySpan<byte> utf8, NativeBufferWriter output, out int failedAt)
    {
        Span<byte> o = output.GetSpan(MaxBytes(utf8.Length));
        int at = 0, w = 0, n = utf8.Length;
        // Unchecked reads and writes: reads stay below n (checked before each), and a character's bytes here are never
        // more than its UTF-8's, so w ≤ at < n ≤ o.Length.
        ref byte src = ref MemoryMarshal.GetReference(utf8);
        ref byte dst = ref MemoryMarshal.GetReference(o);
        ref ushort table = ref MemoryMarshal.GetArrayDataReference(_bytes);
        ulong[] readsBack = _readsBack;
        bool replace = Replace;
        while (at < n)
        {
            int b0 = Unsafe.Add(ref src, at);
            if (b0 < 0x80)
            {
                // ASCII as it is: one byte here, a run of them copied at once (found with vector instructions).
                Unsafe.Add(ref dst, w++) = (byte)b0;
                if (++at < n && Unsafe.Add(ref src, at) < 0x80)
                {
                    int run = utf8[at..].IndexOfAnyExceptInRange((byte)0, (byte)0x7F);
                    if (run < 0) run = n - at;
                    utf8.Slice(at, run).CopyTo(o[w..]);
                    at += run;
                    w += run;
                }
                continue;
            }
            // Two- and three-byte characters decoded here (the BMP: what the tables hold); four-byte ones and bytes
            // that are not UTF-8 by Rune.
            int value, consumed;
            if ((b0 & 0xF0) == 0xE0 && at + 2 < n)
            {
                int b1 = Unsafe.Add(ref src, at + 1), b2 = Unsafe.Add(ref src, at + 2);
                value = (b0 & 0x0F) << 12 | (b1 & 0x3F) << 6 | b2 & 0x3F;
                consumed = ((b1 ^ 0x80) | (b2 ^ 0x80)) < 0x40 && value >= 0x800 && (value < 0xD800 || value > 0xDFFF) ? 3 : 0;
            }
            else if (b0 >= 0xC2 && b0 < 0xE0 && at + 1 < n && (Unsafe.Add(ref src, at + 1) & 0xC0) == 0x80)
            {
                value = (b0 & 0x1F) << 6 | Unsafe.Add(ref src, at + 1) & 0x3F;
                consumed = 2;
            }
            else consumed = value = 0;
            if (consumed == 0)
            {
                if (Rune.DecodeFromUtf8(utf8[at..], out Rune rune, out consumed) != OperationStatus.Done)
                {
                    output.Advance(w);
                    failedAt = at;
                    return false;
                }
                value = rune.Value;
            }
            int entry;
            if (value >= 0x10000 || !replace && (readsBack[value >> 6] & 1UL << value) == 0)
            {
                if (!replace)
                {
                    output.Advance(w);
                    failedAt = at;
                    return false;
                }
                // Past the BMP (no page has these): '?' for each of its two UTF-16 units, as .NET writes it.
                entry = '?' << 8 | '?';
            }
            else entry = Unsafe.Add(ref table, value); // what the page writes ('?' when it has nothing: replacing)
            if (entry > 0xFF) Unsafe.Add(ref dst, w++) = (byte)(entry >> 8);
            Unsafe.Add(ref dst, w++) = (byte)entry;
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
            // Whole characters only: the conversion stops before a character that does not fit, and before bytes that are
            // not UTF-8 (written up to them, then refused).
            OperationStatus status = Utf8.ToUtf16(utf8[at..], chars, out int read, out int written, replaceInvalidSequences: false, isFinalBlock: true);
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
            if (status == OperationStatus.InvalidData)
            {
                failedAt = at;
                return false;
            }
        }
        failedAt = -1;
        return true;
    }
}
