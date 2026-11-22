using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text;

namespace Kaminari.Sazanami.Json;

/// <summary>
/// The bytes of JSON strings: where a run of plain string bytes stops (16 bytes at a time, on .NET's portable vectors),
/// and the unescaped UTF-8 of a string's content.
/// </summary>
internal static class JsonStrings
{
    /// <summary>
    /// The first quote, backslash, control byte or non-ASCII byte at or after <paramref name="from"/>, or the span's
    /// length when there is none. One signed compare finds both control and non-ASCII bytes (those are below 0x20 as
    /// signed bytes).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int Stop(ReadOnlySpan<byte> bytes, int from)
    {
        ref byte start = ref MemoryMarshal.GetReference(bytes);
        int i = from;
        while (i + Vector128<byte>.Count <= bytes.Length)
        {
            var v = Vector128.LoadUnsafe(ref start, (nuint)i);
            var hit = Vector128.LessThan(v.AsSByte(), Vector128.Create((sbyte)0x20)).AsByte()
                | Vector128.Equals(v, Vector128.Create((byte)'"'))
                | Vector128.Equals(v, Vector128.Create((byte)'\\'));
            uint mask = hit.ExtractMostSignificantBits();
            if (mask != 0) return i + BitOperations.TrailingZeroCount(mask);
            i += Vector128<byte>.Count;
        }
        for (; i < bytes.Length; i++)
        {
            byte c = bytes[i];
            if (c < 0x20 || c >= 0x80 || c == '"' || c == '\\') return i;
        }
        return bytes.Length;
    }

    /// <summary>
    /// The first quote, backslash or control byte at or after <paramref name="from"/> (non-ASCII bytes pass), or the
    /// span's length: the rest of a string already known to hold non-ASCII.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static int StopNarrow(ReadOnlySpan<byte> bytes, int from)
    {
        ref byte start = ref MemoryMarshal.GetReference(bytes);
        int i = from;
        while (i + Vector128<byte>.Count <= bytes.Length)
        {
            var v = Vector128.LoadUnsafe(ref start, (nuint)i);
            var hit = Vector128.LessThan(v, Vector128.Create((byte)0x20))
                | Vector128.Equals(v, Vector128.Create((byte)'"'))
                | Vector128.Equals(v, Vector128.Create((byte)'\\'));
            uint mask = hit.ExtractMostSignificantBits();
            if (mask != 0) return i + BitOperations.TrailingZeroCount(mask);
            i += Vector128<byte>.Count;
        }
        for (; i < bytes.Length; i++)
        {
            byte c = bytes[i];
            if (c < 0x20 || c == '"' || c == '\\') return i;
        }
        return bytes.Length;
    }

    /// <summary>
    /// The UTF-8 of a string's <paramref name="content"/> (between its quotes, its escapes valid) with its escapes
    /// replaced, written to <paramref name="destination"/> (at least as long as the content): its length. An escaped
    /// surrogate without its pair is a <see cref="JsonSyntaxException"/> at <paramref name="contentStart"/> plus its
    /// offset (as Utf8JsonReader refuses such a string when it unescapes it).
    /// </summary>
    public static int Unescape(ReadOnlySpan<byte> content, Span<byte> destination, int contentStart)
    {
        if (destination.Length < content.Length)
            throw new ArgumentException("The destination is shorter than the string's escaped text.", nameof(destination));
        int written = 0, i = 0;
        while (true)
        {
            int backslash = content[i..].IndexOf((byte)'\\');
            if (backslash < 0)
            {
                content[i..].CopyTo(destination[written..]);
                return written + content.Length - i;
            }
            content.Slice(i, backslash).CopyTo(destination[written..]);
            written += backslash;
            i += backslash;
            byte kind = content[i + 1];
            if (kind != 'u')
            {
                destination[written++] = kind switch
                {
                    (byte)'b' => (byte)'\b',
                    (byte)'f' => (byte)'\f',
                    (byte)'n' => (byte)'\n',
                    (byte)'r' => (byte)'\r',
                    (byte)'t' => (byte)'\t',
                    _ => kind, // '"', '\\', '/'
                };
                i += 2;
                continue;
            }
            int at = i;
            int code = Hex(content, i + 2);
            i += 6;
            if (code is >= 0xD800 and <= 0xDFFF)
            {
                if (code >= 0xDC00 || i + 6 > content.Length || content[i] != '\\' || content[i + 1] != 'u')
                    throw LoneSurrogate(code, contentStart + at);
                int low = Hex(content, i + 2);
                if (low is < 0xDC00 or > 0xDFFF) throw LoneSurrogate(code, contentStart + at);
                code = 0x10000 + ((code - 0xD800) << 10) + (low - 0xDC00);
                i += 6;
            }
            written += new Rune(code).EncodeToUtf8(destination[written..]);
        }

        static int Hex(ReadOnlySpan<byte> text, int at)
        {
            int value = 0;
            for (int k = at; k < at + 4; k++)
            {
                int c = text[k];
                value = value << 4 | (c <= '9' ? c - '0' : (c | 0x20) - 'a' + 10);
            }
            return value;
        }

        static JsonSyntaxException LoneSurrogate(int code, int position) =>
            new($"The escape '\\u{code:X4}' is half of a surrogate pair without its other half.", position);
    }
}
