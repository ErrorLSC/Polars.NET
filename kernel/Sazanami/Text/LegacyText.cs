using System.Buffers;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;

namespace Kaminari.Sazanami.Text;

/// <summary>
/// A legacy code page decoded by tables built, at first use, from .NET's own encoding of it: one entry per single byte
/// and one per lead/trail pair, each the UTF-8 of the character .NET decodes it to (or invalid); ASCII runs are copied
/// with vector instructions. Only the code pages of <see cref="AllowList"/> are decoded this way: for them the tables
/// were proven equal to .NET's decoding (every single and pair, random sequences); every other page goes through .NET's
/// decoder in order (<see cref="SequentialTextDecoder"/>). A unit is invalid when .NET can only replace it: it decodes
/// to a replacement character (U+FFFD, '?' or U+30FB, by page) that does not encode back to the same bytes. Strictness
/// comes from the caller, never from the encoding's fallback (.NET's exception fallback refuses CP932 rows its default
/// decoding maps, 87 90 ≒ among them). Invalid units: strict stops there; lossy writes U+FFFD for the lead byte alone,
/// so the byte after it (a newline, a quote) is read again.
/// </summary>
internal sealed class LegacyText : TextDecoder
{
    /// <summary>
    /// The code pages decoded by tables: ASCII-compatible single-byte pages, and double-byte pages with leads 0x81–0xFE
    /// whose every character is one or two bytes (no 3- or 4-byte forms, no shift states).
    /// </summary>
    public static ReadOnlySpan<int> AllowList =>
    [
        437, 708, 720, 737, 775, 850, 852, 855, 857, 858, 860, 861, 862, 863, 864, 865, 866, 869, 874,
        1250, 1251, 1252, 1253, 1254, 1255, 1256, 1257, 1258,
        10000, 10004, 10005, 10006, 10007, 10010, 10017, 10029, 10079, 10081, 10082, 20127, 20866, 21866,
        28591, 28592, 28593, 28594, 28595, 28596, 28597, 28598, 28599, 28603, 28605, 38598,
        932, 936, 949, 950, 1361, 20000, 20001, 20002, 20003, 20004, 20005, 20932, 20936, 20949, 51932, 51936, 51949, 50227,
        10001, 10002,
    ];

    private const uint Lead = uint.MaxValue; // a single-table entry meaning "the first byte of a pair"
    private static readonly ConcurrentDictionary<int, Tables> Built = new();

    private readonly Tables _tables;

    private LegacyText(Tables tables, bool replace) : base(replace) => _tables = tables;

    public override TextLayout Layout => TextLayout.AsciiCompatible;

    public int CodePage => _tables.CodePage;

    /// <summary>A table decoder for <paramref name="codePage"/>, or null when it is not on the allow-list.</summary>
    public static LegacyText? For(int codePage, bool replace) =>
        AllowList.Contains(codePage) ? new LegacyText(Built.GetOrAdd(codePage, static cp => Tables.Build(cp)), replace) : null;

    public override OperationStatus Decode(ReadOnlySpan<byte> source, Span<byte> destination, bool final, out int consumed, out int written)
    {
        ref uint single = ref MemoryMarshal.GetArrayDataReference(_tables.Single); // 256 entries: any byte indexes it
        ref uint pair = ref MemoryMarshal.GetArrayDataReference(_tables.Pair); // 128 × 256: any lead ≥ 0x80 and trail
        ref byte s = ref MemoryMarshal.GetReference(source);
        ref byte d = ref MemoryMarshal.GetReference(destination);
        int at = 0, w = 0, length = source.Length, room = destination.Length - 4; // up to here a 4-byte write fits
        while (at < length)
        {
            // An ASCII run: copied as it is.
            int run = source[at..].IndexOfAnyExceptInRange((byte)0, (byte)0x7F);
            if (run < 0) run = length - at;
            if (destination.Length - w < run) return Stop(OperationStatus.DestinationTooSmall, at, w, out consumed, out written);
            source.Slice(at, run).CopyTo(destination[w..]);
            at += run;
            w += run;
            while (at < length)
            {
                uint b = Unsafe.Add(ref s, at);
                if (b < 0x80)
                {
                    // A short ASCII run between non-ASCII characters byte by byte; a long one back to the vector copy.
                    if (length - at >= 16 && Ascii.IsValid(source.Slice(at, 16))) break;
                    if (w == destination.Length) return Stop(OperationStatus.DestinationTooSmall, at, w, out consumed, out written);
                    Unsafe.Add(ref d, w++) = (byte)b;
                    at++;
                    continue;
                }
                uint v = Unsafe.Add(ref single, b);
                int units = 1;
                if (v == Lead)
                {
                    if (at + 1 < length)
                    {
                        v = Unsafe.Add(ref pair, (int)((b - 0x80) << 8 | Unsafe.Add(ref s, at + 1)));
                        units = 2;
                    }
                    else if (!final) return Stop(OperationStatus.NeedMoreData, at, w, out consumed, out written);
                    else v = 0;
                }
                if (v == 0)
                {
                    if (!Replace) return Stop(OperationStatus.InvalidData, at, w, out consumed, out written);
                    v = 0xBD_BF_EF_03; // U+FFFD, for the lead byte alone
                    units = 1;
                }
                int size = (int)(v & 0xFF);
                if (w <= room) Unsafe.WriteUnaligned(ref Unsafe.Add(ref d, w), v >> 8);
                else if (destination.Length - w >= size) for (int i = 0; i < size; i++) Unsafe.Add(ref d, w + i) = (byte)(v >> 8 * (i + 1));
                else return Stop(OperationStatus.DestinationTooSmall, at, w, out consumed, out written);
                w += size;
                at += units;
            }
        }
        return Stop(OperationStatus.Done, at, w, out consumed, out written);

        static OperationStatus Stop(OperationStatus status, int at, int w, out int consumed, out int written)
        {
            consumed = at;
            written = w;
            return status;
        }
    }

    /// <summary>One code page's tables: entries pack the UTF-8 length in the low byte and the bytes above it; 0 is invalid.</summary>
    private sealed class Tables
    {
        public required int CodePage { get; init; }
        public uint[] Single { get; } = new uint[256];
        public uint[] Pair { get; } = new uint[128 * 256];

        public static Tables Build(int codePage)
        {
            Encoding e = TextEncodings.Resolve(codePage);
            var t = new Tables { CodePage = codePage };
            Span<char> chars = stackalloc char[8];
            Span<byte> back = stackalloc byte[8];
            Span<byte> unit = stackalloc byte[2];
            for (int b = 0; b < 0x80; b++) t.Single[b] = Pack((char)b); // ASCII-compatible: proven by the tests per page
            for (int b = 0x80; b < 0x100; b++)
            {
                unit[0] = (byte)b;
                bool lead = false;
                for (int trail = 0; trail < 0x100; trail++)
                {
                    unit[1] = (byte)trail;
                    if (Valid(e, unit, chars, back) is { } c)
                    {
                        t.Pair[(b - 0x80) << 8 | trail] = Pack(c);
                        lead = true;
                    }
                }
                t.Single[b] = lead ? Lead : Valid(e, unit[..1], chars, back) is { } s ? Pack(s) : 0;
            }
            return t;
        }

        private static char? Valid(Encoding e, ReadOnlySpan<byte> unit, Span<char> chars, Span<byte> back)
        {
            if (e.GetChars(unit, chars) != 1 || char.IsSurrogate(chars[0])) return null;
            char c = chars[0];
            if (c is not ('�' or '?' or '・')) return c;
            return back[..e.GetBytes(chars[..1], back)].SequenceEqual(unit) ? c : null;
        }

        private static uint Pack(char c)
        {
            Span<byte> utf8 = stackalloc byte[4];
            int length = Encoding.UTF8.GetBytes([c], utf8);
            uint v = (uint)length;
            for (int i = 0; i < length; i++) v |= (uint)utf8[i] << 8 * (i + 1);
            return v;
        }
    }
}
