using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;

namespace Kaminari.Sazanami.Json;

/// <summary>
/// Finds where a JSON document holding one top-level array can be cut between elements, reading its bytes once in
/// order (in pieces: the state carries over): strings (with their escapes) are passed over, brackets and braces counted,
/// and a comma inside the top-level array (outside any string or nested value) is a place to cut. The scanner does not
/// validate: a document that is not an array (a single object, a scalar) or that breaks the structure gives no cuts
/// past the point where it does, and is left whole to a real parser, which reports its errors.
/// </summary>
/// <remarks>
/// A document in memory can also be cut in parallel, exactly: <see cref="Summarize"/> tells, for a piece of it and
/// whatever state it starts in, whether its quotes are odd in number and how its depth changes (inside strings the
/// brackets outside them count instead); the pieces' summaries, taken in order, give the state at every piece's start
/// (the escape at a start comes from the backslashes just before it, <see cref="EscapedAfter"/>), from which a scan
/// <see cref="Resume"/>s to find the piece's first cut.
/// The vectorized scan follows the structural-index idea of simdjson's first stage, on .NET's portable vectors: 64
/// bytes at a time it builds bit masks of quotes, backslashes, brackets and commas, finds the characters escaped by odd
/// runs of backslashes, the bytes inside strings (a prefix XOR of the unescaped quotes), and counts depth by population
/// counts, walking single bits only where the depth may come down to the array's own level.
/// </remarks>
internal struct JsonArrayScanner
{
    private const int Block = 64;

    private static readonly SearchValues<byte> Outside = SearchValues.Create("\"[]{},\\"u8);
    private static readonly SearchValues<byte> Inside = SearchValues.Create("\"\\"u8);

    private Phase _phase;
    private int _depth;
    private ulong _inString;      // all ones while the previous byte was inside a string
    private ulong _oddBackslash;  // 1 when the previous bytes end an odd run of backslashes (the next byte is escaped)

    private enum Phase : byte { Head, Array, Done }

    /// <summary>A scan that goes on inside an open top-level array, at a known state (none when the array is closed).</summary>
    /// <param name="depth">The depth: 1 between top-level elements.</param>
    /// <param name="inString">Whether the next byte is inside a string.</param>
    /// <param name="escaped">Whether the next byte is escaped (after an odd run of backslashes).</param>
    public static JsonArrayScanner Resume(long depth, bool inString, bool escaped) => new()
    {
        _phase = depth >= 1 ? Phase.Array : Phase.Done,
        _depth = (int)Math.Min(depth, int.MaxValue),
        _inString = inString ? ulong.MaxValue : 0,
        _oddBackslash = escaped ? 1UL : 0,
    };

    /// <summary>Whether a byte after <paramref name="before"/> is escaped: these bytes end with an odd run of backslashes.</summary>
    public static bool EscapedAfter(ReadOnlySpan<byte> before)
    {
        int run = before.Length - 1 - before.LastIndexOfAnyExcept((byte)'\\');
        return (run & 1) == 1;
    }

    /// <summary>
    /// What <paramref name="piece"/> does to the scan state: whether it holds an odd number of quotes (that are not
    /// escaped), and how its brackets change the depth when it starts outside a string and when it starts inside one.
    /// </summary>
    /// <param name="piece">The bytes.</param>
    /// <param name="escaped">Whether the first byte is escaped (<see cref="EscapedAfter"/> of the bytes before).</param>
    public static (bool OddQuotes, long DepthFromOutside, long DepthFromInside) Summarize(ReadOnlySpan<byte> piece, bool escaped)
    {
        ulong carry = escaped ? 1UL : 0, inside = 0;
        long fromOutside = 0, fromInside = 0;
        ref byte start = ref MemoryMarshal.GetReference(piece);
        int at = 0;
        for (; piece.Length - at >= Block; at += Block) Count(Masks.Of(ref Unsafe.Add(ref start, at)), ulong.MaxValue);
        if (at < piece.Length) CountLast(piece[at..]);
        return (inside != 0, fromOutside, fromInside);

        void CountLast(ReadOnlySpan<byte> rest)
        {
            Span<byte> padded = stackalloc byte[Block];
            padded.Fill((byte)' ');
            rest.CopyTo(padded);
            Count(Masks.Of(ref MemoryMarshal.GetReference(padded)), (1UL << rest.Length) - 1);
        }

        void Count(in Masks masks, ulong valid)
        {
            ulong escapedBytes = Escaped(masks.Backslash, ref carry);
            ulong inString = PrefixXor(masks.Quote & ~escapedBytes) ^ inside;
            inside = (ulong)((long)inString >> 63);
            ulong plain = ~escapedBytes & valid;
            // Starting inside a string flips every byte's side: the brackets inside strings are then the ones outside.
            ulong outside = ~inString & plain, flipped = inString & plain;
            fromOutside += BitOperations.PopCount(masks.Open & outside) - BitOperations.PopCount(masks.Close & outside);
            fromInside += BitOperations.PopCount(masks.Open & flipped) - BitOperations.PopCount(masks.Close & flipped);
        }
    }

    /// <summary>Scan one structural byte at a time even where vectors are accelerated (tests compare the two).</summary>
    internal bool Scalar { get; init; }

    /// <summary>Whether the document is (so far) a top-level array still open.</summary>
    public readonly bool InArray => _phase == Phase.Array;

    /// <summary>Whether the scan stopped cutting: the top-level array closed, or the document is not an array.</summary>
    public readonly bool Done => _phase == Phase.Done;

    /// <summary>
    /// Scans <paramref name="data"/> from <paramref name="position"/> and returns the index of the first comma between
    /// top-level elements at or after <paramref name="cutFrom"/> (the scan stands right after it), or -1 when there is
    /// none up to where the scan stopped: the end of the data, or, unless <paramref name="final"/> (nothing follows
    /// the data), the last bytes too few for a vector block (scanned in the next call, from the returned position).
    /// <paramref name="elements"/> counts the commas between top-level elements passed.
    /// </summary>
    /// <param name="data">The bytes, in order; a later call goes on where this one stopped (the data may be moved).</param>
    /// <param name="position">Where to start; set to where the scan stopped.</param>
    /// <param name="cutFrom">The first index a cut may be at.</param>
    /// <param name="final">Whether the data ends the document.</param>
    /// <param name="fileStart">Whether <paramref name="data"/> starts the document (where a byte order mark may be).</param>
    /// <param name="elements">Counts top-level commas.</param>
    public int Scan(ReadOnlySpan<byte> data, ref int position, int cutFrom, bool final, bool fileStart, ref long elements)
    {
        if (_phase == Phase.Head && !Head(data, ref position, final, fileStart)) return -1;
        if (_phase == Phase.Done)
        {
            position = data.Length;
            return -1;
        }
        return Vector128.IsHardwareAccelerated && !Scalar
            ? ScanVector(data, ref position, cutFrom, final, ref elements)
            : ScanScalar(data, ref position, cutFrom, final, ref elements);
    }

    // Before the document's first byte: whitespace (and a byte order mark), then '[' opens the array; anything else
    // ends the scan. False while only whitespace (or the start of a byte order mark) was seen.
    private bool Head(ReadOnlySpan<byte> data, ref int position, bool final, bool fileStart)
    {
        if (fileStart && position == 0)
        {
            if (data.StartsWith("\uFEFF"u8)) position = 3;
            else if (!final && "\uFEFF"u8.StartsWith(data)) return false;
        }
        int at = data[position..].IndexOfAnyExcept(" \t\r\n"u8);
        if (at < 0)
        {
            position = data.Length;
            return false;
        }
        position += at;
        if (data[position] == '[')
        {
            position++;
            _phase = Phase.Array;
            _depth = 1;
        }
        else _phase = Phase.Done;
        return true;
    }

    /// <summary>The scan one structural byte at a time (the reference the vectorized scan is tested against).</summary>
    internal int ScanScalar(ReadOnlySpan<byte> data, ref int position, int cutFrom, bool final, ref long elements)
    {
        int at = position;
        while (at < data.Length)
        {
            if (_oddBackslash != 0)
            {
                // An escaped byte (outside strings this is not JSON, and passed over all the same).
                _oddBackslash = 0;
                at++;
                continue;
            }
            if (_inString != 0)
            {
                int i = data[at..].IndexOfAny(Inside);
                if (i < 0)
                {
                    at = data.Length;
                    break;
                }
                at += i;
                if (data[at] == '"') _inString = 0;
                else _oddBackslash = 1;
                at++;
                continue;
            }
            int j = data[at..].IndexOfAny(Outside);
            if (j < 0)
            {
                at = data.Length;
                break;
            }
            at += j;
            switch (data[at])
            {
                case (byte)'"': _inString = ulong.MaxValue; break;
                case (byte)'\\': _oddBackslash = 1; break;
                case (byte)'[' or (byte)'{': _depth++; break;
                case (byte)']' or (byte)'}':
                    if (--_depth == 0)
                    {
                        _phase = Phase.Done;
                        position = data.Length;
                        return -1;
                    }
                    break;
                default:
                    if (_depth == 1)
                    {
                        elements++;
                        if (at >= cutFrom)
                        {
                            position = at + 1;
                            return at;
                        }
                    }
                    break;
            }
            at++;
        }
        position = at;
        return -1;
    }

    private int ScanVector(ReadOnlySpan<byte> data, ref int position, int cutFrom, bool final, ref long elements)
    {
        ref byte start = ref MemoryMarshal.GetReference(data);
        int at = position;
        for (; data.Length - at >= Block; at += Block)
        {
            int cut = Step(Masks.Of(ref Unsafe.Add(ref start, at)), ulong.MaxValue, at, cutFrom, ref elements);
            if (cut != NoCut) return Stop(cut, data.Length, ref position);
        }
        if (final && at < data.Length)
        {
            int cut = Last(data[at..], at, cutFrom, ref elements);
            if (cut != NoCut) return Stop(cut, data.Length, ref position);
            at = data.Length;
        }
        position = at;
        return -1;
    }

    private const int NoCut = -2;

    // A cut (the scan goes on after it) or the array's end (nothing more to scan).
    private static int Stop(int cut, int length, ref int position)
    {
        position = cut >= 0 ? cut + 1 : length;
        return cut;
    }

    // The document's last bytes (fewer than a block): a block padded with spaces, which change no state.
    private int Last(ReadOnlySpan<byte> rest, int at, int cutFrom, ref long elements)
    {
        Span<byte> padded = stackalloc byte[Block];
        padded.Fill((byte)' ');
        rest.CopyTo(padded);
        return Step(Masks.Of(ref MemoryMarshal.GetReference(padded)), (1UL << rest.Length) - 1, at, cutFrom, ref elements);
    }

    // One block at offset at (its valid bytes): NoCut, a cut's index, or -1 when the array closes in it.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private int Step(in Masks masks, ulong valid, int at, int cutFrom, ref long elements)
    {
        ulong escaped = Escaped(masks.Backslash, ref _oddBackslash);
        ulong inString = PrefixXor(masks.Quote & ~escaped) ^ _inString;
        _inString = (ulong)((long)inString >> 63);
        // Escaped bytes outside strings (not JSON) are passed over, as the byte-at-a-time scan does.
        ulong outside = ~inString & ~escaped & valid;
        ulong open = masks.Open & outside, close = masks.Close & outside;
        int closes = BitOperations.PopCount(close);
        if (_depth - closes > 1)
        {
            // The depth stays above the array's own level within the block: no element boundary here.
            _depth += BitOperations.PopCount(open) - closes;
            return NoCut;
        }
        // Walk the brackets in order; commas matter only while the depth is the array's own (taken a run at a time).
        ulong comma = masks.Comma & outside;
        int from = 0;
        for (ulong brackets = open | close; brackets != 0; brackets &= brackets - 1)
        {
            int i = BitOperations.TrailingZeroCount(brackets);
            if (_depth == 1 && Commas(comma & Range(from, i), at, cutFrom, ref elements) is int cut and >= 0) return cut;
            if ((open & (1UL << i)) != 0) _depth++;
            else if (--_depth == 0)
            {
                _phase = Phase.Done;
                return -1;
            }
            from = i + 1;
        }
        if (_depth == 1 && Commas(comma & Range(from, Block), at, cutFrom, ref elements) is int last and >= 0) return last;
        return NoCut;
    }

    // Bits from (inclusive) to to (exclusive).
    private static ulong Range(int from, int to) =>
        from >= Block ? 0 : (to >= Block ? ulong.MaxValue : (1UL << to) - 1) & ~((1UL << from) - 1);

    // Commas between top-level elements in a block at offset at: counted; the index of the first at or past cutFrom (a
    // cut, the scan state reset to stand right after it), else -1.
    private int Commas(ulong commas, int at, int cutFrom, ref long elements)
    {
        if (commas == 0) return -1;
        int threshold = cutFrom - at;
        ulong candidates = threshold <= 0 ? commas : threshold >= Block ? 0 : commas & (ulong.MaxValue << threshold);
        if (candidates == 0)
        {
            elements += BitOperations.PopCount(commas);
            return -1;
        }
        int j = BitOperations.TrailingZeroCount(candidates);
        elements += BitOperations.PopCount(commas & ((1UL << j) - 1)) + 1;
        // Stopped on a comma between elements: outside strings, after no backslash.
        _inString = 0;
        _oddBackslash = 0;
        return at + j;
    }

    // The bytes escaped by a backslash: those after an odd run of backslashes (a run carries over from the previous block).
    private static ulong Escaped(ulong backslash, ref ulong carry)
    {
        if (backslash == 0)
        {
            ulong first = carry;
            carry = 0;
            return first;
        }
        const ulong even = 0x5555_5555_5555_5555UL, odd = ~even;
        ulong startEdges = backslash & ~(backslash << 1);
        ulong evenStartMask = even ^ carry;
        ulong evenStarts = startEdges & evenStartMask;
        ulong oddStarts = startEdges & ~evenStartMask;
        ulong evenCarries = backslash + evenStarts;
        ulong oddCarries = backslash + oddStarts;
        bool overflow = oddCarries < backslash;
        oddCarries |= carry;
        carry = overflow ? 1UL : 0UL;
        ulong evenCarryEnds = evenCarries & ~backslash;
        ulong oddCarryEnds = oddCarries & ~backslash;
        return (evenCarryEnds & odd) | (oddCarryEnds & even);
    }

    // Bit i set when an odd number of bits at or below i are set.
    private static ulong PrefixXor(ulong x)
    {
        x ^= x << 1;
        x ^= x << 2;
        x ^= x << 4;
        x ^= x << 8;
        x ^= x << 16;
        x ^= x << 32;
        return x;
    }

    // The bit masks of one 64-byte block.
    private readonly struct Masks
    {
        public ulong Quote { get; init; }
        public ulong Backslash { get; init; }
        public ulong Open { get; init; }
        public ulong Close { get; init; }
        public ulong Comma { get; init; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static Masks Of(ref byte block)
        {
            if (Vector256.IsHardwareAccelerated)
            {
                Vector256<byte> a = Vector256.LoadUnsafe(ref block), b = Vector256.LoadUnsafe(ref block, 32);
                return new Masks
                {
                    Quote = Bits(a, b, (byte)'"'),
                    Backslash = Bits(a, b, (byte)'\\'),
                    // '[' 0x5B and '{' 0x7B differ by 0x20 only (as ']' and '}' do).
                    Open = Bits(a | Vector256.Create((byte)0x20), b | Vector256.Create((byte)0x20), (byte)'{'),
                    Close = Bits(a | Vector256.Create((byte)0x20), b | Vector256.Create((byte)0x20), (byte)'}'),
                    Comma = Bits(a, b, (byte)','),
                };
            }
            Vector128<byte> v0 = Vector128.LoadUnsafe(ref block), v1 = Vector128.LoadUnsafe(ref block, 16),
                v2 = Vector128.LoadUnsafe(ref block, 32), v3 = Vector128.LoadUnsafe(ref block, 48);
            Vector128<byte> lower = Vector128.Create((byte)0x20);
            Vector128<byte> l0 = v0 | lower, l1 = v1 | lower, l2 = v2 | lower, l3 = v3 | lower;
            return new Masks
            {
                Quote = Bits(v0, v1, v2, v3, (byte)'"'),
                Backslash = Bits(v0, v1, v2, v3, (byte)'\\'),
                Open = Bits(l0, l1, l2, l3, (byte)'{'),
                Close = Bits(l0, l1, l2, l3, (byte)'}'),
                Comma = Bits(v0, v1, v2, v3, (byte)','),
            };
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong Bits(Vector256<byte> a, Vector256<byte> b, byte value)
        {
            Vector256<byte> c = Vector256.Create(value);
            return Vector256.Equals(a, c).ExtractMostSignificantBits() | (ulong)Vector256.Equals(b, c).ExtractMostSignificantBits() << 32;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static ulong Bits(Vector128<byte> v0, Vector128<byte> v1, Vector128<byte> v2, Vector128<byte> v3, byte value)
        {
            Vector128<byte> c = Vector128.Create(value);
            return Vector128.Equals(v0, c).ExtractMostSignificantBits()
                | (ulong)Vector128.Equals(v1, c).ExtractMostSignificantBits() << 16
                | (ulong)Vector128.Equals(v2, c).ExtractMostSignificantBits() << 32
                | (ulong)Vector128.Equals(v3, c).ExtractMostSignificantBits() << 48;
        }
    }
}
