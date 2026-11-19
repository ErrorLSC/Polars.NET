using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Kaminari.Sazanami.Compression;

/// <summary>
/// The LZ4 block format (lz4_Block_format.md): sequences of a token, literals, and a match (offset, length); the
/// last sequence has literals only. Parquet's LZ4_RAW codec is one such block per page; the LZ4 frame format
/// (<see cref="Lz4Frame"/>, Arrow IPC) holds a sequence of them.
/// </summary>
internal static class Lz4Block
{
    private const int MinMatch = 4;
    private const int LastLiterals = 5;     // the last 5 bytes are always literals
    private const int MatchSafeDistance = 12; // the last match starts at least 12 bytes before the end

    /// <summary>Decompresses a block into <paramref name="destination"/>; returns the bytes written.</summary>
    /// <exception cref="InvalidDataException">The block is damaged or does not fit.</exception>
    public static int Decompress(ReadOnlySpan<byte> source, Span<byte> destination) => Decompress(source, destination, 0);

    /// <summary>
    /// Decompresses a block into <paramref name="window"/> from position <paramref name="start"/>; matches may reach
    /// back into the <paramref name="start"/> bytes before it (the LZ4 frame format's linked blocks). Returns the bytes
    /// written. Bytes of <paramref name="window"/> past those written may be overwritten (copies run in 16-byte steps
    /// where the window leaves room); nothing is read or written outside the two spans.
    /// </summary>
    /// <exception cref="InvalidDataException">The block is damaged or does not fit.</exception>
    public static unsafe int Decompress(ReadOnlySpan<byte> source, Span<byte> window, int start)
    {
        if (source.IsEmpty) return 0;
        if ((uint)start > (uint)window.Length) throw new ArgumentOutOfRangeException(nameof(start));
        fixed (byte* src = source)
        fixed (byte* dst = window)
        {
            return (int)(Decode(src, src + source.Length, dst, dst + start, dst + window.Length) - (dst + start));
        }
    }

    /// <summary>
    /// The decoding loop. Each sequence is checked whole on entry (literals inside the input and the output, the match
    /// inside what was written and the output); within those bounds, short literals and matches far enough back are
    /// copied 16 bytes at a time while the output leaves room for the overrun, the rest exactly.
    /// </summary>
    private static unsafe byte* Decode(byte* ip, byte* iend, byte* low, byte* op, byte* oend)
    {
        while (true)
        {
            if (ip >= iend) throw Damaged();
            uint token = *ip++;
            nuint literals = token >> 4;
            if (literals != 15 && iend - ip >= 16 && oend - op >= 16)
            {
                // Short literals, room on both sides: one 16-byte copy.
                Unsafe.CopyBlockUnaligned(op, ip, 16);
            }
            else
            {
                if (literals == 15) literals += ReadLength(ref ip, iend);
                if (literals > (nuint)(iend - ip) || literals > (nuint)(oend - op)) throw Damaged();
                Buffer.MemoryCopy(ip, op, (long)literals, (long)literals);
            }
            ip += literals;
            op += literals;
            if (ip == iend) return op; // the last sequence ends after its literals
            if (ip > iend - 2) throw Damaged();

            nuint offset = Unsafe.ReadUnaligned<ushort>(ip);
            if (!BitConverter.IsLittleEndian) offset = BinaryPrimitives.ReverseEndianness((ushort)offset);
            ip += 2;
            if (offset == 0 || offset > (nuint)(op - low)) throw Damaged();
            nuint match = token & 0x0F;
            if (match == 15) match += ReadLength(ref ip, iend);
            match += MinMatch;
            if (match > (nuint)(oend - op)) throw Damaged();
            byte* from = op - offset;
            byte* end = op + match;
            if (offset >= 16 && (nuint)(oend - op) >= match + 16)
            {
                // Far enough back to copy 16 bytes at a time; the output has room for the overrun.
                do
                {
                    Unsafe.CopyBlockUnaligned(op, from, 16);
                    op += 16;
                    from += 16;
                }
                while (op < end);
            }
            else if (offset >= 8 && (nuint)(oend - op) >= match + 8)
            {
                do
                {
                    Unsafe.WriteUnaligned(op, Unsafe.ReadUnaligned<ulong>(from));
                    op += 8;
                    from += 8;
                }
                while (op < end);
            }
            else if (offset == 1)
            {
                new Span<byte>(op, (int)match).Fill(*from); // a run of one byte
            }
            else if (offset >= match)
            {
                Buffer.MemoryCopy(from, op, (long)match, (long)match);
            }
            else
            {
                // Overlapping: the bytes before op repeat with period offset; copy what is there, doubling it each time.
                byte* pattern = from;
                while (op < end)
                {
                    nuint n = Math.Min((nuint)(op - pattern), (nuint)(end - op));
                    Buffer.MemoryCopy(pattern, op, (long)n, (long)n);
                    op += n;
                }
            }
            op = end;
        }
    }

    private static unsafe nuint ReadLength(ref byte* ip, byte* iend)
    {
        nuint length = 0;
        uint b;
        do
        {
            if (ip >= iend) throw Damaged();
            b = *ip++;
            length += b;
            if (length > int.MaxValue / 2) throw Damaged();
        }
        while (b == 255);
        return length;
    }

    private static InvalidDataException Damaged() => new("An LZ4 block is damaged.");

    /// <summary>The largest block <paramref name="length"/> input bytes can compress to.</summary>
    public static int MaxCompressedLength(int length) => length + length / 255 + 16;

    /// <summary>Hash table size (log2): 8192 positions, 32 KiB (LZ4 uses as many for blocks of up to 64 KiB).</summary>
    private const int HashLog = 13;

    /// <summary>After 2^SkipTrigger probes without a match the step grows by one (LZ4's acceleration over incompressible data).</summary>
    private const int SkipTrigger = 6;

    /// <summary>The largest input whose positions fit 16 bits (its matches are always within reach).</summary>
    private const int SmallInput = 1 << 16;

    /// <summary>A thread's hash tables of positions, cleared for each input: 16-bit for inputs up to 64 KiB (16 KiB, as LZ4's), 32-bit beyond.</summary>
    [ThreadStatic]
    private static ushort[]? t_small;

    [ThreadStatic]
    private static uint[]? t_large;

    /// <summary>
    /// Compresses <paramref name="source"/> into one block (LZ4's fast compressor: a hash table of 4-byte sequences,
    /// the step growing over data without matches, matches extended backward and compared 8 bytes at a time). The
    /// output depends on the input alone.
    /// </summary>
    /// <returns>The compressed length.</returns>
    public static unsafe int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (destination.Length < MaxCompressedLength(source.Length)) throw new ArgumentException("The destination is too small.", nameof(destination));
        int length = source.Length;
        fixed (byte* src = source)
        fixed (byte* dst = destination)
        {
            byte* oend = dst + destination.Length;
            if (length <= MatchSafeDistance) return (int)(Literals(src, src + length, dst, oend) - dst);
            if (length <= SmallInput)
            {
                ushort[] table = t_small ??= new ushort[1 << HashLog];
                Array.Clear(table);
                fixed (ushort* t = table) return (int)(Encode<Small>(src, length, dst, oend, t) - dst);
            }
            uint[] large = t_large ??= new uint[1 << HashLog];
            Array.Clear(large);
            fixed (uint* t = large) return (int)(Encode<Large>(src, length, dst, oend, t) - dst);
        }
    }

    /// <summary>How positions are kept in the hash table: the encoder is specialized for each.</summary>
    private unsafe interface IPositions
    {
        static abstract uint Get(void* table, uint h);

        static abstract void Set(void* table, uint h, uint position);

        /// <summary>Whether a match this far back can be encoded (offsets are 16-bit).</summary>
        static abstract bool Reachable(nuint distance);
    }

    /// <summary>Inputs up to 64 KiB: 16-bit positions, every match within reach.</summary>
    private readonly unsafe struct Small : IPositions
    {
        public static uint Get(void* table, uint h) => ((ushort*)table)[h];

        public static void Set(void* table, uint h, uint position) => ((ushort*)table)[h] = (ushort)position;

        public static bool Reachable(nuint distance) => true;
    }

    /// <summary>Larger inputs (Parquet pages): 32-bit positions, matches at most 65535 bytes back.</summary>
    private readonly unsafe struct Large : IPositions
    {
        public static uint Get(void* table, uint h) => ((uint*)table)[h];

        public static void Set(void* table, uint h, uint position) => ((uint*)table)[h] = position;

        public static bool Reachable(nuint distance) => distance <= MaxOffset;
    }

    /// <summary>
    /// The encoding loop over a cleared table: an empty entry reads as position 0, a candidate like any other (its bytes
    /// are compared before it is used). Every position in the table lies before the current one.
    /// </summary>
    /// <remarks>
    /// The output never passes <paramref name="oend"/> (the destination holds the bound): literals are copied 8 bytes at a
    /// time without a check, since at least 8 more bytes always follow them (an offset, a token, 5 last literals).
    /// </remarks>
    private static unsafe byte* Encode<T>(byte* src, int length, byte* op, byte* oend, void* t) where T : struct, IPositions
    {
        byte* anchor = src;
        byte* searchEnd = src + length - MatchSafeDistance + 1;   // a match starts at least 12 bytes before the end
        byte* matchlimit = src + length - LastLiterals;          // and ends at least 5 before it
        byte* ip = src;
        T.Set(t, Hash(ip), 0);
        ip++;
        uint forwardH = Hash(ip);
        while (true)
        {
            // Find a match, stepping faster the longer none is found.
            byte* match;
            byte* forwardIp = ip;
            uint step = 1, probes = 1u << SkipTrigger;
            do
            {
                uint h = forwardH;
                ip = forwardIp;
                forwardIp += step;
                step = probes++ >> SkipTrigger;
                if (forwardIp > searchEnd) return Literals(anchor, src + length, op, oend);
                match = src + T.Get(t, h);
                forwardH = Hash(forwardIp);
                T.Set(t, h, (uint)(ip - src));
            }
            while (!T.Reachable((nuint)(ip - match)) || Unsafe.ReadUnaligned<uint>(match) != Unsafe.ReadUnaligned<uint>(ip));
            // Extend it backward over the literals.
            while (ip > anchor && match > src && ip[-1] == match[-1])
            {
                ip--;
                match--;
            }
            byte* token = op++;
            nuint literals = (nuint)(ip - anchor);
            if (literals >= 15)
            {
                *token = 15 << 4;
                op = WriteLength(op, literals - 15);
            }
            else
            {
                *token = (byte)(literals << 4);
            }
            CopyLiterals(op, anchor, literals);
            op += literals;

            while (true)
            {
                // The match: offset, then its length counted 8 bytes at a time.
                Unsafe.WriteUnaligned(op, BitConverter.IsLittleEndian ? (ushort)(ip - match) : BinaryPrimitives.ReverseEndianness((ushort)(ip - match)));
                op += 2;
                nuint extra = Count(ip + MinMatch, match + MinMatch, matchlimit);
                ip += MinMatch + extra;
                if (extra >= 15)
                {
                    *token += 15;
                    op = WriteLength(op, extra - 15);
                }
                else
                {
                    *token += (byte)extra;
                }
                anchor = ip;
                if (ip + 1 >= searchEnd) return Literals(anchor, src + length, op, oend);
                T.Set(t, Hash(ip - 2), (uint)(ip - 2 - src));
                // A match right here: a sequence without literals.
                uint here = Hash(ip);
                match = src + T.Get(t, here);
                T.Set(t, here, (uint)(ip - src));
                if (T.Reachable((nuint)(ip - match)) && Unsafe.ReadUnaligned<uint>(match) == Unsafe.ReadUnaligned<uint>(ip))
                {
                    token = op++;
                    *token = 0;
                    continue;
                }
                break;
            }
            forwardH = Hash(++ip);
        }
    }

    /// <summary>The last sequence: the literals from <paramref name="anchor"/> to the end.</summary>
    private static unsafe byte* Literals(byte* anchor, byte* iend, byte* op, byte* oend)
    {
        nuint last = (nuint)(iend - anchor);
        if (last >= 15)
        {
            *op++ = 15 << 4;
            op = WriteLength(op, last - 15);
        }
        else
        {
            *op++ = (byte)(last << 4);
        }
        Buffer.MemoryCopy(anchor, op, (long)(oend - op), (long)last);
        return op + last;
    }

    private const int MaxOffset = 65535;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe uint Hash(byte* p) => (Unsafe.ReadUnaligned<uint>(p) * 2654435761u) >> (32 - HashLog);

    /// <summary>
    /// Copies literals 8 bytes at a time (up to 7 bytes past them, which the caller overwrites): the input has the bytes
    /// (literals end 12 bytes before it), and the output the room (see <see cref="Encode{T}"/>).
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe void CopyLiterals(byte* op, byte* from, nuint count)
    {
        byte* end = op + count;
        do
        {
            Unsafe.WriteUnaligned(op, Unsafe.ReadUnaligned<ulong>(from));
            op += 8;
            from += 8;
        }
        while (op < end);
    }

    /// <summary>How many bytes from <paramref name="p"/> equal those from <paramref name="m"/>, up to <paramref name="limit"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe nuint Count(byte* p, byte* m, byte* limit)
    {
        byte* start = p;
        byte* limit8 = limit - 7;
        while (p < limit8)
        {
            ulong diff = Unsafe.ReadUnaligned<ulong>(p) ^ Unsafe.ReadUnaligned<ulong>(m);
            if (diff != 0) return (nuint)(p - start) + (nuint)((BitConverter.IsLittleEndian ? BitOperations.TrailingZeroCount(diff) : BitOperations.LeadingZeroCount(diff)) >> 3);
            p += 8;
            m += 8;
        }
        while (p < limit && *p == *m)
        {
            p++;
            m++;
        }
        return (nuint)(p - start);
    }

    private static unsafe byte* WriteLength(byte* op, nuint length)
    {
        for (; length >= 255; length -= 255) *op++ = 255;
        *op++ = (byte)length;
        return op;
    }
}
