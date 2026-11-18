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

    /// <summary>
    /// A thread's hash table of positions. Entries are stored as <see cref="t_base"/> + position: a call reserves a
    /// range of values above every earlier one, so entries of earlier calls read as empty without clearing the table
    /// (the output depends on the input alone).
    /// </summary>
    [ThreadStatic]
    private static int[]? t_table;

    [ThreadStatic]
    private static int t_base;

    /// <summary>Sets this thread's table base (tests: near its end, the next call clears the table and starts over).</summary>
    internal static void SetTableBase(int value) => t_base = value;

    /// <summary>
    /// Compresses <paramref name="source"/> into one block (LZ4's fast compressor: a hash table of 4-byte sequences,
    /// the step growing over data without matches, matches extended backward and compared 8 bytes at a time). The
    /// output depends on the input alone.
    /// </summary>
    /// <returns>The compressed length.</returns>
    public static unsafe int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (destination.Length < MaxCompressedLength(source.Length)) throw new ArgumentException("The destination is too small.", nameof(destination));
        fixed (byte* src = source)
        fixed (byte* dst = destination)
        {
            return (int)(Encode(src, source.Length, dst, dst + destination.Length) - dst);
        }
    }

    private static unsafe byte* Encode(byte* src, int length, byte* op, byte* oend)
    {
        byte* anchor = src;
        byte* iend = src + length;
        if (length > MatchSafeDistance)
        {
            int[] table = t_table ??= new int[1 << HashLog];
            if (t_base == 0 || t_base > int.MaxValue - length - 2)
            {
                Array.Clear(table);
                t_base = 1;
            }
            int bas = t_base; // this call's entries: bas + position
            t_base += length + 1;
            fixed (int* t = table)
            {
                byte* mflimit = iend - MatchSafeDistance;     // a match starts at least 12 bytes before the end
                byte* matchlimit = iend - LastLiterals;      // and ends at least 5 before it
                byte* ip = src;
                t[Hash(ip)] = bas;
                ip++;
                uint forwardH = Hash(ip);
                while (true)
                {
                    // Find a match, stepping faster the longer none is found.
                    byte* match;
                    byte* forwardIp = ip;
                    int step = 1, probes = 1 << SkipTrigger;
                    while (true)
                    {
                        uint h = forwardH;
                        ip = forwardIp;
                        forwardIp += step;
                        step = probes++ >> SkipTrigger;
                        if (forwardIp > mflimit + 1) goto Last;
                        int stored = t[h];
                        forwardH = Hash(forwardIp);
                        t[h] = bas + (int)(ip - src);
                        match = src + (stored - bas);
                        if (stored >= bas && match < ip && ip - match <= MaxOffset && Unsafe.ReadUnaligned<uint>(match) == Unsafe.ReadUnaligned<uint>(ip)) break;
                    }
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
                    Copy(op, anchor, literals, oend);
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
                        if (ip >= mflimit) goto Last;
                        t[Hash(ip - 2)] = bas + (int)(ip - 2 - src);
                        // A match right here: a sequence without literals.
                        uint here = Hash(ip);
                        int at = t[here];
                        t[here] = bas + (int)(ip - src);
                        match = src + (at - bas);
                        if (at >= bas && match < ip && ip - match <= MaxOffset && Unsafe.ReadUnaligned<uint>(match) == Unsafe.ReadUnaligned<uint>(ip))
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
        }
    Last:
        // The last sequence: literals only.
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

    /// <summary>Copies literals, 8 bytes at a time when the output has room for the overrun (the input always has: literals end 12 bytes before it).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe void Copy(byte* op, byte* from, nuint count, byte* oend)
    {
        if ((nuint)(oend - op) >= count + 8)
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
        else
        {
            Buffer.MemoryCopy(from, op, (long)count, (long)count);
        }
    }

    /// <summary>How many bytes from <paramref name="p"/> equal those from <paramref name="m"/>, up to <paramref name="limit"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static unsafe nuint Count(byte* p, byte* m, byte* limit)
    {
        byte* start = p;
        while (p < limit - 7)
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
