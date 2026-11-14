using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace Kaminari.Sazanami.Compression;

/// <summary>
/// Decompression of the raw Snappy format (format_description.txt): the uncompressed length as a varint, then
/// literals and copies (back references of 1-, 2- or 4-byte offsets). Writes straight into the destination: no
/// scratch, no extra copy. While both buffers have a margin left, literals and copies move whole vectors and words
/// without room checks (the margin holds what they move past their own bytes); the last bytes are decoded with every
/// read and write checked against the buffers. Offsets are checked everywhere. Shared by the formats that store Snappy
/// blocks (Parquet pages, Avro blocks).
/// </summary>
internal static unsafe class SnappyBlock
{
    // A step of the fast loop reads at most 65 bytes past the tag and writes at most 72 past op.
    private const int FastMargin = 80;

    /// <summary>The uncompressed length the data declares (its leading varint).</summary>
    /// <exception cref="InvalidDataException">The varint is damaged.</exception>
    public static int UncompressedLength(ReadOnlySpan<byte> source)
    {
        uint length = 0;
        for (int i = 0, shift = 0; ; i++, shift += 7)
        {
            if (i >= source.Length || shift > 28) throw Damaged();
            byte b = source[i];
            length |= (uint)(b & 0x7F) << shift;
            if (b < 0x80) break;
        }
        if (length > int.MaxValue) throw Damaged();
        return (int)length;
    }

    /// <summary>Decompresses <paramref name="source"/> into <paramref name="destination"/>; returns the bytes written.</summary>
    /// <exception cref="InvalidDataException">The data is damaged, or its length is not the destination's.</exception>
    public static int Decompress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        fixed (byte* src = source)
        fixed (byte* dst = destination)
        {
            byte* ip = src, iend = src + source.Length;
            ulong length = 0;
            for (int shift = 0; ; shift += 7)
            {
                if (ip >= iend || shift > 28) throw Damaged();
                byte b = *ip++;
                length |= (ulong)(b & 0x7F) << shift;
                if (b < 0x80) break;
            }
            if (length != (ulong)destination.Length) throw new InvalidDataException($"The Snappy data holds {length} bytes; {destination.Length} were expected.");

            byte* op = dst, oend = dst + destination.Length;
            if (iend - ip > FastMargin && oend - op > FastMargin) DecodeFast(dst, ref ip, iend, ref op, oend);
            while (ip < iend)
            {
                uint tag = *ip++;
                switch (tag & 3)
                {
                    case 0:
                    {
                        // A literal: its length - 1 in the tag, or in 1-4 bytes after it.
                        nuint n = (tag >> 2) + 1;
                        if (n > 60)
                        {
                            int bytes = (int)n - 60;
                            if (iend - ip < bytes) throw Damaged();
                            nuint value = 0;
                            for (int k = 0; k < bytes; k++) value |= (nuint)ip[k] << (8 * k);
                            ip += bytes;
                            n = value + 1;
                        }
                        if ((nuint)(iend - ip) < n || (nuint)(oend - op) < n) throw Damaged();
                        if (n <= 16 && iend - ip >= 16 && oend - op >= 16)
                            Vector128.Store(Vector128.Load(ip), op);
                        else
                            Buffer.MemoryCopy(ip, op, (long)n, (long)n);
                        ip += n;
                        op += n;
                        break;
                    }
                    case 1:
                    {
                        if (ip >= iend) throw Damaged();
                        int n = (int)((tag >> 2) & 7) + 4;
                        int offset = (int)((tag >> 5) << 8) | *ip++;
                        op = Copy(dst, op, oend, offset, n);
                        break;
                    }
                    case 2:
                    {
                        if (iend - ip < 2) throw Damaged();
                        int n = (int)(tag >> 2) + 1;
                        int offset = Unsafe.ReadUnaligned<ushort>(ip);
                        ip += 2;
                        op = Copy(dst, op, oend, offset, n);
                        break;
                    }
                    default:
                    {
                        if (iend - ip < 4) throw Damaged();
                        int n = (int)(tag >> 2) + 1;
                        uint offset = Unsafe.ReadUnaligned<uint>(ip);
                        ip += 4;
                        if (offset > int.MaxValue) throw Damaged();
                        op = Copy(dst, op, oend, (int)offset, n);
                        break;
                    }
                }
            }
            if (op != oend) throw new InvalidDataException($"The Snappy data ends after {op - dst} of its {destination.Length} bytes.");
            return destination.Length;
        }
    }

    /// <summary>
    /// Decodes while both buffers have <see cref="FastMargin"/> bytes left, moving whole vectors and words without
    /// room checks; a long literal is checked like any other.
    /// </summary>
    private static void DecodeFast(byte* dst, ref byte* ipRef, byte* iend, ref byte* opRef, byte* oend)
    {
        byte* ip = ipRef, op = opRef, ifast = iend - FastMargin, ofast = oend - FastMargin;
        while (ip < ifast && op < ofast)
        {
            uint tag = *ip++;
            switch (tag & 3)
            {
                case 0:
                {
                    nuint n = (tag >> 2) + 1;
                    if (n <= 16) Vector128.Store(Vector128.Load(ip), op);
                    else if (n <= 60)
                    {
                        Vector128.Store(Vector128.Load(ip), op);
                        Vector128.Store(Vector128.Load(ip + 16), op + 16);
                        Vector128.Store(Vector128.Load(ip + 32), op + 32);
                        Vector128.Store(Vector128.Load(ip + 48), op + 48);
                    }
                    else
                    {
                        // The length - 1 in the 1-4 bytes after the tag.
                        int bytes = (int)n - 60;
                        n = (Unsafe.ReadUnaligned<uint>(ip) & (uint.MaxValue >> (32 - 8 * bytes))) + (nuint)1;
                        ip += bytes;
                        if ((nuint)(iend - ip) < n || (nuint)(oend - op) < n) throw Damaged();
                        Buffer.MemoryCopy(ip, op, (long)n, (long)n);
                    }
                    ip += n;
                    op += n;
                    break;
                }
                case 1:
                {
                    nuint n = ((tag >> 2) & 7) + 4;
                    nuint offset = (nuint)((tag >> 5) << 8) | *ip++;
                    op = CopyFast(dst, op, offset, n);
                    break;
                }
                case 2:
                {
                    nuint n = (tag >> 2) + 1;
                    nuint offset = Unsafe.ReadUnaligned<ushort>(ip);
                    ip += 2;
                    op = CopyFast(dst, op, offset, n);
                    break;
                }
                default:
                {
                    nuint n = (tag >> 2) + 1;
                    nuint offset = Unsafe.ReadUnaligned<uint>(ip);
                    ip += 4;
                    op = CopyFast(dst, op, offset, n);
                    break;
                }
            }
        }
        ipRef = ip;
        opRef = op;
    }

    /// <summary>A copy of the fast loop: <paramref name="n"/> ≤ 64, and <see cref="FastMargin"/> bytes are left at <paramref name="op"/>.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte* CopyFast(byte* start, byte* op, nuint offset, nuint n)
    {
        if (offset - 1 >= (nuint)(op - start)) throw Damaged(); // 0, or before the start
        byte* from = op - offset;
        if (offset >= 16)
        {
            // Up to four 16-byte blocks, each reading bytes written before it.
            Vector128.Store(Vector128.Load(from), op);
            if (n > 16)
            {
                Vector128.Store(Vector128.Load(from + 16), op + 16);
                if (n > 32)
                {
                    Vector128.Store(Vector128.Load(from + 32), op + 32);
                    if (n > 48) Vector128.Store(Vector128.Load(from + 48), op + 48);
                }
            }
        }
        else if (offset >= 8)
        {
            // Eight bytes at a time: a word may read what the one before wrote, which is the repetition wanted.
            for (nuint k = 0; k < n; k += 8) Unsafe.WriteUnaligned(op + k, Unsafe.ReadUnaligned<ulong>(from + k));
        }
        else
        {
            // A repeat of 1-7 bytes: each word doubles the distance to its source (its bytes past that distance are
            // rewritten by the next), until whole words follow.
            byte* end = op + n;
            while (op - from < 8)
            {
                Unsafe.WriteUnaligned(op, Unsafe.ReadUnaligned<ulong>(from));
                op += op - from;
            }
            for (; op < end; op += 8, from += 8) Unsafe.WriteUnaligned(op, Unsafe.ReadUnaligned<ulong>(from));
            return end;
        }
        return op + n;
    }

    /// <summary>Copies <paramref name="n"/> bytes from <paramref name="offset"/> back (they may overlap the bytes being written).</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static byte* Copy(byte* start, byte* op, byte* oend, int offset, int n)
    {
        if (offset <= 0 || offset > op - start || n > oend - op) throw Damaged();
        byte* from = op - offset;
        if (offset >= 16 && oend - op >= 64)
        {
            // n ≤ 64: up to four 16-byte blocks, each reading bytes written before it (offset ≥ 16).
            Vector128.Store(Vector128.Load(from), op);
            if (n > 16)
            {
                Vector128.Store(Vector128.Load(from + 16), op + 16);
                if (n > 32)
                {
                    Vector128.Store(Vector128.Load(from + 32), op + 32);
                    if (n > 48) Vector128.Store(Vector128.Load(from + 48), op + 48);
                }
            }
        }
        else if (offset >= 8 && oend - op >= n + 8)
        {
            // Eight bytes at a time: a later word may read what an earlier one wrote, which is the repetition wanted
            // (offset ≥ 8 keeps each word's source written before it is read); the last word may write past n (room checked).
            for (int k = 0; k < n; k += 8) Unsafe.WriteUnaligned(op + k, Unsafe.ReadUnaligned<ulong>(from + k));
        }
        else
        {
            for (int k = 0; k < n; k++) op[k] = from[k];
        }
        return op + n;
    }

    private static InvalidDataException Damaged() => new("The Snappy data is damaged.");
}
