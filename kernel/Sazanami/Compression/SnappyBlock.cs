using System.Runtime.CompilerServices;
using System.Runtime.Intrinsics;

namespace Kaminari.Sazanami.Compression;

/// <summary>
/// Decompression of the raw Snappy format (format_description.txt): the uncompressed length as a varint, then
/// literals and copies (back references of 1-, 2- or 4-byte offsets). Writes straight into the destination: no
/// scratch, no extra copy. Short literals and copies move 8 or 16 bytes at a time where both ends have room; every
/// read and write is checked against the buffers. Shared by the formats that store Snappy blocks (Parquet pages,
/// Avro blocks).
/// </summary>
internal static unsafe class SnappyBlock
{
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
