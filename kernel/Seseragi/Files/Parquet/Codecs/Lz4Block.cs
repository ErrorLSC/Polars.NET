using System.Buffers.Binary;
using System.Runtime.CompilerServices;

namespace Kaminari.Seseragi.Files.Parquet.Codecs;

/// <summary>
/// The LZ4 block format (lz4_Block_format.md): sequences of a token, literals, and a match (offset, length); the
/// last sequence has literals only. Parquet's LZ4_RAW codec is one such block per page.
/// </summary>
internal static class Lz4Block
{
    private const int MinMatch = 4;
    private const int LastLiterals = 5;     // the last 5 bytes are always literals
    private const int MatchSafeDistance = 12; // the last match starts at least 12 bytes before the end
    private const int HashBits = 16;

    /// <summary>Decompresses a block into <paramref name="destination"/>; returns the bytes written.</summary>
    /// <exception cref="ParquetFormatException">The block is damaged or does not fit.</exception>
    public static int Decompress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        int ip = 0, op = 0;
        if (source.IsEmpty) return 0;
        while (true)
        {
            if (ip >= source.Length) throw Damaged();
            int token = source[ip++];

            int literals = token >> 4;
            if (literals == 15) literals += ReadLength(source, ref ip);
            if (literals > source.Length - ip || literals > destination.Length - op) throw Damaged();
            source.Slice(ip, literals).CopyTo(destination[op..]);
            ip += literals;
            op += literals;
            if (ip == source.Length) return op; // the last sequence ends after its literals

            if (source.Length - ip < 2) throw Damaged();
            int offset = BinaryPrimitives.ReadUInt16LittleEndian(source[ip..]);
            ip += 2;
            if (offset == 0 || offset > op) throw Damaged();

            int match = token & 0x0F;
            if (match == 15) match += ReadLength(source, ref ip);
            match += MinMatch;
            if (match > destination.Length - op) throw Damaged();

            int from = op - offset;
            if (offset >= match) destination.Slice(from, match).CopyTo(destination[op..]);
            else for (int i = 0; i < match; i++) destination[op + i] = destination[from + i]; // overlapping: repeats
            op += match;
        }
    }

    private static int ReadLength(ReadOnlySpan<byte> source, ref int ip)
    {
        int length = 0;
        byte b;
        do
        {
            if (ip >= source.Length) throw Damaged();
            b = source[ip++];
            length += b;
            if (length > int.MaxValue / 2) throw Damaged();
        }
        while (b == 255);
        return length;
    }

    private static ParquetFormatException Damaged() => new("An LZ4 block is damaged.");

    /// <summary>The largest block <paramref name="length"/> input bytes can compress to.</summary>
    public static int MaxCompressedLength(int length) => length + length / 255 + 16;

    /// <summary>Compresses <paramref name="source"/> into one block (greedy, with a hash table of 4-byte sequences).</summary>
    /// <returns>The compressed length.</returns>
    public static int Compress(ReadOnlySpan<byte> source, Span<byte> destination)
    {
        if (destination.Length < MaxCompressedLength(source.Length)) throw new ArgumentException("The destination is too small.", nameof(destination));
        int op = 0, anchor = 0;
        int limit = source.Length - MatchSafeDistance; // matches start before this
        if (source.Length >= MatchSafeDistance + 1)
        {
            Span<int> table = new int[1 << HashBits];
            table.Fill(-1);
            int ip = 0;
            while (ip < limit)
            {
                uint sequence = BinaryPrimitives.ReadUInt32LittleEndian(source[ip..]);
                int h = Hash(sequence);
                int candidate = table[h];
                table[h] = ip;
                if (candidate < 0 || ip - candidate > 65535 || BinaryPrimitives.ReadUInt32LittleEndian(source[candidate..]) != sequence)
                {
                    ip++;
                    continue;
                }
                // Extend the match forward, keeping the last literals out of it.
                int matchEnd = ip + MinMatch, from = candidate + MinMatch, end = source.Length - LastLiterals;
                while (matchEnd < end && source[matchEnd] == source[from])
                {
                    matchEnd++;
                    from++;
                }
                op = WriteSequence(source[anchor..ip], ip - candidate, matchEnd - ip - MinMatch, destination, op);
                ip = matchEnd;
                anchor = ip;
            }
        }
        // The last sequence: literals only.
        ReadOnlySpan<byte> tail = source[anchor..];
        int length = tail.Length;
        destination[op++] = (byte)(Math.Min(length, 15) << 4);
        if (length >= 15) op = WriteLength(length - 15, destination, op);
        tail.CopyTo(destination[op..]);
        return op + length;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static int Hash(uint sequence) => (int)((sequence * 2654435761u) >> (32 - HashBits));

    private static int WriteSequence(ReadOnlySpan<byte> literals, int offset, int matchExtra, Span<byte> destination, int op)
    {
        int tokenAt = op++;
        int literalCode = Math.Min(literals.Length, 15), matchCode = Math.Min(matchExtra, 15);
        destination[tokenAt] = (byte)((literalCode << 4) | matchCode);
        if (literals.Length >= 15) op = WriteLength(literals.Length - 15, destination, op);
        literals.CopyTo(destination[op..]);
        op += literals.Length;
        BinaryPrimitives.WriteUInt16LittleEndian(destination[op..], (ushort)offset);
        op += 2;
        if (matchExtra >= 15) op = WriteLength(matchExtra - 15, destination, op);
        return op;
    }

    private static int WriteLength(int length, Span<byte> destination, int op)
    {
        while (length >= 255)
        {
            destination[op++] = 255;
            length -= 255;
        }
        destination[op++] = (byte)length;
        return op;
    }
}
