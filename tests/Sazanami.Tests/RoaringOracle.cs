using System.Buffers;
using System.Buffers.Binary;

namespace Sazanami.Tests;

/// <summary>
/// The Delta deletion vector codec as it stood before <c>RoaringBitmap64</c> (Seseragi.Lakehouse
/// <c>DeletionVector.Decode</c> / <c>Serialize</c>, copied verbatim except that it throws
/// <see cref="InvalidDataException"/>): the oracle of the differential tests. Its bytes start with the Delta magic
/// number; the portable 64-bit bitmap follows it.
/// </summary>
internal static class RoaringOracle
{
    private const int Magic = 1681511377;

    /// <summary>The portable 64-bit bitmap of ascending rows (the oracle's bytes without the magic).</summary>
    public static byte[] Portable(long[] sortedRows) => Serialize(sortedRows)[4..];

    /// <summary>Decodes portable 64-bit bitmap bytes (prefixed with the magic number here).</summary>
    public static long[] DecodePortable(ReadOnlySpan<byte> portable)
    {
        byte[] data = new byte[4 + portable.Length];
        BinaryPrimitives.WriteInt32LittleEndian(data, Magic);
        portable.CopyTo(data.AsSpan(4));
        return Decode(data);
    }

    public static long[] Decode(ReadOnlySpan<byte> data)
    {
        try
        {
            if (data.Length < 12 || BinaryPrimitives.ReadInt32LittleEndian(data) != Magic)
                throw new InvalidDataException(
                    "The deletion vector does not start with the magic number of the protocol; it was not written in the Delta format.");

            long bitmaps = BinaryPrimitives.ReadInt64LittleEndian(data[4..]);
            int pos = 12;
            List<long> rows = [];
            for (long i = 0; i < bitmaps; i++)
            {
                long high = (long)BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]) << 32;
                pos += 4;
                pos += DecodeRoaring32(data[pos..], high, rows);
            }
            long[] result = [.. rows];
            for (int i = 1; i < result.Length; i++)
            {
                if (result[i] <= result[i - 1]) throw new InvalidDataException("The deletion vector is not in ascending order.");
            }
            return result;
        }
        catch (Exception e) when (e is ArgumentOutOfRangeException or IndexOutOfRangeException)
        {
            throw new InvalidDataException("The deletion vector is truncated.", e);
        }
    }

    private static int DecodeRoaring32(ReadOnlySpan<byte> data, long high, List<long> rows)
    {
        const int CookieNoRuns = 12346;
        const int CookieRuns = 12347;
        const int NoOffsetThreshold = 4;

        uint cookie = BinaryPrimitives.ReadUInt32LittleEndian(data);
        int pos = 4;
        int containers;
        ReadOnlySpan<byte> runFlags = default;
        bool hasRuns = (cookie & 0xFFFF) == CookieRuns;
        if (hasRuns)
        {
            containers = (int)(cookie >> 16) + 1;
            int flagBytes = (containers + 7) / 8;
            runFlags = data.Slice(pos, flagBytes);
            pos += flagBytes;
        }
        else if (cookie == CookieNoRuns)
        {
            containers = BinaryPrimitives.ReadInt32LittleEndian(data[pos..]);
            pos += 4;
        }
        else
        {
            throw new InvalidDataException($"Unknown roaring bitmap cookie {cookie}.");
        }

        ReadOnlySpan<byte> header = data.Slice(pos, containers * 4);
        pos += containers * 4;
        if (!hasRuns || containers >= NoOffsetThreshold) pos += containers * 4;

        for (int c = 0; c < containers; c++)
        {
            long key = high | ((long)BinaryPrimitives.ReadUInt16LittleEndian(header[(c * 4)..]) << 16);
            int cardinality = BinaryPrimitives.ReadUInt16LittleEndian(header[(c * 4 + 2)..]) + 1;
            bool isRun = hasRuns && (runFlags[c / 8] & (1 << (c % 8))) != 0;

            if (isRun)
            {
                int runs = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]);
                pos += 2;
                for (int r = 0; r < runs; r++)
                {
                    int start = BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]);
                    int length = BinaryPrimitives.ReadUInt16LittleEndian(data[(pos + 2)..]);
                    pos += 4;
                    for (int v = start; v <= start + length; v++) rows.Add(key | (uint)v);
                }
            }
            else if (cardinality <= 4096)
            {
                for (int k = 0; k < cardinality; k++)
                {
                    rows.Add(key | BinaryPrimitives.ReadUInt16LittleEndian(data[pos..]));
                    pos += 2;
                }
            }
            else
            {
                ReadOnlySpan<byte> bits = data.Slice(pos, 8192);
                pos += 8192;
                for (int word = 0; word < 1024; word++)
                {
                    ulong w = BinaryPrimitives.ReadUInt64LittleEndian(bits[(word * 8)..]);
                    while (w != 0)
                    {
                        int bit = System.Numerics.BitOperations.TrailingZeroCount(w);
                        rows.Add(key | (uint)(word * 64 + bit));
                        w &= w - 1;
                    }
                }
            }
        }
        return pos;
    }

    public static byte[] Serialize(long[] DeletedRows)
    {
        var buffer = new ArrayBufferWriter<byte>();
        Span<byte> scratch = stackalloc byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(scratch, Magic);
        buffer.Write(scratch[..4]);

        List<(uint High, int Start, int End)> bitmaps = [];
        for (int i = 0; i < DeletedRows.Length;)
        {
            uint high = (uint)(DeletedRows[i] >> 32);
            int start = i;
            while (i < DeletedRows.Length && (uint)(DeletedRows[i] >> 32) == high) i++;
            bitmaps.Add((high, start, i));
        }
        BinaryPrimitives.WriteInt64LittleEndian(scratch, bitmaps.Count);
        buffer.Write(scratch);
        foreach (var (high, start, end) in bitmaps)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(scratch, high);
            buffer.Write(scratch[..4]);
            EncodeRoaring32(DeletedRows.AsSpan(start, end - start), buffer);
        }
        return buffer.WrittenSpan.ToArray();
    }

    private enum ContainerKind { Array, Bitset, Run }

    private static void EncodeRoaring32(ReadOnlySpan<long> rows, ArrayBufferWriter<byte> output)
    {
        const int CookieNoRuns = 12346;
        const int CookieRuns = 12347;
        const int NoOffsetThreshold = 4;

        List<(ushort Key, int Start, int End, int Runs, ContainerKind Kind)> containers = [];
        for (int i = 0; i < rows.Length;)
        {
            ushort key = (ushort)((uint)rows[i] >> 16);
            int start = i, runs = 0;
            long previous = -2;
            while (i < rows.Length && (ushort)((uint)rows[i] >> 16) == key)
            {
                if (rows[i] != previous + 1) runs++;
                previous = rows[i];
                i++;
            }
            int cardinality = i - start;
            int plainSize = cardinality <= 4096 ? 2 * cardinality : 8192;
            ContainerKind kind = 2 + 4 * runs < plainSize ? ContainerKind.Run
                : cardinality <= 4096 ? ContainerKind.Array : ContainerKind.Bitset;
            containers.Add((key, start, i, runs, kind));
        }

        int count = containers.Count;
        bool hasRuns = containers.Any(c => c.Kind == ContainerKind.Run);
        bool withOffsets = !hasRuns || count >= NoOffsetThreshold;

        var header = new byte[hasRuns ? 4 + (count + 7) / 8 : 8];
        if (hasRuns)
        {
            BinaryPrimitives.WriteUInt32LittleEndian(header, (uint)CookieRuns | (uint)(count - 1) << 16);
            for (int c = 0; c < count; c++)
            {
                if (containers[c].Kind == ContainerKind.Run) header[4 + c / 8] |= (byte)(1 << (c % 8));
            }
        }
        else
        {
            BinaryPrimitives.WriteUInt32LittleEndian(header, CookieNoRuns);
            BinaryPrimitives.WriteInt32LittleEndian(header.AsSpan(4), count);
        }
        output.Write(header);

        Span<byte> pair = stackalloc byte[4];
        foreach (var c in containers)
        {
            BinaryPrimitives.WriteUInt16LittleEndian(pair, c.Key);
            BinaryPrimitives.WriteUInt16LittleEndian(pair[2..], (ushort)(c.End - c.Start - 1));
            output.Write(pair);
        }

        if (withOffsets)
        {
            int offset = header.Length + count * 4 + count * 4;
            foreach (var c in containers)
            {
                BinaryPrimitives.WriteInt32LittleEndian(pair, offset);
                output.Write(pair);
                offset += c.Kind switch
                {
                    ContainerKind.Run => 2 + 4 * c.Runs,
                    ContainerKind.Array => 2 * (c.End - c.Start),
                    _ => 8192,
                };
            }
        }

        foreach (var c in containers)
        {
            ReadOnlySpan<long> values = rows[c.Start..c.End];
            switch (c.Kind)
            {
                case ContainerKind.Run:
                {
                    var data = new byte[2 + 4 * c.Runs];
                    BinaryPrimitives.WriteUInt16LittleEndian(data, (ushort)c.Runs);
                    int pos = 2;
                    for (int i = 0; i < values.Length;)
                    {
                        int start = i;
                        while (i + 1 < values.Length && values[i + 1] == values[i] + 1) i++;
                        BinaryPrimitives.WriteUInt16LittleEndian(data.AsSpan(pos), (ushort)values[start]);
                        BinaryPrimitives.WriteUInt16LittleEndian(data.AsSpan(pos + 2), (ushort)(i - start));
                        pos += 4;
                        i++;
                    }
                    output.Write(data);
                    break;
                }
                case ContainerKind.Array:
                {
                    var data = new byte[2 * values.Length];
                    for (int i = 0; i < values.Length; i++) BinaryPrimitives.WriteUInt16LittleEndian(data.AsSpan(2 * i), (ushort)values[i]);
                    output.Write(data);
                    break;
                }
                default:
                {
                    var words = new ulong[1024];
                    foreach (long v in values)
                    {
                        int low = (ushort)v;
                        words[low >> 6] |= 1UL << (low & 63);
                    }
                    var data = new byte[8192];
                    for (int w = 0; w < 1024; w++) BinaryPrimitives.WriteUInt64LittleEndian(data.AsSpan(8 * w), words[w]);
                    output.Write(data);
                    break;
                }
            }
        }
    }
}
