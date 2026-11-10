using System.Buffers.Binary;
using Kaminari.Seseragi.Delta.Engine;

namespace Kaminari.Seseragi.Delta.Actions;

/// <summary>The rows of one data file that have been deleted, as zero-based row positions.</summary>
public sealed class DeletionVector
{
    private const int Magic = 1681511377;

    /// <summary>The deleted row positions in ascending order.</summary>
    public required long[] DeletedRows { get; init; }

    /// <summary>The number of deleted rows.</summary>
    public long Cardinality => DeletedRows.LongLength;

    /// <summary>Whether a row position is deleted.</summary>
    public bool Contains(long row) => Array.BinarySearch(DeletedRows, row) >= 0;

    /// <summary>Reads the deletion vector a descriptor points at.</summary>
    /// <exception cref="DeltaException">The vector is missing, damaged, or not in the format of the protocol.</exception>
    public static DeletionVector Read(IFileSystem fileSystem, DeletionVectorDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(descriptor);
        byte[] data = descriptor.StorageType switch
        {
            "i" => descriptor.InlineBytes(),
            "u" => ReadFromFile(fileSystem, descriptor),
            "p" => throw new DeltaException("Deletion vectors stored at an absolute path are not read by this kernel yet."),
            _ => throw new DeltaException($"Unknown deletion vector storage type '{descriptor.StorageType}'."),
        };
        long[] rows = Decode(data);
        if (rows.LongLength != descriptor.Cardinality)
            throw new DeltaException(
                $"The deletion vector holds {rows.LongLength} rows but its descriptor says {descriptor.Cardinality}.");
        return new DeletionVector { DeletedRows = rows };
    }

    private static byte[] ReadFromFile(IFileSystem fileSystem, DeletionVectorDescriptor descriptor)
    {
        string path = descriptor.FilePath();
        int offset = descriptor.Offset ?? 0;
        int size = descriptor.SizeInBytes;

        // At the offset: the size (4 bytes, big-endian), the vector, then its CRC-32 (4 bytes, big-endian).
        byte[]? block = fileSystem.ReadRange(path, offset, size + 8);
        if (block is null || block.Length < size + 8 || BinaryPrimitives.ReadInt32BigEndian(block) != size)
            throw NotConformant(path, block is null ? "the file is not at the path the protocol derives" : "the size field does not match");

        ReadOnlySpan<byte> data = block.AsSpan(4, size);
        uint expected = BinaryPrimitives.ReadUInt32BigEndian(block.AsSpan(4 + size));
        if (Crc32(data) != expected)
            throw new DeltaException($"The deletion vector in {path} at offset {offset} fails its checksum.");
        return data.ToArray();
    }

    private static DeltaException NotConformant(string path, string detail) => new(
        $"The deletion vector {path} cannot be read: {detail}. " +
        "Tables whose deletion vectors were written by an early version of this library do not follow the Delta protocol. " +
        "Run optimize on the table with the version that wrote it, so that no file carries a deletion vector, then read it again.");

    /// <summary>Decodes a serialized 64-bit roaring bitmap array into ascending row positions.</summary>
    internal static long[] Decode(ReadOnlySpan<byte> data)
    {
        try
        {
            if (data.Length < 12 || BinaryPrimitives.ReadInt32LittleEndian(data) != Magic)
                throw new DeltaException(
                    "The deletion vector does not start with the magic number of the protocol; it was not written in the Delta format.");

            long bitmaps = BinaryPrimitives.ReadInt64LittleEndian(data[4..]);
            int pos = 12;
            var rows = new List<long>();
            for (long i = 0; i < bitmaps; i++)
            {
                long high = (long)BinaryPrimitives.ReadUInt32LittleEndian(data[pos..]) << 32;
                pos += 4;
                pos += DecodeRoaring32(data[pos..], high, rows);
            }
            long[] result = [.. rows];
            // Bitmaps and containers are stored in ascending key order, so this is a check, not a sort.
            for (int i = 1; i < result.Length; i++)
            {
                if (result[i] <= result[i - 1]) throw new DeltaException("The deletion vector is not in ascending order.");
            }
            return result;
        }
        catch (Exception e) when (e is ArgumentOutOfRangeException or IndexOutOfRangeException)
        {
            throw new DeltaException("The deletion vector is truncated.", e);
        }
    }

    /// <summary>Decodes one 32-bit roaring bitmap in the portable format; returns the number of bytes it takes.</summary>
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
            throw new DeltaException($"Unknown roaring bitmap cookie {cookie}.");
        }

        ReadOnlySpan<byte> header = data.Slice(pos, containers * 4);
        pos += containers * 4;
        if (!hasRuns || containers >= NoOffsetThreshold) pos += containers * 4; // container offsets, not needed for a sequential read

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

    private static readonly uint[] s_crcTable = BuildCrcTable();

    private static uint[] BuildCrcTable()
    {
        var table = new uint[256];
        for (uint i = 0; i < 256; i++)
        {
            uint c = i;
            for (int k = 0; k < 8; k++) c = (c & 1) != 0 ? 0xEDB88320u ^ (c >> 1) : c >> 1;
            table[i] = c;
        }
        return table;
    }

    internal static uint Crc32(ReadOnlySpan<byte> data)
    {
        uint crc = 0xFFFFFFFFu;
        foreach (byte b in data) crc = s_crcTable[(crc ^ b) & 0xFF] ^ (crc >> 8);
        return ~crc;
    }
}
