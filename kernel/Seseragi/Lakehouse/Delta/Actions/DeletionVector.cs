using System.Buffers;
using System.Buffers.Binary;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta.Actions;

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
            "u" or "p" => ReadFromFile(fileSystem, descriptor),
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

    // ------------------------------------------------------------------
    // Writing
    // ------------------------------------------------------------------

    /// <summary>A vector of the given row positions, in any order, duplicates allowed.</summary>
    public static DeletionVector FromRows(IEnumerable<long> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);
        long[] sorted = [.. rows];
        Array.Sort(sorted);
        int n = 0;
        for (int i = 0; i < sorted.Length; i++)
        {
            if (sorted[i] < 0) throw new ArgumentOutOfRangeException(nameof(rows), "Row positions are not negative.");
            if (n == 0 || sorted[i] != sorted[n - 1]) sorted[n++] = sorted[i];
        }
        return new DeletionVector { DeletedRows = n == sorted.Length ? sorted : sorted[..n] };
    }

    /// <summary>The rows deleted by this vector or by <paramref name="other"/>.</summary>
    public DeletionVector Union(DeletionVector? other) =>
        other is null || other.Cardinality == 0 ? this : FromRows(DeletedRows.Concat(other.DeletedRows));

    /// <summary>
    /// The vector as the protocol serializes it: a 64-bit roaring bitmap array in the portable format, preceded by
    /// the magic number. Each container takes the smallest of its array, bitset and run forms.
    /// </summary>
    public byte[] Serialize()
    {
        var buffer = new System.Buffers.ArrayBufferWriter<byte>();
        Span<byte> scratch = stackalloc byte[8];
        BinaryPrimitives.WriteInt32LittleEndian(scratch, Magic);
        buffer.Write(scratch[..4]);

        // One 32-bit bitmap per distinct high word, in ascending order.
        var bitmaps = new List<(uint High, int Start, int End)>();
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

    /// <summary>Encodes one 32-bit roaring bitmap (the low words of <paramref name="rows"/>) in the portable format.</summary>
    private static void EncodeRoaring32(ReadOnlySpan<long> rows, System.Buffers.ArrayBufferWriter<byte> output)
    {
        const int CookieNoRuns = 12346;
        const int CookieRuns = 12347;
        const int NoOffsetThreshold = 4;

        // Containers: rows sharing the high 16 bits of the low word.
        var containers = new List<(ushort Key, int Start, int End, int Runs, ContainerKind Kind)>();
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
            // Offsets count from the start of this bitmap.
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

    /// <summary>A descriptor holding this vector inline (storage type <c>i</c>); meant for small vectors.</summary>
    public DeletionVectorDescriptor ToInlineDescriptor()
    {
        byte[] data = Serialize();
        byte[] padded = data.Length % 4 == 0 ? data : [.. data, .. new byte[4 - data.Length % 4]];
        return new DeletionVectorDescriptor
        {
            StorageType = "i",
            PathOrInlineDv = Z85.Encode(padded),
            SizeInBytes = data.Length,
            Cardinality = Cardinality,
        };
    }

    /// <summary>
    /// Writes vectors into one new file <c>deletion_vector_UUID.bin</c> at the table root and returns their
    /// descriptors (storage type <c>u</c>), in the order given. The file holds a format version byte, then per vector
    /// its size, its bytes and their CRC-32, both numbers big-endian; descriptors point at the size.
    /// </summary>
    /// <exception cref="DeltaException">The file could not be created.</exception>
    public static IReadOnlyList<DeletionVectorDescriptor> WriteFile(IWritableFileSystem fileSystem, IReadOnlyList<DeletionVector> vectors)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(vectors);
        if (vectors.Count == 0) return [];

        var file = new System.Buffers.ArrayBufferWriter<byte>();
        file.Write<byte>([1]); // the format version
        var placed = new List<(int Offset, int Size, long Cardinality)>(vectors.Count);
        Span<byte> number = stackalloc byte[4];
        foreach (DeletionVector vector in vectors)
        {
            byte[] data = vector.Serialize();
            placed.Add((file.WrittenCount, data.Length, vector.Cardinality));
            BinaryPrimitives.WriteInt32BigEndian(number, data.Length);
            file.Write(number);
            file.Write(data);
            BinaryPrimitives.WriteUInt32BigEndian(number, Crc32(data));
            file.Write(number);
        }

        Guid id = Guid.NewGuid();
        string encodedId = Z85.Encode(id.ToByteArray(bigEndian: true));
        string path = $"deletion_vector_{id:D}.bin";
        if (!fileSystem.TryCreate(path, file.WrittenSpan.ToArray()))
            throw new DeltaException($"The deletion vector file {path} already exists.");

        return [.. placed.Select(p => new DeletionVectorDescriptor
        {
            StorageType = "u",
            PathOrInlineDv = encodedId,
            Offset = p.Offset,
            SizeInBytes = p.Size,
            Cardinality = p.Cardinality,
        })];
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
