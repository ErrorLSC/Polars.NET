using System.Buffers;
using System.Buffers.Binary;
using System.IO.Hashing;
using Kaminari.Sazanami.Bitmaps;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Kaminari.Seseragi.Lakehouse.Delta.Actions;

/// <summary>
/// The rows of one data file that have been deleted, as zero-based row positions in a native roaring bitmap
/// (<see cref="RoaringBitmap64"/>, which the vector owns: dispose it).
/// </summary>
public sealed class DeletionVector(RoaringBitmap64 rows) : IDisposable
{
    private const int Magic = 1681511377;

    /// <summary>The deleted row positions.</summary>
    public RoaringBitmap64 Rows { get; } = rows ?? throw new ArgumentNullException(nameof(rows));

    /// <summary>The number of deleted rows.</summary>
    public long Cardinality => Rows.Cardinality;

    /// <summary>Whether a row position is deleted.</summary>
    public bool Contains(long row) => Rows.Contains(row);

    public void Dispose() => Rows.Dispose();

    /// <summary>Reads the deletion vector a descriptor points at.</summary>
    /// <exception cref="DeltaException">The vector is missing, damaged, or not in the format of the protocol.</exception>
    public static DeletionVector Read(IFileSystem fileSystem, DeletionVectorDescriptor descriptor)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(descriptor);
        RoaringBitmap64 rows = descriptor.StorageType switch
        {
            "i" => Decode(descriptor.InlineBytes()),
            "u" or "p" => DecodeFromFile(fileSystem, descriptor),
            _ => throw new DeltaException($"Unknown deletion vector storage type '{descriptor.StorageType}'."),
        };
        if (rows.Cardinality != descriptor.Cardinality)
        {
            long held = rows.Cardinality;
            rows.Dispose();
            throw new DeltaException($"The deletion vector holds {held} rows but its descriptor says {descriptor.Cardinality}.");
        }
        return new DeletionVector(rows);
    }

    /// <summary>The rows of a vector stored in a file: its block read into native memory, checked and decoded.</summary>
    private static RoaringBitmap64 DecodeFromFile(IFileSystem fileSystem, DeletionVectorDescriptor descriptor)
    {
        string path = descriptor.FilePath();
        int offset = descriptor.Offset ?? 0;
        int size = descriptor.SizeInBytes;

        // At the offset: the size (4 bytes, big-endian), the vector, then its CRC-32 (4 bytes, big-endian).
        using var buffer = new NativeBufferWriter(size + 8);
        Span<byte> block = buffer.GetSpan(size + 8)[..(size + 8)];
        int read = fileSystem.ReadRange(path, offset, block);
        if (read < size + 8 || BinaryPrimitives.ReadInt32BigEndian(block) != size)
            throw NotConformant(path, read < 0 ? "the file is not at the path the protocol derives" : "the size field does not match");

        ReadOnlySpan<byte> data = block.Slice(4, size);
        uint expected = BinaryPrimitives.ReadUInt32BigEndian(block[(4 + size)..]);
        if (Crc32.HashToUInt32(data) != expected)
            throw new DeltaException($"The deletion vector in {path} at offset {offset} fails its checksum.");
        return Decode(data); // copies into the bitmap: the block goes back to the pool here
    }

    private static DeltaException NotConformant(string path, string detail) => new(
        $"The deletion vector {path} cannot be read: {detail}. " +
        "Tables whose deletion vectors were written by an early version of this library do not follow the Delta protocol. " +
        "Run optimize on the table with the version that wrote it, so that no file carries a deletion vector, then read it again.");

    /// <summary>Decodes a serialized deletion vector: the magic number, then a portable 64-bit roaring bitmap.</summary>
    internal static RoaringBitmap64 Decode(ReadOnlySpan<byte> data)
    {
        if (data.Length < 4 || BinaryPrimitives.ReadInt32LittleEndian(data) != Magic)
            throw new DeltaException(
                "The deletion vector does not start with the magic number of the protocol; it was not written in the Delta format.");
        try
        {
            return RoaringBitmap64.Deserialize(data[4..]);
        }
        catch (InvalidDataException e)
        {
            throw new DeltaException($"The deletion vector cannot be decoded: {e.Message}", e);
        }
    }

    // ------------------------------------------------------------------
    // Writing
    // ------------------------------------------------------------------

    /// <summary>A vector of the given row positions, in any order, repeats allowed.</summary>
    /// <exception cref="ArgumentException">A row position is negative.</exception>
    public static DeletionVector FromRows(ReadOnlySpan<long> rows) => new(RoaringBitmap64.FromRows(rows));

    /// <summary>A new vector of the rows deleted by this vector or by <paramref name="other"/>.</summary>
    public DeletionVector Union(DeletionVector? other) => new(other is null ? Rows.Clone() : Rows.Union(other.Rows));

    /// <summary>The number of bytes <see cref="Serialize"/> writes.</summary>
    public int SerializedSize => checked(4 + (int)Rows.SerializedSize);

    /// <summary>
    /// Writes the vector as the protocol serializes it: the magic number, then a 64-bit roaring bitmap array in the
    /// portable format. Each container takes the smallest of its array, bitset and run forms.
    /// </summary>
    public void Serialize(IBufferWriter<byte> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        BinaryPrimitives.WriteInt32LittleEndian(output.GetSpan(4), Magic);
        output.Advance(4);
        Rows.Serialize(output);
    }

    /// <summary>A descriptor holding this vector inline (storage type <c>i</c>); meant for small vectors.</summary>
    public DeletionVectorDescriptor ToInlineDescriptor()
    {
        int size = SerializedSize;
        using var buffer = new NativeBufferWriter(size + 3);
        Serialize(buffer);
        buffer.GetSpan(3)[..((4 - size % 4) % 4)].Clear(); // Z85 takes whole 4-byte groups: pad with zeros
        buffer.Advance((4 - size % 4) % 4);
        return new DeletionVectorDescriptor
        {
            StorageType = "i",
            PathOrInlineDv = Z85.Encode(buffer.WrittenSpan),
            SizeInBytes = size,
            Cardinality = Cardinality,
        };
    }

    /// <summary>
    /// Writes vectors into one new file <c>deletion_vector_UUID.bin</c> and returns their descriptors (storage type
    /// <c>u</c>), in the order given. The file holds a format version byte, then per vector its size, its bytes and
    /// their CRC-32, both numbers big-endian; descriptors point at the size.
    /// </summary>
    /// <param name="fileSystem">The table's file system.</param>
    /// <param name="vectors">The vectors to write.</param>
    /// <param name="prefixLength">The length of the random directory the file goes into (the descriptors carry it);
    /// 0 for the table root. See <see cref="FilePrefixes.ForDeletionVectors"/>.</param>
    /// <exception cref="DeltaException">The file could not be created.</exception>
    public static IReadOnlyList<DeletionVectorDescriptor> WriteFile(
        IWritableFileSystem fileSystem, IReadOnlyList<DeletionVector> vectors, int prefixLength = 0)
    {
        ArgumentNullException.ThrowIfNull(fileSystem);
        ArgumentNullException.ThrowIfNull(vectors);
        if (vectors.Count == 0) return [];

        using var file = new NativeBufferWriter();
        file.GetSpan(1)[0] = 1; // the format version
        file.Advance(1);
        var placed = new (int Offset, int Size, long Cardinality)[vectors.Count];
        for (int i = 0; i < vectors.Count; i++)
        {
            DeletionVector vector = vectors[i];
            int offset = file.WrittenCount, size = vector.SerializedSize;
            placed[i] = (offset, size, vector.Cardinality);
            BinaryPrimitives.WriteInt32BigEndian(file.GetSpan(4), size);
            file.Advance(4);
            vector.Serialize(file);
            BinaryPrimitives.WriteUInt32BigEndian(file.GetSpan(4), Crc32.HashToUInt32(file.WrittenSpan.Slice(offset + 4, size)));
            file.Advance(4);
        }

        Guid id = Guid.NewGuid();
        string prefix = FilePrefixes.New(prefixLength);
        string encodedId = prefix + Z85.Encode(id.ToByteArray(bigEndian: true));
        string path = prefix.Length == 0 ? $"deletion_vector_{id:D}.bin" : $"{prefix}/deletion_vector_{id:D}.bin";
        if (!fileSystem.TryCreate(path, file.WrittenMemory))
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
}
