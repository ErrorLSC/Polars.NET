using System.Buffers.Binary;
using Kaminari.Seseragi.Lakehouse.Delta;
using Kaminari.Seseragi.Lakehouse.Delta.Actions;
using Kaminari.Seseragi.Lakehouse.Engine;

namespace Seseragi.Lakehouse.Tests;

public class DeletionVectorTests
{
    private static string TablePath(string name) => Path.Combine(AppContext.BaseDirectory, "TestData", name);

    [Fact]
    public void ReadsADeletionVectorWrittenByDatabricks()
    {
        var fs = new LocalFileSystem(TablePath("table-with-dv-small"));

        Snapshot snapshot = Snapshot.Load(fs);
        AddAction file = Assert.Single(snapshot.Files);
        DeletionVector dv = DeletionVector.Read(fs, file.DeletionVector!);

        Assert.Equal("deletion_vector_61d16c75-6994-46b7-a15b-8b538852e50e.bin", file.DeletionVector!.FilePath());
        Assert.Equal([0L, 9L], dv.DeletedRows);
        Assert.True(dv.Contains(9));
        Assert.False(dv.Contains(5));

        // Before the delete the same file had no deletion vector.
        Assert.Null(Assert.Single(Snapshot.Load(fs, 0).Files).DeletionVector);
    }

    // ---- hand-built vectors for the container kinds the sample does not have ----

    private static byte[] Le32(uint v) { var b = new byte[4]; BinaryPrimitives.WriteUInt32LittleEndian(b, v); return b; }
    private static byte[] Le16(int v) { var b = new byte[2]; BinaryPrimitives.WriteUInt16LittleEndian(b, (ushort)v); return b; }
    private static byte[] Le64(long v) { var b = new byte[8]; BinaryPrimitives.WriteInt64LittleEndian(b, v); return b; }
    private static byte[] Cat(params byte[][] parts) => parts.SelectMany(p => p).ToArray();

    private static byte[] Array64(params (uint High, byte[] Bitmap)[] bitmaps) =>
        Cat([Le32(1681511377), Le64(bitmaps.Length), .. bitmaps.SelectMany(b => new[] { Le32(b.High), b.Bitmap })]);

    [Fact]
    public void Decodes_ArrayContainers_AcrossKeysAndHighWords()
    {
        // No-run cookie, two containers: key 0 -> {1, 2}, key 1 -> {7}; then offsets (ignored).
        byte[] bitmap = Cat(Le32(12346), Le32(2),
            Le16(0), Le16(1), Le16(1), Le16(0),
            Le32(0), Le32(0),
            Le16(1), Le16(2), Le16(7));

        long[] rows = DeletionVector.Decode(Array64((0, bitmap), (1, bitmap)));

        Assert.Equal([1L, 2L, 65536L + 7, (1L << 32) + 1, (1L << 32) + 2, (1L << 32) + 65536 + 7], rows);
    }

    [Fact]
    public void Decodes_RunContainers()
    {
        // Run cookie with one container (count - 1 in the high half), run flag set, no offsets below four containers.
        // Cardinality 6: runs [10..12] and [20..22].
        byte[] bitmap = Cat(Le32(12347), [0b0000_0001],
            Le16(0), Le16(5),
            Le16(2), Le16(10), Le16(2), Le16(20), Le16(2));

        Assert.Equal([10L, 11L, 12L, 20L, 21L, 22L], DeletionVector.Decode(Array64((0, bitmap))));
    }

    [Fact]
    public void Decodes_BitsetContainers()
    {
        // 5000 values (every even number below 10000) need a bitset container.
        var bits = new byte[8192];
        for (int v = 0; v < 10000; v += 2) bits[v / 8] |= (byte)(1 << (v % 8));
        byte[] bitmap = Cat(Le32(12346), Le32(1), Le16(0), Le16(4999), Le32(0), bits);

        long[] rows = DeletionVector.Decode(Array64((0, bitmap)));

        Assert.Equal(5000, rows.Length);
        Assert.Equal(Enumerable.Range(0, 5000).Select(i => (long)i * 2), rows);
    }

    private static byte[] Framed(byte[] data)
    {
        var size = new byte[4];
        BinaryPrimitives.WriteInt32BigEndian(size, data.Length);
        var crc = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(crc, DeletionVector.Crc32(data));
        return Cat([1], size, data, crc);
    }

    private static readonly byte[] s_oneRow = Array64((0, Cat(Le32(12346), Le32(1), Le16(0), Le16(0), Le32(0), Le16(3))));
    private const string Uuid85 = "^-aqEH.-t@S}K{vb[*k^";
    private const string UuidFile = "deletion_vector_d2c639aa-8816-431a-aaf6-d3fe2512ff61.bin";

    [Fact]
    public void ReadsFromAFile_UnderItsPrefix_AndInline()
    {
        var fs = new MemoryFileSystem().PutBytes("ab/" + UuidFile, Framed(s_oneRow));
        var descriptor = new DeletionVectorDescriptor { StorageType = "u", PathOrInlineDv = "ab" + Uuid85, Offset = 1, SizeInBytes = s_oneRow.Length, Cardinality = 1 };
        Assert.Equal([3L], DeletionVector.Read(fs, descriptor).DeletedRows);

        var inline = new DeletionVectorDescriptor { StorageType = "i", PathOrInlineDv = Z85.Encode(Cat(s_oneRow, new byte[(4 - s_oneRow.Length % 4) % 4])), SizeInBytes = s_oneRow.Length, Cardinality = 1 };
        Assert.Equal([3L], DeletionVector.Read(fs, inline).DeletedRows);
    }

    [Fact]
    public void DamagedVectors_AreRejected()
    {
        byte[] framed = Framed(s_oneRow);
        framed[^1] ^= 0xFF;
        var fs = new MemoryFileSystem().PutBytes(UuidFile, framed);
        var descriptor = new DeletionVectorDescriptor { StorageType = "u", PathOrInlineDv = Uuid85, Offset = 1, SizeInBytes = s_oneRow.Length, Cardinality = 1 };
        Assert.Contains("checksum", Assert.Throws<DeltaException>(() => DeletionVector.Read(fs, descriptor)).Message);

        var good = new MemoryFileSystem().PutBytes(UuidFile, Framed(s_oneRow));
        var wrongCount = new DeletionVectorDescriptor { StorageType = "u", PathOrInlineDv = Uuid85, Offset = 1, SizeInBytes = s_oneRow.Length, Cardinality = 2 };
        Assert.Contains("descriptor says 2", Assert.Throws<DeltaException>(() => DeletionVector.Read(good, wrongCount)).Message);

        Assert.Contains("truncated", Assert.Throws<DeltaException>(() => DeletionVector.Decode(s_oneRow.AsSpan(0, s_oneRow.Length - 1))).Message);
    }

    [Fact]
    public void VectorsOfTheEarlyWriter_GiveTheOptimizeAdvice()
    {
        // That writer put the prefix "p" in the descriptor, stored the file elsewhere, and wrote a bare bitmap at offset 0.
        var descriptor = new DeletionVectorDescriptor { StorageType = "u", PathOrInlineDv = "p" + Uuid85, Offset = 0, SizeInBytes = 20, Cardinality = 2 };

        var missing = Assert.Throws<DeltaException>(() => DeletionVector.Read(new MemoryFileSystem(), descriptor));
        Assert.Contains("optimize", missing.Message);

        var bare = new MemoryFileSystem().PutBytes("p/" + UuidFile, Cat(Le32(12346), Le32(1), Le16(0), Le16(1), Le32(16), Le16(1), Le16(2)));
        Assert.Contains("optimize", Assert.Throws<DeltaException>(() => DeletionVector.Read(bare, descriptor)).Message);
    }
}
