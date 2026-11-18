using System.Buffers;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Kaminari.Seseragi.Files.Parquet.Writing;

/// <summary>
/// What a file's footer and page indexes will hold, kept as Thrift bytes in native memory from the moment each row
/// group is laid out; the footer is written from them at the end. Metadata objects kept that long outlive several
/// garbage collections (a sink keeps a thousand files open, each for the whole write) and are copied up to the oldest
/// generation only to die there; the objects a chunk's metadata is built from die young instead. The bytes are those
/// <see cref="FileMetaData.Write"/> writes for the same footer.
/// </summary>
internal sealed class FooterBuilder : IDisposable
{
    private readonly NativeBufferWriter _metadata = new(1 << 12);      // each chunk's ColumnMetaData fields but the Bloom filter's
    private readonly NativeBufferWriter _columnIndexes = new(1 << 12); // in layout order, as they are written
    private readonly NativeBufferWriter _offsetIndexes = new(1 << 12);
    private readonly NativeArray<Chunk> _chunks = new();
    private readonly NativeArray<Group> _groups = new();
    private int _chunkCount, _groupCount;

    private struct Chunk
    {
        public long FileOffset;
        public int Metadata, Head, Tail; // in _metadata: the fields before the Bloom filter's, then those after
        public short HeadLast, TailLast; // the id of each part's last field
        public bool Bloom;
        public int BloomLength;
        public long BloomOffset;
        public int ColumnIndex, ColumnIndexLength; // in _columnIndexes (length -1: none)
        public int OffsetIndex, OffsetIndexLength; // in _offsetIndexes (length -1: none)
        public long ColumnIndexOffset, OffsetIndexOffset; // in the file, once written
    }

    private struct Group
    {
        public long TotalByteSize, NumRows, FileOffset, TotalCompressedSize;
    }

    /// <summary>
    /// Adds a chunk of the row group being laid out (at <paramref name="fileOffset"/>; its data pages at
    /// <paramref name="dataPages"/>, where the offset index's page offsets count from); returns its number, by which
    /// its Bloom filter is placed (<paramref name="bloom"/>: it has one, placed before the footer is written).
    /// </summary>
    public int AddChunk(long fileOffset, ColumnMetaData meta, bool bloom, ColumnIndex? columnIndex, OffsetIndex? offsetIndex, long dataPages)
    {
        ThriftCompactWriter w = ThriftCompactWriter.ThreadScratch();
        meta.WriteHead(w); // from field 0: the fields as the open struct writes them
        int head = w.WrittenSpan.Length;
        short headLast = w.LastField;
        if (bloom) w.AfterField(15); // the filter's fields go between
        meta.WriteTail(w);
        _chunks.Grow(_chunkCount + 1, _chunkCount);
        ref Chunk chunk = ref _chunks[_chunkCount];
        chunk = new Chunk
        {
            FileOffset = fileOffset,
            Metadata = _metadata.WrittenCount,
            Head = head,
            Tail = w.WrittenSpan.Length - head,
            HeadLast = headLast,
            TailLast = w.LastField,
            Bloom = bloom,
            ColumnIndexLength = -1,
            OffsetIndexLength = -1,
        };
        _metadata.Write(w.WrittenSpan);
        if (columnIndex is not null)
        {
            w = ThriftCompactWriter.ThreadScratch();
            columnIndex.Write(w);
            chunk.ColumnIndex = _columnIndexes.WrittenCount;
            chunk.ColumnIndexLength = w.WrittenSpan.Length;
            _columnIndexes.Write(w.WrittenSpan);
        }
        if (offsetIndex is not null)
        {
            w = ThriftCompactWriter.ThreadScratch();
            offsetIndex.Write(w, dataPages);
            chunk.OffsetIndex = _offsetIndexes.WrittenCount;
            chunk.OffsetIndexLength = w.WrittenSpan.Length;
            _offsetIndexes.Write(w.WrittenSpan);
        }
        return _chunkCount++;
    }

    /// <summary>Closes the row group whose chunks were added since the last one.</summary>
    public void AddGroup(long totalByteSize, long rows, long fileOffset, long totalCompressedSize)
    {
        _groups.Grow(_groupCount + 1, _groupCount);
        _groups[_groupCount++] = new Group { TotalByteSize = totalByteSize, NumRows = rows, FileOffset = fileOffset, TotalCompressedSize = totalCompressedSize };
    }

    /// <summary>Places chunk <paramref name="chunk"/>'s Bloom filter (its header and bitset) in the file.</summary>
    public void PlaceBloomFilter(int chunk, long offset, int length)
    {
        ref Chunk c = ref _chunks[chunk];
        c.BloomOffset = offset;
        c.BloomLength = length;
    }

    /// <summary>Every column index, in layout order: placed at <paramref name="position"/> (written there next).</summary>
    public ReadOnlyMemory<byte> PlaceColumnIndexes(long position)
    {
        for (int i = 0; i < _chunkCount; i++)
        {
            ref Chunk c = ref _chunks[i];
            if (c.ColumnIndexLength >= 0) c.ColumnIndexOffset = position + c.ColumnIndex;
        }
        return _columnIndexes.WrittenMemory;
    }

    /// <summary>Every offset index, in layout order: placed at <paramref name="position"/> (written there next).</summary>
    public ReadOnlyMemory<byte> PlaceOffsetIndexes(long position)
    {
        for (int i = 0; i < _chunkCount; i++)
        {
            ref Chunk c = ref _chunks[i];
            if (c.OffsetIndexLength >= 0) c.OffsetIndexOffset = position + c.OffsetIndex;
        }
        return _offsetIndexes.WrittenMemory;
    }

    /// <summary>Writes the footer (<c>FileMetaData</c>, as <see cref="FileMetaData.Write"/> writes it); every row group holds <paramref name="columns"/> chunks.</summary>
    public void WriteFooter(ThriftCompactWriter w, SchemaElement[] schema, long rows, int columns, KeyValue[]? keyValues, string createdBy)
    {
        w.BeginStruct();
        w.I32Field(1, 2); // version
        w.BeginListField(2, ThriftType.Struct, schema.Length);
        foreach (SchemaElement e in schema) e.Write(w);
        w.I64Field(3, rows);
        w.BeginListField(4, ThriftType.Struct, _groupCount);
        ReadOnlySpan<byte> metadata = _metadata.WrittenSpan;
        for (int g = 0; g < _groupCount; g++)
        {
            Group group = _groups[g];
            w.BeginStruct();
            w.BeginListField(1, ThriftType.Struct, columns);
            for (int i = g * columns; i < (g + 1) * columns; i++) WriteChunk(w, in _chunks[i], metadata);
            w.I64Field(2, group.TotalByteSize);
            w.I64Field(3, group.NumRows);
            w.I64Field(5, group.FileOffset);
            w.I64Field(6, group.TotalCompressedSize);
            w.I16Field(7, (short)Math.Min(g, short.MaxValue)); // ordinal
            w.EndStruct();
        }
        if (keyValues is not null) KeyValue.WriteList(w, 5, keyValues);
        w.StringField(6, createdBy);
        w.BeginListField(7, ThriftType.Struct, columns);
        for (int c = 0; c < columns; c++) UnionOfEmptyStructs.Write(w, (short)ColumnOrder.TypeDefinedOrder);
        w.EndStruct();

        static void WriteChunk(ThriftCompactWriter w, in Chunk c, ReadOnlySpan<byte> metadata)
        {
            w.BeginStruct();
            w.I64Field(2, c.FileOffset);
            w.BeginStructField(3);
            w.WriteFields(metadata.Slice(c.Metadata, c.Head), c.HeadLast);
            if (c.Bloom)
            {
                w.I64Field(14, c.BloomOffset);
                w.I32Field(15, c.BloomLength);
            }
            w.WriteFields(metadata.Slice(c.Metadata + c.Head, c.Tail), c.TailLast);
            w.EndStruct();
            if (c.OffsetIndexLength >= 0)
            {
                w.I64Field(4, c.OffsetIndexOffset);
                w.I32Field(5, c.OffsetIndexLength);
            }
            if (c.ColumnIndexLength >= 0)
            {
                w.I64Field(6, c.ColumnIndexOffset);
                w.I32Field(7, c.ColumnIndexLength);
            }
            w.EndStruct();
        }
    }

    /// <summary>Returns the native memory to the pool. Safe to repeat.</summary>
    public void Dispose()
    {
        _metadata.Dispose();
        _columnIndexes.Dispose();
        _offsetIndexes.Dispose();
        _chunks.Dispose();
        _groups.Dispose();
        _chunkCount = _groupCount = 0;
    }
}
