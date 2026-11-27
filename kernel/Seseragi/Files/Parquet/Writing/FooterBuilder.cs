#pragma warning disable CS1573 // parameters documented where they need it
using System.Buffers;
using Kaminari.Sazanami.Buffers;
using Kaminari.Seseragi.Files.Parquet.Encryption;
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
    private List<ChunkCrypto>? _crypto; // an encrypted file: how each chunk is encrypted

    /// <summary>
    /// How a chunk of an encrypted file is encrypted: its <c>crypto_metadata</c> (null: written in plain), whether its
    /// metadata goes into <c>encrypted_column_metadata</c>, and (a plaintext footer) its metadata without statistics in
    /// <c>_metadata</c>, as the full one is kept.
    /// </summary>
    private readonly record struct ChunkCrypto(ColumnCryptoMetaData? Crypto, bool Separate, int Stripped, int StrippedHead, int StrippedTail,
        short StrippedHeadLast, short StrippedTailLast);

    /// <summary>The row groups closed so far (the ordinal of the one being laid out).</summary>
    public int GroupCount => _groupCount;

    /// <summary>Keeps how each chunk is encrypted from now on (before the first chunk).</summary>
    public void EnableEncryption() => _crypto = [];

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
    /// <param name="encryptor">The chunk's encryptor (an encrypted column): its page indexes are encrypted with it.</param>
    /// <param name="crypto">The chunk's <c>crypto_metadata</c> (an encrypted column).</param>
    /// <param name="separate">Whether its metadata goes into <c>encrypted_column_metadata</c>.</param>
    /// <param name="plaintextFooter">Whether the footer stays in plain: the chunk's metadata there has no statistics.</param>
    public int AddChunk(long fileOffset, ColumnMetaData meta, bool bloom, ColumnIndex? columnIndex, OffsetIndex? offsetIndex, long dataPages,
        ColumnEncryptor? encryptor = null, ColumnCryptoMetaData? crypto = null, bool separate = false, bool plaintextFooter = false)
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
        if (_crypto is not null)
        {
            ChunkCrypto how = new(crypto, separate, -1, 0, 0, 0, 0);
            if (crypto is not null && plaintextFooter)
            {
                w = ThriftCompactWriter.ThreadScratch();
                ColumnMetaData stripped = meta.WithoutStatistics();
                stripped.WriteHead(w);
                int strippedHead = w.WrittenSpan.Length;
                short strippedHeadLast = w.LastField;
                if (bloom) w.AfterField(15);
                stripped.WriteTail(w);
                how = how with
                {
                    Stripped = _metadata.WrittenCount, StrippedHead = strippedHead, StrippedTail = w.WrittenSpan.Length - strippedHead,
                    StrippedHeadLast = strippedHeadLast, StrippedTailLast = w.LastField,
                };
                _metadata.Write(w.WrittenSpan);
            }
            _crypto.Add(how);
        }
        if (columnIndex is not null)
        {
            w = ThriftCompactWriter.ThreadScratch();
            columnIndex.Write(w);
            ReadOnlySpan<byte> stored = encryptor is null ? w.WrittenSpan : encryptor.Encrypt(ParquetModule.ColumnIndex, -1, w.WrittenSpan);
            chunk.ColumnIndex = _columnIndexes.WrittenCount;
            chunk.ColumnIndexLength = stored.Length;
            _columnIndexes.Write(stored);
        }
        if (offsetIndex is not null)
        {
            w = ThriftCompactWriter.ThreadScratch();
            offsetIndex.Write(w, dataPages);
            ReadOnlySpan<byte> stored = encryptor is null ? w.WrittenSpan : encryptor.Encrypt(ParquetModule.OffsetIndex, -1, w.WrittenSpan);
            chunk.OffsetIndex = _offsetIndexes.WrittenCount;
            chunk.OffsetIndexLength = stored.Length;
            _offsetIndexes.Write(stored);
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
    /// <param name="plaintextAlgorithm">An encrypted file's algorithm, kept in a plaintext footer.</param>
    /// <param name="metadataEncryptor">The encryptor of the chunk of (row group, column), for its <c>encrypted_column_metadata</c>.</param>
    public void WriteFooter(ThriftCompactWriter w, SchemaElement[] schema, long rows, int columns, KeyValue[]? keyValues, string createdBy,
        EncryptionAlgorithm? plaintextAlgorithm = null, Func<int, int, ColumnEncryptor?>? metadataEncryptor = null)
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
            for (int i = g * columns; i < (g + 1) * columns; i++)
            {
                if (_crypto is { } crypto && crypto[i].Crypto is not null) WriteEncryptedChunk(w, in _chunks[i], crypto[i], metadata, g, i - g * columns);
                else WriteChunk(w, in _chunks[i], metadata);
            }
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
        if (plaintextAlgorithm is { } algorithm)
        {
            w.BeginStructField(8);
            algorithm.WriteBody(w);
            w.EndStruct();
        }
        w.EndStruct();

        static void WriteChunk(ThriftCompactWriter w, in Chunk c, ReadOnlySpan<byte> metadata)
        {
            w.BeginStruct();
            w.I64Field(2, c.FileOffset);
            w.BeginStructField(3);
            WriteMetadataFields(w, in c, metadata.Slice(c.Metadata, c.Head), c.HeadLast, metadata.Slice(c.Metadata + c.Head, c.Tail), c.TailLast);
            w.EndStruct();
            WriteIndexFields(w, in c);
            w.EndStruct();
        }

        // An encrypted chunk: its metadata in plain under an encrypted footer and the footer key (uniform), else
        // sealed into encrypted_column_metadata with the column's key, and in a plaintext footer without statistics.
        void WriteEncryptedChunk(ThriftCompactWriter w, in Chunk c, ChunkCrypto how, ReadOnlySpan<byte> metadata, int group, int column)
        {
            ReadOnlySpan<byte> head = metadata.Slice(c.Metadata, c.Head), tail = metadata.Slice(c.Metadata + c.Head, c.Tail);
            w.BeginStruct();
            w.I64Field(2, c.FileOffset);
            if (how.Stripped >= 0)
            {
                w.BeginStructField(3);
                WriteMetadataFields(w, in c, metadata.Slice(how.Stripped, how.StrippedHead), how.StrippedHeadLast,
                    metadata.Slice(how.Stripped + how.StrippedHead, how.StrippedTail), how.StrippedTailLast);
                w.EndStruct();
            }
            else if (!how.Separate)
            {
                w.BeginStructField(3);
                WriteMetadataFields(w, in c, head, c.HeadLast, tail, c.TailLast);
                w.EndStruct();
            }
            WriteIndexFields(w, in c);
            w.BeginStructField(8);
            how.Crypto!.WriteBody(w);
            w.EndStruct();
            if (how.Separate)
            {
                var inner = new ThriftCompactWriter();
                inner.BeginStruct();
                WriteMetadataFields(inner, in c, head, c.HeadLast, tail, c.TailLast);
                inner.EndStruct();
                using ColumnEncryptor encryptor = metadataEncryptor?.Invoke(group, column)
                    ?? throw new InvalidOperationException("An encrypted column's metadata needs its encryptor.");
                w.BinaryField(9, encryptor.Encrypt(ParquetModule.ColumnMetaData, -1, inner.WrittenSpan));
            }
            w.EndStruct();
        }

        static void WriteMetadataFields(ThriftCompactWriter w, in Chunk c, ReadOnlySpan<byte> head, short headLast, ReadOnlySpan<byte> tail, short tailLast)
        {
            w.WriteFields(head, headLast);
            if (c.Bloom)
            {
                w.I64Field(14, c.BloomOffset);
                w.I32Field(15, c.BloomLength);
            }
            w.WriteFields(tail, tailLast);
        }

        static void WriteIndexFields(ThriftCompactWriter w, in Chunk c)
        {
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
        _crypto = null;
    }
}
