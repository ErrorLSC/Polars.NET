using System.Buffers.Binary;
using Kaminari.Sazanami.Encodings;
using Kaminari.Seseragi.Files.Parquet.Encodings;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Thrift;

namespace Seseragi.Files.Tests.Parquet;

/// <summary>
/// Builds small Parquet files from a schema and, per leaf, its levels and values, written as given (uncompressed v1
/// data pages: one per leaf, or more after an optional dictionary page): for layouts and damage no writer produces.
/// </summary>
internal static class HandBuiltParquet
{
    /// <summary>
    /// A leaf's first page: levels (empty when its maximum is 0), values (PLAIN, or <see cref="Encoding"/>), and how
    /// levels are encoded; optionally a dictionary page before it (PLAIN values) and further data pages after it.
    /// </summary>
    public sealed record Column(string[] Path, ParquetType Type, int MaxDefinition, int MaxRepetition, byte[] Definitions, byte[] Repetitions, byte[] Values,
        ParquetEncoding LevelEncoding = ParquetEncoding.Rle, long? FooterValues = null, long? FooterUncompressedSize = null,
        ParquetEncoding Encoding = ParquetEncoding.Plain, Dictionary? Dictionary = null, DataPage[]? MorePages = null);

    /// <summary>A dictionary page: <paramref name="Count"/> PLAIN values.</summary>
    public sealed record Dictionary(int Count, byte[] Values);

    /// <summary>A further data page of a leaf.</summary>
    public sealed record DataPage(byte[] Definitions, byte[] Repetitions, byte[] Values, ParquetEncoding Encoding = ParquetEncoding.Plain);

    public static byte[] Build(SchemaElement[] schema, long rows, params Column[] leaves)
    {
        var file = new MemoryStream();
        file.Write("PAR1"u8);
        List<ColumnChunk> chunks = [];
        foreach (Column leaf in leaves)
        {
            long offset = file.Position;
            if (leaf.Dictionary is { } dictionary)
            {
                var header = new ThriftCompactWriter();
                new PageHeader
                {
                    Type = PageType.DictionaryPage,
                    UncompressedPageSize = dictionary.Values.Length,
                    CompressedPageSize = dictionary.Values.Length,
                    DictionaryPageHeader = new DictionaryPageHeader { NumValues = dictionary.Count, Encoding = ParquetEncoding.Plain },
                }.Write(header);
                file.Write(header.WrittenSpan);
                file.Write(dictionary.Values);
            }
            long dataOffset = file.Position;
            long count = 0;
            foreach (DataPage data in (DataPage[])[new(leaf.Definitions, leaf.Repetitions, leaf.Values, leaf.Encoding), .. leaf.MorePages ?? []])
            {
                int values = Math.Max(data.Definitions.Length, data.Repetitions.Length);
                if (values == 0) values = (int)rows;
                count += values;
                var body = new MemoryStream();
                if (leaf.MaxRepetition > 0) WriteLevels(body, data.Repetitions, leaf.MaxRepetition, leaf.LevelEncoding);
                if (leaf.MaxDefinition > 0) WriteLevels(body, data.Definitions, leaf.MaxDefinition, leaf.LevelEncoding);
                body.Write(data.Values);
                byte[] page = body.ToArray();
                var header = new ThriftCompactWriter();
                new PageHeader
                {
                    Type = PageType.DataPage,
                    UncompressedPageSize = page.Length,
                    CompressedPageSize = page.Length,
                    DataPageHeader = new DataPageHeader
                    {
                        NumValues = values,
                        Encoding = data.Encoding,
                        DefinitionLevelEncoding = leaf.LevelEncoding,
                        RepetitionLevelEncoding = leaf.LevelEncoding,
                    },
                }.Write(header);
                file.Write(header.WrittenSpan);
                file.Write(page);
            }
            chunks.Add(new ColumnChunk
            {
                FileOffset = offset,
                MetaData = new ColumnMetaData
                {
                    Type = leaf.Type,
                    Encodings = leaf.Dictionary is null ? [ParquetEncoding.Plain, leaf.LevelEncoding] : [ParquetEncoding.Plain, ParquetEncoding.RleDictionary, leaf.LevelEncoding],
                    PathInSchema = leaf.Path,
                    Codec = CompressionCodec.Uncompressed,
                    NumValues = leaf.FooterValues ?? count,
                    TotalUncompressedSize = leaf.FooterUncompressedSize ?? file.Position - offset,
                    TotalCompressedSize = file.Position - offset,
                    DataPageOffset = dataOffset,
                    DictionaryPageOffset = leaf.Dictionary is null ? null : offset,
                },
            });
        }
        var meta = new FileMetaData
        {
            Version = 1,
            Schema = schema,
            NumRows = rows,
            RowGroups = [new RowGroup { Columns = [.. chunks], TotalByteSize = file.Position, NumRows = rows }],
        };
        var footer = new ThriftCompactWriter();
        meta.Write(footer);
        file.Write(footer.WrittenSpan);
        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, footer.WrittenSpan.Length);
        file.Write(length);
        file.Write("PAR1"u8);
        return file.ToArray();
    }

    /// <summary>Levels as RLE runs of one value each (after a 4-byte length), or BIT_PACKED (most significant bit first).</summary>
    private static void WriteLevels(Stream stream, byte[] levels, int max, ParquetEncoding encoding)
    {
        int bits = 32 - System.Numerics.BitOperations.LeadingZeroCount((uint)max);
        if (encoding == ParquetEncoding.BitPacked)
        {
            var packed = new byte[BitUnpacking.Bytes(levels.Length, bits)];
            long bit = 0;
            foreach (byte level in levels)
            {
                for (int k = bits - 1; k >= 0; k--, bit++)
                {
                    if (((level >> k) & 1) != 0) packed[bit >> 3] |= (byte)(0x80 >> (int)(bit & 7));
                }
            }
            stream.Write(packed);
            return;
        }
        var runs = new MemoryStream();
        foreach (byte level in levels)
        {
            runs.WriteByte(1 << 1);
            for (int b = 0; b < (bits + 7) / 8; b++) runs.WriteByte(b == 0 ? level : (byte)0);
        }
        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, (int)runs.Length);
        stream.Write(length);
        stream.Write(runs.ToArray());
    }

    /// <summary>RLE_DICTIONARY values: the bit width, then one RLE run per index.</summary>
    public static byte[] Indices(int bitWidth, params int[] indices)
    {
        List<byte> data = [(byte)bitWidth];
        foreach (int index in indices)
        {
            data.Add(1 << 1);
            for (int b = 0; b < (bitWidth + 7) / 8; b++) data.Add((byte)(index >> (8 * b)));
        }
        return [.. data];
    }

    public static byte[] Int32s(params int[] values) => [.. values.SelectMany(BitConverter.GetBytes)];

    public static byte[] Strings(params string[] values) =>
        [.. values.SelectMany(v => (byte[])[.. BitConverter.GetBytes(System.Text.Encoding.UTF8.GetByteCount(v)), .. System.Text.Encoding.UTF8.GetBytes(v)])];

    public static SchemaElement Group(string name, int children, FieldRepetition? repetition = FieldRepetition.Optional, ConvertedType? converted = null) =>
        new() { Name = name, NumChildren = children, Repetition = repetition, ConvertedType = converted };

    public static SchemaElement Leaf(string name, ParquetType type, FieldRepetition repetition, ConvertedType? converted = null) =>
        new() { Name = name, Type = type, Repetition = repetition, ConvertedType = converted };
}
