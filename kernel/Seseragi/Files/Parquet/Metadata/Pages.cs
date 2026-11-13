using Kaminari.Seseragi.Files.Parquet.Thrift;
using static Kaminari.Seseragi.Files.Parquet.Metadata.ThriftModel;

namespace Kaminari.Seseragi.Files.Parquet.Metadata;

// The page-level structs of parquet.thrift: page headers, bloom filter headers and the page index.

/// <summary>Thrift <c>PageHeader</c>.</summary>
internal sealed class PageHeader
{
    public required PageType Type { get; init; }
    public required int UncompressedPageSize { get; init; }
    public required int CompressedPageSize { get; init; }
    public int? Crc { get; init; }
    public DataPageHeader? DataPageHeader { get; init; }
    public bool HasIndexPageHeader { get; init; }
    public DictionaryPageHeader? DictionaryPageHeader { get; init; }
    public DataPageHeaderV2? DataPageHeaderV2 { get; init; }

    /// <summary>Reads a page header from the start of <paramref name="data"/>; <paramref name="length"/> is its size in bytes.</summary>
    public static PageHeader Parse(ReadOnlySpan<byte> data, out int length)
    {
        var reader = new ThriftCompactReader(data);
        PageHeader header = Read(ref reader);
        length = reader.Position;
        return header;
    }

    public static PageHeader Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        int? type = null, uncompressed = null, compressed = null, crc = null;
        DataPageHeader? data = null;
        bool index = false;
        DictionaryPageHeader? dictionary = null;
        DataPageHeaderV2? dataV2 = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.I32: type = r.ReadI32(); break;
                case 2 when t == ThriftType.I32: uncompressed = r.ReadI32(); break;
                case 3 when t == ThriftType.I32: compressed = r.ReadI32(); break;
                case 4 when t == ThriftType.I32: crc = r.ReadI32(); break;
                case 5 when t == ThriftType.Struct: data = DataPageHeader.Read(ref r); break;
                case 6 when t == ThriftType.Struct:
                    r.Skip(t);
                    index = true;
                    break;
                case 7 when t == ThriftType.Struct: dictionary = DictionaryPageHeader.Read(ref r); break;
                case 8 when t == ThriftType.Struct: dataV2 = DataPageHeaderV2.Read(ref r); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new PageHeader
        {
            Type = (PageType)(type ?? throw Missing("PageHeader.type")),
            UncompressedPageSize = uncompressed ?? throw Missing("PageHeader.uncompressed_page_size"),
            CompressedPageSize = compressed ?? throw Missing("PageHeader.compressed_page_size"),
            Crc = crc,
            DataPageHeader = data,
            HasIndexPageHeader = index,
            DictionaryPageHeader = dictionary,
            DataPageHeaderV2 = dataV2,
        };
    }

    public void Write(ThriftCompactWriter w)
    {
        w.BeginStruct();
        w.I32Field(1, (int)Type);
        w.I32Field(2, UncompressedPageSize);
        w.I32Field(3, CompressedPageSize);
        if (Crc is { } crc) w.I32Field(4, crc);
        if (DataPageHeader is { } data)
        {
            w.BeginStructField(5);
            data.WriteBody(w);
            w.EndStruct();
        }
        if (HasIndexPageHeader) w.EmptyStructField(6);
        if (DictionaryPageHeader is { } dictionary)
        {
            w.BeginStructField(7);
            dictionary.WriteBody(w);
            w.EndStruct();
        }
        if (DataPageHeaderV2 is { } dataV2)
        {
            w.BeginStructField(8);
            dataV2.WriteBody(w);
            w.EndStruct();
        }
        w.EndStruct();
    }
}

/// <summary>Thrift <c>DataPageHeader</c> (data page v1).</summary>
internal sealed class DataPageHeader
{
    public required int NumValues { get; init; }
    public required ParquetEncoding Encoding { get; init; }
    public required ParquetEncoding DefinitionLevelEncoding { get; init; }
    public required ParquetEncoding RepetitionLevelEncoding { get; init; }
    public Statistics? Statistics { get; init; }

    public static DataPageHeader Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        int? numValues = null, encoding = null, definition = null, repetition = null;
        Statistics? statistics = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.I32: numValues = r.ReadI32(); break;
                case 2 when t == ThriftType.I32: encoding = r.ReadI32(); break;
                case 3 when t == ThriftType.I32: definition = r.ReadI32(); break;
                case 4 when t == ThriftType.I32: repetition = r.ReadI32(); break;
                case 5 when t == ThriftType.Struct: statistics = Statistics.Read(ref r); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new DataPageHeader
        {
            NumValues = numValues ?? throw Missing("DataPageHeader.num_values"),
            Encoding = (ParquetEncoding)(encoding ?? throw Missing("DataPageHeader.encoding")),
            DefinitionLevelEncoding = (ParquetEncoding)(definition ?? throw Missing("DataPageHeader.definition_level_encoding")),
            RepetitionLevelEncoding = (ParquetEncoding)(repetition ?? throw Missing("DataPageHeader.repetition_level_encoding")),
            Statistics = statistics,
        };
    }

    public void WriteBody(ThriftCompactWriter w)
    {
        w.I32Field(1, NumValues);
        w.I32Field(2, (int)Encoding);
        w.I32Field(3, (int)DefinitionLevelEncoding);
        w.I32Field(4, (int)RepetitionLevelEncoding);
        if (Statistics is { } statistics)
        {
            w.BeginStructField(5);
            statistics.WriteBody(w);
            w.EndStruct();
        }
    }
}

/// <summary>Thrift <c>DictionaryPageHeader</c>.</summary>
internal sealed class DictionaryPageHeader
{
    public required int NumValues { get; init; }
    public required ParquetEncoding Encoding { get; init; }
    public bool? IsSorted { get; init; }

    public static DictionaryPageHeader Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        int? numValues = null, encoding = null;
        bool? sorted = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.I32: numValues = r.ReadI32(); break;
                case 2 when t == ThriftType.I32: encoding = r.ReadI32(); break;
                case 3 when ThriftCompactReader.IsBool(t): sorted = r.BoolField(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new DictionaryPageHeader
        {
            NumValues = numValues ?? throw Missing("DictionaryPageHeader.num_values"),
            Encoding = (ParquetEncoding)(encoding ?? throw Missing("DictionaryPageHeader.encoding")),
            IsSorted = sorted,
        };
    }

    public void WriteBody(ThriftCompactWriter w)
    {
        w.I32Field(1, NumValues);
        w.I32Field(2, (int)Encoding);
        if (IsSorted is { } sorted) w.BoolField(3, sorted);
    }
}

/// <summary>Thrift <c>DataPageHeaderV2</c>: levels are never compressed and their byte lengths are given.</summary>
internal sealed class DataPageHeaderV2
{
    public required int NumValues { get; init; }
    public required int NumNulls { get; init; }
    public required int NumRows { get; init; }
    public required ParquetEncoding Encoding { get; init; }
    public required int DefinitionLevelsByteLength { get; init; }
    public required int RepetitionLevelsByteLength { get; init; }

    /// <summary>The <c>is_compressed</c> field as written (absent means true).</summary>
    public bool? IsCompressedField { get; init; }
    public Statistics? Statistics { get; init; }

    public bool IsCompressed => IsCompressedField ?? true;

    public static DataPageHeaderV2 Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        int? numValues = null, numNulls = null, numRows = null, encoding = null, definition = null, repetition = null;
        bool? compressed = null;
        Statistics? statistics = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.I32: numValues = r.ReadI32(); break;
                case 2 when t == ThriftType.I32: numNulls = r.ReadI32(); break;
                case 3 when t == ThriftType.I32: numRows = r.ReadI32(); break;
                case 4 when t == ThriftType.I32: encoding = r.ReadI32(); break;
                case 5 when t == ThriftType.I32: definition = r.ReadI32(); break;
                case 6 when t == ThriftType.I32: repetition = r.ReadI32(); break;
                case 7 when ThriftCompactReader.IsBool(t): compressed = r.BoolField(); break;
                case 8 when t == ThriftType.Struct: statistics = Statistics.Read(ref r); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new DataPageHeaderV2
        {
            NumValues = numValues ?? throw Missing("DataPageHeaderV2.num_values"),
            NumNulls = numNulls ?? throw Missing("DataPageHeaderV2.num_nulls"),
            NumRows = numRows ?? throw Missing("DataPageHeaderV2.num_rows"),
            Encoding = (ParquetEncoding)(encoding ?? throw Missing("DataPageHeaderV2.encoding")),
            DefinitionLevelsByteLength = definition ?? throw Missing("DataPageHeaderV2.definition_levels_byte_length"),
            RepetitionLevelsByteLength = repetition ?? throw Missing("DataPageHeaderV2.repetition_levels_byte_length"),
            IsCompressedField = compressed,
            Statistics = statistics,
        };
    }

    public void WriteBody(ThriftCompactWriter w)
    {
        w.I32Field(1, NumValues);
        w.I32Field(2, NumNulls);
        w.I32Field(3, NumRows);
        w.I32Field(4, (int)Encoding);
        w.I32Field(5, DefinitionLevelsByteLength);
        w.I32Field(6, RepetitionLevelsByteLength);
        if (IsCompressedField is { } compressed) w.BoolField(7, compressed);
        if (Statistics is { } statistics)
        {
            w.BeginStructField(8);
            statistics.WriteBody(w);
            w.EndStruct();
        }
    }
}

/// <summary>
/// Thrift <c>BloomFilterHeader</c>; its algorithm, hash and compression unions are kept as the ids of their members
/// (1 is BLOCK, XXHASH and UNCOMPRESSED, the only ones defined).
/// </summary>
internal sealed record BloomFilterHeader(int NumBytes, int Algorithm, int Hash, int Compression)
{
    /// <summary>Reads a bloom filter header from the start of <paramref name="data"/>; the bitset follows its <paramref name="length"/> bytes.</summary>
    public static BloomFilterHeader Parse(ReadOnlySpan<byte> data, out int length)
    {
        var reader = new ThriftCompactReader(data);
        BloomFilterHeader header = Read(ref reader);
        length = reader.Position;
        return header;
    }

    public static BloomFilterHeader Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        int? numBytes = null, algorithm = null, hash = null, compression = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.I32: numBytes = r.ReadI32(); break;
                case 2 when t == ThriftType.Struct: algorithm = UnionOfEmptyStructs.Read(ref r, "BloomFilterAlgorithm"); break;
                case 3 when t == ThriftType.Struct: hash = UnionOfEmptyStructs.Read(ref r, "BloomFilterHash"); break;
                case 4 when t == ThriftType.Struct: compression = UnionOfEmptyStructs.Read(ref r, "BloomFilterCompression"); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new BloomFilterHeader(
            numBytes ?? throw Missing("BloomFilterHeader.numBytes"),
            algorithm ?? throw Missing("BloomFilterHeader.algorithm"),
            hash ?? throw Missing("BloomFilterHeader.hash"),
            compression ?? throw Missing("BloomFilterHeader.compression"));
    }

    public void Write(ThriftCompactWriter w)
    {
        w.BeginStruct();
        w.I32Field(1, NumBytes);
        WriteUnion(w, 2, Algorithm);
        WriteUnion(w, 3, Hash);
        WriteUnion(w, 4, Compression);
        w.EndStruct();

        static void WriteUnion(ThriftCompactWriter w, short id, int member)
        {
            w.BeginStructField(id);
            w.EmptyStructField((short)member);
            w.EndStruct();
        }
    }
}

/// <summary>Thrift <c>PageLocation</c>.</summary>
internal readonly record struct PageLocation(long Offset, int CompressedPageSize, long FirstRowIndex)
{
    public static PageLocation Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        long? offset = null, firstRow = null;
        int? size = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.I64: offset = r.ReadI64(); break;
                case 2 when t == ThriftType.I32: size = r.ReadI32(); break;
                case 3 when t == ThriftType.I64: firstRow = r.ReadI64(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new PageLocation(
            offset ?? throw Missing("PageLocation.offset"),
            size ?? throw Missing("PageLocation.compressed_page_size"),
            firstRow ?? throw Missing("PageLocation.first_row_index"));
    }

    public void Write(ThriftCompactWriter w)
    {
        w.BeginStruct();
        w.I64Field(1, Offset);
        w.I32Field(2, CompressedPageSize);
        w.I64Field(3, FirstRowIndex);
        w.EndStruct();
    }
}

/// <summary>Thrift <c>OffsetIndex</c>: where each page of a column chunk starts.</summary>
internal sealed class OffsetIndex
{
    public required PageLocation[] PageLocations { get; init; }
    public long[]? UnencodedByteArrayDataBytes { get; init; }

    public static OffsetIndex Parse(ReadOnlySpan<byte> data)
    {
        var reader = new ThriftCompactReader(data);
        return Read(ref reader);
    }

    public static OffsetIndex Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        PageLocation[]? locations = null;
        long[]? unencoded = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.List: locations = r.ReadStructList(PageLocation.Read); break;
                case 2 when t == ThriftType.List: unencoded = r.ReadI64List(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new OffsetIndex { PageLocations = locations ?? throw Missing("OffsetIndex.page_locations"), UnencodedByteArrayDataBytes = unencoded };
    }

    public void Write(ThriftCompactWriter w)
    {
        w.BeginStruct();
        w.BeginListField(1, ThriftType.Struct, PageLocations.Length);
        foreach (PageLocation location in PageLocations) location.Write(w);
        if (UnencodedByteArrayDataBytes is { } unencoded) SizeStatistics.WriteI64List(w, 2, unencoded);
        w.EndStruct();
    }
}

/// <summary>Thrift <c>ColumnIndex</c>: per-page bounds and null counts of a column chunk.</summary>
internal sealed class ColumnIndex
{
    public required bool[] NullPages { get; init; }
    public required byte[][] MinValues { get; init; }
    public required byte[][] MaxValues { get; init; }
    public required BoundaryOrder BoundaryOrder { get; init; }
    public long[]? NullCounts { get; init; }
    public long[]? RepetitionLevelHistograms { get; init; }
    public long[]? DefinitionLevelHistograms { get; init; }
    public long[]? NanCounts { get; init; }

    public static ColumnIndex Parse(ReadOnlySpan<byte> data)
    {
        var reader = new ThriftCompactReader(data);
        return Read(ref reader);
    }

    public static ColumnIndex Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        bool[]? nullPages = null;
        byte[][]? mins = null, maxs = null;
        int? order = null;
        long[]? nullCounts = null, repetition = null, definition = null, nanCounts = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.List: nullPages = r.ReadBoolList(); break;
                case 2 when t == ThriftType.List: mins = r.ReadBinaryList(); break;
                case 3 when t == ThriftType.List: maxs = r.ReadBinaryList(); break;
                case 4 when t == ThriftType.I32: order = r.ReadI32(); break;
                case 5 when t == ThriftType.List: nullCounts = r.ReadI64List(); break;
                case 6 when t == ThriftType.List: repetition = r.ReadI64List(); break;
                case 7 when t == ThriftType.List: definition = r.ReadI64List(); break;
                case 8 when t == ThriftType.List: nanCounts = r.ReadI64List(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new ColumnIndex
        {
            NullPages = nullPages ?? throw Missing("ColumnIndex.null_pages"),
            MinValues = mins ?? throw Missing("ColumnIndex.min_values"),
            MaxValues = maxs ?? throw Missing("ColumnIndex.max_values"),
            BoundaryOrder = (BoundaryOrder)(order ?? throw Missing("ColumnIndex.boundary_order")),
            NullCounts = nullCounts,
            RepetitionLevelHistograms = repetition,
            DefinitionLevelHistograms = definition,
            NanCounts = nanCounts,
        };
    }

    public void Write(ThriftCompactWriter w)
    {
        w.BeginStruct();
        w.BeginListField(1, ThriftType.BoolTrue, NullPages.Length);
        foreach (bool page in NullPages) w.WriteBool(page);
        WriteBinaryList(w, 2, MinValues);
        WriteBinaryList(w, 3, MaxValues);
        w.I32Field(4, (int)BoundaryOrder);
        if (NullCounts is { } nullCounts) SizeStatistics.WriteI64List(w, 5, nullCounts);
        if (RepetitionLevelHistograms is { } repetition) SizeStatistics.WriteI64List(w, 6, repetition);
        if (DefinitionLevelHistograms is { } definition) SizeStatistics.WriteI64List(w, 7, definition);
        if (NanCounts is { } nanCounts) SizeStatistics.WriteI64List(w, 8, nanCounts);
        w.EndStruct();

        static void WriteBinaryList(ThriftCompactWriter w, short id, byte[][] values)
        {
            w.BeginListField(id, ThriftType.Binary, values.Length);
            foreach (byte[] v in values) w.WriteBinary(v);
        }
    }
}
