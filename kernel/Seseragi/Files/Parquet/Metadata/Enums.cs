namespace Kaminari.Seseragi.Files.Parquet.Metadata;

// The enums of parquet.thrift. Values a newer writer may add are kept as they are (an enum holds any int); the code
// that reads data refuses what it does not know.

/// <summary>Thrift <c>Type</c>: the physical type of a leaf.</summary>
internal enum ParquetType
{
    Boolean = 0,
    Int32 = 1,
    Int64 = 2,
    Int96 = 3,
    Float = 4,
    Double = 5,
    ByteArray = 6,
    FixedLenByteArray = 7,
}

/// <summary>Thrift <c>ConvertedType</c>: the legacy annotations.</summary>
internal enum ConvertedType
{
    Utf8 = 0,
    Map = 1,
    MapKeyValue = 2,
    List = 3,
    Enum = 4,
    Decimal = 5,
    Date = 6,
    TimeMillis = 7,
    TimeMicros = 8,
    TimestampMillis = 9,
    TimestampMicros = 10,
    UInt8 = 11,
    UInt16 = 12,
    UInt32 = 13,
    UInt64 = 14,
    Int8 = 15,
    Int16 = 16,
    Int32 = 17,
    Int64 = 18,
    Json = 19,
    Bson = 20,
    Interval = 21,
}

/// <summary>Thrift <c>FieldRepetitionType</c>.</summary>
internal enum FieldRepetition
{
    Required = 0,
    Optional = 1,
    Repeated = 2,
}

/// <summary>Thrift <c>Encoding</c>.</summary>
internal enum ParquetEncoding
{
    Plain = 0,
    PlainDictionary = 2,
    Rle = 3,
    BitPacked = 4,
    DeltaBinaryPacked = 5,
    DeltaLengthByteArray = 6,
    DeltaByteArray = 7,
    RleDictionary = 8,
    ByteStreamSplit = 9,
    Alp = 10,
}

/// <summary>Thrift <c>CompressionCodec</c>.</summary>
internal enum CompressionCodec
{
    Uncompressed = 0,
    Snappy = 1,
    Gzip = 2,
    Lzo = 3,
    Brotli = 4,
    Lz4 = 5,
    Zstd = 6,
    Lz4Raw = 7,
}

/// <summary>Thrift <c>PageType</c>.</summary>
internal enum PageType
{
    DataPage = 0,
    IndexPage = 1,
    DictionaryPage = 2,
    DataPageV2 = 3,
}

/// <summary>Thrift <c>BoundaryOrder</c>: how the pages of a column index are ordered.</summary>
internal enum BoundaryOrder
{
    Unordered = 0,
    Ascending = 1,
    Descending = 2,
}

/// <summary>Thrift <c>EdgeInterpolationAlgorithm</c> of a geography.</summary>
internal enum EdgeInterpolationAlgorithm
{
    Spherical = 0,
    Vincenty = 1,
    Thomas = 2,
    Andoyer = 3,
    Karney = 4,
}

/// <summary>The members of the Thrift <c>LogicalType</c> union, by field id.</summary>
internal enum LogicalTypeKind
{
    String = 1,
    Map = 2,
    List = 3,
    Enum = 4,
    Decimal = 5,
    Date = 6,
    Time = 7,
    Timestamp = 8,
    Integer = 10,
    Unknown = 11,
    Json = 12,
    Bson = 13,
    Uuid = 14,
    Float16 = 15,
    Variant = 16,
    Geometry = 17,
    Geography = 18,
    File = 19,
    FiniteVector = 20,
}

/// <summary>The members of the Thrift <c>ColumnOrder</c> union, by field id.</summary>
internal enum ColumnOrder
{
    TypeDefinedOrder = 1,
    Ieee754TotalOrder = 2,
    Int96TimestampOrder = 3,
}
