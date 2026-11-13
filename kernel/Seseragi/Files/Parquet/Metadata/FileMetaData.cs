using Kaminari.Seseragi.Files.Parquet.Thrift;
using static Kaminari.Seseragi.Files.Parquet.Metadata.ThriftModel;

namespace Kaminari.Seseragi.Files.Parquet.Metadata;

// The footer structs of parquet.thrift (apache/parquet-format), each read from and written to the Thrift compact
// protocol. Field ids follow parquet.thrift; unknown fields are skipped on read (and so not written back); a field of
// an unexpected wire type is skipped too, as the generated Thrift code of the reference readers does. Missing
// required fields fail with ParquetFormatException.

/// <summary>Thrift <c>FileMetaData</c>: the footer.</summary>
internal sealed class FileMetaData
{
    public required int Version { get; init; }
    public required SchemaElement[] Schema { get; init; }
    public required long NumRows { get; init; }
    public required RowGroup[] RowGroups { get; init; }
    public KeyValue[]? KeyValueMetadata { get; init; }
    public string? CreatedBy { get; init; }
    public ColumnOrder[]? ColumnOrders { get; init; }
    public EncryptionAlgorithm? EncryptionAlgorithm { get; init; }
    public byte[]? FooterSigningKeyMetadata { get; init; }

    /// <summary>
    /// Reads a footer. Bytes after the struct are left alone: a signed plaintext footer (encryption) has its
    /// 28-byte signature there.
    /// </summary>
    public static FileMetaData Parse(ReadOnlySpan<byte> footer)
    {
        var reader = new ThriftCompactReader(footer);
        return Read(ref reader);
    }

    public static FileMetaData Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        int? version = null;
        SchemaElement[]? schema = null;
        long? numRows = null;
        RowGroup[]? rowGroups = null;
        KeyValue[]? keyValues = null;
        string? createdBy = null;
        ColumnOrder[]? orders = null;
        EncryptionAlgorithm? encryption = null;
        byte[]? signing = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.I32: version = r.ReadI32(); break;
                case 2 when t == ThriftType.List: schema = r.ReadStructList(SchemaElement.Read); break;
                case 3 when t == ThriftType.I64: numRows = r.ReadI64(); break;
                case 4 when t == ThriftType.List: rowGroups = r.ReadStructList(RowGroup.Read); break;
                case 5 when t == ThriftType.List: keyValues = r.ReadStructList(KeyValue.Read); break;
                case 6 when t == ThriftType.Binary: createdBy = r.ReadString(); break;
                case 7 when t == ThriftType.List: orders = r.ReadStructList(ReadColumnOrder); break;
                case 8 when t == ThriftType.Struct: encryption = EncryptionAlgorithm.Read(ref r); break;
                case 9 when t == ThriftType.Binary: signing = r.ReadBytes(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new FileMetaData
        {
            Version = version ?? throw Missing("FileMetaData.version"),
            Schema = schema ?? throw Missing("FileMetaData.schema"),
            NumRows = numRows ?? throw Missing("FileMetaData.num_rows"),
            RowGroups = rowGroups ?? throw Missing("FileMetaData.row_groups"),
            KeyValueMetadata = keyValues,
            CreatedBy = createdBy,
            ColumnOrders = orders,
            EncryptionAlgorithm = encryption,
            FooterSigningKeyMetadata = signing,
        };
    }

    /// <summary>A <c>ColumnOrder</c> union: the id of its member (unknown members are kept by id).</summary>
    private static ColumnOrder ReadColumnOrder(ref ThriftCompactReader r) => (ColumnOrder)UnionOfEmptyStructs.Read(ref r, "ColumnOrder");

    public void Write(ThriftCompactWriter w)
    {
        w.BeginStruct();
        w.I32Field(1, Version);
        w.BeginListField(2, ThriftType.Struct, Schema.Length);
        foreach (SchemaElement e in Schema) e.Write(w);
        w.I64Field(3, NumRows);
        w.BeginListField(4, ThriftType.Struct, RowGroups.Length);
        foreach (RowGroup g in RowGroups) g.Write(w);
        if (KeyValueMetadata is { } kv) KeyValue.WriteList(w, 5, kv);
        if (CreatedBy is { } createdBy) w.StringField(6, createdBy);
        if (ColumnOrders is { } orders)
        {
            w.BeginListField(7, ThriftType.Struct, orders.Length);
            foreach (ColumnOrder o in orders) UnionOfEmptyStructs.Write(w, (short)o);
        }
        if (EncryptionAlgorithm is { } encryption)
        {
            w.BeginStructField(8);
            encryption.WriteBody(w);
            w.EndStruct();
        }
        if (FooterSigningKeyMetadata is { } signing) w.BinaryField(9, signing);
        w.EndStruct();
    }
}

/// <summary>A union whose members are all empty structs, read and written as the id of its member.</summary>
internal static class UnionOfEmptyStructs
{
    public static int Read(ref ThriftCompactReader r, string name)
    {
        r.EnterStruct();
        int member = 0;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            if (t == ThriftType.Struct && member == 0) member = id;
            r.Skip(t);
        }
        r.ExitStruct();
        return member != 0 ? member : throw new ParquetFormatException($"A {name} union in the Parquet metadata has no member.");
    }

    public static void Write(ThriftCompactWriter w, short member)
    {
        w.BeginStruct();
        w.EmptyStructField(member);
        w.EndStruct();
    }
}

/// <summary>Thrift <c>KeyValue</c>.</summary>
internal sealed record KeyValue(string Key, string? Value)
{
    public static KeyValue Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        string? key = null, value = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.Binary: key = r.ReadString(); break;
                case 2 when t == ThriftType.Binary: value = r.ReadString(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new KeyValue(key ?? throw Missing("KeyValue.key"), value);
    }

    public static void WriteList(ThriftCompactWriter w, short id, KeyValue[] list)
    {
        w.BeginListField(id, ThriftType.Struct, list.Length);
        foreach (KeyValue kv in list)
        {
            w.BeginStruct();
            w.StringField(1, kv.Key);
            if (kv.Value is { } value) w.StringField(2, value);
            w.EndStruct();
        }
    }
}

/// <summary>Thrift <c>SchemaElement</c>: one node of the depth-first flattened schema tree.</summary>
internal sealed class SchemaElement
{
    public ParquetType? Type { get; init; }
    public int? TypeLength { get; init; }
    public FieldRepetition? Repetition { get; init; }
    public required string Name { get; init; }
    public int? NumChildren { get; init; }
    public ConvertedType? ConvertedType { get; init; }
    public int? Scale { get; init; }
    public int? Precision { get; init; }
    public int? FieldId { get; init; }
    public LogicalType? LogicalType { get; init; }

    public static SchemaElement Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        ParquetType? type = null;
        int? typeLength = null, numChildren = null, scale = null, precision = null, fieldId = null;
        FieldRepetition? repetition = null;
        string? name = null;
        ConvertedType? converted = null;
        LogicalType? logical = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.I32: type = (ParquetType)r.ReadI32(); break;
                case 2 when t == ThriftType.I32: typeLength = r.ReadI32(); break;
                case 3 when t == ThriftType.I32: repetition = (FieldRepetition)r.ReadI32(); break;
                case 4 when t == ThriftType.Binary: name = r.ReadString(); break;
                case 5 when t == ThriftType.I32: numChildren = r.ReadI32(); break;
                case 6 when t == ThriftType.I32: converted = (ConvertedType)r.ReadI32(); break;
                case 7 when t == ThriftType.I32: scale = r.ReadI32(); break;
                case 8 when t == ThriftType.I32: precision = r.ReadI32(); break;
                case 9 when t == ThriftType.I32: fieldId = r.ReadI32(); break;
                case 10 when t == ThriftType.Struct: logical = LogicalType.Read(ref r); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new SchemaElement
        {
            Type = type,
            TypeLength = typeLength,
            Repetition = repetition,
            Name = name ?? throw Missing("SchemaElement.name"),
            NumChildren = numChildren,
            ConvertedType = converted,
            Scale = scale,
            Precision = precision,
            FieldId = fieldId,
            LogicalType = logical,
        };
    }

    public void Write(ThriftCompactWriter w)
    {
        w.BeginStruct();
        if (Type is { } type) w.I32Field(1, (int)type);
        if (TypeLength is { } typeLength) w.I32Field(2, typeLength);
        if (Repetition is { } repetition) w.I32Field(3, (int)repetition);
        w.StringField(4, Name);
        if (NumChildren is { } numChildren) w.I32Field(5, numChildren);
        if (ConvertedType is { } converted) w.I32Field(6, (int)converted);
        if (Scale is { } scale) w.I32Field(7, scale);
        if (Precision is { } precision) w.I32Field(8, precision);
        if (FieldId is { } fieldId) w.I32Field(9, fieldId);
        if (LogicalType is { } logical)
        {
            w.BeginStructField(10);
            logical.WriteBody(w);
            w.EndStruct();
        }
        w.EndStruct();
    }
}

/// <summary>
/// Thrift <c>LogicalType</c>, a union: <see cref="Kind"/> is the id of its member and the other properties are the
/// member's fields, where it has any. A member this model does not know keeps its id and nothing else.
/// </summary>
internal sealed record LogicalType(LogicalTypeKind Kind)
{
    /// <summary>DECIMAL scale and precision.</summary>
    public int Scale { get; init; }
    public int Precision { get; init; }

    /// <summary>TIME and TIMESTAMP: whether the value is adjusted to UTC, and its unit (null for a unit this model does not know).</summary>
    public bool IsAdjustedToUtc { get; init; }
    public ParquetTimeUnit? Unit { get; init; }

    /// <summary>The id of the <c>TimeUnit</c> member, kept to write an unknown unit back.</summary>
    public short UnitMember { get; init; }

    /// <summary>INTEGER bit width and signedness.</summary>
    public sbyte BitWidth { get; init; }
    public bool IsSigned { get; init; }

    /// <summary>VARIANT specification version, when written.</summary>
    public sbyte? SpecificationVersion { get; init; }

    /// <summary>GEOMETRY and GEOGRAPHY coordinate reference system, when written.</summary>
    public string? Crs { get; init; }

    /// <summary>GEOGRAPHY edge interpolation, when written.</summary>
    public EdgeInterpolationAlgorithm? Algorithm { get; init; }

    /// <summary>FINITE_VECTOR element count.</summary>
    public int NumElements { get; init; }

    /// <summary>Reads the union; null when it has no member.</summary>
    public static LogicalType? Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        LogicalType? result = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            if (t != ThriftType.Struct || result is not null)
            {
                r.Skip(t);
                continue;
            }
            result = (LogicalTypeKind)id switch
            {
                LogicalTypeKind.Decimal => ReadDecimal(ref r),
                LogicalTypeKind.Time or LogicalTypeKind.Timestamp => ReadTime(ref r, (LogicalTypeKind)id),
                LogicalTypeKind.Integer => ReadInteger(ref r),
                LogicalTypeKind.Variant => ReadVariant(ref r),
                LogicalTypeKind.Geometry or LogicalTypeKind.Geography => ReadGeo(ref r, (LogicalTypeKind)id),
                LogicalTypeKind.FiniteVector => ReadFiniteVector(ref r),
                _ => SkipMember(ref r, (LogicalTypeKind)id),
            };
        }
        r.ExitStruct();
        return result;
    }

    private static LogicalType SkipMember(ref ThriftCompactReader r, LogicalTypeKind kind)
    {
        r.Skip(ThriftType.Struct);
        return new LogicalType(kind);
    }

    private static LogicalType ReadDecimal(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        int? scale = null, precision = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.I32: scale = r.ReadI32(); break;
                case 2 when t == ThriftType.I32: precision = r.ReadI32(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new LogicalType(LogicalTypeKind.Decimal)
        {
            Scale = scale ?? throw Missing("DecimalType.scale"),
            Precision = precision ?? throw Missing("DecimalType.precision"),
        };
    }

    private static LogicalType ReadTime(ref ThriftCompactReader r, LogicalTypeKind kind)
    {
        r.EnterStruct();
        bool? adjusted = null;
        short unit = 0;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when ThriftCompactReader.IsBool(t): adjusted = r.BoolField(); break;
                case 2 when t == ThriftType.Struct: unit = (short)UnionOfEmptyStructs.Read(ref r, "TimeUnit"); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        string name = kind == LogicalTypeKind.Time ? "TimeType" : "TimestampType";
        if (unit == 0) throw Missing($"{name}.unit");
        return new LogicalType(kind)
        {
            IsAdjustedToUtc = adjusted ?? throw Missing($"{name}.isAdjustedToUTC"),
            UnitMember = unit,
            Unit = unit switch
            {
                1 => ParquetTimeUnit.Millis,
                2 => ParquetTimeUnit.Micros,
                3 => ParquetTimeUnit.Nanos,
                _ => null,
            },
        };
    }

    private static LogicalType ReadInteger(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        sbyte? bitWidth = null;
        bool? signed = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.Byte: bitWidth = r.ReadI8(); break;
                case 2 when ThriftCompactReader.IsBool(t): signed = r.BoolField(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new LogicalType(LogicalTypeKind.Integer)
        {
            BitWidth = bitWidth ?? throw Missing("IntType.bitWidth"),
            IsSigned = signed ?? throw Missing("IntType.isSigned"),
        };
    }

    private static LogicalType ReadVariant(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        sbyte? version = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            if (id == 1 && t == ThriftType.Byte) version = r.ReadI8();
            else r.Skip(t);
        }
        r.ExitStruct();
        return new LogicalType(LogicalTypeKind.Variant) { SpecificationVersion = version };
    }

    private static LogicalType ReadGeo(ref ThriftCompactReader r, LogicalTypeKind kind)
    {
        r.EnterStruct();
        string? crs = null;
        EdgeInterpolationAlgorithm? algorithm = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.Binary: crs = r.ReadString(); break;
                case 2 when t == ThriftType.I32 && kind == LogicalTypeKind.Geography: algorithm = (EdgeInterpolationAlgorithm)r.ReadI32(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new LogicalType(kind) { Crs = crs, Algorithm = algorithm };
    }

    private static LogicalType ReadFiniteVector(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        int? count = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            if (id == 1 && t == ThriftType.I32) count = r.ReadI32();
            else r.Skip(t);
        }
        r.ExitStruct();
        return new LogicalType(LogicalTypeKind.FiniteVector) { NumElements = count ?? throw Missing("FiniteVectorType.num_elements") };
    }

    /// <summary>Writes the union's fields (the caller opens and closes the struct).</summary>
    public void WriteBody(ThriftCompactWriter w)
    {
        w.BeginStructField((short)Kind);
        switch (Kind)
        {
            case LogicalTypeKind.Decimal:
                w.I32Field(1, Scale);
                w.I32Field(2, Precision);
                break;
            case LogicalTypeKind.Time or LogicalTypeKind.Timestamp:
                w.BoolField(1, IsAdjustedToUtc);
                w.BeginStructField(2);
                w.EmptyStructField(UnitMember);
                w.EndStruct();
                break;
            case LogicalTypeKind.Integer:
                w.I8Field(1, BitWidth);
                w.BoolField(2, IsSigned);
                break;
            case LogicalTypeKind.Variant:
                if (SpecificationVersion is { } version) w.I8Field(1, version);
                break;
            case LogicalTypeKind.Geometry or LogicalTypeKind.Geography:
                if (Crs is { } crs) w.StringField(1, crs);
                if (Algorithm is { } algorithm) w.I32Field(2, (int)algorithm);
                break;
            case LogicalTypeKind.FiniteVector:
                w.I32Field(1, NumElements);
                break;
        }
        w.EndStruct();
    }
}

/// <summary>Thrift <c>RowGroup</c>.</summary>
internal sealed class RowGroup
{
    public required ColumnChunk[] Columns { get; init; }
    public required long TotalByteSize { get; init; }
    public required long NumRows { get; init; }
    public SortingColumn[]? SortingColumns { get; init; }
    public long? FileOffset { get; init; }
    public long? TotalCompressedSize { get; init; }
    public short? Ordinal { get; init; }

    public static RowGroup Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        ColumnChunk[]? columns = null;
        long? totalByteSize = null, numRows = null, fileOffset = null, totalCompressed = null;
        SortingColumn[]? sorting = null;
        short? ordinal = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.List: columns = r.ReadStructList(ColumnChunk.Read); break;
                case 2 when t == ThriftType.I64: totalByteSize = r.ReadI64(); break;
                case 3 when t == ThriftType.I64: numRows = r.ReadI64(); break;
                case 4 when t == ThriftType.List: sorting = r.ReadStructList(SortingColumn.Read); break;
                case 5 when t == ThriftType.I64: fileOffset = r.ReadI64(); break;
                case 6 when t == ThriftType.I64: totalCompressed = r.ReadI64(); break;
                case 7 when t == ThriftType.I16: ordinal = r.ReadI16(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new RowGroup
        {
            Columns = columns ?? throw Missing("RowGroup.columns"),
            TotalByteSize = totalByteSize ?? throw Missing("RowGroup.total_byte_size"),
            NumRows = numRows ?? throw Missing("RowGroup.num_rows"),
            SortingColumns = sorting,
            FileOffset = fileOffset,
            TotalCompressedSize = totalCompressed,
            Ordinal = ordinal,
        };
    }

    public void Write(ThriftCompactWriter w)
    {
        w.BeginStruct();
        w.BeginListField(1, ThriftType.Struct, Columns.Length);
        foreach (ColumnChunk c in Columns) c.Write(w);
        w.I64Field(2, TotalByteSize);
        w.I64Field(3, NumRows);
        if (SortingColumns is { } sorting)
        {
            w.BeginListField(4, ThriftType.Struct, sorting.Length);
            foreach (SortingColumn s in sorting) s.Write(w);
        }
        if (FileOffset is { } fileOffset) w.I64Field(5, fileOffset);
        if (TotalCompressedSize is { } total) w.I64Field(6, total);
        if (Ordinal is { } ordinal) w.I16Field(7, ordinal);
        w.EndStruct();
    }
}

/// <summary>Thrift <c>SortingColumn</c>.</summary>
internal sealed record SortingColumn(int ColumnIndex, bool Descending, bool NullsFirst)
{
    public static SortingColumn Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        int? column = null;
        bool? descending = null, nullsFirst = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.I32: column = r.ReadI32(); break;
                case 2 when ThriftCompactReader.IsBool(t): descending = r.BoolField(); break;
                case 3 when ThriftCompactReader.IsBool(t): nullsFirst = r.BoolField(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new SortingColumn(
            column ?? throw Missing("SortingColumn.column_idx"),
            descending ?? throw Missing("SortingColumn.descending"),
            nullsFirst ?? throw Missing("SortingColumn.nulls_first"));
    }

    public void Write(ThriftCompactWriter w)
    {
        w.BeginStruct();
        w.I32Field(1, ColumnIndex);
        w.BoolField(2, Descending);
        w.BoolField(3, NullsFirst);
        w.EndStruct();
    }
}

/// <summary>Thrift <c>ColumnChunk</c>.</summary>
internal sealed class ColumnChunk
{
    public string? FilePath { get; init; }
    public long FileOffset { get; init; }
    public ColumnMetaData? MetaData { get; init; }
    public long? OffsetIndexOffset { get; init; }
    public int? OffsetIndexLength { get; init; }
    public long? ColumnIndexOffset { get; init; }
    public int? ColumnIndexLength { get; init; }
    public ColumnCryptoMetaData? CryptoMetaData { get; init; }
    public byte[]? EncryptedColumnMetadata { get; init; }

    public static ColumnChunk Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        string? filePath = null;
        long fileOffset = 0; // required, but deprecated: some writers leave it out
        ColumnMetaData? meta = null;
        long? offsetIndexOffset = null, columnIndexOffset = null;
        int? offsetIndexLength = null, columnIndexLength = null;
        ColumnCryptoMetaData? crypto = null;
        byte[]? encrypted = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.Binary: filePath = r.ReadString(); break;
                case 2 when t == ThriftType.I64: fileOffset = r.ReadI64(); break;
                case 3 when t == ThriftType.Struct: meta = ColumnMetaData.Read(ref r); break;
                case 4 when t == ThriftType.I64: offsetIndexOffset = r.ReadI64(); break;
                case 5 when t == ThriftType.I32: offsetIndexLength = r.ReadI32(); break;
                case 6 when t == ThriftType.I64: columnIndexOffset = r.ReadI64(); break;
                case 7 when t == ThriftType.I32: columnIndexLength = r.ReadI32(); break;
                case 8 when t == ThriftType.Struct: crypto = ColumnCryptoMetaData.Read(ref r); break;
                case 9 when t == ThriftType.Binary: encrypted = r.ReadBytes(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new ColumnChunk
        {
            FilePath = filePath,
            FileOffset = fileOffset,
            MetaData = meta,
            OffsetIndexOffset = offsetIndexOffset,
            OffsetIndexLength = offsetIndexLength,
            ColumnIndexOffset = columnIndexOffset,
            ColumnIndexLength = columnIndexLength,
            CryptoMetaData = crypto,
            EncryptedColumnMetadata = encrypted,
        };
    }

    public void Write(ThriftCompactWriter w)
    {
        w.BeginStruct();
        if (FilePath is { } filePath) w.StringField(1, filePath);
        w.I64Field(2, FileOffset);
        if (MetaData is { } meta)
        {
            w.BeginStructField(3);
            meta.WriteBody(w);
            w.EndStruct();
        }
        if (OffsetIndexOffset is { } oio) w.I64Field(4, oio);
        if (OffsetIndexLength is { } oil) w.I32Field(5, oil);
        if (ColumnIndexOffset is { } cio) w.I64Field(6, cio);
        if (ColumnIndexLength is { } cil) w.I32Field(7, cil);
        if (CryptoMetaData is { } crypto)
        {
            w.BeginStructField(8);
            crypto.WriteBody(w);
            w.EndStruct();
        }
        if (EncryptedColumnMetadata is { } encrypted) w.BinaryField(9, encrypted);
        w.EndStruct();
    }
}

/// <summary>Thrift <c>ColumnMetaData</c>.</summary>
internal sealed class ColumnMetaData
{
    public required ParquetType Type { get; init; }
    public required ParquetEncoding[] Encodings { get; init; }
    public required string[] PathInSchema { get; init; }
    public required CompressionCodec Codec { get; init; }
    public required long NumValues { get; init; }
    public required long TotalUncompressedSize { get; init; }
    public required long TotalCompressedSize { get; init; }
    public KeyValue[]? KeyValueMetadata { get; init; }
    public required long DataPageOffset { get; init; }
    public long? IndexPageOffset { get; init; }
    public long? DictionaryPageOffset { get; init; }
    public Statistics? Statistics { get; init; }
    public PageEncodingStats[]? EncodingStats { get; init; }
    public long? BloomFilterOffset { get; init; }
    public int? BloomFilterLength { get; init; }
    public SizeStatistics? SizeStatistics { get; init; }
    public GeospatialStatistics? GeospatialStatistics { get; init; }

    public static ColumnMetaData Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        ParquetType? type = null;
        ParquetEncoding[]? encodings = null;
        string[]? path = null;
        CompressionCodec? codec = null;
        long? numValues = null, uncompressed = null, compressed = null, dataPage = null, indexPage = null, dictionaryPage = null, bloomOffset = null;
        KeyValue[]? keyValues = null;
        Statistics? statistics = null;
        PageEncodingStats[]? encodingStats = null;
        int? bloomLength = null;
        SizeStatistics? sizes = null;
        GeospatialStatistics? geo = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.I32: type = (ParquetType)r.ReadI32(); break;
                case 2 when t == ThriftType.List: encodings = [.. r.ReadI32List().Select(e => (ParquetEncoding)e)]; break;
                case 3 when t == ThriftType.List: path = r.ReadStringList(); break;
                case 4 when t == ThriftType.I32: codec = (CompressionCodec)r.ReadI32(); break;
                case 5 when t == ThriftType.I64: numValues = r.ReadI64(); break;
                case 6 when t == ThriftType.I64: uncompressed = r.ReadI64(); break;
                case 7 when t == ThriftType.I64: compressed = r.ReadI64(); break;
                case 8 when t == ThriftType.List: keyValues = r.ReadStructList(KeyValue.Read); break;
                case 9 when t == ThriftType.I64: dataPage = r.ReadI64(); break;
                case 10 when t == ThriftType.I64: indexPage = r.ReadI64(); break;
                case 11 when t == ThriftType.I64: dictionaryPage = r.ReadI64(); break;
                case 12 when t == ThriftType.Struct: statistics = Statistics.Read(ref r); break;
                case 13 when t == ThriftType.List: encodingStats = r.ReadStructList(PageEncodingStats.Read); break;
                case 14 when t == ThriftType.I64: bloomOffset = r.ReadI64(); break;
                case 15 when t == ThriftType.I32: bloomLength = r.ReadI32(); break;
                case 16 when t == ThriftType.Struct: sizes = SizeStatistics.Read(ref r); break;
                case 17 when t == ThriftType.Struct: geo = GeospatialStatistics.Read(ref r); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new ColumnMetaData
        {
            Type = type ?? throw Missing("ColumnMetaData.type"),
            Encodings = encodings ?? throw Missing("ColumnMetaData.encodings"),
            PathInSchema = path ?? throw Missing("ColumnMetaData.path_in_schema"),
            Codec = codec ?? throw Missing("ColumnMetaData.codec"),
            NumValues = numValues ?? throw Missing("ColumnMetaData.num_values"),
            TotalUncompressedSize = uncompressed ?? throw Missing("ColumnMetaData.total_uncompressed_size"),
            TotalCompressedSize = compressed ?? throw Missing("ColumnMetaData.total_compressed_size"),
            KeyValueMetadata = keyValues,
            DataPageOffset = dataPage ?? throw Missing("ColumnMetaData.data_page_offset"),
            IndexPageOffset = indexPage,
            DictionaryPageOffset = dictionaryPage,
            Statistics = statistics,
            EncodingStats = encodingStats,
            BloomFilterOffset = bloomOffset,
            BloomFilterLength = bloomLength,
            SizeStatistics = sizes,
            GeospatialStatistics = geo,
        };
    }

    /// <summary>Writes the struct's fields (the caller opens and closes it).</summary>
    public void WriteBody(ThriftCompactWriter w)
    {
        w.I32Field(1, (int)Type);
        w.BeginListField(2, ThriftType.I32, Encodings.Length);
        foreach (ParquetEncoding e in Encodings) w.WriteI32((int)e);
        w.BeginListField(3, ThriftType.Binary, PathInSchema.Length);
        foreach (string part in PathInSchema) w.WriteString(part);
        w.I32Field(4, (int)Codec);
        w.I64Field(5, NumValues);
        w.I64Field(6, TotalUncompressedSize);
        w.I64Field(7, TotalCompressedSize);
        if (KeyValueMetadata is { } kv) KeyValue.WriteList(w, 8, kv);
        w.I64Field(9, DataPageOffset);
        if (IndexPageOffset is { } indexPage) w.I64Field(10, indexPage);
        if (DictionaryPageOffset is { } dictionaryPage) w.I64Field(11, dictionaryPage);
        if (Statistics is { } statistics)
        {
            w.BeginStructField(12);
            statistics.WriteBody(w);
            w.EndStruct();
        }
        if (EncodingStats is { } encodingStats)
        {
            w.BeginListField(13, ThriftType.Struct, encodingStats.Length);
            foreach (PageEncodingStats s in encodingStats) s.Write(w);
        }
        if (BloomFilterOffset is { } bloomOffset) w.I64Field(14, bloomOffset);
        if (BloomFilterLength is { } bloomLength) w.I32Field(15, bloomLength);
        if (SizeStatistics is { } sizes)
        {
            w.BeginStructField(16);
            sizes.WriteBody(w);
            w.EndStruct();
        }
        if (GeospatialStatistics is { } geo)
        {
            w.BeginStructField(17);
            geo.WriteBody(w);
            w.EndStruct();
        }
    }
}

/// <summary>Thrift <c>PageEncodingStats</c>.</summary>
internal sealed record PageEncodingStats(PageType PageType, ParquetEncoding Encoding, int Count)
{
    public static PageEncodingStats Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        int? pageType = null, encoding = null, count = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.I32: pageType = r.ReadI32(); break;
                case 2 when t == ThriftType.I32: encoding = r.ReadI32(); break;
                case 3 when t == ThriftType.I32: count = r.ReadI32(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new PageEncodingStats(
            (PageType)(pageType ?? throw Missing("PageEncodingStats.page_type")),
            (ParquetEncoding)(encoding ?? throw Missing("PageEncodingStats.encoding")),
            count ?? throw Missing("PageEncodingStats.count"));
    }

    public void Write(ThriftCompactWriter w)
    {
        w.BeginStruct();
        w.I32Field(1, (int)PageType);
        w.I32Field(2, (int)Encoding);
        w.I32Field(3, Count);
        w.EndStruct();
    }
}

/// <summary>Thrift <c>Statistics</c>: bounds are PLAIN-encoded values (without a length prefix for binary).</summary>
internal sealed class Statistics
{
    /// <summary>Deprecated bounds, in signed byte order for binary.</summary>
    public byte[]? Max { get; init; }
    public byte[]? Min { get; init; }
    public long? NullCount { get; init; }
    public long? DistinctCount { get; init; }
    public byte[]? MaxValue { get; init; }
    public byte[]? MinValue { get; init; }
    public bool? IsMaxValueExact { get; init; }
    public bool? IsMinValueExact { get; init; }
    public long? NanCount { get; init; }

    public static Statistics Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        byte[]? max = null, min = null, maxValue = null, minValue = null;
        long? nullCount = null, distinct = null, nanCount = null;
        bool? maxExact = null, minExact = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.Binary: max = r.ReadBytes(); break;
                case 2 when t == ThriftType.Binary: min = r.ReadBytes(); break;
                case 3 when t == ThriftType.I64: nullCount = r.ReadI64(); break;
                case 4 when t == ThriftType.I64: distinct = r.ReadI64(); break;
                case 5 when t == ThriftType.Binary: maxValue = r.ReadBytes(); break;
                case 6 when t == ThriftType.Binary: minValue = r.ReadBytes(); break;
                case 7 when ThriftCompactReader.IsBool(t): maxExact = r.BoolField(); break;
                case 8 when ThriftCompactReader.IsBool(t): minExact = r.BoolField(); break;
                case 9 when t == ThriftType.I64: nanCount = r.ReadI64(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new Statistics
        {
            Max = max,
            Min = min,
            NullCount = nullCount,
            DistinctCount = distinct,
            MaxValue = maxValue,
            MinValue = minValue,
            IsMaxValueExact = maxExact,
            IsMinValueExact = minExact,
            NanCount = nanCount,
        };
    }

    /// <summary>Writes the struct's fields (the caller opens and closes it).</summary>
    public void WriteBody(ThriftCompactWriter w)
    {
        if (Max is { } max) w.BinaryField(1, max);
        if (Min is { } min) w.BinaryField(2, min);
        if (NullCount is { } nullCount) w.I64Field(3, nullCount);
        if (DistinctCount is { } distinct) w.I64Field(4, distinct);
        if (MaxValue is { } maxValue) w.BinaryField(5, maxValue);
        if (MinValue is { } minValue) w.BinaryField(6, minValue);
        if (IsMaxValueExact is { } maxExact) w.BoolField(7, maxExact);
        if (IsMinValueExact is { } minExact) w.BoolField(8, minExact);
        if (NanCount is { } nanCount) w.I64Field(9, nanCount);
    }
}

/// <summary>Thrift <c>SizeStatistics</c>.</summary>
internal sealed class SizeStatistics
{
    public long? UnencodedByteArrayDataBytes { get; init; }
    public long[]? RepetitionLevelHistogram { get; init; }
    public long[]? DefinitionLevelHistogram { get; init; }

    public static SizeStatistics Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        long? unencoded = null;
        long[]? repetition = null, definition = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.I64: unencoded = r.ReadI64(); break;
                case 2 when t == ThriftType.List: repetition = r.ReadI64List(); break;
                case 3 when t == ThriftType.List: definition = r.ReadI64List(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new SizeStatistics { UnencodedByteArrayDataBytes = unencoded, RepetitionLevelHistogram = repetition, DefinitionLevelHistogram = definition };
    }

    public void WriteBody(ThriftCompactWriter w)
    {
        if (UnencodedByteArrayDataBytes is { } unencoded) w.I64Field(1, unencoded);
        if (RepetitionLevelHistogram is { } repetition) WriteI64List(w, 2, repetition);
        if (DefinitionLevelHistogram is { } definition) WriteI64List(w, 3, definition);
    }

    internal static void WriteI64List(ThriftCompactWriter w, short id, long[] values)
    {
        w.BeginListField(id, ThriftType.I64, values.Length);
        foreach (long v in values) w.WriteI64(v);
    }
}

/// <summary>Thrift <c>BoundingBox</c>.</summary>
internal sealed record BoundingBox(double XMin, double XMax, double YMin, double YMax)
{
    public double? ZMin { get; init; }
    public double? ZMax { get; init; }
    public double? MMin { get; init; }
    public double? MMax { get; init; }

    public static BoundingBox Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        Span<double?> v = stackalloc double?[8];
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            if (id is >= 1 and <= 8 && t == ThriftType.Double) v[id - 1] = r.ReadDouble();
            else r.Skip(t);
        }
        r.ExitStruct();
        return new BoundingBox(
            v[0] ?? throw Missing("BoundingBox.xmin"), v[1] ?? throw Missing("BoundingBox.xmax"),
            v[2] ?? throw Missing("BoundingBox.ymin"), v[3] ?? throw Missing("BoundingBox.ymax"))
        { ZMin = v[4], ZMax = v[5], MMin = v[6], MMax = v[7] };
    }

    public void WriteBody(ThriftCompactWriter w)
    {
        w.DoubleField(1, XMin);
        w.DoubleField(2, XMax);
        w.DoubleField(3, YMin);
        w.DoubleField(4, YMax);
        if (ZMin is { } zMin) w.DoubleField(5, zMin);
        if (ZMax is { } zMax) w.DoubleField(6, zMax);
        if (MMin is { } mMin) w.DoubleField(7, mMin);
        if (MMax is { } mMax) w.DoubleField(8, mMax);
    }
}

/// <summary>Thrift <c>GeospatialStatistics</c>.</summary>
internal sealed class GeospatialStatistics
{
    public BoundingBox? BoundingBox { get; init; }
    public int[]? GeospatialTypes { get; init; }

    public static GeospatialStatistics Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        BoundingBox? box = null;
        int[]? types = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.Struct: box = BoundingBox.Read(ref r); break;
                case 2 when t == ThriftType.List: types = r.ReadI32List(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new GeospatialStatistics { BoundingBox = box, GeospatialTypes = types };
    }

    public void WriteBody(ThriftCompactWriter w)
    {
        if (BoundingBox is { } box)
        {
            w.BeginStructField(1);
            box.WriteBody(w);
            w.EndStruct();
        }
        if (GeospatialTypes is { } types)
        {
            w.BeginListField(2, ThriftType.I32, types.Length);
            foreach (int type in types) w.WriteI32(type);
        }
    }
}

/// <summary>
/// Thrift <c>ColumnCryptoMetaData</c>, a union: encrypted with the footer key, or with a column key (the column's
/// <see cref="Path"/> and optional <see cref="KeyMetadata"/>).
/// </summary>
internal sealed class ColumnCryptoMetaData
{
    public bool WithFooterKey { get; init; }
    public string[]? Path { get; init; }
    public byte[]? KeyMetadata { get; init; }

    public static ColumnCryptoMetaData Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        ColumnCryptoMetaData? result = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            if (result is null && t == ThriftType.Struct && id == 1)
            {
                r.Skip(t);
                result = new ColumnCryptoMetaData { WithFooterKey = true };
            }
            else if (result is null && t == ThriftType.Struct && id == 2) result = ReadColumnKey(ref r);
            else r.Skip(t);
        }
        r.ExitStruct();
        return result ?? throw new ParquetFormatException("A ColumnCryptoMetaData union in the Parquet metadata has no known member.");
    }

    private static ColumnCryptoMetaData ReadColumnKey(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        string[]? path = null;
        byte[]? key = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.List: path = r.ReadStringList(); break;
                case 2 when t == ThriftType.Binary: key = r.ReadBytes(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new ColumnCryptoMetaData { Path = path ?? throw Missing("EncryptionWithColumnKey.path_in_schema"), KeyMetadata = key };
    }

    public void WriteBody(ThriftCompactWriter w)
    {
        if (WithFooterKey)
        {
            w.EmptyStructField(1);
            return;
        }
        w.BeginStructField(2);
        w.BeginListField(1, ThriftType.Binary, Path!.Length);
        foreach (string part in Path) w.WriteString(part);
        if (KeyMetadata is { } key) w.BinaryField(2, key);
        w.EndStruct();
    }
}

/// <summary>
/// Thrift <c>EncryptionAlgorithm</c>, a union of <c>AesGcmV1</c> (<see cref="Member"/> 1) and <c>AesGcmCtrV1</c>
/// (2), which have the same fields.
/// </summary>
internal sealed class EncryptionAlgorithm
{
    public required short Member { get; init; }
    public byte[]? AadPrefix { get; init; }
    public byte[]? AadFileUnique { get; init; }
    public bool? SupplyAadPrefix { get; init; }

    public string Name => Member switch
    {
        1 => "AES_GCM_V1",
        2 => "AES_GCM_CTR_V1",
        _ => $"algorithm {Member}",
    };

    public static EncryptionAlgorithm Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        EncryptionAlgorithm? result = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            if (result is null && t == ThriftType.Struct) result = ReadMember(ref r, id);
            else r.Skip(t);
        }
        r.ExitStruct();
        return result ?? throw new ParquetFormatException("An EncryptionAlgorithm union in the Parquet metadata has no member.");
    }

    private static EncryptionAlgorithm ReadMember(ref ThriftCompactReader r, short member)
    {
        r.EnterStruct();
        byte[]? prefix = null, unique = null;
        bool? supply = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.Binary: prefix = r.ReadBytes(); break;
                case 2 when t == ThriftType.Binary: unique = r.ReadBytes(); break;
                case 3 when ThriftCompactReader.IsBool(t): supply = r.BoolField(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new EncryptionAlgorithm { Member = member, AadPrefix = prefix, AadFileUnique = unique, SupplyAadPrefix = supply };
    }

    public void WriteBody(ThriftCompactWriter w)
    {
        w.BeginStructField(Member);
        if (AadPrefix is { } prefix) w.BinaryField(1, prefix);
        if (AadFileUnique is { } unique) w.BinaryField(2, unique);
        if (SupplyAadPrefix is { } supply) w.BoolField(3, supply);
        w.EndStruct();
    }
}

/// <summary>Thrift <c>FileCryptoMetaData</c>: what precedes the encrypted footer of a <c>PARE</c> file.</summary>
internal sealed class FileCryptoMetaData
{
    public required EncryptionAlgorithm EncryptionAlgorithm { get; init; }
    public byte[]? KeyMetadata { get; init; }

    public static FileCryptoMetaData Read(ref ThriftCompactReader r)
    {
        r.EnterStruct();
        EncryptionAlgorithm? algorithm = null;
        byte[]? key = null;
        short last = 0;
        while (r.NextField(ref last, out short id, out byte t))
        {
            switch (id)
            {
                case 1 when t == ThriftType.Struct: algorithm = EncryptionAlgorithm.Read(ref r); break;
                case 2 when t == ThriftType.Binary: key = r.ReadBytes(); break;
                default: r.Skip(t); break;
            }
        }
        r.ExitStruct();
        return new FileCryptoMetaData { EncryptionAlgorithm = algorithm ?? throw Missing("FileCryptoMetaData.encryption_algorithm"), KeyMetadata = key };
    }

    public void Write(ThriftCompactWriter w)
    {
        w.BeginStruct();
        w.BeginStructField(1);
        EncryptionAlgorithm.WriteBody(w);
        w.EndStruct();
        if (KeyMetadata is { } key) w.BinaryField(2, key);
        w.EndStruct();
    }
}
