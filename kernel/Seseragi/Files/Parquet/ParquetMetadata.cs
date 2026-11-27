using System.Buffers.Binary;
using Kaminari.Seseragi.Files.Parquet.Metadata;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>The time unit of a Parquet timestamp.</summary>
public enum ParquetTimeUnit
{
    /// <summary>Milliseconds.</summary>
    Millis,
    /// <summary>Microseconds.</summary>
    Micros,
    /// <summary>Nanoseconds.</summary>
    Nanos,
}

/// <summary>One node of the flattened Parquet schema (the footer's <c>SchemaElement</c>), as far as statistics need it.</summary>
/// <param name="Name">The node name.</param>
/// <param name="PhysicalType">The physical type of a leaf (Thrift <c>Type</c>: 0 BOOLEAN … 7 FIXED_LEN_BYTE_ARRAY); null for a group.</param>
/// <param name="Repetition">0 required, 1 optional, 2 repeated; null for the root.</param>
/// <param name="ChildCount">The number of children of a group; 0 for a leaf.</param>
/// <param name="TypeLength">The byte length of a FIXED_LEN_BYTE_ARRAY.</param>
/// <param name="DecimalScale">The scale of a decimal, from the logical or the converted type.</param>
/// <param name="TimeUnit">The unit of a timestamp, from the logical or the converted type.</param>
/// <param name="FieldId">The field id, when the writer gave one (Delta's column mapping does).</param>
/// <param name="IntBitWidth">The bit width of an annotated integer (8, 16, 32 or 64), from the logical or the converted type.</param>
/// <param name="DecimalPrecision">The precision of a decimal, from the logical or the converted type.</param>
/// <param name="IsDate">Whether the leaf is annotated as a date.</param>
/// <param name="IsVariant">Whether the group is annotated as a variant (its <c>value</c> and <c>metadata</c> binaries).</param>
/// <param name="IsString">Whether the leaf is annotated as text (STRING, ENUM or JSON; UTF8 as converted type).</param>
/// <param name="IsList">Whether the group is annotated as a list (LIST).</param>
/// <param name="IsMap">Whether the group is annotated as a map (MAP, or MAP_KEY_VALUE as converted type).</param>
/// <param name="IsUnsigned">Whether the annotated integer is unsigned.</param>
/// <param name="IsAdjustedToUtc">Whether a timestamp is adjusted to UTC (an instant); a converted-type timestamp is.</param>
/// <param name="IsTime">Whether the leaf is annotated as a time of day (TIME).</param>
/// <param name="Annotation">The logical type of an annotated node this model names no flag for (UUID, FLOAT16,
/// INTERVAL, GEOMETRY, …), by its Thrift name; null otherwise.</param>
public sealed record ParquetSchemaElement(
    string Name, int? PhysicalType, int? Repetition, int ChildCount, int? TypeLength, int? DecimalScale, ParquetTimeUnit? TimeUnit,
    int? FieldId = null, int? IntBitWidth = null, int? DecimalPrecision = null, bool IsDate = false, bool IsVariant = false,
    bool IsString = false, bool IsList = false, bool IsMap = false, bool IsUnsigned = false, bool IsAdjustedToUtc = true,
    bool IsTime = false, string? Annotation = null);

/// <summary>The statistics of one column chunk: the raw PLAIN-encoded bounds and the null count, each absent when not written.</summary>
public sealed record ParquetColumnStatistics(byte[]? Min, byte[]? Max, long? NullCount);

/// <summary>One column chunk of a row group.</summary>
/// <param name="Path">The path of the leaf in the schema (top-level name first).</param>
/// <param name="ValueCount">The number of values, nulls included (for repeated leaves: elements); -1 when the chunk's metadata is encrypted with a key that was not given.</param>
/// <param name="Statistics">The chunk statistics, or null when none were written.</param>
public sealed record ParquetColumnChunk(IReadOnlyList<string> Path, long ValueCount, ParquetColumnStatistics? Statistics);

/// <summary>One row group.</summary>
public sealed record ParquetRowGroup(long RowCount, IReadOnlyList<ParquetColumnChunk> Columns);

/// <summary>
/// The footer of a Parquet file (its <c>FileMetaData</c>): the schema, the row count and the statistics of every
/// column chunk, as a read-only view of the full metadata.
/// </summary>
public sealed class ParquetMetadata
{
    private const int TailLength = 8; // footer length (4 bytes, little-endian) + "PAR1"

    /// <summary>The flattened schema, the root first.</summary>
    public required IReadOnlyList<ParquetSchemaElement> Schema { get; init; }

    /// <summary>The number of rows in the file.</summary>
    public required long RowCount { get; init; }

    private IReadOnlyList<ParquetRowGroup>? _rowGroups;
    private IReadOnlyList<KeyValuePair<string, string?>>? _keyValues;

    /// <summary>The row groups (made from the footer on first use).</summary>
    public IReadOnlyList<ParquetRowGroup> RowGroups
    {
        get => _rowGroups ??= Thrift is { } meta ? RowGroupsOf(meta) : [];
        init => _rowGroups = value;
    }

    /// <summary>The application that wrote the file, when it says.</summary>
    public string? CreatedBy { get; init; }

    /// <summary>The file's key-value metadata, in footer order (keys may repeat; made from the footer on first use).</summary>
    public IReadOnlyList<KeyValuePair<string, string?>> KeyValueMetadata
    {
        get => _keyValues ??= Thrift?.KeyValueMetadata is { } kv ? [.. kv.Select(p => KeyValuePair.Create(p.Key, p.Value))] : [];
        init => _keyValues = value;
    }

    /// <summary>The full footer this view was made from; null for a view built by hand.</summary>
    internal FileMetaData? Thrift { get; init; }

    /// <summary>The length of the footer this was read from (where the file's data ends), when read from a file.</summary>
    internal int? FooterSize { get; init; }

    /// <summary>The encryption of the file this was read from (Parquet modular encryption); null when it is not encrypted.</summary>
    internal EncryptionAlgorithm? Encryption { get; init; }

    /// <summary>Whether the file this was read from is encrypted (Parquet modular encryption).</summary>
    public bool IsEncrypted => Encryption is not null;

    /// <summary>How much of a file's end to read first: enough for almost every footer.</summary>
    public const int TailGuess = 64 * 1024;

    /// <summary>
    /// Where the footer of a file of <paramref name="size"/> bytes lies, from the file's last bytes
    /// (<paramref name="tail"/>, at least 8). When the range falls inside the tail, parse it from there with
    /// <see cref="Parse"/>; otherwise read that range from the file first.
    /// </summary>
    /// <param name="tail">The file's last bytes.</param>
    /// <param name="size">The file's size.</param>
    /// <param name="name">The file's name, for error messages.</param>
    /// <exception cref="ParquetFormatException">The file is not a Parquet file, is encrypted, or its footer length is damaged.</exception>
    public static (long Offset, int Length) FooterRange(ReadOnlySpan<byte> tail, long size, string name)
    {
        if (size < 12 || tail.Length < TailLength) throw new ParquetFormatException($"{name} is too short to be a Parquet file.");
        int footer = FooterLength(tail[^TailLength..], name);
        if (footer + TailLength > size - 4) throw new ParquetFormatException($"{name} has a footer longer than the file.");
        return (size - TailLength - footer, footer);
    }

    /// <summary>Reads the footer of a whole file in memory.</summary>
    /// <exception cref="ParquetFormatException">The bytes are not a Parquet file, the file is encrypted, or its footer is damaged.</exception>
    public static ParquetMetadata FromFile(ReadOnlySpan<byte> file)
    {
        if (file.Length < 12 || !file[..4].SequenceEqual("PAR1"u8) && !file[..4].SequenceEqual("PARE"u8))
            throw new ParquetFormatException("Not a Parquet file.");
        var (offset, length) = FooterRange(file, file.Length, "the file");
        return Parse(file.Slice((int)offset, length));
    }

    /// <summary>The footer length from a file's last 8 bytes (with <paramref name="allowEncrypted"/>, also of an encrypted footer: <c>PARE</c>).</summary>
    internal static int FooterLength(ReadOnlySpan<byte> tail, string name, bool allowEncrypted = false)
    {
        if (tail[4..].SequenceEqual("PARE"u8))
        {
            if (!allowEncrypted) throw Encrypted(name);
        }
        else if (!tail[4..].SequenceEqual("PAR1"u8)) throw new ParquetFormatException($"{name} is not a Parquet file (no PAR1 at the end).");
        int length = BinaryPrimitives.ReadInt32LittleEndian(tail);
        if (length <= 0) throw new ParquetFormatException($"{name} has an invalid footer length {length}.");
        return length;
    }

    internal static ParquetFormatException Encrypted(string name) =>
        new($"{name} is encrypted (Parquet modular encryption): open it with ParquetFile.Open and its keys (ParquetDecryptionOptions).");

    /// <summary>Parses the Thrift-encoded <c>FileMetaData</c>.</summary>
    /// <exception cref="ParquetFormatException">The bytes are not a valid footer, or the file is encrypted.</exception>
    public static ParquetMetadata Parse(ReadOnlySpan<byte> footer)
    {
        FileMetaData meta = FileMetaData.Parse(footer);
        if (meta.EncryptionAlgorithm is not null) throw Encrypted("The Parquet file");
        return FromThrift(meta);
    }

    /// <summary>Parses a footer kept from a file read before (its whole Thrift bytes: their length is the footer's).</summary>
    internal static ParquetMetadata ParseFooter(ReadOnlySpan<byte> footer, Schema.FooterInterner? interner = null)
    {
        FileMetaData meta = FileMetaData.Parse(footer, interner);
        if (meta.EncryptionAlgorithm is not null) throw Encrypted("The Parquet file");
        return FromThrift(meta, footer.Length);
    }

    /// <summary>The public view of a footer, after checking that its schema tree and row groups agree.</summary>
    /// <exception cref="ParquetFormatException">The schema tree or the column chunks are inconsistent.</exception>
    internal static ParquetMetadata FromThrift(FileMetaData meta, int? footerLength = null, EncryptionAlgorithm? encryption = null)
    {
        int leaves = meta.Shape?.LeafCount ?? SchemaTree.CountLeaves(meta.Schema);
        for (int g = 0; g < meta.RowGroups.Length; g++)
        {
            RowGroup group = meta.RowGroups[g];
            if (group.Columns.Length != leaves)
                throw new ParquetFormatException($"Row group {g} has {group.Columns.Length} column chunks for {leaves} schema leaves.");
            for (int c = 0; c < group.Columns.Length; c++)
            {
                // Encrypted with a key that was not given, a chunk's metadata stays encrypted.
                if (group.Columns[c] is { MetaData: null, CryptoMetaData: null })
                    throw new ParquetFormatException($"Column chunk {c} of row group {g} has no metadata.");
            }
        }
        // The views of the row groups and the key-value pairs are made on first use (readers use the full footer).
        return new ParquetMetadata
        {
            Schema = meta.Shape?.PublicSchema ?? [.. meta.Schema.Select(SchemaElement)],
            RowCount = meta.NumRows,
            CreatedBy = meta.CreatedBy,
            Thrift = meta,
            FooterSize = footerLength,
            Encryption = encryption,
        };
    }

    /// <summary>The public views of the row groups of a footer checked by <see cref="FromThrift"/>.</summary>
    private static ParquetRowGroup[] RowGroupsOf(FileMetaData meta)
    {
        var rowGroups = new ParquetRowGroup[meta.RowGroups.Length];
        for (int g = 0; g < rowGroups.Length; g++)
        {
            RowGroup group = meta.RowGroups[g];
            var chunks = new ParquetColumnChunk[group.Columns.Length];
            for (int c = 0; c < chunks.Length; c++)
            {
                ColumnChunk chunk = group.Columns[c];
                if (chunk.MetaData is not { } column)
                {
                    chunks[c] = new ParquetColumnChunk(chunk.CryptoMetaData?.Path ?? [], -1, null); // encrypted with a key not given
                    continue;
                }
                // Only min_value / max_value: the legacy min / max used a signed byte order for binary.
                Statistics? s = column.Statistics;
                chunks[c] = new ParquetColumnChunk(column.PathInSchema, column.NumValues, s is null ? null : new(s.MinValue, s.MaxValue, s.NullCount));
            }
            rowGroups[g] = new ParquetRowGroup(group.NumRows, chunks);
        }
        return rowGroups;
    }

    internal static ParquetSchemaElement SchemaElement(SchemaElement e)
    {
        int? scale = null, precision = null, bitWidth = null;
        ParquetTimeUnit? unit = null;
        bool date = false, variant = false, text = false, list = false, map = false, unsigned = false, adjusted = true, time = false;
        string? annotation = null;
        if (e.LogicalType is { } logical)
        {
            variant = logical.Kind == LogicalTypeKind.Variant;
            text = logical.Kind is LogicalTypeKind.String or LogicalTypeKind.Enum or LogicalTypeKind.Json;
            list = logical.Kind == LogicalTypeKind.List;
            map = logical.Kind == LogicalTypeKind.Map;
            time = logical.Kind == LogicalTypeKind.Time;
            if (logical.Kind == LogicalTypeKind.Timestamp) adjusted = logical.IsAdjustedToUtc;
            if (logical.Kind == LogicalTypeKind.Integer) unsigned = !logical.IsSigned;
            annotation = logical.Kind switch
            {
                LogicalTypeKind.Uuid or LogicalTypeKind.Float16 or LogicalTypeKind.Bson or LogicalTypeKind.Unknown or LogicalTypeKind.Geometry
                    or LogicalTypeKind.Geography or LogicalTypeKind.File or LogicalTypeKind.FiniteVector => logical.Kind.ToString().ToUpperInvariant(),
                _ => null,
            };
            if (logical.Kind == LogicalTypeKind.Decimal)
            {
                scale = logical.Scale;
                precision = logical.Precision;
            }
            date = logical.Kind == LogicalTypeKind.Date;
            if (logical.Kind == LogicalTypeKind.Timestamp) unit = logical.Unit;
            if (logical.Kind == LogicalTypeKind.Integer) bitWidth = logical.BitWidth;
        }
        ConvertedType? converted = e.ConvertedType;
        if (scale is null && converted == ConvertedType.Decimal)
        {
            scale = e.Scale ?? 0;
            precision = e.Precision;
        }
        if (unit is null && converted == ConvertedType.TimestampMillis) unit = ParquetTimeUnit.Millis;
        if (unit is null && converted == ConvertedType.TimestampMicros) unit = ParquetTimeUnit.Micros;
        date |= converted == ConvertedType.Date;
        text |= converted is ConvertedType.Utf8 or ConvertedType.Enum or ConvertedType.Json;
        list |= converted == ConvertedType.List;
        map |= converted is ConvertedType.Map or ConvertedType.MapKeyValue;
        time |= converted is ConvertedType.TimeMillis or ConvertedType.TimeMicros;
        unsigned |= converted is ConvertedType.UInt8 or ConvertedType.UInt16 or ConvertedType.UInt32 or ConvertedType.UInt64;
        if (converted == ConvertedType.Interval) annotation ??= "INTERVAL";
        if (converted == ConvertedType.Bson) annotation ??= "BSON";
        bitWidth ??= converted switch
        {
            ConvertedType.Int8 or ConvertedType.UInt8 => 8,
            ConvertedType.Int16 or ConvertedType.UInt16 => 16,
            ConvertedType.Int32 or ConvertedType.UInt32 => 32,
            ConvertedType.Int64 or ConvertedType.UInt64 => 64,
            _ => null,
        };
        return new ParquetSchemaElement(
            e.Name, (int?)e.Type, (int?)e.Repetition, e.NumChildren ?? 0, e.TypeLength, scale, unit, e.FieldId,
            bitWidth, precision, date, variant, text, list, map, unsigned, adjusted, time, annotation);
    }
}
