using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using ArrowTimeUnit = Apache.Arrow.Types.TimeUnit;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>The metadata versions of the Arrow format (Schema.fbs <c>MetadataVersion</c>).</summary>
internal enum IpcMetadataVersion : short
{
    V1,
    V2,
    V3,
    V4,
    V5,
}

/// <summary>What an IPC message carries (Message.fbs <c>MessageHeader</c>).</summary>
internal enum IpcMessageKind : byte
{
    None,
    Schema,
    DictionaryBatch,
    RecordBatch,
    Tensor,
    SparseTensor,
}

/// <summary>The codec of a compressed body (Message.fbs <c>CompressionType</c>); <see cref="None"/> for an uncompressed one.</summary>
internal enum IpcCompression
{
    None,
    Lz4Frame,
    Zstd,
}

/// <summary>A field node of a record batch: the length and null count of one array in the flattened tree.</summary>
internal readonly record struct IpcFieldNode(long Length, long NullCount);

/// <summary>A buffer of a record batch: where it lies in the message body.</summary>
internal readonly record struct IpcBuffer(long Offset, long Length);

/// <summary>A block of the file footer: where a message starts, the length of its metadata (prefix and padding included) and of its body.</summary>
internal readonly record struct IpcBlock(long Offset, int MetadataLength, long BodyLength);

/// <summary>A record batch header (also the data of a dictionary batch).</summary>
internal sealed record IpcRecordBatch(long Length, IpcFieldNode[] Nodes, IpcBuffer[] Buffers, IpcCompression Compression, long[] VariadicCounts);

/// <summary>A dictionary batch header.</summary>
internal sealed record IpcDictionaryBatch(long Id, IpcRecordBatch Data, bool IsDelta);

/// <summary>
/// How the reader turns a field's stored values into its Arrow type, when they differ: Polars writes Int128 and
/// UInt128 as an Arrow <c>Int</c> of 128 bits, which no Arrow type matches.
/// </summary>
internal enum IpcConversion : byte
{
    None,

    /// <summary>A signed 128-bit integer read as <c>decimal128(38, 0)</c> (the same 16 bytes).</summary>
    Int128AsDecimal128,

    /// <summary>An unsigned 128-bit integer widened to <c>decimal256(39, 0)</c>.</summary>
    UInt128AsDecimal256,
}

/// <summary>
/// A field of an IPC schema: the Arrow field, and what the body layout needs beyond it: a dictionary-encoded field's
/// dictionary id (its type is then a <see cref="DictionaryType"/>, and <see cref="Children"/> are the dictionary
/// values' children), the children as stored, and a conversion.
/// </summary>
internal sealed class IpcField(Field field, long? dictionaryId, IpcField[] children, IpcConversion conversion)
{
    public Field Field { get; } = field;

    public long? DictionaryId { get; } = dictionaryId;

    public IpcField[] Children { get; } = children;

    public IpcConversion Conversion { get; } = conversion;

    /// <summary>The type the body stores for this field: the dictionary's index type, or the value type.</summary>
    public IArrowType StoredType => Field.DataType is DictionaryType d ? d.IndexType : Field.DataType;

    /// <summary>The field the dictionary batches of a dictionary-encoded field hold (its value type).</summary>
    public IpcField ValueField() => Field.DataType is DictionaryType d
        ? new IpcField(new Field(Field.Name, d.ValueType, true), null, Children, Conversion)
        : this;
}

/// <summary>An IPC schema: the Arrow schema and the field tree with the dictionary ids.</summary>
internal sealed class IpcSchema(Schema schema, IpcField[] fields, IReadOnlyDictionary<long, IpcField> dictionaries, long[] features)
{
    public Schema Schema { get; } = schema;

    public IpcField[] Fields { get; } = fields;

    /// <summary>The dictionary-encoded fields by dictionary id (the first field of an id shared by several).</summary>
    public IReadOnlyDictionary<long, IpcField> Dictionaries { get; } = dictionaries;

    /// <summary>The features the writer declared (Schema.fbs <c>Feature</c>).</summary>
    public long[] Features { get; } = features;
}

/// <summary>One parsed IPC message (Message.fbs <c>Message</c>).</summary>
internal sealed class IpcMessage
{
    public IpcMetadataVersion Version { get; init; }

    public IpcMessageKind Kind { get; init; }

    public long BodyLength { get; init; }

    /// <summary>The message's custom metadata (null when it has none).</summary>
    public IReadOnlyList<KeyValuePair<string, string>>? Metadata { get; init; }

    public IpcSchema? Schema { get; init; }

    public IpcRecordBatch? RecordBatch { get; init; }

    public IpcDictionaryBatch? DictionaryBatch { get; init; }
}

/// <summary>The footer of an IPC file (File.fbs <c>Footer</c>).</summary>
internal sealed record IpcFooter(IpcMetadataVersion Version, IpcSchema Schema, IpcBlock[] Dictionaries, IpcBlock[] RecordBatches,
    IReadOnlyList<KeyValuePair<string, string>>? Metadata);

/// <summary>
/// Parses Arrow IPC metadata (format/Message.fbs, Schema.fbs, File.fbs) from its FlatBuffers, checking every position
/// (<see cref="FlatBufferTable"/>) and every value the reader relies on: known versions, types and units, widths,
/// little-endian data, nesting depth.
/// </summary>
internal static class IpcMetadata
{
    private const int MaxDepth = 64;

    /// <summary>Parses a <c>Message</c> FlatBuffer.</summary>
    /// <exception cref="InvalidDataException">The message is damaged or uses something this reader does not read.</exception>
    public static IpcMessage ParseMessage(ReadOnlySpan<byte> flatBuffer)
    {
        FlatBufferTable m = FlatBufferTable.Root(flatBuffer);
        // Message: 0 version, 1 header_type, 2 header, 3 bodyLength, 4 custom_metadata.
        IpcMetadataVersion version = Version(m.Int16(0));
        var kind = (IpcMessageKind)m.UInt8(1);
        long bodyLength = m.Int64(3);
        if (bodyLength < 0) throw FlatBufferTable.Damaged($"a body length of {bodyLength}");
        var metadata = KeyValues(m, 4);
        if (!m.TryTable(2, out FlatBufferTable header)) throw FlatBufferTable.Damaged("a message without a header");
        return kind switch
        {
            IpcMessageKind.Schema => new IpcMessage { Version = version, Kind = kind, BodyLength = bodyLength, Metadata = metadata, Schema = ParseSchema(header) },
            IpcMessageKind.RecordBatch => new IpcMessage { Version = version, Kind = kind, BodyLength = bodyLength, Metadata = metadata, RecordBatch = ParseRecordBatch(header) },
            IpcMessageKind.DictionaryBatch => new IpcMessage
            {
                Version = version, Kind = kind, BodyLength = bodyLength, Metadata = metadata,
                // DictionaryBatch: 0 id, 1 data, 2 isDelta.
                DictionaryBatch = new IpcDictionaryBatch(header.Int64(0),
                    header.TryTable(1, out FlatBufferTable data) ? ParseRecordBatch(data) : throw FlatBufferTable.Damaged("a dictionary batch without data"),
                    header.Bool(2)),
            },
            IpcMessageKind.Tensor or IpcMessageKind.SparseTensor => throw new InvalidDataException($"Arrow IPC {kind} messages are not supported (deprecated by the format)."),
            _ => throw FlatBufferTable.Damaged($"an unknown message type {(byte)kind}"),
        };
    }

    /// <summary>Parses a <c>Footer</c> FlatBuffer.</summary>
    /// <exception cref="InvalidDataException">The footer is damaged or uses something this reader does not read.</exception>
    public static IpcFooter ParseFooter(ReadOnlySpan<byte> flatBuffer)
    {
        FlatBufferTable f = FlatBufferTable.Root(flatBuffer);
        // Footer: 0 version, 1 schema, 2 dictionaries, 3 recordBatches, 4 custom_metadata.
        // Some writers (Arrow 0.14) left the footer's version at its default (V1): the messages' versions decide.
        short raw = f.Int16(0);
        IpcMetadataVersion version = raw is >= 0 and < (short)IpcMetadataVersion.V4 ? IpcMetadataVersion.V4 : Version(raw);
        if (!f.TryTable(1, out FlatBufferTable schema)) throw FlatBufferTable.Damaged("a footer without a schema");
        return new IpcFooter(version, ParseSchema(schema), Blocks(f, 2), Blocks(f, 3), KeyValues(f, 4));

        static IpcBlock[] Blocks(FlatBufferTable table, int slot)
        {
            // Block (a struct of 24 bytes): offset long, metaDataLength int, padding, bodyLength long.
            int count = table.Vector(slot, 24, out int start);
            if (count == 0) return [];
            ReadOnlySpan<byte> bytes = table.Bytes(start, count * 24);
            var blocks = new IpcBlock[count];
            for (int i = 0; i < count; i++)
            {
                ReadOnlySpan<byte> b = bytes.Slice(i * 24, 24);
                blocks[i] = new IpcBlock(BinaryPrimitives.ReadInt64LittleEndian(b), BinaryPrimitives.ReadInt32LittleEndian(b[8..]), BinaryPrimitives.ReadInt64LittleEndian(b[16..]));
            }
            return blocks;
        }
    }

    private static IpcMetadataVersion Version(short value) => value switch
    {
        >= (short)IpcMetadataVersion.V4 and <= (short)IpcMetadataVersion.V5 => (IpcMetadataVersion)value,
        >= 0 and < (short)IpcMetadataVersion.V4 => throw new InvalidDataException($"Arrow IPC metadata version V{value + 1} (before Arrow 0.8) is not supported."),
        _ => throw new InvalidDataException($"Arrow IPC metadata version {value} is newer than this reader (V5)."),
    };

    private static IpcRecordBatch ParseRecordBatch(FlatBufferTable rb)
    {
        // RecordBatch: 0 length, 1 nodes, 2 buffers, 3 compression, 4 variadicBufferCounts.
        long length = rb.Int64(0);
        if (length < 0) throw FlatBufferTable.Damaged($"a record batch of {length} rows");
        int nodeCount = rb.Vector(1, 16, out int nodesAt);
        var nodes = new IpcFieldNode[nodeCount];
        ReadOnlySpan<byte> nodeBytes = nodeCount == 0 ? default : rb.Bytes(nodesAt, nodeCount * 16);
        for (int i = 0; i < nodeCount; i++)
            nodes[i] = new IpcFieldNode(BinaryPrimitives.ReadInt64LittleEndian(nodeBytes[(i * 16)..]), BinaryPrimitives.ReadInt64LittleEndian(nodeBytes[(i * 16 + 8)..]));
        int bufferCount = rb.Vector(2, 16, out int buffersAt);
        var buffers = new IpcBuffer[bufferCount];
        ReadOnlySpan<byte> bufferBytes = bufferCount == 0 ? default : rb.Bytes(buffersAt, bufferCount * 16);
        for (int i = 0; i < bufferCount; i++)
            buffers[i] = new IpcBuffer(BinaryPrimitives.ReadInt64LittleEndian(bufferBytes[(i * 16)..]), BinaryPrimitives.ReadInt64LittleEndian(bufferBytes[(i * 16 + 8)..]));
        IpcCompression compression = IpcCompression.None;
        if (rb.TryTable(3, out FlatBufferTable c))
        {
            // BodyCompression: 0 codec (LZ4_FRAME = 0, ZSTD = 1; LZ4_FRAME when absent), 1 method (BUFFER = 0).
            compression = c.UInt8(0) switch
            {
                0 => IpcCompression.Lz4Frame,
                1 => IpcCompression.Zstd,
                var codec => throw new InvalidDataException($"Arrow IPC body compression codec {codec} is not supported."),
            };
            if (c.UInt8(1) != 0) throw new InvalidDataException($"Arrow IPC body compression method {c.UInt8(1)} is not supported.");
        }
        int variadicCount = rb.Vector(4, 8, out int variadicAt);
        var variadic = new long[variadicCount];
        for (int i = 0; i < variadicCount; i++) variadic[i] = BinaryPrimitives.ReadInt64LittleEndian(rb.Bytes(variadicAt + 8 * i, 8));
        return new IpcRecordBatch(length, nodes, buffers, compression, variadic);
    }

    private static IpcSchema ParseSchema(FlatBufferTable s)
    {
        // Schema: 0 endianness, 1 fields, 2 custom_metadata, 3 features.
        if (s.Int16(0) != 0) throw new InvalidDataException("Big-endian Arrow IPC data is not supported.");
        Dictionary<long, IpcField> dictionaries = [];
        IpcField[] fields = Fields(s, 1, 0, dictionaries);
        int featureCount = s.Vector(3, 8, out int featuresAt);
        var features = new long[featureCount];
        for (int i = 0; i < featureCount; i++) features[i] = BinaryPrimitives.ReadInt64LittleEndian(s.Bytes(featuresAt + 8 * i, 8));
        var schema = new Schema(fields.Select(f => f.Field), LastOfEachKey(KeyValues(s, 2)));
        return new IpcSchema(schema, fields, dictionaries, features);
    }

    private static IpcField[] Fields(FlatBufferTable table, int slot, int depth, Dictionary<long, IpcField> dictionaries)
    {
        if (depth > MaxDepth) throw FlatBufferTable.Damaged($"fields nested deeper than {MaxDepth} levels");
        int count = table.Vector(slot, 4, out int start);
        if (count == 0) return [];
        var fields = new IpcField[count];
        for (int i = 0; i < count; i++) fields[i] = ParseField(table.TableAt(start, i), depth, dictionaries);
        return fields;
    }

    private static IpcField ParseField(FlatBufferTable f, int depth, Dictionary<long, IpcField> dictionaries)
    {
        // Field: 0 name, 1 nullable, 2 type_type, 3 type, 4 dictionary, 5 children, 6 custom_metadata.
        string name = f.String(0) ?? "";
        bool nullable = f.Bool(1);
        IpcField[] children = Fields(f, 5, depth + 1, dictionaries);
        var metadata = LastOfEachKey(KeyValues(f, 6));
        byte typeTag = f.UInt8(2);
        f.TryTable(3, out FlatBufferTable typeTable); // absent: every parameter takes its default
        IArrowType type;
        IpcConversion conversion;
        try
        {
            (type, conversion) = Type(typeTag, typeTable, children, name);
        }
        catch (ArgumentException e)
        {
            // Apache.Arrow refuses some types the format allows (a struct without fields, fixed-size binary of width 0).
            throw new InvalidDataException($"Field '{name}' has a type Apache.Arrow cannot represent: {e.Message}", e);
        }

        long? dictionaryId = null;
        if (f.TryTable(4, out FlatBufferTable d))
        {
            // DictionaryEncoding: 0 id, 1 indexType (Int), 2 isOrdered, 3 dictionaryKind (DenseArray = 0).
            if (d.Int16(3) != 0) throw new InvalidDataException($"Dictionary kind {d.Int16(3)} of field '{name}' is not supported.");
            IArrowType index = d.TryTable(1, out FlatBufferTable indexTable) ? IndexType(indexTable, name) : Int32Type.Default;
            type = new DictionaryType(index, type, d.Bool(2));
            dictionaryId = d.Int64(0);
        }
        var field = new IpcField(new Field(name, type, nullable, metadata), dictionaryId, children, conversion);
        if (dictionaryId is long id) dictionaries.TryAdd(id, field);
        return field;
    }

    private static IArrowType IndexType(FlatBufferTable t, string name)
    {
        (int width, bool signed) = (t.Int32(0), t.Bool(1));
        return (width, signed) switch
        {
            (8, true) => Int8Type.Default,
            (16, true) => Int16Type.Default,
            (32, true) => Int32Type.Default,
            (64, true) => Int64Type.Default,
            (8, false) => UInt8Type.Default,
            (16, false) => UInt16Type.Default,
            (32, false) => UInt32Type.Default,
            (64, false) => UInt64Type.Default,
            _ => throw new InvalidDataException($"Field '{name}' has dictionary indices of {width} bits."),
        };
    }

    /// <summary>The Arrow type of a field from its Type union tag and table (Schema.fbs), checking its children.</summary>
    private static (IArrowType, IpcConversion) Type(byte tag, FlatBufferTable t, IpcField[] children, string name)
    {
        IArrowType Child(int index) => children[index].Field.DataType;
        void Expect(int count)
        {
            if (children.Length != count)
                throw new InvalidDataException($"Field '{name}' of type {TagName(tag)} has {children.Length} children; {count} expected.");
        }

        switch (tag)
        {
            case 1: Expect(0); return (NullType.Default, IpcConversion.None);
            case 2:
            {
                Expect(0);
                int width = t.Int32(0, 0);
                bool signed = t.Bool(1);
                return (width, signed) switch
                {
                    (8, true) => (Int8Type.Default, IpcConversion.None),
                    (16, true) => (Int16Type.Default, IpcConversion.None),
                    (32, true) => (Int32Type.Default, IpcConversion.None),
                    (64, true) => (Int64Type.Default, IpcConversion.None),
                    (8, false) => (UInt8Type.Default, IpcConversion.None),
                    (16, false) => (UInt16Type.Default, IpcConversion.None),
                    (32, false) => (UInt32Type.Default, IpcConversion.None),
                    (64, false) => (UInt64Type.Default, IpcConversion.None),
                    (128, true) => (new Decimal128Type(38, 0), IpcConversion.Int128AsDecimal128),
                    (128, false) => (new Decimal256Type(39, 0), IpcConversion.UInt128AsDecimal256),
                    _ => throw new InvalidDataException($"Field '{name}' is an integer of {width} bits."),
                };
            }
            case 3:
                Expect(0);
                return (t.Int16(0, 0) switch
                {
                    0 => HalfFloatType.Default,
                    1 => FloatType.Default,
                    2 => DoubleType.Default,
                    var p => throw new InvalidDataException($"Field '{name}' has floating-point precision {p}."),
                }, IpcConversion.None);
            case 4: Expect(0); return (BinaryType.Default, IpcConversion.None);
            case 5: Expect(0); return (StringType.Default, IpcConversion.None);
            case 6: Expect(0); return (BooleanType.Default, IpcConversion.None);
            case 7:
            {
                Expect(0);
                int precision = t.Int32(0, 0), scale = t.Int32(1, 0), width = t.Int32(2, 128);
                return (width switch
                {
                    32 => new Decimal32Type(precision, scale),
                    64 => new Decimal64Type(precision, scale),
                    128 => new Decimal128Type(precision, scale),
                    256 => new Decimal256Type(precision, scale),
                    _ => throw new InvalidDataException($"Field '{name}' is a decimal of {width} bits."),
                }, IpcConversion.None);
            }
            case 8:
                Expect(0);
                return (t.Int16(0, 1) switch
                {
                    0 => Date32Type.Default,
                    1 => Date64Type.Default,
                    var u => throw new InvalidDataException($"Field '{name}' has date unit {u}."),
                }, IpcConversion.None);
            case 9:
            {
                Expect(0);
                ArrowTimeUnit unit = Unit(t.Int16(0, 1), name);
                int width = t.Int32(1, 32);
                return ((unit, width) switch
                {
                    (ArrowTimeUnit.Second or ArrowTimeUnit.Millisecond, 32) => new Time32Type(unit),
                    (ArrowTimeUnit.Microsecond or ArrowTimeUnit.Nanosecond, 64) => new Time64Type(unit),
                    _ => throw new InvalidDataException($"Field '{name}' is a time of {width} bits in {unit}s."),
                }, IpcConversion.None);
            }
            case 10: Expect(0); return (new TimestampType(Unit(t.Int16(0), name), t.String(1)), IpcConversion.None);
            case 11:
                Expect(0);
                return (t.Int16(0, 0) switch
                {
                    0 => new IntervalType(IntervalUnit.YearMonth),
                    1 => new IntervalType(IntervalUnit.DayTime),
                    2 => new IntervalType(IntervalUnit.MonthDayNanosecond),
                    var u => throw new InvalidDataException($"Field '{name}' has interval unit {u}."),
                }, IpcConversion.None);
            case 12: Expect(1); return (new ListType(children[0].Field), IpcConversion.None);
            case 13: return (new StructType(children.Select(c => c.Field).ToList()), IpcConversion.None);
            case 14:
            {
                if (children.Length > 127) throw new InvalidDataException($"Union field '{name}' has {children.Length} children (at most 127).");
                var mode = t.Int16(0, 0) switch
                {
                    0 => UnionMode.Sparse,
                    1 => UnionMode.Dense,
                    var m => throw new InvalidDataException($"Union field '{name}' has mode {m}."),
                };
                int idCount = t.Vector(1, 4, out int idsAt);
                int[] ids;
                if (idCount == 0) ids = [.. Enumerable.Range(0, children.Length)];
                else
                {
                    if (idCount != children.Length) throw new InvalidDataException($"Union field '{name}' has {idCount} type ids for {children.Length} children.");
                    ids = new int[idCount];
                    for (int i = 0; i < idCount; i++)
                    {
                        ids[i] = BinaryPrimitives.ReadInt32LittleEndian(t.Bytes(idsAt + 4 * i, 4));
                        if (ids[i] is < 0 or > 127) throw new InvalidDataException($"Union field '{name}' has type id {ids[i]}.");
                    }
                    if (ids.Distinct().Count() != ids.Length) throw new InvalidDataException($"Union field '{name}' repeats a type id.");
                }
                return (new UnionType(children.Select(c => c.Field), ids, mode), IpcConversion.None);
            }
            case 15:
            {
                Expect(0);
                int width = t.Int32(0, 0);
                if (width < 0) throw new InvalidDataException($"Field '{name}' is fixed-size binary of {width} bytes.");
                return (new FixedSizeBinaryType(width), IpcConversion.None);
            }
            case 16:
            {
                Expect(1);
                int size = t.Int32(0, 0);
                if (size < 0) throw new InvalidDataException($"Field '{name}' is a fixed-size list of {size} items.");
                return (new FixedSizeListType(children[0].Field, size), IpcConversion.None);
            }
            case 17:
            {
                Expect(1);
                if (Child(0) is not StructType { Fields.Count: 2 } entries || children[0].DictionaryId is not null)
                    throw new InvalidDataException($"Map field '{name}' does not hold a struct of a key and a value.");
                return (new MapType(children[0].Field, t.Bool(0)), IpcConversion.None);
            }
            case 18: Expect(0); return (DurationType(Unit(t.Int16(0, 1), name)), IpcConversion.None);
            case 19: Expect(0); return (LargeBinaryType.Default, IpcConversion.None);
            case 20: Expect(0); return (LargeStringType.Default, IpcConversion.None);
            case 21: Expect(1); return (new LargeListType(children[0].Field), IpcConversion.None);
            case 22:
                Expect(2);
                if (Child(0) is not (Int16Type or Int32Type or Int64Type) || children[0].DictionaryId is not null)
                    throw new InvalidDataException($"Run-end encoded field '{name}' has run ends of type {Child(0).Name}.");
                return (new RunEndEncodedType(children[0].Field, children[1].Field), IpcConversion.None);
            case 23: Expect(0); return (BinaryViewType.Default, IpcConversion.None);
            case 24: Expect(0); return (StringViewType.Default, IpcConversion.None);
            case 25: Expect(1); return (new ListViewType(children[0].Field), IpcConversion.None);
            case 26: Expect(1); return (new LargeListViewType(children[0].Field), IpcConversion.None);
            default: throw new InvalidDataException($"Field '{name}' has type {TagName(tag)}, which this reader does not know.");
        }
    }

    private static readonly string[] TagNames = ["NONE", "Null", "Int", "FloatingPoint", "Binary", "Utf8", "Bool", "Decimal", "Date", "Time",
        "Timestamp", "Interval", "List", "Struct", "Union", "FixedSizeBinary", "FixedSizeList", "Map", "Duration", "LargeBinary", "LargeUtf8",
        "LargeList", "RunEndEncoded", "BinaryView", "Utf8View", "ListView", "LargeListView"];

    private static string TagName(byte tag) => tag < TagNames.Length ? TagNames[tag] : $"#{tag}";

    private static ArrowTimeUnit Unit(short value, string name) => value switch
    {
        0 => ArrowTimeUnit.Second,
        1 => ArrowTimeUnit.Millisecond,
        2 => ArrowTimeUnit.Microsecond,
        3 => ArrowTimeUnit.Nanosecond,
        _ => throw new InvalidDataException($"Field '{name}' has time unit {value}."),
    };

    private static DurationType DurationType(ArrowTimeUnit unit) => unit switch
    {
        ArrowTimeUnit.Second => Apache.Arrow.Types.DurationType.Second,
        ArrowTimeUnit.Millisecond => Apache.Arrow.Types.DurationType.Millisecond,
        ArrowTimeUnit.Microsecond => Apache.Arrow.Types.DurationType.Microsecond,
        _ => Apache.Arrow.Types.DurationType.Nanosecond,
    };

    /// <summary>
    /// Metadata as Apache.Arrow's schemas and fields hold it (a dictionary): a key the data repeats takes its last value,
    /// as the footer's and messages' metadata read (the format does not forbid repeats; Apache.Arrow throws on them).
    /// </summary>
    private static List<KeyValuePair<string, string>>? LastOfEachKey(List<KeyValuePair<string, string>>? pairs)
    {
        if (pairs is null || pairs.Count < 2) return pairs;
        Dictionary<string, int> at = [];
        List<KeyValuePair<string, string>> unique = new(pairs.Count);
        foreach (var pair in pairs)
        {
            if (at.TryGetValue(pair.Key, out int i)) unique[i] = pair;
            else
            {
                at[pair.Key] = unique.Count;
                unique.Add(pair);
            }
        }
        return unique;
    }

    private static List<KeyValuePair<string, string>>? KeyValues(FlatBufferTable table, int slot)
    {
        int count = table.Vector(slot, 4, out int start);
        if (count == 0) return null;
        List<KeyValuePair<string, string>> pairs = new(count);
        for (int i = 0; i < count; i++)
        {
            // KeyValue: 0 key, 1 value.
            FlatBufferTable kv = table.TableAt(start, i);
            pairs.Add(new(kv.String(0) ?? "", kv.String(1) ?? ""));
        }
        return pairs;
    }
}
