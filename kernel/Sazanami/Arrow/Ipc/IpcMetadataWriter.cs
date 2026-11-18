using System.Buffers.Binary;
using Apache.Arrow;
using Apache.Arrow.Types;
using ArrowTimeUnit = Apache.Arrow.Types.TimeUnit;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>
/// Writes Arrow IPC metadata (format/Message.fbs, Schema.fbs, File.fbs) as FlatBuffers: schema, record batch,
/// dictionary batch and footer messages, metadata version V5, little-endian.
/// </summary>
internal static class IpcMetadataWriter
{
    /// <summary>
    /// The IPC form of a schema: extension types as their storage with the <c>ARROW:extension:*</c> keys in the field
    /// metadata, dictionary-encoded fields numbered in pre-order (through dictionary values too).
    /// </summary>
    /// <exception cref="NotSupportedException">A type the format cannot hold.</exception>
    public static IpcSchema Schema(Schema schema)
    {
        long next = 0;
        Dictionary<long, IpcField> dictionaries = [];
        IpcField[] fields = [.. schema.FieldsList.Select(Field)];
        return new IpcSchema(new Schema(fields.Select(f => f.Field), schema.Metadata), fields, dictionaries, []);

        IpcField Field(Field field)
        {
            IArrowType type = field.DataType;
            IEnumerable<KeyValuePair<string, string>>? metadata = field.Metadata;
            if (type is ExtensionType extension)
            {
                metadata = [.. (metadata ?? []).Where(p => !p.Key.StartsWith("ARROW:extension:", StringComparison.Ordinal)),
                    new("ARROW:extension:name", extension.Name), new("ARROW:extension:metadata", extension.ExtensionMetadata ?? "")];
                type = extension.StorageType;
            }
            long? id = null;
            IArrowType valueType = type;
            if (type is DictionaryType d)
            {
                id = next++;
                valueType = d.ValueType is ExtensionType e ? throw new NotSupportedException("Dictionaries of extension values are not supported.") : d.ValueType;
            }
            IpcField[] children = valueType is NestedType n ? [.. n.Fields.Select(Field)] : [];
            IArrowType rebuilt = Rebuild(valueType, children);
            if (type is DictionaryType dictionary) rebuilt = new DictionaryType(dictionary.IndexType, rebuilt, dictionary.Ordered);
            var ipc = new IpcField(new Field(field.Name, rebuilt, field.IsNullable, metadata), id, children, IpcConversion.None);
            if (id is long key) dictionaries[key] = ipc;
            return ipc;
        }
    }

    /// <summary>A nested type over children rewritten (extension children become their storage).</summary>
    private static IArrowType Rebuild(IArrowType type, IpcField[] children) => type switch
    {
        _ when children.Length == 0 => type,
        StructType => new StructType([.. children.Select(c => c.Field)]),
        ListType => new ListType(children[0].Field),
        LargeListType => new LargeListType(children[0].Field),
        ListViewType => new ListViewType(children[0].Field),
        LargeListViewType => new LargeListViewType(children[0].Field),
        FixedSizeListType f => new FixedSizeListType(children[0].Field, f.ListSize),
        MapType m => new MapType(children[0].Field, m.KeySorted),
        UnionType u => new UnionType(children.Select(c => c.Field), u.TypeIds, u.Mode),
        RunEndEncodedType => new RunEndEncodedType(children[0].Field, children[1].Field),
        _ => type,
    };

    /// <summary>A schema message (no body).</summary>
    public static byte[] SchemaMessage(IpcSchema schema, IReadOnlyList<KeyValuePair<string, string>>? metadata = null)
    {
        using var w = new FlatBufferWriter();
        int message = MessageTable(w, IpcMessageKind.Schema, 0, metadata, out int header);
        w.Patch(header, SchemaTable(w, schema));
        w.Root(message);
        return w.Written.ToArray();
    }

    /// <summary>A record batch message.</summary>
    public static byte[] RecordBatchMessage(IpcRecordBatch batch, long bodyLength, IReadOnlyList<KeyValuePair<string, string>>? metadata)
    {
        using var w = new FlatBufferWriter();
        int message = MessageTable(w, IpcMessageKind.RecordBatch, bodyLength, metadata, out int header);
        w.Patch(header, RecordBatchTable(w, batch));
        w.Root(message);
        return w.Written.ToArray();
    }

    /// <summary>A dictionary batch message.</summary>
    public static byte[] DictionaryBatchMessage(IpcDictionaryBatch dictionary, long bodyLength)
    {
        using var w = new FlatBufferWriter();
        int message = MessageTable(w, IpcMessageKind.DictionaryBatch, bodyLength, null, out int header);
        // DictionaryBatch: 0 id, 1 data, 2 isDelta.
        int table = w.Table([FlatBufferField.Long(0, dictionary.Id), FlatBufferField.Offset(1), FlatBufferField.Bool(2, dictionary.IsDelta)], out int[] at);
        w.Patch(header, table);
        w.Patch(at[1], RecordBatchTable(w, dictionary.Data));
        w.Root(message);
        return w.Written.ToArray();
    }

    /// <summary>The footer of a file.</summary>
    public static byte[] Footer(IpcSchema schema, IReadOnlyList<IpcBlock> dictionaries, IReadOnlyList<IpcBlock> batches, IReadOnlyList<KeyValuePair<string, string>>? metadata)
    {
        using var w = new FlatBufferWriter();
        // Footer: 0 version, 1 schema, 2 dictionaries, 3 recordBatches, 4 custom_metadata.
        List<FlatBufferField> fields = [FlatBufferField.Short(0, (short)IpcMetadataVersion.V5), FlatBufferField.Offset(1), FlatBufferField.Offset(2), FlatBufferField.Offset(3)];
        if (metadata is { Count: > 0 }) fields.Add(FlatBufferField.Offset(4));
        int footer = w.Table([.. fields], out int[] at);
        w.Root(footer);
        w.Patch(at[1], SchemaTable(w, schema));
        w.Patch(at[2], w.ValueVector(Blocks(dictionaries), dictionaries.Count, 8));
        w.Patch(at[3], w.ValueVector(Blocks(batches), batches.Count, 8));
        if (metadata is { Count: > 0 }) w.Patch(at[4], KeyValues(w, metadata));
        return w.Written.ToArray();

        static byte[] Blocks(IReadOnlyList<IpcBlock> blocks)
        {
            // Block: offset long, metaDataLength int, padding, bodyLength long.
            byte[] bytes = new byte[24 * blocks.Count];
            for (int i = 0; i < blocks.Count; i++)
            {
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(24 * i), blocks[i].Offset);
                BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(24 * i + 8), blocks[i].MetadataLength);
                BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(24 * i + 16), blocks[i].BodyLength);
            }
            return bytes;
        }
    }

    /// <summary>Message: 0 version, 1 header_type, 2 header, 3 bodyLength, 4 custom_metadata. The header is left to patch.</summary>
    private static int MessageTable(FlatBufferWriter w, IpcMessageKind kind, long bodyLength, IReadOnlyList<KeyValuePair<string, string>>? metadata, out int header)
    {
        List<FlatBufferField> fields =
        [
            FlatBufferField.Short(0, (short)IpcMetadataVersion.V5), FlatBufferField.Byte(1, (byte)kind), FlatBufferField.Offset(2),
            FlatBufferField.Long(3, bodyLength),
        ];
        if (metadata is { Count: > 0 }) fields.Add(FlatBufferField.Offset(4));
        int message = w.Table([.. fields], out int[] at);
        header = at[2];
        if (metadata is { Count: > 0 }) w.Patch(at[4], KeyValues(w, metadata));
        return message;
    }

    private static int RecordBatchTable(FlatBufferWriter w, IpcRecordBatch batch)
    {
        // RecordBatch: 0 length, 1 nodes, 2 buffers, 3 compression, 4 variadicBufferCounts.
        List<FlatBufferField> fields = [FlatBufferField.Long(0, batch.Length), FlatBufferField.Offset(1), FlatBufferField.Offset(2)];
        if (batch.Compression != IpcCompression.None) fields.Add(FlatBufferField.Offset(3));
        if (batch.VariadicCounts.Length > 0) fields.Add(FlatBufferField.Offset(4));
        int table = w.Table([.. fields], out int[] at);
        byte[] nodes = new byte[16 * batch.Nodes.Length];
        for (int i = 0; i < batch.Nodes.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(nodes.AsSpan(16 * i), batch.Nodes[i].Length);
            BinaryPrimitives.WriteInt64LittleEndian(nodes.AsSpan(16 * i + 8), batch.Nodes[i].NullCount);
        }
        w.Patch(at[1], w.ValueVector(nodes, batch.Nodes.Length, 8));
        byte[] buffers = new byte[16 * batch.Buffers.Length];
        for (int i = 0; i < batch.Buffers.Length; i++)
        {
            BinaryPrimitives.WriteInt64LittleEndian(buffers.AsSpan(16 * i), batch.Buffers[i].Offset);
            BinaryPrimitives.WriteInt64LittleEndian(buffers.AsSpan(16 * i + 8), batch.Buffers[i].Length);
        }
        w.Patch(at[2], w.ValueVector(buffers, batch.Buffers.Length, 8));
        if (batch.Compression != IpcCompression.None)
        {
            // BodyCompression: 0 codec (LZ4_FRAME 0, ZSTD 1), 1 method (BUFFER 0).
            byte codec = batch.Compression == IpcCompression.Lz4Frame ? (byte)0 : (byte)1;
            w.Patch(at[3], w.Table([FlatBufferField.Byte(0, codec), FlatBufferField.Byte(1, 0)], out _));
        }
        if (batch.VariadicCounts.Length > 0)
        {
            byte[] counts = new byte[8 * batch.VariadicCounts.Length];
            for (int i = 0; i < batch.VariadicCounts.Length; i++) BinaryPrimitives.WriteInt64LittleEndian(counts.AsSpan(8 * i), batch.VariadicCounts[i]);
            w.Patch(at[4], w.ValueVector(counts, batch.VariadicCounts.Length, 8));
        }
        return table;
    }

    private static int SchemaTable(FlatBufferWriter w, IpcSchema schema)
    {
        // Schema: 0 endianness, 1 fields, 2 custom_metadata, 3 features.
        bool hasMetadata = schema.Schema.Metadata is { Count: > 0 };
        List<FlatBufferField> fields = [FlatBufferField.Short(0, 0), FlatBufferField.Offset(1)];
        if (hasMetadata) fields.Add(FlatBufferField.Offset(2));
        int table = w.Table([.. fields], out int[] at);
        w.Patch(at[1], Fields(w, schema.Fields));
        if (hasMetadata) w.Patch(at[2], KeyValues(w, [.. schema.Schema.Metadata]));
        return table;
    }

    private static int Fields(FlatBufferWriter w, IpcField[] fields)
    {
        int vector = w.OffsetVector(fields.Length, out int first);
        for (int i = 0; i < fields.Length; i++) w.Patch(first + 4 * i, Field(w, fields[i]));
        return vector;
    }

    private static int Field(FlatBufferWriter w, IpcField field)
    {
        // Field: 0 name, 1 nullable, 2 type_type, 3 type, 4 dictionary, 5 children, 6 custom_metadata.
        IArrowType type = field.Field.DataType is DictionaryType d ? d.ValueType : field.Field.DataType;
        byte tag = Tag(type, field.Field.Name);
        var metadata = field.Field.Metadata is { Count: > 0 } m ? m.ToList() : null;
        List<FlatBufferField> fields =
        [
            FlatBufferField.Offset(0), FlatBufferField.Bool(1, field.Field.IsNullable), FlatBufferField.Byte(2, tag), FlatBufferField.Offset(3),
            FlatBufferField.Offset(5),
        ];
        if (field.DictionaryId is not null) fields.Add(FlatBufferField.Offset(4));
        if (metadata is not null) fields.Add(FlatBufferField.Offset(6));
        int table = w.Table([.. fields], out int[] at);
        w.Patch(at[0], w.String(field.Field.Name));
        w.Patch(at[3], TypeTable(w, type));
        if (field.DictionaryId is long id)
        {
            var dictionary = (DictionaryType)field.Field.DataType;
            // DictionaryEncoding: 0 id, 1 indexType, 2 isOrdered, 3 dictionaryKind.
            int encoding = w.Table([FlatBufferField.Long(0, id), FlatBufferField.Offset(1), FlatBufferField.Bool(2, dictionary.Ordered), FlatBufferField.Short(3, 0)], out int[] e);
            w.Patch(at[4], encoding);
            var index = (IntegerType)dictionary.IndexType;
            w.Patch(e[1], w.Table([FlatBufferField.Int(0, index.BitWidth), FlatBufferField.Bool(1, index.IsSigned)], out _));
        }
        w.Patch(at[5], Fields(w, field.Children));
        if (metadata is not null) w.Patch(at[6], KeyValues(w, metadata));
        return table;
    }

    /// <summary>The tag of a type in Schema.fbs's Type union.</summary>
    private static byte Tag(IArrowType type, string name) => type switch
    {
        NullType => 1,
        IntegerType => 2,
        HalfFloatType or FloatType or DoubleType => 3,
        BinaryType => 4,
        StringType => 5,
        BooleanType => 6,
        Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type => 7,
        Date32Type or Date64Type => 8,
        Time32Type or Time64Type => 9,
        TimestampType => 10,
        IntervalType => 11,
        ListType => 12,
        StructType => 13,
        UnionType => 14,
        FixedSizeBinaryType => 15,
        FixedSizeListType => 16,
        MapType => 17,
        DurationType => 18,
        LargeBinaryType => 19,
        LargeStringType => 20,
        LargeListType => 21,
        RunEndEncodedType => 22,
        BinaryViewType => 23,
        StringViewType => 24,
        ListViewType => 25,
        LargeListViewType => 26,
        _ => throw new NotSupportedException($"Field '{name}' has type {type.Name}, which Arrow IPC cannot hold."),
    };

    private static int TypeTable(FlatBufferWriter w, IArrowType type)
    {
        FlatBufferField[] fields = type switch
        {
            IntegerType t => [FlatBufferField.Int(0, t.BitWidth), FlatBufferField.Bool(1, t.IsSigned)],
            HalfFloatType => [FlatBufferField.Short(0, 0)],
            FloatType => [FlatBufferField.Short(0, 1)],
            DoubleType => [FlatBufferField.Short(0, 2)],
            Decimal32Type t => [FlatBufferField.Int(0, t.Precision), FlatBufferField.Int(1, t.Scale), FlatBufferField.Int(2, 32)],
            Decimal64Type t => [FlatBufferField.Int(0, t.Precision), FlatBufferField.Int(1, t.Scale), FlatBufferField.Int(2, 64)],
            Decimal128Type t => [FlatBufferField.Int(0, t.Precision), FlatBufferField.Int(1, t.Scale), FlatBufferField.Int(2, 128)],
            Decimal256Type t => [FlatBufferField.Int(0, t.Precision), FlatBufferField.Int(1, t.Scale), FlatBufferField.Int(2, 256)],
            Date32Type => [FlatBufferField.Short(0, 0)],
            Date64Type => [FlatBufferField.Short(0, 1)],
            Time32Type t => [FlatBufferField.Short(0, Unit(t.Unit)), FlatBufferField.Int(1, 32)],
            Time64Type t => [FlatBufferField.Short(0, Unit(t.Unit)), FlatBufferField.Int(1, 64)],
            TimestampType t => t.Timezone is null ? [FlatBufferField.Short(0, Unit(t.Unit))] : [FlatBufferField.Short(0, Unit(t.Unit)), FlatBufferField.Offset(1)],
            DurationType t => [FlatBufferField.Short(0, Unit(t.Unit))],
            IntervalType t => [FlatBufferField.Short(0, t.Unit switch { IntervalUnit.YearMonth => 0, IntervalUnit.DayTime => 1, _ => 2 })],
            UnionType t => [FlatBufferField.Short(0, t.Mode == UnionMode.Dense ? (short)1 : (short)0), FlatBufferField.Offset(1)],
            FixedSizeBinaryType t => [FlatBufferField.Int(0, t.ByteWidth)],
            FixedSizeListType t => [FlatBufferField.Int(0, t.ListSize)],
            MapType t => [FlatBufferField.Bool(0, t.KeySorted)],
            _ => [],
        };
        int table = w.Table(fields, out int[] at);
        switch (type)
        {
            case TimestampType { Timezone: { } zone }:
                w.Patch(at[1], w.String(zone));
                break;
            case UnionType u:
            {
                byte[] ids = new byte[4 * u.TypeIds.Length];
                for (int i = 0; i < u.TypeIds.Length; i++) BinaryPrimitives.WriteInt32LittleEndian(ids.AsSpan(4 * i), u.TypeIds[i]);
                w.Patch(at[1], w.ValueVector(ids, u.TypeIds.Length, 4));
                break;
            }
        }
        return table;
    }

    private static short Unit(ArrowTimeUnit unit) => unit switch
    {
        ArrowTimeUnit.Second => 0,
        ArrowTimeUnit.Millisecond => 1,
        ArrowTimeUnit.Microsecond => 2,
        _ => 3,
    };

    private static int KeyValues(FlatBufferWriter w, IReadOnlyList<KeyValuePair<string, string>> pairs)
    {
        int vector = w.OffsetVector(pairs.Count, out int first);
        for (int i = 0; i < pairs.Count; i++)
        {
            // KeyValue: 0 key, 1 value.
            int table = w.Table([FlatBufferField.Offset(0), FlatBufferField.Offset(1)], out int[] at);
            w.Patch(first + 4 * i, table);
            w.Patch(at[0], w.String(pairs[i].Key));
            w.Patch(at[1], w.String(pairs[i].Value));
        }
        return vector;
    }
}
