using System.Buffers.Binary;
using System.Collections;
using System.Globalization;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Kaminari.Seseragi.Files.Parquet;

/// <summary>
/// Rows of nested values described by <see cref="ParquetNode"/>s (as <see cref="MinimalParquetWriter"/> takes them) as
/// an Arrow batch whose types <see cref="ParquetWriter"/> writes back to the same Parquet types: structs, LIST
/// (<c>element</c>), MAP of strings (<c>key_value</c>), and leaves annotated as strings, 8- and 16-bit integers, dates,
/// decimals (unscaled numbers, or big-endian two's complement bytes for FIXED_LEN_BYTE_ARRAY) and timestamps in
/// microseconds (instants in UTC, or local). Values a row does not name are null.
/// </summary>
internal static class ParquetNodeArrow
{
    /// <summary>The rows as one batch; each row is keyed by the names of the top-level fields.</summary>
    public static RecordBatch ToArrow(IReadOnlyList<ParquetNode> schema, IReadOnlyList<IReadOnlyDictionary<string, object?>> rows)
    {
        IArrowArray[] columns = [.. schema.Select(node => ArrowArrayFactory.BuildArray(Build(node, [.. rows.Select(row => Get(row, node.Name))])))];
        return new RecordBatch(ArrowSchema(schema), columns, rows.Count);
    }

    /// <summary>The Arrow schema of batches of rows of these nodes (the types <see cref="ToArrow"/> builds).</summary>
    public static Apache.Arrow.Schema ArrowSchema(IReadOnlyList<ParquetNode> schema) => new([.. schema.Select(FieldOf)], null);

    private static object? Get(IReadOnlyDictionary<string, object?> fields, string name) => fields.TryGetValue(name, out object? value) ? value : null;

    private static Field FieldOf(ParquetNode node) => new(node.Name, TypeOf(node), node.Repetition != Repetition.Required);

    private static IArrowType TypeOf(ParquetNode node) => node switch
    {
        ParquetLeaf leaf => LeafType(leaf),
        ParquetStruct s => new StructType([.. s.Children.Select(FieldOf)]),
        ParquetList l => new ListType(FieldOf(l.Element with { Name = "element" })),
        ParquetStringMap => new MapType(new Field("key", StringType.Default, false), new Field("value", StringType.Default, true)),
        _ => throw new NotSupportedException($"No Arrow type for the node {node}."),
    };

    private static IArrowType LeafType(ParquetLeaf leaf) => (leaf.Type, leaf.Annotation) switch
    {
        (_, Annotation.Decimal) => new Decimal128Type(leaf.Precision, leaf.Scale),
        (PhysicalType.Boolean, _) => BooleanType.Default,
        (PhysicalType.Int32, Annotation.Int8) => Int8Type.Default,
        (PhysicalType.Int32, Annotation.Int16) => Int16Type.Default,
        (PhysicalType.Int32, Annotation.Date) => Date32Type.Default,
        (PhysicalType.Int32, _) => Int32Type.Default,
        (PhysicalType.Int64, Annotation.TimestampMicros) => new TimestampType(TimeUnit.Microsecond, "UTC"),
        (PhysicalType.Int64, Annotation.LocalTimestampMicros) => new TimestampType(TimeUnit.Microsecond, (string?)null),
        (PhysicalType.Int64, _) => Int64Type.Default,
        (PhysicalType.Float, _) => FloatType.Default,
        (PhysicalType.Double, _) => DoubleType.Default,
        (PhysicalType.ByteArray, _) => leaf.Utf8 ? StringType.Default : BinaryType.Default,
        (PhysicalType.FixedLenByteArray, _) => new FixedSizeBinaryType(leaf.TypeLength),
        _ => throw new NotSupportedException($"No Arrow type for the leaf {leaf}."),
    };

    private static ArrayData Build(ParquetNode node, object?[] values) => node switch
    {
        ParquetLeaf leaf => Leaf(leaf, values),
        ParquetStruct s => Struct(s, values),
        ParquetList l => List(l, values),
        ParquetStringMap m => Map(m, values),
        _ => throw new NotSupportedException($"No Arrow array for the node {node}."),
    };

    /// <summary>The validity bitmap of the values (empty when none is null) and the null count.</summary>
    private static (ArrowBuffer Validity, int Nulls) Validity(object?[] values)
    {
        int nulls = values.Count(v => v is null);
        if (nulls == 0) return (ArrowBuffer.Empty, 0);
        byte[] bits = new byte[(values.Length + 7) / 8];
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is not null) bits[i >> 3] |= (byte)(1 << (i & 7));
        }
        return (new ArrowBuffer(bits), nulls);
    }

    private static ArrayData Struct(ParquetStruct node, object?[] values)
    {
        var (validity, nulls) = Validity(values);
        ArrayData[] children = [.. node.Children.Select(child =>
            Build(child, [.. values.Select(v => v is null ? null : Get((IReadOnlyDictionary<string, object?>)v, child.Name))]))];
        return new ArrayData(TypeOf(node), values.Length, nulls, 0, [validity], children);
    }

    private static ArrayData List(ParquetList node, object?[] values)
    {
        var (validity, nulls) = Validity(values);
        List<object?> items = [];
        int[] offsets = new int[values.Length + 1];
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is IEnumerable list) foreach (object? item in list) items.Add(item);
            offsets[i + 1] = items.Count;
        }
        ArrayData element = Build(node.Element with { Name = "element" }, [.. items]);
        return new ArrayData(TypeOf(node), values.Length, nulls, 0, [validity, Offsets(offsets)], [element]);
    }

    private static ArrayData Map(ParquetStringMap node, object?[] values)
    {
        var (validity, nulls) = Validity(values);
        List<object?> keys = [], items = [];
        int[] offsets = new int[values.Length + 1];
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is IEnumerable<KeyValuePair<string, string?>> map)
            {
                foreach (var (key, value) in map)
                {
                    keys.Add(key);
                    items.Add(value);
                }
            }
            offsets[i + 1] = keys.Count;
        }
        var type = (MapType)TypeOf(node);
        ArrayData entries = new(type.KeyValueType, keys.Count, 0, 0, [ArrowBuffer.Empty],
            [Strings(StringType.Default, [.. keys]), Strings(StringType.Default, [.. items])]);
        return new ArrayData(type, values.Length, nulls, 0, [validity, Offsets(offsets)], [entries]);
    }

    private static ArrowBuffer Offsets(int[] offsets)
    {
        byte[] bytes = new byte[offsets.Length * 4];
        for (int i = 0; i < offsets.Length; i++) BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(i * 4), offsets[i]);
        return new ArrowBuffer(bytes);
    }

    private static ArrayData Strings(IArrowType type, object?[] values)
    {
        var (validity, nulls) = Validity(values);
        int[] offsets = new int[values.Length + 1];
        using var data = new MemoryStream();
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is { } value) data.Write(value as byte[] ?? Encoding.UTF8.GetBytes((string)value));
            offsets[i + 1] = checked((int)data.Length);
        }
        return new ArrayData(type, values.Length, nulls, 0, [validity, Offsets(offsets), new ArrowBuffer(data.ToArray())]);
    }

    private static ArrayData Leaf(ParquetLeaf leaf, object?[] values)
    {
        IArrowType type = LeafType(leaf);
        if (type is StringType or BinaryType) return Strings(type, values);
        var (validity, nulls) = Validity(values);
        if (type is BooleanType)
        {
            byte[] bits = new byte[(values.Length + 7) / 8];
            for (int i = 0; i < values.Length; i++)
            {
                if (values[i] is true) bits[i >> 3] |= (byte)(1 << (i & 7));
            }
            return new ArrayData(type, values.Length, nulls, 0, [validity, new ArrowBuffer(bits)]);
        }
        int width = type switch
        {
            Int8Type => 1,
            Int16Type => 2,
            Int32Type or Date32Type or FloatType => 4,
            Int64Type or TimestampType or DoubleType => 8,
            Decimal128Type => 16,
            FixedSizeBinaryType f => f.ByteWidth,
            _ => throw new NotSupportedException($"No Arrow values for the leaf {leaf}."),
        };
        byte[] bytes = new byte[values.Length * width];
        for (int i = 0; i < values.Length; i++)
        {
            if (values[i] is not { } value) continue;
            Span<byte> at = bytes.AsSpan(i * width, width);
            switch (type)
            {
                case Int8Type: at[0] = (byte)Convert.ToSByte(value, CultureInfo.InvariantCulture); break;
                case Int16Type: BinaryPrimitives.WriteInt16LittleEndian(at, Convert.ToInt16(value, CultureInfo.InvariantCulture)); break;
                case Int32Type or Date32Type: BinaryPrimitives.WriteInt32LittleEndian(at, Convert.ToInt32(value, CultureInfo.InvariantCulture)); break;
                case FloatType: BinaryPrimitives.WriteSingleLittleEndian(at, Convert.ToSingle(value, CultureInfo.InvariantCulture)); break;
                case Int64Type or TimestampType: BinaryPrimitives.WriteInt64LittleEndian(at, Convert.ToInt64(value, CultureInfo.InvariantCulture)); break;
                case DoubleType: BinaryPrimitives.WriteDoubleLittleEndian(at, Convert.ToDouble(value, CultureInfo.InvariantCulture)); break;
                case Decimal128Type: BinaryPrimitives.WriteInt128LittleEndian(at, Unscaled(value, leaf.TypeLength)); break;
                default:
                    var fixedBytes = (byte[])value;
                    if (fixedBytes.Length != width) throw new InvalidOperationException($"The value of '{leaf.Name}' has {fixedBytes.Length} bytes, not {width}.");
                    fixedBytes.CopyTo(at);
                    break;
            }
        }
        return new ArrayData(type, values.Length, nulls, 0, [validity, new ArrowBuffer(bytes)]);
    }

    /// <summary>An unscaled decimal: a number, or big-endian two's complement bytes (sign-extended).</summary>
    private static Int128 Unscaled(object value, int typeLength)
    {
        if (value is not byte[] bytes) return Convert.ToInt64(value, CultureInfo.InvariantCulture);
        if (bytes.Length != typeLength) throw new InvalidOperationException($"A decimal of {bytes.Length} bytes, not {typeLength}.");
        Int128 unscaled = bytes.Length > 0 && (sbyte)bytes[0] < 0 ? Int128.NegativeOne : Int128.Zero;
        foreach (byte b in bytes) unscaled = (unscaled << 8) | b;
        return unscaled;
    }
}
