using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Seseragi.Files.Parquet.Metadata;
using Kaminari.Seseragi.Files.Parquet.Schema;
using Kaminari.Sazanami.Arrow;

namespace Kaminari.Seseragi.Files.Parquet.Writing;

/// <summary>Reads Arrow arrays as the writer needs them: raw values, nulls, dictionary indices.</summary>
internal static class ArrowValues
{
    /// <summary>The null count, or <see cref="int.MaxValue"/> when nulls must be looked for row by row (a dictionary with null values).</summary>
    public static int NullCount(IArrowArray a) => a is DictionaryArray { Dictionary.NullCount: > 0 } ? int.MaxValue : a.NullCount;

    /// <summary>Whether row <paramref name="i"/> is null (in a dictionary column: its index, or the value it points to).</summary>
    public static bool IsNull(IArrowArray a, int i) => a.IsNull(i) || (a is DictionaryArray d && d.Dictionary.IsNull(Index(d, i)));

    /// <summary>The values buffer of a fixed-width array, from its first row.</summary>
    public static ReadOnlySpan<T> Values<T>(IArrowArray a) where T : unmanaged =>
        MemoryMarshal.Cast<byte, T>(a.Data.Buffers[1].Span).Slice(a.Data.Offset, a.Length);

    /// <summary>The fixed-width bytes of row <paramref name="i"/>.</summary>
    public static ReadOnlySpan<byte> Raw(IArrowArray a, int i, int width) => a.Data.Buffers[1].Span.Slice((a.Data.Offset + i) * width, width);

    public static int Index(DictionaryArray d, int i) => d.Indices.Data.DataType.TypeId switch
    {
        ArrowTypeId.Int8 => Values<sbyte>(d.Indices)[i],
        ArrowTypeId.UInt8 => Values<byte>(d.Indices)[i],
        ArrowTypeId.Int16 => Values<short>(d.Indices)[i],
        ArrowTypeId.UInt16 => Values<ushort>(d.Indices)[i],
        ArrowTypeId.Int32 => Values<int>(d.Indices)[i],
        ArrowTypeId.UInt32 => checked((int)Values<uint>(d.Indices)[i]),
        ArrowTypeId.Int64 => checked((int)Values<long>(d.Indices)[i]),
        ArrowTypeId.UInt64 => checked((int)Values<ulong>(d.Indices)[i]),
        _ => throw new NotSupportedException($"Dictionary indices of type {d.Indices.Data.DataType.Name} are not supported."),
    };
}

/// <summary>INT96: nanoseconds of the day and the Julian day, little-endian.</summary>
[StructLayout(LayoutKind.Sequential, Pack = 1)]
internal readonly record struct Int96(long NanosOfDay, int JulianDay);

/// <summary>A leaf column: its schema element and how to make its writer.</summary>
internal sealed record LeafPlan(SchemaElement Element, Func<PageSettings, ColumnWriter> CreateWriter);

/// <summary>
/// A leaf column of the file: the top-level field it belongs to, its path in the Parquet schema, its levels, and the
/// steps from the top-level array to its values (null for a flat column, written without shredding).
/// </summary>
internal sealed record ColumnPlan(int Field, string[] Path, ShredStep[]? Steps, LeafPlan Leaf);

/// <summary>
/// Maps Arrow fields to Parquet columns (the inverse of the reader's mapping): physical type, annotations, how
/// values are converted, and how statistics order them. Values Parquet cannot hold exactly are refused with
/// <see cref="NotSupportedException"/>, never changed.
/// </summary>
internal static class WriteSchema
{
    private const long MillisPerDay = 86_400_000;
    private const long NanosPerDay = 86_400_000_000_000;
    private const int JulianEpochDay = 2_440_588;

    /// <summary>
    /// The Parquet schema of an Arrow schema (depth first, without the root) and its leaf columns. Structs become
    /// groups; lists of every layout three-level LIST groups (<c>list</c> / <c>element</c>); maps MAP groups
    /// (<c>key_value</c> / <c>key</c> / <c>value</c>, keys required); dictionaries of nested values and run-end encoded
    /// arrays the type of their values. Field ids come from the Arrow fields' <c>PARQUET:field_id</c>.
    /// </summary>
    public static (SchemaElement[] Elements, ColumnPlan[] Columns) Plan(Apache.Arrow.Schema schema, ParquetWriteOptions options)
    {
        List<SchemaElement> elements = [];
        List<ColumnPlan> columns = [];
        for (int f = 0; f < schema.FieldsList.Count; f++)
        {
            Field field = schema.FieldsList[f];
            Node(f, field, field.DataType, field.Name, field.IsNullable, ArrowFieldIds.Of(field), 0, 0, [field.Name], [], field.Name);
        }
        return ([.. elements], [.. columns]);

        void Node(int top, Field field, IArrowType type, string name, bool optional, int? id, int def, int rep, string[] path, ShredStep[] steps, string where)
        {
            optional |= type is NullType;
            FieldRepetition repetition = optional ? FieldRepetition.Optional : FieldRepetition.Required;
            int below = def + (optional ? 1 : 0);
            switch (type)
            {
                case StructType st:
                    if (st.Fields.Count == 0)
                        throw new NotSupportedException($"Column '{where}' is a struct without fields, which Parquet cannot hold (a group needs a field).");
                    elements.Add(new SchemaElement { Name = name, Repetition = repetition, NumChildren = st.Fields.Count, FieldId = id });
                    for (int c = 0; c < st.Fields.Count; c++)
                    {
                        Field child = st.Fields[c];
                        Node(top, child, child.DataType, child.Name, child.IsNullable, ArrowFieldIds.Of(child), below, rep, [.. path, child.Name],
                            [.. steps, new StructStep(c, optional, where)], $"{where}.{child.Name}");
                    }
                    return;
                case ListType or LargeListType or FixedSizeListType or ListViewType or LargeListViewType:
                {
                    var (layout, size, item) = type switch
                    {
                        ListType l => (ListLayout.List, 0, l.ValueField),
                        LargeListType l => (ListLayout.LargeList, 0, l.ValueField),
                        FixedSizeListType l => (ListLayout.FixedSize, l.ListSize, l.ValueField),
                        ListViewType l => (ListLayout.View, 0, l.ValueField),
                        _ => (ListLayout.LargeView, 0, ((LargeListViewType)type).ValueField),
                    };
                    elements.Add(new SchemaElement
                    {
                        Name = name, Repetition = repetition, NumChildren = 1, FieldId = id,
                        LogicalType = new LogicalType(LogicalTypeKind.List), ConvertedType = ConvertedType.List,
                    });
                    elements.Add(new SchemaElement { Name = "list", Repetition = FieldRepetition.Repeated, NumChildren = 1 });
                    Node(top, item, item.DataType, "element", item.IsNullable, ArrowFieldIds.Of(item), below + 1, rep + 1, [.. path, "list", "element"],
                        [.. steps, new ListStep(layout, size, rep + 1, optional, where)], where + "[]");
                    return;
                }
                case MapType map:
                {
                    elements.Add(new SchemaElement
                    {
                        Name = name, Repetition = repetition, NumChildren = 1, FieldId = id,
                        LogicalType = new LogicalType(LogicalTypeKind.Map), ConvertedType = ConvertedType.Map,
                    });
                    elements.Add(new SchemaElement { Name = "key_value", Repetition = FieldRepetition.Repeated, NumChildren = 2 });
                    var entries = new ListStep(ListLayout.List, 0, rep + 1, optional, where);
                    Node(top, map.KeyField, map.KeyField.DataType, "key", false, ArrowFieldIds.Of(map.KeyField), below + 1, rep + 1, [.. path, "key_value", "key"],
                        [.. steps, entries, new StructStep(0, false, where)], where + ".key");
                    Node(top, map.ValueField, map.ValueField.DataType, "value", map.ValueField.IsNullable, ArrowFieldIds.Of(map.ValueField), below + 1, rep + 1,
                        [.. path, "key_value", "value"], [.. steps, entries, new StructStep(1, false, where)], where + ".value");
                    return;
                }
                case DictionaryType { ValueType: NestedType } dictionary:
                    Node(top, field, dictionary.ValueType, name, optional, id, def, rep, path, [.. steps, new RemapStep(optional, where)], where);
                    return;
                case RunEndEncodedType runEnds:
                    Node(top, field, runEnds.ValuesDataType, name, optional, id, def, rep, path, [.. steps, new RemapStep(optional, where)], where);
                    return;
                case UnionType:
                    throw new NotSupportedException($"Column '{where}' is a union, which Parquet cannot hold.");
                default:
                {
                    LeafPlan leaf = Leaf(name, type, optional, id, below, rep, options, where);
                    elements.Add(leaf.Element);
                    columns.Add(new ColumnPlan(top, path, steps.Length == 0 ? null : [.. steps, new LeafStep(optional, where)], leaf));
                    return;
                }
            }
        }
    }

    /// <summary>A leaf of <paramref name="fieldType"/> (a dictionary column by its values), at levels <paramref name="maxDef"/> / <paramref name="maxRep"/>.</summary>
    private static LeafPlan Leaf(string name, IArrowType fieldType, bool optional, int? id, int maxDef, int maxRep, ParquetWriteOptions options, string where)
    {
        bool dictionary = fieldType is DictionaryType;
        IArrowType type = fieldType is DictionaryType d ? d.ValueType : fieldType;
        FieldRepetition repetition = optional ? FieldRepetition.Optional : FieldRepetition.Required;

        SchemaElement Element(ParquetType physical, LogicalType? logical = null, ConvertedType? converted = null, int? length = null,
            int? scale = null, int? precision = null) => new()
        {
            Name = name, Type = physical, Repetition = repetition, LogicalType = logical, ConvertedType = converted,
            TypeLength = length, Scale = scale, Precision = precision, FieldId = id,
        };

        LeafPlan Int32(FixedSource<int> source, SchemaElement element, bool unsigned = false, bool legacy = true) => unsigned
            ? new(element, s => new FixedColumnWriter<int, UnsignedOrder32>(s, maxDef, maxRep, Fixed(source, dictionary), default))
            : new(element, s => new FixedColumnWriter<int, SignedOrder<int>>(s, maxDef, maxRep, Fixed(source, dictionary), new SignedOrder<int>(legacy)));

        LeafPlan Int64(FixedSource<long> source, SchemaElement element, bool unsigned = false, bool legacy = true) => unsigned
            ? new(element, s => new FixedColumnWriter<long, UnsignedOrder64>(s, maxDef, maxRep, Fixed(source, dictionary), default))
            : new(element, s => new FixedColumnWriter<long, SignedOrder<long>>(s, maxDef, maxRep, Fixed(source, dictionary), new SignedOrder<long>(legacy)));

        LeafPlan Bytes(BytesSource source, SchemaElement element, int length, BytesOrder order, bool utf8 = false) =>
            new(element, s => new BytesColumnWriter(s, maxDef, maxRep, dictionary ? DictionaryBytes(source) : source, length, order, utf8, where));

        return type switch
        {
            NullType => Int32(static (_, _, _, d) => d.Clear(), Element(ParquetType.Int32, new LogicalType(LogicalTypeKind.Unknown))),
            BooleanType when !dictionary => new(Element(ParquetType.Boolean), s => new BooleanColumnWriter(s, maxDef, maxRep)),
            Int8Type => Int32(Widen<sbyte>, Element(ParquetType.Int32, Integer(8, true), ConvertedType.Int8)),
            Int16Type => Int32(Widen<short>, Element(ParquetType.Int32, Integer(16, true), ConvertedType.Int16)),
            Int32Type => Int32(Copy<int>, Element(ParquetType.Int32)),
            UInt8Type => Int32(Widen<byte>, Element(ParquetType.Int32, Integer(8, false), ConvertedType.UInt8), unsigned: true),
            UInt16Type => Int32(Widen<ushort>, Element(ParquetType.Int32, Integer(16, false), ConvertedType.UInt16), unsigned: true),
            UInt32Type => Int32(Copy<int>, Element(ParquetType.Int32, Integer(32, false), ConvertedType.UInt32), unsigned: true),
            Int64Type => Int64(Copy<long>, Element(ParquetType.Int64)),
            UInt64Type => Int64(Copy<long>, Element(ParquetType.Int64, Integer(64, false), ConvertedType.UInt64), unsigned: true),
            FloatType => new(Element(ParquetType.Float), s => new FixedColumnWriter<float, FloatOrder>(s, maxDef, maxRep, Fixed<float>(Copy<float>, dictionary), default)),
            DoubleType => new(Element(ParquetType.Double), s => new FixedColumnWriter<double, DoubleOrder>(s, maxDef, maxRep, Fixed<double>(Copy<double>, dictionary), default)),
            HalfFloatType => Bytes(static (a, i, _) => ArrowValues.Raw(a, i, 2), Element(ParquetType.FixedLenByteArray, new LogicalType(LogicalTypeKind.Float16), length: 2),
                2, BytesOrder.Float16),
            Date32Type => Int32(Copy<int>, Element(ParquetType.Int32, new LogicalType(LogicalTypeKind.Date), ConvertedType.Date)),
            Date64Type => Int32((a, o, n, dest) => Date64(a, o, n, dest, where), Element(ParquetType.Int32, new LogicalType(LogicalTypeKind.Date), ConvertedType.Date)),
            Time32Type t => Int32(t.Unit == TimeUnit.Second ? static (a, o, n, dest) => Scale(a, o, n, dest, 1000) : Copy<int>,
                Element(ParquetType.Int32, Time(ParquetTimeUnit.Millis))),
            Time64Type t => Int64(Copy<long>, Element(ParquetType.Int64, Time(t.Unit == TimeUnit.Nanosecond ? ParquetTimeUnit.Nanos : ParquetTimeUnit.Micros))),
            TimestampType t when options.Int96Timestamps => new(Element(ParquetType.Int96), s => new FixedColumnWriter<Int96, NoOrder<Int96>>(s, maxDef, maxRep,
                Fixed<Int96>((a, o, n, dest) => ToInt96(a, o, n, dest, t.Unit, where), dictionary), default)),
            TimestampType t => TimestampPlan(t),
            DurationType => Int64(Copy<long>, Element(ParquetType.Int64)),
            Decimal32Type or Decimal64Type or Decimal128Type or Decimal256Type => DecimalPlan(),
            FixedSizeBinaryType f => Bytes((a, i, _) => ArrowValues.Raw(a, i, f.ByteWidth), Element(ParquetType.FixedLenByteArray, length: f.ByteWidth), f.ByteWidth, BytesOrder.Unsigned),
            StringType => Bytes(static (a, i, _) => ((BinaryArray)a).GetBytes(i), Element(ParquetType.ByteArray, new LogicalType(LogicalTypeKind.String), ConvertedType.Utf8),
                0, BytesOrder.Unsigned, utf8: true),
            LargeStringType => Bytes(static (a, i, _) => ((LargeBinaryArray)a).GetBytes(i), Element(ParquetType.ByteArray, new LogicalType(LogicalTypeKind.String), ConvertedType.Utf8),
                0, BytesOrder.Unsigned, utf8: true),
            StringViewType => Bytes(static (a, i, _) => ((BinaryViewArray)a).GetBytes(i), Element(ParquetType.ByteArray, new LogicalType(LogicalTypeKind.String), ConvertedType.Utf8),
                0, BytesOrder.Unsigned, utf8: true),
            BinaryType => Bytes(static (a, i, _) => ((BinaryArray)a).GetBytes(i), Element(ParquetType.ByteArray), 0, BytesOrder.Unsigned),
            LargeBinaryType => Bytes(static (a, i, _) => ((LargeBinaryArray)a).GetBytes(i), Element(ParquetType.ByteArray), 0, BytesOrder.Unsigned),
            BinaryViewType => Bytes(static (a, i, _) => ((BinaryViewArray)a).GetBytes(i), Element(ParquetType.ByteArray), 0, BytesOrder.Unsigned),
            IntervalType interval => Bytes((a, i, scratch) => Interval(a, i, scratch, interval.Unit, where),
                Element(ParquetType.FixedLenByteArray, converted: ConvertedType.Interval, length: 12), 12, BytesOrder.None),
            _ => throw new NotSupportedException($"Column '{where}' is {fieldType.Name}, which this writer does not write."),
        };

        LeafPlan TimestampPlan(TimestampType t)
        {
            TimeUnit stored = options.TimestampUnit ?? (t.Unit == TimeUnit.Second ? TimeUnit.Millisecond : t.Unit);
            bool utc = !string.IsNullOrEmpty(t.Timezone);
            ParquetTimeUnit unit = stored switch
            {
                TimeUnit.Millisecond => ParquetTimeUnit.Millis,
                TimeUnit.Microsecond => ParquetTimeUnit.Micros,
                _ => ParquetTimeUnit.Nanos,
            };
            // The legacy annotations mean UTC-adjusted milliseconds or microseconds (as Arrow C++ writes them).
            ConvertedType? converted = utc && unit != ParquetTimeUnit.Nanos
                ? (unit == ParquetTimeUnit.Millis ? ConvertedType.TimestampMillis : ConvertedType.TimestampMicros) : null;
            FixedSource<long> source = t.Unit == stored ? Copy<long> : (a, o, n, dest) => Rescale(a, o, n, dest, t.Unit, stored, where);
            return Int64(source, Element(ParquetType.Int64, Timestamp(utc, unit), converted));
        }

        LeafPlan DecimalPlan()
        {
            var decimalType = (FixedSizeBinaryType)type;
            var (precision, scale) = DecimalParameters(type);
            int width = decimalType.ByteWidth;
            var logical = new LogicalType(LogicalTypeKind.Decimal) { Precision = precision, Scale = scale };
            if (precision <= 9)
                return Int32((a, o, n, dest) => DecimalToInt32(a, o, n, dest, width, precision, where),
                    Element(ParquetType.Int32, logical, ConvertedType.Decimal, scale: scale, precision: precision), legacy: false);
            if (precision <= 18)
                return Int64((a, o, n, dest) => DecimalToInt64(a, o, n, dest, width, precision, where),
                    Element(ParquetType.Int64, logical, ConvertedType.Decimal, scale: scale, precision: precision), legacy: false);
            int bytes = DecimalBytes(precision);
            return Bytes((a, i, scratch) => DecimalToBigEndian(a, i, scratch, width, bytes, precision, where),
                Element(ParquetType.FixedLenByteArray, logical, ConvertedType.Decimal, bytes, scale, precision), bytes, BytesOrder.SignedDecimal);
        }
    }

    /// <summary>The precision and scale of a decimal type.</summary>
    public static (int Precision, int Scale) DecimalParameters(IArrowType type) => type switch
    {
        Decimal32Type d => (d.Precision, d.Scale),
        Decimal64Type d => (d.Precision, d.Scale),
        Decimal128Type d => (d.Precision, d.Scale),
        Decimal256Type d => (d.Precision, d.Scale),
        _ => throw new ArgumentException($"{type.Name} is not a decimal type.", nameof(type)),
    };

    private static LogicalType Integer(int bits, bool signed) => new(LogicalTypeKind.Integer) { BitWidth = (sbyte)bits, IsSigned = signed };

    private static LogicalType Time(ParquetTimeUnit unit) => new(LogicalTypeKind.Time) { IsAdjustedToUtc = false, Unit = unit, UnitMember = UnitMember(unit) };

    private static LogicalType Timestamp(bool utc, ParquetTimeUnit unit) => new(LogicalTypeKind.Timestamp) { IsAdjustedToUtc = utc, Unit = unit, UnitMember = UnitMember(unit) };

    private static short UnitMember(ParquetTimeUnit unit) => unit switch
    {
        ParquetTimeUnit.Millis => 1,
        ParquetTimeUnit.Micros => 2,
        _ => 3,
    };

    /// <summary>The fewest bytes whose two's complement holds every decimal of <paramref name="precision"/> digits.</summary>
    public static int DecimalBytes(int precision)
    {
        int bytes = 1;
        while (LeafTypes.MaxDigits(bytes) < precision) bytes++;
        return bytes;
    }

    // ------------------------------------------------------------------
    // Sources (null rows may hold anything: checks that fail look at the row's validity first)
    // ------------------------------------------------------------------

    /// <summary>A fixed source for a dictionary column: the dictionary's values converted once per dictionary, gathered by index.</summary>
    private static FixedSource<T> Fixed<T>(FixedSource<T> values, bool dictionary) where T : unmanaged
    {
        if (!dictionary) return values;
        IArrowArray? converted = null;
        T[] cache = [];
        return (array, offset, count, destination) =>
        {
            var d = (DictionaryArray)array;
            if (!ReferenceEquals(converted, d.Dictionary))
            {
                cache = new T[d.Dictionary.Length];
                values(d.Dictionary, 0, cache.Length, cache);
                converted = d.Dictionary;
            }
            for (int i = 0; i < count; i++) destination[i] = d.IsNull(offset + i) ? default : cache[ArrowValues.Index(d, offset + i)];
        };
    }

    private static BytesSource DictionaryBytes(BytesSource values) => (array, i, scratch) =>
    {
        var d = (DictionaryArray)array;
        return values(d.Dictionary, ArrowValues.Index(d, i), scratch);
    };

    private static void Copy<T>(IArrowArray a, int offset, int count, Span<T> destination) where T : unmanaged =>
        ArrowValues.Values<T>(a).Slice(offset, count).CopyTo(destination);

    private static void Widen<T>(IArrowArray a, int offset, int count, Span<int> destination) where T : unmanaged, IBinaryInteger<T>
    {
        ReadOnlySpan<T> values = ArrowValues.Values<T>(a).Slice(offset, count);
        for (int i = 0; i < values.Length; i++) destination[i] = int.CreateTruncating(values[i]);
    }

    private static void Scale(IArrowArray a, int offset, int count, Span<int> destination, int factor)
    {
        ReadOnlySpan<int> values = ArrowValues.Values<int>(a).Slice(offset, count);
        for (int i = 0; i < values.Length; i++) destination[i] = unchecked(values[i] * factor);
    }

    private static void Date64(IArrowArray a, int offset, int count, Span<int> destination, string column)
    {
        ReadOnlySpan<long> values = ArrowValues.Values<long>(a).Slice(offset, count);
        for (int i = 0; i < values.Length; i++)
        {
            long ms = values[i];
            long days = ms / MillisPerDay;
            if ((ms % MillisPerDay != 0 || days != (int)days) && !a.IsNull(offset + i))
                throw new NotSupportedException($"Column '{column}' holds a date64 value ({ms} ms) that is not a whole day of the int32 range; Parquet stores dates in days.");
            destination[i] = (int)days;
        }
    }

    private static long NanosPer(TimeUnit unit) => LeafTypes.Nanos(unit);

    private static void Rescale(IArrowArray a, int offset, int count, Span<long> destination, TimeUnit from, TimeUnit to, string column)
    {
        ReadOnlySpan<long> values = ArrowValues.Values<long>(a).Slice(offset, count);
        long f = NanosPer(from), t = NanosPer(to);
        for (int i = 0; i < values.Length; i++)
        {
            long v = values[i], result;
            bool exact;
            if (f >= t)
            {
                long factor = f / t;
                result = unchecked(v * factor);
                exact = factor == 1 || (v != 0 && result / factor == v) || v == 0;
            }
            else
            {
                long divisor = t / f;
                result = v / divisor;
                exact = v % divisor == 0;
            }
            if (!exact && !a.IsNull(offset + i))
                throw new NotSupportedException($"Column '{column}' holds a timestamp ({v} {from}) that {to} cannot hold exactly.");
            destination[i] = result;
        }
    }

    private static void ToInt96(IArrowArray a, int offset, int count, Span<Int96> destination, TimeUnit unit, string column)
    {
        ReadOnlySpan<long> values = ArrowValues.Values<long>(a).Slice(offset, count);
        long factor = NanosPer(unit);
        for (int i = 0; i < values.Length; i++)
        {
            long v = values[i];
            long nanos = unchecked(v * factor);
            if (factor != 1 && v != 0 && nanos / factor != v)
            {
                if (!a.IsNull(offset + i)) throw new NotSupportedException($"Column '{column}' holds a timestamp ({v} {unit}) beyond the range of INT96 nanoseconds.");
                nanos = 0;
            }
            long day = Math.DivRem(nanos, NanosPerDay, out long ofDay);
            if (ofDay < 0)
            {
                day--;
                ofDay += NanosPerDay;
            }
            destination[i] = new Int96(ofDay, (int)(day + JulianEpochDay));
        }
    }

    private static void DecimalToInt32(IArrowArray a, int offset, int count, Span<int> destination, int width, int precision, string column)
    {
        long limit = Pow10(precision);
        for (int i = 0; i < count; i++)
        {
            ReadOnlySpan<byte> raw = ArrowValues.Raw(a, offset + i, width);
            int value = BinaryPrimitives.ReadInt32LittleEndian(raw);
            if ((!SignExtends(raw, 4, value < 0) || Math.Abs((long)value) >= limit) && !a.IsNull(offset + i)) throw DecimalTooWide(column);
            destination[i] = value;
        }
    }

    private static void DecimalToInt64(IArrowArray a, int offset, int count, Span<long> destination, int width, int precision, string column)
    {
        long limit = Pow10(precision);
        for (int i = 0; i < count; i++)
        {
            ReadOnlySpan<byte> raw = ArrowValues.Raw(a, offset + i, width);
            long value = width == 4 ? BinaryPrimitives.ReadInt32LittleEndian(raw) : BinaryPrimitives.ReadInt64LittleEndian(raw);
            if (((width > 8 && !SignExtends(raw, 8, value < 0)) || value <= -limit || value >= limit) && !a.IsNull(offset + i)) throw DecimalTooWide(column);
            destination[i] = value;
        }
    }

    private static ReadOnlySpan<byte> DecimalToBigEndian(IArrowArray a, int i, Span<byte> scratch, int width, int bytes, int precision, string column)
    {
        ReadOnlySpan<byte> raw = ArrowValues.Raw(a, i, width);
        bool negative = (raw[^1] & 0x80) != 0;
        if (!WithinPrecision(raw, precision)) throw DecimalTooWide(column);
        Span<byte> output = scratch[..bytes];
        for (int k = 0; k < bytes; k++) output[k] = k < width ? raw[bytes - 1 - k] : (negative ? (byte)0xFF : (byte)0);
        return output;
    }

    private static long Pow10(int digits)
    {
        long p = 1;
        for (int k = 0; k < digits; k++) p *= 10;
        return p;
    }

    /// <summary>Whether a little-endian two's complement decimal has at most <paramref name="precision"/> digits.</summary>
    private static bool WithinPrecision(ReadOnlySpan<byte> raw, int precision)
    {
        if (raw.Length == 16)
        {
            Int128 value = BinaryPrimitives.ReadInt128LittleEndian(raw), limit = Int128.One;
            for (int k = 0; k < precision; k++) limit *= 10;
            return value > -limit && value < limit;
        }
        return BigInteger.Abs(new BigInteger(raw, isUnsigned: false, isBigEndian: false)) < BigInteger.Pow(10, precision);
    }

    /// <summary>Whether the bytes from <paramref name="from"/> on are all the sign extension of the value below them.</summary>
    private static bool SignExtends(ReadOnlySpan<byte> raw, int from, bool negative)
    {
        byte fill = negative ? (byte)0xFF : (byte)0;
        foreach (byte b in raw[from..])
        {
            if (b != fill) return false;
        }
        return true;
    }

    private static NotSupportedException DecimalTooWide(string column) =>
        new($"Column '{column}' holds a decimal with more digits than its precision; Parquet stores it in a width the precision fixes.");

    private static ReadOnlySpan<byte> Interval(IArrowArray a, int i, Span<byte> scratch, IntervalUnit unit, string column)
    {
        ReadOnlySpan<byte> raw = ArrowValues.Raw(a, i, unit switch { IntervalUnit.YearMonth => 4, IntervalUnit.DayTime => 8, _ => 16 });
        (long months, long days, long millis) = unit switch
        {
            IntervalUnit.YearMonth => ((long)BinaryPrimitives.ReadInt32LittleEndian(raw), 0L, 0L),
            IntervalUnit.DayTime => (0L, (long)BinaryPrimitives.ReadInt32LittleEndian(raw), (long)BinaryPrimitives.ReadInt32LittleEndian(raw[4..])),
            _ when BinaryPrimitives.ReadInt64LittleEndian(raw[8..]) % 1_000_000 == 0 =>
                ((long)BinaryPrimitives.ReadInt32LittleEndian(raw), (long)BinaryPrimitives.ReadInt32LittleEndian(raw[4..]), BinaryPrimitives.ReadInt64LittleEndian(raw[8..]) / 1_000_000),
            _ => throw IntervalInexact(column),
        };
        if (months < 0 || days < 0 || millis is < 0 or > uint.MaxValue) throw IntervalInexact(column);
        Span<byte> output = scratch[..12];
        BinaryPrimitives.WriteUInt32LittleEndian(output, (uint)months);
        BinaryPrimitives.WriteUInt32LittleEndian(output[4..], (uint)days);
        BinaryPrimitives.WriteUInt32LittleEndian(output[8..], (uint)millis);
        return output;
    }

    private static NotSupportedException IntervalInexact(string column) =>
        new($"Column '{column}' holds an interval Parquet cannot hold: INTERVAL is unsigned months, days and milliseconds.");
}
