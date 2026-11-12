using System.Data.SqlTypes;
using System.Runtime.CompilerServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;

namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// One array of a row view with its layout: typed reads by index, and the nested layouts (struct rows, list items,
/// map entries, dictionary values) built on first use. Indexes are logical indexes into <see cref="Array"/>.
/// </summary>
internal sealed class ArrowRowNode(IArrowArray array, Field field, string path, string? fieldIdKey)
{
    private ArrowRows? _structRows;
    private ArrowRowNode? _items;
    private ArrowRowNode? _keys;
    private ArrowRowNode? _values;

    public IArrowArray Array { get; } = array;

    public Field Field { get; } = field;

    /// <summary>Dotted path from the root, for error messages.</summary>
    public string Path { get; } = path;

    public ArrowTypeId TypeId { get; } = array.Data.DataType.TypeId;

    public int? FieldId { get; } = fieldIdKey is null ? null : ArrowFieldIds.Of(field, fieldIdKey);

    public bool IsNull(int index) => Array.IsNull(index);

    // ---- typed reads -------------------------------------------------------------------------------------------

    /// <summary>Reads the value at <paramref name="index"/> as <typeparamref name="T"/> (a null as <see cref="Null{T}"/>).</summary>
    public T Read<T>(int index)
    {
        if (Array.IsNull(index)) return Null<T>(index);

        if (typeof(T) == typeof(long) || typeof(T) == typeof(long?)) return As<long, T>(ReadInt64(index, typeof(T)));
        if (typeof(T) == typeof(int) || typeof(T) == typeof(int?)) return As<int, T>(ReadInt32(index, typeof(T)));
        if (typeof(T) == typeof(string)) return (T)(object)ReadString(index);
        if (typeof(T) == typeof(bool) || typeof(T) == typeof(bool?)) return As<bool, T>(ReadBoolean(index, typeof(T)));
        if (typeof(T) == typeof(double) || typeof(T) == typeof(double?)) return As<double, T>(ReadDouble(index, typeof(T)));
        if (typeof(T) == typeof(float) || typeof(T) == typeof(float?)) return As<float, T>(ReadSingle(index, typeof(T)));
        if (typeof(T) == typeof(byte[])) return (T)(object)GetBytes(index).ToArray();
        if (typeof(T) == typeof(ArrowRow)) return As<ArrowRow, T>(new ArrowRow(StructRows(), index));
        if (typeof(T) == typeof(short) || typeof(T) == typeof(short?)) return As<short, T>(checked((short)ReadInteger(index, typeof(T), IntegerWidth.Int16)));
        if (typeof(T) == typeof(sbyte) || typeof(T) == typeof(sbyte?)) return As<sbyte, T>(checked((sbyte)ReadInteger(index, typeof(T), IntegerWidth.Int8)));
        if (typeof(T) == typeof(byte) || typeof(T) == typeof(byte?)) return As<byte, T>(checked((byte)ReadUnsigned(index, typeof(T), IntegerWidth.Int8)));
        if (typeof(T) == typeof(ushort) || typeof(T) == typeof(ushort?)) return As<ushort, T>(checked((ushort)ReadUnsigned(index, typeof(T), IntegerWidth.Int16)));
        if (typeof(T) == typeof(uint) || typeof(T) == typeof(uint?)) return As<uint, T>(checked((uint)ReadUnsigned(index, typeof(T), IntegerWidth.Int32)));
        if (typeof(T) == typeof(ulong) || typeof(T) == typeof(ulong?)) return As<ulong, T>(ReadUnsigned(index, typeof(T), IntegerWidth.Int64));
        if (typeof(T) == typeof(Half) || typeof(T) == typeof(Half?)) return As<Half, T>(ReadHalf(index, typeof(T)));
        if (typeof(T) == typeof(decimal) || typeof(T) == typeof(decimal?)) return As<decimal, T>(ReadDecimal(index, typeof(T)));
        if (typeof(T) == typeof(DateOnly) || typeof(T) == typeof(DateOnly?)) return As<DateOnly, T>(ReadDate(index, typeof(T)));
        if (typeof(T) == typeof(DateTimeOffset) || typeof(T) == typeof(DateTimeOffset?)) return As<DateTimeOffset, T>(ReadTimestamp(index, typeof(T)));
        if (typeof(T) == typeof(DateTime) || typeof(T) == typeof(DateTime?)) return As<DateTime, T>(ReadDateTime(index, typeof(T)));
        if (typeof(T) == typeof(Guid) || typeof(T) == typeof(Guid?)) return As<Guid, T>(ReadGuid(index, typeof(T)));
        if (typeof(T) == typeof(SqlDecimal) || typeof(T) == typeof(SqlDecimal?)) return As<SqlDecimal, T>(ReadSqlDecimal(index, typeof(T)));
        throw new NotSupportedException($"Values cannot be read as {typeof(T)}.");
    }

    /// <summary>The value of <typeparamref name="T"/> for a null: null for nullable and reference types, else an
    /// exception.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public T Null<T>(int index) => default(T) is null ? default! : throw NullValue(typeof(T), index);

    public InvalidOperationException NullValue(Type type, int index) =>
        new($"'{Path}' is null at index {index}; read it as a nullable type or check IsNull first ({type} cannot hold null).");

    public object ReadBoxed(int index) => TypeId switch
    {
        ArrowTypeId.Boolean => ReadBoolean(index, typeof(object)),
        ArrowTypeId.Int8 => (sbyte)ReadInteger(index, typeof(object), IntegerWidth.Int8),
        ArrowTypeId.Int16 => (short)ReadInteger(index, typeof(object), IntegerWidth.Int16),
        ArrowTypeId.Int32 or ArrowTypeId.Date32 or ArrowTypeId.Time32 => ReadInt32(index, typeof(object)),
        ArrowTypeId.Int64 or ArrowTypeId.Date64 or ArrowTypeId.Time64 or ArrowTypeId.Timestamp or ArrowTypeId.Duration
            => ReadInt64(index, typeof(object)),
        ArrowTypeId.UInt8 => (byte)ReadUnsigned(index, typeof(object), IntegerWidth.Int8),
        ArrowTypeId.UInt16 => (ushort)ReadUnsigned(index, typeof(object), IntegerWidth.Int16),
        ArrowTypeId.UInt32 => (uint)ReadUnsigned(index, typeof(object), IntegerWidth.Int32),
        ArrowTypeId.UInt64 => ReadUnsigned(index, typeof(object), IntegerWidth.Int64),
        ArrowTypeId.HalfFloat => ReadHalf(index, typeof(object)),
        ArrowTypeId.Float => ReadSingle(index, typeof(object)),
        ArrowTypeId.Double => ReadDouble(index, typeof(object)),
        // By the column's precision, not the value: decimal holds 28 digits, SqlDecimal 38.
        ArrowTypeId.Decimal128 when ((Decimal128Type)Array.Data.DataType).Precision > 28 => ReadSqlDecimal(index, typeof(object)),
        ArrowTypeId.Decimal128 or ArrowTypeId.Decimal256 => ReadDecimal(index, typeof(object)),
        ArrowTypeId.String or ArrowTypeId.LargeString or ArrowTypeId.StringView => ReadString(index),
        ArrowTypeId.Dictionary => DictionaryValues().ReadBoxed(DictionaryIndex(index)),
        ArrowTypeId.Binary or ArrowTypeId.LargeBinary or ArrowTypeId.BinaryView or ArrowTypeId.FixedSizedBinary
            => GetBytes(index).ToArray(),
        ArrowTypeId.Struct => new ArrowRow(StructRows(), index),
        _ => throw new NotSupportedException($"'{Path}' of type {Array.Data.DataType.Name} cannot be read as a value."),
    };

    private enum IntegerWidth { Int8 = 1, Int16 = 2, Int32 = 4, Int64 = 8 }

    private long ReadInt64(int index, Type target) => TypeId switch
    {
        ArrowTypeId.Int64 or ArrowTypeId.Timestamp or ArrowTypeId.Date64 or ArrowTypeId.Time64 or ArrowTypeId.Duration
            => ((PrimitiveArray<long>)Array).Values[index],
        ArrowTypeId.UInt32 => ((UInt32Array)Array).Values[index],
        _ => ReadInt32(index, target),
    };

    private int ReadInt32(int index, Type target) => TypeId switch
    {
        ArrowTypeId.Int32 or ArrowTypeId.Date32 or ArrowTypeId.Time32 => ((PrimitiveArray<int>)Array).Values[index],
        ArrowTypeId.UInt16 => ((UInt16Array)Array).Values[index],
        _ => (int)ReadInteger(index, target, IntegerWidth.Int16),
    };

    // Signed integers no wider than `width` (unsigned ones narrower than it), widened to long.
    private long ReadInteger(int index, Type target, IntegerWidth width) => TypeId switch
    {
        ArrowTypeId.Int8 => ((Int8Array)Array).Values[index],
        ArrowTypeId.Int16 when width >= IntegerWidth.Int16 => ((Int16Array)Array).Values[index],
        ArrowTypeId.UInt8 when width >= IntegerWidth.Int16 => ((UInt8Array)Array).Values[index],
        _ => throw Mismatch(target),
    };

    // Unsigned integers no wider than `width`.
    private ulong ReadUnsigned(int index, Type target, IntegerWidth width) => TypeId switch
    {
        ArrowTypeId.UInt8 => ((UInt8Array)Array).Values[index],
        ArrowTypeId.UInt16 when width >= IntegerWidth.Int16 => ((UInt16Array)Array).Values[index],
        ArrowTypeId.UInt32 when width >= IntegerWidth.Int32 => ((UInt32Array)Array).Values[index],
        ArrowTypeId.UInt64 when width >= IntegerWidth.Int64 => ((UInt64Array)Array).Values[index],
        _ => throw Mismatch(target),
    };

    private bool ReadBoolean(int index, Type target) => TypeId == ArrowTypeId.Boolean
        ? ((BooleanArray)Array).GetValue(index)!.Value
        : throw Mismatch(target);

    private Half ReadHalf(int index, Type target) => TypeId == ArrowTypeId.HalfFloat
        ? ((HalfFloatArray)Array).Values[index]
        : throw Mismatch(target);

    private float ReadSingle(int index, Type target) => TypeId switch
    {
        ArrowTypeId.Float => ((FloatArray)Array).Values[index],
        ArrowTypeId.HalfFloat => (float)((HalfFloatArray)Array).Values[index],
        _ => throw Mismatch(target),
    };

    private double ReadDouble(int index, Type target) => TypeId switch
    {
        ArrowTypeId.Double => ((DoubleArray)Array).Values[index],
        ArrowTypeId.Float or ArrowTypeId.HalfFloat => ReadSingle(index, target),
        _ => throw Mismatch(target),
    };

    private decimal ReadDecimal(int index, Type target) => TypeId switch
    {
        // Apache.Arrow rounds a value with more digits than decimal holds; refuse it instead.
        ArrowTypeId.Decimal128 when ((Decimal128Type)Array.Data.DataType).Precision > 28 => Exact(ReadSqlDecimal(index, target)),
        ArrowTypeId.Decimal128 => ((Decimal128Array)Array).GetValue(index)!.Value,
        ArrowTypeId.Decimal256 when ((Decimal256Type)Array.Data.DataType).Precision > 28 => throw new OverflowException(
            $"'{Path}' has precision {((Decimal256Type)Array.Data.DataType).Precision}; read its unscaled bytes with GetBytes."),
        ArrowTypeId.Decimal256 => ((Decimal256Array)Array).GetValue(index)!.Value,
        _ => throw Mismatch(target),
    };

    private decimal Exact(SqlDecimal value)
    {
        decimal result = value.Value; // throws when out of decimal's range
        return new SqlDecimal(result) == value
            ? result
            : throw new OverflowException($"'{Path}' holds {value}, which decimal cannot hold exactly; read it as SqlDecimal.");
    }

    private SqlDecimal ReadSqlDecimal(int index, Type target) => TypeId == ArrowTypeId.Decimal128
        ? ((Decimal128Array)Array).GetSqlDecimal(index)!.Value
        : throw Mismatch(target);

    private DateOnly ReadDate(int index, Type target) => TypeId switch
    {
        ArrowTypeId.Date32 => ((Date32Array)Array).GetDateOnly(index)!.Value,
        ArrowTypeId.Date64 => ((Date64Array)Array).GetDateOnly(index)!.Value,
        _ => throw Mismatch(target),
    };

    // A zoned timestamp as an instant; a zone-less one is a wall-clock time and has no offset to give.
    private DateTimeOffset ReadTimestamp(int index, Type target) => TypeId == ArrowTypeId.Timestamp
        && !string.IsNullOrEmpty(((TimestampType)Array.Data.DataType).Timezone)
            ? new DateTimeOffset(TimestampTicks(index), TimeSpan.Zero)
            : throw Mismatch(target);

    // Zoned → DateTimeKind.Utc (the instant), zone-less → DateTimeKind.Unspecified (the wall-clock time).
    private DateTime ReadDateTime(int index, Type target) => TypeId == ArrowTypeId.Timestamp
        ? new DateTime(TimestampTicks(index), string.IsNullOrEmpty(((TimestampType)Array.Data.DataType).Timezone)
            ? DateTimeKind.Unspecified
            : DateTimeKind.Utc)
        : throw Mismatch(typeof(DateTime));

    // Ticks since 0001-01-01; nanoseconds that are not whole ticks (100 ns) are refused, not truncated.
    private long TimestampTicks(int index)
    {
        long value = ((TimestampArray)Array).Values[index];
        long ticks = ((TimestampType)Array.Data.DataType).Unit switch
        {
            TimeUnit.Second => checked(value * TimeSpan.TicksPerSecond),
            TimeUnit.Millisecond => checked(value * TimeSpan.TicksPerMillisecond),
            TimeUnit.Microsecond => checked(value * 10),
            _ when value % 100 != 0 => throw new OverflowException(
                $"'{Path}' holds {value} ns, which is not a whole number of 100 ns ticks; read it as long."),
            _ => value / 100,
        };
        return checked(DateTime.UnixEpoch.Ticks + ticks);
    }

    // A uuid: 16 bytes in big-endian (RFC 4122) order, or its text form.
    private Guid ReadGuid(int index, Type target) => TypeId switch
    {
        ArrowTypeId.FixedSizedBinary when ((FixedSizeBinaryType)Array.Data.DataType).ByteWidth == 16
            => new Guid(GetBytes(index), bigEndian: true),
        ArrowTypeId.String or ArrowTypeId.LargeString or ArrowTypeId.StringView or ArrowTypeId.Dictionary
            => Guid.Parse(ReadString(index)),
        _ => throw Mismatch(target),
    };

    private string ReadString(int index) => TypeId switch
    {
        ArrowTypeId.String or ArrowTypeId.LargeString or ArrowTypeId.StringView => Encoding.UTF8.GetString(GetUtf8(index)),
        ArrowTypeId.Dictionary => DictionaryValues().ReadString(DictionaryIndex(index)),
        _ => throw Mismatch(typeof(string)),
    };

    /// <summary>The UTF-8 bytes of a string value (including dictionary-encoded strings).</summary>
    public ReadOnlySpan<byte> GetUtf8(int index) => TypeId switch
    {
        ArrowTypeId.String => ((StringArray)Array).GetBytes(index),
        ArrowTypeId.LargeString => ((LargeStringArray)Array).GetBytes(index),
        ArrowTypeId.StringView => ((StringViewArray)Array).GetBytes(index),
        ArrowTypeId.Dictionary => DictionaryValues().GetUtf8(DictionaryIndex(index)),
        _ => throw Mismatch(typeof(string)),
    };

    /// <summary>The bytes of a binary, string or fixed-size binary value.</summary>
    public ReadOnlySpan<byte> GetBytes(int index) => TypeId switch
    {
        ArrowTypeId.Binary => ((BinaryArray)Array).GetBytes(index),
        ArrowTypeId.LargeBinary => ((LargeBinaryArray)Array).GetBytes(index),
        ArrowTypeId.BinaryView => ((BinaryViewArray)Array).GetBytes(index),
        // Decimals: the unscaled value, little-endian two's complement (Arrow's layout), for widths no CLR type holds.
        ArrowTypeId.FixedSizedBinary or ArrowTypeId.Decimal32 or ArrowTypeId.Decimal64 or ArrowTypeId.Decimal128
            or ArrowTypeId.Decimal256 => ((FixedSizeBinaryArray)Array).GetBytes(index),
        ArrowTypeId.String or ArrowTypeId.LargeString or ArrowTypeId.StringView => GetUtf8(index),
        ArrowTypeId.Dictionary => DictionaryValues().GetBytes(DictionaryIndex(index)),
        _ => throw Mismatch(typeof(byte[])),
    };

    private InvalidCastException Mismatch(Type target) =>
        new($"'{Path}' of type {Array.Data.DataType.Name} cannot be read as {target}.");

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static T As<TValue, T>(TValue value) where TValue : struct
    {
        if (typeof(T) == typeof(TValue)) return Unsafe.As<TValue, T>(ref value);
        TValue? nullable = value;
        return Unsafe.As<TValue?, T>(ref nullable);
    }

    // ---- nested layouts ----------------------------------------------------------------------------------------

    /// <summary>The rows of a struct array (index = this array's index).</summary>
    public ArrowRows StructRows() => _structRows ??= TypeId == ArrowTypeId.Struct
        ? new ArrowRows(((StructType)Array.Data.DataType).Fields, ((StructArray)Array).Fields, Array.Length, Array,
            fieldIdKey, Path)
        : throw Mismatch(typeof(ArrowRow));

    /// <summary>The range of item indexes of the list at <paramref name="index"/>.</summary>
    public (int Start, int Count) ListRange(int index) => TypeId switch
    {
        ArrowTypeId.List => Range(((ListArray)Array).ValueOffsets, index),
        ArrowTypeId.LargeList => LargeRange(((LargeListArray)Array).ValueOffsets, index),
        ArrowTypeId.FixedSizeList => ((Array.Offset + index) * ((FixedSizeListType)Array.Data.DataType).ListSize,
            ((FixedSizeListType)Array.Data.DataType).ListSize),
        _ => throw new InvalidCastException($"'{Path}' of type {Array.Data.DataType.Name} is not a list."),
    };

    private static (int Start, int Count) Range(ReadOnlySpan<int> offsets, int index) =>
        (offsets[index], offsets[index + 1] - offsets[index]);

    private static (int Start, int Count) LargeRange(ReadOnlySpan<long> offsets, int index) =>
        (checked((int)offsets[index]), checked((int)(offsets[index + 1] - offsets[index])));

    /// <summary>The items of a list array (all lists' items; <see cref="ListRange"/> gives each list's).</summary>
    public ArrowRowNode ListItems() => _items ??= TypeId switch
    {
        ArrowTypeId.List => Child(((ListArray)Array).Values, ((ListType)Array.Data.DataType).ValueField),
        ArrowTypeId.LargeList => Child(((LargeListArray)Array).Values, ((LargeListType)Array.Data.DataType).ValueField),
        ArrowTypeId.FixedSizeList => Child(((FixedSizeListArray)Array).Values,
            ((FixedSizeListType)Array.Data.DataType).ValueField),
        _ => throw new InvalidCastException($"'{Path}' of type {Array.Data.DataType.Name} is not a list."),
    };

    /// <summary>The range of entry indexes of the map at <paramref name="index"/>.</summary>
    public (int Start, int Count) MapRange(int index)
    {
        if (TypeId != ArrowTypeId.Map)
            throw new InvalidCastException($"'{Path}' of type {Array.Data.DataType.Name} is not a map.");
        return Range(((MapArray)Array).ValueOffsets, index);
    }

    /// <summary>The key and value arrays of a map (all maps' entries; <see cref="MapRange"/> gives each map's).</summary>
    public (ArrowRowNode Keys, ArrowRowNode Values) MapEntries()
    {
        if (_keys is null)
        {
            if (TypeId != ArrowTypeId.Map)
                throw new InvalidCastException($"'{Path}' of type {Array.Data.DataType.Name} is not a map.");
            StructArray entries = ((MapArray)Array).KeyValues;
            StructType type = (StructType)entries.Data.DataType;
            _values = Child(entries.Fields[1], type.Fields[1]);
            _keys = Child(entries.Fields[0], type.Fields[0]);
        }
        return (_keys, _values!);
    }

    // A dictionary keeps its values node in _values and its indices node in _items.
    private ArrowRowNode DictionaryValues() => _values ??= Child(((DictionaryArray)Array).Dictionary,
        new Field("values", ((DictionaryType)Array.Data.DataType).ValueType, true));

    private int DictionaryIndex(int index)
    {
        ArrowRowNode indices = _items ??= Child(((DictionaryArray)Array).Indices,
            new Field("indices", ((DictionaryType)Array.Data.DataType).IndexType, false));
        return indices.TypeId == ArrowTypeId.UInt64
            ? checked((int)((UInt64Array)indices.Array).Values[index])
            : checked((int)indices.ReadInt64(index, typeof(int)));
    }

    private ArrowRowNode Child(IArrowArray array, Field field) => new(array, field, $"{Path}.{field.Name}", fieldIdKey);
}
