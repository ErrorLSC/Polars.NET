using System.Collections;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;
using Polars.NET.Core.Arrow;

namespace Polars.NET.Core.Data;

public interface ITypedAccessor<T>
{
    T GetTypedValue(int index);
}

public interface IBinarySpanAccessor
{
    ReadOnlySpan<byte> GetBytesSpan(int index);
}

public abstract class ColumnAccessor : IDisposable
{
    public abstract Type TargetType { get; }
    public abstract void SetBatch(IArrowArray array);
    public abstract bool IsNull(int index);
    public abstract object? GetValue(int index);
    public virtual void Dispose() { }
}

public abstract class TypedColumnAccessor<T> : ColumnAccessor, ITypedAccessor<T>
{
    public abstract T GetTypedValue(int index);

    public override object GetValue(int index)
    {
        if (IsNull(index)) return DBNull.Value;
        return GetTypedValue(index)!;
    }
}

public static class ColumnAccessorFactory
{
    public static ColumnAccessor Create(Field field, Type targetType)
    {
        if (targetType.IsGenericType)
        {
            var genericDef = targetType.GetGenericTypeDefinition();
            bool isOption = genericDef == typeof(FSharpOption<>);
            bool isValueOption = genericDef == typeof(FSharpValueOption<>);

            if (isOption || isValueOption)
            {
                Type innerType = targetType.GetGenericArguments()[0];
                var innerAccessor = Create(field, innerType);

                Type accessorClass = isOption ? typeof(FSharpOptionAccessor<>) : typeof(FSharpValueOptionAccessor<>);
                Type concreteAccessorType = accessorClass.MakeGenericType(innerType);

                return (ColumnAccessor)Activator.CreateInstance(concreteAccessorType, innerAccessor, targetType)!;
            }
        }

        Type effectiveType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        if (effectiveType.IsEnum)
        {
            Type enumAccessorType = typeof(EnumAccessor<>).MakeGenericType(effectiveType);
            return (ColumnAccessor)Activator.CreateInstance(enumAccessorType, field)!;
        }

        if (field.DataType is NullType)
        {
            Type nullAccessorType = typeof(NullAccessor<>).MakeGenericType(effectiveType);
            return (ColumnAccessor)Activator.CreateInstance(nullAccessorType)!;
        }

        return field.DataType switch
        {
            ListType or LargeListType or FixedSizeListType => CreateListAccessor(field.DataType, effectiveType),
            StructType => CreateStructAccessor(field.DataType, effectiveType),
            DictionaryType => new DictionaryAccessor(),

            // 1. Primitives
            BooleanType => new BooleanAccessor(),
            Int8Type => new Int8Accessor(),
            Int16Type => new Int16Accessor(),
            Int32Type => new Int32Accessor(),
            UInt8Type => new UInt8Accessor(),
            UInt16Type => new UInt16Accessor(),
            UInt32Type => new UInt32Accessor(),
            UInt64Type => new UInt64Accessor(),

            // 2. Int64 & Magic Types
            Int64Type => effectiveType switch
            {
                { } t when t == typeof(DateTime) => new Int64ToDateTimeAccessor(),
                { } t when t == typeof(TimeSpan) => new Int64ToTimeSpanAccessor(),
                { } t when t == typeof(TimeOnly) => new Int64ToTimeOnlyAccessor(),
                _ => new Int64Accessor()
            },

            HalfFloatType => new HalfFloatAccessor(),
            FloatType => new FloatAccessor(),
            DoubleType => new DoubleAccessor(),

            // 3. String & Binary
            StringViewType or StringType or LargeStringType => new StringViewAccessor(),
            BinaryViewType or BinaryType or LargeBinaryType => effectiveType switch
            {
                { } t when t == typeof(Guid) => new BinaryToGuidAccessor(),
                _ => new BinaryAccessor()
            },

            // 4. Time
            TimestampType => new TimestampAccessor(effectiveType),
            Time32Type => new Time32Accessor(),
            Time64Type => new Time64Accessor(),
            DurationType => new DurationAccessor(),

            Date32Type => new Date32ToDateOnlyAccessor(effectiveType),
            Date64Type => new Date64Accessor(),

            // 5. Decimal
            Decimal128Type or Decimal256Type => new DecimalAccessor(),
            FixedSizeBinaryType => effectiveType switch
            {
                { } t when t == typeof(Guid) => new FixedSizeBinaryToGuidAccessor(),
                _ => new FixedSizeBinaryAccessor()
            },

            // 6. Fallback
            _ => (ColumnAccessor)Activator.CreateInstance(
                typeof(JsonFallbackAccessor<>).MakeGenericType(effectiveType))!
        };
    }

    private static ColumnAccessor CreateListAccessor(IArrowType arrowType, Type targetType)
    {
        Type elementType;
        if (targetType.IsArray)
        {
            elementType = targetType.GetElementType()!;
        }
        else if (targetType.IsGenericType && targetType.GetGenericArguments().Length == 1)
        {
            elementType = targetType.GetGenericArguments()[0];
        }
        else
        {
            IArrowType inner = arrowType switch
            {
                ListType lt => lt.ValueDataType,
                LargeListType llt => llt.ValueDataType,
                FixedSizeListType fslt => fslt.ValueDataType,
                _ => throw new InvalidOperationException($"Unsupported list arrow type: {arrowType}")
            };
            elementType = ArrowTypeResolver.GetNetTypeFromArrowType(inner);
        }

        Type accessorClass = typeof(ListAccessor<>).MakeGenericType(elementType);
        return (ColumnAccessor)Activator.CreateInstance(accessorClass, targetType, arrowType)!;
    }

    private static ColumnAccessor CreateStructAccessor(IArrowType arrowType, Type targetType)
    {
        if (targetType != typeof(byte[]))
        {
            if (targetType.IsArray)
            {
                Type elementType = targetType.GetElementType()!;
                Type accessorClass = typeof(StructToListAccessor<>).MakeGenericType(elementType);
                return (ColumnAccessor)Activator.CreateInstance(accessorClass, targetType, arrowType)!;
            }
            if (targetType.IsGenericType)
            {
                var def = targetType.GetGenericTypeDefinition();
                if (def == typeof(List<>) ||
                    def == typeof(IList<>) ||
                    def == typeof(ICollection<>) ||
                    def == typeof(IReadOnlyList<>) ||
                    def == typeof(IReadOnlyCollection<>) ||
                    def == typeof(IEnumerable<>) ||
                    def == typeof(FSharpList<>) ||
                    def == typeof(IOrderedEnumerable<>))
                {
                    Type elementType = targetType.GetGenericArguments()[0];
                    Type accessorClass = typeof(StructToListAccessor<>).MakeGenericType(elementType);
                    return (ColumnAccessor)Activator.CreateInstance(accessorClass, targetType, arrowType)!;
                }
            }
        }

        return (ColumnAccessor)Activator.CreateInstance(typeof(StructAccessor<>).MakeGenericType(targetType), targetType, arrowType)!;
    }
}

// ==================================================================================
// Concrete Accessors
// ==================================================================================

internal sealed class NullAccessor<T> : TypedColumnAccessor<T>, ITypedAccessor<T?>
{
    public override Type TargetType => typeof(T);
    public override void SetBatch(IArrowArray array) { }
    public override bool IsNull(int index) => true;
    public override T GetTypedValue(int index) => default!;
    T? ITypedAccessor<T?>.GetTypedValue(int index) => default;
}

internal sealed class FixedSizeBinaryToGuidAccessor : TypedColumnAccessor<Guid>,
    ITypedAccessor<Guid>, ITypedAccessor<Guid?>, IBinarySpanAccessor
{
    private FixedSizeBinaryArray? _array;

    public override Type TargetType => typeof(Guid);

    public override void SetBatch(IArrowArray array) => _array = (FixedSizeBinaryArray)array;

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int index) => _array!.GetBytes(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Guid GetTypedValue(int index) => new(_array!.GetBytes(index));

    Guid ITypedAccessor<Guid>.GetTypedValue(int index) => GetTypedValue(index);
    Guid? ITypedAccessor<Guid?>.GetTypedValue(int index) => GetTypedValue(index);
}

internal sealed class FixedSizeBinaryAccessor : TypedColumnAccessor<byte[]>,
    ITypedAccessor<byte[]>,
    ITypedAccessor<Guid>, ITypedAccessor<Guid?>,
    IBinarySpanAccessor
{
    private FixedSizeBinaryArray? _array;

    public override Type TargetType => typeof(byte[]);
    public override void SetBatch(IArrowArray array) => _array = (FixedSizeBinaryArray)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    public override byte[] GetTypedValue(int index) => _array!.GetBytes(index).ToArray();

    Guid ITypedAccessor<Guid>.GetTypedValue(int index) => new(_array!.GetBytes(index));
    Guid? ITypedAccessor<Guid?>.GetTypedValue(int index) => new Guid(_array!.GetBytes(index));
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int index) => _array!.GetBytes(index);
    byte[] ITypedAccessor<byte[]>.GetTypedValue(int index) => GetTypedValue(index);
}

internal sealed class BinaryToGuidAccessor : TypedColumnAccessor<Guid>,
    ITypedAccessor<Guid>, ITypedAccessor<Guid?>, IBinarySpanAccessor
{
    private IArrowArray? _array;

    public override Type TargetType => typeof(Guid);
    public override void SetBatch(IArrowArray array) => _array = array;
    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int index)
    {
        if (_array is BinaryViewArray bv) return bv.GetBytes(index);
        if (_array is LargeBinaryArray lba) return lba.GetBytes(index);
        if (_array is BinaryArray ba) return ba.GetBytes(index);
        throw new InvalidCastException($"Expected Binary-like array, got {_array?.GetType()}");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Guid GetTypedValue(int index)
    {
        ReadOnlySpan<byte> bytes = GetBytesSpan(index);
        if (bytes.Length != 16)
        {
            throw new InvalidDataException($"Cannot convert binary of length {bytes.Length} to Guid.");
        }
        return new Guid(bytes);
    }

    Guid ITypedAccessor<Guid>.GetTypedValue(int index) => GetTypedValue(index);
    Guid? ITypedAccessor<Guid?>.GetTypedValue(int index) => GetTypedValue(index);
}

internal sealed class BooleanAccessor : TypedColumnAccessor<bool>,
    ITypedAccessor<bool>, ITypedAccessor<bool?>
{
    private BooleanArray? _array;
    public override Type TargetType => typeof(bool);
    public override void SetBatch(IArrowArray array) => _array = (BooleanArray)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool GetTypedValue(int index) => _array!.GetValue(index)!.Value;
    bool ITypedAccessor<bool>.GetTypedValue(int index) => GetTypedValue(index);
    bool? ITypedAccessor<bool?>.GetTypedValue(int index) => GetTypedValue(index);
}

internal sealed class Int8Accessor : TypedColumnAccessor<sbyte>,
    ITypedAccessor<sbyte>, ITypedAccessor<byte>, ITypedAccessor<short>, ITypedAccessor<ushort>,
    ITypedAccessor<int>, ITypedAccessor<uint>, ITypedAccessor<long>, ITypedAccessor<ulong>,
    ITypedAccessor<Int128>, ITypedAccessor<UInt128>,
    ITypedAccessor<Half>, ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>, ITypedAccessor<char>,
    ITypedAccessor<sbyte?>, ITypedAccessor<byte?>, ITypedAccessor<short?>, ITypedAccessor<ushort?>,
    ITypedAccessor<int?>, ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>,
    ITypedAccessor<Int128?>, ITypedAccessor<UInt128?>,
    ITypedAccessor<Half?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>, ITypedAccessor<char?>
{
    private Int8Array? _array;
    public override Type TargetType => typeof(sbyte);
    public override void SetBatch(IArrowArray array) => _array = (Int8Array)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override sbyte GetTypedValue(int index) => _array!.Values[index];

    byte ITypedAccessor<byte>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short ITypedAccessor<short>.GetTypedValue(int index) => _array!.Values[index];
    ushort ITypedAccessor<ushort>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int ITypedAccessor<int>.GetTypedValue(int index) => _array!.Values[index];
    uint ITypedAccessor<uint>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index];
    ulong ITypedAccessor<ulong>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    Int128 ITypedAccessor<Int128>.GetTypedValue(int index) => _array!.Values[index];
    UInt128 ITypedAccessor<UInt128>.GetTypedValue(int index) => (UInt128)_array!.Values[index];
    Half ITypedAccessor<Half>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float ITypedAccessor<float>.GetTypedValue(int index) => _array!.Values[index];
    double ITypedAccessor<double>.GetTypedValue(int index) => _array!.Values[index];
    decimal ITypedAccessor<decimal>.GetTypedValue(int index) => _array!.Values[index];
    char ITypedAccessor<char>.GetTypedValue(int index) => (char)_array!.Values[index];

    sbyte? ITypedAccessor<sbyte?>.GetTypedValue(int index) => _array!.Values[index];
    byte? ITypedAccessor<byte?>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short? ITypedAccessor<short?>.GetTypedValue(int index) => _array!.Values[index];
    ushort? ITypedAccessor<ushort?>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int? ITypedAccessor<int?>.GetTypedValue(int index) => _array!.Values[index];
    uint? ITypedAccessor<uint?>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index];
    ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    Int128? ITypedAccessor<Int128?>.GetTypedValue(int index) => _array!.Values[index];
    UInt128? ITypedAccessor<UInt128?>.GetTypedValue(int index) => (UInt128)_array!.Values[index];
    Half? ITypedAccessor<Half?>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float? ITypedAccessor<float?>.GetTypedValue(int index) => _array!.Values[index];
    double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => _array!.Values[index];
    char? ITypedAccessor<char?>.GetTypedValue(int index) => (char)_array!.Values[index];
}

internal sealed class Int16Accessor : TypedColumnAccessor<short>,
    ITypedAccessor<sbyte>, ITypedAccessor<byte>, ITypedAccessor<short>, ITypedAccessor<ushort>,
    ITypedAccessor<int>, ITypedAccessor<uint>, ITypedAccessor<long>, ITypedAccessor<ulong>,
    ITypedAccessor<Int128>, ITypedAccessor<UInt128>,
    ITypedAccessor<Half>, ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>, ITypedAccessor<char>,
    ITypedAccessor<sbyte?>, ITypedAccessor<byte?>, ITypedAccessor<short?>, ITypedAccessor<ushort?>,
    ITypedAccessor<int?>, ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>,
    ITypedAccessor<Int128?>, ITypedAccessor<UInt128?>,
    ITypedAccessor<Half?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>, ITypedAccessor<char?>
{
    private Int16Array? _array;
    public override Type TargetType => typeof(short);
    public override void SetBatch(IArrowArray array) => _array = (Int16Array)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override short GetTypedValue(int index) => _array!.Values[index];

    sbyte ITypedAccessor<sbyte>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte ITypedAccessor<byte>.GetTypedValue(int index) => (byte)_array!.Values[index];
    ushort ITypedAccessor<ushort>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int ITypedAccessor<int>.GetTypedValue(int index) => _array!.Values[index];
    uint ITypedAccessor<uint>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index];
    ulong ITypedAccessor<ulong>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    Int128 ITypedAccessor<Int128>.GetTypedValue(int index) => _array!.Values[index];
    UInt128 ITypedAccessor<UInt128>.GetTypedValue(int index) => (UInt128)_array!.Values[index];
    Half ITypedAccessor<Half>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float ITypedAccessor<float>.GetTypedValue(int index) => _array!.Values[index];
    double ITypedAccessor<double>.GetTypedValue(int index) => _array!.Values[index];
    decimal ITypedAccessor<decimal>.GetTypedValue(int index) => _array!.Values[index];
    char ITypedAccessor<char>.GetTypedValue(int index) => (char)_array!.Values[index];

    sbyte? ITypedAccessor<sbyte?>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte? ITypedAccessor<byte?>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short? ITypedAccessor<short?>.GetTypedValue(int index) => _array!.Values[index];
    ushort? ITypedAccessor<ushort?>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int? ITypedAccessor<int?>.GetTypedValue(int index) => _array!.Values[index];
    uint? ITypedAccessor<uint?>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index];
    ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    Int128? ITypedAccessor<Int128?>.GetTypedValue(int index) => _array!.Values[index];
    UInt128? ITypedAccessor<UInt128?>.GetTypedValue(int index) => (UInt128)_array!.Values[index];
    Half? ITypedAccessor<Half?>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float? ITypedAccessor<float?>.GetTypedValue(int index) => _array!.Values[index];
    double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => _array!.Values[index];
    char? ITypedAccessor<char?>.GetTypedValue(int index) => (char)_array!.Values[index];
}

internal sealed class Int32Accessor : TypedColumnAccessor<int>,
    ITypedAccessor<sbyte>, ITypedAccessor<byte>, ITypedAccessor<short>, ITypedAccessor<ushort>,
    ITypedAccessor<int>, ITypedAccessor<uint>, ITypedAccessor<long>, ITypedAccessor<ulong>,
    ITypedAccessor<Int128>, ITypedAccessor<UInt128>,
    ITypedAccessor<Half>, ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>, ITypedAccessor<char>,
    ITypedAccessor<sbyte?>, ITypedAccessor<byte?>, ITypedAccessor<short?>, ITypedAccessor<ushort?>,
    ITypedAccessor<int?>, ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>,
    ITypedAccessor<Int128?>, ITypedAccessor<UInt128?>,
    ITypedAccessor<Half?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>, ITypedAccessor<char?>
{
    private Int32Array? _array;
    public override Type TargetType => typeof(int);
    public override void SetBatch(IArrowArray array) => _array = (Int32Array)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override int GetTypedValue(int index) => _array!.Values[index];

    sbyte ITypedAccessor<sbyte>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte ITypedAccessor<byte>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short ITypedAccessor<short>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort ITypedAccessor<ushort>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    uint ITypedAccessor<uint>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index];
    ulong ITypedAccessor<ulong>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    Int128 ITypedAccessor<Int128>.GetTypedValue(int index) => _array!.Values[index];
    UInt128 ITypedAccessor<UInt128>.GetTypedValue(int index) => (UInt128)_array!.Values[index];
    Half ITypedAccessor<Half>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float ITypedAccessor<float>.GetTypedValue(int index) => _array!.Values[index];
    double ITypedAccessor<double>.GetTypedValue(int index) => _array!.Values[index];
    decimal ITypedAccessor<decimal>.GetTypedValue(int index) => _array!.Values[index];
    char ITypedAccessor<char>.GetTypedValue(int index) => (char)_array!.Values[index];

    sbyte? ITypedAccessor<sbyte?>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte? ITypedAccessor<byte?>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short? ITypedAccessor<short?>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort? ITypedAccessor<ushort?>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int? ITypedAccessor<int?>.GetTypedValue(int index) => _array!.Values[index];
    uint? ITypedAccessor<uint?>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index];
    ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    Int128? ITypedAccessor<Int128?>.GetTypedValue(int index) => _array!.Values[index];
    UInt128? ITypedAccessor<UInt128?>.GetTypedValue(int index) => (UInt128)_array!.Values[index];
    Half? ITypedAccessor<Half?>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float? ITypedAccessor<float?>.GetTypedValue(int index) => _array!.Values[index];
    double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => _array!.Values[index];
    char? ITypedAccessor<char?>.GetTypedValue(int index) => (char)_array!.Values[index];
}

internal sealed class Int64Accessor : TypedColumnAccessor<long>,
    ITypedAccessor<sbyte>, ITypedAccessor<byte>, ITypedAccessor<short>, ITypedAccessor<ushort>,
    ITypedAccessor<int>, ITypedAccessor<uint>, ITypedAccessor<long>, ITypedAccessor<ulong>,
    ITypedAccessor<Int128>, ITypedAccessor<UInt128>,
    ITypedAccessor<Half>, ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>, ITypedAccessor<char>,
    ITypedAccessor<sbyte?>, ITypedAccessor<byte?>, ITypedAccessor<short?>, ITypedAccessor<ushort?>,
    ITypedAccessor<int?>, ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>,
    ITypedAccessor<Int128?>, ITypedAccessor<UInt128?>,
    ITypedAccessor<Half?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>, ITypedAccessor<char?>
{
    private Int64Array? _array;
    public override Type TargetType => typeof(long);
    public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override long GetTypedValue(int index) => _array!.Values[index];

    sbyte ITypedAccessor<sbyte>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte ITypedAccessor<byte>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short ITypedAccessor<short>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort ITypedAccessor<ushort>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int ITypedAccessor<int>.GetTypedValue(int index) => (int)_array!.Values[index];
    uint ITypedAccessor<uint>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index];
    ulong ITypedAccessor<ulong>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    Int128 ITypedAccessor<Int128>.GetTypedValue(int index) => _array!.Values[index];
    UInt128 ITypedAccessor<UInt128>.GetTypedValue(int index) => (UInt128)_array!.Values[index];
    Half ITypedAccessor<Half>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float ITypedAccessor<float>.GetTypedValue(int index) => _array!.Values[index];
    double ITypedAccessor<double>.GetTypedValue(int index) => _array!.Values[index];
    decimal ITypedAccessor<decimal>.GetTypedValue(int index) => _array!.Values[index];
    char ITypedAccessor<char>.GetTypedValue(int index) => (char)_array!.Values[index];

    sbyte? ITypedAccessor<sbyte?>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte? ITypedAccessor<byte?>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short? ITypedAccessor<short?>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort? ITypedAccessor<ushort?>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int? ITypedAccessor<int?>.GetTypedValue(int index) => (int)_array!.Values[index];
    uint? ITypedAccessor<uint?>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index];
    ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    Int128? ITypedAccessor<Int128?>.GetTypedValue(int index) => _array!.Values[index];
    UInt128? ITypedAccessor<UInt128?>.GetTypedValue(int index) => (UInt128)_array!.Values[index];
    Half? ITypedAccessor<Half?>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float? ITypedAccessor<float?>.GetTypedValue(int index) => _array!.Values[index];
    double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => _array!.Values[index];
    char? ITypedAccessor<char?>.GetTypedValue(int index) => (char)_array!.Values[index];
}

internal sealed class UInt8Accessor : TypedColumnAccessor<byte>,
    ITypedAccessor<sbyte>, ITypedAccessor<byte>, ITypedAccessor<short>, ITypedAccessor<ushort>,
    ITypedAccessor<int>, ITypedAccessor<uint>, ITypedAccessor<long>, ITypedAccessor<ulong>,
    ITypedAccessor<Int128>, ITypedAccessor<UInt128>,
    ITypedAccessor<Half>, ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>, ITypedAccessor<char>,
    ITypedAccessor<sbyte?>, ITypedAccessor<byte?>, ITypedAccessor<short?>, ITypedAccessor<ushort?>,
    ITypedAccessor<int?>, ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>,
    ITypedAccessor<Int128?>, ITypedAccessor<UInt128?>,
    ITypedAccessor<Half?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>, ITypedAccessor<char?>
{
    private UInt8Array? _array;
    public override Type TargetType => typeof(byte);
    public override void SetBatch(IArrowArray array) => _array = (UInt8Array)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override byte GetTypedValue(int index) => _array!.Values[index];

    sbyte ITypedAccessor<sbyte>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    short ITypedAccessor<short>.GetTypedValue(int index) => _array!.Values[index];
    ushort ITypedAccessor<ushort>.GetTypedValue(int index) => _array!.Values[index];
    int ITypedAccessor<int>.GetTypedValue(int index) => _array!.Values[index];
    uint ITypedAccessor<uint>.GetTypedValue(int index) => _array!.Values[index];
    long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index];
    ulong ITypedAccessor<ulong>.GetTypedValue(int index) => _array!.Values[index];
    Int128 ITypedAccessor<Int128>.GetTypedValue(int index) => _array!.Values[index];
    UInt128 ITypedAccessor<UInt128>.GetTypedValue(int index) => _array!.Values[index];
    Half ITypedAccessor<Half>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float ITypedAccessor<float>.GetTypedValue(int index) => _array!.Values[index];
    double ITypedAccessor<double>.GetTypedValue(int index) => _array!.Values[index];
    decimal ITypedAccessor<decimal>.GetTypedValue(int index) => _array!.Values[index];
    char ITypedAccessor<char>.GetTypedValue(int index) => (char)_array!.Values[index];

    sbyte? ITypedAccessor<sbyte?>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte? ITypedAccessor<byte?>.GetTypedValue(int index) => _array!.Values[index];
    short? ITypedAccessor<short?>.GetTypedValue(int index) => _array!.Values[index];
    ushort? ITypedAccessor<ushort?>.GetTypedValue(int index) => _array!.Values[index];
    int? ITypedAccessor<int?>.GetTypedValue(int index) => _array!.Values[index];
    uint? ITypedAccessor<uint?>.GetTypedValue(int index) => _array!.Values[index];
    long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index];
    ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => _array!.Values[index];
    Int128? ITypedAccessor<Int128?>.GetTypedValue(int index) => _array!.Values[index];
    UInt128? ITypedAccessor<UInt128?>.GetTypedValue(int index) => _array!.Values[index];
    Half? ITypedAccessor<Half?>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float? ITypedAccessor<float?>.GetTypedValue(int index) => _array!.Values[index];
    double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => _array!.Values[index];
    char? ITypedAccessor<char?>.GetTypedValue(int index) => (char)_array!.Values[index];
}

internal sealed class UInt16Accessor : TypedColumnAccessor<ushort>,
    ITypedAccessor<sbyte>, ITypedAccessor<byte>, ITypedAccessor<short>, ITypedAccessor<ushort>,
    ITypedAccessor<int>, ITypedAccessor<uint>, ITypedAccessor<long>, ITypedAccessor<ulong>,
    ITypedAccessor<Int128>, ITypedAccessor<UInt128>,
    ITypedAccessor<Half>, ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>, ITypedAccessor<char>,
    ITypedAccessor<sbyte?>, ITypedAccessor<byte?>, ITypedAccessor<short?>, ITypedAccessor<ushort?>,
    ITypedAccessor<int?>, ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>,
    ITypedAccessor<Int128?>, ITypedAccessor<UInt128?>,
    ITypedAccessor<Half?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>, ITypedAccessor<char?>
{
    private UInt16Array? _array;
    public override Type TargetType => typeof(ushort);
    public override void SetBatch(IArrowArray array) => _array = (UInt16Array)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override ushort GetTypedValue(int index) => _array!.Values[index];

    sbyte ITypedAccessor<sbyte>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte ITypedAccessor<byte>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short ITypedAccessor<short>.GetTypedValue(int index) => (short)_array!.Values[index];
    int ITypedAccessor<int>.GetTypedValue(int index) => _array!.Values[index];
    uint ITypedAccessor<uint>.GetTypedValue(int index) => _array!.Values[index];
    long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index];
    ulong ITypedAccessor<ulong>.GetTypedValue(int index) => _array!.Values[index];
    Int128 ITypedAccessor<Int128>.GetTypedValue(int index) => _array!.Values[index];
    UInt128 ITypedAccessor<UInt128>.GetTypedValue(int index) => _array!.Values[index];
    Half ITypedAccessor<Half>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float ITypedAccessor<float>.GetTypedValue(int index) => _array!.Values[index];
    double ITypedAccessor<double>.GetTypedValue(int index) => _array!.Values[index];
    decimal ITypedAccessor<decimal>.GetTypedValue(int index) => _array!.Values[index];
    char ITypedAccessor<char>.GetTypedValue(int index) => (char)_array!.Values[index];

    sbyte? ITypedAccessor<sbyte?>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte? ITypedAccessor<byte?>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short? ITypedAccessor<short?>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort? ITypedAccessor<ushort?>.GetTypedValue(int index) => _array!.Values[index];
    int? ITypedAccessor<int?>.GetTypedValue(int index) => _array!.Values[index];
    uint? ITypedAccessor<uint?>.GetTypedValue(int index) => _array!.Values[index];
    long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index];
    ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => _array!.Values[index];
    Int128? ITypedAccessor<Int128?>.GetTypedValue(int index) => _array!.Values[index];
    UInt128? ITypedAccessor<UInt128?>.GetTypedValue(int index) => _array!.Values[index];
    Half? ITypedAccessor<Half?>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float? ITypedAccessor<float?>.GetTypedValue(int index) => _array!.Values[index];
    double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => _array!.Values[index];
    char? ITypedAccessor<char?>.GetTypedValue(int index) => (char)_array!.Values[index];
}

internal sealed class UInt32Accessor : TypedColumnAccessor<uint>,
    ITypedAccessor<sbyte>, ITypedAccessor<byte>, ITypedAccessor<short>, ITypedAccessor<ushort>,
    ITypedAccessor<int>, ITypedAccessor<uint>, ITypedAccessor<long>, ITypedAccessor<ulong>,
    ITypedAccessor<Int128>, ITypedAccessor<UInt128>,
    ITypedAccessor<Half>, ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>, ITypedAccessor<char>,
    ITypedAccessor<sbyte?>, ITypedAccessor<byte?>, ITypedAccessor<short?>, ITypedAccessor<ushort?>,
    ITypedAccessor<int?>, ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>,
    ITypedAccessor<Int128?>, ITypedAccessor<UInt128?>,
    ITypedAccessor<Half?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>, ITypedAccessor<char?>
{
    private UInt32Array? _array;
    public override Type TargetType => typeof(uint);
    public override void SetBatch(IArrowArray array) => _array = (UInt32Array)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override uint GetTypedValue(int index) => _array!.Values[index];

    sbyte ITypedAccessor<sbyte>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte ITypedAccessor<byte>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short ITypedAccessor<short>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort ITypedAccessor<ushort>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int ITypedAccessor<int>.GetTypedValue(int index) => (int)_array!.Values[index];
    long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index];
    ulong ITypedAccessor<ulong>.GetTypedValue(int index) => _array!.Values[index];
    Int128 ITypedAccessor<Int128>.GetTypedValue(int index) => _array!.Values[index];
    UInt128 ITypedAccessor<UInt128>.GetTypedValue(int index) => _array!.Values[index];
    Half ITypedAccessor<Half>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float ITypedAccessor<float>.GetTypedValue(int index) => _array!.Values[index];
    double ITypedAccessor<double>.GetTypedValue(int index) => _array!.Values[index];
    decimal ITypedAccessor<decimal>.GetTypedValue(int index) => _array!.Values[index];
    char ITypedAccessor<char>.GetTypedValue(int index) => (char)_array!.Values[index];

    sbyte? ITypedAccessor<sbyte?>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte? ITypedAccessor<byte?>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short? ITypedAccessor<short?>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort? ITypedAccessor<ushort?>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int? ITypedAccessor<int?>.GetTypedValue(int index) => (int)_array!.Values[index];
    uint? ITypedAccessor<uint?>.GetTypedValue(int index) => _array!.Values[index];
    long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index];
    ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => _array!.Values[index];
    Int128? ITypedAccessor<Int128?>.GetTypedValue(int index) => _array!.Values[index];
    UInt128? ITypedAccessor<UInt128?>.GetTypedValue(int index) => _array!.Values[index];
    Half? ITypedAccessor<Half?>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float? ITypedAccessor<float?>.GetTypedValue(int index) => _array!.Values[index];
    double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => _array!.Values[index];
    char? ITypedAccessor<char?>.GetTypedValue(int index) => (char)_array!.Values[index];
}

internal sealed class UInt64Accessor : TypedColumnAccessor<ulong>,
    ITypedAccessor<sbyte>, ITypedAccessor<byte>, ITypedAccessor<short>, ITypedAccessor<ushort>,
    ITypedAccessor<int>, ITypedAccessor<uint>, ITypedAccessor<long>, ITypedAccessor<ulong>,
    ITypedAccessor<Int128>, ITypedAccessor<UInt128>,
    ITypedAccessor<Half>, ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>, ITypedAccessor<char>,
    ITypedAccessor<sbyte?>, ITypedAccessor<byte?>, ITypedAccessor<short?>, ITypedAccessor<ushort?>,
    ITypedAccessor<int?>, ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>,
    ITypedAccessor<Int128?>, ITypedAccessor<UInt128?>,
    ITypedAccessor<Half?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>, ITypedAccessor<char?>
{
    private UInt64Array? _array;
    public override Type TargetType => typeof(ulong);
    public override void SetBatch(IArrowArray array) => _array = (UInt64Array)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override ulong GetTypedValue(int index) => _array!.Values[index];

    sbyte ITypedAccessor<sbyte>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte ITypedAccessor<byte>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short ITypedAccessor<short>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort ITypedAccessor<ushort>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int ITypedAccessor<int>.GetTypedValue(int index) => (int)_array!.Values[index];
    uint ITypedAccessor<uint>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long ITypedAccessor<long>.GetTypedValue(int index) => (long)_array!.Values[index];
    Int128 ITypedAccessor<Int128>.GetTypedValue(int index) => _array!.Values[index];
    UInt128 ITypedAccessor<UInt128>.GetTypedValue(int index) => _array!.Values[index];
    Half ITypedAccessor<Half>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float ITypedAccessor<float>.GetTypedValue(int index) => _array!.Values[index];
    double ITypedAccessor<double>.GetTypedValue(int index) => _array!.Values[index];
    decimal ITypedAccessor<decimal>.GetTypedValue(int index) => _array!.Values[index];
    char ITypedAccessor<char>.GetTypedValue(int index) => (char)_array!.Values[index];

    sbyte? ITypedAccessor<sbyte?>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte? ITypedAccessor<byte?>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short? ITypedAccessor<short?>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort? ITypedAccessor<ushort?>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int? ITypedAccessor<int?>.GetTypedValue(int index) => (int)_array!.Values[index];
    uint? ITypedAccessor<uint?>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long? ITypedAccessor<long?>.GetTypedValue(int index) => (long)_array!.Values[index];
    ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => _array!.Values[index];
    Int128? ITypedAccessor<Int128?>.GetTypedValue(int index) => _array!.Values[index];
    UInt128? ITypedAccessor<UInt128?>.GetTypedValue(int index) => _array!.Values[index];
    Half? ITypedAccessor<Half?>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float? ITypedAccessor<float?>.GetTypedValue(int index) => _array!.Values[index];
    double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => _array!.Values[index];
    char? ITypedAccessor<char?>.GetTypedValue(int index) => (char)_array!.Values[index];
}

internal sealed class HalfFloatAccessor : TypedColumnAccessor<Half>,
    ITypedAccessor<sbyte>, ITypedAccessor<byte>, ITypedAccessor<short>, ITypedAccessor<ushort>,
    ITypedAccessor<int>, ITypedAccessor<uint>, ITypedAccessor<long>, ITypedAccessor<ulong>,
    ITypedAccessor<Half>, ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>, ITypedAccessor<string>,
    ITypedAccessor<sbyte?>, ITypedAccessor<byte?>, ITypedAccessor<short?>, ITypedAccessor<ushort?>,
    ITypedAccessor<int?>, ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>,
    ITypedAccessor<Half?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>
{
    private HalfFloatArray? _array;
    public override Type TargetType => typeof(Half);
    public override void SetBatch(IArrowArray array) => _array = (HalfFloatArray)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Half GetTypedValue(int index) => _array!.Values[index];

    sbyte ITypedAccessor<sbyte>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte ITypedAccessor<byte>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short ITypedAccessor<short>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort ITypedAccessor<ushort>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int ITypedAccessor<int>.GetTypedValue(int index) => (int)_array!.Values[index];
    uint ITypedAccessor<uint>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long ITypedAccessor<long>.GetTypedValue(int index) => (long)_array!.Values[index];
    ulong ITypedAccessor<ulong>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    float ITypedAccessor<float>.GetTypedValue(int index) => (float)_array!.Values[index];
    double ITypedAccessor<double>.GetTypedValue(int index) => (double)_array!.Values[index];
    decimal ITypedAccessor<decimal>.GetTypedValue(int index) => (decimal)(float)_array!.Values[index];
    string ITypedAccessor<string>.GetTypedValue(int index) => _array!.Values[index].ToString();

    sbyte? ITypedAccessor<sbyte?>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte? ITypedAccessor<byte?>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short? ITypedAccessor<short?>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort? ITypedAccessor<ushort?>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int? ITypedAccessor<int?>.GetTypedValue(int index) => (int)_array!.Values[index];
    uint? ITypedAccessor<uint?>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long? ITypedAccessor<long?>.GetTypedValue(int index) => (long)_array!.Values[index];
    ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    Half? ITypedAccessor<Half?>.GetTypedValue(int index) => _array!.Values[index];
    float? ITypedAccessor<float?>.GetTypedValue(int index) => (float)_array!.Values[index];
    double? ITypedAccessor<double?>.GetTypedValue(int index) => (double)_array!.Values[index];
    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => (decimal)(float)_array!.Values[index];
}

internal sealed class FloatAccessor : TypedColumnAccessor<float>,
    ITypedAccessor<sbyte>, ITypedAccessor<byte>, ITypedAccessor<short>, ITypedAccessor<ushort>,
    ITypedAccessor<int>, ITypedAccessor<uint>, ITypedAccessor<long>, ITypedAccessor<ulong>,
    ITypedAccessor<Half>, ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>,
    ITypedAccessor<sbyte?>, ITypedAccessor<byte?>, ITypedAccessor<short?>, ITypedAccessor<ushort?>,
    ITypedAccessor<int?>, ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>,
    ITypedAccessor<Half?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>
{
    private FloatArray? _array;
    public override Type TargetType => typeof(float);
    public override void SetBatch(IArrowArray array) => _array = (FloatArray)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override float GetTypedValue(int index) => _array!.Values[index];

    sbyte ITypedAccessor<sbyte>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte ITypedAccessor<byte>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short ITypedAccessor<short>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort ITypedAccessor<ushort>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int ITypedAccessor<int>.GetTypedValue(int index) => (int)_array!.Values[index];
    uint ITypedAccessor<uint>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long ITypedAccessor<long>.GetTypedValue(int index) => (long)_array!.Values[index];
    ulong ITypedAccessor<ulong>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    Half ITypedAccessor<Half>.GetTypedValue(int index) => (Half)_array!.Values[index];
    double ITypedAccessor<double>.GetTypedValue(int index) => _array!.Values[index];
    decimal ITypedAccessor<decimal>.GetTypedValue(int index) => (decimal)_array!.Values[index];

    sbyte? ITypedAccessor<sbyte?>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte? ITypedAccessor<byte?>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short? ITypedAccessor<short?>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort? ITypedAccessor<ushort?>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int? ITypedAccessor<int?>.GetTypedValue(int index) => (int)_array!.Values[index];
    uint? ITypedAccessor<uint?>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long? ITypedAccessor<long?>.GetTypedValue(int index) => (long)_array!.Values[index];
    ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    Half? ITypedAccessor<Half?>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float? ITypedAccessor<float?>.GetTypedValue(int index) => _array!.Values[index];
    double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => (decimal)_array!.Values[index];
}

internal sealed class DoubleAccessor : TypedColumnAccessor<double>,
    ITypedAccessor<sbyte>, ITypedAccessor<byte>, ITypedAccessor<short>, ITypedAccessor<ushort>,
    ITypedAccessor<int>, ITypedAccessor<uint>, ITypedAccessor<long>, ITypedAccessor<ulong>,
    ITypedAccessor<Half>, ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>,
    ITypedAccessor<sbyte?>, ITypedAccessor<byte?>, ITypedAccessor<short?>, ITypedAccessor<ushort?>,
    ITypedAccessor<int?>, ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>,
    ITypedAccessor<Half?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>
{
    private DoubleArray? _array;
    public override Type TargetType => typeof(double);
    public override void SetBatch(IArrowArray array) => _array = (DoubleArray)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override double GetTypedValue(int index) => _array!.Values[index];

    sbyte ITypedAccessor<sbyte>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte ITypedAccessor<byte>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short ITypedAccessor<short>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort ITypedAccessor<ushort>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int ITypedAccessor<int>.GetTypedValue(int index) => (int)_array!.Values[index];
    uint ITypedAccessor<uint>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long ITypedAccessor<long>.GetTypedValue(int index) => (long)_array!.Values[index];
    ulong ITypedAccessor<ulong>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    Half ITypedAccessor<Half>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float ITypedAccessor<float>.GetTypedValue(int index) => (float)_array!.Values[index];
    decimal ITypedAccessor<decimal>.GetTypedValue(int index) => (decimal)_array!.Values[index];

    sbyte? ITypedAccessor<sbyte?>.GetTypedValue(int index) => (sbyte)_array!.Values[index];
    byte? ITypedAccessor<byte?>.GetTypedValue(int index) => (byte)_array!.Values[index];
    short? ITypedAccessor<short?>.GetTypedValue(int index) => (short)_array!.Values[index];
    ushort? ITypedAccessor<ushort?>.GetTypedValue(int index) => (ushort)_array!.Values[index];
    int? ITypedAccessor<int?>.GetTypedValue(int index) => (int)_array!.Values[index];
    uint? ITypedAccessor<uint?>.GetTypedValue(int index) => (uint)_array!.Values[index];
    long? ITypedAccessor<long?>.GetTypedValue(int index) => (long)_array!.Values[index];
    ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => (ulong)_array!.Values[index];
    Half? ITypedAccessor<Half?>.GetTypedValue(int index) => (Half)_array!.Values[index];
    float? ITypedAccessor<float?>.GetTypedValue(int index) => (float)_array!.Values[index];
    double? ITypedAccessor<double?>.GetTypedValue(int index) => _array!.Values[index];
    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => (decimal)_array!.Values[index];
}

internal sealed class DecimalAccessor : TypedColumnAccessor<decimal>,
    ITypedAccessor<decimal>, ITypedAccessor<double>, ITypedAccessor<float>, ITypedAccessor<long>, ITypedAccessor<int>,
    ITypedAccessor<decimal?>, ITypedAccessor<double?>, ITypedAccessor<float?>, ITypedAccessor<long?>, ITypedAccessor<int?>
{
    private const int MaxDotNetDecimalScale = 28;
    private static readonly UInt128 MaxDecimalBits = unchecked(((UInt128)1 << 96) - 1);

    private Decimal128Array? _array128;
    private Decimal256Array? _array256;
    private int _scale;
    private int _precision;

    public override Type TargetType => typeof(decimal);

    public override void SetBatch(IArrowArray array)
    {
        if (array is Decimal128Array d128)
        {
            _array128 = d128;
            _array256 = null;
            _scale = d128.Scale;
            _precision = d128.Precision;
        }
        else if (array is Decimal256Array d256)
        {
            _array256 = d256;
            _array128 = null;
            _scale = d256.Scale;
            _precision = d256.Precision;
        }
        else
        {
            throw new InvalidCastException($"Expected Decimal128Array or Decimal256Array, got {array?.GetType()}");
        }
    }

    public override bool IsNull(int index) => _array128 != null ? _array128.IsNull(index) : _array256!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override decimal GetTypedValue(int index)
    {
        if (_array128 != null)
        {
            if (_scale is >= 0 and <= MaxDotNetDecimalScale)
            {
                ReadOnlySpan<Int128> values = MemoryMarshal.Cast<byte, Int128>(_array128.ValueBuffer.Span);
                Int128 val = values[index + _array128.Offset];
                bool isNegative = val < 0;
                UInt128 uval = isNegative ? unchecked((UInt128)(-val)) : unchecked((UInt128)val);

                if (uval <= MaxDecimalBits)
                {
                    int low = (int)(uval & 0xFFFF_FFFF);
                    int mid = (int)((uval >> 32) & 0xFFFF_FFFF);
                    int high = (int)((uval >> 64) & 0xFFFF_FFFF);
                    return new decimal(low, mid, high, isNegative, (byte)_scale);
                }

                throw new OverflowException($"Decimal value with precision {_precision} exceeds the 96-bit capacity of .NET decimal.");
            }
            return _array128.GetValue(index)!.Value;
        }

        return _array256!.GetValue(index)!.Value;
    }

    double ITypedAccessor<double>.GetTypedValue(int index) => (double)GetTypedValue(index);
    float ITypedAccessor<float>.GetTypedValue(int index) => (float)GetTypedValue(index);
    long ITypedAccessor<long>.GetTypedValue(int index) => (long)GetTypedValue(index);
    int ITypedAccessor<int>.GetTypedValue(int index) => (int)GetTypedValue(index);

    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => GetTypedValue(index);
    double? ITypedAccessor<double?>.GetTypedValue(int index) => (double)GetTypedValue(index);
    float? ITypedAccessor<float?>.GetTypedValue(int index) => (float)GetTypedValue(index);
    long? ITypedAccessor<long?>.GetTypedValue(int index) => (long)GetTypedValue(index);
    int? ITypedAccessor<int?>.GetTypedValue(int index) => (int)GetTypedValue(index);
}

internal sealed class StringViewAccessor : TypedColumnAccessor<string>,
    IBinarySpanAccessor,
    ITypedAccessor<string>,
    ITypedAccessor<char>,
    ITypedAccessor<char?>
{
    private IArrowArray? _array;
    private bool _isView;

    public override Type TargetType => typeof(string);

    public override void SetBatch(IArrowArray array)
    {
        _array = array;
        _isView = array is StringViewArray;
    }

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override string GetTypedValue(int index)
    {
        if (_isView) return ((StringViewArray)_array!).GetString(index);
        if (_array is LargeStringArray lsa) return lsa.GetString(index);
        if (_array is StringArray sa) return sa.GetString(index);

        throw new InvalidCastException($"Expected String or StringView, got {_array?.GetType()}");
    }

    char ITypedAccessor<char>.GetTypedValue(int index)
    {
        string s = GetTypedValue(index);
        return string.IsNullOrEmpty(s) ? '\0' : s[0];
    }

    char? ITypedAccessor<char?>.GetTypedValue(int index)
    {
        string s = GetTypedValue(index);
        return string.IsNullOrEmpty(s) ? '\0' : s[0];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int index)
    {
        if (_isView) return ((StringViewArray)_array!).GetBytes(index);
        if (_array is LargeStringArray lsa) return lsa.GetBytes(index);
        if (_array is StringArray sa) return sa.GetBytes(index);

        throw new InvalidCastException($"Expected String or StringView, got {_array?.GetType()}");
    }
}

internal sealed class DictionaryAccessor : TypedColumnAccessor<string>,
    ITypedAccessor<string>
{
    private DictionaryArray? _array;
    private readonly StringViewAccessor _dictValues = new();

    public override Type TargetType => typeof(string);

    public override void SetBatch(IArrowArray array)
    {
        _array = (DictionaryArray)array;
        _dictValues.SetBatch(_array.Dictionary);
    }

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal static int ReadKeyIndex(IArrowArray indices, int index)
    {
        return indices switch
        {
            UInt32Array u32 => (int)u32.Values[index],
            UInt16Array u16 => u16.Values[index],
            UInt8Array u8 => u8.Values[index],
            Int32Array i32 => i32.Values[index],
            Int16Array i16 => i16.Values[index],
            Int8Array i8 => i8.Values[index],
            Int64Array i64 => (int)i64.Values[index],
            UInt64Array u64 => (int)u64.Values[index],
            _ => throw new NotSupportedException($"Unsupported Dictionary index array type: {indices.GetType().Name}")
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override string GetTypedValue(int index)
    {
        int key = ReadKeyIndex(_array!.Indices, index);
        return _dictValues.GetTypedValue(key);
    }
}

internal sealed class EnumAccessor<TEnum> : TypedColumnAccessor<TEnum>,
    ITypedAccessor<TEnum>, ITypedAccessor<TEnum?>
    where TEnum : struct, Enum
{
    private ColumnAccessor? _numericAccessor;
    private DictionaryArray? _dictArray;
    private TEnum[]? _dictEnumLookup;
    private StringViewAccessor? _stringAccessor;
    private readonly Field _field;

    public EnumAccessor(Field field)
    {
        _field = field;
        if (field.DataType is not DictionaryType and not StringViewType and not StringType and not LargeStringType)
        {
            Type underlyingType = Enum.GetUnderlyingType(typeof(TEnum));
            _numericAccessor = ColumnAccessorFactory.Create(field, underlyingType);
        }
        else if (field.DataType is StringViewType or StringType or LargeStringType)
        {
            _stringAccessor = new StringViewAccessor();
        }
    }

    public override Type TargetType => typeof(TEnum);

    public override void SetBatch(IArrowArray array)
    {
        if (array is DictionaryArray dictArr)
        {
            _dictArray = dictArr;
            var strAcc = new StringViewAccessor();
            strAcc.SetBatch(dictArr.Dictionary);
            int dictLen = dictArr.Dictionary.Length;
            var lookup = new TEnum[dictLen];
            for (int i = 0; i < dictLen; i++)
            {
                if (!strAcc.IsNull(i))
                {
                    string val = strAcc.GetTypedValue(i);
                    if (Enum.TryParse<TEnum>(val, ignoreCase: true, out var parsed))
                    {
                        lookup[i] = parsed;
                    }
                }
            }
            _dictEnumLookup = lookup;
        }
        else if (_stringAccessor != null)
        {
            _stringAccessor.SetBatch(array);
        }
        else
        {
            _numericAccessor ??= ColumnAccessorFactory.Create(_field, Enum.GetUnderlyingType(typeof(TEnum)));
            _numericAccessor.SetBatch(array);
        }
    }

    public override bool IsNull(int index)
    {
        if (_dictArray != null) return _dictArray.IsNull(index);
        if (_stringAccessor != null) return _stringAccessor.IsNull(index);
        return _numericAccessor!.IsNull(index);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TEnum GetTypedValue(int index)
    {
        if (_dictArray != null)
        {
            int key = DictionaryAccessor.ReadKeyIndex(_dictArray.Indices, index);
            return _dictEnumLookup![key];
        }

        if (_stringAccessor != null)
        {
            string s = _stringAccessor.GetTypedValue(index);
            return Enum.Parse<TEnum>(s, ignoreCase: true);
        }

        if (_numericAccessor is ITypedAccessor<int> i32)
        {
            int v = i32.GetTypedValue(index);
            return Unsafe.As<int, TEnum>(ref v);
        }
        if (_numericAccessor is ITypedAccessor<long> i64)
        {
            long v = i64.GetTypedValue(index);
            return Unsafe.As<long, TEnum>(ref v);
        }
        if (_numericAccessor is ITypedAccessor<byte> u8)
        {
            byte v = u8.GetTypedValue(index);
            return Unsafe.As<byte, TEnum>(ref v);
        }
        if (_numericAccessor is ITypedAccessor<short> i16)
        {
            short v = i16.GetTypedValue(index);
            return Unsafe.As<short, TEnum>(ref v);
        }
        if (_numericAccessor is ITypedAccessor<sbyte> i8)
        {
            sbyte v = i8.GetTypedValue(index);
            return Unsafe.As<sbyte, TEnum>(ref v);
        }
        if (_numericAccessor is ITypedAccessor<ushort> u16)
        {
            ushort v = u16.GetTypedValue(index);
            return Unsafe.As<ushort, TEnum>(ref v);
        }
        if (_numericAccessor is ITypedAccessor<uint> u32)
        {
            uint v = u32.GetTypedValue(index);
            return Unsafe.As<uint, TEnum>(ref v);
        }
        if (_numericAccessor is ITypedAccessor<ulong> u64)
        {
            ulong v = u64.GetTypedValue(index);
            return Unsafe.As<ulong, TEnum>(ref v);
        }

        return (TEnum)Enum.ToObject(typeof(TEnum), _numericAccessor!.GetValue(index)!);
    }

    TEnum? ITypedAccessor<TEnum?>.GetTypedValue(int index) => GetTypedValue(index);
}

internal sealed class BinaryAccessor : TypedColumnAccessor<byte[]>, IBinarySpanAccessor,
    ITypedAccessor<byte[]>
{
    private IArrowArray? _array;

    public override Type TargetType => typeof(byte[]);

    public override void SetBatch(IArrowArray array) => _array = array;

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int index)
    {
        if (_array is BinaryArray ba) return ba.GetBytes(index);
        if (_array is LargeBinaryArray lba) return lba.GetBytes(index);
        if (_array is BinaryViewArray bv) return bv.GetBytes(index);

        throw new InvalidCastException($"Expected BinaryArray, got {_array?.GetType()}");
    }

    public override byte[] GetTypedValue(int index) => GetBytesSpan(index).ToArray();
}

// --- Magic Time Types ---

internal sealed class Int64ToDateTimeAccessor : TypedColumnAccessor<DateTime>,
    ITypedAccessor<DateTime>, ITypedAccessor<DateTime?>, ITypedAccessor<DateTimeOffset>, ITypedAccessor<DateTimeOffset?>
{
    private Int64Array? _array;
    public override Type TargetType => typeof(DateTime);
    public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateTime GetTypedValue(int index)
        => DateTime.UnixEpoch.AddMicroseconds(_array!.Values[index]);

    DateTime ITypedAccessor<DateTime>.GetTypedValue(int index) => GetTypedValue(index);
    DateTimeOffset ITypedAccessor<DateTimeOffset>.GetTypedValue(int index) => new(GetTypedValue(index));

    DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index) => GetTypedValue(index);
    DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index) => new DateTimeOffset(GetTypedValue(index));
}

internal sealed class Int64ToTimeSpanAccessor : TypedColumnAccessor<TimeSpan>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>, ITypedAccessor<long>, ITypedAccessor<long?>
{
    private Int64Array? _array;
    public override Type TargetType => typeof(TimeSpan);
    public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeSpan GetTypedValue(int index)
        => new(_array!.Values[index] * 10);

    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index);
    long ITypedAccessor<long>.GetTypedValue(int index) => _array!.Values[index] * 10;
    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => GetTypedValue(index);
    long? ITypedAccessor<long?>.GetTypedValue(int index) => _array!.Values[index] * 10;
}

internal sealed class TimestampAccessor : TypedColumnAccessor<DateTime>,
    ITypedAccessor<DateTime>, ITypedAccessor<DateTime?>,
    ITypedAccessor<DateTimeOffset>, ITypedAccessor<DateTimeOffset?>,
    ITypedAccessor<DateOnly>, ITypedAccessor<DateOnly?>
{
    private TimestampArray? _array;
    private TimeUnit _unit;
    private bool _hasTimeZone;
    private TimeZoneInfo? _timeZoneInfo;
    private readonly Type _targetType;

    public TimestampAccessor(Type? targetType = null)
    {
        _targetType = targetType == typeof(DateTimeOffset) ? typeof(DateTimeOffset) : typeof(DateTime);
    }

    public override Type TargetType => _targetType;

    public override void SetBatch(IArrowArray array)
    {
        _array = (TimestampArray)array;
        var tsType = (TimestampType)_array.Data.DataType;
        _unit = tsType.Unit;
        _hasTimeZone = !string.IsNullOrEmpty(tsType.Timezone);
        _timeZoneInfo = _hasTimeZone ? ResolveTimeZoneSafe(tsType.Timezone!) : null;
    }

    private static TimeZoneInfo? ResolveTimeZoneSafe(string tz)
    {
        try
        {
            return TimeZoneInfo.FindSystemTimeZoneById(tz);
        }
        catch (TimeZoneNotFoundException) { }
        catch (InvalidTimeZoneException) { }

        string cleanTz = tz.StartsWith("UTC", StringComparison.OrdinalIgnoreCase)
            ? tz[3..].Trim()
            : tz.Trim();

        if (cleanTz.StartsWith('+'))
        {
            cleanTz = cleanTz[1..];
        }

        if (TimeSpan.TryParse(cleanTz, out TimeSpan offset))
        {
            return TimeZoneInfo.CreateCustomTimeZone(tz, offset, tz, tz);
        }

        return null;
    }

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long GetTicks(int index)
    {
        long val = _array!.Values[index];
        return _unit switch
        {
            TimeUnit.Nanosecond => val / 100L,
            TimeUnit.Microsecond => val * 10L,
            TimeUnit.Millisecond => val * 10_000L,
            TimeUnit.Second => val * 10_000_000L,
            _ => val
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateTime GetTypedValue(int index)
    {
        long ticks = GetTicks(index);
        return new DateTime(
            DateTime.UnixEpoch.Ticks + ticks,
            _hasTimeZone ? DateTimeKind.Utc : DateTimeKind.Unspecified);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private DateTimeOffset GetDateTimeOffsetValue(int index)
    {
        long ticks = GetTicks(index);
        DateTimeOffset dtoUtc = new(DateTime.UnixEpoch.Ticks + ticks, TimeSpan.Zero);
        if (_timeZoneInfo is null)
        {
            return dtoUtc;
        }
        TimeSpan offset = _timeZoneInfo.GetUtcOffset(dtoUtc);
        return dtoUtc.ToOffset(offset);
    }

    public override object GetValue(int index)
    {
        if (IsNull(index)) return DBNull.Value;
        if (_targetType == typeof(DateTimeOffset)) return GetDateTimeOffsetValue(index);
        return GetTypedValue(index);
    }

    DateTime ITypedAccessor<DateTime>.GetTypedValue(int index) => GetTypedValue(index);
    DateTimeOffset ITypedAccessor<DateTimeOffset>.GetTypedValue(int index) => GetDateTimeOffsetValue(index);
    DateOnly ITypedAccessor<DateOnly>.GetTypedValue(int index) => DateOnly.FromDateTime(GetTypedValue(index));

    DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index) => GetTypedValue(index);
    DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index) => GetDateTimeOffsetValue(index);
    DateOnly? ITypedAccessor<DateOnly?>.GetTypedValue(int index) => DateOnly.FromDateTime(GetTypedValue(index));
}

internal sealed class Date32ToDateOnlyAccessor : TypedColumnAccessor<DateOnly>,
    ITypedAccessor<DateOnly>, ITypedAccessor<DateTime>, ITypedAccessor<DateTimeOffset>,
    ITypedAccessor<DateOnly?>, ITypedAccessor<DateTime?>, ITypedAccessor<DateTimeOffset?>
{
    private Date32Array? _array;
    private const int UnixEpochDayNumber = 719162;
    private readonly Type _targetType;

    public Date32ToDateOnlyAccessor(Type? targetType = null)
    {
        _targetType = targetType == typeof(DateTime) ? typeof(DateTime) : typeof(DateOnly);
    }

    public override Type TargetType => _targetType;
    public override void SetBatch(IArrowArray array) => _array = (Date32Array)array;
    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateOnly GetTypedValue(int index)
        => DateOnly.FromDayNumber(_array!.Values[index] + UnixEpochDayNumber);

    public override object GetValue(int index)
    {
        if (IsNull(index)) return DBNull.Value;
        if (_targetType == typeof(DateTime)) return GetTypedValue(index).ToDateTime(TimeOnly.MinValue);
        return GetTypedValue(index);
    }

    DateOnly ITypedAccessor<DateOnly>.GetTypedValue(int index) => GetTypedValue(index);
    DateTime ITypedAccessor<DateTime>.GetTypedValue(int index)
        => GetTypedValue(index).ToDateTime(TimeOnly.MinValue);
    DateTimeOffset ITypedAccessor<DateTimeOffset>.GetTypedValue(int index)
        => new(GetTypedValue(index).ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);

    DateOnly? ITypedAccessor<DateOnly?>.GetTypedValue(int index) => GetTypedValue(index);
    DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index)
        => GetTypedValue(index).ToDateTime(TimeOnly.MinValue);
    DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index)
        => new DateTimeOffset(GetTypedValue(index).ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
}

internal sealed class Date64Accessor : TypedColumnAccessor<DateTime>,
    ITypedAccessor<DateTime>, ITypedAccessor<DateTimeOffset>, ITypedAccessor<DateOnly>,
    ITypedAccessor<DateTime?>, ITypedAccessor<DateTimeOffset?>, ITypedAccessor<DateOnly?>
{
    private Date64Array? _array;
    public override Type TargetType => typeof(DateTime);
    public override void SetBatch(IArrowArray array) => _array = (Date64Array)array;
    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateTime GetTypedValue(int index)
        => _array!.GetDateTime(index)!.Value;

    DateTime ITypedAccessor<DateTime>.GetTypedValue(int index) => GetTypedValue(index);
    DateTimeOffset ITypedAccessor<DateTimeOffset>.GetTypedValue(int index) => new(GetTypedValue(index));
    DateOnly ITypedAccessor<DateOnly>.GetTypedValue(int index) => DateOnly.FromDateTime(GetTypedValue(index));

    DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index) => GetTypedValue(index);
    DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index) => new DateTimeOffset(GetTypedValue(index));
    DateOnly? ITypedAccessor<DateOnly?>.GetTypedValue(int index) => DateOnly.FromDateTime(GetTypedValue(index));
}

internal sealed class DurationAccessor : TypedColumnAccessor<TimeSpan>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>, ITypedAccessor<long>, ITypedAccessor<long?>
{
    private DurationArray? _array;
    private TimeUnit _unit;

    public override Type TargetType => typeof(TimeSpan);

    public override void SetBatch(IArrowArray array)
    {
        _array = (DurationArray)array;
        _unit = ((DurationType)_array.Data.DataType).Unit;
    }

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeSpan GetTypedValue(int index)
    {
        long val = _array!.Values[index];
        long ticks = _unit switch
        {
            TimeUnit.Nanosecond => val / 100L,
            TimeUnit.Microsecond => val * 10L,
            TimeUnit.Millisecond => val * 10_000L,
            TimeUnit.Second => val * 10_000_000L,
            _ => val
        };
        return new TimeSpan(ticks);
    }

    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index);
    long ITypedAccessor<long>.GetTypedValue(int index) => GetTypedValue(index).Ticks;

    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => GetTypedValue(index);
    long? ITypedAccessor<long?>.GetTypedValue(int index) => GetTypedValue(index).Ticks;
}

internal sealed class Int64ToTimeOnlyAccessor : TypedColumnAccessor<TimeOnly>,
    ITypedAccessor<TimeOnly>, ITypedAccessor<TimeOnly?>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>
{
    private Int64Array? _array;
    public override Type TargetType => typeof(TimeOnly);
    public override void SetBatch(IArrowArray array) => _array = (Int64Array)array;
    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeOnly GetTypedValue(int index)
    {
        long ticks = _array!.Values[index] * 10;
        return new TimeOnly(ticks);
    }

    TimeOnly ITypedAccessor<TimeOnly>.GetTypedValue(int index) => GetTypedValue(index);
    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();
    TimeOnly? ITypedAccessor<TimeOnly?>.GetTypedValue(int index) => GetTypedValue(index);
    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();
}

internal sealed class Time32Accessor : TypedColumnAccessor<TimeOnly>,
    ITypedAccessor<TimeOnly>, ITypedAccessor<TimeOnly?>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>
{
    private Time32Array? _array;
    private bool _isMillisecond;

    public override Type TargetType => typeof(TimeOnly);

    public override void SetBatch(IArrowArray array)
    {
        _array = (Time32Array)array;
        var unit = ((Time32Type)_array.Data.DataType).Unit;
        _isMillisecond = unit == TimeUnit.Millisecond;
    }

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeOnly GetTypedValue(int index)
    {
        int val = _array!.Values[index];
        long ticks = _isMillisecond ? (val * 10_000L) : (val * 10_000_000L);
        return new TimeOnly(ticks);
    }

    TimeOnly ITypedAccessor<TimeOnly>.GetTypedValue(int index) => GetTypedValue(index);
    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();

    TimeOnly? ITypedAccessor<TimeOnly?>.GetTypedValue(int index) => GetTypedValue(index);
    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();
}

internal sealed class Time64Accessor : TypedColumnAccessor<TimeOnly>,
    ITypedAccessor<TimeOnly>, ITypedAccessor<TimeOnly?>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>
{
    private Time64Array? _array;
    private bool _isNanosecond;

    public override Type TargetType => typeof(TimeOnly);

    public override void SetBatch(IArrowArray array)
    {
        _array = (Time64Array)array;
        var unit = ((Time64Type)_array.Data.DataType).Unit;
        _isNanosecond = unit == TimeUnit.Nanosecond;
    }

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeOnly GetTypedValue(int index)
    {
        long val = _array!.Values[index];
        long ticks = _isNanosecond ? (val / 100L) : (val * 10L);
        return new TimeOnly(ticks);
    }

    TimeOnly ITypedAccessor<TimeOnly>.GetTypedValue(int index) => GetTypedValue(index);
    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();

    TimeOnly? ITypedAccessor<TimeOnly?>.GetTypedValue(int index) => GetTypedValue(index);
    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();
}

internal sealed class ListAccessor<TElement> : TypedColumnAccessor<TElement[]>,
    ITypedAccessor<TElement[]>,
    ITypedAccessor<List<TElement>>,
    ITypedAccessor<IList<TElement>>,
    ITypedAccessor<ICollection<TElement>>,
    ITypedAccessor<IReadOnlyList<TElement>>,
    ITypedAccessor<IReadOnlyCollection<TElement>>,
    ITypedAccessor<IEnumerable<TElement>>,
    ITypedAccessor<FSharpList<TElement>>,
    ITypedAccessor<IOrderedEnumerable<TElement>>
{
    private IArrowArray? _array;
    private readonly ColumnAccessor _childAccessor;
    private readonly ITypedAccessor<TElement>? _typedChildAccessor;
    private readonly int _fixedListSize;

    public ListAccessor(Type targetType, IArrowType arrowType)
    {
        TargetType = targetType;

        IArrowType innerArrowType = arrowType switch
        {
            ListType lt => lt.ValueDataType,
            LargeListType llt => llt.ValueDataType,
            FixedSizeListType fslt => fslt.ValueDataType,
            _ => throw new InvalidOperationException("Not a list type")
        };

        if (arrowType is FixedSizeListType fixedType)
        {
            _fixedListSize = fixedType.ListSize;
        }

        var dummyField = new Field("item", innerArrowType, true);
        _childAccessor = ColumnAccessorFactory.Create(dummyField, typeof(TElement));
        _typedChildAccessor = _childAccessor as ITypedAccessor<TElement>;
    }

    public override Type TargetType { get; }

    public override void SetBatch(IArrowArray array)
    {
        _array = array;
        if (_array is ListArray list) _childAccessor.SetBatch(list.Values);
        else if (_array is LargeListArray largeList) _childAccessor.SetBatch(largeList.Values);
        else if (_array is FixedSizeListArray fixedList) _childAccessor.SetBatch(fixedList.Values);
    }

    public override bool IsNull(int index) => _array!.IsNull(index);

    public override TElement[] GetTypedValue(int index)
    {
        IArrowArray valuesArray;
        int offset, length;

        if (_array is ListArray list)
        {
            valuesArray = list.Values;
            offset = list.ValueOffsets[index];
            length = list.ValueOffsets[index + 1] - offset;
        }
        else if (_array is LargeListArray largeList)
        {
            valuesArray = largeList.Values;
            offset = (int)largeList.ValueOffsets[index];
            length = (int)(largeList.ValueOffsets[index + 1] - largeList.ValueOffsets[index]);
        }
        else if (_array is FixedSizeListArray fixedList)
        {
            valuesArray = fixedList.Values;
            offset = (index + fixedList.Offset) * _fixedListSize;
            length = _fixedListSize;
        }
        else
        {
            throw new InvalidDataException("Invalid List Array.");
        }

        if (length == 0) return [];

        bool hasChildNulls = valuesArray.NullCount > 0;

        // Fast-Path when child array has no nulls and exact primitive match
        if (!hasChildNulls)
        {
            if (typeof(TElement) == typeof(int) && valuesArray is Int32Array i32)
                return (TElement[])(object)i32.Values.Slice(offset, length).ToArray();

            if (typeof(TElement) == typeof(long) && valuesArray is Int64Array i64)
                return (TElement[])(object)i64.Values.Slice(offset, length).ToArray();

            if (typeof(TElement) == typeof(double) && valuesArray is DoubleArray dbl)
                return (TElement[])(object)dbl.Values.Slice(offset, length).ToArray();

            if (typeof(TElement) == typeof(float) && valuesArray is FloatArray flt)
                return (TElement[])(object)flt.Values.Slice(offset, length).ToArray();
        }

        if (typeof(TElement) == typeof(string))
        {
            var res = new string[length];
            if (valuesArray is StringViewArray sv)
            {
                for (int i = 0; i < length; i++) res[i] = sv.IsNull(offset + i) ? null! : sv.GetString(offset + i);
                return (TElement[])(object)res;
            }
            if (valuesArray is LargeStringArray lsa)
            {
                for (int i = 0; i < length; i++) res[i] = lsa.IsNull(offset + i) ? null! : lsa.GetString(offset + i);
                return (TElement[])(object)res;
            }
            if (valuesArray is StringArray sa)
            {
                for (int i = 0; i < length; i++) res[i] = sa.IsNull(offset + i) ? null! : sa.GetString(offset + i);
                return (TElement[])(object)res;
            }
        }

        var result = new TElement[length];

        if (_typedChildAccessor != null)
        {
            for (int i = 0; i < length; i++)
            {
                result[i] = _childAccessor.IsNull(offset + i)
                    ? default!
                    : _typedChildAccessor.GetTypedValue(offset + i);
            }
        }
        else
        {
            for (int i = 0; i < length; i++)
            {
                result[i] = _childAccessor.IsNull(offset + i)
                    ? default!
                    : (TElement)_childAccessor.GetValue(offset + i)!;
            }
        }

        return result;
    }

    List<TElement> ITypedAccessor<List<TElement>>.GetTypedValue(int index) => [.. GetTypedValue(index)];
    IList<TElement> ITypedAccessor<IList<TElement>>.GetTypedValue(int index) => [.. GetTypedValue(index)];
    ICollection<TElement> ITypedAccessor<ICollection<TElement>>.GetTypedValue(int index) => [.. GetTypedValue(index)];
    IReadOnlyList<TElement> ITypedAccessor<IReadOnlyList<TElement>>.GetTypedValue(int index) => GetTypedValue(index);
    IReadOnlyCollection<TElement> ITypedAccessor<IReadOnlyCollection<TElement>>.GetTypedValue(int index) => GetTypedValue(index);
    IEnumerable<TElement> ITypedAccessor<IEnumerable<TElement>>.GetTypedValue(int index) => GetTypedValue(index);
    FSharpList<TElement> ITypedAccessor<FSharpList<TElement>>.GetTypedValue(int index) => ListModule.OfArray(GetTypedValue(index));
    IOrderedEnumerable<TElement> ITypedAccessor<IOrderedEnumerable<TElement>>.GetTypedValue(int index) => new PresortedOrderedEnumerable<TElement>(GetTypedValue(index));

    private sealed class PresortedOrderedEnumerable<TItem>(IList<TItem> source) : IOrderedEnumerable<TItem>
    {
        public IOrderedEnumerable<TItem> CreateOrderedEnumerable<TKey>(
            Func<TItem, TKey> keySelector,
            IComparer<TKey>? comparer,
            bool descending)
        {
            return descending
                ? source.OrderByDescending(keySelector, comparer)
                : source.OrderBy(keySelector, comparer);
        }

        public IEnumerator<TItem> GetEnumerator() => source.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => source.GetEnumerator();
    }
}

internal sealed class StructAccessor<T> : TypedColumnAccessor<T>, ITypedAccessor<T>
{
    private StructArray? _array;
    private readonly ColumnAccessor[] _childAccessors;
    private readonly string[]? _dictFieldNames;
    private readonly CompiledArrowRowMapper<T>? _compiledMapper;

    public StructAccessor(Type targetType, IArrowType arrowType)
    {
        TargetType = targetType;

        if (arrowType is not StructType structType)
            throw new InvalidOperationException($"Expected StructType, got {arrowType.Name}");

        if (targetType == typeof(Dictionary<string, object?>) ||
            targetType == typeof(Dictionary<string, object>) ||
            targetType == typeof(object))
        {
            int count = structType.Fields.Count;
            _childAccessors = new ColumnAccessor[count];
            _dictFieldNames = new string[count];
            for (int i = 0; i < count; i++)
            {
                var field = structType.Fields[i];
                _dictFieldNames[i] = field.Name;
                Type netType = ArrowTypeResolver.GetNetTypeFromArrowType(field.DataType);
                _childAccessors[i] = ColumnAccessorFactory.Create(field, netType);
            }
        }
        else
        {
            var structSchema = new Schema(structType.Fields, null);
            _compiledMapper = ArrowRowMapper<T>.GetOrCreate(structSchema);
            _childAccessors = _compiledMapper.CreateAccessors();
        }
    }

    public override Type TargetType { get; }

    public override void SetBatch(IArrowArray array)
    {
        _array = (StructArray)array;
        if (_compiledMapper != null)
        {
            _compiledMapper.BindBatch(_childAccessors, _array.Fields);
        }
        else
        {
            for (int i = 0; i < _childAccessors.Length; i++)
            {
                _childAccessors[i].SetBatch(_array.Fields[i]);
            }
        }
    }

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override T GetTypedValue(int index)
    {
        if (_array!.IsNull(index)) return default!;

        if (_compiledMapper != null)
        {
            return _compiledMapper.HydrateRow(_childAccessors, index);
        }

        var dict = new Dictionary<string, object?>(_childAccessors.Length);
        for (int i = 0; i < _childAccessors.Length; i++)
        {
            dict[_dictFieldNames![i]] = _childAccessors[i].IsNull(index) ? null : _childAccessors[i].GetValue(index);
        }
        return (T)(object)dict;
    }

    T ITypedAccessor<T>.GetTypedValue(int index) => GetTypedValue(index);
}

internal sealed class StructToListAccessor<TElement> : TypedColumnAccessor<TElement[]>,
    ITypedAccessor<TElement[]>,
    ITypedAccessor<List<TElement>>,
    ITypedAccessor<IList<TElement>>,
    ITypedAccessor<ICollection<TElement>>,
    ITypedAccessor<IReadOnlyList<TElement>>,
    ITypedAccessor<IReadOnlyCollection<TElement>>,
    ITypedAccessor<IEnumerable<TElement>>,
    ITypedAccessor<FSharpList<TElement>>
{
    private StructArray? _array;
    private readonly ColumnAccessor[] _childAccessors;
    private readonly ITypedAccessor<TElement>?[] _typedChildAccessors;

    public StructToListAccessor(Type targetType, IArrowType arrowType)
    {
        TargetType = targetType;

        if (arrowType is not StructType structType)
            throw new InvalidOperationException($"Expected StructType, got {arrowType.Name}");

        int count = structType.Fields.Count;
        _childAccessors = new ColumnAccessor[count];
        _typedChildAccessors = new ITypedAccessor<TElement>?[count];
        for (int i = 0; i < count; i++)
        {
            var field = structType.Fields[i];
            _childAccessors[i] = ColumnAccessorFactory.Create(field, typeof(TElement));
            _typedChildAccessors[i] = _childAccessors[i] as ITypedAccessor<TElement>;
        }
    }

    public override Type TargetType { get; }

    public override void SetBatch(IArrowArray array)
    {
        _array = (StructArray)array;
        for (int i = 0; i < _childAccessors.Length; i++)
        {
            _childAccessors[i].SetBatch(_array.Fields[i]);
        }
    }

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TElement[] GetTypedValue(int index)
    {
        if (_array!.IsNull(index)) return null!;

        int count = _childAccessors.Length;
        var result = new TElement[count];
        for (int i = 0; i < count; i++)
        {
            var acc = _childAccessors[i];
            if (acc.IsNull(index))
            {
                result[i] = default!;
            }
            else if (_typedChildAccessors[i] is { } typedAcc)
            {
                result[i] = typedAcc.GetTypedValue(index);
            }
            else
            {
                result[i] = (TElement)acc.GetValue(index)!;
            }
        }
        return result;
    }

    List<TElement> ITypedAccessor<List<TElement>>.GetTypedValue(int index) => [.. GetTypedValue(index)];
    IList<TElement> ITypedAccessor<IList<TElement>>.GetTypedValue(int index) => [.. GetTypedValue(index)];
    ICollection<TElement> ITypedAccessor<ICollection<TElement>>.GetTypedValue(int index) => [.. GetTypedValue(index)];
    IReadOnlyList<TElement> ITypedAccessor<IReadOnlyList<TElement>>.GetTypedValue(int index) => GetTypedValue(index);
    IReadOnlyCollection<TElement> ITypedAccessor<IReadOnlyCollection<TElement>>.GetTypedValue(int index) => GetTypedValue(index);
    IEnumerable<TElement> ITypedAccessor<IEnumerable<TElement>>.GetTypedValue(int index) => GetTypedValue(index);
    FSharpList<TElement> ITypedAccessor<FSharpList<TElement>>.GetTypedValue(int index) => ListModule.OfArray(GetTypedValue(index));
}

internal sealed class FSharpOptionAccessor<T>(ColumnAccessor innerAccessor, Type targetType) : TypedColumnAccessor<FSharpOption<T>>, ITypedAccessor<FSharpOption<T>>
{
    private readonly ITypedAccessor<T>? _typedInnerAccessor = innerAccessor as ITypedAccessor<T>;

    public override Type TargetType => targetType;
    public override void SetBatch(IArrowArray array) => innerAccessor.SetBatch(array);
    public override bool IsNull(int index) => innerAccessor.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override FSharpOption<T> GetTypedValue(int index)
    {
        if (innerAccessor.IsNull(index)) return null!;

        if (_typedInnerAccessor != null)
        {
            T rawValue = _typedInnerAccessor.GetTypedValue(index);
            return FSharpOption<T>.Some(rawValue);
        }

        var boxedValue = innerAccessor.GetValue(index);
        return FSharpOption<T>.Some((T)boxedValue!);
    }

    FSharpOption<T> ITypedAccessor<FSharpOption<T>>.GetTypedValue(int index) => GetTypedValue(index);
}

internal sealed class FSharpValueOptionAccessor<T>(ColumnAccessor innerAccessor, Type targetType) : TypedColumnAccessor<FSharpValueOption<T>>, ITypedAccessor<FSharpValueOption<T>>
{
    private readonly ITypedAccessor<T>? _typedInnerAccessor = innerAccessor as ITypedAccessor<T>;

    public override Type TargetType => targetType;
    public override void SetBatch(IArrowArray array) => innerAccessor.SetBatch(array);
    public override bool IsNull(int index) => innerAccessor.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override FSharpValueOption<T> GetTypedValue(int index)
    {
        if (innerAccessor.IsNull(index)) return FSharpValueOption<T>.ValueNone;

        if (_typedInnerAccessor != null)
        {
            T rawValue = _typedInnerAccessor.GetTypedValue(index);
            return FSharpValueOption<T>.NewValueSome(rawValue);
        }

        var boxedValue = innerAccessor.GetValue(index);
        if (boxedValue == DBNull.Value || boxedValue == null) return FSharpValueOption<T>.ValueNone;

        return FSharpValueOption<T>.NewValueSome((T)boxedValue);
    }

    FSharpValueOption<T> ITypedAccessor<FSharpValueOption<T>>.GetTypedValue(int index) => GetTypedValue(index);
}

internal sealed class JsonFallbackAccessor<T> : TypedColumnAccessor<T>, ITypedAccessor<T>
{
    private IArrowArray? _array;

    public override Type TargetType => typeof(T);
    public override void SetBatch(IArrowArray array) => _array = array;
    public override bool IsNull(int index) => _array!.IsNull(index);

    public override T GetTypedValue(int index)
    {
        var rawValue = ExtractValue(_array!, index);

        if (typeof(T) == typeof(string))
        {
            var json = rawValue is string s ? s : JsonSerializer.Serialize(rawValue);
            return (T)(object)json;
        }

        if (rawValue is T typedValue)
        {
            return typedValue;
        }

        if (rawValue != null)
        {
            var json = JsonSerializer.Serialize(rawValue);
            return (T)(object)json;
        }

        return default!;
    }

    T ITypedAccessor<T>.GetTypedValue(int index) => GetTypedValue(index);

    private static object? ExtractValue(IArrowArray array, int index)
    {
        return array switch
        {
            Int32Array i32 => i32.Values[index],
            Int64Array i64 => i64.Values[index],
            DoubleArray dbl => dbl.Values[index],
            FloatArray flt => flt.Values[index],
            BooleanArray b => b.Values[index],
            StringArray s => s.GetString(index),
            StringViewArray sv => sv.GetString(index),
            Date32Array d32 => d32.GetDateTime(index),
            Date64Array d64 => d64.GetDateTime(index),
            Time64Array t64 => t64.GetDateTime(index),
            TimestampArray ts => ts.GetTimestamp(index)?.DateTime,
            _ => null
        };
    }
}
