using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Numerics;
using System.Collections;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using System.Text.Json;
using Apache.Arrow;
using Apache.Arrow.Arrays;
using Apache.Arrow.Types;
using Microsoft.FSharp.Collections;
using Microsoft.FSharp.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Helpers;

namespace Polars.NET.Core.Data;

public interface ITypedAccessor<T>
{
    T GetTypedValue(int index);
}

public interface IBinarySpanAccessor
{
    ReadOnlySpan<byte> GetBytesSpan(int index);
}

public abstract unsafe class ColumnAccessor : IDisposable
{
    private static readonly Type? NativeMemoryManagerType =
        typeof(IArrowArray).Assembly.GetType("Apache.Arrow.Memory.NativeMemoryManager");

    private IArrowArray? _activeArray;
    private MemoryHandle _nullBitmapHandle;
    private MemoryHandle _valueBufferHandle;
    protected byte* _nullBitmapPtr;
    protected int _offset;
    protected bool _alwaysNull;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public abstract Type TargetType { get; }
    public virtual bool UsesFastNullBitmap => false;
    public abstract void SetBatch(IArrowArray array);
    public abstract bool IsNull(int index);
    public abstract object? GetValue(int index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected static byte* GetBufferPointer(ReadOnlyMemory<byte> memory, ref MemoryHandle handle)
    {
        handle.Dispose();
        if (memory.IsEmpty) return null;
        if (MemoryMarshal.TryGetArray(memory, out _))
        {
            handle = memory.Pin();
            return (byte*)handle.Pointer;
        }
        if (MemoryMarshal.TryGetMemoryManager<byte, MemoryManager<byte>>(memory, out var manager) &&
            !ReferenceEquals(manager.GetType(), NativeMemoryManagerType))
        {
            handle = memory.Pin();
            return (byte*)handle.Pointer;
        }
        return (byte*)Unsafe.AsPointer(ref MemoryMarshal.GetReference(memory.Span));
    }

    protected void BindNullBitmap(IArrowArray array)
    {
        _activeArray = array;
        _offset = array.Offset;
        _alwaysNull = false;
        if (array.NullCount > 0 && array.Data.Buffers.Length > 0 && !array.Data.Buffers[0].IsEmpty)
        {
            _nullBitmapPtr = GetBufferPointer(array.Data.Buffers[0].Memory, ref _nullBitmapHandle);
        }
        else
        {
            _nullBitmapHandle.Dispose();
            _nullBitmapPtr = null;
        }
    }

    protected TVal* BindPrimitiveBuffer<TVal>(IArrowArray array, int bufferIndex = 1) where TVal : unmanaged
    {
        byte* ptr = BindRawBuffer(array, bufferIndex);
        return ptr != null ? (TVal*)ptr + array.Offset : null;
    }

    /// <summary>
    /// Binds the null bitmap and returns the start of the value buffer WITHOUT applying the array offset.
    /// Used for bit-packed buffers (Boolean), where the offset is in bits and must be applied per index.
    /// </summary>
    protected byte* BindRawBuffer(IArrowArray array, int bufferIndex = 1)
    {
        BindNullBitmap(array);
        if (array.Length == 0 || array.Data.Buffers.Length <= bufferIndex || array.Data.Buffers[bufferIndex].IsEmpty)
        {
            _valueBufferHandle.Dispose();
            return null;
        }
        return GetBufferPointer(array.Data.Buffers[bufferIndex].Memory, ref _valueBufferHandle);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool IsNullFast(int index)
    {
        if (_nullBitmapPtr == null) return _alwaysNull;
        int bitIndex = index + _offset;
        return (_nullBitmapPtr[bitIndex >> 3] & (1 << (bitIndex & 7))) == 0;
    }

    public virtual void Dispose()
    {
        _nullBitmapHandle.Dispose();
        _valueBufferHandle.Dispose();
        _nullBitmapPtr = null;
        _activeArray = null;
    }
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
    public static ColumnAccessor Create(Field field, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] Type targetType)
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
            Int8Type => new NumericAccessor<sbyte>(),
            Int16Type => new NumericAccessor<short>(),
            Int32Type => new NumericAccessor<int>(),
            UInt8Type => new NumericAccessor<byte>(),
            UInt16Type => new NumericAccessor<ushort>(),
            UInt32Type => new NumericAccessor<uint>(),
            UInt64Type => new NumericAccessor<ulong>(),

            // 2. Int64 & Magic Types
            Int64Type => effectiveType switch
            {
                { } t when t == typeof(DateTime) => new Int64ToDateTimeAccessor(),
                { } t when t == typeof(TimeSpan) => new Int64ToTimeSpanAccessor(),
                { } t when t == typeof(TimeOnly) => new Int64ToTimeOnlyAccessor(),
                _ => new NumericAccessor<long>()
            },

            HalfFloatType => new NumericAccessor<Half>(),
            FloatType => new NumericAccessor<float>(),
            DoubleType => new NumericAccessor<double>(),

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

    private static ColumnAccessor CreateListAccessor(IArrowType arrowType, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] Type targetType)
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

    private static ColumnAccessor CreateStructAccessor(IArrowType arrowType, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] Type targetType)
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

internal sealed class NullAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] T> : TypedColumnAccessor<T>, ITypedAccessor<T?>
{
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
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

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(Guid);

    public override void SetBatch(IArrowArray array) => _array = (FixedSizeBinaryArray)array;

    public override bool IsNull(int index) => _array!.IsNull(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int index) => _array!.GetBytes(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override Guid GetTypedValue(int index) => new(_array!.GetBytes(index));

    Guid ITypedAccessor<Guid>.GetTypedValue(int index) => GetTypedValue(index);
    Guid? ITypedAccessor<Guid?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
}

internal sealed class FixedSizeBinaryAccessor : TypedColumnAccessor<byte[]>,
    ITypedAccessor<byte[]>,
    ITypedAccessor<Guid>, ITypedAccessor<Guid?>,
    IBinarySpanAccessor
{
    private FixedSizeBinaryArray? _array;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(byte[]);
    public override void SetBatch(IArrowArray array) => _array = (FixedSizeBinaryArray)array;
    public override bool IsNull(int index) => _array!.IsNull(index);
    public override byte[] GetTypedValue(int index) => _array!.GetBytes(index).ToArray();

    Guid ITypedAccessor<Guid>.GetTypedValue(int index) => new(_array!.GetBytes(index));
    Guid? ITypedAccessor<Guid?>.GetTypedValue(int index) => IsNull(index) ? null : new Guid(_array!.GetBytes(index));
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int index) => _array!.GetBytes(index);
    byte[] ITypedAccessor<byte[]>.GetTypedValue(int index) => GetTypedValue(index);
}

internal sealed class BinaryToGuidAccessor : TypedColumnAccessor<Guid>,
    ITypedAccessor<Guid>, ITypedAccessor<Guid?>, IBinarySpanAccessor
{
    private IArrowArray? _array;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
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
    Guid? ITypedAccessor<Guid?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
}

internal sealed unsafe class BooleanAccessor : TypedColumnAccessor<bool>,
    ITypedAccessor<bool>, ITypedAccessor<bool?>
{
    private byte* _bits;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(bool);
    public override bool UsesFastNullBitmap => true;
    public override void SetBatch(IArrowArray array) => _bits = BindRawBuffer(array);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool GetTypedValue(int index)
    {
        int bit = index + _offset;
        return (_bits[bit >> 3] & (1 << (bit & 7))) != 0;
    }
    bool ITypedAccessor<bool>.GetTypedValue(int index) => GetTypedValue(index);
    bool? ITypedAccessor<bool?>.GetTypedValue(int index) => IsNullFast(index) ? null : GetTypedValue(index);
}

/// <summary>
/// Accessor for all fixed-width numeric Arrow columns (Int8..UInt64, Float16/32/64).
/// Every source type exposes the same conversion matrix through <see cref="INumberBase{TSelf}"/>:
/// integer narrowing wraps, float-to-integer saturates (CreateTruncating), and conversion to
/// <see cref="decimal"/> is checked (NaN / out-of-range throws <see cref="OverflowException"/>).
/// TSrc is always a value type, so the JIT specializes each instantiation and the conversions inline.
/// </summary>
internal sealed unsafe class NumericAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] TSrc> : TypedColumnAccessor<TSrc>,
    ITypedAccessor<sbyte>, ITypedAccessor<byte>, ITypedAccessor<short>, ITypedAccessor<ushort>, ITypedAccessor<int>,
    ITypedAccessor<uint>, ITypedAccessor<long>, ITypedAccessor<ulong>, ITypedAccessor<Int128>, ITypedAccessor<UInt128>,
    ITypedAccessor<Half>, ITypedAccessor<float>, ITypedAccessor<double>, ITypedAccessor<decimal>, ITypedAccessor<char>,
    ITypedAccessor<sbyte?>, ITypedAccessor<byte?>, ITypedAccessor<short?>, ITypedAccessor<ushort?>,
    ITypedAccessor<int?>, ITypedAccessor<uint?>, ITypedAccessor<long?>, ITypedAccessor<ulong?>, ITypedAccessor<Int128?>,
    ITypedAccessor<UInt128?>, ITypedAccessor<Half?>, ITypedAccessor<float?>, ITypedAccessor<double?>, ITypedAccessor<decimal?>,
    ITypedAccessor<char?>
    where TSrc : unmanaged, INumberBase<TSrc>
{
    private TSrc* _ptr;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(TSrc);
    public override bool UsesFastNullBitmap => true;
    public override void SetBatch(IArrowArray array) => _ptr = BindPrimitiveBuffer<TSrc>(array);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TSrc GetTypedValue(int index) => _ptr[index];

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static TDst To<TDst>(TSrc value) where TDst : INumberBase<TDst> => TDst.CreateTruncating(value);

    sbyte ITypedAccessor<sbyte>.GetTypedValue(int index) => To<sbyte>(_ptr[index]);
    byte ITypedAccessor<byte>.GetTypedValue(int index) => To<byte>(_ptr[index]);
    short ITypedAccessor<short>.GetTypedValue(int index) => To<short>(_ptr[index]);
    ushort ITypedAccessor<ushort>.GetTypedValue(int index) => To<ushort>(_ptr[index]);
    int ITypedAccessor<int>.GetTypedValue(int index) => To<int>(_ptr[index]);
    uint ITypedAccessor<uint>.GetTypedValue(int index) => To<uint>(_ptr[index]);
    long ITypedAccessor<long>.GetTypedValue(int index) => To<long>(_ptr[index]);
    ulong ITypedAccessor<ulong>.GetTypedValue(int index) => To<ulong>(_ptr[index]);
    Int128 ITypedAccessor<Int128>.GetTypedValue(int index) => To<Int128>(_ptr[index]);
    UInt128 ITypedAccessor<UInt128>.GetTypedValue(int index) => To<UInt128>(_ptr[index]);
    Half ITypedAccessor<Half>.GetTypedValue(int index) => To<Half>(_ptr[index]);
    float ITypedAccessor<float>.GetTypedValue(int index) => To<float>(_ptr[index]);
    double ITypedAccessor<double>.GetTypedValue(int index) => To<double>(_ptr[index]);
    decimal ITypedAccessor<decimal>.GetTypedValue(int index) => decimal.CreateChecked(_ptr[index]);
    char ITypedAccessor<char>.GetTypedValue(int index) => To<char>(_ptr[index]);

    sbyte? ITypedAccessor<sbyte?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<sbyte>(_ptr[index]);
    byte? ITypedAccessor<byte?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<byte>(_ptr[index]);
    short? ITypedAccessor<short?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<short>(_ptr[index]);
    ushort? ITypedAccessor<ushort?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<ushort>(_ptr[index]);
    int? ITypedAccessor<int?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<int>(_ptr[index]);
    uint? ITypedAccessor<uint?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<uint>(_ptr[index]);
    long? ITypedAccessor<long?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<long>(_ptr[index]);
    ulong? ITypedAccessor<ulong?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<ulong>(_ptr[index]);
    Int128? ITypedAccessor<Int128?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<Int128>(_ptr[index]);
    UInt128? ITypedAccessor<UInt128?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<UInt128>(_ptr[index]);
    Half? ITypedAccessor<Half?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<Half>(_ptr[index]);
    float? ITypedAccessor<float?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<float>(_ptr[index]);
    double? ITypedAccessor<double?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<double>(_ptr[index]);
    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => IsNullFast(index) ? null : decimal.CreateChecked(_ptr[index]);
    char? ITypedAccessor<char?>.GetTypedValue(int index) => IsNullFast(index) ? null : To<char>(_ptr[index]);
}

internal sealed unsafe class DecimalAccessor : TypedColumnAccessor<decimal>,
    ITypedAccessor<decimal>, ITypedAccessor<double>, ITypedAccessor<float>, ITypedAccessor<long>, ITypedAccessor<int>,
    ITypedAccessor<decimal?>, ITypedAccessor<double?>, ITypedAccessor<float?>, ITypedAccessor<long?>, ITypedAccessor<int?>
{
    private const int MaxDotNetDecimalScale = 28;
    private static readonly UInt128 MaxDecimalBits = unchecked(((UInt128)1 << 96) - 1);

    private Decimal128Array? _array128;
    private Decimal256Array? _array256;
    private Int128* _ptr128;
    private int _scale;
    private int _precision;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(decimal);
    public override bool UsesFastNullBitmap => true;

    public override void SetBatch(IArrowArray array)
    {
        if (array is Decimal128Array d128)
        {
            _array128 = d128;
            _array256 = null;
            _scale = d128.Scale;
            _precision = d128.Precision;
            _ptr128 = _scale is >= 0 and <= MaxDotNetDecimalScale
                ? BindPrimitiveBuffer<Int128>(d128)
                : null;
            if (_ptr128 == null) BindNullBitmap(d128);
        }
        else if (array is Decimal256Array d256)
        {
            _array256 = d256;
            _array128 = null;
            _ptr128 = null;
            _scale = d256.Scale;
            _precision = d256.Precision;
            BindNullBitmap(d256);
        }
        else
        {
            throw new InvalidCastException($"Expected Decimal128Array or Decimal256Array, got {array?.GetType()}");
        }
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override decimal GetTypedValue(int index)
    {
        if (_ptr128 != null)
        {
            Int128 val = _ptr128[index];
            bool isNegative = val < 0;
            UInt128 uval = isNegative ? unchecked((UInt128)(-val)) : unchecked((UInt128)val);
            if (uval <= ulong.MaxValue)
            {
                ulong u64 = (ulong)uval;
                return new decimal((int)u64, (int)(u64 >> 32), 0, isNegative, (byte)_scale);
            }
            if (uval <= MaxDecimalBits)
            {
                int low = (int)(uval & 0xFFFF_FFFF);
                int mid = (int)((uval >> 32) & 0xFFFF_FFFF);
                int high = (int)((uval >> 64) & 0xFFFF_FFFF);
                return new decimal(low, mid, high, isNegative, (byte)_scale);
            }

            throw new OverflowException($"Decimal value with precision {_precision} exceeds the 96-bit capacity of .NET decimal.");
        }

        if (_array128 != null)
        {
            return _array128.GetValue(index)!.Value;
        }

        return _array256!.GetValue(index)!.Value;
    }

    double ITypedAccessor<double>.GetTypedValue(int index) => (double)GetTypedValue(index);
    float ITypedAccessor<float>.GetTypedValue(int index) => (float)GetTypedValue(index);
    long ITypedAccessor<long>.GetTypedValue(int index) => (long)GetTypedValue(index);
    int ITypedAccessor<int>.GetTypedValue(int index) => (int)GetTypedValue(index);

    decimal? ITypedAccessor<decimal?>.GetTypedValue(int index) => IsNullFast(index) ? null : GetTypedValue(index);
    double? ITypedAccessor<double?>.GetTypedValue(int index) => IsNullFast(index) ? null : (double)GetTypedValue(index);
    float? ITypedAccessor<float?>.GetTypedValue(int index) => IsNullFast(index) ? null : (float)GetTypedValue(index);
    long? ITypedAccessor<long?>.GetTypedValue(int index) => IsNullFast(index) ? null : (long)GetTypedValue(index);
    int? ITypedAccessor<int?>.GetTypedValue(int index) => IsNullFast(index) ? null : (int)GetTypedValue(index);
}

internal sealed unsafe class StringViewAccessor : TypedColumnAccessor<string>,
    IBinarySpanAccessor,
    ITypedAccessor<string>,
    ITypedAccessor<char>,
    ITypedAccessor<char?>
{
    [StructLayout(LayoutKind.Sequential, Size = 16)]
    private readonly struct ArrowStringView { }

    private IArrowArray? _array;
    private StringViewArray? _viewArray;
    private byte* _viewsPtr;
    private MemoryHandle[]? _dataBufferHandles;
    private nint[]? _dataBufferPtrs;

    // Decoded strings by their 16-byte view, direct mapped. Within one array, equal views are equal strings: an
    // inlined view holds the whole string, a referencing view its buffer and offset. Rows gathered from the same
    // source row (a dimension's strings repeated by a join, a filtered or sorted column) share their views, so
    // repeated values decode and allocate once. Cleared per array, since buffer indices are per array.
    // A column whose values hardly repeat (ids, free text) stops using the cache after a probation of
    // lookups with few hits, so it pays only for the probation.
    private const int StringCacheBits = 10;
    private const int CacheProbation = 4096;
    private ulong[]? _cacheKeys;
    private string?[]? _cacheValues;
    private bool _cacheEnabled;
    private int _cacheLookups;
    private int _cacheHits;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(string);
    public override bool UsesFastNullBitmap => true;

    public override void SetBatch(IArrowArray array)
    {
        ReleaseDataBuffers();
        _array = array;
        if (array is StringViewArray viewArray)
        {
            if (_cacheValues is null)
            {
                _cacheKeys = new ulong[2 << StringCacheBits];
                _cacheValues = new string?[1 << StringCacheBits];
            }
            else
            {
                System.Array.Clear(_cacheValues);
            }
            _cacheEnabled = true;
            _cacheLookups = 0;
            _cacheHits = 0;
            _viewArray = viewArray;
            ArrowStringView* rawViews = BindPrimitiveBuffer<ArrowStringView>(viewArray, 1);
            _viewsPtr = (byte*)rawViews;
            int totalBuffers = viewArray.Data.Buffers.Length;
            int dataBufferCount = totalBuffers - 2;
            if (dataBufferCount > 0)
            {
                _dataBufferHandles = new MemoryHandle[dataBufferCount];
                _dataBufferPtrs = new nint[dataBufferCount];
                for (int i = 0; i < dataBufferCount; i++)
                {
                    var buf = viewArray.Data.Buffers[i + 2];
                    if (!buf.IsEmpty)
                    {
                        _dataBufferPtrs[i] = (nint)GetBufferPointer(buf.Memory, ref _dataBufferHandles[i]);
                    }
                }
            }
        }
        else
        {
            BindNullBitmap(array);
            _viewArray = null;
            _viewsPtr = null;
        }
    }

    private void ReleaseDataBuffers()
    {
        if (_dataBufferHandles != null)
        {
            for (int i = 0; i < _dataBufferHandles.Length; i++)
            {
                _dataBufferHandles[i].Dispose();
            }
            _dataBufferHandles = null;
            _dataBufferPtrs = null;
        }
    }

    public override void Dispose()
    {
        ReleaseDataBuffers();
        base.Dispose();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override string GetTypedValue(int index)
    {
        if (_viewsPtr != null)
        {
            byte* viewPtr = _viewsPtr + ((nint)index << 4);
            int length = *(int*)viewPtr;
            if (length == 0) return string.Empty;
            if (!_cacheEnabled) return DecodeView(viewPtr, length);

            ulong k0 = *(ulong*)viewPtr;
            ulong k1 = *(ulong*)(viewPtr + 8);
            if (length < 12)
            {
                // Only the inlined string's own bytes identify it.
                if (length < 4)
                {
                    k0 &= (1UL << (32 + length * 8)) - 1;
                    k1 = 0;
                }
                else
                {
                    k1 &= (1UL << ((length - 4) * 8)) - 1;
                }
            }

            int slot = (int)(((k0 ^ (k1 * 0x9E3779B97F4A7C15UL)) * 0xBF58476D1CE4E5B9UL) >> (64 - StringCacheBits));
            string? cached = _cacheValues![slot];
            if (cached is not null && _cacheKeys![2 * slot] == k0 && _cacheKeys[2 * slot + 1] == k1)
            {
                _cacheHits++;
                return cached;
            }

            if (++_cacheLookups - _cacheHits >= CacheProbation && _cacheHits * 8 < _cacheLookups)
            {
                _cacheEnabled = false;
            }

            string value = DecodeView(viewPtr, length);
            _cacheKeys![2 * slot] = k0;
            _cacheKeys[2 * slot + 1] = k1;
            _cacheValues[slot] = value;
            return value;
        }

        if (_viewArray != null) return _viewArray.GetString(index);
        if (_array is LargeStringArray lsa) return lsa.GetString(index);
        if (_array is StringArray sa) return sa.GetString(index);

        throw new InvalidCastException($"Expected String or StringView, got {_array?.GetType()}");
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private string DecodeView(byte* viewPtr, int length) =>
        length <= 12
            ? Encoding.UTF8.GetString(viewPtr + 4, length)
            : Encoding.UTF8.GetString((byte*)_dataBufferPtrs![*(int*)(viewPtr + 8)] + *(int*)(viewPtr + 12), length);

    char ITypedAccessor<char>.GetTypedValue(int index)
    {
        string s = GetTypedValue(index);
        return string.IsNullOrEmpty(s) ? '\0' : s[0];
    }

    char? ITypedAccessor<char?>.GetTypedValue(int index)
    {
        if (IsNullFast(index)) return null;
        string s = GetTypedValue(index);
        return string.IsNullOrEmpty(s) ? '\0' : s[0];
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ReadOnlySpan<byte> GetBytesSpan(int index)
    {
        if (_viewsPtr != null)
        {
            byte* viewPtr = _viewsPtr + ((nint)index << 4);
            int length = *(int*)viewPtr;
            if (length == 0) return ReadOnlySpan<byte>.Empty;
            if (length <= 12)
            {
                return new ReadOnlySpan<byte>(viewPtr + 4, length);
            }
            int bufIdx = *(int*)(viewPtr + 8);
            int bufOffset = *(int*)(viewPtr + 12);
            byte* dataPtr = (byte*)_dataBufferPtrs![bufIdx] + bufOffset;
            return new ReadOnlySpan<byte>(dataPtr, length);
        }

        if (_viewArray != null) return _viewArray.GetBytes(index);
        if (_array is LargeStringArray lsa) return lsa.GetBytes(index);
        if (_array is StringArray sa) return sa.GetBytes(index);

        throw new InvalidCastException($"Expected String or StringView, got {_array?.GetType()}");
    }
}

internal sealed unsafe class DictionaryAccessor : TypedColumnAccessor<string>,
    ITypedAccessor<string>
{
    private DictionaryArray? _array;
    private string[]? _cachedStrings;
    private MemoryHandle _indicesBufferHandle;
    private byte* _indicesPtr;
    private delegate*<byte*, int, int> _readKeyFn;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(string);
    public override bool UsesFastNullBitmap => true;

    private static int ReadUInt8(byte* ptr, int index) => ptr[index];
    private static int ReadInt8(byte* ptr, int index) => ((sbyte*)ptr)[index];
    private static int ReadUInt16(byte* ptr, int index) => ((ushort*)ptr)[index];
    private static int ReadInt16(byte* ptr, int index) => ((short*)ptr)[index];
    private static int ReadUInt32(byte* ptr, int index) => (int)((uint*)ptr)[index];
    private static int ReadInt32(byte* ptr, int index) => ((int*)ptr)[index];
    private static int ReadUInt64(byte* ptr, int index) => (int)((ulong*)ptr)[index];
    private static int ReadInt64(byte* ptr, int index) => (int)((long*)ptr)[index];

    internal static byte* BindIndexBuffer(
        IArrowArray indices,
        ref MemoryHandle handle,
        out delegate*<byte*, int, int> reader)
    {
        reader = indices switch
        {
            UInt32Array => &ReadUInt32,
            Int32Array => &ReadInt32,
            UInt16Array => &ReadUInt16,
            Int16Array => &ReadInt16,
            UInt8Array => &ReadUInt8,
            Int8Array => &ReadInt8,
            Int64Array => &ReadInt64,
            UInt64Array => &ReadUInt64,
            _ => throw new NotSupportedException($"Unsupported Dictionary index array type: {indices.GetType().Name}")
        };

        if (indices.Length == 0 || indices.Data.Buffers.Length <= 1 || indices.Data.Buffers[1].IsEmpty)
        {
            handle.Dispose();
            return null;
        }

        byte* raw = GetBufferPointer(indices.Data.Buffers[1].Memory, ref handle);
        if (raw == null) return null;

        int elemSize = indices switch
        {
            UInt8Array or Int8Array => 1,
            UInt16Array or Int16Array => 2,
            UInt32Array or Int32Array => 4,
            UInt64Array or Int64Array => 8,
            _ => 4
        };
        return raw + (nint)indices.Offset * elemSize;
    }

    public override void SetBatch(IArrowArray array)
    {
        _array = (DictionaryArray)array;
        BindNullBitmap(_array);
        _indicesPtr = BindIndexBuffer(_array.Indices, ref _indicesBufferHandle, out _readKeyFn);

        var dict = _array.Dictionary;
        int dictLen = dict.Length;
        var cached = new string[dictLen];
        using var tempAcc = new StringViewAccessor();
        tempAcc.SetBatch(dict);
        for (int i = 0; i < dictLen; i++)
        {
            cached[i] = tempAcc.IsNull(i) ? null! : tempAcc.GetTypedValue(i);
        }
        _cachedStrings = cached;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override string GetTypedValue(int index)
    {
        int key = _readKeyFn(_indicesPtr, index);
        return _cachedStrings![key];
    }

    public override void Dispose()
    {
        _indicesBufferHandle.Dispose();
        _indicesPtr = null;
        _cachedStrings = null;
        _array = null;
        base.Dispose();
    }
}

internal sealed unsafe class EnumAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] TEnum> : TypedColumnAccessor<TEnum>,
    ITypedAccessor<TEnum>, ITypedAccessor<TEnum?>
    where TEnum : struct, Enum
{
    private ColumnAccessor? _numericAccessor;
    private DictionaryArray? _dictArray;
    private TEnum[]? _dictEnumLookup;
    private MemoryHandle _indicesBufferHandle;
    private byte* _indicesPtr;
    private delegate*<byte*, int, int> _readKeyFn;
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

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(TEnum);

    public override void SetBatch(IArrowArray array)
    {
        if (array is DictionaryArray dictArr)
        {
            _dictArray = dictArr;
            BindNullBitmap(dictArr);
            _indicesPtr = DictionaryAccessor.BindIndexBuffer(dictArr.Indices, ref _indicesBufferHandle, out _readKeyFn);
            using var strAcc = new StringViewAccessor();
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
            int key = _readKeyFn(_indicesPtr, index);
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

    TEnum? ITypedAccessor<TEnum?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);

    public override void Dispose()
    {
        _indicesBufferHandle.Dispose();
        _indicesPtr = null;
        _numericAccessor?.Dispose();
        _stringAccessor?.Dispose();
        _dictArray = null;
        _dictEnumLookup = null;
        base.Dispose();
    }
}

internal sealed class BinaryAccessor : TypedColumnAccessor<byte[]>, IBinarySpanAccessor,
    ITypedAccessor<byte[]>
{
    private IArrowArray? _array;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
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

internal sealed unsafe class Int64ToDateTimeAccessor : TypedColumnAccessor<DateTime>,
    ITypedAccessor<DateTime>, ITypedAccessor<DateTime?>, ITypedAccessor<DateTimeOffset>, ITypedAccessor<DateTimeOffset?>
{
    private long* _ptr;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(DateTime);
    public override bool UsesFastNullBitmap => true;
    public override void SetBatch(IArrowArray array) => _ptr = BindPrimitiveBuffer<long>((Int64Array)array);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateTime GetTypedValue(int index)
        => DateTime.UnixEpoch.AddMicroseconds(_ptr[index]);

    DateTime ITypedAccessor<DateTime>.GetTypedValue(int index) => GetTypedValue(index);
    DateTimeOffset ITypedAccessor<DateTimeOffset>.GetTypedValue(int index) => new(GetTypedValue(index));

    DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index) => IsNull(index) ? null : new DateTimeOffset(GetTypedValue(index));
}

internal sealed unsafe class Int64ToTimeSpanAccessor : TypedColumnAccessor<TimeSpan>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>, ITypedAccessor<long>, ITypedAccessor<long?>
{
    private long* _ptr;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(TimeSpan);
    public override bool UsesFastNullBitmap => true;
    public override void SetBatch(IArrowArray array) => _ptr = BindPrimitiveBuffer<long>((Int64Array)array);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeSpan GetTypedValue(int index)
        => new(_ptr[index] * 10);

    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index);
    long ITypedAccessor<long>.GetTypedValue(int index) => _ptr[index] * 10;
    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => IsNullFast(index) ? null : GetTypedValue(index);
    long? ITypedAccessor<long?>.GetTypedValue(int index) => IsNullFast(index) ? null : _ptr[index] * 10;
}

internal sealed unsafe class TimestampAccessor : TypedColumnAccessor<DateTime>,
    ITypedAccessor<DateTime>, ITypedAccessor<DateTime?>,
    ITypedAccessor<DateTimeOffset>, ITypedAccessor<DateTimeOffset?>,
    ITypedAccessor<DateOnly>, ITypedAccessor<DateOnly?>
{
    private long* _ptr;
    private TimeUnit _unit;
    private bool _hasTimeZone;
    private TimeZoneInfo? _timeZoneInfo;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    private readonly Type _targetType;

    public TimestampAccessor(Type? targetType = null)
    {
        _targetType = targetType == typeof(DateTimeOffset) ? typeof(DateTimeOffset) : typeof(DateTime);
    }

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => _targetType;
    public override bool UsesFastNullBitmap => true;

    public override void SetBatch(IArrowArray array)
    {
        var tsArray = (TimestampArray)array;
        _ptr = BindPrimitiveBuffer<long>(tsArray);
        var tsType = (TimestampType)tsArray.Data.DataType;
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private long GetTicks(int index)
    {
        long val = _ptr[index];
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

    DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index) => IsNull(index) ? null : GetDateTimeOffsetValue(index);
    DateOnly? ITypedAccessor<DateOnly?>.GetTypedValue(int index) => IsNull(index) ? null : DateOnly.FromDateTime(GetTypedValue(index));
}

internal sealed unsafe class Date32ToDateOnlyAccessor : TypedColumnAccessor<DateOnly>,
    ITypedAccessor<DateOnly>, ITypedAccessor<DateTime>, ITypedAccessor<DateTimeOffset>,
    ITypedAccessor<DateOnly?>, ITypedAccessor<DateTime?>, ITypedAccessor<DateTimeOffset?>
{
    private int* _ptr;
    private const int UnixEpochDayNumber = 719162;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    private readonly Type _targetType;

    public Date32ToDateOnlyAccessor(Type? targetType = null)
    {
        _targetType = targetType == typeof(DateTime) ? typeof(DateTime) : typeof(DateOnly);
    }

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => _targetType;
    public override bool UsesFastNullBitmap => true;
    public override void SetBatch(IArrowArray array) => _ptr = BindPrimitiveBuffer<int>((Date32Array)array);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateOnly GetTypedValue(int index)
        => DateOnly.FromDayNumber(_ptr[index] + UnixEpochDayNumber);

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

    DateOnly? ITypedAccessor<DateOnly?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index)
        => IsNull(index) ? null : GetTypedValue(index).ToDateTime(TimeOnly.MinValue);
    DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index)
        => IsNull(index) ? null : new DateTimeOffset(GetTypedValue(index).ToDateTime(TimeOnly.MinValue), TimeSpan.Zero);
}

internal sealed unsafe class Date64Accessor : TypedColumnAccessor<DateTime>,
    ITypedAccessor<DateTime>, ITypedAccessor<DateTimeOffset>, ITypedAccessor<DateOnly>,
    ITypedAccessor<DateTime?>, ITypedAccessor<DateTimeOffset?>, ITypedAccessor<DateOnly?>
{
    private long* _ptr;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(DateTime);
    public override bool UsesFastNullBitmap => true;
    public override void SetBatch(IArrowArray array) => _ptr = BindPrimitiveBuffer<long>((Date64Array)array);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    // Date64 = milliseconds since epoch; Arrow's GetDateTime returns DateTimeKind.Unspecified.
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override DateTime GetTypedValue(int index)
        => new(DateTime.UnixEpoch.Ticks + _ptr[index] * TimeSpan.TicksPerMillisecond, DateTimeKind.Unspecified);

    DateTime ITypedAccessor<DateTime>.GetTypedValue(int index) => GetTypedValue(index);
    DateTimeOffset ITypedAccessor<DateTimeOffset>.GetTypedValue(int index) => new(GetTypedValue(index));
    DateOnly ITypedAccessor<DateOnly>.GetTypedValue(int index) => DateOnly.FromDateTime(GetTypedValue(index));

    DateTime? ITypedAccessor<DateTime?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    DateTimeOffset? ITypedAccessor<DateTimeOffset?>.GetTypedValue(int index) => IsNull(index) ? null : new DateTimeOffset(GetTypedValue(index));
    DateOnly? ITypedAccessor<DateOnly?>.GetTypedValue(int index) => IsNull(index) ? null : DateOnly.FromDateTime(GetTypedValue(index));
}

internal sealed unsafe class DurationAccessor : TypedColumnAccessor<TimeSpan>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>, ITypedAccessor<long>, ITypedAccessor<long?>
{
    private long* _ptr;
    private TimeUnit _unit;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(TimeSpan);
    public override bool UsesFastNullBitmap => true;

    public override void SetBatch(IArrowArray array)
    {
        var durArray = (DurationArray)array;
        _ptr = BindPrimitiveBuffer<long>(durArray);
        _unit = ((DurationType)durArray.Data.DataType).Unit;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeSpan GetTypedValue(int index)
    {
        long val = _ptr[index];
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

    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    long? ITypedAccessor<long?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index).Ticks;
}

internal sealed unsafe class Int64ToTimeOnlyAccessor : TypedColumnAccessor<TimeOnly>,
    ITypedAccessor<TimeOnly>, ITypedAccessor<TimeOnly?>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>
{
    private long* _ptr;
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(TimeOnly);
    public override bool UsesFastNullBitmap => true;
    public override void SetBatch(IArrowArray array) => _ptr = BindPrimitiveBuffer<long>((Int64Array)array);
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeOnly GetTypedValue(int index)
    {
        long ticks = _ptr[index] * 10;
        return new TimeOnly(ticks);
    }

    TimeOnly ITypedAccessor<TimeOnly>.GetTypedValue(int index) => GetTypedValue(index);
    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();
    TimeOnly? ITypedAccessor<TimeOnly?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index).ToTimeSpan();
}

internal sealed unsafe class Time32Accessor : TypedColumnAccessor<TimeOnly>,
    ITypedAccessor<TimeOnly>, ITypedAccessor<TimeOnly?>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>
{
    private int* _ptr;
    private bool _isMillisecond;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(TimeOnly);
    public override bool UsesFastNullBitmap => true;

    public override void SetBatch(IArrowArray array)
    {
        var t32Array = (Time32Array)array;
        _ptr = BindPrimitiveBuffer<int>(t32Array);
        var unit = ((Time32Type)t32Array.Data.DataType).Unit;
        _isMillisecond = unit == TimeUnit.Millisecond;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeOnly GetTypedValue(int index)
    {
        int val = _ptr[index];
        long ticks = _isMillisecond ? (val * 10_000L) : (val * 10_000_000L);
        return new TimeOnly(ticks);
    }

    TimeOnly ITypedAccessor<TimeOnly>.GetTypedValue(int index) => GetTypedValue(index);
    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();

    TimeOnly? ITypedAccessor<TimeOnly?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index).ToTimeSpan();
}

internal sealed unsafe class Time64Accessor : TypedColumnAccessor<TimeOnly>,
    ITypedAccessor<TimeOnly>, ITypedAccessor<TimeOnly?>,
    ITypedAccessor<TimeSpan>, ITypedAccessor<TimeSpan?>
{
    private long* _ptr;
    private bool _isNanosecond;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(TimeOnly);
    public override bool UsesFastNullBitmap => true;

    public override void SetBatch(IArrowArray array)
    {
        var t64Array = (Time64Array)array;
        _ptr = BindPrimitiveBuffer<long>(t64Array);
        var unit = ((Time64Type)t64Array.Data.DataType).Unit;
        _isNanosecond = unit == TimeUnit.Nanosecond;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override bool IsNull(int index) => IsNullFast(index);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public override TimeOnly GetTypedValue(int index)
    {
        long val = _ptr[index];
        long ticks = _isNanosecond ? (val / 100L) : (val * 10L);
        return new TimeOnly(ticks);
    }

    TimeOnly ITypedAccessor<TimeOnly>.GetTypedValue(int index) => GetTypedValue(index);
    TimeSpan ITypedAccessor<TimeSpan>.GetTypedValue(int index) => GetTypedValue(index).ToTimeSpan();

    TimeOnly? ITypedAccessor<TimeOnly?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
    TimeSpan? ITypedAccessor<TimeSpan?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index).ToTimeSpan();
}

internal sealed class ListAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TElement> : TypedColumnAccessor<TElement[]>,
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

    public ListAccessor([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType, IArrowType arrowType)
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

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
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

    public override void Dispose()
    {
        _childAccessor.Dispose();
        _array = null;
        base.Dispose();
    }

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

internal sealed class StructAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T> : TypedColumnAccessor<T>, ITypedAccessor<T>
{
    private StructArray? _array;
    private readonly ColumnAccessor[] _childAccessors;
    private readonly string[]? _dictFieldNames;
    private readonly CompiledArrowRowMapper<T>? _compiledMapper;

    public StructAccessor([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType, IArrowType arrowType)
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

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
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

    public override void Dispose()
    {
        for (int i = 0; i < _childAccessors.Length; i++)
        {
            _childAccessors[i]?.Dispose();
        }
        _array = null;
        base.Dispose();
    }
}

internal sealed class StructToListAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TElement> : TypedColumnAccessor<TElement[]>,
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

    public StructToListAccessor([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType, IArrowType arrowType)
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

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
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

    public override void Dispose()
    {
        for (int i = 0; i < _childAccessors.Length; i++)
        {
            _childAccessors[i]?.Dispose();
        }
        _array = null;
        base.Dispose();
    }
}

internal sealed class FSharpOptionAccessor<T>(ColumnAccessor innerAccessor, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType) : TypedColumnAccessor<FSharpOption<T>>, ITypedAccessor<FSharpOption<T>>
{
    private readonly ITypedAccessor<T>? _typedInnerAccessor = innerAccessor as ITypedAccessor<T>;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
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

    public override void Dispose()
    {
        innerAccessor.Dispose();
        base.Dispose();
    }
}

internal sealed class FSharpValueOptionAccessor<T>(ColumnAccessor innerAccessor, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] Type targetType) : TypedColumnAccessor<FSharpValueOption<T>>, ITypedAccessor<FSharpValueOption<T>>
{
    private readonly ITypedAccessor<T>? _typedInnerAccessor = innerAccessor as ITypedAccessor<T>;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
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

    public override void Dispose()
    {
        innerAccessor.Dispose();
        base.Dispose();
    }
}

internal sealed class JsonFallbackAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] T> : TypedColumnAccessor<T>, ITypedAccessor<T>
{
    private IArrowArray? _array;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(T);
    public override void SetBatch(IArrowArray array) => _array = array;
    public override bool IsNull(int index) => _array!.IsNull(index);

    public override T GetTypedValue(int index)
    {
        var rawValue = ExtractValue(_array!, index);

        if (typeof(T) == typeof(string))
        {
            var json = rawValue is string s ? s : EncodeExtractedValue(rawValue);
            return (T)(object)json;
        }

        if (rawValue is T typedValue)
        {
            return typedValue;
        }

        if (rawValue != null)
        {
            throw new NotSupportedException(
                $"Cannot read Arrow column of type '{_array!.Data.DataType.Name}' (value type '{rawValue.GetType()}') " +
                $"as '{typeof(T)}'. Only string targets fall back to a JSON representation.");
        }

        return default!;
    }

    T ITypedAccessor<T>.GetTypedValue(int index) => GetTypedValue(index);

    [UnconditionalSuppressMessage("Trimming", "IL2026",
        Justification = "ExtractValue only yields null, numbers, booleans, strings and DateTime, which JsonValueEncoder " +
                        "writes directly; its reflection-based fallback is never reached.")]
    [UnconditionalSuppressMessage("AOT", "IL3050",
        Justification = "See IL2026: extracted values never reach the reflection-based fallback.")]
    private static string EncodeExtractedValue(object? value) => JsonValueEncoder.Encode(value);

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
