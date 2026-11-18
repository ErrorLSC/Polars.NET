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
using Kaminari.Sazanami.Arrow.TypeSystem;
using Kaminari.Sazanami.Json;
using Kaminari.Sazanami.Marshalling;
using Kaminari.Sazanami.Data.Building;

namespace Kaminari.Sazanami.Data.Mapping;

internal sealed class NullAccessor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)] T> : TypedColumnAccessor<T>, ITypedAccessor<T?>
{
    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(T);
    public override void SetBatch(IArrowArray array) { }
    public override bool IsNull(int index) => true;
    public override T GetTypedValue(int index) => default!;
    T? ITypedAccessor<T?>.GetTypedValue(int index) => default;
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

/// <summary>
/// A decimal(p, 0) column as 128-bit integers (<see cref="WideIntegers"/>): its 16 bytes as they are. Into an unsigned
/// integer, a column marked unsigned gives its bits; any other one its values, a negative one refused.
/// </summary>
internal sealed class WideIntegerAccessor<TInteger>(bool unsignedBits) : TypedColumnAccessor<TInteger>, ITypedAccessor<TInteger>, ITypedAccessor<TInteger?>
    where TInteger : struct
{
    private Decimal128Array? _array;

    [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicFields | DynamicallyAccessedMemberTypes.PublicProperties)]
    public override Type TargetType => typeof(TInteger);

    public override void SetBatch(IArrowArray array) => _array = (Decimal128Array)array;

    public override bool IsNull(int index) => _array!.IsNull(index);

    public override TInteger GetTypedValue(int index)
    {
        Int128 value = MemoryMarshal.Read<Int128>(_array!.ValueBuffer.Span[((_array.Offset + index) * 16)..]);
        if (typeof(TInteger) == typeof(Int128)) return (TInteger)(object)value;
        if (value < 0 && !unsignedBits)
            throw new OverflowException($"The decimal {value} is negative; an unsigned 128-bit integer cannot hold it.");
        return (TInteger)(object)(UInt128)value;
    }

    TInteger? ITypedAccessor<TInteger?>.GetTypedValue(int index) => IsNull(index) ? null : GetTypedValue(index);
}
