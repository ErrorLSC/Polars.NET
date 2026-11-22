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
    /// <summary>
    /// The accessor of <paramref name="field"/>'s values as <paramref name="targetType"/>. With
    /// <paramref name="foldNulls"/>, a nested object whose mapped fields are all null is read as null (as a query's outer
    /// join leaves a missing side); without it, as an object of nulls (data holds such rows).
    /// </summary>
    public static ColumnAccessor Create(Field field, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] Type targetType, bool foldNulls = false)
    {
        if (targetType.IsGenericType)
        {
            var genericDef = targetType.GetGenericTypeDefinition();
            bool isOption = genericDef == typeof(FSharpOption<>);
            bool isValueOption = genericDef == typeof(FSharpValueOption<>);

            if (isOption || isValueOption)
            {
                Type innerType = targetType.GetGenericArguments()[0];
                var innerAccessor = Create(field, innerType, foldNulls);

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

        if (MapAccessorTypes.TryGetKeyValue(effectiveType, out Type? keyType, out Type? valueType)
            && MapAccessorTypes.Entries(field.DataType) is not null)
        {
            Type mapAccessorType = typeof(MapAccessor<,>).MakeGenericType(keyType, valueType);
            return (ColumnAccessor)Activator.CreateInstance(mapAccessorType, effectiveType, field.DataType, foldNulls)!;
        }

        if (field.DataType is Decimal128Type { Scale: 0 } && (effectiveType == typeof(Int128) || effectiveType == typeof(UInt128)))
        {
            return effectiveType == typeof(Int128)
                ? new WideIntegerAccessor<Int128>(WideIntegers.KindOf(field) == WideIntegers.Unsigned)
                : new WideIntegerAccessor<UInt128>(WideIntegers.KindOf(field) == WideIntegers.Unsigned);
        }

        if (field.DataType is NullType)
        {
            Type nullAccessorType = typeof(NullAccessor<>).MakeGenericType(effectiveType);
            return (ColumnAccessor)Activator.CreateInstance(nullAccessorType)!;
        }

        return field.DataType switch
        {
            ListType or LargeListType or FixedSizeListType => CreateListAccessor(field.DataType, effectiveType, foldNulls),
            StructType => CreateStructAccessor(field.DataType, effectiveType, foldNulls),
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

    private static ColumnAccessor CreateListAccessor(IArrowType arrowType, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] Type targetType, bool foldNulls)
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
        return (ColumnAccessor)Activator.CreateInstance(accessorClass, targetType, arrowType, foldNulls)!;
    }

    private static ColumnAccessor CreateStructAccessor(IArrowType arrowType, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] Type targetType, bool foldNulls)
    {
        if (targetType != typeof(byte[]))
        {
            if (targetType.IsArray)
            {
                Type elementType = targetType.GetElementType()!;
                Type accessorClass = typeof(StructToListAccessor<>).MakeGenericType(elementType);
                return (ColumnAccessor)Activator.CreateInstance(accessorClass, targetType, arrowType, foldNulls)!;
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
                    return (ColumnAccessor)Activator.CreateInstance(accessorClass, targetType, arrowType, foldNulls)!;
                }
            }
        }

        return (ColumnAccessor)Activator.CreateInstance(typeof(StructAccessor<>).MakeGenericType(targetType), targetType, arrowType, foldNulls)!;
    }
}

// ==================================================================================
// Concrete Accessors
// ==================================================================================
