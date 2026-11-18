using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Interop;

namespace Kaminari.Sazanami.Arrow.Builders;

/// <summary>
/// Validity bitmap that is only materialized once the first null arrives, so all-valid columns carry no bitmap.
/// </summary>
internal struct LazyValidity
{
    private NativeBitmap _bits;
    private bool _materialized;
    private long _reserved; // slots the batch is expected to reach: the bitmap's size once it materializes

    /// <summary>Expects the column to reach <paramref name="slots"/> slots in this batch.</summary>
    public void Reserve(long slots, long lengthNow)
    {
        if (_materialized) _bits.EnsureAdditional(slots - lengthNow);
        else _reserved = slots;
    }

    private void Materialize(long lengthBefore)
    {
        _bits.EnsureAdditional(Math.Max(_reserved, lengthBefore + 1));
        _bits.AppendMany(true, lengthBefore);
        _materialized = true;
    }

    public readonly long NullCount => _materialized ? _bits.UnsetCount : 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void AppendValid()
    {
        if (_materialized) _bits.Append(true);
    }

    /// <summary>Marks <paramref name="count"/> slots valid.</summary>
    public void AppendValid(long count)
    {
        if (_materialized) _bits.AppendMany(true, count);
    }

    /// <summary>
    /// Appends one slot per slot of <paramref name="mask"/> (a byte per slot, valid where not zero, or the bits of a
    /// bitmap, copied as they are); the bitmap is materialized only once a null arrives.
    /// </summary>
    /// <param name="lengthBefore">Number of slots appended before these.</param>
    public void AppendMask(Kernels.SlotMask mask, long lengthBefore)
    {
        if (mask.IsBits)
        {
            if (!_materialized)
            {
                if (mask.CountValid() == mask.Length) return;
                Materialize(lengthBefore);
            }
            _bits.AppendBits(mask.Bitmap, mask.FirstBit, mask.Length);
            return;
        }
        if (!_materialized)
        {
            if (!mask.Bytes.Contains((byte)0)) return;
            Materialize(lengthBefore);
        }
        _bits.AppendMask(mask.Bytes);
    }

    /// <summary>Appends <paramref name="count"/> null slots.</summary>
    public void AppendNulls(long count, long lengthBefore)
    {
        if (count <= 0) return;
        if (!_materialized) Materialize(lengthBefore);
        _bits.AppendMany(false, count);
    }

    /// <param name="lengthBefore">Number of slots appended before this null.</param>
    public void AppendNull(long lengthBefore)
    {
        if (!_materialized) Materialize(lengthBefore);
        _bits.Append(false);
    }

    public unsafe nint Detach(NativeBatchExport export)
    {
        _reserved = 0;
        if (!_materialized) return 0;
        _materialized = false;
        return (nint)export.Own(_bits.Detach(out long bytes), bytes);
    }

    public void Free()
    {
        _bits.Free();
        _materialized = false;
    }
}

/// <summary>
/// Builds one Arrow column directly in native memory. <see cref="Export"/> hands the buffers to a
/// <see cref="NativeBatchExport"/> without copying and leaves the builder empty for the next batch.
/// </summary>
internal abstract unsafe class NativeColumnBuilder(IArrowType type) : IDisposable
{
    protected LazyValidity Validity;

    public IArrowType Type { get; } = type;
    public long Length { get; protected set; }
    public long NullCount => Validity.NullCount;

    /// <summary>Appends a null slot.</summary>
    public abstract void AppendNull();

    /// <summary>Appends a valid placeholder (zero, empty) used for non-nullable children of a null parent.</summary>
    public abstract void AppendDefault();

    public abstract NativeCArray* Export(NativeBatchExport export);

    /// <summary>
    /// Exports what was appended as an array of its own (over its own native memory, freed when the array and every
    /// array sliced from it are disposed) and leaves this builder empty for the next batch.
    /// </summary>
    public IArrowArray ExportArray()
    {
        var export = new NativeBatchExport();
        try
        {
            return export.ImportArray(Export(export), Type);
        }
        catch
        {
            export.FreeAll();
            throw;
        }
    }

    /// <summary>
    /// Expects <paramref name="slots"/> more slots in this batch (and, for variable-length values, about
    /// <paramref name="dataBytes"/> more value bytes): the buffers grow once instead of doubling up while filled.
    /// </summary>
    public void Reserve(long slots, long dataBytes = 0)
    {
        if (slots <= 0) return;
        Validity.Reserve(Length + slots, Length);
        ReserveValues(slots, dataBytes);
    }

    protected virtual void ReserveValues(long slots, long dataBytes) { }

    protected abstract void FreeBuffers();

    public void Dispose()
    {
        FreeBuffers();
        Validity.Free();
        GC.SuppressFinalize(this);
    }

    ~NativeColumnBuilder()
    {
        FreeBuffers();
        Validity.Free();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    protected void MarkValid()
    {
        Validity.AppendValid();
        Length++;
    }

    protected void MarkValid(long count)
    {
        Validity.AppendValid(count);
        Length += count;
    }

    protected void MarkNull()
    {
        Validity.AppendNull(Length);
        Length++;
    }

    /// <summary>Marks one slot per slot of <paramref name="mask"/>, valid where the mask says so.</summary>
    protected void MarkMask(Kernels.SlotMask mask)
    {
        Validity.AppendMask(mask, Length);
        Length += mask.Length;
    }

    protected void MarkNulls(long count)
    {
        Validity.AppendNulls(count, Length);
        Length += count;
    }

    /// <summary>Creates a builder for an Arrow type (the layouts this package produces).</summary>
    public static NativeColumnBuilder Create(IArrowType type) => type switch
    {
        NullType => new NativeNullBuilder(),
        BooleanType => new NativeBooleanBuilder(),
        Int8Type => new NativeFixedWidthBuilder<sbyte>(type),
        UInt8Type => new NativeFixedWidthBuilder<byte>(type),
        Int16Type => new NativeFixedWidthBuilder<short>(type),
        UInt16Type => new NativeFixedWidthBuilder<ushort>(type),
        UInt32Type => new NativeFixedWidthBuilder<uint>(type),
        UInt64Type => new NativeFixedWidthBuilder<ulong>(type),
        Int32Type or Date32Type or Time32Type or IntervalType { Unit: IntervalUnit.YearMonth } => new NativeFixedWidthBuilder<int>(type),
        Int64Type or Date64Type or TimestampType or Time64Type or DurationType => new NativeFixedWidthBuilder<long>(type),
        HalfFloatType => new NativeFixedWidthBuilder<Half>(type),
        FloatType => new NativeFixedWidthBuilder<float>(type),
        DoubleType => new NativeFixedWidthBuilder<double>(type),
        Decimal128Type => new NativeFixedWidthBuilder<Int128>(type),
        Decimal256Type => new NativeFixedWidthBuilder<Int256Bits>(type),
        IntervalType { Unit: IntervalUnit.MonthDayNanosecond } => new NativeFixedWidthBuilder<MonthDayNanos>(type),
        StringType or BinaryType => new NativeBinaryBuilder(type),
        StringViewType or BinaryViewType => new NativeBinaryViewBuilder(type),
        FixedSizeBinaryType f => new NativeFixedSizeBinaryBuilder(f),
        DictionaryType d => new NativeDictionaryBuilder(d),
        MapType m => new NativeListBuilder(m, Create(m.KeyValueType)),
        ListType l => new NativeListBuilder(l, Create(l.ValueDataType)),
        LargeListType l => new NativeLargeListBuilder(l, Create(l.ValueDataType)),
        FixedSizeListType f => new NativeFixedSizeListBuilder(f, Create(f.ValueDataType)),
        StructType s => new NativeStructBuilder(s, [.. s.Fields.Select(f => Create(f.DataType))]),
        _ => throw new NotSupportedException($"No native builder for Arrow type {type}."),
    };
}

[StructLayout(LayoutKind.Sequential, Size = 32)]
internal struct Int256Bits
{
    public ulong P0, P1, P2, P3;
}

[StructLayout(LayoutKind.Sequential)]
internal struct MonthDayNanos(int months, int days, long nanoseconds)
{
    public int Months = months;
    public int Days = days;
    public long Nanoseconds = nanoseconds;
}
