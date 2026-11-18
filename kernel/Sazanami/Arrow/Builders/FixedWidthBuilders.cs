using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Interop;

namespace Kaminari.Sazanami.Arrow.Builders;

internal sealed unsafe class NativeNullBuilder() : NativeColumnBuilder(NullType.Default)
{
    public override void AppendNull() => Length++;
    public override void AppendDefault() => Length++;

    public override NativeCArray* Export(NativeBatchExport export)
    {
        var a = export.NewArray(Length, Length, [], []);
        Length = 0;
        return a;
    }

    protected override void FreeBuffers() { }
}

internal sealed unsafe class NativeBooleanBuilder() : NativeColumnBuilder(BooleanType.Default)
{
    private NativeBitmap _values;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(bool value)
    {
        _values.Append(value);
        MarkValid();
    }

    public override void AppendNull()
    {
        _values.Append(false);
        MarkNull();
    }

    public override void AppendDefault() => Append(false);

    protected override void ReserveValues(long slots, long dataBytes) => _values.EnsureAdditional(slots);

    /// <summary>
    /// Appends one slot per byte of <paramref name="values"/> (true where not zero); with a <paramref name="mask"/>
    /// of the same length, slots whose mask byte is zero are null.
    /// </summary>
    public void AppendValues(ReadOnlySpan<byte> values, Kernels.SlotMask mask = default)
    {
        _values.AppendMask(values);
        if (mask.IsEmpty) MarkValid(values.Length);
        else MarkMask(mask);
    }

    /// <summary>Appends <paramref name="count"/> valid slots from an LSB-first bitmap, starting at bit <paramref name="firstBit"/>.</summary>
    public void AppendBits(ReadOnlySpan<byte> bits, long firstBit, int count)
    {
        _values.AppendBits(bits, firstBit, count);
        MarkValid(count);
    }

    /// <summary>
    /// Appends <paramref name="mask"/>.Length slots whose values are bits of an LSB-first bitmap from bit
    /// <paramref name="firstBit"/>, one per slot (see <see cref="Kernels.MaskExpand.Bits"/>); slots the mask marks are null.
    /// </summary>
    public void AppendBits(ReadOnlySpan<byte> bits, long firstBit, Kernels.SlotMask mask)
    {
        if (mask.IsEmpty) throw new ArgumentException("An empty mask gives no slot count; use the overload with a count.", nameof(mask));
        _values.AppendBits(bits, firstBit, mask.Length);
        MarkMask(mask);
    }

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        var a = export.NewArray(Length, nulls, [validity, (nint)export.Own(_values.Detach(out long bytes), bytes)], []);
        Length = 0;
        return a;
    }

    protected override void FreeBuffers() => _values.Free();
}

internal sealed unsafe class NativeFixedWidthBuilder<T>(IArrowType type) : NativeColumnBuilder(type) where T : unmanaged
{
    private NativeGrowBuffer _values;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(T value)
    {
        _values.Append(value);
        MarkValid();
    }

    protected override void ReserveValues(long slots, long dataBytes) => _values.EnsureAdditional(slots * sizeof(T));

    /// <summary>Appends <paramref name="count"/> valid slots and returns them for the caller to fill.</summary>
    public Span<T> AppendSlots(int count)
    {
        byte* at = _values.Reserve((long)count * sizeof(T));
        MarkValid(count);
        return new Span<T>(at, count);
    }

    /// <summary>
    /// Appends one slot per byte of <paramref name="mask"/>, null where the byte is zero, and returns all of them
    /// (zeroed) for the caller to fill the valid ones.
    /// </summary>
    public Span<T> AppendSlots(ReadOnlySpan<byte> mask)
    {
        Span<T> slots = AppendUninitializedSlots(mask);
        slots.Clear();
        return slots;
    }

    /// <summary>
    /// Appends one slot per byte of <paramref name="mask"/>, null where the byte is zero, and returns them
    /// uninitialized: the caller writes every slot, zero under the nulls.
    /// </summary>
    public Span<T> AppendUninitializedSlots(Kernels.SlotMask mask)
    {
        byte* at = _values.Reserve((long)mask.Length * sizeof(T));
        MarkMask(mask);
        return new Span<T>(at, mask.Length);
    }

    /// <summary>Appends <paramref name="count"/> null slots.</summary>
    public void AppendNulls(int count)
    {
        _values.AppendZeros((long)count * sizeof(T));
        MarkNulls(count);
    }

    /// <summary>Appends values given as raw little-endian bytes (a whole number of <typeparamref name="T"/>).</summary>
    public void AppendRaw(ReadOnlySpan<byte> littleEndian)
    {
        _values.Append(littleEndian);
        MarkValid(littleEndian.Length / sizeof(T));
    }

    public override void AppendNull()
    {
        _values.Append(default(T));
        MarkNull();
    }

    public override void AppendDefault() => Append(default);

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        var a = export.NewArray(Length, nulls, [validity, export.Ptr(ref _values)], []);
        Length = 0;
        return a;
    }

    protected override void FreeBuffers() => _values.Free();
}

internal sealed unsafe class NativeFixedSizeBinaryBuilder(FixedSizeBinaryType type) : NativeColumnBuilder(type)
{
    private NativeGrowBuffer _data;

    public int ByteWidth { get; } = type.ByteWidth;

    protected override void ReserveValues(long slots, long dataBytes) => _data.EnsureAdditional(slots * ByteWidth);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(ReadOnlySpan<byte> value)
    {
        _data.Append(value);
        MarkValid();
    }

    public override void AppendNull()
    {
        _data.AppendZeros(ByteWidth);
        MarkNull();
    }

    /// <summary>
    /// Appends one slot per byte of <paramref name="mask"/> (all valid when empty: <paramref name="count"/>
    /// slots), null where the byte is zero, and returns their bytes (zeroed) for the caller to fill the valid ones.
    /// </summary>
    public Span<byte> AppendSlots(int count, ReadOnlySpan<byte> mask = default)
    {
        long bytes = (long)count * ByteWidth;
        byte* at = _data.Reserve(bytes);
        var slots = new Span<byte>(at, checked((int)bytes));
        slots.Clear();
        if (mask.IsEmpty) MarkValid(count);
        else MarkMask(mask[..count]);
        return slots;
    }

    public override void AppendDefault()
    {
        _data.AppendZeros(ByteWidth);
        MarkValid();
    }

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        var a = export.NewArray(Length, nulls, [validity, export.Ptr(ref _data)], []);
        Length = 0;
        return a;
    }

    protected override void FreeBuffers() => _data.Free();
}
