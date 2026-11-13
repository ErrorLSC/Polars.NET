using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;

namespace Kaminari.Sazanami.Arrow;

/// <summary>
/// Validity bitmap that is only materialized once the first null arrives, so all-valid columns carry no bitmap.
/// </summary>
internal struct LazyValidity
{
    private NativeBitmap _bits;
    private bool _materialized;

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
    /// Appends one slot per byte of <paramref name="mask"/>, valid where the byte is not zero; the bitmap is
    /// materialized only once a zero arrives.
    /// </summary>
    /// <param name="lengthBefore">Number of slots appended before these.</param>
    public void AppendMask(ReadOnlySpan<byte> mask, long lengthBefore)
    {
        if (!_materialized)
        {
            if (!mask.Contains((byte)0)) return;
            _bits.AppendMany(true, lengthBefore);
            _materialized = true;
        }
        _bits.AppendMask(mask);
    }

    /// <summary>Appends <paramref name="count"/> null slots.</summary>
    public void AppendNulls(long count, long lengthBefore)
    {
        if (count <= 0) return;
        if (!_materialized)
        {
            _bits.AppendMany(true, lengthBefore);
            _materialized = true;
        }
        _bits.AppendMany(false, count);
    }

    /// <param name="lengthBefore">Number of slots appended before this null.</param>
    public void AppendNull(long lengthBefore)
    {
        if (!_materialized)
        {
            _bits.AppendMany(true, lengthBefore);
            _materialized = true;
        }
        _bits.Append(false);
    }

    public unsafe nint Detach(NativeBatchExport export)
    {
        if (!_materialized) return 0;
        _materialized = false;
        return (nint)export.Own(_bits.Detach());
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

    /// <summary>Marks one slot per byte of <paramref name="mask"/>: valid where the byte is not zero.</summary>
    protected void MarkMask(ReadOnlySpan<byte> mask)
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
        FixedSizeBinaryType f => new NativeFixedSizeBinaryBuilder(f),
        DictionaryType d => new NativeDictionaryBuilder(d),
        MapType m => new NativeListBuilder(m, Create(m.KeyValueType)),
        ListType l => new NativeListBuilder(l, Create(l.ValueDataType)),
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

    /// <summary>
    /// Appends one slot per byte of <paramref name="values"/> (true where not zero); with a <paramref name="mask"/>
    /// of the same length, slots whose mask byte is zero are null.
    /// </summary>
    public void AppendValues(ReadOnlySpan<byte> values, ReadOnlySpan<byte> mask = default)
    {
        _values.AppendMask(values);
        if (mask.IsEmpty) MarkValid(values.Length);
        else MarkMask(mask);
    }

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        var a = export.NewArray(Length, nulls, [validity, (nint)export.Own(_values.Detach())], []);
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
        byte* at = _values.Reserve((long)mask.Length * sizeof(T));
        var slots = new Span<T>(at, mask.Length);
        slots.Clear();
        MarkMask(mask);
        return slots;
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

/// <summary>Utf8 / Binary with 32-bit offsets.</summary>
internal sealed unsafe class NativeBinaryBuilder : NativeColumnBuilder
{
    private NativeGrowBuffer _offsets;
    private NativeGrowBuffer _data;

    public NativeBinaryBuilder(IArrowType type) : base(type) => _offsets.Append(0);

    /// <summary>Bytes of value data appended to the current batch.</summary>
    public long DataLength => _data.Length;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(ReadOnlySpan<byte> value)
    {
        _data.Append(value);
        AppendOffset();
        MarkValid();
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void AppendOffset()
    {
        long end = _data.Length;
        if (end > int.MaxValue) ThrowTooLarge();
        _offsets.Append((int)end);
    }

    private static void ThrowTooLarge() =>
        throw new InvalidOperationException("A string or binary column exceeds 2 GB in one batch; use a smaller batch size.");

    public override void AppendNull()
    {
        AppendOffset();
        MarkNull();
    }

    /// <summary>Appends <paramref name="count"/> null slots.</summary>
    public void AppendNulls(int count)
    {
        for (int i = 0; i < count; i++) AppendOffset();
        MarkNulls(count);
    }

    public override void AppendDefault()
    {
        AppendOffset();
        MarkValid();
    }

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        var a = export.NewArray(Length, nulls, [validity, export.Ptr(ref _offsets), export.Ptr(ref _data)], []);
        Length = 0;
        _offsets.Append(0);
        return a;
    }

    protected override void FreeBuffers()
    {
        _offsets.Free();
        _data.Free();
    }
}

internal sealed unsafe class NativeFixedSizeBinaryBuilder(FixedSizeBinaryType type) : NativeColumnBuilder(type)
{
    private NativeGrowBuffer _data;

    public int ByteWidth { get; } = type.ByteWidth;

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

/// <summary>
/// Dictionary-encoded strings or binaries with Int32 indices. The dictionary is either fixed up front
/// (<see cref="Symbols"/>: an Avro enum's symbols, re-emitted with every batch) or built per batch in
/// <see cref="Entries"/> (exported with the batch and then emptied).
/// </summary>
internal sealed unsafe class NativeDictionaryBuilder : NativeColumnBuilder
{
    private NativeGrowBuffer _indices;
    private byte[][] _symbols = [];

    public NativeDictionaryBuilder(DictionaryType type) : base(type)
    {
        if (type.IndexType is not Int32Type || type.ValueType is not (StringType or BinaryType))
            throw new NotSupportedException("Native dictionary builders produce Int32 indices over Utf8 or Binary values.");
        Entries = new NativeBinaryBuilder(type.ValueType);
    }

    public IReadOnlyList<string> Symbols
    {
        set => _symbols = [.. value.Select(Encoding.UTF8.GetBytes)];
    }

    /// <summary>The dictionary of the current batch, when no <see cref="Symbols"/> are set.</summary>
    public NativeBinaryBuilder Entries { get; }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(int index)
    {
        _indices.Append(index);
        MarkValid();
    }

    /// <summary>
    /// Appends one slot per byte of <paramref name="mask"/>, null where the byte is zero, and returns their
    /// indices (zeroed) for the caller to fill the valid ones.
    /// </summary>
    public Span<int> AppendSlots(ReadOnlySpan<byte> mask)
    {
        byte* at = _indices.Reserve((long)mask.Length * sizeof(int));
        var slots = new Span<int>(at, mask.Length);
        slots.Clear();
        MarkMask(mask);
        return slots;
    }

    public override void AppendNull()
    {
        _indices.Append(0);
        MarkNull();
    }

    public override void AppendDefault() => Append(0);

    public override NativeCArray* Export(NativeBatchExport export)
    {
        NativeCArray* dictionary;
        if (_symbols.Length > 0)
        {
            using var dict = new NativeBinaryBuilder(((DictionaryType)Type).ValueType);
            foreach (var s in _symbols) dict.Append(s);
            dictionary = dict.Export(export);
        }
        else dictionary = Entries.Export(export);
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        var a = export.NewArray(Length, nulls, [validity, export.Ptr(ref _indices)], [], dictionary);
        Length = 0;
        return a;
    }

    protected override void FreeBuffers()
    {
        _indices.Free();
        Entries.Dispose();
    }
}

/// <summary>List (or Map, which shares the layout) with 32-bit offsets.</summary>
internal sealed unsafe class NativeListBuilder : NativeColumnBuilder
{
    private NativeGrowBuffer _offsets;

    public NativeListBuilder(IArrowType type, NativeColumnBuilder values) : base(type)
    {
        Values = values;
        _offsets.Append(0);
    }

    public NativeColumnBuilder Values { get; }

    /// <summary>Closes a list whose items were appended to <see cref="Values"/> since the previous one.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void EndList()
    {
        AppendOffset();
        MarkValid();
    }

    private void AppendOffset()
    {
        long end = Values.Length;
        if (end > int.MaxValue) throw new InvalidOperationException("A list column holds more than 2^31 items in one batch; use a smaller batch size.");
        _offsets.Append((int)end);
    }

    public override void AppendNull()
    {
        AppendOffset();
        MarkNull();
    }

    public override void AppendDefault()
    {
        AppendOffset();
        MarkValid();
    }

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        nint offsets = export.Ptr(ref _offsets);
        var child = Values.Export(export);
        var a = export.NewArray(Length, nulls, [validity, offsets], [(nint)child]);
        Length = 0;
        _offsets.Append(0);
        return a;
    }

    protected override void FreeBuffers()
    {
        _offsets.Free();
        Values.Dispose();
    }
}

internal sealed unsafe class NativeStructBuilder(StructType type, NativeColumnBuilder[] children) : NativeColumnBuilder(type)
{
    private readonly bool[] _childNullable = [.. type.Fields.Select(f => f.IsNullable)];

    public NativeColumnBuilder[] Children { get; } = children;

    /// <summary>Marks a struct slot valid once its children have each received one value.</summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void EndStruct() => MarkValid();

    /// <summary>Marks <paramref name="count"/> struct slots valid once each child has that many more values.</summary>
    public void EndStructs(long count) => MarkValid(count);

    public override void AppendNull()
    {
        AppendChildPlaceholders();
        MarkNull();
    }

    public override void AppendDefault()
    {
        AppendChildPlaceholders();
        MarkValid();
    }

    private void AppendChildPlaceholders()
    {
        for (int i = 0; i < Children.Length; i++)
        {
            if (_childNullable[i]) Children[i].AppendNull();
            else Children[i].AppendDefault();
        }
    }

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        Span<nint> kids = Children.Length <= 64 ? stackalloc nint[Children.Length] : new nint[Children.Length];
        for (int i = 0; i < Children.Length; i++) kids[i] = (nint)Children[i].Export(export);
        var a = export.NewArray(Length, nulls, [validity], kids);
        Length = 0;
        return a;
    }

    /// <summary>Exports this struct as a record batch (the root array) and imports it.</summary>
    public RecordBatch ExportBatch(Schema schema)
    {
        var export = new NativeBatchExport();
        try
        {
            var root = Export(export);
            return export.Import(root, schema);
        }
        catch
        {
            export.FreeAll();
            throw;
        }
    }

    protected override void FreeBuffers()
    {
        foreach (var c in Children) c.Dispose();
    }
}
