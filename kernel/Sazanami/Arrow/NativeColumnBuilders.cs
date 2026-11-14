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
    /// Appends one slot per byte of <paramref name="mask"/>, valid where the byte is not zero; the bitmap is
    /// materialized only once a zero arrives.
    /// </summary>
    /// <param name="lengthBefore">Number of slots appended before these.</param>
    public void AppendMask(ReadOnlySpan<byte> mask, long lengthBefore)
    {
        if (!_materialized)
        {
            if (!mask.Contains((byte)0)) return;
            Materialize(lengthBefore);
        }
        _bits.AppendMask(mask);
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
        StringViewType or BinaryViewType => new NativeBinaryViewBuilder(type),
        FixedSizeBinaryType f => new NativeFixedSizeBinaryBuilder(f),
        DictionaryType d => new NativeDictionaryBuilder(d),
        MapType m => new NativeListBuilder(m, Create(m.KeyValueType)),
        ListType l => new NativeListBuilder(l, Create(l.ValueDataType)),
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
    public void AppendValues(ReadOnlySpan<byte> values, ReadOnlySpan<byte> mask = default)
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
    public Span<T> AppendUninitializedSlots(ReadOnlySpan<byte> mask)
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

/// <summary>Utf8 / Binary with 32-bit offsets.</summary>
internal sealed unsafe class NativeBinaryBuilder : NativeColumnBuilder
{
    private NativeGrowBuffer _offsets;
    private NativeGrowBuffer _data;

    public NativeBinaryBuilder(IArrowType type) : base(type) => _offsets.Append(0);

    /// <summary>Bytes of value data appended to the current batch.</summary>
    public long DataLength => _data.Length;

    protected override void ReserveValues(long slots, long dataBytes)
    {
        _offsets.EnsureAdditional(slots * sizeof(int));
        _data.EnsureAdditional(dataBytes);
    }

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

    /// <summary>
    /// Appends one slot per row: the values are <paramref name="source"/>[<paramref name="starts"/>[j] ..
    /// +<paramref name="lengths"/>[j]] in order, one per byte of <paramref name="mask"/> that is not zero (nulls where it
    /// is zero; every row a value when <paramref name="mask"/> is empty); <paramref name="totalBytes"/> is their sum.
    /// </summary>
    public void AppendValues(ReadOnlySpan<byte> source, ReadOnlySpan<int> starts, ReadOnlySpan<int> lengths, ReadOnlySpan<byte> mask, long totalBytes)
    {
        int rows = mask.IsEmpty ? starts.Length : mask.Length;
        if (_data.Length + totalBytes > int.MaxValue) ThrowTooLarge();
        _data.EnsureAdditional(totalBytes);
        var offsets = (int*)_offsets.Reserve((long)rows * sizeof(int));
        fixed (byte* from = source)
        {
            for (int i = 0, j = 0; i < rows; i++)
            {
                if (mask.IsEmpty || mask[i] != 0)
                {
                    int length = lengths[j];
                    Buffer.MemoryCopy(from + starts[j], _data.Reserve(length), length, length);
                    j++;
                }
                offsets[i] = (int)_data.Length;
            }
        }
        if (mask.IsEmpty) MarkValid(rows);
        else MarkMask(mask);
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

/// <summary>
/// Utf8View / BinaryView: a 16-byte view per slot (the length; up to 12 bytes inline, else a 4-byte prefix, the
/// data buffer's index and the offset in it) over data buffers. Long values appended go to a data buffer of the
/// batch (a new one before an offset would pass 2 GB); callers may also add buffers of their own
/// (<see cref="AddBuffer"/>, a dictionary's bytes) and write views into them directly.
/// </summary>
internal sealed unsafe class NativeBinaryViewBuilder(IArrowType type) : NativeColumnBuilder(type)
{
    public const int MaxInline = 12;

    private NativeGrowBuffer _views;
    private NativeGrowBuffer _data;
    private int _dataIndex = -1;                       // the buffer index of _data in this batch; -1 until used
    private readonly List<(nint Pointer, long Length)> _buffers = []; // data buffers by index (_data's slot filled on export)

    /// <summary>Copies <paramref name="bytes"/> into a data buffer of this batch; returns its index for views to point into.</summary>
    public int AddBuffer(ReadOnlySpan<byte> bytes)
    {
        byte* p = Helpers.NativeMemoryPool.Alloc(Math.Max(bytes.Length, 8));
        bytes.CopyTo(new Span<byte>(p, bytes.Length));
        _buffers.Add(((nint)p, bytes.Length));
        return _buffers.Count - 1;
    }

    /// <summary>The view of <paramref name="value"/> if it is stored in buffer <paramref name="buffer"/> at <paramref name="offset"/> (inline when short).</summary>
    public static Int128 View(ReadOnlySpan<byte> value, int buffer, int offset)
    {
        Int128 view = default;
        byte* v = (byte*)&view;
        *(int*)v = value.Length;
        if (value.Length <= MaxInline) value.CopyTo(new Span<byte>(v + 4, MaxInline));
        else
        {
            value[..4].CopyTo(new Span<byte>(v + 4, 4));
            *(int*)(v + 8) = buffer;
            *(int*)(v + 12) = offset;
        }
        return view;
    }

    public void Append(ReadOnlySpan<byte> value)
    {
        Int128* view = (Int128*)_views.Reserve(sizeof(Int128));
        if (value.Length <= MaxInline) *view = View(value, 0, 0);
        else
        {
            if (_dataIndex < 0 || _data.Length + value.Length > int.MaxValue) NewDataBuffer();
            *view = View(value, _dataIndex, (int)_data.Length);
            _data.Append(value);
        }
        MarkValid();
    }

    private void NewDataBuffer()
    {
        if (_dataIndex >= 0) _buffers[_dataIndex] = ((nint)_data.Detach(out long length), length);
        _buffers.Add((0, 0));
        _dataIndex = _buffers.Count - 1;
    }

    /// <summary>
    /// Appends one slot per row: the values are <paramref name="source"/>[<paramref name="starts"/>[j] ..
    /// +<paramref name="lengths"/>[j]] in order, one per byte of <paramref name="mask"/> that is not zero (nulls where it
    /// is zero; every row a value when <paramref name="mask"/> is empty). <paramref name="longBytes"/> is the total
    /// length of the values longer than <see cref="MaxInline"/>.
    /// </summary>
    public void AppendValues(ReadOnlySpan<byte> source, ReadOnlySpan<int> starts, ReadOnlySpan<int> lengths, ReadOnlySpan<byte> mask, long longBytes)
    {
        int rows = mask.IsEmpty ? starts.Length : mask.Length;
        if (longBytes > 0 && (_dataIndex < 0 || _data.Length + longBytes > int.MaxValue))
        {
            if (_dataIndex >= 0 || longBytes > int.MaxValue)
            {
                // The values would pass 2 GB in this buffer: one by one, each starting a new buffer when needed.
                for (int i = 0, j = 0; i < rows; i++)
                {
                    if (!mask.IsEmpty && mask[i] == 0) AppendNull();
                    else
                    {
                        Append(source.Slice(starts[j], lengths[j]));
                        j++;
                    }
                }
                return;
            }
            NewDataBuffer();
        }
        _data.EnsureAdditional(longBytes);
        var views = (Int128*)_views.Reserve((long)rows * sizeof(Int128));
        if (WritesPastCaches && Helpers.NonTemporal.IsSupported)
        {
            AppendValuesStaged(source, starts, lengths, mask, views, rows);
            if (mask.IsEmpty) MarkValid(rows);
            else MarkMask(mask);
            return;
        }
        fixed (byte* from = source)
        {
            for (int i = 0, j = 0; i < rows; i++)
            {
                if (!mask.IsEmpty && mask[i] == 0)
                {
                    views[i] = default;
                    continue;
                }
                int start = starts[j], length = lengths[j];
                j++;
                byte* v = (byte*)(views + i);
                if (length <= MaxInline)
                {
                    views[i] = default;
                    *(int*)v = length;
                    Buffer.MemoryCopy(from + start, v + 4, MaxInline, length);
                }
                else
                {
                    int offset = (int)_data.Length;
                    Buffer.MemoryCopy(from + start, _data.Reserve(length), length, length);
                    *(int*)v = length;
                    *(int*)(v + 4) = *(int*)(from + start);
                    *(int*)(v + 8) = _dataIndex;
                    *(int*)(v + 12) = offset;
                }
            }
        }
        if (mask.IsEmpty) MarkValid(rows);
        else MarkMask(mask);
    }

    /// <summary>
    /// Whether <see cref="AppendValues"/> writes views and long values past the caches: built in small blocks that
    /// stay in the cache, each block then stored non-temporally, so the output costs no read before its write. It pays
    /// when memory bandwidth is the limit (many threads decoding at once) and costs a little on a single thread,
    /// whose output often still sits in the cache when it is read.
    /// </summary>
    public bool WritesPastCaches { get; set; }

    private void AppendValuesStaged(ReadOnlySpan<byte> source, ReadOnlySpan<int> starts, ReadOnlySpan<int> lengths, ReadOnlySpan<byte> mask, Int128* views, int rows)
    {
        const int ViewBlock = 256, DataBlock = 16 * 1024;
        Int128* stagedViews = stackalloc Int128[ViewBlock];
        byte* stagedData = stackalloc byte[DataBlock];
        long dataBase = _data.Length; // where the staged bytes go once flushed
        int staged = 0;
        fixed (byte* from = source)
        {
            int k = 0, first = 0;
            for (int i = 0, j = 0; i < rows; i++)
            {
                Int128 view = default;
                if (mask.IsEmpty || mask[i] != 0)
                {
                    int start = starts[j], length = lengths[j];
                    j++;
                    byte* v = (byte*)&view;
                    *(int*)v = length;
                    if (length <= MaxInline) Buffer.MemoryCopy(from + start, v + 4, MaxInline, length);
                    else
                    {
                        if (staged + length > DataBlock) FlushData(stagedData, ref staged, ref dataBase);
                        long offset = dataBase + staged;
                        if (length > DataBlock)
                        {
                            Helpers.NonTemporal.Copy(new ReadOnlySpan<byte>(from + start, length), new Span<byte>(_data.Reserve(length), length));
                            dataBase += length;
                        }
                        else
                        {
                            Buffer.MemoryCopy(from + start, stagedData + staged, length, length);
                            staged += length;
                        }
                        *(int*)(v + 4) = *(int*)(from + start);
                        *(int*)(v + 8) = _dataIndex;
                        *(int*)(v + 12) = (int)offset;
                    }
                }
                stagedViews[k++] = view;
                if (k == ViewBlock)
                {
                    Helpers.NonTemporal.Copy(new ReadOnlySpan<byte>(stagedViews, k * sizeof(Int128)), new Span<byte>(views + first, k * sizeof(Int128)));
                    first += k;
                    k = 0;
                }
            }
            if (k > 0) Helpers.NonTemporal.Copy(new ReadOnlySpan<byte>(stagedViews, k * sizeof(Int128)), new Span<byte>(views + first, k * sizeof(Int128)));
            FlushData(stagedData, ref staged, ref dataBase);
        }
    }

    private void FlushData(byte* stagedData, ref int staged, ref long dataBase)
    {
        if (staged == 0) return;
        Helpers.NonTemporal.Copy(new ReadOnlySpan<byte>(stagedData, staged), new Span<byte>(_data.Reserve(staged), staged));
        dataBase += staged;
        staged = 0;
    }

    /// <summary>Appends <paramref name="count"/> valid slots and returns their views for the caller to fill.</summary>
    public Span<Int128> AppendViewSlots(int count)
    {
        var slots = new Span<Int128>(_views.Reserve((long)count * sizeof(Int128)), count);
        MarkValid(count);
        return slots;
    }

    /// <summary>
    /// Appends one slot per byte of <paramref name="mask"/>, null where it is zero; returns their views
    /// uninitialized: the caller writes every view, zero under the nulls.
    /// </summary>
    public Span<Int128> AppendUninitializedViewSlots(ReadOnlySpan<byte> mask)
    {
        var slots = new Span<Int128>(_views.Reserve((long)mask.Length * sizeof(Int128)), mask.Length);
        MarkMask(mask);
        return slots;
    }

    public override void AppendNull()
    {
        _views.Append(default(Int128));
        MarkNull();
    }

    public override void AppendDefault()
    {
        _views.Append(default(Int128));
        MarkValid();
    }

    protected override void ReserveValues(long slots, long dataBytes)
    {
        _views.EnsureAdditional(slots * sizeof(Int128));
        if (dataBytes > 0) _data.EnsureAdditional(Math.Min(dataBytes, int.MaxValue));
    }

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        if (_dataIndex >= 0) _buffers[_dataIndex] = ((nint)_data.Detach(out long length), length);
        var buffers = new nint[2 + _buffers.Count];
        buffers[0] = validity;
        buffers[1] = export.Ptr(ref _views);
        for (int i = 0; i < _buffers.Count; i++) buffers[2 + i] = (nint)export.Own((byte*)_buffers[i].Pointer, _buffers[i].Length);
        _buffers.Clear();
        _dataIndex = -1;
        var a = export.NewArray(Length, nulls, buffers, []);
        Length = 0;
        return a;
    }

    protected override void FreeBuffers()
    {
        _views.Free();
        _data.Free();
        foreach (var (pointer, _) in _buffers)
        {
            Helpers.NativeMemoryPool.Free((void*)pointer);
        }
        _buffers.Clear();
        _dataIndex = -1;
    }
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

    protected override void ReserveValues(long slots, long dataBytes) => _indices.EnsureAdditional(slots * sizeof(int));

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

    private long _end; // the last offset appended

    /// <summary>The items the lists appended so far span (the last offset).</summary>
    public long ItemCount => _end;

    /// <summary>
    /// Appends one list per entry of <paramref name="lengths"/> (their items appended to <see cref="Values"/> by the
    /// caller); with a <paramref name="mask"/> of the same length, lists whose mask byte is zero are null.
    /// </summary>
    public void AppendLists(ReadOnlySpan<int> lengths, ReadOnlySpan<byte> mask = default)
    {
        long end = _end;
        byte* at = _offsets.Reserve((long)lengths.Length * sizeof(int));
        var offsets = new Span<int>(at, lengths.Length);
        for (int i = 0; i < lengths.Length; i++)
        {
            end += lengths[i];
            if (end > int.MaxValue) throw new InvalidOperationException("A list column holds more than 2^31 items in one batch; use a smaller batch size.");
            offsets[i] = (int)end;
        }
        _end = end;
        if (mask.IsEmpty) MarkValid(lengths.Length);
        else MarkMask(mask);
    }

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
        _end = end;
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
        _end = 0;
        return a;
    }

    protected override void FreeBuffers()
    {
        _offsets.Free();
        Values.Dispose();
    }
}

/// <summary>Fixed-size lists: every slot, null or not, holds <c>ListSize</c> items of <see cref="Values"/>.</summary>
internal sealed unsafe class NativeFixedSizeListBuilder(FixedSizeListType type, NativeColumnBuilder values) : NativeColumnBuilder(type)
{
    public NativeColumnBuilder Values { get; } = values;

    public int ListSize { get; } = type.ListSize;

    /// <summary>Marks one slot per byte of <paramref name="mask"/> (null where it is zero; all valid when empty: <paramref name="count"/> slots), the items appended by the caller.</summary>
    public void AppendSlots(int count, ReadOnlySpan<byte> mask = default)
    {
        if (mask.IsEmpty) MarkValid(count);
        else MarkMask(mask[..count]);
    }

    public override void AppendNull()
    {
        for (int i = 0; i < ListSize; i++) Values.AppendNull();
        MarkNull();
    }

    public override void AppendDefault()
    {
        for (int i = 0; i < ListSize; i++) Values.AppendDefault();
        MarkValid();
    }

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        var child = Values.Export(export);
        var a = export.NewArray(Length, nulls, [validity], [(nint)child]);
        Length = 0;
        return a;
    }

    protected override void FreeBuffers() => Values.Dispose();
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

    /// <summary>Marks one struct slot per byte of <paramref name="mask"/> (null where it is zero), the children filled by the caller.</summary>
    public void EndStructs(ReadOnlySpan<byte> mask) => MarkMask(mask);

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
