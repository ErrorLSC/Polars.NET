using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Interop;

namespace Kaminari.Sazanami.Arrow.Builders;

/// <summary>A builder of strings or binaries that takes values as bytes, whatever its layout.</summary>
internal interface IBytesBuilder
{
    void Append(ReadOnlySpan<byte> value);
}

/// <summary>Utf8 / Binary with 32-bit offsets.</summary>
internal sealed unsafe class NativeBinaryBuilder : NativeColumnBuilder, IBytesBuilder
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

    /// <summary>Appends text, encoded as UTF-8 straight into the value buffer (lone surrogates become U+FFFD).</summary>
    public void AppendUtf8(ReadOnlySpan<char> text)
    {
        long start = _data.Length;
        int most = System.Text.Encoding.UTF8.GetMaxByteCount(text.Length);
        byte* at = _data.Reserve(most);
        int written = System.Text.Encoding.UTF8.GetBytes(text, new Span<byte>(at, most));
        _data.SetLength(start + written);
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

/// <summary>LargeUtf8 / LargeBinary: 64-bit offsets.</summary>
internal sealed unsafe class NativeLargeBinaryBuilder : NativeColumnBuilder, IBytesBuilder
{
    private NativeGrowBuffer _offsets;
    private NativeGrowBuffer _data;

    public NativeLargeBinaryBuilder(IArrowType type) : base(type) => _offsets.Append(0L);

    protected override void ReserveValues(long slots, long dataBytes)
    {
        _offsets.EnsureAdditional(slots * sizeof(long));
        _data.EnsureAdditional(dataBytes);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(ReadOnlySpan<byte> value)
    {
        _data.Append(value);
        _offsets.Append(_data.Length);
        MarkValid();
    }

    /// <summary>Appends text, encoded as UTF-8 straight into the value buffer (lone surrogates become U+FFFD).</summary>
    public void AppendUtf8(ReadOnlySpan<char> text)
    {
        long start = _data.Length;
        int most = System.Text.Encoding.UTF8.GetMaxByteCount(text.Length);
        byte* at = _data.Reserve(most);
        int written = System.Text.Encoding.UTF8.GetBytes(text, new Span<byte>(at, most));
        _data.SetLength(start + written);
        _offsets.Append(_data.Length);
        MarkValid();
    }

    public override void AppendNull()
    {
        _offsets.Append(_data.Length);
        MarkNull();
    }

    public override void AppendDefault()
    {
        _offsets.Append(_data.Length);
        MarkValid();
    }

    public override NativeCArray* Export(NativeBatchExport export)
    {
        long nulls = NullCount;
        nint validity = Validity.Detach(export);
        var a = export.NewArray(Length, nulls, [validity, export.Ptr(ref _offsets), export.Ptr(ref _data)], []);
        Length = 0;
        _offsets.Append(0L);
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
internal sealed unsafe class NativeBinaryViewBuilder(IArrowType type) : NativeColumnBuilder(type), IBytesBuilder
{
    public const int MaxInline = 12;

    private NativeGrowBuffer _views;
    private NativeGrowBuffer _data;
    private int _dataIndex = -1;                       // the buffer index of _data in this batch; -1 until used
    private readonly List<(nint Pointer, long Length)> _buffers = []; // data buffers by index (_data's slot filled on export)

    /// <summary>Copies <paramref name="bytes"/> into a data buffer of this batch; returns its index for views to point into.</summary>
    public int AddBuffer(ReadOnlySpan<byte> bytes)
    {
        byte* p = Buffers.NativeMemoryPool.Alloc(Math.Max(bytes.Length, 8));
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

    /// <summary>Appends text, encoded as UTF-8 straight into the view or the data buffer (lone surrogates become U+FFFD).</summary>
    public void AppendUtf8(ReadOnlySpan<char> text)
    {
        if (text.Length <= MaxInline)
        {
            // Short ASCII text (the common case) goes straight into its view.
            Int128 inline = default;
            byte* v = (byte*)&inline;
            if (System.Text.Ascii.FromUtf16(text, new Span<byte>(v + 4, MaxInline), out int length) == System.Buffers.OperationStatus.Done)
            {
                *(int*)v = length;
                *(Int128*)_views.Reserve(sizeof(Int128)) = inline;
                MarkValid();
                return;
            }
        }
        int most = Encoding.UTF8.GetMaxByteCount(text.Length);
        if (_dataIndex < 0 || _data.Length + most > int.MaxValue) NewDataBuffer();
        long start = _data.Length;
        byte* at = _data.Reserve(most);
        var target = new Span<byte>(at, most);
        int written = System.Text.Ascii.FromUtf16(text, target, out int ascii) == System.Buffers.OperationStatus.Done
            ? ascii
            : Encoding.UTF8.GetBytes(text, target);
        var value = new ReadOnlySpan<byte>(at, written);
        Int128* view = (Int128*)_views.Reserve(sizeof(Int128));
        if (written <= MaxInline)
        {
            *view = View(value, 0, 0);
            _data.SetLength(start);
        }
        else
        {
            *view = View(value, _dataIndex, (int)start);
            _data.SetLength(start + written);
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
    public void AppendValues(ReadOnlySpan<byte> source, ReadOnlySpan<int> starts, ReadOnlySpan<int> lengths, Kernels.SlotMask mask, long longBytes)
    {
        int rows = mask.IsEmpty ? starts.Length : mask.Length;
        if (longBytes > 0 && (_dataIndex < 0 || _data.Length + longBytes > int.MaxValue))
        {
            if (_dataIndex >= 0 || longBytes > int.MaxValue)
            {
                // The values would pass 2 GB in this buffer: one by one, each starting a new buffer when needed.
                for (int i = 0, j = 0; i < rows; i++)
                {
                    if (!mask.IsEmpty && !mask.IsValid(i)) AppendNull();
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
        if (WritesPastCaches && Buffers.NonTemporal.IsSupported)
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
                if (!mask.IsEmpty && !mask.IsValid(i))
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

    private void AppendValuesStaged(ReadOnlySpan<byte> source, ReadOnlySpan<int> starts, ReadOnlySpan<int> lengths, Kernels.SlotMask mask, Int128* views, int rows)
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
                if (mask.IsEmpty || mask.IsValid(i))
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
                            Buffers.NonTemporal.Copy(new ReadOnlySpan<byte>(from + start, length), new Span<byte>(_data.Reserve(length), length));
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
                    Buffers.NonTemporal.Copy(new ReadOnlySpan<byte>(stagedViews, k * sizeof(Int128)), new Span<byte>(views + first, k * sizeof(Int128)));
                    first += k;
                    k = 0;
                }
            }
            if (k > 0) Buffers.NonTemporal.Copy(new ReadOnlySpan<byte>(stagedViews, k * sizeof(Int128)), new Span<byte>(views + first, k * sizeof(Int128)));
            FlushData(stagedData, ref staged, ref dataBase);
        }
    }

    private void FlushData(byte* stagedData, ref int staged, ref long dataBase)
    {
        if (staged == 0) return;
        Buffers.NonTemporal.Copy(new ReadOnlySpan<byte>(stagedData, staged), new Span<byte>(_data.Reserve(staged), staged));
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
    public Span<Int128> AppendUninitializedViewSlots(Kernels.SlotMask mask)
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
            Buffers.NativeMemoryPool.Free((void*)pointer);
        }
        _buffers.Clear();
        _dataIndex = -1;
    }
}
