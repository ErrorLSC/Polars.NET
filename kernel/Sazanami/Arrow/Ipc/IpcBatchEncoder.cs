using System.Buffers;
using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Compression;
using Kaminari.Sazanami.Kernels;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>
/// One buffer of a body being written: bytes in place (an Arrow buffer, pinned) or made here, optionally preceded by
/// the 8-byte length of compressed buffers. A bitmap knows how many of its bits count, so that two bitmaps can be
/// compared without the bits past their length.
/// </summary>
internal readonly record struct IpcPiece(nint Pointer, long Length, long Prefix = 0, bool Prefixed = false, long Bits = -1)
{
    public long Size => Length + (Prefixed ? 8 : 0);

    public unsafe ReadOnlySpan<byte> Span => Length == 0 ? default : new((void*)Pointer, checked((int)Length));
}

/// <summary>
/// The body of one record batch (or dictionary batch) being written: field nodes, buffers in pre-order, variadic
/// buffer counts, and the dictionaries its dictionary-encoded arrays use. Arrays are read where they are: a slice is
/// written as its own rows (offsets rebased, bitmaps shifted, run ends cut), the rest points into the caller's buffers,
/// which stay pinned until this is disposed.
/// </summary>
internal sealed unsafe class IpcEncodedBody : IDisposable
{
    public List<IpcFieldNode> Nodes { get; } = [];

    public List<IpcPiece> Buffers { get; } = [];

    public List<long> VariadicCounts { get; } = [];

    /// <summary>The dictionaries used, by id, in the order first met.</summary>
    public List<(long Id, IpcField Field, ArrayData Dictionary)> Dictionaries { get; } = [];

    private readonly List<MemoryHandle> _pins = [];
    private readonly List<nint> _made = [];

    /// <summary>A block of memory freed with this body.</summary>
    public byte* Make(long bytes)
    {
        byte* p = NativeMemoryPool.Alloc(Math.Max(bytes, 1));
        _made.Add((nint)p);
        return p;
    }

    /// <summary>Takes a <see cref="NativeMemoryPool"/> block made elsewhere, freed with this body.</summary>
    public void Adopt(nint p) => _made.Add(p);

    /// <summary>
    /// Copies the buffers still read from the caller's arrays (those stored as they are) into memory of this body and
    /// lets the caller's go: the body then outlives the arrays it was made from.
    /// </summary>
    public void Own()
    {
        HashSet<nint> made = [.. _made];
        for (int i = 0; i < Buffers.Count; i++)
        {
            IpcPiece piece = Buffers[i];
            if (piece.Length == 0 || made.Contains(piece.Pointer)) continue; // a block of this body (anything else is copied)
            byte* copy = Make(piece.Length);
            Buffer.MemoryCopy((void*)piece.Pointer, copy, piece.Length, piece.Length);
            Buffers[i] = piece with { Pointer = (nint)copy };
        }
        ReleasePins();
    }

    /// <summary>Lets the caller's arrays go (nothing of this body reads them any more).</summary>
    public void ReleasePins()
    {
        foreach (MemoryHandle pin in _pins) pin.Dispose();
        _pins.Clear();
    }

    /// <summary>Where an Arrow buffer's bytes are (pinned until this is disposed).</summary>
    public byte* Pin(ArrowBuffer buffer)
    {
        if (buffer.Length == 0) return null;
        MemoryHandle handle = buffer.Memory.Pin();
        _pins.Add(handle);
        return (byte*)handle.Pointer;
    }

    public void Dispose()
    {
        ReleasePins();
        foreach (nint p in _made) NativeMemoryPool.Free((void*)p);
        _made.Clear();
    }
}

/// <summary>
/// Turns Arrow arrays into the nodes and buffers of an IPC body (Columnar.rst: the buffer layout of each type, in
/// pre-order), and compresses and lays out the buffers (64-byte aligned).
/// </summary>
internal static unsafe class IpcBatchEncoder
{
    /// <summary>The alignment of every buffer in a body (and of bodies in a file).</summary>
    public const int Alignment = 64;

    /// <summary>Encodes rows [<paramref name="first"/>, + <paramref name="count"/>) of <paramref name="columns"/> (one per field).</summary>
    /// <exception cref="ArgumentException">An array does not hold its field's type.</exception>
    public static void Encode(IpcEncodedBody body, IpcField[] fields, IReadOnlyList<ArrayData> columns, int first, int count)
    {
        for (int i = 0; i < fields.Length; i++) Visit(body, fields[i], columns[i], first, count, fields[i].Field.Name);
    }

    private static IArrowType Storage(IArrowType type) => type is ExtensionType e ? e.StorageType : type;

    private static void Visit(IpcEncodedBody body, IpcField field, ArrayData data, int first, int count, string where)
    {
        IArrowType type = field.Field.DataType;
        IArrowType actual = Storage(data.DataType);
        if (actual.TypeId != type.TypeId)
            throw new ArgumentException($"Column '{where}' holds {actual.Name} values; the schema says {type.Name}.");
        if ((long)first + count > data.Length) throw new ArgumentException($"Column '{where}' has {data.Length} values; rows up to {(long)first + count} are written.");
        int at = data.Offset + first; // physical slot of the first row

        switch (type)
        {
            case NullType:
                body.Nodes.Add(new IpcFieldNode(count, count));
                return;
            case UnionType u:
            {
                body.Nodes.Add(new IpcFieldNode(count, 0));
                body.Buffers.Add(Slice(body, data.Buffers[0], at, count, 1));
                if (u.Mode == UnionMode.Dense)
                {
                    body.Buffers.Add(Slice(body, data.Buffers[1], at, count, 4));
                    for (int c = 0; c < field.Children.Length; c++) Visit(body, field.Children[c], data.Children[c], 0, data.Children[c].Length, where + "." + field.Children[c].Field.Name);
                }
                else
                {
                    for (int c = 0; c < field.Children.Length; c++) Visit(body, field.Children[c], data.Children[c], at, count, where + "." + field.Children[c].Field.Name);
                }
                return;
            }
            case RunEndEncodedType:
                RunEndEncoded(body, field, data, at, count, where);
                return;
        }

        int nulls = Nulls(data, at, count);
        body.Nodes.Add(new IpcFieldNode(count, nulls));
        body.Buffers.Add(nulls == 0 ? default : Bitmap(body, data.Buffers[0], at, count));

        switch (type)
        {
            case DictionaryType d:
            {
                body.Buffers.Add(Slice(body, data.Buffers[1], at, count, ((FixedWidthType)d.IndexType).BitWidth / 8));
                if (data.Dictionary is null) throw new ArgumentException($"Column '{where}' is dictionary-encoded but has no dictionary.");
                if (!body.Dictionaries.Exists(e => e.Id == field.DictionaryId)) body.Dictionaries.Add((field.DictionaryId!.Value, field, data.Dictionary));
                return;
            }
            case BooleanType:
                body.Buffers.Add(Bitmap(body, data.Buffers[1], at, count));
                return;
            case StructType:
                for (int c = 0; c < field.Children.Length; c++) Visit(body, field.Children[c], data.Children[c], at, count, where + "." + field.Children[c].Field.Name);
                return;
            case FixedSizeListType f:
                Visit(body, field.Children[0], data.Children[0], at * f.ListSize, count * f.ListSize, where + "." + field.Children[0].Field.Name);
                return;
            case ListType or MapType:
            {
                (IpcPiece offsets, long start, long end) = Offsets<int>(body, data.Buffers[1], at, count);
                body.Buffers.Add(offsets);
                Visit(body, field.Children[0], data.Children[0], (int)start, (int)(end - start), where + "." + field.Children[0].Field.Name);
                return;
            }
            case LargeListType:
            {
                (IpcPiece offsets, long start, long end) = Offsets<long>(body, data.Buffers[1], at, count);
                body.Buffers.Add(offsets);
                Visit(body, field.Children[0], data.Children[0], checked((int)start), checked((int)(end - start)), where + "." + field.Children[0].Field.Name);
                return;
            }
            case ListViewType:
                ListViews<int>(body, field, data, at, count, where);
                return;
            case LargeListViewType:
                ListViews<long>(body, field, data, at, count, where);
                return;
            case BinaryType or StringType:
            {
                (IpcPiece offsets, long start, long end) = Offsets<int>(body, data.Buffers[1], at, count);
                body.Buffers.Add(offsets);
                body.Buffers.Add(Range(body, data.Buffers[2], start, end - start));
                return;
            }
            case LargeBinaryType or LargeStringType:
            {
                (IpcPiece offsets, long start, long end) = Offsets<long>(body, data.Buffers[1], at, count);
                body.Buffers.Add(offsets);
                body.Buffers.Add(Range(body, data.Buffers[2], start, end - start));
                return;
            }
            case BinaryViewType or StringViewType:
                Views(body, data, at, count, where);
                return;
            case FixedSizeBinaryType b:
                body.Buffers.Add(Slice(body, data.Buffers[1], at, count, b.ByteWidth));
                return;
            case FixedWidthType w when w.BitWidth % 8 == 0:
                body.Buffers.Add(Slice(body, data.Buffers[1], at, count, w.BitWidth / 8));
                return;
            default:
                throw new NotSupportedException($"Column '{where}' has type {type.Name}, which Arrow IPC cannot hold.");
        }
    }

    /// <summary>The nulls among slots [<paramref name="at"/>, + <paramref name="count"/>).</summary>
    private static int Nulls(ArrayData data, int at, int count)
    {
        if (data.Buffers.Length == 0 || data.Buffers[0].Length == 0) return 0;
        if (at == data.Offset && count == data.Length && data.NullCount >= 0) return data.NullCount;
        return count - (int)Kernels.Bits.Count(data.Buffers[0].Span, at, count);
    }

    /// <summary>Bytes [<paramref name="at"/> × <paramref name="width"/>, + <paramref name="count"/> × <paramref name="width"/>) of a buffer.</summary>
    private static IpcPiece Slice(IpcEncodedBody body, ArrowBuffer buffer, int at, int count, int width) =>
        Range(body, buffer, (long)at * width, (long)count * width);

    private static IpcPiece Range(IpcEncodedBody body, ArrowBuffer buffer, long start, long length)
    {
        if (length == 0) return default;
        if (start < 0 || start > buffer.Length - length) throw new ArgumentException($"An array's buffer of {buffer.Length} bytes is shorter than its values need ({start + length}).");
        return new IpcPiece((nint)(body.Pin(buffer) + start), length);
    }

    /// <summary>A bitmap's bits [<paramref name="at"/>, + <paramref name="count"/>): in place from a byte boundary, else shifted into a copy.</summary>
    private static IpcPiece Bitmap(IpcEncodedBody body, ArrowBuffer buffer, int at, int count)
    {
        if (count == 0) return default;
        long bytes = (count + 7L) >> 3;
        if ((at & 7) == 0)
        {
            IpcPiece piece = Range(body, buffer, at >> 3, bytes);
            return piece with { Bits = count };
        }
        if (((long)at + count + 7) >> 3 > buffer.Length) throw new ArgumentException("An array's bitmap is shorter than its values.");
        byte* copy = body.Make(bytes);
        var target = new Span<byte>(copy, (int)bytes);
        target.Clear();
        Kernels.Bits.Copy(buffer.Span, at, target, 0, count);
        return new IpcPiece((nint)copy, bytes, Bits: count);
    }

    /// <summary>
    /// The offsets of slots [<paramref name="at"/>, + <paramref name="count"/>], from 0 (in place when they start at 0,
    /// else rebased into a copy), and the range of the data or child they cover.
    /// </summary>
    private static (IpcPiece Offsets, long Start, long End) Offsets<T>(IpcEncodedBody body, ArrowBuffer buffer, int at, int count) where T : unmanaged, IBinaryInteger<T>
    {
        int width = sizeof(T);
        if (count == 0 && buffer.Length < (at + 1L) * width)
        {
            byte* zero = body.Make(width);
            new Span<byte>(zero, width).Clear();
            return (new IpcPiece((nint)zero, width), 0, 0);
        }
        if (buffer.Length < (at + count + 1L) * width) throw new ArgumentException($"An array's offsets ({buffer.Length} bytes) are shorter than its {count} values need.");
        ReadOnlySpan<T> offsets = MemoryMarshal.Cast<byte, T>(buffer.Span).Slice(at, count + 1);
        long start = long.CreateTruncating(offsets[0]), end = long.CreateTruncating(offsets[^1]);
        if (start == 0) return (Slice(body, buffer, at, count + 1, width), 0, end);
        byte* copy = body.Make((long)(count + 1) * width);
        var rebased = new Span<T>(copy, count + 1);
        for (int i = 0; i <= count; i++) rebased[i] = offsets[i] - offsets[0];
        return (new IpcPiece((nint)copy, (long)(count + 1) * width), start, end);
    }

    private static bool Valid(ReadOnlySpan<byte> validity, int slot) => validity.IsEmpty || Kernels.Bits.Get(validity, slot);

    /// <summary>
    /// The data bytes per view up to which a view array's data buffers are written whole without looking at its views:
    /// what a slice may then write in excess is bounded (measured: scanning the views of every array doubled the time of
    /// uncompressed writes with string views).
    /// </summary>
    private const int ViewDataBytesWhole = 64;

    /// <summary>The child items per list view up to which the child is written whole without looking at the views.</summary>
    private const int ListViewItemsWhole = 16;

    /// <summary>
    /// Binary or string views of slots [<paramref name="at"/>, + <paramref name="count"/>). Data buffers of up to
    /// <see cref="ViewDataBytesWhole"/> bytes per view are written whole, with the views as they are. Else (a slice
    /// perhaps: Apache's cannot be told from an array by its offset and length), of each data buffer only the bytes up
    /// to the last its valid views use; when they do not all start at 0, from the first: the views copied, those of null
    /// slots emptied, offsets counted from there. Buffers no view uses are empty (their count stays).
    /// </summary>
    private static void Views(IpcEncodedBody body, ArrayData data, int at, int count, string where)
    {
        int buffers = data.Buffers.Length - 2;
        body.VariadicCounts.Add(buffers);
        if (count == 0)
        {
            body.Buffers.Add(default);
            for (int b = 0; b < buffers; b++) body.Buffers.Add(default);
            return;
        }
        if (data.Buffers[1].Length < (at + (long)count) * 16) throw new ArgumentException($"Column '{where}': its views ({data.Buffers[1].Length} bytes) are fewer than its {count} values.");
        long dataBytes = 0;
        for (int b = 2; b < data.Buffers.Length; b++) dataBytes += data.Buffers[b].Length;
        if (dataBytes <= (long)count * ViewDataBytesWhole)
        {
            body.Buffers.Add(Slice(body, data.Buffers[1], at, count, 16));
            for (int b = 2; b < data.Buffers.Length; b++) body.Buffers.Add(Range(body, data.Buffers[b], 0, data.Buffers[b].Length));
            return;
        }
        ReadOnlySpan<byte> source = data.Buffers[1].Span.Slice(at * 16, count * 16), validity = data.Buffers[0].Span;
        Span<long> first = buffers <= 64 ? stackalloc long[buffers] : new long[buffers];
        Span<long> end = buffers <= 64 ? stackalloc long[buffers] : new long[buffers];
        first.Fill(long.MaxValue);
        end.Clear();
        for (int i = 0; i < count; i++)
        {
            if (!Valid(validity, at + i)) continue;
            ReadOnlySpan<byte> view = source.Slice(i * 16, 16);
            int size = BinaryPrimitives.ReadInt32LittleEndian(view);
            if (size <= 12) continue; // inline
            int b = BinaryPrimitives.ReadInt32LittleEndian(view[8..]), offset = BinaryPrimitives.ReadInt32LittleEndian(view[12..]);
            if ((uint)b >= (uint)buffers || offset < 0 || offset > data.Buffers[2 + b].Length - size)
                throw new ArgumentException($"Column '{where}': view {at + i} refers to bytes outside its data buffers.");
            first[b] = Math.Min(first[b], offset);
            end[b] = Math.Max(end[b], (long)offset + size);
        }
        bool fromZero = true;
        for (int b = 0; b < buffers; b++)
        {
            if (end[b] == 0) first[b] = 0;
            fromZero &= first[b] == 0;
        }
        if (fromZero)
        {
            // The views as they are; each buffer up to its last byte used.
            body.Buffers.Add(Slice(body, data.Buffers[1], at, count, 16));
            for (int b = 0; b < buffers; b++) body.Buffers.Add(end[b] == 0 ? default : Range(body, data.Buffers[2 + b], 0, end[b]));
            return;
        }
        byte* copy = body.Make(count * 16L);
        var views = new Span<byte>(copy, count * 16);
        source.CopyTo(views);
        for (int i = 0; i < count; i++)
        {
            Span<byte> view = views.Slice(i * 16, 16);
            if (!Valid(validity, at + i))
            {
                view.Clear(); // a null slot: an empty view, whatever it held
                continue;
            }
            if (BinaryPrimitives.ReadInt32LittleEndian(view) <= 12) continue;
            int b = BinaryPrimitives.ReadInt32LittleEndian(view[8..]);
            BinaryPrimitives.WriteInt32LittleEndian(view[12..], (int)(BinaryPrimitives.ReadInt32LittleEndian(view[12..]) - first[b]));
        }
        body.Buffers.Add(new IpcPiece((nint)copy, count * 16L));
        for (int b = 0; b < buffers; b++) body.Buffers.Add(end[b] == 0 ? default : Range(body, data.Buffers[2 + b], first[b], end[b] - first[b]));
    }

    /// <summary>
    /// List views of slots [<paramref name="at"/>, + <paramref name="count"/>). A child of up to
    /// <see cref="ListViewItemsWhole"/> items per view, or one the valid views use whole, is written whole with the
    /// offsets and sizes as they are. Else (a slice perhaps) only the items from the first to the last the valid views
    /// use, the offsets and sizes copied, counted from there, null and empty views made (0, 0): pyarrow validates the
    /// views of null slots too, so none may point past the child cut.
    /// </summary>
    private static void ListViews<T>(IpcEncodedBody body, IpcField field, ArrayData data, int at, int count, string where) where T : unmanaged, IBinaryInteger<T>
    {
        int width = sizeof(T);
        ArrayData child = data.Children[0];
        string inner = where + "." + field.Children[0].Field.Name;
        if (count == 0)
        {
            body.Buffers.Add(default);
            body.Buffers.Add(default);
            Visit(body, field.Children[0], child, 0, 0, inner);
            return;
        }
        long first = 0, end = child.Length;
        if (child.Length > (long)count * ListViewItemsWhole)
        {
            long need = (at + (long)count) * width;
            if (data.Buffers[1].Length < need || data.Buffers[2].Length < need)
                throw new ArgumentException($"Column '{where}': its offsets or sizes are fewer than its {count} values.");
            ReadOnlySpan<T> offsets = MemoryMarshal.Cast<byte, T>(data.Buffers[1].Span).Slice(at, count), sizes = MemoryMarshal.Cast<byte, T>(data.Buffers[2].Span).Slice(at, count);
            ReadOnlySpan<byte> validity = data.Buffers[0].Span;
            (first, end) = (long.MaxValue, 0);
            for (int i = 0; i < count; i++)
            {
                long offset = long.CreateSaturating(offsets[i]), size = long.CreateSaturating(sizes[i]);
                if (!Valid(validity, at + i) || size == 0) continue;
                if (offset < 0 || size < 0 || offset > child.Length - size)
                    throw new ArgumentException($"Column '{where}': list view {at + i} ({offset}, {size}) reaches past its {child.Length} items.");
                first = Math.Min(first, offset);
                end = Math.Max(end, offset + size);
            }
            if (end == 0) first = 0;
            if (first != 0 || end != child.Length)
            {
                T* o = (T*)body.Make((long)count * width), z = (T*)body.Make((long)count * width);
                for (int i = 0; i < count; i++)
                {
                    bool used = Valid(validity, at + i) && sizes[i] != T.Zero;
                    o[i] = used ? offsets[i] - T.CreateTruncating(first) : T.Zero;
                    z[i] = used ? sizes[i] : T.Zero;
                }
                body.Buffers.Add(new IpcPiece((nint)o, (long)count * width));
                body.Buffers.Add(new IpcPiece((nint)z, (long)count * width));
                Visit(body, field.Children[0], child, checked((int)first), checked((int)(end - first)), inner);
                return;
            }
        }
        body.Buffers.Add(Slice(body, data.Buffers[1], at, count, width));
        body.Buffers.Add(Slice(body, data.Buffers[2], at, count, width));
        Visit(body, field.Children[0], child, 0, child.Length, inner);
    }

    /// <summary>A run-end encoded slice: the runs it covers, their ends cut to it and counted from it.</summary>
    private static void RunEndEncoded(IpcEncodedBody body, IpcField field, ArrayData data, int at, int count, string where)
    {
        body.Nodes.Add(new IpcFieldNode(count, 0));
        ArrayData ends = data.Children[0], values = data.Children[1];
        int width = ((FixedWidthType)Storage(ends.DataType)).BitWidth / 8;
        ReadOnlySpan<byte> raw = ends.Buffers[1].Span[(ends.Offset * width)..];
        int runs = ends.Length, first = 0;
        while (first < runs && End(raw, width, first) <= at) first++;
        int last = first;
        if (count > 0)
        {
            while (last < runs && End(raw, width, last) < (long)at + count) last++;
            if (last == runs) throw new ArgumentException($"Column '{where}': the run ends stop before its rows do.");
            last++;
        }
        int kept = last - first;
        byte* copy = body.Make(Math.Max(1, (long)kept * width));
        for (int k = 0; k < kept; k++)
        {
            long end = Math.Min(End(raw, width, first + k), (long)at + count) - at;
            switch (width)
            {
                case 2: ((short*)copy)[k] = (short)end; break;
                case 4: ((int*)copy)[k] = (int)end; break;
                default: ((long*)copy)[k] = end; break;
            }
        }
        // The run ends: a node and its two buffers (no nulls); then the values of the runs kept.
        body.Nodes.Add(new IpcFieldNode(kept, 0));
        body.Buffers.Add(default);
        body.Buffers.Add(kept == 0 ? default : new IpcPiece((nint)copy, (long)kept * width));
        Visit(body, field.Children[1], values, first, kept, where + "." + field.Children[1].Field.Name);
    }

    private static long End(ReadOnlySpan<byte> raw, int width, int k) => width switch
    {
        2 => BinaryPrimitives.ReadInt16LittleEndian(raw[(2 * k)..]),
        4 => BinaryPrimitives.ReadInt32LittleEndian(raw[(4 * k)..]),
        _ => BinaryPrimitives.ReadInt64LittleEndian(raw[(8 * k)..]),
    };

    /// <summary>The uncompressed bytes of an LZ4 buffer compressed as one unit of work: one block (measured best: 16 Ki-row batches have buffers of a few blocks).</summary>
    internal const int Lz4UnitBytes = Lz4Frame.BlockBytes;

    /// <summary>
    /// The uncompressed bytes of a Zstandard frame: a larger buffer is compressed as several frames one after the other
    /// (a valid Zstandard stream, which every reader decompresses whole), side by side.
    /// </summary>
    internal const int ZstdFrameBytes = 32 << 20; // measured (10M rows, one batch, 16 threads): 8 MiB +3.7 % bytes, 32 MiB +0.8 % at the same speed

    /// <summary>
    /// The bytes of a chunk of a streamed body's output (<see cref="Compress"/> with a stream): each worker packs the
    /// units it compresses into a chunk of its own, then the next; chunks of one size return to the pool and are taken
    /// again as buffers are written. A unit whose bound is larger (a Zstandard frame) has a block of its own.
    /// </summary>
    internal const int StreamChunkBytes = 4 << 20;

    /// <summary>
    /// The LZ4 blocks of a unit of a streamed body: each unit is written as one piece, so that the writer makes a
    /// write per MiB rather than per block (measured: one block per unit made writing the bottleneck, +20 %).
    /// </summary>
    internal const int StreamLz4UnitBlocks = 16;

    /// <summary>
    /// Compresses each buffer with <paramref name="codec"/>: the uncompressed length first, or -1 and the bytes as they
    /// are when compression does not make them smaller. Empty buffers stay empty. The work runs on up to
    /// <paramref name="parallelism"/> threads in units handed out front to back: a buffer each, or for LZ4 its
    /// independent 64 KiB blocks (<see cref="Lz4UnitBytes"/>), for Zstandard frames of <paramref name="zstdFrameBytes"/>;
    /// the thread that finishes a buffer's last unit settles it. The bytes are the same whatever the number of threads.
    /// Without a stream, each buffer's units go into slots of one output, closed up into the buffer's bytes (the body's).
    /// With <paramref name="stream"/>, units are packed into chunks (<see cref="StreamChunkBytes"/>) and each buffer is
    /// handed over as its run of pieces as soon as it is settled (a buffer stored as it is is first copied into memory
    /// of the stream's when <paramref name="detach"/>: the caller's arrays may go once this returns).
    /// </summary>
    public static void Compress(IpcEncodedBody body, IpcCompression codec, int level, int parallelism, int zstdFrameBytes = ZstdFrameBytes,
        IpcBodyStream? stream = null, bool detach = false, CancellationToken cancellationToken = default)
    {
        if (codec == IpcCompression.None) return;
        var pieces = body.Buffers;
        bool lz4 = codec == IpcCompression.Lz4Frame;
        int blocks = stream is null ? 1 : StreamLz4UnitBlocks; // LZ4 blocks per unit
        int unitBytes = lz4 ? blocks * Lz4UnitBytes : zstdFrameBytes;
        long slot = lz4 ? blocks * (4L + Lz4Block.MaxCompressedLength(Lz4Frame.BlockBytes)) : ZstdBlock.MaxCompressedLength(unitBytes); // a unit's bound
        int header = lz4 ? Lz4Frame.HeaderLength : 0, end = lz4 ? Lz4Frame.EndLength : 0;
        // Per buffer: its first unit and the units still to compress. A unit is a buffer whole (-1) or its k-th part.
        var firstUnit = new int[pieces.Count];
        var left = new int[pieces.Count];
        List<(int Piece, int Unit, long Start, int Length)> units = [];
        for (int i = 0; i < pieces.Count; i++)
        {
            long length = pieces[i].Length;
            if (length == 0)
            {
                stream?.Publish(i, new IpcStreamedBuffer(default));
                continue;
            }
            firstUnit[i] = units.Count;
            if (length > unitBytes)
            {
                int count = 0;
                for (long at = 0; at < length; at += unitBytes) units.Add((i, count++, at, (int)Math.Min(unitBytes, length - at)));
                left[i] = count;
            }
            else
            {
                units.Add((i, -1, 0, checked((int)length)));
                left[i] = 1;
            }
        }
        var result = new IpcPiece[pieces.Count];
        var sizes = new int[units.Count];
        if (stream is null) Contiguous();
        else Chunked(stream);
        pieces.Clear();
        pieces.AddRange(result);

        // Each worker takes the next unit until none is left: units finish about in order, so that a stream sees the
        // buffers from the first on as the work goes (a range per worker would finish them all at the end).
        void Run(Action<int, Worker> work)
        {
            int next = -1;
            bool failed = false;
            int workers = Math.Min(Math.Max(1, parallelism), units.Count);
            if (workers == 0) return;
            Parallel.For(0, workers, new ParallelOptions { MaxDegreeOfParallelism = workers }, _ =>
            {
                var worker = new Worker();
                try
                {
                    while (!Volatile.Read(ref failed))
                    {
                        int u = Interlocked.Increment(ref next);
                        if (u >= units.Count) break;
                        cancellationToken.ThrowIfCancellationRequested();
                        work(u, worker);
                    }
                }
                catch
                {
                    Volatile.Write(ref failed, true);
                    throw;
                }
                finally
                {
                    worker.Chunk?.Unref(); // the worker's own reference
                }
            });
        }

        ReadOnlySpan<byte> Source(int u) => pieces[units[u].Piece].Span.Slice((int)units[u].Start, units[u].Length);

        // Compresses unit u into target (its bound).
        int CompressUnit(int u, Span<byte> target)
        {
            ReadOnlySpan<byte> source = Source(u);
            if (units[u].Unit < 0) return codec == IpcCompression.Zstd ? ZstdBlock.Compress(source, target, level) : Lz4Frame.Compress(source, target);
            // LZ4 blocks (the frame's header and end mark go around the blocks), or a Zstandard frame.
            if (!lz4) return ZstdBlock.Compress(source, target, level);
            int op = 0;
            for (int at = 0; at < source.Length; at += Lz4Frame.BlockBytes) op = Lz4Frame.Block(source[at..Math.Min(source.Length, at + Lz4Frame.BlockBytes)], target, op);
            return op;
        }

        long UnitBound(int u) => units[u].Unit < 0 ? Bound(units[u].Length) : slot;

        // The bytes buffer i takes compressed (its units done).
        long Size(int i)
        {
            int first = firstUnit[i];
            if (units[first].Unit < 0) return sizes[first];
            long size = header + end;
            for (int k = first; k < units.Count && units[k].Piece == i; k++) size += sizes[k];
            return size;
        }

        // Without a stream: each buffer's output allocated up front (measured: allocating while the workers fault pages
        // costs 6 %), its units in slots, closed up when the last is done.
        void Contiguous()
        {
            var outputs = new nint[pieces.Count];
            try
            {
                for (int i = 0; i < pieces.Count; i++)
                {
                    if (pieces[i].Length == 0) continue;
                    int first = firstUnit[i];
                    outputs[i] = (nint)NativeMemoryPool.Alloc(units[first].Unit < 0 ? Bound(units[first].Length) : header + left[i] * slot + end);
                }
                Run((u, _) =>
                {
                    int i = units[u].Piece;
                    byte* into = units[u].Unit < 0 ? (byte*)outputs[i] : (byte*)outputs[i] + header + units[u].Unit * slot;
                    sizes[u] = CompressUnit(u, new Span<byte>(into, checked((int)UnitBound(u))));
                    if (Interlocked.Decrement(ref left[i]) == 0) Close(i);
                });
                for (int i = 0; i < outputs.Length; i++)
                {
                    if (outputs[i] == 0) continue;
                    body.Adopt(outputs[i]);
                    outputs[i] = 0;
                }
            }
            finally
            {
                foreach (nint p in outputs)
                {
                    if (p != 0) NativeMemoryPool.Free((void*)p);
                }
            }

            // Buffer i's units are all done: its bytes, kept (smaller) or stored as they are.
            void Close(int i)
            {
                IpcPiece piece = pieces[i];
                byte* frame = (byte*)outputs[i];
                int first = firstUnit[i];
                long size = Size(i);
                if (units[first].Unit >= 0)
                {
                    // Close up the slots (each moves down, never past the next).
                    long op = lz4 ? Lz4Frame.WriteHeader(new Span<byte>(frame, Lz4Frame.HeaderLength)) : 0;
                    for (int k = first; k < units.Count && units[k].Piece == i; k++)
                    {
                        Buffer.MemoryCopy(frame + header + units[k].Unit * slot, frame + op, sizes[k], sizes[k]);
                        op += sizes[k];
                    }
                    if (lz4) BinaryPrimitives.WriteUInt32LittleEndian(new Span<byte>(frame + op, 4), 0); // end mark
                }
                if (size < piece.Length)
                {
                    result[i] = new IpcPiece((nint)frame, size, piece.Length, true);
                    return;
                }
                NativeMemoryPool.Free(frame);
                outputs[i] = 0;
                result[i] = piece with { Prefix = -1, Prefixed = true };
            }
        }

        // With a stream: units packed into the workers' chunks, buffers handed over as runs of pieces.
        void Chunked(IpcBodyStream stream)
        {
            var placed = new IpcChunkPiece[units.Count];
            Run(Work);

            void Work(int u, Worker worker)
            {
                long bound = UnitBound(u);
                if (worker.Chunk is not { } chunk || chunk.Bytes - worker.Used < bound)
                {
                    worker.Chunk?.Unref();
                    worker.Chunk = null; // dropped: not again if the next one cannot be had
                    worker.Chunk = chunk = stream.NewChunk(Math.Max(StreamChunkBytes, bound));
                    worker.Used = 0;
                }
                byte* at = chunk.Pointer + worker.Used;
                sizes[u] = CompressUnit(u, new Span<byte>(at, checked((int)bound)));
                chunk.Ref();
                placed[u] = new IpcChunkPiece(chunk, (nint)at, sizes[u]);
                worker.Used += sizes[u];
                int i = units[u].Piece;
                if (Interlocked.Decrement(ref left[i]) == 0) Settle(i);
            }

            // Buffer i's units are all done: handed over compressed (its run of pieces) or as it is.
            void Settle(int i)
            {
                IpcPiece piece = pieces[i];
                int first = firstUnit[i], count = 1;
                while (first + count < units.Count && units[first + count].Piece == i) count++;
                long size = Size(i);
                if (size < piece.Length)
                {
                    result[i] = new IpcPiece(0, size, piece.Length, true);
                    stream.Publish(i, new IpcStreamedBuffer(result[i], placed[first..(first + count)], lz4 && units[first].Unit >= 0));
                    return;
                }
                for (int k = first; k < first + count; k++) placed[k].Chunk.Unref(); // not used
                if (!detach)
                {
                    result[i] = piece with { Prefix = -1, Prefixed = true };
                    stream.Publish(i, new IpcStreamedBuffer(result[i]));
                    return;
                }
                byte* copy = NativeMemoryPool.Alloc(piece.Length);
                Buffer.MemoryCopy((void*)piece.Pointer, copy, piece.Length, piece.Length);
                result[i] = piece with { Pointer = (nint)copy, Prefix = -1, Prefixed = true };
                stream.Publish(i, new IpcStreamedBuffer(result[i], Owned: (nint)copy));
            }
        }

        long Bound(int length) => codec == IpcCompression.Zstd ? ZstdBlock.MaxCompressedLength(length) : Lz4Frame.MaxCompressedLength(length);
    }

    /// <summary>A compressing thread's state: its chunk (one reference of it its own) and how much of it is used.</summary>
    private sealed class Worker
    {
        public IpcChunk? Chunk;
        public long Used;
    }

    /// <summary>Where each buffer goes in the body (64-byte aligned), and the body's length.</summary>
    public static (IpcBuffer[] Buffers, long BodyLength) Layout(IReadOnlyList<IpcPiece> pieces)
    {
        var buffers = new IpcBuffer[pieces.Count];
        long at = 0;
        for (int i = 0; i < pieces.Count; i++)
        {
            buffers[i] = new IpcBuffer(at, pieces[i].Size);
            at = (at + pieces[i].Size + Alignment - 1) / Alignment * Alignment;
        }
        return (buffers, at);
    }
}
