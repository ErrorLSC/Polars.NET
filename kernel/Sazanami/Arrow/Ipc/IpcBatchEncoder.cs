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
        foreach (MemoryHandle pin in _pins) pin.Dispose();
        _pins.Clear();
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
            case ListViewType or LargeListViewType:
            {
                int width = type is ListViewType ? 4 : 8;
                body.Buffers.Add(Slice(body, data.Buffers[1], at, count, width));
                body.Buffers.Add(Slice(body, data.Buffers[2], at, count, width));
                Visit(body, field.Children[0], data.Children[0], 0, data.Children[0].Length, where + "." + field.Children[0].Field.Name); // views point anywhere in it
                return;
            }
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
            {
                body.Buffers.Add(Slice(body, data.Buffers[1], at, count, 16));
                // The data buffers are written whole: views point into them by index and offset.
                for (int b = 2; b < data.Buffers.Length; b++) body.Buffers.Add(Range(body, data.Buffers[b], 0, data.Buffers[b].Length));
                body.VariadicCounts.Add(data.Buffers.Length - 2);
                return;
            }
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
    /// Compresses each buffer with <paramref name="codec"/>: the uncompressed length first, or -1 and the bytes as they
    /// are when compression does not make them smaller. Empty buffers stay empty. The work runs on up to
    /// <paramref name="parallelism"/> threads in units: a buffer each, or for LZ4 its independent 64 KiB blocks
    /// (<see cref="Lz4UnitBytes"/>), for Zstandard frames of <paramref name="zstdFrameBytes"/>, each written into its
    /// slot of the buffer's output, the slots then closed up. The bytes are the same whatever the number of threads.
    /// </summary>
    public static void Compress(IpcEncodedBody body, IpcCompression codec, int level, int parallelism, int zstdFrameBytes = ZstdFrameBytes)
    {
        if (codec == IpcCompression.None) return;
        var pieces = body.Buffers;
        bool lz4 = codec == IpcCompression.Lz4Frame;
        int unitBytes = lz4 ? Lz4UnitBytes : zstdFrameBytes;
        long slot = lz4 ? 4L + Lz4Block.MaxCompressedLength(Lz4Frame.BlockBytes) : ZstdBlock.MaxCompressedLength(unitBytes); // a unit's bound
        int header = lz4 ? Lz4Frame.HeaderLength : 0, end = lz4 ? Lz4Frame.EndLength : 0;
        // Per buffer: its output (a frame, or slots for the units of a split one), allocated before the work starts.
        var outputs = new nint[pieces.Count];
        List<(int Piece, int Unit, long Start, int Length)> units = [];
        try
        {
            for (int i = 0; i < pieces.Count; i++)
            {
                long length = pieces[i].Length;
                if (length == 0) continue;
                if (length > unitBytes)
                {
                    int count = 0;
                    for (long at = 0; at < length; at += unitBytes) units.Add((i, count++, at, (int)Math.Min(unitBytes, length - at)));
                    outputs[i] = (nint)NativeMemoryPool.Alloc(header + count * slot + end);
                }
                else
                {
                    units.Add((i, -1, 0, checked((int)length)));
                    long bound = codec == IpcCompression.Zstd ? ZstdBlock.MaxCompressedLength((int)length) : Lz4Frame.MaxCompressedLength(length);
                    outputs[i] = (nint)NativeMemoryPool.Alloc(bound);
                }
            }
            var sizes = new int[units.Count];
            Parallel.For(0, units.Count, new ParallelOptions { MaxDegreeOfParallelism = Math.Max(1, parallelism) }, u =>
            {
                var (i, unit, start, length) = units[u];
                ReadOnlySpan<byte> source = pieces[i].Span.Slice((int)start, length);
                if (unit < 0)
                {
                    var target = new Span<byte>((void*)outputs[i], checked((int)(codec == IpcCompression.Zstd
                        ? ZstdBlock.MaxCompressedLength(length) : Lz4Frame.MaxCompressedLength(length))));
                    sizes[u] = codec == IpcCompression.Zstd ? ZstdBlock.Compress(source, target, level) : Lz4Frame.Compress(source, target);
                    return;
                }
                // Into the unit's slot: LZ4 blocks (the frame's header and end mark are written when the slots are closed
                // up), or a Zstandard frame.
                var into = new Span<byte>((byte*)outputs[i] + header + unit * slot, checked((int)slot));
                sizes[u] = lz4 ? Lz4Frame.Block(source, into, 0) : ZstdBlock.Compress(source, into, level);
            });
            var result = new IpcPiece[pieces.Count];
            for (int u = 0; u < units.Count;)
            {
                int i = units[u].Piece;
                IpcPiece piece = pieces[i];
                byte* frame = (byte*)outputs[i];
                long size;
                int next = u + 1;
                if (units[u].Unit < 0) size = sizes[u];
                else
                {
                    // Close up the slots (each moves down, never past the next).
                    long op = lz4 ? Lz4Frame.WriteHeader(new Span<byte>(frame, Lz4Frame.HeaderLength)) : 0;
                    for (; ; next++)
                    {
                        int k = next - 1;
                        Buffer.MemoryCopy(frame + header + units[k].Unit * slot, frame + op, sizes[k], sizes[k]);
                        op += sizes[k];
                        if (next == units.Count || units[next].Piece != i) break;
                    }
                    if (lz4) BinaryPrimitives.WriteUInt32LittleEndian(new Span<byte>(frame + op, 4), 0); // end mark
                    size = op + end;
                }
                body.Adopt(outputs[i]);
                outputs[i] = 0;
                result[i] = size < piece.Length ? new IpcPiece((nint)frame, size, piece.Length, true) : piece with { Prefix = -1, Prefixed = true };
                u = next;
            }
            pieces.Clear();
            pieces.AddRange(result);
        }
        finally
        {
            foreach (nint p in outputs)
            {
                if (p != 0) NativeMemoryPool.Free((void*)p);
            }
        }
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
