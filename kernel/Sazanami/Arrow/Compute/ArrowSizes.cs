using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Kernels;
using Kaminari.Sazanami.Arrow.Interop;

namespace Kaminari.Sazanami.Arrow.Compute;

/// <summary>
/// Two sizes of Arrow data, which the bytes of its buffers mix up once arrays are sliced or share buffers (a slice
/// keeps its parent's buffers; an engine's string views point into data buffers many arrays share):
/// <list type="bullet">
/// <item><see cref="LogicalBytes(RecordBatch)"/>: the bytes the rows themselves hold, as a compact copy of just them
/// would take (validity bits when a row is null, values, offsets and the values they span, views and the long values
/// they point to, list items, run ends and values of their runs, a dictionary whole); the same for a slice as for the
/// copy of it, whatever else shares the buffers. For sizing work by rows: segments, row groups, files.</item>
/// <item><see cref="RetainedBytes(IEnumerable{RecordBatch})"/>: the memory the batches keep alive, each buffer counted
/// once however many arrays (or slices) hold it. For memory budgets.</item>
/// </list>
/// </summary>
internal static class ArrowSizes
{
    /// <summary>The logical bytes of a batch's rows (every column, children and dictionaries included).</summary>
    public static long LogicalBytes(RecordBatch batch)
    {
        long bytes = 0;
        for (int c = 0; c < batch.ColumnCount; c++) bytes += LogicalBytes(batch.Column(c).Data, 0, batch.Column(c).Length);
        return bytes;
    }

    /// <summary>The logical bytes of rows <paramref name="offset"/> .. +<paramref name="length"/> of a batch.</summary>
    public static long LogicalBytes(RecordBatch batch, int offset, int length)
    {
        long bytes = 0;
        for (int c = 0; c < batch.ColumnCount; c++) bytes += LogicalBytes(batch.Column(c).Data, offset, length);
        return bytes;
    }

    /// <summary>The logical bytes of an array's rows <paramref name="offset"/> .. +<paramref name="length"/>.</summary>
    public static long LogicalBytes(IArrowArray array, int offset, int length) => LogicalBytes(array.Data, offset, length);

    /// <summary>
    /// The logical bytes of rows <paramref name="start"/> .. +<paramref name="length"/> of <paramref name="data"/>
    /// (counted from its first row: its own offset applies).
    /// </summary>
    internal static long LogicalBytes(ArrayData data, long start, long length)
    {
        if (length <= 0) return 0;
        long at = data.Offset + start; // the position in the buffers
        return data.DataType switch
        {
            NullType => 0,
            DictionaryType => Validity(data, at, length) + FixedWidth(((DictionaryType)data.DataType).IndexType, length)
                + (data.Dictionary is { } dictionary ? LogicalBytes(dictionary, 0, dictionary.Length) : 0),
            // The large and view layouts first: their types may derive from the plain ones.
            LargeStringType or LargeBinaryType => Validity(data, at, length) + 8 * (length + 1) + Span(Cast<long>(data.Buffers[1]), at, length),
            StringViewType or BinaryViewType => Validity(data, at, length) + 16 * length + LongViewBytes(data, at, length),
            StringType or BinaryType => Validity(data, at, length) + 4 * (length + 1) + Span(Cast<int>(data.Buffers[1]), at, length),
            LargeListViewType => Validity(data, at, length) + 16 * length + ViewItems(data, Cast<long>(data.Buffers[1]), Cast<long>(data.Buffers[2]), at, length),
            ListViewType => Validity(data, at, length) + 8 * length + ViewItems(data, Cast<int>(data.Buffers[1]), Cast<int>(data.Buffers[2]), at, length),
            LargeListType => Validity(data, at, length) + 8 * (length + 1) + Items(data, Cast<long>(data.Buffers[1]), at, length),
            ListType or MapType => Validity(data, at, length) + 4 * (length + 1) + Items(data, Cast<int>(data.Buffers[1]), at, length),
            StructType => Validity(data, at, length) + Children(data, at, length),
            FixedSizeListType f => Validity(data, at, length) + LogicalBytes(data.Children[0], at * f.ListSize, length * f.ListSize),
            RunEndEncodedType => Runs(data, at, length),
            UnionType u => Union(data, u, at, length),
            FixedWidthType f => Validity(data, at, length) + FixedWidth(f, length),
            _ => throw new NotSupportedException($"The size of {data.DataType.Name} arrays."),
        };
    }

    /// <summary>The validity bits of the rows, when one of them is null (a compact copy keeps no bitmap otherwise).</summary>
    private static long Validity(ArrayData data, long at, long length) =>
        data.NullCount != 0 && data.Buffers is [{ IsEmpty: false } bitmap, ..] && Bits.Count(bitmap.Span, at, length) < length ? (length + 7) / 8 : 0;

    private static long FixedWidth(IArrowType type, long length) =>
        type is FixedWidthType f ? (length * f.BitWidth + 7) / 8 : throw new NotSupportedException($"The size of {type.Name} values.");

    /// <summary>The values the offsets of rows at .. +length span.</summary>
    private static long Span<T>(ReadOnlySpan<T> offsets, long at, long length) where T : unmanaged, IBinaryInteger<T> =>
        Value(offsets[(int)(at + length)]) - Value(offsets[(int)at]);

    /// <summary>
    /// The bytes of the values of views at .. +length held outside the views (longer than 12 bytes); a null row's view
    /// counts nothing, whatever it holds.
    /// </summary>
    private static long LongViewBytes(ArrayData data, long at, long length)
    {
        // Each view: its length (int32), then 12 bytes (inline value, or prefix, buffer index and offset).
        ReadOnlySpan<int> words = Cast<int>(data.Buffers[1]).Slice((int)(at * 4), (int)(length * 4));
        long bytes = 0;
        if (data.NullCount != 0 && data.Buffers[0] is { IsEmpty: false } bitmap)
        {
            ReadOnlySpan<byte> valid = bitmap.Span;
            for (int k = 0; k < length; k++)
            {
                int size = words[4 * k];
                if (size > 12 && Bits.Get(valid, at + k)) bytes += size;
            }
            return bytes;
        }
        for (int k = 0; k < words.Length; k += 4)
        {
            int size = words[k];
            if (size > 12) bytes += size;
        }
        return bytes;
    }

    private static long Children(ArrayData data, long at, long length)
    {
        long bytes = 0;
        foreach (ArrayData child in data.Children ?? []) bytes += LogicalBytes(child, at, length);
        return bytes;
    }

    /// <summary>The items of lists at .. +length (offsets into the child).</summary>
    private static long Items<T>(ArrayData data, ReadOnlySpan<T> offsets, long at, long length) where T : unmanaged, IBinaryInteger<T>
    {
        long first = Value(offsets[(int)at]), end = Value(offsets[(int)(at + length)]);
        return LogicalBytes(data.Children[0], first, end - first);
    }

    /// <summary>The items of list views at .. +length, element by element (consecutive ones taken together).</summary>
    private static long ViewItems<T>(ArrayData data, ReadOnlySpan<T> offsets, ReadOnlySpan<T> sizes, long at, long length) where T : unmanaged, IBinaryInteger<T>
    {
        ArrayData child = data.Children[0];
        long bytes = 0, runStart = 0, runEnd = 0;
        for (long k = at; k < at + length; k++)
        {
            long size = Value(sizes[(int)k]);
            if (size == 0) continue;
            long first = Value(offsets[(int)k]);
            if (first != runEnd)
            {
                bytes += LogicalBytes(child, runStart, runEnd - runStart);
                runStart = first;
            }
            runEnd = first + size;
        }
        return bytes + LogicalBytes(child, runStart, runEnd - runStart);
    }

    /// <summary>The runs covering logical rows at .. +length: their run ends and values.</summary>
    private static long Runs(ArrayData data, long at, long length)
    {
        ArrayData ends = data.Children[0], values = data.Children[1];
        int first = RunOf(ends, at), last = RunOf(ends, at + length - 1);
        return LogicalBytes(ends, first, last - first + 1) + LogicalBytes(values, first, last - first + 1);

        // The first run whose end is above the row.
        static int RunOf(ArrayData ends, long row)
        {
            int lo = 0, hi = ends.Length - 1;
            while (lo < hi)
            {
                int mid = (lo + hi) >>> 1;
                if (RunEnd(ends, mid) > row) hi = mid;
                else lo = mid + 1;
            }
            return lo;
        }

        static long RunEnd(ArrayData ends, int k) => ends.DataType.TypeId switch
        {
            ArrowTypeId.Int16 => Cast<short>(ends.Buffers[1])[ends.Offset + k],
            ArrowTypeId.Int32 => Cast<int>(ends.Buffers[1])[ends.Offset + k],
            _ => Cast<long>(ends.Buffers[1])[ends.Offset + k],
        };
    }

    /// <summary>
    /// A union's type ids (and offsets, dense), and its children: a sparse union's over the same rows, a dense one's
    /// over the range of each child its rows point into.
    /// </summary>
    private static long Union(ArrayData data, UnionType type, long at, long length)
    {
        if (type.Mode == UnionMode.Sparse) return length + Children(data, at, length);
        ReadOnlySpan<sbyte> ids = Cast<sbyte>(data.Buffers[0]).Slice((int)at, (int)length);
        ReadOnlySpan<int> offsets = Cast<int>(data.Buffers[1]).Slice((int)at, (int)length);
        long bytes = 5 * length;
        for (int c = 0; c < data.Children.Length; c++)
        {
            sbyte id = (sbyte)type.TypeIds[c];
            int first = int.MaxValue, last = -1;
            for (int k = 0; k < ids.Length; k++)
            {
                if (ids[k] != id) continue;
                first = Math.Min(first, offsets[k]);
                last = Math.Max(last, offsets[k]);
            }
            if (last >= 0) bytes += LogicalBytes(data.Children[c], first, last - first + 1);
        }
        return bytes;
    }

    /// <summary>
    /// The memory the batches keep alive: every buffer (children and dictionaries included) counted once, however many
    /// arrays or slices hold it. Buffers of managed arrays are known by their array (counted whole), others by their
    /// address (the longest view of an address counted); distinct buffers inside one allocation count apart.
    /// </summary>
    public static long RetainedBytes(IEnumerable<RecordBatch> batches)
    {
        var seen = new RetainedBuffers();
        foreach (RecordBatch batch in batches)
        {
            for (int c = 0; c < batch.ColumnCount; c++) seen.Add(batch.Column(c).Data);
        }
        return seen.Bytes;
    }

    /// <inheritdoc cref="RetainedBytes(IEnumerable{RecordBatch})"/>
    public static long RetainedBytes(RecordBatch batch) => RetainedBytes([batch]);

    private sealed class RetainedBuffers
    {
        private readonly Dictionary<object, long> _arrays = new(ReferenceEqualityComparer.Instance);
        private readonly Dictionary<nint, long> _addresses = [];

        public long Bytes => _arrays.Values.Sum() + _addresses.Values.Sum();

        public void Add(ArrayData data)
        {
            foreach (ArrowBuffer buffer in data.Buffers ?? []) Add(buffer.Memory);
            foreach (ArrayData child in data.Children ?? []) Add(child);
            if (data.Dictionary is { } dictionary) Add(dictionary);
        }

        private unsafe void Add(ReadOnlyMemory<byte> memory)
        {
            if (memory.IsEmpty) return;
            if (MemoryMarshal.TryGetArray(memory, out ArraySegment<byte> segment))
            {
                _arrays[segment.Array!] = segment.Array!.Length;
                return;
            }
            nint address = (nint)Unsafe.AsPointer(ref MemoryMarshal.GetReference(memory.Span));
            _addresses[address] = Math.Max(_addresses.GetValueOrDefault(address), memory.Length);
        }
    }

    private static ReadOnlySpan<T> Cast<T>(ArrowBuffer buffer) where T : struct => MemoryMarshal.Cast<byte, T>(buffer.Span);

    private static long Value<T>(T value) where T : unmanaged, IBinaryInteger<T> => long.CreateTruncating(value);
}
