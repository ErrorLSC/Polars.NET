using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Buffers;
using Kaminari.Sazanami.Filters;
using Kaminari.Sazanami.Kernels;
using Kaminari.Sazanami.Arrow.Builders;
using Kaminari.Sazanami.Arrow.Interop;

namespace Kaminari.Sazanami.Arrow.Compute;

/// <summary>
/// Gathers the rows of an Arrow array (flat, or structs, lists, large lists, maps and fixed-size lists of them) that an
/// index list names, in any order and with repeats, into a new array over native memory of its own (freed when it is
/// disposed, as the arrays of <see cref="NativeBatchExport"/>). <see cref="NullRow"/> takes a null row. The input is
/// left as it is; it must stay alive while it is taken from.
/// </summary>
/// <remarks>
/// The indices are checked once, up front (their bounds); the gathers then run without checks. Values are gathered
/// one by one: .NET's portable vectors have no gather, and the loads of rows in random order are bound by memory, not
/// by instructions. Long values of views and the dictionaries of dictionary arrays are shared with the input instead
/// of copied when the input's memory is a native batch's (as <see cref="ArrowFilter"/> does, with the same rule for
/// views: shared when enough rows are taken, <see cref="ArrowFilter.ShareViewDataFrom"/>). Elements of lists are
/// taken by index lists of their own, in native scratch memory.
/// </remarks>
internal static unsafe class ArrowTake
{
    /// <summary>The index that takes a null row (outer-join and MERGE output).</summary>
    public const int NullRow = -1;

    /// <summary>The rows <paramref name="rows"/> names of <paramref name="array"/>, in that order.</summary>
    /// <param name="rows">Row indices into <paramref name="array"/>: 0 ≤ i &lt; its length, or <see cref="NullRow"/>.</param>
    /// <param name="shareViewData">Long view values: true shares the input's data buffers, false copies the values
    /// taken, null shares when the rows taken are at least <see cref="ArrowFilter.ShareViewDataFrom"/> of the input's.</param>
    /// <exception cref="ArgumentOutOfRangeException">An index outside the array.</exception>
    /// <exception cref="OverflowException">The bytes or list elements taken pass 2 GB / <see cref="int.MaxValue"/> (repeats can grow an array).</exception>
    /// <exception cref="NotSupportedException">A type without a native layout here (list views, unions, run-end encoded).</exception>
    public static IArrowArray Take(IArrowArray array, ReadOnlySpan<int> rows, bool? shareViewData = null)
    {
        ArgumentNullException.ThrowIfNull(array);
        bool nullRows = Check(rows, array.Length);
        using NativeArray<int>? scratch = nullRows ? new() : null;
        return TakeArray(array.Data, rows, Safe(rows, scratch), nullRows, shareViewData);
    }

    /// <summary>
    /// Every column of <paramref name="batch"/> taken by the same <paramref name="rows"/> (see
    /// <see cref="Take(IArrowArray, ReadOnlySpan{int}, bool?)"/>). With <paramref name="maxDegreeOfParallelism"/> above
    /// 1 the columns are taken side by side, each into memory of its own; else all into one batch's memory.
    /// </summary>
    public static RecordBatch Take(RecordBatch batch, ReadOnlySpan<int> rows, bool? shareViewData = null, int maxDegreeOfParallelism = 1)
    {
        ArgumentNullException.ThrowIfNull(batch);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxDegreeOfParallelism, 1);
        bool nullRows = Check(rows, batch.Length);
        using NativeArray<int>? scratch = nullRows ? new() : null;
        ReadOnlySpan<int> safe = Safe(rows, scratch);
        int columns = batch.ColumnCount;
        if (maxDegreeOfParallelism == 1 || columns < 2)
        {
            var export = new NativeBatchExport();
            try
            {
                var children = new nint[columns];
                for (int c = 0; c < columns; c++) children[c] = (nint)Build(export, batch.Column(c).Data, rows, safe, nullRows, shareViewData);
                return export.Import(export.NewArray(rows.Length, 0, [], children), batch.Schema);
            }
            catch
            {
                export.FreeAll();
                throw;
            }
        }

        var taken = new IArrowArray?[columns];
        int count = rows.Length;
        fixed (int* r = rows, s = safe)
        {
            nint rowsAt = (nint)r, safeAt = (nint)s;
            try
            {
                Parallel.For(0, columns, new ParallelOptions { MaxDegreeOfParallelism = maxDegreeOfParallelism }, c =>
                    taken[c] = TakeArray(batch.Column(c).Data, new ReadOnlySpan<int>((int*)rowsAt, count), new ReadOnlySpan<int>((int*)safeAt, count),
                        nullRows, shareViewData));
            }
            catch (Exception e)
            {
                foreach (IArrowArray? array in taken) array?.Dispose();
                // As the serial path throws: the first column's failure, not Parallel's wrapper.
                if (e is AggregateException { InnerExceptions: [Exception first, ..] }) ExceptionDispatchInfo.Throw(first);
                throw;
            }
        }
        return new RecordBatch(batch.Schema, taken!, count);
    }

    /// <summary>Checks the indices against the array's length; returns whether any takes a null row.</summary>
    private static bool Check(ReadOnlySpan<int> rows, int length)
    {
        if (rows.IsEmpty) return false;
        MinMax.Integers(rows, out int min, out int max);
        if (min < NullRow || max >= length)
            throw new ArgumentOutOfRangeException(nameof(rows), $"Row indices run from {min} to {max}; the array has {length} rows ({NullRow} takes a null row).");
        return min == NullRow;
    }

    /// <summary>
    /// The indices with null rows taking row 0 (for the gathers of values, which the validity then hides), in
    /// <paramref name="scratch"/>; the indices themselves without one (no null rows).
    /// </summary>
    private static ReadOnlySpan<int> Safe(ReadOnlySpan<int> rows, NativeArray<int>? scratch)
    {
        if (scratch is null) return rows;
        scratch.Reserve(rows.Length);
        Span<int> safe = scratch.AsSpan(0, rows.Length);
        for (int i = 0; i < rows.Length; i++) safe[i] = Math.Max(rows[i], 0);
        return safe;
    }

    private static IArrowArray TakeArray(ArrayData data, ReadOnlySpan<int> rows, ReadOnlySpan<int> safe, bool nullRows, bool? shareViewData)
    {
        if (data.DataType is NullType) return new NullArray(rows.Length);
        var export = new NativeBatchExport();
        try
        {
            return export.ImportArray(Build(export, data, rows, safe, nullRows, shareViewData), data.DataType);
        }
        catch
        {
            export.FreeAll();
            throw;
        }
    }

    /// <param name="rows">The indices (<see cref="NullRow"/> included when <paramref name="nullRows"/>).</param>
    /// <param name="safe">The same indices without null rows (taking row 0 instead).</param>
    private static NativeCArray* Build(NativeBatchExport export, ArrayData data, ReadOnlySpan<int> rows, ReadOnlySpan<int> safe, bool nullRows, bool? shareViewData)
    {
        int count = rows.Length, length = data.Length;
        if (data.DataType is NullType) return export.NewArray(count, count, [], []);
        // No rows to take values from: every index is a null row.
        if (length == 0 && count > 0) return Nulls(export, data, count);

        nint validity = 0;
        long nulls = 0;
        bool bitmap = data.NullCount != 0 && !data.Buffers[0].IsEmpty;
        if (bitmap || nullRows)
        {
            var bits = (ulong*)export.AllocateUninitialized(Masks.Words(count) * 8L);
            int set = bitmap ? GatherBits(data.Buffers[0].Span, data.Offset, length, rows, nullRows, bits) : NotNull(rows, bits);
            nulls = count - set;
            if (nulls > 0) validity = (nint)bits;
        }

        switch (data.DataType)
        {
            case DictionaryType d:
            {
                if (d.IndexType is not IntegerType index) throw new NotSupportedException($"Dictionary indices of type {d.IndexType.Name}.");
                nint indices = GatherFixed(export, data.Buffers[1].Span, data.Offset, length, index.BitWidth / 8, safe);
                return export.NewArray(count, nulls, [validity, indices], [], ArrowFilter.Dictionary(export, data.Dictionary!));
            }
            case BooleanType:
            {
                var bits = (ulong*)export.AllocateUninitialized(Masks.Words(count) * 8L);
                GatherBits(data.Buffers[1].Span, data.Offset, length, safe, nullRows: false, bits);
                return export.NewArray(count, nulls, [validity, (nint)bits], []);
            }
            case FixedSizeBinaryType f when f.ByteWidth is not (1 or 2 or 4 or 8 or 16 or 32):
                return export.NewArray(count, nulls, [validity, GatherBytes(export, data.Buffers[1].Span, data.Offset, length, f.ByteWidth, safe)], []);
            case FixedSizeBinaryType f:
                return export.NewArray(count, nulls, [validity, GatherFixed(export, data.Buffers[1].Span, data.Offset, length, f.ByteWidth, safe)], []);
            case FixedWidthType f:
                return export.NewArray(count, nulls, [validity, GatherFixed(export, data.Buffers[1].Span, data.Offset, length, f.BitWidth / 8, safe)], []);
            case StringType or BinaryType:
                return Offsets<int>(export, data, rows, validity, nulls);
            case LargeStringType or LargeBinaryType:
                return Offsets<long>(export, data, rows, validity, nulls);
            case StringViewType or BinaryViewType:
            {
                var views = (Int128*)GatherFixed(export, data.Buffers[1].Span, data.Offset, length, 16, safe);
                bool share = shareViewData ?? count >= length * ArrowFilter.ShareViewDataFrom;
                return ArrowFilter.ViewData(export, data, views, count, validity, nulls, share);
            }
            case StructType s:
            {
                var children = new nint[s.Fields.Count];
                for (int c = 0; c < children.Length; c++)
                    children[c] = (nint)Build(export, data.Children[c].Slice(data.Offset, length), rows, safe, nullRows, shareViewData);
                return export.NewArray(count, nulls, [validity], children);
            }
            case ListType or MapType:
                return Lists<int>(export, data, rows, validity, nulls, shareViewData);
            case LargeListType:
                return Lists<long>(export, data, rows, validity, nulls, shareViewData);
            case FixedSizeListType f:
                return FixedSizeLists(export, data, f.ListSize, rows, nullRows, validity, nulls, shareViewData);
            default:
                throw new NotSupportedException($"Taking rows of {data.DataType.Name} arrays is not supported.");
        }
    }

    /// <summary>Lists, maps and large lists: the elements of the rows taken (by an index list of their own), offsets rebuilt from 0.</summary>
    private static NativeCArray* Lists<TOffset>(NativeBatchExport export, ArrayData data, ReadOnlySpan<int> rows, nint validity, long nulls, bool? shareViewData)
        where TOffset : unmanaged, IBinaryInteger<TOffset>
    {
        int count = rows.Length, length = data.Length;
        ReadOnlySpan<TOffset> offsets = data.Buffers[1].Span.CastTo<TOffset>().Slice(data.Offset, length + 1);
        int first = int.CreateChecked(offsets[0]), span = int.CreateChecked(offsets[length]) - first;
        long total = 0;
        foreach (int r in rows)
        {
            if (r >= 0) total += long.CreateTruncating(offsets[r + 1] - offsets[r]);
        }
        if (total > int.MaxValue) throw new OverflowException($"The rows taken hold {total} list elements, more than an array takes.");

        var targetOffsets = (TOffset*)export.AllocateUninitialized((count + 1L) * sizeof(TOffset));
        using var scratch = new NativeArray<int>((int)total);
        Span<int> elements = scratch.Span[..(int)total];
        int at = 0;
        targetOffsets[0] = TOffset.Zero;
        for (int i = 0; i < count; i++)
        {
            int r = rows[i];
            if (r >= 0)
            {
                int begin = int.CreateTruncating(offsets[r]) - first, end = int.CreateTruncating(offsets[r + 1]) - first;
                for (int e = begin; e < end; e++) elements[at++] = e;
            }
            targetOffsets[i + 1] = TOffset.CreateTruncating(at);
        }
        NativeCArray* child = Build(export, data.Children[0].Slice(first, span), elements, elements, nullRows: false, shareViewData);
        return export.NewArray(count, nulls, [validity, (nint)targetOffsets], [(nint)child]);
    }

    /// <summary>Fixed-size lists: each row taken takes its ListSize elements (null rows null elements).</summary>
    private static NativeCArray* FixedSizeLists(NativeBatchExport export, ArrayData data, int size, ReadOnlySpan<int> rows, bool nullRows, nint validity,
        long nulls, bool? shareViewData)
    {
        int count = rows.Length;
        long total = (long)count * size;
        if (total > int.MaxValue) throw new OverflowException($"The rows taken hold {total} list elements, more than an array takes.");
        using var scratch = new NativeArray<int>((int)total);
        using NativeArray<int>? safeScratch = nullRows ? new() : null;
        Span<int> elements = scratch.Span[..(int)total];
        for (int i = 0; i < count; i++)
        {
            int r = rows[i];
            Span<int> row = elements.Slice(i * size, size);
            if (r < 0) row.Fill(NullRow);
            else
            {
                for (int j = 0; j < size; j++) row[j] = r * size + j;
            }
        }
        ArrayData child = data.Children[0].Slice(data.Offset * size, data.Length * size);
        NativeCArray* taken = Build(export, child, elements, Safe(elements, safeScratch), nullRows, shareViewData);
        return export.NewArray(count, nulls, [validity], [(nint)taken]);
    }

    /// <summary>Strings and binaries (int or long offsets): the values of the rows taken, null rows empty.</summary>
    private static NativeCArray* Offsets<TOffset>(NativeBatchExport export, ArrayData data, ReadOnlySpan<int> rows, nint validity, long nulls)
        where TOffset : unmanaged, IBinaryInteger<TOffset>
    {
        int count = rows.Length;
        ReadOnlySpan<TOffset> offsets = data.Buffers[1].Span.CastTo<TOffset>().Slice(data.Offset, data.Length + 1);
        ReadOnlySpan<byte> bytes = data.Buffers[2].Span;
        long total = 0;
        foreach (int r in rows)
        {
            if (r >= 0) total += long.CreateTruncating(offsets[r + 1] - offsets[r]);
        }
        if (total > int.MaxValue) throw new OverflowException($"The rows taken hold {total} bytes, more than a buffer takes.");

        var targetOffsets = (TOffset*)export.AllocateUninitialized((count + 1L) * sizeof(TOffset));
        byte* targetBytes = (byte*)export.AllocateUninitialized(total);
        long at = 0;
        targetOffsets[0] = TOffset.Zero;
        for (int i = 0; i < count; i++)
        {
            int r = rows[i];
            if (r >= 0)
            {
                int begin = int.CreateTruncating(offsets[r]), size = int.CreateTruncating(offsets[r + 1]) - begin;
                ShortCopy.Copy(bytes.Slice(begin, size), new Span<byte>(targetBytes + at, size));
                at += size;
            }
            targetOffsets[i + 1] = TOffset.CreateTruncating(at);
        }
        return export.NewArray(count, nulls, [validity, (nint)targetOffsets, (nint)targetBytes], []);
    }

    /// <summary>An array of <paramref name="count"/> nulls of <paramref name="data"/>'s type (taken from an empty array: all null rows).</summary>
    private static NativeCArray* Nulls(NativeBatchExport export, ArrayData data, int count)
    {
        nint validity = (nint)export.Allocate(Masks.Words(count) * 8L);
        nint Zeros(long bytes) => (nint)export.Allocate(bytes);
        return data.DataType switch
        {
            NullType => export.NewArray(count, count, [], []),
            DictionaryType { IndexType: IntegerType index } => export.NewArray(count, count, [validity, Zeros((long)count * (index.BitWidth / 8))], [],
                ArrowFilter.Dictionary(export, data.Dictionary!)),
            BooleanType => export.NewArray(count, count, [validity, Zeros(Masks.Words(count) * 8L)], []),
            FixedSizeBinaryType f => export.NewArray(count, count, [validity, Zeros((long)count * f.ByteWidth)], []),
            FixedWidthType f => export.NewArray(count, count, [validity, Zeros((long)count * (f.BitWidth / 8))], []),
            StringType or BinaryType => export.NewArray(count, count, [validity, Zeros((count + 1L) * sizeof(int)), Zeros(0)], []),
            LargeStringType or LargeBinaryType => export.NewArray(count, count, [validity, Zeros((count + 1L) * sizeof(long)), Zeros(0)], []),
            StringViewType or BinaryViewType => export.NewArray(count, count, [validity, Zeros(16L * count)], []),
            StructType s => export.NewArray(count, count, [validity], [.. Enumerable.Range(0, s.Fields.Count).Select(c => (nint)Nulls(export, data.Children[c], count))]),
            ListType or MapType => export.NewArray(count, count, [validity, Zeros((count + 1L) * sizeof(int))], [(nint)Build(export, data.Children[0], [], [], false, null)]),
            LargeListType => export.NewArray(count, count, [validity, Zeros((count + 1L) * sizeof(long))], [(nint)Build(export, data.Children[0], [], [], false, null)]),
            FixedSizeListType f => export.NewArray(count, count, [validity], [(nint)Nulls(export, data.Children[0], checked(count * f.ListSize))]),
            _ => throw new NotSupportedException($"Taking rows of {data.DataType.Name} arrays is not supported."),
        };
    }

    /// <summary>
    /// Gathers the bits of a bitmap (from <paramref name="firstBit"/>) at <paramref name="rows"/> into
    /// <paramref name="target"/> (null rows clear when <paramref name="nullRows"/>); returns how many are set.
    /// </summary>
    private static int GatherBits(ReadOnlySpan<byte> bitmap, long firstBit, int length, ReadOnlySpan<int> rows, bool nullRows, ulong* target)
    {
        if ((firstBit + length + 7) >> 3 > bitmap.Length) throw new ArgumentException($"A bitmap of {bitmap.Length} bytes for {length} rows from bit {firstBit}.");
        ref byte bytes = ref MemoryMarshal.GetReference(bitmap);
        int set = 0;
        for (int start = 0; start < rows.Length; start += 64)
        {
            ReadOnlySpan<int> chunk = rows.Slice(start, Math.Min(64, rows.Length - start));
            ulong word = 0;
            if (nullRows)
            {
                for (int k = 0; k < chunk.Length; k++)
                {
                    int r = chunk[k];
                    if (r >= 0) word |= Bit(ref bytes, firstBit + r) << k;
                }
            }
            else
            {
                for (int k = 0; k < chunk.Length; k++) word |= Bit(ref bytes, firstBit + chunk[k]) << k;
            }
            target[start >> 6] = word;
            set += BitOperations.PopCount(word);
        }
        return set;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        static ulong Bit(ref byte bytes, long bit) => (ulong)(Unsafe.Add(ref bytes, (nint)(bit >> 3)) >> (int)(bit & 7)) & 1;
    }

    /// <summary>Validity of rows without nulls of their own: set but for null rows; returns how many are set.</summary>
    private static int NotNull(ReadOnlySpan<int> rows, ulong* target)
    {
        int set = 0;
        for (int start = 0; start < rows.Length; start += 64)
        {
            ReadOnlySpan<int> chunk = rows.Slice(start, Math.Min(64, rows.Length - start));
            ulong word = 0;
            for (int k = 0; k < chunk.Length; k++) word |= (ulong)(~chunk[k] >>> 31) << k;
            target[start >> 6] = word;
            set += BitOperations.PopCount(word);
        }
        return set;
    }

    /// <summary>Values <paramref name="width"/> bytes wide (rows offset .. offset + length of the buffer) at <paramref name="rows"/>.</summary>
    private static nint GatherFixed(NativeBatchExport export, ReadOnlySpan<byte> buffer, int offset, int length, int width, ReadOnlySpan<int> rows)
    {
        void* values = export.AllocateUninitialized((long)rows.Length * width);
        switch (width)
        {
            case 1: Gather(buffer.CastTo<byte>().Slice(offset, length), rows, (byte*)values); break;
            case 2: Gather(buffer.CastTo<ushort>().Slice(offset, length), rows, (ushort*)values); break;
            case 4: Gather(buffer.CastTo<uint>().Slice(offset, length), rows, (uint*)values); break;
            case 8: Gather(buffer.CastTo<ulong>().Slice(offset, length), rows, (ulong*)values); break;
            case 16: Gather(buffer.CastTo<Int128>().Slice(offset, length), rows, (Int128*)values); break;
            case 32: Gather(buffer.CastTo<Int256Bits>().Slice(offset, length), rows, (Int256Bits*)values); break;
            default: throw new NotSupportedException($"Values {width} bytes wide.");
        }
        return (nint)values;
    }

    /// <summary>Copies the values at <paramref name="rows"/> (checked before) to <paramref name="target"/>, four loads in flight.</summary>
    private static void Gather<T>(ReadOnlySpan<T> source, ReadOnlySpan<int> rows, T* target) where T : unmanaged
    {
        ref T from = ref MemoryMarshal.GetReference(source);
        ref int row = ref MemoryMarshal.GetReference(rows);
        int n = rows.Length, i = 0;
        for (; i <= n - 4; i += 4)
        {
            T a = Unsafe.Add(ref from, Unsafe.Add(ref row, i));
            T b = Unsafe.Add(ref from, Unsafe.Add(ref row, i + 1));
            T c = Unsafe.Add(ref from, Unsafe.Add(ref row, i + 2));
            T d = Unsafe.Add(ref from, Unsafe.Add(ref row, i + 3));
            target[i] = a;
            target[i + 1] = b;
            target[i + 2] = c;
            target[i + 3] = d;
        }
        for (; i < n; i++) target[i] = Unsafe.Add(ref from, Unsafe.Add(ref row, i));
    }

    /// <summary>Fixed-size binaries of an odd width: each value copied.</summary>
    private static nint GatherBytes(NativeBatchExport export, ReadOnlySpan<byte> buffer, int offset, int length, int width, ReadOnlySpan<int> rows)
    {
        byte* values = (byte*)export.AllocateUninitialized((long)rows.Length * width);
        ref byte from = ref MemoryMarshal.GetReference(buffer.Slice(checked(offset * width), checked(length * width)));
        for (int i = 0; i < rows.Length; i++)
            Unsafe.CopyBlockUnaligned(ref values[(nint)i * width], ref Unsafe.Add(ref from, (nint)rows[i] * width), (uint)width);
        return (nint)values;
    }
}
