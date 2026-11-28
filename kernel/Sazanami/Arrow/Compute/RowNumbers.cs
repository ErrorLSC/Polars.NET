using System.Numerics;
using System.Runtime.InteropServices;
using Apache.Arrow;
using Apache.Arrow.Types;
using Kaminari.Sazanami.Arrow.Builders;

namespace Kaminari.Sazanami.Arrow.Compute;

/// <summary>
/// Row index columns: Int64 arrays numbering rows (in native memory of their own, freed when disposed), for readers that
/// put a row's number in the data before any filter beside the rows they deliver.
/// </summary>
internal static class RowNumbers
{
    /// <summary>The numbers <paramref name="start"/>, <paramref name="start"/> + 1, … of <paramref name="length"/> rows.</summary>
    public static IArrowArray Range(long start, int length)
    {
        using var builder = new NativeFixedWidthBuilder<long>(Int64Type.Default);
        Span<long> numbers = builder.AppendSlots(length);
        for (int i = 0; i < numbers.Length; i++) numbers[i] = start + i;
        return builder.ExportArray();
    }

    /// <summary>
    /// The numbers <paramref name="start"/> + <c>i</c> of the rows <c>i</c> whose bits are set in <paramref name="kept"/>
    /// (<see cref="Filters.Masks"/> layout), the first <paramref name="length"/> of them.
    /// </summary>
    public static IArrowArray Selected(long start, ReadOnlySpan<ulong> kept, int length)
    {
        using var builder = new NativeFixedWidthBuilder<long>(Int64Type.Default);
        Span<long> numbers = builder.AppendSlots(length);
        int at = 0;
        for (int w = 0; w < kept.Length && at < length; w++)
        {
            for (ulong word = kept[w]; word != 0 && at < length; word &= word - 1)
                numbers[at++] = start + 64L * w + BitOperations.TrailingZeroCount(word);
        }
        if (at < length) throw new ArgumentException($"The mask selects {at} rows, fewer than {length}.", nameof(kept));
        return builder.ExportArray();
    }

    /// <summary>
    /// The numbers of the rows of runs of consecutive rows (<c>Start</c> from <paramref name="first"/>), in order,
    /// <paramref name="length"/> of them.
    /// </summary>
    public static IArrowArray Runs(ReadOnlySpan<(long Start, int Count)> runs, long first, int length)
    {
        using var builder = new NativeFixedWidthBuilder<long>(Int64Type.Default);
        Span<long> numbers = builder.AppendSlots(length);
        int at = 0;
        foreach (var (start, count) in runs)
        {
            for (int i = 0; i < count; i++) numbers[at++] = first + start + i;
        }
        return builder.ExportArray();
    }

    /// <summary>A copy of <paramref name="numbers"/> (in native memory of its own), <paramref name="by"/> added to each.</summary>
    public static IArrowArray Shifted(IArrowArray numbers, long by)
    {
        var array = (Int64Array)numbers;
        using var builder = new NativeFixedWidthBuilder<long>(Int64Type.Default);
        Span<long> values = builder.AppendSlots(array.Length);
        array.Values.CopyTo(values);
        IArrowArray copy = builder.ExportArray();
        Shift(copy, by);
        return copy;
    }

    /// <summary>
    /// Adds <paramref name="by"/> to every number of <paramref name="numbers"/> in place: an array made here (or a
    /// filtered copy of one) that its caller alone holds; numbers known relative to a start learned later.
    /// </summary>
    public static void Shift(IArrowArray numbers, long by)
    {
        if (by == 0) return;
        var array = (Int64Array)numbers;
        Span<long> values = MemoryMarshal.Cast<byte, long>(MemoryMarshal.AsMemory(array.ValueBuffer.Memory).Span).Slice(array.Offset, array.Length);
        int i = 0;
        if (Vector.IsHardwareAccelerated && values.Length >= Vector<long>.Count)
        {
            var add = new Vector<long>(by);
            Span<Vector<long>> vectors = MemoryMarshal.Cast<long, Vector<long>>(values);
            for (int v = 0; v < vectors.Length; v++) vectors[v] += add;
            i = vectors.Length * Vector<long>.Count;
        }
        for (; i < values.Length; i++) values[i] += by;
    }
}
