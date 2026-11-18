using System.Buffers.Binary;
using System.Numerics;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Runtime.Intrinsics;
using System.Text.Unicode;
using Kaminari.Sazanami.Kernels;

namespace Kaminari.Sazanami.Arrow.Ipc;

/// <summary>
/// The structural checks an IPC reader runs before it hands out arrays, each O(n) and vectorized where the data
/// allows: offsets non-decreasing and within their data, views and list views within their buffers, dictionary
/// indices and dense union offsets within their targets, run ends increasing. Values themselves (decimal precision,
/// date64 whole days) are not checked; UTF-8 only when asked.
/// </summary>
internal static class IpcValidation
{
    /// <summary>Offsets of <paramref name="length"/> + 1 entries: from 0 or more, non-decreasing, the last within <paramref name="limit"/>.</summary>
    public static void Offsets<T>(ReadOnlySpan<T> offsets, long limit, string where) where T : unmanaged, IBinaryInteger<T>
    {
        if (offsets.IsEmpty) return;
        if (T.IsNegative(offsets[0])) throw Invalid(where, $"its first offset is {offsets[0]}");
        if (!NonDecreasing(offsets)) throw Invalid(where, "its offsets decrease");
        if (long.CreateSaturating(offsets[^1]) > limit) throw Invalid(where, $"its last offset {offsets[^1]} is past its data ({limit})");
    }

    /// <summary>Whether each value is at least the one before it.</summary>
    public static bool NonDecreasing<T>(ReadOnlySpan<T> values) where T : unmanaged, IBinaryInteger<T>
    {
        int i = 0;
        ref T r = ref MemoryMarshal.GetReference(values);
        if (Vector256.IsHardwareAccelerated && Vector256<T>.IsSupported)
        {
            for (; i + Vector256<T>.Count < values.Length; i += Vector256<T>.Count)
            {
                if (Vector256.LessThanAny(Vector256.LoadUnsafe(ref r, (nuint)i + 1), Vector256.LoadUnsafe(ref r, (nuint)i))) return false;
            }
        }
        else if (Vector128.IsHardwareAccelerated && Vector128<T>.IsSupported)
        {
            for (; i + Vector128<T>.Count < values.Length; i += Vector128<T>.Count)
            {
                if (Vector128.LessThanAny(Vector128.LoadUnsafe(ref r, (nuint)i + 1), Vector128.LoadUnsafe(ref r, (nuint)i))) return false;
            }
        }
        for (; i + 1 < values.Length; i++)
        {
            if (values[i + 1] < values[i]) return false;
        }
        return true;
    }

    /// <summary>Strictly increasing values from 1 up (run ends).</summary>
    public static void RunEnds<T>(ReadOnlySpan<T> ends, long length, string where) where T : unmanaged, IBinaryInteger<T>
    {
        if (ends.IsEmpty)
        {
            if (length > 0) throw Invalid(where, $"it has no runs for {length} rows");
            return;
        }
        if (ends[0] <= T.Zero) throw Invalid(where, $"its first run ends at {ends[0]}");
        for (int i = 1; i < ends.Length; i++)
        {
            if (ends[i] <= ends[i - 1]) throw Invalid(where, "its run ends do not increase");
        }
        if (long.CreateSaturating(ends[^1]) < length) throw Invalid(where, $"its runs end at {ends[^1]}, before its {length} rows");
    }

    /// <summary>Indices of the valid slots (all when <paramref name="validity"/> is empty) within [0, <paramref name="count"/>).</summary>
    public static void Indices<T>(ReadOnlySpan<T> indices, ReadOnlySpan<byte> validity, long count, string where) where T : unmanaged, IBinaryInteger<T>
    {
        if (indices.IsEmpty) return;
        if (validity.IsEmpty)
        {
            MinMax.Integers(indices, out T min, out T max);
            if (T.IsNegative(min) || long.CreateSaturating(max) >= count)
                throw Invalid(where, $"an index ({(T.IsNegative(min) ? min : max)}) is outside its {count} values");
            return;
        }
        for (int i = 0; i < indices.Length; i++)
        {
            if (!Bits.Get(validity, i)) continue;
            T v = indices[i];
            if (T.IsNegative(v) || long.CreateSaturating(v) >= count) throw Invalid(where, $"index {v} at {i} is outside its {count} values");
        }
    }

    /// <summary>The views of the valid slots: lengths, buffer indexes and offsets within the data buffers, inline prefixes matching.</summary>
    public static void Views(ReadOnlySpan<byte> views, int length, ReadOnlySpan<byte> validity, IpcSlice[] data, string where)
    {
        for (int i = 0; i < length; i++)
        {
            if (!validity.IsEmpty && !Bits.Get(validity, i)) continue;
            ReadOnlySpan<byte> view = views.Slice(i * 16, 16);
            int size = BinaryPrimitives.ReadInt32LittleEndian(view);
            if (size < 0) throw Invalid(where, $"view {i} has length {size}");
            if (size <= 12) continue;
            int buffer = BinaryPrimitives.ReadInt32LittleEndian(view[8..]), offset = BinaryPrimitives.ReadInt32LittleEndian(view[12..]);
            if ((uint)buffer >= (uint)data.Length) throw Invalid(where, $"view {i} refers to data buffer {buffer} of {data.Length}");
            if (offset < 0 || offset > data[buffer].Length - size) throw Invalid(where, $"view {i} reaches past data buffer {buffer}");
            if (!view.Slice(4, 4).SequenceEqual(data[buffer].Span.Slice(offset, 4))) throw Invalid(where, $"view {i}'s prefix differs from its data");
        }
    }

    /// <summary>The list views of the valid slots: offsets and sizes from 0 up, within the child (an empty one too).</summary>
    public static void ListViews<T>(ReadOnlySpan<T> offsets, ReadOnlySpan<T> sizes, ReadOnlySpan<byte> validity, long child, string where)
        where T : unmanaged, IBinaryInteger<T>
    {
        for (int i = 0; i < offsets.Length; i++)
        {
            if (!validity.IsEmpty && !Bits.Get(validity, i)) continue;
            long offset = long.CreateSaturating(offsets[i]), size = long.CreateSaturating(sizes[i]);
            if (size < 0) throw Invalid(where, $"list view {i} has size {size}");
            if (offset < 0 || offset > child - size) throw Invalid(where, $"list view {i} ({offset}, {size}) reaches past its {child} items");
        }
    }

    /// <summary>The type ids of a union, each one it declares; for a dense union, offsets within each child.</summary>
    public static void Union(ReadOnlySpan<byte> typeIds, ReadOnlySpan<int> offsets, int[] declared, int[] childLengths, bool dense, string where)
    {
        Span<sbyte> child = stackalloc sbyte[128];
        child.Fill(-1);
        for (int k = 0; k < declared.Length; k++) child[declared[k]] = (sbyte)k;
        for (int i = 0; i < typeIds.Length; i++)
        {
            byte id = typeIds[i];
            int k = id < 128 ? child[id] : -1;
            if (k < 0) throw Invalid(where, $"slot {i} has type id {(sbyte)id}, which the union does not declare");
            if (dense && (uint)offsets[i] >= (uint)childLengths[k]) throw Invalid(where, $"slot {i} has offset {offsets[i]} in a child of {childLengths[k]} values");
        }
    }

    /// <summary>The strings of the valid slots are UTF-8.</summary>
    public static void Utf8Strings<T>(ReadOnlySpan<T> offsets, ReadOnlySpan<byte> data, ReadOnlySpan<byte> validity, string where)
        where T : unmanaged, IBinaryInteger<T>
    {
        for (int i = 0; i + 1 < offsets.Length; i++)
        {
            if (!validity.IsEmpty && !Bits.Get(validity, i)) continue;
            int start = int.CreateTruncating(offsets[i]), end = int.CreateTruncating(offsets[i + 1]);
            if (!Utf8.IsValid(data[start..end])) throw Invalid(where, $"string {i} is not UTF-8");
        }
    }

    /// <summary>The string views of the valid slots are UTF-8.</summary>
    public static void Utf8Views(ReadOnlySpan<byte> views, int length, ReadOnlySpan<byte> validity, IpcSlice[] data, string where)
    {
        for (int i = 0; i < length; i++)
        {
            if (!validity.IsEmpty && !Bits.Get(validity, i)) continue;
            ReadOnlySpan<byte> view = views.Slice(i * 16, 16);
            int size = BinaryPrimitives.ReadInt32LittleEndian(view);
            ReadOnlySpan<byte> text = size <= 12 ? view.Slice(4, size)
                : data[BinaryPrimitives.ReadInt32LittleEndian(view[8..])].Span.Slice(BinaryPrimitives.ReadInt32LittleEndian(view[12..]), size);
            if (!Utf8.IsValid(text)) throw Invalid(where, $"string {i} is not UTF-8");
        }
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    public static InvalidDataException Invalid(string where, string what) => new($"Arrow IPC data is invalid: {where}: {what}.");
}
