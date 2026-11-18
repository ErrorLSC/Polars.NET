using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Kaminari.Sazanami.Kernels;
using Kaminari.Sazanami.Arrow.Interop;

namespace Kaminari.Sazanami.Ordering;

/// <summary>
/// Rows put in group order by a counting sort: one counting pass over the group ids, a prefix sum, one stable scatter
/// of row numbers. O(n + groups), no comparisons; within a group the rows keep their order. The caller owns every
/// buffer (native scratch reused batch after batch): nothing is allocated here.
/// </summary>
/// <remarks>
/// The permutation feeds <see cref="Arrow.ArrowTake"/>, which gathers the rows into group order; the starts cut the
/// result into groups. TODO: a parallel version for whole-batch inputs (radix-partitioned joins and aggregations,
/// exchange): counts per segment on each thread, merged into per-segment cursors, then the segments scattered side by
/// side. Not needed while callers work in chunks of about a row group (the managed sink: well under a millisecond each).
/// </remarks>
internal static class CountingSort
{
    /// <summary>
    /// The rows of <paramref name="groups"/> in group order: <paramref name="permutation"/> receives row numbers
    /// (group 0's rows first, each group's in their order), <paramref name="starts"/> where each group starts (its
    /// length is the group count + 1; the last entry is the row count). Returns whether the rows were in group order
    /// already (ids never going down): the permutation is then the identity, and a caller may skip the gather.
    /// </summary>
    /// <param name="groups">A group id per row: 0 ≤ id &lt; starts.Length − 1 (checked once, up front).</param>
    /// <exception cref="ArgumentOutOfRangeException">An id outside the groups.</exception>
    /// <exception cref="ArgumentException">The permutation is shorter than the rows, or the starts are empty.</exception>
    public static bool Permutation(ReadOnlySpan<int> groups, Span<int> starts, Span<int> permutation)
    {
        if (starts.IsEmpty) throw new ArgumentException("The starts take the group count + 1 entries.", nameof(starts));
        if (permutation.Length < groups.Length)
            throw new ArgumentException($"A permutation of {permutation.Length} entries for {groups.Length} rows.", nameof(permutation));
        int count = starts.Length - 1;
        starts.Clear();
        if (groups.IsEmpty) return true;
        MinMax.Integers(groups, out int min, out int max);
        if (min < 0 || max >= count)
            throw new ArgumentOutOfRangeException(nameof(groups), $"Group ids run from {min} to {max}; there are {count} groups.");

        ref int id = ref MemoryMarshal.GetReference(groups);
        ref int at = ref MemoryMarshal.GetReference(starts);
        ref int row = ref MemoryMarshal.GetReference(permutation);
        int n = groups.Length;

        // Each group's count in the entry after its own; whether the ids never go down.
        int previous = 0;
        bool ordered = true;
        for (int i = 0; i < n; i++)
        {
            int g = Unsafe.Add(ref id, i);
            Unsafe.Add(ref at, g + 1)++;
            ordered &= g >= previous;
            previous = g;
        }
        for (int g = 1; g <= count; g++) Unsafe.Add(ref at, g) += Unsafe.Add(ref at, g - 1);
        if (ordered)
        {
            for (int i = 0; i < n; i++) Unsafe.Add(ref row, i) = i;
            return true;
        }

        // The starts serve as cursors (each ends at the next group's start), then move back one place.
        for (int i = 0; i < n; i++) Unsafe.Add(ref row, Unsafe.Add(ref at, Unsafe.Add(ref id, i))++) = i;
        for (int g = count; g > 0; g--) Unsafe.Add(ref at, g) = Unsafe.Add(ref at, g - 1);
        at = 0;
        return false;
    }
}
