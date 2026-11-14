using System.Buffers.Binary;
using System.Collections;
using System.Numerics;

namespace Kaminari.Sazanami.Rows;

/// <summary>
/// A set of row numbers as sorted, disjoint, non-adjacent ranges (half-open: a range covers rows
/// <c>[Start, Start + Count)</c>). Immutable; the set operations merge in one pass over both operands. Readers take it
/// to select rows (a deletion vector is <c>FromIndices(deleted).Complement(rowCount)</c>), pruning produces it.
/// </summary>
public sealed class RowRanges : IReadOnlyList<(long Start, long Count)>, IEquatable<RowRanges>
{
    private readonly long[] _starts;
    private readonly long[] _ends;

    private RowRanges(long[] starts, long[] ends)
    {
        _starts = starts;
        _ends = ends;
        long rows = 0;
        for (int i = 0; i < starts.Length; i++) rows += ends[i] - starts[i];
        RowCount = rows;
    }

    /// <summary>No rows.</summary>
    public static RowRanges Empty { get; } = new([], []);

    /// <summary>Rows <c>[0, <paramref name="count"/>)</c>.</summary>
    public static RowRanges All(long count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        return count == 0 ? Empty : new([0], [count]);
    }

    /// <summary>The union of ranges given in any order (they may overlap or touch; empty ones are dropped).</summary>
    /// <exception cref="ArgumentOutOfRangeException">A range starts below 0, has a negative count, or ends past <see cref="long.MaxValue"/>.</exception>
    public static RowRanges Of(ReadOnlySpan<(long Start, long Count)> ranges)
    {
        var sorted = new (long Start, long End)[ranges.Length];
        int n = 0;
        foreach (var (start, count) in ranges)
        {
            if (start < 0 || count < 0 || start > long.MaxValue - count)
                throw new ArgumentOutOfRangeException(nameof(ranges), $"[{start}, +{count}) is not a range of row numbers.");
            if (count > 0) sorted[n++] = (start, start + count);
        }
        Array.Sort(sorted, 0, n);
        return Merge(sorted.AsSpan(0, n));
    }

    /// <summary>The rows at <paramref name="indices"/> (any order, repeats allowed).</summary>
    /// <exception cref="ArgumentOutOfRangeException">An index is negative.</exception>
    public static RowRanges FromIndices(ReadOnlySpan<long> indices)
    {
        long[] sorted = indices.ToArray();
        Array.Sort(sorted);
        if (sorted.Length > 0 && sorted[0] < 0) throw new ArgumentOutOfRangeException(nameof(indices), sorted[0], "Row numbers are not negative.");
        List<long> starts = [], ends = [];
        foreach (long row in sorted)
        {
            if (ends.Count > 0 && row <= ends[^1])
            {
                if (row == ends[^1]) ends[^1] = row + 1;
                continue;
            }
            starts.Add(row);
            ends.Add(row + 1);
        }
        return starts.Count == 0 ? Empty : new([.. starts], [.. ends]);
    }

    /// <summary>
    /// The rows whose bits are set in an LSB-first bitmap of <paramref name="count"/> bits (Arrow's layout), numbered
    /// from <paramref name="firstRow"/>.
    /// </summary>
    /// <exception cref="ArgumentException">The bitmap holds fewer than <paramref name="count"/> bits.</exception>
    public static RowRanges FromBits(ReadOnlySpan<byte> bitmap, long count, long firstRow = 0)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        ArgumentOutOfRangeException.ThrowIfNegative(firstRow);
        if (bitmap.Length < (count + 7) / 8) throw new ArgumentException($"The bitmap holds fewer than {count} bits.", nameof(bitmap));
        List<long> starts = [], ends = [];
        long at = 0;
        bool inside = false;
        Span<byte> eight = stackalloc byte[8];
        while (at < count)
        {
            // 64 bits from bit `at` (whole bytes; the tail of the last word is masked off).
            int byteAt = (int)(at >> 3), shift = (int)(at & 7);
            eight.Clear();
            bitmap.Slice(byteAt, Math.Min(8, bitmap.Length - byteAt)).CopyTo(eight);
            ulong word = BinaryPrimitives.ReadUInt64LittleEndian(eight) >> shift;
            int valid = (int)Math.Min(64 - shift, count - at);
            if (valid < 64) word &= (1UL << valid) - 1;
            int bit = 0;
            while (bit < valid)
            {
                // The next change: the next 1 when outside a run, the next 0 inside one.
                ulong look = (inside ? ~word : word) >> bit;
                if (bit > 0 || valid < 64) look &= valid - bit >= 64 ? ulong.MaxValue : (1UL << (valid - bit)) - 1;
                if (look == 0) break;
                bit += BitOperations.TrailingZeroCount(look);
                if (inside) ends.Add(firstRow + at + bit);
                else starts.Add(firstRow + at + bit);
                inside = !inside;
            }
            at += valid;
        }
        if (inside) ends.Add(firstRow + count);
        return starts.Count == 0 ? Empty : new([.. starts], [.. ends]);
    }

    private static RowRanges Merge(ReadOnlySpan<(long Start, long End)> sorted)
    {
        List<long> starts = [], ends = [];
        foreach (var (start, end) in sorted)
        {
            if (ends.Count > 0 && start <= ends[^1])
            {
                if (end > ends[^1]) ends[^1] = end;
                continue;
            }
            starts.Add(start);
            ends.Add(end);
        }
        return starts.Count == 0 ? Empty : new([.. starts], [.. ends]);
    }

    /// <summary>The number of ranges.</summary>
    public int Count => _starts.Length;

    /// <summary>The number of rows.</summary>
    public long RowCount { get; }

    /// <summary>Whether there are no rows.</summary>
    public bool IsEmpty => _starts.Length == 0;

    /// <summary>The <paramref name="index"/>th range.</summary>
    public (long Start, long Count) this[int index] => (_starts[index], _ends[index] - _starts[index]);

    /// <summary>The row after the last one (0 when empty).</summary>
    public long End => _ends.Length == 0 ? 0 : _ends[^1];

    /// <summary>Whether <paramref name="row"/> is in the set.</summary>
    public bool Contains(long row)
    {
        int i = Array.BinarySearch(_starts, row);
        if (i >= 0) return true;
        i = ~i - 1;
        return i >= 0 && row < _ends[i];
    }

    /// <summary>Rows in either set.</summary>
    public RowRanges Union(RowRanges other)
    {
        ArgumentNullException.ThrowIfNull(other);
        var all = new (long, long)[Count + other.Count];
        int a = 0, b = 0, n = 0;
        while (a < Count || b < other.Count)
        {
            bool takeA = b == other.Count || a < Count && _starts[a] <= other._starts[b];
            all[n++] = takeA ? (_starts[a], _ends[a++]) : (other._starts[b], other._ends[b++]);
        }
        return Merge(all);
    }

    /// <summary>Rows in both sets.</summary>
    public RowRanges Intersect(RowRanges other)
    {
        ArgumentNullException.ThrowIfNull(other);
        List<long> starts = [], ends = [];
        int a = 0, b = 0;
        while (a < Count && b < other.Count)
        {
            long start = Math.Max(_starts[a], other._starts[b]), end = Math.Min(_ends[a], other._ends[b]);
            if (start < end)
            {
                starts.Add(start);
                ends.Add(end);
            }
            if (_ends[a] < other._ends[b]) a++;
            else b++;
        }
        return starts.Count == 0 ? Empty : new([.. starts], [.. ends]);
    }

    /// <summary>Rows in this set and not in <paramref name="other"/>.</summary>
    public RowRanges Except(RowRanges other)
    {
        ArgumentNullException.ThrowIfNull(other);
        return Intersect(other.Complement(Math.Max(End, other.End)));
    }

    /// <summary>The rows of <c>[0, <paramref name="count"/>)</c> not in this set.</summary>
    public RowRanges Complement(long count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        List<long> starts = [], ends = [];
        long at = 0;
        for (int i = 0; i < Count && at < count; i++)
        {
            if (_starts[i] > at)
            {
                starts.Add(at);
                ends.Add(Math.Min(_starts[i], count));
            }
            at = Math.Max(at, _ends[i]);
        }
        if (at < count)
        {
            starts.Add(at);
            ends.Add(count);
        }
        return starts.Count == 0 ? Empty : new([.. starts], [.. ends]);
    }

    /// <summary>The rows within <c>[<paramref name="start"/>, + <paramref name="count"/>)</c>, renumbered from 0.</summary>
    public RowRanges Slice(long start, long count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(start);
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        long end = start + count;
        List<long> starts = [], ends = [];
        int i = Array.BinarySearch(_ends, start);
        for (i = i < 0 ? ~i : i + 1; i < Count && _starts[i] < end; i++)
        {
            long s = Math.Max(_starts[i], start), e = Math.Min(_ends[i], end);
            if (s < e)
            {
                starts.Add(s - start);
                ends.Add(e - start);
            }
        }
        return starts.Count == 0 ? Empty : new([.. starts], [.. ends]);
    }

    /// <summary>The first <paramref name="rows"/> rows of the set (all of them when it holds fewer).</summary>
    public RowRanges Take(long rows)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(rows);
        if (rows >= RowCount) return this;
        List<long> starts = [], ends = [];
        for (int i = 0; i < Count && rows > 0; i++)
        {
            long length = Math.Min(_ends[i] - _starts[i], rows);
            starts.Add(_starts[i]);
            ends.Add(_starts[i] + length);
            rows -= length;
        }
        return starts.Count == 0 ? Empty : new([.. starts], [.. ends]);
    }

    /// <summary>The ranges, in order.</summary>
    public IEnumerator<(long Start, long Count)> GetEnumerator()
    {
        for (int i = 0; i < Count; i++) yield return this[i];
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <inheritdoc/>
    public bool Equals(RowRanges? other) =>
        other is not null && _starts.AsSpan().SequenceEqual(other._starts) && _ends.AsSpan().SequenceEqual(other._ends);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => Equals(obj as RowRanges);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        var hash = new HashCode();
        for (int i = 0; i < Count; i++)
        {
            hash.Add(_starts[i]);
            hash.Add(_ends[i]);
        }
        return hash.ToHashCode();
    }

    /// <inheritdoc/>
    public override string ToString() =>
        "{" + string.Join(", ", this.Select(r => r.Count == 1 ? $"{r.Start}" : $"{r.Start}..{r.Start + r.Count - 1}")) + "}";
}
