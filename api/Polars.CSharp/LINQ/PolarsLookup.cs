#pragma warning disable CS1591
using System.Diagnostics.CodeAnalysis;
using System.Collections;
using Polars.NET.Core.Data;
using Kaminari.Sazanami.Data;

namespace Polars.CSharp.Linq;

/// <summary>
/// Lightweight, non-ref-struct enumerator for lazy row hydration within a partition DataFrame.
/// Avoids CS4013 yield/await boundary restrictions on ref structs.
/// </summary>
internal sealed class PolarsPartitionEnumerator<TElement> : IEnumerator<TElement>
{
    private readonly ArrowBatchCursor<TElement> _cursor;
    private readonly long _height;
    private long _index = -1L;
    private TElement? _current;

    internal PolarsPartitionEnumerator(ArrowBatchCursor<TElement> cursor)
    {
        _cursor = cursor;
        _height = cursor.Length;
    }

    public bool MoveNext()
    {
        long next = _index + 1L;
        if (next < _height)
        {
            _index = next;
            _current = _cursor.ReadRow((int)next);
            return true;
        }

        _current = default;
        return false;
    }

    public TElement Current => _current!;
    object? IEnumerator.Current => Current;
    public void Reset() { _index = -1L; _current = default; }
    public void Dispose() { }
}

/// <summary>
/// Represents a group partition backed by a sliced Polars DataFrame.
/// </summary>
public sealed class PolarsGrouping<TKey, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TElement> : IGrouping<TKey, TElement>, IReadOnlyList<TElement>
{
    private ArrowBatchCursor<TElement>? _cursor;

    public TKey Key { get; }

    /// <summary>
    /// Gets the underlying partition DataFrame representing this group.
    /// </summary>
    public DataFrame DataFrame { get; }

    /// <summary>
    /// Gets the total number of elements in this group partition in O(1).
    /// </summary>
    public int Count => checked((int)DataFrame.Height);

    private ArrowBatchCursor<TElement> EnsureCursor()
        => _cursor ??= DataFrameRowMapper<TElement>.CreateCursor(DataFrame.Handle);

    public TElement this[int index]
    {
        get
        {
            if ((uint)index >= (uint)DataFrame.Height)
                throw new ArgumentOutOfRangeException(nameof(index), $"Index {index} is out of bounds for partition height {DataFrame.Height}.");
            return EnsureCursor().ReadRow(index);
        }
    }

    internal PolarsGrouping(TKey key, DataFrame df)
    {
        Key = key;
        DataFrame = df;
    }

    public IEnumerator<TElement> GetEnumerator()
    {
        return new PolarsPartitionEnumerator<TElement>(EnsureCursor());
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>
/// ILookup implementation backed by native Polars partitions. Groups enumerate in the order their keys first
/// appear; a null key forms a group of its own, as in LINQ to Objects.
/// </summary>
public sealed class PolarsLookup<TKey, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] TElement> : ILookup<TKey, TElement>
{
    // Non-null keys, wrapped because Dictionary keys are constrained to notnull while TKey may be nullable.
    private readonly record struct NonNullKey(TKey Value);

    private sealed class NonNullKeyComparer(IEqualityComparer<TKey> inner) : IEqualityComparer<NonNullKey>
    {
        public bool Equals(NonNullKey x, NonNullKey y) => inner.Equals(x.Value, y.Value);
        public int GetHashCode(NonNullKey key) => inner.GetHashCode(key.Value!);
    }

    private readonly List<PolarsGrouping<TKey, TElement>> _ordered;
    private readonly Dictionary<NonNullKey, PolarsGrouping<TKey, TElement>> _groups;
    private readonly PolarsGrouping<TKey, TElement>? _nullGroup;

    internal PolarsLookup(List<PolarsGrouping<TKey, TElement>> groups, IEqualityComparer<TKey>? comparer)
    {
        _ordered = groups;
        _groups = new Dictionary<NonNullKey, PolarsGrouping<TKey, TElement>>(
            groups.Count,
            new NonNullKeyComparer(comparer ?? EqualityComparer<TKey>.Default));
        foreach (var group in groups)
        {
            if (group.Key is null) _nullGroup = group;
            else _groups[new NonNullKey(group.Key)] = group;
        }
    }

    public int Count => _ordered.Count;

    public IEnumerable<TElement> this[TKey key]
    {
        get
        {
            if (TryGetGroup(key, out var group))
            {
                return group;
            }

            return [];
        }
    }

    public bool Contains(TKey key) => TryGetGroup(key, out _);

    private bool TryGetGroup(TKey key, out PolarsGrouping<TKey, TElement> group)
    {
        if (key is null)
        {
            group = _nullGroup!;
            return _nullGroup is not null;
        }

        return _groups.TryGetValue(new NonNullKey(key), out group!);
    }

    /// <summary>
    /// Strongly-typed struct enumerator that enables foreach inference to PolarsGrouping.
    /// </summary>
    public List<PolarsGrouping<TKey, TElement>>.Enumerator GetEnumerator()
        => _ordered.GetEnumerator();

    IEnumerator<IGrouping<TKey, TElement>> IEnumerable<IGrouping<TKey, TElement>>.GetEnumerator()
        => _ordered.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator()
        => _ordered.GetEnumerator();
}
