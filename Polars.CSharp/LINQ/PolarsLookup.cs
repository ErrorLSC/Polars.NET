#pragma warning disable CS1591
using System.Collections;
using Polars.NET.Core.Data;

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
public sealed class PolarsGrouping<TKey, TElement> : IGrouping<TKey, TElement>, IReadOnlyList<TElement>
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
        => _cursor ??= ArrowRowMapper<TElement>.CreateCursor(DataFrame.Handle);

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
/// ILookup implementation backed by native Polars partitions.
/// </summary>
public sealed class PolarsLookup<TKey, TElement> : ILookup<TKey, TElement>
    where TKey : notnull
{
    private readonly Dictionary<TKey, PolarsGrouping<TKey, TElement>> _groups;

    internal PolarsLookup(Dictionary<TKey, PolarsGrouping<TKey, TElement>> groups)
    {
        _groups = groups;
    }

    public int Count => _groups.Count;

    public IEnumerable<TElement> this[TKey key]
    {
        get
        {
            if (_groups.TryGetValue(key, out var group))
            {
                return group;
            }

            return [];
        }
    }

    public bool Contains(TKey key) => _groups.ContainsKey(key);

    /// <summary>
    /// Strongly-typed struct enumerator that enables foreach inference to PolarsGrouping.
    /// </summary>
    public Dictionary<TKey, PolarsGrouping<TKey, TElement>>.ValueCollection.Enumerator GetEnumerator()
        => _groups.Values.GetEnumerator();

    IEnumerator<IGrouping<TKey, TElement>> IEnumerable<IGrouping<TKey, TElement>>.GetEnumerator()
        => _groups.Values.GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator()
        => _groups.Values.GetEnumerator();
}
