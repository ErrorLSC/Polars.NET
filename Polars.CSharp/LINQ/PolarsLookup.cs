#pragma warning disable CS1591
using System.Collections;

namespace Polars.CSharp.Linq;

/// <summary>
/// Lightweight, non-ref-struct enumerator for lazy row hydration within a partition DataFrame.
/// Avoids CS4013 yield/await boundary restrictions on ref structs.
/// </summary>
internal sealed class PolarsPartitionEnumerator<TElement> : IEnumerator<TElement>
{
    private readonly DataFrame _df;
    private readonly Func<DataFrame, long, TElement> _mapper;
    private readonly long _height;
    private long _index = -1L;
    private TElement? _current;

    internal PolarsPartitionEnumerator(DataFrame df, Func<DataFrame, long, TElement> mapper)
    {
        _df = df;
        _mapper = mapper;
        _height = df.Height;
    }

    public bool MoveNext()
    {
        long next = _index + 1L;
        if (next < _height)
        {
            _index = next;
            _current = _mapper(_df, _index);
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
/// Implements both IGrouping and IReadOnlyList for O(1) Count and indexed access.
/// </summary>
public sealed class PolarsGrouping<TKey, TElement> : IGrouping<TKey, TElement>, IReadOnlyList<TElement>
{
    private readonly DataFrame _df;
    private readonly Func<DataFrame, long, TElement> _mapper;

    public TKey Key { get; }

    /// <summary>
    /// Gets the underlying partition DataFrame representing this group.
    /// </summary>
    public DataFrame DataFrame => _df;

    /// <summary>
    /// Gets the total number of elements in this group partition in O(1).
    /// </summary>
    public int Count => checked((int)_df.Height);

    public TElement this[int index]
    {
        get
        {
            if ((uint)index >= (uint)_df.Height)
                throw new ArgumentOutOfRangeException(nameof(index), $"Index {index} is out of bounds for partition height {_df.Height}.");
            return _mapper(_df, index);
        }
    }

    internal PolarsGrouping(TKey key, DataFrame df)
    {
        Key = key;
        _df = df;
        _mapper = RowMapper<TElement>.GetOrCreate(df);
    }

    public IEnumerator<TElement> GetEnumerator()
    {
        return new PolarsPartitionEnumerator<TElement>(_df, _mapper);
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}

/// <summary>
/// High-performance, zero-overhead ILookup implementation backed by native Polars partitions.
/// Mirrors BCL Lookup semantics by providing strongly typed group enumeration.
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
    {
        return _groups.Values.GetEnumerator();
    }

    IEnumerator<IGrouping<TKey, TElement>> IEnumerable<IGrouping<TKey, TElement>>.GetEnumerator()
    {
        return _groups.Values.GetEnumerator();
    }

    IEnumerator IEnumerable.GetEnumerator()
    {
        return _groups.Values.GetEnumerator();
    }
}