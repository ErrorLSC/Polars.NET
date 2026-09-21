using Polars.NET.Core;
using System.Runtime.CompilerServices;

namespace Polars.CSharp;

/// <summary>
/// Enumerator for <see cref="Series"/>.
/// </summary>
public ref struct SeriesEnumerator<T>
{
    private readonly Series _series;
    private readonly long _length;
    private readonly bool _hasNulls;
    private long _index;
    private T? _current;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal SeriesEnumerator(Series series)
    {
        _series = series;
        _length = series.Length;
        _hasNulls = series.HasNulls();
        _index = -1;
        _current = default;
    }

    /// <summary>
    /// Moves the enumerator to the next element.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MoveNext()
    {
        long nextIndex = _index + 1;
        if (nextIndex < _length)
        {
            _index = nextIndex;

            if (_hasNulls && _series.IsNullAt(_index, uncheck: true))
            {
                _current = default;
            }
            else
            {
                _current = _series.GetValue<T>(_index, uncheck: true);
            }
            return true;
        }

        _current = default;
        return false;
    }

    /// <summary>
    /// Gets the current element.
    /// </summary>
    public readonly T? Current
    {
        get => _current;
    }

    /// <summary>
    /// Resets the enumerator to its initial position.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Reset()
    {
        _index = -1;
        _current = default;
    }
    /// <summary>
    /// Gets the total number of elements in the series as int32.
    /// </summary>
    public readonly int Count => checked((int)_length);
    /// <summary>
    /// Gets the total number of elements in the series as int64.
    /// </summary>
    public readonly long Length => _length;
    /// <summary>
    /// Returns the first element of the series, or throws an InvalidOperationException if empty.
    /// </summary>
    public readonly T? First()
    {
        if (_length == 0)
        {
            throw new InvalidOperationException("Series contains no elements.");
        }

        if (_hasNulls && _series.IsNullAt(0, uncheck: true))
        {
            return default;
        }

        return _series.GetValue<T>(0, uncheck: true);
    }

    /// <summary>
    /// Returns the last element of the series, or throws an InvalidOperationException if empty.
    /// </summary>
    public readonly T? Last()
    {
        if (_length == 0)
        {
            throw new InvalidOperationException("Series contains no elements.");
        }

        if (_hasNulls && _series.IsNullAt(_length - 1, uncheck: true))
        {
            return default;
        }

        return _series.GetValue<T>(_length - 1, uncheck: true);
    }

    /// <summary>
    /// Returns the first element of the series, or a default value if the series is empty.
    /// </summary>
    public readonly T? FirstOrDefault(T? defaultValue = default)
    {
        if (_length == 0)
        {
            return defaultValue;
        }

        if (_hasNulls && _series.IsNullAt(0, uncheck: true))
        {
            return default;
        }

        return _series.GetValue<T>(0, uncheck: true);
    }

    /// <summary>
    /// Returns the last element of the series, or a default value if the series is empty.
    /// </summary>
    public readonly T? LastOrDefault(T? defaultValue = default)
    {
        if (_length == 0)
        {
            return defaultValue;
        }

        if (_hasNulls && _series.IsNullAt(_length - 1, uncheck: true))
        {
            return default;
        }

        return _series.GetValue<T>(_length - 1, uncheck: true);
    }

    /// <summary>
    /// Determines whether the series contains any elements.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly bool Any() => _length > 0;

    /// <summary>
    /// Returns the enumerator itself.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly SeriesEnumerator<T> GetEnumerator() => this;
    /// <summary>
    /// Materializes the remaining elements into a new array.
    /// </summary>
    public T?[] ToArray()
    {
        if (_length > int.MaxValue)
        {
            throw new OverflowException("Series length exceeds maximum supported array size.");
        }

        // Delegate to high-performance batch materialization if starting from the beginning
        if (_index == -1)
        {
            return _series.ToArray<T>();
        }

        int remaining = (int)(_length - (_index + 1));
        if (remaining <= 0)
        {
            return [];
        }

        var array = new T?[remaining];
        int targetIdx = 0;
        while (MoveNext())
        {
            array[targetIdx++] = _current;
        }
        return array;
    }

    /// <summary>
    /// Materializes the remaining elements into a new List.
    /// </summary>
    public List<T?> ToList()
    {
        if (_length > int.MaxValue)
        {
            throw new OverflowException("Series length exceeds maximum supported list size.");
        }

        int remaining = (int)(_length - (_index + 1));
        var list = new List<T?>(Math.Max(0, remaining));
        while (MoveNext())
        {
            list.Add(_current);
        }
        return list;
    }
}

public partial class Series : IDisposable, IPolarsSeries
{
    /// <summary>
    /// Returns a zero-allocation stack enumerator for strongly-typed iteration.
    /// Usage: foreach (int val in series.As{int}()) { ... }
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SeriesEnumerator<T> As<T>() => new(this);

    /// <summary>
    /// Pattern-based GetEnumerator providing zero-allocation foreach on the Series itself.
    /// Matched by the C# compiler ahead of any interface call.
    /// Usage: foreach (var item in series) { ... }
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public SeriesEnumerator<object?> GetEnumerator() => new(this);
}
