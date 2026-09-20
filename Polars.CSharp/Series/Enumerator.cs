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
    /// Returns the enumerator itself.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly SeriesEnumerator<T> GetEnumerator() => this;
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
