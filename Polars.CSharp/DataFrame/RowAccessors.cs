#pragma warning disable CS1591
using System.Collections;
using System.Runtime.CompilerServices;
using Polars.NET.Core.Data;

namespace Polars.CSharp;

internal static class RowMapper<T>
{
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static ArrowBatchCursor<T> CreateCursor(DataFrame df)
        => ArrowRowMapper<T>.CreateCursor(df.Handle);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public static T ReadRow(DataFrame df, long index)
    {
        using var cursor = ArrowRowMapper<T>.CreateCursor(df.Handle);
        return cursor.ReadRow(checked((int)index));
    }
}

/// <summary>
/// Stack-only, zero-allocation enumerator for DataFrame row hydration.
/// Eliminates state-machine allocations and maximizes hot-path throughput.
/// </summary>
public ref struct DataFrameRowEnumerator<T>
{
    private readonly ArrowBatchCursor<T> _cursor;
    private long _index;
    private T? _current;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal DataFrameRowEnumerator(DataFrame df)
    {
        _cursor = ArrowRowMapper<T>.CreateCursor(df.Handle);
        Length = _cursor.Length;
        _index = -1L;
        _current = default;
    }

    /// <summary>
    /// Advances the enumerator to the next row and returns whether a row was successfully read.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public bool MoveNext()
    {
        long next = _index + 1L;
        if (next < Length)
        {
            _index = next;
            _current = _cursor.ReadRow((int)next);
            return true;
        }

        _current = default;
        return false;
    }

    /// <summary>
    /// Gets the current row as a strongly-typed object.
    /// </summary>
    public readonly T Current
    {
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        get => _current!;
    }

    /// <summary>
    /// Returns the enumerator itself, for use in foreach loops.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly DataFrameRowEnumerator<T> GetEnumerator() => this;

    /// <summary>
    /// Gets a value indicating whether the DataFrame is empty.
    /// </summary>
    public readonly bool IsEmpty => Length == 0;

    /// <summary>
    /// Gets the total number of rows as an integer.
    /// Throws <see cref="OverflowException"/> if row count exceeds <see cref="int.MaxValue"/>.
    /// </summary>
    public readonly int Count => checked((int)Length);

    /// <summary>
    /// Gets the total number of rows as a long.
    /// </summary>
    public readonly long Length { get; }

    /// <summary>
    /// Reads a mapped row at the specified physical index directly without looping through previous rows.
    /// </summary>
    public readonly T ElementAt(long index)
    {
        if (index < 0 || index >= Length)
            throw new ArgumentOutOfRangeException(nameof(index), $"Index {index} is out of bounds for DataFrame height {Length}.");
        return _cursor.ReadRow((int)index);
    }

    /// <summary>
    /// Reads a mapped row at the specified physical index, or returns the default value if out of bounds.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly T? ElementAtOrDefault(long index, T? defaultValue = default)
    {
        if (index < 0 || index >= Length)
            return defaultValue;
        return _cursor.ReadRow((int)index);
    }

    /// <summary>
    /// Copies all mapped rows into the destination span without any heap allocations.
    /// Returns the number of rows copied.
    /// </summary>
    public readonly int CopyTo(Span<T> destination)
    {
        if (Length > destination.Length)
            throw new ArgumentException($"Destination span length ({destination.Length}) is smaller than row count ({Length}).", nameof(destination));

        int total = (int)Length;
        for (int i = 0; i < total; i++)
        {
            destination[i] = _cursor.ReadRow(i);
        }
        return total;
    }

    /// <summary>
    /// Attempts to copy all mapped rows into the destination span without heap allocation.
    /// </summary>
    public readonly bool TryCopyTo(Span<T> destination, out int written)
    {
        if (Length > destination.Length)
        {
            written = 0;
            return false;
        }

        int total = (int)Length;
        for (int i = 0; i < total; i++)
        {
            destination[i] = _cursor.ReadRow(i);
        }
        written = total;
        return true;
    }

    /// <summary>
    /// Returns the first mapped row of the DataFrame.
    /// Throws <see cref="InvalidOperationException"/> if the DataFrame contains no rows.
    /// </summary>
    public readonly T First()
    {
        if (Length == 0)
        {
            throw new InvalidOperationException("DataFrame contains no rows.");
        }

        return ElementAt(0);
    }

    /// <summary>
    /// Returns the first mapped row of the DataFrame, or default if empty.
    /// </summary>
    public readonly T? FirstOrDefault(T? defaultValue = default)
    {
        if (Length == 0)
        {
            return defaultValue;
        }

        return ElementAt(0);
    }

    /// <summary>
    /// Returns the last mapped row of the DataFrame.
    /// Throws <see cref="InvalidOperationException"/> if the DataFrame contains no rows.
    /// </summary>
    public readonly T Last()
    {
        if (Length == 0)
        {
            throw new InvalidOperationException("DataFrame contains no rows.");
        }

        // O(1) direct access instead of exhausting MoveNext()
        return ElementAt(Length - 1);
    }

    /// <summary>
    /// Returns the last mapped row of the DataFrame, or default if empty.
    /// </summary>
    public readonly T? LastOrDefault(T? defaultValue = default)
    {
        if (Length == 0)
        {
            return defaultValue;
        }

        return ElementAt(Length - 1);
    }

    /// <summary>
    /// Materializes all mapped rows into an array with exact pre-allocation.
    /// </summary>
    public T[] ToArray()
    {
        if (Length == 0) return [];
        if (Length > int.MaxValue)
            throw new OverflowException($"DataFrame row count ({Length}) exceeds Int32.MaxValue.");

        var array = new T[(int)Length];
        int idx = 0;
        while (MoveNext())
        {
            array[idx++] = Current;
        }
        return array;
    }

    /// <summary>
    /// Materializes all mapped rows into a List with exact pre-allocation.
    /// </summary>
    public List<T> ToList()
    {
        if (Length == 0) return [];
        if (Length > int.MaxValue)
            throw new OverflowException($"DataFrame row count ({Length}) exceeds Int32.MaxValue.");

        var list = new List<T>((int)Length);
        while (MoveNext())
        {
            list.Add(Current);
        }
        return list;
    }
}

/// <summary>
/// A heap-allocated, lazy IEnumerable wrapper around a Polars DataFrame.
/// Defers row hydration to iteration time and avoids full eager array allocations.
/// Implements IReadOnlyCollection for O(1) Count inspection.
/// </summary>
public sealed class PolarsRowEnumerable<T> : IReadOnlyCollection<T>, ICollection<T>
{
    private readonly DataFrame _df;

    public int Count => checked((int)_df.Height);
    public bool IsReadOnly => true;

    internal PolarsRowEnumerable(DataFrame df)
    {
        _df = df;
    }

    public void CopyTo(T[] array, int arrayIndex)
    {
        ArgumentNullException.ThrowIfNull(array);
        using var cursor = ArrowRowMapper<T>.CreateCursor(_df.Handle);
        int len = cursor.Length;
        if (arrayIndex < 0 || arrayIndex + len > array.Length)
            throw new ArgumentOutOfRangeException(nameof(arrayIndex));

        for (int i = 0; i < len; i++)
        {
            array[arrayIndex + i] = cursor.ReadRow(i);
        }
    }

    public void Add(T item) => throw new NotSupportedException();
    public void Clear() => throw new NotSupportedException();
    public bool Contains(T item) => System.Linq.Enumerable.Contains(this, item);
    public bool Remove(T item) => throw new NotSupportedException();

    // Fast-path: duck-typed struct enumerator for foreach loops (Zero heap allocation)
    public Enumerator GetEnumerator() => new(_df);

    // Generic interface fallback
    IEnumerator<T> IEnumerable<T>.GetEnumerator() => new Enumerator(_df);

    // Non-generic interface fallback
    IEnumerator IEnumerable.GetEnumerator() => new Enumerator(_df);

    public struct Enumerator : IEnumerator<T>
    {
        private readonly ArrowBatchCursor<T> _cursor;
        private readonly long _height;
        private long _index;
        private T? _current;

        internal Enumerator(DataFrame df)
        {
            _cursor = ArrowRowMapper<T>.CreateCursor(df.Handle);
            _height = _cursor.Length;
            _index = -1L;
            _current = default;
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

        public readonly T Current => _current!;
        readonly object? IEnumerator.Current => Current;
        public void Reset() { _index = -1L; _current = default; }
        public readonly void Dispose() => _cursor?.Dispose();
    }
}

public partial class DataFrame : IDisposable, IEnumerable<Series>
{
    // ==========================================
    // Object Mapping (To Records)
    // ==========================================

    /// <summary>
    /// Enumerates DataFrame rows as strongly-typed objects using pre-compiled Fast Path mappings.
    /// </summary>
    /// <typeparam name="T">The target DTO type with a parameterless constructor.</typeparam>
    /// <returns>An IEnumerable of hydrated objects.</returns>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public DataFrameRowEnumerator<T> Rows<T>()
        => new(this);

    /// <summary>
    /// Get data for selected row.
    /// </summary>
    public object?[] Row(long index)
    {
        if (index < 0 || index >= Height)
            throw new IndexOutOfRangeException($"Row index {index} is out of bounds. Height: {Height}");

        int width = checked((int)Width);
        var rowData = new object?[width];

        GetRow(index, rowData.AsSpan());
        return rowData;
    }

    /// <summary>
    /// Fills the destination span with all values of a single row.
    /// </summary>
    /// <param name="index">The 64-bit row index.</param>
    /// <param name="destination">The destination span whose length must be at least the DataFrame width.</param>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void GetRow(long index, Span<object?> destination)
    {
        if (index < 0 || index >= Height)
            throw new IndexOutOfRangeException($"Row index {index} is out of bounds. Height: {Height}");

        int width = checked((int)Width);
        if (destination.Length < width)
            throw new ArgumentException($"Destination span length ({destination.Length}) is smaller than DataFrame width ({width}).", nameof(destination));

        for (int i = 0; i < width; i++)
        {
            using var col = Column(i);
            destination[i] = col.GetValue<object?>(index, uncheck: true);
        }
    }
}
