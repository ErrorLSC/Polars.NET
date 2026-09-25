using System.Linq.Expressions;
using System.Reflection;
using System.Collections.Concurrent;
using System.Runtime.CompilerServices;

namespace Polars.CSharp;

internal static class RowMapper<T>
{
    // Cache compiled mapper per schema signature
    private static readonly ConcurrentDictionary<string, Func<DataFrame, long, T>> Cache = new();

    private static string ComputeSchemaSignature(DataFrame df)
    {
        return string.Join("|", df.ColumnNames.Select(c => $"{c}:{df[c].DataTypeName}"));
    }

    public static Func<DataFrame, long, T> GetOrCreate(DataFrame df)
    {
        string signature = ComputeSchemaSignature(df);
        if (Cache.TryGetValue(signature, out var cachedMapper))
        {
            return cachedMapper;
        }

        var mapper = BuildMapper(df);
        Cache.TryAdd(signature, mapper);
        return mapper;
    }

    private static Func<DataFrame, long, T> BuildMapper(DataFrame df)
    {
        var targetType = typeof(T);
        var dfParam = Expression.Parameter(typeof(DataFrame), "df");
        var idxParam = Expression.Parameter(typeof(long), "rowIndex");

        var getValueMethodDef = typeof(Series).GetMethod(nameof(Series.GetValue), [typeof(long), typeof(bool)])!;
        var columnIndexerMethod = typeof(DataFrame).GetMethod("get_Item", [typeof(int)])!;

        var colMap = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        for (int i = 0; i < (int)df.Width; i++)
        {
            colMap[df.ColumnNames[i]] = i;
        }

        Expression CreateReadExpr(int colIdx, Type returnType)
        {
            var colExpr = Expression.Call(dfParam, columnIndexerMethod, Expression.Constant(colIdx));
            var getValueMethod = getValueMethodDef.MakeGenericMethod(returnType);
            return Expression.Call(colExpr, getValueMethod, idxParam, Expression.Constant(true, typeof(bool)));
        }

        // Helper to construct a single non-tuple object or record instance from DataFrame columns
        Expression BuildSingleObjectExpr(Type type)
        {
            var ctors = type.GetConstructors(BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance);
            var primaryCtor = ctors.OrderByDescending(c => c.GetParameters().Length).FirstOrDefault();

            if (primaryCtor != null && primaryCtor.GetParameters().Length > 0)
            {
                var ctorParams = primaryCtor.GetParameters();
                var ctorArgs = new Expression[ctorParams.Length];
                for (int i = 0; i < ctorParams.Length; i++)
                {
                    var param = ctorParams[i];
                    if (colMap.TryGetValue(param.Name!, out int colIdx))
                    {
                        ctorArgs[i] = CreateReadExpr(colIdx, param.ParameterType);
                    }
                    else
                    {
                        ctorArgs[i] = Expression.Default(param.ParameterType);
                    }
                }
                return Expression.New(primaryCtor, ctorArgs);
            }

            // Fallback: Default constructor + property bindings
            var defaultCtor = type.GetConstructor(
                BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance,
                null,
                Type.EmptyTypes,
                null);

            var instanceVar = Expression.Variable(type, "subInstance");
            Expression createInstanceExpr = defaultCtor != null
                ? Expression.New(defaultCtor)
                : Expression.New(type);

            var blockExpressions = new List<Expression> { Expression.Assign(instanceVar, createInstanceExpr) };
            var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
            foreach (var prop in props)
            {
                if (!prop.CanWrite) continue;
                if (colMap.TryGetValue(prop.Name, out int colIdx))
                {
                    var readExpr = CreateReadExpr(colIdx, prop.PropertyType);
                    blockExpressions.Add(Expression.Assign(Expression.Property(instanceVar, prop), readExpr));
                }
            }
            blockExpressions.Add(instanceVar);
            return Expression.Block([instanceVar], blockExpressions);
        }

        // Branch 1: ValueTuple composite mapping (e.g. ValueTuple<MemberRecord, Employee, Department>)
        if (targetType.IsValueType && targetType.FullName != null && targetType.FullName.StartsWith("System.ValueTuple`"))
        {
            var genericArgs = targetType.GetGenericArguments();
            var tupleCtor = targetType.GetConstructor(genericArgs)
                ?? throw new NotSupportedException($"ValueTuple constructor matching arguments could not be resolved for '{targetType.FullName}'.");

            var tupleArgs = new Expression[genericArgs.Length];
            for (int i = 0; i < genericArgs.Length; i++)
            {
                var subType = genericArgs[i];
                // If it's a direct primitive or scalar in the tuple
                if (colMap.TryGetValue($"Item{i + 1}", out int colIdx))
                {
                    tupleArgs[i] = CreateReadExpr(colIdx, subType);
                }
                else
                {
                    // Construct composite sub-entity
                    tupleArgs[i] = BuildSingleObjectExpr(subType);
                }
            }

            var tupleNewExpr = Expression.New(tupleCtor, tupleArgs);
            return Expression.Lambda<Func<DataFrame, long, T>>(tupleNewExpr, dfParam, idxParam).Compile();
        }

        // Branch 2: Standard single object or record
        var singleObjExpr = BuildSingleObjectExpr(targetType);
        return Expression.Lambda<Func<DataFrame, long, T>>(singleObjExpr, dfParam, idxParam).Compile();
    }
}

/// <summary>
/// Stack-only, zero-allocation enumerator for DataFrame row hydration.
/// Eliminates state-machine allocations and maximizes hot-path throughput.
/// </summary>
public ref struct DataFrameRowEnumerator<T>
{
    private readonly DataFrame _df;
    private readonly Func<DataFrame, long, T> _mapper;
    private readonly long _height;
    private long _index;
    private T? _current;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    internal DataFrameRowEnumerator(DataFrame df, Func<DataFrame, long, T> mapper)
    {
        _df = df;
        _mapper = mapper;
        _height = df.Height;
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
        if (next < _height)
        {
            _index = next;
            _current = _mapper(_df, _index);
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
    public readonly bool IsEmpty => _height == 0;
    /// <summary>
    /// Gets the total number of rows as an integer.
    /// Throws <see cref="OverflowException"/> if row count exceeds <see cref="int.MaxValue"/>.
    /// </summary>
    public readonly int Count => checked((int)_height);
    /// <summary>
    /// Gets the total number of rows as a long.
    /// </summary>
    public readonly long Length => _height;
    /// <summary>
    /// Reads a mapped row at the specified physical index directly without looping through previous rows.
    /// </summary>
    public readonly T ElementAt(long index)
    {
        if (index < 0 || index >= _height)
            throw new ArgumentOutOfRangeException(nameof(index), $"Index {index} is out of bounds for DataFrame height {_height}.");
        return _mapper(_df, index);
    }
    /// <summary>
    /// Reads a mapped row at the specified physical index, or returns the default value if out of bounds.
    /// </summary>
    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public readonly T? ElementAtOrDefault(long index, T? defaultValue = default)
    {
        if (index < 0 || index >= _height)
            return defaultValue;
        return _mapper(_df, index);
    }

    /// <summary>
    /// Copies all mapped rows into the destination span without any heap allocations.
    /// Returns the number of rows copied.
    /// </summary>
    public readonly int CopyTo(Span<T> destination)
    {
        if (_height > destination.Length)
            throw new ArgumentException($"Destination span length ({destination.Length}) is smaller than row count ({_height}).", nameof(destination));

        int total = (int)_height;
        for (int i = 0; i < total; i++)
        {
            destination[i] = _mapper(_df, i);
        }
        return total;
    }
    /// <summary>
    /// Attempts to copy all mapped rows into the destination span without heap allocation.
    /// </summary>
    public readonly bool TryCopyTo(Span<T> destination, out int written)
    {
        if (_height > destination.Length)
        {
            written = 0;
            return false;
        }

        int total = (int)_height;
        for (int i = 0; i < total; i++)
        {
            destination[i] = _mapper(_df, i);
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
        if (_height == 0)
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
        if (_height == 0)
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
        if (_height == 0)
        {
            throw new InvalidOperationException("DataFrame contains no rows.");
        }

        // O(1) direct access instead of exhausting MoveNext()
        return ElementAt(_height - 1);
    }

    /// <summary>
    /// Returns the last mapped row of the DataFrame, or default if empty.
    /// </summary>
    public readonly T? LastOrDefault(T? defaultValue = default)
    {
        if (_height == 0)
        {
            return defaultValue;
        }

        return ElementAt(_height - 1);
    }
    /// <summary>
    /// Materializes all mapped rows into an array with exact pre-allocation.
    /// </summary>
    public T[] ToArray()
    {
        if (_height == 0) return [];
        if (_height > int.MaxValue)
            throw new OverflowException($"DataFrame row count ({_height}) exceeds Int32.MaxValue.");

        var array = new T[(int)_height];
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
        if (_height == 0) return [];
        if (_height > int.MaxValue)
            throw new OverflowException($"DataFrame row count ({_height}) exceeds Int32.MaxValue.");

        var list = new List<T>((int)_height);
        while (MoveNext())
        {
            list.Add(Current);
        }
        return list;
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
    {
        var mapper = RowMapper<T>.GetOrCreate(this);
        return new DataFrameRowEnumerator<T>(this, mapper);
    }

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
