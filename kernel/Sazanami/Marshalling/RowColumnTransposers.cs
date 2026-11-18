namespace Kaminari.Sazanami.Marshalling;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

/// <summary>
/// Where finished columns go. An engine implements this to receive columns in its own representation.
/// </summary>
/// <typeparam name="TColumn">The engine's column type.</typeparam>
public interface IColumnSink<TColumn> where TColumn : class
{
    /// <summary>Builds a column from contiguous values, or returns null when this path does not support <typeparamref name="T"/>.</summary>
    TColumn? CreateFromSpan<T>(string name, ReadOnlySpan<T> data);

    /// <summary>Builds a column of any supported element type.</summary>
    TColumn CreateGeneric<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.Interfaces)] T>(string name, IEnumerable<T> data);
}

/// <summary>
/// Internal columnar abstraction for buffering row values and exporting them to a column sink.
/// </summary>
public interface IColumnBuffer
{
    void AddObject(object? value);
    TColumn ToColumn<TColumn>(string name, IColumnSink<TColumn> sink) where TColumn : class;
}

/// <summary>
/// Strongly-typed column buffer backed by a contiguous array.
/// Hands the sink a span of the buffer to avoid memory duplication.
/// </summary>
/// <typeparam name="T">The scalar or nullable element type.</typeparam>
internal sealed class ColumnBuffer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.Interfaces)] T>(int capacity) : IColumnBuffer
{
    private T?[] _buffer = capacity > 0 ? new T?[capacity] : [];
    private int _count = 0;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(T? value)
    {
        if ((uint)_count >= (uint)_buffer.Length)
        {
            Resize();
        }
        _buffer[_count++] = value;
    }

    public void AddObject(object? value)
    {
        if (value is null)
        {
            Append(default);
            return;
        }

        if (value is T exact)
        {
            Append(exact);
            return;
        }

        var underlying = ClrTypeHelper.GetUnderlyingOrSelf(typeof(T));
        Append((T?)Convert.ChangeType(value, underlying));
    }

    public TColumn ToColumn<TColumn>(string name, IColumnSink<TColumn> sink) where TColumn : class
    {
        ReadOnlySpan<T?> span = _buffer.AsSpan(0, _count);
        return sink.CreateFromSpan(name, span) ?? sink.CreateGeneric(name, _buffer.Take(_count));
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Resize()
    {
        int newCap = _buffer.Length == 0 ? 16 : _buffer.Length * 2;
        Array.Resize(ref _buffer, newCap);
    }
}

/// <summary>
/// Fallback buffer for complex nested or non-primitive types.
/// </summary>
internal sealed class StructColumnBuffer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.Interfaces)] T>(int capacity) : IColumnBuffer
{
    private readonly List<T?> _items = new(capacity);

    public void AddObject(object? value)
    {
        _items.Add((T?)value);
    }

    public TColumn ToColumn<TColumn>(string name, IColumnSink<TColumn> sink) where TColumn : class
    {
        if (_items.Count == 0)
        {
            return sink.CreateGeneric(name, Array.Empty<T>());
        }

        return sink.CreateGeneric(name, _items);
    }
}

/// <summary>
/// Factory for creating strongly-typed columnar buffers.
/// </summary>
internal static class ColumnBufferFactory
{
    private static readonly ConcurrentDictionary<Type, Func<int, IColumnBuffer>> SimpleFactories = new();
    private static readonly ConcurrentDictionary<Type, Func<int, IColumnBuffer>> StructFactories = new();

    public static IColumnBuffer Create(Type propType, int capacity)
    {
        if (ClrTypeHelper.IsSupportedSimpleType(propType))
        {
            Type targetType = ClrTypeHelper.EnsureNullable(propType);
            return SimpleFactories.GetOrAdd(targetType, static ([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.Interfaces)] t) =>
            {
                var constructed = typeof(ColumnBuffer<>).MakeGenericType(t);
                var ctor = constructed.GetConstructor([typeof(int)])!;
                var param = Expression.Parameter(typeof(int), "cap");
                return Expression.Lambda<Func<int, IColumnBuffer>>(Expression.New(ctor, param), param).Compile();
            })(capacity);
        }

        return StructFactories.GetOrAdd(propType, static ([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.Interfaces)] t) =>
        {
            var constructed = typeof(StructColumnBuffer<>).MakeGenericType(t);
            var ctor = constructed.GetConstructor([typeof(int)])!;
            var param = Expression.Parameter(typeof(int), "cap");
            return Expression.Lambda<Func<int, IColumnBuffer>>(Expression.New(ctor, param), param).Compile();
        })(capacity);
    }
}

/// <summary>
/// Pre-compiled extractor for Structure-of-Arrays (SoA) object containers.
/// Caches property extractors using Expression Trees to directly generate columns without boxing.
/// </summary>
/// <typeparam name="T">The container type (e.g. anonymous type or SoA DTO).</typeparam>
/// <typeparam name="TColumn">The column type of the sink.</typeparam>
public static class SoAColumnExtractor<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T, TColumn> where TColumn : class
{
    private static readonly (string Name, Func<T, string, IColumnSink<TColumn>, TColumn> CreateColumn)[] ColumnFactories;

    static SoAColumnExtractor()
    {
        var properties = ClrTypeHelper.GetModelProperties(typeof(T));
        var factories = new List<(string Name, Func<T, string, IColumnSink<TColumn>, TColumn> CreateColumn)>();

        var containerParam = Expression.Parameter(typeof(T), "container");
        var nameParam = Expression.Parameter(typeof(string), "colName");
        var sinkParam = Expression.Parameter(typeof(IColumnSink<TColumn>), "sink");

        var createGenericMethod = typeof(IColumnSink<TColumn>).GetMethod(nameof(IColumnSink<TColumn>.CreateGeneric))!;

        foreach (var prop in properties)
        {
            if (!prop.CanRead || prop.GetIndexParameters().Length > 0) continue;

            var propType = prop.PropertyType;
            var propAccess = Expression.Property(containerParam, prop);
            var elementType = ClrTypeHelper.TryGetEnumerableElementType(propType);

            Expression createHandleExpr;

            if (elementType != null)
            {
                var specializedMethod = createGenericMethod.MakeGenericMethod(elementType);
                var targetEnumerableType = typeof(IEnumerable<>).MakeGenericType(elementType);
                Expression typedProp = propType == targetEnumerableType
                    ? propAccess
                    : Expression.Convert(propAccess, targetEnumerableType);

                createHandleExpr = Expression.Call(sinkParam, specializedMethod, nameParam, typedProp);
            }
            else
            {
                // Fallback for non-generic collections or object sequences
                var castMethod = typeof(Enumerable).GetMethod(nameof(Enumerable.Cast), BindingFlags.Public | BindingFlags.Static)!
                    .MakeGenericMethod(typeof(object));

                var specializedMethod = createGenericMethod.MakeGenericMethod(typeof(object));
                var castCall = Expression.Call(castMethod, Expression.Convert(propAccess, typeof(System.Collections.IEnumerable)));

                createHandleExpr = Expression.Call(sinkParam, specializedMethod, nameParam, castCall);
            }

            var lambda = Expression.Lambda<Func<T, string, IColumnSink<TColumn>, TColumn>>(createHandleExpr, containerParam, nameParam, sinkParam);
            factories.Add((prop.Name, lambda.Compile()));
        }

        ColumnFactories = [.. factories];
    }

    public static (string Name, Func<T, string, IColumnSink<TColumn>, TColumn> CreateColumn)[] GetFactories() => ColumnFactories;
}

internal static class DictBufferAppenderFactory
{
    private static readonly ConcurrentDictionary<Type, Action<IColumnBuffer, object?, bool>> Cache = new();

    private static readonly MethodInfo AppendValGenericMethodDef = typeof(DictBufferAppenderFactory)
        .GetMethod(nameof(AppendVal), BindingFlags.NonPublic | BindingFlags.Static)!;

    public static Action<IColumnBuffer, object?, bool> GetAppender(Type columnType)
    {
        return Cache.GetOrAdd(columnType, CreateAppender);
    }

    private static Action<IColumnBuffer, object?, bool> CreateAppender(Type type)
    {
        Type underlying = ClrTypeHelper.GetUnderlyingOrSelf(type);

        // 1. String fast-path (Reference Type)
        if (underlying == typeof(string))
        {
            return (buf, val, _) =>
            {
                if (buf is ColumnBuffer<string> strBuf)
                {
                    strBuf.Append(val?.ToString());
                }
                else
                {
                    buf.AddObject(val);
                }
            };
        }

        // 2. All supported primitive / scalar struct types (Value Types)
        if (ClrTypeHelper.IsSupportedSimpleType(underlying) && underlying.IsValueType)
        {
            var specializedMethod = AppendValGenericMethodDef.MakeGenericMethod(underlying);
            return (Action<IColumnBuffer, object?, bool>)specializedMethod.CreateDelegate(
                typeof(Action<IColumnBuffer, object?, bool>)
            );
        }

        // 3. Fallback for complex nested types (DTOs, anonymous classes, structs)
        return (buf, val, strict) =>
        {
            try
            {
                buf.AddObject(val);
            }
            catch when (!strict)
            {
                buf.AddObject(null);
            }
        };
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void AppendVal<T>(IColumnBuffer buffer, object? val, bool strict) where T : struct
    {
        if (buffer is ColumnBuffer<T?> typedBuf)
        {
            if (val is null)
            {
                typedBuf.Append(null);
                return;
            }

            if (val is T exact)
            {
                typedBuf.Append(exact);
                return;
            }

            try
            {
                T converted = (T)Convert.ChangeType(val, typeof(T));
                typedBuf.Append(converted);
            }
            catch (Exception ex)
            {
                if (strict)
                    throw new InvalidCastException($"Cannot cast value '{val}' of type '{val.GetType()}' to '{typeof(T)}'.", ex);

                typedBuf.Append(null);
            }
        }
        else
        {
            buffer.AddObject(val);
        }
    }
}

/// <summary>
/// High-performance columnar transposer for row-oriented dictionaries.
/// Handles dynamic schema inference, type promotion, and columnar buffer population.
/// </summary>
public static class DictSchemaTransposer
{
    /// <summary>
    /// Transposes row-oriented dictionaries into columns.
    /// </summary>
    /// <param name="records">The materialized dictionary row records.</param>
    /// <param name="sink">Receives the finished columns.</param>
    /// <param name="targetKeys">Optional fixed column keys to extract (e.g. from an explicit schema).</param>
    /// <param name="strict">Whether to throw on type mismatches or coerce to null.</param>
    /// <param name="inferSchemaLength">Maximum number of rows to sample for schema inference.</param>
    /// <returns>An array of column names and corresponding columns.</returns>
    public static (string Name, TColumn Column)[] Transpose<TColumn>(
        IReadOnlyList<IDictionary<string, object?>> records,
        IColumnSink<TColumn> sink,
        IReadOnlyList<string>? targetKeys = null,
        bool strict = true,
        int? inferSchemaLength = 100) where TColumn : class
    {
        int recordCount = records.Count;
        if (recordCount == 0) return [];

        int rowsToInfer = Math.Min(inferSchemaLength ?? recordCount, recordCount);
        var columnTypeMap = new Dictionary<string, Type?>(StringComparer.Ordinal);

        // ==========================================
        // 1. Schema Inference Phase
        // ==========================================
        if (targetKeys?.Count > 0)
        {
            for (int i = 0; i < targetKeys.Count; i++)
            {
                columnTypeMap[targetKeys[i]] = null;
            }

            for (int rowIdx = 0; rowIdx < rowsToInfer; rowIdx++)
            {
                var row = records[rowIdx];
                for (int keyIdx = 0; keyIdx < targetKeys.Count; keyIdx++)
                {
                    string colName = targetKeys[keyIdx];
                    if (row.TryGetValue(colName, out var val) && val != null)
                    {
                        Type newType = val.GetType();
                        Type? existingType = columnTypeMap[colName];

                        columnTypeMap[colName] = existingType == null
                            ? newType
                            : (existingType != newType ? ClrTypeHelper.PromoteType(existingType, newType) : existingType);
                    }
                }
            }
        }
        else
        {
            // First collect all keys across the infer window to preserve stable order
            for (int rowIdx = 0; rowIdx < rowsToInfer; rowIdx++)
            {
                var row = records[rowIdx];
                foreach (KeyValuePair<string, object?> kv in row)
                {
                    if (!columnTypeMap.ContainsKey(kv.Key))
                    {
                        columnTypeMap[kv.Key] = null;
                    }
                }
            }

            // Then infer types across rows
            for (int rowIdx = 0; rowIdx < rowsToInfer; rowIdx++)
            {
                var row = records[rowIdx];
                foreach (KeyValuePair<string, object?> kv in row)
                {
                    if (kv.Value != null)
                    {
                        Type newType = kv.Value.GetType();
                        Type? existingType = columnTypeMap[kv.Key];

                        columnTypeMap[kv.Key] = existingType == null
                            ? newType
                            : (existingType != newType ? ClrTypeHelper.PromoteType(existingType, newType) : existingType);
                    }
                }
            }
        }

        // ==========================================
        // 2. Pre-Allocate Buffers & Cache Appenders
        // ==========================================
        var orderedKeys = targetKeys ?? columnTypeMap.Keys.ToArray();
        int numCols = orderedKeys.Count;

        var buffers = new IColumnBuffer[numCols];
        var appenders = new Action<IColumnBuffer, object?, bool>[numCols];

        for (int i = 0; i < numCols; i++)
        {
            string key = orderedKeys[i];
            Type resolvedType = columnTypeMap.TryGetValue(key, out var t) && t != null ? t : typeof(string);
            buffers[i] = ColumnBufferFactory.Create(resolvedType, recordCount);
            appenders[i] = DictBufferAppenderFactory.GetAppender(resolvedType);
        }

        // ==========================================
        // 3. Fast Row Loading Phase
        // ==========================================
        for (int rowIdx = 0; rowIdx < recordCount; rowIdx++)
        {
            var row = records[rowIdx];
            for (int colIdx = 0; colIdx < numCols; colIdx++)
            {
                string colName = orderedKeys[colIdx];

                // Explicitly guard against missing key leaving 'val' undefined
                if (!row.TryGetValue(colName, out var val))
                {
                    val = null;
                }

                try
                {
                    appenders[colIdx](buffers[colIdx], val, strict);
                }
                catch (Exception ex) when (strict && ex is not InvalidCastException)
                {
                    throw new InvalidCastException($"Strict mode error on column '{colName}' with value '{val}': {ex.Message}", ex);
                }
            }
        }

        // ==========================================
        // 4. Materialize to Column Tuples
        // ==========================================
        var results = new (string Name, TColumn Column)[numCols];
        for (int i = 0; i < numCols; i++)
        {
            results[i] = (orderedKeys[i], buffers[i].ToColumn(orderedKeys[i], sink));
        }

        return results;
    }
}
