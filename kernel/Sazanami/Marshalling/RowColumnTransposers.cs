namespace Kaminari.Sazanami.Marshalling;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using Kaminari.Sazanami.Data.Mapping;

/// <summary>
/// Where finished columns go. An engine implements this to receive columns in its own representation.
/// </summary>
/// <typeparam name="TColumn">The engine's column type.</typeparam>
public interface IColumnSink<TColumn> where TColumn : class
{
    /// <summary>Builds a column from contiguous values, or returns null when this path does not support <typeparamref name="T"/>.</summary>
    TColumn? CreateFromSpan<T>(string name, ReadOnlySpan<T> data);

    /// <summary>Builds a column of any supported element type.</summary>
    TColumn CreateGeneric<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(string name, IEnumerable<T> data);

    /// <summary>
    /// Builds a struct column of <paramref name="fields"/> (columns of one length, in order), null where
    /// <paramref name="isNull"/> says so. The fields stay the caller's.
    /// </summary>
    TColumn CreateStruct(string name, (string Name, TColumn Column)[] fields, ReadOnlySpan<bool> isNull);
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
internal sealed class ColumnBuffer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(int capacity) : IColumnBuffer
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
internal sealed class StructColumnBuffer<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(int capacity) : IColumnBuffer
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
            return sink.CreateGeneric<T>(name, []);
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
            return SimpleFactories.GetOrAdd(targetType, static ([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] t) =>
            {
                var constructed = typeof(ColumnBuffer<>).MakeGenericType(t);
                var ctor = constructed.GetConstructor([typeof(int)])!;
                var param = Expression.Parameter(typeof(int), "cap");
                return Expression.Lambda<Func<int, IColumnBuffer>>(Expression.New(ctor, param), param).Compile();
            })(capacity);
        }

        return StructFactories.GetOrAdd(propType, static ([DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] t) =>
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
    /// <param name="mapColumns">
    /// Columns of nested dictionaries (string keys, object values) to build as maps, their values' type inferred from
    /// the values; elsewhere such dictionaries are records, a struct column of their keys (as JSON objects are).
    /// Dictionaries of typed values are maps either way.
    /// </param>
    /// <returns>An array of column names and corresponding columns.</returns>
    public static (string Name, TColumn Column)[] Transpose<TColumn>(
        IReadOnlyList<IDictionary<string, object?>> records,
        IColumnSink<TColumn> sink,
        IReadOnlyList<string>? targetKeys = null,
        bool strict = true,
        int? inferSchemaLength = 100,
        IReadOnlySet<string>? mapColumns = null) where TColumn : class
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

        var buffers = new IColumnBuffer?[numCols];
        var appenders = new Action<IColumnBuffer, object?, bool>?[numCols];

        for (int i = 0; i < numCols; i++)
        {
            string key = orderedKeys[i];
            Type resolvedType = columnTypeMap.TryGetValue(key, out var t) && t != null ? t : typeof(string);
            if (IsObjectRecord(resolvedType)) continue; // built from the rows' records below
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
                if (buffers[colIdx] is null) continue;
                string colName = orderedKeys[colIdx];

                // Explicitly guard against missing key leaving 'val' undefined
                if (!row.TryGetValue(colName, out var val))
                {
                    val = null;
                }

                try
                {
                    appenders[colIdx]!(buffers[colIdx]!, val, strict);
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
            string name = orderedKeys[i];
            results[i] = (name, buffers[i] is { } buffer
                ? buffer.ToColumn(name, sink)
                : mapColumns?.Contains(name) == true
                    ? MapColumn(records, name, sink, strict, rowsToInfer)
                    : StructColumn(records, name, sink, strict, inferSchemaLength));
        }

        return results;
    }

    /// <summary>Whether values of <paramref name="type"/> are records: dictionaries of string keys and object values.</summary>
    private static bool IsObjectRecord(Type type) =>
        type != typeof(string) && (type.IsGenericType ? [type, .. type.GetInterfaces()] : type.GetInterfaces()).Any(static i => i.IsGenericType
            && (i.GetGenericTypeDefinition() == typeof(IDictionary<,>) || i.GetGenericTypeDefinition() == typeof(IReadOnlyDictionary<,>))
            && i.GetGenericArguments() is [var key, var value] && key == typeof(string) && value == typeof(object));

    private static object? Value(IDictionary<string, object?> row, string column) => row.TryGetValue(column, out object? value) ? value : null;

    /// <summary>A record's entries (an IDictionary or an IReadOnlyDictionary of string keys and object values).</summary>
    private static IDictionary<string, object?> Entries(object record) => record switch
    {
        IDictionary<string, object?> dictionary => dictionary,
        IReadOnlyDictionary<string, object?> readOnly => readOnly.ToDictionary(StringComparer.Ordinal),
        _ => throw new InvalidCastException($"A {record.GetType().Name} is no dictionary of string keys and object values."),
    };

    /// <summary>A column of records as a struct column: their keys transposed as its fields, recursively; null rows null.</summary>
    private static TColumn StructColumn<TColumn>(IReadOnlyList<IDictionary<string, object?>> records, string name, IColumnSink<TColumn> sink,
        bool strict, int? inferSchemaLength) where TColumn : class
    {
        var nested = new IDictionary<string, object?>[records.Count];
        var isNull = new bool[records.Count];
        IDictionary<string, object?> empty = new Dictionary<string, object?>(0);
        for (int i = 0; i < nested.Length; i++)
        {
            object? value = Value(records[i], name);
            isNull[i] = value is null;
            nested[i] = value is null ? empty : Entries(value);
        }
        (string Name, TColumn Column)[] fields = Transpose(nested, sink, null, strict, inferSchemaLength);
        try
        {
            return sink.CreateStruct(name, fields, isNull);
        }
        finally
        {
            foreach (var (_, column) in fields) (column as IDisposable)?.Dispose();
        }
    }

    /// <summary>
    /// A column of records as a map column: string keys, the values' type promoted over the values the inference rows
    /// hold (text when there are none); a value of another type is converted (strict: or refused).
    /// </summary>
    private static TColumn MapColumn<TColumn>(IReadOnlyList<IDictionary<string, object?>> records, string name, IColumnSink<TColumn> sink,
        bool strict, int rowsToInfer) where TColumn : class
    {
        Type? valueType = null;
        for (int i = 0; i < rowsToInfer; i++)
        {
            if (Value(records[i], name) is not { } record) continue;
            foreach (var (_, value) in Entries(record))
                if (value is not null) valueType = valueType is null ? value.GetType() : ClrTypeHelper.PromoteType(valueType, value.GetType());
        }
        Type core = ClrTypeHelper.UnwrapCoreType(valueType ?? typeof(string));
        if (IsObjectRecord(core))
            throw new NotSupportedException(
                $"The map column '{name}' holds dictionaries as values: build its rows from typed dictionaries (e.g. Dictionary<string, Dictionary<string, long>>), or leave the column a struct.");
        Type stored = core.IsValueType ? typeof(Nullable<>).MakeGenericType(core) : core;
        Type dictionaryType = typeof(Dictionary<,>).MakeGenericType(typeof(string), stored);
        Array maps = Array.CreateInstance(dictionaryType, records.Count);
        for (int i = 0; i < records.Count; i++)
        {
            if (Value(records[i], name) is not { } record) continue;
            var map = (System.Collections.IDictionary)Activator.CreateInstance(dictionaryType)!;
            foreach (var (key, value) in Entries(record)) map.Add(key, Converted(value, core, strict, name));
            maps.SetValue(map, i);
        }
        MethodInfo create = typeof(IColumnSink<TColumn>).GetMethod(nameof(IColumnSink<TColumn>.CreateGeneric))!.MakeGenericMethod(dictionaryType);
        try
        {
            return (TColumn)create.Invoke(sink, [name, maps])!;
        }
        catch (TargetInvocationException e) when (e.InnerException is { } inner)
        {
            System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(inner);
            throw;
        }

        static object? Converted(object? value, Type core, bool strict, string column)
        {
            if (value is null || value.GetType() == core) return value;
            try
            {
                return core == typeof(string)
                    ? value is IFormattable formattable ? formattable.ToString(null, System.Globalization.CultureInfo.InvariantCulture) : value.ToString()
                    : Convert.ChangeType(value, core, System.Globalization.CultureInfo.InvariantCulture);
            }
            catch (Exception e) when (e is InvalidCastException or FormatException or OverflowException)
            {
                if (!strict) return null;
                throw new InvalidCastException($"Strict mode error on map column '{column}': the value '{value}' is no {core.Name}.", e);
            }
        }
    }
}
