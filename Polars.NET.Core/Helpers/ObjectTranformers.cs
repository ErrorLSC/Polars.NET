namespace Polars.NET.Core.Helpers;

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;

/// <summary>
/// Internal columnar abstraction for buffering row values and exporting to a SeriesHandle.
/// </summary>
public interface IColumnBuffer
{
    void AddObject(object? value);
    SeriesHandle ToSeriesHandle(string name);
}

/// <summary>
/// Strongly-typed column buffer backed by a contiguous array.
/// Dispatches to SeriesFactory.CreateSpan to avoid memory duplication.
/// </summary>
/// <typeparam name="T">The scalar or nullable element type.</typeparam>
internal sealed class ColumnBuffer<T>(int capacity) : IColumnBuffer
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

        var underlying = PolarsTypeHelper.GetUnderlyingOrSelf(typeof(T));
        Append((T?)Convert.ChangeType(value, underlying));
    }

    public SeriesHandle ToSeriesHandle(string name)
    {
        ReadOnlySpan<T?> span = _buffer.AsSpan(0, _count);
        var handle = SeriesFactory.CreateSpan(name, span);
        if (handle == null || handle.IsInvalid)
        {
            return SeriesFactory.CreateGenericType(name, _buffer.Take(_count));
        }
        return handle;
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private void Resize()
    {
        int newCap = _buffer.Length == 0 ? 16 : _buffer.Length * 2;
        Array.Resize(ref _buffer, newCap);
    }
}

/// <summary>
/// Fallback buffer for complex nested or non-primitive types via Arrow engine.
/// </summary>
internal sealed class StructColumnBuffer<T>(int capacity) : IColumnBuffer
{
    private readonly List<T?> _items = new(capacity);

    public void AddObject(object? value)
    {
        _items.Add((T?)value);
    }

    public SeriesHandle ToSeriesHandle(string name)
    {
        if (_items.Count == 0)
        {
            return SeriesFactory.CreateGenericType(name, Array.Empty<T>());
        }

        return SeriesFactory.CreateGenericType(name, _items);
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
        if (PolarsTypeHelper.IsSupportedSimpleType(propType))
        {
            Type targetType = PolarsTypeHelper.EnsureNullable(propType);
            return SimpleFactories.GetOrAdd(targetType, static t =>
            {
                var constructed = typeof(ColumnBuffer<>).MakeGenericType(t);
                var ctor = constructed.GetConstructor([typeof(int)])!;
                var param = Expression.Parameter(typeof(int), "cap");
                return Expression.Lambda<Func<int, IColumnBuffer>>(Expression.New(ctor, param), param).Compile();
            })(capacity);
        }

        return StructFactories.GetOrAdd(propType, static t =>
        {
            var constructed = typeof(StructColumnBuffer<>).MakeGenericType(t);
            var ctor = constructed.GetConstructor([typeof(int)])!;
            var param = Expression.Parameter(typeof(int), "cap");
            return Expression.Lambda<Func<int, IColumnBuffer>>(Expression.New(ctor, param), param).Compile();
        })(capacity);
    }
}

/// <summary>
/// High-performance compiled object-to-columnar transposer pipeline for POCO and F# Records.
/// </summary>
/// <typeparam name="T">The record or POCO row model type.</typeparam>
public static class ObjectSchemaTransposer<T>
{
    private static readonly PropertyInfo[] Properties;
    private static readonly bool CanUseBatchFastPath;
    private static readonly Func<T[], SeriesHandle[]>? BatchFastTransposer;
    private static readonly Func<SeriesHandle[]>? EmptySeriesFactory;
    private static readonly Action<T, IColumnBuffer[]>? StreamingRowWriter;

    static ObjectSchemaTransposer()
    {
        Type rowType = typeof(T);
        Properties = PolarsTypeHelper.GetModelProperties(rowType);

        if (Properties.Length == 0)
        {
            return;
        }

        CanUseBatchFastPath = Array.TrueForAll(Properties, static p => PolarsTypeHelper.IsSupportedSimpleType(p.PropertyType));

        if (CanUseBatchFastPath)
        {
            BatchFastTransposer = CompileBatchTransposer(Properties);
            EmptySeriesFactory = CompileEmptySeriesFactory(Properties);
        }

        StreamingRowWriter = CompileStreamingWriter(Properties);
    }

    public static PropertyInfo[] ModelProperties => Properties;
    public static bool IsBatchFastPathSupported => CanUseBatchFastPath;

    public static SeriesHandle[] Transpose(IEnumerable<T> data)
    {
        ArgumentNullException.ThrowIfNull(data);

        if (Properties.Length == 0)
        {
            return [];
        }

        // 1. Direct array fast path (Straight-line compiled IL loop, zero delegate calls)
        if (CanUseBatchFastPath && BatchFastTransposer != null)
        {
            if (data is T[] array)
            {
                if (array.Length == 0)
                {
                    return EmptySeriesFactory!();
                }
                return BatchFastTransposer(array);
            }
        }

        // 2. Fallback streaming path (single pass row consumption)
        return TransposeStreaming(data);
    }

    public static SeriesHandle[] CreateEmptySeriesHandles()
    {
        if (EmptySeriesFactory != null)
        {
            return EmptySeriesFactory();
        }

        var handles = new SeriesHandle[Properties.Length];
        for (int i = 0; i < Properties.Length; i++)
        {
            var p = Properties[i];
            var buffer = ColumnBufferFactory.Create(p.PropertyType, 0);
            handles[i] = buffer.ToSeriesHandle(p.Name);
        }
        return handles;
    }

    public static IColumnBuffer[] CreateBuffers(int capacity)
    {
        var buffers = new IColumnBuffer[Properties.Length];
        for (int i = 0; i < Properties.Length; i++)
        {
            buffers[i] = ColumnBufferFactory.Create(Properties[i].PropertyType, capacity);
        }
        return buffers;
    }

    public static Action<T, IColumnBuffer[]> RowWriter => StreamingRowWriter!;

    private static SeriesHandle[] TransposeStreaming(IEnumerable<T> data)
    {
        int capacity = data is ICollection<T> col ? col.Count : 16;
        var buffers = new IColumnBuffer[Properties.Length];

        for (int i = 0; i < Properties.Length; i++)
        {
            buffers[i] = ColumnBufferFactory.Create(Properties[i].PropertyType, capacity);
        }

        var writer = StreamingRowWriter!;
        foreach (var item in data)
        {
            writer(item, buffers);
        }

        var handles = new SeriesHandle[Properties.Length];
        for (int i = 0; i < Properties.Length; i++)
        {
            handles[i] = buffers[i].ToSeriesHandle(Properties[i].Name);
        }

        return handles;
    }

    #region Expression Compilers

    /// <summary>
    /// Compiles an unrolled batch transposer.
    /// Eliminates all reflection and delegate overhead by compiling the projection loop directly into IL.
    /// </summary>
    private static Func<T[], SeriesHandle[]> CompileBatchTransposer(PropertyInfo[] props)
    {
        var recordsParam = Expression.Parameter(typeof(T[]), "records");
        var seriesExpressions = new Expression[props.Length];

        var createColMethodDef = typeof(ObjectSchemaTransposer<T>)
            .GetMethod(nameof(CreateColumnHandleFromExtracted), BindingFlags.NonPublic | BindingFlags.Static)!;

        for (int i = 0; i < props.Length; i++)
        {
            var prop = props[i];
            Type fieldType = prop.PropertyType;

            // Generate compiled IL: (T[] data) -> TField[] colData
            var compiledExtractor = BuildCompiledExtractor(prop, fieldType);

            var specificMethod = createColMethodDef.MakeGenericMethod(fieldType);
            var callExpr = Expression.Call(
                null,
                specificMethod,
                recordsParam,
                Expression.Constant(prop.Name, typeof(string)),
                Expression.Constant(compiledExtractor)
            );

            seriesExpressions[i] = callExpr;
        }

        var newArrayExpr = Expression.NewArrayInit(typeof(SeriesHandle), seriesExpressions);
        return Expression.Lambda<Func<T[], SeriesHandle[]>>(newArrayExpr, recordsParam).Compile();
    }

    /// <summary>
    /// Builds straight-line IL projection:
    /// var col = new TField[records.Length];
    /// for(int i = 0; i < records.Length; i++) col[i] = records[i].Prop;
    /// return col;
    /// </summary>
    private static object BuildCompiledExtractor(PropertyInfo prop, Type fieldType)
    {
        var recordsParam = Expression.Parameter(typeof(T[]), "data");
        var lenVar = Expression.Variable(typeof(int), "len");
        var colDataVar = Expression.Variable(fieldType.MakeArrayType(), "colData");
        var indexVar = Expression.Variable(typeof(int), "i");
        var breakLabel = Expression.Label("loopBreak");

        var lenAssign = Expression.Assign(lenVar, Expression.ArrayLength(recordsParam));
        var colDataAssign = Expression.Assign(colDataVar, Expression.NewArrayBounds(fieldType, lenVar));
        var indexInit = Expression.Assign(indexVar, Expression.Constant(0));

        var loop = Expression.Loop(
            Expression.Block(
                Expression.IfThen(
                    Expression.GreaterThanOrEqual(indexVar, lenVar),
                    Expression.Goto(breakLabel)
                ),
                Expression.Assign(
                    Expression.ArrayAccess(colDataVar, indexVar),
                    Expression.Property(Expression.ArrayAccess(recordsParam, indexVar), prop)
                ),
                Expression.PostIncrementAssign(indexVar)
            ),
            breakLabel
        );

        var body = Expression.Block(
            [lenVar, colDataVar, indexVar],
            lenAssign,
            colDataAssign,
            indexInit,
            loop,
            colDataVar
        );

        var delegateType = typeof(Func<,>).MakeGenericType(typeof(T[]), fieldType.MakeArrayType());
        return Expression.Lambda(delegateType, body, recordsParam).Compile();
    }

    private static SeriesHandle CreateColumnHandleFromExtracted<TField>(
        T[] records,
        string name,
        Func<T[], TField[]> extractor)
    {
        TField[] colData = extractor(records);
        var handle = SeriesFactory.CreateSpan<TField>(name, colData.AsSpan());
        
        // Fallback for types not handled by CreateSpan SIMD/Unsafe paths
        if (handle == null || handle.IsInvalid)
        {
            return SeriesFactory.CreateGenericType(name, colData);
        }

        return handle;
    }

    private static Func<SeriesHandle[]> CompileEmptySeriesFactory(PropertyInfo[] props)
    {
        var seriesExpressions = new Expression[props.Length];

        var createEmptyColMethodDef = typeof(ObjectSchemaTransposer<T>)
            .GetMethod(nameof(CreateEmptyColumnHandle), BindingFlags.NonPublic | BindingFlags.Static)!;

        for (int i = 0; i < props.Length; i++)
        {
            var prop = props[i];
            Type fieldType = prop.PropertyType;

            var specificMethod = createEmptyColMethodDef.MakeGenericMethod(fieldType);
            var callExpr = Expression.Call(
                null,
                specificMethod,
                Expression.Constant(prop.Name, typeof(string))
            );

            seriesExpressions[i] = callExpr;
        }

        var newArrayExpr = Expression.NewArrayInit(typeof(SeriesHandle), seriesExpressions);
        return Expression.Lambda<Func<SeriesHandle[]>>(newArrayExpr).Compile();
    }

    private static SeriesHandle CreateEmptyColumnHandle<TField>(string colName)
    {
        var handle = SeriesFactory.CreateSpan(colName, ReadOnlySpan<TField>.Empty);
        if (handle == null || handle.IsInvalid)
        {
            return SeriesFactory.CreateGenericType(colName, Array.Empty<TField>());
        }
        return handle;
    }

    private static Action<T, IColumnBuffer[]> CompileStreamingWriter(PropertyInfo[] props)
    {
        var itemParam = Expression.Parameter(typeof(T), "item");
        var buffersParam = Expression.Parameter(typeof(IColumnBuffer[]), "buffers");
        var blockExpressions = new List<Expression>();

        var addObjMethod = typeof(IColumnBuffer).GetMethod(nameof(IColumnBuffer.AddObject))!;

        for (int i = 0; i < props.Length; i++)
        {
            var prop = props[i];
            var propType = prop.PropertyType;
            var propAccess = Expression.Property(itemParam, prop);
            var bufferElement = Expression.ArrayIndex(buffersParam, Expression.Constant(i));

            if (PolarsTypeHelper.IsSupportedSimpleType(propType))
            {
                var targetType = PolarsTypeHelper.EnsureNullable(propType);
                var concreteBufferType = typeof(ColumnBuffer<>).MakeGenericType(targetType);
                var appendMethod = concreteBufferType.GetMethod(nameof(ColumnBuffer<int>.Append), [targetType])!;

                var bufferCast = Expression.Convert(bufferElement, concreteBufferType);
                Expression propVal = propAccess;

                if (targetType != propType)
                {
                    propVal = Expression.Convert(propVal, targetType);
                }

                var appendCall = Expression.Call(bufferCast, appendMethod, propVal);
                blockExpressions.Add(appendCall);
            }
            else
            {
                var addCall = Expression.Call(
                    bufferElement,
                    addObjMethod,
                    Expression.Convert(propAccess, typeof(object))
                );
                blockExpressions.Add(addCall);
            }
        }

        return Expression.Lambda<Action<T, IColumnBuffer[]>>(
            Expression.Block(blockExpressions),
            itemParam,
            buffersParam
        ).Compile();
    }

    #endregion
}

/// <summary>
/// Pre-compiled extractor for Structure-of-Arrays (SoA) object containers.
/// Caches property extractors using Expression Trees to directly generate SeriesHandles without boxing.
/// </summary>
/// <typeparam name="T">The container type (e.g. anonymous type or SoA DTO).</typeparam>
public static class SoAColumnExtractor<T>
{
    private static readonly (string Name, Func<T, string, SeriesHandle> CreateSeriesHandle)[] ColumnFactories;

    static SoAColumnExtractor()
    {
        var properties = PolarsTypeHelper.GetModelProperties(typeof(T));
        var factories = new List<(string Name, Func<T, string, SeriesHandle> CreateSeriesHandle)>();

        var containerParam = Expression.Parameter(typeof(T), "container");
        var nameParam = Expression.Parameter(typeof(string), "colName");

        // MethodInfo for SeriesFactory.CreateGenericType<TElement>(string, IEnumerable<TElement>)
        var createGenericMethod = typeof(SeriesFactory).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(SeriesFactory.CreateGenericType) &&
                        m.IsGenericMethodDefinition &&
                        m.GetParameters().Length == 2);

        foreach (var prop in properties)
        {
            if (!prop.CanRead || prop.GetIndexParameters().Length > 0) continue;

            var propType = prop.PropertyType;
            var propAccess = Expression.Property(containerParam, prop);
            var elementType = PolarsTypeHelper.TryGetEnumerableElementType(propType);

            Expression createHandleExpr;

            if (elementType != null)
            {
                var specializedMethod = createGenericMethod.MakeGenericMethod(elementType);
                var targetEnumerableType = typeof(IEnumerable<>).MakeGenericType(elementType);
                Expression typedProp = propType == targetEnumerableType
                    ? propAccess
                    : Expression.Convert(propAccess, targetEnumerableType);

                createHandleExpr = Expression.Call(specializedMethod, nameParam, typedProp);
            }
            else
            {
                // Fallback for non-generic collections or object sequences
                var castMethod = typeof(Enumerable).GetMethod(nameof(Enumerable.Cast), BindingFlags.Public | BindingFlags.Static)!
                    .MakeGenericMethod(typeof(object));

                var specializedMethod = createGenericMethod.MakeGenericMethod(typeof(object));
                var castCall = Expression.Call(castMethod, Expression.Convert(propAccess, typeof(System.Collections.IEnumerable)));

                createHandleExpr = Expression.Call(specializedMethod, nameParam, castCall);
            }

            var lambda = Expression.Lambda<Func<T, string, SeriesHandle>>(createHandleExpr, containerParam, nameParam);
            factories.Add((prop.Name, lambda.Compile()));
        }

        ColumnFactories = factories.ToArray();
    }

    public static (string Name, Func<T, string, SeriesHandle> CreateSeriesHandle)[] GetFactories() => ColumnFactories;
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
        Type underlying = PolarsTypeHelper.GetUnderlyingOrSelf(type);

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
        if (PolarsTypeHelper.IsSupportedSimpleType(underlying) && underlying.IsValueType)
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
    /// Transposes row-oriented dictionaries into columnar SeriesHandles.
    /// </summary>
    /// <param name="records">The materialized dictionary row records.</param>
    /// <param name="targetKeys">Optional fixed column keys to extract (e.g. from an explicit schema).</param>
    /// <param name="strict">Whether to throw on type mismatches or coerce to null.</param>
    /// <param name="inferSchemaLength">Maximum number of rows to sample for schema inference.</param>
    /// <returns>An array of column names and corresponding SeriesHandles.</returns>
    public static (string Name, SeriesHandle Handle)[] Transpose(
        IReadOnlyList<IDictionary<string, object?>> records,
        IReadOnlyList<string>? targetKeys = null,
        bool strict = true,
        int? inferSchemaLength = 100)
    {
        int recordCount = records.Count;
        if (recordCount == 0) return [];

        int rowsToInfer = Math.Min(inferSchemaLength ?? recordCount, recordCount);
        var columnTypeMap = new Dictionary<string, Type?>(StringComparer.Ordinal);

        // ==========================================
        // 1. Schema Inference Phase
        // ==========================================
        if (targetKeys != null && targetKeys.Count > 0)
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
                            : (existingType != newType ? PolarsTypeHelper.PromoteType(existingType, newType) : existingType);
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
                            : (existingType != newType ? PolarsTypeHelper.PromoteType(existingType, newType) : existingType);
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
        var results = new (string Name, SeriesHandle Handle)[numCols];
        for (int i = 0; i < numCols; i++)
        {
            results[i] = (orderedKeys[i], buffers[i].ToSeriesHandle(orderedKeys[i]));
        }

        return results;
    }
}

/// <summary>
/// High-performance Core factory that ingests row entities or maps directly into a native DataFrameHandle.
/// </summary>
public static class DataFrameBuilder
{
    /// <summary>
    /// Transposes row entities (POCOs, DTOs, Structs, or F# records) into a native DataFrameHandle.
    /// Employs compiled batch straight-line loops for simple scalar arrays, and fallback streaming buffers for complex types.
    /// </summary>
    /// <typeparam name="T">The entity row model type.</typeparam>
    /// <param name="rows">The sequence of row models.</param>
    /// <returns>A native <see cref="DataFrameHandle"/> wrapping the ingested columnar DataFrame.</returns>
    public static DataFrameHandle FromRows<T>(IEnumerable<T> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        // 1. Primitive / Single-Column Fast Path
        Type type = typeof(T);
        if (PolarsTypeHelper.IsSupportedSimpleType(type))
        {
            var seriesHandle = SeriesFactory.CreateGenericType("value", rows);
            return PolarsWrapper.DataFrameNew([seriesHandle]);
        }

        // 2. Delegate to pre-compiled ObjectSchemaTransposer<T>
        SeriesHandle[] seriesHandles = ObjectSchemaTransposer<T>.Transpose(rows);
        if (seriesHandles.Length == 0)
        {
            return PolarsWrapper.DataFrameNew([]);
        }

        // 3. Assemble SeriesHandles directly into DataFrameHandle
        return PolarsWrapper.DataFrameNew(seriesHandles);
    }

    /// <summary>
    /// Transposes dictionary rows into a native DataFrameHandle with automatic schema inference and type promotion.
    /// </summary>
    /// <param name="records">The materialized dictionary row records.</param>
    /// <param name="targetKeys">Optional fixed column names to extract.</param>
    /// <param name="strict">Whether to throw on invalid type casting or coerce into nulls.</param>
    /// <param name="inferSchemaLength">The sampling rows limit for dynamic schema inference.</param>
    /// <returns>A native <see cref="DataFrameHandle"/> wrapping the ingested columnar DataFrame.</returns>
    public static DataFrameHandle FromDicts(
        IReadOnlyList<IDictionary<string, object?>> records,
        IReadOnlyList<string>? targetKeys = null,
        bool strict = true,
        int? inferSchemaLength = 100)
    {
        ArgumentNullException.ThrowIfNull(records);

        var colTuples = DictSchemaTransposer.Transpose(records, targetKeys, strict, inferSchemaLength);
        if (colTuples.Length == 0)
        {
            return PolarsWrapper.DataFrameNew([]);
        }

        var handles = new SeriesHandle[colTuples.Length];
        for (int i = 0; i < colTuples.Length; i++)
        {
            handles[i] = colTuples[i].Handle;
        }

        return PolarsWrapper.DataFrameNew(handles);
    }
    /// <summary>
    /// Ingests a generic sequence of dictionary/map rows into a native DataFrameHandle.
    /// Handles dynamic schema inference, type promotion, and columnar transposition entirely in Core.
    /// </summary>
    /// <param name="data">The row dictionaries (supports F# Map, Dictionary, etc.).</param>
    /// <param name="targetKeys">Optional target column keys to project.</param>
    /// <param name="strict">Whether to throw on type conversion errors or coerce to null.</param>
    /// <param name="inferSchemaLength">Max number of rows to sample for schema inference.</param>
    /// <returns>A native <see cref="DataFrameHandle"/> containing transposed columns.</returns>
    public static DataFrameHandle FromDicts(
        IEnumerable<IDictionary<string, object?>> data,
        IReadOnlyList<string>? targetKeys = null,
        bool strict = true,
        int? inferSchemaLength = 100)
    {
        ArgumentNullException.ThrowIfNull(data);

        var records = data as IReadOnlyList<IDictionary<string, object?>> ?? [.. data];
        if (records.Count == 0)
        {
            return PolarsWrapper.DataFrameNew([]);
        }

        var colTuples = DictSchemaTransposer.Transpose(records, targetKeys, strict, inferSchemaLength);
        if (colTuples.Length == 0)
        {
            return PolarsWrapper.DataFrameNew([]);
        }

        var handles = new SeriesHandle[colTuples.Length];
        for (int i = 0; i < colTuples.Length; i++)
        {
            handles[i] = colTuples[i].Handle;
        }

        return PolarsWrapper.DataFrameNew(handles);
    }
    /// <summary>
    /// Creates a native DataFrameHandle from an object where properties represent column data collections (Structure-of-Arrays).
    /// Leverages compiled expression trees in SoAColumnExtractor to eliminate reflection overhead on repeated container types.
    /// Compatible with C# anonymous objects and F# anonymous records.
    /// </summary>
    /// <typeparam name="T">The container model type.</typeparam>
    /// <param name="columns">The column container instance.</param>
    /// <returns>A native <see cref="DataFrameHandle"/>.</returns>
    public static DataFrameHandle FromColumns<T>(T columns) where T : class
    {
        ArgumentNullException.ThrowIfNull(columns);

        var factories = SoAColumnExtractor<T>.GetFactories();
        if (factories.Length == 0)
        {
            return PolarsWrapper.DataFrameNew(ReadOnlySpan<SeriesHandle>.Empty);
        }

        var handles = new SeriesHandle[factories.Length];
        for (int i = 0; i < factories.Length; i++)
        {
            var (name, createSeriesHandle) = factories[i];
            handles[i] = createSeriesHandle(columns, name);
        }

        return PolarsWrapper.DataFrameNew(handles);
    }

    /// <summary>
    /// Non-generic reflection-based fallback for column container objects.
    /// </summary>
    [RequiresUnreferencedCode("Uses reflection to extract properties from unconstrained objects.")]
    public static DataFrameHandle FromColumns(object columns)
    {
        ArgumentNullException.ThrowIfNull(columns);

        var properties = PolarsTypeHelper.GetModelProperties(columns.GetType());
        if (properties.Length == 0)
        {
            return PolarsWrapper.DataFrameNew(ReadOnlySpan<SeriesHandle>.Empty);
        }

        var handles = new List<SeriesHandle>(properties.Length);

        for (int i = 0; i < properties.Length; i++)
        {
            var p = properties[i];
            var val = p.GetValue(columns) 
                ?? throw new ArgumentNullException(nameof(columns), $"Column property '{p.Name}' cannot be null.");

            var elemType = PolarsTypeHelper.TryGetEnumerableElementType(val.GetType()) ?? typeof(object);
            var method = typeof(SeriesFactory).GetMethod(
                nameof(SeriesFactory.CreateGenericType),
                BindingFlags.Public | BindingFlags.Static
            )!.MakeGenericMethod(elemType);

            var handle = (SeriesHandle)method.Invoke(null, [p.Name, val])!;
            handles.Add(handle);
        }

        return PolarsWrapper.DataFrameNew(handles.ToArray());
    }

    /// <summary>
    /// Ingests an explicit sequence of named column enumerables into a native DataFrameHandle.
    /// </summary>
    public static DataFrameHandle FromColumns(IEnumerable<(string Name, Array Data)> columns)
    {
        ArgumentNullException.ThrowIfNull(columns);

        var handles = new List<SeriesHandle>();
        foreach (var (name, data) in columns)
        {
            var handle = SeriesFactory.Create(name, data);
            handles.Add(handle);
        }

        return PolarsWrapper.DataFrameNew(handles.ToArray());
    }
    
}