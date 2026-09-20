using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using System.Data;
using System.Reflection;
using Polars.NET.Core.Helpers;
using Apache.Arrow.Ipc;
using System.Diagnostics.CodeAnalysis;
using System.Buffers;
using System.Runtime.CompilerServices;
using System.Linq.Expressions;
using System.Collections.Concurrent;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// Defines a column buffer that materializes into a Polars Series.
/// </summary>
internal interface IColumnBuffer
{
    void AddObject(object? value);
    Series ToSeries(string name);
}

/// <summary>
/// Strongly-typed column buffer backed by a contiguous pre-allocated array.
/// Eliminates boxing on the hot-path via direct typed appending.
/// </summary>
/// <typeparam name="T">The scalar element type.</typeparam>
internal sealed class ColumnBuffer<T> : IColumnBuffer
{
    private T?[] _buffer;
    private int _count;

    public ColumnBuffer(int capacity)
    {
        _buffer = capacity > 0 ? new T?[capacity] : [];
        _count = 0;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public void Append(T? value)
    {
        if (_count == _buffer.Length)
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

        var underlying = Nullable.GetUnderlyingType(typeof(T)) ?? typeof(T);
        Append((T?)Convert.ChangeType(value, underlying));
    }

    public Series ToSeries(string name)
    {
        // Trim or borrow span directly to avoid extra heap allocations
        ReadOnlySpan<T?> span = _buffer.AsSpan(0, _count);
        return Series.FromSpan(name, span);
    }

    private void Resize()
    {
        int newCap = _buffer.Length == 0 ? 16 : _buffer.Length * 2;
        System.Array.Resize(ref _buffer, newCap);
    }
}

/// <summary>
/// Fallback buffer for complex nested types (DTOs, anonymous classes, Structs)
/// Converts nested objects into a Polars Struct Series using the Arrow engine.
/// </summary>
internal sealed class StructColumnBuffer<T>(int capacity) : IColumnBuffer
{
    private readonly List<T?> _items = new(capacity);

    public void AddObject(object? value)
    {
        _items.Add((T?)value);
    }

    public Series ToSeries(string name)
    {
        if (_items.Count == 0)
        {
            return Series.From(name, System.Array.Empty<T>());
        }

        return Series.From(name, _items.ToArray());
    }
}

/// <summary>
/// Factory for creating strongly-typed column buffers.
/// </summary>
internal static class ColumnBufferFactory
{
    public static IColumnBuffer Create(Type propType, int capacity)
    {
        if (PolarsTypeHelper.IsSupportedSimpleType(propType))
        {
            Type targetType = PolarsTypeHelper.EnsureNullable(propType);
            return BufferInvoker.CreateSimple(targetType, capacity);
        }

        return BufferInvoker.CreateStruct(propType, capacity);
    }

    private static class BufferInvoker
    {
        private static readonly ConcurrentDictionary<Type, Func<int, IColumnBuffer>> SimpleFactories = new();
        private static readonly ConcurrentDictionary<Type, Func<int, IColumnBuffer>> StructFactories = new();

        public static IColumnBuffer CreateSimple(Type t, int cap) =>
            SimpleFactories.GetOrAdd(t, static type =>
            {
                var constructed = typeof(ColumnBuffer<>).MakeGenericType(type);
                var ctor = constructed.GetConstructor([typeof(int)])!;
                var param = Expression.Parameter(typeof(int), "cap");
                return Expression.Lambda<Func<int, IColumnBuffer>>(
                    Expression.New(ctor, param), param).Compile();
            })(cap);

        public static IColumnBuffer CreateStruct(Type t, int cap) =>
            StructFactories.GetOrAdd(t, static type =>
            {
                var constructed = typeof(StructColumnBuffer<>).MakeGenericType(type);
                var ctor = constructed.GetConstructor([typeof(int)])!;
                var param = Expression.Parameter(typeof(int), "cap");
                return Expression.Lambda<Func<int, IColumnBuffer>>(
                    Expression.New(ctor, param), param).Compile();
            })(cap);
    }
}

/// <summary>
/// Pre-compiled high-performance writer that transposes POCO/DTO object properties directly into column buffers.
/// Eliminates property reflection and boxing on the ingestion hot-path.
/// </summary>
/// <typeparam name="T">The POCO/DTO type.</typeparam>
internal static class PocoColumnWriter<T>
{
    private static readonly Action<T, IColumnBuffer[]> CachedWriter;
    private static readonly PropertyInfo[] CachedProperties;

    static PocoColumnWriter()
    {
        var targetType = typeof(T);
        var props = targetType.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var validProps = new List<PropertyInfo>();

        foreach (var p in props)
        {
            if (p.CanRead) validProps.Add(p);
        }

        CachedProperties = [.. validProps];
        CachedWriter = BuildWriter(CachedProperties);
    }

    public static PropertyInfo[] Properties => CachedProperties;
    public static Action<T, IColumnBuffer[]> Writer => CachedWriter;

    private static Action<T, IColumnBuffer[]> BuildWriter(PropertyInfo[] props)
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
                var targetType = propType;
                if (propType.IsValueType && Nullable.GetUnderlyingType(propType) == null)
                {
                    targetType = typeof(Nullable<>).MakeGenericType(propType);
                }

                var concreteBufferType = typeof(ColumnBuffer<>).MakeGenericType(targetType);
                var appendMethod = concreteBufferType.GetMethod("Append", [targetType])!;

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
                // Complex Struct / Anonymous Type -> Fallback to AddObject
                var addCall = Expression.Call(
                    bufferElement,
                    addObjMethod,
                    Expression.Convert(propAccess, typeof(object))
                );
                blockExpressions.Add(addCall);
            }
        }

        var body = Expression.Block(blockExpressions);
        return Expression.Lambda<Action<T, IColumnBuffer[]>>(body, itemParam, buffersParam).Compile();
    }
}
/// <summary>
/// Pre-cached delegate factory for directly appending boxed row values into strong-typed column buffers.
/// </summary>
internal static class DictBufferAppenderFactory
{
    private static readonly ConcurrentDictionary<Type, Action<IColumnBuffer, object?, bool>> Cache = new();

    // Cache open generic MethodInfo: AppendVal<T>(IColumnBuffer, object?, bool)
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
            return (buf, val, strict) =>
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
            if (val == null)
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
/// Pre-compiled extractor for Structure-of-Arrays (SoA) object containers.
/// Caches property getters using Expression Trees to eliminate reflection overhead.
/// </summary>
/// <typeparam name="T">The container type (e.g. anonymous type or SoA DTO).</typeparam>
internal static class SoAColumnExtractor<T>
{
    private static readonly (string Name, Func<T, string, Series> CreateSeries)[] ColumnFactories;

    static SoAColumnExtractor()
    {
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var factories = new List<(string Name, Func<T, string, Series> CreateSeries)>();

        var containerParam = Expression.Parameter(typeof(T), "container");
        var nameParam = Expression.Parameter(typeof(string), "colName");

        var seriesFromMethod = typeof(Series).GetMethods(BindingFlags.Public | BindingFlags.Static)
            .First(m => m.Name == nameof(Series.From) &&
                        m.IsGenericMethodDefinition &&
                        m.GetParameters().Length == 2 &&
                        m.GetParameters()[1].ParameterType.GetGenericTypeDefinition() == typeof(IEnumerable<>));

        foreach (var prop in properties)
        {
            if (!prop.CanRead) continue;

            var propType = prop.PropertyType;
            var propAccess = Expression.Property(containerParam, prop);
            var elementType = PolarsTypeHelper.TryGetEnumerableElementType(propType);

            Expression createSeriesExpr;

            if (elementType != null)
            {
                var specializedFrom = seriesFromMethod.MakeGenericMethod(elementType);
                var targetEnumerableType = typeof(IEnumerable<>).MakeGenericType(elementType);
                Expression typedProp = propType == targetEnumerableType
                    ? propAccess
                    : Expression.Convert(propAccess, targetEnumerableType);

                createSeriesExpr = Expression.Call(specializedFrom, nameParam, typedProp);
            }
            else
            {
                // Fallback directly to Series.From<object?>
                var specializedFrom = seriesFromMethod.MakeGenericMethod(typeof(object));
                createSeriesExpr = Expression.Call(
                    specializedFrom,
                    nameParam,
                    Expression.Convert(propAccess, typeof(IEnumerable<object>))
                );
            }

            var lambda = Expression.Lambda<Func<T, string, Series>>(createSeriesExpr, containerParam, nameParam);
            factories.Add((prop.Name, lambda.Compile()));
        }

        ColumnFactories = [.. factories];
    }

    public static (string Name, Func<T, string, Series> CreateSeries)[] GetFactories() => ColumnFactories;
}

public partial class DataFrame : IDisposable, IEnumerable<Series>, IPolarsDataFrame
{

    // ==========================================
    // Object Mapping (From Records)
    // ==========================================
    /// <summary>
    /// Create a DataFrame from a collection of strongly-typed objects (POCOs).
    /// <para>
    /// This method uses reflection to inspect the properties of the class <typeparamref name="T"/>
    /// and maps them to DataFrame columns.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The class type of the records.</typeparam>
    /// <param name="data">The collection of objects to load.</param>
    /// <returns>A new DataFrame.</returns>
    /// <example>
    /// <code>
    /// public class Student
    /// {
    ///     public string Name { get; set; }
    ///     public int Age { get; set; }
    ///     public double GPA { get; set; }
    /// }
    ///
    /// var students = new List&lt;Student&gt;
    /// {
    ///     new Student { Name = "Alice", Age = 20, GPA = 3.5 },
    ///     new Student { Name = "Bob", Age = 22, GPA = 3.8 },
    ///     new Student { Name = "Charlie", Age = 19, GPA = 3.2 }
    /// };
    ///
    /// var df = DataFrame.From(students);
    /// df.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────────┬─────┬─────┐
    /// │ Name    ┆ Age ┆ GPA │
    /// │ ---     ┆ --- ┆ --- │
    /// │ str     ┆ i32 ┆ f64 │
    /// ╞═════════╪═════╪═════╡
    /// │ Alice   ┆ 20  ┆ 3.5 │
    /// │ Bob     ┆ 22  ┆ 3.8 │
    /// │ Charlie ┆ 19  ┆ 3.2 │
    /// └─────────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public static DataFrame From<T>(IEnumerable<T> data)
    {
        if (data == null) return [];
        Type type = typeof(T);

        // =========================================================
        // 1. Primitive Types (Single Column)
        // =========================================================
        if (PolarsTypeHelper.IsSupportedSimpleType(type))
        {
            var s = Series.From("value", data);
            return [s];
        }

        // =========================================================
        // 2. Complex Type: Pivot (Row -> Column)
        // =========================================================
        return FromPocoManual(data, type);
    }
    /// <inheritdoc cref="From"/>
    public static DataFrame FromRows<T>(IEnumerable<T> data)
        => From(data);

    private static DataFrame FromPocoManual<T>(IEnumerable<T> data, Type type)
    {
        var props = PocoColumnWriter<T>.Properties;
        if (props.Length == 0) return [];

        int capacity = data is ICollection<T> c ? c.Count : 16;
        var buffers = new IColumnBuffer[props.Length];

        for (int i = 0; i < props.Length; i++)
        {
            buffers[i] = ColumnBufferFactory.Create(props[i].PropertyType, capacity);
        }

        // Use pre-compiled static delegate
        var writer = PocoColumnWriter<T>.Writer;
        foreach (var item in data)
        {
            writer(item, buffers);
        }

        Series[] seriesList = new Series[props.Length];
        for (int i = 0; i < props.Length; i++)
        {
            seriesList[i] = buffers[i].ToSeries(props[i].Name);
        }

        return [.. seriesList];
    }
    /// <summary>
    /// Creates a DataFrame from an object where properties represent column data arrays or lists (Structure of Arrays layout).
    /// Leverages compiled expression trees to eliminate reflection on repeated container types.
    /// </summary>
    /// <example>
    /// var df = DataFrame.FromColumns(new {
    ///     Time = new[] { dt1, dt2 },
    ///     Val = new[] { 1.0, 2.0 }
    /// });
    /// </example>
    public static DataFrame FromColumns<T>(T columns) where T : class
    {
        ArgumentNullException.ThrowIfNull(columns);

        var factories = SoAColumnExtractor<T>.GetFactories();
        if (factories.Length == 0) return [];

        var seriesList = new Series[factories.Length];

        for (int i = 0; i < factories.Length; i++)
        {
            var (name, createSeries) = factories[i];
            // Inlined execution of the pre-compiled Series.From<TElement> delegate
            seriesList[i] = createSeries(columns, name);
        }

        return [.. seriesList];
    }

    /// <summary>
    /// Non-generic backward-compatible overload for Structure of Arrays data layout.
    /// </summary>
    [RequiresUnreferencedCode("Uses reflection to extract properties from unconstrained objects.")]
    public static DataFrame FromColumns(object columns)
    {
        ArgumentNullException.ThrowIfNull(columns);

        var properties = columns.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var cols = new (string Name, object Data)[properties.Length];

        for (int i = 0; i < properties.Length; i++)
        {
            var p = properties[i];
            var val = p.GetValue(columns) ?? throw new ArgumentNullException(nameof(columns), $"Property '{p.Name}' cannot be null.");
            cols[i] = (p.Name, val);
        }

        return FromColumns(cols);
    }
    /// <summary>
    /// Create DataFrame from explicitly named columns.
    /// No reflection used. Best performance.
    /// </summary>
    public static DataFrame FromColumns(params (string Name, object Data)[] columns)
    {
        if (columns == null || columns.Length == 0)
            return []; // Return empty DF

        var seriesList = new List<Series>(columns.Length);

        foreach (var (name, val) in columns.AsSpan())
        {
            if (val == null)
                throw new ArgumentNullException($"Column '{name}' data cannot be null.");

            try
            {

                if (val is System.Array arr)
                {
                    var handle = SeriesFactory.Create(name, arr);
                    seriesList.Add(new Series(handle));
                }
                else
                {
                    seriesList.Add(Series.From(name, (dynamic)val));
                }
            }
            catch (Exception ex)
            {
                throw new NotSupportedException($"Column '{name}' has unsupported data type: {val.GetType().Name}.", ex);
            }
        }

        return [.. seriesList];
    }

    /// <summary>
    /// Create a DataFrame from a collection of Series.
    /// </summary>
    public DataFrame(params Series[] series)
    {
        if (series == null || series.Length == 0)
        {
            Handle = PolarsWrapper.DataFrameNew([]);
            return;
        }

        var handles = series.Select(s => s.Handle).ToArray();

        Handle = PolarsWrapper.DataFrameNew(handles);
    }
    /// <inheritdoc cref="DataFrame(Series[])"/>
    public DataFrame(ReadOnlySpan<Series> series)
    {
        if (series.Length == 0)
        {
            Handle = PolarsWrapper.DataFrameNew([]);
            return;
        }

        var pool = ArrayPool<SeriesHandle>.Shared;
        SeriesHandle[] rentedArray = pool.Rent(series.Length);

        try
        {
            for (int i = 0; i < series.Length; i++)
            {
                rentedArray[i] = series[i].Handle;
            }

            Handle = PolarsWrapper.DataFrameNew(new ReadOnlySpan<SeriesHandle>(rentedArray, 0, series.Length));
        }
        finally
        {
            pool.Return(rentedArray, clearArray: true);
        }
    }
    /// <summary>
    /// Create a DataFrame from a collection of Series.
    /// <para>
    /// This is the functional equivalent of the constructor <c>new DataFrame(series)</c>,
    /// provided for consistency with other <c>From...</c> factory methods.
    /// </para>
    /// </summary>
    /// <param name="series">The series to combine into a DataFrame.</param>
    /// <returns>A new DataFrame containing the provided series.</returns>
    /// <exception cref="ArgumentException">Thrown if series have different lengths.</exception>
    /// <example>
    /// <code>
    /// var s1 = new Series("id", [1, 2, 3]);
    /// var s2 = new Series("name", ["Alice", "Bob", "Charlie"]);
    ///
    /// var df = DataFrame.FromSeries(s1, s2);
    /// Console.WriteLine(df);
    /// </code>
    /// </example>
    public static DataFrame FromSeries(params Series[] series)
        => new(series);
    /// <inheritdoc cref="FromSeries(Series[])"/>
    public static DataFrame FromColumns(params Series[] series)
        => [.. series];
    /// <inheritdoc cref="FromSeries(Series[])"/>
    public static DataFrame FromSeries(IEnumerable<Series> series)
        => new([.. series]);
    /// <inheritdoc cref="FromSeries(Series[])"/>
    public static DataFrame FromSeries(ReadOnlySpan<Series> series)
        => new(series);

    /// <summary>
    /// Stream C# objects into Polars.
    /// </summary>
    public static DataFrame FromEnumerable<T>(IEnumerable<T> data, int batchSize = 100_000, Apache.Arrow.Schema? providedSchema = null)
    {
        var schema = providedSchema ?? ArrowConverter.GetSchemaFromType<T>();
        var stream = data.ToArrowBatches(batchSize);

        var handle = ArrowStreamInterop.ImportEager(stream, schema);

        if (handle.IsInvalid) return From(Enumerable.Empty<T>());
        return new DataFrame(handle);
    }
    /// <summary>
    /// Stream data into Polars using Arrow C Stream Interface.
    /// This method supports datasets larger than available RAM by streaming chunks directly to Polars.
    /// </summary>
    /// <param name="data">Source data collection</param>
    /// <param name="batchSize">Rows per chunk (default 100,000)</param>
    /// <param name="providedSchema">Stream schema provided by user</param>
    [Obsolete("Renamed to FromEnumerable")]
    public static DataFrame FromArrowStream<T>(IEnumerable<T> data, int batchSize = 100_000, Apache.Arrow.Schema? providedSchema = null)
    {
        var schema = providedSchema ?? ArrowConverter.GetSchemaFromType<T>();
        var stream = data.ToArrowBatches(batchSize);

        var handle = ArrowStreamInterop.ImportEager(stream, schema);

        if (handle.IsInvalid)
        {
            return From(Enumerable.Empty<T>());
        }

        return new DataFrame(handle);
    }

    /// <summary>
    /// Safely consume any foreign Arrow C Stream (Strict mode).
    /// Adapts physical memory layouts (e.g., Utf8View) automatically.
    /// </summary>
    public static DataFrame FromArrowStream(IArrowArrayStream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);

        var handle = ArrowStreamInterop.ImportForeignStream(stream);

        var df = new DataFrame(handle);
        df.HoldResource(stream);
        return df;
    }
    // ==========================================
    // Dictionary Mapping (Dynamic/JSON Data)
    // ==========================================

    /// <summary>
    /// Create a DataFrame from a single dictionary (represents 1 row).
    /// </summary>
    public static DataFrame FromDict(IDictionary<string, object?> data, IntoSchema? schema = null, IntoSchema? schemaOverrides = null, bool strict = true)
        => FromDicts([data], schema, schemaOverrides, strict);

    /// <summary>
    /// Create a DataFrame from a collection of dictionaries.
    /// </summary>
    /// <param name="data">The collection of dictionaries representing rows.</param>
    /// <param name="schema">Strict blueprint. If provided, overrides dynamic inference. Extra keys in data are ignored.</param>
    /// <param name="schemaOverrides">Patch blueprint. Infers all keys, but overrides specific column types.</param>
    /// <param name="strict">If true, throws on unpromotable type mismatch.</param>
    /// <param name="inferSchemaLength">Rows to inspect for C# type inference.</param>
    public static DataFrame FromDicts(
        IEnumerable<IDictionary<string, object?>> data,
        IntoSchema? schema = null,
        IntoSchema? schemaOverrides = null,
        bool strict = true,
        uint? inferSchemaLength = 100)
    {
        if (data == null) return [];

        var records = data as ICollection<IDictionary<string, object?>> ?? [.. data];
        int recordCount = records.Count;
        if (recordCount == 0) return [];

        try
        {
            var actualSchema = schema?.Consume();
            var overrides = schemaOverrides?.Consume();

            // ==========================================
            // 1. Schema Inference Phase
            // ==========================================
            var columnTypeMap = new Dictionary<string, Type?>(StringComparer.Ordinal);
            int rowsToInfer = (int?)inferSchemaLength ?? recordCount;

            if (actualSchema != null)
            {
                foreach (var colName in actualSchema.Keys)
                {
                    columnTypeMap[colName] = null;
                }

                int rowCount = 0;
                var targetKeys = actualSchema.Keys.ToList();

                foreach (var row in records)
                {
                    if (rowCount >= rowsToInfer) break;

                    foreach (var colName in targetKeys)
                    {
                        if (row.TryGetValue(colName, out var val) && val != null)
                        {
                            Type newType = val.GetType();
                            Type? existingType = columnTypeMap[colName];

                            columnTypeMap[colName] = existingType == null
                                ? newType
                                : (existingType != newType ? PolarsTypeHelper.PromoteType(existingType, newType) : existingType);
                        }
                    }
                    rowCount++;
                }
            }
            else
            {
                int rowCount = 0;
                foreach (var row in records)
                {
                    if (rowCount >= rowsToInfer) break;

                    foreach (var (k, v) in row)
                    {
                        if (!columnTypeMap.TryGetValue(k, out Type? existingType))
                        {
                            columnTypeMap[k] = v?.GetType();
                        }
                        else if (v != null)
                        {
                            Type newType = v.GetType();
                            if (existingType == null)
                                columnTypeMap[k] = newType;
                            else if (existingType != newType)
                                columnTypeMap[k] = PolarsTypeHelper.PromoteType(existingType, newType);
                        }
                    }
                    rowCount++;
                }
            }

            // ==========================================
            // 2. Pre-Index Buffers & Typed Appenders
            // ==========================================
            var orderedKeys = (actualSchema != null ? actualSchema.Keys : columnTypeMap.Keys).ToArray();
            int numCols = orderedKeys.Length;

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
            // 3. Fast Row Loading Phase (Zero Dictionary Overhead for Buffers)
            // ==========================================
            foreach (var row in records)
            {
                for (int colIdx = 0; colIdx < numCols; colIdx++)
                {
                    string colName = orderedKeys[colIdx];
                    row.TryGetValue(colName, out var val);

                    try
                    {
                        appenders[colIdx](buffers[colIdx], val, strict);
                    }
                    catch (Exception ex) when (strict && !(ex is InvalidCastException))
                    {
                        throw new InvalidCastException($"Strict mode error on column '{colName}' with value '{val}': {ex.Message}", ex);
                    }
                }
            }

            // ==========================================
            // 4. Materialize to DataFrame
            // ==========================================
            var seriesList = new Series[numCols];
            for (int i = 0; i < numCols; i++)
            {
                seriesList[i] = buffers[i].ToSeries(orderedKeys[i]);
            }

            var df = new DataFrame(seriesList);

            // ==========================================
            // 5. Schema Overrides / Strict Casting
            // ==========================================
            if (actualSchema != null || overrides != null)
            {
                var castExprs = new List<Expr>((int)df.Width);
                foreach (var col in df.Columns)
                {
                    if (actualSchema != null &&
                        actualSchema.TryGetValue(col, out var targetType) &&
                        targetType.Kind != DataTypeKind.Unknown)
                    {
                        castExprs.Add(Pl.Col(col).Cast(targetType));
                    }
                    else if (overrides != null &&
                                overrides.TryGetValue(col, out var overrideType) &&
                                overrideType.Kind != DataTypeKind.Unknown)
                    {
                        castExprs.Add(Pl.Col(col).Cast(overrideType));
                    }
                    else
                    {
                        castExprs.Add(Pl.Col(col));
                    }
                }
                df = df.Select([.. castExprs]);
            }

            return df;
        }
        finally
        {
            if (schema.HasValue)
            {
                schema.Value.DisposeTempSchema();
            }
        }
    }
}
