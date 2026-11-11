using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using Kaminari.Sazanami.Helpers;
using Polars.NET.Core.Wrappers;

namespace Polars.NET.Core.Helpers;

/// <summary>
/// Receives finished columns as native Series.
/// </summary>
public sealed class SeriesHandleSink : IColumnSink<SeriesHandle>
{
    public static readonly SeriesHandleSink Instance = new();

    private SeriesHandleSink() { }

    public SeriesHandle? CreateFromSpan<T>(string name, ReadOnlySpan<T> data)
    {
        var handle = SeriesFactory.CreateSpan(name, data);
        return handle?.IsInvalid == false ? handle : null;
    }

    public SeriesHandle CreateGeneric<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicProperties | DynamicallyAccessedMemberTypes.Interfaces)] T>(string name, IEnumerable<T> data) =>
        SeriesFactory.CreateGenericType(name, data);
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
    public static DataFrameHandle FromRows<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(IEnumerable<T> rows)
    {
        ArgumentNullException.ThrowIfNull(rows);

        // 1. Primitive / Single-Column Fast Path
        Type type = typeof(T);
        if (ClrTypeHelper.IsSupportedSimpleType(type))
        {
            var seriesHandle = SeriesFactory.CreateGenericType("value", rows);
            return PolarsWrapper.DataFrameNew([seriesHandle]);
        }

        // 2. Delegate to the pre-compiled transposer
        SeriesHandle[] seriesHandles = ObjectSchemaTransposer<T, SeriesHandle>.Transpose(rows, SeriesHandleSink.Instance);
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

        var colTuples = DictSchemaTransposer.Transpose(records, SeriesHandleSink.Instance, targetKeys, strict, inferSchemaLength);
        if (colTuples.Length == 0)
        {
            return PolarsWrapper.DataFrameNew([]);
        }

        var handles = new SeriesHandle[colTuples.Length];
        for (int i = 0; i < colTuples.Length; i++)
        {
            handles[i] = colTuples[i].Column;
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

        var colTuples = DictSchemaTransposer.Transpose(records, SeriesHandleSink.Instance, targetKeys, strict, inferSchemaLength);
        if (colTuples.Length == 0)
        {
            return PolarsWrapper.DataFrameNew([]);
        }

        var handles = new SeriesHandle[colTuples.Length];
        for (int i = 0; i < colTuples.Length; i++)
        {
            handles[i] = colTuples[i].Column;
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
    public static DataFrameHandle FromColumns<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.All)] T>(T columns) where T : class
    {
        ArgumentNullException.ThrowIfNull(columns);

        var factories = SoAColumnExtractor<T, SeriesHandle>.GetFactories();
        if (factories.Length == 0)
        {
            return PolarsWrapper.DataFrameNew([]);
        }

        var handles = new SeriesHandle[factories.Length];
        for (int i = 0; i < factories.Length; i++)
        {
            var (name, createColumn) = factories[i];
            handles[i] = createColumn(columns, name, SeriesHandleSink.Instance);
        }

        return PolarsWrapper.DataFrameNew(handles);
    }

    /// <summary>
    /// Non-generic reflection-based fallback for column container objects.
    /// </summary>
    [RequiresUnreferencedCode("Uses reflection to extract properties from unconstrained objects.")]
    [RequiresDynamicCode("Builds generic code for element types only known at run time; with native AOT, use the typed overloads.")]
    public static DataFrameHandle FromColumns(object columns)
    {
        ArgumentNullException.ThrowIfNull(columns);

        var properties = ClrTypeHelper.GetModelProperties(columns.GetType());
        if (properties.Length == 0)
        {
            return PolarsWrapper.DataFrameNew([]);
        }

        var handles = new List<SeriesHandle>(properties.Length);

        for (int i = 0; i < properties.Length; i++)
        {
            var p = properties[i];
            var val = p.GetValue(columns)
                ?? throw new ArgumentNullException(nameof(columns), $"Column property '{p.Name}' cannot be null.");

            var elemType = ClrTypeHelper.TryGetEnumerableElementType(val.GetType()) ?? typeof(object);
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
    [RequiresDynamicCode("Arrays of element types only known at run time (enums, nested types) build generic code; with native AOT, use typed columns.")]
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