using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using System.Data;
using System.Reflection;
using Polars.NET.Core.Helpers;
using Apache.Arrow.Ipc;
using System.Diagnostics.CodeAnalysis;
using System.Buffers;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

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

        // 1. Primitive / Single Scalar column fast path
        if (PolarsTypeHelper.IsSupportedSimpleType(type))
        {
            var s = Series.From("value", data);
            return [s];
        }

        // 2. Transpose row objects into SeriesHandles via the unified Core transposer
        SeriesHandle[] handles = ObjectSchemaTransposer<T>.Transpose(data);
        if (handles.Length == 0)
        {
            return [];
        }

        // Wrap raw SeriesHandles into C# Series objects
        Series[] series = new Series[handles.Length];
        for (int i = 0; i < handles.Length; i++)
        {
            series[i] = new Series(handles[i]);
        }

        return [.. series];
    }
    /// <inheritdoc cref="From"/>
    public static DataFrame FromRows<T>(IEnumerable<T> data)
        => From(data);

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
            var (name, createSeriesHandle) = factories[i];
            SeriesHandle handle = createSeriesHandle(columns, name);
            seriesList[i] = new Series(handle);
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
        var seriesList = new List<Series>(properties.Length);

        foreach (var p in properties)
        {
            if (!p.CanRead || p.GetIndexParameters().Length > 0) continue;

            var val = p.GetValue(columns) 
                ?? throw new ArgumentNullException(nameof(columns), $"Property '{p.Name}' cannot be null.");

            var elemType = PolarsTypeHelper.TryGetEnumerableElementType(val.GetType()) ?? typeof(object);
            var method = typeof(SeriesFactory).GetMethod(
                nameof(SeriesFactory.CreateGenericType),
                BindingFlags.Public | BindingFlags.Static
            )!.MakeGenericMethod(elemType);

            var handle = (SeriesHandle)method.Invoke(null, [p.Name, val])!;
            seriesList.Add(new Series(handle));
        }

        return [.. seriesList];
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
        return new(handle);
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

        return new(handle);
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

        var records = data as IReadOnlyList<IDictionary<string, object?>> ?? data.ToList();
        if (records.Count == 0) return [];

        try
        {
            var actualSchema = schema?.Consume();
            var overrides = schemaOverrides?.Consume();

            IReadOnlyList<string>? targetKeys = actualSchema?.Keys.ToList();

            // 1. Delegate schema inference and buffer transpose to Core
            var columnResults = DictSchemaTransposer.Transpose(
                records,
                targetKeys,
                strict,
                (int?)inferSchemaLength
            );

            if (columnResults.Length == 0) return [];

            // 2. Wrap into C# Series array
            var seriesList = new Series[columnResults.Length];
            for (int i = 0; i < columnResults.Length; i++)
            {
                seriesList[i] = new Series(columnResults[i].Handle);
            }

            var df = new DataFrame(seriesList);

            // 3. Schema Overrides / Strict Casting
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