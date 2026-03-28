using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Apache.Arrow;
using System.Data;
using Polars.NET.Core.Data;
using System.Reflection;
using Polars.NET.Core.Helpers;
using Apache.Arrow.Ipc;
using System.Diagnostics.CodeAnalysis;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
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
        if (data == null) return new DataFrame();
        Type type = typeof(T);

        // =========================================================
        // 1. Primitive Types (Single Column)
        // =========================================================
        if (IsSimpleType(type))
        {
            var s = Series.From("value", data);
            return new DataFrame(s);
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
        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        if (props.Length == 0) return new DataFrame();

        int capacity = data is ICollection<T> c ? c.Count : 16;
        var buffers = new IColumnBuffer[props.Length];

        for (int i = 0; i < props.Length; i++)
        {
            buffers[i] = ColumnBufferFactory.Create(props[i].PropertyType, capacity);
        }

        foreach (var item in data)
        {
            for (int i = 0; i < props.Length; i++)
            {
                buffers[i].Add(props[i].GetValue(item));
            }
        }

        var seriesList = new Series[props.Length];
        for (int i = 0; i < props.Length; i++)
        {
            seriesList[i] = buffers[i].ToSeries(props[i].Name);
        }

        return new DataFrame(seriesList);
    }

    private static bool IsSimpleType(Type type)
    {
        return type.IsPrimitive || 
               type == typeof(string) || 
               type == typeof(DateOnly) || 
               type == typeof(decimal) || 
               type == typeof(DateTime) || 
               type == typeof(TimeSpan) ||
               type == typeof(TimeOnly) || 
               type == typeof(DateTimeOffset) || 
               (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>) && IsSimpleType(Nullable.GetUnderlyingType(type)!));
    }
    /// <summary>
    /// Create a DataFrame from an object where properties represent columns (Arrays/Lists).
    /// This is useful for "Structure of Arrays" (SoA) data layout.
    /// </summary>
    /// <example>
    /// var df = DataFrame.FromColumns(new { 
    ///     Time = new[] { dt1, dt2 }, 
    ///     Val = new[] { 1.0, 2.0 } 
    /// });
    /// </example>
    [RequiresUnreferencedCode("Uses reflection and dynamic to extract properties from anonymous types.")]
    public static DataFrame FromColumns(object columns)
    {
        ArgumentNullException.ThrowIfNull(columns);

        var properties = columns.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
        
        var cols = new (string, object)[properties.Length];

        for (int i = 0; i < properties.Length; i++)
        {
            var p = properties[i];
            var val = p.GetValue(columns) ?? throw new ArgumentNullException($"Property '{p.Name}' cannot be null.");
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
            return new DataFrame(); // Return empty DF

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

        return new DataFrame([.. seriesList]);
    }
    // =========================================================
    // Internal Column Buffers
    // =========================================================
    private interface IColumnBuffer
    {
        void Add(object? val);
        Series ToSeries(string name);
    }

    /// <summary>
    /// Strong Type Column Buffer 
    /// </summary>
    /// <typeparam name="TCol">Column Type(Example: int, string, DateTime?)</typeparam>
    private sealed class ColumnBuffer<TCol>(int capacity) : IColumnBuffer
    {
        private readonly List<TCol?> _data = new(capacity);

        public void Add(object? val)
        {
            _data.Add((TCol?)val);
        }

        public Series ToSeries(string name)
            => Series.From(name, _data.ToArray());
        
    }

    private static class ColumnBufferFactory
    {
        public static IColumnBuffer Create(Type propType, int capacity)
        {
            
            Type targetType = propType;

            if (propType.IsValueType && Nullable.GetUnderlyingType(propType) == null)
            {
                targetType = typeof(Nullable<>).MakeGenericType(propType);
            }

            var bufferType = typeof(ColumnBuffer<>).MakeGenericType(targetType);
            return (IColumnBuffer)Activator.CreateInstance(bufferType, capacity)!;
        }
    }
    /// <summary>
    /// Create a DataFrame from a list of Series.
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
    /// var s1 = new Series("id", new[] { 1, 2, 3 });
    /// var s2 = new Series("name", new[] { "Alice", "Bob", "Charlie" });
    /// 
    /// var df = DataFrame.FromSeries(s1, s2);
    /// Console.WriteLine(df);
    /// </code>
    /// </example>
    public static DataFrame FromSeries(params Series[] series)
        => new(series);
    /// <summary>
    /// Create a DataFrame from a collection of Series.
    /// <para>
    /// This is the syntax sugar for FromSeries().
    /// </para>
    /// </summary>
    public static DataFrame FromColumns(params Series[] series)
        => new(series);
    /// <summary>
    /// Create a DataFrame from a collection of Series.
    /// </summary>
    /// <param name="series">The series to combine.</param>
    public static DataFrame FromSeries(IEnumerable<Series> series)
        => new([.. series]);
    /// <summary>
    /// Stream C# objects into Polars.
    /// </summary>
    public static DataFrame FromEnumerable<T>(IEnumerable<T> data, int batchSize = 100_000, Schema? providedSchema = null)
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
    public static DataFrame FromArrowStream<T>(IEnumerable<T> data, int batchSize = 100_000,Schema? providedSchema = null)
    {
        var schema = providedSchema ?? ArrowConverter.GetSchemaFromType<T>();
        var stream = data.ToArrowBatches(batchSize);

        var handle = ArrowStreamInterop.ImportEager(stream,schema);

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
}