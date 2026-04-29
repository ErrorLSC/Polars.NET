using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Apache.Arrow;
using System.Data;
using Polars.NET.Core.Data;
using System.Reflection;
using Polars.NET.Core.Helpers;
using Apache.Arrow.Ipc;
using System.Diagnostics.CodeAnalysis;
using System.Buffers;
using Pl = Polars.CSharp.Polars;

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
        if (data == null) return [];
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
        if (props.Length == 0) return [];

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
        
        private static readonly Type _underlyingType = Nullable.GetUnderlyingType(typeof(TCol)) ?? typeof(TCol);

        public void Add(object? val)
        {
            if (val == null)
            {
                _data.Add(default);
                return;
            }

            if (val is TCol exactVal)
            {
                _data.Add(exactVal);
                return;
            }

            _data.Add((TCol)Convert.ChangeType(val, _underlyingType));
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
    // ==========================================
    // Dictionary Mapping (Dynamic/JSON Data)
    // ==========================================
    
    /// <summary>
    /// Create a DataFrame from a single dictionary (represents 1 row).
    /// </summary>
    public static DataFrame FromDict(IDictionary<string, object?> data, IntoSchema? schema = null,IntoSchema? schemaOverrides=null,bool strict=true)
        => FromDicts([data], schema,schemaOverrides,strict);

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
        if (records.Count == 0) return [];

        try
        {
            var actualSchema = schema?.Consume().ToDictionary();
            var overrides = schemaOverrides?.Consume().ToDictionary();

            var columnTypes = new Dictionary<string, Type?>();
            int rowsToInfer = (int?)inferSchemaLength ?? records.Count;

            // ==========================================
            // Schema Inference Phase
            // ==========================================
            if (actualSchema != null)
            {
                // Use explicit schema
                foreach (var colName in actualSchema.Keys)
                {
                    columnTypes[colName] = null; 
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
                            Type? existingType = columnTypes[colName];

                            if (existingType == null)
                                columnTypes[colName] = newType;
                            else if (existingType != newType)
                                columnTypes[colName] = PromoteType(existingType, newType); 
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

                    foreach (var kvp in row)
                    {
                        if (!columnTypes.TryGetValue(kvp.Key, out Type? existingType))
                        {
                            columnTypes[kvp.Key] = kvp.Value?.GetType();
                        }
                        else if (kvp.Value != null)
                        {
                            Type newType = kvp.Value.GetType();
                            if (existingType == null)
                                columnTypes[kvp.Key] = newType;
                            else if (existingType != newType)
                                columnTypes[kvp.Key] = PromoteType(existingType, newType); 
                        }
                    }
                    rowCount++;
                }
            }

            // ==========================================
            // Buffer Loading Phase
            // ==========================================
            var buffers = new Dictionary<string, IColumnBuffer>();
            foreach (var kvp in columnTypes)
            {
                Type colType = kvp.Value ?? typeof(string);
                buffers[kvp.Key] = ColumnBufferFactory.Create(colType, records.Count);
            }

            foreach (var row in records)
            {
                foreach (var colName in columnTypes.Keys)
                {
                    row.TryGetValue(colName, out var val);
                    try
                    {
                        buffers[colName].Add(val);
                    }
                    catch (InvalidCastException ex)
                    {
                        if (strict) throw new InvalidCastException($"Strict mode error on column '{colName}'...", ex);
                        buffers[colName].Add(null);
                    }
                }
            }

            // ==========================================
            // Type Cast Phase
            // ==========================================
            var seriesList = new Series[buffers.Count];
            int i = 0;
            var orderedKeys = actualSchema != null ? actualSchema.Keys : columnTypes.Keys; 
            
            foreach (var key in orderedKeys)
            {
                seriesList[i++] = buffers[key].ToSeries(key);
            }

            var df = new DataFrame(seriesList);

            // ==========================================
            // Schema 
            // ==========================================
            if (actualSchema != null || overrides != null)
            {
                var castExprs = new List<Expr>();
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

    /// <summary>
    /// Type Promotion
    /// </summary>
    private static Type PromoteType(Type typeA, Type typeB)
    {
        if (typeA == typeB) return typeA;

        Type baseA = Nullable.GetUnderlyingType(typeA) ?? typeA;
        Type baseB = Nullable.GetUnderlyingType(typeB) ?? typeB;

        if (baseA == baseB) return baseA; 

        if (IsNumeric(baseA) && IsNumeric(baseB))
        {
            if (baseA == typeof(double) || baseB == typeof(double)) return typeof(double);
            if (baseA == typeof(float) || baseB == typeof(float)) return typeof(double); 
            if (baseA == typeof(decimal) || baseB == typeof(decimal)) return typeof(decimal);
            if (baseA == typeof(long) || baseB == typeof(long)) return typeof(long);
            
            return typeof(long); 
        }

        return typeof(string);
    }

    private static bool IsNumeric(Type type)
    {
        return type == typeof(byte) || type == typeof(sbyte) ||
               type == typeof(short) || type == typeof(ushort) ||
               type == typeof(int) || type == typeof(uint) ||
               type == typeof(long) || type == typeof(ulong) || type == typeof(Half) ||
               type == typeof(float) || type == typeof(double) ||
               type == typeof(decimal) || type == typeof(Int128) || type == typeof(UInt128);
    }
    /// <summary>
    /// Reconstruct a DataFrame from a string representation (ASCII table).
    /// </summary>
    /// <param name="repr">The formatted table string.</param>
    public static DataFrame FromRepr(string repr)
    {
        if (string.IsNullOrWhiteSpace(repr)) return [];

        var lines = repr.Split(["\r\n", "\n"], StringSplitOptions.RemoveEmptyEntries)
                        .Select(l => l.Trim())
                        .Where(l => l.StartsWith('│') || l.StartsWith('|')) 
                        .ToList();

        if (lines.Count < 3) return []; 

        var headers = ReprParser.SplitCells(lines[0]);
        
        var dtypeLineIndex = lines[1].Contains("---") ? 2 : 1;
        var dtypesStr = ReprParser.SplitCells(lines[dtypeLineIndex]);
        
        var ellipsisIndexes = headers.Select((h, i) => h == "..." || h == "…" ? i : -1)
                                     .Where(i => i != -1).ToHashSet();

        var validHeaders = new List<string>();
        var validDtypes = new List<DataType>();
        var columnBuffers = new List<List<string?>>();

        for (int i = 0; i < headers.Length; i++)
        {
            if (ellipsisIndexes.Contains(i)) continue; 
            
            validHeaders.Add(headers[i]);
            validDtypes.Add(ReprParser.ParseShortDtype(dtypesStr[i]));
            columnBuffers.Add([]);
        }

        for (int i = dtypeLineIndex + 1; i < lines.Count; i++)
        {
            if (lines[i].Contains("╞══") || lines[i].Contains("|---")) continue; 

            var cells = ReprParser.SplitCells(lines[i]);
            
            int bufferIdx = 0;
            for (int col = 0; col < cells.Length; col++)
            {
                if (ellipsisIndexes.Contains(col)) continue;

                string rawVal = cells[col];
                if (rawVal.Equals("null", StringComparison.OrdinalIgnoreCase))
                {
                    columnBuffers[bufferIdx].Add(null);
                }
                else
                {
                    if (rawVal.StartsWith('\"') && rawVal.EndsWith('\"'))
                        rawVal = rawVal[1..^1];
                    columnBuffers[bufferIdx].Add(rawVal);
                }
                bufferIdx++;
            }
        }

        var stringSeries = new Series[validHeaders.Count];
        for (int i = 0; i < validHeaders.Count; i++)
        {
            stringSeries[i] = Series.From(validHeaders[i], columnBuffers[i].ToArray());
        }

        var df = new DataFrame(stringSeries);

        var castExprs = new List<Expr>();
        for (int i = 0; i < df.Columns.Length; i++)
        {
            var colName = df.Columns[i];
            var targetDtype = validDtypes[i];
            
            if (targetDtype.Kind == DataTypeKind.Datetime)
            {
                castExprs.Add(Pl.Col(colName).Str.ToDatetime("%Y-%m-%d %H:%M:%S%.f").Cast(targetDtype));
            }
            else if (targetDtype.Kind == DataTypeKind.Date)
            {
                castExprs.Add(Pl.Col(colName).Str.ToDate("%Y-%m-%d").Cast(targetDtype));
            }
            else if (targetDtype.Kind == DataTypeKind.Time)
            {
                castExprs.Add(Pl.Col(colName).Str.ToTime("%H:%M:%S%.f").Cast(targetDtype));
            }
            else if (targetDtype.Kind != DataTypeKind.String)
            {
                castExprs.Add(Pl.Col(colName).Cast(targetDtype));
            }
            else
            {
                castExprs.Add(Pl.Col(colName));
            }
        }

        return df.Select([.. castExprs]);
    }
}