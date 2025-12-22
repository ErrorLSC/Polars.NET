using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Apache.Arrow;
using System.Data;
using Polars.NET.Core.Data;
using System.Collections.Concurrent;
using System.Collections;
using System.Text;

namespace Polars.CSharp;

/// <summary>
/// DataFrame represents a 2-dimensional labeled data structure similar to a table or spreadsheet.
/// </summary>
public class DataFrame : IDisposable,IEnumerable<Series>
{
    internal DataFrameHandle Handle { get; }

    internal DataFrame(DataFrameHandle handle)
    {
        Handle = handle;
    }

    // ==========================================
    // Metadata
    // ==========================================

    /// <summary>
    /// Get the schema of the DataFrame as a dictionary (Column Name -> Data Type String).
    /// </summary>
    public Dictionary<string, DataType> Schema
    {
        get
        {
            // [重构] 遍历列构建 Schema，确保类型安全且无需解析字符串
            
            int width = (int)PolarsWrapper.DataFrameWidth(Handle);
            var schema = new Dictionary<string, DataType>(width);

            for (int i = 0; i < width; i++)
            {
                // 1. 获取第 i 列的 SeriesHandle (这是 Rust 端 Clone 出来的新 Handle，需要释放)
                using var seriesHandle = PolarsWrapper.DataFrameGetColumnAt(Handle, i);
                
                // 2. 获取列名 (Wrapper 需封装 pl_series_name)
                // 假设 PolarsWrapper.GetSeriesName(SeriesHandle) 存在
                string name = PolarsWrapper.SeriesName(seriesHandle);

                // 3. 获取 DataType (直接拿 Handle)
                var dtHandle = PolarsWrapper.GetSeriesDataType(seriesHandle);
                
                // 4. 构造 DataType 对象并加入字典
                // 注意：DataType 对象会接管 dtHandle 的生命周期
                schema[name] = new DataType(dtHandle);
            }

            return schema;
        }
    }
    /// <summary>
    /// Prints the schema to the console in a tree format.
    /// Useful for debugging column names and data types.
    /// </summary>
    public void PrintSchema()
    {
        var schema = Schema; // 获取刚刚实现的 Dictionary
        
        Console.WriteLine("root");
        foreach (var kvp in schema)
        {
            // 格式模仿 Spark:  |-- name: type
            Console.WriteLine($" |-- {kvp.Key}: {kvp.Value}");
        }
    }
    // /// <summary>
    // /// Get a string representation of the DataFrame schema.
    // /// </summary>
    // /// <returns></returns>
    // public override string ToString()
    // {
    //     return $"DataFrame: {Height}x{Width} {string.Join(", ", Schema.Select(kv => $"{kv.Key}:{kv.Value}"))}";
    // }
    // ==========================================
    // Static IO Read
    // ==========================================
    /// <summary>
    /// Reads a CSV file into a DataFrame.
    /// </summary>
    /// <param name="path">Path to the CSV file.</param>
    /// <param name="schema">Optional schema dictionary.</param>
    /// <param name="hasHeader">Whether the CSV has a header row.</param>
    /// <param name="separator">Character used as separator.</param>
    /// <param name="skipRows">Choose how many rows should be skipped.</param>
    /// <param name="tryParseDates">Whether to automatically try parsing dates/datetimes. Default is true.</param>
    /// <returns>A new DataFrame.</returns>
    public static DataFrame ReadCsv(
        string path, 
        Dictionary<string, DataType>? schema = null,
        bool hasHeader = true, 
        char separator = ',',
        ulong skipRows = 0,
        bool tryParseDates = true) // [新增参数]
    {
        // 将 C# 的 DataType 转换为底层的 DataTypeHandle
        var schemaHandles = schema?.ToDictionary(
            kv => kv.Key, 
            kv => kv.Value.Handle
        );

        var handle = PolarsWrapper.ReadCsv(
            path, 
            schemaHandles, 
            hasHeader, 
            separator, 
            skipRows,
            tryParseDates // 传递给 Wrapper
        );

        return new DataFrame(handle);
    }
    /// <summary>
    /// Read Parquet File
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static DataFrame ReadParquet(string path)
    {
        //
        return new DataFrame(PolarsWrapper.ReadParquet(path));
    }
    /// <summary>
    /// Read JSON File
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static DataFrame ReadJson(string path)
    {
        //
        return new DataFrame(PolarsWrapper.ReadJson(path));
    }
    /// <summary>
    /// Read IPC File
    /// </summary>
    /// <param name="path"></param>
    /// <returns></returns>
    public static DataFrame ReadIpc(string path)
    {
        //
        return new DataFrame(PolarsWrapper.ReadIpc(path));
    }

    /// <summary>
    /// Create DataFrame from Apache Arrow RecordBatch.
    /// </summary>
    public static DataFrame FromArrow(RecordBatch batch)
    {
        // 调用 Core 层的 Bridge
        var handle = ArrowFfiBridge.ImportDataFrame(batch);
        return new DataFrame(handle);
    }
    /// <summary>
    /// Transfer a RecordBatch to Arrow
    /// </summary>
    /// <returns></returns>
    public RecordBatch ToArrow()
    {
        //
        return ArrowFfiBridge.ExportDataFrame(Handle);
    }
    /// <summary>
    /// Asynchronously reads a CSV file into a DataFrame.
    /// </summary>
    public static async Task<DataFrame> ReadCsvAsync(
        string path,
        Dictionary<string, DataType>? schema = null,
        bool hasHeader = true,
        char separator = ',',
        ulong skipRows = 0,
        bool tryParseDates = true) // [新增参数]
    {
        var schemaHandles = schema?.ToDictionary(
            kv => kv.Key, 
            kv => kv.Value.Handle
        );

        var handle = await PolarsWrapper.ReadCsvAsync(
            path, 
            schemaHandles, 
            hasHeader, 
            separator, 
            skipRows,
            tryParseDates // 传递给 Wrapper
        );

        return new DataFrame(handle);
    }
    /// <summary>
    /// Read a Parquet file asynchronously.
    /// </summary>
    public static async Task<DataFrame> ReadParquetAsync(string path)
    {
        var handle = await PolarsWrapper.ReadParquetAsync(path);
        return new DataFrame(handle);
    }

    /// <summary>
    /// Create a DataFrame directly from a DbDataReader.
    /// Supports basic types, Arrays (as List), and POCOs (as Struct).
    /// </summary>
    /// <param name="reader">The open data reader.</param>
    /// <param name="batchSize">Number of rows per Arrow batch.</param>
    public static DataFrame ReadDatabase(IDataReader reader, int batchSize = 50_000)
    {
        // 1. 显式获取 Schema (为了传给 Exporter)
        // 复用了你的"邪修"逻辑，支持嵌套类型推断
        var schema = reader.GetArrowSchema();

        // 2. 获取流
        // 复用了 Buffer Pool + ArrowConverter
        var batchEnumerable = reader.ToArrowBatches(batchSize);
        
        // 3. 获取枚举器 (准备移交控制权)
        var enumerator = batchEnumerable.GetEnumerator();

        // 4. 调用互操作层
        try 
        {
            var handle = ArrowStreamInterop.ImportEager(enumerator, schema);
            return new DataFrame(handle);
        }
        catch
        {
            // 如果出错，记得清理 enumerator
            enumerator.Dispose();
            throw;
        }
    }
    // ==========================================
    // Properties
    // ==========================================
    /// <summary>
    /// Return DataFrame Height
    /// </summary>
    public long Height => PolarsWrapper.DataFrameHeight(Handle); //
    /// <summary>
    /// Return DataFrame Width
    /// </summary>
    public long Width => PolarsWrapper.DataFrameWidth(Handle);   //
    /// <summary>
    /// Return DataFrame Columns' Name
    /// </summary>
    public string[] Columns => PolarsWrapper.GetColumnNames(Handle); //

    // ==========================================
    // Scalar Access (Direct)
    // ==========================================

    /// <summary>
    /// Get a value from the DataFrame at the specified row and column.
    /// This is efficient for single-value lookups (no Arrow conversion).
    /// </summary>
    public T? GetValue<T>(long rowIndex, string colName)
    {
        // 1. 获取 Series (假设已有索引器 this[string column])
        // 注意：这里不要用 using，因为 Series 的所有权属于 DataFrame，不能 Dispose
        var series = this[colName];
        
        // 2. 委托给 Series.GetValue<T>
        return series.GetValue<T>(rowIndex);
    }

    /// <summary>
    /// Get value by row index and column name (object type).
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="colName"></param>
    /// <returns></returns>
    public object? this[long rowIndex, string colName]
    {
        get
        {
            var series = this[colName];
            return series[rowIndex]; // 委托给 Series 的 object 索引器
        }
    }

    // ==========================================
    // DataFrame Operations
    // ==========================================
    /// <summary>
    /// Select columns
    /// </summary>
    /// <param name="exprs"></param>
    /// <returns></returns>
    public DataFrame Select(params Expr[] exprs)
    {
        // 必须 Clone Handle，因为 Wrapper 会消耗它们
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        
        //
        return new DataFrame(PolarsWrapper.Select(Handle, handles));
    }
    /// <summary>
    /// Filter rows based on a boolean expression. 
    /// </summary>
    /// <param name="expr"></param>
    /// <returns></returns>
    public DataFrame Filter(Expr expr)
    {
        var h = PolarsWrapper.CloneExpr(expr.Handle);
        //
        return new DataFrame(PolarsWrapper.Filter(Handle, h));
    }
    /// <summary>
    /// Filter rows based on a boolean expression. 
    /// </summary>
    /// <param name="exprs"></param>
    /// <returns></returns>
    public DataFrame WithColumns(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        //
        return new DataFrame(PolarsWrapper.WithColumns(Handle, handles));
    }
    /// <summary>
    /// Sort (Order By) the DataFrame.
    /// </summary>
    /// <param name="by"></param>
    /// <param name="descending"></param>
    /// <returns></returns>
    public DataFrame Sort(Expr by, bool descending = false)
    {
        var h = PolarsWrapper.CloneExpr(by.Handle);
        //
        return new DataFrame(PolarsWrapper.Sort(Handle, h, descending));
    }
    /// <summary>
    /// Return head lines from a DataFrame
    /// </summary>
    /// <param name="n"></param>
    /// <returns></returns>
    public DataFrame Head(int n = 5)
    {
        //
        return new DataFrame(PolarsWrapper.Head(Handle, (uint)n));
    }
    /// <summary>
    /// Return tail lines from a DataFrame
    /// </summary>
    /// <param name="n"></param>
    /// <returns></returns>
    public DataFrame Tail(int n = 5)
    {
        //
        return new DataFrame(PolarsWrapper.Tail(Handle, (uint)n));
    }
    /// <summary>
    /// Explode a list or structure in a Column
    /// </summary>
    /// <param name="exprs"></param>
    /// <returns></returns>
    public DataFrame Explode(params Expr[] exprs)
    {
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        //
        return new DataFrame(PolarsWrapper.Explode(Handle, handles));
    }
    /// <summary>
    /// Decompose a struct column into multiple columns.
    /// </summary>
    /// <param name="columns">The struct columns to unnest.</param>
    public DataFrame Unnest(params string[] columns)
    {
        var newHandle = PolarsWrapper.Unnest(Handle, columns);
        return new DataFrame(newHandle);
    }
    // ==========================================
    // Data Cleaning / Structure Ops
    // ==========================================

    /// <summary>
    /// Drop a column by name.
    /// </summary>
    public DataFrame Drop(string columnName)
    {
        // Wrapper: Drop(df, name)
        // 注意：Polars 操作通常返回新 DataFrame，原 DataFrame 可能会被消耗（取决于 Rust 实现）。
        // 如果 Rust 的 pl_dataframe_drop 是消耗性的 (Move)，我们这里应该 new DataFrame(handle)。
        return new DataFrame(PolarsWrapper.Drop(Handle, columnName));
    }

    /// <summary>
    /// Rename a column.
    /// </summary>
    public DataFrame Rename(string oldName, string newName)
    {
        return new DataFrame(PolarsWrapper.Rename(Handle, oldName, newName));
    }

    /// <summary>
    /// Drop rows containing null values.
    /// </summary>
    /// <param name="subset">Column names to consider. If null/empty, checks all columns.</param>
    public DataFrame DropNulls(params string[]? subset)
    {
        // Wrapper 处理了 subset 为 null 的情况
        return new DataFrame(PolarsWrapper.DropNulls(Handle, subset));
    }

    // ==========================================
    // Sampling
    // ==========================================

    /// <summary>
    /// Sample n rows from the DataFrame.
    /// </summary>
    public DataFrame Sample(ulong n, bool withReplacement = false, bool shuffle = true, ulong? seed = null)
    {
        return new DataFrame(PolarsWrapper.SampleN(Handle, n, withReplacement, shuffle, seed));
    }

    /// <summary>
    /// Sample a fraction of rows from the DataFrame.
    /// </summary>
    public DataFrame Sample(double fraction, bool withReplacement = false, bool shuffle = true, ulong? seed = null)
    {
        return new DataFrame(PolarsWrapper.SampleFrac(Handle, fraction, withReplacement, shuffle, seed));
    }
    // ==========================================
    // Combining DataFrames
    // ==========================================
    /// <summary>
    /// Join with another DataFrame
    /// </summary>
    /// <param name="other"></param>
    /// <param name="leftOn"></param>
    /// <param name="rightOn"></param>
    /// <param name="how"></param>
    /// <returns></returns>
    public DataFrame Join(DataFrame other, Expr[] leftOn, Expr[] rightOn, JoinType how = JoinType.Inner)
    {
        var lHandles = leftOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rHandles = rightOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        
        //
    return new DataFrame(PolarsWrapper.Join(
            Handle, 
            other.Handle, 
            lHandles, 
            rHandles, 
            how.ToNative()
        ));
    }
    
    /// <summary>
    /// Concatenate multiple DataFrames
    /// </summary>
    /// <param name="dfs"></param>
    /// <param name="how"></param>
    /// <returns></returns>
    public static DataFrame Concat(IEnumerable<DataFrame> dfs, ConcatType how = ConcatType.Vertical)
    {
        var handles = dfs.Select(d => PolarsWrapper.CloneDataFrame(d.Handle)).ToArray();
        
        //
        return new DataFrame(PolarsWrapper.Concat(handles, how.ToNative()));
    }

    // ==========================================
    // GroupBy
    // ==========================================
    /// <summary>
    /// Group by keys and apply aggregations.
    /// </summary>
    /// <param name="by"></param>
    /// <returns></returns>
    public GroupByBuilder GroupBy(params Expr[] by)
    {
        // 返回一个构建器，不立即执行
        return new GroupByBuilder(this, by);
    }

    // ==========================================
    // Pivot / Unpivot
    // ==========================================
    /// <summary>
    /// Pivot the DataFrame from long to wide format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="columns"></param>
    /// <param name="values"></param>
    /// <param name="agg"></param>
    /// <returns></returns>
    public DataFrame Pivot(string[] index, string[] columns, string[] values, PivotAgg agg = PivotAgg.First)
    {
        //
        return new DataFrame(PolarsWrapper.Pivot(Handle, index, columns, values, agg.ToNative()));
    }
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public DataFrame Unpivot(string[] index, string[] on, string variableName = "variable", string valueName = "value")
    {
        //
        return new DataFrame(PolarsWrapper.Unpivot(Handle, index, on, variableName, valueName));
    }
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public DataFrame Melt(string[] index, string[] on, string variableName = "variable", string valueName = "value") 
        => Unpivot(index, on, variableName, valueName);

    // ==========================================
    // IO Write
    // ==========================================
    /// <summary>
    /// Write DataFrame to CSV File
    /// </summary>
    /// <param name="path"></param>
    public void WriteCsv(string path)
    {
        //
        PolarsWrapper.WriteCsv(Handle, path);
    }
    /// <summary>
    /// Write DataFrame to Parquet File
    /// </summary>
    /// <param name="path"></param>
    public void WriteParquet(string path)
    {
        //
        PolarsWrapper.WriteParquet(Handle, path);
    }
    /// <summary>
    /// Write DataFrame to IPC File    
    /// </summary>
    /// <param name="path"></param>
    public void WriteIpc(string path)
    {
        PolarsWrapper.WriteIpc(Handle, path);
    }
    /// <summary>
    /// Write DataFrame to JSON File
    /// </summary>
    /// <param name="path"></param>
    public void WriteJson(string path)
    {
        PolarsWrapper.WriteJson(Handle, path);
    }
    /// <summary>
    /// 将 DataFrame 的数据按 Batch 导出（零拷贝）。
    /// 这是实现自定义 Eager Sink (如 WriteDatabase) 的基础。
    /// </summary>
    /// <param name="onBatchReceived">接收 RecordBatch 的回调</param>
    public void ExportBatches(Action<RecordBatch> onBatchReceived)
    {
        // 调用 Wrapper，传递 Handle (DataFrameHandle)
        // 注意：这是只读操作，不需要 TransferOwnership
        PolarsWrapper.ExportBatches(Handle, onBatchReceived);
    }
    /// <summary>
    /// 通用写入接口：将 DataFrame 转换为 IDataReader 并交给 writerAction 处理。
    /// 用户可以在 writerAction 里使用 SqlBulkCopy, NpgsqlBinaryImporter 等任意工具。
    /// </summary>
    public void WriteTo( Action<IDataReader> writerAction, int bufferSize = 5,Dictionary<string, Type>? typeOverrides = null)
    {
        using var buffer = new BlockingCollection<RecordBatch>(bufferSize);

        // Consumer Task
        var consumerTask = Task.Run(() => 
        {
            // 这里创建了 ArrowToDbStream，它是 IDataReader 的实现
            using var reader = new ArrowToDbStream(buffer.GetConsumingEnumerable(),typeOverrides = null);
            
            // [关键] 将 reader 交给用户的回调函数
            // 用户在这里执行 bulk.WriteToServer(reader)
            writerAction(reader);
        });

        // Producer
        try
        {
            ExportBatches(buffer.Add);
        }
        finally
        {
            buffer.CompleteAdding();
        }

        consumerTask.Wait();
    }
    /// <summary>
    /// Generate a summary statistics DataFrame (count, mean, std, min, 25%, 50%, 75%, max).
    /// Similar to pandas/polars describe().
    /// </summary>
    public DataFrame Describe()
    {
        // 1. 筛选数值列
        var schema = Schema;
        var numericCols = schema
            .Where(kv => kv.Value.IsNumeric)
            .Select(kv => kv.Key)
            .ToList();

        if (numericCols.Count == 0)
            throw new InvalidOperationException("No numeric columns to describe.");

        // 2. 定义统计指标
        // 每个指标是一个 Tuple: (Name, Func<colName, Expr>)
        var metrics = new List<(string Name, Func<string, Expr> Op)>
        {
            ("count",      c => Polars.Col(c).Count().Cast(DataType.Float64)),
            ("null_count", c => Polars.Col(c).IsNull().Sum().Cast(DataType.Float64)),
            ("mean",       c => Polars.Col(c).Mean()),
            ("std",        c => Polars.Col(c).Std()),
            ("min",        c => Polars.Col(c).Min().Cast(DataType.Float64)),
            ("25%",        c => Polars.Col(c).Quantile(0.25, "nearest").Cast(DataType.Float64)),
            ("50%",        c => Polars.Col(c).Median().Cast(DataType.Float64)),
            ("75%",        c => Polars.Col(c).Quantile(0.75, "nearest").Cast(DataType.Float64)),
            ("max",        c => Polars.Col(c).Max().Cast(DataType.Float64))
        };

        // 3. 计算每一行 (Row Frames)
        var rowFrames = new List<DataFrame>();
        
        try
        {
            foreach (var (statName, op) in metrics)
            {
                // 构建 Select 表达式列表: [ Lit(statName).Alias("statistic"), op(col1), op(col2)... ]
                var exprs = new List<Expr>
                {
                    Polars.Lit(statName).Alias("statistic")
                };

                foreach (var col in numericCols)
                {
                    exprs.Add(op(col));
                }

                // 执行 Select -> 得到 1 行 N 列的 DataFrame
                // 注意：Select 返回新 DF，我们需要收集起来
                rowFrames.Add(Select(exprs.ToArray()));
            }

            // 4. 垂直拼接
            // 需要 Wrapper 支持 Concat(DataFrameHandle[])
            return Concat(rowFrames);
        }
        finally
        {
            // 清理中间产生的临时 DataFrames
            foreach (var frame in rowFrames)
            {
                frame.Dispose();
            }
        }
    }
    // ==========================================
    // Display (Show)
    // ==========================================
    /// <summary>
    /// Returns the string representation of the DataFrame (ASCII table).
    /// This allows Console.WriteLine(df) to print the table directly.
    /// </summary>
    public override string ToString()
    {
        if (Handle.IsInvalid) return "DataFrame (Disposed)";
        
        // 调用 Rust 获取漂亮的表格
        return PolarsWrapper.DataFrameToString(Handle);
    }

    /// <summary>
    /// Print the DataFrame to Console.
    /// </summary>
    public void Show()
    {
        Console.WriteLine(ToString());
    }
    // ==========================================
    // Interop
    // ==========================================

    /// <summary>
    /// Clone the DataFrame
    /// </summary>
    /// <returns></returns>
    public DataFrame Clone()
    {
        //
        return new DataFrame(PolarsWrapper.CloneDataFrame(Handle));
    }
    /// <summary>
    /// Dispose the DataFrame and release resources.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
    }
    // ==========================================
    // Object Mapping (From Records)
    // ==========================================

    /// <summary>
    /// Create a DataFrame from a collection of objects (POCOs).
    /// High-performance implementation using Arrow Struct conversion.
    /// </summary>
    public static DataFrame From<T>(IEnumerable<T> data)
    {
        // 1. 利用我们强大的 ArrowConverter 将对象列表转为 Struct Series
        // 这是一次性遍历，性能最高，且支持嵌套类型
        using var structSeries = Series.From("data", data);
        
        // 2. 将 Series 包装为 DataFrame
        using var tmpDf = new DataFrame(structSeries);
        
        // 3. 调用 Polars 的 Unnest 将 Struct 字段炸开为独立列
        return tmpDf.Unnest("data");
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
    public static DataFrame FromColumns(object columns)
    {
        // 1. 调用 Core 层进行反射和转换
        // 返回的是 List<(string Name, IArrowArray Array)>
        var rawColumns = ArrowConverter.BuildColumnsFromObject(columns);

        // 2. 将 Arrow Arrays 包装为 Series
        // 这一步必须在 API 层做，因为 Series 是 API 层的类
        var seriesList = new List<Series>(rawColumns.Count);
        foreach (var (name, array) in rawColumns)
        {
            seriesList.Add(Series.FromArrow(name, array));
        }

        // 3. 组装 DataFrame
        return new DataFrame(seriesList.ToArray());
    }
    /// <summary>
    /// Create a DataFrame from a list of Series.
    /// </summary>
    public DataFrame(params Series[] series)
    {
        if (series == null || series.Length == 0)
        {
            Handle = PolarsWrapper.DataFrameNew(System.Array.Empty<SeriesHandle>());
            return;
        }

        // 提取 Handles
        // 注意：NativeBindings.pl_dataframe_new 通常会 Clone 这些 Series，
        // 所以 C# 端的 Series 对象依然拥有原本 Handle 的所有权，用户可以在外面继续使用 series[i]。
        var handles = series.Select(s => s.Handle).ToArray();
        
        Handle = PolarsWrapper.DataFrameNew(handles);
    }
    /// <summary>
    /// [High Performance] Stream data into Polars using Arrow C Stream Interface.
    /// This method supports datasets larger than available RAM by streaming chunks directly to Polars.
    /// </summary>
    /// <param name="data">Source data collection</param>
    /// <param name="batchSize">Rows per chunk (default 100,000)</param>
    public static DataFrame FromArrowStream<T>(IEnumerable<T> data, int batchSize = 100_000)
    {
        // 1. 转为 Arrow 流
        var stream = data.ToArrowBatches(batchSize);

        // 2. 尝试 Eager 导入 (Core 层自动处理探测和缝合)
        var handle = ArrowStreamInterop.ImportEager(stream);

        // 3. 处理空流情况
        // 如果 handle 无效，说明流里没有数据，ArrowStreamInterop 没法推断 Schema
        if (handle.IsInvalid)
        {
            // 回退到反射机制生成空 DataFrame (因为我们需要 T 的结构)
            return From(Enumerable.Empty<T>());
        }

        return new DataFrame(handle);
    }
    
    // ==========================================
    // Object Mapping (To Records)
    // ==========================================

    /// <summary>
    /// Convert DataFrame to a list of strongly-typed objects.
    /// This triggers a conversion to Arrow format internally.
    /// </summary>
    public IEnumerable<T> Rows<T>() where T : new()
    {
        // 1. 转为 Arrow RecordBatch (这是 Polars.CSharp 这一层特有的能力)
        // ToArrow() 方法本身应该已经在 DataFrame 类里实现了
        using var batch = ToArrow(); 

        // 2. 委托给 Core 层去解析
        foreach (var item in ArrowReader.ReadRecordBatch<T>(batch))
        {
            yield return item;
        }
    }

    // ==========================================
    // Conversion to Lazy
    // ==========================================

    /// <summary>
    /// Convert the DataFrame into a LazyFrame.
    /// This allows building a query plan and optimizing execution.
    /// </summary>
    public LazyFrame Lazy()
    {
        // 1. 先克隆 DataFrame Handle。
        var clonedHandle = PolarsWrapper.CloneDataFrame(Handle);
        
        // 2. 转换为 LazyFrame
        var lfHandle = PolarsWrapper.DataFrameToLazy(clonedHandle);
        
        return new LazyFrame(lfHandle);
    }
    // ==========================================
    // Series Access (Column Selection)
    // ==========================================

    /// <summary>
    /// Get a column as a Series by name.
    /// </summary>
    public Series Column(string name)
    {
        // 调用 Wrapper 获取 SeriesHandle (Rust 侧通常是 Clone Arc，引用计数+1)
        var sHandle = PolarsWrapper.DataFrameGetColumn(Handle, name);
        
        // 返回新的 Series 对象，它接管 Handle 的生命周期
        return new Series(name, sHandle);
    }

    /// <summary>
    /// Get a column as a Series by name (Indexer syntax).
    /// Usage: var s = df["age"];
    /// </summary>
    public Series this[string columnName]
    {
        get => Column(columnName);
    }

    /// <exception cref="IndexOutOfRangeException"></exception>
    /// <summary>
    /// Get a column by its positional index (0-based).
    /// </summary>
    public Series Column(int index)
    {
        // 直接调用 Wrapper
        // 错误检查已经在 Wrapper 里做过了 (抛 IndexOutOfRangeException)
        var h = PolarsWrapper.DataFrameGetColumnAt(Handle, index);
        return new Series(h);
    }
    /// <summary>
    /// Get all columns as a list of Series.
    /// Order is guaranteed to match the physical column order.
    /// </summary>
    public Series[] GetColumns()
    {
        // 用底层的按索引获取列名 (有序)
        var names = PolarsWrapper.GetColumnNames(Handle);
        
        var cols = new Series[names.Length];
        for (int i = 0; i < names.Length; i++)
        {
            cols[i] = Column(names[i]);
        }
        return cols;
    }
    
    /// <summary>
    /// Get column names in order.
    /// </summary>
    public string[] ColumnNames => PolarsWrapper.GetColumnNames(Handle);
    /// <summary>
    /// Indexer to get a column by position.
    /// Usage: var s = df[0];
    /// </summary>
    public Series this[int index] => Column(index);
    /// <summary>
    /// Syntax Suger: df[row, colName]
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="columnName"></param>
    /// <returns></returns>
    public object? this[int rowIndex, string columnName]
    {
        get
        {
            // 1. 先拿列 (Series)
            var series = Column(columnName);
            // 2. 再拿值 (利用刚才写的 Series 索引器)
            return series[rowIndex];
        }
    }
    /// <summary>
    /// Syntax Suger
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="columnIndex"></param>
    /// <returns></returns>
    public object? this[int rowIndex, int columnIndex]
    {
        get
        {
            var series = Column(columnIndex);
            return series[rowIndex];
        }
    }
    /// <summary>
    /// 获取指定行的数据，返回对象数组。
    /// 类似于 DataTable.Rows[i].ItemArray
    /// </summary>
    public object?[] Row(int index)
    {
        if (index < 0 || index >= Height)
            throw new IndexOutOfRangeException($"Row index {index} is out of bounds. Height: {Height}");

        var rowData = new object?[Width];
        for (int i = 0; i < Width; i++)
        {
            // 复用之前实现的 this[row, col] 索引器
            rowData[i] = this[index, i];
        }
        return rowData;
    }
    /// <summary>
    /// Enable foreach (var series in df) { ... }
    /// </summary>
    /// <returns></returns>
    public IEnumerator<Series> GetEnumerator()
    {
        for (int i = 0; i < Width; i++)
        {
            yield return Column(i);
        }
    }

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    /// <summary>
    /// Generates an HTML representation of the DataFrame.
    /// Useful for rendering in Jupyter/Polyglot Notebooks.
    /// </summary>
    /// <param name="limit">Max rows to display (default 10).</param>
    public string ToHtml(int limit = 10)
    {
        var sb = new StringBuilder();
        
        // 1. 基础样式 (仿 Polars/Pandas 风格)
        sb.Append(@"
<style>
.pl-dataframe { font-family: sans-serif; border-collapse: collapse; width: auto; }
.pl-dataframe th { background-color: #f0f0f0; color: #333; font-weight: bold; text-align: left; padding: 8px; border-bottom: 2px solid #ddd; }
.pl-dataframe td { padding: 8px; border-bottom: 1px solid #ddd; text-align: left; color: #444; }
.pl-dataframe tr:hover { background-color: #f9f9f9; }
.pl-dtype { font-size: 0.8em; color: #888; display: block; font-weight: normal; }
.pl-null { color: #d66; font-style: italic; }
.pl-dim { font-size: 0.8em; color: #666; margin-top: 5px; }
</style>");

        sb.Append("<table class='pl-dataframe'>");

        // 2. 表头 (Name + Type)
        sb.Append("<thead><tr>");
        
        // 既然我们要遍历列，这里顺便把列类型也取出来
        // 我们可以利用 Schema 或者直接取 Series 的 dtype
        for (int i = 0; i < Width; i++)
        {
            var col = Column(i);
            // HTML Encode 防止列名里有 <script> 等坏东西
            var colName = System.Net.WebUtility.HtmlEncode(col.Name);
            var colType = col.DataType.ToString(); // 或者更短的 string 
            
            sb.Append($"<th>{colName}<span class='pl-dtype'>{colType}</span></th>");
        }
        sb.Append("</tr></thead>");

        // 3. 数据体 (Body)
        sb.Append("<tbody>");
        
        int rowsToShow = (int)Math.Min(Height, limit);
        for (int r = 0; r < rowsToShow; r++)
        {
            sb.Append("<tr>");
            var rowData = Row(r); // 利用刚才实现的 Row() 方法
            
            foreach (var val in rowData)
            {
                if (val == null || val == DBNull.Value)
                {
                    sb.Append("<td class='pl-null'>null</td>");
                }
                else
                {
                    // 针对不同类型做一点简单的格式化
                    string cellStr = val switch
                    {
                        DateTime dt => dt.ToString("yyyy-MM-dd HH:mm:ss"),
                        // 还可以针对 float 做精度截断
                        double d => d.ToString("G6"), 
                        _ => val.ToString() ?? ""
                    };
                    
                    // 截断过长的字符串
                    if (cellStr.Length > 50) cellStr = cellStr[..47] + "...";
                    
                    sb.Append($"<td>{System.Net.WebUtility.HtmlEncode(cellStr)}</td>");
                }
            }
            sb.Append("</tr>");
        }
        sb.Append("</tbody>");
        sb.Append("</table>");

        // 4. 底部信息 (Shape)
        var hiddenRows = Height - rowsToShow;
        if (hiddenRows > 0)
        {
            sb.Append($"<div class='pl-dim'>... and {hiddenRows} more rows.</div>");
        }
        sb.Append($"<div class='pl-dim'>shape: ({Height}, {Width})</div>");

        return sb.ToString();
    }
}