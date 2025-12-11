using Polars.Native;
using Apache.Arrow;
using System.Reflection;
using System.Text.Json;
namespace Polars.CSharp;

/// <summary>
/// DataFrame represents a 2-dimensional labeled data structure similar to a table or spreadsheet.
/// </summary>
public class DataFrame : IDisposable
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
    public Dictionary<string, string> Schema
    {
        get
        {
            var json = PolarsWrapper.GetDataFrameSchemaString(Handle);
            if (string.IsNullOrEmpty(json) || json == "{}")
            {
                return [];
            }

            try 
            {
                return JsonSerializer.Deserialize<Dictionary<string, string>>(json) 
                       ?? [];
            }
            catch
            {
                // 容错：如果 JSON 解析失败，返回空字典
                return [];
            }
        }
    }
    /// <summary>
    /// Prints the schema to the console in a tree format.
    /// Useful for debugging column names and data types.
    /// </summary>
    public void PrintSchema()
    {
        var schema = this.Schema; // 获取刚刚实现的 Dictionary
        
        System.Console.WriteLine("root");
        foreach (var kvp in schema)
        {
            // 格式模仿 Spark:  |-- name: type
            System.Console.WriteLine($" |-- {kvp.Key}: {kvp.Value}");
        }
    }
    /// <summary>
    /// Get a string representation of the DataFrame schema.
    /// </summary>
    /// <returns></returns>
    public override string ToString()
    {
        return $"DataFrame: {Height}x{Width} {string.Join(", ", Schema.Select(kv => $"{kv.Key}:{kv.Value}"))}";
    }
    // ==========================================
    // Static IO Read
    // ==========================================
    /// <summary>
    /// Read CSV File
    /// </summary>
    /// <param name="path"></param>
    /// <param name="tryParseDates"></param>
    /// <returns></returns>
    public static DataFrame ReadCsv(string path, bool tryParseDates = true)
    {
        //
        return new DataFrame(PolarsWrapper.ReadCsv(path, tryParseDates));
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
    /// Create DataFrame from Arrow RecordBatch
    /// </summary>
    public static DataFrame FromArrow(RecordBatch batch)
    {
        //
        return new DataFrame(PolarsWrapper.FromArrow(batch));
    }
    /// <summary>
    /// Read a CSV file asynchronously.
    /// </summary>
    public static async Task<DataFrame> ReadCsvAsync(string path, bool tryParseDates = true)
    {
        var handle = await PolarsWrapper.ReadCsvAsync(path, tryParseDates);
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
        // 假设 Wrapper 里的 Drop 返回的是新的 DataFrameHandle。
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
            this.Handle, 
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
    // ==========================================
    // Display (Show)
    // ==========================================
    /// <summary>
    /// Print the DataFrame to Console in a tabular format.
    /// </summary>
    /// <param name="rows">Number of rows to show.</param>
    /// <param name="maxColWidth">Maximum characters per column before truncation.</param>
    public void Show(int rows = 10, int maxColWidth = 30)
    {
        // 1. 获取预览数据 (Head)
        // 限制 rows 不超过实际高度
        int n = (int)Math.Min(rows, this.Height);
        if (n <= 0) 
        {
            Console.WriteLine("Empty DataFrame");
            return;
        }

        // 使用 Head 获取前 n 行
        using var previewDf = this.Head(n);
        using var batch = previewDf.ToArrow();

        // 2. 准备列信息
        var columns = batch.Schema.FieldsList;
        int colCount = columns.Count;
        var colWidths = new int[colCount];
        var colNames = new string[colCount];

        // 3. 计算每列的最佳宽度
        // 宽度 = Max(列名长度, 前n行中最长值的长度)
        for (int i = 0; i < colCount; i++)
        {
            colNames[i] = columns[i].Name;
            int maxLen = colNames[i].Length;

            var colArray = batch.Column(i);

            // 扫描数据计算宽度 (为了性能，只扫描显示的这几行)
            for (int r = 0; r < n; r++)
            {
                // 使用我们之前写的 FormatValue！
                string val = colArray.FormatValue(r);
                if (val.Length > maxLen) maxLen = val.Length;
            }

            // 应用最大宽度限制
            colWidths[i] = Math.Min(maxLen, maxColWidth) + 2; // +2 padding
        }

        // 4. 打印 Header
        Console.WriteLine($"shape: ({Height}, {Width})");
        Console.Write("┌");
        for (int i = 0; i < colCount; i++)
        {
            // 简单的边框绘制
            Console.Write(new string('─', colWidths[i]));
            Console.Write(i == colCount - 1 ? "┐" : "┬");
        }
        Console.WriteLine();

        Console.Write("│");
        for (int i = 0; i < colCount; i++)
        {
            string content = Truncate(colNames[i], colWidths[i] - 2);
            Console.Write($" {content.PadRight(colWidths[i] - 2)} │");
        }
        Console.WriteLine();

        // 分隔线
        Console.Write("├");
        for (int i = 0; i < colCount; i++)
        {
            Console.Write(new string('─', colWidths[i]));
            Console.Write(i == colCount - 1 ? "┤" : "┼");
        }
        Console.WriteLine();

        // 5. 打印数据行
        for (int r = 0; r < n; r++)
        {
            Console.Write("│");
            for (int i = 0; i < colCount; i++)
            {
                string val = batch.Column(i).FormatValue(r);
                string content = Truncate(val, colWidths[i] - 2);
                
                // 数值右对齐，其他左对齐 (简单起见全部左对齐，或根据类型判断)
                // 这里统一左对齐
                Console.Write($" {content.PadRight(colWidths[i] - 2)} │");
            }
            Console.WriteLine();
        }

        // 底部边框
        Console.Write("└");
        for (int i = 0; i < colCount; i++)
        {
            Console.Write(new string('─', colWidths[i]));
            Console.Write(i == colCount - 1 ? "┘" : "┴");
        }
        Console.WriteLine();
        
        if (Height > n)
        {
            Console.WriteLine($"--- (showing {n} of {Height} rows) ---");
        }
    }
    /// <summary>
    /// Truncate a string to a maximum length, adding "..." if truncated.
    /// </summary>
    /// <param name="s"></param>
    /// <param name="maxLength"></param>
    /// <returns></returns>
    private string Truncate(string s, int maxLength)
    {
        if (string.IsNullOrEmpty(s)) return "";
        if (s.Length <= maxLength) return s;
        return string.Concat(s.AsSpan(0, Math.Max(0, maxLength - 3)), "...");
    }
    // ==========================================
    // Scalar Access & Interop
    // ==========================================
    /// <summary>
    /// Get Scalar Int from DataFrame
    /// </summary>
    /// <param name="colName"></param>
    /// <param name="row"></param>
    /// <returns></returns>
    public long? GetInt(string colName, int row) 
        => PolarsWrapper.GetInt(Handle, colName, row); //
    /// <summary>
    /// Get Scalar Double from DataFrame
    /// </summary>
    /// <param name="colName"></param>
    /// <param name="row"></param>
    /// <returns></returns>
    public double? GetDouble(string colName, int row) 
        => PolarsWrapper.GetDouble(Handle, colName, row); //
    /// <summary>
    /// Get Scalar String from DataFrame
    /// </summary>
    /// <param name="colName"></param>
    /// <param name="row"></param>
    /// <returns></returns>
    public string? GetString(string colName, int row) 
        => PolarsWrapper.GetString(Handle, colName, row); //
    /// <summary>
    /// Transfer a RecordBatch to Arrow
    /// </summary>
    /// <returns></returns>
    public RecordBatch ToArrow()
    {
        //
        return PolarsWrapper.Collect(Handle);
    }
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
    /// Create a DataFrame from a collection of objects (Records/Classes).
    /// Uses reflection to map Properties to Columns.
    /// </summary>
    public static DataFrame From<T>(IEnumerable<T> items)
    {
        var properties = typeof(T).GetProperties(BindingFlags.Public | BindingFlags.Instance);
        var seriesHandles = new SeriesHandle[properties.Length];
        var itemsList = items as IList<T> ?? items.ToList();
        var createdHandles = new List<SeriesHandle>();

        try
        {
            for (int i = 0; i < properties.Length; i++)
            {
                var prop = properties[i];
                var colName = prop.Name;
                var colType = prop.PropertyType;

                // 1. 获取原始数据 (IEnumerable<object>)
                var rawValues = itemsList.Select(item => prop.GetValue(item));

                // 2. [关键修复] 动态调用 Enumerable.Cast<ColType>()
                // 将 IEnumerable<object> 转换为 IEnumerable<int> / IEnumerable<decimal?> 等
                var castMethod = typeof(Enumerable)
                    .GetMethod(nameof(Enumerable.Cast), BindingFlags.Public | BindingFlags.Static)!
                    .MakeGenericMethod(colType);
                
                var castedValues = castMethod.Invoke(null, [rawValues]);

                // 3. 调用 Series.Create<ColType>
                var createMethod = typeof(Series)
                    .GetMethod(nameof(Series.Create), BindingFlags.Public | BindingFlags.Static)!
                    .MakeGenericMethod(colType);
                
                // 现在传入的是类型匹配的 castedValues
                var seriesObj = (Series)createMethod.Invoke(null, [colName, castedValues!])!;
                
                seriesHandles[i] = seriesObj.Handle;
                createdHandles.Add(seriesObj.Handle); 
            }

            // 调用 Wrapper 创建 DataFrame
            // 你的 Wrapper: DataFrameNew(SeriesHandle[])
            return new DataFrame(PolarsWrapper.DataFrameNew(seriesHandles));
        }
        finally
        {      
            foreach (var h in createdHandles)
            {
                if (!h.IsInvalid) h.Dispose();
            }
        }
    }
    // ==========================================
    // Object Mapping (To Records)
    // ==========================================

    /// <summary>
    /// Convert DataFrame rows back to a list of objects.
    /// Note: This materializes the data (ToArrow) and uses reflection.
    /// </summary>
    public IEnumerable<T> Rows<T>() where T : new()
    {
        // 1. 导出数据到 Arrow
        // ToArrow 会自动 Rechunk，所以我们拿到的是连续内存
        using var batch = this.ToArrow();
        int rowCount = batch.Length;

        // 2. 准备反射元数据 (缓存起来避免循环内反射)
        var type = typeof(T);
        var properties = type.GetProperties(BindingFlags.Public | BindingFlags.Instance)
                             .Where(p => p.CanWrite) // 必须能写入
                             .ToArray();

        // 3. 预先查找每一列对应的 Arrow Array，并检查类型兼容性
        var columnAccessors = new Func<int, object?>[properties.Length];

        for (int i = 0; i < properties.Length; i++)
        {
            var prop = properties[i];
            var colName = prop.Name;
            
            // 查找列 (不区分大小写可能会更友好，但这里先严格匹配)
            var col = batch.Column(colName);
            if (col == null) 
            {
                // 如果找不到列，可以选择抛错或者忽略（保留默认值）
                // 这里我们选择忽略，对应的 accessor 为 null
                columnAccessors[i] = _ => null; 
                continue;
            }

            // 创建读取器委托 (针对目标属性类型进行适配)
            columnAccessors[i] = CreateAccessor(col, prop.PropertyType);
        }

        // 4. 遍历行，构造对象
        for (int i = 0; i < rowCount; i++)
        {
            var item = new T();
            for (int p = 0; p < properties.Length; p++)
            {
                var accessor = columnAccessors[p];
                if (accessor != null) // 如果该属性有对应的列
                {
                    // 获取值
                    var val = accessor(i);
                    // 只有非 null 才赋值 (避免覆盖默认值)
                    if (val != null)
                    {
                        properties[p].SetValue(item, val);
                    }
                }
            }
            yield return item;
        }
    }

    // --- 智能类型转换工厂 ---
    // 根据 Arrow 列类型 和 C# 目标类型，生成最高效的读取器
    private static Func<int, object?> CreateAccessor(IArrowArray array, Type targetType)
    {
        // 获取底层类型 (处理 Nullable<T>)
        var underlyingType = Nullable.GetUnderlyingType(targetType) ?? targetType;

        // 1. String
        if (underlyingType == typeof(string))
        {
            return array.GetStringValue; // 使用之前写的扩展方法
        }

        // 2. Int / Long (Arrow 默认通常是 Int64)
        if (underlyingType == typeof(int) || underlyingType == typeof(long))
        {
            return idx => 
            {
                long? val = array.GetInt64Value(idx); // 扩展方法获取 long?
                if (!val.HasValue) return null;
                
                // 窄化转换
                if (underlyingType == typeof(int)) return (int)val.Value;
                return val.Value;
            };
        }

        // 3. Double / Float
        if (underlyingType == typeof(double) || underlyingType == typeof(float))
        {
            return idx => 
            {
                // [优化] 直接调用扩展方法 array.GetDoubleValue(idx)
                // 它的内部实现已经包含了对 DoubleArray, FloatArray, IntArray 的 switch/case
                // 所以这里不需要再写 if (array is DoubleArray) 了
                double? v = array.GetDoubleValue(idx);
                
                if (!v.HasValue) return null;
                
                // 如果目标属性是 float，需要从 double 强转回来 (窄化转换)
                if (underlyingType == typeof(float)) return (float)v.Value;
                
                return v.Value;
            };
        }

        // 4. Decimal (核心！)
        if (underlyingType == typeof(decimal))
        {
            return idx =>
            {
                if (array is Decimal128Array decArr)
                {
                    return decArr.GetValue(idx); // Arrow 自动处理了 Scale，返回 C# decimal?
                }
                // 兼容：如果 Polars 传回的是 Double (还没转 Decimal)，尝试强转
                if (array is DoubleArray dArr)
                {
                    var v = dArr.GetValue(idx);
                    return v.HasValue ? (decimal)v.Value : null;
                }
                return null;
            };
        }

        // 5. Bool
        if (underlyingType == typeof(bool))
        {
            return idx => 
            {
                if (array is BooleanArray bArr) return bArr.GetValue(idx);
                return null;
            };
        }
        // 6. DateTime
        if (underlyingType == typeof(DateTime))
        {
            return idx => 
            {
                // 调用 ArrowExtensions.GetDateTime (它会自动处理 Date32/Date64/Timestamp)
                DateTime? val = array.GetDateTime(idx);
                
                if (!val.HasValue) return null;
                
                // 如果目标是 DateTime (非空)，直接返回 Value
                // 如果目标是 DateTime?，也返回 Value (会被装箱)
                return val.Value;
            };
        }
        // 默认回退 (低效但安全)
        return _ => null;
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
        // 为什么？因为 Rust 的 into_lazy() 会消耗掉 DataFrame。
        // 如果我们直接传 Handle，这个 C# DataFrame 对象就会变废（底层指针被释放或转移），
        // 用户如果再次使用这个 DataFrame 就会崩。
        // 为了符合 C# 的直觉（调用 .Lazy() 不应该销毁原对象），我们先 Clone 一份传给 Lazy。
        var clonedHandle = PolarsWrapper.CloneDataFrame(Handle);
        
        // 2. 转换为 LazyFrame
        var lfHandle = PolarsWrapper.DataFrameToLazy(clonedHandle);
        
        return new LazyFrame(lfHandle);
    }
}