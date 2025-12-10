using Polars.Native;
using Apache.Arrow;
using System.Reflection;
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
    /// <summary>
    /// Output DataFrame structure to string
    /// </summary>
    /// <returns></returns>
    public override string ToString()
    {
        return $"DataFrame: {Height}x{Width} [{string.Join(", ", Columns)}]";
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
                
                var castedValues = castMethod.Invoke(null, new object[] { rawValues });

                // 3. 调用 Series.Create<ColType>
                var createMethod = typeof(Series)
                    .GetMethod(nameof(Series.Create), BindingFlags.Public | BindingFlags.Static)!
                    .MakeGenericMethod(colType);
                
                // 现在传入的是类型匹配的 castedValues
                var seriesObj = (Series)createMethod.Invoke(null, new object[] { colName, castedValues! })!;
                
                seriesHandles[i] = seriesObj.Handle;
                createdHandles.Add(seriesObj.Handle); 
            }

            // 调用 Wrapper 创建 DataFrame
            // 你的 Wrapper: DataFrameNew(SeriesHandle[])
            return new DataFrame(PolarsWrapper.DataFrameNew(seriesHandles));
        }
        finally
        {
            // [生命周期管理]
            // DataFrameNew 调用完后，Rust 已经有了这些 Series (不管是 Move 还是 Clone)。
            // 如果 Rust 是 Clone 的，我们这里必须 Dispose 旧的 Handles。
            // 如果 Rust 是 Move 的，我们这里必须 SetHandleAsInvalid (TransferOwnership)。
            
            // 通常 pl_dataframe_new(columns) 会消耗 columns。
            // 如果你的 C# Wrapper 用的是 DangerousGetHandle，说明没交出所有权。
            // 那意味着 Rust 那边必须 Clone。
            // 如果 Rust Clone 了，那我们这里就可以安全释放 C# 侧的 Handles 了。
            
            foreach (var h in createdHandles)
            {
                if (!h.IsInvalid) h.Dispose();
            }
        }
    }
}