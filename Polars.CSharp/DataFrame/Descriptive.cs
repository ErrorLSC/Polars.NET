using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using System.Text;
using Apache.Arrow.Types;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// True if the DataFrame contains no rows.
    /// </summary>
    public bool IsEmpty => Height == 0;
    /// <summary>
    /// Get a mask of all duplicated rows in this DataFrame.
    /// </summary>
    public Series IsDuplicated() => new(PolarsWrapper.DataFrameIsDuplicated(Handle));

    /// <summary>
    /// Get a mask of all unique rows in this DataFrame.
    /// </summary>
    public Series IsUnique() => new(PolarsWrapper.DataFrameIsUnique(Handle));
    /// <summary>
    /// Return an estimation of the total (heap) allocated size of the DataFrame.
    /// Estimated size is given in the specified unit (bytes by default).
    /// </summary>
    /// <param name="unit">Scale the returned size to the given unit (uses 1024 base).</param>
    /// <returns>The estimated size as a double.</returns>
    public double EstimatedSize(SizeUnit unit = SizeUnit.Bytes)
    {
        long bytes = PolarsWrapper.DataFrameEstimatedSize(Handle);

        return unit switch
        {
            SizeUnit.Bytes     => bytes, 
            SizeUnit.Kilobytes => bytes / 1024.0,
            SizeUnit.Megabytes => bytes / Math.Pow(1024, 2),
            SizeUnit.Gigabytes => bytes / Math.Pow(1024, 3),
            SizeUnit.Terabytes => bytes / Math.Pow(1024, 4),
            _ => throw new ArgumentOutOfRangeException(nameof(unit), $"Unsupported size unit: {unit}")
        };
    }

    /// <summary>
    /// Generate a summary statistics DataFrame (count, mean, std, min, 25%, 50%, 75%, max).
    /// Similar to pandas/polars describe().
    /// </summary>
    public DataFrame Describe()
    {
        using var schema = Schema;
        
        var numericCols = new List<string>();

        foreach (var name in schema.ColumnNames)
        {
            using var dtype = schema[name];
            
            if (dtype.IsNumeric)
            {
                numericCols.Add(name);
            }
        }

        if (numericCols.Count == 0)
            throw new InvalidOperationException("No numeric columns to describe.");

        // 2. Define statistical metrics
        var metrics = new List<(string Name, Func<string, Expr> Op)>
        {
            ("count",      c => Polars.Col(c).Count().Cast(DataType.Float64)),
            ("null_count", c => Polars.Col(c).IsNull().Sum().Cast(DataType.Float64)),
            ("mean",       c => Polars.Col(c).Mean()),
            ("std",        c => Polars.Col(c).Std()),
            ("min",        c => Polars.Col(c).Min().Cast(DataType.Float64)),
            ("25%",        c => Polars.Col(c).Quantile(0.25, QuantileMethod.Nearest).Cast(DataType.Float64)),
            ("50%",        c => Polars.Col(c).Median().Cast(DataType.Float64)),
            ("75%",        c => Polars.Col(c).Quantile(0.75, QuantileMethod.Nearest).Cast(DataType.Float64)),
            ("max",        c => Polars.Col(c).Max().Cast(DataType.Float64))
        };

        var rowFrames = new List<DataFrame>();
        
        try
        {
            foreach (var (statName, op) in metrics)
            {
                var exprs = new List<Expr>
                {
                    Polars.Lit(statName).Alias("statistic")
                };

                foreach (var col in numericCols)
                {
                    exprs.Add(op(col));
                }

                rowFrames.Add(Select(exprs));
            }

            return Concat(rowFrames);
        }
        finally
        {
            foreach (var frame in rowFrames)
            {
                frame.Dispose();
            }
        }
    }
    /// <summary>
    /// Print a dense preview of the DataFrame to the standard output.
    /// The formatting shows one line per column so that wide dataframes display cleanly.
    /// </summary>
    /// <param name="maxItemsPerColumn">Maximum number of items to show per column.</param>
    /// <param name="maxColnameLength">Maximum length of the displayed column names.</param>
    public void Glimpse(int maxItemsPerColumn = 10, int maxColnameLength = 50)
        => Console.Write(GlimpseString(maxItemsPerColumn, maxColnameLength));

    /// <summary>
    /// Return a dense preview of the DataFrame as a formatted string.
    /// </summary>
    public string GlimpseString(int maxItemsPerColumn = 10, int maxColnameLength = 50)
    {
        long nRows = Height;
        long nCols = Width;
        long limit = Math.Min(maxItemsPerColumn, nRows);

        var schema = Schema;
        var cols = schema.ToList();

        using var headDf = Head((int)limit);
        using var strDf = headDf.Select(Expr.All().Cast(DataType.String));

        var rowInfos = new List<(string Name, string DType, string Values)>(cols.Count);
        int maxNameLen = 0;
        int maxDtypeLen = 0;

        for (int colIdx = 0; colIdx < cols.Count; colIdx++)
        {
            var colName = cols[colIdx].Name;
            var dtype = cols[colIdx].Type;

            string displayColName = colName.Length > maxColnameLength
                ? string.Concat(colName.AsSpan(0, maxColnameLength - 1), "…")
                : colName;
            
            maxNameLen = Math.Max(maxNameLen, displayColName.Length);

            string dtypeStr = $"<{dtype.Kind}>"; 
            maxDtypeLen = Math.Max(maxDtypeLen, dtypeStr.Length);

            using var strSeries = strDf[colIdx]; 
            
            var valStrs = new List<string>((int)limit);
            for (long rowIdx = 0; rowIdx < limit; rowIdx++)
            {
                if (strSeries.IsNullAt(rowIdx))
                {
                    valStrs.Add("null");
                }
                else
                {
                    string? s = strSeries.GetValue<string>(rowIdx);
                    
                    if (dtype.Kind == DataTypeKind.String)
                    {
                        valStrs.Add($"'{s}'");
                    }
                    else
                    {
                        valStrs.Add(s ?? "null");
                    }
                }
            }

            rowInfos.Add((displayColName, dtypeStr, string.Join(", ", valStrs)));
        }

        var sb = new StringBuilder();
        sb.AppendLine($"Rows: {nRows}");
        sb.AppendLine($"Columns: {nCols}");

        foreach (var (Name, DType, Values) in rowInfos)
        {
            sb.AppendLine($"$ {Name.PadRight(maxNameLen)} {DType.PadLeft(maxDtypeLen)} {Values}");
        }

        return sb.ToString();
    }

    /// <summary>
    /// Return a dense preview of the DataFrame as a new DataFrame.
    /// Schema: "column" (String), "dtype" (String), "values" (List[String])
    /// </summary>
    public DataFrame GlimpseFrame(int maxItemsPerColumn = 10, int maxColnameLength = 50)
    {
        long nRows = Height;
        int limit = (int)Math.Min(maxItemsPerColumn, nRows);

        var cols = Schema.ToList();
        
        using var headDf = Head(limit);
        using var strDf = headDf.Select(Expr.All().Cast(DataType.String));

        var outColNames = new string[cols.Count];
        var outDtypes = new string[cols.Count];
        
        var outValues = new string[cols.Count][];

        for (int colIdx = 0; colIdx < cols.Count; colIdx++)
        {
            var colName = cols[colIdx].Name;
            var dtype = cols[colIdx].Type;

            outColNames[colIdx] = colName.Length > maxColnameLength
                ? string.Concat(colName.AsSpan(0, maxColnameLength - 1), "…")
                : colName;

            outDtypes[colIdx] = dtype.ToString();

            using var strSeries = strDf[colIdx];
            
            var valArray = new string[limit];
            
            for (long rowIdx = 0; rowIdx < limit; rowIdx++)
            {
                if (strSeries.IsNullAt(rowIdx))
                {
                    valArray[rowIdx] = "null";
                }
                else
                {
                    string? s = strSeries.GetValue<string>(rowIdx);
                    valArray[rowIdx] = dtype == DataType.String ? $"'{s}'" : (s ?? "null");
                }
            }

            outValues[colIdx] = valArray;
        }
        var s1 = Series.From("column", outColNames);
        var s2 = Series.From("dtype", outDtypes);
        
        var s3 = Series.From("values", outValues); 

        return DataFrame.FromSeries(s1, s2, s3);
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
        return PolarsWrapper.DataFrameToString(Handle);
    }

    /// <summary>
    /// Print the DataFrame to Console.
    /// </summary>
    public void Show() => Console.WriteLine(ToString());
    /// <summary>
    /// Generates an HTML representation of the DataFrame.
    /// Useful for rendering in Jupyter/Polyglot Notebooks.
    /// </summary>
    /// <param name="limit">Max rows to display (default 10).</param>
    public string ToHtml(int limit = 10)
    {
        int rowsToShow = (int)Math.Min(Height, limit);
        using var previewDf = this.Head(rowsToShow);
        
        using var batch = ArrowFfiBridge.ExportDataFrame(previewDf.Handle);
        var schema = batch.Schema;

        var sb = new StringBuilder();
        
        sb.Append(@"
<style>
.pl-dataframe { font-family: 'Consolas', 'Monaco', monospace; font-size: 13px; border-collapse: collapse; border: 1px solid #e0e0e0; }
.pl-dataframe th { background-color: #f0f0f0; font-weight: bold; text-align: left; padding: 6px 12px; border-bottom: 2px solid #ccc; }
.pl-dataframe td { padding: 6px 12px; border-bottom: 1px solid #f0f0f0; white-space: pre; color: #333; }
.pl-dataframe tr:nth-child(even) { background-color: #f9f9f9; }
.pl-dataframe tr:hover { background-color: #f1f1f1; }
.pl-dtype { font-size: 10px; color: #999; display: block; margin-top: 2px; font-weight: normal; }
.pl-null { color: #d0d0d0; font-style: italic; }
.pl-dim { font-family: sans-serif; font-size: 12px; color: #666; margin-bottom: 8px; }
</style>");

        // Dimensions Info
        sb.Append($"<div class='pl-dim'>Polars DataFrame: <b>({Height} rows, {Width} columns)</b></div>");
        
        sb.Append("<div style='overflow-x:auto'><table class='pl-dataframe'>");

        // Header (From Arrow Schema)
        sb.Append("<thead><tr>");
        foreach (var field in schema.FieldsList)
        {
            var colName = System.Net.WebUtility.HtmlEncode(field.Name);
            var colType = field.DataType.Name; 
            
            sb.Append($"<th>{colName}<span class='pl-dtype'>{colType}</span></th>");
        }
        sb.Append("</tr></thead>");

        // Body (From Arrow Batch)
        sb.Append("<tbody>");
        
        int rowCount = batch.Length;
        int colCount = batch.ColumnCount;

        for (int r = 0; r < rowCount; r++)
        {
            sb.Append("<tr>");
            for (int c = 0; c < colCount; c++)
            {
                var colArray = batch.Column(c);
                var field = schema.GetFieldByIndex(c);
                
                string valStr = colArray.FormatValue(r);

                if (valStr != "null")
                {
                    if (field.DataType.TypeId == ArrowTypeId.Double)
                    {
                        if (double.TryParse(valStr, out double d)) 
                            valStr = d.ToString("G10");
                    }
                    else if (field.DataType.TypeId == ArrowTypeId.Float)
                    {
                        if (float.TryParse(valStr, out float f)) 
                            valStr = f.ToString("G7");
                    }
                }

                if (valStr == "null")
                {
                    sb.Append("<td class='pl-null'>null</td>");
                }
                else
                {
                    if (valStr.Length > 100) valStr = valStr[..97] + "...";
                    sb.Append($"<td>{System.Net.WebUtility.HtmlEncode(valStr)}</td>");
                }
            }
            sb.Append("</tr>");
        }
        
        // Footer for hidden rows
        long remaining = Height - rowsToShow;
        if (remaining > 0)
        {
             sb.Append($"<tr><td colspan='{colCount}' style='text-align:center; font-style:italic; color:#999; padding: 10px'>... {remaining} more rows ...</td></tr>");
        }

        sb.Append("</tbody></table></div>");

        return sb.ToString();
    }
}