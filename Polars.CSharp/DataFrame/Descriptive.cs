using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using System.Text;
using Apache.Arrow.Types;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// True if the DataFrame contains no rows.
    /// </summary>
    public bool IsEmpty => Height == 0;
    /// <summary>
    /// True if the DataFrame has no columns and no rows (0x0).
    /// Represents a completely uninitialized or structurally void DataFrame.
    /// </summary>
    public bool IsVoid => Width == 0;
    /// <summary>
    /// Get a mask of all duplicated rows in this DataFrame.
    /// </summary>
    public Series IsDuplicated() => new(PolarsWrapper.DataFrameIsDuplicated(Handle));

    /// <summary>
    /// Get a mask of all unique rows in this DataFrame.
    /// </summary>
    public Series IsUnique() => new(PolarsWrapper.DataFrameIsUnique(Handle));
    /// <summary>
    /// Check whether the DataFrame is sorted by the given columns.
    /// </summary>
    /// <param name="by">Column name(s) to check.</param>
    /// <param name="descending">Sort in descending order. When sorting by multiple columns, can be specified per column by passing a sequence of booleans.</param>
    /// <param name="nullsLast">Place null values last. When sorting by multiple columns, can be specified per column by passing a sequence of booleans.</param>
    public bool IsSorted(
        IEnumerable<string> by,
        IEnumerable<bool>? descending = null,
        IEnumerable<bool>? nullsLast = null)
    {
        string[] byArray = by as string[] ?? [.. by];
        bool[]? descArray = descending as bool[] ?? (descending is not null ? [.. descending] : null);
        bool[]? nullsLastArray = nullsLast as bool[] ?? (nullsLast is not null ? [.. nullsLast] : null);

        return PolarsWrapper.IsSorted(Handle, byArray, descArray, nullsLastArray);
    }
    /// <summary>
    /// Overload for checking if DataFrame is sorted by a single column.
    /// </summary>
    public bool IsSorted(string by, bool descending = false, bool nullsLast = false)
        => IsSorted([by], [descending], [nullsLast]);

    /// <summary>
    /// Return an estimation of the total (heap) allocated size of the Series/DataFrame.
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
            ("count",      c => Pl.Col(c).Count().Cast(DataType.Float64)),
            ("null_count", c => Pl.Col(c).IsNull().Sum().Cast(DataType.Float64)),
            ("mean",       c => Pl.Col(c).Mean()),
            ("std",        c => Pl.Col(c).Std()),
            ("min",        c => Pl.Col(c).Min().Cast(DataType.Float64)),
            ("25%",        c => Pl.Col(c).Quantile(0.25, QuantileMethod.Nearest).Cast(DataType.Float64)),
            ("50%",        c => Pl.Col(c).Median().Cast(DataType.Float64)),
            ("75%",        c => Pl.Col(c).Quantile(0.75, QuantileMethod.Nearest).Cast(DataType.Float64)),
            ("max",        c => Pl.Col(c).Max().Cast(DataType.Float64))
        };

        var rowFrames = new List<DataFrame>();

        try
        {
            foreach (var (statName, op) in metrics)
            {
                var exprs = new List<Expr>
                {
                    Pl.Lit(statName).Alias("statistic")
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
        using var strDf = headDf.Select(Pl.All().Cast(DataType.String));

        var rowInfos = new List<(string Name, string DType, string Values)>(cols.Count);
        int maxNameLen = 0;
        int maxDtypeLen = 0;

        for (int colIdx = 0; colIdx < cols.Count; colIdx++)
        {
            var colName = cols[colIdx].Name;
            var dtype = cols[colIdx].Type;

            string displayColName = colName.Length > maxColnameLength
                ? $"{colName.AsSpan(0, maxColnameLength - 1)}…"
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
        using var strDf = headDf.Select(Pl.All().Cast(DataType.String));

        var outColNames = new string[cols.Count];
        var outDtypes = new string[cols.Count];

        var outValues = new string[cols.Count][];

        for (int colIdx = 0; colIdx < cols.Count; colIdx++)
        {
            var colName = cols[colIdx].Name;
            var dtype = cols[colIdx].Type;

            outColNames[colIdx] = colName.Length > maxColnameLength
                ? $"{colName.AsSpan(0, maxColnameLength - 1)}…"
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
    /// Generates a rich HTML table representation of the DataFrame for interactive environments (e.g., LINQPad, Jupyter).
    /// </summary>
    /// <returns>An HTML string representing the preview of the DataFrame.</returns>
    public string ToHtml()
    {
        int? configRows = CoreConfig.TableMaxRows;
        int rowsToShowSetting = (configRows > 0) ? configRows.Value : 10;
        int rowsToShow = (int)Math.Min(Height, rowsToShowSetting);

        int? configCols = CoreConfig.TableMaxCols;
        int maxColsToShow = (configCols > 0) ? configCols.Value : 8;

        int rawColCount = (int)Width;
        bool isColTruncated = rawColCount > maxColsToShow;
        int colCount = isColTruncated ? maxColsToShow : rawColCount;

        // 1. Fetch native Polars Schema
        var schema = this.Schema;
        var columnNames = schema.Keys.Take(colCount).ToArray();

        using var previewDf = this.Head(rowsToShow);

        var sb = new StringBuilder();

        sb.Append(@"
<style>
.pl-dataframe { font-family: 'Consolas', 'Monaco', monospace; font-size: 13px; border-collapse: collapse; border: 1px solid rgba(128, 128, 128, 0.2); }
.pl-dataframe th { font-weight: bold; text-align: left; padding: 6px 12px; border-bottom: 2px solid rgba(128, 128, 128, 0.3); color: inherit; }
.pl-dataframe td { padding: 6px 12px; border-bottom: 1px solid rgba(128, 128, 128, 0.2); white-space: pre; color: inherit; }
.pl-dataframe tr:nth-child(even) { background-color: rgba(128, 128, 128, 0.05); }
.pl-dataframe tr:hover { background-color: rgba(128, 128, 128, 0.1); }
.pl-dtype { font-size: 10px; color: rgba(128, 128, 128, 0.8); display: block; margin-top: 2px; font-weight: normal; }
.pl-null { color: rgba(128, 128, 128, 0.5); font-style: italic; }
.pl-dim { font-family: sans-serif; font-size: 12px; opacity: 0.8; margin-bottom: 8px; }
</style>");

        // 2. Dimensions metadata
        sb.Append($"<div class='pl-dim'>Polars DataFrame: <b>({Height:N0} rows, {rawColCount} columns)</b></div>");
        sb.Append("<div style='overflow-x:auto'><table class='pl-dataframe'>");

        // 3. Table Header using native Polars Schema & DataType
        sb.Append("<thead><tr>");
        for (int c = 0; c < colCount; c++)
        {
            string colName = columnNames[c];
            var dt = schema[colName]; // Native Polars DataType
            string typeName = System.Net.WebUtility.HtmlEncode(dt.ToString());

            sb.Append($"<th>{System.Net.WebUtility.HtmlEncode(colName)}<span class='pl-dtype'>{typeName}</span></th>");
        }
        if (isColTruncated)
        {
            sb.Append("<th>...</th>");
        }
        sb.Append("</tr></thead>");

        // 4. Table Body from sliced Series
        sb.Append("<tbody>");
        var previewCols = new Series[colCount];
        for (int c = 0; c < colCount; c++)
        {
            previewCols[c] = previewDf[columnNames[c]];
        }

        try
        {
            for (int r = 0; r < rowsToShow; r++)
            {
                sb.Append("<tr>");
                for (int c = 0; c < colCount; c++)
                {
                    var series = previewCols[c];
                    object? val = series[r];

                    if (val is null)
                    {
                        sb.Append("<td class='pl-null'>null</td>");
                    }
                    else
                    {
                        string valStr = val switch
                        {
                            double d => d.ToString("G10"),
                            float f => f.ToString("G7"),
                            _ => val.ToString() ?? "null"
                        };

                        if (valStr.Length > 100)
                        {
                            valStr = $"{valStr.AsSpan(0, 97)}...";
                        }

                        sb.Append($"<td>{System.Net.WebUtility.HtmlEncode(valStr)}</td>");
                    }
                }

                if (isColTruncated)
                {
                    sb.Append("<td style='font-style:italic; opacity: 0.5; text-align:center'>...</td>");
                }
                sb.Append("</tr>");
            }
        }
        finally
        {
            // Clean up sliced Series handles
            for (int c = 0; c < colCount; c++)
            {
                previewCols[c]?.Dispose();
            }
        }

        // 5. Remaining rows indicator
        int finalDisplayColSpan = isColTruncated ? colCount + 1 : colCount;
        long remaining = Height - rowsToShow;
        if (remaining > 0)
        {
            sb.Append($"<tr><td colspan='{finalDisplayColSpan}' style='text-align:center; font-style:italic; opacity: 0.6; padding: 10px'>... {remaining:N0} more rows ...</td></tr>");
        }

        sb.Append("</tbody></table></div>");
        return sb.ToString();
    }

    /// <summary>
    /// Duck-typed method called automatically by LINQPad to customize Dump() rendering.
    /// Uses reflection to wrap HTML in LINQPad.Util.RawHtml without introducing compile-time dependencies.
    /// </summary>
    internal object ToDump()
    {
        string html = ToHtml();

        var utilType = Type.GetType("LINQPad.Util, LINQPad")
                    ?? Type.GetType("LINQPad.Util, LINQPad.Runtime")
                    ?? AppDomain.CurrentDomain.GetAssemblies()
                        .FirstOrDefault(a => a.GetName().Name?.StartsWith("LINQPad") is true)
                        ?.GetType("LINQPad.Util");

        if (utilType != null)
        {
            var rawHtmlMethod = utilType.GetMethod("RawHtml", [typeof(string)]);
            if (rawHtmlMethod != null)
            {
                return rawHtmlMethod.Invoke(null, [html]) ?? html;
            }
        }

        return html;
    }
}