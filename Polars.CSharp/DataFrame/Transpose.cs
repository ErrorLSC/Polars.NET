using Polars.NET.Core;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;

/// <summary>
/// DataFrame represents a 2-dimensional labeled data structure similar to a table or spreadsheet.
/// </summary>
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// 
    /// </summary>
    /// <param name="includeHeader"></param>
    /// <param name="headerName"></param>
    /// <returns></returns>
    public DataFrame Transpose(bool includeHeader = false, string headerName = "column")
    {
        string? keepNamesAs = includeHeader ? headerName : null;
        
        var newHandle = PolarsWrapper.DataFrameTranspose(Handle, keepNamesAs, null, null);
        return new DataFrame(newHandle);
    }
    /// <summary>
    /// 
    /// </summary>
    /// <param name="columnName"></param>
    /// <param name="includeHeader"></param>
    /// <param name="headerName"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public DataFrame Transpose(string columnName, bool includeHeader = false, string headerName = "column")
    {
        if (string.IsNullOrWhiteSpace(columnName))
            throw new ArgumentException("Column name cannot be null or empty.", nameof(columnName));

        string? keepNamesAs = includeHeader ? headerName : null;
        
        var newHandle = PolarsWrapper.DataFrameTranspose(Handle, keepNamesAs, columnName, null);
        return new DataFrame(newHandle);
    }
    /// <summary>
    /// 
    /// </summary>
    /// <param name="customNames"></param>
    /// <param name="includeHeader"></param>
    /// <param name="headerName"></param>
    /// <returns></returns>
    public DataFrame Transpose(IEnumerable<string> customNames, bool includeHeader = false, string headerName = "column")
    {
        ArgumentNullException.ThrowIfNull(customNames);

        string? keepNamesAs = includeHeader ? headerName : null;
        
        string[] namesArray = [.. customNames];
        
        var newHandle = PolarsWrapper.DataFrameTranspose(Handle, keepNamesAs, null, namesArray);
        return new DataFrame(newHandle);
    }
}