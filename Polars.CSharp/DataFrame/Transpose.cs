#pragma warning disable CS1573
using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// DataFrame represents a 2-dimensional labeled data structure similar to a table or spreadsheet.
/// </summary>
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Transpose a DataFrame over the diagonal.
    /// </summary>
    /// <param name="includeHeader">If set, the column names will be added as first column.</param>
    /// <param name="headerName">If include_header is set, this determines the name of the column that will be inserted.</param>
    /// <returns></returns>
    public DataFrame Transpose(bool includeHeader = false, string headerName = "column")
    {
        string? keepNamesAs = includeHeader ? headerName : null;
        
        var newHandle = PolarsWrapper.DataFrameTranspose(Handle, keepNamesAs, null, null);
        return new DataFrame(newHandle);
    }
    /// <summary>
    /// Transpose a DataFrame over the diagonal.
    /// </summary>
    /// <param name="columnName">Optional iterable yielding strings or a string naming an existing column. These will name the value (non-header) columns in the transposed data.</param>
    /// <inheritdoc cref="Transpose(bool,string)"/>
    public DataFrame Transpose(string columnName, bool includeHeader = false, string headerName = "column")
    {
        if (string.IsNullOrWhiteSpace(columnName))
            throw new ArgumentException("Column name cannot be null or empty.", nameof(columnName));

        string? keepNamesAs = includeHeader ? headerName : null;
        
        var newHandle = PolarsWrapper.DataFrameTranspose(Handle, keepNamesAs, columnName, null);
        return new DataFrame(newHandle);
    }
    /// <summary>
    /// Transpose a DataFrame over the diagonal.
    /// </summary>
    /// <param name="customNames">Optional iterable yielding strings or a string naming an existing column. These will name the value (non-header) columns in the transposed data.</param>
    /// <inheritdoc cref="Transpose(bool,string)"/>
    public DataFrame Transpose(IEnumerable<string> customNames, bool includeHeader = false, string headerName = "column")
    {
        ArgumentNullException.ThrowIfNull(customNames);

        string? keepNamesAs = includeHeader ? headerName : null;
        
        string[] namesArray = [.. customNames];
        
        var newHandle = PolarsWrapper.DataFrameTranspose(Handle, keepNamesAs, null, namesArray);
        return new DataFrame(newHandle);
    }
}