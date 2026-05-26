using Polars.NET.Core;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Writes the DataFrame to an Excel file (.xlsx) using the native high-performance engine.
    /// <para>
    /// <b>Performance:</b> Uses columnar writing strategies for maximum speed (via <c>rust_xlsxwriter</c>).
    /// </para>
    /// <para>
    /// <b>Data Integrity:</b> 
    /// <br/>- <c>UInt64</c>, <c>Int128</c>, <c>UInt128</c> will be automatically written as <b>Text</b> to prevent Excel's 53-bit floating-point precision loss.
    /// <br/>- <c>Date</c> and <c>Datetime</c> are written as native Excel date objects with specified formatting.
    /// </para>
    /// </summary>
    /// <param name="path">The file path to save the .xlsx file.</param>
    /// <param name="sheetName">Name of the worksheet. Defaults to "Sheet1" if null.</param>
    /// <param name="dateFormat">
    /// Excel format string for <c>Date</c> columns (e.g., "yyyy-mm-dd"). 
    /// If null, defaults to "yyyy-mm-dd".
    /// </param>
    /// <param name="datetimeFormat">
    /// Excel format string for <c>Datetime</c> columns (e.g., "yyyy-mm-dd hh:mm:ss"). 
    /// If null, defaults to "yyyy-mm-dd hh:mm:ss".
    /// </param>
    public void WriteExcel(
        string path, 
        string? sheetName = null,
        string? dateFormat = null, 
        string? datetimeFormat = null)
    {
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("File path cannot be empty.", nameof(path));

        PolarsWrapper.WriteExcel(Handle, path, sheetName, dateFormat, datetimeFormat);
    }
}