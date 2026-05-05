using Polars.NET.Core;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Read an Excel file (.xlsx) into a DataFrame.
    /// <para>
    /// This uses the high-performance native Rust 'calamine' engine. 
    /// It is significantly faster and more memory-efficient than traditional .NET Excel libraries.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the .xlsx file.</param>
    /// <param name="sheetName">Name of the sheet to read. If provided, it takes precedence over <paramref name="sheetIndex"/>.</param>
    /// <param name="sheetIndex">Index of the sheet to read (0-based). Default is 0 (the first sheet).</param>
    /// <param name="schema">
    /// Optional schema overrides. 
    /// Provide this to strictly enforce specific column types (e.g., forcing a numeric column to be read as String).
    /// </param>
    /// <param name="hasHeader">Indicates if the first row contains header names. Default is true.</param>
    /// <param name="inferSchemaLen">
    /// Number of rows to use for schema inference. 
    /// (Note: The underlying engine scans implicitly, but this is kept for API alignment).
    /// </param>
    /// <param name="dropEmptyRows">If true, rows where all cells are empty or null will be skipped. Default is true.</param>
    /// <param name="raiseIfEmpty">If true, throws an exception if the sheet is empty or contains no data. Default is true.</param>
    /// <returns>A new DataFrame containing the Excel data.</returns>
    /// <exception cref="FileNotFoundException">Thrown if the file does not exist.</exception>
    public static DataFrame ReadExcel(
        string path,
        string? sheetName = null,
        ulong sheetIndex = 0,
        IntoSchema? schema = null,
        bool hasHeader = true,
        ulong inferSchemaLen = 100,
        bool dropEmptyRows = true,
        bool raiseIfEmpty = true)
    {
        if (!File.Exists(path))
            throw new FileNotFoundException($"Excel file not found: {path}");

        var schemaHandle = schema?.Consume().Handle;

        var h = PolarsWrapper.ReadExcel(
            path,
            sheetName,
            sheetIndex,
            schemaHandle,
            hasHeader,
            inferSchemaLen,
            dropEmptyRows,
            raiseIfEmpty
        );

        return new DataFrame(h);
    }
}