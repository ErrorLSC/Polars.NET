using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable, IEnumerable<Series>
{
    /// <summary>
    /// Read a sheet of an Excel workbook into a DataFrame.
    /// <para>
    /// .xlsx, .xlsb, .xls (Excel 97–2003), .ods and flat .fods workbooks are streamed by the managed reader of
    /// Kaminari.Seseragi.Files (the format is detected from the content).
    /// </para>
    /// <para>
    /// Column types are inferred from the first <paramref name="inferSchemaLen"/> rows: whole numbers give Int64, other
    /// numbers Float64, booleans Boolean, text String; by number format, dates give Date, dates with times
    /// Datetime(μs), times of day Time, elapsed times Duration(μs); any other mix gives String. A later value that does
    /// not fit raises an error naming its cell. Error values (#N/A …) read as nulls.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the workbook.</param>
    /// <param name="sheetName">Name of the sheet to read (case-insensitive). Takes precedence over <paramref name="sheetIndex"/>.</param>
    /// <param name="sheetIndex">Index of the sheet to read (0-based tab position; hidden sheets and chart sheets count, and a chart sheet cannot be read). Default is 0.</param>
    /// <param name="schema">
    /// Column types by name instead of inferred ones (numbers convert to integer, floating-point, decimal, string and
    /// temporal types; text is parsed; a time zone reads the workbook's clock times as that zone's local time).
    /// </param>
    /// <param name="hasHeader">The first row with a value holds the column names. Default is true. Blank names become
    /// <c>__UNNAMED__i</c>, repeated ones get <c>_1</c>, <c>_2</c>…</param>
    /// <param name="inferSchemaLen">Rows the column types are inferred from (0 reads every column as String).</param>
    /// <param name="dropEmptyRows">Skip rows without values. Default is true.</param>
    /// <param name="raiseIfEmpty">Throw if the sheet has no data rows. Default is true.</param>
    /// <param name="columns">Columns to read, by name (in sheet order).</param>
    /// <param name="dropEmptyColumns">Leave out columns without a header name and without values in the first rows. Default is true.</param>
    /// <param name="tableName">Read a table (list object) by name instead of a sheet: its range, its header, no totals row. .xlsx and .xlsb only (.xls and .ods workbooks have none).</param>
    /// <returns>A new DataFrame containing the sheet's data.</returns>
    /// <exception cref="FileNotFoundException">Thrown if the file does not exist.</exception>
    public static DataFrame ReadExcel(
        string path,
        string? sheetName = null,
        ulong sheetIndex = 0,
        IntoSchema? schema = null,
        bool hasHeader = true,
        ulong inferSchemaLen = 100,
        bool dropEmptyRows = true,
        bool raiseIfEmpty = true,
        string[]? columns = null,
        bool dropEmptyColumns = true,
        string? tableName = null)
    {
        var schemaHandle = schema?.Consume().Handle;
        return new DataFrame(PolarsWrapper.ReadExcel(path, sheetName, sheetIndex, schemaHandle, hasHeader, inferSchemaLen,
            dropEmptyRows, raiseIfEmpty, columns, dropEmptyColumns, tableName));
    }

    /// <summary>
    /// Read a sheet of a workbook in a stream into a DataFrame (the stream is left open; one that cannot seek
    /// is buffered in native memory first). See <see cref="ReadExcel(string, string, ulong, IntoSchema?, bool, ulong, bool, bool, string[], bool, string)"/>.
    /// </summary>
    public static DataFrame ReadExcel(
        Stream stream,
        string? sheetName = null,
        ulong sheetIndex = 0,
        IntoSchema? schema = null,
        bool hasHeader = true,
        ulong inferSchemaLen = 100,
        bool dropEmptyRows = true,
        bool raiseIfEmpty = true,
        string[]? columns = null,
        bool dropEmptyColumns = true,
        string? tableName = null)
    {
        var schemaHandle = schema?.Consume().Handle;
        return new DataFrame(PolarsWrapper.ReadExcel(stream, sheetName, sheetIndex, schemaHandle, hasHeader, inferSchemaLen,
            dropEmptyRows, raiseIfEmpty, columns, dropEmptyColumns, tableName));
    }
}

public partial class LazyFrame
{
    /// <summary>
    /// Lazily scan a sheet of a workbook. The header and the first <paramref name="inferSchemaLen"/> rows are
    /// read now for the schema; the sheet is streamed batch by batch each time the query runs. The parameters are those
    /// of <see cref="DataFrame.ReadExcel(string, string, ulong, IntoSchema?, bool, ulong, bool, bool, string[], bool, string)"/>.
    /// </summary>
    public static LazyFrame ScanExcel(
        string path,
        string? sheetName = null,
        ulong sheetIndex = 0,
        IntoSchema? schema = null,
        bool hasHeader = true,
        ulong inferSchemaLen = 100,
        bool dropEmptyRows = true,
        bool raiseIfEmpty = true,
        string[]? columns = null,
        bool dropEmptyColumns = true,
        string? tableName = null)
    {
        var schemaHandle = schema?.Consume().Handle;
        return new(PolarsWrapper.ScanExcel(path, sheetName, sheetIndex, schemaHandle, hasHeader, inferSchemaLen,
            dropEmptyRows, raiseIfEmpty, columns, dropEmptyColumns, tableName));
    }
}
