using Polars.NET.Core;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Read a DataFrame from a CSV file.
    /// <para>
    /// Note: This method internally uses LazyFrame.ScanCsv and collects the result. 
    /// For larger-than-memory datasets or better query optimization, consider using LazyFrame.ScanCsv" directly.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the CSV file.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <param name="hasHeader">Whether the CSV file has a header. Defaults to true.</param>
    /// <param name="separator">Character used as separator. Defaults to ','.</param>
    /// <param name="quoteChar">Character used for quoting. Defaults to '"'. Set to '\0' to disable.</param>
    /// <param name="eolChar">Character used as End-Of-Line. Defaults to '\n'.</param>
    /// <param name="ignoreErrors">Try to keep reading lines if some are invalid. Defaults to false.</param>
    /// <param name="tryParseDates">Try to automatically parse dates. Defaults to true.</param>
    /// <param name="lowMemory">Use valid JSON lines to reduce memory usage. Defaults to false.</param>
    /// <param name="rechunk">Rechunk the memory to contiguous chunks after reading. Defaults to false.</param>
    /// <param name="skipRows">Number of rows to skip from the start. Defaults to 0.</param>
    /// <param name="skipRowsAfterHeader">Skip this number of rows after the header location. Defaults to 0.</param>
    /// <param name="nRows">Stop reading after n rows. If null, read all.</param>
    /// <param name="inferSchemaLength">Number of rows to scan for schema inference. If null, use Polars default (100).</param>
    /// <param name="inferSchemaFiles">How many files to use when inferring schema.</param>
    /// <param name="schema">Provide a schema to ignore schema inference.</param>
    /// <param name="dtypeOverride">Optional PolarsSchema to specify column types or overwrite inference.</param>
    /// <param name="encoding">Encoding of the CSV file. Defaults to Utf8.</param>
    /// <param name="nullValues">List of strings to consider as null values.</param>
    /// <param name="missingIsNull">Treat missing fields as null. Defaults to true.</param>
    /// <param name="commentPrefix">Lines starting with this prefix will be ignored.</param>
    /// <param name="decimalComma">Use comma as decimal separator. Defaults to false.</param>
    /// <param name="truncateRaggedLines">Truncate lines that are longer than the schema. Defaults to false.</param>
    /// <param name="rowIndexName">If provided, add a column with the row index.</param>
    /// <param name="rowIndexOffset">Offset for the row index. Defaults to 0.</param>
    /// <param name="cloudOptions">Options for cloud storage authentication and configuration.</param>
    public static DataFrame ReadCsv(
        string path,
        string[]? columns = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool rechunk = false,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = null,
        ulong? inferSchemaFiles = 18446744073709551615,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        CsvEncoding encoding = CsvEncoding.UTF8,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        CloudOptions? cloudOptions = null)
    {
        var lf = LazyFrame.ScanCsv(
            path: path,
            schema: schema,
            dtypeOverride:dtypeOverride,
            hasHeader: hasHeader,
            separator: separator,
            quoteChar: quoteChar,
            eolChar: eolChar,
            ignoreErrors: ignoreErrors,
            tryParseDates: tryParseDates,
            lowMemory: lowMemory,
            rechunk:rechunk,
            skipRows: skipRows,
            skipRowsAfterHeader: skipRowsAfterHeader,
            nRows: nRows,
            inferSchemaLength: inferSchemaLength,
            inferSchemaFiles:inferSchemaFiles,
            rowIndexName: rowIndexName,
            rowIndexOffset: rowIndexOffset,
            encoding: encoding,
            nullValues: nullValues,
            missingIsNull: missingIsNull,
            commentPrefix: commentPrefix,
            decimalComma: decimalComma,
            truncateRaggedLines: truncateRaggedLines,
            cloudOptions: cloudOptions
        );

        if (columns != null && columns.Length > 0)
        {
            lf = lf.Select(Cs.ByName(columns));
        }

        return lf.Collect();
    }
    /// <summary>
    /// Read a DataFrame from a CSV memory buffer.
    /// </summary>
    /// <param name="buffer">Memory buffer with the CSV file.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <param name="hasHeader">Whether the CSV file has a header. Defaults to true.</param>
    /// <param name="separator">Character used as separator. Defaults to ','.</param>
    /// <param name="quoteChar">Character used for quoting. Defaults to '"'. Set to '\0' to disable.</param>
    /// <param name="eolChar">Character used as End-Of-Line. Defaults to '\n'.</param>
    /// <param name="ignoreErrors">Try to keep reading lines if some are invalid. Defaults to false.</param>
    /// <param name="tryParseDates">Try to automatically parse dates. Defaults to true.</param>
    /// <param name="lowMemory">Use valid JSON lines to reduce memory usage. Defaults to false.</param>
    /// <param name="rechunk">Rechunk the memory to contiguous chunks after reading. Defaults to false.</param>
    /// <param name="skipRows">Number of rows to skip from the start. Defaults to 0.</param>
    /// <param name="skipRowsAfterHeader">Skip this number of rows after the header location. Defaults to 0.</param>
    /// <param name="nRows">Stop reading after n rows. If null, read all.</param>
    /// <param name="inferSchemaLength">Number of rows to scan for schema inference. If null, scan all file.</param>
    /// <param name="inferSchemaFiles">How many files to use when inferring schema.</param>
    /// <param name="schema">Optional PolarsSchema to specify all column names and types.</param>
    /// <param name="dtypeOverride">Optional PolarsSchema to specify column types or overwrite inference.</param>
    /// <param name="encoding">Encoding of the CSV file. Defaults to Utf8.</param>
    /// <param name="nullValues">List of strings to consider as null values.</param>
    /// <param name="missingIsNull">Treat missing fields as null. Defaults to true.</param>
    /// <param name="commentPrefix">Lines starting with this prefix will be ignored.</param>
    /// <param name="decimalComma">Use comma as decimal separator. Defaults to false.</param>
    /// <param name="truncateRaggedLines">Truncate lines that are longer than the schema. Defaults to false.</param>
    /// <param name="rowIndexName">If provided, add a column with the row index.</param>
    /// <param name="rowIndexOffset">Offset for the row index. Defaults to 0.</param>
    public static DataFrame ReadCsv(
        byte[] buffer,
        string[]? columns = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool rechunk = false,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = null,
        ulong? inferSchemaFiles = 18446744073709551615,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        CsvEncoding encoding = CsvEncoding.UTF8,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0
    )
    {
        var lf = LazyFrame.ScanCsv(
            buffer,
            schema: schema,
            dtypeOverride:dtypeOverride,
            hasHeader: hasHeader,
            separator: separator,
            quoteChar: quoteChar,
            eolChar: eolChar,
            ignoreErrors: ignoreErrors,
            tryParseDates: tryParseDates,
            lowMemory: lowMemory,
            rechunk:rechunk,
            skipRows: skipRows,
            skipRowsAfterHeader:skipRowsAfterHeader,
            nRows: nRows,
            inferSchemaLength: inferSchemaLength,
            inferSchemaFiles:inferSchemaFiles,
            rowIndexName: rowIndexName,
            rowIndexOffset: rowIndexOffset,
            encoding: encoding,
            nullValues: nullValues,
            missingIsNull: missingIsNull,
            commentPrefix: commentPrefix,
            decimalComma: decimalComma,
            truncateRaggedLines: truncateRaggedLines
        );

        if (columns != null && columns.Length > 0)
        {
            lf = lf.Select(Cs.ByName(columns));
        }

        return lf.Collect();
    }
    /// <summary>
    /// Read a DataFrame from a CSV memory stream.
    /// </summary>
    /// <param name="stream">Memory stream with the CSV file.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <param name="hasHeader">Whether the CSV file has a header. Defaults to true.</param>
    /// <param name="separator">Character used as separator. Defaults to ','.</param>
    /// <param name="quoteChar">Character used for quoting. Defaults to '"'. Set to '\0' to disable.</param>
    /// <param name="eolChar">Character used as End-Of-Line. Defaults to '\n'.</param>
    /// <param name="ignoreErrors">Try to keep reading lines if some are invalid. Defaults to false.</param>
    /// <param name="tryParseDates">Try to automatically parse dates. Defaults to true.</param>
    /// <param name="lowMemory">Use valid JSON lines to reduce memory usage. Defaults to false.</param>
    /// <param name="rechunk">Rechunk the memory to contiguous chunks after reading. Defaults to false.</param>
    /// <param name="skipRows">Number of rows to skip from the start. Defaults to 0.</param>
    /// <param name="skipRowsAfterHeader">Skip this number of rows after the header location. Defaults to 0.</param>
    /// <param name="nRows">Stop reading after n rows. If null, read all.</param>
    /// <param name="inferSchemaLength">Number of rows to scan for schema inference.</param>
    /// <param name="inferSchemaFiles">How many files to use when inferring schema.</param>
    /// <param name="schema">Optional PolarsSchema to specify all column names and types.</param>
    /// <param name="dtypeOverride">Optional PolarsSchema to specify column types or overwrite inference.</param>
    /// <param name="encoding">Encoding of the CSV file. Defaults to Utf8.</param>
    /// <param name="nullValues">List of strings to consider as null values.</param>
    /// <param name="missingIsNull">Treat missing fields as null. Defaults to true.</param>
    /// <param name="commentPrefix">Lines starting with this prefix will be ignored.</param>
    /// <param name="decimalComma">Use comma as decimal separator. Defaults to false.</param>
    /// <param name="truncateRaggedLines">Truncate lines that are longer than the schema. Defaults to false.</param>
    /// <param name="rowIndexName">If provided, add a column with the row index.</param>
    /// <param name="rowIndexOffset">Offset for the row index. Defaults to 0.</param>
    public static DataFrame ReadCsv(
        Stream stream,
        string[]? columns = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool rechunk = false,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = null,
        ulong? inferSchemaFiles = null,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null, 
        CsvEncoding encoding = CsvEncoding.UTF8,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0
    )
    {
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        var lf = LazyFrame.ScanCsv(
            ms.ToArray(),
            schema: schema,
            dtypeOverride:dtypeOverride,
            hasHeader: hasHeader,
            separator: separator,
            quoteChar: quoteChar,
            eolChar: eolChar,
            ignoreErrors: ignoreErrors,
            rechunk:rechunk,
            tryParseDates: tryParseDates,
            lowMemory: lowMemory,
            skipRows: skipRows,
            skipRowsAfterHeader: skipRowsAfterHeader,
            nRows: nRows,
            inferSchemaLength: inferSchemaLength,
            inferSchemaFiles: inferSchemaFiles,
            rowIndexName: rowIndexName,
            rowIndexOffset: rowIndexOffset,
            encoding: encoding,
            nullValues: nullValues,
            missingIsNull: missingIsNull,
            commentPrefix: commentPrefix,
            decimalComma: decimalComma,
            truncateRaggedLines: truncateRaggedLines
        );

        if (columns != null && columns.Length > 0)
        {
            lf = lf.Select(Cs.ByName(columns));
        }

        return lf.Collect();
    }
    /// <summary>
    /// Asynchronously read a DataFrame from a CSV file.
    /// </summary>
    public static async Task<DataFrame> ReadCsvAsync(
        string path,
        string[]? columns = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',
        char eolChar = '\n',
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool rechunk = false,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = null,
        ulong? inferSchemaFiles = 18446744073709551615,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        CsvEncoding encoding = CsvEncoding.UTF8,
        string[]? nullValues = null,
        bool missingIsNull = true,
        string? commentPrefix = null,
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        CloudOptions? cloudOptions = null)
    {
        var lf = LazyFrame.ScanCsv(
            path: path,
            schema: schema,
            dtypeOverride:dtypeOverride,
            hasHeader: hasHeader,
            separator: separator,
            quoteChar: quoteChar,
            eolChar: eolChar,
            ignoreErrors: ignoreErrors,
            tryParseDates: tryParseDates,
            lowMemory: lowMemory,
            rechunk:rechunk,
            skipRows: skipRows,
            skipRowsAfterHeader: skipRowsAfterHeader,
            nRows: nRows,
            inferSchemaLength: inferSchemaLength,
            inferSchemaFiles:inferSchemaFiles,
            rowIndexName: rowIndexName,
            rowIndexOffset: rowIndexOffset,
            encoding: encoding,
            nullValues: nullValues,
            missingIsNull: missingIsNull,
            commentPrefix: commentPrefix,
            decimalComma: decimalComma,
            truncateRaggedLines: truncateRaggedLines,
            cloudOptions: cloudOptions
        );

        if (columns != null && columns.Length > 0)
        {
            var colsToSelect = new List<string>(columns);
            
            if (!string.IsNullOrEmpty(rowIndexName) && !colsToSelect.Contains(rowIndexName))
            {
                colsToSelect.Add(rowIndexName);
            }

            lf = lf.Select(Cs.ByName([.. colsToSelect]));
        }

        return await lf.CollectAsync(useStreaming: true);
    }
}