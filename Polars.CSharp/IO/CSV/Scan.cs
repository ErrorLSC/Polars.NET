using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Represents a lazily evaluated DataFrame.
/// Until the query is executed, operations are just recorded in a query plan.
/// Once executed, the data is materialized in memory.
/// </summary>
public partial class LazyFrame : IDisposable,IPolarsLazyFrame
{
    /// <summary>
    /// Lazily scans a CSV file into a LazyFrame.
    /// <para>
    /// This allows for query optimization (predicate pushdown, projection pushdown) 
    /// and streaming processing of datasets larger than memory.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the CSV file.</param>
    /// <param name="schema">Optional PolarsSchema to specify all column names and types.</param>
    /// <param name="dtypeOverride">Optional PolarsSchema to specify column types or overwrite inference.</param>
    /// <param name="hasHeader">Whether the CSV file has a header row. Defaults to true.</param>
    /// <param name="separator">The character used as a field separator. Defaults to ','.</param>
    /// <param name="quoteChar">The character used for quoting fields. Defaults to '"'. Set to '\0' to disable quoting.</param>
    /// <param name="eolChar">The character used as End-Of-Line. Defaults to '\n'.</param>
    /// <param name="ignoreErrors">Whether to ignore parsing errors (skip bad rows). Defaults to false.</param>
    /// <param name="tryParseDates">Whether to automatically try parsing dates/datetimes. Defaults to true.</param>
    /// <param name="lowMemory">Reduce memory usage at the cost of performance. Defaults to false.</param>
    /// <param name="cache">Cache the result after reading. Defaults to true.</param>
    /// <param name="glob">Expand path given via globbing rules. Defaults to true.</param>
    /// <param name="rechunk">Rechunk the memory to contiguous chunks after reading. Defaults to false.</param>
    /// <param name="raiseIfEmpty">Raise an error if CSV is empty (otherwise return an empty frame). Defaults to true.</param>
    /// <param name="skipRows">Number of rows to skip at the start of the file. Defaults to 0.</param>
    /// <param name="skipRowsAfterHeader">Skip this number of rows after the header location. Defaults to 0.</param>
    /// <param name="skipLines">Skip the first n lines during parsing without respecting CSV escaping. Defaults to 0.</param>
    /// <param name="nRows">Stop reading after n rows. If null, reads the entire file.</param>
    /// <param name="inferSchemaLength">Number of rows to scan for schema inference. Defaults to 100. Set to 0 to disable inference.</param>
    /// <param name="nThreads">Sets the number of threads used for CSV parsing. Default is null for auto setting.</param>
    /// <param name="chunkSize">Set the chunk size for each thread. Default is null for auto setting.</param>
    /// <param name="rowIndexName">If provided, adds a column with the row index using this name.</param>
    /// <param name="rowIndexOffset">Offset to start the row index from. Defaults to 0.</param>
    /// <param name="includeFilePaths">If provided, adds a column with the file path using this name.</param>
    /// <param name="encoding">File encoding (UTF8 or LossyUTF8). Defaults to UTF8.</param>
    /// <param name="nullValues">List of strings to consider as null values. E.g., ["NA", "null"].</param>
    /// <param name="missingIsNull">Treat missing fields (empty strings between delimiters) as null. Defaults to true.</param>
    /// <param name="commentPrefix">Lines starting with this prefix will be ignored. E.g., "#".</param>
    /// <param name="decimalComma">Use comma ',' as the decimal separator (European style). Defaults to false.</param>
    /// <param name="truncateRaggedLines">Truncate lines that are longer than the schema. Defaults to false.</param>
    /// <param name="cloudOptions">Options for cloud storage authentication and configuration.</param>
    /// <returns>A new LazyFrame.</returns>
    public static LazyFrame ScanCsv(
        string path,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',           
        char eolChar = '\n',            
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool cache = true,
        bool glob = true,
        bool rechunk = false,
        bool raiseIfEmpty = true,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong skipLines = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        ulong? nThreads = null,
        ulong? chunkSize = null,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        string? includeFilePaths = null,
        CsvEncoding encoding = CsvEncoding.UTF8,
        string[]? nullValues = null,    
        bool missingIsNull = true,      
        string? commentPrefix = null,   
        bool decimalComma = false,
        bool truncateRaggedLines = false,
        CloudOptions? cloudOptions = null)      
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = 
            CloudOptions.ParseCloudOptions(cloudOptions);

        var handle = PolarsWrapper.ScanCsv(
            path,
            schema?.Consume().Handle,
            dtypeOverride?.Consume().Handle,
            hasHeader,
            separator,
            quoteChar,
            eolChar,
            ignoreErrors,
            tryParseDates,
            lowMemory,
            cache,
            glob,
            rechunk,
            raiseIfEmpty,
            skipRows,
            skipRowsAfterHeader,
            skipLines,
            nRows,
            inferSchemaLength,
            nThreads,
            chunkSize,
            rowIndexName,
            rowIndexOffset,
            includeFilePaths,
            encoding.ToNative(),
            nullValues,
            missingIsNull,
            commentPrefix,
            decimalComma,
            truncateRaggedLines,
            provider.ToNative(),
            (nuint)retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        );

        return new LazyFrame(handle);
    }

    /// <summary>
    /// Lazily scans a CSV from an in-memory byte array.
    /// <para>
    /// Useful for processing data from Web APIs, S3, or embedded resources without writing to disk.
    /// </para>
    /// </summary>
    /// <param name="buffer">The byte array containing CSV data.</param>
    /// <param name="schema">Optional PolarsSchema to specify all column names and types.</param>
    /// <param name="dtypeOverride">Optional PolarsSchema to specify column types or overwrite inference.</param>
    /// <param name="hasHeader">Whether the CSV data has a header row. Defaults to true.</param>
    /// <param name="separator">The character used as a field separator. Defaults to ','.</param>
    /// <param name="quoteChar">The character used for quoting fields. Defaults to '"'. Set to '\0' to disable.</param>
    /// <param name="eolChar">The character used as End-Of-Line. Defaults to '\n'.</param>
    /// <param name="ignoreErrors">Whether to ignore parsing errors. Defaults to false.</param>
    /// <param name="tryParseDates">Whether to automatically try parsing dates. Defaults to true.</param>
    /// <param name="lowMemory">Reduce memory usage at the cost of performance. Defaults to false.</param>
    /// <param name="cache">Cache the result after reading. Defaults to true.</param>
    /// <param name="glob">Expand path given via globbing rules. Defaults to true.</param>
    /// <param name="rechunk">Rechunk the memory to contiguous chunks after reading. Defaults to false.</param>
    /// <param name="raiseIfEmpty">Raise an error if CSV is empty. Defaults to true.</param>
    /// <param name="skipRows">Number of rows to skip at the start. Defaults to 0.</param>
    /// <param name="skipRowsAfterHeader">Skip this number of rows after the header location. Defaults to 0.</param>
    /// <param name="skipLines">Skip the first n lines during parsing without respecting CSV escaping. Defaults to 0.</param>
    /// <param name="nRows">Stop reading after n rows. If null, reads all data.</param>
    /// <param name="inferSchemaLength">Number of rows to scan for schema inference. Defaults to 100.</param>
    /// <param name="nThreads">Sets the number of threads used for CSV parsing. Default is null for auto setting.</param>
    /// <param name="chunkSize">Set the chunk size for each thread. Default is null for auto setting.</param>
    /// <param name="rowIndexName">If provided, adds a column with the row index using this name.</param>
    /// <param name="rowIndexOffset">Offset to start the row index from. Defaults to 0.</param>
    /// <param name="includeFilePaths">If provided, adds a column with the file path using this name.</param>
    /// <param name="encoding">Data encoding (UTF8 or LossyUTF8). Defaults to UTF8.</param>
    /// <param name="nullValues">List of strings to consider as null values.</param>
    /// <param name="missingIsNull">Treat missing fields as null. Defaults to true.</param>
    /// <param name="commentPrefix">Lines starting with this prefix will be ignored.</param>
    /// <param name="decimalComma">Use comma ',' as the decimal separator. Defaults to false.</param>
    /// <param name="truncateRaggedLines">Truncate lines that are longer than the schema. Defaults to false.</param>
    /// <returns>A new LazyFrame.</returns>
    public static LazyFrame ScanCsv(
        byte[] buffer,
        IntoSchema? schema = null,
        IntoSchema? dtypeOverride = null,
        bool hasHeader = true,
        char separator = ',',
        char? quoteChar = '"',          
        char eolChar = '\n',           
        bool ignoreErrors = false,
        bool tryParseDates = true,
        bool lowMemory = false,
        bool cache = true,
        bool glob = true,
        bool rechunk = false,
        bool raiseIfEmpty = true,
        ulong skipRows = 0,
        ulong skipRowsAfterHeader = 0,
        ulong skipLines = 0,
        ulong? nRows = null,
        ulong? inferSchemaLength = 100,
        ulong? nThreads = null,
        ulong? chunkSize = null,
        string? rowIndexName = null,
        ulong rowIndexOffset = 0,
        string? includeFilePaths = null,
        CsvEncoding encoding = CsvEncoding.UTF8,
        string[]? nullValues = null,   
        bool missingIsNull = true,     
        string? commentPrefix = null,  
        bool decimalComma = false,
        bool truncateRaggedLines = false)     
    {
        var handle = PolarsWrapper.ScanCsv(
            buffer,
            schema?.Consume().Handle,
            dtypeOverride?.Consume().Handle,
            hasHeader,
            separator,
            quoteChar,
            eolChar,
            ignoreErrors,
            tryParseDates,
            lowMemory,
            cache,
            glob,
            rechunk,
            raiseIfEmpty,
            skipRows,
            skipRowsAfterHeader,
            skipLines,
            nRows,
            inferSchemaLength,
            nThreads,
            chunkSize,
            rowIndexName,
            rowIndexOffset,
            includeFilePaths,
            encoding.ToNative(),
            nullValues,
            missingIsNull,
            commentPrefix,
            decimalComma,
            truncateRaggedLines
        );

        return new LazyFrame(handle);
    }
}