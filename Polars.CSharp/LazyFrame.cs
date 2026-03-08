#pragma warning disable CS1573

using System.Collections.Concurrent;
using System.Data;
using Apache.Arrow;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Data;
using Polars.NET.Core.Helpers;

namespace Polars.CSharp;

/// <summary>
/// Represents a lazily evaluated DataFrame.
/// Until the query is executed, operations are just recorded in a query plan.
/// Once executed, the data is materialized in memory.
/// </summary>
public class LazyFrame : IDisposable,IPolarsLazyFrame
{
    internal LazyFrameHandle Handle { get; }

    internal LazyFrame(LazyFrameHandle handle)
    {
        Handle = handle;
    }

    // ==========================================
    // Scan IO
    // ==========================================
    /// <summary>
    /// Lazily scans a CSV file into a LazyFrame.
    /// <para>
    /// This allows for query optimization (predicate pushdown, projection pushdown) 
    /// and streaming processing of datasets larger than memory.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the CSV file.</param>
    /// <param name="schema">Optional PolarsSchema to specify column types or overwrite inference.</param>
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
        PolarsSchema? schema = null,
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
            schema?.Handle,
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
    /// <param name="schema">Optional PolarsSchema to specify column types or overwrite inference.</param>
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
        PolarsSchema? schema = null,
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
            schema?.Handle,
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
    /// <summary>
    /// Lazily read from a parquet file or multiple files via glob patterns.
    /// </summary>
    /// <param name="path">Path to file or glob pattern (e.g. "data/*.parquet" or "s3://bucket/data.parquet").</param>
    /// <param name="nRows">Limit number of rows to read (optimization).</param>
    /// <param name="parallel">Parallel strategy.</param>
    /// <param name="lowMemory">Reduce memory usage at the expense of performance.</param>
    /// <param name="useStatistics">Use parquet statistics to optimize the query plan.</param>
    /// <param name="glob">Expand glob patterns (default: true).</param>
    /// <param name="allowMissingColumns">Allow missing columns when reading multiple files.</param>
    /// <param name="rechunk">Rechunk the memory to contiguous chunks when reading. (default: false)</param>
    /// <param name="cache">Cache the result after reading. (default: true)</param>
    /// <param name="rowIndexName">If provided, adds a column with the row number.</param>
    /// <param name="rowIndexOffset">Offset for the row index.</param>
    /// <param name="includePathColumn">If provided, adds a column with the source file path.</param>
    /// <param name="schema">
    /// Manually specify the schema of the file(s). 
    /// Useful if the file footer is missing or to avoid I/O overhead of reading the schema.</param>
    /// <param name="hivePartitioning">Enable Hive partitioning inference (default: false).</param>
    /// <param name="hivePartitionSchema">
    /// Manually specify the schema for Hive partitioning columns.
    /// Use this to ensure specific types for partition keys (e.g. string instead of int).
    /// </param>
    /// <param name="tryParseHiveDates">
    /// Whether to try parsing dates in Hive partitioning paths (default: false).
    /// </param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public static LazyFrame ScanParquet(
        string path,
        ulong? nRows = null,
        ParallelStrategy parallel = ParallelStrategy.Auto,
        bool lowMemory = false,
        bool useStatistics = true,
        bool glob = true,
        bool allowMissingColumns = false,
        bool rechunk = false, 
        bool cache = true,    
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        PolarsSchema? schema = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false,
        CloudOptions? cloudOptions = null) 
    {
        var schemaHandle = schema?.Handle;
        var hiveSchemaHandle = hivePartitionSchema?.Handle;

        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        var h = PolarsWrapper.ScanParquet(
            path,
            nRows,
            parallel.ToNative(),
            lowMemory,
            useStatistics,
            glob,
            allowMissingColumns,
            rechunk, 
            cache,   
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schemaHandle,     
            hivePartitioning,
            hiveSchemaHandle, 
            tryParseHiveDates,
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
            );

        return new LazyFrame(h);
    }
    /// <summary>
    /// Lazily read parquet from an in-memory byte array.
    /// </summary>
    public static LazyFrame ScanParquet(
        byte[] buffer,
        ulong? nRows = null,
        ParallelStrategy parallel = ParallelStrategy.Auto,
        bool lowMemory = false,
        bool useStatistics = true,
        bool allowMissingColumns = false,
        bool rechunk = false, // New
        bool cache = true,    // New
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        PolarsSchema? schema = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false)
    {
        var schemaHandle = schema?.Handle;
        var hiveSchemaHandle = hivePartitionSchema?.Handle;

        var h = PolarsWrapper.ScanParquet(
            buffer,
            nRows,
            parallel.ToNative(),
            lowMemory,
            useStatistics,
            false, // glob = false for memory
            allowMissingColumns,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schemaHandle,
            hivePartitioning,
            hiveSchemaHandle,
            tryParseHiveDates
        );

        return new LazyFrame(h);
    }
    // ---------------------------------------------------------
    // Scan IPC (File / Cloud)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read an Arrow IPC (Feather v2) file, multiple files via glob patterns, or cloud storage.
    /// </summary>
    /// <param name="path">Path to the IPC file, glob pattern, or cloud path (e.g., "s3://...").</param>
    /// <param name="schema">
    /// Optional schema to enforce. If not provided, the schema is inferred from the file footer.
    /// </param>
    /// <param name="nRows">
    /// Limit the number of rows to scan. 
    /// Note: In Lazy mode, this acts as a 'Pre-Slice' pushdown.
    /// </param>
    /// <param name="rechunk">Rechunk the memory to be contiguous (default: false).</param>
    /// <param name="cache">Cache the result of the scan (default: true).</param>
    /// <param name="glob">Expand glob patterns (default: true).</param>
    /// <param name="rowIndexName">If provided, adds a column with the row index.</param>
    /// <param name="rowIndexOffset">Offset for the row index (default: 0).</param>
    /// <param name="includePathColumn">If provided, adds a column with the source file path.</param>
    /// <param name="hivePartitioning">Enable Hive partitioning inference (default: false).</param>
    /// <param name="hivePartitionSchema">Manually specify the schema for Hive partitioning columns.</param>
    /// <param name="tryParseHiveDates">Whether to try parsing dates in Hive partitioning paths (default: true).</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public static LazyFrame ScanIpc(
        string path,
        PolarsSchema? schema = null,
        ulong? nRows = null,
        bool rechunk = false,
        bool cache = true,
        bool glob = true,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = true,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = 
            CloudOptions.ParseCloudOptions(cloudOptions);

        var h = PolarsWrapper.ScanIpc(
            path,
            nRows,
            rechunk,
            cache,
            glob,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schema?.Handle,
            hivePartitioning,
            hivePartitionSchema?.Handle,
            tryParseHiveDates,
            provider.ToNative(),
            (nuint)retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        );

        return new LazyFrame(h);
    }

    // ---------------------------------------------------------
    // Scan IPC (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read Arrow IPC (Feather v2) from in-memory bytes.
    /// </summary>
    public static LazyFrame ScanIpc(
        byte[] buffer,
        PolarsSchema? schema = null,
        ulong? nRows = null,
        bool rechunk = false,
        bool cache = true,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false)
    {
        var h = PolarsWrapper.ScanIpc(
            buffer,
            nRows,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schema?.Handle,
            hivePartitioning,
            hivePartitionSchema?.Handle,
            tryParseHiveDates
        );

        return new LazyFrame(h);
    }

    // ---------------------------------------------------------
    // Scan IPC (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read Arrow IPC (Feather v2) from a Stream.
    /// </summary>
    /// <remarks>
    /// This reads the stream fully into memory to construct the Lazy execution plan.
    /// </remarks>
    public static LazyFrame ScanIpc(
        Stream stream,
        PolarsSchema? schema = null,
        ulong? nRows = null,
        bool rechunk = false,
        bool cache = true,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false)
    {
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        
        return ScanIpc(
            ms.ToArray(),
            schema,
            nRows,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            hivePartitioning,
            hivePartitionSchema,
            tryParseHiveDates
        );
    }
    // ---------------------------------------------------------
    // Scan NDJSON (File)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read a newline delimited JSON file (NDJSON).
    /// </summary>
    /// <param name="path">Path to the NDJSON file.</param>
    /// <param name="schema">
    /// Manually specify schema for specific columns (Overwrite semantics).
    /// Columns not specified will be inferred.
    /// </param>
    /// <param name="inferSchemaLength">
    /// Number of rows to scan for schema inference. 
    /// If null, uses Polars default (usually 100).
    /// </param>
    /// <param name="batchSize">Batch size for reading (optimization).</param>
    /// <param name="nRows">Limit the number of rows to read.</param>
    /// <param name="lowMemory">Reduce memory usage at the expense of performance.</param>
    /// <param name="rechunk">Rechunk the output to have contiguous memory (default: false).</param>
    /// <param name="ignoreErrors">Ignore parsing errors (skip malformed lines).</param>
    /// <param name="rowIndexName">If provided, adds a column with the row index.</param>
    /// <param name="rowIndexOffset">Offset for the row index (default: 0).</param>
    /// <param name="includePathColumn">If provided, adds a column with the source file path.</param>
   /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public static LazyFrame ScanNdjson(
        string path,
        PolarsSchema? schema = null,
        ulong? inferSchemaLength = null,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        CloudOptions? cloudOptions = null)
    {
        if (!File.Exists(path)) 
            throw new FileNotFoundException($"NDJSON file not found: {path}");
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = 
            CloudOptions.ParseCloudOptions(cloudOptions);
        var schemaHandle = schema?.Handle;

        var h = PolarsWrapper.ScanNdjson(
            path,
            schemaHandle,
            batchSize,
            inferSchemaLength,
            nRows,
            lowMemory,
            rechunk,
            ignoreErrors,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        );

        return new LazyFrame(h);
    }

    // ---------------------------------------------------------
    // Scan NDJSON (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read NDJSON from an in-memory byte array.
    /// </summary>
    public static LazyFrame ScanNdjson(
        byte[] buffer,
        PolarsSchema? schema = null,
        ulong? inferSchemaLength = null,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0)
    {
        var schemaHandle = schema?.Handle;

        var h = PolarsWrapper.ScanNdjson(
            buffer,
            schemaHandle,
            batchSize,
            inferSchemaLength,
            nRows,
            lowMemory,
            rechunk,
            ignoreErrors,
            rowIndexName,
            rowIndexOffset,
            null // includePathColumn
        );

        return new LazyFrame(h);
    }

    // ---------------------------------------------------------
    // Scan NDJSON (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Lazily read NDJSON from a Stream.
    /// </summary>
    public static LazyFrame ScanNdjson(
        Stream stream,
        PolarsSchema? schema = null,
        ulong? inferSchemaLength = null,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0)
    {
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        
        return ScanNdjson(
            ms.ToArray(),
            schema,
            inferSchemaLength,
            batchSize,
            nRows,
            lowMemory,
            rechunk,
            ignoreErrors,
            rowIndexName,
            rowIndexOffset
        );
    }

    private static IEnumerable<RecordBatch> EnsureStreamSafety(IEnumerable<RecordBatch> source)
    {
        using var enumerator = source.GetEnumerator();

        while (enumerator.MoveNext())
        {
            var batch = enumerator.Current;
            yield return batch;
            batch.Dispose();
        }
    }

    /// <summary>
    /// Scan Enumberable As LazyFrame
    /// </summary>
    /// <returns></returns>
    public static LazyFrame ScanEnumerable<T>(
        IEnumerable<T> data, 
        Schema? schema = null, 
        int batchSize = 100_000,
        bool useBuffered = false)
    {
        // 1. Get Schema (Cached)
        schema ??= ArrowConverter.GetSchemaFromType<T>();

        // 2. Buffered Mode
        if (useBuffered)
        {
            var scope = new IpcStreamService.TempIpcScope<T>(data, batchSize); 
            var handleBuffered = ScanIpc(scope.FilePath!).Handle;
            return new ScopedLazyFrame(handleBuffered, scope);
        }

        // 3. Streaming Mode (Memory Pointer)
        IEnumerable<RecordBatch> SafeGenerator()
        {
            bool hasYielded = false;

            foreach (var batch in data.ToArrowBatches(batchSize).Prefetch())
            {
                hasYielded = true;
                yield return batch;
            }

            if (!hasYielded)
            {
                yield return ArrowConverter.GetEmptyBatch<T>();
            }
        }

        var handle = ArrowStreamInterop.ScanStream(
            () => EnsureStreamSafety(SafeGenerator()), 
            schema
        );
        
        return new LazyFrame(handle);
    }

    /// <summary>
    /// Scan RecordBatch Stream
    /// If schema is provied, first batch won't be consumed for getting schema.
    /// </summary>
    public static LazyFrame ScanRecordBatches(IEnumerable<RecordBatch> stream, Schema schema)
    {
        if (schema == null) throw new ArgumentNullException(nameof(schema));

        var handle = ArrowStreamInterop.ScanStream(
            () => EnsureStreamSafety(stream),
            schema
        );
        return new LazyFrame(handle);
    }
    /// <summary>
    /// Scan Database to LazyFrame
    /// </summary>
    /// <param name="reader"></param>
    /// <param name="batchSize"></param>
    /// <returns></returns>
    public static LazyFrame ScanDatabase(IDataReader reader, int batchSize = 50_000)
    {
        var schema = reader.GetArrowSchema();
        
        var stream = reader.ToArrowBatches(batchSize).Prefetch();

        return ScanRecordBatches(stream, schema);
    }
    /// <summary>
    /// Create a LazyFrame from a database query using a reader factory.
    /// <para>
    /// <b>Recommended:</b> This is the preferred method for interacting with databases in a Lazy context.
    /// </para>
    /// <para>
    /// It accepts a factory function that creates a NEW <see cref="IDataReader"/> on demand.
    /// This allows Polars to:
    /// <list type="bullet">
    /// <item>Inspect the schema upfront (using a probe reader).</item>
    /// <item>Re-execute the query if the execution plan requires multiple passes.</item>
    /// <item>Allow you to call <see cref="Collect"/> multiple times on the same LazyFrame.</item>
    /// </list>
    /// </para>
    /// </summary>
    /// <param name="readerFactory">A function that returns a new, open <see cref="IDataReader"/> instance each time it is called.</param>
    /// <param name="batchSize">The size of the Arrow record batch (rows). Larger values reduce overhead.</param>
    /// <returns>A new LazyFrame linked to the database stream.</returns>
    /// <example>
    /// <code>
    /// // Define a factory that returns a new Reader for the query
    /// Func&lt;IDataReader&gt; factory = () =>
    /// {
    ///     var cmd = connection.CreateCommand();
    ///     cmd.CommandText = """
    ///         SELECT name, score
    ///         FROM User
    ///         WHERE score > $min_score OR score IS NULL
    ///     """;
    ///     cmd.Parameters.AddWithValue("$min_score", 60.0);
    ///     return cmd.ExecuteReader();
    /// };
    /// 
    /// // Scan and apply transformations lazily
    /// var lf = LazyFrame.ScanDatabase(factory);
    /// 
    /// var result = lf
    ///     .WithColumns(
    ///         Col("score").FillNull(0.0).Alias("clean_score")
    ///     )
    ///     .Collect();
    ///     
    /// result.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────────┬───────┬─────────────┐
    /// │ name    ┆ score ┆ clean_score │
    /// │ ---     ┆ ---   ┆ ---         │
    /// │ str     ┆ f64   ┆ f64         │
    /// ╞═════════╪═══════╪═════════════╡
    /// │ Alice   ┆ 99.5  ┆ 99.5        │
    /// │ Bob     ┆ 85.0  ┆ 85.0        │
    /// │ Charlie ┆ null  ┆ 0.0         │
    /// └─────────┴───────┴─────────────┘
    /// */
    /// </code>
    /// </example>
    public static LazyFrame ScanDatabase(Func<IDataReader> readerFactory, int batchSize = 50_000)
    {
        Schema schema;
        // Probe schema
        using (var probe = readerFactory())
        {
            schema = probe.GetArrowSchema();
        }

        // Replayable stream
        IEnumerable<RecordBatch> StreamFactory()
        {
            using var reader = readerFactory();
            // Get stream
            var stream = reader.ToArrowBatches(batchSize);
            
            stream = stream.Prefetch();

            foreach (var batch in stream) 
                yield return batch;
        }

        var handle = ArrowStreamInterop.ScanStream(
            () => EnsureStreamSafety(StreamFactory()),
            schema
        );
        return new LazyFrame(handle);
    }
    /// <summary>
    /// A LazyFrame with resource scope which needs to be disposed.
    /// </summary>
    public class ScopedLazyFrame : LazyFrame
    {
        private readonly IDisposable? _resource;

        internal ScopedLazyFrame(LazyFrameHandle handle, IDisposable? resource) 
            : base(handle) 
        {
            _resource = resource;
        }
        /// <summary>
        /// Dispose temp file and lazyframe
        /// </summary>
        public new void Dispose()
        {
            base.Dispose();
            
            _resource?.Dispose();
        }
    }
    /// <summary>
    /// [Buffered] Create a LazyFrame from an existing DataReader.
    /// <para><b>Note:</b> This consumes the reader IMMEDIATELY and writes to a temp file.</para>
    /// <para>Returns a <see cref="ScopedLazyFrame"/> which must be disposed to delete the temp file.</para>
    /// </summary>
    public static ScopedLazyFrame ScanDatabaseBuffered(IDataReader reader, int batchSize = 50_000)
    {
        // DataReader cannot be reset, so we must buffer it to disk immediately
        var scope = new IpcStreamService.TempIpcScopeReader(reader, batchSize);
        
        var handle = ScanIpc(scope.FilePath!).Handle;
        
        return new ScopedLazyFrame(handle, scope);
    }

    /// -----------------------------------
    /// Delta Lake
    /// -----------------------------------
    /// <summary>
    /// Create a LazyFrame by scanning a Delta Lake table.
    /// </summary>
    /// <param name="path">Path to the Delta Lake table (folder containing _delta_log).</param>
    /// <param name="cloudOptions">Options for cloud storage authentication (AWS S3, Azure, GCP, etc).</param>
    /// <param name="version">The version of the table to read (e.g., 0, 1). Mutually exclusive with <paramref name="datetime"/>.</param>
    /// <param name="datetime">The timestamp to read (ISO-8601 string, e.g., "2026-02-09T12:00:00Z"). Mutually exclusive with <paramref name="version"/>.</param>
    /// <inheritdoc cref="LazyFrame.ScanParquet(string, ulong?, ParallelStrategy, bool, bool, bool, bool, bool, bool, string?, uint, string?, PolarsSchema?, bool, PolarsSchema?, bool, CloudOptions?)"/>
    /// <returns>A new LazyFrame.</returns>
    public static LazyFrame ScanDelta(
        string path,
        long? version = null,
        string? datetime = null,
        ulong? nRows = null,
        ParallelStrategy parallel = ParallelStrategy.Auto,
        bool lowMemory = false,
        bool useStatistics = true,
        bool glob = true,
        bool rechunk = false, 
        bool cache = true,    
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        PolarsSchema? schema = null,
        bool hivePartitioning = true,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = true,
        CloudOptions? cloudOptions = null)
    {
        if (version.HasValue && datetime != null)
        {
            throw new ArgumentException("Cannot specify both 'version' and 'datetime' for Delta Time Travel.");
        }

        var schemaHandle = schema?.Handle;
        var hiveSchemaHandle = hivePartitionSchema?.Handle;

        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        var h = PolarsWrapper.ScanDelta(
            path,
            version,
            datetime,
            nRows,
            parallel.ToNative(),
            lowMemory,
            useStatistics,
            glob,
            rechunk, 
            cache,   
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schemaHandle,     
            hivePartitioning,
            hiveSchemaHandle, 
            tryParseHiveDates,
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        );

        return new LazyFrame(h);
    }
   
    /// <summary>
    /// Sink the LazyFrame to a Delta Lake table with partition discovery.
    /// <para>
    /// This operation performs a "blind write" of partitioned Parquet files (Hive-style) 
    /// and then commits a transaction to the Delta Log, registering the new files.
    /// </para>
    /// </summary>
    /// <param name="path">
    /// Path to the root of the Delta Table. Can be local (e.g. "./data/table") 
    /// or remote (e.g. "s3://bucket/table").
    /// </param>
    /// <param name="partitionBy">
    /// The selector(s) to partition the data by. 
    /// Directories will be created in the format "col=value".
    /// </param>
    /// <param name="mode">
    /// Save mode (Append, Overwrite, ErrorIfExists, Ignore). Default is Append.
    /// </param>
    /// <param name="canEvolve">Define whether schema evolution is allowed, default: false</param>
    /// <param name="includeKeys">
    /// Whether to include the partition keys in the Parquet files themselves. 
    /// Default is true (recommended for Delta Lake compatibility).
    /// </param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    /// <param name="compression">Compression codec to use (Snappy, Zstd, etc.).</param>
    /// <param name="compressionLevel">Compression level (depends on the codec).</param>
    /// <param name="statistics">
    /// Write statistics to the Parquet file. 
    /// Delta Lake uses these stats for data skipping, so 'true' is highly recommended.
    /// </param>
    /// <param name="rowGroupSize">Target row group size (in rows).</param>
    /// <param name="dataPageSize">Target data page size (in bytes).</param>
    /// <param name="compatLevel">IPC format compatibility.</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist.</param>
    /// <param name="cloudOptions">Options for cloud storage authentication and configuration.</param>
    public void SinkDelta(
        string path,
        Selector? partitionBy = null,
        DeltaSaveMode mode = DeltaSaveMode.Append,
        bool canEvolve=false,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        ParquetCompression compression = ParquetCompression.Snappy,
        int compressionLevel = -1,
        bool statistics = true, 
        uint rowGroupSize = 0,
        uint dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = 
            CloudOptions.ParseCloudOptions(cloudOptions);
        using var partitionByH = partitionBy?.CloneHandle(); 
        PolarsWrapper.SinkDelta(
            Handle,
            path,
            
            // --- Delta Options ---
            mode.ToNative(), 
            canEvolve,
            // --- Partition Params ---
            partitionByH,
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile > 0 ? (nuint)maxRowsPerFile : 0,
            approxBytesPerFile > 0 ? (ulong)approxBytesPerFile : 0,

            // --- Parquet Options ---
            compression.ToNative(),
            compressionLevel,
            statistics,
            rowGroupSize > 0 ? rowGroupSize : 0,
            dataPageSize > 0 ? dataPageSize : 0,
            compatLevel, 

            // --- Unified Options ---
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,

            // --- Cloud Params ---
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        );
    }
    /// <inheritdoc cref="SinkDelta(string, Selector?, DeltaSaveMode, bool, bool, bool, int, long, ParquetCompression, int, bool, uint, uint, int, bool, SyncOnClose, bool, CloudOptions?)"/>
    public void SinkDelta(
        string path,
        string[]? partitionBy,
        DeltaSaveMode mode = DeltaSaveMode.Append,
        bool canEvolve = false,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        ParquetCompression compression = ParquetCompression.Snappy,
        int compressionLevel = -1,
        bool statistics = true, 
        uint rowGroupSize = 0,
        uint dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        using var selector = (partitionBy != null && partitionBy.Length > 0) 
            ? Selector.Cols(partitionBy) 
            : null;

        SinkDelta(
            path, selector, mode, canEvolve, includeKeys, keysPreGrouped, maxRowsPerFile, 
            approxBytesPerFile, compression, compressionLevel, statistics, rowGroupSize, 
            dataPageSize, compatLevel, maintainOrder, syncOnClose, mkdir, cloudOptions
        );
    }

    /// <inheritdoc cref="SinkDelta(string, Selector?, DeltaSaveMode, bool, bool, bool, int, long, ParquetCompression, int, bool, uint, uint, int, bool, SyncOnClose, bool, CloudOptions?)"/>
    public void SinkDelta(
        string path,
        string partitionBy,
        DeltaSaveMode mode = DeltaSaveMode.Append,
        bool canEvolve = false,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        ParquetCompression compression = ParquetCompression.Snappy,
        int compressionLevel = -1,
        bool statistics = true, 
        uint rowGroupSize = 0,
        uint dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
            => SinkDelta(
                path, [partitionBy], mode, canEvolve, includeKeys, keysPreGrouped, 
                maxRowsPerFile, approxBytesPerFile, compression, compressionLevel, statistics, 
                rowGroupSize, dataPageSize, compatLevel, maintainOrder, syncOnClose, mkdir, cloudOptions
            );
    
    /// <summary>
    /// Merge a LazyFrame into a Delta Lake table with full SQL MERGE semantics.
    /// Provides fine-grained control over Update, Insert, and Delete behaviors.
    /// Notice: In this method, Delete > Update > Insert > Ignore.
    /// If you need other orders, please use LazyFrame.MergeDeltaOrdered
    /// </summary>
    /// <param name="path">Uri to the Delta Lake table (local or cloud).</param>
    /// <param name="mergeKeys">The column names to join on (must exist in both Source and Target).</param>
    /// <param name="matchedUpdateCond">
    /// Condition for 'WHEN MATCHED THEN UPDATE'. 
    /// If null, defaults to true (always update when matched).
    /// </param>
    /// <param name="matchedDeleteCond">
    /// Condition for 'WHEN MATCHED THEN DELETE'. 
    /// If null, defaults to false (never delete when matched).
    /// </param>
    /// <param name="notMatchedInsertCond">
    /// Condition for 'WHEN NOT MATCHED THEN INSERT'. 
    /// If null, defaults to true (always insert new rows).
    /// </param>
    /// <param name="notMatchedBySourceDeleteCond">
    /// Condition for 'WHEN NOT MATCHED BY SOURCE THEN DELETE' (Target rows not in Source). 
    /// If null, defaults to false (retain target-only rows).
    /// </param>
    /// <param name="canEvolve">Define whether schema evolution is allowed, default: false</param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration.</param>
    [Obsolete("This method is deprecated because its execution order of matched/not-matched actions is hardcoded and may lead to silent data corruption in complex scenarios. Please use 'MergeDeltaOrdered(...)' combined with the '.WhenMatched...()' chaining methods to ensure strict SQL MERGE semantics.")]
    public void MergeDelta(
        string path,
        string[] mergeKeys,
        Expr? matchedUpdateCond = null,
        Expr? matchedDeleteCond = null,
        Expr? notMatchedInsertCond = null,
        Expr? notMatchedBySourceDeleteCond = null,
        bool canEvolve=false,
        CloudOptions? cloudOptions = null)
    {
        // 1. Parse Cloud Options
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        // 2. Clone Handles        
        using var clonedLf = CloneHandle();
        
        using var hUpdate = matchedUpdateCond?.CloneHandle();
        using var hDelete = matchedDeleteCond?.CloneHandle();
        using var hInsert = notMatchedInsertCond?.CloneHandle();
        using var hSrcDelete = notMatchedBySourceDeleteCond?.CloneHandle();

        // 3. Call Wrapper
        PolarsWrapper.DeltaMerge(
            clonedLf,
            path,
            mergeKeys,
            hUpdate,
            hDelete,
            hInsert,
            hSrcDelete,
            canEvolve,
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        );
    }
    /// <summary>
    /// Starts a fluent builder to merge a LazyFrame into a Delta Lake table with strict, order-preserving SQL MERGE semantics.
    /// <para>
    /// Unlike traditional merge methods, this builder guarantees that chained actions (Update, Delete, Insert) 
    /// are evaluated exactly in the order they are defined. If no actions are specified before execution, 
    /// it intelligently defaults to a standard Upsert (WhenMatchedUpdate + WhenNotMatchedInsert).
    /// </para>
    /// </summary>
    /// <param name="path">The URI to the target Delta Lake table (local or cloud).</param>
    /// <param name="mergeKeys">The column names to join on (must exist in both the Source DataFrame and Target Delta table).</param>
    /// <param name="canEvolve">If set to true, allows schema evolution (e.g., adding new columns from the Source to the Target). Default is false.</param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration (e.g., AWS S3, Azure Blob).</param>
    /// <returns>A <see cref="DeltaMergeBuilder"/> instance used to chain match conditions, culminating in a call to <c>.Execute()</c>.</returns>
    /// <example>
    /// <code>
    /// lf.MergeDeltaOrdered("s3://bucket/my_table", new[] { "Id" })
    ///   .WhenMatchedDelete(Delta.Source("Status") == "Deleted")      // Evaluated 1st
    ///   .WhenMatchedUpdate(Delta.Source("Stock") > Delta.Target("Stock")) // Evaluated 2nd
    ///   .WhenNotMatchedInsert()                                      // Evaluated 3rd
    ///   .Execute();
    /// </code>
    /// </example>
    public DeltaMergeBuilder MergeDeltaOrdered(
        string path, 
        string[] mergeKeys, 
        bool canEvolve = false, 
        CloudOptions? cloudOptions = null)
    {
        return new DeltaMergeBuilder(this, path, mergeKeys, canEvolve, cloudOptions);
    }
    // ==========================================
    // Meta / Inspection
    // ==========================================

    /// <summary>
    /// Gets the Schema of the LazyFrame.
    /// Note: The returned PolarsSchema object is IDisposable. 
    /// Usage in 'using' block is recommended if accessed frequently.
    /// </summary>
    public PolarsSchema Schema
    {
        get
        {
            var handle = PolarsWrapper.GetLazySchema(Handle);
            
            return new PolarsSchema(handle);
        }
    }
    IPolarsSchema IPolarsLazyFrame.Schema => this.Schema;
    /// <summary>
    /// Prints the schema to the console.
    /// </summary>
    public void PrintSchema()
    {
        using var schema = Schema;
        
        Console.WriteLine("root");
        
        foreach (var name in schema.ColumnNames)
        {
            var type = schema[name]; 
            Console.WriteLine($" |-- {name}: {type.Kind}");
        }
    }

    /// <summary>
    /// Get an explanation of the optimized query plan.
    /// <para>
    /// Returns a string representation of the logical plan after Polars optimizers 
    /// (predicate pushdown, projection pushdown, etc.) have run.
    /// </para>
    /// </summary>
    /// <param name="optimized">If true, show the optimized plan. If false, show the logical plan as built.</param>
    /// <returns>The plan as a string.</returns>
    /// <example>
    /// <code>
    /// var q = df.Lazy()
    ///     .Filter(Col("group") != "C")
    ///     .WithColumns((Col("val") * 2).Alias("val_x_2"))
    ///     .Select("group", "val_x_2");
    /// 
    /// Console.WriteLine(q.Explain());
    /// /* Output (Optimized Plan):
    /// simple π 2/2 ["group", "val_x_2"]
    ///    WITH_COLUMNS:
    ///    [[(col("val")) * (2)].alias("val_x_2")] 
    ///     FILTER [(col("group")) != ("C")]
    ///     FROM
    ///       DF ["group", "val"]; PROJECT["group", "val"] 2/2 COLUMNS
    /// */
    /// </code>
    /// </example>
    public string Explain(bool optimized = true)
    {
        return PolarsWrapper.Explain(Handle, optimized);
    }
    /// <summary>
    /// Clone the LazyFrame, creating a new independent copy.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Clone()
    {
        return new LazyFrame(PolarsWrapper.LazyClone(Handle));
    }
    internal LazyFrameHandle CloneHandle()
    {
        return PolarsWrapper.LazyClone(Handle);
    }
    // ==========================================
    // Transformations
    // ==========================================
    /// <summary>
    /// Select specific columns or expressions.
    /// </summary>
    /// <example>
    /// <code>
    /// // Select "a" and calculate "b" * 2
    /// lf.Select(Col("a"), (Col("b") * 2).Alias("b_double"));
    /// </code>
    /// </example>
    public LazyFrame Select(params Expr[] exprs)
    {
        var lfClone = CloneHandle();
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new LazyFrame(PolarsWrapper.LazySelect(lfClone, handles));
    }
    /// <summary>
    /// Select columns by name.
    /// <para>Syntactic sugar for <c>Select(Expr.Col(name))</c>.</para>
    /// </summary>
    public LazyFrame Select(params string[] columns)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Select(exprs);
    }
    /// <summary>
    /// Filter rows based on a boolean expression.
    /// <para>
    /// In a LazyFrame, this operation is added to the logical plan and is optimized before execution.
    /// Polars will attempt to push this filter down as close to the data source as possible (Predicate Pushdown).
    /// </para>
    /// </summary>
    /// <param name="expr">A boolean expression.</param>
    /// <returns>A new LazyFrame with the filter applied.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B", "C" },
    ///     val = new[] { 1, 2, 3, 4, 5 }
    /// });
    /// 
    /// // Build a lazy query:
    /// // 1. Filter out group 'C'
    /// // 2. Multiply 'val' by 2
    /// // 3. Select specific columns
    /// var q = df.Lazy()
    ///     .Filter(Col("group") != "C")
    ///     .WithColumns((Col("val") * 2).Alias("val_x_2"))
    ///     .Select("group", "val_x_2");
    /// 
    /// // Execute
    /// q.Collect().Show();
    /// /* Output:
    /// shape: (4, 2)
    /// ┌───────┬─────────┐
    /// │ group ┆ val_x_2 │
    /// │ ---   ┆ ---     │
    /// │ str   ┆ i32     │
    /// ╞═══════╪═════════╡
    /// │ A     ┆ 2       │
    /// │ A     ┆ 4       │
    /// │ B     ┆ 6       │
    /// │ B     ┆ 8       │
    /// └───────┴─────────┘
    /// */
    /// </code>
    /// </example>
    public LazyFrame Filter(Expr expr)
    {
        var lfClone = CloneHandle();
        var h = PolarsWrapper.CloneExpr(expr.Handle);
        //
        return new LazyFrame(PolarsWrapper.LazyFilter(lfClone, h));
    }
    /// <summary>
    /// Add or modify columns based on expressions.
    /// </summary>
    /// <example>
    /// <code>
    /// // Add a new column "c" while keeping "a" and "b"
    /// lf.WithColumns((Col("a") + Col("b")).Alias("c"));
    /// </code>
    /// </example>
    public LazyFrame WithColumns(params Expr[] exprs)
    {
        var lfClone = CloneHandle();
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new LazyFrame(PolarsWrapper.LazyWithColumns(lfClone, handles));
    }
    /// <summary>
    /// Slice the LazyFrame.
    /// <para>This operation is lazy; it only affects the query plan.</para>
    /// </summary>
    /// <param name="offset">Start index. Negative values count from the end.</param>
    /// <param name="length">Number of rows to return.</param>
    public LazyFrame Slice(long offset, uint length)
    {
        var handle = PolarsWrapper.LazySlice(CloneHandle(), offset, length);
        return new LazyFrame(handle);
    }
    /// <summary>
    /// Renames columns in the <see cref="LazyFrame"/>.
    /// </summary>
    /// <param name="existing">An array of existing column names to be renamed.</param>
    /// <param name="newNames">An array of new column names, corresponding by index to the names in <paramref name="existing"/>.</param>
    /// <param name="strict">
    /// If <c>true</c>, an error is raised if any column in <paramref name="existing"/> is not found in the schema. 
    /// If <c>false</c>, columns that are not found are silently ignored. Default is <c>true</c>.
    /// </param>
    /// <returns>A new <see cref="LazyFrame"/> with the rename operation applied.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="existing"/> or <paramref name="newNames"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when the length of <paramref name="existing"/> does not match the length of <paramref name="newNames"/>.</exception>
    public LazyFrame Rename(string[] existing, string[] newNames, bool strict = true)
    {
        if (existing == null || newNames == null)
            throw new ArgumentNullException("Column names cannot be null");
            
        if (existing.Length != newNames.Length)
            throw new ArgumentException("Column arrays length mismatch");

        var newHandle = PolarsWrapper.LazyRename(Handle, existing, newNames, strict);
        
        return new LazyFrame(newHandle);
    }
    /// <summary>
    /// Slice the LazyFrame (Convenience overload).
    /// </summary>
    public LazyFrame Slice(long offset, int length)
    {
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length), "Length must be non-negative.");
        return Slice(offset, (uint)length);
    }
    /// <summary>
    /// Sort the LazyFrame by a single column.
    /// </summary>
    public LazyFrame Sort(
        string column, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
        => Sort([Polars.Col(column)], [descending], [nullsLast], maintainOrder);
    /// <summary>
    /// Sort using a single expression.
    /// </summary>
    public LazyFrame Sort(
        Expr expr, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort([expr], [descending], [nullsLast], maintainOrder);
    }
    /// <summary>
    /// Sort using multiple exprs and single option.
    /// </summary>
    /// <param name="exprs"></param>
    /// <param name="descending"></param>
    /// <param name="nullsLast"></param>
    /// <param name="maintainOrder"></param>
    /// <returns></returns>
    public LazyFrame Sort(
        Expr[] exprs, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort(exprs, [descending], [nullsLast], maintainOrder);
    }
    /// <summary>
    /// Sort the LazyFrame by multiple columns (all ascending or all descending).
    /// </summary>
    public LazyFrame Sort(
        string[] columns, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Sort(exprs, [descending], [nullsLast], maintainOrder);
    }

    /// <summary>
    /// Lazily sort the DataFrame by multiple columns.
    /// <para>
    /// This operation is added to the logical plan. 
    /// Use <see cref="TopK(int, string, bool)"/> if you only need the top/bottom N rows, as it is more efficient.
    /// </para>
    /// </summary>
    /// <param name="columns">Names of the columns to sort by.</param>
    /// <param name="descending">Sort order for each column.</param>
    /// <param name="nullsLast">Whether nulls go last for each column.</param>
    /// <param name="maintainOrder">Whether to maintain the relative order of rows with equal keys.</param>
    /// <seealso cref="DataFrame.Sort(string[], bool[], bool[], bool)"/>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .Sort(
    ///       columns: new[] { "group", "val" }, 
    ///       descending: new[] { false, true }, 
    ///       nullsLast: new[] { false, false }
    ///   )
    ///   .Collect();
    /// /* Output:
    /// shape: (5, 2)
    /// ┌───────┬─────┐
    /// │ group ┆ val │
    /// │ ---   ┆ --- │
    /// │ str   ┆ i32 │
    /// ╞═══════╪═════╡
    /// │ A     ┆ 10  │
    /// │ A     ┆ 8   │
    /// │ ...   ┆ ... │
    /// └───────┴─────┘
    /// */
    /// </code>
    /// </example>
    public LazyFrame Sort(
        string[] columns, 
        bool[] descending, 
        bool[] nullsLast, 
        bool maintainOrder = false)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Sort(exprs, descending, nullsLast, maintainOrder);
    }

    /// <summary>
    /// Sort the LazyFrame by multiple exprs.
    /// </summary>
    public LazyFrame Sort(
        Expr[] exprs, 
        bool[] descending, 
        bool[] nullsLast, 
        bool maintainOrder = false)
    {
        var clonedHandles = new ExprHandle[exprs.Length];
        for (int i = 0; i < exprs.Length; i++)
        {
            clonedHandles[i] = PolarsWrapper.CloneExpr(exprs[i].Handle);
        }

        var h = PolarsWrapper.LazyFrameSort(
            Handle, 
            clonedHandles, 
            descending, 
            nullsLast, 
            maintainOrder
        );
        
        return new LazyFrame(h);
    }
    /// <summary>
    /// Get the top k rows according to the given expressions.
    /// <para>This selects the largest values.</para>
    /// </summary>
    /// <param name="k">Number of rows to return.</param>
    /// <param name="by">Expressions to sort by.</param>
    /// <param name="reverse">
    /// If true, select the smallest values (reverse the sort order) for that column.
    /// </param>
    public LazyFrame TopK(int k, Expr[] by, bool[] reverse)
    {
        if (by.Length != reverse.Length)
            throw new ArgumentException("Length of 'by' and 'reverse' must match.");

        var lfHandle = CloneHandle(); // Consume self
        var clonedHandles = new ExprHandle[by.Length];
        for (int i = 0; i < by.Length; i++)
        {
            clonedHandles[i] = PolarsWrapper.CloneExpr(by[i].Handle);
        }

        var h = PolarsWrapper.LazyFrameTopK(lfHandle, (uint)k, clonedHandles, reverse);
        return new LazyFrame(h);
    }

    /// <summary>
    /// Get the top k rows according to a single expression.
    /// </summary>
    public LazyFrame TopK(int k, Expr by, bool reverse = false)
    {
        return TopK(k, [by], [reverse]);
    }
    
    /// <summary>
    /// Get the top k rows according to a single column name.
    /// </summary>
    /// <param name="k"></param>
    /// <param name="colName"></param>
    /// <param name="reverse"></param>
    /// <returns></returns>
    public LazyFrame TopK(int k, string colName, bool reverse = false)
    {
        return TopK(k, Polars.Col(colName), reverse);
    }

    /// <summary>
    /// Get the bottom k rows according to the given expressions.
    /// <para>This selects the smallest values.</para>
    /// </summary>
    public LazyFrame BottomK(int k, Expr[] by, bool[] reverse)
    {
        if (by.Length != reverse.Length)
            throw new ArgumentException("Length of 'by' and 'reverse' must match.");

        var lfHandle = CloneHandle();
        var clonedHandles = new ExprHandle[by.Length];
        for (int i = 0; i < by.Length; i++)
        {
            clonedHandles[i] = PolarsWrapper.CloneExpr(by[i].Handle);
        }

        var h = PolarsWrapper.LazyFrameBottomK(lfHandle, (uint)k, clonedHandles, reverse);
        return new LazyFrame(h);
    }

    /// <summary>
    /// Get the bottom k rows according to a single expression.
    /// </summary>
    public LazyFrame BottomK(int k, Expr by, bool reverse = false)
    {
        return BottomK(k, [by], [reverse]);
    }
    /// <summary>
    /// Get the bottom k rows according to a single column name.
    /// </summary>
    /// <param name="k"></param>
    /// <param name="colName"></param>
    /// <param name="reverse"></param>
    /// <returns></returns>
    public LazyFrame BottomK(int k, string colName, bool reverse = false)
    {
        return BottomK(k, Polars.Col(colName), reverse);
    }
    /// <summary>
    /// Limit the number of rows in the LazyFrame.
    /// </summary>
    /// <param name="n"></param>
    /// <returns></returns>
    public LazyFrame Limit(uint n)
    {
        var lfClone = CloneHandle();
        return new LazyFrame(PolarsWrapper.LazyLimit(lfClone, n));
    }
    /// <summary>
    /// Lazily unnest struct columns.
    /// <para>
    /// Currently uses a Selector to perform the unnesting.
    /// </para>
    /// </summary>
    /// <seealso cref="DataFrame.Unnest(string[], string?)"/>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .Unnest("User")
    ///   .Collect();
    /// </code>
    /// </example>
    public LazyFrame Unnest(Selector selector,string? separator = null)
    {
        var lfClone = CloneHandle();
        var sClone = selector.CloneHandle();
        var h = PolarsWrapper.LazyFrameUnnest(lfClone, sClone,separator);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Unnest specific struct columns by name.
    /// (Syntactic sugar for Unnest(Selector.Cols(...)))
    /// </summary>
    public LazyFrame Unnest(params string[] columns)
    {
        using var sel = Selector.Cols(columns);
        return Unnest(sel,null);
    }
    /// <summary>
    /// Drop selected columns by selector.
    /// </summary>
    /// <param name="selector"></param>
    /// <returns></returns>
    public LazyFrame Drop(Selector selector)
    {
        var lfClone = CloneHandle();
        var sClone = selector.CloneHandle();
        var h = PolarsWrapper.LazyFrameDrop(lfClone, sClone);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Drop selected columns by column names.
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public LazyFrame Drop(params string[] columns)
    {
        using var sel = Selector.Cols(columns);
        return Drop(sel);
    }
    /// <summary>
    /// Keep unique rows (stable) based on a subset of columns defined by a Selector.
    /// </summary>
    /// <param name="subset">Selector defining the subset of columns. If null, uses all columns.</param>
    /// <param name="keep">Strategy to keep duplicates (First, Last, Any, None).</param>
    public LazyFrame Unique(Selector? subset = null, UniqueKeepStrategy keep = UniqueKeepStrategy.First)
    {
        _ = subset?.CloneHandle(); // Clone handle or pass reference depending on ownership model

        var h = PolarsWrapper.LazyUniqueStable(CloneHandle(), subset?.Handle!, keep.ToNative());
        return new LazyFrame(h);
    }
    /// <summary>
    /// Keep unique rows (stable) based on specific column names.
    /// </summary>
    public LazyFrame Unique(UniqueKeepStrategy keep, params string[] columns)
    {
        using var sel = Selector.Cols(columns);
        return Unique(sel, keep);
    }
    /// <summary>
    /// Keep unique First rows (stable) based on a subset of columns defined column names.
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public LazyFrame Unique(params string[] columns)
    {
        return Unique(UniqueKeepStrategy.First, columns);
    }
    /// <summary>
    /// Explode list-like columns into multiple rows.
    /// </summary>
    /// <param name="selector"></param>
    /// <param name="emptyAsNull">
    /// If <c>true</c>, empty lists are exploded into a single <c>null</c> value. 
    /// If <c>false</c>, rows with empty lists are removed from the result.
    /// </param>
    /// <param name="keepNulls">
    /// If <c>true</c>, <c>null</c> values in the column are preserved as <c>null</c> in the result. 
    /// If <c>false</c>, rows with <c>null</c> values are removed.
    /// </param>
    /// <returns></returns>
    public LazyFrame Explode(Selector selector,bool emptyAsNull=true,bool keepNulls=true)
    {
        var lfClone = CloneHandle();
        var sClone = selector.CloneHandle();
        var h = PolarsWrapper.LazyExplode(lfClone, sClone,emptyAsNull,keepNulls);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Explode list-like columns into multiple rows.
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public LazyFrame Explode(params string[] columns)
    {
        using var sel = Selector.Cols(columns);
        return Explode(sel);
    }

    // ==========================================
    // Reshaping
    // ==========================================
    /// <summary>
    /// Pivot the LazyFrame.
    /// <para>
    /// <b>Important:</b> Lazy pivot requires an eager <paramref name="onColumns"/> DataFrame 
    /// to determine the output schema (column names) during the planning phase.
    /// </para>
    /// </summary>
    /// <param name="index">Selector for the index column(s) (the rows).</param>
    /// <param name="columns">Selector for the column(s) to pivot (the new column headers).</param>
    /// <param name="values">Selector for the value column(s) to populate the cells.</param>
    /// <param name="onColumns">
    /// An <b>Eager DataFrame</b> containing the unique values of the <paramref name="columns"/>.
    /// <br/>This is strictly used for schema inference.
    /// </param>
    /// <param name="aggregateExpr">Optional expression to aggregate the values. If null, uses <paramref name="aggregateFunction"/>.</param>
    /// <param name="aggregateFunction">Aggregation function to use if <paramref name="aggregateExpr"/> is null. Default is First.</param>
    /// <param name="maintainOrder">Sort the result by the index column.</param>
    /// <param name="separator">Separator used to combine column names when multiple value columns are selected.</param>
    /// <returns>A new LazyFrame with the pivot operation applied.</returns>
    public LazyFrame Pivot(
        Selector index,
        Selector columns,
        Selector values,
        DataFrame onColumns,
        Expr? aggregateExpr = null,
        PivotAgg aggregateFunction = PivotAgg.First,
        bool maintainOrder = true,
        string? separator = null)
    {
        using var indexH = index.CloneHandle();
        using var columnsH = columns.CloneHandle(); 
        using var valuesH = values.CloneHandle();
        using var aggExprH = aggregateExpr?.CloneHandle();

        var h = PolarsWrapper.LazyPivot(
            Handle,
            columnsH,   // on
            onColumns.Handle,  // onColumns (Eager DF Handle)
            indexH,     // index
            valuesH,    // values
            aggExprH,          // aggExpr (Wrapper handles null internally)
            aggregateFunction.ToNative(),
            maintainOrder,
            separator
        );

        return new LazyFrame(h);
    }

    // -------------------------------------------------------------------------
    // 2. Overload: String Array (Syntax Sugar)
    // -------------------------------------------------------------------------

    /// <summary>
    /// Pivot the LazyFrame using column names.
    /// </summary>
    /// <param name="index">Column names to use as the index.</param>
    /// <param name="columns">Column names to use for the new column headers.</param>
    /// <param name="values">Column names to use for the values.</param>
    /// <param name="onColumns">
    /// An <b>Eager DataFrame</b> containing the unique values of the <paramref name="columns"/>.
    /// </param>
    /// <param name="aggregateFunction">Aggregation function. Default is First.</param>
    /// <param name="maintainOrder">Sort the result by the index column.</param>
    /// <param name="separator">Separator for generated column names.</param>
    public LazyFrame Pivot(
        string[] index,
        string[] columns,
        string[] values,
        DataFrame onColumns,
        PivotAgg aggregateFunction = PivotAgg.First,
        bool maintainOrder = true,
        string? separator = null)
    {
        using var sIndex = Selector.Cols(index);
        using var sColumns = Selector.Cols(columns);
        using var sValues = Selector.Cols(values);

        return Pivot(
            sIndex,
            sColumns,
            sValues,
            onColumns, 
            aggregateExpr: null,
            aggregateFunction: aggregateFunction,
            maintainOrder: maintainOrder,
            separator: separator
        );
    }

    /// <summary>
    /// Pivot the LazyFrame using column names and a custom aggregation expression.
    /// </summary>
    public LazyFrame Pivot(
        string[] index,
        string[] columns,
        string[] values,
        DataFrame onColumns,
        Expr aggregateExpr,
        bool maintainOrder = true,
        string? separator = null)
    {
        using var sIndex = Selector.Cols(index);
        using var sColumns = Selector.Cols(columns);
        using var sValues = Selector.Cols(values);

        return Pivot(
            sIndex,
            sColumns,
            sValues,
            onColumns,
            aggregateExpr: aggregateExpr,
            aggregateFunction: PivotAgg.First, // Ignored
            maintainOrder: maintainOrder,
            separator: separator
        );
    }
    /// <summary>
    /// Unpivot (Melt) the LazyFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public LazyFrame Unpivot(Selector index, Selector? on, string variableName = "variable", string valueName = "value")
    {
        var lfClone = CloneHandle();
        var indexClone = index.CloneHandle();
        var onClone = on?.CloneHandle();
        return new LazyFrame(PolarsWrapper.LazyUnpivot(lfClone, indexClone, onClone, variableName, valueName));
    }
    /// <summary>
    /// Unpivot using column names (String Array overload).
    /// Wraps strings into Selectors automatically.
    /// </summary>
    public LazyFrame Unpivot(string[] index, string[]? on, string variableName = "variable", string valueName = "value")
    {
        using var sIndex = Selector.Cols(index);
        using var sOn = on is not null ? Selector.Cols(on) : null;

        return Unpivot(sIndex, sOn, variableName, valueName);
    }
    /// <summary>
    /// Unpivot using single column names (Convenience overload).
    /// </summary>
    public LazyFrame Unpivot(string index, string? on, string variableName = "variable", string valueName = "value")
    {
        string[]? onCols = on is null ? null : [on];
        return Unpivot([index], onCols, variableName, valueName);
    }
    /// <summary>
    /// Melt the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public LazyFrame Melt(Selector index, Selector? on, string variableName = "variable", string valueName = "value") 
        => Unpivot(index, on, variableName, valueName);
    /// <summary>
    /// Lazily concatenate multiple LazyFrames.
    /// <para>
    /// This adds a concat node to the query plan. 
    /// For vertical concatenation, schemas must align (or be capable of supertype unification).
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// LazyFrame.Concat(new[] { lf1, lf2 }, ConcatType.Vertical)
    ///          .Collect();
    /// </code>
    /// </example>
    public static LazyFrame Concat(
        IEnumerable<LazyFrame> lfs, 
        ConcatType how = ConcatType.Vertical, 
        bool rechunk = false, 
        bool parallel = true)
    {
        var lfClones = lfs.Select(l => l.CloneHandle()).ToArray();
        var handles = lfClones.Select(l => l).ToArray();
        return new LazyFrame(PolarsWrapper.LazyConcat(handles, how.ToNative(), rechunk, parallel));
    }

    // ==========================================
    // Join
    // ==========================================
    /// <summary>
    /// Lazily join with another LazyFrame.
    /// <para>
    /// Polars will optimize the join execution order. 
    /// Note: Both frames must be LazyFrames.
    /// </para>
    /// </summary>
    /// <seealso cref="DataFrame.Join(DataFrame, Expr[], Expr[], JoinType,string?,JoinValidation,JoinCoalesce,JoinMaintainOrder,JoinSide,bool,long?,ulong)"/>
    /// <example>
    /// <code>
    /// var lf1 = df1.Lazy();
    /// var lf2 = df2.Lazy();
    /// 
    /// // Lazy Left Join
    /// var joined = lf1.Join(lf2, Col("id"), Col("id"), JoinType.Left)
    ///                 .Collect();
    ///                 
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────┬─────────┬───────┐
    /// │ id  ┆ name    ┆ score │
    /// │ --- ┆ ---     ┆ ---   │
    /// │ i32 ┆ str     ┆ i32   │
    /// ╞═════╪═════════╪═══════╡
    /// │ 1   ┆ Alice   ┆ 90    │
    /// │ 2   ┆ Bob     ┆ 80    │
    /// │ 3   ┆ Charlie ┆ null  │
    /// └─────┴─────────┴───────┘
    /// */
    /// </code>
    /// </example>
    public LazyFrame Join(LazyFrame other,        
        Expr[] leftOn, 
        Expr[] rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lOn = leftOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rOn = rightOn.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var lfClone = CloneHandle();
        var otherClone = other.CloneHandle();
        return new LazyFrame(PolarsWrapper.Join(
            lfClone, 
            otherClone, 
            lOn, 
            rOn, 
            how.ToNative(),
            suffix,
            validation.ToNative(),
            coalesce.ToNative(),
            maintainOrder.ToNative(),
            joinSide.ToNative(),
            nullsEqual,
            sliceOffset,
            sliceLen
        ));
    }
    /// <summary>
    /// Join with another LazyFrame using column names.
    /// </summary>
    public LazyFrame Join(LazyFrame other,         
        string[] leftOn, 
        string[] rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lExprs = leftOn.Select(Polars.Col).ToArray();
        var rExprs = rightOn.Select(Polars.Col).ToArray();
        return Join(
            other, 
            lExprs, 
            rExprs, 
            how, 
            suffix, 
            validation, 
            coalesce, 
            maintainOrder, 
            joinSide,
            nullsEqual, 
            sliceOffset, 
            sliceLen
        );
    }

    /// <summary>
    /// Join with another LazyFrame using a single column pair.
    /// </summary>
    public LazyFrame Join(LazyFrame other,
        string leftOn, 
        string rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return Join(
            other, 
            [leftOn], 
            [rightOn], 
            how, 
            suffix, 
            validation, 
            coalesce, 
            maintainOrder, 
            joinSide,
            nullsEqual, 
            sliceOffset, 
            sliceLen
        );
    }

    /// <summary>
    /// Perform an As-of join (also known as a time-series join).
    /// <para>
    /// This is similar to a left join except that we match on nearest key rather than equal keys.
    /// The join keys must be sorted.
    /// </para>
    /// </summary>
    /// <param name="other">The right LazyFrame to join with.</param>
    /// <param name="leftOn">Join key of the left LazyFrame. Must be sorted.</param>
    /// <param name="rightOn">Join key of the right LazyFrame. Must be sorted.</param>
    /// <param name="toleranceStr">
    /// Tolerance as a time duration string (e.g., "2h", "10s", "1d"). 
    /// Matches that are further away than this duration are discarded.
    /// </param>
    /// <param name="toleranceInt">
    /// Tolerance as a numeric integer (e.g., for integer-based timestamps or simple counters).
    /// </param>
    /// <param name="toleranceFloat">
    /// Tolerance as a floating point number.
    /// </param>
    /// <param name="strategy">
    /// The strategy to determine which value is "nearest" (Backward, Forward, or Nearest).
    /// Defaults to <see cref="AsofStrategy.Backward"/>.
    /// </param>
    /// <param name="leftBy">
    /// Columns to match exactly (equivalence join) before performing the as-of join. 
    /// Useful for joining separate time-series per group (e.g., by "Symbol").
    /// </param>
    /// <param name="rightBy">
    /// Columns to match exactly in the right DataFrame.
    /// </param>
    /// <param name="allowEq">
    /// If true, allow exact matches to be included in the result. 
    /// If false, a match must be strictly unequal (e.g. less than for Backward strategy) to the key.
    /// </param>
    /// <param name="checkSorted">
    /// Check if the join keys are sorted. 
    /// If false, the user must ensure keys are sorted; otherwise results are undefined (but execution is faster).
    /// </param>
    /// <param name="suffix">Suffix to append to columns with name conflicts. Defaults to "_right".</param>
    /// <param name="validation">Check if join keys are unique (mostly relevant for the 'by' columns).</param>
    /// <param name="coalesce">How to coalesce the join keys.</param>
    /// <param name="maintainOrder">How to maintain the order of the join.</param>
    /// <param name="joinSide">pecifies the strategy for the hash join build side.</param>
    /// <param name="nullsEqual">Consider nulls as equal.</param>
    /// <param name="sliceOffset">Slice the result starting at this offset (optimization).</param>
    /// <param name="sliceLen">Length of the slice to keep.</param>
    /// <example>
    /// <code>
    /// // Trades: Events happening at specific times
    /// var trades = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 10, 20, 30 },
    ///     stock = new[] { "A", "A", "A" }
    /// }).Lazy();
    /// 
    /// // Quotes: Price updates (irregular intervals)
    /// // 9->100, 15->101, 25->102, 40->103
    /// var quotes = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 9, 15, 25, 40 },
    ///     bid = new[] { 100, 101, 102, 103 }
    /// }).Lazy();
    /// 
    /// // Find the latest quote BEFORE or AT the trade time
    /// var asof = trades.JoinAsOf(
    ///     quotes, 
    ///     leftOn: Col("time"), 
    ///     rightOn: Col("time"),
    ///     strategy: AsofStrategy.Backward
    /// );
    /// 
    /// var df = asof.Collect();
    /// df.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌──────┬───────┬─────┐
    /// │ time ┆ stock ┆ bid │
    /// │ ---  ┆ ---   ┆ --- │
    /// │ i32  ┆ str   ┆ i32 │
    /// ╞══════╪═══════╪═════╡
    /// │ 10   ┆ A     ┆ 100 │ // Matches time 9
    /// │ 20   ┆ A     ┆ 101 │ // Matches time 15
    /// │ 30   ┆ A     ┆ 102 │ // Matches time 25
    /// └──────┴───────┴─────┘
    /// */
    /// </code>
    /// </example>
    internal LazyFrame JoinAsOf(
        LazyFrame other, 
        Expr leftOn, Expr rightOn, 
        string? toleranceStr = null,
        long? toleranceInt = null,
        double? toleranceFloat = null,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lfClone = CloneHandle();
        var otherClone = other.CloneHandle();
        
        var lOn = PolarsWrapper.CloneExpr(leftOn.Handle);
        var rOn = PolarsWrapper.CloneExpr(rightOn.Handle);
        
        var lBy = leftBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        var rBy = rightBy?.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();

        return new LazyFrame(PolarsWrapper.JoinAsOf(
            lfClone, otherClone,
            [lOn], [rOn], // Wrap single Expr into array
            lBy, rBy,
            strategy.ToNative(),
            toleranceStr,
            toleranceInt,
            toleranceFloat,
            allowEq,
            checkSorted,
            suffix,
            validation.ToNative(),
            coalesce.ToNative(),
            maintainOrder.ToNative(),
            joinSide.ToNative(),
            nullsEqual,
            sliceOffset,
            sliceLen
        ));
    }
    // 1. String Tolerance
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide ,bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a time duration string (e.g., "2h", "10s", "1d"). 
    /// Matches that are further away than this duration are discarded.
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, string tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceStr: tolerance, strategy: strategy, leftBy: leftBy, rightBy: rightBy);

    // 2. TimeSpan Tolerance
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder, JoinSide,bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a <see cref="TimeSpan"/>. 
    /// Matches that are further away than this duration are discarded.
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, TimeSpan tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceStr: DurationFormatter.ToPolarsString(tolerance), strategy: strategy, leftBy: leftBy, rightBy: rightBy);

    // 3. Int Tolerance (e.g. integer timestamps)
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a numeric integer (e.g., for integer-based timestamps or simple counters).
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, long tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceInt: tolerance, strategy: strategy, leftBy: leftBy, rightBy: rightBy);

    // 4. Double Tolerance (e.g. float keys)
    /// <inheritdoc cref="JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    /// <param name="tolerance">
    /// Tolerance as a floating point number.
    /// </param>
    public LazyFrame JoinAsOf(LazyFrame other, Expr leftOn, Expr rightOn, double tolerance, AsofStrategy strategy = AsofStrategy.Backward, Expr[]? leftBy = null, Expr[]? rightBy = null)
        => JoinAsOf(other, leftOn, rightOn, toleranceFloat: tolerance, strategy: strategy, leftBy: leftBy, rightBy: rightBy);
    // ==========================================
    // GroupBy
    // ==========================================
    /// <summary>
    /// Start a lazy GroupBy operation.
    /// </summary>
    /// <remarks>
    /// Unlike <see cref="DataFrame.GroupBy(Expr[])"/> which returns a <see cref="GroupByBuilder"/>,
    /// this returns a <see cref="LazyGroupBy"/> object which allows constructing the aggregation plan.
    /// </remarks>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .GroupBy("group")
    ///   .Agg(Col("val").Sum().Alias("sum_val"))
    ///   .Collect();
    ///   
    /// /* Output:
    /// shape: (2, 2)
    /// ┌───────┬─────────┐
    /// │ group ┆ sum_val │
    /// │ ---   ┆ ---     │
    /// │ str   ┆ i32     │
    /// ╞═══════╪═════════╡
    /// │ A     ┆ 3       │
    /// │ B     ┆ 7       │
    /// └───────┴─────────┘
    /// */
    /// </code>
    /// </example>
    public LazyGroupBy GroupBy(params Expr[] keys)
    {
        var lfClone = CloneHandle();
        
        return new LazyGroupBy(lfClone, keys);
    }
    /// <summary>
    /// Group by a single column name.
    /// <para>
    /// Explicit overload to ensure the string is treated as a Column, not a Literal.
    /// </para>
    /// </summary>
    public LazyGroupBy GroupBy(string name)
        => GroupBy([Polars.Col(name)]);

    /// <summary>
    /// Group by multiple column names.
    /// <para>
    /// Explicit overload to ensure strings are treated as Columns, not Literals.
    /// </para>
    /// </summary>
    public LazyGroupBy GroupBy(params string[] names)
    {
        var exprs = names.Select(n => Polars.Col(n)).ToArray();
        return GroupBy(exprs);
    }
    /// <summary>
    /// Lazily group based on a time index using dynamic windows.
    /// <para>
    /// This defines a dynamic groupby in the query plan.
    /// </para>
    /// </summary>
    /// <seealso cref="DataFrame.GroupByDynamic"/>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .GroupByDynamic("time", every: TimeSpan.FromHours(1))
    ///   .Agg(Col("val").Sum().Alias("total"))
    ///   .Collect();
    /// </code>
    /// </example>
    public LazyDynamicGroupBy GroupByDynamic(
        string indexColumn,
        TimeSpan every,
        TimeSpan? period = null,
        TimeSpan? offset = null,
        Expr[]? by = null,
        Label label = Label.Left,
        bool includeBoundaries = false,
        ClosedWindow closedWindow = ClosedWindow.Left,
        StartBy startBy = StartBy.WindowBound
    )
    {
        string everyStr = DurationFormatter.ToPolarsString(every);
        string periodStr = DurationFormatter.ToPolarsString(period) ?? everyStr;
        string offsetStr = DurationFormatter.ToPolarsString(offset) ?? "0s";

        var keys = by ?? [];
        return new LazyDynamicGroupBy(
            CloneHandle(),
            indexColumn,
            everyStr,
            periodStr,
            offsetStr,
            keys,
            label, 
            includeBoundaries,
            closedWindow,
            startBy
        );
    }
    // ==========================================
    // Execution (Collect)
    // ==========================================

    /// <summary>
    /// Execute the query plan and return a DataFrame.
    /// </summary>
    public DataFrame Collect(bool useStreaming=false)
        => new(PolarsWrapper.LazyCollect(Handle,useStreaming));

    IPolarsDataFrame IPolarsLazyFrame.Collect(bool useStreaming)
    {
        return this.Collect(useStreaming);
    }

    /// <summary>
    /// Execute the query plan using the streaming engine.
    /// </summary>
    public DataFrame CollectStreaming()
        => new(PolarsWrapper.CollectStreaming(Handle));
    /// <summary>
    /// Execute the query plan asynchronously and return a DataFrame.
    /// </summary>
    public async Task<DataFrame> CollectAsync(bool useStreaming = false, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var dfHandle = await PolarsWrapper.LazyCollectAsync(Handle, useStreaming, cancellationToken)
                                          .ConfigureAwait(false);

        return new DataFrame(dfHandle);
    }

    async Task<IPolarsDataFrame> IPolarsLazyFrame.CollectAsync(bool useStreaming, CancellationToken cancellationToken)
        => await CollectAsync(useStreaming, cancellationToken).ConfigureAwait(false);
    // ==========================================
    // Output Sink (IO)
    // ==========================================
    /// <summary>
    /// Sink the LazyFrame to a Parquet file.
    /// <para>
    /// This allows for streaming execution, processing the data in chunks and writing it to the file
    /// without loading the entire dataset into memory.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the output file.</param>
    /// <param name="compression">Compression codec to use.</param>
    /// <param name="compressionLevel">Compression level (depends on the codec).</param>
    /// <param name="statistics">Write statistics to the parquet file.</param>
    /// <param name="rowGroupSize">Target row group size (in rows).</param>
    /// <param name="dataPageSize">Target data page size (in bytes).</param>
    /// <param name="compatLevel">IPC format compatibility, -1: oldest, 0: default, 1: newest.</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist (Local file system only).</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public void SinkParquet(
        string path,
        ParquetCompression compression = ParquetCompression.Snappy,
        int compressionLevel = -1,
        bool statistics = false,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkParquet(
            Handle,
            path,
            compression.ToNative(),
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,
            provider.ToNative(),
            retries,
            retryTimeoutMs,      
            retryInitBackoffMs,
            retryMaxBackoffMs, 
            cacheTtl,
            keys,
            values
        );
    }
    /// <summary>
    /// Sink the LazyFrame to a set of Parquet files, partitioned by the specified selector.
    /// <para>
    /// This writes the dataset to a directory, splitting the data into multiple files based on the
    /// partition key(s) defined in <paramref name="partitionBy"/>.
    /// </para>
    /// </summary>
    /// <param name="path">Base path to the output directory.</param>
    /// <param name="partitionBy">The selector(s) to partition the data by.</param>
    /// <param name="includeKeys">Whether to include the partition keys in the output files.</param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// Use with caution: if the data is not grouped, the output may be incorrect.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    /// <param name="compression">Compression codec to use.</param>
    /// <param name="compressionLevel">Compression level (depends on the codec).</param>
    /// <param name="statistics">Write statistics to the parquet file.</param>
    /// <param name="rowGroupSize">Target row group size (in rows).</param>
    /// <param name="dataPageSize">Target data page size (in bytes).</param>
    /// <param name="compatLevel">IPC format compatibility, -1: oldest, 0: default, 1: newest.</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist (Local file system only).</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public void SinkParquetPartitioned(
        string path,
        Selector partitionBy,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        ParquetCompression compression = ParquetCompression.Snappy,
        int compressionLevel = -1,
        bool statistics = false,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        // Parse cloud options using the helper
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkParquetPartitioned(
            Handle,
            path,
            
            // --- Partition Params ---
            partitionBy.Handle, 
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile > 0 ? (nuint)maxRowsPerFile : 0,
            approxBytesPerFile > 0 ? (ulong)approxBytesPerFile : 0,

            // --- Parquet Options ---
            compression.ToNative(),
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,

            // --- Unified Options ---
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,

            // --- Cloud Params ---
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        );
    }
    /// <summary>
    /// Sink the LazyFrame to a Parquet format in memory.
    /// <para>
    /// This allows for streaming execution directly into a byte array without writing to disk.
    /// </para>
    /// </summary>
    public byte[] SinkParquetMemory(
        ParquetCompression compression = ParquetCompression.ZSTD,
        int compressionLevel = 3, 
        bool statistics = true,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true)
    {
        return PolarsWrapper.SinkParquetMemory(
            Handle,
            compression.ToNative(),
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,
            maintainOrder
        );
    }
    /// <summary>
    /// Sink the LazyFrame to an IPC (Arrow) file.
    /// <para>
    /// This allows for streaming execution.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the output file.</param>
    /// <param name="compression">Compression method to use.</param>
    /// <param name="compatLevel">Compatibility level (default -1 = newest).</param>
    /// <param name="recordBatchSize">Number of rows per record batch (0 = default).</param>
    /// <param name="recordBatchStatistics">Write statistics to the record batch header (default = true).</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist (Local file system only).</param>
    /// <param name="cloudOptions">Options for cloud storage.</param>
    public void SinkIpc(
        string path,
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkIpc(
            Handle,
            path,
            compression.ToNative(),
            compatLevel,
            recordBatchSize,
            recordBatchStatistics,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,
            // Cloud
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        );
    }
    /// <inheritdoc cref="SinkIpc"/>
    /// <param name="partitionBy">The selector(s) to partition the data by.</param>
    /// <param name="includeKeys">Whether to include the partition keys in the output files.</param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// Use with caution: if the data is not grouped, the output may be incorrect.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    public void SinkIpcPartitioned(
        string path,
        Selector partitionBy,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkIpcPartitioned(
            Handle,
            path,
            // --- Partition Params ---
            partitionBy.Handle, 
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile > 0 ? (nuint)maxRowsPerFile : 0,
            approxBytesPerFile > 0 ? (ulong)approxBytesPerFile : 0,
            compression.ToNative(),
            compatLevel,
            recordBatchSize,
            recordBatchStatistics,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,
            // Cloud
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        );
    }
    /// <summary>
    /// Sink the LazyFrame to an IPC (Arrow) format in memory.
    /// <para>
    /// This allows for streaming execution directly into a byte array without writing to disk.
    /// </para>
    /// </summary>
    /// <param name="compression">Compression method to use.</param>
    /// <param name="compatLevel">Compatibility level (default -1 = newest).</param>
    /// <param name="recordBatchSize">Number of rows per record batch (0 = default).</param>
    /// <param name="recordBatchStatistics">Write statistics to the record batch header (default = true).</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <returns>A byte array containing the serialized IPC data.</returns>
    public byte[] SinkIpcMemory(
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true)
    {
        return PolarsWrapper.SinkIpcMemory(
            Handle,
            compression.ToNative(),
            compatLevel,
            recordBatchSize,
            recordBatchStatistics,
            maintainOrder
        );
    }
    /// <summary>
    /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) file.
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="compression">Compression method (Gzip/Zstd).</param>
    /// <param name="compressionLevel">Compression level.</param>
    /// <param name="checkExtension">Whether to check if the file extension matches '.json' or '.ndjson'.</param>
    /// <param name="maintainOrder">Maintain the order of data.</param>
    /// <param name="syncOnClose">Sync to disk on close.</param>
    /// <param name="mkdir">Create parent directories.</param>
    /// <param name="cloudOptions">Cloud storage options.</param>
    public void SinkJson(
        string path,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkJson(
            Handle,
            path,
            compression.ToNative(),
            compressionLevel,
            checkExtension,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,
            // Cloud
            provider.ToNative(),
            retries,
            timeout,
            initBackoff,
            maxBackoff,
            cacheTtl,
            keys,
            values
        );
    }
    /// <inheritdoc cref="SinkJson"/>
    /// <param name="partitionBy">The selector(s) to partition the data by.</param>
    /// <param name="includeKeys">Whether to include the partition keys in the output files.</param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// Use with caution: if the data is not grouped, the output may be incorrect.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    public void SinkJsonPartitioned(
        string path,
        Selector partitionBy,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkJsonPartitioned(
            Handle,
            path,
            // --- Partition Params ---
            partitionBy.Handle, 
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile > 0 ? (nuint)maxRowsPerFile : 0,
            approxBytesPerFile > 0 ? (ulong)approxBytesPerFile : 0,
            compression.ToNative(),
            compressionLevel,
            checkExtension,
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,
            // Cloud
            provider.ToNative(),
            retries,
            timeout,
            initBackoff,
            maxBackoff,
            cacheTtl,
            keys,
            values
        );
    }
    /// <summary>
    /// Alias for SinkJson with format=JsonLines.
    /// </summary>
    public void SinkNdJson(
        string path,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        SinkJson(path,compression,compressionLevel,checkExtension, maintainOrder, syncOnClose, mkdir,cloudOptions);
    }
    /// <summary>
    /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) format in memory.
    /// </summary>
    public byte[] SinkJsonMemory(
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true)
    {
        return PolarsWrapper.SinkJsonMemory(
            Handle,
            compression.ToNative(),
            compressionLevel,
            checkExtension,
            maintainOrder
        );
    }
    /// <summary>
    /// Execute the LazyFrame and sink the result to a CSV file.
    /// <para>
    /// This operation allows processing datasets larger than memory by streaming results 
    /// directly to the file system.
    /// </para>
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="includeHeader">Whether to include the header row. Defaults to true.</param>
    /// <param name="includeBom">Whether to include the UTF-8 Byte Order Mark (BOM). Defaults to false.</param>
    /// <param name="separator">The character used as a field separator. Defaults to ','.</param>
    /// <param name="quoteChar">The character used for quoting fields. Defaults to '"'.</param>
    /// <param name="quoteStyle">The quoting style to use. Defaults to <see cref="QuoteStyle.Necessary"/>.</param>
    /// <param name="nullValue">The string representation for null values. Defaults to empty string.</param>
    /// <param name="lineTerminator">The character sequence used to terminate lines. Defaults to "\n".</param>
    /// <param name="floatScientific">
    /// Whether to always use scientific notation for floats. 
    /// If null (default), formatting is automatic.
    /// </param>
    /// <param name="floatPrecision">
    /// The number of decimal places to write for floats. 
    /// If null (default), uses full precision.
    /// </param>
    /// <param name="decimalComma">Whether to use a comma ',' as the decimal separator. Defaults to false.</param>
    /// <param name="dateFormat">Format string for Date columns. If null, uses ISO 8601.</param>
    /// <param name="timeFormat">Format string for Time columns. If null, uses ISO 8601.</param>
    /// <param name="datetimeFormat">Format string for Datetime columns. If null, uses ISO 8601.</param>
    /// <param name="checkExtension">Whether to check if the file extension matches '.csv'. Defaults to true.</param>
    /// <param name="compression">Compression method (Gzip/Zstd). Defaults to None.</param>
    /// <param name="compressionLevel">Compression level (depends on the codec). -1 for default.</param>
    /// <param name="maintainOrder">
    /// Whether to maintain the order of the data. 
    /// Setting this to false can improve performance in streaming mode. Defaults to true.
    /// </param>
    /// <param name="syncOnClose">File synchronization behavior on close (e.g., flush to disk). Defaults to None.</param>
    /// <param name="mkdir">Recursively create the output directory if it does not exist. Defaults to false.</param>
    /// <param name="batchSize">
    /// The batch size for writing rows. 
    /// 0 means use the Polars default.
    /// </param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public void SinkCsv(
        string path,
        bool includeHeader = true,
        bool includeBom = false,
        char separator = ',',
        char quoteChar = '"',
        QuoteStyle quoteStyle = QuoteStyle.Necessary,
        string? nullValue = null,
        string? lineTerminator = "\n",
        bool? floatScientific = null,
        int? floatPrecision = null,
        bool decimalComma = false,
        string? dateFormat = null,
        string? timeFormat = null,
        string? datetimeFormat = null,
        bool checkExtension = true,
        ExternalCompression compression = ExternalCompression.Uncompressed, 
        int compressionLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        int batchSize = 0,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkCsv(
            Handle,
            path,
            
            // CSV Writer Options
            includeHeader,
            includeBom,
            batchSize,       
            checkExtension,  

            // Compression
            compression.ToNative(), 
            compressionLevel,

            // Serialize Options 
            separator,
            quoteChar,
            quoteStyle.ToNative(),
            nullValue,
            lineTerminator,
            dateFormat,
            timeFormat,
            datetimeFormat,
            floatScientific,
            floatPrecision,
            decimalComma,

            // Unified Sink Options
            maintainOrder,
            syncOnClose.ToNative(), 
            mkdir,

            // Cloud Options
            provider.ToNative(),
            retries,
            retryTimeoutMs,      
            retryInitBackoffMs,  
            retryMaxBackoffMs,   
            cacheTtl,
            keys,
            values
        );
    }
    /// <summary>
    /// Execute the LazyFrame and sink the result to a CSV file.
    /// <para>
    /// This operation allows processing datasets larger than memory by streaming results 
    /// directly to the file system.
    /// </para>
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="partitionBy">The selector(s) to partition the data by.</param>
    /// <param name="includeKeys">Whether to include the partition keys in the output files.</param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// Use with caution: if the data is not grouped, the output may be incorrect.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    /// <param name="includeHeader">Whether to include the header row. Defaults to true.</param>
    /// <param name="includeBom">Whether to include the UTF-8 Byte Order Mark (BOM). Defaults to false.</param>
    /// <param name="separator">The character used as a field separator. Defaults to ','.</param>
    /// <param name="quoteChar">The character used for quoting fields. Defaults to '"'.</param>
    /// <param name="quoteStyle">The quoting style to use. Defaults to <see cref="QuoteStyle.Necessary"/>.</param>
    /// <param name="nullValue">The string representation for null values. Defaults to empty string.</param>
    /// <param name="lineTerminator">The character sequence used to terminate lines. Defaults to "\n".</param>
    /// <param name="floatScientific">
    /// Whether to always use scientific notation for floats. 
    /// If null (default), formatting is automatic.
    /// </param>
    /// <param name="floatPrecision">
    /// The number of decimal places to write for floats. 
    /// If null (default), uses full precision.
    /// </param>
    /// <param name="decimalComma">Whether to use a comma ',' as the decimal separator. Defaults to false.</param>
    /// <param name="dateFormat">Format string for Date columns. If null, uses ISO 8601.</param>
    /// <param name="timeFormat">Format string for Time columns. If null, uses ISO 8601.</param>
    /// <param name="datetimeFormat">Format string for Datetime columns. If null, uses ISO 8601.</param>
    /// <param name="checkExtension">Whether to check if the file extension matches '.csv'. Defaults to true.</param>
    /// <param name="compression">Compression method (Gzip/Zstd). Defaults to None.</param>
    /// <param name="compressionLevel">Compression level (depends on the codec). -1 for default.</param>
    /// <param name="maintainOrder">
    /// Whether to maintain the order of the data. 
    /// Setting this to false can improve performance in streaming mode. Defaults to true.
    /// </param>
    /// <param name="syncOnClose">File synchronization behavior on close (e.g., flush to disk). Defaults to None.</param>
    /// <param name="mkdir">Recursively create the output directory if it does not exist. Defaults to false.</param>
    /// <param name="batchSize">
    /// The batch size for writing rows. 
    /// 0 means use the Polars default.
    /// </param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public void SinkCsvPartitioned(
        string path,
        Selector partitionBy,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        bool includeHeader = true,
        bool includeBom = false,
        char separator = ',',
        char quoteChar = '"',
        QuoteStyle quoteStyle = QuoteStyle.Necessary,
        string? nullValue = null,
        string? lineTerminator = "\n",
        bool? floatScientific = null,
        int? floatPrecision = null,
        bool decimalComma = false,
        string? dateFormat = null,
        string? timeFormat = null,
        string? datetimeFormat = null,
        bool checkExtension = true,
        ExternalCompression compression = ExternalCompression.Uncompressed, 
        int compressionLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        int batchSize = 0,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.SinkCsvPartitioned(
            Handle,
            path,
            // --- Partition Params ---
            partitionBy.Handle, 
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile > 0 ? (nuint)maxRowsPerFile : 0,
            approxBytesPerFile > 0 ? (ulong)approxBytesPerFile : 0,
            // CSV Writer Options
            includeHeader,
            includeBom,
            batchSize,       
            checkExtension,  

            // Compression
            compression.ToNative(), 
            compressionLevel,

            // Serialize Options 
            separator,
            quoteChar,
            quoteStyle.ToNative(),
            nullValue,
            lineTerminator,
            dateFormat,
            timeFormat,
            datetimeFormat,
            floatScientific,
            floatPrecision,
            decimalComma,

            // Unified Sink Options
            maintainOrder,
            syncOnClose.ToNative(), 
            mkdir,

            // Cloud Options
            provider.ToNative(),
            retries,
            retryTimeoutMs,      
            retryInitBackoffMs,  
            retryMaxBackoffMs,   
            cacheTtl,
            keys,
            values
        );
    }
    /// <summary>
    /// Sink the LazyFrame to a CSV format in memory.
    /// <para>
    /// This allows for streaming execution directly into a byte array without writing to disk.
    /// </para>
    /// </summary>
    public byte[] SinkCsvMemory(
        bool includeBom = false,
        bool includeHeader = true,
        int batchSize = 1024,
        bool checkExtension = false, 
        ExternalCompression compressionCode = 0,    
        int compressionLevel = 0,
        string? dateFormat = null,
        string? timeFormat = null,
        string? datetimeFormat = null,
        int floatScientific = -1,
        int floatPrecision = -1,
        bool decimalComma = false,
        byte separator = (byte)',',
        byte quoteChar = (byte)'"',
        string? nullValue = null,
        string? lineTerminator = "\n",
        QuoteStyle quoteStyle = QuoteStyle.Necessary,         
        bool maintainOrder = true)
    {
        return PolarsWrapper.SinkCsvMemory(
            Handle,
            includeBom,
            includeHeader,
            batchSize,
            checkExtension,
            compressionCode.ToNative(),
            compressionLevel,
            dateFormat,
            timeFormat,
            datetimeFormat,
            floatScientific,
            floatPrecision,
            decimalComma,
            separator,
            quoteChar,
            nullValue,
            lineTerminator,
            quoteStyle.ToNative(),
            maintainOrder
        );
    }
    /// <summary>
    /// Streaming Sink to Batchs
    /// </summary>
    public void SinkBatches(Action<RecordBatch> onBatchReceived)
    {
        using var newLfHandle = PolarsWrapper.SinkBatches(CloneHandle(), onBatchReceived);

        using var lfRes = new LazyFrame(newLfHandle);
        using var _ = lfRes.Collect(); 
    }
    /// <summary>
    /// Stream the result of the LazyFrame calculation into an <see cref="IDataReader"/>.
    /// <para>
    /// This allows processing huge datasets that don't fit in memory by handling them in chunks (RecordBatches).
    /// </para>
    /// <para>
    /// Common use cases:
    /// <list type="bullet">
    /// <item>Bulk inserting data into SQL Databases (using SqlBulkCopy or NpgsqlBinaryImporter).</item>
    /// <item>Streaming data to other .NET libraries that consume IDataReader.</item>
    /// </list>
    /// </para>
    /// </summary>
    /// <param name="writerAction">
    /// A callback that receives the <see cref="IDataReader"/>. 
    /// This action executes on a separate thread (Consumer) while the Polars engine (Producer) pumps data.
    /// </param>
    /// <param name="bufferSize">
    /// The number of RecordBatches to buffer in memory. 
    /// If the buffer is full, the Polars engine will pause until the consumer reads more data (Backpressure).
    /// </param>
    /// <param name="typeOverrides">Optional schema overrides to guide the type mapping.</param>
    /// <example>
    /// <code>
    /// // Simulate a large lazy computation
    /// var lf = DataFrame.FromColumns(new { id = new[] { 0, 1, 2, 3, 4 } }).Lazy();
    /// 
    /// // Stream result to a database writer (simulated here)
    /// lf.SinkTo(reader => 
    /// {
    ///     Console.WriteLine("[DB Writer] Started receiving data...");
    ///     while (reader.Read())
    ///     {
    ///         var val = reader.GetValue(0);
    ///         Console.WriteLine($"[DB Writer] Insert row: {val}");
    ///     }
    ///     Console.WriteLine("[DB Writer] Done.");
    /// }, bufferSize: 2);
    /// 
    /// /* Output:
    /// [DB Writer] Started receiving data...
    /// [DB Writer] Insert row: 0
    /// [DB Writer] Insert row: 1
    /// ...
    /// [DB Writer] Done.
    /// */
    /// </code>
    /// </example>
    public void SinkTo(Action<IDataReader> writerAction, int bufferSize = 5,Dictionary<string, Type>? typeOverrides = null)
    {
        // 1. Producer-Consumer buffer
        using var buffer = new BlockingCollection<RecordBatch>(boundedCapacity: bufferSize);

        // 2. Start consumer (DB Writer)
        var consumerTask = Task.Run(() => 
        {
            
            // ArrowToDbStream is responsible for disguising Buffer as DataReader
            // It automatically handles Dispose, so Batch will be released after writerAction finishes reading
            using var reader = new ArrowToDbStream(buffer.GetConsumingEnumerable(),typeOverrides);
            // Hand over the reader to user logic
            // Users call bulk.WriteToServer(reader) here
            writerAction(reader);
        });

        // 3. Start producer (Polars Engine - blocking execution in current thread)
        try
        {
            // Push data produced by Rust into Buffer
            // If Buffer is full, this will block, thereby automatically backpressuring the Rust engine
            SinkBatches(buffer.Add);
        }
        finally
        {
            // 4. Notify consumer: no more data
            buffer.CompleteAdding();
        }

        // 5. Wait for consumer to finish writing and throw possible exceptions
        consumerTask.Wait();
    }

    /// <summary>
    /// Dispose the LazyFrame and release native resources.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
        GC.SuppressFinalize(this); 
    }
}