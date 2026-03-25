#pragma warning disable CS1573

using System.Collections.Concurrent;
using System.Data;
using Apache.Arrow;
using Apache.Arrow.Adbc;
using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Polars.NET.Core.Data;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable,IPolarsLazyFrame
{
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
        PolarsSchema? schema = null,
        PolarsSchema? dtypeOverride = null,
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
            dtypeOverride?.Handle,
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
        PolarsSchema? schema = null,
        PolarsSchema? dtypeOverride = null,
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
            dtypeOverride?.Handle,
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
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
       // ==========================================
    // Static IO Read
    // ==========================================
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
    /// <param name="skipRows">Number of rows to skip from the start. Defaults to 0.</param>
    /// <param name="nRows">Stop reading after n rows. If null, read all.</param>
    /// <param name="inferSchemaLength">Number of rows to scan for schema inference. If null, use Polars default (100).</param>
    /// <param name="schema">Provide a schema to ignore schema inference.</param>
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
        int skipRows = 0,
        int? nRows = null,
        int? inferSchemaLength = null,
        PolarsSchema? schema = null,
        PolarsSchema? dtypeOverride = null,
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
            skipRows: skipRows >= 0 ? (ulong)skipRows : 0,
            nRows: nRows.HasValue ? (ulong)nRows.Value : null,
            inferSchemaLength: inferSchemaLength.HasValue ? (ulong)inferSchemaLength.Value : 100,
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
            lf = lf.Select(Selector.Cols(columns));
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
    /// <param name="skipRows">Number of rows to skip from the start. Defaults to 0.</param>
    /// <param name="nRows">Stop reading after n rows. If null, read all.</param>
    /// <param name="inferSchemaLength">Number of rows to scan for schema inference. If null, use Polars default (100).</param>
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
        int skipRows = 0,
        int? nRows = null,
        int? inferSchemaLength = null,
        PolarsSchema? schema = null,
        PolarsSchema? dtypeOverride = null,
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
            skipRows: skipRows >= 0 ? (ulong)skipRows : 0,
            nRows: nRows.HasValue ? (ulong)nRows.Value : null,
            inferSchemaLength: inferSchemaLength.HasValue ? (ulong)inferSchemaLength.Value : 100,
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
            lf = lf.Select(Selector.Cols(columns));
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
    /// <param name="skipRows">Number of rows to skip from the start. Defaults to 0.</param>
    /// <param name="nRows">Stop reading after n rows. If null, read all.</param>
    /// <param name="inferSchemaLength">Number of rows to scan for schema inference. If null, use Polars default (100).</param>
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
        int skipRows = 0,
        int? nRows = null,
        int? inferSchemaLength = null,
        PolarsSchema? schema = null,
        PolarsSchema? dtypeOverride = null, 
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
            tryParseDates: tryParseDates,
            lowMemory: lowMemory,
            skipRows: skipRows >= 0 ? (ulong)skipRows : 0,
            nRows: nRows.HasValue ? (ulong)nRows.Value : null,
            inferSchemaLength: inferSchemaLength.HasValue ? (ulong)inferSchemaLength.Value : 100,
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
            lf = lf.Select(Selector.Cols(columns));
        }

        return lf.Collect();
    }
    /// <summary>
    /// Read a DataFrame from a Parquet file or multiple files via glob patterns.
    /// <para>
    /// Note: This method internally uses LazyFrame.ScanParquet and collects the result. 
    /// For larger-than-memory datasets or better query optimization, consider using LazyFrame.ScanParquet directly.
    /// </para>
    /// </summary>
    /// <param name="path">Path to file or glob pattern (e.g. "data/*.parquet" or "s3://bucket/data.parquet").</param>
    /// <param name="columns">Columns to select. If null, select all columns. (Optimized via projection pushdown).</param>
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
    /// <param name="schema">Manually specify the schema of the file(s).</param>
    /// <param name="hivePartitionSchema">Manually specify the schema for Hive partitioning columns.</param>
    /// <param name="tryParseHiveDates">Whether to try parsing dates in Hive partitioning paths (default: true).</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public static DataFrame ReadParquet(
        string path,
        string[]? columns = null,
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
        var lf = LazyFrame.ScanParquet(
            path,
            nRows,
            parallel,
            lowMemory,
            useStatistics,
            glob,
            allowMissingColumns,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schema,
            hivePartitioning,
            hivePartitionSchema,
            tryParseHiveDates,
            cloudOptions
        );

        if (columns != null && columns.Length > 0)
        {
            var colsToSelect = new List<string>(columns);
            
            if (!string.IsNullOrEmpty(rowIndexName) && !colsToSelect.Contains(rowIndexName))
            {
                colsToSelect.Add(rowIndexName);
            }

            if (!string.IsNullOrEmpty(includePathColumn) && !colsToSelect.Contains(includePathColumn))
            {
                colsToSelect.Add(includePathColumn);
            }

            lf = lf.Select(Selector.Cols(colsToSelect.ToArray()));
        }

        return lf.Collect();
    }

    /// <summary>
    /// Read Parquet from an in-memory byte array.
    /// <para>
    /// Note: This method internally uses LazyFrame.ScanParquet and collects the result.
    /// </para>
    /// </summary>
    /// <param name="buffer">The byte array containing the parquet file.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <inheritdoc cref="ReadParquet(string, string[], ulong?, ParallelStrategy, bool, bool, bool, bool, bool, bool, string?, uint, string?, PolarsSchema?,bool, PolarsSchema?, bool, CloudOptions?)"/>
    public static DataFrame ReadParquet(
        byte[] buffer,
        string[]? columns = null,
        ulong? nRows = null,
        ParallelStrategy parallel = ParallelStrategy.Auto,
        bool lowMemory = false,
        bool useStatistics = true,
        bool allowMissingColumns = false,
        bool rechunk = false, 
        bool cache = true,    
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        PolarsSchema? schema = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false)
    {
        var lf = LazyFrame.ScanParquet(
            buffer,
            nRows,
            parallel,
            lowMemory,
            useStatistics,
            allowMissingColumns,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schema,
            hivePartitioning,
            hivePartitionSchema,
            tryParseHiveDates
        );

        if (columns != null && columns.Length > 0)
        {
            var colsToSelect = new List<string>(columns);
            
            if (!string.IsNullOrEmpty(rowIndexName) && !colsToSelect.Contains(rowIndexName))
            {
                colsToSelect.Add(rowIndexName);
            }

            if (!string.IsNullOrEmpty(includePathColumn) && !colsToSelect.Contains(includePathColumn))
            {
                colsToSelect.Add(includePathColumn);
            }

            lf = lf.Select(Selector.Cols(colsToSelect.ToArray()));
        }

        return lf.Collect();
    }

    /// <summary>
    /// Read Parquet from a Stream.
    /// <para>
    /// Note: This method copies the stream to memory and then uses the Lazy execution engine.
    /// </para>
    /// </summary>
    /// <param name="stream">The input stream containing the parquet file.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <inheritdoc cref="ReadParquet(string, string[], ulong?, ParallelStrategy, bool, bool, bool, bool, bool, bool, string?, uint, string?, PolarsSchema?,bool, PolarsSchema?, bool, CloudOptions?)"/>
    public static DataFrame ReadParquet(
        Stream stream,
        string[]? columns = null,
        ulong? nRows = null,
        ParallelStrategy parallel = ParallelStrategy.Auto,
        bool lowMemory = false,
        bool useStatistics = true,
        bool allowMissingColumns = false,
        bool rechunk = false, 
        bool cache = true,    
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        PolarsSchema? schema = null,
        bool hivePartitioning = false,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false)
    {
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        
        return ReadParquet(
            ms.ToArray(),
            columns,
            nRows,
            parallel,
            lowMemory,
            useStatistics,
            allowMissingColumns,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schema,
            hivePartitioning,
            hivePartitionSchema,
            tryParseHiveDates
        );
    }

    /// <summary>
    /// Read a JSON file into a DataFrame.
    /// </summary>
    /// <param name="path">Path to the JSON file.</param>
    /// <param name="columns">Select specific columns to read.</param>
    /// <param name="schema">Manually specify the schema (recommended for stability).</param>
    /// <param name="inferSchemaLen">Number of rows to scan for schema inference (default: scan all).</param>
    /// <param name="batchSize">Batch size for reading (optimization).</param>
    /// <param name="ignoreErrors">Ignore parsing errors (skip malformed lines).</param>
    /// <param name="jsonFormat">Format: Json Array or Json Lines (NDJSON).</param>
    public static DataFrame ReadJson(
        string path,
        string[]? columns = null,
        PolarsSchema? schema = null,
        ulong? inferSchemaLen = null,
        ulong? batchSize = null,
        bool ignoreErrors = false,
        JsonFormat jsonFormat = JsonFormat.Json)
    {
        if (!File.Exists(path)) 
            throw new FileNotFoundException($"JSON file not found: {path}");

        var schemaHandle = schema?.Handle;

        var h = PolarsWrapper.ReadJson(
            path,
            columns,
            schemaHandle,
            inferSchemaLen,
            batchSize,
            ignoreErrors,
            jsonFormat.ToNative()
        );

        return new DataFrame(h);
    }

    // ---------------------------------------------------------
    // Read JSON (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Read JSON from an in-memory byte array.
    /// </summary>
    public static DataFrame ReadJson(
        byte[] buffer,
        string[]? columns = null,
        PolarsSchema? schema = null,
        ulong? inferSchemaLen = null,
        ulong? batchSize = null,
        bool ignoreErrors = false,
        JsonFormat jsonFormat = JsonFormat.Json)
    {
        var schemaHandle = schema?.Handle;

        var h = PolarsWrapper.ReadJson(
            buffer,
            columns,
            schemaHandle,
            inferSchemaLen,
            batchSize,
            ignoreErrors,
            jsonFormat.ToNative()
        );

        return new DataFrame(h);
    }

    // ---------------------------------------------------------
    // Read JSON (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Read JSON from a Stream.
    /// </summary>
    public static DataFrame ReadJson(
        Stream stream,
        string[]? columns = null,
        PolarsSchema? schema = null,
        ulong? inferSchemaLen = null,
        ulong? batchSize = null,
        bool ignoreErrors = false,
        JsonFormat jsonFormat = JsonFormat.Json)
    {
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        
        return ReadJson(
            ms.ToArray(), 
            columns, 
            schema, 
            inferSchemaLen, 
            batchSize, 
            ignoreErrors, 
            jsonFormat
        );
    }

    // ---------------------------------------------------------
    // Read IPC (File / Cloud)
    // ---------------------------------------------------------

    /// <summary>
    /// Read an Arrow IPC (Feather v2) file into a DataFrame.
    /// <para>
    /// Note: This method internally uses LazyFrame.ScanIpc and collects the result. 
    /// For larger-than-memory datasets or better query optimization, consider using LazyFrame.ScanIpc directly.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the IPC file, glob pattern, or cloud path (e.g., "s3://...").</param>
    /// <param name="columns">Columns to select. If null, select all columns. (Optimized via projection pushdown).</param>
    /// <param name="schema">Optional schema to enforce. If not provided, the schema is inferred from the file footer.</param>
    /// <param name="nRows">Limit the number of rows to scan.</param>
    /// <param name="rechunk">Make sure the DataFrame is contiguous in memory (default: false).</param>
    /// <param name="cache">Cache the result of the scan (default: true).</param>
    /// <param name="glob">Expand glob patterns (default: true).</param>
    /// <param name="rowIndexName">If provided, adds a column with the row index.</param>
    /// <param name="rowIndexOffset">Offset for the row index (default: 0).</param>
    /// <param name="includePathColumn">If provided, adds a column with the source file path.</param>
    /// <param name="hivePartitioning">Enable Hive partitioning inference (default: false).</param>
    /// <param name="hivePartitionSchema">Manually specify the schema for Hive partitioning columns.</param>
    /// <param name="tryParseHiveDates">Whether to try parsing dates in Hive partitioning paths (default: true).</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public static DataFrame ReadIpc(
        string path,
        string[]? columns = null,
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
        var lf = LazyFrame.ScanIpc(
            path,
            schema,
            nRows,
            rechunk,
            cache,
            glob,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            hivePartitioning,
            hivePartitionSchema,
            tryParseHiveDates,
            cloudOptions
        );

        if (columns != null && columns.Length > 0)
        {
            var colsToSelect = new List<string>(columns);
            
            if (!string.IsNullOrEmpty(rowIndexName) && !colsToSelect.Contains(rowIndexName))
            {
                colsToSelect.Add(rowIndexName);
            }

            if (!string.IsNullOrEmpty(includePathColumn) && !colsToSelect.Contains(includePathColumn))
            {
                colsToSelect.Add(includePathColumn);
            }

            lf = lf.Select(Selector.Cols(colsToSelect.ToArray()));
        }

        return lf.Collect();
    }

    // ---------------------------------------------------------
    // Read IPC (Memory / Bytes)
    // ---------------------------------------------------------

    /// <summary>
    /// Read Arrow IPC (Feather v2) from in-memory bytes.
    /// <para>
    /// Note: This method internally uses LazyFrame.ScanIpc and collects the result.
    /// </para>
    /// </summary>
    /// <param name="buffer">The byte array containing the IPC data.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <inheritdoc cref="ReadIpc(string, string[], PolarsSchema?, ulong?, bool, bool, bool, string?, uint, string?, bool, PolarsSchema?, bool, CloudOptions?)"/>
    public static DataFrame ReadIpc(
        byte[] buffer,
        string[]? columns = null,
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
        var lf = LazyFrame.ScanIpc(
            buffer,
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

        if (columns != null && columns.Length > 0)
        {
            var colsToSelect = new List<string>(columns);
            
            if (!string.IsNullOrEmpty(rowIndexName) && !colsToSelect.Contains(rowIndexName))
            {
                colsToSelect.Add(rowIndexName);
            }

            if (!string.IsNullOrEmpty(includePathColumn) && !colsToSelect.Contains(includePathColumn))
            {
                colsToSelect.Add(includePathColumn);
            }

            lf = lf.Select(Selector.Cols(colsToSelect.ToArray()));
        }

        return lf.Collect();
    }

    // ---------------------------------------------------------
    // Read IPC (Stream)
    // ---------------------------------------------------------

    /// <summary>
    /// Read Arrow IPC (Feather v2) from a Stream.
    /// <para>
    /// Note: This reads the stream fully into memory and then uses the Lazy execution engine.
    /// </para>
    /// </summary>
    /// <param name="stream">The input stream containing the IPC data.</param>
    /// <param name="columns">Columns to select. If null, select all columns.</param>
    /// <inheritdoc cref="ReadIpc(string, string[], PolarsSchema?, ulong?, bool, bool, bool, string?, uint, string?, bool, PolarsSchema?, bool, CloudOptions?)"/>
    public static DataFrame ReadIpc(
        Stream stream,
        string[]? columns = null,
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
        
        return ReadIpc(
            ms.ToArray(),
            columns,
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
    // Read Excel (Native Rust Engine)
    // ---------------------------------------------------------

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
        PolarsSchema? schema = null,
        bool hasHeader = true,
        ulong inferSchemaLen = 100,
        bool dropEmptyRows = true,
        bool raiseIfEmpty = true)
    {
        if (!File.Exists(path))
            throw new FileNotFoundException($"Excel file not found: {path}");

        var schemaHandle = schema?.Handle;

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
    /// <summary>
    /// Create DataFrame from Apache Arrow RecordBatch.
    /// </summary>
    public static DataFrame FromArrow(RecordBatch batch)
    {
        var handle = ArrowFfiBridge.ImportDataFrame(batch);
        return new DataFrame(handle);
    }
    /// <summary>
    /// Transfer a DataFrame to Arrow
    /// </summary>
    /// <returns></returns>
    public RecordBatch ToArrow()
        => ArrowFfiBridge.ExportDataFrame(Handle);
    /// <summary>
    /// Asynchronously read a DataFrame from a CSV file.
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
    /// <param name="skipRows">Number of rows to skip from the start. Defaults to 0.</param>
    /// <param name="nRows">Stop reading after n rows. If null, read all.</param>
    /// <param name="inferSchemaLength">Number of rows to scan for schema inference. If null, use Polars default (100).</param>
    /// <param name="schema">Provide a schema to ignore schema inference.</param>
    /// <param name="encoding">Encoding of the CSV file. Defaults to Utf8.</param>
    /// <param name="nullValues">List of strings to consider as null values.</param>
    /// <param name="missingIsNull">Treat missing fields as null. Defaults to true.</param>
    /// <param name="commentPrefix">Lines starting with this prefix will be ignored.</param>
    /// <param name="decimalComma">Use comma as decimal separator. Defaults to false.</param>
    /// <param name="truncateRaggedLines">Truncate lines that are longer than the schema. Defaults to false.</param>
    /// <param name="rowIndexName">If provided, add a column with the row index.</param>
    /// <param name="rowIndexOffset">Offset for the row index. Defaults to 0.</param>
    /// <param name="cloudOptions">Options for cloud storage authentication and configuration.</param>
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
        int skipRows = 0,
        int? nRows = null,
        int? inferSchemaLength = null,
        PolarsSchema? schema = null,
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
            hasHeader: hasHeader,
            separator: separator,
            quoteChar: quoteChar,
            eolChar: eolChar,
            ignoreErrors: ignoreErrors,
            tryParseDates: tryParseDates,
            lowMemory: lowMemory,
            skipRows: skipRows >= 0 ? (ulong)skipRows : 0,
            nRows: nRows.HasValue ? (ulong)nRows.Value : null,
            inferSchemaLength: inferSchemaLength.HasValue ? (ulong)inferSchemaLength.Value : 100,
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

            lf = lf.Select(Selector.Cols([.. colsToSelect]));
        }

        return await lf.CollectAsync(useStreaming: true);
    }
    /// <summary>
    /// Read a Parquet file asynchronously.
    /// </summary>
    public static async Task<DataFrame> ReadParquetAsync(
        string path,
        string[]? columns = null,
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
        var lf = LazyFrame.ScanParquet(
            path,
            nRows,
            parallel,
            lowMemory,
            useStatistics,
            glob,
            allowMissingColumns,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schema,
            hivePartitioning,
            hivePartitionSchema,
            tryParseHiveDates,
            cloudOptions
        );

        if (columns != null && columns.Length > 0)
        {
            var colsToSelect = new List<string>(columns);
            
            if (!string.IsNullOrEmpty(rowIndexName) && !colsToSelect.Contains(rowIndexName))
            {
                colsToSelect.Add(rowIndexName);
            }

            if (!string.IsNullOrEmpty(includePathColumn) && !colsToSelect.Contains(includePathColumn))
            {
                colsToSelect.Add(includePathColumn);
            }

            lf = lf.Select(Selector.Cols([.. colsToSelect]));
        }

        return await lf.CollectAsync(useStreaming:true);
    }
    /// <summary>
    /// Read an Avro file into a DataFrame.
    /// </summary>
    /// <param name="path">The path to the Avro file.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <returns>A new DataFrame.</returns>
    public static DataFrame ReadAvro(
        string path, 
        ulong? nRows = null, 
        string[]? columns = null, 
        int[]? projection = null)
    {
        var handle = PolarsWrapper.ReadAvro(path, nRows, columns, projection);
        return new DataFrame(handle);
    }
    /// <summary>
    /// Read an Avro memory buffer into a DataFrame.
    /// </summary>
    /// <param name="buffer">The byte array containing Avro data.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <returns>A new DataFrame.</returns>
    public static DataFrame ReadAvro(
        byte[] buffer, 
        ulong? nRows = null, 
        string[]? columns = null, 
        int[]? projection = null)
    {
        var handle = PolarsWrapper.ReadAvro(buffer, nRows, columns, projection);
        return new DataFrame(handle);
    }
    /// <summary>
    /// Read an Avro memory Stream into a DataFrame.
    /// </summary>
    /// <param name="stream">The stream containing Avro data.</param>
    /// <param name="nRows">Stop reading when `nRows` are read.</param>
    /// <param name="columns">Columns to select/project by name.</param>
    /// <param name="projection">Columns to select/project by index.</param>
    /// <returns>A new DataFrame.</returns>
    public static DataFrame ReadAvro(
        Stream stream, 
        ulong? nRows = null, 
        string[]? columns = null, 
        int[]? projection = null)
    {
        using var ms = new MemoryStream();
        stream.CopyTo(ms);
        var handle = PolarsWrapper.ReadAvro(ms.ToArray(), nRows, columns, projection);
        return new DataFrame(handle);
    }
    /// <summary>
    /// Read a delta table into a new DataFrame
    /// </summary>
    /// <inheritdoc cref="LazyFrame.ScanDelta(string, long?, string?, ulong?, ParallelStrategy, bool, bool, bool, bool, bool, string?, uint, string?, PolarsSchema?, bool, PolarsSchema?, bool, CloudOptions?)"/>
    public static DataFrame ReadDelta(
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
        
        var lf = LazyFrame.ScanDelta(
            path,
            version,
            datetime,
            nRows,
            parallel,
            lowMemory,
            useStatistics,
            glob,
            rechunk, 
            cache,   
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schema,     
            hivePartitioning,
            hivePartitionSchema, 
            tryParseHiveDates,
            cloudOptions
        );

        return lf.Collect();
    }
    /// <summary>
    /// Create a DataFrame directly from a <see cref="IDataReader"/>.
    /// <para>
    /// This method streams data from the reader into Arrow batches, allowing for memory-efficient 
    /// loading of large datasets from databases (e.g., SQL Server, PostgreSQL, SQLite).
    /// </para>
    /// <para>
    /// It automatically maps C# types to Polars types (e.g., <see cref="decimal"/> to Decimal128, <see cref="DateTime"/> to Timestamp).
    /// </para>
    /// </summary>
    /// <param name="reader">The open IDataReader instance.</param>
    /// <param name="batchSize">The number of rows to process per Arrow batch. Default is 50,000.</param>
    /// <returns>A new DataFrame.</returns>
    /// <example>
    /// <code>
    /// // Mocking a DataTable as a data source
    /// var table = new System.Data.DataTable();
    /// table.Columns.Add("Product", typeof(string));
    /// table.Columns.Add("Price", typeof(decimal)); // Correctly maps to Polars Decimal128
    /// 
    /// table.Rows.Add("Laptop", 1234.56m);
    /// table.Rows.Add("Mouse", 99.99m);
    /// 
    /// using IDataReader reader = table.CreateDataReader();
    /// 
    /// var df = DataFrame.ReadDatabase(reader);
    /// df.Show();
    /// /* Output:
    /// shape: (2, 2)
    /// ┌─────────┬─────────────────────────┐
    /// │ Product ┆ Price                   │
    /// │ ---     ┆ ---                     │
    /// │ str     ┆ decimal[38,18]          │
    /// ╞═════════╪═════════════════════════╡
    /// │ Laptop  ┆ 1234.560000000000000000 │
    /// │ Mouse   ┆ 99.990000000000000000   │
    /// └─────────┴─────────────────────────┘
    /// */
    /// </code>
    /// </example>
    public static DataFrame ReadDatabase(IDataReader reader, int batchSize = 50_000)
    {
        // Get Schema 
        var schema = reader.GetArrowSchema();

        var batchEnumerable = reader.ToArrowBatches(batchSize).Prefetch();

        var handle = ArrowStreamInterop.ImportEager(batchEnumerable, schema);
        
        if (handle.IsInvalid)
        {
            var emptyBatch = new RecordBatch(schema, [], 0);
            return new DataFrame(ArrowFfiBridge.ImportDataFrame(emptyBatch));
        }
        
        return new DataFrame(handle);
    }
        // ==========================================
    // IO Write
    // ==========================================
    /// <summary>
    /// Write DataFrame to a comma-separated values (CSV) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming and cloud storage.
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
    public void WriteCsv(
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

        var lf = Lazy();

        lf.SinkCsv(
            path,
            includeHeader,
            includeBom,
            separator,
            quoteChar,
            quoteStyle,
            nullValue,
            lineTerminator,
            floatScientific,
            floatPrecision,
            decimalComma,
            dateFormat,
            timeFormat,
            datetimeFormat,
            checkExtension,
            compression,
            compressionLevel,
            maintainOrder,
            syncOnClose,
            mkdir,
            batchSize,
            cloudOptions
        );
    }
    /// <summary>
    /// Write DataFrame to a partitioned comma-separated values (CSV) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming and cloud storage.
    /// </para>
    /// </summary>
    /// <param name="path">The output base directory path.</param>
    /// <param name="partitionBy">The selector(s) to partition the data by.</param>
    /// <param name="includeKeys">Whether to include the partition keys in the output files.</param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// Use with caution: if the data is not grouped, the output may be incorrect.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
    /// <inheritdoc cref="WriteCsv"/>
    public void WriteCsvPartitioned(
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

        var lf = Lazy();

        lf.SinkCsvPartitioned(
            path,
            // --- Partition Params ---
            partitionBy, 
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile,
            approxBytesPerFile,
            includeHeader,
            includeBom,
            separator,
            quoteChar,
            quoteStyle,
            nullValue,
            lineTerminator,
            floatScientific,
            floatPrecision,
            decimalComma,
            dateFormat,
            timeFormat,
            datetimeFormat,
            checkExtension,
            compression,
            compressionLevel,
            maintainOrder,
            syncOnClose,
            mkdir,
            batchSize,
            cloudOptions
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkCsvMemory"/>
    public byte[] WriteCsvMemory(
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
        var lf = Lazy();
        return lf.SinkCsvMemory(
            includeBom,
            includeHeader,
            batchSize,
            checkExtension,
            compressionCode,
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
            quoteStyle,
            maintainOrder
        );
    }
    /// <summary>
    /// Write DataFrame to a Parquet file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming, statistics, and cloud storage.
    /// </para>
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="compression">Compression method. Defaults to Snappy.</param>
    /// <param name="compressionLevel">Compression level. -1 means default.</param>
    /// <param name="statistics">Compute and write column statistics. Defaults to false.</param>
    /// <param name="rowGroupSize">Number of rows per row group. 0 means use default.</param>
    /// <param name="dataPageSize">Size of data page in bytes. 0 means use default.</param>
    /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
    public void WriteParquet(
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
        var lf = Lazy();

        lf.SinkParquet(
            path,
            compression,
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,
            maintainOrder,
            syncOnClose,
            mkdir,
            cloudOptions
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkParquetPartitioned"/>
    public void WriteParquetPartitioned(
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
        var lf = Lazy();
        lf.SinkParquetPartitioned(
            path,
            
            // --- Partition Params ---
            partitionBy, 
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile,
            approxBytesPerFile,

            // --- Parquet Options ---
            compression,
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,

            // --- Unified Options ---
            maintainOrder,
            syncOnClose,
            mkdir,
            cloudOptions

        );
    }
    /// <inheritdoc cref="LazyFrame.SinkParquetMemory"/>
    public byte[] WriteParquetMemory(
        ParquetCompression compression = ParquetCompression.ZSTD,
        int compressionLevel = 3, 
        bool statistics = true,
        int rowGroupSize = 0,
        int dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true)
    {
        var lf = Lazy();
        return lf.SinkParquetMemory(
            compression,
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,
            maintainOrder
        );
    }
    /// <summary>
    /// Write DataFrame to an IPC (Arrow/Feather) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming and cloud storage.
    /// </para>
    /// </summary>
    /// <param name="path">The output file path.</param>
    /// <param name="compression">Compression method (None, LZ4, ZSTD). Defaults to None.</param>
    /// <param name="compatLevel">Compatibility level (default -1 = newest).</param>
    /// <param name="recordBatchSize">Number of rows per record batch (0 = default).</param>
    /// <param name="recordBatchStatistics">Write statistics to the record batch header (default = true).</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Create parent directories if they don't exist (Local file system only).</param>
    /// <param name="cloudOptions">Options for cloud storage.</param>
    public void WriteIpc(
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
        var lf = Lazy();

        lf.SinkIpc(
            path,
            compression,
            compatLevel,
            recordBatchSize,
            recordBatchStatistics,
            maintainOrder,
            syncOnClose,
            mkdir,
            cloudOptions
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkIpcPartitioned"/>
    public void WriteIpcPartitioned(
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
        var lf = Lazy();
        lf.SinkIpcPartitioned(
            path,partitionBy,includeKeys,keysPreGrouped,maxRowsPerFile,approxBytesPerFile,
            compression,compatLevel,recordBatchSize,recordBatchStatistics,maintainOrder,syncOnClose,mkdir,cloudOptions
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkIpcMemory(IpcCompression, int, int, bool, bool)"/>
    public byte[] WriteIpcMemory(
        IpcCompression compression = IpcCompression.None,
        int compatLevel = -1,
        int recordBatchSize = 0,
        bool recordBatchStatistics = true,
        bool maintainOrder = true)
    {
        var lf = Lazy();
        return lf.SinkIpcMemory(
            compression,
            compatLevel,
            recordBatchSize,
            recordBatchStatistics,
            maintainOrder
        );
    }
    /// <summary>
    /// Write DataFrame to a JSON file.
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="format">JSON format (Json Array or JsonLines). Defaults to Json.</param>
    public void WriteJson(string path, JsonFormat format = JsonFormat.Json)
    {
        PolarsWrapper.WriteJson(Handle, path, format.ToNative());
    }
    /// <summary>
    /// Writes the DataFrame to a JSON format in memory.
    /// </summary>
    /// <param name="jsonFormat">The JSON format to use (Json or JsonLines).</param>
    /// <returns>A byte array containing the JSON data.</returns>
    public byte[] WriteJsonMemory(JsonFormat jsonFormat = JsonFormat.Json)
    {
        return PolarsWrapper.WriteJsonMemory(
            Handle,
            jsonFormat.ToNative()
        );
    }
    /// <inheritdoc cref="LazyFrame.SinkJsonPartitioned"/>
    public void WriteJsonPartitioned(
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
        var lf = Lazy();
        lf.SinkJsonPartitioned(
            path,partitionBy,includeKeys,keysPreGrouped,maxRowsPerFile,approxBytesPerFile,
            compression,compressionLevel,checkExtension,maintainOrder,syncOnClose,mkdir,cloudOptions
        );
    }

    /// <summary>
    /// Write DataFrame to a Newline Delimited JSON (NDJSON) file.
    /// <para>
    /// This uses the Lazy execution engine internally to support streaming, compression, and cloud storage.
    /// </para>
    /// </summary>
    /// <param name="path">Output file path.</param>
    /// <param name="compression">Compression method (Gzip/Zstd). Defaults to None.</param>
    /// <param name="compressionLevel">Compression level. -1 for default.</param>
    /// <param name="checkExtension">Whether to check if the file extension matches '.json' or '.ndjson'.</param>
    /// <param name="maintainOrder">Maintain the order of data.</param>
    /// <param name="syncOnClose">Sync to disk on close.</param>
    /// <param name="mkdir">Create parent directories.</param>
    /// <param name="cloudOptions">Cloud storage options.</param>
    public void WriteNdJson(
        string path,
        ExternalCompression compression = ExternalCompression.Uncompressed,
        int compressionLevel = -1,
        bool checkExtension = true,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var lf = Lazy();

        lf.SinkJson(
            path,
            compression,
            compressionLevel,
            checkExtension,
            maintainOrder,
            syncOnClose,
            mkdir,
            cloudOptions
        );
    }
    // ---------------------------------------------------------
    // Write Excel (Native)
    // ---------------------------------------------------------

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
    /// <summary>
    /// Write the DataFrame to an Apache Avro file.
    /// </summary>
    /// <param name="path">The file path to write to.</param>
    /// <param name="compression">The compression algorithm to use.</param>
    /// <param name="name">The name of the Avro record.</param>
    public void WriteAvro(
        string path, 
        AvroCompression compression = AvroCompression.Uncompressed, 
        string name = "")
    {
        PolarsWrapper.WriteAvro(this.Handle, path, compression.ToNative(), name);
    }
    /// <summary>
    /// Write the DataFrame to an Apache Avro memory buffer.
    /// </summary>
    /// <param name="compression">The compression algorithm to use.</param>
    /// <param name="name">The name of the Avro record.</param>
    /// <returns>A byte array containing the Avro data.</returns>
    public byte[] WriteAvroMemory(
        AvroCompression compression = AvroCompression.Uncompressed, 
        string name = "")
    {
        return PolarsWrapper.WriteAvroToMemory(Handle, compression.ToNative(), name);
    }
    /// <summary>
    /// Write a DataFrame into a delta table
    /// </summary>
    /// <inheritdoc cref="LazyFrame.SinkDelta(string, Selector?, DeltaSaveMode, bool, bool, bool, int, long, ParquetCompression, int, bool, uint, uint, int, bool, SyncOnClose, bool, CloudOptions?)"/>
    public void WriteDelta(
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
        var lf = Lazy();
        lf.SinkDelta(
            path,
            partitionBy,
            // --- Delta Options ---
            mode, 
            canEvolve,
            // --- Partition Params ---
            
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile,
            approxBytesPerFile,

            // --- Parquet Options ---
            compression,
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel, 

            // --- Unified Options ---
            maintainOrder,
            syncOnClose,
            mkdir,

            // --- Cloud Params ---
            cloudOptions
        );
    }
    /// <inheritdoc cref="WriteDelta(string, Selector?, DeltaSaveMode, bool, bool, bool, int, long, ParquetCompression, int, bool, uint, uint, int, bool, SyncOnClose, bool, CloudOptions?)"/>
    public void WriteDelta(
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

        WriteDelta(
            path, selector, mode, canEvolve, includeKeys, keysPreGrouped, maxRowsPerFile, 
            approxBytesPerFile, compression, compressionLevel, statistics, rowGroupSize, 
            dataPageSize, compatLevel, maintainOrder, syncOnClose, mkdir, cloudOptions
        );
    }

    /// <inheritdoc cref="WriteDelta(string, Selector?, DeltaSaveMode, bool, bool, bool, int, long, ParquetCompression, int, bool, uint, uint, int, bool, SyncOnClose, bool, CloudOptions?)"/>
    public void WriteDelta(
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
            => WriteDelta(
                path, [partitionBy], mode, canEvolve, includeKeys, keysPreGrouped, 
                maxRowsPerFile, approxBytesPerFile, compression, compressionLevel, statistics, 
                rowGroupSize, dataPageSize, compatLevel, maintainOrder, syncOnClose, mkdir, cloudOptions
            );

    /// <summary>
    /// Merge a DataFrame into a Delta Lake table with full SQL MERGE semantics.
    /// Provides fine-grained control over Update, Insert, and Delete behaviors.
    /// Notice: In this method, Delete > Update > Insert > Ignore.
    /// If you need other orders, please use LazyFrame.MergeDeltaOrdered
    /// </summary>
    /// <inheritdoc cref="LazyFrame.MergeDelta(string, string[], Expr?, Expr?, Expr?, Expr?, bool, CloudOptions?)"/>
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
        var lf = Lazy();
        lf.MergeDelta(
            path,
            mergeKeys,
            matchedUpdateCond,
            matchedDeleteCond,
            notMatchedInsertCond,
            notMatchedBySourceDeleteCond,
            canEvolve,
            cloudOptions
        );
    }
    /// <summary>
    /// Starts a fluent builder to merge a DataFrame into a Delta Lake table with strict, order-preserving SQL MERGE semantics.
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
    /// df.MergeDeltaOrdered("s3://bucket/my_table", new[] { "Id" })
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
        return new DeltaMergeBuilder(Lazy(), path, mergeKeys, canEvolve, cloudOptions);
    }
        /// <summary>
    /// Generate DataFrame from ADBC query results
    /// </summary>
    /// <param name="statement"></param>
    /// <returns></returns>
    /// <exception cref="InvalidOperationException"></exception>
    public static DataFrame ReadAdbc(AdbcStatement statement)
    {
        ArgumentNullException.ThrowIfNull(statement);

        var result = statement.ExecuteQuery();

        if (result.Stream == null)
        {
            throw new InvalidOperationException("ADBC query executed, but returned a null Arrow stream.");
        }

        return FromArrowStream(result.Stream);
    }
    /// <summary>
    /// Executes a SQL query directly against an ADBC connection and reads the result into a zero-copy Polars DataFrame.
    /// Pure syntactic sugar: automatically manages the creation and disposal of the underlying AdbcStatement.
    /// </summary>
    /// <param name="connection">The active ADBC connection (e.g., DuckDB, SQLite).</param>
    /// <param name="sqlQuery">The SQL query string to execute.</param>
    /// <returns>A fully materialized Polars DataFrame containing the query results.</returns>
    public static DataFrame ReadAdbc(AdbcConnection connection, string sqlQuery)
    {
        ArgumentNullException.ThrowIfNull(connection);
        if (string.IsNullOrWhiteSpace(sqlQuery))
            throw new ArgumentException("SQL query cannot be null or whitespace.", nameof(sqlQuery));

        // Since Polars synchronously materializes the entire stream during FromArrowStream,
        // it is perfectly safe to dispose the statement immediately after the read completes.
        using AdbcStatement statement = connection.CreateStatement();
        statement.SqlQuery = sqlQuery;

        // Route to the core execution method
        return ReadAdbc(statement);
    }
    /// <summary>
    /// Zero-copy bulk ingest of the current DataFrame into an ADBC database (e.g., DuckDB, SQLite).
    /// </summary>
    /// <param name="statement">An AdbcStatement configured with ingest options (e.g., target table).</param>
    /// <returns>The UpdateResult containing the number of rows affected.</returns>
    public UpdateResult WriteToAdbc(AdbcStatement statement)
    {
        ArgumentNullException.ThrowIfNull(statement);

        try
        {
            // Delegate all unsafe pointer handling, FFI bindings, and execution to the Core layer.
            // This ensures no raw pointers leak into the managed high-level API.
            return AdbcInterop.ExecuteIngest(statement, Handle);
        }
        finally
        {
            // Crucial: Pin the DataFrame to prevent the Garbage Collector from 
            // reclaiming the underlying Rust memory while the ADBC C++ engine is actively pulling data.
            GC.KeepAlive(this);
        }
    }
    /// <summary>
    /// Zero-copy bulk ingest of the current DataFrame into an ADBC database table.
    /// Pure syntactic sugar: automatically manages the creation, configuration, and disposal of the underlying AdbcStatement.
    /// </summary>
    /// <param name="connection">The active ADBC connection (e.g., DuckDB, SQLite).</param>
    /// <param name="tableName">The name of the target table to ingest data into.</param>
    /// <param name="ingestMode">The ingestion mode (e.g., "adbc.ingest.mode.create" or "adbc.ingest.mode.append"). Defaults to create.</param>
    /// <returns>The UpdateResult containing the number of rows affected.</returns>
    public UpdateResult WriteToAdbc(AdbcConnection connection, string tableName,AdbcIngestMode ingestMode = AdbcIngestMode.Create)
    {
        ArgumentNullException.ThrowIfNull(connection);
        if (string.IsNullOrWhiteSpace(tableName))
            throw new ArgumentException("Target table name cannot be null or whitespace.", nameof(tableName));

        // Let the framework handle the Statement lifecycle
        using AdbcStatement statement = connection.CreateStatement();
        
        // Configure ADBC bulk ingest options automatically
        statement.SetOption("adbc.ingest.target_table", tableName);
        
        string modeString = ingestMode switch
        {
            AdbcIngestMode.Create  => "adbc.ingest.mode.create",
            AdbcIngestMode.Append  => "adbc.ingest.mode.append",
            AdbcIngestMode.Replace => "adbc.ingest.mode.replace",
            _ => throw new ArgumentOutOfRangeException(nameof(ingestMode), $"Unsupported ingest mode: {ingestMode}")
        };

        statement.SetOption("adbc.ingest.mode", modeString);

        return WriteToAdbc(statement);
    }
}