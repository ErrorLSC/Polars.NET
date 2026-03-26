using Polars.NET.Core;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable,IPolarsLazyFrame
{
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
}