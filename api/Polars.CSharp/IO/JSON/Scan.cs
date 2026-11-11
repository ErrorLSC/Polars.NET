using Polars.NET.Core.Wrappers;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable
{
    /// <summary>
    /// Lazily read a newline delimited JSON file (NDJSON).
    /// </summary>
    /// <param name="path">Path to the NDJSON file.</param>
    /// <param name="schema">
    /// Manually specify schema for specific columns (Overwrite semantics).
    /// Columns not specified will be inferred.
    /// </param>
    /// <param name="inferSchemaLength">Rows to infer the schema from (100 by default). Null or 0 infers it from the whole input, read into memory first (slow); values above 16,777,216 do the same.</param>
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
        IntoSchema? schema = null,
        ulong? inferSchemaLength = 100,
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
        var schemaHandle = schema?.Consume().Handle;

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
    /// <remarks>The engine copies the bytes.</remarks>
    public static LazyFrame ScanNdjson(
        byte[] buffer,
        IntoSchema? schema = null,
        ulong? inferSchemaLength = 100,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0)
        => ScanNdjsonCore(NativeBytes.Copy(buffer), schema, inferSchemaLength, batchSize, nRows, lowMemory, rechunk, ignoreErrors, rowIndexName, rowIndexOffset);

    /// <summary>
    /// Lazily read NDJSON from an in-memory byte array.
    /// </summary>
    /// <remarks>The engine copies the bytes.</remarks>
    public static LazyFrame ScanNdjson(
        ReadOnlySpan<byte> buffer,
        IntoSchema? schema = null,
        ulong? inferSchemaLength = 100,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0)
        => ScanNdjsonCore(NativeBytes.Copy(buffer), schema, inferSchemaLength, batchSize, nRows, lowMemory, rechunk, ignoreErrors, rowIndexName, rowIndexOffset);

    /// <summary>
    /// Lazily read NDJSON from an in-memory byte array.
    /// </summary>
    /// <remarks>The engine reads the memory in place, without a copy: the memory stays pinned until the last frame reading it is released, and must not change meanwhile. Wrap unmanaged memory in a <see cref="System.Buffers.MemoryManager{T}"/> and free it only once the engine unpins it (<see cref="System.Buffers.MemoryManager{T}.Unpin"/>): a query can still be finishing on a background thread right after its frames are released.</remarks>
    public static LazyFrame ScanNdjson(
        ReadOnlyMemory<byte> buffer,
        IntoSchema? schema = null,
        ulong? inferSchemaLength = 100,
        ulong? batchSize = null,
        ulong? nRows = null,
        bool lowMemory = false,
        bool rechunk = false,
        bool ignoreErrors = false,
        string? rowIndexName = null,
        uint rowIndexOffset = 0)
        => ScanNdjsonCore(NativeBytes.Share(buffer), schema, inferSchemaLength, batchSize, nRows, lowMemory, rechunk, ignoreErrors, rowIndexName, rowIndexOffset);

    private static LazyFrame ScanNdjsonCore(
        NativeBytes buffer,
        IntoSchema? schema,
        ulong? inferSchemaLength,
        ulong? batchSize,
        ulong? nRows,
        bool lowMemory,
        bool rechunk,
        bool ignoreErrors,
        string? rowIndexName,
        uint rowIndexOffset)
    {
        var schemaHandle = schema?.Consume().Handle;

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
        IntoSchema? schema = null,
        ulong? inferSchemaLength = 100,
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