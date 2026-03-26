using Polars.NET.Core;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable,IPolarsLazyFrame
{
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
}