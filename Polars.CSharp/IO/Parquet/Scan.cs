using Polars.NET.Core;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable,IPolarsLazyFrame
{
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
        IntoSchema? schema = null,
        bool hivePartitioning = false,
        IntoSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false,
        CloudOptions? cloudOptions = null) 
    {
        var schemaHandle = schema?.Consume().Handle;
        var hiveSchemaHandle = hivePartitionSchema?.Consume().Handle;

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
        IntoSchema? schema = null,
        bool hivePartitioning = false,
        IntoSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = false)
    {
        var schemaHandle = schema?.Consume().Handle;
        var hiveSchemaHandle = hivePartitionSchema?.Consume().Handle;

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
}