#pragma warning disable CS1573

using Polars.NET.Core;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable,IPolarsLazyFrame
{
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