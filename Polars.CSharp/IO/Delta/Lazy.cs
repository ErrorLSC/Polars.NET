#pragma warning disable CS1573

using Polars.NET.Core;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable
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
    /// <inheritdoc cref="LazyFrame.ScanParquet(string, ulong?, ParallelStrategy, bool, bool, bool, bool, bool, bool, string?, uint, string?, IntoSchema?, bool, IntoSchema?, bool, CloudOptions?)"/>
    /// <returns>A new LazyFrame.</returns>
    public static LazyFrame ScanDelta(
        string path,
        ulong? version = null,
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
        IntoSchema? schema = null,
        bool hivePartitioning = true,
        IntoSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = true,
        CloudOptions? cloudOptions = null)
    {
        if (version.HasValue && datetime != null)
        {
            throw new ArgumentException("Cannot specify both 'version' and 'datetime' for Delta Time Travel.");
        }

        var schemaHandle = schema?.Consume().Handle;
        var hiveSchemaHandle = hivePartitionSchema?.Consume().Handle;

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
        IntoSelector? partitionBy = null,
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
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) =
            CloudOptions.ParseCloudOptions(cloudOptions);
        using var partitionByH = partitionBy?.Consume().Handle;
        PolarsWrapper.SinkDelta(
            CloneHandle(),
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