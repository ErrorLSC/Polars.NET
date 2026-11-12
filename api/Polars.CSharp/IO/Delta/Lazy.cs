#pragma warning disable CS1573

using Polars.NET.Core.Wrappers;

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
    /// <inheritdoc cref="ScanParquet(string, ulong?, ParallelStrategy, bool, bool, bool, bool, bool, bool, string?, uint, string?, IntoSchema?, bool, IntoSchema?, bool, CloudOptions?)"/>
    /// <param name="schema">Return these columns of the table, cast to these types; a column the table does not have is an error.</param>
    /// <param name="glob">Ignored since 1.2.0 (the managed Delta kernel takes the data files from the log); removed in 2.0.</param>
    /// <param name="hivePartitioning">Ignored since 1.2.0 (the managed Delta kernel takes partition values from the log, not from paths); removed in 2.0.</param>
    /// <param name="hivePartitionSchema">Ignored since 1.2.0 (the managed Delta kernel types partition values by the table schema); removed in 2.0.</param>
    /// <param name="tryParseHiveDates">Ignored since 1.2.0 (the managed Delta kernel types partition values by the table schema); removed in 2.0.</param>
    /// <param name="rowIdColumn">Adds each row's row id (Int64) under this name: stable across updates, merges and
    /// optimize (Spark's <c>_metadata.row_id</c>). The table must have row tracking on (<c>delta.enableRowTracking</c>).</param>
    /// <param name="rowCommitVersionColumn">Adds the version that last changed each row (Int64) under this name
    /// (Spark's <c>_metadata.row_commit_version</c>). The table must have row tracking on.</param>
    /// <param name="variantAsJson">Reads variant columns (at any depth) as JSON text, as Spark's <c>to_json</c> renders them,
    /// instead of as stored (a struct of the <c>value</c> and <c>metadata</c> binaries). Timestamps in variants are rendered in UTC.
    /// A table with variant columns is read through Polars.NET's own patch: every file the scan selects is read into memory
    /// when the scan is created and kept until the frame is released (no ranged reads, no column pruning).</param>
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
        CloudOptions? cloudOptions = null,
        string? rowIdColumn = null,
        string? rowCommitVersionColumn = null,
        bool variantAsJson = false)
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
            values,
            rowIdColumn,
            rowCommitVersionColumn,
            variantAsJson
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
    /// <param name="includeKeys">Ignored since 1.2.0 (the managed Delta kernel never writes partition columns into data files: Delta keeps them in the log); removed in 2.0.</param>
    /// <param name="keysPreGrouped">
    /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
    /// </param>
    /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
    /// <param name="approxBytesPerFile">Approximate in-memory size of the rows of one file, as the engine estimates it (files on disk are smaller, being compressed). 0: the table's delta.targetFileSize, else 128 MB.</param>
    /// <param name="compression">Compression codec to use (Snappy, Zstd, etc.).</param>
    /// <param name="compressionLevel">Compression level (depends on the codec).</param>
    /// <param name="statistics">Ignored since 1.2.0 (the managed Delta kernel always writes statistics: data skipping reads them; the table properties delta.dataSkippingNumIndexedCols / delta.dataSkippingStatsColumns choose the columns); removed in 2.0.</param>
    /// <param name="rowGroupSize">Target row group size (in rows).</param>
    /// <param name="dataPageSize">Target data page size (in bytes).</param>
    /// <param name="compatLevel">Ignored since 1.2.0 (the managed Delta kernel writes Parquet data files, to which the Arrow compatibility level does not apply); removed in 2.0.</param>
    /// <param name="maintainOrder">Maintain the order of the data.</param>
    /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
    /// <param name="mkdir">Ignored since 1.2.0 (the managed Delta kernel always creates the table directory); removed in 2.0.</param>
    /// <param name="cloudOptions">Options for cloud storage authentication and configuration.</param>
    /// <param name="clusterBy">Liquid clustering columns (dotted for nested fields, at most four) of a table this
    /// write creates; <see cref="Delta.Optimize"/> then clusters its data by them. On an existing table they must be
    /// its clustering columns (change them with <see cref="Delta.ClusterBy"/>). Cannot be combined with
    /// <paramref name="partitionBy"/>.</param>
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
        CloudOptions? cloudOptions = null,
        IEnumerable<string>? clusterBy = null)
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
            values,
            clusterBy?.ToArray()
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
    /// lf.MergeDeltaOrdered("s3://bucket/my_table", ["Id"])
    ///   .WhenMatchedDelete(m => m.Source("Status") == "Deleted") // Evaluated 1st
    ///   .WhenMatchedUpdate(m => m.Source("Stock") > m.Target("Stock")) // Evaluated 2nd
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

    /// <summary>
    /// Updates the target rows that match a row of this frame on the merge keys, with that row's values
    /// (SQL <c>MERGE ... WHEN MATCHED UPDATE</c>). Unmatched rows of this frame are ignored: nothing is inserted.
    /// </summary>
    /// <param name="path">The URI to the target Delta Lake table (local or cloud).</param>
    /// <param name="mergeKeys">The columns that identify a row, present in both this frame and the table.</param>
    /// <param name="condition">Optional: only matched rows satisfying it are updated. Evaluated as in <c>WhenMatchedUpdate</c>.</param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration.</param>
    /// <remarks>
    /// Identity columns cannot be updated, so this frame must not carry one. For a table keyed by an identity column,
    /// use <see cref="Delta.Update(string, Expr?, IReadOnlyDictionary{string, Expr}, CloudOptions?)"/> with a predicate.
    /// </remarks>
    public void UpdateDelta(
        string path,
        string[] mergeKeys,
        Expr? condition = null,
        CloudOptions? cloudOptions = null)
    {
        var builder = MergeDeltaOrdered(path, mergeKeys, false, cloudOptions);
        (condition is null ? builder.WhenMatchedUpdate() : builder.WhenMatchedUpdate(condition)).Execute();
    }

    /// <summary>
    /// Updates the target rows that match a row of this frame on the merge keys, with that row's values, where a
    /// condition over both sides holds: <c>m =&gt; m.Source("Version") &gt; m.Target("Version")</c>.
    /// </summary>
    /// <param name="path">The URI to the target Delta Lake table (local or cloud).</param>
    /// <param name="mergeKeys">The columns that identify a row, present in both this frame and the table.</param>
    /// <param name="condition">Builds the condition; <c>m.Source(name)</c> is this frame's column, <c>m.Target(name)</c> the table's.</param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration.</param>
    public void UpdateDelta(
        string path,
        string[] mergeKeys,
        Func<MergeContext, Expr> condition,
        CloudOptions? cloudOptions = null)
    {
        ArgumentNullException.ThrowIfNull(condition);
        MergeDeltaOrdered(path, mergeKeys, false, cloudOptions).WhenMatchedUpdate(condition).Execute();
    }
}