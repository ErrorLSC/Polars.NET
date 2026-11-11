using Polars.NET.Core;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable, IEnumerable<Series>, IPolarsDataFrame
{

    /// <summary>
    /// Read a delta table into a new DataFrame
    /// </summary>
    /// <inheritdoc cref="LazyFrame.ScanDelta(string, ulong?, string?, ulong?, ParallelStrategy, bool, bool, bool, bool, bool, string?, uint, string?, IntoSchema?, bool, IntoSchema?, bool, CloudOptions?, string?, string?)"/>
    public static DataFrame ReadDelta(
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
        string? rowCommitVersionColumn = null)
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
            cloudOptions,
            rowIdColumn,
            rowCommitVersionColumn
        );

        return lf.Collect();
    }

    /// <summary>
    /// Write a DataFrame into a delta table
    /// </summary>
    /// <inheritdoc cref="LazyFrame.SinkDelta(string, IntoSelector?, DeltaSaveMode, bool, bool, bool, int, long, ParquetCompression, int, bool, uint, uint, int, bool, SyncOnClose, bool, CloudOptions?)"/>
    public void WriteDelta(
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
        return new DeltaMergeBuilder(Lazy(), path, mergeKeys, canEvolve, cloudOptions);
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