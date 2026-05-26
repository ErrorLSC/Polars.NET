using Cs = Polars.CSharp.Polars.Selectors;

using Polars.NET.Core;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{

    /// <summary>
    /// Read a delta table into a new DataFrame
    /// </summary>
    /// <inheritdoc cref="LazyFrame.ScanDelta(string, ulong?, string?, ulong?, ParallelStrategy, bool, bool, bool, bool, bool, string?, uint, string?, IntoSchema?, bool, IntoSchema?, bool, CloudOptions?)"/>
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
    /// Write a DataFrame into a delta table
    /// </summary>
    /// <inheritdoc cref="LazyFrame.SinkDelta(string, IntoSelector?, DeltaSaveMode, bool, bool, bool, int, long, ParquetCompression, int, bool, uint, uint, int, bool, SyncOnClose, bool, CloudOptions?)"/>
    public void WriteDelta(
        string path,
        IntoSelector? partitionBy = null,
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

 
}