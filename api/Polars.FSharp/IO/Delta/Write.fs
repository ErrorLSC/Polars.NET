#nowarn "64"
#nowarn "44"

namespace Polars.FSharp

open Polars.NET.Core.Wrappers
open System.Runtime.CompilerServices

[<AutoOpen>]
module DeltaWrite =
    type LazyFrame with
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
        member this.SinkDelta(
            path: string,
            ?partitionBy: Selector,
            ?mode: DeltaSaveMode,
            ?canEvolve: bool,
            ?includeKeys: bool,
            ?keysPreGrouped: bool,
            ?maxRowsPerFile: int,
            ?approxBytesPerFile: int64,
            ?compression: ParquetCompression,
            ?compressionLevel: int,
            ?statistics: bool,
            ?rowGroupSize: uint32,
            ?dataPageSize: uint32,
            ?compatLevel: int,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions
        ) =
            // 1. Resolve Defaults
            let pMode = defaultArg mode DeltaSaveMode.Append
            let pEvolve = defaultArg canEvolve false
            let pIncKeys = defaultArg includeKeys true
            let pPreGrouped = defaultArg keysPreGrouped false
            let pMaxRows = defaultArg maxRowsPerFile 0
            let pApproxBytes = defaultArg approxBytesPerFile 0L
            
            let pComp = defaultArg compression ParquetCompression.Snappy
            let pCompLevel = defaultArg compressionLevel -1
            let pStats = defaultArg statistics true
            let pRowGrpSz = defaultArg rowGroupSize 0u
            let pDataPgSz = defaultArg dataPageSize 0u
            let pCompat = defaultArg compatLevel -1
            
            let pMaintain = defaultArg maintainOrder true
            let pSync = defaultArg syncOnClose SyncOnClose.NoSync
            let pMkdir = defaultArg mkdir false

            // 2. Type Conversions for Limits (handling zero-checks safely)
            let maxRowsNuint = if pMaxRows > 0 then unativeint pMaxRows else 0un
            let approxBytesUlong = if pApproxBytes > 0L then uint64 pApproxBytes else 0UL
            let rowGrpSzNuint = unativeint pRowGrpSz
            let dataPgSzNuint = unativeint pDataPgSz

            // 3. Unpack Cloud Options
            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
                CloudOptions.ParseCloudOptions cloudOptions

            // 4. Safe Handle Binding for Optional Selector
            use partitionByH = 
                match partitionBy with
                | Some s -> s.CloneHandle()
                | None -> null

            // 5. Native Call
            PolarsWrapper.SinkDelta(
                this.CloneHandle(),
                path,
                
                // --- Delta Options ---
                pMode.ToNative(),
                pEvolve,
                
                // --- Partition Params ---
                partitionByH,
                pIncKeys,
                pPreGrouped,
                maxRowsNuint,
                approxBytesUlong,
                
                // --- Parquet Options ---
                pComp.ToNative(),
                pCompLevel,
                pStats,
                rowGrpSzNuint,  
                dataPgSzNuint,
                pCompat,
                
                // --- Unified Options ---
                pMaintain,
                pSync.ToNative(),
                pMkdir,
                
                // --- Cloud Params ---
                cProv,
                cRet,
                cToMs,
                cInitMs,
                cMaxMs,
                cCache,
                cKeys,
                cVals
            )
    type DataFrame with
        /// <summary>
        /// Write the DataFrame to a Delta Lake table with partition discovery.
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
        member this.WriteDelta(
            path: string,
            ?partitionBy: Selector,
            ?mode: DeltaSaveMode,
            ?canEvolve: bool,
            ?includeKeys: bool,
            ?keysPreGrouped: bool,
            ?maxRowsPerFile: int,
            ?approxBytesPerFile: int64,
            ?compression: ParquetCompression,
            ?compressionLevel: int,
            ?statistics: bool,
            ?rowGroupSize: uint32,
            ?dataPageSize: uint32,
            ?compatLevel: int,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions
        ) =
            this.Lazy().SinkDelta(
                path,
                
                // --- Delta Options ---
                ?partitionBy=partitionBy,
                ?mode=mode,
                ?canEvolve=canEvolve,
                // --- Partition Params ---
                ?includeKeys=includeKeys,
                ?keysPreGrouped=keysPreGrouped,
                ?maxRowsPerFile=maxRowsPerFile,
                ?approxBytesPerFile=approxBytesPerFile,
                
                // --- Parquet Options ---
                ?compression=compression,
                ?compressionLevel=compressionLevel,
                ?statistics=statistics,
                ?rowGroupSize=rowGroupSize,  
                ?dataPageSize=dataPageSize,
                ?compatLevel=compatLevel,
                
                // --- Unified Options ---
                ?maintainOrder=maintainOrder,
                ?syncOnClose=syncOnClose,
                ?mkdir=mkdir,
                
                // --- Cloud Params ---
                ?cloudOptions=cloudOptions
            )

[<Extension>]
type LazyFrameDeltaExtensions =
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
    [<Extension>]
    static member MergeDeltaOrdered(
        this: LazyFrame,
        path: string,
        mergeKeys: seq<string>,
        ?canEvolve: bool,
        ?cloudOptions: CloudOptions
    ) : DeltaMergeBuilder =
        let keysArr = mergeKeys |> Seq.toArray
        let evolve = defaultArg canEvolve false
        new DeltaMergeBuilder(this, path, keysArr, evolve, cloudOptions)

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
    /// use <c>Delta.Update</c> with a predicate.
    /// </remarks>
    [<Extension>]
    static member UpdateDelta(
        this: LazyFrame,
        path: string,
        mergeKeys: seq<string>,
        ?condition: Expr,
        ?cloudOptions: CloudOptions
    ) : unit =
        let keysArr = mergeKeys |> Seq.toArray
        let builder = new DeltaMergeBuilder(this, path, keysArr, false, cloudOptions)
        builder.WhenMatchedUpdate(?condition = condition).Execute()

[<Extension>]
type DataFrameDeltaExtensions =
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
    [<Extension>]
    static member MergeDeltaOrdered(
        this: DataFrame,
        path: string,
        mergeKeys: seq<string>,
        ?canEvolve: bool,
        ?cloudOptions: CloudOptions
    ) : DeltaMergeBuilder =
        let keysArr = mergeKeys |> Seq.toArray
        let evolve = defaultArg canEvolve false
        new DeltaMergeBuilder(this.Lazy(), path, keysArr, evolve, cloudOptions)

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
    /// use <c>Delta.Update</c> with a predicate.
    /// </remarks>
    [<Extension>]
    static member UpdateDelta(
        this: DataFrame,
        path: string,
        mergeKeys: seq<string>,
        ?condition: Expr,
        ?cloudOptions: CloudOptions
    ) : unit =
        let keysArr = mergeKeys |> Seq.toArray
        let builder = new DeltaMergeBuilder(this.Lazy(), path, keysArr, false, cloudOptions)
        builder.WhenMatchedUpdate(?condition = condition).Execute()
