#nowarn "64"
#nowarn "44"

namespace Polars.FSharp

open Polars.NET.Core
open System
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
        /// <param name="canEvolve">Allow schema evolution during the merge.</param>
        /// <param name="cloudOptions">Cloud storage credentials and configuration.</param>
        [<Obsolete("This method is deprecated because its execution order of matched/not-matched actions is hardcoded and may lead to silent data corruption in complex scenarios. Please use 'MergeDeltaOrdered(...)' combined with the '.WhenMatched...()' chaining methods to ensure strict SQL MERGE semantics.")>]
        member this.MergeDelta(
            path: string,
            mergeKeys: seq<string>,
            ?matchedUpdateCond: Expr,
            ?matchedDeleteCond: Expr,
            ?notMatchedInsertCond: Expr,
            ?notMatchedBySourceDeleteCond: Expr,
            ?canEvolve: bool,
            ?cloudOptions: CloudOptions
        ) =
            // 1. Resolve Defaults & Sequences
            let pEvolve = defaultArg canEvolve false
            let keysArr = mergeKeys |> Seq.toArray

            // 2. Parse Cloud Options
            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
                CloudOptions.ParseCloudOptions cloudOptions

            // 3. Clone Handles (safely disposing them at the end of the scope)
            use clonedLf = this.CloneHandle()
            
            use hUpdate = 
                match matchedUpdateCond with
                | Some e -> e.CloneHandle()
                | None -> null
                
            use hDelete = 
                match matchedDeleteCond with
                | Some e -> e.CloneHandle()
                | None -> null
                
            use hInsert = 
                match notMatchedInsertCond with
                | Some e -> e.CloneHandle()
                | None -> null
                
            use hSrcDelete = 
                match notMatchedBySourceDeleteCond with
                | Some e -> e.CloneHandle()
                | None -> null

            // 4. Native Call
            PolarsWrapper.DeltaMerge(
                clonedLf,
                path,
                keysArr,
                hUpdate,
                hDelete,
                hInsert,
                hSrcDelete,
                pEvolve,
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
        /// <summary>
        /// Merge a DataFrame into a Delta Lake table with full SQL MERGE semantics.
        /// Provides fine-grained control over Update, Insert, and Delete behaviors.
        /// Notice: In this method, Delete > Update > Insert > Ignore.
        /// If you need other orders, please use DataFrame.MergeDeltaOrdered
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
        /// <param name="canEvolve">Allow schema evolution during the merge.</param>
        /// <param name="cloudOptions">Cloud storage credentials and configuration.</param>
        [<Obsolete("This method is deprecated because its execution order of matched/not-matched actions is hardcoded and may lead to silent data corruption in complex scenarios. Please use 'MergeDeltaOrdered(...)' combined with the '.WhenMatched...()' chaining methods to ensure strict SQL MERGE semantics.")>]
        member this.MergeDelta(
            path: string,
            mergeKeys: seq<string>,
            ?matchedUpdateCond: Expr,
            ?matchedDeleteCond: Expr,
            ?notMatchedInsertCond: Expr,
            ?notMatchedBySourceDeleteCond: Expr,
            ?canEvolve: bool,
            ?cloudOptions: CloudOptions
        ) =
            this.Lazy().MergeDelta(
                path,
                mergeKeys,
                ?matchedUpdateCond=matchedUpdateCond,
                ?matchedDeleteCond=matchedDeleteCond,
                ?notMatchedInsertCond=notMatchedInsertCond,
                ?notMatchedBySourceDeleteCond=notMatchedBySourceDeleteCond,
                ?canEvolve=canEvolve,
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