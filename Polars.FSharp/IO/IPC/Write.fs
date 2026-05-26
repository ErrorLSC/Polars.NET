namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module IPCWrite =
    type LazyFrame with
       /// <summary>
        /// Sink the LazyFrame to an IPC (Arrow) file.
        /// <para>
        /// This allows for streaming execution.
        /// </para>
        /// </summary>
        /// <param name="path">Output file path.</param>
        /// <param name="compression">Compression method (NoCompression, LZ4, ZSTD). Defaults to NoCompression.</param>
        /// <param name="compatLevel">Arrow compatibility level. -1 means newest. Defaults to -1.</param>
        /// <param name="recordBatchSize">Number of rows per record batch (0 = default).</param>
        /// <param name="recordBatchStatistics">Write statistics to the record batch header. Defaults to true.</param>
        /// <param name="maintainOrder">Whether to maintain the order of the data. Defaults to true.</param>
        /// <param name="syncOnClose">File synchronization behavior on close. Defaults to None.</param>
        /// <param name="mkdir">Recursively create the directory if it does not exist. Defaults to false.</param>
        /// <param name="cloudOptions">Options for cloud storage.</param>
        member this.SinkIpc(
            path: string, 
            ?compression: IpcCompression, 
            ?compatLevel: int,
            ?recordBatchSize: int,
            ?recordBatchStatistics: bool,
            ?maintainOrder: bool, 
            ?syncOnClose: SyncOnClose, 
            ?mkdir: bool,
            ?cloudOptions: CloudOptions
        ) =
            // 1. Resolve Defaults
            let comp = defaultArg compression IpcCompression.NoCompression
            let compat = defaultArg compatLevel -1
            let batchSize = defaultArg recordBatchSize 0
            let batchStats = defaultArg recordBatchStatistics true
            let mo = defaultArg maintainOrder true
            let sync = defaultArg syncOnClose SyncOnClose.NoSync
            let mkd = defaultArg mkdir false

            // 2. Unpack Cloud Options via Helper
            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
                CloudOptions.ParseCloudOptions cloudOptions

            // 3. Native Call
            PolarsWrapper.SinkIpc(
                this.CloneHandle(), 
                path,
                comp.ToNative(),
                compat,
                batchSize,
                batchStats,
                mo,
                sync.ToNative(),
                mkd,
                // Cloud
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
        /// Sink the LazyFrame to an IPC (Arrow) file, partitioned by the given selector.
        /// </summary>
        /// <param name="partitionBy">The selector(s) to partition the data by.</param>
        /// <param name="includeKeys">Whether to include the partition keys in the output files.</param>
        /// <param name="keysPreGrouped">
        /// Assert that the keys are already pre-grouped. This can speed up the operation if true.
        /// Use with caution: if the data is not grouped, the output may be incorrect.
        /// </param>
        /// <param name="maxRowsPerFile">Maximum number of rows per file. 0 means no limit.</param>
        /// <param name="approxBytesPerFile">Approximate size in bytes per file. 0 means no limit.</param>
        member this.SinkIpcPartitioned(
            path: string,
            partitionBy: Selector,
            ?includeKeys: bool,
            ?keysPreGrouped: bool,
            ?maxRowsPerFile: int,
            ?approxBytesPerFile: int64,
            ?compression: IpcCompression,
            ?compatLevel: int,
            ?recordBatchSize: int,
            ?recordBatchStatistics: bool,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions
        ) =
            // 1. Resolve Defaults
            let incKeys = defaultArg includeKeys true
            let preGrouped = defaultArg keysPreGrouped false
            let maxRows = defaultArg maxRowsPerFile 0
            let approxBytes = defaultArg approxBytesPerFile 0L
            let comp = defaultArg compression IpcCompression.NoCompression
            let compat = defaultArg compatLevel -1
            let batchSize = defaultArg recordBatchSize 0
            let batchStats = defaultArg recordBatchStatistics true
            let mo = defaultArg maintainOrder true
            let sync = defaultArg syncOnClose SyncOnClose.NoSync
            let mkd = defaultArg mkdir false

            // 2. Type Conversions for Limits
            let maxRowsNuint = if maxRows > 0 then unativeint maxRows else 0un
            let approxBytesUlong = if approxBytes > 0L then uint64 approxBytes else 0UL

            // 3. Unpack Cloud Options via external static helper
            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
                CloudOptions.ParseCloudOptions cloudOptions

            // 4. Native Call
            PolarsWrapper.SinkIpcPartitioned(
                this.CloneHandle(), 
                path,
                partitionBy.CloneHandle(), // Pass native handle of Selector
                incKeys,
                preGrouped,
                maxRowsNuint,
                approxBytesUlong,
                comp.ToNative(),
                compat,
                batchSize,
                batchStats,
                mo,
                sync.ToNative(),
                mkd,
                // Cloud
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
        /// Sink the LazyFrame to an IPC (Arrow) format in memory.
        /// <para>
        /// This allows for streaming execution directly into a byte array without writing to disk.
        /// </para>
        /// </summary>
        member this.SinkIpcMemory(
            ?compression: IpcCompression,
            ?compatLevel: int,
            ?recordBatchSize: int,
            ?recordBatchStatistics: bool,
            ?maintainOrder: bool
        ) : byte[] =
        
            let comp = defaultArg compression IpcCompression.NoCompression
            let compat = defaultArg compatLevel -1
            let batchSize = defaultArg recordBatchSize 0
            let batchStats = defaultArg recordBatchStatistics true
            let mo = defaultArg maintainOrder true

            PolarsWrapper.SinkIpcMemory(
                this.CloneHandle(),
                comp.ToNative(),
                compat,
                batchSize,
                batchStats,
                mo
            )
    type DataFrame with
        /// <summary>
        /// Write DataFrame to an IPC (Arrow/Feather) file.
        /// <para>
        /// This uses the Lazy execution engine internally to support streaming and cloud storage.
        /// </para>
        /// </summary>
        member this.WriteIpc(
            path: string,
            ?compression: IpcCompression,
            ?compatLevel: int,
            ?recordBatchSize: int,
            ?recordBatchStatistics: bool,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions
        ) =
            this.Lazy().SinkIpc(
                path,
                ?compression = compression,
                ?compatLevel = compatLevel,
                ?recordBatchSize = recordBatchSize,
                ?recordBatchStatistics = recordBatchStatistics,
                ?maintainOrder = maintainOrder,
                ?syncOnClose = syncOnClose,
                ?mkdir = mkdir,
                ?cloudOptions = cloudOptions
            ) |> ignore
            
            this

        /// <summary>
        /// Write DataFrame to a partitioned IPC (Arrow/Feather) file.
        /// <para>
        /// This uses the Lazy execution engine internally to support streaming and cloud storage.
        /// </para>
        /// </summary>
        member this.WriteIpcPartitioned(
            path: string,
            partitionBy: Selector,
            ?includeKeys: bool,
            ?keysPreGrouped: bool,
            ?maxRowsPerFile: int,
            ?approxBytesPerFile: int64,
            ?compression: IpcCompression,
            ?compatLevel: int,
            ?recordBatchSize: int,
            ?recordBatchStatistics: bool,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions
        ) =
            this.Lazy().SinkIpcPartitioned(
                path,
                partitionBy,
                ?includeKeys = includeKeys,
                ?keysPreGrouped = keysPreGrouped,
                ?maxRowsPerFile = maxRowsPerFile,
                ?approxBytesPerFile = approxBytesPerFile,
                ?compression = compression,
                ?compatLevel = compatLevel,
                ?recordBatchSize = recordBatchSize,
                ?recordBatchStatistics = recordBatchStatistics,
                ?maintainOrder = maintainOrder,
                ?syncOnClose = syncOnClose,
                ?mkdir = mkdir,
                ?cloudOptions = cloudOptions
            ) |> ignore
            
            this

        /// <summary>
        /// Write DataFrame to an IPC (Arrow/Feather) format in memory.
        /// </summary>
        /// <returns>A byte array containing the serialized IPC data.</returns>
        member this.WriteIpcMemory(
            ?compression: IpcCompression,
            ?compatLevel: int,
            ?recordBatchSize: int,
            ?recordBatchStatistics: bool,
            ?maintainOrder: bool
        ) : byte[] =
            this.Lazy().SinkIpcMemory(
                ?compression = compression,
                ?compatLevel = compatLevel,
                ?recordBatchSize = recordBatchSize,
                ?recordBatchStatistics = recordBatchStatistics,
                ?maintainOrder = maintainOrder
            )