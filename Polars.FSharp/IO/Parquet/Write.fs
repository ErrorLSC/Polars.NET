namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module ParquetWrite =
    type LazyFrame with
        /// <summary>
        /// Sink the LazyFrame to a Parquet file.
        /// <para>
        /// This allows for streaming execution, processing the data in chunks and writing it to the file
        /// without loading the entire dataset into memory.
        /// </para>
        /// </summary>
        /// <param name="path">Path to the output file.</param>
        /// <param name="compression">Compression codec to use.</param>
        /// <param name="compressionLevel">Compression level (depends on the codec).</param>
        /// <param name="statistics">Write statistics to the parquet file.</param>
        /// <param name="rowGroupSize">Target row group size (in rows).</param>
        /// <param name="dataPageSize">Target data page size (in bytes).</param>
        /// <param name="compatLevel">IPC format compatibility, -1: oldest, 0: default, 1: newest.</param>
        /// <param name="maintainOrder">Maintain the order of the data.</param>
        /// <param name="syncOnClose">Whether to sync the file to disk on close.</param>
        /// <param name="mkdir">Create parent directories if they don't exist (Local file system only).</param>
        /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
        member this.SinkParquet(
            path: string,
            ?compression: ParquetCompression,
            ?compressionLevel: int,
            ?statistics: bool,
            ?rowGroupSize: int,
            ?dataPageSize: int,
            ?compatLevel: int,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions
        ) =
            // 1. Resolve Defaults
            let comp = defaultArg compression ParquetCompression.Snappy
            let compLevel = defaultArg compressionLevel -1
            let stats = defaultArg statistics false
            let rgs = defaultArg rowGroupSize 0
            let dps = defaultArg dataPageSize 0
            let compat = defaultArg compatLevel -1
            let mo = defaultArg maintainOrder true
            let sync = defaultArg syncOnClose SyncOnClose.NoSync
            let mkd = defaultArg mkdir false

            // 2. Unpack Cloud Options via external static helper
            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
                CloudOptions.ParseCloudOptions cloudOptions

            // 3. Native Call
            PolarsWrapper.SinkParquet(
                this.CloneHandle(), 
                path,
                comp.ToNative(),
                compLevel,
                stats,
                rgs,
                dps,
                compat,
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
        /// Sink the LazyFrame to a set of Parquet files, partitioned by the specified selector.
        /// <para>
        /// This writes the dataset to a directory, splitting the data into multiple files based on the
        /// partition key(s) defined in <paramref name="partitionBy"/>.
        /// </para>
        /// </summary>
        member this.SinkParquetPartitioned(
            path: string,
            partitionBy: Selector,
            ?includeKeys: bool,
            ?keysPreGrouped: bool,
            ?maxRowsPerFile: int,
            ?approxBytesPerFile: int64,
            ?compression: ParquetCompression,
            ?compressionLevel: int,
            ?statistics: bool,
            ?rowGroupSize: int,
            ?dataPageSize: int,
            ?compatLevel: int,
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

            let comp = defaultArg compression ParquetCompression.Snappy
            let compLevel = defaultArg compressionLevel -1
            let stats = defaultArg statistics false
            let rgs = defaultArg rowGroupSize 0
            let dps = defaultArg dataPageSize 0
            let compat = defaultArg compatLevel -1
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
            PolarsWrapper.SinkParquetPartitioned(
                this.CloneHandle(), 
                path,
                partitionBy.CloneHandle(), // Pass native handle of Selector
                incKeys,
                preGrouped,
                maxRowsNuint,
                approxBytesUlong,
                comp.ToNative(),
                compLevel,
                stats,
                rgs,
                dps,
                compat,
                mo,
                sync.ToNative(),
                mkd,
                // Cloud Params
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
        /// Sink the LazyFrame to a Parquet format in memory.
        /// <para>
        /// This allows for streaming execution directly into a byte array without writing to disk.
        /// </para>
        /// </summary>
        /// <returns>A byte array containing the serialized Parquet data.</returns>
        member this.SinkParquetMemory(
            ?compression: ParquetCompression,
            ?compressionLevel: int,
            ?statistics: bool,
            ?rowGroupSize: int,
            ?dataPageSize: int,
            ?compatLevel: int,
            ?maintainOrder: bool
        ) : byte[] =
            // Note: For Memory Sink, ZSTD and Level 3 are the typical defaults used in the C# wrapper
            let comp = defaultArg compression ParquetCompression.Zstd
            let compLevel = defaultArg compressionLevel 3
            let stats = defaultArg statistics true
            let rgs = defaultArg rowGroupSize 0
            let dps = defaultArg dataPageSize 0
            let compat = defaultArg compatLevel -1
            let mo = defaultArg maintainOrder true

            PolarsWrapper.SinkParquetMemory(
                this.CloneHandle(),
                comp.ToNative(),
                compLevel,
                stats,
                rgs,
                dps,
                compat,
                mo
            )
    type DataFrame with
    /// <summary>
        /// Write DataFrame to a Parquet file.
        /// <para>
        /// This uses the Lazy execution engine internally to support streaming, statistics, and cloud storage.
        /// </para>
        /// </summary>
        member this.WriteParquet(
            path: string,
            ?compression: ParquetCompression,
            ?compressionLevel: int,
            ?statistics: bool,
            ?rowGroupSize: int,
            ?dataPageSize: int,
            ?compatLevel: int,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions
        ) =
            this.Lazy().SinkParquet(
                path,
                ?compression = compression,
                ?compressionLevel = compressionLevel,
                ?statistics = statistics,
                ?rowGroupSize = rowGroupSize,
                ?dataPageSize = dataPageSize,
                ?compatLevel = compatLevel,
                ?maintainOrder = maintainOrder,
                ?syncOnClose = syncOnClose,
                ?mkdir = mkdir,
                ?cloudOptions = cloudOptions
            )
            this
        /// <summary>
        /// Write DataFrame to a partitioned Parquet file.
        /// <para>
        /// This uses the Lazy execution engine internally to support streaming and cloud storage.
        /// </para>
        /// </summary>
        member this.WriteParquetPartitioned(
            path: string,
            partitionBy: Selector,
            ?includeKeys: bool,
            ?keysPreGrouped: bool,
            ?maxRowsPerFile: int,
            ?approxBytesPerFile: int64,
            ?compression: ParquetCompression,
            ?compressionLevel: int,
            ?statistics: bool,
            ?rowGroupSize: int,
            ?dataPageSize: int,
            ?compatLevel: int,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions
        ) =
            this.Lazy().SinkParquetPartitioned(
                path,
                partitionBy,
                ?includeKeys = includeKeys,
                ?keysPreGrouped = keysPreGrouped,
                ?maxRowsPerFile = maxRowsPerFile,
                ?approxBytesPerFile = approxBytesPerFile,
                ?compression = compression,
                ?compressionLevel = compressionLevel,
                ?statistics = statistics,
                ?rowGroupSize = rowGroupSize,
                ?dataPageSize = dataPageSize,
                ?compatLevel = compatLevel,
                ?maintainOrder = maintainOrder,
                ?syncOnClose = syncOnClose,
                ?mkdir = mkdir,
                ?cloudOptions = cloudOptions
            )
            this
        /// <summary>
        /// Write DataFrame to a Parquet format in memory.
        /// </summary>
        /// <returns>A byte array containing the serialized Parquet data.</returns>
        member this.WriteParquetMemory(
            ?compression: ParquetCompression,
            ?compressionLevel: int,
            ?statistics: bool,
            ?rowGroupSize: int,
            ?dataPageSize: int,
            ?compatLevel: int,
            ?maintainOrder: bool
        ) : byte[] =
            this.Lazy().SinkParquetMemory(
                ?compression = compression,
                ?compressionLevel = compressionLevel,
                ?statistics = statistics,
                ?rowGroupSize = rowGroupSize,
                ?dataPageSize = dataPageSize,
                ?compatLevel = compatLevel,
                ?maintainOrder = maintainOrder
            )