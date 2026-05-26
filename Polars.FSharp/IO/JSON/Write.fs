namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module JsonWrite =
    type LazyFrame with
        /// <summary>
        /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) file.
        /// </summary>
        /// <param name="path">Output file path.</param>
        /// <param name="compression">Compression method (Gzip/Zstd).</param>
        /// <param name="compressionLevel">Compression level.</param>
        /// <param name="checkExtension">Whether to check if the file extension matches '.json' or '.ndjson'.</param>
        /// <param name="maintainOrder">Maintain the order of data.</param>
        /// <param name="syncOnClose">Sync to disk on close.</param>
        /// <param name="mkdir">Create parent directories.</param>
        /// <param name="cloudOptions">Cloud storage options.</param>
        member this.SinkJson(
            path: string,
            ?compression: ExternalCompression,
            ?compressionLevel: int,
            ?checkExtension: bool,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions
        ) =
            // 1. Resolve Defaults
            let comp = defaultArg compression ExternalCompression.Uncompressed
            let compLevel = defaultArg compressionLevel -1
            let chkExt = defaultArg checkExtension true
            let mo = defaultArg maintainOrder true
            let sync = defaultArg syncOnClose SyncOnClose.NoSync
            let mkd = defaultArg mkdir false

            // 2. Unpack Cloud Options via external static helper
            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
                CloudOptions.ParseCloudOptions cloudOptions

            // 3. Native Call
            PolarsWrapper.SinkJson(
                this.CloneHandle(), 
                path,
                comp.ToNative(),
                compLevel,
                chkExt,
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
        /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) file, partitioned by the given selector.
        /// </summary>
        member this.SinkJsonPartitioned(
            path: string,
            partitionBy: Selector,
            ?includeKeys: bool,
            ?keysPreGrouped: bool,
            ?maxRowsPerFile: int,
            ?approxBytesPerFile: int64,
            ?compression: ExternalCompression,
            ?compressionLevel: int,
            ?checkExtension: bool,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions
        ) =
            let incKeys = defaultArg includeKeys true
            let preGrouped = defaultArg keysPreGrouped false
            let maxRows = defaultArg maxRowsPerFile 0
            let approxBytes = defaultArg approxBytesPerFile 0L
            let comp = defaultArg compression ExternalCompression.Uncompressed
            let compLevel = defaultArg compressionLevel -1
            let chkExt = defaultArg checkExtension true
            let mo = defaultArg maintainOrder true
            let sync = defaultArg syncOnClose SyncOnClose.NoSync
            let mkd = defaultArg mkdir false

            let maxRowsNuint = if maxRows > 0 then unativeint maxRows else 0un
            let approxBytesUlong = if approxBytes > 0L then uint64 approxBytes else 0UL

            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
                CloudOptions.ParseCloudOptions cloudOptions

            PolarsWrapper.SinkJsonPartitioned(
                this.CloneHandle(), 
                path,
                partitionBy.CloneHandle(),
                incKeys,
                preGrouped,
                maxRowsNuint,
                approxBytesUlong,
                comp.ToNative(),
                compLevel,
                chkExt,
                mo,
                sync.ToNative(),
                mkd,
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
        /// Alias for SinkJson (Lazily evaluated JSON is always NDJSON/JsonLines).
        /// </summary>
        member this.SinkNdJson(
            path: string,
            ?compression: ExternalCompression,
            ?compressionLevel: int,
            ?checkExtension: bool,
            ?maintainOrder: bool,
            ?syncOnClose: SyncOnClose,
            ?mkdir: bool,
            ?cloudOptions: CloudOptions
        ) =
            this.SinkJson(
                path, 
                ?compression = compression, 
                ?compressionLevel = compressionLevel,
                ?checkExtension = checkExtension,
                ?maintainOrder = maintainOrder, 
                ?syncOnClose = syncOnClose, 
                ?mkdir = mkdir,
                ?cloudOptions = cloudOptions
            )

        /// <summary>
        /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) format in memory.
        /// </summary>
        /// <returns>A byte array containing the serialized NDJSON data.</returns>
        member this.SinkJsonMemory(
            ?compression: ExternalCompression,
            ?compressionLevel: int,
            ?checkExtension: bool,
            ?maintainOrder: bool
        ) : byte[] =
            let comp = defaultArg compression ExternalCompression.Uncompressed
            let compLevel = defaultArg compressionLevel -1
            let chkExt = defaultArg checkExtension true
            let mo = defaultArg maintainOrder true

            PolarsWrapper.SinkJsonMemory(
                this.CloneHandle(),
                comp.ToNative(),
                compLevel,
                chkExt,
                mo
            )
    type DataFrame with
        /// <summary>   
        /// Write DataFrame to a JSON file.
        /// </summary>
        member this.WriteJson(path: string, ?format: JsonFormat) =
            let format = defaultArg format JsonFormat.Json
            PolarsWrapper.WriteJson(this.Handle, path, format.ToNative())
            this
        /// <summary>
        /// Write DataFrame to a NDJSON (JsonLines) file.
        /// </summary>
        member this.WriteNdJson(path: string) =
            this.WriteJson(path, JsonFormat.JsonLines)