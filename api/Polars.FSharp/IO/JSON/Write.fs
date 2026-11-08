namespace Polars.FSharp

open System.IO
open System.IO.Pipelines
open System.Threading
open System.Threading.Tasks
open Polars.NET.Core.Wrappers

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
        /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) format written into a <see cref="T:System.IO.Stream"/>.
        /// Output is streamed in chunks as it is produced. The stream is flushed but not disposed.
        /// </summary>
        member this.SinkJson(
            stream: Stream,
            ?compression: ExternalCompression,
            ?compressionLevel: int,
            ?checkExtension: bool,
            ?maintainOrder: bool
        ) : unit =
            let comp = defaultArg compression ExternalCompression.Uncompressed
            let compLevel = defaultArg compressionLevel -1
            let chkExt = defaultArg checkExtension true
            let mo = defaultArg maintainOrder true
            let handle = this.CloneHandle()
            Polars.NET.Core.NativeStreamWriter.Write(stream, fun target ->
                PolarsWrapper.SinkJsonCallback(handle, target, comp.ToNative(), compLevel, chkExt, mo))

        /// <summary>
        /// Asynchronously sink the LazyFrame to a NDJSON (Newline Delimited JSON) format written into a <see cref="T:System.IO.Stream"/>.
        /// Safe for destinations that disallow synchronous writes (e.g. ASP.NET Core response bodies).
        /// The stream is flushed but not disposed.
        /// </summary>
        member this.SinkJsonAsync(
            stream: Stream,
            ?compression: ExternalCompression,
            ?compressionLevel: int,
            ?checkExtension: bool,
            ?maintainOrder: bool,
            ?cancellationToken: CancellationToken
        ) : Task =
            let comp = defaultArg compression ExternalCompression.Uncompressed
            let compLevel = defaultArg compressionLevel -1
            let chkExt = defaultArg checkExtension true
            let mo = defaultArg maintainOrder true
            let ct = defaultArg cancellationToken CancellationToken.None
            let handle = this.CloneHandle()
            Polars.NET.Core.NativeStreamWriter.WriteAsync(stream, (fun target ->
                PolarsWrapper.SinkJsonCallback(handle, target, comp.ToNative(), compLevel, chkExt, mo)), ct)

        /// <summary>
        /// Asynchronously sink the LazyFrame to a NDJSON (Newline Delimited JSON) format written into a <see cref="T:System.IO.Pipelines.PipeWriter"/>.
        /// The writer is flushed but not completed.
        /// </summary>
        member this.SinkJsonAsync(
            writer: PipeWriter,
            ?compression: ExternalCompression,
            ?compressionLevel: int,
            ?checkExtension: bool,
            ?maintainOrder: bool,
            ?cancellationToken: CancellationToken
        ) : Task =
            let comp = defaultArg compression ExternalCompression.Uncompressed
            let compLevel = defaultArg compressionLevel -1
            let chkExt = defaultArg checkExtension true
            let mo = defaultArg maintainOrder true
            let ct = defaultArg cancellationToken CancellationToken.None
            let handle = this.CloneHandle()
            Polars.NET.Core.NativeStreamWriter.WriteAsync(writer, (fun target ->
                PolarsWrapper.SinkJsonCallback(handle, target, comp.ToNative(), compLevel, chkExt, mo)), ct)

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

        /// <summary>
        /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) format in native memory, without copying into the managed heap.
        /// </summary>
        /// <returns>A <see cref="T:Polars.NET.Core.NativeBuffer"/> owning the serialized NDJSON data. Dispose it to release the native memory.</returns>
        member this.SinkJsonNativeMemory(
            ?compression: ExternalCompression,
            ?compressionLevel: int,
            ?checkExtension: bool,
            ?maintainOrder: bool
        ) : Polars.NET.Core.NativeBuffer =
            let comp = defaultArg compression ExternalCompression.Uncompressed
            let compLevel = defaultArg compressionLevel -1
            let chkExt = defaultArg checkExtension true
            let mo = defaultArg maintainOrder true

            PolarsWrapper.SinkJsonNativeMemory(
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
        /// Write DataFrame to a JSON format written into a <see cref="T:System.IO.Stream"/>.
        /// Output is streamed in chunks as it is produced. The stream is flushed but not disposed.
        /// </summary>
        /// <returns>The same DataFrame, for chaining.</returns>
        member this.WriteJson(stream: Stream, ?format: JsonFormat) : DataFrame =
            let format = defaultArg format JsonFormat.Json
            Polars.NET.Core.NativeStreamWriter.Write(stream, fun target ->
                PolarsWrapper.WriteJsonCallback(this.Handle, target, format.ToNative()))
            this

        /// <summary>
        /// Asynchronously write DataFrame to a JSON format written into a <see cref="T:System.IO.Stream"/>.
        /// Safe for destinations that disallow synchronous writes (e.g. ASP.NET Core response bodies).
        /// The stream is flushed but not disposed.
        /// </summary>
        member this.WriteJsonAsync(stream: Stream, ?format: JsonFormat, ?cancellationToken: CancellationToken) : Task =
            let format = defaultArg format JsonFormat.Json
            let ct = defaultArg cancellationToken CancellationToken.None
            Polars.NET.Core.NativeStreamWriter.WriteAsync(stream, (fun target ->
                PolarsWrapper.WriteJsonCallback(this.Handle, target, format.ToNative())), ct)

        /// <summary>
        /// Asynchronously write DataFrame to a JSON format written into a <see cref="T:System.IO.Pipelines.PipeWriter"/>.
        /// The writer is flushed but not completed.
        /// </summary>
        member this.WriteJsonAsync(writer: PipeWriter, ?format: JsonFormat, ?cancellationToken: CancellationToken) : Task =
            let format = defaultArg format JsonFormat.Json
            let ct = defaultArg cancellationToken CancellationToken.None
            Polars.NET.Core.NativeStreamWriter.WriteAsync(writer, (fun target ->
                PolarsWrapper.WriteJsonCallback(this.Handle, target, format.ToNative())), ct)

        /// <summary>
        /// Write DataFrame to a NDJSON (JsonLines) file.
        /// </summary>
        member this.WriteNdJson(path: string) =
            this.WriteJson(path, JsonFormat.JsonLines)