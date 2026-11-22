namespace Polars.FSharp

open System.IO
open System.IO.Pipelines
open System.Threading
open System.Threading.Tasks
open Polars.NET.Core.Json

[<AutoOpen>]
module JsonWrite =
    type LazyFrame with
        /// <summary>
        /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) file, published whole once written: the query runs on
        /// the streaming engine and its batches are encoded on every core as they come.
        /// </summary>
        /// <param name="path">Output file path (<c>s3://…</c>, <c>az://…</c>, <c>gs://…</c> on a store).</param>
        /// <param name="compression">Compression method (Gzip/Zstd).</param>
        /// <param name="compressionLevel">Compression level.</param>
        /// <param name="checkExtension">Whether the path's extension must match the compression: none or <c>.gz</c> / <c>.zst</c> when compressed, not one of those when not.</param>
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
            ManagedJson.SinkJson(
                this.CloneHandle(),
                path,
                (defaultArg compression ExternalCompression.Uncompressed).ToNative(),
                defaultArg compressionLevel -1,
                defaultArg checkExtension true,
                defaultArg maintainOrder true,
                (defaultArg syncOnClose SyncOnClose.NoSync).ToNative(),
                defaultArg mkdir false,
                CloudOptions.ToCloudArgs cloudOptions)

        /// <summary>
        /// Sink the LazyFrame to a NDJSON (Newline Delimited JSON) dataset partitioned by the given selector: Hive
        /// directories of the keys' values (<c>key=value</c>), files named <c>part-{index:D5}-{write id}.jsonl</c>
        /// (<c>.gz</c>, <c>.zst</c> when compressed), each published whole. Keys said to be pre-grouped while they are not
        /// write more files, never wrong ones.
        /// </summary>
        /// <param name="existingData">What happens to files under <paramref name="path"/> already (an error by default).</param>
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
            ?cloudOptions: CloudOptions,
            ?existingData: ExistingData
        ) =
            ignore (checkExtension, mkdir)
            let maxRows = defaultArg maxRowsPerFile 0
            let approxBytes = defaultArg approxBytesPerFile 0L
            // A copy of the selector, as the C# API takes one: the caller's stays untouched whatever the call does with it.
            let keys =
                use selector = partitionBy.Clone()
                ManagedJson.SelectedColumns(this.Handle, selector.Handle)
            ManagedJson.SinkJsonPartitioned(
                this.CloneHandle(),
                path,
                keys,
                defaultArg includeKeys true,
                defaultArg keysPreGrouped false,
                (if maxRows > 0 then uint64 maxRows else 0UL),
                (if approxBytes > 0L then uint64 approxBytes else 0UL),
                (defaultArg compression ExternalCompression.Uncompressed).ToNative(),
                defaultArg compressionLevel -1,
                defaultArg maintainOrder true,
                (defaultArg syncOnClose SyncOnClose.NoSync).ToNative(),
                (defaultArg existingData ExistingData.Error).ToNative(),
                CloudOptions.ToCloudArgs cloudOptions)

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
            ignore checkExtension
            ManagedJson.SinkJson(
                this.CloneHandle(), stream, (defaultArg compression ExternalCompression.Uncompressed).ToNative(),
                defaultArg compressionLevel -1, defaultArg maintainOrder true)

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
            ignore checkExtension
            ManagedJson.SinkJsonAsync(
                this.CloneHandle(), stream, (defaultArg compression ExternalCompression.Uncompressed).ToNative(),
                defaultArg compressionLevel -1, defaultArg maintainOrder true, defaultArg cancellationToken CancellationToken.None)

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
            ignore checkExtension
            ManagedJson.SinkJsonAsync(
                this.CloneHandle(), writer, (defaultArg compression ExternalCompression.Uncompressed).ToNative(),
                defaultArg compressionLevel -1, defaultArg maintainOrder true, defaultArg cancellationToken CancellationToken.None)

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
            ignore checkExtension
            ManagedJson.SinkJsonToMemory(
                this.CloneHandle(), (defaultArg compression ExternalCompression.Uncompressed).ToNative(),
                defaultArg compressionLevel -1, defaultArg maintainOrder true)

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
            ignore checkExtension
            ManagedJson.SinkJsonNativeMemory(
                this.CloneHandle(), (defaultArg compression ExternalCompression.Uncompressed).ToNative(),
                defaultArg compressionLevel -1, defaultArg maintainOrder true)

    type DataFrame with
        /// <summary>
        /// Write DataFrame to a JSON file (replaced; published whole once written).
        /// </summary>
        member this.WriteJson(path: string, ?format: JsonFormat) =
            ManagedJson.WriteJson(this.Handle, path, (defaultArg format JsonFormat.Json).ToNative())
            this

        /// <summary>
        /// Write DataFrame to a JSON format written into a <see cref="T:System.IO.Stream"/>.
        /// Output is streamed in chunks as it is produced. The stream is flushed but not disposed.
        /// </summary>
        /// <returns>The same DataFrame, for chaining.</returns>
        member this.WriteJson(stream: Stream, ?format: JsonFormat) : DataFrame =
            ManagedJson.WriteJson(this.Handle, stream, (defaultArg format JsonFormat.Json).ToNative())
            this

        /// <summary>
        /// Asynchronously write DataFrame to a JSON format written into a <see cref="T:System.IO.Stream"/>.
        /// Safe for destinations that disallow synchronous writes (e.g. ASP.NET Core response bodies).
        /// The stream is flushed but not disposed.
        /// </summary>
        member this.WriteJsonAsync(stream: Stream, ?format: JsonFormat, ?cancellationToken: CancellationToken) : Task =
            ManagedJson.WriteJsonAsync(
                this.Handle, stream, (defaultArg format JsonFormat.Json).ToNative(), defaultArg cancellationToken CancellationToken.None)

        /// <summary>
        /// Asynchronously write DataFrame to a JSON format written into a <see cref="T:System.IO.Pipelines.PipeWriter"/>.
        /// The writer is flushed but not completed.
        /// </summary>
        member this.WriteJsonAsync(writer: PipeWriter, ?format: JsonFormat, ?cancellationToken: CancellationToken) : Task =
            ManagedJson.WriteJsonAsync(
                this.Handle, writer, (defaultArg format JsonFormat.Json).ToNative(), defaultArg cancellationToken CancellationToken.None)

        /// <summary>
        /// Write DataFrame to a NDJSON (JsonLines) file.
        /// </summary>
        member this.WriteNdJson(path: string) =
            this.WriteJson(path, JsonFormat.JsonLines)
