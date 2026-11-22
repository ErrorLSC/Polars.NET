namespace Polars.FSharp

open Polars.NET.Core.Json

[<AutoOpen>]
module JsonRead =
    open System.IO

    // The schema handles given (null: none); the schemas are the caller's.
    let private handleOf (schema: PolarsSchema option) = match schema with Some s -> s.Handle | None -> null

    // 100 rows by default; null (none given) never: 0 or a value above 16,777,216 infers from the whole input.
    let private inferLength (rows: uint64 option) = System.Nullable(defaultArg rows 100UL)

    type LazyFrame with
        /// <summary>
        /// Lazily read newline delimited JSON (NDJSON): a file, a directory of files, a glob, or any of them on a cloud
        /// store. The files are listed and the columns inferred now (from the first file holding records); the data is
        /// read each time the query runs: only the columns it reads and the leading rows it keeps.
        /// </summary>
        /// <param name="path">The file, directory or glob (<c>s3://…</c>, <c>az://…</c>, <c>gs://…</c> on a store).</param>
        /// <param name="schema">Every column and its type: nothing is inferred, fields it does not name are passed over, fields a record lacks are nulls.</param>
        /// <param name="inferSchemaLen">Rows to infer the schema from (100 by default); 0 or a value above 16,777,216 infers it from the whole input (of the first file).</param>
        /// <param name="batchSize">Rows per batch read.</param>
        /// <param name="nRows">Limit the number of rows to read.</param>
        /// <param name="lowMemory">Accepted for compatibility; reading streams its input whatever it says.</param>
        /// <param name="rechunk">Accepted for compatibility; it changes nothing.</param>
        /// <param name="ignoreErrors">A value that does not fit its column's type is a null instead of an error (syntax errors still are errors).</param>
        /// <param name="rowIndexName">If provided, adds a first column (UInt32) numbering the rows across the files.</param>
        /// <param name="rowIndexOffset">Offset for the row index (default: 0).</param>
        /// <param name="includePathColumn">If provided, adds a last column with each row's file path.</param>
        /// <param name="cloudOptions">Options for cloud storage.</param>
        /// <param name="schemaOverrides">The types of some inferred columns (with <paramref name="schema"/>: of some of its columns).</param>
        /// <param name="hivePartitioning">Whether <c>key=value</c> directories of the paths are columns (off by default).</param>
        static member ScanNdjson(path: string,
                                ?schema: PolarsSchema,
                                ?inferSchemaLen: uint64,
                                ?batchSize: uint64,
                                ?nRows: uint64,
                                ?lowMemory: bool,
                                ?rechunk: bool,
                                ?ignoreErrors: bool,
                                ?rowIndexName: string,
                                ?rowIndexOffset: uint32,
                                ?includePathColumn: string,
                                ?cloudOptions: CloudOptions,
                                ?schemaOverrides: PolarsSchema,
                                ?hivePartitioning: bool) : LazyFrame =
            ignore (lowMemory, rechunk)
            let h =
                ManagedJson.ScanNdjson(
                    path, handleOf schema, handleOf schemaOverrides, Option.toNullable batchSize, inferLength inferSchemaLen,
                    Option.toNullable nRows, defaultArg ignoreErrors false, Option.toObj rowIndexName, defaultArg rowIndexOffset 0u,
                    Option.toObj includePathColumn, defaultArg hivePartitioning false, CloudOptions.ToCloudArgs cloudOptions)
            new LazyFrame(h)

        /// <summary>
        /// Lazily read NDJSON from in-memory bytes.
        /// </summary>
        /// <remarks>The bytes are copied (into native memory the scan keeps); the <c>ReadOnlyMemory&lt;byte&gt;</c> overload reads them in place.</remarks>
        static member ScanNdjson(buffer: byte[],
                                ?schema: PolarsSchema,
                                ?inferSchemaLen: uint64,
                                ?batchSize: uint64,
                                ?nRows: uint64,
                                ?lowMemory: bool,
                                ?rechunk: bool,
                                ?ignoreErrors: bool,
                                ?rowIndexName: string,
                                ?rowIndexOffset: uint32,
                                ?schemaOverrides: PolarsSchema) : LazyFrame =
            ignore (lowMemory, rechunk)
            let h =
                ManagedJson.ScanNdjson(
                    System.ReadOnlySpan<byte>(buffer), handleOf schema, handleOf schemaOverrides, Option.toNullable batchSize,
                    inferLength inferSchemaLen, Option.toNullable nRows, defaultArg ignoreErrors false, Option.toObj rowIndexName,
                    defaultArg rowIndexOffset 0u)
            new LazyFrame(h)

        /// <summary>
        /// Lazily read NDJSON from in-memory bytes.
        /// </summary>
        /// <remarks>Reads the memory in place, without a copy: it stays pinned until the last frame reading it is released, and must not change meanwhile. Free unmanaged memory only once it is unpinned (<c>MemoryManager.Unpin</c>): a query can still be finishing on a background thread right after its frames are released.</remarks>
        static member ScanNdjson(buffer: System.ReadOnlyMemory<byte>,
                                ?schema: PolarsSchema,
                                ?inferSchemaLen: uint64,
                                ?batchSize: uint64,
                                ?nRows: uint64,
                                ?lowMemory: bool,
                                ?rechunk: bool,
                                ?ignoreErrors: bool,
                                ?rowIndexName: string,
                                ?rowIndexOffset: uint32,
                                ?schemaOverrides: PolarsSchema) : LazyFrame =
            ignore (lowMemory, rechunk)
            let h =
                ManagedJson.ScanNdjson(
                    buffer, handleOf schema, handleOf schemaOverrides, Option.toNullable batchSize, inferLength inferSchemaLen,
                    Option.toNullable nRows, defaultArg ignoreErrors false, Option.toObj rowIndexName, defaultArg rowIndexOffset 0u)
            new LazyFrame(h)

        /// <summary>
        /// Lazily read NDJSON from a Stream: read now (from its position; the stream is left open), once, into native
        /// memory the scan keeps.
        /// </summary>
        static member ScanNdjson(stream: Stream,
                                ?schema: PolarsSchema,
                                ?inferSchemaLen: uint64,
                                ?batchSize: uint64,
                                ?nRows: uint64,
                                ?lowMemory: bool,
                                ?rechunk: bool,
                                ?ignoreErrors: bool,
                                ?rowIndexName: string,
                                ?rowIndexOffset: uint32,
                                ?schemaOverrides: PolarsSchema) : LazyFrame =
            ignore (lowMemory, rechunk)
            let h =
                ManagedJson.ScanNdjson(
                    stream, handleOf schema, handleOf schemaOverrides, Option.toNullable batchSize, inferLength inferSchemaLen,
                    Option.toNullable nRows, defaultArg ignoreErrors false, Option.toObj rowIndexName, defaultArg rowIndexOffset 0u)
            new LazyFrame(h)

    type DataFrame with
        /// <summary>
        /// Read a JSON file into a DataFrame: one document (an array of objects, or one object) or newline-delimited
        /// JSON, decoded on every core.
        /// </summary>
        /// <param name="path">Path to the JSON file.</param>
        /// <param name="columns">Select specific columns to read.</param>
        /// <param name="schema">Every column and its type: nothing is inferred, fields it does not name are passed over, fields a record lacks are nulls.</param>
        /// <param name="inferSchemaLen">Rows to infer the schema from (100 by default); 0 or a value above 16,777,216 infers it from the whole input.</param>
        /// <param name="batchSize">Rows per batch read.</param>
        /// <param name="ignoreErrors">A value that does not fit its column's type is a null instead of an error (syntax errors still are errors).</param>
        /// <param name="jsonFormat">Format: Json (a document) or JsonLines (NDJSON).</param>
        /// <param name="schemaOverrides">The types of some inferred columns (with <paramref name="schema"/>: of some of its columns).</param>
        static member ReadJson(path: string,
                            ?columns: string seq,
                            ?schema: PolarsSchema,
                            ?inferSchemaLen: uint64,
                            ?batchSize: uint64,
                            ?ignoreErrors: bool,
                            ?jsonFormat: JsonFormat,
                            ?schemaOverrides: PolarsSchema) : DataFrame =
            let cols = columns |> Option.map Seq.toArray |> Option.toObj
            let fmt = defaultArg jsonFormat JsonFormat.Json
            let h =
                ManagedJson.ReadJson(
                    path, cols, handleOf schema, handleOf schemaOverrides, inferLength inferSchemaLen, Option.toNullable batchSize,
                    defaultArg ignoreErrors false, fmt.ToNative())
            new DataFrame(h)

        /// <summary>
        /// Read JSON from in-memory bytes (read in place during the call).
        /// </summary>
        static member ReadJson(buffer: byte[],
                            ?columns: string seq,
                            ?schema: PolarsSchema,
                            ?inferSchemaLen: uint64,
                            ?batchSize: uint64,
                            ?ignoreErrors: bool,
                            ?jsonFormat: JsonFormat,
                            ?schemaOverrides: PolarsSchema) : DataFrame =
            let cols = columns |> Option.map Seq.toArray |> Option.toObj
            let fmt = defaultArg jsonFormat JsonFormat.Json
            let h =
                ManagedJson.ReadJson(
                    System.ReadOnlyMemory<byte>(buffer), cols, handleOf schema, handleOf schemaOverrides, inferLength inferSchemaLen,
                    Option.toNullable batchSize, defaultArg ignoreErrors false, fmt.ToNative())
            new DataFrame(h)

        /// <summary>
        /// Read JSON from a Stream, decoded as it arrives (from its position; the stream is left open).
        /// </summary>
        static member ReadJson(stream: Stream,
                            ?columns: string seq,
                            ?schema: PolarsSchema,
                            ?inferSchemaLen: uint64,
                            ?batchSize: uint64,
                            ?ignoreErrors: bool,
                            ?jsonFormat: JsonFormat,
                            ?schemaOverrides: PolarsSchema) : DataFrame =
            let cols = columns |> Option.map Seq.toArray |> Option.toObj
            let fmt = defaultArg jsonFormat JsonFormat.Json
            let h =
                ManagedJson.ReadJson(
                    stream, cols, handleOf schema, handleOf schemaOverrides, inferLength inferSchemaLen, Option.toNullable batchSize,
                    defaultArg ignoreErrors false, fmt.ToNative())
            new DataFrame(h)
