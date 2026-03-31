namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module JsonRead =
    open System.IO
    type LazyFrame with
        /// <summary>
        /// Lazily read a NDJSON file.
        /// </summary>
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
                                ?cloudOptions: CloudOptions) : LazyFrame =
            
            let schemaHandle = match schema with Some s -> s.Handle | None -> null
            let inferLen = Option.toNullable inferSchemaLen
            let batch = Option.toNullable batchSize
            let rows = Option.toNullable nRows
            
            let lowMem = defaultArg lowMemory false
            let rechk = defaultArg rechunk false
            let ignoreErr = defaultArg ignoreErrors false
            
            let idxName = Option.toObj rowIndexName
            let idxOffset = defaultArg rowIndexOffset 0u
            let pathCol = Option.toObj includePathColumn

            // Parse Cloud Options
            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
                CloudOptions.ParseCloudOptions cloudOptions

            let h = PolarsWrapper.ScanNdjson(
                path, 
                schemaHandle, 
                batch, 
                inferLen, 
                rows, 
                lowMem, 
                rechk, 
                ignoreErr, 
                idxName, 
                idxOffset, 
                pathCol,
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
            new LazyFrame(h)

        /// <summary>
        /// Lazily read NDJSON from in-memory bytes.
        /// </summary>
        static member ScanNdjson(buffer: byte[],
                                ?schema: PolarsSchema,
                                ?inferSchemaLen: uint64,
                                ?batchSize: uint64,
                                ?nRows: uint64,
                                ?lowMemory: bool,
                                ?rechunk: bool,
                                ?ignoreErrors: bool,
                                ?rowIndexName: string,
                                ?rowIndexOffset: uint32) : LazyFrame =
            
            let schemaHandle = match schema with Some s -> s.Handle | None -> null
            let inferLen = Option.toNullable inferSchemaLen
            let batch = Option.toNullable batchSize
            let rows = Option.toNullable nRows
            
            let lowMem = defaultArg lowMemory false
            let rechk = defaultArg rechunk false
            let ignoreErr = defaultArg ignoreErrors false
            
            let idxName = Option.toObj rowIndexName
            let idxOffset = defaultArg rowIndexOffset 0u
            
            let h = PolarsWrapper.ScanNdjson(
                buffer, 
                schemaHandle, 
                batch, 
                inferLen, 
                rows, 
                lowMem, 
                rechk, 
                ignoreErr, 
                idxName, 
                idxOffset, 
                null 
            )
            new LazyFrame(h)

        /// <summary>
        /// Lazily read NDJSON from a Stream.
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
                                ?rowIndexOffset: uint32) : LazyFrame =
            
            use ms = new MemoryStream()
            stream.CopyTo ms
            let bytes = ms.ToArray()

            LazyFrame.ScanNdjson(
                bytes,
                ?schema=schema,
                ?inferSchemaLen=inferSchemaLen,
                ?batchSize=batchSize,
                ?nRows=nRows,
                ?lowMemory=lowMemory,
                ?rechunk=rechunk,
                ?ignoreErrors=ignoreErrors,
                ?rowIndexName=rowIndexName,
                ?rowIndexOffset=rowIndexOffset
            )
    type DataFrame with
        /// <summary>
        /// Read a JSON file into a DataFrame.
        /// </summary>
        static member ReadJson(path: string, 
                            ?columns: string seq, 
                            ?schema: PolarsSchema, 
                            ?inferSchemaLen: uint64, 
                            ?batchSize: uint64, 
                            ?ignoreErrors: bool,
                            ?jsonFormat: JsonFormat) : DataFrame =
            
            let cols = columns |> Option.map Seq.toArray |> Option.toObj
            
            let schemaHandle = match schema with Some s -> s.Handle | None -> null
            
            let inferLen = Option.toNullable inferSchemaLen
            let batch = Option.toNullable batchSize
            
            let ignoreErr = defaultArg ignoreErrors false
            
            let fmt = defaultArg jsonFormat JsonFormat.Json

            let h = PolarsWrapper.ReadJson(path, cols, schemaHandle, inferLen, batch, ignoreErr, fmt.ToNative())
            new DataFrame(h)

        /// <summary>
        /// Read JSON from in-memory bytes.
        /// </summary>
        static member ReadJson(buffer: byte[], 
                            ?columns: string seq, 
                            ?schema: PolarsSchema, 
                            ?inferSchemaLen: uint64, 
                            ?batchSize: uint64, 
                            ?ignoreErrors: bool,
                            ?jsonFormat: JsonFormat) : DataFrame =
            
            let cols = columns |> Option.map Seq.toArray |> Option.toObj
            let schemaHandle = match schema with Some s -> s.Handle | None -> null
            let inferLen = Option.toNullable inferSchemaLen
            let batch = Option.toNullable batchSize
            let ignoreErr = defaultArg ignoreErrors false
            
            let fmt = defaultArg jsonFormat JsonFormat.Json

            let h = PolarsWrapper.ReadJson(buffer, cols, schemaHandle, inferLen, batch, ignoreErr, fmt.ToNative())
            new DataFrame(h)

        /// <summary>
        /// Read JSON from a Stream.
        /// </summary>
        static member ReadJson(stream: Stream, 
                            ?columns: string seq, 
                            ?schema: PolarsSchema, 
                            ?inferSchemaLen: uint64, 
                            ?batchSize: uint64, 
                            ?ignoreErrors: bool,
                            ?jsonFormat: JsonFormat) : DataFrame =
            
            use ms = new MemoryStream()
            stream.CopyTo ms
            let bytes = ms.ToArray()
            
            DataFrame.ReadJson(
                bytes, 
                ?columns=columns, 
                ?schema=schema, 
                ?inferSchemaLen=inferSchemaLen, 
                ?batchSize=batchSize, 
                ?ignoreErrors=ignoreErrors, 
                ?jsonFormat=jsonFormat
            )