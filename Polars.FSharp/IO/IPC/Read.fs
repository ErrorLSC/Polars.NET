namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module IPCRead =
    open System.IO
    type LazyFrame with
        /// <summary>
        /// Lazily read an Arrow IPC (Feather v2) file, multiple files via glob patterns, or cloud storage.
        /// </summary>
        static member ScanIpc(
            path: string,
            ?schema: PolarsSchema,
            ?nRows: uint64,
            ?rechunk: bool,
            ?cache: bool,
            ?glob: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?hivePartitioning: bool,
            ?hivePartitionSchema: PolarsSchema,
            ?tryParseHiveDates: bool,
            ?cloudOptions: CloudOptions
        ) : LazyFrame =
            
            // Resolve Optional Handles
            let schemaHandle = match schema with Some s -> s.Handle | None -> null
            let hiveSchemaHandle = match hivePartitionSchema with Some s -> s.Handle | None -> null
            
            // Resolve Defaults
            let rows = Option.toNullable nRows
            let rechk = defaultArg rechunk false
            let useCache = defaultArg cache true 
            let useGlob = defaultArg glob true
            let idxName = Option.toObj rowIndexName
            let idxOffset = defaultArg rowIndexOffset 0u
            let pathCol = Option.toObj includePathColumn
            let hive = defaultArg hivePartitioning false
            let hiveDates = defaultArg tryParseHiveDates true

            // Parse Cloud Options
            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
                CloudOptions.ParseCloudOptions cloudOptions

            let h = PolarsWrapper.ScanIpc(
                path, 
                rows, 
                rechk, 
                useCache, 
                useGlob,
                idxName, 
                idxOffset, 
                pathCol, 
                schemaHandle,
                hive,
                hiveSchemaHandle,
                hiveDates,
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

        // ---------------------------------------------------------
        // Scan IPC (Memory / Bytes)
        // ---------------------------------------------------------

        /// <summary>
        /// Lazily read Arrow IPC (Feather v2) from in-memory bytes.
        /// </summary>
        static member ScanIpc(
            buffer: byte[],
            ?schema: PolarsSchema,
            ?nRows: uint64,
            ?rechunk: bool,
            ?cache: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?hivePartitioning: bool,
            ?hivePartitionSchema: PolarsSchema,
            ?tryParseHiveDates: bool
        ) : LazyFrame =
            
            let schemaHandle = match schema with Some s -> s.Handle | None -> null
            let hiveSchemaHandle = match hivePartitionSchema with Some s -> s.Handle | None -> null
            
            let rows = Option.toNullable nRows
            let rechk = defaultArg rechunk false
            let useCache = defaultArg cache true
            let idxName = Option.toObj rowIndexName
            let idxOffset = defaultArg rowIndexOffset 0u
            let pathCol = Option.toObj includePathColumn
            let hive = defaultArg hivePartitioning false
            let hiveDates = defaultArg tryParseHiveDates false

            let h = PolarsWrapper.ScanIpc(
                buffer, 
                rows, 
                rechk, 
                useCache, 
                idxName, 
                idxOffset, 
                pathCol,
                schemaHandle, 
                hive,
                hiveSchemaHandle,
                hiveDates
            )
            new LazyFrame(h)

        // ---------------------------------------------------------
        // Scan IPC (Stream)
        // ---------------------------------------------------------

        /// <summary>
        /// Lazily read Arrow IPC (Feather v2) from a Stream.
        /// </summary>
        /// <remarks>
        /// This reads the stream fully into memory to construct the Lazy execution plan.
        /// </remarks>
        static member ScanIpc(
            stream: System.IO.Stream,
            ?schema: PolarsSchema,
            ?nRows: uint64,
            ?rechunk: bool,
            ?cache: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?hivePartitioning: bool,
            ?hivePartitionSchema: PolarsSchema,
            ?tryParseHiveDates: bool
        ) : LazyFrame =
            
            use ms = new System.IO.MemoryStream()
            stream.CopyTo(ms)
            let bytes = ms.ToArray()

            LazyFrame.ScanIpc(
                bytes,
                ?schema = schema,
                ?nRows = nRows,
                ?rechunk = rechunk,
                ?cache = cache,
                ?rowIndexName = rowIndexName,
                ?rowIndexOffset = rowIndexOffset,
                ?includePathColumn = includePathColumn,
                ?hivePartitioning = hivePartitioning,
                ?hivePartitionSchema = hivePartitionSchema,
                ?tryParseHiveDates = tryParseHiveDates
            )
    type DataFrame with
        /// <summary>
        /// Read an Arrow IPC (Feather v2) file into a DataFrame.
        /// <para>
        /// Note: This method internally uses LazyFrame.ScanIpc and collects the result.
        /// </para>
        /// </summary>
        static member ReadIpc(
            path: string,
            ?columns: seq<string>,
            ?nRows: uint64,
            ?rechunk: bool,
            ?cache: bool,
            ?glob: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?schema: PolarsSchema,
            ?hivePartitioning: bool,
            ?hivePartitionSchema: PolarsSchema,
            ?tryParseHiveDates: bool,
            ?cloudOptions: CloudOptions
        ) : DataFrame =
            
            let mutable lf = LazyFrame.ScanIpc(
                path,
                ?nRows = nRows,
                ?rechunk = rechunk,
                ?cache = cache,
                ?glob = glob,
                ?rowIndexName = rowIndexName,
                ?rowIndexOffset = rowIndexOffset,
                ?includePathColumn = includePathColumn,
                ?schema = schema,
                ?hivePartitioning = hivePartitioning,
                ?hivePartitionSchema = hivePartitionSchema,
                ?tryParseHiveDates = tryParseHiveDates,
                ?cloudOptions = cloudOptions
            )

            match columns with
            | Some cols -> 
                let mutable colList = cols |> Seq.toList
                if not (List.isEmpty colList) then
                    match rowIndexName with
                    | Some rName when not (List.contains rName colList) -> colList <- colList @ [rName]
                    | _ -> ()

                    match includePathColumn with
                    | Some pName when not (List.contains pName colList) -> colList <- colList @ [pName]
                    | _ -> ()

                    use sel = new Selector(PolarsWrapper.SelectorCols(colList |> List.toArray))
                    lf <- lf.Select [sel :> IColumnExpr]
            | None -> ()

            lf.Collect()

        // ---------------------------------------------------------
        // Read IPC (Memory / Bytes)
        // ---------------------------------------------------------

        /// <summary>
        /// Read IPC from in-memory bytes.
        /// </summary>
        static member ReadIpc(
            buffer: byte[],
            ?columns: seq<string>,
            ?nRows: uint64,
            ?rechunk: bool,
            ?cache: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?schema: PolarsSchema,
            ?hivePartitioning: bool,
            ?hivePartitionSchema: PolarsSchema,
            ?tryParseHiveDates: bool
        ) : DataFrame =
            
            let mutable lf = LazyFrame.ScanIpc(
                buffer,
                ?nRows = nRows,
                ?rechunk = rechunk,
                ?cache = cache,
                ?rowIndexName = rowIndexName,
                ?rowIndexOffset = rowIndexOffset,
                ?includePathColumn = includePathColumn,
                ?schema = schema,
                ?hivePartitioning = hivePartitioning,
                ?hivePartitionSchema = hivePartitionSchema,
                ?tryParseHiveDates = tryParseHiveDates
            )

            match columns with
            | Some cols -> 
                let mutable colList = cols |> Seq.toList
                if not (List.isEmpty colList) then
                    match rowIndexName with
                    | Some rName when not (List.contains rName colList) -> colList <- colList @ [rName]
                    | _ -> ()

                    match includePathColumn with
                    | Some pName when not (List.contains pName colList) -> colList <- colList @ [pName]
                    | _ -> ()

                    use sel = new Selector(PolarsWrapper.SelectorCols(colList |> List.toArray))
                    lf <- lf.Select [sel :> IColumnExpr]
            | None -> ()

            lf.Collect()

        // ---------------------------------------------------------
        // Read IPC (Stream)
        // ---------------------------------------------------------

        /// <summary>
        /// Read IPC from a Stream.
        /// </summary>
        static member ReadIpc(
            stream: System.IO.Stream,
            ?columns: seq<string>,
            ?nRows: uint64,
            ?rechunk: bool,
            ?cache: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?schema: PolarsSchema,
            ?hivePartitioning: bool,
            ?hivePartitionSchema: PolarsSchema,
            ?tryParseHiveDates: bool
        ) : DataFrame =
            
            use ms = new System.IO.MemoryStream()
            stream.CopyTo ms
            let bytes = ms.ToArray()

            DataFrame.ReadIpc(
                bytes,
                ?columns = columns,
                ?nRows = nRows,
                ?rechunk = rechunk,
                ?cache = cache,
                ?rowIndexName = rowIndexName,
                ?rowIndexOffset = rowIndexOffset,
                ?includePathColumn = includePathColumn,
                ?schema = schema,
                ?hivePartitioning = hivePartitioning,
                ?hivePartitionSchema = hivePartitionSchema,
                ?tryParseHiveDates = tryParseHiveDates
            )