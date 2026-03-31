namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module ParquetRead =
    open System.IO
    type LazyFrame with
        /// <summary> Scan a parquet file into a LazyFrame. </summary>
        /// <summary>
        /// Lazily read from a parquet file or a common cloud store (S3, GCS, Azure, etc.).
        /// </summary>
        /// <param name="path">Path to file or cloud location.</param>
        /// <param name="nRows">Stop reading after n rows.</param>
        /// <param name="parallel">Parallel strategy (Auto, Columns, RowGroups, None).</param>
        /// <param name="lowMemory">Reduce memory pressure at the expense of performance.</param>
        /// <param name="useStatistics">Use parquet statistics to prune row groups.</param>
        /// <param name="glob">Expand path using globbing rules.</param>
        /// <param name="allowMissingColumns">If true, do not fail if columns are missing.</param>
        /// <param name="rechunk">Rechunk the memory to contiguous chunks when reading. (default: false)</param>
        /// <param name="cache">Cache the result after reading. (default: true)</param>
        /// <param name="rowIndexName">If provided, add a row index column with this name.</param>
        /// <param name="rowIndexOffset">Start index for the row index column.</param>
        /// <param name="includePathColumn">If provided, add a column with the path of the file.</param>
        /// <param name="schema">Overwrite the schema of the dataset.</param>
        /// <param name="hivePartitionSchema">The schema of the hive partitions.</param>
        /// <param name="tryParseHiveDates">Attempt to parse hive values as Date/Datetime.</param>
        /// <param name="cloudOptions">Options for cloud storage (AWS S3, Azure Blob, GCS, etc.).</param>
        static member ScanParquet
            (
                path: string,
                ?nRows: uint64,
                ?parallelStrategy: ParallelStrategy,
                ?lowMemory: bool,
                ?useStatistics: bool,
                ?glob: bool,
                ?allowMissingColumns: bool,
                ?rechunk:bool,
                ?cache:bool,
                ?rowIndexName: string,
                ?rowIndexOffset: uint32,
                ?includePathColumn: string,
                ?schema: PolarsSchema,
                ?hivePartitioning: bool,
                ?hivePartitionSchema: PolarsSchema,
                ?tryParseHiveDates: bool,
                ?cloudOptions: CloudOptions
            ) =
            // Defaults
            let pParallel = defaultArg parallelStrategy ParallelStrategy.Auto
            let pLowMem = defaultArg lowMemory false
            let pStats = defaultArg useStatistics true
            let pGlob = defaultArg glob true
            let pAllowMissing = defaultArg allowMissingColumns false
            let pRowIndexOffset = defaultArg rowIndexOffset 0u
            let pTryHive = defaultArg tryParseHiveDates false
            let pRechunk = defaultArg rechunk false
            let pCache = defaultArg cache false
            let pHivePartitioning = defaultArg hivePartitioning false

            // F# Types -> C# Interop Types
            
            // nRows: uint64 option -> ulong? (Nullable<ulong>)
            let pNRows = 
                nRows 
                |> Option.toNullable

            // Schema: Schema Object -> SchemaHandle (Raw Pointer holder)
            let hSchema = 
                schema 
                |> Option.map (fun s -> s.Handle) 
                |> Option.toObj

            let hHiveSchema = 
                hivePartitionSchema 
                |> Option.map (fun s -> s.Handle) 
                |> Option.toObj

            // Cloud Options Unwrapping Logic
            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
                CloudOptions.ParseCloudOptions cloudOptions

            // Call Wrapper
            let handle = PolarsWrapper.ScanParquet(
                path,
                pNRows,
                pParallel.ToNative(),
                pLowMem,
                pStats,
                pGlob,
                pAllowMissing,
                pRechunk,
                pCache,
                Option.toObj rowIndexName,
                pRowIndexOffset,
                Option.toObj includePathColumn,
                hSchema,
                pHivePartitioning,
                hHiveSchema,
                pTryHive,
                cProv,
                cRet,
                cToMs,
                cInitMs,
                cMaxMs,
                cCache,
                cKeys,
                cVals
            )
            
            new LazyFrame(handle)
        /// <summary>
        /// [Memory] Lazily read parquet from a byte array (in-memory buffer).
        /// </summary>
        /// <param name="buffer">The byte array containing parquet data.</param>
        /// <param name="nRows">Stop reading after n rows.</param>
        /// <param name="parallel">Parallel strategy (Auto, Columns, RowGroups, None).</param>
        /// <param name="lowMemory">Reduce memory pressure at the expense of performance.</param>
        /// <param name="useStatistics">Use parquet statistics to prune row groups.</param>
        /// <param name="glob">Globbing patterns (usually irrelevant for memory scan,always false).</param>
        /// <param name="allowMissingColumns">If true, do not fail if columns are missing.</param>
        /// <param name="rechunk">Rechunk the memory to contiguous chunks when reading. (default: false)</param>
        /// <param name="cache">Cache the result after reading. (default: true)</param>
        /// <param name="rowIndexName">If provided, add a row index column with this name.</param>
        /// <param name="rowIndexOffset">Start index for the row index column.</param>
        /// <param name="includePathColumn">If provided, add a column with the path (usually irrelevant for memory).</param>
        /// <param name="schema">Overwrite the schema of the dataset.</param>
        /// <param name="hivePartitionSchema">The schema of the hive partitions.</param>
        /// <param name="tryParseHiveDates">Attempt to parse hive values as Date/Datetime.</param>
        static member ScanParquet
            (
                buffer: byte[],
                ?nRows: uint64,
                ?parallelStrategy: ParallelStrategy,
                ?lowMemory: bool,
                ?useStatistics: bool,
                ?glob: bool,
                ?allowMissingColumns: bool,
                ?rechunk:bool,
                ?cache:bool,
                ?rowIndexName: string,
                ?rowIndexOffset: uint32,
                ?includePathColumn: string,
                ?schema: PolarsSchema,       
                ?hivePartitioning: bool,
                ?hivePartitionSchema: PolarsSchema,   
                ?tryParseHiveDates: bool
            ) =
            let pParallel = defaultArg parallelStrategy ParallelStrategy.Auto
            let pLowMem = defaultArg lowMemory false
            let pStats = defaultArg useStatistics true
            let _pGlob = defaultArg glob true
            let pAllowMissing = defaultArg allowMissingColumns false
            let pRowIndexOffset = defaultArg rowIndexOffset 0u
            let pTryHive = defaultArg tryParseHiveDates false
            let pRechunk = defaultArg rechunk false
            let pCache = defaultArg cache false
            let pHivePartitioning = defaultArg hivePartitioning false

            // 2. Type Conversions
            let pNRows = 
                nRows 
                |> Option.toNullable

            // Extract Handle from PolarsSchema wrapper
            let hSchema = 
                schema 
                |> Option.map (fun s -> s.Handle) 
                |> Option.toObj

            let hHiveSchema = 
                hivePartitionSchema 
                |> Option.map (fun s -> s.Handle) 
                |> Option.toObj

            // 3. Call C# Wrapper (Memory Overload)
            let handle = PolarsWrapper.ScanParquet(
                buffer,
                pNRows,
                pParallel.ToNative(),
                pLowMem,
                pStats,
                false,
                pAllowMissing,
                pRechunk,
                pCache,
                Option.toObj rowIndexName,
                pRowIndexOffset,
                Option.toObj includePathColumn,
                hSchema,
                pHivePartitioning,
                hHiveSchema,
                pTryHive
            )

            new LazyFrame(handle)
    type DataFrame with
           /// <summary>
        /// Read a parquet file into a DataFrame (Eager).
        /// <para>
        /// Note: This method internally uses LazyFrame.ScanParquet and collects the result.
        /// </para>
        /// </summary>
        static member ReadParquet(
            path: string,
            ?columns: seq<string>,
            ?nRows: uint64,
            ?parallelStrategy: ParallelStrategy,
            ?lowMemory: bool,
            ?useStatistics: bool,
            ?glob: bool,
            ?allowMissingColumns: bool,
            ?rechunk: bool,
            ?cache: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?schema: PolarsSchema,
            ?hivePartitioning: bool,
            ?hivePartitionSchema: PolarsSchema,
            ?tryParseHiveDates: bool,
            ?cloudOptions: CloudOptions
        ) : DataFrame =
            
            let mutable (lf: LazyFrame) = LazyFrame.ScanParquet(
                path,
                ?nRows = nRows,
                ?parallelStrategy = parallelStrategy,
                ?lowMemory = lowMemory,
                ?useStatistics = useStatistics,
                ?glob = glob,
                ?allowMissingColumns = allowMissingColumns,
                ?rechunk = rechunk,
                ?cache = cache,
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

                    use sel = new Selector(PolarsWrapper.SelectorCols (colList |> List.toArray))
                    lf <- lf.Select [sel :> IColumnExpr]
            | None -> ()

            lf.Collect()

        // ---------------------------------------------------------
        // Read Parquet (Memory / Bytes)
        // ---------------------------------------------------------

        /// <summary>
        /// Read Parquet from an in-memory byte array.
        /// </summary>
        static member ReadParquet(
            buffer: byte[],
            ?columns: seq<string>,
            ?nRows: uint64,
            ?parallelStrategy: ParallelStrategy,
            ?lowMemory: bool,
            ?useStatistics: bool,
            ?allowMissingColumns: bool,
            ?rechunk: bool,
            ?cache: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?schema: PolarsSchema,
            ?hivePartitioning:bool,
            ?hivePartitionSchema: PolarsSchema,
            ?tryParseHiveDates: bool
        ) : DataFrame =
            
            let mutable (lf: LazyFrame) = LazyFrame.ScanParquet(
                buffer,
                ?nRows = nRows,
                ?parallelStrategy = parallelStrategy,
                ?lowMemory = lowMemory,
                ?useStatistics = useStatistics,
                ?allowMissingColumns = allowMissingColumns,
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
        // Read Parquet (Stream)
        // ---------------------------------------------------------

        /// <summary>
        /// Read Parquet from a Stream.
        /// </summary>
        static member ReadParquet(
            stream: System.IO.Stream,
            ?columns: seq<string>,
            ?nRows: uint64,
            ?parallelStrategy: ParallelStrategy,
            ?lowMemory: bool,
            ?useStatistics: bool,
            ?allowMissingColumns: bool,
            ?rechunk: bool,
            ?cache: bool,
            ?rowIndexName: string,
            ?rowIndexOffset: uint32,
            ?includePathColumn: string,
            ?schema: PolarsSchema,
            ?hivePartitioning:bool,
            ?hivePartitionSchema: PolarsSchema,
            ?tryParseHiveDates: bool
        ) : DataFrame =
            
            use ms = new System.IO.MemoryStream()
            stream.CopyTo ms
            let bytes = ms.ToArray()

            DataFrame.ReadParquet(
                buffer = bytes,
                ?columns = columns,
                ?nRows = nRows,
                ?parallelStrategy = parallelStrategy,
                ?lowMemory = lowMemory,
                ?useStatistics = useStatistics,
                ?allowMissingColumns = allowMissingColumns,
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

        /// <summary> Asynchronously read a Parquet file. </summary>
        static member ReadParquetAsync (        
                path: string,
                ?columns: seq<string>,
                ?nRows: uint64,
                ?parallelStrategy: ParallelStrategy,
                ?lowMemory: bool,
                ?useStatistics: bool,
                ?glob: bool,
                ?allowMissingColumns: bool,
                ?rechunk: bool,
                ?cache: bool,
                ?rowIndexName: string,
                ?rowIndexOffset: uint32,
                ?includePathColumn: string,
                ?schema: PolarsSchema,
                ?hivePartitioning:bool,
                ?hivePartitionSchema: PolarsSchema,
                ?tryParseHiveDates: bool,
                ?cloudOptions: CloudOptions) = 
            task {
                return DataFrame.ReadParquet(
                    path,
                    ?columns = columns,
                    ?nRows = nRows,
                    ?parallelStrategy = parallelStrategy,
                    ?lowMemory = lowMemory,
                    ?useStatistics = useStatistics,
                    ?glob = glob,
                    ?allowMissingColumns = allowMissingColumns,
                    ?rechunk = rechunk,
                    ?cache = cache,
                    ?rowIndexName = rowIndexName,
                    ?rowIndexOffset = rowIndexOffset,
                    ?includePathColumn = includePathColumn,
                    ?schema = schema,
                    ?hivePartitioning = hivePartitioning,
                    ?hivePartitionSchema = hivePartitionSchema,
                    ?tryParseHiveDates = tryParseHiveDates,
                    ?cloudOptions = cloudOptions
                )
            }
