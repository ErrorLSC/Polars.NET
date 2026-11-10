namespace Polars.FSharp

open Polars.NET.Core.Wrappers

[<AutoOpen>]
module DeltaRead =
    type LazyFrame with
        /// <summary>
        /// Create a LazyFrame by scanning a Delta Lake table.
        /// </summary>
        /// <param name="path">Path to the Delta Lake table (folder containing _delta_log).</param>
        /// <param name="version">The version of the table to read (e.g., 0L, 1L). Mutually exclusive with <paramref name="datetime"/>.</param>
        /// <param name="datetime">The timestamp to read (ISO-8601 string, e.g., "2026-02-09T12:00:00Z"; without an offset it is UTC). Mutually exclusive with <paramref name="version"/>.</param>
        /// <param name="schema">Return these columns of the table, cast to these types; a column the table does not have is an error.</param>
        /// <param name="glob">Ignored since 1.2.0 (the managed Delta kernel takes the data files from the log); removed in 2.0.</param>
        /// <param name="hivePartitioning">Ignored since 1.2.0 (the managed Delta kernel takes partition values from the log, not from paths); removed in 2.0.</param>
        /// <param name="hivePartitionSchema">Ignored since 1.2.0 (the managed Delta kernel types partition values by the table schema); removed in 2.0.</param>
        /// <param name="tryParseHiveDates">Ignored since 1.2.0 (the managed Delta kernel types partition values by the table schema); removed in 2.0.</param>
        /// <returns>A new LazyFrame.</returns>
        static member ScanDelta(
            path: string,
            ?version: uint64,
            ?datetime: string,
            ?nRows: uint64,
            ?parallelStrategy: ParallelStrategy,
            ?lowMemory: bool,
            ?useStatistics: bool,
            ?glob: bool,
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
        ) : LazyFrame =
            
            // Time Travel Mutual Exclusivity Check
            if version.IsSome && datetime.IsSome then
                invalidArg "version/datetime" "Cannot specify both 'version' and 'datetime' for Delta Time Travel."

            // Resolve Defaults & Nullables
            let pVersion = Option.toNullable version
            let pDatetime = Option.toObj datetime
            let pNRows = Option.toNullable nRows
            
            let pParallel = defaultArg parallelStrategy ParallelStrategy.Auto
            let pLowMem = defaultArg lowMemory false
            let pUseStats = defaultArg useStatistics true
            let pGlob = defaultArg glob true
            let pRechunk = defaultArg rechunk false
            let pCache = defaultArg cache true
            
            let pRowIdxName = Option.toObj rowIndexName
            let pRowIdxOff = defaultArg rowIndexOffset 0u
            let pIncPathCol = Option.toObj includePathColumn
            
            let schemaHandle = match schema with Some s -> s.Handle | None -> null
            
            // Hive Defaults (Delta typically defaults to Hive partitioning = true)
            let pHivePart = defaultArg hivePartitioning true
            let hiveSchemaHandle = match hivePartitionSchema with Some s -> s.Handle | None -> null
            let pTryHiveDates = defaultArg tryParseHiveDates true

            // Cloud Options
            let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
                CloudOptions.ParseCloudOptions cloudOptions

            let h = PolarsWrapper.ScanDelta(
                path,
                pVersion,
                pDatetime,
                pNRows,
                pParallel.ToNative(),
                pLowMem,
                pUseStats,
                pGlob,
                pRechunk,
                pCache,
                pRowIdxName,
                pRowIdxOff,
                pIncPathCol,
                schemaHandle,
                pHivePart,
                hiveSchemaHandle,
                pTryHiveDates,
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
    type DataFrame with
        /// <summary>
        /// Create a DataFrame by reading a Delta Lake table.
        /// </summary>
        /// <param name="path">Path to the Delta Lake table (folder containing _delta_log).</param>
        /// <param name="version">The version of the table to read (e.g., 0L, 1L). Mutually exclusive with <paramref name="datetime"/>.</param>
        /// <param name="datetime">The timestamp to read (ISO-8601 string, e.g., "2026-02-09T12:00:00Z"). Mutually exclusive with <paramref name="version"/>.</param>
        /// <returns>A new LazyFrame.</returns>
        static member ReadDelta(
            path: string,
            ?version: uint64,
            ?datetime: string,
            ?nRows: uint64,
            ?parallelStrategy: ParallelStrategy,
            ?lowMemory: bool,
            ?useStatistics: bool,
            ?glob: bool,
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
            let lf = LazyFrame.ScanDelta(
                path,
                ?version=version,
                ?datetime=datetime,
                ?nRows=nRows,
                ?parallelStrategy=parallelStrategy,
                ?lowMemory=lowMemory,
                ?useStatistics=useStatistics,
                ?glob=glob,
                ?rechunk=rechunk,
                ?cache=cache,
                ?rowIndexName=rowIndexName,
                ?rowIndexOffset=rowIndexOffset,
                ?includePathColumn=includePathColumn,
                ?schema=schema,
                ?hivePartitioning=hivePartitioning,
                ?hivePartitionSchema=hivePartitionSchema,
                ?tryParseHiveDates=tryParseHiveDates,
                ?cloudOptions=cloudOptions
            )
            lf.Collect()