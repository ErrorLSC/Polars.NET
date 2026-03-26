namespace Polars.FSharp

open Polars.NET.Core
open System

/// <summary>
/// Common Delta Lake Table Features.
/// </summary>
module DeltaTableFeatures =

    /// <summary>
    /// Enables Deletion Vectors for efficient deletes/updates (Merge-on-Read).
    /// Requires Reader v3, Writer v7.
    /// </summary>
    [<Literal>]
    let DeletionVectors = "deletionVectors"

    /// <summary>
    /// Enables Change Data Feed (CDF) to track row-level changes.
    /// </summary>
    [<Literal>]
    let ChangeDataFeed = "changeDataFeed"

    /// <summary>
    /// Enables Column Mapping (allows renaming columns and special characters).
    /// </summary>
    [<Literal>]
    let ColumnMapping = "columnMapping"

    /// <summary>
    /// Enforces Append-Only retention (prevents deletes/updates).
    /// </summary>
    [<Literal>]
    let AppendOnly = "appendOnly"
    
    /// <summary>
    /// Enables Check Constraints on columns.
    /// </summary>
    [<Literal>]
    let CheckConstraints = "checkConstraints"

    /// <summary>
    /// Enables TimestampWithoutTimezone.
    /// </summary>
    [<Literal>]
    let TimestampWithoutTimezone = "TimestampWithoutTimezone"

    /// <summary>
    /// Enables IcebergCompatV1
    /// </summary>
    [<Literal>]
    let IcebergCompatV1 = "IcebergCompatV1"

/// <summary>
/// Common configuration keys for Delta Tables.
/// </summary>
module DeltaTableProperties =

    /// <summary>
    /// The shortest duration that deleted files are kept before Vacuum deletes them.
    /// Default: 7 days (e.g., "interval 7 days").
    /// </summary>
    [<Literal>]
    let DeletedFileRetentionDuration = "delta.deletedFileRetentionDuration"

    /// <summary>
    /// How long the history of the Delta Log is kept.
    /// Default: 30 days (e.g., "interval 30 days").
    /// </summary>
    [<Literal>]
    let LogRetentionDuration = "delta.logRetentionDuration"

    /// <summary>
    /// The target file size in bytes for Optimize/Bin-packing.
    /// Default: 134217728 (128 MB).
    /// </summary>
    [<Literal>]
    let TargetFileSize = "delta.targetFileSize"

    /// <summary>
    /// Whether to collect stats for columns (min/max/nulls).
    /// Default: true.
    /// </summary>
    [<Literal>]
    let DataSkippingNumIndexedCols = "delta.dataSkippingNumIndexedCols"
    
    /// <summary>
    /// If true, enables the Change Data Feed (CDF).
    /// </summary>
    [<Literal>]
    let EnableChangeDataFeed = "delta.enableChangeDataFeed"

/// <summary>
/// Methods for DeltaLake 
/// </summary>
type Delta =

    /// <summary>
    /// Represents a column from the incoming Source LazyFrame.
    /// (Internally maps to "{name}_src_tmp")
    /// </summary>
    static member Source(columnName: string) =
        Expr.Col $"{columnName}_src_tmp"

    /// <summary>
    /// Represents a column from the existing Target Delta Table.
    /// (Alias for standard Col(), adds semantic clarity)
    /// </summary>
    static member Target(columnName: string) =
        Expr.Col columnName

    /// <summary>
    /// Deletes rows from a Delta Lake table that match a given predicate.
    /// <para>
    /// By default, this operation performs a Copy-on-Write (CoW), meaning any underlying data files containing matching rows are entirely rewritten.
    /// </para>
    /// <para>
    /// Note: If Deletion Vectors are enabled on the table, this operation automatically shifts to a Merge-on-Read (MoR) approach. 
    /// Instead of expensive file rewrites, deleted rows are swiftly marked in a separate deletion vector file, which drastically improves deletion performance.
    /// </para>
    /// </summary>
    /// <param name="path">Path to the Delta table.</param>
    /// <param name="predicate">Filter expression to identify rows to delete.</param>
    /// <param name="cloudOptions">Cloud storage configuration.</param>
    static member Delete(
        path: string,
        predicate: Expr,
        ?cloudOptions: CloudOptions
    ) =
        // 1. Unpack Cloud Options
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // 2. Safely clone the expression handle
        use clonedPredicate = predicate.CloneHandle()

        // 3. Native Call
        PolarsWrapper.DeltaDelete(
            path,
            clonedPredicate,
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
    /// Recursively delete files and directories in the table that are no longer needed by the table for maintaining the transaction history.
    /// </summary>
    /// <param name="path">The root URI of the Delta table (e.g., "s3://bucket/table").</param>
    /// <param name="retentionHours">
    /// The retention threshold in hours. Files removed before this threshold will be deleted. 
    /// Set to -1 to use the default retention period configured in the table (usually 168 hours / 7 days).
    /// </param>
    /// <param name="enforceRetention">
    /// If true, prevents the vacuum command from deleting files that have not yet reached the retention threshold. 
    /// Setting this to false allows forcing deletion of recent files (use with caution).
    /// </param>
    /// <param name="dryRun">
    /// If true, simply returns the number of files that would be deleted, without actually deleting them.
    /// </param>
    /// <param name="vacuumModeFull">
    /// Specifies the vacuum mode:
    /// <para>false (Lite): Only removes files that are explicitly referenced as removed in the transaction log. Faster.</para>
    /// <para>true (Full): Scans the entire storage directory to find and remove any files not referenced in the transaction log (including orphan files from failed writes). Slower but more thorough.</para>
    /// </param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration (e.g. AWS Keys, Azure Secrets).</param>
    /// <returns>The number of files deleted (or selected for deletion in dry-run mode).</returns>
    static member Vacuum(
        path: string,
        ?retentionHours: int,
        ?enforceRetention: bool,
        ?dryRun: bool,
        ?vacuumModeFull: bool,
        ?cloudOptions: CloudOptions
    ) : int64 =
        let pRetention = defaultArg retentionHours -1
        
        if retentionHours.IsSome && retentionHours.Value < 0 then
            invalidArg "retentionHours" "Retention hours cannot be negative."

        let pEnforce = defaultArg enforceRetention true
        let pDryRun = defaultArg dryRun false
        let pFull = defaultArg vacuumModeFull false

        // Vacuum doesn't need full retry mechanisms, just auth keys
        let _, _, _, _, _, _, cKeys, cVals = CloudOptions.ParseCloudOptions cloudOptions

        PolarsWrapper.Vacuum(
            path,
            pRetention,
            pEnforce,
            pDryRun,
            pFull,
            cKeys,
            cVals
        )

    /// <summary>
    /// Restores the Delta table to a previous state defined by a specific version or timestamp.
    /// <para>Note: This operation creates a new commit (version) that reflects the state of the table at the target point.</para>
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="version">The specific version number to restore to. Set to null if restoring by timestamp.</param>
    /// <param name="timestamp">The timestamp to restore to. Set to null if restoring by version.</param>
    /// <param name="ignoreMissingFiles">
    /// If true, the restore operation will ignore missing files (e.g., those deleted by Vacuum). 
    /// If false, it will fail if any required files are missing.
    /// </param>
    /// <param name="protocolDowngradeAllowed">Whether to allow downgrading the Delta Protocol version during restore (rarely needed).</param>
    /// <param name="cloudOptions">Cloud storage credentials.</param>
    /// <returns>The new version number of the table after the restore operation.</returns>
    static member Restore(
        path: string,
        ?version: int64,
        ?timestamp: System.DateTime,
        ?ignoreMissingFiles: bool,
        ?protocolDowngradeAllowed: bool,
        ?cloudOptions: CloudOptions
    ) : int64 =
        // 1. Validation: Version and Timestamp are mutually exclusive
        if version.IsSome && timestamp.IsSome then
            invalidArg "version/timestamp" "Cannot specify both 'version' and 'timestamp' for Restore."
        
        if version.IsNone && timestamp.IsNone then
            invalidArg "version/timestamp" "Must specify either 'version' or 'timestamp' for Restore."

        // 2. Prepare Parameters
        let targetVer = defaultArg version -1L
        
        let targetTs = 
            match timestamp with
            | Some dt -> 
                let utcTime = dt.ToUniversalTime()
                System.DateTimeOffset(utcTime).ToUnixTimeMilliseconds()
            | None -> -1L

        let pIgnore = defaultArg ignoreMissingFiles false
        let pDowngrade = defaultArg protocolDowngradeAllowed false

        // 3. Parse Cloud Options
        let _, _, _, _, _, _, cKeys, cVals = CloudOptions.ParseCloudOptions cloudOptions

        // 4. Call Wrapper
        PolarsWrapper.Restore(
            path,
            targetVer,
            targetTs,
            pIgnore,
            pDowngrade,
            cKeys,
            cVals
        )
    /// <summary>
    /// Returns provenance information, including the operation, user, timestamp, etc., for each write to a table.
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="limit">The number of latest commits to retrieve. Set to 0 (default) to retrieve all history.</param>
    /// <param name="cloudOptions">Cloud storage credentials.</param>
    /// <returns>A DataFrame containing the commit history.</returns>
    static member History(
        path: string,
        ?limit: int,
        ?cloudOptions: CloudOptions
    ) : DataFrame =
        let pLimit = defaultArg limit 0
        let _, _, _, _, _, _, cKeys, cVals = CloudOptions.ParseCloudOptions cloudOptions
        
        // Fetch JSON string from Rust core
        let json = PolarsWrapper.History(path, pLimit, cKeys, cVals)
        let buffer = System.Text.Encoding.UTF8.GetBytes json
        
        let mutable df = DataFrame.ReadJson(buffer, jsonFormat = JsonFormat.Json, inferSchemaLen = 2000UL)

        // =========================================================
        // Post-Processing
        // =========================================================
        let cols = df.ColumnNames |> Seq.toList

        // i64 -> Datetime
        if Seq.contains "timestamp" cols then
            let expr = Expr.Col("timestamp").Cast(DataType.Datetime(TimeUnit.Milliseconds, Some "UCT")).Alias "timestamp"
            df <- df.WithColumns [ expr :> IColumnExpr ]

        // operationParameters (Struct -> Columns)
        if Seq.contains "operationParameters" cols then
            df <- df.UnnestColumn "operationParameters"

        if Seq.contains "operationMetrics" cols then
            df <- df.UnnestColumn "operationMetrics"

        // Sort by version/timestamp
        if Seq.contains "version" cols then
            df <- df.Sort("version", descending = true)
        else
            df <- df.Sort("timestamp", descending = true)

        // Reorder columns to put priority info first
        let priorityCols = [| "version"; "timestamp"; "operation"; "mode"; "predicate"; "userName" |]
        let currentCols = df.ColumnNames |> Seq.toArray
        
        let selection = 
            priorityCols 
            |> Array.filter (fun c -> Array.contains c currentCols)
        
        let rest = 
            currentCols 
            |> Array.filter (fun c -> not (Array.contains c priorityCols))
            
        let finalCols = Array.append selection rest
        
        df.Select(finalCols |> Array.map Expr.Col |> Array.map (fun x -> x :> IColumnExpr))

    /// <summary>
    /// Optimizes the layout of the Delta table by compacting small files (bin-packing) and optionally applying Z-Order clustering.
    /// <para>
    /// This operation significantly improves read performance by reducing the number of files and co-locating related data.
    /// </para>
    /// <para>
    /// Note: If Deletion Vectors (DV) are enabled on the table, any soft-deleted rows tracked by the vectors 
    /// will be physically removed (materialized) from the newly compacted Parquet files, effectively clearing 
    /// the deletion vectors for the optimized partitions.
    /// </para>
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="targetSizeMb">The target file size in Megabytes for the compacted files (default: 128 MB).</param>
    /// <param name="partitionFilters">
    /// Optional sequence of partition key-value pairs to restrict optimization to specific partitions.
    /// <para>Example: <c>[ ("date", "2024-01-01") ]</c></para>
    /// </param>
    /// <param name="zOrderColumns">
    /// Optional sequence of column names to apply Z-Order clustering. 
    /// <para>Z-Ordering co-locates data based on these columns, significantly speeding up queries that filter on them.</para>
    /// </param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration.</param>
    /// <returns>The number of new data files created during the optimization process.</returns>
    static member Optimize(
        path: string,
        ?targetSizeMb: int64,
        ?partitionFilters: seq<string * string>,
        ?zOrderColumns: seq<string>,
        ?cloudOptions: CloudOptions
    ) : int64 =
        // 1. Validation
        if System.String.IsNullOrWhiteSpace path then
            invalidArg "path" "Path cannot be empty."

        let pTargetSize = defaultArg targetSizeMb 128L
        if pTargetSize <= 0L then
            invalidArg "targetSizeMb" "Target size must be greater than 0 MB."

        // 2. Prepare Parameters
        let filterJson = 
            match partitionFilters with
            | Some filters -> 
                let d = dict filters // Convert F# seq to IDictionary
                if d.Count > 0 then System.Text.Json.JsonSerializer.Serialize(d) else null
            | None -> null

        let zOrderColsArr = 
            match zOrderColumns with
            | Some cols -> 
                let arr = cols |> Seq.toArray
                if arr.Length > 0 then arr else null
            | None -> null

        // 3. Parse Cloud Options
        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        // 4. Call Wrapper
        let result = PolarsWrapper.Optimize(
            path,
            pTargetSize,
            filterJson,
            zOrderColsArr,
            cProv,
            cRet,
            cToMs,
            cInitMs,
            cMaxMs,
            cCache,
            cKeys,
            cVals
        )

        int64 result
    /// <summary>
    /// Enables a specific feature on the Delta Table.
    /// <para>
    /// WARNING: Enabling features may upgrade the Delta Protocol version (e.g., to Reader v3 / Writer v7).
    /// Older readers may not be able to read the table after this operation.
    /// </para>
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="featureName">The name of the feature to enable (use <see cref="DeltaTableFeatures"/> constants).</param>
    /// <param name="allowProtocolIncrease">
    /// If true, allows the operation to upgrade the table's Protocol Version if the feature requires it.
    /// If false, and the current protocol is too low, the operation will fail.
    /// </param>
    /// <param name="cloudOptions">Cloud storage credentials.</param>
    static member AddFeature(
        path: string,
        featureName: string,
        ?allowProtocolIncrease: bool,
        ?cloudOptions: CloudOptions
    ) =
        let pAllow = defaultArg allowProtocolIncrease true
        
        // Configuration updates only need cloud auth credentials
        let _, _, _, _, _, _, cKeys, cVals = CloudOptions.ParseCloudOptions cloudOptions

        PolarsWrapper.AddFeature(
            path,
            featureName,
            pAllow,
            cKeys,
            cVals
        )

    /// <summary>
    /// Sets or updates properties on the Delta Table.
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="properties">A sequence of key-value pairs to set.</param>
    /// <param name="raiseIfNotExists">
    /// If true, the operation fails if you try to update a property that doesn't strictly exist (rarely used, default false).
    /// </param>
    /// <param name="cloudOptions">Cloud storage credentials.</param>
    static member SetTableProperties(
        path: string,
        properties: seq<string * string>,
        ?raiseIfNotExists: bool,
        ?cloudOptions: CloudOptions
    ) =
        let propsArr = properties |> Seq.toArray
        if propsArr.Length = 0 then
            invalidArg "properties" "Properties sequence cannot be empty."

        // Elegantly unzip the array of tuples into two separate arrays of keys and values
        let propKeys, propValues = Array.unzip propsArr

        let pRaise = defaultArg raiseIfNotExists false
        
        // Configuration updates only need cloud auth credentials
        let _, _, _, _, _, _, cKeys, cVals = CloudOptions.ParseCloudOptions cloudOptions

        PolarsWrapper.SetTableProperties(
            path,
            propKeys,
            propValues,
            pRaise,
            cKeys,
            cVals
        )

/// <summary>
/// Databricks Unity Catalog Client
/// </summary>
/// <remarks>
/// Init Unity Catalog connection
/// </remarks>
/// <param name="workspaceUrl">Databricks workspaceUrl(Example: https://adb-123.azuredatabricks.net)</param>
/// <param name="bearerToken">Personal Access Token (PAT) or OAuth Token</param>
and UnityCatalog(workspaceUrl: string, bearerToken: string) =
    
    let handle = PolarsWrapper.InitUnityCatalog(workspaceUrl, bearerToken)
    let mutable isDisposed = false

    member internal this.Handle = handle

    /// <summary>
    /// Dispose handle
    /// </summary>
    /// <param name="disposing"></param>
    abstract member Dispose: bool -> unit
    default this.Dispose(disposing: bool) =
        if not isDisposed then
            if disposing then
                if not (isNull (box handle)) then
                    handle.Dispose()
            isDisposed <- true

    interface IDisposable with
        member this.Dispose() =
            this.Dispose true
            GC.SuppressFinalize this

    /// <summary>
    /// Scans a table managed by Unity Catalog into a <see cref="LazyFrame"/>.
    /// This method performs a two-step resolution: 
    /// 1. It fetches the physical storage location and dynamic credentials from Unity Catalog.
    /// 2. It initializes a Polars scan on the underlying Delta files using the resolved credentials.
    /// </summary>
    member this.ScanCatalogTable(
        catalogName: string,
        schemaName: string,
        tableName: string,
        ?version: int64,
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
        ?cloudOptions: CloudOptions) =

        if version.IsSome && datetime.IsSome then
            invalidArg "version" "Cannot specify both 'version' and 'datetime' for Delta Time Travel."

        let parallelStrat = defaultArg parallelStrategy ParallelStrategy.Auto
        let lowMem = defaultArg lowMemory false
        let useStats = defaultArg useStatistics true
        let useGlob = defaultArg glob true
        let doRechunk = defaultArg rechunk false
        let doCache = defaultArg cache true
        let rowIdxOffset = defaultArg rowIndexOffset 0u
        let useHivePart = defaultArg hivePartitioning true
        let parseHiveDates = defaultArg tryParseHiveDates true

        let schemaHandle = match schema with Some s -> s.Handle | None -> null
        let hiveSchemaHandle = match hivePartitionSchema with Some s -> s.Handle | None -> null

        let provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values = 
            CloudOptions.ParseCloudOptions cloudOptions

        let h = PolarsWrapper.ScanCatalogTable(
            this.Handle,
            catalogName,
            schemaName,
            tableName,
            Option.toNullable version,
            Option.toObj datetime,
            Option.toNullable nRows,
            parallelStrat.ToNative(),
            lowMem,
            useStats,
            useGlob,
            doRechunk, 
            doCache,   
            Option.toObj rowIndexName,
            rowIdxOffset,
            Option.toObj includePathColumn,
            schemaHandle,     
            useHivePart,
            hiveSchemaHandle, 
            parseHiveDates,
            provider, 
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        )

        new LazyFrame(h)


    /// <summary>
    /// Sink the LazyFrame to a Unity Catalog managed Delta Lake table with partition discovery.
    /// </summary>
    member this.SinkCatalogTable(
        catalogName: string,
        schemaName: string,
        tableName: string,
        lf: LazyFrame,
        ?partitionBy: Selector,
        ?mode: DeltaSaveMode,
        ?canEvolve: bool,
        ?includeKeys: bool,
        ?keysPreGrouped: bool,
        ?maxRowsPerFile: int,
        ?approxBytesPerFile: int64,
        ?compression: ParquetCompression,
        ?compressionLevel: int,
        ?statistics: bool,
        ?rowGroupSize: uint32,
        ?dataPageSize: uint32,
        ?compatLevel: int,
        ?maintainOrder: bool,
        ?syncOnClose: SyncOnClose,
        ?mkdir: bool,
        ?cloudOptions: CloudOptions) =

        let saveMode = defaultArg mode DeltaSaveMode.Append
        let evolve = defaultArg canEvolve false
        let incKeys = defaultArg includeKeys true
        let preGrouped = defaultArg keysPreGrouped false
        let maxRows = defaultArg maxRowsPerFile 0
        let approxBytes = defaultArg approxBytesPerFile 0L
        let comp = defaultArg compression ParquetCompression.Snappy
        let compLevel = defaultArg compressionLevel -1
        let stats = defaultArg statistics true
        let rowGrpSize = defaultArg rowGroupSize 0u
        let dataPgSize = defaultArg dataPageSize 0u
        let compat = defaultArg compatLevel -1
        let order = defaultArg maintainOrder true
        let syncClose = defaultArg syncOnClose SyncOnClose.NoSync
        let makeDir = defaultArg mkdir false

        let provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values = 
            CloudOptions.ParseCloudOptions cloudOptions

        use partitionByH = 
            match partitionBy with 
            | Some p -> p.CloneHandle() 
            | None -> null

        let actualMaxRows = if maxRows > 0 then unativeint maxRows else 0un
        let actualApproxBytes = if approxBytes > 0L then uint64 approxBytes else 0UL

        PolarsWrapper.SinkCatalogTable(
            this.Handle,
            catalogName,
            schemaName,
            tableName,
            lf.Handle,
            // --- Delta Options ---
            saveMode.ToNative(), 
            evolve,
            // --- Partition Params ---
            partitionByH,
            incKeys,
            preGrouped,
            actualMaxRows,
            actualApproxBytes,
            // --- Parquet Options ---
            comp.ToNative(),
            compLevel,
            stats,
            (if rowGrpSize > 0u then unativeint rowGrpSize else 0un),
            (if dataPgSize > 0u then unativeint dataPgSize else 0un),
            compat, 
            // --- Unified Options ---
            order,
            syncClose.ToNative(),
            makeDir,
            // --- Cloud Params ---
            provider,
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        )
    /// <summary>
    /// Initializes a MERGE (Upsert) operation against a Unity Catalog table using a <see cref="LazyFrame"/> as the data source.
    /// This method provides an ACID-compliant way to perform atomic updates, inserts, or deletes by joining the source data with the target table.
    /// </summary>
    /// <remarks>
    /// This is a builder pattern. No data is modified until <c>.Execute()</c> is called on the resulting <see cref="DeltaMergeBuilder"/>.
    /// The operation performs a join between the source and target based on the <paramref name="mergeKeys"/>.
    /// </remarks>
    member this.MergeCatalogRecords(
        catalogName: string,
        schemaName: string,
        tableName: string,
        sourceData: LazyFrame,
        mergeKeys: string array,
        ?canEvolve: bool,
        ?cloudOptions: CloudOptions) =

        let evolve = defaultArg canEvolve false

        new DeltaMergeBuilder(
            sourceData, 
            this, 
            catalogName, 
            schemaName, 
            tableName, 
            mergeKeys, 
            evolve, 
            cloudOptions
        )

    /// <summary>
    /// Initializes a MERGE (Upsert) operation against a Unity Catalog table using an in-memory <see cref="DataFrame"/>.
    /// The <paramref name="sourceData"/> is automatically converted to a <see cref="LazyFrame"/> to leverage Polars' query optimization.
    /// </summary>
    /// <remarks>
    /// This is a builder pattern. No data is modified until <c>.Execute()</c> is called on the resulting <see cref="DeltaMergeBuilder"/>.
    /// </remarks>
    member this.MergeCatalogRecords(
        catalogName: string,
        schemaName: string,
        tableName: string,
        sourceData: DataFrame,
        mergeKeys: string array,
        ?canEvolve: bool,
        ?cloudOptions: CloudOptions) =
        
        this.MergeCatalogRecords(
            catalogName, 
            schemaName, 
            tableName, 
            sourceData.Lazy(), 
            mergeKeys, 
            ?canEvolve = canEvolve, 
            ?cloudOptions = cloudOptions
        )
    /// <summary>
    /// Creates a new table registration within the Unity Catalog.
    /// </summary>
    /// <param name="catalogName">The name of the catalog where the table will be created.</param>
    /// <param name="schemaName">The name of the schema (database) within the catalog.</param>
    /// <param name="tableName">The name of the table to be created.</param>
    /// <param name="polarsSchema">The structural definition of the table columns and data types via <see cref="PolarsSchema"/>.</param>
    /// <param name="tableType">
    /// The management type of the table. 
    /// <see cref="CatalogTableType.Managed"/>: Unity Catalog manages both metadata and physical data.
    /// <see cref="CatalogTableType.External"/>: Unity Catalog manages metadata, but data resides at a specific <paramref name="storageLocation"/>.
    /// </param>
    /// <param name="storageLocation">
    /// The physical URI for the table data (e.g., s3://my-bucket/path/). 
    /// Required if <paramref name="tableType"/> is set to External; typically null for Managed tables.
    /// </param>
    member this.CreateCatalogTable(
        catalogName: string,
        schemaName: string,
        tableName: string,
        polarsSchema: PolarsSchema,
        ?tableType: CatalogTableType,
        ?storageLocation: string) =

        let actualTableType = defaultArg tableType CatalogTableType.Managed

        PolarsWrapper.CreateCatalogTable(
            this.Handle,
            catalogName,
            schemaName,
            tableName,
            polarsSchema.Handle,
            actualTableType.ToNative(),
            Option.toObj storageLocation
        )

    /// <summary>
    /// Deletes (drops) a table from the Unity Catalog. 
    /// For Managed tables, this typically removes both the metadata and the underlying physical data. 
    /// For External tables, only the metadata registration is removed, leaving the physical data intact in your cloud storage.
    /// </summary>
    /// <param name="catalogName">The name of the catalog containing the table.</param>
    /// <param name="schemaName">The name of the schema (database) containing the table.</param>
    /// <param name="tableName">The name of the table to be deleted.</param>
    member this.DeleteCatalogTable(
        catalogName: string,
        schemaName: string,
        tableName: string) =

        PolarsWrapper.DeleteCatalogTable(
            this.Handle,
            catalogName,
            schemaName,
            tableName
        )
    /// <summary>
    /// Deletes records from a Unity Catalog table that match the specified predicate.
    /// This operation is ACID-compliant and leverages the underlying Delta Lake protocol. 
    /// If Deletion Vectors (DVs) are enabled on the table, it performs a metadata-only delete; 
    /// otherwise, it performs a physical rewrite of the affected data files.
    /// </summary>
    /// <param name="catalogName">The name of the catalog containing the table.</param>
    /// <param name="schemaName">The name of the schema (database) containing the table.</param>
    /// <param name="tableName">The name of the table from which records will be removed.</param>
    /// <param name="predicate">A boolean <see cref="Expr"/> defining the condition for rows to be deleted.</param>
    /// <param name="cloudOptions">Cloud-specific configurations including storage credentials and retry policies for concurrent conflicts.</param>
    member this.DeleteCatalogRecords(
        catalogName: string,
        schemaName: string,
        tableName: string,
        predicate: Expr,
        ?cloudOptions: CloudOptions) =

        let provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values = 
            CloudOptions.ParseCloudOptions cloudOptions

        use clonedPredicate = predicate.CloneHandle()

        PolarsWrapper.DeleteCatalogRecords(
            this.Handle,
            catalogName,
            schemaName,
            tableName,
            clonedPredicate,
            provider, 
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        )

    /// <summary>
    /// Optimizes the storage layout of a Unity Catalog Delta table by compacting small files and optionally applying Z-Order clustering.
    /// This operation significantly improves read performance by reducing file-open overhead and enabling efficient data skipping.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Compaction:</b> Groups small Parquet files into larger ones based on the target size.
    /// </para>
    /// <para>
    /// <b>Z-Ordering:</b> If <paramref name="zOrderColumns"/> are provided, the data is rearranged into multidimensional clusters. 
    /// </para>
    /// <para>
    /// <b>Deletion Vector (DV) Materialization:</b> If the table uses DVs, this operation physically removes any soft-deleted rows.
    /// </para>
    /// </remarks>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table to optimize.</param>
    /// <param name="targetSizeMb">The target size for the compacted Parquet files in Megabytes. Default is 128 MB.</param>
    /// <param name="partitionFilters">Optional filters to limit the optimization to specific partitions. If null, the entire table is processed.</param>
    /// <param name="zOrderColumns">An optional collection of column names to use for Z-Order clustering.</param>
    /// <param name="cloudOptions">Cloud-specific configurations, including storage credentials and retry logic.</param>
    /// <returns>The total number of files that were successfully compacted/optimized during the operation.</returns>
    member this.OptimizeCatalogTable(
        catalogName: string,
        schemaName: string,
        tableName: string,
        ?targetSizeMb: int64,
        ?partitionFilters: Map<string, string>,
        ?zOrderColumns: seq<string>,
        ?cloudOptions: CloudOptions) =

        let targetSize = defaultArg targetSizeMb 128L
        if targetSize <= 0L then
            invalidArg "targetSizeMb" "Target size must be greater than 0 MB."

        let filterJson = 
            match partitionFilters with
            | Some filters when not filters.IsEmpty -> 
                System.Text.Json.JsonSerializer.Serialize(filters |> Map.toSeq |> dict)
            | _ -> null

        let zOrderColsArr =
            match zOrderColumns with
            | Some cols -> 
                let arr = Seq.toArray cols
                if arr.Length = 0 then null else arr
            | None -> null

        let provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values = 
            CloudOptions.ParseCloudOptions cloudOptions

        let result = PolarsWrapper.CatalogOptimize(
            this.Handle,
            catalogName, 
            schemaName, 
            tableName, 
            targetSize,
            filterJson,
            zOrderColsArr,
            provider,
            retries,
            timeout,
            initBackoff,
            maxBackoff,
            cacheTtl,
            keys,
            values
        )

        int64 result

    /// <summary>
    /// Physically deletes data files that are no longer referenced by the Delta table and are older than the specified retention threshold.
    /// This operation helps reduce storage costs and ensures compliance by removing historical data that is no longer needed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Warning:</b> This is a destructive operation. Once files are vacuumed, you can no longer "Time Travel" to versions 
    /// that relied on those files.
    /// </para>
    /// <para>
    /// <b>Retention Period:</b> By default, Delta Lake prevents deleting files younger than 168 hours (7 days) to protect 
    /// active readers and concurrent transactions.
    /// </para>
    /// </remarks>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table to vacuum.</param>
    /// <param name="retentionHours">
    /// The number of hours to retain historical files. Files modified within this window will not be deleted. 
    /// If null, the table's default retention setting is used.
    /// </param>
    /// <param name="enforceRetention">
    /// If true, the operation will fail if <paramref name="retentionHours"/> is less than the table's minimum retention 
    /// (usually 168h). Set to false only for emergency cleanup, at the risk of corrupting active readers.
    /// </param>
    /// <param name="dryRun">If true, the method returns the number of files that *would* be deleted without actually removing them.</param>
    /// <param name="vacuumModeFull">
    /// If true, performs a full scan of the object store to find unreferenced files. 
    /// If false (Lite mode), uses the Delta log to identify files to be removed.
    /// </param>
    /// <param name="cloudOptions">Cloud-specific configurations and credentials resolved via Unity Catalog.</param>
    /// <returns>The total number of physical files deleted from storage (or identified for deletion if <paramref name="dryRun"/> is true).</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="retentionHours"/> is a negative value.</exception>
    member this.DeltaVacuum(
        catalogName: string,
        schemaName: string,
        tableName: string,
        ?retentionHours: int,
        ?enforceRetention: bool,
        ?dryRun: bool,
        ?vacuumModeFull: bool,
        ?cloudOptions: CloudOptions) =

        let _, _, _, _, _, _, keys, values = CloudOptions.ParseCloudOptions cloudOptions
        
        match retentionHours with
        | Some hours when hours < 0 -> invalidArg "retentionHours" "Retention hours cannot be negative."
        | _ -> ()

        let retentionArg = defaultArg retentionHours -1
        let enforce = defaultArg enforceRetention true
        let dry = defaultArg dryRun false
        let fullMode = defaultArg vacuumModeFull false

        PolarsWrapper.CatalogVacuum(
            this.Handle,
            catalogName,
            schemaName,
            tableName,
            retentionArg,
            enforce,
            dry,
            fullMode,
            keys,
            values
        )

    /// <summary>
    /// Restores a Unity Catalog Delta table to a previous state based on a version number or a timestamp.
    /// This operation is atomic and ACID-compliant.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>How it works:</b> Instead of physically deleting newer data, Restore creates a new commit in the transaction log 
    /// that reverts the table's metadata and file references to the target state. 
    /// </para>
    /// <para>
    /// <b>Safety:</b> This operation will fail if any required data files have been physically removed (e.g., by a <c>Vacuum</c> operation) 
    /// unless <paramref name="ignoreMissingFiles"/> is set to true.
    /// </para>
    /// </remarks>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table to restore.</param>
    /// <param name="version">
    /// The specific Delta table version to restore to. 
    /// Mutually exclusive with <paramref name="timestamp"/>.
    /// </param>
    /// <param name="timestamp">
    /// The specific <see cref="DateTime"/> to restore the table state to. 
    /// Mutually exclusive with <paramref name="version"/>.
    /// </param>
    /// <param name="ignoreMissingFiles">
    /// If true, allows the restore to proceed even if some underlying data files are missing from storage. 
    /// Use with caution as this may result in a partial restoration.
    /// </param>
    /// <param name="protocolDowngradeAllowed">
    /// If true, allows the table to be restored to a version that requires a lower protocol reader/writer version 
    /// than the current one.
    /// </param>
    /// <param name="cloudOptions">Cloud-specific configurations and credentials resolved via Unity Catalog.</param>
    /// <returns>The new version number of the table after the restore operation is committed.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when both <paramref name="version"/> and <paramref name="timestamp"/> are provided, 
    /// or when neither is provided.
    /// </exception>
    member this.DeltaRestore(
        catalogName: string,
        schemaName: string,
        tableName: string,
        ?version: int64,
        ?timestamp: DateTime,
        ?ignoreMissingFiles: bool,
        ?protocolDowngradeAllowed: bool,
        ?cloudOptions: CloudOptions) =

        match version, timestamp with
        | Some _, Some _ -> invalidArg "version" "Cannot specify both 'version' and 'timestamp' for Restore."
        | None, None -> invalidArg "version" "Must specify either 'version' or 'timestamp' for Restore."
        | _ -> ()

        let targetVer = defaultArg version -1L
        
        let targetTs = 
            match timestamp with
            | Some ts -> 
                let utcTime = ts.ToUniversalTime()
                DateTimeOffset(utcTime).ToUnixTimeMilliseconds()
            | None -> -1L

        let ignoreMissing = defaultArg ignoreMissingFiles false
        let protocolDowngrade = defaultArg protocolDowngradeAllowed false

        let _, _, _, _, _, _, keys, values = CloudOptions.ParseCloudOptions cloudOptions

        PolarsWrapper.CatalogRestore(
            this.Handle,
            catalogName,
            schemaName,
            tableName,
            targetVer,
            targetTs,
            ignoreMissing,
            protocolDowngrade,
            keys,
            values
        )
    /// <summary>
    /// Retrieves the commit history (audit trail) for a Unity Catalog Delta table as a <see cref="DataFrame"/>.
    /// This includes metadata about who performed what operation, when it occurred, and what parameters were used.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Data Processing:</b> The raw JSON history from the Delta log is automatically processed:
    /// <list type="bullet">
    /// <item><description>Timestamps are cast to UTC <see cref="DataType.Datetime"/>.</description></item>
    /// <item><description>Complex fields like <c>operationMetrics</c> and <c>operationParameters</c> are unnested into flat columns.</description></item>
    /// <item><description>Columns are reordered to place essential information (version, timestamp, operation) at the beginning.</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// <b>Note on Versioning:</b> For standard DML operations, the version column might be null in the raw log; 
    /// rows are sorted by version or timestamp in descending order (latest first) by default.
    /// </para>
    /// </remarks>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table to audit.</param>
    /// <param name="limit">The maximum number of history commits to retrieve. If 0, retrieves all available history.</param>
    /// <param name="cloudOptions">Cloud-specific configurations and credentials resolved via Unity Catalog.</param>
    /// <returns>A <see cref="DataFrame"/> containing the flattened and formatted history of the table.</returns>
    member this.DeltaHistory(
        catalogName: string,
        schemaName: string,
        tableName: string,
        ?limit: int,
        ?cloudOptions: CloudOptions) : DataFrame =

        let pLimit = defaultArg limit 0
        let _, _, _, _, _, _, keys, values = CloudOptions.ParseCloudOptions cloudOptions

        // Fetch JSON string from Rust core via Unity Catalog
        let json = PolarsWrapper.CatalogHistory(this.Handle, catalogName, schemaName, tableName, pLimit, keys, values)
        let buffer = System.Text.Encoding.UTF8.GetBytes json
        
        let mutable df = DataFrame.ReadJson(buffer, jsonFormat = JsonFormat.Json, inferSchemaLen = 2000UL)

        // =========================================================
        // Post-Processing
        // =========================================================
        let cols = df.ColumnNames |> Seq.toList

        // i64 -> Datetime
        if Seq.contains "timestamp" cols then
            let expr = Expr.Col("timestamp").Cast(DataType.Datetime(TimeUnit.Milliseconds, Some "UCT")).Alias("timestamp")
            df <- df.WithColumns [ expr :> IColumnExpr ]

        // operationMetrics (Struct -> Columns)
        if Seq.contains "operationMetrics" cols then
            df <- df.UnnestColumn "operationMetrics"

        // operationParameters (Struct -> Columns)
        if Seq.contains "operationParameters" cols then
            df <- df.UnnestColumn "operationParameters"

        // Sort by version/timestamp
        if Seq.contains "version" cols then
            df <- df.Sort("version", descending = true)
        else
            df <- df.Sort("timestamp", descending = true)

        // Reorder columns to put priority info first
        let priorityCols = [| "version"; "timestamp"; "operation"; "mode"; "predicate"; "userName" |]
        let currentCols = df.ColumnNames |> Seq.toArray
        
        let selection = 
            priorityCols 
            |> Array.filter (fun c -> Array.contains c currentCols)
        
        let rest = 
            currentCols 
            |> Array.filter (fun c -> not (Array.contains c priorityCols))
            
        let finalCols = Array.append selection rest
        
        df.Select(finalCols |> Array.map Expr.Col |> Array.map (fun x -> x :> IColumnExpr))

/// <summary>
/// Builder for Delta Merge (Upsert) operations.
/// Supports both local paths and Unity Catalog tables.
/// </summary>
and DeltaMergeBuilder private (
    sourceLf: LazyFrame,
    mergeKeys: string array,
    canEvolve: bool,
    cloudOptions: CloudOptions option,
    path: string option,
    catalog: UnityCatalog option,
    catalogName: string option,
    schemaName: string option,
    tableName: string option
) =
    let actions = ResizeArray<MergeActionType * Expr>()

    /// <summary>
    /// Physical Path Merge Builder
    /// </summary>
    internal new(sourceLf: LazyFrame, path: string, mergeKeys: string array, canEvolve: bool, cloudOptions: CloudOptions option) =
        new DeltaMergeBuilder(sourceLf, mergeKeys, canEvolve, cloudOptions, Some path, None, None, None, None)

    /// <summary>
    /// Unity Catalog Merge Builder
    /// </summary>
    internal new(sourceLf: LazyFrame, catalog: UnityCatalog, catalogName: string, schemaName: string, tableName: string, mergeKeys: string array, canEvolve: bool, cloudOptions: CloudOptions option) =
        new DeltaMergeBuilder(sourceLf, mergeKeys, canEvolve, cloudOptions, None, Some catalog, Some catalogName, Some schemaName, Some tableName)

    /// <summary>
    /// Update the matched target row with source data.
    /// </summary>
    member this.WhenMatchedUpdate(?condition: Expr) =
        actions.Add((MergeActionType.MatchedUpdate, defaultArg condition (pl.lit true)))
        this

    /// <summary>
    /// Delete the matched target row.
    /// </summary>
    member this.WhenMatchedDelete(?condition: Expr) =
        actions.Add((MergeActionType.MatchedDelete, defaultArg condition (pl.lit true)))
        this

    /// <summary>
    /// Insert a new row from the source data when it does not match any target row.
    /// </summary>
    member this.WhenNotMatchedInsert(?condition: Expr) =
        actions.Add((MergeActionType.NotMatchedInsert, defaultArg condition (pl.lit true)))
        this

    /// <summary>
    /// Delete the target row when it does not exist in the source data.
    /// </summary>
    member this.WhenNotMatchedBySourceDelete(?condition: Expr) =
        actions.Add((MergeActionType.NotMatchedBySourceDelete, defaultArg condition (pl.lit true)))
        this

    /// <summary>
    /// Executes the constructed merge operation against the Delta Table.
    /// </summary>
    member this.Execute() =
        if actions.Count = 0 then
            this.WhenMatchedUpdate() |> ignore
            this.WhenNotMatchedInsert() |> ignore

        let actionTypes = actions |> Seq.map (fun (t, _) -> t.ToNative()) |> Seq.toArray
        let actionExprs = actions |> Seq.map (fun (_, expr) -> expr.CloneHandle()) |> Seq.toArray

        let cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals = 
            CloudOptions.ParseCloudOptions cloudOptions

        match catalog with
        | Some cat ->
            PolarsWrapper.CatalogMergeOrdered(
                cat.Handle,
                catalogName.Value,
                schemaName.Value,
                tableName.Value,
                sourceLf.CloneHandle(),
                mergeKeys,
                actionTypes,
                actionExprs,
                canEvolve,
                cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals
            )
        | None ->
            PolarsWrapper.DeltaMergeOrdered(
                sourceLf.CloneHandle(), 
                path.Value,
                mergeKeys,
                actionTypes,
                actionExprs,
                canEvolve,
                cProv, cRet, cToMs, cInitMs, cMaxMs, cCache, cKeys, cVals
            )