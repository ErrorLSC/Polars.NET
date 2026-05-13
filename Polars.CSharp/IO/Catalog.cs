#pragma warning disable CS1573
using System.Text;
using System.Text.Json;
using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Databricks Unity Catalog Client
/// </summary>
/// <remarks>
/// Init Unity Catalog connection
/// </remarks>
/// <param name="workspaceUrl">Databricks workspaceUrl(Example: https://adb-123.azuredatabricks.net)</param>
/// <param name="bearerToken">Personal Access Token (PAT) or OAuth Token</param>
public class UnityCatalog(string workspaceUrl, string bearerToken) : IDisposable
{
    internal CatalogHandle Handle { get;  } = PolarsWrapper.InitUnityCatalog(workspaceUrl, bearerToken);
    private bool _isDisposed;

    /// <summary>
    /// Scans a table managed by Unity Catalog into a <see cref="LazyFrame"/>.
    /// This method performs a two-step resolution: 
    /// 1. It fetches the physical storage location and dynamic credentials from Unity Catalog.
    /// 2. It initializes a Polars scan on the underlying Delta files using the resolved credentials.
    /// </summary>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table to scan.</param>
    /// <inheritdoc cref="LazyFrame.ScanDelta"/>
    /// <returns>A <see cref="LazyFrame"/> for lazy execution of the catalog table scan.</returns>
    /// <exception cref="ArgumentException">Thrown when both <paramref name="version"/> and <paramref name="datetime"/> are provided.</exception>
    public LazyFrame ScanCatalogTable(
        string catalogName,
        string schemaName,
        string tableName,
        ulong? version = null,
        string? datetime = null,
        ulong? nRows = null,
        ParallelStrategy parallel = ParallelStrategy.Auto,
        bool lowMemory = false,
        bool useStatistics = true,
        bool glob = true,
        bool rechunk = false, 
        bool cache = true,    
        string? rowIndexName = null,
        uint rowIndexOffset = 0,
        string? includePathColumn = null,
        IntoSchema? schema = null,
        bool hivePartitioning = true,
        IntoSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = true,
        CloudOptions? cloudOptions = null)
    {
        if (version.HasValue && datetime != null)
        {
            throw new ArgumentException("Cannot specify both 'version' and 'datetime' for Delta Time Travel.");
        }

        var schemaHandle = schema?.Consume().Handle;
        var hiveSchemaHandle = hivePartitionSchema?.Consume().Handle;

        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        var h = PolarsWrapper.ScanCatalogTable(
            Handle,
            catalogName,
            schemaName,
            tableName,
            version,
            datetime,
            nRows,
            parallel.ToNative(),
            lowMemory,
            useStatistics,
            glob,
            rechunk, 
            cache,   
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schemaHandle,     
            hivePartitioning,
            hiveSchemaHandle, 
            tryParseHiveDates,
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        );

        return new LazyFrame(h);
    }
    /// <summary>
    /// Sink the LazyFrame to a Unity Catalog managed Delta Lake table with partition discovery.
    /// </summary>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table to sink.</param>
    /// <param name="lf">The LazyFrame ready to be sinked.</param>
    /// <inheritdoc cref="LazyFrame.SinkDelta(string, IntoSelector?, DeltaSaveMode, bool, bool, bool, int, long, ParquetCompression, int, bool, uint, uint, int, bool, SyncOnClose, bool, CloudOptions?)"/>
    public void SinkCatalogTable(
        string catalogName,
        string schemaName,
        string tableName,
        LazyFrame lf,
        IntoSelector? partitionBy = null,
        DeltaSaveMode mode = DeltaSaveMode.Append,
        bool canEvolve=false,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        ParquetCompression compression = ParquetCompression.Snappy,
        int compressionLevel = -1,
        bool statistics = true, 
        uint rowGroupSize = 0,
        uint dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = 
            CloudOptions.ParseCloudOptions(cloudOptions);
        using var partitionByH = partitionBy?.Consume().CloneHandle(); 
        PolarsWrapper.SinkCatalogTable(
            Handle,
            catalogName,
            schemaName,
            tableName,
            lf.Handle,
            // --- Delta Options ---
            mode.ToNative(), 
            canEvolve,
            // --- Partition Params ---
            partitionByH,
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile > 0 ? (nuint)maxRowsPerFile : 0,
            approxBytesPerFile > 0 ? (ulong)approxBytesPerFile : 0,

            // --- Parquet Options ---
            compression.ToNative(),
            compressionLevel,
            statistics,
            rowGroupSize > 0 ? rowGroupSize : 0,
            dataPageSize > 0 ? dataPageSize : 0,
            compatLevel, 

            // --- Unified Options ---
            maintainOrder,
            syncOnClose.ToNative(),
            mkdir,

            // --- Cloud Params ---
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        );
    }
    /// <summary>
    /// Creates a new table registration within the Unity Catalog.
    /// </summary>
    /// <param name="catalogName">The name of the catalog where the table will be created.</param>
    /// <param name="schemaName">The name of the schema (database) within the catalog.</param>
    /// <param name="tableName">The name of the table to be created.</param>
    /// <param name="IntoSchema">The structural definition of the table columns and data types via <see cref="IntoSchema"/>.</param>
    /// <param name="tableType">
    /// The management type of the table. 
    /// <see cref="CatalogTableType.Managed"/>: Unity Catalog manages both metadata and physical data.
    /// <see cref="CatalogTableType.External"/>: Unity Catalog manages metadata, but data resides at a specific <paramref name="storageLocation"/>.
    /// </param>
    /// <param name="storageLocation">
    /// The physical URI for the table data (e.g., s3://my-bucket/path/). 
    /// Required if <paramref name="tableType"/> is set to External; typically null for Managed tables.
    /// </param>
    public void CreateCatalogTable(
        string catalogName,
        string schemaName,
        string tableName,
        IntoSchema IntoSchema,
        CatalogTableType tableType = CatalogTableType.Managed,
        string? storageLocation = null
    )
    => PolarsWrapper.CreateCatalogTable(
        Handle,
        catalogName,
        schemaName,
        tableName,
        IntoSchema.Consume().Handle,
        tableType.ToNative(),
        storageLocation);
    /// <summary>
    /// Deletes (drops) a table from the Unity Catalog. 
    /// For Managed tables, this typically removes both the metadata and the underlying physical data. 
    /// For External tables, only the metadata registration is removed, leaving the physical data intact in your cloud storage.
    /// </summary>
    /// <param name="catalogName">The name of the catalog containing the table.</param>
    /// <param name="schemaName">The name of the schema (database) containing the table.</param>
    /// <param name="tableName">The name of the table to be deleted.</param>
    public void DeleteCatalogTable(
        string catalogName,
        string schemaName,
        string tableName
    )
        => PolarsWrapper.DeleteCatalogTable(
            Handle,
            catalogName,
            schemaName,
            tableName);
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
    public void DeleteCatalogRecords(
        string catalogName,
        string schemaName,
        string tableName,
        Expr predicate,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        using var clonedPredicate = predicate.CloneHandle();

        PolarsWrapper.DeleteCatalogRecords(
            Handle,
            catalogName,
            schemaName,
            tableName,
            clonedPredicate,
            provider.ToNative(),
            retries,
            retryTimeoutMs,
            retryInitBackoffMs,
            retryMaxBackoffMs,
            cacheTtl,
            keys,
            values
        );
    }
    /// <summary>
    /// Initializes a MERGE (Upsert) operation against a Unity Catalog table using a <see cref="LazyFrame"/> as the data source.
    /// This method provides an ACID-compliant way to perform atomic updates, inserts, or deletes by joining the source data with the target table.
    /// </summary>
    /// <remarks>
    /// This is a builder pattern. No data is modified until <c>.Execute()</c> is called on the resulting <see cref="DeltaMergeBuilder"/>.
    /// The operation performs a join between the source and target based on the <paramref name="mergeKeys"/>.
    /// </remarks>
    /// <param name="catalogName">The Unity Catalog name (e.g., "main").</param>
    /// <param name="schemaName">The schema/database name within the catalog.</param>
    /// <param name="tableName">The target Delta table name registered in the catalog.</param>
    /// <param name="sourceData">A <see cref="LazyFrame"/> containing the data to be merged into the target table.</param>
    /// <param name="mergeKeys">An array of column names used as join keys to match source rows with target rows.</param>
    /// <param name="canEvolve">If true, enables Schema Evolution, allowing the target table's schema to be updated if the source contains new columns.</param>
    /// <param name="cloudOptions">Cloud-specific configurations (S3/Azure/GCS) including dynamic credentials resolved via Unity Catalog.</param>
    /// <returns>A <see cref="DeltaMergeBuilder"/> used to configure "WhenMatched" and "WhenNotMatched" clauses.</returns>
    public DeltaMergeBuilder MergeCatalogRecords(
        string catalogName,
        string schemaName,
        string tableName,
        LazyFrame sourceData,
        string[] mergeKeys,
        bool canEvolve = false,
        CloudOptions? cloudOptions = null)
    {
        return new DeltaMergeBuilder(
            sourceData, 
            this, 
            catalogName, 
            schemaName, 
            tableName, 
            mergeKeys, 
            canEvolve, 
            cloudOptions
        );
    }
    /// <summary>
    /// Initializes a MERGE (Upsert) operation against a Unity Catalog table using an in-memory <see cref="DataFrame"/>.
    /// The <paramref name="sourceData"/> is automatically converted to a <see cref="LazyFrame"/> to leverage Polars' query optimization.
    /// </summary>
    /// <remarks>
    /// This is a builder pattern. No data is modified until <c>.Execute()</c> is called on the resulting <see cref="DeltaMergeBuilder"/>.
    /// </remarks>
    /// <param name="catalogName">The Unity Catalog name (e.g., "main").</param>
    /// <param name="schemaName">The schema/database name within the catalog.</param>
    /// <param name="tableName">The target Delta table name registered in the catalog.</param>
    /// <param name="sourceData">A <see cref="DataFrame"/> containing the data to be merged.</param>
    /// <param name="mergeKeys">An array of column names used as join keys to match source rows with target rows.</param>
    /// <param name="canEvolve">If true, enables Schema Evolution to handle new columns in the source data.</param>
    /// <param name="cloudOptions">Cloud-specific configurations and credentials.</param>
    /// <returns>A <see cref="DeltaMergeBuilder"/> used to configure "WhenMatched" and "WhenNotMatched" clauses.</returns>
    public DeltaMergeBuilder MergeCatalogRecords(
        string catalogName,
        string schemaName,
        string tableName,
        DataFrame sourceData,
        string[] mergeKeys,
        bool canEvolve = false,
        CloudOptions? cloudOptions = null)
    {
        return new DeltaMergeBuilder(
            sourceData.Lazy(), 
            this, 
            catalogName, 
            schemaName, 
            tableName, 
            mergeKeys, 
            canEvolve, 
            cloudOptions
        );
    }
    /// <summary>
    /// Optimizes the storage layout of a Unity Catalog Delta table by compacting small files and optionally applying Z-Order clustering.
    /// This operation significantly improves read performance by reducing file-open overhead and enabling efficient data skipping.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Compaction:</b> Groups small Parquet files into larger ones based on the <paramref name="targetSizeMb"/>.
    /// </para>
    /// <para>
    /// <b>Z-Ordering:</b> If <paramref name="zOrderColumns"/> are provided, the data is rearranged into multidimensional clusters. 
    /// This co-locates related information, allowing the engine to skip entire files during queries that filter on these columns.
    /// </para>
    /// <para>
    /// <b>Deletion Vector (DV) Materialization:</b> If the table uses DVs, this operation physically removes any soft-deleted rows 
    /// tracked by the vectors. The newly written files will be "clean," and their associated deletion vectors will be removed.
    /// </para>
    /// </remarks>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table to optimize.</param>
    /// <param name="targetSizeMb">The target size for the compacted Parquet files in Megabytes. Default is 128 MB.</param>
    /// <param name="partitionFilters">
    /// Optional filters to limit the optimization to specific partitions (e.g., <c>{"date": "2024-01-01"}</c>). 
    /// If null, the entire table is processed.
    /// </param>
    /// <param name="zOrderColumns">An optional collection of column names to use for Z-Order clustering.</param>
    /// <param name="cloudOptions">Cloud-specific configurations, including storage credentials and retry logic for concurrent commits.</param>
    /// <returns>The total number of files that were successfully compacted/optimized during the operation.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="targetSizeMb"/> is less than or equal to 0.</exception>
    public long OptimizeCatalogTable(
        string catalogName,
        string schemaName,
        string tableName,
        long targetSizeMb = 128,
        Dictionary<string, string>? partitionFilters = null,
        IEnumerable<string>? zOrderColumns = null,
        CloudOptions? cloudOptions = null)
    {
        if (targetSizeMb <= 0)
            throw new ArgumentException("Target size must be greater than 0 MB.", nameof(targetSizeMb));

        // Serialize partition filters to JSON string for the Rust backend
        string? filterJson = null;
        if (partitionFilters != null && partitionFilters.Count > 0)
        {
            filterJson = JsonSerializer.Serialize(partitionFilters);
        }

        // Convert Z-Order columns to array (Rust FFI expects string[] or null)
        string[]? zOrderColsArr = zOrderColumns?.ToArray();
        if (zOrderColsArr != null && zOrderColsArr.Length == 0)
        {
            zOrderColsArr = null;
        }

        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) = 
            CloudOptions.ParseCloudOptions(cloudOptions);

        ulong result = PolarsWrapper.CatalogOptimize(
            Handle,
            catalogName, 
            schemaName, 
            tableName, 
            targetSizeMb,
            filterJson,
            zOrderColsArr,
            provider.ToNative(),
            retries,
            timeout,
            initBackoff,
            maxBackoff,
            cacheTtl,
            keys,
            values
        );

        return (long)result;
    }
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
    public long DeltaVacuum(
        string catalogName,
        string schemaName,
        string tableName,
        int? retentionHours = null,
        bool enforceRetention = true,
        bool dryRun = false,
        bool vacuumModeFull = false, 
        CloudOptions? cloudOptions = null)
    {
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);
        int retentionArg = retentionHours ?? -1;
        if (retentionHours.HasValue && retentionHours.Value < 0)
            throw new ArgumentException("Retention hours cannot be negative.", nameof(retentionHours));
        return PolarsWrapper.CatalogVacuum(
            Handle,
            catalogName, 
            schemaName, 
            tableName, 
            retentionArg,
            enforceRetention,
            dryRun,
            vacuumModeFull,
            keys,
            values
        );
    }
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
    public ulong RestoreTable(
        string catalogName,
        string schemaName,
        string tableName,
        ulong? version = null,
        DateTime? timestamp = null,
        bool ignoreMissingFiles = false,
        bool protocolDowngradeAllowed = false,
        CloudOptions? cloudOptions = null)
    {
        if (version.HasValue && timestamp.HasValue)
            throw new ArgumentException("Cannot specify both 'version' and 'timestamp'.");
            
        if (!version.HasValue && !timestamp.HasValue)
            throw new ArgumentException("Must specify either 'version' or 'timestamp'.");

        long targetTs = -1;
        ulong targetVer = 0;

        if (timestamp.HasValue)
        {
            targetTs = new DateTimeOffset(timestamp.Value.ToUniversalTime()).ToUnixTimeMilliseconds();
            if (targetTs < 0)
                throw new ArgumentException("Restore timestamp must be >= 1970-01-01T00:00:00.000Z");
        }
        else if (version.HasValue)
        {
            targetVer = version.Value;
        }

        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        return PolarsWrapper.CatalogRestore(
            Handle,
            catalogName,
            schemaName,
            tableName,
            targetVer,
            targetTs,
            ignoreMissingFiles,
            protocolDowngradeAllowed,
            keys,
            values
        );
    }

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
    public DataFrame DeltaHistory(
        string catalogName,
        string schemaName,
        string tableName,
        int limit = 0,
        CloudOptions? cloudOptions = null)
    {
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);
        string json = PolarsWrapper.CatalogHistory(        
            Handle,
            catalogName, 
            schemaName, 
            tableName, limit, keys, values);
        byte[] buffer = Encoding.UTF8.GetBytes(json);
        
        var df = DataFrame.ReadJson(buffer, jsonFormat: JsonFormat.Json, inferSchemaLen: 2000);

        // =========================================================
        // Post-Processing
        // =========================================================

        // i64 -> Datetime
        if (df.ColumnNames.Contains("timestamp"))
        {
            df = df.WithColumns(
                Polars.Col("timestamp")
                    .Cast(DataType.Datetime(TimeUnit.Milliseconds,"UCT")) 
                    .Alias("timestamp") 
            );
        }
        if (df.ColumnNames.Contains("operationMetrics"))
        {
            df = df.Unnest("operationMetrics");
        }

        // operationParameters (Struct -> Columns)
        if (df.ColumnNames.Contains("operationParameters"))
        {
            df = df.Unnest("operationParameters");
        }

        if (df.ColumnNames.Contains("version"))
        {
            df = df.Sort("version", descending: true);
        }
        else
        {
            df = df.Sort("timestamp", descending: true);
        }

        string[] priorityCols = ["version", "timestamp", "operation", "mode", "predicate", "userName"];
        var existingCols = df.ColumnNames;
        var selection = priorityCols.Where(c => existingCols.Contains(c)).ToList();
        
        selection.AddRange(existingCols.Except(priorityCols));
        
        return df.Select(selection);
    }
    /// <summary>
    /// Dispose handle
    /// </summary>
    /// <param name="disposing"></param>
    protected virtual void Dispose(bool disposing)
    {
        if (!_isDisposed)
        {
            if (disposing)
            {
                Handle?.Dispose();
            }
            
            _isDisposed = true;
        }
    }

    /// <summary>
    /// Dispose handle
    /// </summary>
    public void Dispose()
    {
        Dispose(disposing: true);
        GC.SuppressFinalize(this);
    }
}

public readonly partial struct Polars 
{
    /// <summary>
    /// Init Unity Catalog connection
    /// </summary>
    /// <param name="workspaceUrl">Databricks workspaceUrl(Example: https://adb-123.azuredatabricks.net)</param>
    /// <param name="bearerToken">Personal Access Token (PAT) or OAuth Token</param>
    public static UnityCatalog UnityCatalog(string workspaceUrl, string bearerToken)
        => new(workspaceUrl, bearerToken);
}

/// <summary>
/// 
/// </summary>
public static class UnityCatalogExtensions
{
    /// <summary>
    /// Sinks the <see cref="LazyFrame"/> to a Unity Catalog table.
    /// </summary>
    /// <inheritdoc cref="UnityCatalog.SinkCatalogTable(string, string, string, LazyFrame, IntoSelector?, DeltaSaveMode, bool, bool, bool, int, long, ParquetCompression, int, bool, uint, uint, int, bool, SyncOnClose, bool, CloudOptions?)"/>
    public static void SinkCatalogTable(
        this LazyFrame lf,
        UnityCatalog catalog,
        string catalogName,
        string schemaName,
        string tableName,
        IntoSelector? partitionBy = null,
        DeltaSaveMode mode = DeltaSaveMode.Append,
        bool canEvolve = false,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        ParquetCompression compression = ParquetCompression.Snappy,
        int compressionLevel = -1,
        bool statistics = true, 
        uint rowGroupSize = 0,
        uint dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        catalog.SinkCatalogTable(
            catalogName, schemaName, tableName, lf,
            partitionBy, mode, canEvolve, includeKeys, keysPreGrouped,
            maxRowsPerFile, approxBytesPerFile, compression, compressionLevel,
            statistics, rowGroupSize, dataPageSize, compatLevel,
            maintainOrder, syncOnClose, mkdir, cloudOptions
        );
    }

    /// <summary>
    /// Writes the <see cref="DataFrame"/> into a Unity Catalog table by converting it to a <see cref="LazyFrame"/>.
    /// </summary>
    /// <inheritdoc cref="UnityCatalog.SinkCatalogTable(string, string, string, LazyFrame, IntoSelector?, DeltaSaveMode, bool, bool, bool, int, long, ParquetCompression, int, bool, uint, uint, int, bool, SyncOnClose, bool, CloudOptions?)"/>
    public static void WriteCatalogTable(
        this DataFrame df,
        UnityCatalog catalog,
        string catalogName,
        string schemaName,
        string tableName,
        IntoSelector? partitionBy = null,
        DeltaSaveMode mode = DeltaSaveMode.Append,
        bool canEvolve = false,
        bool includeKeys = true,
        bool keysPreGrouped = false,
        int maxRowsPerFile = 0,
        long approxBytesPerFile = 0,
        ParquetCompression compression = ParquetCompression.Snappy,
        int compressionLevel = -1,
        bool statistics = true, 
        uint rowGroupSize = 0,
        uint dataPageSize = 0,
        int compatLevel = -1,
        bool maintainOrder = true,
        SyncOnClose syncOnClose = SyncOnClose.None,
        bool mkdir = false,
        CloudOptions? cloudOptions = null)
    {
        catalog.SinkCatalogTable(
            catalogName, schemaName, tableName, df.Lazy(),
            partitionBy, mode, canEvolve, includeKeys, keysPreGrouped,
            maxRowsPerFile, approxBytesPerFile, compression, compressionLevel,
            statistics, rowGroupSize, dataPageSize, compatLevel,
            maintainOrder, syncOnClose, mkdir, cloudOptions
        );
    }
    /// <summary>
    /// Starts building a Merge (Upsert) operation using this <see cref="LazyFrame"/> as the source.
    /// </summary>
    /// <inheritdoc cref="UnityCatalog.MergeCatalogRecords(string, string, string, LazyFrame, string[], bool, CloudOptions?)"/>
    public static DeltaMergeBuilder MergeCatalogRecords(
        this LazyFrame sourceData,
        UnityCatalog catalog,
        string catalogName,
        string schemaName,
        string tableName,
        string[] mergeKeys,
        bool canEvolve = false,
        CloudOptions? cloudOptions = null)
    => catalog.MergeCatalogRecords(
        catalogName, 
        schemaName, 
        tableName, 
        sourceData,
        mergeKeys, 
        canEvolve, 
        cloudOptions
    );
    /// <summary>
    /// Starts building a Merge (Upsert) operation using this <see cref="DataFrame"/> as the source.
    /// </summary>
    /// <inheritdoc cref="UnityCatalog.MergeCatalogRecords(string, string, string, DataFrame, string[], bool, CloudOptions?)"/>
    public static DeltaMergeBuilder MergeCatalogRecords(
        this DataFrame sourceData,
        UnityCatalog catalog,
        string catalogName,
        string schemaName,
        string tableName,
        string[] mergeKeys,
        bool canEvolve = false,
        CloudOptions? cloudOptions = null)
    => catalog.MergeCatalogRecords(
        catalogName, 
        schemaName, 
        tableName, 
        sourceData,
        mergeKeys, 
        canEvolve, 
        cloudOptions
    );
}