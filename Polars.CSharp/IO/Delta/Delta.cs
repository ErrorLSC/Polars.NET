using System.Text;
using System.Text.Json;
using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Common Delta Lake Table Features.
/// </summary>
public static class DeltaTableFeatures
{
    /// <summary>
    /// Enables Deletion Vectors for efficient deletes/updates (Merge-on-Read).
    /// Requires Reader v3, Writer v7.
    /// </summary>
    public const string DeletionVectors = "deletionVectors";

    /// <summary>
    /// Enables Change Data Feed (CDF) to track row-level changes.
    /// </summary>
    public const string ChangeDataFeed = "changeDataFeed";

    /// <summary>
    /// Enables Column Mapping (allows renaming columns and special characters).
    /// </summary>
    public const string ColumnMapping = "columnMapping";

    /// <summary>
    /// Enforces Append-Only retention (prevents deletes/updates).
    /// </summary>
    public const string AppendOnly = "appendOnly";

    // /// <summary>
    // /// Enables Identity Columns (auto-incrementing IDs).
    // /// </summary>
    // public const string IdentityColumns = "identityColumns";
    
    /// <summary>
    /// Enables Check Constraints on columns.
    /// </summary>
    public const string CheckConstraints = "checkConstraints";

    /// <summary>
    /// Enables TimestampWithoutTimezone.
    /// </summary>
    public const string TimestampWithoutTimezone = "TimestampWithoutTimezone";
    
    // /// <summary>
    // /// Enables V2 Checkpointing (JSON + Parquet sidecars).
    // /// </summary>
    // public const string V2Checkpoint = "v2Checkpoint";

    /// <summary>
    /// Enables IcebergCompatV1
    /// </summary>
    public const string IcebergCompatV1 = "IcebergCompatV1";
}

/// <summary>
/// Common configuration keys for Delta Tables.
/// </summary>
public static class DeltaTableProperties
{
    /// <summary>
    /// The shortest duration that deleted files are kept before Vacuum deletes them.
    /// Default: 7 days (e.g., "interval 7 days").
    /// </summary>
    public const string DeletedFileRetentionDuration = "delta.deletedFileRetentionDuration";

    /// <summary>
    /// How long the history of the Delta Log is kept.
    /// Default: 30 days (e.g., "interval 30 days").
    /// </summary>
    public const string LogRetentionDuration = "delta.logRetentionDuration";

    /// <summary>
    /// The target file size in bytes for Optimize/Bin-packing.
    /// Default: 134217728 (128 MB).
    /// </summary>
    public const string TargetFileSize = "delta.targetFileSize";

    /// <summary>
    /// Whether to collect stats for columns (min/max/nulls).
    /// Default: true.
    /// </summary>
    public const string DataSkippingNumIndexedCols = "delta.dataSkippingNumIndexedCols";
    
    /// <summary>
    /// If true, enables the Change Data Feed (CDF).
    /// </summary>
    public const string EnableChangeDataFeed = "delta.enableChangeDataFeed";
}
/// <summary>
/// Methods for DeltaLake 
/// </summary>
public static class Delta
{
    /// <summary>
    /// Represents a column from the incoming Source LazyFrame.
    /// (Internally maps to "{name}_src_tmp")
    /// </summary>
    public static Expr Source(string columnName)
    {
        return Polars.Col($"{columnName}_src_tmp");
    }

    /// <summary>
    /// Represents a column from the existing Target Delta Table.
    /// (Alias for standard Col(), adds semantic clarity)
    /// </summary>
    public static Expr Target(string columnName)
    {
        return Polars.Col(columnName);
    }
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
    public static void Delete(
        string path,
        Expr predicate,
        CloudOptions? cloudOptions = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        using var clonedPredicate = predicate.CloneHandle();

        PolarsWrapper.DeltaDelete(
            path,
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
    public static long Vacuum(
        string path,
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
        return PolarsWrapper.Vacuum(
            path,
            retentionArg,
            enforceRetention,
            dryRun,
            vacuumModeFull,
            keys,
            values
        );
    }
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
    /// <exception cref="ArgumentException">Thrown if neither or both version and timestamp are provided.</exception>
    public static long Restore(
        string path,
        long? version = null,
        DateTime? timestamp = null,
        bool ignoreMissingFiles = false,
        bool protocolDowngradeAllowed = false,
        CloudOptions? cloudOptions = null)
    {
        // 1. Validation: Version and Timestamp are mutually exclusive
        if (version.HasValue && timestamp.HasValue)
            throw new ArgumentException("Cannot specify both 'version' and 'timestamp' for Restore.");

        if (!version.HasValue && !timestamp.HasValue)
            throw new ArgumentException("Must specify either 'version' or 'timestamp' for Restore.");

        // 2. Prepare Parameters
        // Rust uses -1 to indicate "not set"
        long targetVer = version ?? -1;
        long targetTs = -1;

        if (timestamp.HasValue)
        {
            // Convert DateTime to Unix Milliseconds
            DateTime utcTime = timestamp.Value.ToUniversalTime();
            targetTs = new DateTimeOffset(utcTime).ToUnixTimeMilliseconds();
        }

        // 3. Parse Cloud Options
        // We only need keys/values for the Delta Lake object store
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        // 4. Call Wrapper
        return PolarsWrapper.Restore(
            path,
            targetVer,
            targetTs,
            ignoreMissingFiles,
            protocolDowngradeAllowed,
            keys,
            values
        );
    }
    /// <summary>
    /// Returns provenance information, including the operation, user, timestamp, etc., for each write to a table.
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="limit">The number of latest commits to retrieve. Set to 0 (default) to retrieve all history.</param>
    /// <param name="cloudOptions">Cloud storage credentials.</param>
    /// <returns>A DataFrame containing the commit history.</returns>
    public static DataFrame History(
        string path,
        int limit = 0,
        CloudOptions? cloudOptions = null)
    {
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);
        string json = PolarsWrapper.History(path, limit, keys, values);
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

        // operationParameters (Struct -> Columns)
        if (df.ColumnNames.Contains("operationParameters"))
        {
            df = df.Unnest("operationParameters");
        }
        
        if (df.ColumnNames.Contains("operationMetrics"))
        {
            df = df.Unnest("operationMetrics");
        }

        if (df.ColumnNames.Contains("version"))
        {
            df = df.Sort("version", descending: true);
        }
        else
        {
            df = df.Sort("timestamp", descending: true);
        }

        var priorityCols = new[] { "version", "timestamp", "operation", "mode", "predicate", "userName" };
        var existingCols = df.ColumnNames;
        var selection = priorityCols.Where(c => existingCols.Contains(c)).ToList();
        
        selection.AddRange(existingCols.Except(priorityCols));
        
        return df.Select(selection.Select(c => Polars.Col(c)).ToArray());
    }
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
    /// Optional dictionary of partition key-value pairs to restrict optimization to specific partitions.
    /// <para>Example: <c>new Dictionary&lt;string, string&gt; { { "date", "2024-01-01" } }</c></para>
    /// </param>
    /// <param name="zOrderColumns">
    /// Optional list of column names to apply Z-Order clustering. 
    /// <para>Z-Ordering co-locates data based on these columns, significantly speeding up queries that filter on them.</para>
    /// </param>
    /// <param name="cloudOptions">Cloud storage credentials and configuration.</param>
    /// <returns>The number of new data files created during the optimization process.</returns>
    /// <exception cref="ArgumentException">Thrown if the path is empty or target size is invalid.</exception>
    public static long Optimize(
        string path,
        long targetSizeMb = 128,
        Dictionary<string, string>? partitionFilters = null,
        IEnumerable<string>? zOrderColumns = null,
        CloudOptions? cloudOptions = null)
    {
        // 1. Validation
        if (string.IsNullOrWhiteSpace(path))
            throw new ArgumentException("Path cannot be empty.", nameof(path));

        if (targetSizeMb <= 0)
            throw new ArgumentException("Target size must be greater than 0 MB.", nameof(targetSizeMb));

        // 2. Prepare Parameters
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

        // 3. Parse Cloud Options
        // Unlike Restore, Optimize needs all cloud retry/timeout settings passed down
        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) = 
            CloudOptions.ParseCloudOptions(cloudOptions);

        // 4. Call Wrapper
        // The wrapper returns ulong (nuint), we cast to long for API consistency
        ulong result = PolarsWrapper.Optimize(
            path,
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
    public static void AddFeature(
        string path, 
        string featureName, 
        bool allowProtocolIncrease = true, 
        CloudOptions? cloudOptions = null)
    {
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.AddFeature(
            path,
            featureName,
            allowProtocolIncrease,
            keys,
            values
        );
    }
    /// <summary>
    /// Sets or updates properties on the Delta Table.
    /// </summary>
    /// <param name="path">The root URI of the Delta table.</param>
    /// <param name="properties">A dictionary of key-value pairs to set.</param>
    /// <param name="raiseIfNotExists">
    /// If true, the operation fails if you try to update a property that doesn't strictly exist (rarely used, default false).
    /// </param>
    /// <param name="cloudOptions">Cloud storage credentials.</param>
    public static void SetTableProperties(
        string path,
        Dictionary<string, string> properties,
        bool raiseIfNotExists = false,
        CloudOptions? cloudOptions = null)
    {
        if (properties == null || properties.Count == 0)
            throw new ArgumentException("Properties dictionary cannot be empty.", nameof(properties));

        // 1. Prepare Properties (Unpack Dictionary)
        var propKeys = properties.Keys.ToArray();
        var propValues = properties.Values.ToArray();

        // 2. Prepare Cloud Options
        var (_, _, _, _, _, _, cloudKeys, cloudValues) = CloudOptions.ParseCloudOptions(cloudOptions);

        // 3. Call Wrapper
        PolarsWrapper.SetTableProperties(
            path,
            propKeys,
            propValues,
            raiseIfNotExists,
            cloudKeys,
            cloudValues
        );
    }
    /// <summary>
    /// Reads the Change Data Feed (CDC) stream for a Delta table between the specified versions.
    /// The table must have 'delta.enableChangeDataFeed' set to 'true'.
    /// </summary>
    /// <param name="path">The URI or local path of the Delta table.</param>
    /// <param name="startVersion">The starting version (inclusive).</param>
    /// <param name="endVersion">The ending version (inclusive).</param>
    /// <param name="cloudOptions">Optional cloud storage credentials and retry policies.</param>
    /// <returns>A LazyFrame containing the change data stream.</returns>
    public static LazyFrame ReadChangeDataFeed(
        string path, 
        long startVersion, 
        long endVersion, 
        CloudOptions? cloudOptions = null)
    {
        if (startVersion > endVersion)
        {
            throw new ArgumentException("startVersion cannot be greater than endVersion.");
        }

        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) = 
            CloudOptions.ParseCloudOptions(cloudOptions);

        var handle = PolarsWrapper.DeltaReadCdc(
            path,
            startVersion,
            endVersion,
            provider.ToNative(),
            retries, 
            timeout, 
            initBackoff, 
            maxBackoff, 
            cacheTtl, 
            keys, 
            values
        );

        return new LazyFrame(handle);
    }
    /// <summary>
    /// Reads the Change Data Feed (CDC) stream for a Delta table between the specified datetimes.
    /// The table must have 'delta.enableChangeDataFeed' set to 'true'.
    /// </summary>
    /// <param name="path">The URI or local path of the Delta table.</param>
    /// <param name="startTimestamp">The starting datetime (inclusive).</param>
    /// <param name="endTimestamp">Optional. The ending datetime (inclusive). Defaults to DateTime.UtcNow if null.</param>
    /// <param name="cloudOptions">Optional cloud storage credentials and retry policies.</param>
    /// <returns>A LazyFrame containing the change data stream filtered by the exact milliseconds.</returns>
    public static LazyFrame ReadChangeDataFeed(
        string path, 
        DateTime startTimestamp, 
        DateTime? endTimestamp = null, 
        CloudOptions? cloudOptions = null)
    {
        DateTime actualEnd = endTimestamp ?? DateTime.UtcNow;

        if (startTimestamp > actualEnd)
        {
            throw new ArgumentException("startTimestamp cannot be greater than endTimestamp.");
        }

        long startMs = new DateTimeOffset(startTimestamp.ToUniversalTime()).ToUnixTimeMilliseconds();
        long endMs = new DateTimeOffset(actualEnd.ToUniversalTime()).ToUnixTimeMilliseconds();

        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) = 
            CloudOptions.ParseCloudOptions(cloudOptions);

        var handle = PolarsWrapper.DeltaReadCdcByTime(
            path,
            startMs,
            endMs,
            provider.ToNative(),
            retries, 
            timeout, 
            initBackoff, 
            maxBackoff, 
            cacheTtl, 
            keys, 
            values
        );

        return new LazyFrame(handle);
    }
}

/// <summary>
/// A builder for constructing ordered Delta Lake Merge operations.
/// This ensures that matched and not-matched conditions are evaluated exactly 
/// in the order they are chained by the user, adhering to standard SQL MERGE semantics.
/// </summary>
public class DeltaMergeBuilder
{
    private readonly LazyFrame _sourceLf;
    // --- Physical Path ---
    private readonly string? _path;
    // --- Catalog  ---
    private readonly UnityCatalog? _catalog;
    private readonly string? _catalogName;
    private readonly string? _schemaName;
    private readonly string? _tableName;
    private readonly string[] _mergeKeys;
    private readonly bool _canEvolve;
    private readonly CloudOptions? _cloudOptions;

    // Use a List to strictly preserve the exact order of method calls
    private readonly List<(MergeActionType ActionType, Expr Condition)> _actions = [];

    /// <summary>
    /// Physical Path Merge Builder
    /// </summary>
    internal DeltaMergeBuilder(
        LazyFrame sourceLf, 
        string path, 
        string[] mergeKeys, 
        bool canEvolve = false, 
        CloudOptions? cloudOptions = null)
    {
        _sourceLf = sourceLf;
        _path = path;
        _mergeKeys = mergeKeys;
        _canEvolve = canEvolve;
        _cloudOptions = cloudOptions;
    }

    /// <summary>
    /// Unity Catalog Merge Builder
    /// </summary>
    internal DeltaMergeBuilder(
        LazyFrame sourceLf, 
        UnityCatalog catalog,
        string catalogName,
        string schemaName,
        string tableName,
        string[] mergeKeys, 
        bool canEvolve = false, 
        CloudOptions? cloudOptions = null)
    {
        _sourceLf = sourceLf;
        _catalog = catalog;
        _catalogName = catalogName;
        _schemaName = schemaName;
        _tableName = tableName;
        _mergeKeys = mergeKeys;
        _canEvolve = canEvolve;
        _cloudOptions = cloudOptions;
    }

    /// <summary>
    /// Update the matched target row with source data.
    /// </summary>
    /// <param name="condition">Optional condition for the update to occur. If null, updates all matched rows.</param>
    public DeltaMergeBuilder WhenMatchedUpdate(Expr? condition = null)
    {
        // If condition is null, we default to Polars.Lit(true) for unconditional action
        _actions.Add((MergeActionType.MatchedUpdate, condition ?? Polars.Lit(true)));
        return this;
    }

    /// <summary>
    /// Delete the matched target row.
    /// </summary>
    /// <param name="condition">Optional condition for the delete to occur. If null, deletes all matched rows.</param>
    public DeltaMergeBuilder WhenMatchedDelete(Expr? condition = null)
    {
        _actions.Add((MergeActionType.MatchedDelete, condition ?? Polars.Lit(true)));
        return this;
    }

    /// <summary>
    /// Insert a new row from the source data when it does not match any target row.
    /// </summary>
    /// <param name="condition">Optional condition for the insert to occur. If null, inserts all unmatched source rows.</param>
    public DeltaMergeBuilder WhenNotMatchedInsert(Expr? condition = null)
    {
        _actions.Add((MergeActionType.NotMatchedInsert, condition ?? Polars.Lit(true)));
        return this;
    }

    /// <summary>
    /// Delete the target row when it does not exist in the source data.
    /// </summary>
    /// <param name="condition">Optional condition for the target delete to occur. If null, deletes all unmatched target rows.</param>
    public DeltaMergeBuilder WhenNotMatchedBySourceDelete(Expr? condition = null)
    {
        _actions.Add((MergeActionType.NotMatchedBySourceDelete, condition ?? Polars.Lit(true)));
        return this;
    }

    /// <summary>
    /// Executes the constructed merge operation against the Delta Table.
    /// </summary>
    public void Execute()
    {
        if (_actions.Count == 0)
        {
            WhenMatchedUpdate();     
            WhenNotMatchedInsert();  
        }

        var actionTypes = new PlMergeActionType[_actions.Count];
        var actionExprs = new ExprHandle[_actions.Count];

        for (int i = 0; i < _actions.Count; i++)
        {
            actionTypes[i] = _actions[i].ActionType.ToNative();
            actionExprs[i] = _actions[i].Condition.CloneHandle(); 
        }

        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) = 
            CloudOptions.ParseCloudOptions(_cloudOptions);

        if (_catalog != null)
        {
            PolarsWrapper.CatalogMergeOrdered(
                _catalog.Handle,
                _catalogName!,
                _schemaName!,
                _tableName!,
                _sourceLf.CloneHandle(),
                _mergeKeys,
                actionTypes,
                actionExprs,
                _canEvolve,
                provider.ToNative(),
                retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values
            );
        }
        else
        {
            PolarsWrapper.DeltaMergeOrdered(
                _sourceLf.CloneHandle(), 
                _path!,
                _mergeKeys,
                actionTypes,
                actionExprs,
                _canEvolve,
                provider.ToNative(),
                retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values
            );
        }
    }
}