using System.Text;
using System.Text.Json;
using Apache.Arrow;
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
    /// Scan Catalog Table
    /// </summary>
    /// <param name="catalogName"></param>
    /// <param name="schemaName"></param>
    /// <param name="tableName"></param>
    /// <param name="version"></param>
    /// <param name="datetime"></param>
    /// <param name="nRows"></param>
    /// <param name="parallel"></param>
    /// <param name="lowMemory"></param>
    /// <param name="useStatistics"></param>
    /// <param name="glob"></param>
    /// <param name="rechunk"></param>
    /// <param name="cache"></param>
    /// <param name="rowIndexName"></param>
    /// <param name="rowIndexOffset"></param>
    /// <param name="includePathColumn"></param>
    /// <param name="schema"></param>
    /// <param name="hivePartitioning"></param>
    /// <param name="hivePartitionSchema"></param>
    /// <param name="tryParseHiveDates"></param>
    /// <param name="cloudOptions"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public LazyFrame ScanCatalogTable(
        string catalogName,
        string schemaName,
        string tableName,
        long? version = null,
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
        PolarsSchema? schema = null,
        bool hivePartitioning = true,
        PolarsSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = true,
        CloudOptions? cloudOptions = null)
    {
        if (version.HasValue && datetime != null)
        {
            throw new ArgumentException("Cannot specify both 'version' and 'datetime' for Delta Time Travel.");
        }

        var schemaHandle = schema?.Handle;
        var hiveSchemaHandle = hivePartitionSchema?.Handle;

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
    /// 
    /// </summary>
    /// <param name="catalogName"></param>
    /// <param name="schemaName"></param>
    /// <param name="tableName"></param>
    /// <param name="lf"></param>
    /// <param name="partitionBy"></param>
    /// <param name="mode"></param>
    /// <param name="canEvolve"></param>
    /// <param name="includeKeys"></param>
    /// <param name="keysPreGrouped"></param>
    /// <param name="maxRowsPerFile"></param>
    /// <param name="approxBytesPerFile"></param>
    /// <param name="compression"></param>
    /// <param name="compressionLevel"></param>
    /// <param name="statistics"></param>
    /// <param name="rowGroupSize"></param>
    /// <param name="dataPageSize"></param>
    /// <param name="compatLevel"></param>
    /// <param name="maintainOrder"></param>
    /// <param name="syncOnClose"></param>
    /// <param name="mkdir"></param>
    /// <param name="cloudOptions"></param>
    public void SinkCatalogTable(
        string catalogName,
        string schemaName,
        string tableName,
        LazyFrame lf,
        Selector? partitionBy = null,
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
        using var partitionByH = partitionBy?.CloneHandle(); 
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
    /// 
    /// </summary>
    /// <param name="catalogName"></param>
    /// <param name="schemaName"></param>
    /// <param name="tableName"></param>
    /// <param name="polarsSchema"></param>
    /// <param name="tableType"></param>
    /// <param name="storageLocation"></param>
    public void CreateCatalogTable(
        string catalogName,
        string schemaName,
        string tableName,
        PolarsSchema polarsSchema,
        CatalogTableType tableType = CatalogTableType.Managed,
        string? storageLocation = null
    )
    => PolarsWrapper.CreateCatalogTable(
        Handle,
        catalogName,
        schemaName,
        tableName,
        polarsSchema.Handle,
        tableType.ToNative(),
        storageLocation);
    /// <summary>
    /// 
    /// </summary>
    /// <param name="catalogName"></param>
    /// <param name="schemaName"></param>
    /// <param name="tableName"></param>
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
    /// 
    /// </summary>
    /// <param name="catalogName"></param>
    /// <param name="schemaName"></param>
    /// <param name="tableName"></param>
    /// <param name="predicate"></param>
    /// <param name="cloudOptions"></param>
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
    /// Starts building a Merge (Upsert) operation for a Unity Catalog table.
    /// </summary>
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
    /// Starts building a Merge (Upsert) operation for a Unity Catalog table.
    /// </summary>
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
    public long OptimizeCatalogTable(
        string catalogName,
        string schemaName,
        string tableName,
        long targetSizeMb = 128,
        Dictionary<string, string>? partitionFilters = null,
        IEnumerable<string>? zOrderColumns = null,
        CloudOptions? cloudOptions = null)
    {
        // 1. Validation
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
    /// 
    /// </summary>
    /// <param name="catalogName"></param>
    /// <param name="schemaName"></param>
    /// <param name="tableName"></param>
    /// <param name="retentionHours"></param>
    /// <param name="enforceRetention"></param>
    /// <param name="dryRun"></param>
    /// <param name="vacuumModeFull"></param>
    /// <param name="cloudOptions"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
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
    /// 
    /// </summary>
    /// <param name="catalogName"></param>
    /// <param name="schemaName"></param>
    /// <param name="tableName"></param>
    /// <param name="version"></param>
    /// <param name="timestamp"></param>
    /// <param name="ignoreMissingFiles"></param>
    /// <param name="protocolDowngradeAllowed"></param>
    /// <param name="cloudOptions"></param>
    /// <returns></returns>
    /// <exception cref="ArgumentException"></exception>
    public long DeltaRestore(
        string catalogName,
        string schemaName,
        string tableName,
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
    /// 
    /// </summary>
    /// <param name="catalogName"></param>
    /// <param name="schemaName"></param>
    /// <param name="tableName"></param>
    /// <param name="limit"></param>
    /// <param name="cloudOptions"></param>
    /// <returns></returns>
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
        
        return df.Select(selection.Select(Polars.Col).ToArray());
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

public static partial class Polars 
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
    /// 将 LazyFrame 作为数据流，极其丝滑地写入 Unity Catalog 数据湖屋。
    /// （注意：底层会立即触发 Polars Streaming 引擎执行计算并落盘）
    /// </summary>
    public static void SinkCatalogTable(
        this LazyFrame lf,
        UnityCatalog catalog,
        string catalogName,
        string schemaName,
        string tableName,
        Selector? partitionBy = null,
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
        // 直接路由给 Catalog 对象去执行真实逻辑
        catalog.SinkCatalogTable(
            catalogName, schemaName, tableName, lf,
            partitionBy, mode, canEvolve, includeKeys, keysPreGrouped,
            maxRowsPerFile, approxBytesPerFile, compression, compressionLevel,
            statistics, rowGroupSize, dataPageSize, compatLevel,
            maintainOrder, syncOnClose, mkdir, cloudOptions
        );
    }

    /// <summary>
    /// 将物理 DataFrame 写入 Unity Catalog 数据湖屋。
    /// 自动处理鉴权、路径发现以及分区策略推断！
    /// </summary>
    public static void WriteCatalogTable(
        this DataFrame df,
        UnityCatalog catalog,
        string catalogName,
        string schemaName,
        string tableName,
        Selector? partitionBy = null,
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
        // DataFrame 转为 LazyFrame 后再走一遍 Sink 流
        catalog.SinkCatalogTable(
            catalogName, schemaName, tableName, df.Lazy(),
            partitionBy, mode, canEvolve, includeKeys, keysPreGrouped,
            maxRowsPerFile, approxBytesPerFile, compression, compressionLevel,
            statistics, rowGroupSize, dataPageSize, compatLevel,
            maintainOrder, syncOnClose, mkdir, cloudOptions
        );
    }
}