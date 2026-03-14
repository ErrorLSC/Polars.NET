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

    // ==========================================
    // 预留的 API 位置（咱们下一步要填满它们！）
    // ==========================================

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