using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public static partial class PolarsWrapper
{
    public static CatalogHandle InitUnityCatalog(string workspaceUrl, string bearerToken)
        => ErrorHelper.Check(NativeBindings.pl_catalog_unity_new(workspaceUrl, bearerToken));
    public unsafe static LazyFrameHandle ScanCatalogTable
    (
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        long? version,
        string? datetime,
        ulong? nRows,
        PlParallelStrategy parallel,
        bool lowMemory,
        bool useStatistics,
        bool glob,
        // bool allowMissingColumns,
        bool rechunk,
        bool cache,
        string? rowIndexName,
        uint rowIndexOffset,
        string? includePathColumn,
        SchemaHandle? schema,
        bool hivePartitioning,
        SchemaHandle? hivePartitionSchema,
        bool tryParseHiveDates,
        PlCloudProvider cloudProvider,
        nuint cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues)
    {
        long versionVal = version.GetValueOrDefault();
        IntPtr versionPtr = version.HasValue ? (IntPtr)(&versionVal) : IntPtr.Zero;

        ulong nRowsVal = nRows.GetValueOrDefault();
        IntPtr nRowsPtr = nRows.HasValue ? (IntPtr)(&nRowsVal) : IntPtr.Zero;

        using var schemaLock = new SafeHandleLock<SchemaHandle>(
            schema != null ? [schema] : null
        );
        IntPtr schemaPtr = schema != null ? schemaLock.Pointers[0] : IntPtr.Zero;

        using var hiveLock = new SafeHandleLock<SchemaHandle>(
            hivePartitionSchema != null ? [hivePartitionSchema] : null
        );
        IntPtr hiveSchemaPtr = hivePartitionSchema != null ? hiveLock.Pointers[0] : IntPtr.Zero;
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);
        var h = NativeBindings.pl_scan_catalog_table(
            handle,
            catalogName,
            schemaName,
            tableName,
            versionPtr,
            datetime,
            nRowsPtr,
            parallel,
            lowMemory,
            useStatistics,
            glob,
            rechunk, 
            cache,  
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schemaPtr,
            hivePartitioning,
            hiveSchemaPtr,
            tryParseHiveDates,
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues,
            cloudLen
        );

        return ErrorHelper.Check(h);
    }
    public static void SinkCatalogTable
    (
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        LazyFrameHandle lf,
        // --- Delta Options ---
        PlDeltaSaveMode mode,
        bool canEvolve,
        // --- Partition Params ---
        SelectorHandle? partitionBy,
        bool includeKeys,
        bool keysPreGrouped,
        nuint maxRowsPerFile,
        ulong approxBytesPerFile,

        // --- Parquet Options ---
        PlParquetCompression compression,
        int compressionLevel,
        bool statistics,
        nuint rowGroupSize,
        nuint dataPageSize,
        int compatLevel,

        // --- Unified Options ---
        bool maintainOrder,
        PlSyncOnClose syncOnClose,
        bool mkdir,

        // --- Cloud Params ---
        PlCloudProvider cloudProvider,
        nuint cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues)
    {
        nuint rgs = rowGroupSize > 0 ? rowGroupSize : 0;
        nuint dps = dataPageSize > 0 ? dataPageSize : 0;
        
        int safeCompatLevel = compatLevel;
        if (safeCompatLevel < -1) safeCompatLevel = -1;
        else if (safeCompatLevel > 1) safeCompatLevel = 1;

        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);
        IntPtr partitionByHandle = partitionBy?.TransferOwnership() ?? IntPtr.Zero;
        NativeBindings.pl_sink_catalog_table(
            handle,
            catalogName,
            schemaName,
            tableName,
            lf,
            mode,
            canEvolve,
            // Partition Params
            partitionByHandle,
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile,
            approxBytesPerFile,

            // Parquet Options
            compression,
            compressionLevel,
            statistics,
            rgs,
            dps,
            safeCompatLevel,

            // Unified Options
            maintainOrder,
            syncOnClose,
            mkdir,

            // Cloud Params
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues,
            cloudLen
        );

        lf.TransferOwnership();

        ErrorHelper.CheckVoid();
    }
    public static void CreateCatalogTable
    (
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        SchemaHandle schema,
        PlCatalogTableType tableType,
        string? storageLocation
    )
    {
        NativeBindings.pl_catalog_create_table(
            handle,
            catalogName,
            schemaName,
            tableName,
            schema,
            tableType,
            storageLocation
        );
        ErrorHelper.CheckVoid();
    }
    public static void DeleteCatalogTable
    (
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName
    )
    {
        NativeBindings.pl_catalog_delete_table(
            handle,
            catalogName,
            schemaName,
            tableName
        );
        ErrorHelper.CheckVoid();
    }
}