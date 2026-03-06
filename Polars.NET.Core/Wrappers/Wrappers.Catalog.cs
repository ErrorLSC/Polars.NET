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
}