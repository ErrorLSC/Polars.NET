using System.Runtime.InteropServices;
using Apache.Arrow;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
{
    public static CatalogHandle InitUnityCatalog(string workspaceUrl, string bearerToken)
        => ErrorHelper.Check(NativeBindings.pl_catalog_unity_new(workspaceUrl, bearerToken));
    public unsafe static LazyFrameHandle ScanCatalogTable
    (
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        ulong? version,
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
        ulong versionVal = version.GetValueOrDefault();
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
    public static void DeleteCatalogRecords(
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        ExprHandle predicate,
        // Cloud Options
        PlCloudProvider cloudProvider,
        nuint cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);
        
        NativeBindings.pl_catalog_delete_records(
            handle,
            catalogName,
            schemaName,
            tableName,
            predicate,
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

        predicate.TransferOwnership();

        ErrorHelper.CheckVoid();
    }
    public static void CatalogMergeOrdered(
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        LazyFrameHandle sourceLf,
        string[] mergeKeys,
        PlMergeActionType[] actionTypes,
        ExprHandle[] actionExprs,
        bool can_evolve,
        // Cloud Options
        PlCloudProvider cloudProvider,
        nuint cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        if (mergeKeys == null || mergeKeys.Length == 0)
        {
            throw new ArgumentException("Merge keys cannot be null or empty.", nameof(mergeKeys));
        }
        
        if (actionTypes == null || actionExprs == null || actionTypes.Length != actionExprs.Length)
        {
            throw new ArgumentException("Action types and expressions must be non-null and of the same length.");
        }

        nuint mergeKeysLen = (nuint)mergeKeys.Length;
        nuint actionsCount = (nuint)actionTypes.Length;

        IntPtr[] exprPtrs = new IntPtr[actionsCount];
        for (int i = 0; i < (int)actionsCount; i++)
        {
            if (actionExprs[i] == null)
            {
                throw new ArgumentNullException(nameof(actionExprs), $"Expression at index {i} cannot be null.");
            }
            
            exprPtrs[i] = actionExprs[i].TransferOwnership();
        }

        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_catalog_merge_ordered(
            handle,
            catalogName,
            schemaName,
            tableName,
            sourceLf,
            mergeKeys,
            mergeKeysLen,
            
            actionTypes,  
            exprPtrs,     
            actionsCount, 
            
            can_evolve,
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

        sourceLf.TransferOwnership();

        ErrorHelper.CheckVoid();
    }
    public static ulong CatalogOptimize(
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        long targetSizeMb,
        string? filterJson,
        string[]? zOrderCols,
        // Cloud Options
        PlCloudProvider cloudProvider,
        UIntPtr cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint zOrderLen = (nuint)(zOrderCols?.Length ?? 0);
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_catalog_optimize(
            handle,
            catalogName,
            schemaName,
            tableName,
            targetSizeMb,
            filterJson,
            zOrderCols,
            zOrderLen,
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues,
            cloudLen,
            out nuint optimizedFilesCount
        );

        ErrorHelper.CheckVoid();

        return optimizedFilesCount;
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
    public static long CatalogVacuum(
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        int retentionHours,
        bool enforceRetention,
        bool dryRun,
        bool vacuumModeFull,
        // Delta Cloud Options
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_catalog_delta_vacuum(
            handle,
            catalogName,
            schemaName,
            tableName,
            retentionHours,
            enforceRetention,
            dryRun,
            vacuumModeFull,
            cloudKeys,
            cloudValues,
            cloudLen,
            out var filesDeleted
        );

        ErrorHelper.CheckVoid();
        return (long)filesDeleted;
    }
    public static ulong CatalogRestore(
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        ulong targetVersion,
        long targetTimestamp,
        bool ignoreMissingFiles,
        bool protocolDowngradeAllowed,
        // Delta Cloud Options
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_catalog_delta_restore(
            handle,
            catalogName,
            schemaName,
            tableName,
            targetVersion,
            targetTimestamp,
            ignoreMissingFiles,
            protocolDowngradeAllowed,
            cloudKeys,
            cloudValues,
            cloudLen,
            out var newVersion
        );

        ErrorHelper.CheckVoid();
        return newVersion;
    }
    public static string CatalogHistory(
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        int limit,
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);
        nuint limitNative = (nuint)(limit < 0 ? 0 : limit); // <0 or 0 means All

        IntPtr jsonPtr = IntPtr.Zero;

        try
        {
            NativeBindings.pl_catalog_delta_history(
                handle,
                catalogName,
                schemaName,
                tableName,
                limitNative,
                cloudKeys,
                cloudValues,
                cloudLen,
                out jsonPtr
            );
            
            ErrorHelper.CheckVoid();

            string? json = Marshal.PtrToStringUTF8(jsonPtr);
            return json ?? "[]";
        }
        finally
        {
            if (jsonPtr != IntPtr.Zero)
            {
                NativeBindings.pl_free_string(jsonPtr);
            }
        }
    }
}