using System.Runtime.InteropServices;
using Polars.NET.Core.Native;

namespace Polars.NET.Core;

public readonly partial struct PolarsWrapper
{
    public static long Vacuum(
        string path,
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

        NativeBindings.pl_io_delta_vacuum(
            path,
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
    public static long Restore(
        string path,
        long targetVersion,
        long targetTimestamp,
        bool ignoreMissingFiles,
        bool protocolDowngradeAllowed,
        // Delta Cloud Options
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_io_delta_restore(
            path,
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
    public static string History(
        string path,
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
            NativeBindings.pl_io_delta_history(
                path,
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
    public static ulong Optimize(
        string path,
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

        NativeBindings.pl_io_delta_optimize(
            path,
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
    public static void AddFeature(
        string path,
        string featureName,
        bool allowProtocolIncrease,
        // Cloud Options
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_io_delta_add_feature(
            path,
            featureName,
            allowProtocolIncrease,
            cloudKeys,
            cloudValues,
            cloudLen
        );

        ErrorHelper.CheckVoid();
    }
    public static void SetTableProperties(
        string path,
        string[] propKeys,
        string[] propValues,
        bool raiseIfNotExists,
        // Cloud Options
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        nuint propLen = (nuint)propKeys.Length;
        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_io_delta_set_table_properties(
            path,
            propKeys,
            propValues,
            propLen,
            raiseIfNotExists,
            cloudKeys,
            cloudValues,
            cloudLen
        );

        ErrorHelper.CheckVoid();
    }
    // ---------------------------------------------------------
    // DeltaLake
    // ---------------------------------------------------------
    public unsafe static LazyFrameHandle ScanDelta(
        string path,
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
        var h = NativeBindings.pl_scan_delta(
            path,
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
    public static void SinkDelta(
        LazyFrameHandle lf,
        string path,
        
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
        string[]? cloudValues
    )
    {
        nuint rgs = rowGroupSize > 0 ? rowGroupSize : 0;
        nuint dps = dataPageSize > 0 ? dataPageSize : 0;
        
        int safeCompatLevel = compatLevel;
        if (safeCompatLevel < -1) safeCompatLevel = -1;
        else if (safeCompatLevel > 1) safeCompatLevel = 1;

        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);
        IntPtr partitionByHandle = partitionBy?.TransferOwnership() ?? IntPtr.Zero;
        NativeBindings.pl_sink_delta(
            lf,
            path,
            
            // Delta Options
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
    public static void DeltaDelete(
        string path,
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
        
        NativeBindings.pl_io_delta_delete(
            path,
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
    public static void DeltaMerge(
        LazyFrameHandle sourceLf,
        string path,
        string[] mergeKeys,
        ExprHandle? matchedUpdateCond,
        ExprHandle? matchedDeleteCond,
        ExprHandle? notMatchedInsertCond,
        ExprHandle? notMatchedBySourceDeletedCond,
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
        nuint mergeKeysLen = (nuint)mergeKeys.Length;
        IntPtr updateHandle = matchedUpdateCond?.TransferOwnership() ?? IntPtr.Zero;
        IntPtr deleteHandle = matchedDeleteCond?.TransferOwnership() ?? IntPtr.Zero;
        IntPtr insertHandle = notMatchedInsertCond?.TransferOwnership() ?? IntPtr.Zero;
        IntPtr srcDeleteHandle = notMatchedBySourceDeletedCond?.TransferOwnership() ?? IntPtr.Zero;

        nuint cloudLen = (nuint)(cloudKeys?.Length ?? 0);

        NativeBindings.pl_io_delta_merge(
            sourceLf,
            path,
            mergeKeys,
            mergeKeysLen,
            updateHandle,
            deleteHandle,
            insertHandle,
            srcDeleteHandle,
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
    public static void DeltaMergeOrdered(
        LazyFrameHandle sourceLf,
        string path,
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

        NativeBindings.pl_io_delta_merge_ordered(
            sourceLf,
            path,
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
    public static LazyFrameHandle DeltaReadCdc(
        string path,
        long startVersion,
        long endVersion,
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

        NativeBindings.pl_io_delta_read_cdc(
            path,
            startVersion,
            endVersion,
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues,
            cloudLen,
            out LazyFrameHandle outLfPtr
        );

        return outLfPtr;
    }
    public static LazyFrameHandle DeltaReadCdcByTime(
        string path,
        long startTimestampMs,
        long endTimestampMs,
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

        NativeBindings.pl_io_delta_read_cdc_by_time(
            path,
            startTimestampMs,
            endTimestampMs,
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues,
            cloudLen,
            out LazyFrameHandle outLfPtr
        );

        return outLfPtr;
    }
}