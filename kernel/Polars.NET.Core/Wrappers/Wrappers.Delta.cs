using System.Runtime.InteropServices;
using Polars.NET.Core.Native;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;

namespace Polars.NET.Core.Wrappers;

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
        // retentionHours < 0: the table's retention.
        var result = Delta.DeltaKernelVacuum.Vacuum(path, retentionHours < 0 ? null : retentionHours, enforceRetention, dryRun,
            vacuumModeFull, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
        return result.Files.Count;
    }
    public static ulong Restore(
        string path,
        ulong targetVersion,
        long targetTimestamp,
        bool ignoreMissingFiles,
        bool protocolDowngradeAllowed,
        // Delta Cloud Options
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        // targetTimestamp < 0: restore by version.
        var cloud = Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues);
        var result = targetTimestamp >= 0
            ? Delta.DeltaKernelHistory.Restore(path, timestamp: DateTimeOffset.FromUnixTimeMilliseconds(targetTimestamp),
                ignoreMissingFiles: ignoreMissingFiles, protocolDowngradeAllowed: protocolDowngradeAllowed, cloud: cloud)
            : Delta.DeltaKernelHistory.Restore(path, version: checked((long)targetVersion),
                ignoreMissingFiles: ignoreMissingFiles, protocolDowngradeAllowed: protocolDowngradeAllowed, cloud: cloud);
        // Nothing to restore (the table is already as the target): the current version.
        long version = result.Version ?? Delta.DeltaKernelHistory.Read(path, 1, cloud)[0].Version;
        return checked((ulong)version);
    }
    public static string History(
        string path,
        int limit,
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        // limit <= 0: all commits.
        var entries = Delta.DeltaKernelHistory.Read(path, limit < 0 ? 0 : limit, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
        return Delta.DeltaKernelHistory.ToJson(entries, renameCollisions: true); // flattened into one row per commit
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
        Dictionary<string, string?>? filters = null;
        if (!string.IsNullOrEmpty(filterJson))
        {
            using var doc = System.Text.Json.JsonDocument.Parse(filterJson);
            filters = new Dictionary<string, string?>(StringComparer.Ordinal);
            foreach (var property in doc.RootElement.EnumerateObject())
                filters[property.Name] = property.Value.ValueKind == System.Text.Json.JsonValueKind.Null ? null : property.Value.ToString();
        }
        var result = Delta.DeltaKernelOptimize.Optimize(path, checked(targetSizeMb * 1024 * 1024), filters,
            zOrderCols is { Length: > 0 } ? zOrderCols : null, new Delta.CloudArgs(cloudProvider, cloudRetries, cloudRetryTimeoutMs, cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs, cloudCacheTtl, cloudKeys, cloudValues).WithProviderFrom(path));
        // The number of data files the optimize wrote.
        return (ulong)result.AddedFiles;
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
        Delta.DeltaKernelAlter.AddFeature(path, featureName, allowProtocolIncrease, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
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
        if (propKeys.Length != propValues.Length) throw new ArgumentException("Every property needs a value.", nameof(propValues));
        var properties = new Dictionary<string, string>(StringComparer.Ordinal);
        for (int i = 0; i < propKeys.Length; i++) properties[propKeys[i]] = propValues[i];
        Delta.DeltaKernelAlter.SetTableProperties(path, properties, raiseIfNotExists, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    }
    // ---------------------------------------------------------
    // DeltaLake
    // ---------------------------------------------------------
    public static LazyFrameHandle ScanDelta(
        string path,
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
        string[]? cloudValues,
        string? rowIdColumn = null,
        string? rowCommitVersionColumn = null)
    {
        // The managed Delta kernel resolves the table; partition values come from the log, so the glob and Hive
        // options do not apply.
        _ = glob; _ = hivePartitioning; _ = hivePartitionSchema; _ = tryParseHiveDates;
        var cloud = new Delta.CloudArgs(cloudProvider, cloudRetries, cloudRetryTimeoutMs, cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs, cloudCacheTtl, cloudKeys, cloudValues).WithProviderFrom(path);
        DateTimeOffset? asOf = datetime is null ? null : Delta.DeltaKernelScan.ParseAsOf(datetime);
        long? atVersion = version is ulong v ? checked((long)v) : null;
        var options = new Delta.KernelScanOptions(nRows, rowIndexName, rowIndexOffset, includePathColumn, parallel, lowMemory,
            useStatistics, rechunk, cache, schema, rowIdColumn, rowCommitVersionColumn);
        return Delta.DeltaKernelScan.Scan(path, atVersion, asOf, filter: null, cloud, options);
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
        // The managed Delta kernel writes: the engine streams the files, partition columns are never written into
        // them (Delta keeps them in the log), statistics are always written (data skipping reads them), directories
        // are always created, and the Arrow compatibility level does not apply to Parquet data files.
        _ = includeKeys; _ = statistics; _ = compatLevel; _ = mkdir;
        var cloud = new Delta.CloudArgs(cloudProvider, cloudRetries, cloudRetryTimeoutMs, cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs, cloudCacheTtl, cloudKeys, cloudValues).WithProviderFrom(path);
        string[]? partitions = null;
        if (partitionBy is not null)
        {
            // The selector's columns, in its order, from the frame's schema.
            using LazyFrameHandle probe = LazySelect(LazyClone(lf), [SelectorToExpr(partitionBy)]);
            partitions = [.. Helpers.PolarsSchemaExtractor.GetFields(probe).Select(f => { f.Type.Dispose(); return f.Name; })];
            if (partitions.Length == 0) partitions = null;
        }
        var kernelMode = mode switch
        {
            PlDeltaSaveMode.Append => Delta.KernelWriteMode.Append,
            PlDeltaSaveMode.Overwrite => Delta.KernelWriteMode.Overwrite,
            PlDeltaSaveMode.ErrorIfExists => Delta.KernelWriteMode.ErrorIfExists,
            PlDeltaSaveMode.Ignore => Delta.KernelWriteMode.Ignore,
            _ => throw new ArgumentOutOfRangeException(nameof(mode)),
        };
        var options = new Delta.KernelWriteOptions(
            MaxRowsPerFile: maxRowsPerFile,
            ApproxBytesPerFile: approxBytesPerFile == 0 ? null : approxBytesPerFile, // 0: the table's target size
            Compression: compression,
            CompressionLevel: compressionLevel,
            RowGroupSize: (int)Math.Min(rowGroupSize, int.MaxValue),
            DataPageSize: (int)Math.Min(dataPageSize, int.MaxValue),
            KeysPreGrouped: keysPreGrouped,
            MaintainOrder: maintainOrder,
            SyncOnClose: syncOnClose);
        Delta.DeltaKernelWrite.Write(path, lf, kernelMode, partitions, cloud, configuration: null, canEvolve, options);
    }
    /// <summary>
    /// Creates an empty Delta table (version 0) with the schema (borrowed), partition columns, table properties and
    /// identity columns given: the arrays of the identity columns run in parallel.
    /// </summary>
    public static void DeltaCreateTable(
        string path,
        SchemaHandle schema,
        string[]? partitionBy,
        string[]? propertyKeys,
        string[]? propertyValues,
        string[]? identityColumns,
        long[]? identityStarts,
        long[]? identitySteps,
        bool[]? identityGeneratedAlways,
        PlCloudProvider cloudProvider,
        nuint cloudRetries,
        ulong cloudRetryTimeoutMs,
        ulong cloudRetryInitBackoffMs,
        ulong cloudRetryMaxBackoffMs,
        ulong cloudCacheTtl,
        string[]? cloudKeys,
        string[]? cloudValues)
    {
        ArgumentNullException.ThrowIfNull(schema);
        Dictionary<string, string>? properties = null;
        if (propertyKeys is { Length: > 0 })
        {
            if (propertyValues is null || propertyValues.Length != propertyKeys.Length)
                throw new ArgumentException("Every property needs a value.", nameof(propertyValues));
            properties = new Dictionary<string, string>(StringComparer.Ordinal);
            for (int i = 0; i < propertyKeys.Length; i++) properties[propertyKeys[i]] = propertyValues[i];
        }
        var identity = new List<Delta.KernelIdentityColumn>();
        for (int i = 0; i < (identityColumns?.Length ?? 0); i++)
        {
            identity.Add(new Delta.KernelIdentityColumn(
                identityColumns![i], identityStarts?[i] ?? 1, identitySteps?[i] ?? 1, identityGeneratedAlways?[i] ?? true));
        }
        using DataFrameHandle empty = DataFrameFromSchema(schema, 0);
        var tableSchema = Delta.DeltaKernelWrite.SchemaOf(empty);
        var cloud = new Delta.CloudArgs(cloudProvider, cloudRetries, cloudRetryTimeoutMs, cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs, cloudCacheTtl, cloudKeys, cloudValues).WithProviderFrom(path);
        Delta.DeltaKernelCreate.Create(path, tableSchema, partitionBy is { Length: > 0 } ? partitionBy : null, properties, identity, cloud);
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
        Delta.DeltaKernelDelete.Delete(path, predicate, fileFilter: null, new Delta.CloudArgs(cloudProvider, cloudRetries, cloudRetryTimeoutMs, cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs, cloudCacheTtl, cloudKeys, cloudValues).WithProviderFrom(path));
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

        var clauses = new List<Delta.MergeClause>(actionTypes.Length);
        for (int i = 0; i < actionTypes.Length; i++)
        {
            ExprHandle condition = actionExprs[i] ?? throw new ArgumentNullException(nameof(actionExprs), $"Expression at index {i} cannot be null.");
            Delta.MergeClauseKind kind = actionTypes[i] switch
            {
                PlMergeActionType.MatchedUpdate => Delta.MergeClauseKind.MatchedUpdate,
                PlMergeActionType.MatchedDelete => Delta.MergeClauseKind.MatchedDelete,
                PlMergeActionType.NotMatchedInsert => Delta.MergeClauseKind.NotMatchedInsert,
                PlMergeActionType.NotMatchedBySourceDelete => Delta.MergeClauseKind.NotMatchedBySourceDelete,
                _ => throw new ArgumentOutOfRangeException(nameof(actionTypes)),
            };
            clauses.Add(new Delta.MergeClause(kind, condition));
        }
        Delta.DeltaKernelMerge.Merge(path, sourceLf, mergeKeys, clauses, can_evolve, new Delta.CloudArgs(cloudProvider, cloudRetries, cloudRetryTimeoutMs, cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs, cloudCacheTtl, cloudKeys, cloudValues).WithProviderFrom(path));
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
        return Delta.DeltaKernelScan.ReadChangeFeed(path, startVersion, endVersion, requireEnabled: true, new Delta.CloudArgs(cloudProvider, cloudRetries, cloudRetryTimeoutMs, cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs, cloudCacheTtl, cloudKeys, cloudValues).WithProviderFrom(path));
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
        return Delta.DeltaKernelScan.ReadChangeFeed(path, DateTimeOffset.FromUnixTimeMilliseconds(startTimestampMs),
            DateTimeOffset.FromUnixTimeMilliseconds(endTimestampMs), requireEnabled: true, new Delta.CloudArgs(cloudProvider, cloudRetries, cloudRetryTimeoutMs, cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs, cloudCacheTtl, cloudKeys, cloudValues).WithProviderFrom(path));
    }
}