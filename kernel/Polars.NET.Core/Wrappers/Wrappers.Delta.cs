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
        string[]? cloudValues,
        bool full = false
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
            cloudRetryMaxBackoffMs, cloudCacheTtl, cloudKeys, cloudValues).WithProviderFrom(path), full);
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
    /// <summary>Drops a table feature (Spark's ALTER TABLE … DROP FEATURE [TRUNCATE HISTORY]).</summary>
    public static void DropFeature(string path, string featureName, string[]? cloudKeys, string[]? cloudValues, bool truncateHistory = false) =>
        Delta.DeltaKernelAlter.DropFeature(path, featureName, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues), truncateHistory);
    /// <summary>
    /// Rewrites the files with deletion vectors and the files holding dropped columns (Spark's REORG … APPLY (PURGE));
    /// returns the number of files rewritten.
    /// </summary>
    public static ulong DeltaPurge(
        string path, string[]? cloudKeys, string[]? cloudValues, string[]? filterColumns = null, string?[]? filterValues = null) =>
        (ulong)(Delta.DeltaKernelOptimize.Purge(path, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues), Filters(filterColumns, filterValues))?.RemovedFiles ?? 0);

    /// <summary>Partition filters as columns and values side by side (a null value: the column is null); null when none.</summary>
    internal static Dictionary<string, string?>? Filters(string[]? columns, string?[]? values)
    {
        if (columns is not { Length: > 0 }) return null;
        if (values is null || values.Length != columns.Length) throw new ArgumentException("Each partition filter column needs one value.", nameof(values));
        Dictionary<string, string?> filters = new(StringComparer.Ordinal);
        for (int i = 0; i < columns.Length; i++) filters[columns[i]] = values[i];
        return filters;
    }
    /// <summary>Changes a table's liquid clustering columns (empty: none).</summary>
    public static void DeltaClusterBy(string path, string[] columns, string[]? cloudKeys, string[]? cloudValues)
    {
        ArgumentNullException.ThrowIfNull(columns);
        Delta.DeltaKernelAlter.ClusterBy(path, columns, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    }
    /// <summary>Renames a column of a table (column mapping needed).</summary>
    public static void DeltaRenameColumn(string path, string column, string newName, string[]? cloudKeys, string[]? cloudValues) =>
        Delta.DeltaKernelAlter.RenameColumn(path, column, newName, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    /// <summary>Drops columns of a table in one commit (column mapping needed).</summary>
    public static void DeltaDropColumns(string path, string[] columns, bool ifExists, string[]? cloudKeys, string[]? cloudValues)
    {
        ArgumentNullException.ThrowIfNull(columns);
        Delta.DeltaKernelAlter.DropColumns(path, columns, ifExists, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    }
    /// <summary>Widens the type of a column of a table (type widening needed).</summary>
    public static void DeltaChangeColumnType(string path, string column, DataTypeHandle type, string[]? cloudKeys, string[]? cloudValues) =>
        Delta.DeltaKernelAlter.ChangeColumnType(path, column, type, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    /// <summary>Lets a column hold nulls (ALTER COLUMN … DROP NOT NULL).</summary>
    public static void DeltaDropColumnNotNull(string path, string column, string[]? cloudKeys, string[]? cloudValues) =>
        Delta.DeltaKernelAlter.DropNotNull(path, column, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    /// <summary>Sets a column's comment (ALTER COLUMN … COMMENT).</summary>
    public static void DeltaSetColumnComment(string path, string column, string comment, string[]? cloudKeys, string[]? cloudValues) =>
        Delta.DeltaKernelAlter.SetComment(path, column, comment, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    /// <summary>Adds a CHECK constraint (ALTER TABLE … ADD CONSTRAINT), checked against the table's rows.</summary>
    public static void DeltaAddConstraint(string path, string name, string expression, string[]? cloudKeys, string[]? cloudValues) =>
        Delta.DeltaKernelAlter.AddConstraint(path, name, expression, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    /// <summary>Drops a CHECK constraint (ALTER TABLE … DROP CONSTRAINT [IF EXISTS]).</summary>
    public static void DeltaDropConstraint(string path, string name, bool ifExists, string[]? cloudKeys, string[]? cloudValues) =>
        Delta.DeltaKernelAlter.DropConstraint(path, name, ifExists, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    /// <summary>Sets a column's default (ALTER COLUMN … SET DEFAULT).</summary>
    public static void DeltaSetColumnDefault(string path, string column, string expression, string[]? cloudKeys, string[]? cloudValues) =>
        Delta.DeltaKernelAlter.SetColumnDefault(path, column, expression, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    /// <summary>Drops a column's default (ALTER COLUMN … DROP DEFAULT).</summary>
    public static void DeltaDropColumnDefault(string path, string column, string[]? cloudKeys, string[]? cloudValues) =>
        Delta.DeltaKernelAlter.DropColumnDefault(path, column, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    /// <summary>
    /// The session time zone of the Delta kernel: the zone the SQL expressions of check constraints, generated
    /// columns and column defaults read instants in (an IANA name; UTC by default). Process-wide.
    /// </summary>
    public static string DeltaSessionTimeZone
    {
        get => Delta.DeltaSession.TimeZone;
        set => Delta.DeltaSession.TimeZone = value;
    }
    /// <summary>Generated columns from parallel arrays of columns and expressions.</summary>
    internal static List<Kaminari.Seseragi.Lakehouse.Delta.DeltaGeneratedColumn> GeneratedColumns(string[]? columns, string[]? expressions)
    {
        if ((columns?.Length ?? 0) != (expressions?.Length ?? 0)) throw new ArgumentException("Each generated column needs one expression.", nameof(expressions));
        return [.. (columns ?? []).Select((c, i) => new Kaminari.Seseragi.Lakehouse.Delta.DeltaGeneratedColumn(c, expressions![i]))];
    }
    /// <summary>Column defaults from parallel arrays of columns and expressions.</summary>
    internal static List<Kaminari.Seseragi.Lakehouse.Delta.DeltaColumnDefault> ColumnDefaults(string[]? columns, string[]? expressions)
    {
        if ((columns?.Length ?? 0) != (expressions?.Length ?? 0)) throw new ArgumentException("Each column default needs one expression.", nameof(expressions));
        return [.. (columns ?? []).Select((c, i) => new Kaminari.Seseragi.Lakehouse.Delta.DeltaColumnDefault(c, expressions![i]))];
    }
    /// <summary>Adds columns (ALTER TABLE … ADD COLUMNS): the arrays run in parallel; the types are borrowed.</summary>
    public static void DeltaAddColumns(
        string path, string[] columns, DataTypeHandle[] types, string?[] comments, string?[] afters, bool[] firsts,
        string[]? cloudKeys, string[]? cloudValues)
    {
        ArgumentNullException.ThrowIfNull(columns);
        if (types.Length != columns.Length || comments.Length != columns.Length || afters.Length != columns.Length || firsts.Length != columns.Length)
            throw new ArgumentException("Each column needs a type, a comment, a position.", nameof(types));
        Delta.KernelNewColumn[] added = [.. columns.Select((c, i) => new Delta.KernelNewColumn(c, types[i], comments[i], afters[i], firsts[i]))];
        Delta.DeltaKernelAlter.AddColumns(path, added, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    }
    /// <summary>Removes table properties (ALTER TABLE … UNSET TBLPROPERTIES [IF EXISTS]).</summary>
    public static void DeltaUnsetTableProperties(string path, string[] keys, bool ifExists, string[]? cloudKeys, string[]? cloudValues)
    {
        ArgumentNullException.ThrowIfNull(keys);
        Delta.DeltaKernelAlter.UnsetProperties(path, keys, ifExists, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    }
    /// <summary>What DESCRIBE DETAIL tells about a table.</summary>
    public static Kaminari.Seseragi.Lakehouse.Delta.TableDetail DeltaDetail(string path, string[]? cloudKeys, string[]? cloudValues) =>
        Delta.DeltaKernelAlter.Detail(path, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    /// <summary>Shallow-clones a table (CREATE TABLE … SHALLOW CLONE); version below 0: the latest (or the one the timestamp picks).</summary>
    public static void DeltaClone(
        string sourcePath, string targetPath, long? version, DateTimeOffset? timestamp, string[]? propertyKeys, string[]? propertyValues,
        string[]? cloudKeys, string[]? cloudValues) =>
        Delta.DeltaKernelClone.Clone(sourcePath, targetPath, version, timestamp, Properties(propertyKeys, propertyValues),
            Delta.CloudArgs.ForLocation(sourcePath, cloudKeys, cloudValues));
    /// <summary>Converts a directory of Parquet files into a Delta table (CONVERT TO DELTA); the partition schema is borrowed.</summary>
    public static void DeltaConvertToDelta(string path, SchemaHandle? partitionSchema, bool collectStatistics, string[]? cloudKeys, string[]? cloudValues)
    {
        Kaminari.Seseragi.Lakehouse.Delta.Schema.StructType? partitions = null;
        if (partitionSchema is not null)
        {
            using DataFrameHandle empty = DataFrameFromSchema(partitionSchema, 0);
            partitions = Delta.DeltaKernelWrite.SchemaOf(empty);
        }
        Delta.DeltaKernelConvert.Convert(path, partitions, collectStatistics, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
    }
    /// <summary>Moves a column (ALTER COLUMN … FIRST / AFTER; after null: first).</summary>
    public static void DeltaMoveColumn(string path, string column, string? after, string[]? cloudKeys, string[]? cloudValues) =>
        Delta.DeltaKernelAlter.MoveColumn(path, column, after, Delta.CloudArgs.ForLocation(path, cloudKeys, cloudValues));
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
        string? rowCommitVersionColumn = null,
        bool variantAsJson = false,
        Kaminari.Samidare.IR.ScalarExpr? rowFilter = null,
        Action<int>? scannedFiles = null)
    {
        // The managed Delta kernel resolves the table; partition values come from the log, so the glob and Hive
        // options do not apply.
        _ = glob; _ = hivePartitioning; _ = hivePartitionSchema; _ = tryParseHiveDates;
        var cloud = new Delta.CloudArgs(cloudProvider, cloudRetries, cloudRetryTimeoutMs, cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs, cloudCacheTtl, cloudKeys, cloudValues).WithProviderFrom(path);
        DateTimeOffset? asOf = datetime is null ? null : Delta.DeltaKernelScan.ParseAsOf(datetime);
        long? atVersion = version is ulong v ? checked((long)v) : null;
        var options = new Delta.KernelScanOptions(nRows, rowIndexName, rowIndexOffset, includePathColumn, parallel, lowMemory,
            useStatistics, rechunk, cache, schema, rowIdColumn, rowCommitVersionColumn, variantAsJson);
        LazyFrameHandle lf = Delta.DeltaKernelScan.ScanPath(path, atVersion, asOf, null, cloud, scannedFiles, options, rowFilter,
            out long read, out bool castsTypes);
        // A query over this frame (LINQ) may scan it again at the same version, leaving out the files its filter rules
        // out: not when a given schema changes a column's type (the filter then reads values other than the
        // statistics describe).
        if (Delta.DeltaScanOrigin.AllowsSkipping(nRows, rowIndexName) && !castsTypes)
        {
            // The given schema is the caller's, released after this call: the origin keeps its own copy (the
            // collector releases it with the origin).
            SchemaHandle? ownSchema = schema is null ? null : SchemaClone(schema);
            lf.DeltaOrigin = new Delta.DeltaScanOrigin(read, (filter, scanned) => ScanDelta(path, (ulong)read, null, nRows, parallel, lowMemory,
                useStatistics, glob, rechunk, cache, rowIndexName, rowIndexOffset, includePathColumn, ownSchema, hivePartitioning,
                null, tryParseHiveDates, cloudProvider, cloudRetries, cloudRetryTimeoutMs, cloudRetryInitBackoffMs,
                cloudRetryMaxBackoffMs, cloudCacheTtl, cloudKeys, cloudValues, rowIdColumn, rowCommitVersionColumn, variantAsJson,
                filter, scanned));
        }
        return lf;
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
        string[]? cloudValues,
        string[]? clusterBy = null,
        IReadOnlyDictionary<string, string>? configuration = null,
        IReadOnlyList<string>? catalogFeatures = null
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
        Delta.DeltaKernelWrite.Write(path, lf, kernelMode, partitions, cloud, configuration, canEvolve, options,
            clusterBy is { Length: > 0 } ? clusterBy : null, catalogFeatures);
    }
    /// <summary>
    /// Creates an empty Delta table (version 0) with the schema (borrowed), partition columns, table properties,
    /// identity columns, generated columns and column defaults given: the arrays of each kind run in parallel.
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
        string[]? cloudValues,
        string[]? clusterBy = null,
        string[]? generatedColumns = null,
        string[]? generationExpressions = null,
        string[]? defaultColumns = null,
        string[]? defaultExpressions = null,
        bool replace = false,
        bool orCreate = false)
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
        if (replace)
            Delta.DeltaKernelCreate.Replace(path, tableSchema, partitionBy is { Length: > 0 } ? partitionBy : null, properties, identity, cloud,
                clusterBy is { Length: > 0 } ? clusterBy : null,
                GeneratedColumns(generatedColumns, generationExpressions), ColumnDefaults(defaultColumns, defaultExpressions), orCreate);
        else
            Delta.DeltaKernelCreate.Create(path, tableSchema, partitionBy is { Length: > 0 } ? partitionBy : null, properties, identity, cloud,
                clusterBy is { Length: > 0 } ? clusterBy : null,
                GeneratedColumns(generatedColumns, generationExpressions), ColumnDefaults(defaultColumns, defaultExpressions));
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
    public static void DeltaUpdate(
        string path,
        ExprHandle? predicate,
        string[] columns,
        ExprHandle[] values,
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
        if (columns.Length != values.Length)
            throw new ArgumentException("Each updated column needs one value.", nameof(values));
        var assignments = new List<(string Column, ExprHandle Value)>(columns.Length);
        for (int i = 0; i < columns.Length; i++) assignments.Add((columns[i], values[i]));
        Delta.DeltaKernelUpdate.Update(path, predicate, assignments, fileFilter: null, new Delta.CloudArgs(cloudProvider, cloudRetries, cloudRetryTimeoutMs, cloudRetryInitBackoffMs,
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
        string[]? cloudValues,
        bool allowSchemaChanges = false
    )
    {
        return Delta.DeltaKernelScan.ReadChangeFeed(path, startVersion, endVersion, requireEnabled: true, new Delta.CloudArgs(cloudProvider, cloudRetries, cloudRetryTimeoutMs, cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs, cloudCacheTtl, cloudKeys, cloudValues).WithProviderFrom(path), allowSchemaChanges);
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
        string[]? cloudValues,
        bool allowSchemaChanges = false
    )
    {
        return Delta.DeltaKernelScan.ReadChangeFeed(path, DateTimeOffset.FromUnixTimeMilliseconds(startTimestampMs),
            DateTimeOffset.FromUnixTimeMilliseconds(endTimestampMs), requireEnabled: true, new Delta.CloudArgs(cloudProvider, cloudRetries, cloudRetryTimeoutMs, cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs, cloudCacheTtl, cloudKeys, cloudValues).WithProviderFrom(path), allowSchemaChanges);
    }
}