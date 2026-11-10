using Kaminari.Seseragi.Catalog;
using Kaminari.Seseragi.Catalog.Unity;
using Polars.NET.Core.Enums;
using Polars.NET.Core.Exceptions;

namespace Polars.NET.Core.Wrappers;

// Unity Catalog operations. The catalog itself is spoken to by the managed Seseragi client: each operation
// asks it for the table location and temporary credentials, then runs the path-based Delta operation with them.
public readonly partial struct PolarsWrapper
{
    public static CatalogHandle InitUnityCatalog(string workspaceUrl, string bearerToken)
    {
        try
        {
            return new CatalogHandle(new UnityCatalogClient(workspaceUrl, bearerToken));
        }
        catch (ArgumentException e)
        {
            throw new PolarsException(e.Message, e);
        }
    }

    /// <summary>
    /// Asks the catalog where the table lives and for temporary credentials, and lays those credentials
    /// over the caller's cloud options. Credentials are fetched once per call and are not renewed.
    /// </summary>
    private static TableAccess ResolveCatalogTable(
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        bool needsWrite,
        ref string[]? cloudKeys,
        ref string[]? cloudValues)
    {
        ArgumentNullException.ThrowIfNull(handle);

        TableAccess access;
        try
        {
            access = handle.Client.ResolveTableAccess(catalogName, schemaName, tableName, needsWrite);
        }
        catch (CatalogException e)
        {
            throw new PolarsException($"Failed to resolve catalog table {catalogName}.{schemaName}.{tableName}: {e.Message}", e);
        }

        int baseLen = Math.Min(cloudKeys?.Length ?? 0, cloudValues?.Length ?? 0);
        var baseOptions = new KeyValuePair<string, string>[baseLen];
        for (int i = 0; i < baseLen; i++)
        {
            baseOptions[i] = new(cloudKeys![i], cloudValues![i]);
        }

        Dictionary<string, string> merged = access.MergeStorageOptions(baseOptions);
        cloudKeys = [.. merged.Keys];
        cloudValues = [.. merged.Values];
        return access;
    }

    public static LazyFrameHandle ScanCatalogTable
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
        TableAccess access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, false, ref cloudKeys, ref cloudValues);

        string? format = access.Table.DataSourceFormat;
        if (format is null)
            throw new PolarsException("Data source format is missing from Unity Catalog response");
        if (!string.Equals(format, UnityDataSourceFormats.Delta, StringComparison.OrdinalIgnoreCase))
            throw new PolarsException($"Unsupported data source format: {format}");

        return ScanDelta(
            access.Location,
            version,
            datetime,
            nRows,
            parallel,
            lowMemory,
            useStatistics,
            glob,
            rechunk,
            cache,
            rowIndexName,
            rowIndexOffset,
            includePathColumn,
            schema,
            hivePartitioning,
            hivePartitionSchema,
            tryParseHiveDates,
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues);
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
        TableAccess access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);

        SinkDelta(
            lf,
            access.Location,
            mode,
            canEvolve,
            partitionBy,
            includeKeys,
            keysPreGrouped,
            maxRowsPerFile,
            approxBytesPerFile,
            compression,
            compressionLevel,
            statistics,
            rowGroupSize,
            dataPageSize,
            compatLevel,
            maintainOrder,
            syncOnClose,
            mkdir,
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues);
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
        TableAccess access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);

        DeltaDelete(
            access.Location,
            predicate,
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues);
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
        TableAccess access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);

        DeltaMergeOrdered(
            sourceLf,
            access.Location,
            mergeKeys,
            actionTypes,
            actionExprs,
            can_evolve,
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues);
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
        TableAccess access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);

        return Optimize(
            access.Location,
            targetSizeMb,
            filterJson,
            zOrderCols,
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues);
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
        ArgumentNullException.ThrowIfNull(handle);
        if (schema is null || schema.IsInvalid) throw new PolarsException("Schema pointer is null");

        string unityTableType = tableType switch
        {
            PlCatalogTableType.Managed => UnityTableTypes.Managed,
            PlCatalogTableType.External => UnityTableTypes.External,
            _ => throw new PolarsException("Invalid table_type"),
        };

        ulong len = GetSchemaLen(schema);
        var columns = new List<ColumnInfo>((int)len);
        for (ulong i = 0; i < len; i++)
        {
            GetSchemaFieldAt(schema, i, out string name, out DataTypeHandle typeHandle);
            using (typeHandle)
            {
                columns.Add(ColumnInfo.Create(name, ToUnityType(typeHandle), position: (int)i));
            }
        }

        try
        {
            handle.Client.CreateTable(
                catalogName,
                schemaName,
                tableName,
                unityTableType,
                UnityDataSourceFormats.Delta,
                columns,
                storageLocation);
        }
        catch (CatalogException e)
        {
            throw new PolarsException($"Failed to create table: {e.Message}", e);
        }
    }

    /// <summary>
    /// Maps a column type to its Unity Catalog type. A list of a two-field struct named key and value is a map.
    /// </summary>
    private static UnityDataType ToUnityType(DataTypeHandle dtype)
    {
        PlDataType kind = GetDataTypeKind(dtype);
        switch (kind)
        {
            case PlDataType.Boolean: return UnityDataType.Boolean;
            case PlDataType.Int8: return UnityDataType.Byte;
            case PlDataType.Int16: return UnityDataType.Short;
            case PlDataType.Int32: return UnityDataType.Int;
            case PlDataType.Int64: return UnityDataType.Long;
            case PlDataType.Float32: return UnityDataType.Float;
            case PlDataType.Float64: return UnityDataType.Double;
            case PlDataType.Date: return UnityDataType.Date;
            case PlDataType.String: return UnityDataType.String;
            case PlDataType.Binary: return UnityDataType.Binary;
            case PlDataType.Null: return UnityDataType.Null;

            case PlDataType.Datetime:
                if (GetTimeUnit(dtype) == PlTimeUnit.Microseconds)
                {
                    return string.IsNullOrEmpty(GetTimeZone(dtype)) ? UnityDataType.TimestampNtz : UnityDataType.Timestamp;
                }
                break;

            case PlDataType.Decimal:
                GetDecimalInfo(dtype, out int precision, out int scale);
                return UnityDataType.Decimal(precision, scale);

            case PlDataType.List:
            {
                using DataTypeHandle inner = GetInnerType(dtype);
                if (GetDataTypeKind(inner) == PlDataType.Struct && GetStructLen(inner) == 2)
                {
                    GetStructField(inner, 0, out string keyName, out DataTypeHandle keyType);
                    GetStructField(inner, 1, out string valueName, out DataTypeHandle valueType);
                    using (keyType)
                    using (valueType)
                    {
                        if (keyName == "key" && valueName == "value")
                        {
                            return UnityDataType.Map(ToUnityType(keyType), ToUnityType(valueType));
                        }
                    }
                }
                return UnityDataType.Array(ToUnityType(inner));
            }

            case PlDataType.Struct:
            {
                ulong len = GetStructLen(dtype);
                var fields = new UnityField[len];
                for (ulong i = 0; i < len; i++)
                {
                    GetStructField(dtype, i, out string name, out DataTypeHandle fieldType);
                    using (fieldType)
                    {
                        fields[i] = new UnityField(name, ToUnityType(fieldType));
                    }
                }
                return UnityDataType.Struct(fields);
            }
        }

        throw new PolarsException($"Unsupported type for a Unity Catalog column: {GetDataTypeString(dtype)}");
    }

    public static void DeleteCatalogTable
    (
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName
    )
    {
        ArgumentNullException.ThrowIfNull(handle);
        try
        {
            handle.Client.DeleteTable(catalogName, schemaName, tableName);
        }
        catch (CatalogException e)
        {
            throw new PolarsException($"Failed to delete table: {e.Message}", e);
        }
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
        TableAccess access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);
        return Vacuum(access.Location, retentionHours, enforceRetention, dryRun, vacuumModeFull, cloudKeys, cloudValues);
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
        TableAccess access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);
        return Restore(access.Location, targetVersion, targetTimestamp, ignoreMissingFiles, protocolDowngradeAllowed, cloudKeys, cloudValues);
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
        TableAccess access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, false, ref cloudKeys, ref cloudValues);
        return History(access.Location, limit, cloudKeys, cloudValues);
    }

    public static void CatalogAddFeature(
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        string featureName,
        bool allowProtocolIncrease,
        string[]? cloudKeys,
        string[]? cloudValues
    )
    {
        TableAccess access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);
        AddFeature(access.Location, featureName, allowProtocolIncrease, cloudKeys, cloudValues);
    }

    public static LazyFrameHandle CatalogReadCdc(
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        ulong startVersion,
        ulong endVersion,
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
        TableAccess access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, false, ref cloudKeys, ref cloudValues);

        return DeltaReadCdc(
            access.Location,
            checked((long)startVersion),
            checked((long)endVersion),
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues);
    }

    public static LazyFrameHandle CatalogReadCdcByTime(
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
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
        TableAccess access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, false, ref cloudKeys, ref cloudValues);

        return DeltaReadCdcByTime(
            access.Location,
            startTimestampMs,
            endTimestampMs,
            cloudProvider,
            cloudRetries,
            cloudRetryTimeoutMs,
            cloudRetryInitBackoffMs,
            cloudRetryMaxBackoffMs,
            cloudCacheTtl,
            cloudKeys,
            cloudValues);
    }
}
