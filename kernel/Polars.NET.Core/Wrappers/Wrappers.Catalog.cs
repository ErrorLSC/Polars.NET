using Kaminari.Seseragi.Lakehouse.Catalog;
using Kaminari.Seseragi.Lakehouse.Catalog.Unity;
using Polars.NET.Core.Delta;
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
    /// over the caller's cloud options. Credentials that expire are renewed: the options also name the table's
    /// renewable source (<see cref="HostCredentials"/>), which asks the catalog again before they expire. A
    /// catalog-managed table is read and committed through the catalog while the result is not disposed; an operation
    /// the catalog does not allow on such a table (<paramref name="refusedWhenManaged"/>) is refused before anything
    /// else happens.
    /// </summary>
    private static CatalogTable ResolveCatalogTable(
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        bool needsWrite,
        ref string[]? cloudKeys,
        ref string[]? cloudValues,
        string? refusedWhenManaged = null) =>
        ResolveCatalogTableCore(handle, catalogName, schemaName, tableName, needsWrite, ref cloudKeys, ref cloudValues, refusedWhenManaged, allowMissing: false)!;

    /// <summary><see cref="ResolveCatalogTable"/>, or null when the catalog has no such table.</summary>
    private static CatalogTable? TryResolveCatalogTable(
        CatalogHandle handle, string catalogName, string schemaName, string tableName, bool needsWrite, ref string[]? cloudKeys, ref string[]? cloudValues) =>
        ResolveCatalogTableCore(handle, catalogName, schemaName, tableName, needsWrite, ref cloudKeys, ref cloudValues, null, allowMissing: true);

    private static CatalogTable? ResolveCatalogTableCore(
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        bool needsWrite,
        ref string[]? cloudKeys,
        ref string[]? cloudValues,
        string? refusedWhenManaged,
        bool allowMissing)
    {
        ArgumentNullException.ThrowIfNull(handle);

        TableAccess access;
        try
        {
            TableAccess? found = allowMissing
                ? handle.Client.TryResolveTableAccess(catalogName, schemaName, tableName, needsWrite)
                : handle.Client.ResolveTableAccess(catalogName, schemaName, tableName, needsWrite);
            if (found is null) return null;
            access = found;
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
        if (access.Credentials.ExpiresAt is not null)
            merged[HostCredentials.Key] = handle.RenewableCredentials(access.Table.TableId, needsWrite, access.Credentials)
                .ToString(System.Globalization.CultureInfo.InvariantCulture);
        cloudKeys = [.. merged.Keys];
        cloudValues = [.. merged.Values];

        if (!IsCatalogManaged(access.Table)) return new CatalogTable(access, null);
        if (refusedWhenManaged is not null)
            throw new PolarsException(
                $"{refusedWhenManaged} is not supported on the catalog-managed table {catalogName}.{schemaName}.{tableName} (Delta Spark refuses it too).");
        var catalog = new UnityCatalogCommitter(handle.Client, catalogName, schemaName, tableName);
        return new CatalogTable(access, CatalogScope.Enter(access.Location, catalog));

        static bool IsCatalogManaged(TableInfo table) =>
            table.Properties.TryGetValue("delta.feature.catalogManaged", out string? state)
            && state is "supported" or "enabled";
    }

    /// <summary>
    /// A catalog table resolved for one operation: where it is, and for a catalog-managed table its catalog, in scope
    /// (<see cref="CatalogScope"/>) until disposed.
    /// </summary>
    private sealed class CatalogTable(TableAccess access, IDisposable? scope) : IDisposable
    {
        public TableInfo Table => access.Table;
        public string Location => access.Location;
        public void Dispose() => scope?.Dispose();
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
        string[]? cloudValues,
        string? rowIdColumn = null,
        string? rowCommitVersionColumn = null,
        bool variantAsJson = false)
    {
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, false, ref cloudKeys, ref cloudValues);

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
            cloudValues,
            rowIdColumn,
            rowCommitVersionColumn,
            variantAsJson);
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
        string[]? cloudValues,
        string[]? clusterBy = null)
    {
        string[]? callerKeys = cloudKeys, callerValues = cloudValues;
        CatalogTable? access = TryResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);
        if (access is null)
        {
            // No such table: a managed table is created with the rows as its version 0, as Spark's CREATE TABLE AS
            // SELECT creates one through the catalog.
            StagedTable staged;
            try
            {
                staged = StageManagedTable(handle, catalogName, schemaName, tableName, null, callerKeys, callerValues);
            }
            catch (PolarsException e)
            {
                throw new PolarsException(
                    $"The catalog table {catalogName}.{schemaName}.{tableName} does not exist, and it could not be created: {e.Message}", e);
            }
            Sink(staged.Staging.Location, staged.Cloud.Keys, staged.Cloud.Values, staged.Configuration, staged.Features);
            RegisterManagedTable(handle, catalogName, schemaName, tableName, staged);
            return;
        }
        using (access) Sink(access.Location, cloudKeys, cloudValues, null, null);

        void Sink(string location, string[]? keys, string[]? values, IReadOnlyDictionary<string, string>? configuration, IReadOnlyList<string>? catalogFeatures) =>
            SinkDelta(
                lf,
                location,
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
                keys,
                values,
                clusterBy,
                configuration,
                catalogFeatures);
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
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);

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
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);

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
        string[]? cloudValues,
        bool full = false
    )
    {
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues, "OPTIMIZE");

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
            cloudValues,
            full);
    }

    /// <summary>Changes a catalog table's liquid clustering columns (empty: none).</summary>
    public static void CatalogClusterBy(
        CatalogHandle handle, string catalogName, string schemaName, string tableName, string[] columns, string[]? cloudKeys, string[]? cloudValues)
    {
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);
        DeltaClusterBy(access.Location, columns, cloudKeys, cloudValues);
    }

    /// <summary>Renames a column of a catalog table (column mapping needed).</summary>
    public static void CatalogRenameColumn(
        CatalogHandle handle, string catalogName, string schemaName, string tableName, string column, string newName,
        string[]? cloudKeys, string[]? cloudValues)
    {
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues, "ALTER TABLE RENAME COLUMN");
        DeltaRenameColumn(access.Location, column, newName, cloudKeys, cloudValues);
    }

    /// <summary>Drops columns of a catalog table in one commit (column mapping needed).</summary>
    public static void CatalogDropColumns(
        CatalogHandle handle, string catalogName, string schemaName, string tableName, string[] columns, bool ifExists,
        string[]? cloudKeys, string[]? cloudValues)
    {
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);
        DeltaDropColumns(access.Location, columns, ifExists, cloudKeys, cloudValues);
    }

    /// <summary>Widens the type of a column of a catalog table (type widening needed).</summary>
    public static void CatalogChangeColumnType(
        CatalogHandle handle, string catalogName, string schemaName, string tableName, string column, DataTypeHandle type,
        string[]? cloudKeys, string[]? cloudValues)
    {
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);
        DeltaChangeColumnType(access.Location, column, type, cloudKeys, cloudValues);
    }

    /// <summary>
    /// Creates a catalog table. A managed table is created as Delta Spark creates one through Unity Catalog's Delta
    /// API: staged by the catalog, version 0 written at the staged location (catalog-managed, with the protocol and
    /// properties the catalog requires and the ones it suggests), then registered. An external table is registered at
    /// <paramref name="storageLocation"/> only (create its Delta table there by path).
    /// </summary>
    public static void CreateCatalogTable
    (
        CatalogHandle handle,
        string catalogName,
        string schemaName,
        string tableName,
        SchemaHandle schema,
        PlCatalogTableType tableType,
        string? storageLocation,
        string[]? partitionBy = null,
        string[]? clusterBy = null,
        string[]? propertyKeys = null,
        string[]? propertyValues = null,
        string[]? identityColumns = null,
        long[]? identityStarts = null,
        long[]? identitySteps = null,
        bool[]? identityGeneratedAlways = null,
        string[]? cloudKeys = null,
        string[]? cloudValues = null
    )
    {
        ArgumentNullException.ThrowIfNull(handle);
        if (schema is null || schema.IsInvalid) throw new PolarsException("Schema pointer is null");

        if (tableType == PlCatalogTableType.Managed)
        {
            Dictionary<string, string>? properties = Properties(propertyKeys, propertyValues);
            List<Delta.KernelIdentityColumn> identity = IdentityColumns(identityColumns, identityStarts, identitySteps, identityGeneratedAlways);
            using DataFrameHandle empty = DataFrameFromSchema(schema, 0);
            CreateManagedTable(
                handle, catalogName, schemaName, tableName, Delta.DeltaKernelWrite.SchemaOf(empty),
                partitionBy is { Length: > 0 } ? partitionBy : null, clusterBy is { Length: > 0 } ? clusterBy : null,
                properties, identity, cloudKeys, cloudValues);
            return;
        }
        if (tableType != PlCatalogTableType.External) throw new PolarsException("Invalid table_type");
        if (partitionBy is { Length: > 0 } || clusterBy is { Length: > 0 } || propertyKeys is { Length: > 0 } || identityColumns is { Length: > 0 })
            throw new PolarsException(
                "Partitioning, clustering, properties and identity columns are declared when a managed table is created; " +
                "for an external table, create the Delta table at its location (Delta.CreateTable), then register it.");

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
                UnityTableTypes.External,
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
    /// Creates a managed, catalog-managed table as Delta Spark does with Unity Catalog's Delta API: the name checked,
    /// the table staged, version 0 written at the staged location with the catalog's credentials (the catalog's
    /// required protocol and properties, its suggested ones the caller does not override), then the table registered
    /// with what version 0 holds.
    /// </summary>
    private static void CreateManagedTable(
        CatalogHandle handle, string catalogName, string schemaName, string tableName, Kaminari.Seseragi.Lakehouse.Delta.Schema.StructType schema,
        string[]? partitionBy, string[]? clusterBy, Dictionary<string, string>? properties, List<Delta.KernelIdentityColumn> identity,
        string[]? cloudKeys, string[]? cloudValues)
    {
        try
        {
            if (handle.Client.DeltaTableExists(catalogName, schemaName, tableName))
                throw new PolarsException($"The catalog table {catalogName}.{schemaName}.{tableName} already exists.");
        }
        catch (CatalogException e)
        {
            throw new PolarsException($"Failed to create the catalog table {catalogName}.{schemaName}.{tableName}: {e.Message}", e);
        }
        StagedTable staged = StageManagedTable(handle, catalogName, schemaName, tableName, properties, cloudKeys, cloudValues);
        Delta.DeltaKernelCreate.CreateCatalogManaged(
            staged.Staging.Location, schema, partitionBy, staged.Configuration, identity, staged.Cloud, clusterBy, staged.Features);
        RegisterManagedTable(handle, catalogName, schemaName, tableName, staged);
    }

    /// <summary>A managed table the catalog staged: what its version 0 gets, and how to reach its location.</summary>
    internal sealed record StagedTable(UnityStagingTable Staging, Dictionary<string, string> Configuration, IReadOnlyList<string> Features, Delta.CloudArgs Cloud);

    /// <summary>
    /// Stages a managed table: the catalog's location and credentials (laid over the caller's cloud options, for
    /// writing version 0, renewed through the catalog when they expire), the properties version 0 gets (the catalog's merged with <paramref name="properties"/>) and
    /// the features the catalog asks for.
    /// </summary>
    internal static StagedTable StageManagedTable(
        CatalogHandle handle, string catalogName, string schemaName, string tableName, Dictionary<string, string>? properties,
        string[]? cloudKeys, string[]? cloudValues)
    {
        UnityStagingTable staging;
        Dictionary<string, string> configuration;
        IReadOnlyList<string> features;
        try
        {
            staging = handle.Client.CreateStagingTable(catalogName, schemaName, tableName);
            configuration = staging.TableProperties(properties);
            features = staging.Features();
        }
        catch (CatalogException e)
        {
            throw new PolarsException($"Failed to create the catalog table {catalogName}.{schemaName}.{tableName}: {e.Message}", e);
        }
        var options = new Dictionary<string, string>(StringComparer.Ordinal);
        for (int i = 0; i < Math.Min(cloudKeys?.Length ?? 0, cloudValues?.Length ?? 0); i++) options[cloudKeys![i]] = cloudValues![i];
        foreach (var (key, value) in staging.Credentials.ToStorageOptions()) options[key] = value;
        // Credentials that expire are renewed while version 0 is written (a CREATE TABLE AS SELECT may outlast them).
        if (staging.Credentials.ExpiresAt is not null)
            options[HostCredentials.Key] = handle.RenewableStagingCredentials(staging.TableId, staging.Location, staging.Credentials)
                .ToString(System.Globalization.CultureInfo.InvariantCulture);
        var cloud = new Delta.CloudArgs(Keys: [.. options.Keys], Values: [.. options.Values]).WithProviderFrom(staging.Location);
        return new StagedTable(staging, configuration, features, cloud);
    }

    /// <summary>Registers a staged table with what its version 0 (written at the staged location) holds.</summary>
    private static void RegisterManagedTable(CatalogHandle handle, string catalogName, string schemaName, string tableName, StagedTable staged)
    {
        string location = staged.Staging.Location;
        Delta.DeltaKernelCreate.CreatedTable created = Delta.DeltaKernelCreate.Version0(location, staged.Cloud);
        try
        {
            handle.Client.CreateDeltaTable(catalogName, schemaName, UnityManagedTables.CreateRequest(
                tableName, location, created.Metadata, created.Protocol, created.Domains, created.Timestamp));
        }
        catch (CatalogException e)
        {
            string name = $"{catalogName}.{schemaName}.{tableName}";
            throw new PolarsException(e.StatusCode is null
                ? $"Version 0 of {name} was written at {location}, but whether the catalog registered the table is not known (no answer): {e.Message}"
                : $"Version 0 of {name} was written at {location}, but the catalog did not register the table: {e.Message}", e);
        }
    }

    private static Dictionary<string, string>? Properties(string[]? keys, string[]? values)
    {
        if (keys is not { Length: > 0 }) return null;
        if (values is null || values.Length != keys.Length) throw new ArgumentException("Every property needs a value.", nameof(values));
        var properties = new Dictionary<string, string>(StringComparer.Ordinal);
        for (int i = 0; i < keys.Length; i++) properties[keys[i]] = values[i];
        return properties;
    }

    private static List<Delta.KernelIdentityColumn> IdentityColumns(string[]? columns, long[]? starts, long[]? steps, bool[]? generatedAlways)
    {
        List<Delta.KernelIdentityColumn> identity = [];
        for (int i = 0; i < (columns?.Length ?? 0); i++)
            identity.Add(new Delta.KernelIdentityColumn(columns![i], starts?[i] ?? 1, steps?[i] ?? 1, generatedAlways?[i] ?? true));
        return identity;
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
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues, "VACUUM");
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
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);
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
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, false, ref cloudKeys, ref cloudValues);
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
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues);
        AddFeature(access.Location, featureName, allowProtocolIncrease, cloudKeys, cloudValues);
    }

    /// <summary>Drops a table feature of a catalog table.</summary>
    public static void CatalogDropFeature(
        CatalogHandle handle, string catalogName, string schemaName, string tableName, string featureName, string[]? cloudKeys, string[]? cloudValues)
    {
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, true, ref cloudKeys, ref cloudValues, "DROP FEATURE");
        DropFeature(access.Location, featureName, cloudKeys, cloudValues);
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
        string[]? cloudValues,
        bool allowSchemaChanges = false
    )
    {
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, false, ref cloudKeys, ref cloudValues);

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
            cloudValues,
            allowSchemaChanges);
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
        string[]? cloudValues,
        bool allowSchemaChanges = false
    )
    {
        using CatalogTable access = ResolveCatalogTable(handle, catalogName, schemaName, tableName, false, ref cloudKeys, ref cloudValues);

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
            cloudValues,
            allowSchemaChanges);
    }
}
