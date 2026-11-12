#pragma warning disable CS1573
using System.Text;
using System.Text.Json;
using Polars.NET.Core;
using Polars.NET.Core.Wrappers;
using Polars.NET.Core.Helpers;
using Kaminari.Sazanami.Helpers;

namespace Polars.CSharp;

/// <summary>
/// Databricks Unity Catalog Client
/// </summary>
/// <remarks>
/// Init Unity Catalog connection
/// </remarks>
public class UnityCatalog(string workspaceUrl, string bearerToken) : IDisposable
{
    internal CatalogHandle Handle { get; } = PolarsWrapper.InitUnityCatalog(workspaceUrl, bearerToken);
    private bool _isDisposed;

    /// <summary>
    /// Scans a table managed by Unity Catalog into a <see cref="LazyFrame"/>.
    /// This method performs a two-step resolution:
    /// 1. It fetches the physical storage location and dynamic credentials from Unity Catalog.
    /// 2. It initializes a Polars scan on the underlying Delta files using the resolved credentials.
    /// Credentials that expire are fetched again before they do, also when the frame is collected long after; once
    /// this catalog is disposed, the frame works until the credentials it holds expire.
    /// </summary>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table to scan.</param>
    /// <inheritdoc cref="LazyFrame.ScanDelta"/>
    /// <param name="rowIdColumn">Adds each row's row id (Int64) under this name: stable across updates, merges and
    /// optimize (Spark's <c>_metadata.row_id</c>). The table must have row tracking on (<c>delta.enableRowTracking</c>).</param>
    /// <param name="rowCommitVersionColumn">Adds the version that last changed each row (Int64) under this name
    /// (Spark's <c>_metadata.row_commit_version</c>). The table must have row tracking on.</param>
    /// <param name="variantAsJson">Reads variant columns (at any depth) as JSON text, as Spark's <c>to_json</c> renders them,
    /// instead of as stored (a struct of the <c>value</c> and <c>metadata</c> binaries). Timestamps in variants are rendered in UTC.
    /// A table with variant columns is read through Polars.NET's own patch: every file the scan selects is read into memory
    /// when the scan is created and kept until the frame is released (no ranged reads; the engine still decodes only what it needs).</param>
    /// <returns>A <see cref="LazyFrame"/> for lazy execution of the catalog table scan.</returns>
    /// <exception cref="ArgumentException">Thrown when both <paramref name="version"/> and <paramref name="datetime"/> are provided.</exception>
    public LazyFrame ScanCatalogTable(
        string catalogName,
        string schemaName,
        string tableName,
        ulong? version = null,
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
        IntoSchema? schema = null,
        bool hivePartitioning = true,
        IntoSchema? hivePartitionSchema = null,
        bool tryParseHiveDates = true,
        CloudOptions? cloudOptions = null,
        string? rowIdColumn = null,
        string? rowCommitVersionColumn = null,
        bool variantAsJson = false)
    {
        if (version.HasValue && datetime != null)
        {
            throw new ArgumentException("Cannot specify both 'version' and 'datetime' for Delta Time Travel.");
        }

        var schemaHandle = schema?.Consume().Handle;
        var hiveSchemaHandle = hivePartitionSchema?.Consume().Handle;

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
            values,
            rowIdColumn,
            rowCommitVersionColumn,
            variantAsJson
        );

        return new LazyFrame(h);
    }
    /// <summary>
    /// Sink the LazyFrame to a Unity Catalog managed Delta Lake table with partition discovery. A table that does not
    /// exist is created as a managed, catalog-managed table with the rows as its version 0, as Spark's
    /// <c>CREATE TABLE AS SELECT</c> (see <see cref="CreateCatalogTable"/>), whatever the mode.
    /// </summary>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table to sink.</param>
    /// <param name="lf">The LazyFrame ready to be sinked.</param>
    /// <inheritdoc cref="LazyFrame.SinkDelta(string, IntoSelector?, DeltaSaveMode, bool, bool, bool, int, long, ParquetCompression, int, bool, uint, uint, int, bool, SyncOnClose, bool, CloudOptions?, IEnumerable{string}?)"/>
    public void SinkCatalogTable(
        string catalogName,
        string schemaName,
        string tableName,
        LazyFrame lf,
        IntoSelector? partitionBy = null,
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
        CloudOptions? cloudOptions = null,
        IEnumerable<string>? clusterBy = null)
    {
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) =
            CloudOptions.ParseCloudOptions(cloudOptions);
        using var partitionByH = partitionBy?.Consume().CloneHandle();
        PolarsWrapper.SinkCatalogTable(
            Handle,
            catalogName,
            schemaName,
            tableName,
            lf.CloneHandle(),
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
            values,
            clusterBy?.ToArray()
        );
    }
    /// <summary>
    /// Changes the liquid clustering columns of a catalog table (no columns: none); see <see cref="Delta.ClusterBy"/>.
    /// </summary>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="columns">The clustering columns, dotted for nested fields; at most four. Empty for none.</param>
    /// <param name="cloudOptions">Cloud-specific configurations, laid under the credentials the catalog vends.</param>
    public void ClusterCatalogTable(
        string catalogName, string schemaName, string tableName, IEnumerable<string> columns, CloudOptions? cloudOptions = null)
    {
        ArgumentNullException.ThrowIfNull(columns);
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);
        PolarsWrapper.CatalogClusterBy(Handle, catalogName, schemaName, tableName, [.. columns], keys, values);
    }
    /// <summary>Renames a column of a catalog table; see <see cref="Delta.RenameColumn"/>.</summary>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="column">The column, dotted for a nested field.</param>
    /// <param name="newName">The new name.</param>
    /// <param name="cloudOptions">Cloud-specific configurations, laid under the credentials the catalog vends.</param>
    public void RenameCatalogColumn(
        string catalogName, string schemaName, string tableName, string column, string newName, CloudOptions? cloudOptions = null)
    {
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);
        PolarsWrapper.CatalogRenameColumn(Handle, catalogName, schemaName, tableName, column, newName, keys, values);
    }
    /// <summary>Drops columns of a catalog table; see <see cref="Delta.DropColumns"/>.</summary>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="columns">The columns, dotted for nested fields.</param>
    /// <param name="ifExists">Skip columns the table does not have.</param>
    /// <param name="cloudOptions">Cloud-specific configurations, laid under the credentials the catalog vends.</param>
    public void DropCatalogColumns(
        string catalogName, string schemaName, string tableName, IEnumerable<string> columns, bool ifExists = false, CloudOptions? cloudOptions = null)
    {
        ArgumentNullException.ThrowIfNull(columns);
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);
        PolarsWrapper.CatalogDropColumns(Handle, catalogName, schemaName, tableName, [.. columns], ifExists, keys, values);
    }
    /// <summary>Widens the type of a column of a catalog table; see <see cref="Delta.ChangeColumnType"/>.</summary>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="column">The column, dotted for a nested field.</param>
    /// <param name="type">The new type.</param>
    /// <param name="cloudOptions">Cloud-specific configurations, laid under the credentials the catalog vends.</param>
    public void ChangeCatalogColumnType(
        string catalogName, string schemaName, string tableName, string column, DataType type, CloudOptions? cloudOptions = null)
    {
        ArgumentNullException.ThrowIfNull(type);
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);
        PolarsWrapper.CatalogChangeColumnType(Handle, catalogName, schemaName, tableName, column, type.Handle, keys, values);
    }
    /// <summary>
    /// Creates a table in Unity Catalog. A managed table is created as Delta Spark creates one through the catalog's
    /// Delta API: the catalog stages it, version 0 is written at the location the catalog gives, then the catalog
    /// registers it. Such a table is catalog-managed (<c>catalogManaged</c>): it is read and written through the
    /// catalog (<see cref="ScanCatalogTable"/>, <see cref="SinkCatalogTable"/>, …), never by its path. It gets the
    /// protocol and properties the catalog requires and, unless <paramref name="properties"/> sets them otherwise,
    /// those it suggests (Unity Catalog suggests column mapping, row tracking and random file prefixes), as Spark.
    /// An external table is only registered at <paramref name="storageLocation"/>: create its Delta table there first
    /// (<see cref="Delta.CreateTable"/>).
    /// </summary>
    /// <param name="catalogName">The name of the catalog where the table will be created.</param>
    /// <param name="schemaName">The name of the schema (database) within the catalog.</param>
    /// <param name="tableName">The name of the table to be created.</param>
    /// <param name="IntoSchema">The columns of the table.</param>
    /// <param name="tableType">
    /// <see cref="CatalogTableType.Managed"/>: the catalog chooses the location and manages the table.
    /// <see cref="CatalogTableType.External"/>: the data stays at <paramref name="storageLocation"/>.
    /// </param>
    /// <param name="storageLocation">The location of an external table (e.g., s3://my-bucket/path/); null for a managed table.</param>
    /// <param name="partitionBy">Partition columns of a managed table; none when null.</param>
    /// <param name="clusterBy">Liquid clustering columns of a managed table (see <see cref="Delta.CreateTable"/>).</param>
    /// <param name="properties">Table properties of a managed table. A property the catalog requires cannot be set to
    /// another value.</param>
    /// <param name="identityColumns">Identity columns of a managed table.</param>
    /// <param name="cloudOptions">Cloud-specific configurations, laid under the credentials the catalog vends.</param>
    /// <remarks>Fails when the table exists, or when a declaration is not valid.</remarks>
    public void CreateCatalogTable(
        string catalogName,
        string schemaName,
        string tableName,
        IntoSchema IntoSchema,
        CatalogTableType tableType = CatalogTableType.Managed,
        string? storageLocation = null,
        string[]? partitionBy = null,
        IEnumerable<string>? clusterBy = null,
        Dictionary<string, string>? properties = null,
        IEnumerable<DeltaIdentityColumn>? identityColumns = null,
        CloudOptions? cloudOptions = null)
    {
        DeltaIdentityColumn[] identity = identityColumns?.ToArray() ?? [];
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);
        // The schema is borrowed: only one made from a dictionary, names etc. is disposed afterwards.
        try
        {
            PolarsWrapper.CreateCatalogTable(
                Handle,
                catalogName,
                schemaName,
                tableName,
                IntoSchema.Consume().Handle,
                tableType.ToNative(),
                storageLocation,
                partitionBy,
                clusterBy?.ToArray(),
                properties?.Keys.ToArray(),
                properties?.Values.ToArray(),
                [.. identity.Select(c => c.Column)],
                [.. identity.Select(c => c.Start)],
                [.. identity.Select(c => c.Step)],
                [.. identity.Select(c => c.GeneratedAlways)],
                keys,
                values);
        }
        finally
        {
            IntoSchema.DisposeTempSchema();
        }
    }
    /// <summary>
    /// Deletes (drops) a table from the Unity Catalog.
    /// For Managed tables, this typically removes both the metadata and the underlying physical data.
    /// For External tables, only the metadata registration is removed, leaving the physical data intact in your cloud storage.
    /// </summary>
    /// <param name="catalogName">The name of the catalog containing the table.</param>
    /// <param name="schemaName">The name of the schema (database) containing the table.</param>
    /// <param name="tableName">The name of the table to be deleted.</param>
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
    /// Updates the columns of the rows of a Unity Catalog table that match a predicate, as SQL
    /// <c>UPDATE table SET col = expr WHERE predicate</c>; see <see cref="Delta.Update"/>. Catalog-managed tables are
    /// committed through the catalog.
    /// </summary>
    /// <param name="catalogName">The name of the catalog containing the table.</param>
    /// <param name="schemaName">The name of the schema (database) containing the table.</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="predicate">Filter expression to identify rows to update; null updates every row.</param>
    /// <param name="set">Column names mapped to the expressions of their new values.</param>
    /// <param name="cloudOptions">Cloud-specific configurations, laid under the credentials the catalog vends.</param>
    /// <example>
    /// <code>
    /// uc.UpdateCatalogRecords("main", "sales", "orders", Col("Id") == 3, new Dictionary&lt;string, Expr&gt; { ["Price"] = Col("Price") * 2 });
    /// </code>
    /// </example>
    public void UpdateCatalogRecords(
        string catalogName,
        string schemaName,
        string tableName,
        Expr? predicate,
        IReadOnlyDictionary<string, Expr> set,
        CloudOptions? cloudOptions = null)
    {
        ArgumentNullException.ThrowIfNull(set);
        if (set.Count == 0) throw new ArgumentException("An update sets at least one column.", nameof(set));
        var (provider, retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values) =
            CloudOptions.ParseCloudOptions(cloudOptions);
        // The native call consumes the handles it is given.
        PolarsWrapper.UpdateCatalogRecords(
            Handle, catalogName, schemaName, tableName,
            predicate?.CloneHandle(),
            [.. set.Keys],
            [.. set.Values.Select(v => v.CloneHandle())],
            provider.ToNative(), retries, retryTimeoutMs, retryInitBackoffMs, retryMaxBackoffMs, cacheTtl, keys, values);
    }
    /// <summary>
    /// Deletes records from a Unity Catalog table that match the specified predicate.
    /// This operation is ACID-compliant and leverages the underlying Delta Lake protocol.
    /// If Deletion Vectors (DVs) are enabled on the table, it performs a metadata-only delete;
    /// otherwise, it performs a physical rewrite of the affected data files.
    /// </summary>
    /// <param name="catalogName">The name of the catalog containing the table.</param>
    /// <param name="schemaName">The name of the schema (database) containing the table.</param>
    /// <param name="tableName">The name of the table from which records will be removed.</param>
    /// <param name="predicate">A boolean <see cref="Expr"/> defining the condition for rows to be deleted.</param>
    /// <param name="cloudOptions">Cloud-specific configurations including storage credentials and retry policies for concurrent conflicts.</param>
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
    /// Initializes a MERGE (Upsert) operation against a Unity Catalog table using a <see cref="LazyFrame"/> as the data source.
    /// This method provides an ACID-compliant way to perform atomic updates, inserts, or deletes by joining the source data with the target table.
    /// </summary>
    /// <remarks>
    /// This is a builder pattern. No data is modified until <c>.Execute()</c> is called on the resulting <see cref="DeltaMergeBuilder"/>.
    /// The operation performs a join between the source and target based on the <paramref name="mergeKeys"/>.
    /// </remarks>
    /// <param name="catalogName">The Unity Catalog name (e.g., "main").</param>
    /// <param name="schemaName">The schema/database name within the catalog.</param>
    /// <param name="tableName">The target Delta table name registered in the catalog.</param>
    /// <param name="sourceData">A <see cref="LazyFrame"/> containing the data to be merged into the target table.</param>
    /// <param name="mergeKeys">An array of column names used as join keys to match source rows with target rows.</param>
    /// <param name="canEvolve">If true, enables Schema Evolution, allowing the target table's schema to be updated if the source contains new columns.</param>
    /// <param name="cloudOptions">Cloud-specific configurations (S3/Azure/GCS) including dynamic credentials resolved via Unity Catalog.</param>
    /// <returns>A <see cref="DeltaMergeBuilder"/> used to configure "WhenMatched" and "WhenNotMatched" clauses.</returns>
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
    /// Initializes a MERGE (Upsert) operation against a Unity Catalog table using an in-memory <see cref="DataFrame"/>.
    /// The <paramref name="sourceData"/> is automatically converted to a <see cref="LazyFrame"/> to leverage Polars' query optimization.
    /// </summary>
    /// <remarks>
    /// This is a builder pattern. No data is modified until <c>.Execute()</c> is called on the resulting <see cref="DeltaMergeBuilder"/>.
    /// </remarks>
    /// <param name="catalogName">The Unity Catalog name (e.g., "main").</param>
    /// <param name="schemaName">The schema/database name within the catalog.</param>
    /// <param name="tableName">The target Delta table name registered in the catalog.</param>
    /// <param name="sourceData">A <see cref="DataFrame"/> containing the data to be merged.</param>
    /// <param name="mergeKeys">An array of column names used as join keys to match source rows with target rows.</param>
    /// <param name="canEvolve">If true, enables Schema Evolution to handle new columns in the source data.</param>
    /// <param name="cloudOptions">Cloud-specific configurations and credentials.</param>
    /// <returns>A <see cref="DeltaMergeBuilder"/> used to configure "WhenMatched" and "WhenNotMatched" clauses.</returns>
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
    /// Optimizes the storage layout of a Unity Catalog Delta table by compacting small files and optionally applying Z-Order clustering.
    /// This operation significantly improves read performance by reducing file-open overhead and enabling efficient data skipping.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Compaction:</b> Groups small Parquet files into larger ones based on the <paramref name="targetSizeMb"/>.
    /// </para>
    /// <para>
    /// <b>Z-Ordering:</b> If <paramref name="zOrderColumns"/> are provided, the data is rearranged into multidimensional clusters.
    /// This co-locates related information, allowing the engine to skip entire files during queries that filter on these columns.
    /// </para>
    /// <para>
    /// <b>Deletion Vector (DV) Materialization:</b> If the table uses DVs, this operation physically removes any soft-deleted rows
    /// tracked by the vectors. The newly written files will be "clean," and their associated deletion vectors will be removed.
    /// </para>
    /// </remarks>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table to optimize.</param>
    /// <param name="targetSizeMb">The target size for the compacted Parquet files in Megabytes. Default is 128 MB.</param>
    /// <param name="partitionFilters">
    /// Optional filters to limit the optimization to specific partitions (e.g., <c>{"date": "2024-01-01"}</c>).
    /// If null, the entire table is processed.
    /// </param>
    /// <param name="zOrderColumns">An optional collection of column names to use for Z-Order clustering.</param>
    /// <param name="cloudOptions">Cloud-specific configurations, including storage credentials and retry logic for concurrent commits.</param>
    /// <param name="full">On a table with liquid clustering, also recluster the data clustered by earlier clustering columns (see <see cref="Delta.Optimize"/>).</param>
    /// <returns>The total number of files that were successfully compacted/optimized during the operation.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="targetSizeMb"/> is less than or equal to 0.</exception>
    public long OptimizeCatalogTable(
        string catalogName,
        string schemaName,
        string tableName,
        long targetSizeMb = 128,
        Dictionary<string, string>? partitionFilters = null,
        IEnumerable<string>? zOrderColumns = null,
        CloudOptions? cloudOptions = null,
        bool full = false)
    {
        if (targetSizeMb <= 0)
            throw new ArgumentException("Target size must be greater than 0 MB.", nameof(targetSizeMb));

        // Serialize partition filters to JSON string for the Rust backend
        string? filterJson = null;
        if (partitionFilters?.Count > 0)
        {
            filterJson = JsonDictionaryWriter.SerializeDictionary(partitionFilters);
        }

        // Convert Z-Order columns to array (Rust FFI expects string[] or null)
        string[]? zOrderColsArr = zOrderColumns?.ToArray();
        if (zOrderColsArr?.Length == 0)
        {
            zOrderColsArr = null;
        }

        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) =
            CloudOptions.ParseCloudOptions(cloudOptions);

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
            values,
            full
        );

        return (long)result;
    }
    /// <summary>
    /// Physically deletes data files that are no longer referenced by the Delta table and are older than the specified retention threshold.
    /// This operation helps reduce storage costs and ensures compliance by removing historical data that is no longer needed.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Warning:</b> This is a destructive operation. Once files are vacuumed, you can no longer "Time Travel" to versions
    /// that relied on those files.
    /// </para>
    /// <para>
    /// <b>Retention Period:</b> By default, Delta Lake prevents deleting files younger than 168 hours (7 days) to protect
    /// active readers and concurrent transactions.
    /// </para>
    /// </remarks>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table to vacuum.</param>
    /// <param name="retentionHours">
    /// The number of hours to retain historical files. Files modified within this window will not be deleted.
    /// If null, the table's default retention setting is used.
    /// </param>
    /// <param name="enforceRetention">
    /// If true, the operation will fail if <paramref name="retentionHours"/> is less than the table's minimum retention
    /// (usually 168h). Set to false only for emergency cleanup, at the risk of corrupting active readers.
    /// </param>
    /// <param name="dryRun">If true, the method returns the number of files that *would* be deleted without actually removing them.</param>
    /// <param name="vacuumModeFull">
    /// If true, performs a full scan of the object store to find unreferenced files.
    /// If false (Lite mode), uses the Delta log to identify files to be removed.
    /// </param>
    /// <param name="cloudOptions">Cloud-specific configurations and credentials resolved via Unity Catalog.</param>
    /// <returns>The total number of physical files deleted from storage (or identified for deletion if <paramref name="dryRun"/> is true).</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="retentionHours"/> is a negative value.</exception>
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
        if (retentionHours < 0)
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
    /// Restores a Unity Catalog Delta table to a previous state based on a version number or a timestamp.
    /// This operation is atomic and ACID-compliant.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>How it works:</b> Instead of physically deleting newer data, Restore creates a new commit in the transaction log
    /// that reverts the table's metadata and file references to the target state.
    /// </para>
    /// <para>
    /// <b>Safety:</b> This operation will fail if any required data files have been physically removed (e.g., by a <c>Vacuum</c> operation)
    /// unless <paramref name="ignoreMissingFiles"/> is set to true.
    /// </para>
    /// </remarks>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table to restore.</param>
    /// <param name="version">
    /// The specific Delta table version to restore to.
    /// Mutually exclusive with <paramref name="timestamp"/>.
    /// </param>
    /// <param name="timestamp">
    /// The specific <see cref="DateTime"/> to restore the table state to.
    /// Mutually exclusive with <paramref name="version"/>.
    /// </param>
    /// <param name="ignoreMissingFiles">
    /// If true, allows the restore to proceed even if some underlying data files are missing from storage.
    /// Use with caution as this may result in a partial restoration.
    /// </param>
    /// <param name="protocolDowngradeAllowed">
    /// If true, allows the table to be restored to a version that requires a lower protocol reader/writer version
    /// than the current one.
    /// </param>
    /// <param name="cloudOptions">Cloud-specific configurations and credentials resolved via Unity Catalog.</param>
    /// <returns>The new version number of the table after the restore operation is committed.</returns>
    /// <exception cref="ArgumentException">
    /// Thrown when both <paramref name="version"/> and <paramref name="timestamp"/> are provided,
    /// or when neither is provided.
    /// </exception>
    public ulong DeltaRestore(
        string catalogName,
        string schemaName,
        string tableName,
        ulong? version = null,
        DateTime? timestamp = null,
        bool ignoreMissingFiles = false,
        bool protocolDowngradeAllowed = false,
        CloudOptions? cloudOptions = null)
    {
        if (version.HasValue && timestamp.HasValue)
            throw new ArgumentException("Cannot specify both 'version' and 'timestamp'.");

        if (!version.HasValue && !timestamp.HasValue)
            throw new ArgumentException("Must specify either 'version' or 'timestamp'.");

        long targetTs = -1;
        ulong targetVer = 0;

        if (timestamp.HasValue)
        {
            targetTs = new DateTimeOffset(timestamp.Value.ToUniversalTime()).ToUnixTimeMilliseconds();
            if (targetTs < 0)
                throw new ArgumentException("Restore timestamp must be >= 1970-01-01T00:00:00.000Z");
        }
        else if (version.HasValue)
        {
            targetVer = version.Value;
        }

        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

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
    /// Retrieves the commit history (audit trail) for a Unity Catalog Delta table as a <see cref="DataFrame"/>.
    /// This includes metadata about who performed what operation, when it occurred, and what parameters were used.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>Data Processing:</b> The raw JSON history from the Delta log is automatically processed:
    /// <list type="bullet">
    /// <item><description>Timestamps are cast to UTC <see cref="DataType.Datetime"/>.</description></item>
    /// <item><description>Complex fields like <c>operationMetrics</c> and <c>operationParameters</c> are unnested into flat columns.</description></item>
    /// <item><description>Columns are reordered to place essential information (version, timestamp, operation) at the beginning.</description></item>
    /// </list>
    /// </para>
    /// <para>
    /// <b>Note on Versioning:</b> For standard DML operations, the version column might be null in the raw log;
    /// rows are sorted by version or timestamp in descending order (latest first) by default.
    /// </para>
    /// </remarks>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table to audit.</param>
    /// <param name="limit">The maximum number of history commits to retrieve. If 0, retrieves all available history.</param>
    /// <param name="cloudOptions">Cloud-specific configurations and credentials resolved via Unity Catalog.</param>
    /// <returns>A <see cref="DataFrame"/> containing the flattened and formatted history of the table.</returns>
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

        var df = DataFrame.ReadJson(buffer, inferSchemaLen: 2000, jsonFormat: JsonFormat.Json);

        // =========================================================
        // Post-Processing
        // =========================================================

        // i64 -> Datetime
        if (df.ColumnNames.Contains("timestamp"))
        {
            df = df.WithColumns(
                Polars.Col("timestamp")
                    .Cast(DataType.Datetime(TimeUnit.Milliseconds, "UCT"))
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

        return df.Select(selection);
    }

    /// <summary>Drops a table feature of a catalog table; see <see cref="Delta.DropFeature"/>.</summary>
    /// <param name="catalogName">The name of the catalog (e.g., "main").</param>
    /// <param name="schemaName">The name of the schema/database (e.g., "default").</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="featureName">The feature, any case.</param>
    /// <param name="cloudOptions">Cloud-specific configurations, laid under the credentials the catalog vends.</param>
    public void DropFeature(string catalogName, string schemaName, string tableName, string featureName, CloudOptions? cloudOptions = null)
    {
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);
        PolarsWrapper.CatalogDropFeature(Handle, catalogName, schemaName, tableName, featureName, keys, values);
    }
    /// <summary>
    /// Adds a feature to the Unity Catalog Delta table.
    /// </summary>
    /// <param name="catalogName">The catalog name.</param>
    /// <param name="schemaName">The schema name.</param>
    /// <param name="tableName">The table name.</param>
    /// <param name="featureName">The feature name (e.g. "deletionVectors").</param>
    /// <param name="allowProtocolIncrease">
    /// If true, allows the operation to upgrade the table's Protocol Version if the feature requires it.
    /// </param>
    /// <param name="cloudOptions">Optional cloud storage options and retry policies.</param>
    public void AddFeature(
        string catalogName,
        string schemaName,
        string tableName,
        string featureName,
        bool allowProtocolIncrease = true,
        CloudOptions? cloudOptions = null)
    {
        var (_, _, _, _, _, _, keys, values) = CloudOptions.ParseCloudOptions(cloudOptions);

        PolarsWrapper.CatalogAddFeature(
            Handle,
            catalogName,
            schemaName,
            tableName,
            featureName,
            allowProtocolIncrease,
            keys,
            values
        );
    }

    /// <summary>
    /// Reads the Change Data Feed (CDC) stream for a Unity Catalog Delta table between the specified versions.
    /// The table must have 'delta.enableChangeDataFeed' set to 'true'.
    /// </summary>
    /// <param name="catalogName">The name of the catalog.</param>
    /// <param name="schemaName">The name of the schema/database.</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="startVersion">The starting version (inclusive).</param>
    /// <param name="endVersion">The ending version (inclusive).</param>
    /// <param name="cloudOptions">Optional cloud storage credentials and retry policies.</param>
    /// <param name="allowSchemaChanges">Read a range across a change Spark refuses to read across (a column renamed, dropped or changed in type), with the schema the changes are read with; by default such a range is refused, as in Spark, since the feed does not record the change (read the parts before and after it separately).</param>
    /// <returns>A LazyFrame containing the change data stream.</returns>
    public LazyFrame ReadChangeDataFeed(
        string catalogName,
        string schemaName,
        string tableName,
        long startVersion,
        long endVersion,
        CloudOptions? cloudOptions = null,
        bool allowSchemaChanges = false)
    {
        if (startVersion > endVersion)
        {
            throw new ArgumentException("startVersion cannot be greater than endVersion.");
        }

        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) =
            CloudOptions.ParseCloudOptions(cloudOptions);

        var handle = PolarsWrapper.CatalogReadCdc(
            Handle,
            catalogName,
            schemaName,
            tableName,
            (ulong)startVersion,
            (ulong)endVersion,
            provider.ToNative(),
            retries,
            timeout,
            initBackoff,
            maxBackoff,
            cacheTtl,
            keys,
            values,
            allowSchemaChanges
        );

        return new LazyFrame(handle);
    }

    /// <summary>
    /// Reads the Change Data Feed (CDC) stream for a Unity Catalog Delta table between the specified datetimes.
    /// The table must have 'delta.enableChangeDataFeed' set to 'true'.
    /// </summary>
    /// <param name="catalogName">The name of the catalog.</param>
    /// <param name="schemaName">The name of the schema/database.</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="startTimestamp">The starting datetime (inclusive).</param>
    /// <param name="endTimestamp">Optional. The ending datetime (inclusive). Defaults to DateTime.UtcNow if null.</param>
    /// <param name="cloudOptions">Optional cloud storage credentials and retry policies.</param>
    /// <param name="allowSchemaChanges">Read a range across a change Spark refuses to read across (a column renamed, dropped or changed in type), with the schema the changes are read with; by default such a range is refused, as in Spark, since the feed does not record the change (read the parts before and after it separately).</param>
    /// <returns>A LazyFrame containing the change data stream filtered by the exact milliseconds.</returns>
    public LazyFrame ReadChangeDataFeed(
        string catalogName,
        string schemaName,
        string tableName,
        DateTime startTimestamp,
        DateTime? endTimestamp = null,
        CloudOptions? cloudOptions = null,
        bool allowSchemaChanges = false)
    {
        DateTime actualEnd = endTimestamp ?? DateTime.UtcNow;

        if (startTimestamp > actualEnd)
        {
            throw new ArgumentException("startTimestamp cannot be greater than endTimestamp.");
        }

        long startMs = new DateTimeOffset(startTimestamp.ToUniversalTime()).ToUnixTimeMilliseconds();
        long endMs = new DateTimeOffset(actualEnd.ToUniversalTime()).ToUnixTimeMilliseconds();

        var (provider, retries, timeout, initBackoff, maxBackoff, cacheTtl, keys, values) =
            CloudOptions.ParseCloudOptions(cloudOptions);

        var handle = PolarsWrapper.CatalogReadCdcByTime(
            Handle,
            catalogName,
            schemaName,
            tableName,
            startMs,
            endMs,
            provider.ToNative(),
            retries,
            timeout,
            initBackoff,
            maxBackoff,
            cacheTtl,
            keys,
            values,
            allowSchemaChanges
        );

        return new LazyFrame(handle);
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

public readonly partial struct Polars
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
/// Extension methods for <see cref="UnityCatalog"/>
/// </summary>
public static class UnityCatalogExtensions
{
    /// <summary>
    /// Sinks the <see cref="LazyFrame"/> to a Unity Catalog table.
    /// </summary>
    /// <inheritdoc cref="UnityCatalog.SinkCatalogTable(string, string, string, LazyFrame, IntoSelector?, DeltaSaveMode, bool, bool, bool, int, long, ParquetCompression, int, bool, uint, uint, int, bool, SyncOnClose, bool, CloudOptions?, IEnumerable{string}?)"/>
    public static void SinkCatalogTable(
        this LazyFrame lf,
        UnityCatalog catalog,
        string catalogName,
        string schemaName,
        string tableName,
        IntoSelector? partitionBy = null,
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
        CloudOptions? cloudOptions = null,
        IEnumerable<string>? clusterBy = null)
    {
        catalog.SinkCatalogTable(
            catalogName, schemaName, tableName, lf,
            partitionBy, mode, canEvolve, includeKeys, keysPreGrouped,
            maxRowsPerFile, approxBytesPerFile, compression, compressionLevel,
            statistics, rowGroupSize, dataPageSize, compatLevel,
            maintainOrder, syncOnClose, mkdir, cloudOptions, clusterBy
        );
    }

    /// <summary>
    /// Writes the <see cref="DataFrame"/> into a Unity Catalog table by converting it to a <see cref="LazyFrame"/>.
    /// </summary>
    /// <inheritdoc cref="UnityCatalog.SinkCatalogTable(string, string, string, LazyFrame, IntoSelector?, DeltaSaveMode, bool, bool, bool, int, long, ParquetCompression, int, bool, uint, uint, int, bool, SyncOnClose, bool, CloudOptions?, IEnumerable{string}?)"/>
    public static void WriteCatalogTable(
        this DataFrame df,
        UnityCatalog catalog,
        string catalogName,
        string schemaName,
        string tableName,
        IntoSelector? partitionBy = null,
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
        CloudOptions? cloudOptions = null,
        IEnumerable<string>? clusterBy = null)
    {
        catalog.SinkCatalogTable(
            catalogName, schemaName, tableName, df.Lazy(),
            partitionBy, mode, canEvolve, includeKeys, keysPreGrouped,
            maxRowsPerFile, approxBytesPerFile, compression, compressionLevel,
            statistics, rowGroupSize, dataPageSize, compatLevel,
            maintainOrder, syncOnClose, mkdir, cloudOptions, clusterBy
        );
    }
    /// <summary>
    /// Starts building a Merge (Upsert) operation using this <see cref="LazyFrame"/> as the source.
    /// </summary>
    /// <inheritdoc cref="UnityCatalog.MergeCatalogRecords(string, string, string, LazyFrame, string[], bool, CloudOptions?)"/>
    public static DeltaMergeBuilder MergeCatalogRecords(
        this LazyFrame sourceData,
        UnityCatalog catalog,
        string catalogName,
        string schemaName,
        string tableName,
        string[] mergeKeys,
        bool canEvolve = false,
        CloudOptions? cloudOptions = null)
    => catalog.MergeCatalogRecords(
        catalogName,
        schemaName,
        tableName,
        sourceData,
        mergeKeys,
        canEvolve,
        cloudOptions
    );
    /// <summary>
    /// Starts building a Merge (Upsert) operation using this <see cref="DataFrame"/> as the source.
    /// </summary>
    /// <inheritdoc cref="UnityCatalog.MergeCatalogRecords(string, string, string, DataFrame, string[], bool, CloudOptions?)"/>
    public static DeltaMergeBuilder MergeCatalogRecords(
        this DataFrame sourceData,
        UnityCatalog catalog,
        string catalogName,
        string schemaName,
        string tableName,
        string[] mergeKeys,
        bool canEvolve = false,
        CloudOptions? cloudOptions = null)
    => catalog.MergeCatalogRecords(
        catalogName,
        schemaName,
        tableName,
        sourceData,
        mergeKeys,
        canEvolve,
        cloudOptions
    );

    /// <summary>
    /// Updates the rows of a Unity Catalog table that match a row of this frame on the merge keys, with that row's
    /// values (SQL <c>MERGE ... WHEN MATCHED UPDATE</c>); see <see cref="LazyFrame.UpdateDelta(string, string[], Expr?, CloudOptions?)"/>.
    /// Unmatched rows of this frame are ignored: nothing is inserted.
    /// </summary>
    /// <param name="sourceData">The rows with the new values.</param>
    /// <param name="catalog">The catalog connection.</param>
    /// <param name="catalogName">The name of the catalog containing the table.</param>
    /// <param name="schemaName">The name of the schema (database) containing the table.</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="mergeKeys">The columns that identify a row, present in both this frame and the table.</param>
    /// <param name="condition">Optional: only matched rows satisfying it are updated.</param>
    /// <param name="cloudOptions">Cloud-specific configurations, laid under the credentials the catalog vends.</param>
    public static void UpdateCatalogRecords(
        this LazyFrame sourceData, UnityCatalog catalog, string catalogName, string schemaName, string tableName,
        string[] mergeKeys, Expr? condition = null, CloudOptions? cloudOptions = null) =>
        catalog.MergeCatalogRecords(catalogName, schemaName, tableName, sourceData, mergeKeys, false, cloudOptions)
            .WhenMatchedUpdate(condition).Execute();

    /// <summary>
    /// <see cref="UpdateCatalogRecords(LazyFrame, UnityCatalog, string, string, string, string[], Expr?, CloudOptions?)"/>
    /// where a condition over both sides holds: <c>m =&gt; m.Source("Version") &gt; m.Target("Version")</c>.
    /// </summary>
    /// <param name="sourceData">The rows with the new values.</param>
    /// <param name="catalog">The catalog connection.</param>
    /// <param name="catalogName">The name of the catalog containing the table.</param>
    /// <param name="schemaName">The name of the schema (database) containing the table.</param>
    /// <param name="tableName">The name of the table.</param>
    /// <param name="mergeKeys">The columns that identify a row, present in both this frame and the table.</param>
    /// <param name="condition">Builds the condition; <c>m.Source(name)</c> is this frame's column, <c>m.Target(name)</c> the table's.</param>
    /// <param name="cloudOptions">Cloud-specific configurations, laid under the credentials the catalog vends.</param>
    public static void UpdateCatalogRecords(
        this LazyFrame sourceData, UnityCatalog catalog, string catalogName, string schemaName, string tableName,
        string[] mergeKeys, Func<MergeContext, Expr> condition, CloudOptions? cloudOptions = null)
    {
        ArgumentNullException.ThrowIfNull(condition);
        catalog.MergeCatalogRecords(catalogName, schemaName, tableName, sourceData, mergeKeys, false, cloudOptions)
            .WhenMatchedUpdate(condition).Execute();
    }

    /// <inheritdoc cref="UpdateCatalogRecords(LazyFrame, UnityCatalog, string, string, string, string[], Expr?, CloudOptions?)"/>
    public static void UpdateCatalogRecords(
        this DataFrame sourceData, UnityCatalog catalog, string catalogName, string schemaName, string tableName,
        string[] mergeKeys, Expr? condition = null, CloudOptions? cloudOptions = null) =>
        sourceData.Lazy().UpdateCatalogRecords(catalog, catalogName, schemaName, tableName, mergeKeys, condition, cloudOptions);

    /// <inheritdoc cref="UpdateCatalogRecords(LazyFrame, UnityCatalog, string, string, string, string[], Func{MergeContext, Expr}, CloudOptions?)"/>
    public static void UpdateCatalogRecords(
        this DataFrame sourceData, UnityCatalog catalog, string catalogName, string schemaName, string tableName,
        string[] mergeKeys, Func<MergeContext, Expr> condition, CloudOptions? cloudOptions = null) =>
        sourceData.Lazy().UpdateCatalogRecords(catalog, catalogName, schemaName, tableName, mergeKeys, condition, cloudOptions);
}
