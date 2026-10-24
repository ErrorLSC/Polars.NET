---
uid: Polars.CSharp.UnityCatalog
title: "UnityCatalog"
---

# UnityCatalog

Namespace: `Polars.CSharp`

Databricks Unity Catalog Client

## Constructors & Static Methods

### UnityCatalog

Databricks Unity Catalog Client

```csharp
public UnityCatalog(string workspaceUrl, string bearerToken)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `workspaceUrl` | `string` | - |
| `bearerToken` | `string` | - |

> [!NOTE]
> Init Unity Catalog connection

---

## Instance Methods

### AddFeature

Adds a feature to the Unity Catalog Delta table.

```csharp
public void AddFeature(
    string catalogName,
    string schemaName,
    string tableName,
    string featureName,
    bool allowProtocolIncrease = true,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalogName` | `string` | The catalog name. |
| `schemaName` | `string` | The schema name. |
| `tableName` | `string` | The table name. |
| `featureName` | `string` | The feature name (e.g. "deletionVectors"). |
| `allowProtocolIncrease` | `bool` | If true, allows the operation to upgrade the table's Protocol Version if the feature requires it. |
| `cloudOptions` | `CloudOptions?` | Optional cloud storage options and retry policies. |

---

### CreateCatalogTable

Creates a new table registration within the Unity Catalog.

```csharp
public void CreateCatalogTable(
    string catalogName,
    string schemaName,
    string tableName,
    IntoSchema IntoSchema,
    CatalogTableType tableType = CatalogTableType.Managed,
    string? storageLocation = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalogName` | `string` | The name of the catalog where the table will be created. |
| `schemaName` | `string` | The name of the schema (database) within the catalog. |
| `tableName` | `string` | The name of the table to be created. |
| `IntoSchema` | `IntoSchema` | The structural definition of the table columns and data types via `IntoSchema`. |
| `tableType` | `CatalogTableType` | The management type of the table. `CatalogTableType.Managed`: Unity Catalog manages both metadata and physical data. `CatalogTableType.External`: Unity Catalog manages metadata, but data resides at a specific `storageLocation`. |
| `storageLocation` | `string?` | The physical URI for the table data (e.g., s3://my-bucket/path/). Required if `tableType` is set to External; typically null for Managed tables. |

---

### DeleteCatalogRecords

Deletes records from a Unity Catalog table that match the specified predicate. This operation is ACID-compliant and leverages the underlying Delta Lake protocol. If Deletion Vectors (DVs) are enabled on the table, it performs a metadata-only delete; otherwise, it performs a physical rewrite of the affected data files.

```csharp
public void DeleteCatalogRecords(
    string catalogName,
    string schemaName,
    string tableName,
    Expr predicate,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalogName` | `string` | The name of the catalog containing the table. |
| `schemaName` | `string` | The name of the schema (database) containing the table. |
| `tableName` | `string` | The name of the table from which records will be removed. |
| `predicate` | `Expr` | A boolean `Expr` defining the condition for rows to be deleted. |
| `cloudOptions` | `CloudOptions?` | Cloud-specific configurations including storage credentials and retry policies for concurrent conflicts. |

---

### DeleteCatalogTable

Deletes (drops) a table from the Unity Catalog. For Managed tables, this typically removes both the metadata and the underlying physical data. For External tables, only the metadata registration is removed, leaving the physical data intact in your cloud storage.

```csharp
public void DeleteCatalogTable(string catalogName, string schemaName, string tableName)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalogName` | `string` | The name of the catalog containing the table. |
| `schemaName` | `string` | The name of the schema (database) containing the table. |
| `tableName` | `string` | The name of the table to be deleted. |

---

### DeltaHistory

Retrieves the commit history (audit trail) for a Unity Catalog Delta table as a `DataFrame`. This includes metadata about who performed what operation, when it occurred, and what parameters were used.

```csharp
public DataFrame DeltaHistory(
    string catalogName,
    string schemaName,
    string tableName,
    int limit = 0,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalogName` | `string` | The name of the catalog (e.g., "main"). |
| `schemaName` | `string` | The name of the schema/database (e.g., "default"). |
| `tableName` | `string` | The name of the table to audit. |
| `limit` | `int` | The maximum number of history commits to retrieve. If 0, retrieves all available history. |
| `cloudOptions` | `CloudOptions?` | Cloud-specific configurations and credentials resolved via Unity Catalog. |

**Returns:** A `DataFrame` containing the flattened and formatted history of the table.

> [!NOTE]
> Data Processing: The raw JSON history from the Delta log is automatically processed: 
> 
> - Timestamps are cast to UTC `DataType.Datetime`.
> - Complex fields like `operationMetrics` and `operationParameters` are unnested into flat columns.
> - Columns are reordered to place essential information (version, timestamp, operation) at the beginning.
> 
> Note on Versioning: For standard DML operations, the version column might be null in the raw log; rows are sorted by version or timestamp in descending order (latest first) by default.

---

### DeltaRestore

Restores a Unity Catalog Delta table to a previous state based on a version number or a timestamp. This operation is atomic and ACID-compliant.

```csharp
public ulong DeltaRestore(
    string catalogName,
    string schemaName,
    string tableName,
    ulong? version = null,
    DateTime? timestamp = null,
    bool ignoreMissingFiles = false,
    bool protocolDowngradeAllowed = false,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalogName` | `string` | The name of the catalog (e.g., "main"). |
| `schemaName` | `string` | The name of the schema/database (e.g., "default"). |
| `tableName` | `string` | The name of the table to restore. |
| `version` | `ulong?` | The specific Delta table version to restore to. Mutually exclusive with `timestamp`. |
| `timestamp` | `DateTime?` | The specific `System.DateTime` to restore the table state to. Mutually exclusive with `version`. |
| `ignoreMissingFiles` | `bool` | If true, allows the restore to proceed even if some underlying data files are missing from storage. Use with caution as this may result in a partial restoration. |
| `protocolDowngradeAllowed` | `bool` | If true, allows the table to be restored to a version that requires a lower protocol reader/writer version than the current one. |
| `cloudOptions` | `CloudOptions?` | Cloud-specific configurations and credentials resolved via Unity Catalog. |

**Returns:** The new version number of the table after the restore operation is committed.

> [!NOTE]
> How it works: Instead of physically deleting newer data, Restore creates a new commit in the transaction log that reverts the table's metadata and file references to the target state.
> 
> Safety: This operation will fail if any required data files have been physically removed (e.g., by a `Vacuum` operation) unless `ignoreMissingFiles` is set to true.

---

### DeltaVacuum

Physically deletes data files that are no longer referenced by the Delta table and are older than the specified retention threshold. This operation helps reduce storage costs and ensures compliance by removing historical data that is no longer needed.

```csharp
public long DeltaVacuum(
    string catalogName,
    string schemaName,
    string tableName,
    int? retentionHours = null,
    bool enforceRetention = true,
    bool dryRun = false,
    bool vacuumModeFull = false,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalogName` | `string` | The name of the catalog (e.g., "main"). |
| `schemaName` | `string` | The name of the schema/database (e.g., "default"). |
| `tableName` | `string` | The name of the table to vacuum. |
| `retentionHours` | `int?` | The number of hours to retain historical files. Files modified within this window will not be deleted. If null, the table's default retention setting is used. |
| `enforceRetention` | `bool` | If true, the operation will fail if `retentionHours` is less than the table's minimum retention (usually 168h). Set to false only for emergency cleanup, at the risk of corrupting active readers. |
| `dryRun` | `bool` | If true, the method returns the number of files that *would* be deleted without actually removing them. |
| `vacuumModeFull` | `bool` | If true, performs a full scan of the object store to find unreferenced files. If false (Lite mode), uses the Delta log to identify files to be removed. |
| `cloudOptions` | `CloudOptions?` | Cloud-specific configurations and credentials resolved via Unity Catalog. |

**Returns:** The total number of physical files deleted from storage (or identified for deletion if `dryRun` is true).

> [!NOTE]
> Warning: This is a destructive operation. Once files are vacuumed, you can no longer "Time Travel" to versions that relied on those files.
> 
> Retention Period: By default, Delta Lake prevents deleting files younger than 168 hours (7 days) to protect active readers and concurrent transactions.

---

### MergeCatalogRecords

Initializes a MERGE (Upsert) operation against a Unity Catalog table using a `LazyFrame` as the data source. This method provides an ACID-compliant way to perform atomic updates, inserts, or deletes by joining the source data with the target table.

Initializes a MERGE (Upsert) operation against a Unity Catalog table using an in-memory `DataFrame`. The `sourceData` is automatically converted to a `LazyFrame` to leverage Polars' query optimization.

```csharp
public DeltaMergeBuilder MergeCatalogRecords(
    string catalogName,
    string schemaName,
    string tableName,
    LazyFrame sourceData,
    string[] mergeKeys,
    bool canEvolve = false,
    CloudOptions? cloudOptions = null
)

public DeltaMergeBuilder MergeCatalogRecords(
    string catalogName,
    string schemaName,
    string tableName,
    DataFrame sourceData,
    string[] mergeKeys,
    bool canEvolve = false,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalogName` | `string` | The Unity Catalog name (e.g., "main"). |
| `schemaName` | `string` | The schema/database name within the catalog. |
| `tableName` | `string` | The target Delta table name registered in the catalog. |
| `sourceData` | `LazyFrame / DataFrame` | A `LazyFrame` containing the data to be merged into the target table. |
| `mergeKeys` | `string[]` | An array of column names used as join keys to match source rows with target rows. |
| `canEvolve` | `bool` | If true, enables Schema Evolution, allowing the target table's schema to be updated if the source contains new columns. |
| `cloudOptions` | `CloudOptions?` | Cloud-specific configurations (S3/Azure/GCS) including dynamic credentials resolved via Unity Catalog. |

**Returns:** A `DeltaMergeBuilder` used to configure "WhenMatched" and "WhenNotMatched" clauses.

> [!NOTE]
> This is a builder pattern. No data is modified until `.Execute()` is called on the resulting `DeltaMergeBuilder`. The operation performs a join between the source and target based on the `mergeKeys`.
> This is a builder pattern. No data is modified until `.Execute()` is called on the resulting `DeltaMergeBuilder`.

---

### OptimizeCatalogTable

Optimizes the storage layout of a Unity Catalog Delta table by compacting small files and optionally applying Z-Order clustering. This operation significantly improves read performance by reducing file-open overhead and enabling efficient data skipping.

```csharp
public long OptimizeCatalogTable(
    string catalogName,
    string schemaName,
    string tableName,
    long targetSizeMb = 128,
    Dictionary<string, string>? partitionFilters = null,
    IEnumerable<string>? zOrderColumns = null,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalogName` | `string` | The name of the catalog (e.g., "main"). |
| `schemaName` | `string` | The name of the schema/database (e.g., "default"). |
| `tableName` | `string` | The name of the table to optimize. |
| `targetSizeMb` | `long` | The target size for the compacted Parquet files in Megabytes. Default is 128 MB. |
| `partitionFilters` | `Dictionary<string, string>?` | Optional filters to limit the optimization to specific partitions (e.g., `{"date": "2024-01-01"}`). If null, the entire table is processed. |
| `zOrderColumns` | `IEnumerable<string>?` | An optional collection of column names to use for Z-Order clustering. |
| `cloudOptions` | `CloudOptions?` | Cloud-specific configurations, including storage credentials and retry logic for concurrent commits. |

**Returns:** The total number of files that were successfully compacted/optimized during the operation.

> [!NOTE]
> Compaction: Groups small Parquet files into larger ones based on the `targetSizeMb`.
> 
> Z-Ordering: If `zOrderColumns` are provided, the data is rearranged into multidimensional clusters. This co-locates related information, allowing the engine to skip entire files during queries that filter on these columns.
> 
> Deletion Vector (DV) Materialization: If the table uses DVs, this operation physically removes any soft-deleted rows tracked by the vectors. The newly written files will be "clean," and their associated deletion vectors will be removed.

---

### ReadChangeDataFeed

Reads the Change Data Feed (CDC) stream for a Unity Catalog Delta table between the specified versions. The table must have 'delta.enableChangeDataFeed' set to 'true'.

Reads the Change Data Feed (CDC) stream for a Unity Catalog Delta table between the specified datetimes. The table must have 'delta.enableChangeDataFeed' set to 'true'.

```csharp
public LazyFrame ReadChangeDataFeed(
    string catalogName,
    string schemaName,
    string tableName,
    long startVersion,
    long endVersion,
    CloudOptions? cloudOptions = null
)

public LazyFrame ReadChangeDataFeed(
    string catalogName,
    string schemaName,
    string tableName,
    DateTime startTimestamp,
    DateTime? endTimestamp = null,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalogName` | `string` | The name of the catalog. |
| `schemaName` | `string` | The name of the schema/database. |
| `tableName` | `string` | The name of the table. |
| `startVersion` | `long` | The starting version (inclusive). |
| `endVersion` | `long` | The ending version (inclusive). |
| `cloudOptions` | `CloudOptions?` | Optional cloud storage credentials and retry policies. |
| `startTimestamp` | `DateTime` | The starting datetime (inclusive). |
| `endTimestamp` | `DateTime?` | Optional. The ending datetime (inclusive). Defaults to DateTime.UtcNow if null. |

**Returns:** A LazyFrame containing the change data stream. / A LazyFrame containing the change data stream filtered by the exact milliseconds.

---

### ScanCatalogTable

Scans a table managed by Unity Catalog into a `LazyFrame`. This method performs a two-step resolution: 1. It fetches the physical storage location and dynamic credentials from Unity Catalog. 2. It initializes a Polars scan on the underlying Delta files using the resolved credentials.

```csharp
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
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalogName` | `string` | The name of the catalog (e.g., "main"). |
| `schemaName` | `string` | The name of the schema/database (e.g., "default"). |
| `tableName` | `string` | The name of the table to scan. |
| `version` | `ulong?` | The version of the table to read (e.g., 0, 1). Mutually exclusive with `datetime`. |
| `datetime` | `string?` | The timestamp to read (ISO-8601 string, e.g., "2026-02-09T12:00:00Z"). Mutually exclusive with `version`. |
| `nRows` | `ulong?` | Limit number of rows to read (optimization). |
| `parallel` | `ParallelStrategy` | Parallel strategy. |
| `lowMemory` | `bool` | Reduce memory usage at the expense of performance. |
| `useStatistics` | `bool` | Use parquet statistics to optimize the query plan. |
| `glob` | `bool` | Expand glob patterns (default: true). |
| `rechunk` | `bool` | Rechunk the memory to contiguous chunks when reading. (default: false) |
| `cache` | `bool` | Cache the result after reading. (default: true) |
| `rowIndexName` | `string?` | If provided, adds a column with the row number. |
| `rowIndexOffset` | `uint` | Offset for the row index. |
| `includePathColumn` | `string?` | If provided, adds a column with the source file path. |
| `schema` | `IntoSchema?` | Manually specify the schema of the file(s). Useful if the file footer is missing or to avoid I/O overhead of reading the schema. |
| `hivePartitioning` | `bool` | Enable Hive partitioning inference (default: false). |
| `hivePartitionSchema` | `IntoSchema?` | Manually specify the schema for Hive partitioning columns. Use this to ensure specific types for partition keys (e.g. string instead of int). |
| `tryParseHiveDates` | `bool` | Whether to try parsing dates in Hive partitioning paths (default: false). |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage authentication (AWS S3, Azure, GCP, etc). |

**Returns:** A `LazyFrame` for lazy execution of the catalog table scan.

---

### SinkCatalogTable

Sink the LazyFrame to a Unity Catalog managed Delta Lake table with partition discovery.

```csharp
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
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalogName` | `string` | The name of the catalog (e.g., "main"). |
| `schemaName` | `string` | The name of the schema/database (e.g., "default"). |
| `tableName` | `string` | The name of the table to sink. |
| `lf` | `LazyFrame` | The LazyFrame ready to be sinked. |
| `partitionBy` | `IntoSelector?` | The selector(s) to partition the data by. Directories will be created in the format "col=value". |
| `mode` | `DeltaSaveMode` | Save mode (Append, Overwrite, ErrorIfExists, Ignore). Default is Append. |
| `canEvolve` | `bool` | Define whether schema evolution is allowed, default: false |
| `includeKeys` | `bool` | Whether to include the partition keys in the Parquet files themselves. Default is true (recommended for Delta Lake compatibility). |
| `keysPreGrouped` | `bool` | Assert that the keys are already pre-grouped. This can speed up the operation if true. |
| `maxRowsPerFile` | `int` | Maximum number of rows per file. 0 means no limit. |
| `approxBytesPerFile` | `long` | Approximate size in bytes per file. 0 means no limit. |
| `compression` | `ParquetCompression` | Compression codec to use (Snappy, Zstd, etc.). |
| `compressionLevel` | `int` | Compression level (depends on the codec). |
| `statistics` | `bool` | Write statistics to the Parquet file. Delta Lake uses these stats for data skipping, so 'true' is highly recommended. |
| `rowGroupSize` | `uint` | Target row group size (in rows). |
| `dataPageSize` | `uint` | Target data page size (in bytes). |
| `compatLevel` | `int` | IPC format compatibility. |
| `maintainOrder` | `bool` | Maintain the order of the data. |
| `syncOnClose` | `SyncOnClose` | Whether to sync the file to disk on close. |
| `mkdir` | `bool` | Create parent directories if they don't exist. |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage authentication and configuration. |

---

