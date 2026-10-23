---
uid: Polars.CSharp.Delta
title: Delta
---

# Delta

Namespace: `Polars.CSharp`

Methods for DeltaLake

## Constructors & Static Methods

### AddFeature

Enables a specific feature on the Delta Table. 

WARNING: Enabling features may upgrade the Delta Protocol version (e.g., to Reader v3 / Writer v7). Older readers may not be able to read the table after this operation.

```csharp
public static void AddFeature(
    string path,
    string featureName,
    bool allowProtocolIncrease = true,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | The root URI of the Delta table. |
| `featureName` | `string` | The name of the feature to enable (use `DeltaTableFeatures` constants). |
| `allowProtocolIncrease` | `bool` | If true, allows the operation to upgrade the table's Protocol Version if the feature requires it. If false, and the current protocol is too low, the operation will fail. |
| `cloudOptions` | `CloudOptions?` | Cloud storage credentials. |

---

### Delete

Deletes rows from a Delta Lake table that match a given predicate. 

By default, this operation performs a Copy-on-Write (CoW), meaning any underlying data files containing matching rows are entirely rewritten.

If Deletion Vectors are enabled on the table, this operation automatically shifts to a Merge-on-Read (MoR) approach. Instead of expensive file rewrites, deleted rows are swiftly marked in a separate deletion vector file, which drastically improves deletion performance.

```csharp
public static void Delete(string path, Expr predicate, CloudOptions? cloudOptions = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Path to the Delta table. |
| `predicate` | `Expr` | Filter expression to identify rows to delete. |
| `cloudOptions` | `CloudOptions?` | Cloud storage configuration. |

---

### History

Returns provenance information, including the operation, user, timestamp, etc., for each write to a table.

```csharp
public static DataFrame History(string path, int limit = 0, CloudOptions? cloudOptions = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | The root URI of the Delta table. |
| `limit` | `int` | The number of latest commits to retrieve. Set to 0 (default) to retrieve all history. |
| `cloudOptions` | `CloudOptions?` | Cloud storage credentials. |

**Returns:** A DataFrame containing the commit history.

---

### Optimize

Optimizes the layout of the Delta table by compacting small files (bin-packing) and optionally applying Z-Order clustering. 

This operation significantly improves read performance by reducing the number of files and co-locating related data.

If Deletion Vectors (DV) are enabled on the table, any soft-deleted rows tracked by the vectors will be physically removed (materialized) from the newly compacted Parquet files, effectively clearing the deletion vectors for the optimized partitions.

```csharp
public static long Optimize(
    string path,
    long targetSizeMb = 128,
    Dictionary<string, string>? partitionFilters = null,
    IEnumerable<string>? zOrderColumns = null,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | The root URI of the Delta table. |
| `targetSizeMb` | `long` | The target file size in Megabytes for the compacted files (default: 128 MB). |
| `partitionFilters` | `Dictionary<string, string>?` | Optional dictionary of partition key-value pairs to restrict optimization to specific partitions. <br>Example: `new Dictionary<string, string> { { "date", "2024-01-01" } }` |
| `zOrderColumns` | `IEnumerable<string>?` | Optional list of column names to apply Z-Order clustering. <br>Z-Ordering co-locates data based on these columns, significantly speeding up queries that filter on them. |
| `cloudOptions` | `CloudOptions?` | Cloud storage credentials and configuration. |

**Returns:** The number of new data files created during the optimization process.

---

### ReadChangeDataFeed

Reads the Change Data Feed (CDC) stream for a Delta table between the specified versions. The table must have 'delta.enableChangeDataFeed' set to 'true'.

Reads the Change Data Feed (CDC) stream for a Delta table between the specified datetimes. The table must have 'delta.enableChangeDataFeed' set to 'true'.

```csharp
public static LazyFrame ReadChangeDataFeed(
    string path,
    long startVersion,
    long endVersion,
    CloudOptions? cloudOptions = null
)

public static LazyFrame ReadChangeDataFeed(
    string path,
    DateTime startTimestamp,
    DateTime? endTimestamp = null,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | The URI or local path of the Delta table. |
| `startVersion` | `long` | The starting version (inclusive). |
| `endVersion` | `long` | The ending version (inclusive). |
| `cloudOptions` | `CloudOptions?` | Optional cloud storage credentials and retry policies. |
| `startTimestamp` | `DateTime` | The starting datetime (inclusive). |
| `endTimestamp` | `DateTime?` | Optional. The ending datetime (inclusive). Defaults to DateTime.UtcNow if null. |

**Returns:** A LazyFrame containing the change data stream. / A LazyFrame containing the change data stream filtered by the exact milliseconds.

---

### Restore

Restores the Delta table to a previous state defined by a specific version or timestamp. 

Note: This operation creates a new commit (version) that reflects the state of the table at the target point.

```csharp
public static ulong Restore(
    string path,
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
| `path` | `string` | The root URI of the Delta table. |
| `version` | `ulong?` | The specific version number to restore to. Set to null if restoring by timestamp. |
| `timestamp` | `DateTime?` | The timestamp to restore to. Set to null if restoring by version. |
| `ignoreMissingFiles` | `bool` | If true, the restore operation will ignore missing files (e.g., those deleted by Vacuum). If false, it will fail if any required files are missing. |
| `protocolDowngradeAllowed` | `bool` | Whether to allow downgrading the Delta Protocol version during restore (rarely needed). |
| `cloudOptions` | `CloudOptions?` | Cloud storage credentials. |

**Returns:** The new version number of the table after the restore operation.

---

### SetTableProperties

Sets or updates properties on the Delta Table.

```csharp
public static void SetTableProperties(
    string path,
    Dictionary<string, string> properties,
    bool raiseIfNotExists = false,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | The root URI of the Delta table. |
| `properties` | `Dictionary<string, string>` | A dictionary of key-value pairs to set. |
| `raiseIfNotExists` | `bool` | If true, the operation fails if you try to update a property that doesn't strictly exist (rarely used, default false). |
| `cloudOptions` | `CloudOptions?` | Cloud storage credentials. |

---

### Source

Represents a column from the incoming Source LazyFrame. (Internally maps to "{name}_src_tmp")

```csharp
public static Expr Source(string columnName)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columnName` | `string` | - |

---

### Target

Represents a column from the existing Target Delta Table. (Alias for standard Col(), adds semantic clarity)

```csharp
public static Expr Target(string columnName)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columnName` | `string` | - |

---

### Vacuum

Recursively delete files and directories in the table that are no longer needed by the table for maintaining the transaction history.

```csharp
public static long Vacuum(
    string path,
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
| `path` | `string` | The root URI of the Delta table (e.g., "s3://bucket/table"). |
| `retentionHours` | `int?` | The retention threshold in hours. Files removed before this threshold will be deleted. Set to -1 to use the default retention period configured in the table (usually 168 hours / 7 days). |
| `enforceRetention` | `bool` | If true, prevents the vacuum command from deleting files that have not yet reached the retention threshold. Setting this to false allows forcing deletion of recent files (use with caution). |
| `dryRun` | `bool` | If true, simply returns the number of files that would be deleted, without actually deleting them. |
| `vacuumModeFull` | `bool` | Specifies the vacuum mode: <br>false (Lite): Only removes files that are explicitly referenced as removed in the transaction log. Faster.<br><br>true (Full): Scans the entire storage directory to find and remove any files not referenced in the transaction log (including orphan files from failed writes). Slower but more thorough. |
| `cloudOptions` | `CloudOptions?` | Cloud storage credentials and configuration (e.g. AWS Keys, Azure Secrets). |

**Returns:** The number of files deleted (or selected for deletion in dry-run mode).

---

