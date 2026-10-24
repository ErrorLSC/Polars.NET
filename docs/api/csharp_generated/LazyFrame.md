---
uid: Polars.CSharp.LazyFrame
title: "LazyFrame"
---

# LazyFrame

Namespace: `Polars.CSharp`

Represents a lazily evaluated DataFrame. Until the query is executed, operations are just recorded in a query plan. Once executed, the data is materialized in memory.

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `ColumnNames` | `string[]` | Return LazyFrame Columns' Name |
| `Columns` | `string[]` | Return LazyFrame Columns' Name |
| `DataTypes` | `IReadOnlyList<DataType>` | Return LazyFrame Columns' DataType |
| `Schema` | `PolarsSchema` | Gets the Schema of the LazyFrame. The returned PolarsSchema object is IDisposable. Usage in 'using' block is recommended if accessed frequently. |

## Constructors & Static Methods

### Concat

Lazily concatenate multiple LazyFrames. 

This adds a concat node to the query plan. For vertical concatenation, schemas must align (or be capable of supertype unification).

```csharp
public static LazyFrame Concat(
    IEnumerable<LazyFrame> lfs,
    ConcatType how = ConcatType.Vertical,
    bool rechunk = false,
    bool parallel = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `lfs` | `IEnumerable<LazyFrame>` | - |
| `how` | `ConcatType` | - |
| `rechunk` | `bool` | - |
| `parallel` | `bool` | - |

**Example**

```csharp
LazyFrame.Concat(new[] { lf1, lf2 }, ConcatType.Vertical)
         .Collect();
```

---

### ScanCsv

Lazily scans a CSV file into a LazyFrame. 

This allows for query optimization (predicate pushdown, projection pushdown) and streaming processing of datasets larger than memory.

Lazily scans a CSV from an in-memory byte array. 

Useful for processing data from Web APIs, S3, or embedded resources without writing to disk.

```csharp
public static LazyFrame ScanCsv(
    string path,
    IntoSchema? schema = null,
    IntoSchema? dtypeOverride = null,
    bool hasHeader = true,
    char separator = ',',
    char? quoteChar = '"',
    char eolChar = '\n',
    bool ignoreErrors = false,
    bool tryParseDates = true,
    bool lowMemory = false,
    bool cache = true,
    bool glob = true,
    bool rechunk = false,
    bool raiseIfEmpty = true,
    ulong skipRows = 0,
    ulong skipRowsAfterHeader = 0,
    ulong skipLines = 0,
    ulong? nRows = null,
    ulong? inferSchemaLength = null,
    ulong? inferSchemaFiles = 18446744073709551615,
    ulong? nThreads = null,
    ulong? chunkSize = null,
    string? rowIndexName = null,
    ulong rowIndexOffset = 0,
    string? includeFilePaths = null,
    CsvEncoding encoding = CsvEncoding.UTF8,
    string[]? nullValues = null,
    bool missingIsNull = true,
    string? commentPrefix = null,
    bool decimalComma = false,
    bool truncateRaggedLines = false,
    CloudOptions? cloudOptions = null
)

public static LazyFrame ScanCsv(
    byte[] buffer,
    IntoSchema? schema = null,
    IntoSchema? dtypeOverride = null,
    bool hasHeader = true,
    char separator = ',',
    char? quoteChar = '"',
    char eolChar = '\n',
    bool ignoreErrors = false,
    bool tryParseDates = true,
    bool lowMemory = false,
    bool cache = true,
    bool glob = true,
    bool rechunk = false,
    bool raiseIfEmpty = true,
    ulong skipRows = 0,
    ulong skipRowsAfterHeader = 0,
    ulong skipLines = 0,
    ulong? nRows = null,
    ulong? inferSchemaLength = null,
    ulong? inferSchemaFiles = 18446744073709551615,
    ulong? nThreads = null,
    ulong? chunkSize = null,
    string? rowIndexName = null,
    ulong rowIndexOffset = 0,
    string? includeFilePaths = null,
    CsvEncoding encoding = CsvEncoding.UTF8,
    string[]? nullValues = null,
    bool missingIsNull = true,
    string? commentPrefix = null,
    bool decimalComma = false,
    bool truncateRaggedLines = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Path to the CSV file. |
| `schema` | `IntoSchema?` | Optional PolarsSchema to specify all column names and types. |
| `dtypeOverride` | `IntoSchema?` | Optional PolarsSchema to specify column types or overwrite inference. |
| `hasHeader` | `bool` | Whether the CSV file has a header row. Defaults to true. |
| `separator` | `char` | The character used as a field separator. Defaults to ','. |
| `quoteChar` | `char?` | The character used for quoting fields. Defaults to '"'. Set to '\0' to disable quoting. |
| `eolChar` | `char` | The character used as End-Of-Line. Defaults to '\n'. |
| `ignoreErrors` | `bool` | Whether to ignore parsing errors (skip bad rows). Defaults to false. |
| `tryParseDates` | `bool` | Whether to automatically try parsing dates/datetimes. Defaults to true. |
| `lowMemory` | `bool` | Reduce memory usage at the cost of performance. Defaults to false. |
| `cache` | `bool` | Cache the result after reading. Defaults to true. |
| `glob` | `bool` | Expand path given via globbing rules. Defaults to true. |
| `rechunk` | `bool` | Rechunk the memory to contiguous chunks after reading. Defaults to false. |
| `raiseIfEmpty` | `bool` | Raise an error if CSV is empty (otherwise return an empty frame). Defaults to true. |
| `skipRows` | `ulong` | Number of rows to skip at the start of the file. Defaults to 0. |
| `skipRowsAfterHeader` | `ulong` | Skip this number of rows after the header location. Defaults to 0. |
| `skipLines` | `ulong` | Skip the first n lines during parsing without respecting CSV escaping. Defaults to 0. |
| `nRows` | `ulong?` | Stop reading after n rows. If null, reads the entire file. |
| `inferSchemaLength` | `ulong?` | Number of rows to scan for schema inference. Default is null, means scan all file |
| `inferSchemaFiles` | `ulong?` | How many files to use when inferring schema. |
| `nThreads` | `ulong?` | Sets the number of threads used for CSV parsing. Default is null for auto setting. |
| `chunkSize` | `ulong?` | Set the chunk size for each thread. Default is null for auto setting. |
| `rowIndexName` | `string?` | If provided, adds a column with the row index using this name. |
| `rowIndexOffset` | `ulong` | Offset to start the row index from. Defaults to 0. |
| `includeFilePaths` | `string?` | If provided, adds a column with the file path using this name. |
| `encoding` | `CsvEncoding` | File encoding (UTF8 or LossyUTF8). Defaults to UTF8. |
| `nullValues` | `string[]?` | List of strings to consider as null values. E.g., ["NA", "null"]. |
| `missingIsNull` | `bool` | Treat missing fields (empty strings between delimiters) as null. Defaults to true. |
| `commentPrefix` | `string?` | Lines starting with this prefix will be ignored. E.g., "#". |
| `decimalComma` | `bool` | Use comma ',' as the decimal separator (European style). Defaults to false. |
| `truncateRaggedLines` | `bool` | Truncate lines that are longer than the schema. Defaults to false. |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage authentication and configuration. |
| `buffer` | `byte[]` | The byte array containing CSV data. |

**Returns:** A new LazyFrame.

---

### ScanDatabase

Scan Database to LazyFrame

Create a LazyFrame from a database query using a reader factory. 

Recommended: This is the preferred method for interacting with databases in a Lazy context.

It accepts a factory function that creates a NEW `System.Data.IDataReader` on demand. This allows Polars to: 

- Inspect the schema upfront (using a probe reader).
- Re-execute the query if the execution plan requires multiple passes.
- Allow you to call `LazyFrame.Collect` multiple times on the same LazyFrame.

```csharp
public static LazyFrame ScanDatabase(IDataReader reader, int batchSize = 50000)
public static LazyFrame ScanDatabase(Func<IDataReader> readerFactory, int batchSize = 50000)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `reader` | `IDataReader` | - |
| `batchSize` | `int` | The size of the Arrow record batch (rows). Larger values reduce overhead. |
| `readerFactory` | `Func<IDataReader>` | A function that returns a new, open `System.Data.IDataReader` instance each time it is called. |

**Returns:** A new LazyFrame linked to the database stream.

**Example**

```csharp
// Define a factory that returns a new Reader for the query
Func<IDataReader> factory = () =>
{
    var cmd = connection.CreateCommand();
    cmd.CommandText = """
        SELECT name, score
        FROM User
        WHERE score > $min_score OR score IS NULL
    """;
    cmd.Parameters.AddWithValue("$min_score", 60.0);
    return cmd.ExecuteReader();
};

// Scan and apply transformations lazily
var lf = LazyFrame.ScanDatabase(factory);

var result = lf
    .WithColumns(
        Col("score").FillNull(0.0).Alias("clean_score")
    )
    .Collect();

result.Show();
/* Output:
shape: (3, 3)
┌─────────┬───────┬─────────────┐
│ name    ┆ score ┆ clean_score │
│ ---     ┆ ---   ┆ ---         │
│ str     ┆ f64   ┆ f64         │
╞═════════╪═══════╪═════════════╡
│ Alice   ┆ 99.5  ┆ 99.5        │
│ Bob     ┆ 85.0  ┆ 85.0        │
│ Charlie ┆ null  ┆ 0.0         │
└─────────┴───────┴─────────────┘
*/
```

---

### ScanDatabaseBuffered

[Buffered] Create a LazyFrame from an existing DataReader. 

Note: This consumes the reader IMMEDIATELY and writes to a temp file.

Returns a `LazyFrame.ScopedLazyFrame` which must be disposed to delete the temp file.

```csharp
public static ScopedLazyFrame ScanDatabaseBuffered(IDataReader reader, int batchSize = 50000)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `reader` | `IDataReader` | - |
| `batchSize` | `int` | - |

---

### ScanDelta

Create a LazyFrame by scanning a Delta Lake table.

```csharp
public static LazyFrame ScanDelta(
    string path,
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
| `path` | `string` | Path to the Delta Lake table (folder containing _delta_log). |
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

**Returns:** A new LazyFrame.

---

### ScanEnumerable

Scan Enumberable As LazyFrame

```csharp
public static LazyFrame ScanEnumerable<T>(
    IEnumerable<T> data,
    Schema? schema = null,
    int batchSize = 100000,
    bool useBuffered = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `data` | `IEnumerable<T>` | - |
| `schema` | `Schema?` | - |
| `batchSize` | `int` | - |
| `useBuffered` | `bool` | - |

---

### ScanIpc

Lazily read an Arrow IPC (Feather v2) file, multiple files via glob patterns, or cloud storage.

Lazily read Arrow IPC (Feather v2) from in-memory bytes.

Lazily read Arrow IPC (Feather v2) from a Stream.

```csharp
public static LazyFrame ScanIpc(
    string path,
    IntoSchema? schema = null,
    ulong? nRows = null,
    bool rechunk = false,
    bool cache = true,
    bool glob = true,
    string? rowIndexName = null,
    uint rowIndexOffset = 0,
    string? includePathColumn = null,
    bool hivePartitioning = false,
    IntoSchema? hivePartitionSchema = null,
    bool tryParseHiveDates = true,
    CloudOptions? cloudOptions = null
)

public static LazyFrame ScanIpc(
    byte[] buffer,
    IntoSchema? schema = null,
    ulong? nRows = null,
    bool rechunk = false,
    bool cache = true,
    string? rowIndexName = null,
    uint rowIndexOffset = 0,
    string? includePathColumn = null,
    bool hivePartitioning = false,
    IntoSchema? hivePartitionSchema = null,
    bool tryParseHiveDates = false
)

public static LazyFrame ScanIpc(
    Stream stream,
    IntoSchema? schema = null,
    ulong? nRows = null,
    bool rechunk = false,
    bool cache = true,
    string? rowIndexName = null,
    uint rowIndexOffset = 0,
    string? includePathColumn = null,
    bool hivePartitioning = false,
    IntoSchema? hivePartitionSchema = null,
    bool tryParseHiveDates = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Path to the IPC file, glob pattern, or cloud path (e.g., "s3://..."). |
| `schema` | `IntoSchema?` | Optional schema to enforce. If not provided, the schema is inferred from the file footer. |
| `nRows` | `ulong?` | Limit the number of rows to scan. In Lazy mode, this acts as a 'Pre-Slice' pushdown. |
| `rechunk` | `bool` | Rechunk the memory to be contiguous (default: false). |
| `cache` | `bool` | Cache the result of the scan (default: true). |
| `glob` | `bool` | Expand glob patterns (default: true). |
| `rowIndexName` | `string?` | If provided, adds a column with the row index. |
| `rowIndexOffset` | `uint` | Offset for the row index (default: 0). |
| `includePathColumn` | `string?` | If provided, adds a column with the source file path. |
| `hivePartitioning` | `bool` | Enable Hive partitioning inference (default: false). |
| `hivePartitionSchema` | `IntoSchema?` | Manually specify the schema for Hive partitioning columns. |
| `tryParseHiveDates` | `bool` | Whether to try parsing dates in Hive partitioning paths (default: true). |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage (AWS S3, Azure Blob, GCS, etc.). |
| `buffer` | `byte[]` | - |
| `stream` | `Stream` | - |

> [!NOTE]
> This reads the stream fully into memory to construct the Lazy execution plan.

---

### ScanNdjson

Lazily read a newline delimited JSON file (NDJSON).

Lazily read NDJSON from an in-memory byte array.

Lazily read NDJSON from a Stream.

```csharp
public static LazyFrame ScanNdjson(
    string path,
    IntoSchema? schema = null,
    ulong? inferSchemaLength = null,
    ulong? batchSize = null,
    ulong? nRows = null,
    bool lowMemory = false,
    bool rechunk = false,
    bool ignoreErrors = false,
    string? rowIndexName = null,
    uint rowIndexOffset = 0,
    string? includePathColumn = null,
    CloudOptions? cloudOptions = null
)

public static LazyFrame ScanNdjson(
    byte[] buffer,
    IntoSchema? schema = null,
    ulong? inferSchemaLength = null,
    ulong? batchSize = null,
    ulong? nRows = null,
    bool lowMemory = false,
    bool rechunk = false,
    bool ignoreErrors = false,
    string? rowIndexName = null,
    uint rowIndexOffset = 0
)

public static LazyFrame ScanNdjson(
    Stream stream,
    IntoSchema? schema = null,
    ulong? inferSchemaLength = null,
    ulong? batchSize = null,
    ulong? nRows = null,
    bool lowMemory = false,
    bool rechunk = false,
    bool ignoreErrors = false,
    string? rowIndexName = null,
    uint rowIndexOffset = 0
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Path to the NDJSON file. |
| `schema` | `IntoSchema?` | Manually specify schema for specific columns (Overwrite semantics). Columns not specified will be inferred. |
| `inferSchemaLength` | `ulong?` | Number of rows to scan for schema inference. If null, uses Polars default (usually 100). |
| `batchSize` | `ulong?` | Batch size for reading (optimization). |
| `nRows` | `ulong?` | Limit the number of rows to read. |
| `lowMemory` | `bool` | Reduce memory usage at the expense of performance. |
| `rechunk` | `bool` | Rechunk the output to have contiguous memory (default: false). |
| `ignoreErrors` | `bool` | Ignore parsing errors (skip malformed lines). |
| `rowIndexName` | `string?` | If provided, adds a column with the row index. |
| `rowIndexOffset` | `uint` | Offset for the row index (default: 0). |
| `includePathColumn` | `string?` | If provided, adds a column with the source file path. |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage (AWS S3, Azure Blob, GCS, etc.). |
| `buffer` | `byte[]` | - |
| `stream` | `Stream` | - |

---

### ScanParquet

Lazily read from a parquet file or multiple files via glob patterns.

Lazily read parquet from an in-memory byte array.

```csharp
public static LazyFrame ScanParquet(
    string path,
    ulong? nRows = null,
    ParallelStrategy parallel = ParallelStrategy.Auto,
    bool lowMemory = false,
    bool useStatistics = true,
    bool glob = true,
    bool allowMissingColumns = false,
    bool rechunk = false,
    bool cache = true,
    string? rowIndexName = null,
    uint rowIndexOffset = 0,
    string? includePathColumn = null,
    IntoSchema? schema = null,
    bool hivePartitioning = false,
    IntoSchema? hivePartitionSchema = null,
    bool tryParseHiveDates = false,
    CloudOptions? cloudOptions = null
)

public static LazyFrame ScanParquet(
    byte[] buffer,
    ulong? nRows = null,
    ParallelStrategy parallel = ParallelStrategy.Auto,
    bool lowMemory = false,
    bool useStatistics = true,
    bool allowMissingColumns = false,
    bool rechunk = false,
    bool cache = true,
    string? rowIndexName = null,
    uint rowIndexOffset = 0,
    string? includePathColumn = null,
    IntoSchema? schema = null,
    bool hivePartitioning = false,
    IntoSchema? hivePartitionSchema = null,
    bool tryParseHiveDates = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Path to file or glob pattern (e.g. "data/*.parquet" or "s3://bucket/data.parquet"). |
| `nRows` | `ulong?` | Limit number of rows to read (optimization). |
| `parallel` | `ParallelStrategy` | Parallel strategy. |
| `lowMemory` | `bool` | Reduce memory usage at the expense of performance. |
| `useStatistics` | `bool` | Use parquet statistics to optimize the query plan. |
| `glob` | `bool` | Expand glob patterns (default: true). |
| `allowMissingColumns` | `bool` | Allow missing columns when reading multiple files. |
| `rechunk` | `bool` | Rechunk the memory to contiguous chunks when reading. (default: false) |
| `cache` | `bool` | Cache the result after reading. (default: true) |
| `rowIndexName` | `string?` | If provided, adds a column with the row number. |
| `rowIndexOffset` | `uint` | Offset for the row index. |
| `includePathColumn` | `string?` | If provided, adds a column with the source file path. |
| `schema` | `IntoSchema?` | Manually specify the schema of the file(s). Useful if the file footer is missing or to avoid I/O overhead of reading the schema. |
| `hivePartitioning` | `bool` | Enable Hive partitioning inference (default: false). |
| `hivePartitionSchema` | `IntoSchema?` | Manually specify the schema for Hive partitioning columns. Use this to ensure specific types for partition keys (e.g. string instead of int). |
| `tryParseHiveDates` | `bool` | Whether to try parsing dates in Hive partitioning paths (default: false). |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage (AWS S3, Azure Blob, GCS, etc.). |
| `buffer` | `byte[]` | - |

---

### ScanRecordBatches

Scan RecordBatch Stream If schema is provied, first batch won't be consumed for getting schema.

```csharp
public static LazyFrame ScanRecordBatches(IEnumerable<RecordBatch> stream, Schema schema)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `stream` | `IEnumerable<RecordBatch>` | - |
| `schema` | `Schema` | - |

---

## Instance Methods

### BottomK

Get the Bottom k rows according to a single expression, string, or selector. Usage: lf.BottomK(5, "Sales") or lf.BottomK(10, Pl.Col("Age") * 2, reverse: true)

Get the Bottom k rows according to multiple column names with a uniform reverse setting. Usage: lf.BottomK(5, ["Score", "Age"], reverse: false)

Get the Bottom k rows with specific reverse directions using Tuples. Usage: lf.BottomK(5, ("Score", false), ("Age", true), (Cs.Numeric(), false))

Master method: Get the Bottom k rows by multiple Expressions with optional parallel boolean arrays. Usage: lf.BottomK(10, myColumns.Select(c =&gt; Pl.Col(c)))

```csharp
public LazyFrame BottomK(int k, IntoExprColumn by, bool reverse = false)
public LazyFrame BottomK(int k, IEnumerable<string> by, bool reverse = false)
public LazyFrame BottomK(int k, params (IntoExprColumn, bool)[] configs)
public LazyFrame BottomK(int k, IEnumerable<Expr> by, bool[]? reverse = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `k` | `int` | - |
| `by` | `IntoExprColumn / IEnumerable<string> / IEnumerable<Expr>` | - |
| `reverse` | `bool / bool[]?` | - |
| `configs` | `(IntoExprColumn, bool)[]` | - |

---

### Cast

Cast all columns to the specified target type.

Cast a specific Expression, String, Selector, or Type to a DataType.

Cast multiple targets using tuples. Supports String, Expr, Selector mixed with Type, DataType, or DataTypeExpr! Usage: lf.Cast( ("Age", typeof(int)), (Cs.Numeric(), DataType.Float32), (DataType.Float64, DataType.Float32) )

Cast columns to the types specified in a Schema, DataFrame, or LazyFrame.

```csharp
public LazyFrame Cast(IntoDataTypeExpr dtype, bool strict = true, bool wrapNumerical = false)
public LazyFrame Cast(
    IntoSelector selector,
    IntoDataTypeExpr dtype,
    bool strict = true,
    bool wrapNumerical = false
)

public LazyFrame Cast(params (IntoSelector, IntoDataTypeExpr)[] casts)
public LazyFrame Cast(IntoSchema schemaWrapper, bool strict = true, bool wrapNumerical = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dtype` | `IntoDataTypeExpr` | - |
| `strict` | `bool` | - |
| `wrapNumerical` | `bool` | - |
| `selector` | `IntoSelector` | - |
| `casts` | `(IntoSelector, IntoDataTypeExpr)[]` | - |
| `schemaWrapper` | `IntoSchema` | - |

---

### Clear

Create an empty (n=0) or n-row null-filled (n&gt;0) copy of the LazyFrame. Returns a n-row null-filled LazyFrame with an identical schema.

```csharp
public LazyFrame Clear(long n = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `long` | Number of (null-filled) rows to return in the cleared frame. |

**Returns:** A new LazyFrame.

---

### Clone

Clone the LazyFrame, creating a new independent copy.

```csharp
public LazyFrame Clone()
```

---

### Collect

Execute the query plan and return a DataFrame.

```csharp
public DataFrame Collect(Engine engine = Engine.Auto)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `engine` | `Engine` | - |

---

### CollectAsync

Execute the query plan asynchronously and return a DataFrame.

```csharp
public Task<DataFrame> CollectAsync(Engine engine = Engine.Auto, CancellationToken cancellationToken = default)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `engine` | `Engine` | - |
| `cancellationToken` | `CancellationToken` | - |

---

### CollectBatchesAsync

Streams materialized DataFrame batches asynchronously using Polars native streaming engine. Memory consumption is bounded by the batch size O(batch).

```csharp
public IAsyncEnumerable<DataFrame> CollectBatchesAsync(
    long chunkSize = 0,
    Engine engine = Engine.Auto,
    bool maintainOrder = false,
    CancellationToken cancellationToken = default
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `chunkSize` | `long` | Maximum row count per native batch (0 uses Polars default batch size). |
| `engine` | `Engine` | Execution engine selection. |
| `maintainOrder` | `bool` | Whether to enforce deterministic row ordering across batches. |
| `cancellationToken` | `CancellationToken` | Optional cooperative cancellation token. |

**Returns:** An asynchronous stream of DataFrame batches.

---

### CollectSchema

Resolve the schema of this LazyFrame.

```csharp
public PolarsSchema CollectSchema()
```

---

### Count

Return the number of non-null elements for each column.

```csharp
public LazyFrame Count()
```

---

### Describe

Generate a summary statistics DataFrame (count, mean, std, min, 25%, 50%, 75%, max). Similar to pandas/polars describe(). Notice: This will collect LazyFrame once, but LazyFrame won't be consumed.

```csharp
public DataFrame Describe()
```

---

### Drop

Drop columns based on Strings, Selectors, Types, or Expressions. Usage: lf.Drop("Id", "Name", Cs.Numeric(), typeof(float), Pl.Col("Status"));

Bridge overload to support C# 12 collection expressions. Usage: lf.Drop(["Id", "Name"])

```csharp
public LazyFrame Drop(params IntoSelector[] columnsToDrop)
public LazyFrame Drop(IEnumerable<string> columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columnsToDrop` | `IntoSelector[]` | - |
| `columns` | `IEnumerable<string>` | - |

---

### DropNans

Drop rows containing one or more NaN values.

Drop rows with NaN in specific columns.

Drop NaN in all columns.

```csharp
public LazyFrame DropNans(params IntoSelector[] subsets)
public LazyFrame DropNans(IEnumerable<string> subset)
public LazyFrame DropNans()
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `subsets` | `IntoSelector[]` | Optional subset of columns to consider. If null, evaluates all columns. |
| `subset` | `IEnumerable<string>` | - |

---

### DropNulls

Drop rows containing one or more Null values in ALL columns.

Drop rows with Nulls in a specific subset of columns (Selectors, Strings, Types, Exprs). Usage: lf.DropNulls("Id", Cs.Numeric(), typeof(string), Pl.Col("Status"));

Bridge overload to support C# 12 collection expressions. Usage: lf.DropNulls(["Id", "Name"])

```csharp
public LazyFrame DropNulls()
public LazyFrame DropNulls(params IntoSelector[] subsets)
public LazyFrame DropNulls(IEnumerable<string> subset)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `subsets` | `IntoSelector[]` | - |
| `subset` | `IEnumerable<string>` | - |

---

### Explain

Get an explanation of the optimized query plan. 

Returns a string representation of the logical plan after Polars optimizers (predicate pushdown, projection pushdown, etc.) have run.

```csharp
public string Explain(bool optimized = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `optimized` | `bool` | If true, show the optimized plan. If false, show the logical plan as built. |

**Returns:** The plan as a string.

**Example**

```csharp
var q = df.Lazy()
    .Filter(Pl.Col("group") != "C")
    .WithColumns((Pl.Col("val") * 2).Alias("val_x_2"))
    .Select("group", "val_x_2");

Console.WriteLine(q.Explain());
/* Output (Optimized Plan):
simple π 2/2 ["group", "val_x_2"]
   WITH_COLUMNS:
   [[(col("val")) * (2)].alias("val_x_2")] 
    FILTER [(col("group")) != ("C")]
    FROM
      DF ["group", "val"]; PROJECT["group", "val"] 2/2 COLUMNS
*/
```

---

### Explode

Explode list-like columns into multiple rows.

Bridge overload to support C# 12 collection expressions. Usage: lf.Explode(["list1", "list2"]) or lf.Explode(["list1", "list2"], keepNulls: false)

```csharp
public LazyFrame Explode(IntoSelector columns, bool emptyAsNull = true, bool keepNulls = true)
public LazyFrame Explode(IEnumerable<string> columns, bool emptyAsNull = true, bool keepNulls = true)
public LazyFrame Explode(params string[] columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `IntoSelector / IEnumerable<string> / string[]` | the columns need to be exploded |
| `emptyAsNull` | `bool` | If `true`, empty lists are exploded into a single `null` value. If `false`, rows with empty lists are removed from the result. |
| `keepNulls` | `bool` | If `true`, `null` values in the column are preserved as `null` in the result. If `false`, rows with `null` values are removed. |

---

### FillNan

Fill NaN values in floating point columns with a specified Expression.

```csharp
public LazyFrame FillNan(Expr fillValue)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fillValue` | `Expr` | - |

---

### FillNull

Fill null values in all columns with a specified Expression.

Fill null values in all columns with a specified literal value.

```csharp
public LazyFrame FillNull(Expr fillValue)
public LazyFrame FillNull(object value)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fillValue` | `Expr` | - |
| `value` | `object` | - |

---

### Filter

Filter rows based on a boolean expression. 

In a LazyFrame, this operation is added to the logical plan and is optimized before execution. Polars will attempt to push this filter down as close to the data source as possible (Predicate Pushdown).

Filter rows based on a boolean series.

Filter rows based on a boolean array.

```csharp
public LazyFrame Filter(Expr expr)
public LazyFrame Filter(Series series)
public LazyFrame Filter(IEnumerable<bool> mask)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | A boolean expression. |
| `series` | `Series` | - |
| `mask` | `IEnumerable<bool>` | - |

**Returns:** A new LazyFrame with the filter applied.

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    group = new[] { "A", "A", "B", "B", "C" },
    val = new[] { 1, 2, 3, 4, 5 }
});

// Build a lazy query:
// 1. Filter out group 'C'
// 2. Multiply 'val' by 2
// 3. Select specific columns
var q = df.Lazy()
    .Filter(Col("group") != "C")
    .WithColumns((Col("val") * 2).Alias("val_x_2"))
    .Select("group", "val_x_2");

// Execute
q.Collect().Show();
/* Output:
shape: (4, 2)
┌───────┬─────────┐
│ group ┆ val_x_2 │
│ ---   ┆ ---     │
│ str   ┆ i32     │
╞═══════╪═════════╡
│ A     ┆ 2       │
│ A     ┆ 4       │
│ B     ┆ 6       │
│ B     ┆ 8       │
└───────┴─────────┘
*/
```

---

### First

Get the first row.

```csharp
public LazyFrame First()
```

---

### Gather

Selects rows from this LazyFrame at the given indices.

```csharp
public LazyFrame Gather(IntoIndexLazyFrame index, bool nullOnOob = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `IntoIndexLazyFrame` | The indices of the rows to select. |
| `nullOnOob` | `bool` | If true when an index is out-of-bounds a null row will be generated instead of raising an error. |

---

### GatherEvery

Take every nth row in the Frame and return as a new Frame.

```csharp
public LazyFrame GatherEvery(int n, int offset = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | Gather every n-th row. |
| `offset` | `int` | Starting Index |

---

### GroupBy

Group by multiple expressions, strings, or literals. Default behavior: maintains group order.

Group by a single key with explicit control over maintainOrder.

```csharp
public LazyGroupBy GroupBy(IEnumerable<IntoExprColumn> keys, bool maintainOrder = true)
public LazyGroupBy GroupBy(IntoExprColumn key, bool maintainOrder = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `keys` | `IEnumerable<IntoExprColumn>` | - |
| `maintainOrder` | `bool` | - |
| `key` | `IntoExprColumn` | - |

**Example**

```csharp
df.Lazy()
  .GroupBy("group")
  .Agg(Col("val").Sum().Alias("sum_val"))
  .Collect();

/* Output:
shape: (2, 2)
┌───────┬─────────┐
│ group ┆ sum_val │
│ ---   ┆ ---     │
│ str   ┆ i32     │
╞═══════╪═════════╡
│ A     ┆ 3       │
│ B     ┆ 7       │
└───────┴─────────┘
*/
```

> [!NOTE]
> Unlike `DataFrame.GroupBy` which returns a `GroupByBuilder`, this returns a `LazyGroupBy` object which allows constructing the aggregation plan.

---

### GroupByDynamic

Lazily group based on a time index using dynamic windows. 

This defines a dynamic groupby in the query plan.

```csharp
public LazyGroupBy GroupByDynamic(
    IntoSelector indexColumn,
    IntoDuration every,
    IntoDuration? period = null,
    IntoDuration? offset = null,
    IEnumerable<IntoExprColumn>? groupBy = null,
    Label label = Label.Left,
    bool includeBoundaries = false,
    ClosedInterval ClosedInterval = ClosedInterval.Left,
    StartBy startBy = StartBy.WindowBound
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `indexColumn` | `IntoSelector` | - |
| `every` | `IntoDuration` | - |
| `period` | `IntoDuration?` | - |
| `offset` | `IntoDuration?` | - |
| `groupBy` | `IEnumerable<IntoExprColumn>?` | - |
| `label` | `Label` | - |
| `includeBoundaries` | `bool` | - |
| `ClosedInterval` | `ClosedInterval` | - |
| `startBy` | `StartBy` | - |

**Example**

```csharp
df.Lazy()
  .GroupByDynamic("time", every: TimeSpan.FromHours(1))
  .Agg(Pl.Col("val").Sum().Alias("total"))
  .Collect();
```

---

### Head

Get the first n rows.

```csharp
public LazyFrame Head(uint n = 5)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `uint` | Number of rows to return. |

---

### Interpolate

Interpolate intermediate values. The interpolation method is linear. Nulls at the beginning and end of the series remain null.

```csharp
public LazyFrame Interpolate()
```

---

### Join

Lazily join with another LazyFrame. 

Polars will optimize the join execution order. Both frames must be LazyFrames.

Join using specific left and right columns (Strings or Exprs). Usage: lf.Join(other, leftOn: "userId", rightOn: Pl.Col("id"))

Join using multiple shared column names. Usage: lf.Join(other, on: ["date", "region"])

Join using specific multiple expressions or column names. Usage: lf.Join(other, leftOn: ["lid", "ldate"], rightOn: ["rid", "rdate"])

Join using multiple shared expressions (or mixed strings/exprs via IntoExpr). Usage: lf.Join(other, on: [Pl.Col("date"), Pl.Col("region")])

Join using specific multiple expressions. (C# Array Covariance Helper) Usage: lf.Join(other, leftOn: new[] { Pl.Col("A") }, rightOn: new[] { Pl.Col("B") })

Join using multiple shared expressions. (C# Array Covariance Helper) Usage: lf.Join(other, on: new[] { Pl.Col("A"), Pl.Col("B") })

```csharp
public LazyFrame Join(
    LazyFrame other,
    IntoExprColumn on,
    JoinType how = JoinType.Inner,
    string? suffix = null,
    JoinValidation validation = JoinValidation.ManyToMany,
    JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
    JoinSide joinSide = JoinSide.LetPolarsDecide,
    bool nullsEqual = false,
    long? sliceOffset = null,
    ulong sliceLen = 0
)

public LazyFrame Join(
    LazyFrame other,
    IntoExprColumn leftOn,
    IntoExprColumn rightOn,
    JoinType how = JoinType.Inner,
    string? suffix = null,
    JoinValidation validation = JoinValidation.ManyToMany,
    JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
    JoinSide joinSide = JoinSide.LetPolarsDecide,
    bool nullsEqual = false,
    long? sliceOffset = null,
    ulong sliceLen = 0
)

public LazyFrame Join(
    LazyFrame other,
    IEnumerable<string> on,
    JoinType how = JoinType.Inner,
    string? suffix = null,
    JoinValidation validation = JoinValidation.ManyToMany,
    JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
    JoinSide joinSide = JoinSide.LetPolarsDecide,
    bool nullsEqual = false,
    long? sliceOffset = null,
    ulong sliceLen = 0
)

public LazyFrame Join(
    LazyFrame other,
    IEnumerable<IntoExprColumn> leftOn,
    IEnumerable<IntoExprColumn> rightOn,
    JoinType how = JoinType.Inner,
    string? suffix = null,
    JoinValidation validation = JoinValidation.ManyToMany,
    JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
    JoinSide joinSide = JoinSide.LetPolarsDecide,
    bool nullsEqual = false,
    long? sliceOffset = null,
    ulong sliceLen = 0
)

public LazyFrame Join(
    LazyFrame other,
    IEnumerable<IntoExprColumn> on,
    JoinType how = JoinType.Inner,
    string? suffix = null,
    JoinValidation validation = JoinValidation.ManyToMany,
    JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
    JoinSide joinSide = JoinSide.LetPolarsDecide,
    bool nullsEqual = false,
    long? sliceOffset = null,
    ulong sliceLen = 0
)

public LazyFrame Join(
    LazyFrame other,
    IEnumerable<Expr> leftOn,
    IEnumerable<Expr> rightOn,
    JoinType how = JoinType.Inner,
    string? suffix = null,
    JoinValidation validation = JoinValidation.ManyToMany,
    JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
    JoinSide joinSide = JoinSide.LetPolarsDecide,
    bool nullsEqual = false,
    long? sliceOffset = null,
    ulong sliceLen = 0
)

public LazyFrame Join(
    LazyFrame other,
    IEnumerable<Expr> on,
    JoinType how = JoinType.Inner,
    string? suffix = null,
    JoinValidation validation = JoinValidation.ManyToMany,
    JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
    JoinSide joinSide = JoinSide.LetPolarsDecide,
    bool nullsEqual = false,
    long? sliceOffset = null,
    ulong sliceLen = 0
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `LazyFrame` | - |
| `on` | `IntoExprColumn / IEnumerable<string> / IEnumerable<IntoExprColumn> / IEnumerable<Expr>` | - |
| `how` | `JoinType` | - |
| `suffix` | `string?` | - |
| `validation` | `JoinValidation` | - |
| `coalesce` | `JoinCoalesce` | - |
| `maintainOrder` | `JoinMaintainOrder` | - |
| `joinSide` | `JoinSide` | - |
| `nullsEqual` | `bool` | - |
| `sliceOffset` | `long?` | - |
| `sliceLen` | `ulong` | - |
| `leftOn` | `IntoExprColumn / IEnumerable<IntoExprColumn> / IEnumerable<Expr>` | - |
| `rightOn` | `IntoExprColumn / IEnumerable<IntoExprColumn> / IEnumerable<Expr>` | - |

**Example**

```csharp
var lf1 = df1.Lazy();
var lf2 = df2.Lazy();

// Lazy Left Join
var joined = lf1.Join(lf2, Col("id"), JoinType.Left)
                .Collect();

/* Output:
shape: (3, 3)
┌─────┬─────────┬───────┐
│ id  ┆ name    ┆ score │
│ --- ┆ ---     ┆ ---   │
│ i32 ┆ str     ┆ i32   │
╞═════╪═════════╪═══════╡
│ 1   ┆ Alice   ┆ 90    │
│ 2   ┆ Bob     ┆ 80    │
│ 3   ┆ Charlie ┆ null  │
└─────┴─────────┴───────┘
*/
```

---

### JoinAsOf

Join AsOf using a shared 'on' column and optional shared 'by' columns. Usage: lf.JoinAsOf(other, on: "time", tolerance: "2h", by: ["ticker"])

Join AsOf using independent 'leftOn'/'rightOn' and 'leftBy'/'rightBy' columns. Usage: lf.JoinAsOf(other, leftOn: "time_L", rightOn: "time_R", tolerance: TimeSpan.FromMinutes(5))

```csharp
public LazyFrame JoinAsOf(
    LazyFrame other,
    IntoExprColumn on,
    AsOfTolerance? tolerance = null,
    IEnumerable<string>? by = null,
    AsofStrategy strategy = AsofStrategy.Backward,
    bool allowEq = true,
    bool checkSorted = true,
    string? suffix = null,
    JoinValidation validation = JoinValidation.ManyToMany,
    JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
    JoinSide joinSide = JoinSide.LetPolarsDecide,
    bool nullsEqual = false,
    long? sliceOffset = null,
    ulong sliceLen = 0
)

public LazyFrame JoinAsOf(
    LazyFrame other,
    IntoExprColumn leftOn,
    IntoExprColumn rightOn,
    AsOfTolerance? tolerance = null,
    IEnumerable<string>? leftBy = null,
    IEnumerable<string>? rightBy = null,
    AsofStrategy strategy = AsofStrategy.Backward,
    bool allowEq = true,
    bool checkSorted = true,
    string? suffix = null,
    JoinValidation validation = JoinValidation.ManyToMany,
    JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
    JoinSide joinSide = JoinSide.LetPolarsDecide,
    bool nullsEqual = false,
    long? sliceOffset = null,
    ulong sliceLen = 0
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `LazyFrame` | The right LazyFrame to join with. |
| `on` | `IntoExprColumn` | - |
| `tolerance` | `AsOfTolerance?` | Tolerance as a time duration string (e.g., "2h", "10s", "1d"), int or double or TimeSpan. Matches that are further away than this duration are discarded. |
| `by` | `IEnumerable<string>?` | - |
| `strategy` | `AsofStrategy` | The strategy to determine which value is "nearest" (Backward, Forward, or Nearest). Defaults to `AsofStrategy.Backward`. |
| `allowEq` | `bool` | If true, allow exact matches to be included in the result. If false, a match must be strictly unequal (e.g. less than for Backward strategy) to the key. |
| `checkSorted` | `bool` | Check if the join keys are sorted. If false, the user must ensure keys are sorted; otherwise results are undefined (but execution is faster). |
| `suffix` | `string?` | Suffix to append to columns with name conflicts. Defaults to "_right". |
| `validation` | `JoinValidation` | Check if join keys are unique (mostly relevant for the 'by' columns). |
| `coalesce` | `JoinCoalesce` | How to coalesce the join keys. |
| `maintainOrder` | `JoinMaintainOrder` | How to maintain the order of the join. |
| `joinSide` | `JoinSide` | pecifies the strategy for the hash join build side. |
| `nullsEqual` | `bool` | Consider nulls as equal. |
| `sliceOffset` | `long?` | Slice the result starting at this offset (optimization). |
| `sliceLen` | `ulong` | Length of the slice to keep. |
| `leftOn` | `IntoExprColumn` | Join key of the left LazyFrame. Must be sorted. |
| `rightOn` | `IntoExprColumn` | Join key of the right LazyFrame. Must be sorted. |
| `leftBy` | `IEnumerable<string>?` | Columns to match exactly (equivalence join) before performing the as-of join. Useful for joining separate time-series per group (e.g., by "Symbol"). |
| `rightBy` | `IEnumerable<string>?` | Columns to match exactly in the right DataFrame. |

**Example**

```csharp
// Trades: Events happening at specific times
var trades = DataFrame.FromColumns(new
{
    time = new[] { 10, 20, 30 },
    stock = new[] { "A", "A", "A" }
}).Lazy();

// Quotes: Price updates (irregular intervals)
// 9->100, 15->101, 25->102, 40->103
var quotes = DataFrame.FromColumns(new
{
    time = new[] { 9, 15, 25, 40 },
    bid = new[] { 100, 101, 102, 103 }
}).Lazy();

// Find the latest quote BEFORE or AT the trade time
var asof = trades.JoinAsOf(
    quotes, 
    leftOn: Pl.Col("time"), 
    rightOn: Pl.Col("time"),
    strategy: AsofStrategy.Backward
);

var df = asof.Collect();
df.Show();
/* Output:
shape: (3, 3)
┌──────┬───────┬─────┐
│ time ┆ stock ┆ bid │
│ ---  ┆ ---   ┆ --- │
│ i32  ┆ str   ┆ i32 │
╞══════╪═══════╪═════╡
│ 10   ┆ A     ┆ 100 │ // Matches time 9
│ 20   ┆ A     ┆ 101 │ // Matches time 15
│ 30   ┆ A     ┆ 102 │ // Matches time 25
└──────┴───────┴─────┘
*/
```

---

### JoinWhere

Join with another LazyFrame using multiple boolean expressions. Usage: lf.JoinWhere(other, [Pl.Col("start") &gt; Pl.Col("end"), Pl.Col("id") != null], how: JoinType.Left)

Join using a single condition (Most common use case). Usage: lf.JoinWhere(other, Pl.Col("A") &gt; Pl.Col("B"), how: JoinType.Inner)

Usage: lf.JoinWhere(other, Pl.Col("A") &gt; Pl.Col("B"), Pl.Col("C") &gt; 0)

```csharp
public LazyFrame JoinWhere(
    LazyFrame other,
    IEnumerable<IntoExprColumn> predicates,
    JoinType how = JoinType.Inner,
    string? suffix = null,
    JoinValidation validation = JoinValidation.ManyToMany,
    JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
    bool nullsEqual = false
)

public LazyFrame JoinWhere(
    LazyFrame other,
    IntoExprColumn predicate,
    JoinType how = JoinType.Inner,
    string? suffix = null,
    JoinValidation validation = JoinValidation.ManyToMany,
    JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
    bool nullsEqual = false
)

public LazyFrame JoinWhere(LazyFrame other, params IntoExprColumn[] predicates)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `LazyFrame` | - |
| `predicates` | `IEnumerable<IntoExprColumn> / IntoExprColumn[]` | - |
| `how` | `JoinType` | - |
| `suffix` | `string?` | - |
| `validation` | `JoinValidation` | - |
| `coalesce` | `JoinCoalesce` | - |
| `maintainOrder` | `JoinMaintainOrder` | - |
| `nullsEqual` | `bool` | - |
| `predicate` | `IntoExprColumn` | - |

---

### Last

Get the last row.

```csharp
public LazyFrame Last()
```

---

### Limit

Get the first n rows.

```csharp
public LazyFrame Limit(uint n = 5)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `uint` | Number of rows to return. |

---

### MatchToSchema

Align the Frame's schema to the target schema. Columns not in the target schema are handled according to the extraColumns policy.

```csharp
public LazyFrame MatchToSchema(
    PolarsSchema schema,
    ExtraColumnsPolicy extraColumns = ExtraColumnsPolicy.Raise,
    MatchSchemaConfig? defaultConfig = null,
    IReadOnlyDictionary<string, MatchSchemaConfig>? columnOverrides = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `schema` | `PolarsSchema` | The target schema to match against. |
| `extraColumns` | `ExtraColumnsPolicy` | Policy for handling top-level columns present in the frame but not in the target schema. |
| `defaultConfig` | `MatchSchemaConfig?` | The default configuration applied to all columns for handling missing data and type casting. |
| `columnOverrides` | `IReadOnlyDictionary<string, MatchSchemaConfig>?` | Specific configuration overrides mapped by column name. |

**Returns:** A new `LazyFrame` with the aligned schema.

---

### Max

Aggregate the columns in the Frame to their maximum value.

```csharp
public LazyFrame Max()
```

---

### Mean

Aggregate the columns in the Frame to their mean value.

```csharp
public LazyFrame Mean()
```

---

### Median

Aggregate the columns in the Frame to their median value.

```csharp
public LazyFrame Median()
```

---

### Melt

Unpivot (Melt) the LazyFrame from wide to long format. Supports mixing Selectors, Expressions, Types, or single Strings. Usage: lf.Unpivot("Date", Cs.Numeric()) or lf.Unpivot(Cs.StartsWith("id"), "Status")

Bridge overload to support C# 12 collection expressions. Usage: lf.Unpivot(["Date", "Region"], ["Q1", "Q2", "Q3"])

```csharp
public LazyFrame Melt(
    IntoSelector index,
    IntoSelector? on = null,
    string variableName = "variable",
    string valueName = "value"
)

public LazyFrame Melt(
    IEnumerable<string> index,
    IEnumerable<string>? on = null,
    string variableName = "variable",
    string valueName = "value"
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `IntoSelector / IEnumerable<string>` | - |
| `on` | `IntoSelector? / IEnumerable<string>?` | - |
| `variableName` | `string` | - |
| `valueName` | `string` | - |

---

### Merge

Initiates a Merge (Upsert) builder to seamlessly apply changes from a source DataFrame.

Initiates a Merge builder using a Selector or Column Expression. Usage: lf.Merge(other, Cs.Numeric()) or lf.Merge(other, Pl.Col("^id_.*$"))

```csharp
public LazyFrameMergeBuilder Merge(LazyFrame source, params string[] on)
public LazyFrameMergeBuilder Merge(LazyFrame source, IntoSelector on)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `source` | `LazyFrame` | - |
| `on` | `string[] / IntoSelector` | - |

---

### MergeCatalogRecords

Starts building a Merge (Upsert) operation using this `LazyFrame` as the source.

```csharp
public DeltaMergeBuilder MergeCatalogRecords(
    UnityCatalog catalog,
    string catalogName,
    string schemaName,
    string tableName,
    string[] mergeKeys,
    bool canEvolve = false,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalog` | `UnityCatalog` | - |
| `catalogName` | `string` | The Unity Catalog name (e.g., "main"). |
| `schemaName` | `string` | The schema/database name within the catalog. |
| `tableName` | `string` | The target Delta table name registered in the catalog. |
| `mergeKeys` | `string[]` | An array of column names used as join keys to match source rows with target rows. |
| `canEvolve` | `bool` | If true, enables Schema Evolution, allowing the target table's schema to be updated if the source contains new columns. |
| `cloudOptions` | `CloudOptions?` | Cloud-specific configurations (S3/Azure/GCS) including dynamic credentials resolved via Unity Catalog. |

**Returns:** A `DeltaMergeBuilder` used to configure "WhenMatched" and "WhenNotMatched" clauses.

> [!NOTE]
> This is a builder pattern. No data is modified until `.Execute()` is called on the resulting `DeltaMergeBuilder`. The operation performs a join between the source and target based on the `mergeKeys`.

---

### MergeDelta

Merge a LazyFrame into a Delta Lake table with full SQL MERGE semantics. Provides fine-grained control over Update, Insert, and Delete behaviors. Notice: In this method, Delete &gt; Update &gt; Insert &gt; Ignore. If you need other orders, please use LazyFrame.MergeDeltaOrdered

```csharp
public void MergeDelta(
    string path,
    string[] mergeKeys,
    Expr? matchedUpdateCond = null,
    Expr? matchedDeleteCond = null,
    Expr? notMatchedInsertCond = null,
    Expr? notMatchedBySourceDeleteCond = null,
    bool canEvolve = false,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Uri to the Delta Lake table (local or cloud). |
| `mergeKeys` | `string[]` | The column names to join on (must exist in both Source and Target). |
| `matchedUpdateCond` | `Expr?` | Condition for 'WHEN MATCHED THEN UPDATE'. If null, defaults to true (always update when matched). |
| `matchedDeleteCond` | `Expr?` | Condition for 'WHEN MATCHED THEN DELETE'. If null, defaults to false (never delete when matched). |
| `notMatchedInsertCond` | `Expr?` | Condition for 'WHEN NOT MATCHED THEN INSERT'. If null, defaults to true (always insert new rows). |
| `notMatchedBySourceDeleteCond` | `Expr?` | Condition for 'WHEN NOT MATCHED BY SOURCE THEN DELETE' (Target rows not in Source). If null, defaults to false (retain target-only rows). |
| `canEvolve` | `bool` | Define whether schema evolution is allowed, default: false |
| `cloudOptions` | `CloudOptions?` | Cloud storage credentials and configuration. |

---

### MergeDeltaOrdered

Starts a fluent builder to merge a LazyFrame into a Delta Lake table with strict, order-preserving SQL MERGE semantics. 

Unlike traditional merge methods, this builder guarantees that chained actions (Update, Delete, Insert) are evaluated exactly in the order they are defined. If no actions are specified before execution, it intelligently defaults to a standard Upsert (WhenMatchedUpdate + WhenNotMatchedInsert).

```csharp
public DeltaMergeBuilder MergeDeltaOrdered(
    string path,
    string[] mergeKeys,
    bool canEvolve = false,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | The URI to the target Delta Lake table (local or cloud). |
| `mergeKeys` | `string[]` | The column names to join on (must exist in both the Source DataFrame and Target Delta table). |
| `canEvolve` | `bool` | If set to true, allows schema evolution (e.g., adding new columns from the Source to the Target). Default is false. |
| `cloudOptions` | `CloudOptions?` | Cloud storage credentials and configuration (e.g., AWS S3, Azure Blob). |

**Returns:** A `DeltaMergeBuilder` instance used to chain match conditions, culminating in a call to `.Execute()`.

**Example**

```csharp
lf.MergeDeltaOrdered("s3://bucket/my_table", new[] { "Id" })
  .WhenMatchedDelete(Delta.Source("Status") == "Deleted")      // Evaluated 1st
  .WhenMatchedUpdate(Delta.Source("Stock") > Delta.Target("Stock")) // Evaluated 2nd
  .WhenNotMatchedInsert()                                      // Evaluated 3rd
  .Execute();
```

---

### MergeSorted

Take two sorted DataFrames and merge them by the sorted key. The output of this operation will also be sorted. It is the callers responsibility that the frames are sorted in ascending order by the key, with null keys at the end, otherwise the order of the output will not make sense. The schemas of both LazyFrames must be equal.

```csharp
public LazyFrame MergeSorted(LazyFrame other, string key, bool maintainOrder = false)
public LazyFrame MergeSorted(LazyFrame other, string[] key, bool maintainOrder = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `LazyFrame` | Other DataFrame that must be merged |
| `key` | `string / string[]` | Key that is sorted. |
| `maintainOrder` | `bool` | If True, the output is guaranteed to have left-biased ordering for equal keys: rows from the left frame appear before rows from the right frame when their keys are equal. |

---

### Min

Aggregate the columns in the Frame to their minimum value.

```csharp
public LazyFrame Min()
```

---

### NullCount

Aggregate the columns in the Frame as the sum of their null value count.

```csharp
public LazyFrame NullCount()
```

---

### Pipe

Offers a structured way to apply a sequence of user-defined functions (UDFs).

```csharp
public T Pipe<T>(Func<LazyFrame, T> function)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `function` | `Func<LazyFrame, T>` | A function that receives the current expression and returns a value of type `T`. Typically this function wraps several Polars API calls that operate on the given expression. |

**Returns:** The result of applying `function` to this LazyFrame.

---

### PipeWithSchema

Allows to alter the lazy frame during the plan stage with the resolved schema. 

In contrast to pipe, this method does not execute function immediately but only during the plan stage. This allows to use the resolved schema of the input to dynamically alter the lazyframe. This also means that any exceptions raised by function will only be emitted during the plan stage.

```csharp
public LazyFrame PipeWithSchema(Func<LazyFrame, PolarsSchema, LazyFrame> function)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `function` | `Func<LazyFrame, PolarsSchema, LazyFrame>` | Callable; will receive the frame as the first parameter and the resolved schema as the second parameter. |

---

### Pivot

Pivot the LazyFrame. 

Important: Lazy pivot requires an eager `onColumns` DataFrame to determine the output schema (column names) during the planning phase.

Pivot the LazyFrame using column names.

```csharp
public LazyFrame Pivot(
    IntoSelector on,
    IntoPivotHint onColumns,
    IntoSelector? index = null,
    IntoSelector? values = null,
    Expr? aggregateExpr = null,
    PivotAgg aggregateFunction = PivotAgg.First,
    bool maintainOrder = false,
    string separator = "_",
    PivotColumnNaming columnNaming = PivotColumnNaming.Auto
)

public LazyFrame Pivot(
    IEnumerable<string> on,
    IntoPivotHint onColumns,
    IEnumerable<string>? index = null,
    IEnumerable<string>? values = null,
    Expr? aggregateExpr = null,
    PivotAgg aggregateFunction = PivotAgg.First,
    bool maintainOrder = false,
    string separator = "_",
    PivotColumnNaming columnNaming = PivotColumnNaming.Auto
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `on` | `IntoSelector / IEnumerable<string>` | Selector for the column(s) to pivot (the new column headers). |
| `onColumns` | `IntoPivotHint` | A DataFrame / Series / string array containing the unique values of the `onColumns`. This is strictly used for schema inference. |
| `index` | `IntoSelector? / IEnumerable<string>?` | Selector for the index column(s) (the rows). |
| `values` | `IntoSelector? / IEnumerable<string>?` | Selector for the value column(s) to populate the cells. |
| `aggregateExpr` | `Expr?` | Optional expression to aggregate the values. If null, uses `aggregateFunction`. |
| `aggregateFunction` | `PivotAgg` | Aggregation function to use if `aggregateExpr` is null. Default is First. |
| `maintainOrder` | `bool` | Sort the result by the index column. |
| `separator` | `string` | Separator used to combine column names when multiple value columns are selected. |
| `columnNaming` | `PivotColumnNaming` | How resulting column names will be constructed. |

**Returns:** A new LazyFrame with the pivot operation applied.

---

### PrintSchema

Prints the schema to the console.

```csharp
public void PrintSchema()
```

---

### Quantile

Aggregate the columns in the Frame to their quantile value.

```csharp
public LazyFrame Quantile(double quantile, QuantileMethod method = QuantileMethod.Linear)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `quantile` | `double` | Quantile between 0.0 and 1.0. |
| `method` | `QuantileMethod` | ['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method. |

---

### Remove

Remove rows that match the predicate. This is the exact opposite of Filter. Rows where the predicate evaluates to False or Null are kept.

Remove rows based on a boolean series.

Remove rows based on a boolean array.

```csharp
public LazyFrame Remove(Expr predicate)
public LazyFrame Remove(Series predicate)
public LazyFrame Remove(IEnumerable<bool> mask)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `predicate` | `Expr / Series` | - |
| `mask` | `IEnumerable<bool>` | - |

---

### Rename

Renames columns in the `LazyFrame`.

```csharp
public LazyFrame Rename(string[] existing, string[] newNames, bool strict = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `existing` | `string[]` | An array of existing column names to be renamed. |
| `newNames` | `string[]` | An array of new column names, corresponding by index to the names in `existing`. |
| `strict` | `bool` | If `true`, an error is raised if any column in `existing` is not found in the schema. If `false`, columns that are not found are silently ignored. Default is `true`. |

**Returns:** A new `LazyFrame` with the rename operation applied.

---

### Reverse

Reverse the LazyFrame.

```csharp
public LazyFrame Reverse()
```

---

### Rolling

Lazily group based on a time index using rolling windows.

```csharp
public LazyGroupBy Rolling(
    IntoSelector indexColumn,
    IntoDuration period,
    IntoDuration? offset = null,
    IEnumerable<IntoExprColumn>? groupBy = null,
    ClosedInterval ClosedInterval = ClosedInterval.Right
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `indexColumn` | `IntoSelector` | - |
| `period` | `IntoDuration` | - |
| `offset` | `IntoDuration?` | - |
| `groupBy` | `IEnumerable<IntoExprColumn>?` | - |
| `ClosedInterval` | `ClosedInterval` | - |

---

### Select

Select columns from the LazyFrame. Accepts Expr, Selector, or string column names.

Bridge overload to support IEnumerable IntoExpr exprs. Usage: lf.Select(["Id", "Name", "Date"])

Bridge overload to support C# 12 collection expressions. Usage: lf.Select(["Id", "Name", "Date"])

Select columns from the LazyFrame by column names.

Bridge overload to support dynamic LINQ generation of Expressions. Usage: lf.Select(myColumns.Select(c =&gt; Pl.Col(c) * 2))

```csharp
public LazyFrame Select(params IntoExprColumn[] exprs)
public LazyFrame Select(IEnumerable<IntoExprColumn> exprs)
public LazyFrame Select(IEnumerable<string> columns)
public LazyFrame Select(params string[] columns)
public LazyFrame Select(IEnumerable<Expr> exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IntoExprColumn[] / IEnumerable<IntoExprColumn> / IEnumerable<Expr>` | - |
| `columns` | `IEnumerable<string> / string[]` | - |

**Example**

```csharp
// Select "a" and calculate "b" * 2
lf.Select(Col("a"), (Col("b") * 2).Alias("b_double"));
```

---

### SetSorted

Mark one or multiple columns as sorted. This is an optimizer hint and will not actually execute a sorting operation.

```csharp
public LazyFrame SetSorted(IntoSelector columns, bool descending = false, bool nullsLast = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `IntoSelector` | The columns to mark as sorted. Accepts strings, arrays, or Selectors (e.g. Cs.Temporal()). |
| `descending` | `bool` | Whether the columns are sorted in descending order. |
| `nullsLast` | `bool` | Whether null values appear last. |

**Returns:** A new Frame with the sorted hints applied to the query plan.

---

### Show

Print the LazyFrame to Console.

```csharp
public void Show()
```

---

### SinkBatches

Streaming Sink to Batchs

```csharp
public void SinkBatches(Action<RecordBatch> onBatchReceived)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `onBatchReceived` | `Action<RecordBatch>` | - |

---

### SinkCatalogTable

Sinks the `LazyFrame` to a Unity Catalog table.

```csharp
public void SinkCatalogTable(
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
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `catalog` | `UnityCatalog` | - |
| `catalogName` | `string` | The name of the catalog (e.g., "main"). |
| `schemaName` | `string` | The name of the schema/database (e.g., "default"). |
| `tableName` | `string` | The name of the table to sink. |
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

### SinkCsv

Execute the LazyFrame and sink the result to a CSV file. 

This operation allows processing datasets larger than memory by streaming results directly to the file system.

```csharp
public void SinkCsv(
    string path,
    bool includeHeader = true,
    bool includeBom = false,
    char separator = ',',
    char quoteChar = '"',
    QuoteStyle quoteStyle = QuoteStyle.Necessary,
    string? nullValue = null,
    string? lineTerminator = "\n",
    bool? floatScientific = null,
    int? floatPrecision = null,
    bool decimalComma = false,
    string? dateFormat = null,
    string? timeFormat = null,
    string? datetimeFormat = null,
    bool checkExtension = true,
    ExternalCompression compression = ExternalCompression.Uncompressed,
    int compressionLevel = -1,
    bool maintainOrder = true,
    SyncOnClose syncOnClose = SyncOnClose.None,
    bool mkdir = false,
    int batchSize = 0,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | The output file path. |
| `includeHeader` | `bool` | Whether to include the header row. Defaults to true. |
| `includeBom` | `bool` | Whether to include the UTF-8 Byte Order Mark (BOM). Defaults to false. |
| `separator` | `char` | The character used as a field separator. Defaults to ','. |
| `quoteChar` | `char` | The character used for quoting fields. Defaults to '"'. |
| `quoteStyle` | `QuoteStyle` | The quoting style to use. Defaults to `QuoteStyle.Necessary`. |
| `nullValue` | `string?` | The string representation for null values. Defaults to empty string. |
| `lineTerminator` | `string?` | The character sequence used to terminate lines. Defaults to "\n". |
| `floatScientific` | `bool?` | Whether to always use scientific notation for floats. If null (default), formatting is automatic. |
| `floatPrecision` | `int?` | The number of decimal places to write for floats. If null (default), uses full precision. |
| `decimalComma` | `bool` | Whether to use a comma ',' as the decimal separator. Defaults to false. |
| `dateFormat` | `string?` | Format string for Date columns. If null, uses ISO 8601. |
| `timeFormat` | `string?` | Format string for Time columns. If null, uses ISO 8601. |
| `datetimeFormat` | `string?` | Format string for Datetime columns. If null, uses ISO 8601. |
| `checkExtension` | `bool` | Whether to check if the file extension matches '.csv'. Defaults to true. |
| `compression` | `ExternalCompression` | Compression method (Gzip/Zstd). Defaults to None. |
| `compressionLevel` | `int` | Compression level (depends on the codec). -1 for default. |
| `maintainOrder` | `bool` | Whether to maintain the order of the data. Setting this to false can improve performance in streaming mode. Defaults to true. |
| `syncOnClose` | `SyncOnClose` | File synchronization behavior on close (e.g., flush to disk). Defaults to None. |
| `mkdir` | `bool` | Recursively create the output directory if it does not exist. Defaults to false. |
| `batchSize` | `int` | The batch size for writing rows. 0 means use the Polars default. |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage (AWS S3, Azure Blob, GCS, etc.). |

---

### SinkCsvMemory

Sink the LazyFrame to a CSV format in memory. 

This allows for streaming execution directly into a byte array without writing to disk.

```csharp
public byte[] SinkCsvMemory(
    bool includeBom = false,
    bool includeHeader = true,
    int batchSize = 1024,
    bool checkExtension = false,
    ExternalCompression compressionCode = ExternalCompression.Uncompressed,
    int compressionLevel = 0,
    string? dateFormat = null,
    string? timeFormat = null,
    string? datetimeFormat = null,
    int floatScientific = -1,
    int floatPrecision = -1,
    bool decimalComma = false,
    byte separator = 44,
    byte quoteChar = 34,
    string? nullValue = null,
    string? lineTerminator = "\n",
    QuoteStyle quoteStyle = QuoteStyle.Necessary,
    bool maintainOrder = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `includeBom` | `bool` | - |
| `includeHeader` | `bool` | - |
| `batchSize` | `int` | - |
| `checkExtension` | `bool` | - |
| `compressionCode` | `ExternalCompression` | - |
| `compressionLevel` | `int` | - |
| `dateFormat` | `string?` | - |
| `timeFormat` | `string?` | - |
| `datetimeFormat` | `string?` | - |
| `floatScientific` | `int` | - |
| `floatPrecision` | `int` | - |
| `decimalComma` | `bool` | - |
| `separator` | `byte` | - |
| `quoteChar` | `byte` | - |
| `nullValue` | `string?` | - |
| `lineTerminator` | `string?` | - |
| `quoteStyle` | `QuoteStyle` | - |
| `maintainOrder` | `bool` | - |

---

### SinkCsvPartitioned

Execute the LazyFrame and sink the result to a CSV file. 

This operation allows processing datasets larger than memory by streaming results directly to the file system.

```csharp
public void SinkCsvPartitioned(
    string path,
    IntoSelector partitionBy,
    bool includeKeys = true,
    bool keysPreGrouped = false,
    int maxRowsPerFile = 0,
    long approxBytesPerFile = 0,
    bool includeHeader = true,
    bool includeBom = false,
    char separator = ',',
    char quoteChar = '"',
    QuoteStyle quoteStyle = QuoteStyle.Necessary,
    string? nullValue = null,
    string? lineTerminator = "\n",
    bool? floatScientific = null,
    int? floatPrecision = null,
    bool decimalComma = false,
    string? dateFormat = null,
    string? timeFormat = null,
    string? datetimeFormat = null,
    bool checkExtension = true,
    ExternalCompression compression = ExternalCompression.Uncompressed,
    int compressionLevel = -1,
    bool maintainOrder = true,
    SyncOnClose syncOnClose = SyncOnClose.None,
    bool mkdir = false,
    int batchSize = 0,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | The output file path. |
| `partitionBy` | `IntoSelector` | The selector(s) to partition the data by. |
| `includeKeys` | `bool` | Whether to include the partition keys in the output files. |
| `keysPreGrouped` | `bool` | Assert that the keys are already pre-grouped. This can speed up the operation if true. Use with caution: if the data is not grouped, the output may be incorrect. |
| `maxRowsPerFile` | `int` | Maximum number of rows per file. 0 means no limit. |
| `approxBytesPerFile` | `long` | Approximate size in bytes per file. 0 means no limit. |
| `includeHeader` | `bool` | Whether to include the header row. Defaults to true. |
| `includeBom` | `bool` | Whether to include the UTF-8 Byte Order Mark (BOM). Defaults to false. |
| `separator` | `char` | The character used as a field separator. Defaults to ','. |
| `quoteChar` | `char` | The character used for quoting fields. Defaults to '"'. |
| `quoteStyle` | `QuoteStyle` | The quoting style to use. Defaults to `QuoteStyle.Necessary`. |
| `nullValue` | `string?` | The string representation for null values. Defaults to empty string. |
| `lineTerminator` | `string?` | The character sequence used to terminate lines. Defaults to "\n". |
| `floatScientific` | `bool?` | Whether to always use scientific notation for floats. If null (default), formatting is automatic. |
| `floatPrecision` | `int?` | The number of decimal places to write for floats. If null (default), uses full precision. |
| `decimalComma` | `bool` | Whether to use a comma ',' as the decimal separator. Defaults to false. |
| `dateFormat` | `string?` | Format string for Date columns. If null, uses ISO 8601. |
| `timeFormat` | `string?` | Format string for Time columns. If null, uses ISO 8601. |
| `datetimeFormat` | `string?` | Format string for Datetime columns. If null, uses ISO 8601. |
| `checkExtension` | `bool` | Whether to check if the file extension matches '.csv'. Defaults to true. |
| `compression` | `ExternalCompression` | Compression method (Gzip/Zstd). Defaults to None. |
| `compressionLevel` | `int` | Compression level (depends on the codec). -1 for default. |
| `maintainOrder` | `bool` | Whether to maintain the order of the data. Setting this to false can improve performance in streaming mode. Defaults to true. |
| `syncOnClose` | `SyncOnClose` | File synchronization behavior on close (e.g., flush to disk). Defaults to None. |
| `mkdir` | `bool` | Recursively create the output directory if it does not exist. Defaults to false. |
| `batchSize` | `int` | The batch size for writing rows. 0 means use the Polars default. |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage (AWS S3, Azure Blob, GCS, etc.). |

---

### SinkDelta

Sink the LazyFrame to a Delta Lake table with partition discovery. 

This operation performs a "blind write" of partitioned Parquet files (Hive-style) and then commits a transaction to the Delta Log, registering the new files.

```csharp
public void SinkDelta(
    string path,
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
| `path` | `string` | Path to the root of the Delta Table. Can be local (e.g. "./data/table") or remote (e.g. "s3://bucket/table"). |
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

### SinkIpc

Sink the LazyFrame to an IPC (Arrow) file. 

This allows for streaming execution.

```csharp
public void SinkIpc(
    string path,
    IpcCompression compression = IpcCompression.None,
    int compatLevel = -1,
    int recordBatchSize = 0,
    bool recordBatchStatistics = true,
    bool maintainOrder = true,
    SyncOnClose syncOnClose = SyncOnClose.None,
    bool mkdir = false,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Path to the output file. |
| `compression` | `IpcCompression` | Compression method to use. |
| `compatLevel` | `int` | Compatibility level (default -1 = newest). |
| `recordBatchSize` | `int` | Number of rows per record batch (0 = default). |
| `recordBatchStatistics` | `bool` | Write statistics to the record batch header (default = true). |
| `maintainOrder` | `bool` | Maintain the order of the data. |
| `syncOnClose` | `SyncOnClose` | Whether to sync the file to disk on close. |
| `mkdir` | `bool` | Create parent directories if they don't exist (Local file system only). |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage. |

---

### SinkIpcMemory

Sink the LazyFrame to an IPC (Arrow) format in memory. 

This allows for streaming execution directly into a byte array without writing to disk.

```csharp
public byte[] SinkIpcMemory(
    IpcCompression compression = IpcCompression.None,
    int compatLevel = -1,
    int recordBatchSize = 0,
    bool recordBatchStatistics = true,
    bool maintainOrder = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `compression` | `IpcCompression` | Compression method to use. |
| `compatLevel` | `int` | Compatibility level (default -1 = newest). |
| `recordBatchSize` | `int` | Number of rows per record batch (0 = default). |
| `recordBatchStatistics` | `bool` | Write statistics to the record batch header (default = true). |
| `maintainOrder` | `bool` | Maintain the order of the data. |

**Returns:** A byte array containing the serialized IPC data.

---

### SinkIpcPartitioned

Sink the LazyFrame to an IPC (Arrow) file. 

This allows for streaming execution.

```csharp
public void SinkIpcPartitioned(
    string path,
    IntoSelector partitionBy,
    bool includeKeys = true,
    bool keysPreGrouped = false,
    int maxRowsPerFile = 0,
    long approxBytesPerFile = 0,
    IpcCompression compression = IpcCompression.None,
    int compatLevel = -1,
    int recordBatchSize = 0,
    bool recordBatchStatistics = true,
    bool maintainOrder = true,
    SyncOnClose syncOnClose = SyncOnClose.None,
    bool mkdir = false,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Path to the output file. |
| `partitionBy` | `IntoSelector` | The selector(s) to partition the data by. |
| `includeKeys` | `bool` | Whether to include the partition keys in the output files. |
| `keysPreGrouped` | `bool` | Assert that the keys are already pre-grouped. This can speed up the operation if true. Use with caution: if the data is not grouped, the output may be incorrect. |
| `maxRowsPerFile` | `int` | Maximum number of rows per file. 0 means no limit. |
| `approxBytesPerFile` | `long` | Approximate size in bytes per file. 0 means no limit. |
| `compression` | `IpcCompression` | Compression method to use. |
| `compatLevel` | `int` | Compatibility level (default -1 = newest). |
| `recordBatchSize` | `int` | Number of rows per record batch (0 = default). |
| `recordBatchStatistics` | `bool` | Write statistics to the record batch header (default = true). |
| `maintainOrder` | `bool` | Maintain the order of the data. |
| `syncOnClose` | `SyncOnClose` | Whether to sync the file to disk on close. |
| `mkdir` | `bool` | Create parent directories if they don't exist (Local file system only). |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage. |

---

### SinkJson

Sink the LazyFrame to a NDJSON (Newline Delimited JSON) file.

```csharp
public void SinkJson(
    string path,
    ExternalCompression compression = ExternalCompression.Uncompressed,
    int compressionLevel = -1,
    bool checkExtension = true,
    bool maintainOrder = true,
    SyncOnClose syncOnClose = SyncOnClose.None,
    bool mkdir = false,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Output file path. |
| `compression` | `ExternalCompression` | Compression method (Gzip/Zstd). |
| `compressionLevel` | `int` | Compression level. |
| `checkExtension` | `bool` | Whether to check if the file extension matches '.json' or '.ndjson'. |
| `maintainOrder` | `bool` | Maintain the order of data. |
| `syncOnClose` | `SyncOnClose` | Sync to disk on close. |
| `mkdir` | `bool` | Create parent directories. |
| `cloudOptions` | `CloudOptions?` | Cloud storage options. |

---

### SinkJsonMemory

Sink the LazyFrame to a NDJSON (Newline Delimited JSON) format in memory.

```csharp
public byte[] SinkJsonMemory(
    ExternalCompression compression = ExternalCompression.Uncompressed,
    int compressionLevel = -1,
    bool checkExtension = true,
    bool maintainOrder = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `compression` | `ExternalCompression` | - |
| `compressionLevel` | `int` | - |
| `checkExtension` | `bool` | - |
| `maintainOrder` | `bool` | - |

---

### SinkJsonPartitioned

Sink the LazyFrame to a NDJSON (Newline Delimited JSON) file.

```csharp
public void SinkJsonPartitioned(
    string path,
    IntoSelector partitionBy,
    bool includeKeys = true,
    bool keysPreGrouped = false,
    int maxRowsPerFile = 0,
    long approxBytesPerFile = 0,
    ExternalCompression compression = ExternalCompression.Uncompressed,
    int compressionLevel = -1,
    bool checkExtension = true,
    bool maintainOrder = true,
    SyncOnClose syncOnClose = SyncOnClose.None,
    bool mkdir = false,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Output file path. |
| `partitionBy` | `IntoSelector` | The selector(s) to partition the data by. |
| `includeKeys` | `bool` | Whether to include the partition keys in the output files. |
| `keysPreGrouped` | `bool` | Assert that the keys are already pre-grouped. This can speed up the operation if true. Use with caution: if the data is not grouped, the output may be incorrect. |
| `maxRowsPerFile` | `int` | Maximum number of rows per file. 0 means no limit. |
| `approxBytesPerFile` | `long` | Approximate size in bytes per file. 0 means no limit. |
| `compression` | `ExternalCompression` | Compression method (Gzip/Zstd). |
| `compressionLevel` | `int` | Compression level. |
| `checkExtension` | `bool` | Whether to check if the file extension matches '.json' or '.ndjson'. |
| `maintainOrder` | `bool` | Maintain the order of data. |
| `syncOnClose` | `SyncOnClose` | Sync to disk on close. |
| `mkdir` | `bool` | Create parent directories. |
| `cloudOptions` | `CloudOptions?` | Cloud storage options. |

---

### SinkNdJson

Alias for SinkJson with format=JsonLines.

```csharp
public void SinkNdJson(
    string path,
    ExternalCompression compression = ExternalCompression.Uncompressed,
    int compressionLevel = -1,
    bool checkExtension = true,
    bool maintainOrder = true,
    SyncOnClose syncOnClose = SyncOnClose.None,
    bool mkdir = false,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | - |
| `compression` | `ExternalCompression` | - |
| `compressionLevel` | `int` | - |
| `checkExtension` | `bool` | - |
| `maintainOrder` | `bool` | - |
| `syncOnClose` | `SyncOnClose` | - |
| `mkdir` | `bool` | - |
| `cloudOptions` | `CloudOptions?` | - |

---

### SinkParquet

Sink the LazyFrame to a Parquet file. 

This allows for streaming execution, processing the data in chunks and writing it to the file without loading the entire dataset into memory.

```csharp
public void SinkParquet(
    string path,
    ParquetCompression compression = ParquetCompression.Snappy,
    int compressionLevel = -1,
    bool statistics = false,
    int rowGroupSize = 0,
    int dataPageSize = 0,
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
| `path` | `string` | Path to the output file. |
| `compression` | `ParquetCompression` | Compression codec to use. |
| `compressionLevel` | `int` | Compression level (depends on the codec). |
| `statistics` | `bool` | Write statistics to the parquet file. |
| `rowGroupSize` | `int` | Target row group size (in rows). |
| `dataPageSize` | `int` | Target data page size (in bytes). |
| `compatLevel` | `int` | IPC format compatibility, -1: oldest, 0: default, 1: newest. |
| `maintainOrder` | `bool` | Maintain the order of the data. |
| `syncOnClose` | `SyncOnClose` | Whether to sync the file to disk on close. |
| `mkdir` | `bool` | Create parent directories if they don't exist (Local file system only). |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage (AWS S3, Azure Blob, GCS, etc.). |

---

### SinkParquetMemory

Sink the LazyFrame to a Parquet format in memory. 

This allows for streaming execution directly into a byte array without writing to disk.

```csharp
public byte[] SinkParquetMemory(
    ParquetCompression compression = ParquetCompression.ZSTD,
    int compressionLevel = 3,
    bool statistics = true,
    int rowGroupSize = 0,
    int dataPageSize = 0,
    int compatLevel = -1,
    bool maintainOrder = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `compression` | `ParquetCompression` | - |
| `compressionLevel` | `int` | - |
| `statistics` | `bool` | - |
| `rowGroupSize` | `int` | - |
| `dataPageSize` | `int` | - |
| `compatLevel` | `int` | - |
| `maintainOrder` | `bool` | - |

---

### SinkParquetPartitioned

Sink the LazyFrame to a set of Parquet files, partitioned by the specified selector. 

This writes the dataset to a directory, splitting the data into multiple files based on the partition key(s) defined in `partitionBy`.

```csharp
public void SinkParquetPartitioned(
    string path,
    IntoSelector partitionBy,
    bool includeKeys = true,
    bool keysPreGrouped = false,
    int maxRowsPerFile = 0,
    long approxBytesPerFile = 0,
    ParquetCompression compression = ParquetCompression.Snappy,
    int compressionLevel = -1,
    bool statistics = false,
    int rowGroupSize = 0,
    int dataPageSize = 0,
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
| `path` | `string` | Base path to the output directory. |
| `partitionBy` | `IntoSelector` | The selector(s) to partition the data by. |
| `includeKeys` | `bool` | Whether to include the partition keys in the output files. |
| `keysPreGrouped` | `bool` | Assert that the keys are already pre-grouped. This can speed up the operation if true. Use with caution: if the data is not grouped, the output may be incorrect. |
| `maxRowsPerFile` | `int` | Maximum number of rows per file. 0 means no limit. |
| `approxBytesPerFile` | `long` | Approximate size in bytes per file. 0 means no limit. |
| `compression` | `ParquetCompression` | Compression codec to use. |
| `compressionLevel` | `int` | Compression level (depends on the codec). |
| `statistics` | `bool` | Write statistics to the parquet file. |
| `rowGroupSize` | `int` | Target row group size (in rows). |
| `dataPageSize` | `int` | Target data page size (in bytes). |
| `compatLevel` | `int` | IPC format compatibility, -1: oldest, 0: default, 1: newest. |
| `maintainOrder` | `bool` | Maintain the order of the data. |
| `syncOnClose` | `SyncOnClose` | Whether to sync the file to disk on close. |
| `mkdir` | `bool` | Create parent directories if they don't exist (Local file system only). |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage (AWS S3, Azure Blob, GCS, etc.). |

---

### SinkTo

Stream the result of the LazyFrame calculation into an `System.Data.IDataReader`. 

This allows processing huge datasets that don't fit in memory by handling them in chunks (RecordBatches).

Common use cases: 

- Bulk inserting data into SQL Databases (using SqlBulkCopy or NpgsqlBinaryImporter).
- Streaming data to other .NET libraries that consume IDataReader.

```csharp
public void SinkTo(Action<IDataReader> writerAction, int bufferSize = 5, Dictionary<string, Type>? typeOverrides = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `writerAction` | `Action<IDataReader>` | A callback that receives the `System.Data.IDataReader`. This action executes on a separate thread (Consumer) while the Polars engine (Producer) pumps data. |
| `bufferSize` | `int` | The number of RecordBatches to buffer in memory. If the buffer is full, the Polars engine will pause until the consumer reads more data (Backpressure). |
| `typeOverrides` | `Dictionary<string, Type>?` | Optional schema overrides to guide the type mapping. |

**Example**

```csharp
// Simulate a large lazy computation
var lf = DataFrame.FromColumns(new { id = new[] { 0, 1, 2, 3, 4 } }).Lazy();

// Stream result to a database writer (simulated here)
lf.SinkTo(reader => 
{
    Console.WriteLine("[DB Writer] Started receiving data...");
    while (reader.Read())
    {
        var val = reader.GetValue(0);
        Console.WriteLine($"[DB Writer] Insert row: {val}");
    }
    Console.WriteLine("[DB Writer] Done.");
}, bufferSize: 2);

/* Output:
[DB Writer] Started receiving data...
[DB Writer] Insert row: 0
[DB Writer] Insert row: 1
...
[DB Writer] Done.
*/
```

---

### Slice

Slice the LazyFrame. 

This operation is lazy; it only affects the query plan.

```csharp
public LazyFrame Slice(long offset, uint? length = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `offset` | `long` | Start index. Negative values count from the end. |
| `length` | `uint?` | Number of rows to return. |

---

### Sort

Sort the LazyFrame by a single column name, selector, or column expression.

Lazily sort the DataFrame by multiple columns. 

This operation is added to the logical plan. Use `LazyFrame.TopK` if you only need the top/bottom N rows, as it is more efficient.

Sort the LazyFrame using specific sorting configurations for each column/selector.

```csharp
public LazyFrame Sort(
    IntoSelector by,
    bool descending = false,
    bool nullsLast = false,
    bool maintainOrder = false
)

public LazyFrame Sort(
    IEnumerable<IntoSelector> by,
    bool[]? descending = null,
    bool[]? nullsLast = null,
    bool maintainOrder = false
)

public LazyFrame Sort(params (IntoSelector, bool)[] sortConfigs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IntoSelector / IEnumerable<IntoSelector>` | Names of the columns to sort by. |
| `descending` | `bool / bool[]?` | Sort order for each column. |
| `nullsLast` | `bool / bool[]?` | Whether nulls go last for each column. |
| `maintainOrder` | `bool` | Whether to maintain the relative order of rows with equal keys. |
| `sortConfigs` | `(IntoSelector, bool)[]` | - |

**Example**

```csharp
df.Lazy()
  .Sort(
      by: new[] { "group", "val" }, 
      descending: new[] { false, true }, 
      nullsLast: new[] { false, false }
  )
  .Collect();
/* Output:
shape: (5, 2)
┌───────┬─────┐
│ group ┆ val │
│ ---   ┆ --- │
│ str   ┆ i32 │
╞═══════╪═════╡
│ A     ┆ 10  │
│ A     ┆ 8   │
│ ...   ┆ ... │
└───────┴─────┘
*/
```

---

### Std

Aggregate the columns in the Frame to their standard deviation value.

```csharp
public LazyFrame Std(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1. |

---

### Sum

Aggregate the columns in the Frame to their sum value.

```csharp
public LazyFrame Sum()
```

---

### Tail

Get the last n rows.

```csharp
public LazyFrame Tail(uint n = 5)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `uint` | Number of rows to return. |

---

### ToAsyncEnumerable

Executes the LazyFrame via native streaming batch engine,

```csharp
public IAsyncEnumerable<T> ToAsyncEnumerable<T>(
    long chunkSize = 0,
    Engine engine = Engine.Auto,
    bool maintainOrder = false,
    CancellationToken cancellationToken = default
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `chunkSize` | `long` | Maximum row count per native batch (0 uses Polars default batch size). |
| `engine` | `Engine` | Execution engine selection. |
| `maintainOrder` | `bool` | Whether to enforce deterministic row ordering across batches. |
| `cancellationToken` | `CancellationToken` | Optional cooperative cancellation token. |

**Returns:** An asynchronous stream of hydrated row objects.

---

### ToAsyncEnumerableWithBackpressure

Executes the LazyFrame and hydrates rows via a bounded channel pipeline,

```csharp
public IAsyncEnumerable<T> ToAsyncEnumerableWithBackpressure<T>(
    int boundedCapacity = 20000,
    long chunkSize = 0,
    Engine engine = Engine.Auto,
    bool maintainOrder = false,
    CancellationToken cancellationToken = default
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `boundedCapacity` | `int` | Maximum capacity of the intermediate buffer channel. |
| `chunkSize` | `long` | Maximum row count per native batch (0 uses Polars default batch size). |
| `engine` | `Engine` | Execution engine selection. |
| `maintainOrder` | `bool` | Whether to enforce deterministic row ordering across batches. |
| `cancellationToken` | `CancellationToken` | Optional cooperative cancellation token. |

**Returns:** An asynchronous stream of hydrated row objects with backpressure.

---

### ToBatchAsyncEnumerable

Streams materialized DataFrame batches asynchronously from a LazyFrame. Each batch is emitted as soon as computed by the native streaming engine.

```csharp
public IAsyncEnumerable<DataFrame> ToBatchAsyncEnumerable(
    long chunkSize = 0,
    Engine engine = Engine.Auto,
    bool maintainOrder = false,
    CancellationToken cancellationToken = default
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `chunkSize` | `long` | Maximum row count per native batch (0 uses Polars default batch size). |
| `engine` | `Engine` | Execution engine selection. |
| `maintainOrder` | `bool` | Whether to enforce deterministic row ordering across batches. |
| `cancellationToken` | `CancellationToken` | Optional cooperative cancellation token. |

**Returns:** An asynchronous stream of DataFrame batches.

---

### ToBatchAsyncEnumerableWithBackpressure

Streams DataFrame batches via a bounded channel pipeline, overlapping batch execution and consumption while bounding in-flight batch count.

```csharp
public IAsyncEnumerable<DataFrame> ToBatchAsyncEnumerableWithBackpressure(
    int boundedCapacity = 16,
    long chunkSize = 0,
    Engine engine = Engine.Auto,
    bool maintainOrder = false,
    CancellationToken cancellationToken = default
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `boundedCapacity` | `int` | Maximum number of in-flight DataFrame batches buffered. |
| `chunkSize` | `long` | Maximum row count per native batch (0 uses Polars default batch size). |
| `engine` | `Engine` | Execution engine selection. |
| `maintainOrder` | `bool` | Whether to enforce deterministic row ordering across batches. |
| `cancellationToken` | `CancellationToken` | Optional cooperative cancellation token. |

**Returns:** An asynchronous stream of DataFrame batches with backpressure.

---

### ToHtml

Generates an HTML visualization of the LazyFrame execution plan and schema for LINQPad.

```csharp
public string ToHtml(bool optimized = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `optimized` | `bool` | Whether to explain the optimized logical plan. |

**Returns:** An HTML representation formatted with monospace styling.

---

### TopK

Get the top k rows according to a single expression, string, or selector. Usage: lf.TopK(5, "Sales") or lf.TopK(10, Pl.Col("Age") * 2, reverse: true)

Get the top k rows according to multiple column names with a uniform reverse setting. Usage: lf.TopK(5, ["Score", "Age"], reverse: false)

Get the top k rows with specific reverse directions using Tuples. Usage: lf.TopK(5, ("Score", false), ("Age", true), (Cs.Numeric(), false))

Master method: Get the top k rows by multiple Expressions with optional parallel boolean arrays. Usage: lf.TopK(10, myColumns.Select(c =&gt; Pl.Col(c)))

```csharp
public LazyFrame TopK(int k, IntoExprColumn by, bool reverse = false)
public LazyFrame TopK(int k, IEnumerable<string> by, bool reverse = false)
public LazyFrame TopK(int k, params (IntoExprColumn, bool)[] configs)
public LazyFrame TopK(int k, IEnumerable<Expr> by, bool[]? reverse = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `k` | `int` | - |
| `by` | `IntoExprColumn / IEnumerable<string> / IEnumerable<Expr>` | - |
| `reverse` | `bool / bool[]?` | - |
| `configs` | `(IntoExprColumn, bool)[]` | - |

---

### Unique

Keep unique rows (stable) based on a subset of columns defined by a Selector.

Keep unique rows based on specific column names.

Keep unique rows based on a subset of columns (Selector, strings, Types, etc.).

```csharp
public LazyFrame Unique(Selector? subset = null, UniqueKeepStrategy keep = UniqueKeepStrategy.First, bool maintainOrder = false)
public LazyFrame Unique(IEnumerable<string> columns, UniqueKeepStrategy keep = UniqueKeepStrategy.First, bool maintainOrder = false)
public LazyFrame Unique(IntoSelector subset, UniqueKeepStrategy keep = UniqueKeepStrategy.First, bool maintainOrder = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `subset` | `Selector? / IntoSelector` | Selector defining the subset of columns. If null, uses all columns. |
| `keep` | `UniqueKeepStrategy` | Strategy to keep duplicates (First, Last, Any, None). |
| `maintainOrder` | `bool` | Whether to maintain the original order of the rows (stable). |
| `columns` | `IEnumerable<string>` | A collection of column names to group by. |

---

### Unnest

Lazily unnest struct columns using a Selector, String, DataType, or Expr. Usage: lf.Unnest("User") or lf.Unnest("User", separator: "_")

Lazily unnest specific struct columns. Bridge overload to support C# 12 collection expressions. Usage: lf.Unnest(["User", "Profile"]) or lf.Unnest(["User", "Profile"], separator: "_")

```csharp
public LazyFrame Unnest(IntoSelector columns, string? separator = null)
public LazyFrame Unnest(IEnumerable<string> columns, string? separator = null)
public LazyFrame Unnest(params string[] columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `IntoSelector / IEnumerable<string> / string[]` | - |
| `separator` | `string?` | - |

---

### Unpivot

Unpivot (Melt) the LazyFrame from wide to long format. Supports mixing Selectors, Expressions, Types, or single Strings. Usage: lf.Unpivot("Date", Cs.Numeric()) or lf.Unpivot(Cs.StartsWith("id"), "Status")

Bridge overload to support C# 12 collection expressions. Usage: lf.Unpivot(["Date", "Region"], ["Q1", "Q2", "Q3"])

```csharp
public LazyFrame Unpivot(
    IntoSelector index,
    IntoSelector? on = null,
    string variableName = "variable",
    string valueName = "value"
)

public LazyFrame Unpivot(
    IEnumerable<string> index,
    IEnumerable<string>? on = null,
    string variableName = "variable",
    string valueName = "value"
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `IntoSelector / IEnumerable<string>` | - |
| `on` | `IntoSelector? / IEnumerable<string>?` | - |
| `variableName` | `string` | - |
| `valueName` | `string` | - |

---

### Update

Update the values in this Frame with the values in other.

```csharp
public LazyFrame Update(
    LazyFrame other,
    IEnumerable<string>? on = null,
    JoinType how = JoinType.Left,
    IEnumerable<string>? leftOn = null,
    IEnumerable<string>? rightOn = null,
    bool includeNulls = false,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `LazyFrame` | Frame that will be used to update the values |
| `on` | `IEnumerable<string>?` | Column names that will be joined on. If set to Null (default), the implicit row index of each frame is used as a join key. |
| `how` | `JoinType` | 'Left' will keep all rows from the left table; rows may be duplicated if multiple rows in the right frame match the left row’s key.<br><br>‘Inner’: keeps only those rows where the key exists in both frames.<br> ‘Full’: will update existing rows where the key matches while also adding any new rows contained in the given frame. |
| `leftOn` | `IEnumerable<string>?` | Join column(s) of the left Frame. |
| `rightOn` | `IEnumerable<string>?` | Join column(s) of the right Frame. |
| `includeNulls` | `bool` | Overwrite values in the left frame with null values from the right frame. If set to False (default), null values in the right frame are ignored. |
| `maintainOrder` | `JoinMaintainOrder` | Which order of rows from the inputs to preserve. See join() for details. Unlike join this function preserves the left order by default. |

---

### Var

Aggregate the columns in the Frame to their variance value.

```csharp
public LazyFrame Var(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1. |

---

### WithColumns

Add or modify columns based on Expressions, Strings, Selectors, or Series.

Add or overwrite multiple columns from a collection of Expressions.

Add or overwrite multiple columns from a collection of mixed IntoExpr types.

Add or overwrite multiple columns from a collection of string column names.

```csharp
public LazyFrame WithColumns(params IntoExprColumn[] exprs)
public LazyFrame WithColumns(IEnumerable<Expr> exprs)
public LazyFrame WithColumns(IEnumerable<IntoExprColumn> exprs)
public LazyFrame WithColumns(IEnumerable<string> columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IntoExprColumn[] / IEnumerable<Expr> / IEnumerable<IntoExprColumn>` | - |
| `columns` | `IEnumerable<string>` | - |

**Example**

```csharp
// Add a new column "c", overwrite with a Series, and select a string column!
lf.WithColumns(Pl.Col("a") + 1, mySeries, "ExistingCol", Cs.Numeric() * 2);
```

---

### WithRowIndex

Add a column at index 0 that counts the rows. 

This can be useful to generate a unique index or to maintain order in operations that would otherwise discard it.

```csharp
public LazyFrame WithRowIndex(string name = "index", int? offset = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | The name of the new row index column. Defaults to "index". |
| `offset` | `int?` | The starting value of the row index. Defaults to 0. |

**Returns:** A new LazyFrame with the row index column added.

---

