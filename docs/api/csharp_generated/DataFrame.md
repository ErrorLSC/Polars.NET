---
uid: Polars.CSharp.DataFrame
title: "DataFrame"
---

# DataFrame

Namespace: `Polars.CSharp`

DataFrame represents a 2-dimensional labeled data structure similar to a table or spreadsheet.

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `ColumnNames` | `string[]` | Get column names in order. |
| `Columns` | `string[]` | Return DataFrame Columns' Name |
| `DataTypes` | `IReadOnlyList<DataType>` | Return DataFrame Columns' DataType |
| `Height` | `long` | Return DataFrame Height |
| `IsEmpty` | `bool` | True if the DataFrame contains no rows. |
| `IsVoid` | `bool` | True if the DataFrame has no columns and no rows (0x0). Represents a completely uninitialized or structurally void DataFrame. |
| `Len` | `long` | Return DataFrame Height |
| `Schema` | `PolarsSchema` | Gets the Schema of the DataFrame. Returns a disposable PolarsSchema object. |
| `Shape` | `(long, long)` | Return DataFrame Shape(Len,Width) |
| `Width` | `long` | Return DataFrame Width |
| `this[Index rowIndex, IntoSelector columnSelector]` | `DataFrame` | e.g. df[^2, Cs.Numeric()], df[5, typeof(double)], df[0, Pl.Col("Id")] |
| `this[Index rowIndex, string colName]` | `object` | e.g. df[^1, "A"] |
| `this[Index rowIndex, string[] columnNames]` | `DataFrame` | e.g. df[0, ["A", "B"]], df[^1, ["A", "B"]] |
| `this[IntoSelector columnSelector]` | `DataFrame` | e.g. df[Cs.Numeric()], df[typeof(int)], df[Pl.Col("Id")] |
| `this[Range rowRange, IntoSelector columnSelector]` | `DataFrame` | e.g. df[1..10, Cs.Numeric()], df[..^1, typeof(int)], df[1..5, "A"], df[0..2, Pl.Col("Id")] |
| `this[Range rowRange, string columnName]` | `Series` | e.g. df[1..10, "A"] |
| `this[Range rowRange, string[] columnNames]` | `DataFrame` | e.g. df[1..10, ["A", "B"]] |
| `this[Range rowRange]` | `DataFrame` | e.g. df[1..5] / df[..^1] |
| `this[Series maskOrIndices, IntoSelector columnSelector]` | `DataFrame` | e.g. df[df["Age"] &gt; 18, Cs.Numeric()] e.g. df[Pl.Series("index", [1, 3, 5]), Cs.Numeric()] |
| `this[Series maskOrIndices, string columnName]` | `Series` | e.g. var adultNames = df[df["Age"] &gt;= 18, "Name"]; e.g. var specificNames = df[Polars.Series(new[] { 1, 3, 5 }), "Name"]; |
| `this[Series maskOrIndices, string[] columnNames]` | `DataFrame` | e.g. df[df["Age"] &gt; 18, ["Name", "Score"]] e.g. df[Polars.Series(new[] { 1, 3, 5 }), ["Name", "Score"]] |
| `this[Series maskOrIndices]` | `DataFrame` | e.g. df[df["Age"] &gt; 18] |
| `this[int index]` | `Series` | Indexer to get a column by position. Usage: var s = df[0]; |
| `this[int rowIndex, int columnIndex]` | `object` | Syntax Sugar |
| `this[int rowIndex, string colName]` | `object` | df[rowIndex, colName] = value |
| `this[string columnName]` | `Series` | df["foo"] = series |
| `this[string[] columnNames]` | `DataFrame` | e.g. df[["A", "B"]] = df2[["X", "Y"]]; e.g. df[["A", "B"]] = [s1, s2]; |

## Constructors & Static Methods

### DataFrame

Create a DataFrame from a collection of Series.

```csharp
public DataFrame(params Series[] series)
public DataFrame(ReadOnlySpan<Series> series)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `series` | `Series[] / ReadOnlySpan<Series>` | - |

---

### Concat

Concatenate multiple DataFrames either vertically (union) or horizontally.

```csharp
public static DataFrame Concat(IEnumerable<DataFrame> dfs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dfs` | `IEnumerable<DataFrame>` | A collection of DataFrames to concatenate. |

**Returns:** A new combined DataFrame.

**Example**

```csharp
var df1 = DataFrame.FromColumns(new
{
    a = new[] { 1, 2 },
    b = new[] { 10, 20 }
});

var df2 = DataFrame.FromColumns(new
{
    a = new[] { 3, 4 },
    b = new[] { 30, 40 }
});

// Vertical Concat (Union rows)
var catVertical = DataFrame.ConcatVertical(new[] { df1, df2 });
catVertical.Show();
/* Output:
shape: (4, 2)
┌─────┬─────┐
│ a   ┆ b   │
│ --- ┆ --- │
│ i32 ┆ i32 │
╞═════╪═════╡
│ 1   ┆ 10  │
│ 2   ┆ 20  │
│ 3   ┆ 30  │
│ 4   ┆ 40  │
└─────┴─────┘
*/

// Horizontal Concat (Append columns)
var df3 = DataFrame.FromColumns(new { c = new[] { "x", "y" } });
var catHorizontal = DataFrame.ConcatHorizontal(new[] { df1, df3 });
catHorizontal.Show();
/* Output:
shape: (2, 3)
┌─────┬─────┬─────┐
│ a   ┆ b   ┆ c   │
│ --- ┆ --- ┆ --- │
│ i32 ┆ i32 ┆ str │
╞═════╪═════╪═════╡
│ 1   ┆ 10  ┆ x   │
│ 2   ┆ 20  ┆ y   │
└─────┴─────┴─────┘
*/
```

---

### ConcatDiagonal

Diagonal concatenation of DataFrames.

```csharp
public static DataFrame ConcatDiagonal(IEnumerable<DataFrame> dfs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dfs` | `IEnumerable<DataFrame>` | - |

---

### ConcatHorizontal

Horizontal concatenation of DataFrames.

```csharp
public static DataFrame ConcatHorizontal(
    IEnumerable<DataFrame> dfs,
    bool checkDuplicates = true,
    bool strict = true,
    bool unitLengthAsScalar = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dfs` | `IEnumerable<DataFrame>` | DataFrames to concat. |
| `checkDuplicates` | `bool` | If true, check that the column names are unique. If multiple columns have the same name, they will be dropped. |
| `strict` | `bool` | For Horizontal: if true, error on height mismatch. |
| `unitLengthAsScalar` | `bool` | For Horizontal: if true, broadcast length-1 DataFrames to match height. |

---

### From

Create a DataFrame from a collection of strongly-typed objects (POCOs). 

This method uses reflection to inspect the properties of the class `T` and maps them to DataFrame columns.

```csharp
public static DataFrame From<T>(IEnumerable<T> data)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `data` | `IEnumerable<T>` | The collection of objects to load. |

**Returns:** A new DataFrame.

**Example**

```csharp
public class Student
{
    public string Name { get; set; }
    public int Age { get; set; }
    public double GPA { get; set; }
}

var students = new List<Student>
{
    new Student { Name = "Alice", Age = 20, GPA = 3.5 },
    new Student { Name = "Bob", Age = 22, GPA = 3.8 },
    new Student { Name = "Charlie", Age = 19, GPA = 3.2 }
};

var df = DataFrame.From(students);
df.Show();
/* Output:
shape: (3, 3)
┌─────────┬─────┬─────┐
│ Name    ┆ Age ┆ GPA │
│ ---     ┆ --- ┆ --- │
│ str     ┆ i32 ┆ f64 │
╞═════════╪═════╪═════╡
│ Alice   ┆ 20  ┆ 3.5 │
│ Bob     ┆ 22  ┆ 3.8 │
│ Charlie ┆ 19  ┆ 3.2 │
└─────────┴─────┴─────┘
*/
```

---

### FromArrow

Create DataFrame from Apache Arrow RecordBatch.

```csharp
public static DataFrame FromArrow(RecordBatch batch)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `batch` | `RecordBatch` | - |

---

### FromArrowStream

Safely consume any foreign Arrow C Stream (Strict mode). Adapts physical memory layouts (e.g., Utf8View) automatically.

```csharp
public static DataFrame FromArrowStream(IArrowArrayStream stream)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `stream` | `IArrowArrayStream` | - |

---

### FromColumns

Creates a DataFrame from an object where properties represent column data arrays or lists (Structure of Arrays layout). Leverages compiled expression trees to eliminate reflection on repeated container types.

Non-generic backward-compatible overload for Structure of Arrays data layout.

Create DataFrame from explicitly named columns. No reflection used. Best performance.

Create a DataFrame from a collection of Series. 

This is the functional equivalent of the constructor `new DataFrame(series)`, provided for consistency with other `From...` factory methods.

```csharp
public static DataFrame FromColumns<T>(T columns)
public static DataFrame FromColumns(object columns)
public static DataFrame FromColumns(params (string, object)[] columns)
public static DataFrame FromColumns(params Series[] series)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `T / object / (string, object)[]` | - |
| `series` | `Series[]` | The series to combine into a DataFrame. |

**Returns:** A new DataFrame containing the provided series.

**Example**

```csharp
var df = DataFrame.FromColumns(new { Time = new[] { dt1, dt2 }, Val = new[] { 1.0, 2.0 } });
```
```csharp
var s1 = new Series("id", [1, 2, 3]);
var s2 = new Series("name", ["Alice", "Bob", "Charlie"]);

var df = DataFrame.FromSeries(s1, s2);
Console.WriteLine(df);
```

---

### FromDict

Create a DataFrame from a single dictionary (represents 1 row).

```csharp
public static DataFrame FromDict(
    IDictionary<string, object> data,
    IntoSchema? schema = null,
    IntoSchema? schemaOverrides = null,
    bool strict = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `data` | `IDictionary<string, object>` | - |
| `schema` | `IntoSchema?` | - |
| `schemaOverrides` | `IntoSchema?` | - |
| `strict` | `bool` | - |

---

### FromDicts

Create a DataFrame from a collection of dictionaries.

```csharp
public static DataFrame FromDicts(
    IEnumerable<IDictionary<string, object>> data,
    IntoSchema? schema = null,
    IntoSchema? schemaOverrides = null,
    bool strict = true,
    uint? inferSchemaLength = 100
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `data` | `IEnumerable<IDictionary<string, object>>` | The collection of dictionaries representing rows. |
| `schema` | `IntoSchema?` | Strict blueprint. If provided, overrides dynamic inference. Extra keys in data are ignored. |
| `schemaOverrides` | `IntoSchema?` | Patch blueprint. Infers all keys, but overrides specific column types. |
| `strict` | `bool` | If true, throws on unpromotable type mismatch. |
| `inferSchemaLength` | `uint?` | Rows to inspect for C# type inference. |

---

### FromEnumerable

Stream C# objects into Polars by Arrow Interface

```csharp
public static DataFrame FromEnumerable<T>(IEnumerable<T> data, int batchSize = 100000, Schema? providedSchema = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `data` | `IEnumerable<T>` | - |
| `batchSize` | `int` | - |
| `providedSchema` | `Schema?` | - |

---

### FromRows

Create a DataFrame from a collection of strongly-typed objects (POCOs). 

This method uses reflection to inspect the properties of the class `T` and maps them to DataFrame columns.

```csharp
public static DataFrame FromRows<T>(IEnumerable<T> data)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `data` | `IEnumerable<T>` | The collection of objects to load. |

**Returns:** A new DataFrame.

**Example**

```csharp
public class Student
{
    public string Name { get; set; }
    public int Age { get; set; }
    public double GPA { get; set; }
}

var students = new List<Student>
{
    new Student { Name = "Alice", Age = 20, GPA = 3.5 },
    new Student { Name = "Bob", Age = 22, GPA = 3.8 },
    new Student { Name = "Charlie", Age = 19, GPA = 3.2 }
};

var df = DataFrame.From(students);
df.Show();
/* Output:
shape: (3, 3)
┌─────────┬─────┬─────┐
│ Name    ┆ Age ┆ GPA │
│ ---     ┆ --- ┆ --- │
│ str     ┆ i32 ┆ f64 │
╞═════════╪═════╪═════╡
│ Alice   ┆ 20  ┆ 3.5 │
│ Bob     ┆ 22  ┆ 3.8 │
│ Charlie ┆ 19  ┆ 3.2 │
└─────────┴─────┴─────┘
*/
```

---

### FromSeries

Create a DataFrame from a collection of Series. 

This is the functional equivalent of the constructor `new DataFrame(series)`, provided for consistency with other `From...` factory methods.

```csharp
public static DataFrame FromSeries(params Series[] series)
public static DataFrame FromSeries(IEnumerable<Series> series)
public static DataFrame FromSeries(ReadOnlySpan<Series> series)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `series` | `Series[] / IEnumerable<Series> / ReadOnlySpan<Series>` | The series to combine into a DataFrame. |

**Returns:** A new DataFrame containing the provided series.

**Example**

```csharp
var s1 = new Series("id", [1, 2, 3]);
var s2 = new Series("name", ["Alice", "Bob", "Charlie"]);

var df = DataFrame.FromSeries(s1, s2);
Console.WriteLine(df);
```

---

### ReadAdbc

Generate DataFrame from ADBC query results

Executes a SQL query directly against an ADBC connection and reads the result into a zero-copy Polars DataFrame. Pure syntactic sugar: automatically manages the creation and disposal of the underlying AdbcStatement.

```csharp
public static DataFrame ReadAdbc(AdbcStatement statement)
public static DataFrame ReadAdbc(AdbcConnection connection, string sqlQuery)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `statement` | `AdbcStatement` | - |
| `connection` | `AdbcConnection` | The active ADBC connection (e.g., DuckDB, SQLite). |
| `sqlQuery` | `string` | The SQL query string to execute. |

**Returns:** A fully materialized Polars DataFrame containing the query results.

---

### ReadAvro

Read an Avro file into a DataFrame.

Read an Avro memory buffer into a DataFrame.

Read an Avro memory Stream into a DataFrame.

```csharp
public static DataFrame ReadAvro(
    string path,
    ulong? nRows = null,
    string[]? columns = null,
    int[]? projection = null
)

public static DataFrame ReadAvro(
    byte[] buffer,
    ulong? nRows = null,
    string[]? columns = null,
    int[]? projection = null
)

public static DataFrame ReadAvro(
    Stream stream,
    ulong? nRows = null,
    string[]? columns = null,
    int[]? projection = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | The path to the Avro file. |
| `nRows` | `ulong?` | Stop reading when `nRows` are read. |
| `columns` | `string[]?` | Columns to select/project by name. |
| `projection` | `int[]?` | Columns to select/project by index. |
| `buffer` | `byte[]` | The byte array containing Avro data. |
| `stream` | `Stream` | The stream containing Avro data. |

**Returns:** A new DataFrame.

---

### ReadCsv

Read a DataFrame from a CSV file.

Read a DataFrame from a CSV memory buffer.

Read a DataFrame from a CSV memory stream.

```csharp
public static DataFrame ReadCsv(
    string path,
    string[]? columns = null,
    bool hasHeader = true,
    char separator = ',',
    char? quoteChar = '"',
    char eolChar = '\n',
    bool ignoreErrors = false,
    bool tryParseDates = true,
    bool lowMemory = false,
    bool rechunk = false,
    ulong skipRows = 0,
    ulong skipRowsAfterHeader = 0,
    ulong? nRows = null,
    ulong? inferSchemaLength = null,
    ulong? inferSchemaFiles = 18446744073709551615,
    IntoSchema? schema = null,
    IntoSchema? dtypeOverride = null,
    CsvEncoding encoding = CsvEncoding.UTF8,
    string[]? nullValues = null,
    bool missingIsNull = true,
    string? commentPrefix = null,
    bool decimalComma = false,
    bool truncateRaggedLines = false,
    string? rowIndexName = null,
    ulong rowIndexOffset = 0,
    CloudOptions? cloudOptions = null
)

public static DataFrame ReadCsv(
    byte[] buffer,
    string[]? columns = null,
    bool hasHeader = true,
    char separator = ',',
    char? quoteChar = '"',
    char eolChar = '\n',
    bool ignoreErrors = false,
    bool tryParseDates = true,
    bool lowMemory = false,
    bool rechunk = false,
    ulong skipRows = 0,
    ulong skipRowsAfterHeader = 0,
    ulong? nRows = null,
    ulong? inferSchemaLength = null,
    ulong? inferSchemaFiles = 18446744073709551615,
    IntoSchema? schema = null,
    IntoSchema? dtypeOverride = null,
    CsvEncoding encoding = CsvEncoding.UTF8,
    string[]? nullValues = null,
    bool missingIsNull = true,
    string? commentPrefix = null,
    bool decimalComma = false,
    bool truncateRaggedLines = false,
    string? rowIndexName = null,
    ulong rowIndexOffset = 0
)

public static DataFrame ReadCsv(
    Stream stream,
    string[]? columns = null,
    bool hasHeader = true,
    char separator = ',',
    char? quoteChar = '"',
    char eolChar = '\n',
    bool ignoreErrors = false,
    bool tryParseDates = true,
    bool lowMemory = false,
    bool rechunk = false,
    ulong skipRows = 0,
    ulong skipRowsAfterHeader = 0,
    ulong? nRows = null,
    ulong? inferSchemaLength = null,
    ulong? inferSchemaFiles = null,
    IntoSchema? schema = null,
    IntoSchema? dtypeOverride = null,
    CsvEncoding encoding = CsvEncoding.UTF8,
    string[]? nullValues = null,
    bool missingIsNull = true,
    string? commentPrefix = null,
    bool decimalComma = false,
    bool truncateRaggedLines = false,
    string? rowIndexName = null,
    ulong rowIndexOffset = 0
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Path to the CSV file. |
| `columns` | `string[]?` | Columns to select. If null, select all columns. |
| `hasHeader` | `bool` | Whether the CSV file has a header. Defaults to true. |
| `separator` | `char` | Character used as separator. Defaults to ','. |
| `quoteChar` | `char?` | Character used for quoting. Defaults to '"'. Set to '\0' to disable. |
| `eolChar` | `char` | Character used as End-Of-Line. Defaults to '\n'. |
| `ignoreErrors` | `bool` | Try to keep reading lines if some are invalid. Defaults to false. |
| `tryParseDates` | `bool` | Try to automatically parse dates. Defaults to true. |
| `lowMemory` | `bool` | Use valid JSON lines to reduce memory usage. Defaults to false. |
| `rechunk` | `bool` | Rechunk the memory to contiguous chunks after reading. Defaults to false. |
| `skipRows` | `ulong` | Number of rows to skip from the start. Defaults to 0. |
| `skipRowsAfterHeader` | `ulong` | Skip this number of rows after the header location. Defaults to 0. |
| `nRows` | `ulong?` | Stop reading after n rows. If null, read all. |
| `inferSchemaLength` | `ulong?` | Number of rows to scan for schema inference. If null, use Polars default (100). |
| `inferSchemaFiles` | `ulong?` | How many files to use when inferring schema. |
| `schema` | `IntoSchema?` | Provide a schema to ignore schema inference. |
| `dtypeOverride` | `IntoSchema?` | Optional PolarsSchema to specify column types or overwrite inference. |
| `encoding` | `CsvEncoding` | Encoding of the CSV file. Defaults to Utf8. |
| `nullValues` | `string[]?` | List of strings to consider as null values. |
| `missingIsNull` | `bool` | Treat missing fields as null. Defaults to true. |
| `commentPrefix` | `string?` | Lines starting with this prefix will be ignored. |
| `decimalComma` | `bool` | Use comma as decimal separator. Defaults to false. |
| `truncateRaggedLines` | `bool` | Truncate lines that are longer than the schema. Defaults to false. |
| `rowIndexName` | `string?` | If provided, add a column with the row index. |
| `rowIndexOffset` | `ulong` | Offset for the row index. Defaults to 0. |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage authentication and configuration. |
| `buffer` | `byte[]` | Memory buffer with the CSV file. |
| `stream` | `Stream` | Memory stream with the CSV file. |

---

### ReadCsvAsync

Asynchronously read a DataFrame from a CSV file.

```csharp
public static Task<DataFrame> ReadCsvAsync(
    string path,
    string[]? columns = null,
    bool hasHeader = true,
    char separator = ',',
    char? quoteChar = '"',
    char eolChar = '\n',
    bool ignoreErrors = false,
    bool tryParseDates = true,
    bool lowMemory = false,
    bool rechunk = false,
    ulong skipRows = 0,
    ulong skipRowsAfterHeader = 0,
    ulong? nRows = null,
    ulong? inferSchemaLength = null,
    ulong? inferSchemaFiles = 18446744073709551615,
    IntoSchema? schema = null,
    IntoSchema? dtypeOverride = null,
    CsvEncoding encoding = CsvEncoding.UTF8,
    string[]? nullValues = null,
    bool missingIsNull = true,
    string? commentPrefix = null,
    bool decimalComma = false,
    bool truncateRaggedLines = false,
    string? rowIndexName = null,
    ulong rowIndexOffset = 0,
    CloudOptions? cloudOptions = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | - |
| `columns` | `string[]?` | - |
| `hasHeader` | `bool` | - |
| `separator` | `char` | - |
| `quoteChar` | `char?` | - |
| `eolChar` | `char` | - |
| `ignoreErrors` | `bool` | - |
| `tryParseDates` | `bool` | - |
| `lowMemory` | `bool` | - |
| `rechunk` | `bool` | - |
| `skipRows` | `ulong` | - |
| `skipRowsAfterHeader` | `ulong` | - |
| `nRows` | `ulong?` | - |
| `inferSchemaLength` | `ulong?` | - |
| `inferSchemaFiles` | `ulong?` | - |
| `schema` | `IntoSchema?` | - |
| `dtypeOverride` | `IntoSchema?` | - |
| `encoding` | `CsvEncoding` | - |
| `nullValues` | `string[]?` | - |
| `missingIsNull` | `bool` | - |
| `commentPrefix` | `string?` | - |
| `decimalComma` | `bool` | - |
| `truncateRaggedLines` | `bool` | - |
| `rowIndexName` | `string?` | - |
| `rowIndexOffset` | `ulong` | - |
| `cloudOptions` | `CloudOptions?` | - |

---

### ReadDatabase

Create a DataFrame directly from a `System.Data.IDataReader`. 

This method streams data from the reader into Arrow batches, allowing for memory-efficient loading of large datasets from databases (e.g., SQL Server, PostgreSQL, SQLite).

It automatically maps C# types to Polars types (e.g., `System.Decimal` to Decimal128, `System.DateTime` to Timestamp).

```csharp
public static DataFrame ReadDatabase(IDataReader reader, int batchSize = 50000)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `reader` | `IDataReader` | The open IDataReader instance. |
| `batchSize` | `int` | The number of rows to process per Arrow batch. Default is 50,000. |

**Returns:** A new DataFrame.

**Example**

```csharp
// Mocking a DataTable as a data source
var table = new System.Data.DataTable();
table.Columns.Add("Product", typeof(string));
table.Columns.Add("Price", typeof(decimal)); // Correctly maps to Polars Decimal128

table.Rows.Add("Laptop", 1234.56m);
table.Rows.Add("Mouse", 99.99m);

using IDataReader reader = table.CreateDataReader();

var df = DataFrame.ReadDatabase(reader);
df.Show();
/* Output:
shape: (2, 2)
┌─────────┬─────────────────────────┐
│ Product ┆ Price                   │
│ ---     ┆ ---                     │
│ str     ┆ decimal[38,18]          │
╞═════════╪═════════════════════════╡
│ Laptop  ┆ 1234.560000000000000000 │
│ Mouse   ┆ 99.990000000000000000   │
└─────────┴─────────────────────────┘
*/
```

---

### ReadDelta

Read a delta table into a new DataFrame

```csharp
public static DataFrame ReadDelta(
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

### ReadExcel

Read an Excel file (.xlsx) into a DataFrame. 

This uses the high-performance native Rust 'calamine' engine. It is significantly faster and more memory-efficient than traditional .NET Excel libraries.

```csharp
public static DataFrame ReadExcel(
    string path,
    string? sheetName = null,
    ulong sheetIndex = 0,
    IntoSchema? schema = null,
    bool hasHeader = true,
    ulong inferSchemaLen = 100,
    bool dropEmptyRows = true,
    bool raiseIfEmpty = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Path to the .xlsx file. |
| `sheetName` | `string?` | Name of the sheet to read. If provided, it takes precedence over `sheetIndex`. |
| `sheetIndex` | `ulong` | Index of the sheet to read (0-based). Default is 0 (the first sheet). |
| `schema` | `IntoSchema?` | Optional schema overrides. Provide this to strictly enforce specific column types (e.g., forcing a numeric column to be read as String). |
| `hasHeader` | `bool` | Indicates if the first row contains header names. Default is true. |
| `inferSchemaLen` | `ulong` | Number of rows to use for schema inference. (Note: The underlying engine scans implicitly, but this is kept for API alignment). |
| `dropEmptyRows` | `bool` | If true, rows where all cells are empty or null will be skipped. Default is true. |
| `raiseIfEmpty` | `bool` | If true, throws an exception if the sheet is empty or contains no data. Default is true. |

**Returns:** A new DataFrame containing the Excel data.

---

### ReadIpc

Read an Arrow IPC (Feather v2) file into a DataFrame.

Read Arrow IPC (Feather v2) from in-memory bytes.

Read Arrow IPC (Feather v2) from a Stream.

```csharp
public static DataFrame ReadIpc(
    string path,
    string[]? columns = null,
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

public static DataFrame ReadIpc(
    byte[] buffer,
    string[]? columns = null,
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

public static DataFrame ReadIpc(
    Stream stream,
    string[]? columns = null,
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
| `columns` | `string[]?` | Columns to select. If null, select all columns. (Optimized via projection pushdown). |
| `schema` | `IntoSchema?` | Optional schema to enforce. If not provided, the schema is inferred from the file footer. |
| `nRows` | `ulong?` | Limit the number of rows to scan. |
| `rechunk` | `bool` | Make sure the DataFrame is contiguous in memory (default: false). |
| `cache` | `bool` | Cache the result of the scan (default: true). |
| `glob` | `bool` | Expand glob patterns (default: true). |
| `rowIndexName` | `string?` | If provided, adds a column with the row index. |
| `rowIndexOffset` | `uint` | Offset for the row index (default: 0). |
| `includePathColumn` | `string?` | If provided, adds a column with the source file path. |
| `hivePartitioning` | `bool` | Enable Hive partitioning inference (default: false). |
| `hivePartitionSchema` | `IntoSchema?` | Manually specify the schema for Hive partitioning columns. |
| `tryParseHiveDates` | `bool` | Whether to try parsing dates in Hive partitioning paths (default: true). |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage (AWS S3, Azure Blob, GCS, etc.). |
| `buffer` | `byte[]` | The byte array containing the IPC data. |
| `stream` | `Stream` | The input stream containing the IPC data. |

---

### ReadIpcStream

Read into a DataFrame from Arrow IPC record batch stream.

```csharp
public static DataFrame ReadIpcStream(
    string path,
    string[]? columns = null,
    uint[]? projection = null,
    ulong? nRows = null,
    string? rowIndexName = null,
    uint rowIndexOffset = 0,
    bool rechunk = true
)

public static DataFrame ReadIpcStream(
    ReadOnlySpan<byte> buffer,
    string[]? columns = null,
    uint[]? projection = null,
    ulong? nRows = null,
    string? rowIndexName = null,
    uint rowIndexOffset = 0,
    bool rechunk = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Path to the IPC stream file or a byte buffer |
| `columns` | `string[]?` | Columns to select. |
| `projection` | `uint[]?` | Column indices to select. |
| `nRows` | `ulong?` | Stop reading from IPC stream after reading n rows |
| `rowIndexName` | `string?` | Insert a row index column with the given name into the DataFrame as the first column. If set to None (default), no row index column is created. |
| `rowIndexOffset` | `uint` | Start the row index at this offset.Only used if row_index_name is set. |
| `rechunk` | `bool` | Make sure that all data is contiguous. |
| `buffer` | `ReadOnlySpan<byte>` | - |

---

### ReadIpcStreamSchema

Get the schema of an IPC file without reading data.

```csharp
public static PolarsSchema ReadIpcStreamSchema(string path)
public static PolarsSchema ReadIpcStreamSchema(ReadOnlySpan<byte> buffer)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | - |
| `buffer` | `ReadOnlySpan<byte>` | - |

---

### ReadJson

Read a JSON file into a DataFrame.

Read JSON from an in-memory byte array.

Read JSON from a Stream.

```csharp
public static DataFrame ReadJson(
    string path,
    string[]? columns = null,
    IntoSchema? schema = null,
    ulong? inferSchemaLen = null,
    ulong? batchSize = null,
    bool ignoreErrors = false,
    JsonFormat jsonFormat = JsonFormat.Json
)

public static DataFrame ReadJson(
    byte[] buffer,
    string[]? columns = null,
    IntoSchema? schema = null,
    ulong? inferSchemaLen = null,
    ulong? batchSize = null,
    bool ignoreErrors = false,
    JsonFormat jsonFormat = JsonFormat.Json
)

public static DataFrame ReadJson(
    Stream stream,
    string[]? columns = null,
    IntoSchema? schema = null,
    ulong? inferSchemaLen = null,
    ulong? batchSize = null,
    bool ignoreErrors = false,
    JsonFormat jsonFormat = JsonFormat.Json
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Path to the JSON file. |
| `columns` | `string[]?` | Select specific columns to read. |
| `schema` | `IntoSchema?` | Manually specify the schema (recommended for stability). |
| `inferSchemaLen` | `ulong?` | Number of rows to scan for schema inference (default: scan all). |
| `batchSize` | `ulong?` | Batch size for reading (optimization). |
| `ignoreErrors` | `bool` | Ignore parsing errors (skip malformed lines). |
| `jsonFormat` | `JsonFormat` | Format: Json Array or Json Lines (NDJSON). |
| `buffer` | `byte[]` | - |
| `stream` | `Stream` | - |

---

### ReadParquet

Read a DataFrame from a Parquet file or multiple files via glob patterns.

Read Parquet from an in-memory byte array.

Read Parquet from a Stream.

```csharp
public static DataFrame ReadParquet(
    string path,
    string[]? columns = null,
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

public static DataFrame ReadParquet(
    byte[] buffer,
    string[]? columns = null,
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

public static DataFrame ReadParquet(
    Stream stream,
    string[]? columns = null,
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
| `columns` | `string[]?` | Columns to select. If null, select all columns. (Optimized via projection pushdown). |
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
| `schema` | `IntoSchema?` | Manually specify the schema of the file(s). |
| `hivePartitioning` | `bool` | - |
| `hivePartitionSchema` | `IntoSchema?` | Manually specify the schema for Hive partitioning columns. |
| `tryParseHiveDates` | `bool` | Whether to try parsing dates in Hive partitioning paths (default: true). |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage (AWS S3, Azure Blob, GCS, etc.). |
| `buffer` | `byte[]` | The byte array containing the parquet file. |
| `stream` | `Stream` | The input stream containing the parquet file. |

---

### ReadParquetAsync

Read a Parquet file asynchronously.

```csharp
public static Task<DataFrame> ReadParquetAsync(
    string path,
    string[]? columns = null,
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
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | - |
| `columns` | `string[]?` | - |
| `nRows` | `ulong?` | - |
| `parallel` | `ParallelStrategy` | - |
| `lowMemory` | `bool` | - |
| `useStatistics` | `bool` | - |
| `glob` | `bool` | - |
| `allowMissingColumns` | `bool` | - |
| `rechunk` | `bool` | - |
| `cache` | `bool` | - |
| `rowIndexName` | `string?` | - |
| `rowIndexOffset` | `uint` | - |
| `includePathColumn` | `string?` | - |
| `schema` | `IntoSchema?` | - |
| `hivePartitioning` | `bool` | - |
| `hivePartitionSchema` | `IntoSchema?` | - |
| `tryParseHiveDates` | `bool` | - |
| `cloudOptions` | `CloudOptions?` | - |

---

## Operators

### operator + (Addition)

e.g. df + 1

```csharp
public static DataFrame operator +(DataFrame df, int scalar)
public static DataFrame operator +(DataFrame df, double scalar)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `df` | `DataFrame` | - |
| `scalar` | `int / double` | - |

---

### operator / (Division)

```csharp
public static DataFrame operator /(DataFrame df, int scalar)
public static DataFrame operator /(DataFrame df, double scalar)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `df` | `DataFrame` | - |
| `scalar` | `int / double` | - |

---

### operator == (Equality)

```csharp
public static bool operator ==(DataFrame? left, DataFrame? right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `DataFrame?` | - |
| `right` | `DataFrame?` | - |

---

### operator != (Inequality)

```csharp
public static bool operator !=(DataFrame? left, DataFrame? right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `DataFrame?` | - |
| `right` | `DataFrame?` | - |

---

### operator * (Multiply)

```csharp
public static DataFrame operator *(DataFrame df, int scalar)
public static DataFrame operator *(DataFrame df, double scalar)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `df` | `DataFrame` | - |
| `scalar` | `int / double` | - |

---

### operator - (Subtraction)

```csharp
public static DataFrame operator -(DataFrame df, int scalar)
public static DataFrame operator -(DataFrame df, double scalar)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `df` | `DataFrame` | - |
| `scalar` | `int / double` | - |

---

## Instance Methods

### Add

Appends a new column to the end of the DataFrame, or replaces an existing column if the name already exists.

```csharp
public void Add(Series series)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `series` | `Series` | The Series to add or update. The column name is determined by the Series name. |

> [!NOTE]
> This method enables the C# collection initializer syntax (e.g., `var df = new DataFrame { s1, s2 };`). If the DataFrame already contains a column with the same name as the provided `series`, it performs an highly efficient in-place replacement. Otherwise, it appends the new column to the right.

---

### AlignChunks

Align chunks

```csharp
public DataFrame AlignChunks()
```

---

### AsDataReader

Export DataFrame As DbDataReader (Zero-Copy Enabled)

```csharp
public DbDataReader AsDataReader(int bufferSize = 5, Dictionary<string, Type>? typeOverrides = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `bufferSize` | `int` | - |
| `typeOverrides` | `Dictionary<string, Type>?` | - |

---

### AsEnumerable

Returns a lazy heap-allocated IEnumerable over DataFrame rows. Ideal for LINQ composition, interface boundaries, and streaming scenarios.

```csharp
public PolarsRowEnumerable<T> AsEnumerable<T>()
```

---

### AsTensor

Converts selected DataFrame columns into a managed 2D Row-Major Tensor [Rows, Columns]. Automatically handles memory fragmentation and performs the Column-Major to Row-Major transposition required by most Machine Learning frameworks (like ONNX, TorchSharp, ML.NET).

```csharp
public Tensor<T> AsTensor<T>(params string[] columnNames)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columnNames` | `string[]` | Specific columns to extract. If empty, extracts all columns. |

**Returns:** A 2D `System.Numerics.Tensors.Tensor` representing the feature matrix.

---

### BottomK

Get the top k rows according to the given expressions. 

This selects the largest values.

Get the Bottom k rows according to a single expression.

Master method: Get the Bottom k rows by multiple Expressions with optional parallel boolean arrays. Usage: df.BottomK(10, myColumns.Select(c =&gt; Pl.Col(c)))

Get the Bottom k rows with specific reverse directions using Tuples. Usage: df.BottomK(5, ("Score", false), ("Age", true), (Cs.Numeric(), false))

```csharp
public DataFrame BottomK(int k, IEnumerable<Expr> by, bool[] reverse)
public DataFrame BottomK(int k, IntoExprColumn by, bool reverse = false)
public DataFrame BottomK(int k, IEnumerable<string> colName, bool reverse = false)
public DataFrame BottomK(int k, params (IntoExprColumn, bool)[] configs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `k` | `int` | - |
| `by` | `IEnumerable<Expr> / IntoExprColumn` | - |
| `reverse` | `bool[]` | - |
| `colName` | `IEnumerable<string>` | - |
| `configs` | `(IntoExprColumn, bool)[]` | - |

---

### Cast

Cast all columns to the specified target type.

Cast a specific Expression, String, Selector, or Type to a DataType.

Cast multiple targets using tuples. Supports String, Expr, Selector mixed with Type, DataType, or DataTypeExpr! Usage: df.Cast( ("Age", typeof(int)), (Cs.Numeric(), DataType.Float32), (DataType.Float64, DataType.Float32) )

Cast columns to the types specified in a Schema, DataFrame, or LazyFrame.

```csharp
public DataFrame Cast(IntoDataTypeExpr dtype, bool strict = true, bool wrapNumerical = false)
public DataFrame Cast(
    IntoSelector selector,
    IntoDataTypeExpr dtype,
    bool strict = true,
    bool wrapNumerical = false
)

public DataFrame Cast(params (IntoSelector, IntoDataTypeExpr)[] casts)
public DataFrame Cast(IntoSchema schemaWrapper, bool strict = true, bool wrapNumerical = false)
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

Create an empty (n=0) or n-row null-filled (n&gt;0) copy of the DataFrame. Returns a n-row null-filled DataFrame with an identical schema.

```csharp
public DataFrame Clear(long n = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `long` | Number of (null-filled) rows to return in the cleared frame. |

**Returns:** A new DataFrame.

---

### Clone

Clone the DataFrame

```csharp
public DataFrame Clone()
```

---

### CollectSchema

Gets the Schema of the DataFrame.Same as Schema field.

```csharp
public PolarsSchema CollectSchema()
```

---

### Column

Get a column as a Series by name.

Get a column by its positional index (0-based).

```csharp
public Series Column(string name)
public Series Column(long index)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | - |
| `index` | `long` | - |

---

### Count

Return the number of non-null elements for each column.

```csharp
public DataFrame Count()
```

---

### Describe

Generate a summary statistics DataFrame (count, mean, std, min, 25%, 50%, 75%, max). Similar to pandas/polars describe().

```csharp
public DataFrame Describe()
```

---

### Drop

Drop one or more columns from the DataFrame. Returns a new DataFrame.

Drop columns using Polars Selectors or Expressions. Example: df.Drop(Cs.EndsWith("_tmp").ToExpr())

```csharp
public DataFrame Drop(params string[] columns)
public DataFrame Drop(params IntoSelector[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `string[]` | - |
| `exprs` | `IntoSelector[]` | - |

---

### DropInPlace

Drop a column in-place and return it as a Series. This mutates the original DataFrame.

```csharp
public Series DropInPlace(string name)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | The name of the column to drop. |

**Returns:** The dropped column as a Series.

---

### DropNans

Drop rows containing one or more NaN values.

Drop rows with NaN in specific columns.

Drop rows with NaN by specific Expressions.

```csharp
public DataFrame DropNans()
public DataFrame DropNans(params IntoSelector[] subsets)
public DataFrame DropNans(IEnumerable<string> subsets)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `subsets` | `IntoSelector[] / IEnumerable<string>` | - |

---

### DropNulls

Drop rows containing one or more Null values.

Drop rows with Nulls in specific columns.

Drop rows with Nulls by specific Expressions.

```csharp
public DataFrame DropNulls()
public DataFrame DropNulls(IEnumerable<string> subsets)
public DataFrame DropNulls(params IntoSelector[] subsets)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `subsets` | `IEnumerable<string> / IntoSelector[]` | - |

---

### EstimatedSize

Return an estimation of the total (heap) allocated size of the Series/DataFrame. Estimated size is given in the specified unit (bytes by default).

```csharp
public double EstimatedSize(SizeUnit unit = SizeUnit.Bytes)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `unit` | `SizeUnit` | Scale the returned size to the given unit (uses 1024 base). |

**Returns:** The estimated size as a double.

---

### Explode

Explode list/array columns into multiple rows. 

This un-nests list columns. If multiple columns are provided, they are exploded in parallel. The list columns being exploded must have the same length for each row.

Explode list/array columns into multiple rows using selector.

Bridge overload to support C# 12 collection expressions. Usage: df.Explode(["list1", "list2"])

```csharp
public DataFrame Explode(params string[] columns)
public DataFrame Explode(IntoSelector columns, bool emptyAsNull = true, bool keepNulls = true)
public DataFrame Explode(IEnumerable<string> columns, bool emptyAsNull = true, bool keepNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `string[] / IntoSelector / IEnumerable<string>` | Expressions selecting the columns to explode. |
| `emptyAsNull` | `bool` | If `true`, empty lists are exploded into a single `null` value. If `false`, rows with empty lists are removed from the result. |
| `keepNulls` | `bool` | If `true`, `null` values in the column are preserved as `null` in the result. If `false`, rows with `null` values are removed. |

**Returns:** A new DataFrame where list elements are expanded into rows.

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    id = new[] { 1, 2 },
    tags = new[] { new[] { "apple", "orange" }, new[] { "banana" } },
    scores = new[] { new[] { 10, 20 }, new[] { 30 } }
});

// Explode "tags" and "scores" columns simultaneously
var exploded = df.Explode(["tags","scores"]);

exploded.Show();
/* Output:
shape: (3, 3)
┌─────┬────────┬────────┐
│ id  ┆ tags   ┆ scores │
│ --- ┆ ---    ┆ ---    │
│ i32 ┆ str    ┆ i32    │
╞═════╪════════╪════════╡
│ 1   ┆ apple  ┆ 10     │
│ 1   ┆ orange ┆ 20     │
│ 2   ┆ banana ┆ 30     │
└─────┴────────┴────────┘
*/
```

---

### ExportBatches

Export DataFrame to Record Batch

```csharp
public void ExportBatches(Action<RecordBatch> onBatchReceived)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `onBatchReceived` | `Action<RecordBatch>` | Receive RecordBatch Callback |

---

### Extend

Extend the memory backed by this DataFrame with the values from other. Different from vstack which adds the chunks from other to the chunks of this DataFrame, extend appends the data from other to the underlying memory locations and thus may cause a reallocation. If this does not cause a reallocation, the resulting data structure will not have any extra chunks and thus will yield faster queries.

```csharp
public DataFrame Extend(DataFrame other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `DataFrame` | The DataFrame to extend. |

---

### FillNan

Fill NaN values in floating point columns with a specified Expression.

```csharp
public DataFrame FillNan(Expr fillValue)
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
public DataFrame FillNull(Expr fillValue)
public DataFrame FillNull(object value)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fillValue` | `Expr` | - |
| `value` | `object` | - |

---

### Filter

Filter rows based on a boolean expression (predicate). 

Retains only the rows where the expression evaluates to true.

Filter rows based on a boolean series.

Filter rows based on a boolean array.

```csharp
public DataFrame Filter(Expr expr)
public DataFrame Filter(Series series)
public DataFrame Filter(IEnumerable<bool> mask)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | A boolean expression to filter by (e.g., Col("a") &gt; 5). |
| `series` | `Series` | A boolean series as mask |
| `mask` | `IEnumerable<bool>` | A boolean IEnumerable as mask |

**Returns:** A new DataFrame containing only the matching rows.

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    foo = new[] { 1, 2, 3 },
    bar = new[] { 6, 7, 8 },
    ham = new[] { "a", "b", "c" }
});

// Keep rows where "foo" is greater than 1
var filtered = df.Filter(Col("foo") > 1);

filtered.Show();
/* Output:
shape: (2, 3)
┌─────┬─────┬─────┐
│ foo ┆ bar ┆ ham │
│ --- ┆ --- ┆ --- │
│ i32 ┆ i32 ┆ str │
╞═════╪═════╪═════╡
│ 2   ┆ 7   ┆ b   │
│ 3   ┆ 8   ┆ c   │
└─────┴─────┴─────┘
*/
```

---

### Fold

Apply a horizontal reduction on a DataFrame.

Fold over all columns with a custom starting accumulator.

```csharp
public Series Fold(Func<Series, Series, Series> operation)
public Series Fold(Series initial, Func<Series, Series, Series> operation)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `operation` | `Func<Series, Series, Series>` | function that takes two Series and returns a Series. |
| `initial` | `Series` | - |

---

### Gather

Selects rows from this DataFrame at the given indices.

```csharp
public DataFrame Gather(IntoIndexLazyFrame index, bool nullOnOob = false)
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
public DataFrame GatherEvery(int n, int offset = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | Gather every n-th row. |
| `offset` | `int` | Starting Index |

---

### GetColumns

Get all columns as a list of Series. Order is guaranteed to match the physical column order.

```csharp
public Series[] GetColumns()
```

---

### GetEnumerator

Enable foreach (var series in df) { ... }

```csharp
public IEnumerator<Series> GetEnumerator()
```

---

### GetRow

Fills the destination span with all values of a single row.

```csharp
public void GetRow(long index, Span<object> destination)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `long` | The 64-bit row index. |
| `destination` | `Span<object>` | The destination span whose length must be at least the DataFrame width. |

---

### GetValue

Get a value from the DataFrame at the specified row and column. This is efficient for single-value lookups (no Arrow conversion).

```csharp
public T GetValue<T>(long rowIndex, string colName)
public T GetValue<T>(string colName, long rowIndex)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `rowIndex` | `long` | - |
| `colName` | `string` | - |

---

### Glimpse

Print a dense preview of the DataFrame to the standard output. The formatting shows one line per column so that wide dataframes display cleanly.

```csharp
public void Glimpse(int maxItemsPerColumn = 10, int maxColnameLength = 50)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `maxItemsPerColumn` | `int` | Maximum number of items to show per column. |
| `maxColnameLength` | `int` | Maximum length of the displayed column names. |

---

### GlimpseFrame

Return a dense preview of the DataFrame as a new DataFrame. Schema: "column" (String), "dtype" (String), "values" (List[String])

```csharp
public DataFrame GlimpseFrame(int maxItemsPerColumn = 10, int maxColnameLength = 50)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `maxItemsPerColumn` | `int` | - |
| `maxColnameLength` | `int` | - |

---

### GlimpseString

Return a dense preview of the DataFrame as a formatted string.

```csharp
public string GlimpseString(int maxItemsPerColumn = 10, int maxColnameLength = 50)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `maxItemsPerColumn` | `int` | - |
| `maxColnameLength` | `int` | - |

---

### GroupBy

Start a GroupBy operation to perform aggregations on groups of data. 

This returns a `GroupByBuilder` which allows you to specify the aggregation functions (like Sum, Min, Max, Count) using the `.Agg()` method.

Group by a single key with explicit control over maintainOrder.

The core GroupBy implementation. All other overloads route here.

```csharp
public GroupByBuilder GroupBy(IntoExprColumn key, bool maintainOrder = true)
public GroupByBuilder GroupBy(IEnumerable<IntoExprColumn> keys, bool maintainOrder = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `key` | `IntoExprColumn` | - |
| `maintainOrder` | `bool` | - |
| `keys` | `IEnumerable<IntoExprColumn>` | - |

**Returns:** A builder object to define aggregations.

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    group = new[] { "A", "A", "B", "B", "B" },
    val1 = new[] { 1, 2, 3, 4, 5 },
    val2 = new[] { 10, 20, 10, 20, 30 }
});

// Group by "group" and calculate:
// 1. Sum of val1
// 2. Max of val2 (aliased to "max_val2")
// 3. Count of elements in the group
var grouped = df.GroupBy(Col("group")).Agg(
    Pl.Col("val1").Sum(),
    Pl.Col("val2").Max().Alias("max_val2"),
    Pl.Col("val1").Count().Alias("count")
);

grouped.Sort("group").Show();
/* Output:
shape: (2, 4)
┌───────┬──────┬──────────┬───────┐
│ group ┆ val1 ┆ max_val2 ┆ count │
│ ---   ┆ ---  ┆ ---      ┆ ---   │
│ str   ┆ i32  ┆ i32      ┆ u32   │
╞═══════╪══════╪══════════╪═══════╡
│ A     ┆ 3    ┆ 20       ┆ 2     │
│ B     ┆ 12   ┆ 30       ┆ 3     │
└───────┴──────┴──────────┴───────┘
*/
```

---

### GroupByDynamic

Group based on a time index using dynamic windows (Rolling/Resampling). 

This is essential for time-series analysis, allowing you to downsample data (e.g., 1-minute data to 1-hour bars).

```csharp
public GroupByBuilder GroupByDynamic(
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
| `indexColumn` | `IntoSelector` | The column containing time/date data (must be sorted). |
| `every` | `IntoDuration` | The interval at which to start a new window (e.g., "1h", "1d"). Also known as the step size. |
| `period` | `IntoDuration?` | The duration of each window. If null, defaults to `every`. |
| `offset` | `IntoDuration?` | Offset to shift the window start times. |
| `groupBy` | `IEnumerable<IntoExprColumn>?` | Optional extra columns to group by (e.g., group by "stock_symbol" AND time window). |
| `label` | `Label` | Which label to use for the window (Left boundary, Right boundary, or Datapoint). |
| `includeBoundaries` | `bool` | Whether to include the window boundaries in the output. |
| `ClosedInterval` | `ClosedInterval` | Which side of the window interval is closed (inclusive). |
| `startBy` | `StartBy` | Strategy to determine the start of the first window. |

**Returns:** A `GroupByBuilder` object to define aggregations.

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    time = new[] 
    { 
        new DateTime(2024, 1, 1, 10, 0, 0),
        new DateTime(2024, 1, 1, 10, 30, 0), // Inside 10:00-11:00 window
        new DateTime(2024, 1, 1, 11, 0, 0), // Start of next window
        new DateTime(2024, 1, 1, 11, 15, 0),
        new DateTime(2024, 1, 2, 09, 0, 0)
    },
    val = new[] { 1, 2, 3, 4, 5 }
});

// Group into 1-hour windows based on "time"
var dynamicGrouped = df.GroupByDynamic(
    indexColumn: "time", 
    every: TimeSpan.FromHours(1)
).Agg(
    Pl.Col("val").Sum().Alias("total_val"),
    Pl.Col("val").Count().Alias("count")
);

dynamicGrouped.Show();
/* Output:
shape: (3, 3)
┌─────────────────────┬───────────┬───────┐
│ time                ┆ total_val ┆ count │
│ ---                 ┆ ---       ┆ ---   │
│ datetime[μs]        ┆ i32       ┆ u32   │
╞═════════════════════╪═══════════╪═══════╡
│ 2024-01-01 10:00:00 ┆ 3         ┆ 2     │ // 10:00 + 10:30
│ 2024-01-01 11:00:00 ┆ 7         ┆ 2     │ // 11:00 + 11:15
│ 2024-01-02 09:00:00 ┆ 5         ┆ 1     │
└─────────────────────┴───────────┴───────┘
*/
```

---

### HStack

Horizontally stack columns to the DataFrame. Returns a new DataFrame with the new columns appended.

Horizontally stack columns to the DataFrame.

```csharp
public DataFrame HStack(IEnumerable<Series> columns)
public DataFrame HStack(params Series[] columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `IEnumerable<Series> / Series[]` | The series to stack. |

---

### HashRows

Hash and combine the rows in this DataFrame.

```csharp
public Series HashRows(ulong? seed = 42)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `seed` | `ulong?` | Random seed parameter. Defaults to 42 for reproducible hashing. |

**Returns:** A Series containing the UInt64 hashes.

---

### Head

Return head lines from a DataFrame

```csharp
public DataFrame Head(int n = 5)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | - |

---

### InsertColumn

Insert a column into the DataFrame at a specified index. Accepts a string (column name), a Series, a primitive value, or an Expr.

```csharp
public DataFrame InsertColumn(int index, IntoExprColumn column)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `int` | - |
| `column` | `IntoExprColumn` | - |

---

### Interpolate

Interpolate intermediate values. The interpolation method is linear. Nulls at the beginning and end of the series remain null.

```csharp
public DataFrame Interpolate()
```

---

### IsDuplicated

Get a mask of all duplicated rows in this DataFrame.

```csharp
public Series IsDuplicated()
```

---

### IsSorted

Check whether the DataFrame is sorted by the given columns.

Overload for checking if DataFrame is sorted by a single column.

```csharp
public bool IsSorted(IEnumerable<string> by, IEnumerable<bool>? descending = null, IEnumerable<bool>? nullsLast = null)
public bool IsSorted(string by, bool descending = false, bool nullsLast = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IEnumerable<string>` | Column name(s) to check. |
| `descending` | `IEnumerable<bool>?` | Sort in descending order. When sorting by multiple columns, can be specified per column by passing a sequence of booleans. |
| `nullsLast` | `IEnumerable<bool>?` | Place null values last. When sorting by multiple columns, can be specified per column by passing a sequence of booleans. |

---

### IsUnique

Get a mask of all unique rows in this DataFrame.

```csharp
public Series IsUnique()
```

---

### IterColumns

Returns an iterator over the columns of this DataFrame.

```csharp
public IEnumerator<Series> IterColumns()
```

---

### IterRows

Iterates over all rows in the DataFrame as strongly-typed models using a zero-allocation stack enumerator, executing the specified action for each row without allocating heap objects.

```csharp
public void IterRows<T>(Action<T> action)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `action` | `Action<T>` | The action to execute for each row. |

---

### IterSlices

Returns an iterator over slices of this DataFrame.

```csharp
public IEnumerable<DataFrame> IterSlices(int nRows = 10000)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `nRows` | `int` | The number of rows per slice. Default is 10,000. |

**Returns:** An enumerable collection of DataFrame slices.

---

### Join

Join with another DataFrame using expression expressions.

Join with another DataFrame using column names.

Join using specific multiple expressions or column names. Usage: lf.Join(other, leftOn: ["lid", "ldate"], rightOn: ["rid", "rdate"])

Join with another DataFrame using a single column pair (Convenience overload).

Join using specific multiple expressions. (C# Array Covariance Helper) Usage: lf.Join(other, leftOn: [Pl.Col("A")], rightOn: [Pl.Col("B")])

Join using multiple shared expressions. (C# Array Covariance Helper) Usage: df.Join(other, on: [Pl.Col("A"), Pl.Col("B")])

```csharp
public DataFrame Join(
    DataFrame other,
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

public DataFrame Join(
    DataFrame other,
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

public DataFrame Join(
    DataFrame other,
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

public DataFrame Join(
    DataFrame other,
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

public DataFrame Join(
    DataFrame other,
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

public DataFrame Join(
    DataFrame other,
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

public DataFrame Join(
    DataFrame other,
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
| `other` | `DataFrame` | DataFrame to join with. |
| `on` | `IntoExprColumn / IEnumerable<string> / IEnumerable<IntoExprColumn> / IEnumerable<Expr>` | Join Key |
| `how` | `JoinType` | Join type (Inner, Left, etc.). |
| `suffix` | `string?` | Suffix to append to columns with same name in right DataFrame. Default "_right". |
| `validation` | `JoinValidation` | Check if join keys are unique. |
| `coalesce` | `JoinCoalesce` | How to coalesce the join keys. |
| `maintainOrder` | `JoinMaintainOrder` | How to maintain the order of the join. |
| `joinSide` | `JoinSide` | Specifies the strategy for the hash join build side. |
| `nullsEqual` | `bool` | Consider nulls as equal. |
| `sliceOffset` | `long?` | Slice the result starting at this offset. |
| `sliceLen` | `ulong` | Length of the slice. |
| `leftOn` | `IntoExprColumn / IEnumerable<IntoExprColumn> / IEnumerable<Expr>` | Column names in the left DataFrame to join on. |
| `rightOn` | `IntoExprColumn / IEnumerable<IntoExprColumn> / IEnumerable<Expr>` | Column names in the right DataFrame to join on. |

**Returns:** A new DataFrame resulting from the join.

**Example**

```csharp
var dfCustomers = DataFrame.FromColumns(new
{
    id = new[] { 1, 2, 3 },
    name = new[] { "Alice", "Bob", "Charlie" }
});

var dfOrders = DataFrame.FromColumns(new
{
    id = new[] { 1, 2, 4 },
    amount = new[] { 100, 200, 500 }
});

// Perform an Inner Join on the "id" column
var joined = dfCustomers.Join(
    dfOrders, 
    leftOn: new[] { "id" }, 
    rightOn: new[] { "id" }, 
    how: JoinType.Inner
);

joined.Show();
/* Output:
shape: (2, 3)
┌─────┬───────┬────────┐
│ id  ┆ name  ┆ amount │
│ --- ┆ ---   ┆ ---    │
│ i32 ┆ str   ┆ i32    │
╞═════╪═══════╪════════╡
│ 1   ┆ Alice ┆ 100    │
│ 2   ┆ Bob   ┆ 200    │
└─────┴───────┴────────┘
*/
```

---

### JoinAsOf

Join AsOf using a shared 'on' column and optional shared 'by' columns. Usage: lf.JoinAsOf(other, on: "time", tolerance: "2h", by: ["ticker"])

Join AsOf using independent 'leftOn'/'rightOn' and 'leftBy'/'rightBy' columns. Usage: lf.JoinAsOf(other, leftOn: "time_L", rightOn: "time_R", tolerance: TimeSpan.FromMinutes(5))

```csharp
public DataFrame JoinAsOf(
    DataFrame other,
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

public DataFrame JoinAsOf(
    DataFrame other,
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
| `other` | `DataFrame` | The right LazyFrame to join with. |
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
});

// Quotes: Price updates (irregular intervals)
// 9->100, 15->101, 25->102, 40->103
var quotes = DataFrame.FromColumns(new
{
    time = new[] { 9, 15, 25, 40 },
    bid = new[] { 100, 101, 102, 103 }
});

// Find the latest quote BEFORE or AT the trade time
var asof = trades.JoinAsOf(
    quotes, 
    leftOn: Pl.Col("time"), 
    rightOn: Pl.Col("time"),
    strategy: AsofStrategy.Backward
);

asof.Show();
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

Join with another DataFrame using multiple boolean expressions. Usage: lf.JoinWhere(other, [Pl.Col("start") &gt; Pl.Col("end"), Pl.Col("id") != null], how: JoinType.Left)

Join using a single condition (Most common use case). Usage: lf.JoinWhere(other, Pl.Col("A") &gt; Pl.Col("B"), how: JoinType.Inner)

Usage: lf.JoinWhere(other, Pl.Col("A") &gt; Pl.Col("B"), Pl.Col("C") &gt; 0)

```csharp
public DataFrame JoinWhere(
    DataFrame other,
    IEnumerable<IntoExprColumn> predicates,
    JoinType how = JoinType.Inner,
    string? suffix = null,
    JoinValidation validation = JoinValidation.ManyToMany,
    JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
    bool nullsEqual = false
)

public DataFrame JoinWhere(
    DataFrame other,
    IntoExprColumn predicate,
    JoinType how = JoinType.Inner,
    string? suffix = null,
    JoinValidation validation = JoinValidation.ManyToMany,
    JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
    JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
    bool nullsEqual = false
)

public DataFrame JoinWhere(DataFrame other, params IntoExprColumn[] predicates)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `DataFrame` | - |
| `predicates` | `IEnumerable<IntoExprColumn> / IntoExprColumn[]` | - |
| `how` | `JoinType` | - |
| `suffix` | `string?` | - |
| `validation` | `JoinValidation` | - |
| `coalesce` | `JoinCoalesce` | - |
| `maintainOrder` | `JoinMaintainOrder` | - |
| `nullsEqual` | `bool` | - |
| `predicate` | `IntoExprColumn` | - |

---

### Lazy

Convert the DataFrame into a LazyFrame. This allows building a query plan and optimizing execution.

```csharp
public LazyFrame Lazy()
```

---

### MapRows

Transforms DataFrame rows into a new DataFrame using a strongly-typed mapping function. 

Operates entirely in managed memory with pre-compiled property extraction and ingestion delegates, eliminating per-row reflection and intermediate boxing allocations.

```csharp
public DataFrame MapRows<TIn, TOut>(Func<TIn, TOut> mapFunc)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `mapFunc` | `Func<TIn, TOut>` | The transformation delegate applied to each row. |

**Returns:** A new DataFrame materialized from the mapped output records.

---

### MatchToSchema

Align the Frame's schema to the target schema. Columns not in the target schema are handled according to the extraColumns policy.

```csharp
public DataFrame MatchToSchema(
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
public DataFrame Max()
```

---

### MaxHorizontal

Get the maximum value horizontally across columns.

```csharp
public Series MaxHorizontal()
```

---

### Mean

Aggregate the columns in the Frame to their mean value.

```csharp
public DataFrame Mean()
```

---

### MeanHorizontal

Compute the mean of all values horizontally across columns.

```csharp
public Series MeanHorizontal(bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | Ignore null values (default). If set to False, any null value in the input will lead to a null output. |

---

### Median

Aggregate the columns in the Frame to their median value.

```csharp
public DataFrame Median()
```

---

### Melt

Unpivot (Melt) the DataFrame from wide to long format.

Unpivot (Melt) the DataFrame from wide to long format. 

This is the reverse of `DataFrame.Pivot`. It collapses multiple columns into key-value pairs.

```csharp
public DataFrame Melt(
    IntoSelector index,
    IntoSelector? on = null,
    string variableName = "variable",
    string valueName = "value"
)

public DataFrame Melt(
    IEnumerable<string> index,
    IEnumerable<string>? on = null,
    string variableName = "variable",
    string valueName = "value"
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `IntoSelector / IEnumerable<string>` | Column names to keep as identifiers (id_vars). |
| `on` | `IntoSelector? / IEnumerable<string>?` | Column names to unpivot/melt (value_vars). |
| `variableName` | `string` | Name for the new variable column (default "variable"). |
| `valueName` | `string` | Name for the new value column (default "value"). |

**Returns:** A long-format DataFrame.

**Example**

```csharp
// Using the 'pivoted' DataFrame from the Pivot example
// shape: (2, 3) [date, US, CN]

// Unpivot back to long format
var melted = pivoted.Unpivot(
    index: new[] { "date" },
    on: new[] { "CN", "US" },
    variableName: "country",
    valueName: "sales"
);

melted.Sort(new[] { "date", "country" }).Show();
/* Output:
shape: (4, 3)
┌────────────┬─────────┬───────┐
│ date       ┆ country ┆ sales │
│ ---        ┆ ---     ┆ ---   │
│ str        ┆ str     ┆ i32   │
╞════════════╪═════════╪═══════╡
│ 2024-01-01 ┆ CN      ┆ 200   │
│ 2024-01-01 ┆ US      ┆ 100   │
│ 2024-01-02 ┆ CN      ┆ 220   │
│ 2024-01-02 ┆ US      ┆ 110   │
└────────────┴─────────┴───────┘
*/
```

---

### Merge

Initiates a Merge (Upsert) builder to apply changes from a source DataFrame.

Initiates an eager Merge builder using a Selector or Column Expression.

Initiates a strongly-typed Merge operation between two DataFrames.

```csharp
public DataFrameMergeBuilder Merge(DataFrame source, params string[] on)
public DataFrameMergeBuilder Merge(DataFrame source, IntoSelector on)
public TypedMergeBuilder<TTarget, TSource> Merge<TTarget, TSource, TKey>(DataFrame source, Expression<Func<TTarget, TKey>> targetKey, Expression<Func<TSource, TKey>> sourceKey)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `source` | `DataFrame` | The source DataFrame containing updates/inserts. |
| `on` | `string[] / IntoSelector` | The column names to merge on. |
| `targetKey` | `Expression<Func<TTarget, TKey>>` | - |
| `sourceKey` | `Expression<Func<TSource, TKey>>` | - |

---

### MergeCatalogRecords

Starts building a Merge (Upsert) operation using this `DataFrame` as the source.

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
| `canEvolve` | `bool` | If true, enables Schema Evolution to handle new columns in the source data. |
| `cloudOptions` | `CloudOptions?` | Cloud-specific configurations and credentials. |

**Returns:** A `DeltaMergeBuilder` used to configure "WhenMatched" and "WhenNotMatched" clauses.

> [!NOTE]
> This is a builder pattern. No data is modified until `.Execute()` is called on the resulting `DeltaMergeBuilder`.

---

### MergeDelta

Merge a DataFrame into a Delta Lake table with full SQL MERGE semantics. Provides fine-grained control over Update, Insert, and Delete behaviors. Notice: In this method, Delete &gt; Update &gt; Insert &gt; Ignore. If you need other orders, please use LazyFrame.MergeDeltaOrdered

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

Starts a fluent builder to merge a DataFrame into a Delta Lake table with strict, order-preserving SQL MERGE semantics. 

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
df.MergeDeltaOrdered("s3://bucket/my_table", new[] { "Id" })
  .WhenMatchedDelete(Delta.Source("Status") == "Deleted")      // Evaluated 1st
  .WhenMatchedUpdate(Delta.Source("Stock") > Delta.Target("Stock")) // Evaluated 2nd
  .WhenNotMatchedInsert()                                      // Evaluated 3rd
  .Execute();
```

---

### MergeSorted

Take two sorted DataFrames and merge them by the sorted key. The output of this operation will also be sorted. It is the callers responsibility that the frames are sorted in ascending order by the key, with null keys at the end, otherwise the order of the output will not make sense. The schemas of both LazyFrames must be equal.

```csharp
public DataFrame MergeSorted(DataFrame other, string key, bool maintainOrder = false)
public DataFrame MergeSorted(DataFrame other, string[] key, bool maintainOrder = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `DataFrame` | Other DataFrame that must be merged |
| `key` | `string / string[]` | Key that is sorted. |
| `maintainOrder` | `bool` | If True, the output is guaranteed to have left-biased ordering for equal keys: rows from the left frame appear before rows from the right frame when their keys are equal. |

---

### Min

Aggregate the columns in the Frame to their minimum value.

```csharp
public DataFrame Min()
```

---

### MinHorizontal

Get the Minimum value horizontally across columns.

```csharp
public Series MinHorizontal()
```

---

### NChunks

Get the number of chunks used by the first Column of this DataFrame. (Equivalent to strategy='first')

```csharp
public long NChunks()
```

---

### NChunksAll

Get an array containing the number of chunks for all columns in this DataFrame. (Equivalent to strategy='all')

```csharp
public long[] NChunksAll()
```

---

### NUnique

Return the number of unique rows (based on all columns).

Return the number of unique rows based on a subset of columns (Selector, Type, string, etc.).

Return the number of unique rows based on specific column names (supports ["A", "B"] syntax).

```csharp
public long NUnique()
public long NUnique(IntoSelector subset)
public long NUnique(IEnumerable<string> columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `subset` | `IntoSelector` | - |
| `columns` | `IEnumerable<string>` | - |

---

### NullCount

Aggregate the columns in the Frame as the sum of their null value count.

```csharp
public DataFrame NullCount()
```

---

### PartitionBy

Split into multiple DataFrames partitioned by groups.

Convenience overload for Params usage.

Group by the given columns and return the groups as separate dataframes.

```csharp
public DataFrame[] PartitionBy(params IntoSelector[] by)
public DataFrame[] PartitionBy(IntoSelector by, bool maintainOrder = true, bool includeKey = true)
public DataFrame[] PartitionBy(IEnumerable<IntoSelector> by, bool maintainOrder = true, bool includeKey = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IntoSelector[] / IEnumerable<IntoSelector>` | Column name(s) or selector(s) to group by. |
| `maintainOrder` | `bool` | Ensure that the order of the groups is consistent with the input data. This is slower than a default partition by operation. |
| `includeKey` | `bool` | Include the columns used to partition the DataFrame in the output. |

---

### PartitionByAsDict

Split into multiple DataFrames, returning a dictionary mapping the group keys to the DataFrames.

Partitions the DataFrame by specified columns into a strongly typed dictionary. Supports domain model keys (record struct / record class), scalar identifiers (string, int), and tuples.

```csharp
public Dictionary<object[], DataFrame> PartitionByAsDict(params IntoSelector[] by)
public Dictionary<object[], DataFrame> PartitionByAsDict(IntoSelector by, bool maintainOrder = true, bool includeKey = true)
public Dictionary<object[], DataFrame> PartitionByAsDict(IEnumerable<IntoSelector> by, bool maintainOrder = true, bool includeKey = true)
public Dictionary<TKey, DataFrame> PartitionByAsDict<TKey>(
    IEnumerable<IntoSelector> by,
    bool maintainOrder = true,
    bool includeKey = true,
    IEqualityComparer<TKey>? comparer = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IntoSelector[] / IEnumerable<IntoSelector>` | Column name(s) or selector(s) to group by. |
| `maintainOrder` | `bool` | Ensure that the order of the groups is consistent with the input data. This is slower than a default partition by operation. |
| `includeKey` | `bool` | Include the columns used to partition the DataFrame in the output. |
| `comparer` | `IEqualityComparer<TKey>?` | Optional equality comparer for custom domain key hashing. |

**Returns:** A dictionary mapping strongly typed domain keys to their respective partition DataFrames.

---

### Pipe

Offers a structured way to apply a sequence of user-defined functions (UDFs).

```csharp
public T Pipe<T>(Func<DataFrame, T> function)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `function` | `Func<DataFrame, T>` | A function that receives the current expression and returns a value of type `T`. Typically this function wraps several Polars API calls that operate on the given expression. |

**Returns:** The result of applying `function` to this DataFrame.

---

### Pivot

Pivot the DataFrame using Selectors for column selection. This is the most flexible pivot method.

Pivot the DataFrame from long to wide format. 

This creates a new column for each unique value in the `on` argument. The values in the new columns come from the `values` column.

```csharp
public DataFrame Pivot(
    IntoSelector on,
    IntoSelector? index = null,
    IntoSelector? values = null,
    Expr? aggregateExpr = null,
    PivotAgg aggregateFunction = PivotAgg.First,
    bool maintainOrder = false,
    string separator = "_",
    PivotColumnNaming columnNaming = PivotColumnNaming.Auto
)

public DataFrame Pivot(
    IEnumerable<string> on,
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
| `on` | `IntoSelector / IEnumerable<string>` | Selector for the column(s) to pivot. |
| `index` | `IntoSelector? / IEnumerable<string>?` | Selector for the index column(s). |
| `values` | `IntoSelector? / IEnumerable<string>?` | Selector for the value column(s). |
| `aggregateExpr` | `Expr?` | Optional expression to aggregate the values. If null, uses `aggregateFunction`. |
| `aggregateFunction` | `PivotAgg` | Aggregation function to use if `aggregateExpr` is null. Default is First. |
| `maintainOrder` | `bool` | Keep the original order of the rows (index). |
| `separator` | `string` | Separator used to combine column names when multiple value columns are selected. |
| `columnNaming` | `PivotColumnNaming` | How resulting column names will be constructed. |

**Returns:** A wide-format DataFrame.

**Example**

```csharp
var dfLong = DataFrame.FromColumns(new
{
    date = new[] { "2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02" },
    country = new[] { "US", "CN", "US", "CN" },
    sales = new[] { 100, 200, 110, 220 }
});

// Pivot: rows=date, cols=country, values=sales
var pivoted = dfLong.Pivot(
    index: new[] { "date" }, 
    columns: new[] { "country" }, 
    values: new[] { "sales" }
);

pivoted.Show();
/* Output:
shape: (2, 3)
┌────────────┬─────┬─────┐
│ date       ┆ US  ┆ CN  │
│ ---        ┆ --- ┆ --- │
│ str        ┆ i32 ┆ i32 │
╞════════════╪═════╪═════╡
│ 2024-01-01 ┆ 100 ┆ 200 │
│ 2024-01-02 ┆ 110 ┆ 220 │
└────────────┴─────┴─────┘
*/
```

---

### PrintSchema

Prints the schema of the DataFrame to the standard output.

```csharp
public void PrintSchema()
```

---

### Quantile

Aggregate the columns in the Frame to their quantile value.

```csharp
public DataFrame Quantile(double quantile, QuantileMethod method = QuantileMethod.Linear)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `quantile` | `double` | Quantile between 0.0 and 1.0. |
| `method` | `QuantileMethod` | ['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method. |

---

### Rechunk

Rechunk the data in this DataFrame to a contiguous allocation. This will make sure all subsequent operations have optimal and predictable performance.

```csharp
public DataFrame Rechunk()
```

---

### Remove

Remove rows that match the predicate. This is the exact opposite of Filter. Rows where the predicate evaluates to False or Null are kept.

Remove rows based on a boolean series.

Remove rows based on a boolean array.

```csharp
public DataFrame Remove(Expr predicate)
public DataFrame Remove(Series predicate)
public DataFrame Remove(IEnumerable<bool> mask)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `predicate` | `Expr / Series` | - |
| `mask` | `IEnumerable<bool>` | - |

---

### Rename

Rename a column.

Rename a list of columns.

Rename columns using a dictionary mapping old names to new names.

Rename columns using a list of (OldName, NewName) tuples.

```csharp
public DataFrame Rename(string oldName, string newName)
public DataFrame Rename(string[] oldNames, string[] newNames)
public DataFrame Rename(IReadOnlyDictionary<string, string> mapping)
public DataFrame Rename(params (string, string)[] renames)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `oldName` | `string` | - |
| `newName` | `string` | - |
| `oldNames` | `string[]` | - |
| `newNames` | `string[]` | - |
| `mapping` | `IReadOnlyDictionary<string, string>` | - |
| `renames` | `(string, string)[]` | - |

**Example**

```csharp
df.Rename(("old1", "new1"), ("old2", "new2"));
```

---

### ReplaceColumn

Replace a column by its index in-place. Supports negative indexing (e.g., -1 replaces the last column).

Replace a column by its name in-place.

```csharp
public DataFrame ReplaceColumn(int index, Series newColumn, bool keepName = false)
public DataFrame ReplaceColumn(string columnName, Series newColumn, bool keepName = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `int` | The index of the column to replace. Negative values count from the end. |
| `newColumn` | `Series` | The new Series to insert. |
| `keepName` | `bool` | If true, keeps the original column name. If false, uses the new Series's name. Default is false. |
| `columnName` | `string` | The name of the column to replace. |

**Returns:** The current DataFrame instance to support method chaining.

---

### Reverse

Reverse the DataFrame.

```csharp
public DataFrame Reverse()
```

---

### Rolling

Group based on a time index using rolling windows.

```csharp
public GroupByBuilder Rolling(
    IntoSelector indexColumn,
    IntoDuration period,
    IntoDuration? offset = null,
    IEnumerable<IntoExprColumn>? groupBy = null,
    ClosedInterval ClosedInterval = ClosedInterval.Left
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

### Row

Get data for selected row.

```csharp
public object[] Row(long index)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `long` | - |

---

### Rows

Enumerates DataFrame rows as strongly-typed objects using pre-compiled Fast Path mappings.

```csharp
public DataFrameRowEnumerator<T> Rows<T>()
```

**Returns:** An IEnumerable of hydrated objects.

---

### Sample

Sample from this DataFrame.

```csharp
public DataFrame Sample(
    ulong n = 1,
    bool withReplacement = false,
    bool? shuffle = null,
    ulong? seed = null
)

public DataFrame Sample(
    double fraction,
    bool withReplacement = false,
    bool? shuffle = null,
    ulong? seed = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `ulong` | Number of items to return. Defaults to 1 |
| `withReplacement` | `bool` | Allow values to be sampled more than once. |
| `shuffle` | `bool?` | If set to True, the order of the sampled rows will be shuffled. If set to False (default), the order of the returned rows will be neither stable nor fully random. |
| `seed` | `ulong?` | Seed for the random number generator. If set to None (default), a random seed is generated for each time the sample is called. |
| `fraction` | `double` | Fraction of items to return. |

---

### Select

Select columns from the DataFrame and apply expressions to them. 

This is the primary way to project data, rename columns, or compute new columns based on existing ones. The result will only contain the columns specified in the expression list.

Select columns by name (convenience overload). 

This is a shortcut for creating `Polars.Col` expressions for each column name.

Select columns by name (convenience overload).

Select columns by expressions (convenience overload).

Bridge overload to support IEnumerable IntoExpr exprs. Usage: lf.Select(["Id", "Name", "Date"])

```csharp
public DataFrame Select(params IntoExprColumn[] columns)
public DataFrame Select(IEnumerable<string> columns)
public DataFrame Select(params string[] columns)
public DataFrame Select(IEnumerable<Expr> exprs)
public DataFrame Select(IEnumerable<IntoExprColumn> exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `IntoExprColumn[] / IEnumerable<string> / string[]` | A list of expressions defining the columns to select or compute. |
| `exprs` | `IEnumerable<Expr> / IEnumerable<IntoExprColumn>` | - |

**Returns:** A new DataFrame containing only the selected/computed columns. / A new DataFrame containing only the selected columns.

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    foo = new[] { 1, 2, 3 },
    bar = new[] { 6, 7, 8 },
    ham = new[] { "a", "b", "c" }
});

// Select "foo" column and compute a new column "bar_x_2"
var selected = df.Select(
    Col("foo"),
    (Col("bar") * 2).Alias("bar_x_2")
);

selected.Show();
/* Output:
shape: (3, 2)
┌─────┬─────────┐
│ foo ┆ bar_x_2 │
│ --- ┆ ---     │
│ i32 ┆ i32     │
╞═════╪═════════╡
│ 1   ┆ 12      │
│ 2   ┆ 14      │
│ 3   ┆ 16      │
└─────┴─────────┘
*/
```

> [!NOTE]
> For more advanced selections (renaming, calculations), use `DataFrame.Select`.

---

### SetSorted

Mark one or multiple columns as sorted. This is an optimizer hint and will not actually execute a sorting operation.

```csharp
public DataFrame SetSorted(IntoSelector columns, bool descending = false, bool nullsLast = false)
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

Print the DataFrame to Console.

```csharp
public void Show()
```

---

### ShrinkToFit

Shrink DataFrame memory usage.

```csharp
public DataFrame ShrinkToFit()
```

**Returns:** A new DataFrame

---

### ShrinkToFitInplace

Shrink DataFrame memory usage.This won't return a new DataFrame

```csharp
public void ShrinkToFitInplace()
```

---

### Slice

Slice the DataFrame along the rows.

e.g. df.Slice(1..5) or df.Slice(..^10)

```csharp
public DataFrame Slice(long offset, ulong? length = null)
public DataFrame Slice(Range range)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `offset` | `long` | Start index. Negative values work as expected (counting from the end). |
| `length` | `ulong?` | Length of the slice. |
| `range` | `Range` | - |

**Returns:** A new sliced DataFrame.

---

### Sort

Sort the LazyFrame by a single column name, selector, or column expression.

Sort the DataFrame by multiple columns (all ascending or all descending).

Sort the LazyFrame using specific sorting configurations for each column/selector.

```csharp
public DataFrame Sort(
    IntoSelector by,
    bool descending = false,
    bool nullsLast = false,
    bool maintainOrder = false
)

public DataFrame Sort(
    IEnumerable<IntoSelector> by,
    bool[]? descending = null,
    bool[]? nullsLast = null,
    bool maintainOrder = false
)

public DataFrame Sort(params (IntoSelector, bool)[] sortConfigs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IntoSelector / IEnumerable<IntoSelector>` | - |
| `descending` | `bool / bool[]?` | - |
| `nullsLast` | `bool / bool[]?` | - |
| `maintainOrder` | `bool` | - |
| `sortConfigs` | `(IntoSelector, bool)[]` | - |

---

### Std

Aggregate the columns in the Frame to their standard deviation value.

```csharp
public DataFrame Std(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1. |

---

### Sum

Aggregate the columns in the Frame to their sum value.

```csharp
public DataFrame Sum()
```

---

### SumHorizontal

Sum all values horizontally across columns. (ignore_nulls defaults to true)

```csharp
public Series SumHorizontal(bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | - |

---

### Tail

Return tail lines from a DataFrame

```csharp
public DataFrame Tail(int n = 5)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | - |

---

### Take

Alias for gather

```csharp
public DataFrame Take(IntoIndexLazyFrame index, bool nullOnOob = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `IntoIndexLazyFrame` | - |
| `nullOnOob` | `bool` | - |

---

### ToArrow

Transfer a DataFrame to Arrow

```csharp
public RecordBatch ToArrow()
```

---

### ToArrowStream

Export DataFrame to Arrow C Data Interface Stream. Supports zero-copy and lazy chunked reading.

```csharp
public IArrowArrayStream ToArrowStream(ReadOnlySpan<int> columnIndices = default, ulong? seed = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columnIndices` | `ReadOnlySpan<int>` | Optional column indices to prune the export (Projection Pushdown). |
| `seed` | `ulong?` | Optional random seed to shuffle chunks. |

**Returns:** Standard IArrowArrayStream

---

### ToAsyncEnumerable

Exposes in-memory DataFrame rows as a cooperative, cancellation-aware IAsyncEnumerable.

```csharp
public IAsyncEnumerable<T> ToAsyncEnumerable<T>(int chunkSize = 10000, CancellationToken cancellationToken = default)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `chunkSize` | `int` | Number of rows hydrated synchronously per cooperative slice. |
| `cancellationToken` | `CancellationToken` | Optional cooperative cancellation token. |

**Returns:** An asynchronous stream of hydrated row objects.

---

### ToDict

Convert the DataFrame to a dictionary of column name to Series.

```csharp
public IReadOnlyDictionary<string, Series> ToDict()
```

---

### ToDicts

Convert every row to a dictionary of column name to value. Corresponds to Python's `DataFrame.to_dicts()`.

```csharp
public List<Dictionary<string, object>> ToDicts()
```

**Returns:** A list of dictionaries, one per row, mapping column names to their values as `object?`.

---

### ToDummies

Convert categorical/string variables into dummy/indicator variables (One-Hot Encoding).

```csharp
public DataFrame ToDummies(
    IntoSelector? columns = null,
    string separator = "_",
    bool dropFirst = false,
    bool dropNulls = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `IntoSelector?` | Optional. The columns to encode. Accepts strings, string arrays, or Selectors (e.g. Cs.String()). If null, all string/categorical columns are encoded. |
| `separator` | `string` | The separator used in the generated column names. |
| `dropFirst` | `bool` | Whether to drop the first dummy variable to avoid collinearity (k-1 dummies). |
| `dropNulls` | `bool` | Whether to ignore null values when creating dummies. |

**Returns:** A new DataFrame with one-hot encoded columns.

---

### ToHtml

Generates a rich HTML table representation of the DataFrame for interactive environments (e.g., LINQPad, Jupyter).

```csharp
public string ToHtml()
```

**Returns:** An HTML string representing the preview of the DataFrame.

---

### ToStruct

Convert a DataFrame to a Series of type Struct.

```csharp
public Series ToStruct(string name = "")
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | Name for the struct Series. |

---

### TopK

Get the top k rows according to the given expressions. 

This selects the largest values.

Get the top k rows according to a single expression.

Master method: Get the top k rows by multiple Expressions with optional parallel boolean arrays. Usage: df.TopK(10, myColumns.Select(c =&gt; Pl.Col(c)))

Get the top k rows with specific reverse directions using Tuples. Usage: df.TopK(5, ("Score", false), ("Age", true), (Cs.Numeric(), false))

```csharp
public DataFrame TopK(int k, IEnumerable<Expr> by, bool[] reverse)
public DataFrame TopK(int k, IntoExprColumn by, bool reverse = false)
public DataFrame TopK(int k, IEnumerable<string> colName, bool reverse = false)
public DataFrame TopK(int k, params (IntoExprColumn, bool)[] configs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `k` | `int` | - |
| `by` | `IEnumerable<Expr> / IntoExprColumn` | - |
| `reverse` | `bool[]` | - |
| `colName` | `IEnumerable<string>` | - |
| `configs` | `(IntoExprColumn, bool)[]` | - |

---

### Transpose

Transpose a DataFrame over the diagonal.

```csharp
public DataFrame Transpose(bool includeHeader = false, string headerName = "column")
public DataFrame Transpose(string columnName, bool includeHeader = false, string headerName = "column")
public DataFrame Transpose(IEnumerable<string> customNames, bool includeHeader = false, string headerName = "column")
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `includeHeader` | `bool` | If set, the column names will be added as first column. |
| `headerName` | `string` | If include_header is set, this determines the name of the column that will be inserted. |
| `columnName` | `string` | Optional iterable yielding strings or a string naming an existing column. These will name the value (non-header) columns in the transposed data. |
| `customNames` | `IEnumerable<string>` | Optional iterable yielding strings or a string naming an existing column. These will name the value (non-header) columns in the transposed data. |

---

### Unique

Returns a new DataFrame with unique rows based on ALL columns.

Returns a new DataFrame with unique rows based on specific column names. Supports C# 12 collection expressions like ["Id", "Date"].

Returns a new DataFrame with unique rows based on a subset of columns (Selector, Type, DataType).

```csharp
public DataFrame Unique(
    UniqueKeepStrategy keep = UniqueKeepStrategy.First,
    bool maintainOrder = false,
    long? offset = null,
    long? len = null
)

public DataFrame Unique(
    IEnumerable<string> subset,
    UniqueKeepStrategy keep = UniqueKeepStrategy.First,
    bool maintainOrder = false,
    long? offset = null,
    long? len = null
)

public DataFrame Unique(
    IntoSelector subset,
    UniqueKeepStrategy keep = UniqueKeepStrategy.First,
    bool maintainOrder = false,
    long? offset = null,
    long? len = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `keep` | `UniqueKeepStrategy` | The strategy for which duplicate rows to retain (First, Last, Any, or None). |
| `maintainOrder` | `bool` | Keep the same order as the original DataFrame. |
| `offset` | `long?` | The starting index from which to begin the slice of unique results. |
| `len` | `long?` | The maximum number of rows to include in the result. |
| `subset` | `IEnumerable<string> / IntoSelector` | - |

---

### Unnest

Decompose a struct column into multiple columns. 

This is useful for flattening nested JSON-like data.

Decompose a struct column into multiple columns (one for each field in the struct). 

This is useful for flattening nested JSON-like data or composite types. The original struct column is replaced by its fields.

Decompose a struct column into multiple columns (one for each field in the struct).

```csharp
public DataFrame Unnest(IntoSelector columns, string? separator = null)
public DataFrame Unnest(IEnumerable<string> columns, string? separator = null)
public DataFrame Unnest(params string[] columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `IntoSelector / IEnumerable<string> / string[]` | The names of the struct columns to unnest. |
| `separator` | `string?` | Optional separator to append to the struct field names (e.g., "struct_col.field"). If null, the field names replace the struct column name directly. |

**Returns:** A new DataFrame with the struct columns expanded.

**Example**

```csharp
var data = new[] { new { User = new { Name = "Alice", Age = 30 } } };
var df = DataFrame.From(data);

// Unnest with a separator to avoid name collisions
// Result columns: "User_Name", "User_Age"
var unnested = df.Unnest(new[] { "User" }, separator: "_");
unnested.Show();
/* Output:
shape: (1, 2)
┌───────────┬──────────┐
│ User_Name ┆ User_Age │
│ ---       ┆ ---      │
│ str       ┆ i32      │
╞═══════════╪══════════╡
│ Alice     ┆ 30       │
└───────────┴──────────┘
*/
```
```csharp
// Create a DataFrame with a nested structure (simulating JSON)
var nestedData = new[]
{
    new { Id = 1, User = new { Name = "Alice", City = "NY" } },
    new { Id = 2, User = new { Name = "Bob",   City = "LA" } }
};

var df = DataFrame.From(nestedData);

// Unnest the "User" column into "Name" and "City"
var unnested = df.Unnest("User");

unnested.Show();
/* Output:
shape: (2, 3)
┌─────┬───────┬──────┐
│ Id  ┆ Name  ┆ City │
│ --- ┆ ---   ┆ ---  │
│ i32 ┆ str   ┆ str  │
╞═════╪═══════╪══════╡
│ 1   ┆ Alice ┆ NY   │
│ 2   ┆ Bob   ┆ LA   │
└─────┴───────┴──────┘
*/
```

---

### Unpivot

Unpivot (Melt) the DataFrame from wide to long format. 

This is the reverse of `DataFrame.Pivot`. It collapses multiple columns into key-value pairs.

Unpivot (Melt) the DataFrame from wide to long format.

```csharp
public DataFrame Unpivot(
    IEnumerable<string> index,
    IEnumerable<string>? on = null,
    string variableName = "variable",
    string valueName = "value"
)

public DataFrame Unpivot(
    IntoSelector index,
    IntoSelector? on = null,
    string variableName = "variable",
    string valueName = "value"
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `IEnumerable<string> / IntoSelector` | Column names to keep as identifiers (id_vars). |
| `on` | `IEnumerable<string>? / IntoSelector?` | Column names to unpivot/melt (value_vars). |
| `variableName` | `string` | Name for the new variable column (default "variable"). |
| `valueName` | `string` | Name for the new value column (default "value"). |

**Returns:** A long-format DataFrame.

**Example**

```csharp
// Using the 'pivoted' DataFrame from the Pivot example
// shape: (2, 3) [date, US, CN]

// Unpivot back to long format
var melted = pivoted.Unpivot(
    index: new[] { "date" },
    on: new[] { "CN", "US" },
    variableName: "country",
    valueName: "sales"
);

melted.Sort(new[] { "date", "country" }).Show();
/* Output:
shape: (4, 3)
┌────────────┬─────────┬───────┐
│ date       ┆ country ┆ sales │
│ ---        ┆ ---     ┆ ---   │
│ str        ┆ str     ┆ i32   │
╞════════════╪═════════╪═══════╡
│ 2024-01-01 ┆ CN      ┆ 200   │
│ 2024-01-01 ┆ US      ┆ 100   │
│ 2024-01-02 ┆ CN      ┆ 220   │
│ 2024-01-02 ┆ US      ┆ 110   │
└────────────┴─────────┴───────┘
*/
```

---

### Unstack

Unstack a long table to a wide form without doing an aggregation. This can be much faster than a pivot, because it can skip the grouping phase.

Unstack specific columns by names.

Unstack all columns

```csharp
public DataFrame Unstack(
    long step,
    IntoSelector columns,
    UnstackDirection how = UnstackDirection.Vertical,
    object[]? fillValues = null
)

public DataFrame Unstack(
    long step,
    IEnumerable<string> columns,
    UnstackDirection how = UnstackDirection.Vertical,
    object[]? fillValues = null
)

public DataFrame Unstack(long step, UnstackDirection how = UnstackDirection.Vertical, object[]? fillValues = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `step` | `long` | Number of rows in the unstacked frame. |
| `columns` | `IntoSelector / IEnumerable<string>` | Column name(s) or selector(s) to include in the operation. If set to None (default), use all columns. |
| `how` | `UnstackDirection` | Direction of the unstack. |
| `fillValues` | `object[]?` | Fill values that don’t fit the new size with this value. |

---

### Update

Update the values in this Frame with the values in other.

```csharp
public DataFrame Update(
    DataFrame other,
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
| `other` | `DataFrame` | Frame that will be used to update the values |
| `on` | `IEnumerable<string>?` | Column names that will be joined on. If set to Null (default), the implicit row index of each frame is used as a join key. |
| `how` | `JoinType` | 'Left' will keep all rows from the left table; rows may be duplicated if multiple rows in the right frame match the left row’s key.<br><br>‘Inner’: keeps only those rows where the key exists in both frames.<br> ‘Full’: will update existing rows where the key matches while also adding any new rows contained in the given frame. |
| `leftOn` | `IEnumerable<string>?` | Join column(s) of the left Frame. |
| `rightOn` | `IEnumerable<string>?` | Join column(s) of the right Frame. |
| `includeNulls` | `bool` | Overwrite values in the left frame with null values from the right frame. If set to False (default), null values in the right frame are ignored. |
| `maintainOrder` | `JoinMaintainOrder` | Which order of rows from the inputs to preserve. See join() for details. Unlike join this function preserves the left order by default. |

---

### Upsample

Upsample a DataFrame at a regular frequency.

```csharp
public DataFrame Upsample(
    IntoSelector timeColumn,
    IntoDuration every,
    IntoSelector? groupBy = null,
    bool maintainOrder = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `timeColumn` | `IntoSelector` | The column used for the time/datetime index. |
| `every` | `IntoDuration` | The interval to upsample to (e.g., "1d", "1h", or a TimeSpan). |
| `groupBy` | `IntoSelector?` | Optional. Group by these columns before upsampling. Accepts strings, Selectors, or Exprs. |
| `maintainOrder` | `bool` | If true, maintains the original order of the groups. |

**Returns:** A new DataFrame with missing time steps filled with nulls.

---

### VStack

Vertically stack another DataFrame to this one. Checks that the schema matches.

```csharp
public DataFrame VStack(DataFrame other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `DataFrame` | The DataFrame to stack vertically. |

---

### Var

Aggregate the columns in the Frame to their variance value.

```csharp
public DataFrame Var(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1. |

---

### WithColumns

Add new columns to the DataFrame or replace existing ones using expressions. 

Unlike `DataFrame.Select`, this method keeps all original columns in the DataFrame and appends the new ones (or replaces them if the names match).

Add or overwrite multiple columns from a collection of Expressions.

Add or overwrite multiple columns from a collection of mixed IntoExpr types.

Add or overwrite multiple columns from a collection of string column names.

```csharp
public DataFrame WithColumns(params IntoExprColumn[] exprs)
public DataFrame WithColumns(IEnumerable<Expr> exprs)
public DataFrame WithColumns(IEnumerable<IntoExprColumn> exprs)
public DataFrame WithColumns(IEnumerable<string> columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IntoExprColumn[] / IEnumerable<Expr> / IEnumerable<IntoExprColumn>` | Expressions defining the new columns to add. |
| `columns` | `IEnumerable<string>` | - |

**Returns:** A new DataFrame with the original columns plus the new/modified columns.

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    foo = new[] { 1, 2, 3 },
    bar = new[] { 6, 7, 8 },
    ham = new[] { "a", "b", "c" }
});

// Add a "sum" column (foo + bar) while keeping others
var withCols = df.WithColumns(
    (Col("foo") + Col("bar")).Alias("sum")
);

withCols.Show();
/* Output:
shape: (3, 4)
┌─────┬─────┬─────┬─────┐
│ foo ┆ bar ┆ ham ┆ sum │
│ --- ┆ --- ┆ --- ┆ --- │
│ i32 ┆ i32 ┆ str ┆ i32 │
╞═════╪═════╪═════╪═════╡
│ 1   ┆ 6   ┆ a   ┆ 7   │
│ 2   ┆ 7   ┆ b   ┆ 9   │
│ 3   ┆ 8   ┆ c   ┆ 11  │
└─────┴─────┴─────┴─────┘
*/
```

---

### WithRowIndex

Add a column at index 0 that counts the rows. 

This can be useful to generate a unique index or to maintain order in operations that would otherwise discard it.

```csharp
public DataFrame WithRowIndex(string name = "index", int? offset = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | The name of the new row index column. Defaults to "index". |
| `offset` | `int?` | The starting value of the row index. Defaults to 0. |

**Returns:** A new DataFrame with the row index column added.

---

### WriteAvro

Write the DataFrame to an Apache Avro file.

```csharp
public void WriteAvro(string path, AvroCompression compression = AvroCompression.Uncompressed, string name = "")
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | The file path to write to. |
| `compression` | `AvroCompression` | The compression algorithm to use. |
| `name` | `string` | The name of the Avro record. |

---

### WriteAvroMemory

Write the DataFrame to an Apache Avro memory buffer.

```csharp
public byte[] WriteAvroMemory(AvroCompression compression = AvroCompression.Uncompressed, string name = "")
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `compression` | `AvroCompression` | The compression algorithm to use. |
| `name` | `string` | The name of the Avro record. |

**Returns:** A byte array containing the Avro data.

---

### WriteCatalogTable

Writes the `DataFrame` into a Unity Catalog table by converting it to a `LazyFrame`.

```csharp
public void WriteCatalogTable(
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

### WriteCsv

Write DataFrame to a comma-separated values (CSV) file. 

This uses the Lazy execution engine internally to support streaming and cloud storage.

```csharp
public void WriteCsv(
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

### WriteCsvMemory

Sink the LazyFrame to a CSV format in memory. 

This allows for streaming execution directly into a byte array without writing to disk.

```csharp
public byte[] WriteCsvMemory(
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

### WriteCsvPartitioned

Write DataFrame to a partitioned comma-separated values (CSV) file. 

This uses the Lazy execution engine internally to support streaming and cloud storage.

```csharp
public void WriteCsvPartitioned(
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
| `path` | `string` | The output base directory path. |
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

### WriteDelta

Write a DataFrame into a delta table

```csharp
public void WriteDelta(
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

### WriteExcel

Writes the DataFrame to an Excel file (.xlsx) using the native high-performance engine. 

Performance: Uses columnar writing strategies for maximum speed (via `rust_xlsxwriter`).

Data Integrity:- `UInt64`, `Int128`, `UInt128` will be automatically written as Text to prevent Excel's 53-bit floating-point precision loss. - `Date` and `Datetime` are written as native Excel date objects with specified formatting.

```csharp
public void WriteExcel(
    string path,
    string? sheetName = null,
    string? dateFormat = null,
    string? datetimeFormat = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | The file path to save the .xlsx file. |
| `sheetName` | `string?` | Name of the worksheet. Defaults to "Sheet1" if null. |
| `dateFormat` | `string?` | Excel format string for `Date` columns (e.g., "yyyy-mm-dd"). If null, defaults to "yyyy-mm-dd". |
| `datetimeFormat` | `string?` | Excel format string for `Datetime` columns (e.g., "yyyy-mm-dd hh:mm:ss"). If null, defaults to "yyyy-mm-dd hh:mm:ss". |

---

### WriteIpc

Write DataFrame to an IPC (Arrow/Feather) file. 

This uses the Lazy execution engine internally to support streaming and cloud storage.

```csharp
public void WriteIpc(
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
| `path` | `string` | The output file path. |
| `compression` | `IpcCompression` | Compression method (None, LZ4, ZSTD). Defaults to None. |
| `compatLevel` | `int` | Compatibility level (default -1 = newest). |
| `recordBatchSize` | `int` | Number of rows per record batch (0 = default). |
| `recordBatchStatistics` | `bool` | Write statistics to the record batch header (default = true). |
| `maintainOrder` | `bool` | Maintain the order of the data. |
| `syncOnClose` | `SyncOnClose` | Whether to sync the file to disk on close. |
| `mkdir` | `bool` | Create parent directories if they don't exist (Local file system only). |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage. |

---

### WriteIpcMemory

Sink the LazyFrame to an IPC (Arrow) format in memory. 

This allows for streaming execution directly into a byte array without writing to disk.

```csharp
public byte[] WriteIpcMemory(
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

### WriteIpcPartitioned

Sink the LazyFrame to an IPC (Arrow) file. 

This allows for streaming execution.

```csharp
public void WriteIpcPartitioned(
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

### WriteIpcStream

Write to Arrow IPC record batch stream.

```csharp
public void WriteIpcStream(string path, IpcCompression compression = IpcCompression.None, int compatLevel = -1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Path to the IPC stream file or a byte buffer |
| `compression` | `IpcCompression` | Compression method. Defaults to None. |
| `compatLevel` | `int` | Use a specific compatibility level when exporting Polars’ internal data structures. |

---

### WriteIpcStreamMemory

Write to Arrow IPC record batch stream.

```csharp
public byte[] WriteIpcStreamMemory(IpcCompression compression = IpcCompression.None, int compatLevel = -1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `compression` | `IpcCompression` | Compression method. Defaults to None. |
| `compatLevel` | `int` | Use a specific compatibility level when exporting Polars’ internal data structures. |

---

### WriteJson

Write DataFrame to a JSON file.

```csharp
public void WriteJson(string path, JsonFormat format = JsonFormat.Json)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | Output file path. |
| `format` | `JsonFormat` | JSON format (Json Array or JsonLines). Defaults to Json. |

---

### WriteJsonMemory

Writes the DataFrame to a JSON format in memory.

```csharp
public byte[] WriteJsonMemory(JsonFormat jsonFormat = JsonFormat.Json)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `jsonFormat` | `JsonFormat` | The JSON format to use (Json or JsonLines). |

**Returns:** A byte array containing the JSON data.

---

### WriteJsonPartitioned

Sink the LazyFrame to a NDJSON (Newline Delimited JSON) file.

```csharp
public void WriteJsonPartitioned(
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

### WriteNdJson

Write DataFrame to a Newline Delimited JSON (NDJSON) file. 

This uses the Lazy execution engine internally to support streaming, compression, and cloud storage.

```csharp
public void WriteNdJson(
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
| `compression` | `ExternalCompression` | Compression method (Gzip/Zstd). Defaults to None. |
| `compressionLevel` | `int` | Compression level. -1 for default. |
| `checkExtension` | `bool` | Whether to check if the file extension matches '.json' or '.ndjson'. |
| `maintainOrder` | `bool` | Maintain the order of data. |
| `syncOnClose` | `SyncOnClose` | Sync to disk on close. |
| `mkdir` | `bool` | Create parent directories. |
| `cloudOptions` | `CloudOptions?` | Cloud storage options. |

---

### WriteParquet

Write DataFrame to a Parquet file. 

This uses the Lazy execution engine internally to support streaming, statistics, and cloud storage.

```csharp
public void WriteParquet(
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
| `path` | `string` | Output file path. |
| `compression` | `ParquetCompression` | Compression method. Defaults to Snappy. |
| `compressionLevel` | `int` | Compression level. -1 means default. |
| `statistics` | `bool` | Compute and write column statistics. Defaults to false. |
| `rowGroupSize` | `int` | Number of rows per row group. 0 means use default. |
| `dataPageSize` | `int` | Size of data page in bytes. 0 means use default. |
| `compatLevel` | `int` | - |
| `maintainOrder` | `bool` | - |
| `syncOnClose` | `SyncOnClose` | - |
| `mkdir` | `bool` | - |
| `cloudOptions` | `CloudOptions?` | Options for cloud storage (AWS S3, Azure Blob, GCS, etc.). |

---

### WriteParquetMemory

Sink the LazyFrame to a Parquet format in memory. 

This allows for streaming execution directly into a byte array without writing to disk.

```csharp
public byte[] WriteParquetMemory(
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

### WriteParquetPartitioned

Sink the LazyFrame to a set of Parquet files, partitioned by the specified selector. 

This writes the dataset to a directory, splitting the data into multiple files based on the partition key(s) defined in `partitionBy`.

```csharp
public void WriteParquetPartitioned(
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

### WriteTo

Common Write Interface:Transform DataFrame to IDataReader

```csharp
public void WriteTo(Action<IDataReader> writerAction, int bufferSize = 5, Dictionary<string, Type>? typeOverrides = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `writerAction` | `Action<IDataReader>` | - |
| `bufferSize` | `int` | - |
| `typeOverrides` | `Dictionary<string, Type>?` | - |

---

### WriteToAdbc

Zero-copy bulk ingest of the current DataFrame into an ADBC database (e.g., DuckDB, SQLite).

Zero-copy bulk ingest of the current DataFrame into an ADBC database table. Pure syntactic sugar: automatically manages the creation, configuration, and disposal of the underlying AdbcStatement.

```csharp
public UpdateResult WriteToAdbc(AdbcStatement statement)
public UpdateResult WriteToAdbc(AdbcConnection connection, string tableName, AdbcIngestMode ingestMode = AdbcIngestMode.Create)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `statement` | `AdbcStatement` | An AdbcStatement configured with ingest options (e.g., target table). |
| `connection` | `AdbcConnection` | The active ADBC connection (e.g., DuckDB, SQLite). |
| `tableName` | `string` | The name of the target table to ingest data into. |
| `ingestMode` | `AdbcIngestMode` | The ingestion mode (e.g., "adbc.ingest.mode.create" or "adbc.ingest.mode.append"). Defaults to create. |

**Returns:** The UpdateResult containing the number of rows affected.

---

