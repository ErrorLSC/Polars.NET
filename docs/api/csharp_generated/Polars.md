---
uid: Polars.CSharp.Polars
title: "Polars"
---

# Polars

Namespace: `Polars.CSharp`

Polars Static Helpers

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `Binary` | `DataType` | Binary type. |
| `Boolean` | `DataType` | Boolean type. |
| `Config` | `PolarsConfig` | Create a new PolarsConfig instance. |
| `Date` | `DataType` | Data type representing a calendar date. <br>The underlying representation of this type is a 32-bit signed integer. The integer indicates the number of days since the Unix epoch (1970-01-01). The number can be negative to indicate dates before the epoch. |
| `Float16` | `DataType` | 16-bit floating point type. |
| `Float32` | `DataType` | 32-bit floating point type. |
| `Float64` | `DataType` | 64-bit floating point type. |
| `Int128` | `DataType` | 128-bit signed integer type. |
| `Int16` | `DataType` | 16-bit signed integer type. |
| `Int32` | `DataType` | 32-bit signed integer type. |
| `Int64` | `DataType` | 64-bit signed integer type. |
| `Int8` | `DataType` | 8-bit signed integer type. |
| `Null` | `DataType` | Data type representing null values. |
| `SameAsInput` | `DataType` | - |
| `String` | `DataType` | UTF-8 encoded string type. |
| `Time` | `DataType` | Data type representing the time of day. <br>The underlying representation of this type is a 64-bit signed integer. The integer indicates the number of nanoseconds since midnight. |
| `UInt16` | `DataType` | 16-bit unsigned integer type. |
| `UInt32` | `DataType` | 32-bit unsigned integer type. |
| `UInt64` | `DataType` | 64-bit unsigned integer type. |
| `UInt8` | `DataType` | 8-bit unsigned integer type. |
| `Unknown` | `DataType` | Type representing DataType values that could not be determined statically. |
| `Utf8` | `DataType` | alias of String |

## Constructors & Static Methods

### AlignFrames

Align a sequence of frames using common values from one or more columns as a key. 

Frames that do not contain the given key values have rows injected (with nulls filling the non-key columns), and each resulting frame is sorted by the key.

The original column order of input frames is not changed unless select is specified (in which case the final column order is determined from that). In the case where duplicate key values exist, the alignment behaviour is determined by the given alignment strategy specified in the how parameter (by default this is a full outer join, but if your data is suitable you can get a large speedup by setting how="left" instead).

Note that this function does not result in a joined frame - you receive the same number of frames back that you passed in, but each is now aligned by key and has the same number of rows.

```csharp
public static LazyFrame[] AlignFrames(
    IEnumerable<LazyFrame> frames,
    IEnumerable<IntoExprColumn> on,
    JoinType how = JoinType.Outer,
    IEnumerable<IntoExprColumn>? select = null,
    bool descending = false
)

public static DataFrame[] AlignFrames(
    IEnumerable<DataFrame> frames,
    IEnumerable<IntoExprColumn> on,
    JoinType how = JoinType.Outer,
    IEnumerable<IntoExprColumn>? select = null,
    bool descending = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `frames` | `IEnumerable<LazyFrame> / IEnumerable<DataFrame>` | Sequence of DataFrames or LazyFrames. |
| `on` | `IEnumerable<IntoExprColumn>` | One or more columns whose unique values will be used to align the frames. |
| `how` | `JoinType` | By default the row alignment values are determined using a full outer join strategy across all frames; if you know that the first frame contains all required keys, you can set how=left for a large performance increase. |
| `select` | `IEnumerable<IntoExprColumn>?` | Optional post-alignment column select to constrain and/or order the columns returned from the newly aligned frames. |
| `descending` | `bool` | Sort the alignment column values in descending order; can be a single boolean or a list of booleans associated with each column in on. |

---

### AlignFramesAsync

Align a sequence of frames using common values from one or more columns as a key. 

Frames that do not contain the given key values have rows injected (with nulls filling the non-key columns), and each resulting frame is sorted by the key.

The original column order of input frames is not changed unless select is specified (in which case the final column order is determined from that). In the case where duplicate key values exist, the alignment behaviour is determined by the given alignment strategy specified in the how parameter (by default this is a full outer join, but if your data is suitable you can get a large speedup by setting how="left" instead).

Note that this function does not result in a joined frame - you receive the same number of frames back that you passed in, but each is now aligned by key and has the same number of rows.

```csharp
public static Task<DataFrame[]> AlignFramesAsync(
    IEnumerable<DataFrame> frames,
    IEnumerable<IntoExprColumn> on,
    JoinType how = JoinType.Outer,
    IEnumerable<IntoExprColumn>? select = null,
    bool descending = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `frames` | `IEnumerable<DataFrame>` | Sequence of DataFrames or LazyFrames. |
| `on` | `IEnumerable<IntoExprColumn>` | One or more columns whose unique values will be used to align the frames. |
| `how` | `JoinType` | By default the row alignment values are determined using a full outer join strategy across all frames; if you know that the first frame contains all required keys, you can set how=left for a large performance increase. |
| `select` | `IEnumerable<IntoExprColumn>?` | Optional post-alignment column select to constrain and/or order the columns returned from the newly aligned frames. |
| `descending` | `bool` | Sort the alignment column values in descending order; can be a single boolean or a list of booleans associated with each column in on. |

---

### All

Evaluate a bitwise AND operation on the specified columns. This function is syntactic sugar for col(names).all().

Evaluate a bitwise AND operation on the specified columns, with explicit null-handling. This function is syntactic sugar for col(names).all(ignoreNulls).

```csharp
public static Expr All(params string[] names)
public static Expr All(bool ignoreNulls, params string[] names)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `names` | `string[]` | The names of the columns to aggregate. |
| `ignoreNulls` | `bool` | Whether to ignore null values in the aggregation. |

**Returns:** A boolean aggregation expression.

---

### AllHorizontal

Compute the logical AND horizontally across columns. 

Kleene logic is used to deal with nulls: if the column contains any null values and no True values, the output is null.

```csharp
public static Expr AllHorizontal(params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IntoExprColumn[]` | Column(s) to use in the aggregation. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals. |

---

### Any

Evaluate a bitwise OR operation.

```csharp
public static Expr Any(bool ignoreNulls, params string[] names)
public static Expr Any(params string[] names)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | If set to True (default), null values are ignored. If there are no non-null values, the output is False. If set to False, Kleene logic is used to deal with nulls: if the column contains any null values and no True values, the output is null. |
| `names` | `string[]` | Name(s) of the columns to use in the aggregation. |

---

### AnyHorizontal

Compute the logical OR horizontally across columns. 

Kleene logic is used to deal with nulls: if the column contains any null values and no True values, the output is null.

```csharp
public static Expr AnyHorizontal(params IntoExprColumn[] exprs)
public static Expr AnyHorizontal(IEnumerable<IntoExprColumn> exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IntoExprColumn[] / IEnumerable<IntoExprColumn>` | Column(s) to use in the aggregation. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals. |

---

### ArcTan2

Compute two argument arctan in radians. Returns the angle (in radians) in the plane between the positive x-axis and the ray from the origin to (x,y).

```csharp
public static Expr ArcTan2(IntoExprColumn y, IntoExprColumn x)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `y` | `IntoExprColumn` | Column name or Expression. |
| `x` | `IntoExprColumn` | Column name or Expression. |

---

### ArgSortBy

Return the row indices that would sort the column(s).

```csharp
public static Expr ArgSortBy(
    IntoExprColumn expr,
    bool descending = false,
    bool nullsLast = false,
    bool multithreaded = true,
    bool maintainOrder = false
)

public static Expr ArgSortBy(
    IEnumerable<IntoExprColumn> exprs,
    bool descending = false,
    bool nullsLast = false,
    bool multithreaded = true,
    bool maintainOrder = false
)

public static Expr ArgSortBy(
    IEnumerable<IntoExprColumn> exprs,
    IEnumerable<bool> descending,
    IEnumerable<bool> nullsLast,
    bool multithreaded = true,
    bool maintainOrder = false
)

public static Expr ArgSortBy(params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `IntoExprColumn` | Column(s) to arg sort by. Accepts expression input. Strings are parsed as column names. |
| `descending` | `bool / IEnumerable<bool>` | Sort in descending order. When sorting by multiple columns, can be specified per column by passing a sequence of booleans. |
| `nullsLast` | `bool / IEnumerable<bool>` | Place null values last. |
| `multithreaded` | `bool` | Sort using multiple threads. |
| `maintainOrder` | `bool` | Whether the order should be maintained if elements are equal. |
| `exprs` | `IEnumerable<IntoExprColumn> / IntoExprColumn[]` | - |

---

### ArgWhere

Return indices where condition evaluates True.

```csharp
public static Expr ArgWhere(IntoExpr condition)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `condition` | `IntoExpr` | Boolean expression/Series to evaluate |

---

### ArgWhereAsSeries

Return indices where condition evaluates True.

```csharp
public static Series ArgWhereAsSeries(IntoExpr condition)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `condition` | `IntoExpr` | Boolean expression/Series to evaluate |

---

### Array

Fixed length list type.

```csharp
public static DataType Array(DataType inner, params uint[] shape)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `inner` | `DataType` | The DataType of the values within each array. |
| `shape` | `uint[]` | The shape of the arrays. |

---

### AsStruct

Combine multiple expressions into a Struct expression.

```csharp
public static Expr AsStruct(params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IntoExprColumn[]` | - |

---

### BusinessDayCount

Count the number of business days between start and end (not including end).

```csharp
public static Expr BusinessDayCount(
    IntoExprColumn start,
    IntoExprColumn end,
    bool[]? weekMask = null,
    IntoDateExpr? holidays = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn` | Start dates. |
| `end` | `IntoExprColumn` | End dates. |
| `weekMask` | `bool[]?` | Which days of the week to count. The default is Monday to Friday. If you wanted to count only Monday to Thursday, you would pass (True, True, True, True, False, False, False). |
| `holidays` | `IntoDateExpr?` | Holidays to exclude from the count. |

---

### Categorical

A named collection of categories for Categorical. 

Two categories are considered equal (and will use the same physical mapping of categories to strings) if they have the same name, namespace and physical backing type, even if they are created in separate calls to Categories.

```csharp
public static DataType Categorical(Categories? categories = null)
public static DataType Categorical(string categories)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `categories` | `Categories? / string` | - |

---

### Categories

A named collection of categories for Categorical. 

Two categories are considered equal (and will use the same physical mapping of categories to strings) if they have the same name, namespace and physical backing type, even if they are created in separate calls to Categories.

```csharp
public static Categories Categories(string? name = null, string? nameSpace = "", CategoricalPhysical physical = CategoricalPhysical.U32)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string?` | The name of this Categories. If set to None or an empty string, this refers to the global categories. |
| `nameSpace` | `string?` | An optional namespace for this Categories. Defaults to the empty string. If the name is empty or None indicating the global categories, the namespace must also be empty. |
| `physical` | `CategoricalPhysical` | The physical type used to represent the categories. Defaults to UInt32. |

---

### Coalesce

(Lazy) Evaluates the arguments in order and returns the first non-null value.

```csharp
public static Expr Coalesce(params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IntoExprColumn[]` | Expressions to evaluate. Strings, Literals, Series are automatically converted. |

**Returns:** A new expression.

---

### CoalesceAsSeries

(Eager) Evaluates the arguments eagerly and returns the first non-null value as a Series.

```csharp
public static Series CoalesceAsSeries(params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IntoExprColumn[]` | Expressions or Series to evaluate. |

---

### Col

Column Expr (name: string)

Column Exprs (name: string)

```csharp
public static Expr Col(string name)
public static Expr Col(params string[] names)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | - |
| `names` | `string[]` | - |

---

### CollectAll

Collect multiple LazyFrames concurrently.

```csharp
public static DataFrame[] CollectAll(IEnumerable<LazyFrame> frames, Engine engine = Engine.Auto)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `frames` | `IEnumerable<LazyFrame>` | - |
| `engine` | `Engine` | - |

---

### CollectAllAsync

Collect multiple LazyFrames concurrently and asynchronously.

```csharp
public static Task<DataFrame[]> CollectAllAsync(IEnumerable<LazyFrame> frames, Engine engine = Engine.Auto)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `frames` | `IEnumerable<LazyFrame>` | - |
| `engine` | `Engine` | - |

---

### CombineDateAndTime

Combine a Date expression and a Time expression into a Datetime expression. 

Useful for combining a Date column with a Time column, or combining literal arrays.

Note: Only sub-second units (`TimeUnit.Nanoseconds`, `TimeUnit.Microseconds`, `TimeUnit.Milliseconds`) are supported.

```csharp
public static Expr CombineDateAndTime(Expr date, Expr time, TimeUnit tu = TimeUnit.Microseconds)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `date` | `Expr` | Expression for the Date component (can be a Column, Literal, or calculation). |
| `time` | `Expr` | Expression for the Time component. |
| `tu` | `TimeUnit` | The desired TimeUnit for the resulting Datetime (default: Microseconds). |

**Returns:** A new expression evaluating to Datetime.

**Example**

```csharp
// 1. Combine Columns
df.Select(Pl.Combine(Pl.Col("date_col"), Pl.Col("time_col")));

// 2. Combine Arrays (Literals)
var dates = new[] { new DateOnly(2024, 1, 1), new DateOnly(2024, 1, 2) };
var times = new[] { new TimeOnly(10, 0), new TimeOnly(11, 0) };

df.Select(
    Pl.Combine(Pl.Lit(dates), Pl.Lit(times)).Alias("combined")
);
```

---

### ConcatArray

Concat multiple array expressions into a single array expression.

```csharp
public static Expr ConcatArray(params Expr[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `Expr[]` | - |

---

### ConcatExpr

Concat multiple expressions into a single expression.

```csharp
public static Expr ConcatExpr(bool rechunk = false, params Expr[] exprs)
public static Expr ConcatExpr(params Expr[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `rechunk` | `bool` | - |
| `exprs` | `Expr[]` | - |

---

### ConcatList

Concat multiple list expressions into a single list expression.

```csharp
public static Expr ConcatList(params Expr[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `Expr[]` | - |

---

### ConcatString

Horizontally concatenate columns into a single string column.

```csharp
public static Expr ConcatString(string separator = ",", bool ignoreNulls = false, params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `separator` | `string` | String that will be used to separate the values of each column. |
| `ignoreNulls` | `bool` | Ignore null values (default is False). If set to False, null values will be propagated. if the row contains any null values, the output is null. |
| `exprs` | `IntoExprColumn[]` | Columns to concatenate into a single string column. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals. Non-String columns are cast to String. |

---

### Corr

Compute the Pearson’s or Spearman rank correlation between two columns.

```csharp
public static Expr Corr(
    IntoExprColumn a,
    IntoExprColumn b,
    CorrelationMethod method = CorrelationMethod.Pearson,
    bool propagateNans = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `a` | `IntoExprColumn` | Column name or Expression. |
| `b` | `IntoExprColumn` | Column name or Expression. |
| `method` | `CorrelationMethod` | Correlation method. |
| `propagateNans` | `bool` | If True any NaN encountered will lead to NaN in the output. Defaults to False where NaN are regarded as larger than any finite number and thus lead to the highest rank. |

---

### CorrAsSeries

Compute the Pearson’s or Spearman rank correlation between two columns.

```csharp
public static Series CorrAsSeries(
    IntoExprColumn a,
    IntoExprColumn b,
    CorrelationMethod method = CorrelationMethod.Pearson,
    bool propagateNans = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `a` | `IntoExprColumn` | Column name or Expression. |
| `b` | `IntoExprColumn` | Column name or Expression. |
| `method` | `CorrelationMethod` | Correlation method. |
| `propagateNans` | `bool` | If True any NaN encountered will lead to NaN in the output. Defaults to False where NaN are regarded as larger than any finite number and thus lead to the highest rank. |

---

### Count

Return the number of non-null values in the column.

```csharp
public static Expr Count(params string[] names)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `names` | `string[]` | - |

---

### Cov

Compute the covariance between two columns/ expressions.

```csharp
public static Expr Cov(IntoExprColumn a, IntoExprColumn b, byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `a` | `IntoExprColumn` | Column name or Expression. |
| `b` | `IntoExprColumn` | Column name or Expression. |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1. |

---

### CovAsSeries

Compute the covariance between two columns/ expressions.

```csharp
public static Series CovAsSeries(IntoExprColumn a, IntoExprColumn b, byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `a` | `IntoExprColumn` | Column name or Expression. |
| `b` | `IntoExprColumn` | Column name or Expression. |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1. |

---

### CreateDataFrame

Create a DataFrame from a collection of Series. Example: Pl.DataFrame(Pl.Series("a", new[] {1, 2}), Pl.Series("b", new[] {3, 4}))

Create a DataFrame from a collection of strongly-typed objects (POCOs). Example: Pl.DataFrame(studentList)

Create a DataFrame from an anonymous object where properties represent columns. Example: Pl.DataFrame(new { A = new[] { 1, 2 }, B = new[] { "x", "y" } })

Create a DataFrame from explicitly named column tuples. Example: Pl.DataFrame(("A", new[] { 1, 2 }), ("B", new[] { "x", "y" }))

```csharp
public static DataFrame CreateDataFrame(params Series[] series)
public static DataFrame CreateDataFrame<T>(IEnumerable<T> data)
public static DataFrame CreateDataFrame(object columns)
public static DataFrame CreateDataFrame(params (string, object)[] columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `series` | `Series[]` | - |
| `data` | `IEnumerable<T>` | - |
| `columns` | `object / (string, object)[]` | - |

---

### CreateSchema

Ordered mapping of column names to their data type.

Create an empty Polars Schema.

```csharp
public static PolarsSchema CreateSchema(IntoSchema schema)
public static PolarsSchema CreateSchema()
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `schema` | `IntoSchema` | The schema definition given by column names and their associated Polars data type. Accepts a mapping, or an iterable of tuples, Arrow Schemas,Frames. |

---

### CreateSeries

Create a Series from an IEnumerable of objects, primitives, or nested lists. Example: Pl.Series("Name", list)

Create a Series directly from an array (Fast Path). Example: Pl.Series("Age", new int[] { 25, 30 })

Create a Series from a 2D matrix.

Create a Series from a ReadOnlySpan (Zero allocation path).

Materialize a single logical Expression into a physical Series. Example: Pl.Series(Pl.Lit(42).RepeatBy(5))

```csharp
public static Series CreateSeries<T>(string name, IEnumerable<T> data)
public static Series CreateSeries<T>(string name, T[] data)
public static Series CreateSeries<T>(string name, T[,] data)
public static Series CreateSeries<T>(string name, ReadOnlySpan<T> data)
public static Series CreateSeries(Expr expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | - |
| `data` | `IEnumerable<T> / T[] / T[,] / ReadOnlySpan<T>` | - |
| `expr` | `Expr` | - |

---

### CumCount

Return the cumulative count of the non-null values in the column.This function is syntactic sugar for Col(column).CumCount().

```csharp
public static Expr CumCount(string column, bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `string` | Name of the columns to use. |
| `reverse` | `bool` | reverse the operation |

---

### CumSum

Cumulatively sum all values. Syntactic sugar for Col(names).CumSum().

```csharp
public static Expr CumSum(params string[] columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `string[]` | Name(s) of the columns to use in the aggregation. |

---

### DataTypeOf

Gets the DataType of an expression. Equivalent to Python's polars.dtype_of()

```csharp
public static DataTypeExpr DataTypeOf(Expr expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | - |

---

### DateRange

Generate a date range.

```csharp
public static Expr DateRange(
    IntoExprColumn? start = null,
    IntoExprColumn? end = null,
    IntoDuration? interval = null,
    ClosedInterval closed = ClosedInterval.Both
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn?` | Lower bound of the date range. |
| `end` | `IntoExprColumn?` | Upper bound of the date range. |
| `interval` | `IntoDuration?` | Interval of the range periods, “1w2d” # 1 week, 2 days.Default is 1 day. |
| `closed` | `ClosedInterval` | Define which sides of the range are closed |

**Returns:** Column of data type Date

---

### DateRangeAsSeries

Generate a date range.

```csharp
public static Series DateRangeAsSeries(
    IntoExprColumn? start = null,
    IntoExprColumn? end = null,
    IntoDuration? interval = null,
    ClosedInterval closed = ClosedInterval.Both,
    string name = "date"
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn?` | Lower bound of the date range. |
| `end` | `IntoExprColumn?` | Upper bound of the date range. |
| `interval` | `IntoDuration?` | Interval of the range periods, “1w2d” # 1 week, 2 days.Default is 1 day. |
| `closed` | `ClosedInterval` | Define which sides of the range are closed |
| `name` | `string` | - |

**Returns:** Column of data type Date

---

### DateRanges

Create a column of date ranges. DataType will be a list of dates

```csharp
public static Expr DateRanges(
    IntoExprColumn? start = null,
    IntoExprColumn? end = null,
    IntoDuration? interval = null,
    ClosedInterval closed = ClosedInterval.Both
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn?` | Lower bound of the date range. |
| `end` | `IntoExprColumn?` | Upper bound of the date range. |
| `interval` | `IntoDuration?` | Interval of the range periods, “1w2d” # 1 week, 2 days.Default is 1 day. |
| `closed` | `ClosedInterval` | Define which sides of the range are closed |

**Returns:** Column of data type Date

---

### DateRangesAsSeries

Create a column of date ranges. DataType will be a list of dates

```csharp
public static Series DateRangesAsSeries(
    IntoExprColumn? start = null,
    IntoExprColumn? end = null,
    IntoDuration? interval = null,
    ClosedInterval closed = ClosedInterval.Both,
    string name = "date"
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn?` | Lower bound of the date range. |
| `end` | `IntoExprColumn?` | Upper bound of the date range. |
| `interval` | `IntoDuration?` | Interval of the range periods, “1w2d” # 1 week, 2 days.Default is 1 day. |
| `closed` | `ClosedInterval` | Define which sides of the range are closed |
| `name` | `string` | - |

**Returns:** Column of data type Date

---

### Datetime

Data type representing a calendar date and time of day.

```csharp
public static DataType Datetime(TimeUnit unit = TimeUnit.Microseconds, string? timeZone = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `unit` | `TimeUnit` | Unit of time. Defaults to microseconds. |
| `timeZone` | `string?` | Time zone string. When used to match dtypes, can set this to “*” to check for Datetime columns that have any (non-null) timezone. |

---

### DatetimeRange

Generate a datetime range.

```csharp
public static Expr DatetimeRange(
    IntoExprColumn? start = null,
    IntoExprColumn? end = null,
    IntoDuration? interval = null,
    ClosedInterval closed = ClosedInterval.Both,
    TimeUnit unit = TimeUnit.Microseconds,
    string? timeZone = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn?` | Lower bound of the datetime range. |
| `end` | `IntoExprColumn?` | Upper bound of the datetime range. |
| `interval` | `IntoDuration?` | Interval of the range periods |
| `closed` | `ClosedInterval` | Define which sides of the range are closed |
| `unit` | `TimeUnit` | Time unit of the resulting Datetime data type. |
| `timeZone` | `string?` | Time zone of the resulting Datetime data type. |

---

### DatetimeRangeAsSeries

Generate a datetime range.

```csharp
public static Series DatetimeRangeAsSeries(
    IntoExprColumn? start = null,
    IntoExprColumn? end = null,
    IntoDuration? interval = null,
    ClosedInterval closed = ClosedInterval.Both,
    TimeUnit unit = TimeUnit.Microseconds,
    string? timeZone = null,
    string name = "datetime"
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn?` | Lower bound of the datetime range. |
| `end` | `IntoExprColumn?` | Upper bound of the datetime range. |
| `interval` | `IntoDuration?` | Interval of the range periods |
| `closed` | `ClosedInterval` | Define which sides of the range are closed |
| `unit` | `TimeUnit` | Time unit of the resulting Datetime data type. |
| `timeZone` | `string?` | Time zone of the resulting Datetime data type. |
| `name` | `string` | - |

---

### DatetimeRanges

Create a column of datetime ranges.

```csharp
public static Expr DatetimeRanges(
    IntoExprColumn? start = null,
    IntoExprColumn? end = null,
    IntoDuration? interval = null,
    ClosedInterval closed = ClosedInterval.Both,
    TimeUnit unit = TimeUnit.Microseconds,
    string? timeZone = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn?` | Lower bound of the datetime range. |
| `end` | `IntoExprColumn?` | Upper bound of the datetime range. |
| `interval` | `IntoDuration?` | Interval of the range periods |
| `closed` | `ClosedInterval` | Define which sides of the range are closed |
| `unit` | `TimeUnit` | Time unit of the resulting Datetime data type. |
| `timeZone` | `string?` | Time zone of the resulting Datetime data type. |

---

### DatetimeRangesAsSeries

Create a column of datetime ranges.

```csharp
public static Series DatetimeRangesAsSeries(
    IntoExprColumn? start = null,
    IntoExprColumn? end = null,
    IntoDuration? interval = null,
    ClosedInterval closed = ClosedInterval.Both,
    TimeUnit unit = TimeUnit.Microseconds,
    string? timeZone = null,
    string name = "datetime"
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn?` | Lower bound of the datetime range. |
| `end` | `IntoExprColumn?` | Upper bound of the datetime range. |
| `interval` | `IntoDuration?` | Interval of the range periods |
| `closed` | `ClosedInterval` | Define which sides of the range are closed |
| `unit` | `TimeUnit` | Time unit of the resulting Datetime data type. |
| `timeZone` | `string?` | Time zone of the resulting Datetime data type. |
| `name` | `string` | - |

---

### Decimal

Decimal 128-bit type with an optional precision and non-negative scale.

```csharp
public static DataType Decimal(int precision = 38, int scale = 9)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `precision` | `int` | Maximum number of digits in each number.Max is 38. |
| `scale` | `int` | Number of digits to the right of the decimal point in each number. |

---

### Duration

Data type representing a time duration. 

The underlying representation of this type is a 64-bit signed integer. The integer indicates an amount of time units and can be negative to indicate negative time offsets.

```csharp
public static DataType Duration(TimeUnit unit = TimeUnit.Microseconds)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `unit` | `TimeUnit` | Unit of time. Defaults to microseconds. |

---

### Element

Alias for an element being evaluated in an eval or filter expression.

```csharp
public static Expr Element()
```

---

### Enum

A fixed categorical encoding of a unique set of strings.

```csharp
public static DataType Enum(Categories categories)
public static DataType Enum(Series categories)
public static DataType Enum(string[] categories)
public static DataType Enum<T>()
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `categories` | `Categories / Series / string[]` | The categories in the dataset; must be a unique set of strings, or an existing .NET enum type. |

---

### EscapeRegex

Escapes string regex meta characters.

```csharp
public static string EscapeRegex(string s)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `string` | The string to escape. |

**Returns:** A string with regex meta characters escaped.

---

### Exclude

Represent all columns except for the given columns. Syntactic sugar for Pl.All().Exclude(columns).

```csharp
public static Expr Exclude(params string[] columns)
public static Expr Exclude(ReadOnlySpan<DataType> columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `string[] / ReadOnlySpan<DataType>` | The name or datatype of the column(s) to exclude. Accepts regular expression input. Regular expressions should start with ^ and end with $. |

---

### ExplainAll

Explain multiple LazyFrames as if passed to collect_all. Common Subplan Elimination is applied on the combined plan, meaning that diverging queries will run only once.

```csharp
public static string ExplainAll(IEnumerable<LazyFrame> lazyFrames)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `lazyFrames` | `IEnumerable<LazyFrame>` | A list of LazyFrames to collect. |

**Returns:** Explained plan.

---

### Extension

Create an Extension data type

```csharp
public static DataType Extension(string name, DataType inner, string? metadata = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | The registered name of the extension type (e.g. "geoarrow.wkb") |
| `inner` | `DataType` | The physical storage data type |
| `metadata` | `string?` | Optional metadata string |

---

### Field

Definition of a single field within a Struct DataType.

```csharp
public static Field Field(string name, DataType dtype)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | The name of the field within its parent Struct. |
| `dtype` | `DataType` | The DataType of the field’s values. |

---

### First

Get the first column or value.

```csharp
public static Expr First(params string[] columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `string[]` | One or more column names. |

---

### Fold

Accumulate over multiple columns horizontally/row-wise with a left fold.

```csharp
public static Expr Fold(Expr acc, Func<Expr, Expr, Expr> f, IEnumerable<Expr> exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `acc` | `Expr` | - |
| `f` | `Func<Expr, Expr, Expr>` | - |
| `exprs` | `IEnumerable<Expr>` | - |

---

### Format

Format expressions as a string.

```csharp
public static Expr Format(string format, params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `format` | `string` | - |
| `exprs` | `IntoExprColumn[]` | - |

---

### FromEpoch

Parses an integer column (seconds, milliseconds, etc.) into a Datetime or Date expression.

```csharp
public static Expr FromEpoch(IntoExprColumn column, EpochTimeUnit timeUnit = EpochTimeUnit.Second)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `IntoExprColumn` | - |
| `timeUnit` | `EpochTimeUnit` | - |

---

### GetExtensionType

Get the extension type registration info for the given extension name.

```csharp
public static ExtensionInfo GetExtensionType(string extName)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `extName` | `string` | The registered name. |

**Returns:** An `ExtensionInfo` indicating how the type is registered.

---

### Head

Get the first n rows. This function is syntactic sugar for Pl.Col(column).Head(n).

```csharp
public static Expr Head(string column, int n = 10)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `string` | Column name. |
| `n` | `int` | Number of rows to return |

---

### IfElse

If-Else control flow: if predicate evaluates to true, return trueExpr, otherwise return falseExpr. Similar to SQL's CASE WHEN ... THEN ... ELSE ... END.

```csharp
public static Expr IfElse(Expr predicate, Expr trueExpr, Expr falseExpr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `predicate` | `Expr` | - |
| `trueExpr` | `Expr` | - |
| `falseExpr` | `Expr` | - |

---

### Implode

Aggregate all column values into a list. This function is syntactic sugar for Pl.Col(name).Implode().

```csharp
public static Expr Implode(string column, bool maintainOrder = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `string` | Column name |
| `maintainOrder` | `bool` | Whether to preserve the order of elements in the list. Setting this to False can improve performance, especially within GroupBy. |

---

### IntRange

Generate a range of integers as an Expression.

```csharp
public static Expr IntRange(
    IntoExprColumn start,
    IntoExprColumn? end = null,
    long step = 1,
    IntoDataTypeExpr? datatype = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn` | Start of the range (inclusive). |
| `end` | `IntoExprColumn?` | End of the range (exclusive). If set to Null (default), the value of start is used and start is set to 0. |
| `step` | `long` | Step size of the range. |
| `datatype` | `IntoDataTypeExpr?` | Integer data type of the ranges. Defaults to Int64. |

**Returns:** A Literal Expression containing the integer series.

---

### IntRangeAsSeries

Generate a range of integers as a Series.

```csharp
public static Series IntRangeAsSeries(
    IntoExprColumn start,
    IntoExprColumn? end = null,
    long step = 1,
    string name = "int",
    IntoDataTypeExpr? dtype = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn` | Start of the range (inclusive). |
| `end` | `IntoExprColumn?` | End of the range (exclusive). If set to Null (default), the value of start is used and start is set to 0. |
| `step` | `long` | Step size of the range. |
| `name` | `string` | The name of generated series. |
| `dtype` | `IntoDataTypeExpr?` | Integer data type of the ranges. Defaults to Int64. |

**Returns:** A Literal Expression containing the integer series.

---

### IntRanges

Generate a range of integers for each row of the input columns. Resulting column is of dtype List(dtype).

```csharp
public static Expr IntRanges(
    IntoExprColumn start,
    IntoExprColumn? end = null,
    IntoExprColumn? step = null,
    IntoDataTypeExpr? datatype = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn` | - |
| `end` | `IntoExprColumn?` | - |
| `step` | `IntoExprColumn?` | - |
| `datatype` | `IntoDataTypeExpr?` | - |

---

### IntRangesAsSeries

Generate a range of integers for each row of the input columns. Resulting column is of dtype List(dtype).

```csharp
public static Series IntRangesAsSeries(
    IntoExprColumn start,
    IntoExprColumn? end = null,
    IntoExprColumn? step = null,
    string name = "int",
    IntoDataTypeExpr? datatype = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn` | - |
| `end` | `IntoExprColumn?` | - |
| `step` | `IntoExprColumn?` | - |
| `name` | `string` | - |
| `datatype` | `IntoDataTypeExpr?` | - |

---

### JsonNormalize

Normalize semi-structured deserialized JSON data into a flat table. Dictionary objects that will not be unnested/normalized are encoded as json string data.

Normalize a JSON string representing a single object or an array of objects.

```csharp
public static DataFrame JsonNormalize(
    object data,
    string separator = ".",
    int? maxLevel = null,
    IntoSchema? schema = null,
    bool strict = true,
    uint? inferSchemaLength = 100,
    Func<object, string>? encoder = null
)

public static DataFrame JsonNormalize(
    string json,
    string separator = ".",
    int? maxLevel = null,
    IntoSchema? schema = null,
    bool strict = true,
    uint? inferSchemaLength = 100,
    Func<object, string>? encoder = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `data` | `object` | Deserialized JSON objects. |
| `separator` | `string` | Nested records will generate names separated by sep. e.g., for separator=".", {"foo": {"bar": 0}} -&gt; foo.bar. |
| `maxLevel` | `int?` | Max number of levels(depth of dict) to normalize. If None, normalizes all levels. |
| `schema` | `IntoSchema?` | Overwrite the Schema when the normalized data is passed to the DataFrame constructor. |
| `strict` | `bool` | Whether Polars should be strict when constructing the DataFrame. |
| `inferSchemaLength` | `uint?` | Number of rows to take into consideration to determine the schema. |
| `encoder` | `Func<object, string>?` | Custom JSON encoder function; if not given, JsonSerializer.Serialize is used. |
| `json` | `string` | A valid JSON string. |

---

### Last

Get the last column or value.

```csharp
public static Expr Last(params string[] columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `string[]` | One or more column names. |

---

### Len

Return the lines count of current context.

```csharp
public static Expr Len()
```

---

### LinearSpace

Generate a series of equally-spaced points.

```csharp
public static Expr LinearSpace(
    IntoExprColumn start,
    IntoExprColumn end,
    IntoExprColumn numSamples,
    ClosedInterval closed = ClosedInterval.Both
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn` | Lower bound of the linear space. |
| `end` | `IntoExprColumn` | Upper bound of the linear space. |
| `numSamples` | `IntoExprColumn` | Number of samples to generate. |
| `closed` | `ClosedInterval` | Whether the intervals are closed or open. |

---

### LinearSpaceAsSeries

Generate a series of equally-spaced points.

```csharp
public static Series LinearSpaceAsSeries(
    IntoExprColumn start,
    IntoExprColumn end,
    IntoExprColumn numSamples,
    ClosedInterval closed = ClosedInterval.Both,
    string name = "linear_space"
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn` | Lower bound of the linear space. |
| `end` | `IntoExprColumn` | Upper bound of the linear space. |
| `numSamples` | `IntoExprColumn` | Number of samples to generate. |
| `closed` | `ClosedInterval` | Whether the intervals are closed or open. |
| `name` | `string` | - |

---

### LinearSpaces

Create a column of linearly-spaced sequences for each row.

```csharp
public static Expr LinearSpaces(
    IntoExprColumn start,
    IntoExprColumn end,
    IntoExprColumn numSamples,
    ClosedInterval closed = ClosedInterval.Both,
    bool asArray = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn` | Lower bound. |
| `end` | `IntoExprColumn` | Upper bound. |
| `numSamples` | `IntoExprColumn` | Number of samples. |
| `closed` | `ClosedInterval` | Whether the intervals are closed or open. |
| `asArray` | `bool` | If true, returns an Array dtype instead of List. Requires numSamples to be a constant. |

---

### LinearSpacesAsSeries

Create a column of linearly-spaced sequences for each row.

```csharp
public static Series LinearSpacesAsSeries(
    IntoExprColumn start,
    IntoExprColumn end,
    IntoExprColumn numSamples,
    ClosedInterval closed = ClosedInterval.Both,
    bool asArray = false,
    string name = "linear_spaces"
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn` | Lower bound. |
| `end` | `IntoExprColumn` | Upper bound. |
| `numSamples` | `IntoExprColumn` | Number of samples. |
| `closed` | `ClosedInterval` | Whether the intervals are closed or open. |
| `asArray` | `bool` | If true, returns an Array dtype instead of List. Requires numSamples to be a constant. |
| `name` | `string` | - |

---

### List

Variable length list type.

```csharp
public static DataType List(DataType inner)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `inner` | `DataType` | The DataType of the values within each list. |

---

### Lit

Convert Series into Literal Expr. 

The Series is cloned implicitly, so the original Series remains valid.

```csharp
public static Expr Lit(string? value)
public static Expr Lit(sbyte value)
public static Expr Lit(byte value)
public static Expr Lit(short value)
public static Expr Lit(ushort value)
public static Expr Lit(int value)
public static Expr Lit(uint value)
public static Expr Lit(long value)
public static Expr Lit(ulong value)
public static Expr Lit(Int128 value)
public static Expr Lit(double value)
public static Expr Lit(DateTime value)
public static Expr Lit(DateTimeOffset value)
public static Expr Lit(DateOnly value)
public static Expr Lit(TimeOnly value)
public static Expr Lit(TimeSpan value)
public static Expr Lit(bool value)
public static Expr Lit(Half value)
public static Expr Lit(float value)
public static Expr Lit(decimal value)
public static Expr Lit(Series series)
public static Expr Lit<T>(T[] values)
public static Expr Lit<T>(IEnumerable<T> values)
public static Expr Lit<T>(ReadOnlySpan<T> values)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `value` | `string? / sbyte / short / ushort / int / uint / long / ulong / Int128 / double / DateTime / DateTimeOffset / DateOnly / TimeOnly / TimeSpan / bool / Half / float / decimal` | - |
| `series` | `Series` | - |
| `values` | `T[] / IEnumerable<T> / ReadOnlySpan<T>` | - |

---

### LitBinary

Creates a scalar Binary Literal Expression from a byte array.

```csharp
public static Expr LitBinary(byte[] value)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `value` | `byte[]` | - |

---

### LitNull

```csharp
public static Expr LitNull()
```

---

### LitStruct

Create a Struct Expression from a single anonymous object or class instance. 

Example: `LitStruct(new { A = 1, B = "hi" })`

Create a Struct Expression from an array of objects. 

The properties of the objects become the fields of the struct.

```csharp
public static Expr LitStruct<T>(T value)
public static Expr LitStruct<T>(T[] values)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `value` | `T` | - |
| `values` | `T[]` | - |

---

### Max

Get the maximum value. Syntactic sugar for Col(names).Max().

```csharp
public static Expr Max(params string[] names)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `names` | `string[]` | Name(s) of the columns to use in the aggregation. |

---

### MaxHorizontal

Get the maximum value horizontally across columns.

```csharp
public static Expr MaxHorizontal(params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IntoExprColumn[]` | Column(s) to use in the aggregation. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals. |

---

### Mean

Get the Mean value. Syntactic sugar for Col(names).Mean().

```csharp
public static Expr Mean(params string[] names)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `names` | `string[]` | Name(s) of the columns to use in the aggregation. |

---

### MeanHorizontal

Compute the mean of all values horizontally across columns.

```csharp
public static Expr MeanHorizontal(IEnumerable<Expr> exprs, bool ignoreNulls = true)
public static Expr MeanHorizontal(IEnumerable<IntoExprColumn> exprs, bool ignoreNulls = true)
public static Expr MeanHorizontal(params IntoExprColumn[] exprs)
public static Expr MeanHorizontal(bool ignoreNulls, params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IEnumerable<Expr> / IEnumerable<IntoExprColumn> / IntoExprColumn[]` | Column(s) to use in the aggregation. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals. |
| `ignoreNulls` | `bool` | Ignore null values (default). If set to False, any null value in the input will lead to a null output. |

---

### Median

Get the median value.This function is syntactic sugar for Pl.Col(columns).Median().

```csharp
public static Expr Median(params string[] names)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `names` | `string[]` | One or more column names. |

---

### Min

Get the Minimum value. Syntactic sugar for Col(names).Min().

```csharp
public static Expr Min(params string[] names)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `names` | `string[]` | Name(s) of the columns to use in the aggregation. |

---

### MinHorizontal

Get the Minimum value horizontally across columns.

```csharp
public static Expr MinHorizontal(params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IntoExprColumn[]` | Column(s) to use in the aggregation. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals. |

---

### NUnique

Count unique values.This function is syntactic sugar for Pl.Col(columns).NUnique().

```csharp
public static Expr NUnique(params string[] columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `string[]` | One or more column names. |

---

### Nth

Get the nth column(s) of the context.

```csharp
public static Expr Nth(ReadOnlySpan<long> indices, bool strict = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `indices` | `ReadOnlySpan<long>` | One or more indices representing the columns to retrieve. |
| `strict` | `bool` | By default, all specified indices must be valid; if any index is out of bounds, an error is raised. If set to False, out-of-bounds indices are ignored. |

---

### Ones

Construct a column of length n filled with ones. This is syntactic sugar for the repeat function.

```csharp
public static Expr Ones(int n, DataType? dtype = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | Length of the resulting column. |
| `dtype` | `DataType?` | Data type of the resulting column. Defaults to Float64. |

---

### OnesAsSeries

Construct a column of length n filled with ones. This is syntactic sugar for the repeat function.

```csharp
public static Series OnesAsSeries(int n, DataType? dtype = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | Length of the resulting column. |
| `dtype` | `DataType?` | Data type of the resulting column. Defaults to Float64. |

---

### Quantile

Syntactic sugar for Pl.Col("foo").Quantile(..).

```csharp
public static Expr Quantile(string column, double quantile, QuantileMethod interpolation = QuantileMethod.Nearest)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `string` | Column name. |
| `quantile` | `double` | Quantile between 0.0 and 1.0. |
| `interpolation` | `QuantileMethod` | Interpolation method. |

---

### Reduce

Reduce multiple columns horizontally/row-wise with a left fold.

```csharp
public static Expr Reduce(Func<Expr, Expr, Expr> f, IEnumerable<Expr> exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `f` | `Func<Expr, Expr, Expr>` | - |
| `exprs` | `IEnumerable<Expr>` | - |

---

### RegisterExtensionType

Register the extension type for the given extension name.

Register the extension type to be passed through purely as physical storage.

```csharp
public static void RegisterExtensionType<T>(string extName, ExtensionFactory factory)
public static void RegisterExtensionType(string extName, bool asStorage)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `extName` | `string` | The registered name. |
| `factory` | `ExtensionFactory` | The factory delegate to deserialize the extension type. |
| `asStorage` | `bool` | Must be true. If false, use the generic generic method to register a class. |

---

### Repeat

Construct a column of length n filled with the given value.

```csharp
public static Expr Repeat(IntoExpr? value, int n, DataType? dtype = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `value` | `IntoExpr?` | Value to repeat. |
| `n` | `int` | Length of the resulting column. |
| `dtype` | `DataType?` | Data type of the resulting column. If set to None (default), data type is inferred from the given value. Defaults to Int32 for integer values, unless Int64 is required to fit the given value. Defaults to Float64 for float values. |

---

### RepeatAsSeries

Construct a column of length n filled with the given value.

```csharp
public static Series RepeatAsSeries(IntoExpr? value, int n, DataType? dtype = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `value` | `IntoExpr?` | Value to repeat. |
| `n` | `int` | Length of the resulting column. |
| `dtype` | `DataType?` | Data type of the resulting column. If set to None (default), data type is inferred from the given value. Defaults to Int32 for integer values, unless Int64 is required to fit the given value. Defaults to Float64 for float values. |

---

### RollingCorr

Compute the rolling correlation between two columns/ expressions. The window at a given row includes the row itself and the window_size - 1 elements before it.

```csharp
public static Expr RollingCorr(
    IntoExprColumn a,
    IntoExprColumn b,
    uint windowSize,
    uint? minSamples = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `a` | `IntoExprColumn` | Column name or Expression. |
| `b` | `IntoExprColumn` | Column name or Expression. |
| `windowSize` | `uint` | The length of the window. |
| `minSamples` | `uint?` | The number of values in the window that should be non-null before computing a result. If None, it will be set equal to window size. |

---

### RollingCov

Compute the rolling covariance between two columns/ expressions. The window at a given row includes the row itself and the window_size - 1 elements before it.

```csharp
public static Expr RollingCov(
    IntoExprColumn a,
    IntoExprColumn b,
    uint windowSize,
    uint? minSamples = null,
    byte ddof = 1
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `a` | `IntoExprColumn` | Column name or Expression. |
| `b` | `IntoExprColumn` | Column name or Expression. |
| `windowSize` | `uint` | The length of the window. |
| `minSamples` | `uint?` | The number of values in the window that should be non-null before computing a result. If None, it will be set equal to window size. |
| `ddof` | `byte` | Delta degrees of freedom. The divisor used in calculations is N - ddof, where N represents the number of elements. |

---

### Select

Run polars expressions without a context. This is syntactic sugar for running df.select on an empty DataFrame.

```csharp
public static LazyFrame Select(params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IntoExprColumn[]` | Column(s) to select, specified as positional arguments. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals. |

---

### SelectEager

Run polars expressions without a context. This is syntactic sugar for running df.select on an empty DataFrame.

```csharp
public static DataFrame SelectEager(params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IntoExprColumn[]` | Column(s) to select, specified as positional arguments. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals. |

---

### SelfDataType

Represents the intrinsic data type of the current element. Equivalent to Python's polars.self_dtype()

```csharp
public static DataTypeExpr SelfDataType()
```

---

### Sql

Create a new SQL Context.

```csharp
public static SqlContext Sql()
```

---

### SqlExpr

Create a Polars Expr from a SQL string.

```csharp
public static Expr SqlExpr(string sql)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `sql` | `string` | The SQL expression string. |

**Returns:** A Polars Expr representing the SQL logic.

---

### SqlExprs

Create an array of Polars Exprs from a collection of SQL strings.

```csharp
public static Expr[] SqlExprs(IEnumerable<string> sqls)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `sqls` | `IEnumerable<string>` | The collection of SQL expression strings. |

**Returns:** An array of Polars Expr objects.

---

### Std

Get the standard deviation. This function is syntactic sugar for Pl.Col(column).Std(ddof).

```csharp
public static Expr Std(string column, byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `string` | Column name. |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1. |

---

### Struct

Struct composite type.

Collect several expressions and combine them into a single Struct column.

```csharp
public static DataType Struct(params Field[] fields)
public static DataType Struct(IReadOnlyDictionary<string, DataType> fields)
public static Expr Struct(params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fields` | `Field[] / IReadOnlyDictionary<string, DataType>` | The fields that make up the struct. Can be either a sequence of Field objects or a mapping of column names to data types. |
| `exprs` | `IntoExprColumn[]` | - |

---

### StructSeries

Collect several expressions and combine them into a single Struct Series

```csharp
public static Series StructSeries(params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IntoExprColumn[]` | - |

---

### Sum

Get the Sum value. Syntactic sugar for Col(names).Sum().

```csharp
public static Expr Sum(params string[] names)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `names` | `string[]` | Name(s) of the columns to use in the aggregation. |

---

### SumHorizontal

Sum all values horizontally across columns.

Sum all values horizontally across columns. Supports passing a collection dynamically.

Sum all values horizontally across columns. (ignore_nulls defaults to true)

Sum all values horizontally across columns, with explicit null-handling.

```csharp
public static Expr SumHorizontal(IEnumerable<Expr> exprs, bool ignoreNulls = true)
public static Expr SumHorizontal(IEnumerable<IntoExprColumn> exprs, bool ignoreNulls = true)
public static Expr SumHorizontal(params IntoExprColumn[] exprs)
public static Expr SumHorizontal(bool ignoreNulls, params IntoExprColumn[] exprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exprs` | `IEnumerable<Expr> / IEnumerable<IntoExprColumn> / IntoExprColumn[]` | An iterable of expressions |
| `ignoreNulls` | `bool` | Whether to ignore null values (default: true). |

---

### Tail

Get the Last n rows. This function is syntactic sugar for Pl.Col(column).Tail(n).

```csharp
public static Expr Tail(string column, int n = 10)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `string` | Column name. |
| `n` | `int` | Number of rows to return |

---

### ThreadPoolSize

Return the number of threads in the Polars thread pool.

```csharp
public static ulong ThreadPoolSize()
```

---

### TimeRange

Generate a time range.

```csharp
public static Expr TimeRange(
    IntoExprColumn? start = null,
    IntoExprColumn? end = null,
    IntoDuration? interval = null,
    ClosedInterval closed = ClosedInterval.Both
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn?` | Lower bound of the time range. If omitted, defaults to TimeOnly.MinValue |
| `end` | `IntoExprColumn?` | Upper bound of the time range. If omitted, defaults to TimeOnly.MaxValue |
| `interval` | `IntoDuration?` | Interval of the range periods |
| `closed` | `ClosedInterval` | Define which sides of the range are closed. |

---

### TimeRangeAsSeries

Generate a time range.

```csharp
public static Series TimeRangeAsSeries(
    IntoExprColumn? start = null,
    IntoExprColumn? end = null,
    IntoDuration? interval = null,
    ClosedInterval closed = ClosedInterval.Both,
    string name = "time"
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn?` | Lower bound of the time range. If omitted, defaults to TimeOnly.MinValue |
| `end` | `IntoExprColumn?` | Upper bound of the time range. If omitted, defaults to TimeOnly.MaxValue |
| `interval` | `IntoDuration?` | Interval of the range periods |
| `closed` | `ClosedInterval` | Define which sides of the range are closed. |
| `name` | `string` | - |

---

### TimeRanges

Create a column of time ranges.

```csharp
public static Expr TimeRanges(
    IntoExprColumn? start = null,
    IntoExprColumn? end = null,
    IntoDuration? interval = null,
    ClosedInterval closed = ClosedInterval.Both
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn?` | Lower bound of the time range. If omitted, defaults to TimeOnly.MinValue |
| `end` | `IntoExprColumn?` | Upper bound of the time range. If omitted, defaults to TimeOnly.MaxValue |
| `interval` | `IntoDuration?` | Interval of the range periods |
| `closed` | `ClosedInterval` | Define which sides of the range are closed. |

---

### TimeRangesAsSeries

Create a column of time ranges.

```csharp
public static Series TimeRangesAsSeries(
    IntoExprColumn? start = null,
    IntoExprColumn? end = null,
    IntoDuration? interval = null,
    ClosedInterval closed = ClosedInterval.Both,
    string name = "time"
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `start` | `IntoExprColumn?` | Lower bound of the time range. If omitted, defaults to TimeOnly.MinValue |
| `end` | `IntoExprColumn?` | Upper bound of the time range. If omitted, defaults to TimeOnly.MaxValue |
| `interval` | `IntoDuration?` | Interval of the range periods |
| `closed` | `ClosedInterval` | Define which sides of the range are closed. |
| `name` | `string` | - |

---

### UnityCatalog

Init Unity Catalog connection

```csharp
public static UnityCatalog UnityCatalog(string workspaceUrl, string bearerToken)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `workspaceUrl` | `string` | Databricks workspaceUrl(Example: https://adb-123.azuredatabricks.net) |
| `bearerToken` | `string` | Personal Access Token (PAT) or OAuth Token |

---

### UnregisterExtensionType

Unregister the extension type for the given extension name.

```csharp
public static void UnregisterExtensionType(string extName)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `extName` | `string` | The registered name. |

---

### Var

Get the variance. This function is syntactic sugar for Pl.Col(column).Var(ddof).

```csharp
public static Expr Var(string column, byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `string` | Column name. |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1. |

---

### When

Start a `when-then-otherwise` expression.

```csharp
public static When When(IntoExprColumn condition)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `condition` | `IntoExprColumn` | - |

---

### Zeros

Construct a column of length n filled with zeros. This is syntactic sugar for the repeat function.

```csharp
public static Expr Zeros(int n, DataType? dtype = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | Length of the resulting column. |
| `dtype` | `DataType?` | Data type of the resulting column. Defaults to Float64. |

---

### ZerosAsSeries

Construct a column of length n filled with zeros. This is syntactic sugar for the repeat function.

```csharp
public static Series ZerosAsSeries(int n, DataType? dtype = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | Length of the resulting column. |
| `dtype` | `DataType?` | Data type of the resulting column. Defaults to Float64. |

---

