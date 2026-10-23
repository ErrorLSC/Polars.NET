---
uid: Polars.CSharp.LinqExtensions
title: LinqExtensions (IQueryable & Polars LINQ)
---

# LinqExtensions (IQueryable & Polars LINQ)

Namespace: `Polars.CSharp.Linq`

Provides LINQ IQueryable extensions for C# Polars DataFrame and LazyFrame.

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `DefaultInspector` | `Action<string>` | Gets or sets the default plan inspector action used when none is explicitly specified. Defaults to `System.Console.WriteLine`. |

## Constructors & Static Methods

### AsAsyncEnumerable

Returns the input typed as IAsyncEnumerable for streaming async enumeration.

```csharp
public static IAsyncEnumerable<T> AsAsyncEnumerable<T>(IQueryable<T> source)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `source` | `IQueryable<T>` | - |

---

### AsEnumerable

Exposes a Series as a strongly-typed IEnumerable sequence of elements. For Struct Series, elements are materialized into objects matching the struct fields. For scalar Series, elements are materialized directly.

```csharp
public static IEnumerable<T> AsEnumerable<T>(Series series)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `series` | `Series` | - |

---

### AsPolarsQueryable

Converts a Series into an IQueryable LINQ query provider backed by Polars.

Converts a Series into an IQueryable LINQ query provider backed by Polars, inferring the anonymous type or entity type directly from a sample sequence.

Convert DataReader to Polars Queryable

Transposes an in-memory sequence into a native Polars LazyFrame and exposes it as a Polars LINQ queryable.

```csharp
public static IQueryable<T> AsPolarsQueryable<T>(Series series)
public static IQueryable<T> AsPolarsQueryable<T>(Series series, IEnumerable<T> source)
public static IQueryable<T> AsPolarsQueryable<T>(IDataReader reader, int batchSize = 50000)
public static IQueryable<T> AsPolarsQueryable<T>(IEnumerable<T> source)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `series` | `Series` | - |
| `source` | `IEnumerable<T>` | The in-memory collection |
| `reader` | `IDataReader` | - |
| `batchSize` | `int` | - |

**Returns:** A queryable supported by the Polars native execution engine

---

### AsQueryable

Converts a LazyFrame into an IQueryable LINQ query provider. Operations will be translated to Polars Expressions and compiled into execution plans.

Converts an eager DataFrame into an IQueryable LINQ query provider backed by LazyFrame.

Converts a DataFrame into an IQueryable LINQ query provider, inferring the anonymous type or entity type directly from a sample sequence (e.g. source array).

Converts a LazyFrame into an IQueryable LINQ query provider, inferring the anonymous type or entity type directly from a sample sequence.

Converts a Series into an IQueryable LINQ query provider backed by Polars. For Struct Series, it is unnested into a multi-column DataFrame matching the struct fields. For scalar and other Series, it is converted into a single-column DataFrame.

Converts a Series into an IQueryable LINQ query provider, inferring the anonymous type or entity type directly from a sample sequence.

```csharp
public static IQueryable<T> AsQueryable<T>(LazyFrame lf)
public static IQueryable<T> AsQueryable<T>(DataFrame df)
public static IQueryable<T> AsQueryable<T>(DataFrame df, IEnumerable<T> source)
public static IQueryable<T> AsQueryable<T>(LazyFrame lf, IEnumerable<T> source)
public static IQueryable<T> AsQueryable<T>(Series series)
public static IQueryable<T> AsQueryable<T>(Series series, IEnumerable<T> source)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `lf` | `LazyFrame` | - |
| `df` | `DataFrame` | The source DataFrame. |
| `source` | `IEnumerable<T>` | The source collection used solely for static type inference. |
| `series` | `Series` | - |

**Returns:** A strongly-typed PolarsQuery provider.

---

### Average

Computes the average of the column.

Computes the average of the nullable column.

```csharp
public static double Average<T>(T column)
public static double Average<T>(T? column)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T / T?` | - |

---

### Count

Counts the number of non-null values in the column.

Counts the number of non-null values in the nullable column.

```csharp
public static int Count<T>(T column)
public static int Count<T>(T? column)
public static int Count(string? column)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T / T? / string?` | - |

---

### CumCount

Computes the cumulative count of the column.

```csharp
public static long CumCount<T>(T? column, bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |
| `reverse` | `bool` | - |

---

### CumMax

Computes the cumulative maximum of the column.

```csharp
public static T CumMax<T>(T? column, bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |
| `reverse` | `bool` | - |

---

### CumMin

Computes the cumulative minimum of the column.

```csharp
public static T CumMin<T>(T? column, bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |
| `reverse` | `bool` | - |

---

### CumProd

Computes the cumulative product of the column.

```csharp
public static T CumProd<T>(T? column, bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |
| `reverse` | `bool` | - |

---

### CumSum

Computes the cumulative sum of the column.

```csharp
public static T CumSum<T>(T? column, bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |
| `reverse` | `bool` | - |

---

### Diff

Computes the difference between the current value and the value n periods ago, with a specified null behavior.

```csharp
public static T Diff<T>(T? column)
public static T Diff<T>(T? column, long n)
public static T Diff<T>(T? column, long n, NullBehavior nullBehavior)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | The column to compute differences for. |
| `n` | `long` | The number of periods to look back. |
| `nullBehavior` | `NullBehavior` | The behavior for handling null values. |

**Returns:** The column with differences computed.

---

### Explain

Returns the Polars native query execution plan, including warnings if parts of the query fallback to managed client-side execution.

```csharp
public static string Explain<T>(IQueryable<T> source, bool optimized = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `source` | `IQueryable<T>` | The source queryable. |
| `optimized` | `bool` | Whether to display the Polars optimized logical plan or the raw plan. |

---

### InspectPlan

Inspects the query plan using the default inspector (`Linq.LinqExtensions.DefaultInspector`)

Inspects the query plan at the current pipeline stage and invokes a custom inspector action,

```csharp
public static IQueryable<T> InspectPlan<T>(IQueryable<T> source, bool optimized = true)
public static IQueryable<T> InspectPlan<T>(IQueryable<T> source, Action<string> inspector, bool optimized = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `source` | `IQueryable<T>` | The source queryable. |
| `optimized` | `bool` | Whether to display the optimized plan. Defaults to true. |
| `inspector` | `Action<string>` | Action receiving the diagnostic plan string. |

---

### Interpolate

Interpolates null values using the specified user-facing InterpolationMethod (defaults to Linear).

```csharp
public static T Interpolate<T>(T? column, InterpolationMethod method = InterpolationMethod.Linear)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |
| `method` | `InterpolationMethod` | - |

---

### InterpolateBy

Interpolates null values based on the values of another column (e.g., timestamps or indices).

```csharp
public static T InterpolateBy<T, TBy>(T? column, TBy? byColumn)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |
| `byColumn` | `TBy?` | - |

---

### Max

Finds the maximum value in the column.

Finds the maximum value in the nullable column.

Finds the maximum value in the string column.

```csharp
public static T Max<T>(T column)
public static T? Max<T>(T? column)
public static string Max(string? column)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T / T? / string?` | - |

---

### Mean

Computes the mean of the column.

Computes the mean of the nullable column.

```csharp
public static double Mean<T>(T? column)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |

---

### Median

Computes the median value of the column.

```csharp
public static double? Median<T>(T? column)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |

---

### Min

Finds the minimum value in the column.

Finds the minimum value in the nullable column.

Finds the minimum value in the string column.

```csharp
public static T Min<T>(T column)
public static T? Min<T>(T? column)
public static string Min(string? column)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T / T? / string?` | - |

---

### Over

Computes a windowed expression over a specified partition and order, with an optional descending order.

```csharp
public static T Over<T, TPartition>(T? expr, TPartition? partitionBy)
public static T Over<T>(T? expr, params object[] partitionBy)
public static T Over<T, TPartition, TOrder>(
    T? expr,
    TPartition? partitionBy,
    TOrder? orderBy,
    bool descending = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `T?` | The expression to compute the windowed operation for. |
| `partitionBy` | `TPartition? / object[]` | The column(s) to partition by. |
| `orderBy` | `TOrder?` | The column(s) to order by. |
| `descending` | `bool` | Indicates whether to sort in descending order. |

**Returns:** The column with the windowed expression computed.

---

### PctChange

Computes the percentage change between current and previous values.

```csharp
public static double? PctChange<T>(T? column, long n = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |
| `n` | `long` | - |

---

### Quantile

Computes the quantile of the column with the specified probability and method.

```csharp
public static double? Quantile<T>(T? column, double quantile, QuantileMethod method = QuantileMethod.Nearest)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |
| `quantile` | `double` | - |
| `method` | `QuantileMethod` | - |

---

### Rank

Computes the rank of values using the default Average method.

Computes the rank of values with a specified sorting order.

Computes the rank of values using user-facing RankMethod.

Computes the rank of values using user-facing RankMethod with seed.

```csharp
public static double Rank<T>(T? column)
public static double Rank<T>(T? column, bool descending)
public static double Rank<T>(T? column, RankMethod method, bool descending = false)
public static double Rank<T>(
    T? column,
    RankMethod method,
    bool descending,
    ulong? seed
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |
| `descending` | `bool` | - |
| `method` | `RankMethod` | - |
| `seed` | `ulong?` | - |

---

### Shift

Shifts the values in a column by a specified offset, introducing nulls for the shifted positions.

```csharp
public static T Shift<T>(T? column, long offset)
public static T Shift<T, TOffset>(T? column, TOffset? offsetColumn)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | The column to shift. |
| `offset` | `long` | The offset by which to shift the values. |
| `offsetColumn` | `TOffset?` | - |

**Returns:** The shifted column.

---

### Std

Computes the sample standard deviation (ddof = 1 by default).

Computes sample standard deviation of a group by selector with default ddof = 1.

Computes standard deviation of a group by selector with explicit degree of freedom (ddof).

```csharp
public static double Std<T>(T? column, byte ddof = 1)
public static double Std<TSource, TResult>(this IEnumerable<TSource> source, Expression<Func<TSource, TResult>> selector)
public static double Std<TSource, TResult>(this IEnumerable<TSource> source, Expression<Func<TSource, TResult>> selector, byte ddof)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |
| `ddof` | `byte` | - |
| `source` | `IEnumerable<TSource>` | - |
| `selector` | `Expression<Func<TSource, TResult>>` | - |

---

### Sum

Computes the sum of the column.

Computes the sum of the nullable column.

```csharp
public static T Sum<T>(T column)
public static T? Sum<T>(T? column)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T / T?` | - |

---

### ToArrayAsync

Asynchronously streams the query results and materializes them into an Array.

```csharp
public static Task<T[]> ToArrayAsync<T>(IQueryable<T> source, CancellationToken cancellationToken = default)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `source` | `IQueryable<T>` | - |
| `cancellationToken` | `CancellationToken` | - |

---

### ToDataFrame

Materializes an in-memory sequence of entities into an eager DataFrame. If the sequence is an IQueryable, native query execution and compilation is prioritized.

Compiles the LINQ query pipeline, executes Collect, and returns the resulting eager DataFrame.

```csharp
public static DataFrame ToDataFrame<T>(IEnumerable<T> source)
public static DataFrame ToDataFrame<T>(IQueryable<T> query)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `source` | `IEnumerable<T>` | The sequence of elements. |
| `query` | `IQueryable<T>` | - |

**Returns:** A new DataFrame containing the elements.

---

### ToDataFrameAsync

Asynchronously compiles and executes the query, materializing the result directly into a native Polars DataFrame.

Asynchronously drains an IAsyncEnumerable sequence of objects directly into a Polars DataFrame.

```csharp
public static Task<DataFrame> ToDataFrameAsync<T>(IQueryable<T> source, Engine engine = Engine.Auto, CancellationToken cancellationToken = default)
public static Task<DataFrame> ToDataFrameAsync<T>(IAsyncEnumerable<T> source, CancellationToken cancellationToken = default)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `source` | `IQueryable<T> / IAsyncEnumerable<T>` | The source queryable. |
| `engine` | `Engine` | Execution engine selection (Auto, InMemory, Streaming, or Gpu). |
| `cancellationToken` | `CancellationToken` | Optional cooperative cancellation token. |

**Returns:** A task yielding the resulting DataFrame.

---

### ToLazyFrame

Compiles the LINQ query pipeline and converts it back to a Polars LazyFrame.

Converts an in-memory sequence of entities into a LazyFrame. If the sequence is an IQueryable, native query execution and compilation is prioritized.

```csharp
public static LazyFrame ToLazyFrame<T>(IQueryable<T> query)
public static LazyFrame ToLazyFrame<T>(IEnumerable<T> source)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `query` | `IQueryable<T>` | - |
| `source` | `IEnumerable<T>` | The sequence of elements. |

**Returns:** A new LazyFrame containing the elements.

---

### ToListAsync

Asynchronously enumerates the query and materializes all results into a List.

```csharp
public static Task<List<T>> ToListAsync<T>(IQueryable<T> source, CancellationToken cancellationToken = default)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `source` | `IQueryable<T>` | - |
| `cancellationToken` | `CancellationToken` | - |

---

### ToLookup

Converts a queryable sequence into a PolarsLookup, grouping elements by a specified key selector and selecting elements with a specified selector.

```csharp
public static PolarsLookup<TKey, TSource> ToLookup<TSource, TKey>(IQueryable<TSource> source, Expression<Func<TSource, TKey>> keySelector, IEqualityComparer<TKey>? comparer = null)
public static PolarsLookup<TKey, TElement> ToLookup<TSource, TKey, TElement>(
    IQueryable<TSource> source,
    Expression<Func<TSource, TKey>> keySelector,
    Expression<Func<TSource, TElement>> elementSelector,
    IEqualityComparer<TKey>? comparer = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `source` | `IQueryable<TSource>` | The queryable sequence to convert. |
| `keySelector` | `Expression<Func<TSource, TKey>>` | The function to extract the key from each element. |
| `comparer` | `IEqualityComparer<TKey>?` | The equality comparer to use for the keys. |
| `elementSelector` | `Expression<Func<TSource, TElement>>` | The function to extract the element from each element. |

**Returns:** The resulting PolarsLookup.

---

### ToSeries

Materializes an in-memory sequence of entities into a Polars Series. Object collections(non-scalar collections) will be converted into a Struct Series.

Compiles a LINQ query and materializes it into a Polars Series. If the query yields a single column, returns that column directly. If the query yields multiple columns, assembles them into a native Struct Series.

```csharp
public static Series ToSeries<T>(IEnumerable<T> source, string name = "")
public static Series ToSeries<T>(IQueryable<T> query, string name = "")
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `source` | `IEnumerable<T>` | The sequence or query of elements. |
| `name` | `string` | Optional name for the Series. Defaults to empty string. |
| `query` | `IQueryable<T>` | The query sequence. |

**Returns:** A new Series containing the materialized values. / A new Series containing the query result.

---

### Var

Computes the sample variance (ddof = 1 by default).

Computes sample variance of a group by selector with default ddof = 1.

Computes variance of a group by selector with explicit degree of freedom (ddof).

```csharp
public static double Var<T>(T? column, byte ddof = 1)
public static double Var<TSource, TResult>(this IEnumerable<TSource> source, Expression<Func<TSource, TResult>> selector)
public static double Var<TSource, TResult>(this IEnumerable<TSource> source, Expression<Func<TSource, TResult>> selector, byte ddof)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `column` | `T?` | - |
| `ddof` | `byte` | - |
| `source` | `IEnumerable<TSource>` | - |
| `selector` | `Expression<Func<TSource, TResult>>` | - |

---

