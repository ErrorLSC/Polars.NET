---
uid: Polars.CSharp.Expr.List
title: Expr.List (ListOps)
---

# Expr.List (ListOps)

Namespace: `Polars.CSharp`

Operations on List columns. Access via `Expr.List`.

## Instance Methods

### Agg

Run any polars aggregation expression against the list’ elements.

```csharp
public Expr Agg(Expr expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | Expression to run. Note that you can select an element with Pl.Element(). |

---

### All

Evaluate whether all boolean values in a list are true.

```csharp
public Expr All(bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | - |

---

### Any

Evaluate whether any boolean value in a list is true.

```csharp
public Expr Any(bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | - |

---

### ArgMax

Retrieve the index of the maximum value in every sublist.

```csharp
public Expr ArgMax()
```

---

### ArgMin

Retrieve the index of the minimum value in every sublist.

```csharp
public Expr ArgMin()
```

---

### Concat

Concat this list expression with other list expressions, column names, or series. Perfectly matches Python's list[Expr | str] | Expr | str | Series.

Concat this list expression with a constant array of values. The constant array will be broadcasted to every row.

```csharp
public Expr Concat(params IntoExprColumn[] others)
public Expr Concat<T>(ReadOnlySpan<T> other)
public Expr Concat<T>(IEnumerable<T> other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `others` | `IntoExprColumn[]` | Other expressions to append. |
| `other` | `ReadOnlySpan<T> / IEnumerable<T>` | - |

---

### Contains

Check if the list contains a specific item.

```csharp
public Expr Contains(Expr item, bool nullsEqual = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `item` | `Expr` | - |
| `nullsEqual` | `bool` | - |

---

### Diff

Calculate the first discrete difference between shifted items of every sublist.

```csharp
public Expr Diff(long n = 1, NullBehavior nullBehavior = NullBehavior.Ignore)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `long` | Number of slots to shift. |
| `nullBehavior` | `NullBehavior` | How to handle null values. |

---

### DropNulls

Drop all null values in the list. The original order of the remaining elements is preserved.

```csharp
public Expr DropNulls()
```

---

### Eval

Run any polars expression against the lists’ elements.

```csharp
public Expr Eval(Expr expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | Expression to run. Note that you can select an element with Pl.Element(). |

---

### Explode

Returns a column with a separate row for every list element.

```csharp
public Expr Explode(bool emptyAsNull = true, bool keepNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `emptyAsNull` | `bool` | Explode an empty list into a null. |
| `keepNulls` | `bool` | Explode a null list into a null. |

**Returns:** Series with the data type of the list elements.

---

### First

Get the first element of the list.

```csharp
public Expr First()
```

---

### Gather

Take sublists by multiple indices.The indices may be defined in a single column, or by sublists in another column of dtype List.

```csharp
public Expr Gather(IntoExprColumn indices, bool nullOnOob = false)
public Expr Gather(ReadOnlySpan<int> indices, bool nullOnOob = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `indices` | `IntoExprColumn / ReadOnlySpan<int>` | Indices to return per sublist |
| `nullOnOob` | `bool` | Behavior if an index is out of bounds: True -> set as null False -> raise an error Note that defaulting to raising an error is much cheaper |

---

### GatherEvery

Take every n-th value start from offset in sublists.

```csharp
public Expr GatherEvery(IntoExprColumn n, IntoExprColumn offset)
public Expr GatherEvery(IntoExprColumn n)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `IntoExprColumn` | Gather every n-th element. |
| `offset` | `IntoExprColumn` | Starting Index |

---

### Get

Get the value at a specific index.

```csharp
public Expr Get(long index, bool nullOnOob = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `long` | The index to retrieve (can be negative for reverse indexing). |
| `nullOnOob` | `bool` | Behavior if an index is out of bounds: True -> set as null False -> raise an error |

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    student = new[] { "Alice", "Bob", "Charlie" },
    scores = new[] { 
        new[] { 100, 90, 80 },
    Pl.Col("student"),
    Pl.Col("scores").List.Len().Alias("course_count"),
    Pl.Col("scores").List.Sum().Alias("total_score"),
    Pl.Col("scores").List.Get(0).Alias("first_score")
).Show();
/* Output:
shape: (3, 4)
┌─────────┬──────────────┬─────────────┬─────────────┐
│ student ┆ course_count ┆ total_score ┆ first_score │
│ ---     ┆ ---          ┆ ---         ┆ ---         │
│ str     ┆ u32          ┆ i32         ┆ i32         │
╞═════════╪══════════════╪═════════════╪═════════════╡
│ Alice   ┆ 3            ┆ 270         ┆ 100         │
│ Bob     ┆ 2            ┆ 120         ┆ 60          │
│ Charlie ┆ 0            ┆ 0           ┆ null        │
└─────────┴──────────────┴─────────────┴─────────────┘
*/

// To Explode (Flatten) the list, use DataFrame.Explode:
// df.Explode(Pl.Col("scores"));
```

---

### Head

Slice the first n values of every sublist.

```csharp
public Expr Head(IntoExprColumn n)
public Expr Head(long n = 5)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `IntoExprColumn / long` | Number of values to return for each sublist. |

---

### Join

Join the list elements into a single string with a separator.

```csharp
public Expr Join(string separator, bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `separator` | `string` | string to separate the items with |
| `ignoreNulls` | `bool` | Ignore null values (default). If set to False, null values will be propagated. If the sub-list contains any null values, the output is null. |

---

### Last

Get the last element of the list.

```csharp
public Expr Last()
```

---

### Len

Return the number of elements in each list.

```csharp
public Expr Len()
```

---

### Max

Calculate the max of the list elements.

```csharp
public Expr Max()
```

---

### Mean

Calculate the mean of the list elements.

```csharp
public Expr Mean()
```

---

### Median

Calculate the median of the list elements.

```csharp
public Expr Median()
```

---

### Min

Calculate the min of the list elements.

```csharp
public Expr Min()
```

---

### NUnique

Count the number of unique values in every sub-lists.

```csharp
public Expr NUnique()
```

---

### Reverse

Reverse elements in list.

```csharp
public Expr Reverse()
```

---

### SampleFrac

Sample from this list.

```csharp
public Expr SampleFrac(
    IntoExprColumn fraction,
    bool withReplacement = false,
    bool? shuffle = null,
    ulong? seed = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fraction` | `IntoExprColumn` | Fraction of items to return. |
| `withReplacement` | `bool` | Allow values to be sampled more than once. |
| `shuffle` | `bool?` | Shuffle the order of sampled data points. |
| `seed` | `ulong?` | Seed for the random number generator. If set to None (default), a random seed is generated for each sample operation. |

---

### SampleN

Sample from this list.

```csharp
public Expr SampleN(
    IntoExprColumn n,
    bool withReplacement = false,
    bool shuffle = false,
    ulong? seed = null
)

public Expr SampleN(bool withReplacement = false, bool shuffle = false, ulong? seed = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `IntoExprColumn` | Number of items to return.Defaults to 1 |
| `withReplacement` | `bool` | Allow values to be sampled more than once. |
| `shuffle` | `bool` | Shuffle the order of sampled data points. |
| `seed` | `ulong?` | Seed for the random number generator. If set to None (default), a random seed is generated for each sample operation. |

---

### SetDifference

Compute the SET DIFFERENCE between the elements in this list and the elements of other.

```csharp
public Expr SetDifference(IntoExprColumn other)
public Expr SetDifference<T>(ReadOnlySpan<T> other)
public Expr SetDifference<T>(IEnumerable<T> other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExprColumn / ReadOnlySpan<T> / IEnumerable<T>` | Right hand side of the set operation. |

---

### SetIntersection

Compute the SET INTERSECTION between the elements in this list and the elements of other.

```csharp
public Expr SetIntersection(IntoExprColumn other)
public Expr SetIntersection<T>(ReadOnlySpan<T> other)
public Expr SetIntersection<T>(IEnumerable<T> other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExprColumn / ReadOnlySpan<T> / IEnumerable<T>` | Right hand side of the set operation. |

---

### SetSymmetricDifference

Compute the SET SYMMETRIC DIFFERENCE between the elements in this list and the elements of other.

```csharp
public Expr SetSymmetricDifference(IntoExprColumn other)
public Expr SetSymmetricDifference<T>(ReadOnlySpan<T> other)
public Expr SetSymmetricDifference<T>(IEnumerable<T> other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExprColumn / ReadOnlySpan<T> / IEnumerable<T>` | Right hand side of the set operation. |

---

### SetUnion

Compute the SET UNION between the elements in this list and the elements of other.

```csharp
public Expr SetUnion(IntoExprColumn other)
public Expr SetUnion<T>(ReadOnlySpan<T> other)
public Expr SetUnion<T>(IEnumerable<T> other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExprColumn / ReadOnlySpan<T> / IEnumerable<T>` | Right hand side of the set operation. |

---

### Shift

Shift list values by the given number of indices.

```csharp
public Expr Shift(Expr n)
public Expr Shift()
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `Expr` | Number of indices to shift forward. If a negative value is passed, values are shifted in the opposite direction instead. |

---

### Slice

Slice every sublist.

```csharp
public Expr Slice(IntoExprColumn offset, IntoExprColumn? length = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `offset` | `IntoExprColumn` | Start index. Negative indexing is supported. |
| `length` | `IntoExprColumn?` | Length of the slice. If null, the slice is taken to the end of the list. |

---

### Sort

Sort the lists in this column.

```csharp
public Expr Sort(bool descending = false, bool nullsLast = false, bool maintainOrder = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `descending` | `bool` | Sort in descending order. |
| `nullsLast` | `bool` | Place null values last. |
| `maintainOrder` | `bool` | - |

---

### Std

Get the standard deviation.

```csharp
public Expr Std(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1. |

**Returns:** A series which length is 1

---

### Sum

Calculate the sum of the list elements (element-wise).

```csharp
public Expr Sum()
```

---

### Tail

Slice the last n values of every sublist.

```csharp
public Expr Tail(IntoExprColumn n)
public Expr Tail(long n = 5)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `IntoExprColumn / long` | Number of values to return for each sublist. |

---

### ToArray

Convert a List column into an Array column with the same inner data type.

```csharp
public Expr ToArray(long width)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `width` | `long` | Width of the resulting Array column. |

**Returns:** Series of data type Array.

---

### ToStruct

Convert the series of type List to a series of type Struct.

Convert the series of type List to a series of type Struct. using a function to generate field names dynamically.

Convert the list to a struct type.

```csharp
public Expr ToStruct(params string[] fields)
public Expr ToStruct(Func<int, string> nameGenerator, int fieldCount)
public Expr ToStruct(int upperBound)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fields` | `string[]` | Desired field names |
| `nameGenerator` | `Func<int, string>` | A function that takes an index and returns a string. |
| `fieldCount` | `int` | The expected number of fields to generate. |
| `upperBound` | `int` | The maximum number of struct fields to create. Must be provided to allow schema inference if fields are not explicitly named. |

---

### Unique

Get the unique/distinct values in the list.

```csharp
public Expr Unique(bool maintainOrder = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `maintainOrder` | `bool` | Maintain order of data. This requires more work. |

---

### Var

Get the variance.

```csharp
public Expr Var(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1. |

**Returns:** A series which length is 1

---

