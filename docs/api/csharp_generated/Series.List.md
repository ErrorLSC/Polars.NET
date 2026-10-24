---
uid: Polars.CSharp.Series.List
title: "Series.List (SeriesListOps)"
---

# Series.List (SeriesListOps)

Namespace: `Polars.CSharp`

Series List Ops Namespace

## Instance Methods

### Agg

Run any polars aggregation expression against the list’ elements.

```csharp
public Series Agg(Expr expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | Expression to run. Note that you can select an element with Pl.Element(). |

---

### All

Evaluate whether all boolean values in a list are true.

```csharp
public Series All()
```

---

### Any

Evaluate whether any boolean value in a list is true.

```csharp
public Series Any()
```

---

### ArgMax

Retrieve the index of the maximum value in every sublist.

```csharp
public Series ArgMax()
```

---

### ArgMin

Retrieve the index of the minimum value in every sublist.

```csharp
public Series ArgMin()
```

---

### Concat

Concat this list series with another list series or expression.

Concat this list series with multiple list series or expressions. Evaluates all concatenations in a single optimized pass.

Concat this list series with a constant array of values.

```csharp
public Series Concat(IntoExprColumn other)
public Series Concat(params IntoExprColumn[] others)
public Series Concat<T>(ReadOnlySpan<T> other)
public Series Concat<T>(IEnumerable<T> other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExprColumn / ReadOnlySpan<T> / IEnumerable<T>` | - |
| `others` | `IntoExprColumn[]` | - |

---

### Contains

Check if the list contains the given item.

```csharp
public Series Contains(Expr item, bool nullsEqual = false)
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
public Series Diff(long n = 1, NullBehavior nullBehavior = NullBehavior.Ignore)
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
public Series DropNulls()
```

---

### Eval

Run any polars expression against the lists’ elements.

```csharp
public Series Eval(Expr expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | Expression to run. Note that you can select an element with Pl.Element(). |

---

### Explode

Returns a column with a separate row for every list element.

```csharp
public Series Explode(bool emptyAsNull = true, bool keepNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `emptyAsNull` | `bool` | Explode an empty list into a null. |
| `keepNulls` | `bool` | Explode a null list into a null. |

**Returns:** Series with the data type of the list elements.

---

### First

Get the first value of the sublists.

```csharp
public Series First()
```

---

### Gather

Take sublists by multiple indices.The indices may be defined in a single column, or by sublists in another column of dtype List.

```csharp
public Series Gather(IntoExprColumn indices, bool nullOnOob = false)
public Series Gather(ReadOnlySpan<int> indices, bool maintainOrder = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `indices` | `IntoExprColumn / ReadOnlySpan<int>` | Indices to return per sublist |
| `nullOnOob` | `bool` | Behavior if an index is out of bounds: True -&gt; set as null False -&gt; raise an error Note that defaulting to raising an error is much cheaper |
| `maintainOrder` | `bool` | - |

---

### GatherEvery

Take every n-th value start from offset in sublists.

```csharp
public Series GatherEvery(IntoExprColumn n, IntoExprColumn offset)
public Series GatherEvery(IntoExprColumn n)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `IntoExprColumn` | Gather every n-th element. |
| `offset` | `IntoExprColumn` | Starting Index |

---

### Get

Get the element at the given index.

```csharp
public Series Get(long index, bool nullOnOob = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `long` | - |
| `nullOnOob` | `bool` | - |

---

### Head

Slice the first n values of every sublist.

```csharp
public Series Head(IntoExprColumn n)
public Series Head(long n = 5)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `IntoExprColumn / long` | Number of values to return for each sublist. |

---

### Join

Join the list elements into a single string with a separator.

```csharp
public Series Join(string separator, bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `separator` | `string` | string to separate the items with |
| `ignoreNulls` | `bool` | Ignore null values (default). If set to False, null values will be propagated. If the sub-list contains any null values, the output is null. |

---

### Last

Get the first value of the sublists.

```csharp
public Series Last()
```

---

### Len

Get the length of the sublists.

```csharp
public Series Len()
```

---

### Max

Calculate the max of the list elements.

```csharp
public Series Max()
```

---

### Mean

Calculate the mean of the list elements.

```csharp
public Series Mean()
```

---

### Median

Calculate the median of the list elements.

```csharp
public Series Median()
```

---

### Min

Calculate the min of the list elements.

```csharp
public Series Min()
```

---

### NUnique

Count the number of unique values in every sub-lists.

```csharp
public Series NUnique()
```

---

### Reverse

Reverse elements in list.

```csharp
public Series Reverse()
```

---

### SampleFrac

Sample from this list.

```csharp
public Series SampleFrac(
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
public Series SampleN(
    IntoExprColumn n,
    bool withReplacement = false,
    bool shuffle = false,
    ulong? seed = null
)

public Series SampleN(bool withReplacement = false, bool shuffle = false, ulong? seed = null)
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
public Series SetDifference(IntoExprColumn other)
public Series SetDifference<T>(ReadOnlySpan<T> other)
public Series SetDifference<T>(IEnumerable<T> other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExprColumn / ReadOnlySpan<T> / IEnumerable<T>` | Right hand side of the set operation. |

---

### SetIntersection

Compute the SET INTERSECTION between the elements in this list and the elements of other.

```csharp
public Series SetIntersection(IntoExprColumn other)
public Series SetIntersection<T>(ReadOnlySpan<T> other)
public Series SetIntersection<T>(IEnumerable<T> other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExprColumn / ReadOnlySpan<T> / IEnumerable<T>` | Right hand side of the set operation. |

---

### SetSymmetricDifference

Compute the SET SYMMETRIC DIFFERENCE between the elements in this list and the elements of other.

```csharp
public Series SetSymmetricDifference(IntoExprColumn other)
public Series SetSymmetricDifference<T>(ReadOnlySpan<T> other)
public Series SetSymmetricDifference<T>(IEnumerable<T> other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExprColumn / ReadOnlySpan<T> / IEnumerable<T>` | Right hand side of the set operation. |

---

### SetUnion

Compute the SET UNION between the elements in this list and the elements of other.

```csharp
public Series SetUnion(IntoExprColumn other)
public Series SetUnion<T>(ReadOnlySpan<T> other)
public Series SetUnion<T>(IEnumerable<T> other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExprColumn / ReadOnlySpan<T> / IEnumerable<T>` | Right hand side of the set operation. |

---

### Shift

Shift list values by the given number of indices.

```csharp
public Series Shift(Expr n)
public Series Shift()
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `Expr` | Number of indices to shift forward. If a negative value is passed, values are shifted in the opposite direction instead. |

---

### Slice

Slice every sublist.

```csharp
public Series Slice(IntoExprColumn offset, IntoExprColumn? length = null)
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
public Series Sort(bool descending = false, bool nullsLast = false, bool maintainOrder = false)
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
public Series Std(byte ddof = 1)
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
public Series Sum()
```

---

### Tail

Slice the last n values of every sublist.

```csharp
public Series Tail(IntoExprColumn n)
public Series Tail(long n = 5)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `IntoExprColumn / long` | Number of values to return for each sublist. |

---

### ToArray

Convert a List column into an Array column with the same inner data type.

```csharp
public Series ToArray(long width)
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

```csharp
public Series ToStruct(params string[] fields)
public Series ToStruct(Func<int, string> nameGenerator, int fieldCount)
public Series ToStruct(int upperBound)
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
public Series Unique(bool maintainOrder = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `maintainOrder` | `bool` | Maintain order of data. This requires more work. |

---

### Var

Get the variance.

```csharp
public Series Var(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1. |

**Returns:** A series which length is 1

---

