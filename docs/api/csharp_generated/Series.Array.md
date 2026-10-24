---
uid: Polars.CSharp.Series.Array
title: "Series.Array (SeriesArrayOps)"
---

# Series.Array (SeriesArrayOps)

Namespace: `Polars.CSharp`

Wrapper for Array (Fixed-Size List) operations on a Series.

## Instance Methods

### Agg

Run any polars aggregation expression against the arrays’ elements.

```csharp
public Series Agg(Expr expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | - |

---

### All

Check if all elements in the sub-array are true.

```csharp
public Series All(bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | - |

---

### Any

Check if any element in the sub-array is true.

```csharp
public Series Any(bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | - |

---

### ArgMax

Get the index of the maximum value in every sub-array.

```csharp
public Series ArgMax()
```

---

### ArgMin

Get the index of the minimum value in every sub-array.

```csharp
public Series ArgMin()
```

---

### Contains

Check if sub-array contains a specific item.

```csharp
public Series Contains(Expr item, bool nullsEqual = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `item` | `Expr` | - |
| `nullsEqual` | `bool` | - |

---

### CountMatches

Count how often the value produced by element occurs.

```csharp
public Series CountMatches(Expr element)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `element` | `Expr` | An expression that produces a single value |

---

### Eval

Run any polars expression against the arrays’ elements.

```csharp
public Series Eval(Expr expr, bool asList = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | Expression to run. Note that you can select an element with pl.element() |
| `asList` | `bool` | Collect the resulting data as a list. This allows for expressions which output a variable amount of data. |

---

### Explode

Explode the array column into multiple rows. The resulting Series will be longer than the original.

```csharp
public Series Explode(bool emptyAsNull = true, bool keepNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `emptyAsNull` | `bool` | - |
| `keepNulls` | `bool` | - |

---

### First

Get the first value of the sub-arrays.

```csharp
public Series First()
```

---

### Get

Get element at index from every sub-array.

```csharp
public Series Get(long index, bool nullOnOob = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `long` | - |
| `nullOnOob` | `bool` | - |

---

### Join

Join elements with a separator.

```csharp
public Series Join(string separator, bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `separator` | `string` | - |
| `ignoreNulls` | `bool` | - |

---

### Last

Get the last value of the sub-arrays.

```csharp
public Series Last()
```

---

### Len

Return the number of elements in each array.

```csharp
public Series Len()
```

---

### Max

Compute the max value of every sub-array.

```csharp
public Series Max()
```

---

### Mean

Compute the mean of every sub-array.

```csharp
public Series Mean()
```

---

### Median

Compute the median of every sub-array.

```csharp
public Series Median()
```

---

### Min

Compute the min value of every sub-array.

```csharp
public Series Min()
```

---

### NUnique

Count the number of unique values in every sub-arrays.

```csharp
public Series NUnique()
```

---

### Reverse

Reverse elements in every sub-array.

```csharp
public Series Reverse()
```

---

### Sort

Sort elements in every sub-array.

```csharp
public Series Sort(bool descending = false, bool nullsLast = false, bool maintainOrder = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `descending` | `bool` | - |
| `nullsLast` | `bool` | - |
| `maintainOrder` | `bool` | - |

---

### Std

Compute the standard deviation of every sub-array.

```csharp
public Series Std(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | - |

---

### Sum

Compute the sum of every sub-array.

```csharp
public Series Sum()
```

---

### ToList

Cast to variable-size List.

```csharp
public Series ToList()
```

---

### ToStruct

Convert array to struct. Useful for splitting embeddings into feature columns.

Convert array to a struct using a function to generate field names dynamically.

```csharp
public Series ToStruct(params string[] fields)
public Series ToStruct(Func<int, string> nameGenerator, int fieldCount)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fields` | `string[]` | - |
| `nameGenerator` | `Func<int, string>` | A function that takes an index and returns a string. |
| `fieldCount` | `int` | The expected number of fields to generate. |

---

### Unique

Get unique elements in every sub-array.

```csharp
public Series Unique(bool maintainOrder = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `maintainOrder` | `bool` | - |

---

### Var

Compute the variance of every sub-array.

```csharp
public Series Var(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | - |

---

