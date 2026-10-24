---
uid: Polars.CSharp.Expr.Array
title: "Expr.Array (ArrayOps)"
---

# Expr.Array (ArrayOps)

Namespace: `Polars.CSharp`

Offers methods for accessing fields within array columns.

## Instance Methods

### Agg

Run any polars aggregation expression against the arrays’ elements.

```csharp
public Expr Agg(Expr expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | - |

---

### All

Check if all elements in the sub-array are true.

```csharp
public Expr All(bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | - |

---

### Any

Check if any element in the sub-array is true.

```csharp
public Expr Any(bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | - |

---

### ArgMax

Get the index of the maximum value in every sub-array.

```csharp
public Expr ArgMax()
```

---

### ArgMin

Get the index of the minimum value in every sub-array.

```csharp
public Expr ArgMin()
```

---

### Concat

Concat this Array expression with other Array expressions.

```csharp
public Expr Concat(params Expr[] others)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `others` | `Expr[]` | Other list expressions to append. |

---

### Contains

Check if sub-array contains a specific item.

```csharp
public Expr Contains(Expr item, bool nullsEqual = false)
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
public Expr CountMatches(Expr element)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `element` | `Expr` | An expression that produces a single value |

---

### Eval

Run any polars expression against the arrays’ elements.

```csharp
public Expr Eval(Expr expr, bool asList = false)
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
public Expr Explode(bool emptyAsNull = true, bool keepNulls = true)
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
public Expr First()
```

---

### Get

Get element at index from every sub-array.

```csharp
public Expr Get(long index, bool nullOnOob = true)
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
public Expr Join(string separator, bool ignoreNulls = true)
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
public Expr Last()
```

---

### Len

Return the number of elements in each array.

```csharp
public Expr Len()
```

---

### Max

Compute the max value of every sub-array.

```csharp
public Expr Max()
```

---

### Mean

Compute the mean of every sub-array.

```csharp
public Expr Mean()
```

---

### Median

Compute the median of every sub-array.

```csharp
public Expr Median()
```

---

### Min

Compute the min value of every sub-array.

```csharp
public Expr Min()
```

---

### NUnique

Count the number of unique values in every sub-arrays.

```csharp
public Expr NUnique()
```

---

### Reverse

Reverse elements in every sub-array.

```csharp
public Expr Reverse()
```

---

### Sort

Sort elements in every sub-array.

```csharp
public Expr Sort(bool descending = false, bool nullsLast = false, bool maintainOrder = false)
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
public Expr Std(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | - |

---

### Sum

Compute the sum of every sub-array.

```csharp
public Expr Sum()
```

---

### ToList

Cast to variable-size List.

```csharp
public Expr ToList()
```

---

### ToStruct

Convert array to struct.

Convert array to a struct using a function to generate field names dynamically.

```csharp
public Expr ToStruct(params string[] fields)
public Expr ToStruct(Func<int, string> nameGenerator, int fieldCount)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fields` | `string[]` | Desired field names |
| `nameGenerator` | `Func<int, string>` | A function that takes an index and returns a string. |
| `fieldCount` | `int` | The expected number of fields to generate. |

---

### Unique

Get unique elements in every sub-array.

```csharp
public Expr Unique(bool maintainOrder = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `maintainOrder` | `bool` | - |

---

### Var

Compute the variance of every sub-array.

```csharp
public Expr Var(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | - |

---

