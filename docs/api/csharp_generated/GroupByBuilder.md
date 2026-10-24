---
uid: Polars.CSharp.GroupByBuilder
title: "GroupByBuilder"
---

# GroupByBuilder

Namespace: `Polars.CSharp`

Unified builder for DataFrame GroupBy operations (Standard, Dynamic, Rolling)

## Instance Methods

### Agg

Aggregate with specified expressions. Under the hood, this routes through the Lazy engine to maximize performance and optimizations.

```csharp
public DataFrame Agg(params Expr[] aggs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `aggs` | `Expr[]` | Aggregation expressions |

**Returns:** A new aggregated DataFrame

---

### All

Aggregate all columns into lists.

```csharp
public DataFrame All()
```

---

### Count

Count the number of values in each group.

```csharp
public DataFrame Count()
```

---

### First

Aggregate the first values in the group.

```csharp
public DataFrame First(bool ignoreNulls = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | Ignore null values (default False). If set to True, the first non-null value for each aggregation is returned, otherwise None is returned if no non-null value exists. |

---

### GetEnumerator

Allows iteration over the groups of the group by operation.

```csharp
public IEnumerator<(object[], DataFrame)> GetEnumerator()
```

---

### GetGroups

Allows iteration over the groups of the group by operation.(Strong Typed)

```csharp
public IEnumerable<(TKey, DataFrame)> GetGroups<TKey>()
```

---

### Having

Filter groups with a predicate after aggregation.

```csharp
public GroupByBuilder Having(Expr predicate)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `predicate` | `Expr` | - |

---

### Head

```csharp
public DataFrame Head(int n = 10)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | - |

---

### Last

Aggregate the last values in the group.

```csharp
public DataFrame Last(bool ignoreNulls = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | Ignore null values (default False). If set to True, the last non-null value for each aggregation is returned, otherwise None is returned if no non-null value exists. |

---

### Len

Return the number of rows in each group.

```csharp
public DataFrame Len(string name = "len")
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | Assign a name to the resulting column; if unset, defaults to “len”. |

---

### Max

Reduce the groups to the maximal value.

```csharp
public DataFrame Max()
```

---

### Mean

Reduce the groups to the mean value.

```csharp
public DataFrame Mean()
```

---

### Median

Reduce the groups to the median value.

```csharp
public DataFrame Median()
```

---

### Min

Reduce the groups to the minimal value.

```csharp
public DataFrame Min()
```

---

### NUnique

Count the unique values per group.

```csharp
public DataFrame NUnique()
```

---

### Quantile

Compute the quantile per group.

```csharp
public DataFrame Quantile(double quantile, QuantileMethod interpolation = QuantileMethod.Linear)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `quantile` | `double` | Quantile between 0.0 and 1.0. |
| `interpolation` | `QuantileMethod` | Interpolation method. |

---

### Sum

Reduce the groups to the sum.

```csharp
public DataFrame Sum()
```

---

### Tail

```csharp
public DataFrame Tail(int n = 10)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | - |

---

