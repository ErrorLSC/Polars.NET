---
uid: Polars.CSharp.LazyGroupBy
title: LazyGroupBy
---

# LazyGroupBy

Namespace: `Polars.CSharp`

A unified builder for Standard, Dynamic, and Rolling GroupBy operations.

## Instance Methods

### Agg

Apply aggregations to the group.

```csharp
public LazyFrame Agg(params Expr[] aggs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `aggs` | `Expr[]` | - |

---

### All

Aggregate all columns into lists.

```csharp
public LazyFrame All()
```

---

### Count

Count the number of values in each group.

```csharp
public LazyFrame Count()
```

---

### First

Aggregate the first values in the group.

```csharp
public LazyFrame First(bool ignoreNulls = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | Ignore null values (default False). If set to True, the first non-null value for each aggregation is returned, otherwise None is returned if no non-null value exists. |

---

### Having

Filter groups with a list of predicates after aggregation. Using this method is equivalent to adding the predicates to the aggregation and filtering afterwards.

```csharp
public LazyGroupBy Having(Expr predicate)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `predicate` | `Expr` | Expressions that evaluate to a boolean value for each group. Typically, this requires the use of an aggregation function. |

---

### Head

Get the first n rows of each group.

```csharp
public LazyFrame Head(int n = 10)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | - |

---

### Last

Aggregate the last values in the group.

```csharp
public LazyFrame Last(bool ignoreNulls = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | Ignore null values (default False). If set to True, the last non-null value for each aggregation is returned, otherwise None is returned if no non-null value exists. |

---

### Len

Return the number of rows in each group.

```csharp
public LazyFrame Len(string name = "len")
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | Assign a name to the resulting column; if unset, defaults to “len”. |

---

### Max

Reduce the groups to the maximal value.

```csharp
public LazyFrame Max()
```

---

### Mean

Reduce the groups to the mean value.

```csharp
public LazyFrame Mean()
```

---

### Median

Reduce the groups to the median value.

```csharp
public LazyFrame Median()
```

---

### Min

Reduce the groups to the minimal value.

```csharp
public LazyFrame Min()
```

---

### NUnique

Count the unique values per group.

```csharp
public LazyFrame NUnique()
```

---

### Quantile

Compute the quantile per group.

```csharp
public LazyFrame Quantile(double quantile, QuantileMethod interpolation = QuantileMethod.Linear)
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
public LazyFrame Sum()
```

---

### Tail

Get the last n rows of each group.

```csharp
public LazyFrame Tail(int n = 10)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | - |

---

