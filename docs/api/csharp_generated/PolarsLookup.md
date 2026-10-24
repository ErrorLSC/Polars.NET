---
uid: Polars.CSharp.PolarsLookup
title: "PolarsLookup<TKey, TElement>"
---

# PolarsLookup&lt;TKey, TElement&gt;

Namespace: `Polars.CSharp.Linq`

ILookup implementation backed by native Polars partitions.

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `Count` | `int` | - |
| `this[TKey key]` | `IEnumerable<TElement>` | - |

## Instance Methods

### Contains

```csharp
public bool Contains(TKey key)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `key` | `TKey` | - |

---

### GetEnumerator

Strongly-typed struct enumerator that enables foreach inference to PolarsGrouping.

```csharp
public Enumerator<TKey, PolarsGrouping<TKey, TElement>> GetEnumerator()
```

---

