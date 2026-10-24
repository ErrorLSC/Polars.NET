---
uid: Polars.CSharp.PolarsGrouping
title: "PolarsGrouping<TKey, TElement>"
---

# PolarsGrouping&lt;TKey, TElement&gt;

Namespace: `Polars.CSharp.Linq`

Represents a group partition backed by a sliced Polars DataFrame.

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `Count` | `int` | Gets the total number of elements in this group partition in O(1). |
| `DataFrame` | `DataFrame` | Gets the underlying partition DataFrame representing this group. |
| `Key` | `TKey` | - |
| `this[int index]` | `TElement` | - |

## Instance Methods

### GetEnumerator

```csharp
public IEnumerator<TElement> GetEnumerator()
```

---

