---
uid: Polars.CSharp.PolarsRowEnumerable
title: "PolarsRowEnumerable<T>"
---

# PolarsRowEnumerable&lt;T&gt;

Namespace: `Polars.CSharp`

A heap-allocated, lazy IEnumerable wrapper around a Polars DataFrame. Defers row hydration to iteration time and avoids full eager array allocations. Implements IReadOnlyCollection for O(1) Count inspection.

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `Count` | `int` | - |

## Instance Methods

### GetEnumerator

```csharp
public Enumerator<T> GetEnumerator()
```

---

