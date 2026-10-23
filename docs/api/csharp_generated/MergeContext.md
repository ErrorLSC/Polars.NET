---
uid: Polars.CSharp.MergeContext
title: MergeContext
---

# MergeContext

Namespace: `Polars.CSharp`

Provides a context for referencing Source and Target columns safely during a Merge operation.

## Instance Methods

### Source

References a column from the incoming Source DataFrame.

```csharp
public Expr Source(string columnName)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columnName` | `string` | - |

---

### Target

References a column from the existing Target DataFrame.

```csharp
public Expr Target(string columnName)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columnName` | `string` | - |

---

