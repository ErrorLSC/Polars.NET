---
uid: Polars.CSharp.Series.Ext
title: Series.Ext (SeriesExtensionOps)
---

# Series.Ext (SeriesExtensionOps)

Namespace: `Polars.CSharp`

Wrapper for extensio datatype operations on a Series.

## Instance Methods

### Storage

Get the storage values of an extension data type. If the input does not have an extension data type, it is returned as-is.

```csharp
public Series Storage()
```

---

### To

Convert to an extension dtype.The input must be of the storage type of the extension dtype.

```csharp
public Series To(IntoDataTypeExpr dtype)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dtype` | `IntoDataTypeExpr` | - |

---

