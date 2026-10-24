---
uid: Polars.CSharp.Expr.Ext
title: "Expr.Ext (ExtensionOps)"
---

# Expr.Ext (ExtensionOps)

Namespace: `Polars.CSharp`

Operations on extension datatype columns. Access via `Expr.Ext`.

## Instance Methods

### Storage

Get the storage values of an extension data type. If the input does not have an extension data type, it is returned as-is.

```csharp
public Expr Storage()
```

---

### To

Convert to an extension dtype.The input must be of the storage type of the extension dtype.

```csharp
public Expr To(IntoDataTypeExpr dtype)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dtype` | `IntoDataTypeExpr` | - |

---

