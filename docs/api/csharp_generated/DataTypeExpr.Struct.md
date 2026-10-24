---
uid: Polars.CSharp.DataTypeExpr.Struct
title: "DataTypeExpr.Struct (StructNameSpace)"
---

# DataTypeExpr.Struct (StructNameSpace)

Namespace: `Polars.CSharp`

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `this[int index]` | `DataTypeExpr` | Get the DataType of field with a specific field index. |
| `this[long index]` | `DataTypeExpr` | Get the DataType of field with a specific field index. |
| `this[string name]` | `DataTypeExpr` | Get the DataType of field by name. |

## Instance Methods

### FieldDataType

Get the DataType of field with a specific field name.

Get the DataType of field with a specific field index.

```csharp
public DataTypeExpr FieldDataType(string name)
public DataTypeExpr FieldDataType(long index)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | - |
| `index` | `long` | - |

---

### FieldNames

Get the field names in a struct as a list.

```csharp
public Expr FieldNames()
```

---

