---
uid: Polars.CSharp.Expr.Struct
title: Expr.Struct (StructOps)
---

# Expr.Struct (StructOps)

Namespace: `Polars.CSharp`

Operations on Struct columns. Access via `Expr.Struct`.

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `this[long index]` | `Expr` | Retrieve a field by its index. |
| `this[string name]` | `Expr` | Retrieve one of the fields of this Struct as a new expr. |
| `this[string[] name]` | `Expr` | Retrieve a field from the struct by name. |

## Instance Methods

### Drop

Drop one or more fields from the struct.

```csharp
public Expr Drop(IEnumerable<string> names, bool strict = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `names` | `IEnumerable<string>` | Names of the fields to drop. |
| `strict` | `bool` | If True, raise an error if any of the specified fields do not exist in the struct. |

---

### Field

Retrieve a field from the struct by name.

Retrieve a field by its index.

```csharp
public Expr Field(params string[] name)
public Expr Field(long index)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string[]` | The name of the field. |
| `index` | `long` | - |

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    id = new[] { 1, 2 },
    product = new[] 
    { 
        new { Name = "Laptop", Specs = new { Ram = 16, SSD = 512 } },
        new { Name = "Mouse",  Specs = new { Ram = 0,  SSD = 0   } }
    }
});

df.Select(
    Pl.Col("id"),
    Pl.Col("product").Struct.Field("Name").Alias("prod_name"),
    // Nested Access
    Pl.Col("product").Struct.Field("Specs").Struct.Field("Ram").Alias("ram_gb")
).Show();
/* Output:
shape: (2, 3)
┌─────┬───────────┬────────┐
│ id  ┆ prod_name ┆ ram_gb │
│ --- ┆ ---       ┆ ---    │
│ i32 ┆ str       ┆ i32    │
╞═════╪═══════════╪════════╡
│ 1   ┆ Laptop    ┆ 16     │
│ 2   ┆ Mouse     ┆ 0      │
└─────┴───────────┴────────┘
*/
```

---

### JsonEncode

Convert the struct column into a JSON string column. Useful for debugging or exporting to systems that support JSON strings.

```csharp
public Expr JsonEncode()
```

---

### RenameFields

Rename the fields of the struct.

```csharp
public Expr RenameFields(params string[] names)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `names` | `string[]` | The new names for the fields. |

**Example**

```csharp
df.Select(
    Pl.Col("product").Struct.RenameFields(["NewName", "NewSpecs"])
);
```

---

### Unnest

Expand the struct into its individual fields.Alias for Expr.Struct.field("*").

```csharp
public Expr Unnest()
```

---

### WithFields

Add or overwrite fields of this struct.This is similar to with_columns on DataFrame.

```csharp
public Expr WithFields(params IntoExprColumn[] fields)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fields` | `IntoExprColumn[]` | - |

---

