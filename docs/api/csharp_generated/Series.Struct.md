---
uid: Polars.CSharp.Series.Struct
title: "Series.Struct (SeriesStructOps)"
---

# Series.Struct (SeriesStructOps)

Namespace: `Polars.CSharp`

Series Struct Ops Namespace

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `Fields` | `string[]` | Get the names of the fields. |
| `Schema` | `IReadOnlyDictionary<string, DataType>` | Get the struct definition as a name/dtype schema. |
| `this[long index]` | `Series` | Retrieve a field from the struct by index. |
| `this[string name]` | `Series` | Retrieve one of the fields of this Struct as a new Series. |
| `this[string[] names]` | `Series` | Retrieve a field from the struct by name. Returns a new Series of that field's type. |

## Instance Methods

### Drop

Drop one or more fields from the struct.

```csharp
public Series Drop(IEnumerable<string> names, bool strict = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `names` | `IEnumerable<string>` | Names of the fields to drop. |
| `strict` | `bool` | If True, raise an error if any of the specified fields do not exist in the struct. |

---

### Field

Retrieve a field from the struct by name. Returns a new Series of that field's type.

Retrieve a field from the struct by index.

```csharp
public Series Field(params string[] name)
public Series Field(long index)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string[]` | - |
| `index` | `long` | - |

---

### JsonEncode

Convert struct to JSON string.

```csharp
public Series JsonEncode()
```

---

### RenameFields

Rename the fields of the struct.

```csharp
public Series RenameFields(params string[] names)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `names` | `string[]` | - |

---

### Unnest

Unnest the struct column into a DataFrame. Each field of the struct becomes a separate column.

```csharp
public DataFrame Unnest()
```

---

### WithFields

Add or overwrite fields of this struct.This is similar to with_columns on DataFrame.

```csharp
public Series WithFields(params IntoExprColumn[] expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `IntoExprColumn[]` | Field(s) to add, specified as positional arguments. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals. |

---

