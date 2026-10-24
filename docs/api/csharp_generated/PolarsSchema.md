---
uid: Polars.CSharp.PolarsSchema
title: "PolarsSchema"
---

# PolarsSchema

Namespace: `Polars.CSharp`

Represents a Polars Schema (Name -&gt; DataType mapping).

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `ColumnNames` | `IReadOnlyList<string>` | - |
| `Count` | `int` | - |
| `DataTypes` | `IReadOnlyList<DataType>` | Gets the list of data types for the columns in the schema. |
| `Keys` | `IEnumerable<string>` | - |
| `Names` | `IReadOnlyList<string>` | - |
| `Values` | `IEnumerable<DataType>` | - |
| `this[string name]` | `DataType` | - |

## Constructors & Static Methods

### PolarsSchema

Create a new empty Schema.

```csharp
public PolarsSchema()
```

---

### From

Create a Schema from a Dictionary.

Create a schema from a collection of named fields.

Create a Schema directly from a .NET type.

Create a Schema directly from a System.Type.

```csharp
public static PolarsSchema From(IReadOnlyDictionary<string, DataType> fields)
public static PolarsSchema From(IEnumerable<(string, DataType)> fields)
public static PolarsSchema From<T>()
public static PolarsSchema From(Type type)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fields` | `IReadOnlyDictionary<string, DataType> / IEnumerable<(string, DataType)>` | - |
| `type` | `Type` | - |

---

### FromArrowSchema

Creates a Polars Schema from an Apache.Arrow.Schema.

```csharp
public static PolarsSchema FromArrowSchema(Schema arrowSchema)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `arrowSchema` | `Schema` | - |

---

## Operators

### operator == (Equality)

```csharp
public static bool operator ==(PolarsSchema? left, PolarsSchema? right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `PolarsSchema?` | - |
| `right` | `PolarsSchema?` | - |

---

### operator != (Inequality)

```csharp
public static bool operator !=(PolarsSchema? left, PolarsSchema? right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `PolarsSchema?` | - |
| `right` | `PolarsSchema?` | - |

---

## Instance Methods

### Add

Add a field to the schema.

```csharp
public PolarsSchema Add(string name, DataType dtype)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | Column name. |
| `dtype` | `DataType` | Column data type. |

**Returns:** The schema instance (Fluent API).

---

### ContainsKey

```csharp
public bool ContainsKey(string key)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `key` | `string` | - |

---

### GetEnumerator

```csharp
public IEnumerator<Field> GetEnumerator()
```

---

### Len

```csharp
public int Len()
```

---

### ToArrowSchema

Convert a Polars Schema to an Apache.Arrow.Schema.

```csharp
public Schema ToArrowSchema()
```

---

### ToDataFrame

Create an empty (n=0) or n-row null-filled (n&gt;0) copy of the DataFrame. Returns a n-row null-filled DataFrame with an identical schema. n can be greater than the current number of rows in the DataFrame.

```csharp
public DataFrame ToDataFrame(long length = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `length` | `long` | - |

**Returns:** An empty DataFrame with columns and types matching the schema.

---

### ToFrozenDictionary

Converts the Schema to a FrozenDictionary. Slower creation time, but blazing fast read performance. Ideal for caching.

```csharp
public FrozenDictionary<string, DataType> ToFrozenDictionary()
```

---

### ToHtml

Generates a clean HTML table representation of the Schema for interactive environments.

```csharp
public string ToHtml()
```

**Returns:** An HTML formatted string representing the schema fields and types.

---

### ToLazyFrame

Create an empty copy of the current LazyFrame, with zero to ‘n’ rows. Returns a copy with an identical schema but no data.

```csharp
public LazyFrame ToLazyFrame(long length = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `length` | `long` | - |

**Returns:** An empty DataFrame with columns and types matching the schema.

---

### ToList

Returns the schema as an ordered list of fields.

```csharp
public IReadOnlyList<(string, DataType)> ToList()
```

---

### TryGetValue

```csharp
public bool TryGetValue(string key, out DataType value)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `key` | `string` | - |
| `value` | `DataType` | - |

---

