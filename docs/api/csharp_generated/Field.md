---
uid: Polars.CSharp.Field
title: "Field"
---

# Field

Namespace: `Polars.CSharp`

Definition of a single field within a Struct DataType.

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `DataType` | `DataType` | - |
| `Name` | `string` | - |

## Constructors & Static Methods

### Field

Definition of a single field within a Struct DataType.

```csharp
public Field(string name, DataType dataType)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | The name of the field within its parent Struct. |
| `dataType` | `DataType` | The DataType of the field’s values. |

---

## Operators

### operator == (Equality)

```csharp
public static bool operator ==(Field left, Field right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Field` | - |
| `right` | `Field` | - |

---

### implicit operator

```csharp
public static implicit operator Field((string, DataType) tuple)
public static implicit operator Field(KeyValuePair<string, DataType> kvp)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `tuple` | `(string, DataType)` | - |
| `kvp` | `KeyValuePair<string, DataType>` | - |

---

### operator != (Inequality)

```csharp
public static bool operator !=(Field left, Field right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Field` | - |
| `right` | `Field` | - |

---

