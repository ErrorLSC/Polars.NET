---
uid: Polars.CSharp.DataTypeExpr
title: "DataTypeExpr"
---

# DataTypeExpr

Namespace: `Polars.CSharp`

A lazily instantiated DataType that can be used in an Expr. This expression is made to represent a DataType that can be used to reference a datatype in a lazy context.

## Sub-namespaces

| Sub-namespace | Page | Description |
| :--- | :--- | :--- |
| `.Array` | [DataTypeExpr.Array](DataTypeExpr.Array.md) | - |
| `.List` | [DataTypeExpr.List](DataTypeExpr.List.md) | - |
| `.Struct` | [DataTypeExpr.Struct](DataTypeExpr.Struct.md) | - |

## Instance Methods

### Clone

Clone DataTypeExpr

```csharp
public DataTypeExpr Clone()
```

---

### CollectDataType

Materialize the DataTypeExpr in a specific context.

Materialize the DataTypeExpr using a DataFrame's schema as context.

Materialize the DataTypeExpr using a LazyFrame's schema as context.

```csharp
public DataType CollectDataType(PolarsSchema schema)
public DataType CollectDataType(DataFrame df)
public DataType CollectDataType(LazyFrame lf)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `schema` | `PolarsSchema` | - |
| `df` | `DataFrame` | - |
| `lf` | `LazyFrame` | - |

---

### DefaultValue

Get a default value of a specific type. 

Integers and floats are their zero value as default, unless otherwise specified

Temporals are a physical zero as default

Decimal is zero as default

String and Binary are an empty string

List is an empty list, unless otherwise specified

Array is the inner default value repeated over the shape

Struct is the inner default value for all fields

Enum is the first category if it exists

Null, Object and Categorical are null.

```csharp
public Expr DefaultValue(int n = 1, bool numericToOne = false, int numListValues = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | Number of types you want the value |
| `numericToOne` | `bool` | Use 1 instead of 0 as the default value for numeric types |
| `numListValues` | `int` | The amount of values a list contains |

---

### Display

Get a formatted version of the output DataType.

```csharp
public Expr Display()
```

---

### InnerDataType

Get the inner DataType of a List or Array.

```csharp
public DataTypeExpr InnerDataType()
```

---

### Matches

Get whether the output DataType is matches a certain selector.

```csharp
public Expr Matches(Selector selector)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `selector` | `Selector` | - |

---

### ToLiteral

Convert DataTypeExpr into DataType Literal. Returns null if the expression is not a literal (e.g. SelfDtype).

```csharp
public DataType ToLiteral()
```

**Returns:** A DataType instance or null.

---

### ToSignedInteger

Get the signed integer version of the same bitsize.

```csharp
public DataTypeExpr ToSignedInteger()
```

---

### ToUnsignedInteger

Get the unsigned integer version of the same bitsize.

```csharp
public DataTypeExpr ToUnsignedInteger()
```

---

### WrapInArray

Get the DataType wrapped in an array.

```csharp
public DataTypeExpr WrapInArray(uint width)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `width` | `uint` | Array Width |

---

### WrapInList

Get the DataType wrapped in a list.

```csharp
public DataTypeExpr WrapInList()
```

---

