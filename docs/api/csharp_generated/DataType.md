---
uid: Polars.CSharp.DataType
title: "DataType"
---

# DataType

Namespace: `Polars.CSharp`

Represents a Polars data type. Wraps the underlying Rust DataType Handle and provides high-level metadata.

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `ArrayShape` | `uint[]` | The shape of the array |
| `ArrayWidth` | `ulong` | If this is an Array type, returns the fixed width. Returns 0 if not an Array type. |
| `Binary` | `DataType` | - |
| `Boolean` | `DataType` | - |
| `Categories` | `Categories` | Return underlying Categories for categorical type, for other types returns null |
| `Date` | `DataType` | - |
| `EnumCategories` | `FrozenCategories` | Return underlying FrozenCategories for enum type, for other types returns null |
| `Float16` | `DataType` | - |
| `Float32` | `DataType` | - |
| `Float64` | `DataType` | - |
| `InnerType` | `DataType` | Return the inner type of list/array. Non-List/Array input will return null. |
| `Int128` | `DataType` | - |
| `Int16` | `DataType` | - |
| `Int32` | `DataType` | - |
| `Int64` | `DataType` | - |
| `Int8` | `DataType` | - |
| `IsCategorical` | `bool` | - |
| `IsDecimal` | `bool` | - |
| `IsEnum` | `bool` | - |
| `IsExtension` | `bool` | - |
| `IsFloat` | `bool` | - |
| `IsInteger` | `bool` | - |
| `IsNested` | `bool` | - |
| `IsNumeric` | `bool` | Returns true if the type is a numeric type. |
| `IsSignedInteger` | `bool` | - |
| `IsTemporal` | `bool` | - |
| `IsUnsignedInteger` | `bool` | - |
| `Kind` | `DataTypeKind` | Gets the high-level kind of this data type. |
| `Null` | `DataType` | - |
| `Precision` | `int` | Gets the decimal precision. |
| `SameAsInput` | `DataType` | - |
| `Scale` | `int` | Gets the decimal scale. |
| `String` | `DataType` | - |
| `StructFields` | `IReadOnlyList<Field>` | Gets the fields of a Struct type. Returns null if this DataType is not a Struct. |
| `Time` | `DataType` | - |
| `TimeUnit` | `TimeUnit` | Gets the time unit for Datetime and Duration types. |
| `TimeZone` | `string` | Gets the timezone string for Datetime type. Returns null if the datetime has no timezone (naive datetime). |
| `TimeZoneInfo` | `TimeZoneInfo` | Gets the timezone info for Datetime type. Returns null if the datetime has no timezone (naive datetime). |
| `UInt128` | `DataType` | - |
| `UInt16` | `DataType` | - |
| `UInt32` | `DataType` | - |
| `UInt64` | `DataType` | - |
| `UInt8` | `DataType` | - |
| `Unknown` | `DataType` | - |

## Constructors & Static Methods

### Array

Create a Fixed-Size List (Array) data type. 

Example: DataType.Array(DataType.Int32, 3),DataType.Array(typeof(int), 3)

```csharp
public static DataType Array(DataType inner, params uint[] shape)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `inner` | `DataType` | The data type of the elements. |
| `shape` | `uint[]` | The shape of the arrays. |

---

### Categorical

Create a Categorical type

```csharp
public static DataType Categorical(Categories? categories = null)
public static DataType Categorical(string categories)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `categories` | `Categories? / string` | - |

---

### Datetime

Create a datetime type with unit and timezone

```csharp
public static DataType Datetime(TimeUnit unit = TimeUnit.Microseconds, string? timeZone = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `unit` | `TimeUnit` | precision (ns, us, ms) |
| `timeZone` | `string?` | timezone string (e.g. "Asia/Shanghai")， null for no timezone (Naive) |

---

### Decimal

Create a Decimal type

```csharp
public static DataType Decimal(int precision = 38, int scale = 9)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `precision` | `int` | - |
| `scale` | `int` | - |

---

### Duration

Creates a Duration type. Default is Microseconds. Usage: DataType.Duration(TimeUnit.Nanoseconds)

```csharp
public static DataType Duration(TimeUnit unit = TimeUnit.Microseconds)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `unit` | `TimeUnit` | - |

---

### Enum

Create a Enum type

```csharp
public static DataType Enum(FrozenCategories categories)
public static DataType Enum(Series categories)
public static DataType Enum(string[] categories)
public static DataType Enum(Categories categories)
public static DataType Enum<T>()
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `categories` | `FrozenCategories / Series / string[]` | - |

---

### Extension

Create an Extension data type

```csharp
public static DataType Extension(string name, DataType inner, string? metadata = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | The registered name of the extension type (e.g. "geoarrow.wkb") |
| `inner` | `DataType` | The physical storage data type |
| `metadata` | `string?` | Optional metadata string |

---

### FromArrowType

Generate DataType from ArrowType

```csharp
public static DataType FromArrowType(IArrowType arrowType)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `arrowType` | `IArrowType` | - |

---

### FromNetType

Create a DataType from a .NET Type

```csharp
public static DataType FromNetType<T>()
```

---

### List

Creates a List type. Usage: DataType.List(DataType.Int32)

```csharp
public static DataType List(DataType innerType)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `innerType` | `DataType` | - |

---

### Struct

Create Struct composite type.

```csharp
public static DataType Struct(IEnumerable<Field> fields)
public static DataType Struct(params Field[] fields)
public static DataType Struct(IReadOnlyDictionary<string, DataType> fields)
public static DataType Struct(string[] names, DataType[] types)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fields` | `IEnumerable<Field> / Field[] / IReadOnlyDictionary<string, DataType>` | Definition of a single field within a Struct DataType. |
| `names` | `string[]` | - |
| `types` | `DataType[]` | - |

---

## Operators

### operator == (Equality)

```csharp
public static bool operator ==(DataType? left, DataType? right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `DataType?` | - |
| `right` | `DataType?` | - |

---

### implicit operator

Implicitly convert System.Type to Polars DataType. Example: DataType dt = typeof(int); // dt is now DataType.Int32

```csharp
public static implicit operator DataType(Type type)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `type` | `Type` | - |

---

### operator != (Inequality)

```csharp
public static bool operator !=(DataType? left, DataType? right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `DataType?` | - |
| `right` | `DataType?` | - |

---

## Instance Methods

### GetArrowType

Get Apache Arrow Type

```csharp
public IArrowType GetArrowType()
```

---

### GetNetType

Get the .NET type corresponding to this DataType

```csharp
public Type GetNetType()
```

---

### ToDataTypeExpr

Convert this DataType to a DataTypeExpr literal. Equivalent to Python's polars.DataType.to_dtype_expr()

```csharp
public DataTypeExpr ToDataTypeExpr()
```

---

