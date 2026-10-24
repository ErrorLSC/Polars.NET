---
uid: Polars.CSharp.Expr.Bin
title: "Expr.Bin (BinaryOps)"
---

# Expr.Bin (BinaryOps)

Namespace: `Polars.CSharp`

Operations on binary columns. Access via `Expr.Bin`.

## Instance Methods

### Contains

Check if binaries in Series contain a binary substring.

```csharp
public Expr Contains(BytesOrExpr literal)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `literal` | `BytesOrExpr` | The binary substring to look for |

**Returns:** Expression/Series of data type Boolean.

---

### Decode

Decode values using the provided encoding.

```csharp
public Expr Decode(TransferEncoding encoding, bool strict = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `encoding` | `TransferEncoding` | The encoding to use. |
| `strict` | `bool` | Raise an error if the underlying value cannot be decoded, otherwise mask out with a null value. |

**Returns:** Expression/Series of data type Binary.

---

### Encode

Encode a value using the provided encoding.

```csharp
public Expr Encode(TransferEncoding encoding)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `encoding` | `TransferEncoding` | The encoding to use. |

**Returns:** Expression/Series of data type Binary.

---

### EndsWith

Check if values end with a binary substring.

```csharp
public Expr EndsWith(BytesOrExpr suffix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `suffix` | `BytesOrExpr` | Suffix substring. |

**Returns:** Expression/Series of data type Boolean.

---

### Head

Take the first n bytes of the binary values.

```csharp
public Expr Head(IntOrExpr? n = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `IntOrExpr?` | Length of the slice (integer or expression). Negative indexing is supported |

**Returns:** Expression/Series of data type Binary

---

### Reinterpret

Interpret bytes as another type.Supported types are numerical or temporal dtypes, or an Array of these dtypes.

```csharp
public Expr Reinterpret(IntoDataTypeExpr dtype, Endianness endianness = Endianness.Little)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dtype` | `IntoDataTypeExpr` | Which type to interpret binary column into. |
| `endianness` | `Endianness` | Which endianness to use when interpreting bytes, by default “little”. |

**Returns:** Expression/Series of data type dtype. Note that rows of the binary array where the length does not match the size in bytes of the output array (number of items * byte size of item) will become NULL.

---

### Size

Computes the size of the binary data in the specified unit. The default unit is Bytes. Scaling to other units will result in a Float64 expression.

```csharp
public Expr Size(SizeUnit unit = SizeUnit.Bytes)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `unit` | `SizeUnit` | The unit to scale the binary size to. |

**Returns:** A new Polars Expression representing the scaled size.

---

### Slice

Slice the binary values.

```csharp
public Expr Slice(IntOrExpr offset, IntOrExpr? length)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `offset` | `IntOrExpr` | Start index. Negative indexing is supported. |
| `length` | `IntOrExpr?` | Length of the slice. If set to None (default), the slice is taken to the end of the value. |

**Returns:** Expression/Series of data type Binary.

---

### StartsWith

Check if values start with a binary substring.

```csharp
public Expr StartsWith(BytesOrExpr prefix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `prefix` | `BytesOrExpr` | Prefix substring. |

**Returns:** Expression/Series of data type Boolean.

---

### Tail

Take the last n bytes of the binary values.

```csharp
public Expr Tail(IntOrExpr? n = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `IntOrExpr?` | Length of the slice (integer or expression). Negative indexing is supported |

**Returns:** Expression/Series of data type Binary

---

