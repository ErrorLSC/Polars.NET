---
uid: Polars.CSharp.Expr.Cat
title: Expr.Cat (CategoricalOps)
---

# Expr.Cat (CategoricalOps)

Namespace: `Polars.CSharp`

Operations on Catgorical columns. Access via `Expr.Cat`.

## Instance Methods

### EndsWith

Check if string representations of values end with a substring.

```csharp
public Expr EndsWith(string suffix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `suffix` | `string` | Suffix substring. |

---

### GetCategories

Get the categories stored in this data type.

```csharp
public Expr GetCategories()
```

---

### LenBytes

Return the byte-length of the string representation of each value.

```csharp
public Expr LenBytes()
```

**Returns:** Expression/Series of data type UInt32.

---

### LenChars

Return the number of characters of the string representation of each value.

```csharp
public Expr LenChars()
```

**Returns:** Expression/Series of data type UInt32.

---

### Physical

Get the physical values of a categorical or enum data type.

```csharp
public Expr Physical()
```

---

### StartsWith

Check if string representations of values start with a substring.

```csharp
public Expr StartsWith(string prefix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `prefix` | `string` | Prefix substring. |

---

### To

Convert to a categorical or enum dtype. The input must be of the physical type of the categorical or enum dtype.

```csharp
public Expr To(IntoDataTypeExpr dtype, bool strict = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dtype` | `IntoDataTypeExpr` | The target categorical or enum dtype. |
| `strict` | `bool` | Whether to panic when encountering an illegal category. |

---

