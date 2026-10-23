---
uid: Polars.CSharp.Selector
title: Selector
---

# Selector

Namespace: `Polars.CSharp`

Represents a column selector strategy (e.g., All, Exclude). convertable to Expr.

## Operators

### operator + (Addition)

```csharp
public static Expr operator +(Selector s, int other)
public static Expr operator +(Selector s, long other)
public static Expr operator +(Selector s, double other)
public static Expr operator +(Selector s, float other)
public static Expr operator +(Selector s, string v)
public static Expr operator +(int other, Selector s)
public static Expr operator +(long other, Selector s)
public static Expr operator +(double other, Selector s)
public static Expr operator +(float other, Selector s)
public static Expr operator +(string other, Selector s)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `other` | `int / long / double / float / string` | - |
| `v` | `string` | - |

---

### operator & (Bitwise / Logical AND)

```csharp
public static Expr operator &(Selector s, bool v)
public static Expr operator &(bool v, Selector s)
public static Selector operator &(Selector left, Selector right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `v` | `bool` | - |
| `left` | `Selector` | - |
| `right` | `Selector` | - |

---

### operator | (Bitwise / Logical OR)

```csharp
public static Expr operator |(Selector s, bool v)
public static Expr operator |(bool v, Selector s)
public static Selector operator |(Selector left, Selector right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `v` | `bool` | - |
| `left` | `Selector` | - |
| `right` | `Selector` | - |

---

### operator / (Division)

```csharp
public static Expr operator /(Selector s, int other)
public static Expr operator /(Selector s, long other)
public static Expr operator /(Selector s, double other)
public static Expr operator /(Selector s, float other)
public static Expr operator /(int other, Selector s)
public static Expr operator /(long other, Selector s)
public static Expr operator /(double other, Selector s)
public static Expr operator /(float other, Selector s)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `other` | `int / long / double / float` | - |

---

### operator == (Equality)

```csharp
public static Expr operator ==(Selector s, int other)
public static Expr operator ==(Selector s, long other)
public static Expr operator ==(Selector s, double other)
public static Expr operator ==(Selector s, float other)
public static Expr operator ==(Selector s, string v)
public static Expr operator ==(int other, Selector s)
public static Expr operator ==(long other, Selector s)
public static Expr operator ==(double other, Selector s)
public static Expr operator ==(float other, Selector s)
public static Expr operator ==(string other, Selector s)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `other` | `int / long / double / float / string` | - |
| `v` | `string` | - |

---

### operator ^ (Exclusive OR)

```csharp
public static Expr operator ^(Selector s, int v)
public static Expr operator ^(Selector s, long v)
public static Expr operator ^(Selector s, bool v)
public static Expr operator ^(int v, Selector s)
public static Expr operator ^(long v, Selector s)
public static Expr operator ^(bool v, Selector s)
public static Selector operator ^(Selector left, Selector right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `v` | `int / long / bool` | - |
| `left` | `Selector` | - |
| `right` | `Selector` | - |

---

### operator > (Greater Than)

```csharp
public static Expr operator >(Selector s, int other)
public static Expr operator >(Selector s, long other)
public static Expr operator >(Selector s, double other)
public static Expr operator >(Selector s, float other)
public static Expr operator >(int other, Selector s)
public static Expr operator >(long other, Selector s)
public static Expr operator >(double other, Selector s)
public static Expr operator >(float other, Selector s)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `other` | `int / long / double / float` | - |

---

### operator >= (Greater Or Equal)

```csharp
public static Expr operator >=(Selector s, int other)
public static Expr operator >=(Selector s, long other)
public static Expr operator >=(Selector s, double other)
public static Expr operator >=(Selector s, float other)
public static Expr operator >=(int other, Selector s)
public static Expr operator >=(long other, Selector s)
public static Expr operator >=(double other, Selector s)
public static Expr operator >=(float other, Selector s)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `other` | `int / long / double / float` | - |

---

### implicit operator

Implicitly convert Selector to Expr. This is the magic that allows df.Select(Polars.All())

```csharp
public static implicit operator Expr(Selector selector)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `selector` | `Selector` | - |

---

### operator != (Inequality)

```csharp
public static Expr operator !=(Selector s, int other)
public static Expr operator !=(Selector s, long other)
public static Expr operator !=(Selector s, double other)
public static Expr operator !=(Selector s, float other)
public static Expr operator !=(Selector s, string v)
public static Expr operator !=(int other, Selector s)
public static Expr operator !=(long other, Selector s)
public static Expr operator !=(double other, Selector s)
public static Expr operator !=(float other, Selector s)
public static Expr operator !=(string other, Selector s)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `other` | `int / long / double / float / string` | - |
| `v` | `string` | - |

---

### operator < (Less Than)

```csharp
public static Expr operator <(Selector s, int other)
public static Expr operator <(Selector s, long other)
public static Expr operator <(Selector s, double other)
public static Expr operator <(Selector s, float other)
public static Expr operator <(int other, Selector s)
public static Expr operator <(long other, Selector s)
public static Expr operator <(double other, Selector s)
public static Expr operator <(float other, Selector s)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `other` | `int / long / double / float` | - |

---

### operator <= (Less Or Equal)

```csharp
public static Expr operator <=(Selector s, int other)
public static Expr operator <=(Selector s, long other)
public static Expr operator <=(Selector s, double other)
public static Expr operator <=(Selector s, float other)
public static Expr operator <=(int other, Selector s)
public static Expr operator <=(long other, Selector s)
public static Expr operator <=(double other, Selector s)
public static Expr operator <=(float other, Selector s)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `other` | `int / long / double / float` | - |

---

### operator ! (Logical NOT)

```csharp
public static Selector operator !(Selector s)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |

---

### operator % (Modulus)

```csharp
public static Expr operator %(Selector s, int other)
public static Expr operator %(Selector s, long other)
public static Expr operator %(Selector s, double other)
public static Expr operator %(Selector s, float other)
public static Expr operator %(int other, Selector s)
public static Expr operator %(long other, Selector s)
public static Expr operator %(double other, Selector s)
public static Expr operator %(float other, Selector s)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `other` | `int / long / double / float` | - |

---

### operator * (Multiply)

```csharp
public static Expr operator *(Selector s, int other)
public static Expr operator *(Selector s, long other)
public static Expr operator *(Selector s, double other)
public static Expr operator *(Selector s, float other)
public static Expr operator *(int other, Selector s)
public static Expr operator *(long other, Selector s)
public static Expr operator *(double other, Selector s)
public static Expr operator *(float other, Selector s)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `other` | `int / long / double / float` | - |

---

### operator ~ (Bitwise NOT)

```csharp
public static Selector operator ~(Selector s)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |

---

### operator - (Subtraction)

```csharp
public static Expr operator -(Selector s, int other)
public static Expr operator -(Selector s, long other)
public static Expr operator -(Selector s, double other)
public static Expr operator -(Selector s, float other)
public static Expr operator -(int other, Selector s)
public static Expr operator -(long other, Selector s)
public static Expr operator -(double other, Selector s)
public static Expr operator -(float other, Selector s)
public static Selector operator -(Selector left, Selector right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `s` | `Selector` | - |
| `other` | `int / long / double / float` | - |
| `left` | `Selector` | - |
| `right` | `Selector` | - |

---

## Instance Methods

### Clone

```csharp
public Selector Clone()
```

---

### Exclude

Exclude columns by name.

Exclude columns matching any of the specified Selectors. Usage: cs.Numeric().Exclude(cs.First(), cs.Last())

Exclude columns matching any of the specified Data Types. Usage: cs.All().Exclude(PlDataType.String, PlDataType.Boolean)

```csharp
public Selector Exclude(params string[] names)
public Selector Exclude(ReadOnlySpan<Selector> selectors)
public Selector Exclude(ReadOnlySpan<DataType> dtypes)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `names` | `string[]` | - |
| `selectors` | `ReadOnlySpan<Selector>` | - |
| `dtypes` | `ReadOnlySpan<DataType>` | - |

---

### ToExpr

Convert the selector to an Expression. This allows using selectors inside Select(), WithColumns(), etc.

```csharp
public Expr ToExpr()
```

---

