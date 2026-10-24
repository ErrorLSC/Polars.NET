---
uid: Polars.CSharp.Expr
title: "Expr"
---

# Expr

Namespace: `Polars.CSharp`

A Polars Expr

## Sub-namespaces

| Sub-namespace | Page | Description |
| :--- | :--- | :--- |
| `.Array` | [Expr.Array](Expr.Array.md) | Access array operations. |
| `.Bin` | [Expr.Bin](Expr.Bin.md) | Access binary operations. |
| `.Cat` | [Expr.Cat](Expr.Cat.md) | Access categorical operations. |
| `.Dt` | [Expr.Dt](Expr.Dt.md) | Access temporal (Date/Time) operations. |
| `.Ext` | [Expr.Ext](Expr.Ext.md) | Access expression extension datatype operations. |
| `.List` | [Expr.List](Expr.List.md) | Access list operations. |
| `.Meta` | [Expr.Meta](Expr.Meta.md) | Access expression meta operations. |
| `.Name` | [Expr.Name](Expr.Name.md) | Access column renaming operations. |
| `.Str` | [Expr.Str](Expr.Str.md) | Access string manipulation operations. |
| `.Struct` | [Expr.Struct](Expr.Struct.md) | Access struct operations. |

## Constructors & Static Methods

### Neg

```csharp
public static Expr Neg(Expr expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | - |

---

### SqlExpr

Create a Polars Expr from a SQL string.

```csharp
public static Expr SqlExpr(string sql)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `sql` | `string` | The SQL expression string. |

**Returns:** A Polars Expr representing the SQL logic.

---

### SqlExprs

Create an array of Polars Exprs from a collection of SQL strings.

```csharp
public static Expr[] SqlExprs(IEnumerable<string> sqls)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `sqls` | `IEnumerable<string>` | The collection of SQL expression strings. |

**Returns:** An array of Polars Expr objects.

---

## Operators

### operator + (Addition)

Creates an expression representing the addition of two expressions. 

Supports element-wise addition. If one operand is a scalar/literal, it is broadcast to the length of the other.

```csharp
public static Expr operator +(Expr left, Expr right)
public static Expr operator +(Expr left, object right)
public static Expr operator +(object left, Expr right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr / object` | The left expression. |
| `right` | `Expr / object` | The right expression. |

**Returns:** A numeric expression representing the sum.

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    a = new[] { 1, 2, 3, 4, null as int? },
    b = new[] { 10, 20, 30, 40, 50 }
});

// Arithmetic: Column + Column, Column * Scalar
df.Select(
    (Col("a") + Col("b")).Alias("sum_ab"),
    (Col("a") * 2).Alias("a_double")
).Show();
/* Output:
shape: (5, 2)
┌────────┬──────────┐
│ sum_ab ┆ a_double │
│ ---    ┆ ---      │
│ i32    ┆ i32      │
╞════════╪══════════╡
│ 11     ┆ 2        │
│ 22     ┆ 4        │
│ 33     ┆ 6        │
│ 44     ┆ 8        │
│ null   ┆ null     │
└────────┴──────────┘
*/
```

---

### operator &amp; (Bitwise / Logical AND)

Creates an expression representing the logical AND operation.

```csharp
public static Expr operator &(Expr left, Expr right)
public static Expr operator &(Expr left, bool right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr` | The left boolean expression. |
| `right` | `Expr / bool` | The right boolean expression. |

**Returns:** A boolean expression that evaluates to true if both operands are true.

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    b = new[] { 10, 20, 30, 40, 50 },
    cond = new[] { true, false, true, false, true }
});

// Logical AND: (b > 20) AND cond
df.Select(
    Pl.Col("b"),
    Pl.Col("cond"),
    ((Pl.Col("b") > 20) & Pl.Col("cond")).Alias("logic_and")
).Show();
/* Output:
shape: (5, 3)
┌─────┬───────┬───────────┐
│ b   ┆ cond  ┆ logic_and │
│ --- ┆ ---   ┆ ---       │
│ i32 ┆ bool  ┆ bool      │
╞═════╪═══════╪═══════════╡
│ 10  ┆ true  ┆ false     │
│ 20  ┆ false ┆ false     │
│ 30  ┆ true  ┆ true      │
│ 40  ┆ false ┆ false     │
│ 50  ┆ true  ┆ true      │
└─────┴───────┴───────────┘
*/
```

---

### operator | (Bitwise / Logical OR)

Creates an expression representing the logical OR operation.

```csharp
public static Expr operator |(Expr left, Expr right)
public static Expr operator |(Expr left, bool right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr` | The left boolean expression. |
| `right` | `Expr / bool` | The right boolean expression. |

**Returns:** A boolean expression that evaluates to true if at least one operand is true.

---

### operator / (Division)

Creates an expression representing the division of the left expression by the right expression.

```csharp
public static Expr operator /(Expr left, Expr right)
public static Expr operator /(Expr left, object right)
public static Expr operator /(object left, Expr right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr / object` | The left expression. |
| `right` | `Expr / object` | The right expression. |

**Returns:** A numeric expression representing the quotient.

---

### operator == (Equality)

Creates an expression evaluating if the left operand is equal to the right operand.

```csharp
public static Expr operator ==(Expr left, Expr right)
public static Expr operator ==(Expr left, object right)
public static Expr operator ==(object left, Expr right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr / object` | The left expression. |
| `right` | `Expr / object` | The right expression. |

**Returns:** A boolean expression representing the comparison.

---

### operator ^ (Exclusive OR)

Creates an expression representing the logical XOR operation.

```csharp
public static Expr operator ^(Expr left, Expr right)
public static Expr operator ^(Expr left, bool right)
public static Expr operator ^(bool left, Expr right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr / bool` | - |
| `right` | `Expr / bool` | - |

---

### operator &gt; (Greater Than)

Creates an expression evaluating if the left operand is greater than the right operand.

```csharp
public static Expr operator >(Expr left, Expr right)
public static Expr operator >(Expr left, object right)
public static Expr operator >(object left, Expr right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr / object` | The left expression. |
| `right` | `Expr / object` | The right expression. |

**Returns:** A boolean expression representing the comparison.

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    a = new[] { 1, 2, 3, 4, null as int? },
    b = new[] { 10, 20, 30, 40, 50 }
});

// Compare columns: b > 20
// Note: Null comparisons propagate null (Logic: 50 > null is null)
df.Select(
    Pl.Col("b"),
    (Pl.Col("b") > 20).Alias("b_gt_20"),
    (Pl.Col("a") == 2).Alias("a_eq_2")
).Show();
/* Output:
shape: (5, 3)
┌─────┬─────────┬────────┐
│ b   ┆ b_gt_20 ┆ a_eq_2 │
│ --- ┆ ---     ┆ ---    │
│ i32 ┆ bool    ┆ bool   │
╞═════╪═════════╪════════╡
│ 10  ┆ false   ┆ false  │
│ 20  ┆ false   ┆ true   │
│ 30  ┆ true    ┆ false  │
│ 40  ┆ true    ┆ false  │
│ 50  ┆ true    ┆ null   │
└─────┴─────────┴────────┘
*/
```

---

### operator &gt;= (Greater Or Equal)

Creates an expression evaluating if the left operand is greater than or equal to the right operand.

```csharp
public static Expr operator >=(Expr left, Expr right)
public static Expr operator >=(Expr left, object right)
public static Expr operator >=(object left, Expr right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr / object` | The left expression. |
| `right` | `Expr / object` | The right expression. |

**Returns:** A boolean expression representing the comparison.

---

### implicit operator

```csharp
public static implicit operator Expr(int value)
public static implicit operator Expr(double value)
public static implicit operator Expr(DateTime value)
public static implicit operator Expr(DateTimeOffset value)
public static implicit operator Expr(DateOnly value)
public static implicit operator Expr(TimeOnly value)
public static implicit operator Expr(TimeSpan value)
public static implicit operator Expr(bool value)
public static implicit operator Expr(float value)
public static implicit operator Expr(long value)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `value` | `int / double / DateTime / DateTimeOffset / DateOnly / TimeOnly / TimeSpan / bool / float / long` | - |

---

### operator != (Inequality)

Creates an expression evaluating if the left operand is not equal to the right operand.

```csharp
public static Expr operator !=(Expr left, Expr right)
public static Expr operator !=(Expr left, object right)
public static Expr operator !=(object left, Expr right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr / object` | The left expression. |
| `right` | `Expr / object` | The right expression. |

**Returns:** A boolean expression representing the comparison.

---

### operator &lt;&lt; (Left Shift)

Bitwise left shift operation. 

Equivalent to C# `&lt;&lt;` operator. Shifting left by N is equivalent to multiplying by 2^N.

```csharp
public static Expr operator <<(Expr left, int right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr` | The expression to shift. |
| `right` | `int` | The number of bits to shift. |

**Returns:** A numeric expression with bits shifted left.

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    vals = new[] { 1, 2, 4, 8, 16 },    // Powers of 2
    neg  = new[] { -2, -4, -8, -16, -32 } // Negative numbers
});

// 1. Left shift by 1 (x * 2)
// 2. Right shift by 1 (x / 2)
// 3. Negative Right shift (Arithmetic Shift, preserves sign)
df.Select(
    Col("vals"),
    (Col("vals") << 1).Alias("shl_1"), 
    (Col("vals") >> 1).Alias("shr_1"), 
    Col("neg"),
    (Col("neg") >> 1).Alias("neg_shr_1") 
).Show();
/* Output:
shape: (5, 5)
┌──────┬───────┬───────┬─────┬───────────┐
│ vals ┆ shl_1 ┆ shr_1 ┆ neg ┆ neg_shr_1 │
│ ---  ┆ ---   ┆ ---   ┆ --- ┆ ---       │
│ i32  ┆ i32   ┆ i32   ┆ i32 ┆ i32       │
╞══════╪═══════╪═══════╪═════╪═══════════╡
│ 1    ┆ 2     ┆ 0     ┆ -2  ┆ -1        │
│ 2    ┆ 4     ┆ 1     ┆ -4  ┆ -2        │
│ 4    ┆ 8     ┆ 2     ┆ -8  ┆ -4        │
│ 8    ┆ 16    ┆ 4     ┆ -16 ┆ -8        │
│ 16   ┆ 32    ┆ 8     ┆ -32 ┆ -16       │
└──────┴───────┴───────┴─────┴───────────┘
*/
```

---

### operator &lt; (Less Than)

Creates an expression evaluating if the left operand is less than the right operand.

```csharp
public static Expr operator <(Expr left, Expr right)
public static Expr operator <(Expr left, object right)
public static Expr operator <(object left, Expr right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr / object` | The left expression. |
| `right` | `Expr / object` | The right expression. |

**Returns:** A boolean expression representing the comparison.

---

### operator &lt;= (Less Or Equal)

Creates an expression evaluating if the left operand is less than or equal to the right operand.

```csharp
public static Expr operator <=(Expr left, Expr right)
public static Expr operator <=(Expr left, object right)
public static Expr operator <=(object left, Expr right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr / object` | The left expression. |
| `right` | `Expr / object` | The right expression. |

**Returns:** A boolean expression representing the comparison.

---

### operator ! (Logical NOT)

Creates an expression representing the logical NOT operation.

```csharp
public static Expr operator !(Expr expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | The boolean expression to negate. |

**Returns:** A boolean expression that evaluates to the opposite truth value.

---

### operator % (Modulus)

Creates an expression representing the remaining of the left expression by the right expression.

```csharp
public static Expr operator %(Expr left, Expr right)
public static Expr operator %(Expr left, object right)
public static Expr operator %(object left, Expr right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr / object` | The left expression. |
| `right` | `Expr / object` | The right expression. |

**Returns:** A numeric expression representing the quotient.

---

### operator * (Multiply)

Creates an expression representing the multiplication of two expressions.

```csharp
public static Expr operator *(Expr left, Expr right)
public static Expr operator *(Expr left, object right)
public static Expr operator *(object left, Expr right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr / object` | The left expression. |
| `right` | `Expr / object` | The right expression. |

**Returns:** A numeric expression representing the product.

---

### operator ~ (Bitwise NOT)

```csharp
public static Expr operator ~(Expr expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | - |

---

### operator &gt;&gt; (Right Shift)

Bitwise right shift operation. 

For signed integers, this is an arithmetic shift (preserves the sign bit). For unsigned integers, this is a logical shift (fills with zeros).

```csharp
public static Expr operator >>(Expr left, int right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr` | The expression to shift. |
| `right` | `int` | The number of bits to shift. |

**Returns:** A numeric expression with bits shifted right.

---

### operator - (Subtraction)

Creates an expression representing the subtraction of the right expression from the left expression.

```csharp
public static Expr operator -(Expr left, Expr right)
public static Expr operator -(Expr left, object right)
public static Expr operator -(object left, Expr right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Expr / object` | The left expression. |
| `right` | `Expr / object` | The right expression. |

**Returns:** A numeric expression representing the difference.

---

### operator - (Unary Negation)

Negates the expression numerically. Equivalent to Polars `-expr`.

```csharp
public static Expr operator -(Expr expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | The expression to negate. |

**Returns:** A new negated `Expr`.

---

## Instance Methods

### Abs

Calculate the absolute value of the expression.

```csharp
public Expr Abs()
```

---

### Alias

Set a new name for a column

```csharp
public Expr Alias(string name)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string` | - |

---

### All

Check if all values in the boolean expression are `true`. 

This is a boolean aggregation.

```csharp
public Expr All(bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | If `true`, null values are ignored. If `false` (default), the result propagates nulls (i.e., if there is a null and no false, the result might be null). |

**Returns:** A new expression representing the boolean result.

---

### Any

Check if any value in the boolean expression is `true`. 

This is a boolean aggregation.

```csharp
public Expr Any(bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | If `true`, null values are ignored. If `false` (default), the result propagates nulls. |

**Returns:** A new expression representing the boolean result.

---

### Append

Append expressions. This is done by adding the chunks of other to this Series.

```csharp
public Expr Append(IntoExpr other, bool upcast = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExpr` | Expression to append. |
| `upcast` | `bool` | Cast both Series to the same supertype. |

---

### ApproxNUnique

Approximate count of unique values. This is done using the HyperLogLog++ algorithm for cardinality estimation.

```csharp
public Expr ApproxNUnique()
```

---

### ArcCos

Compute the element-wise inverse cosine.

```csharp
public Expr ArcCos()
```

---

### ArcCosh

```csharp
public Expr ArcCosh()
```

---

### ArcSin

Compute the element-wise inverse sine.

```csharp
public Expr ArcSin()
```

---

### ArcSinh

```csharp
public Expr ArcSinh()
```

---

### ArcTan

Compute the element-wise inverse tangent.

```csharp
public Expr ArcTan()
```

---

### ArcTanh

```csharp
public Expr ArcTanh()
```

---

### ArgMax

Get the index of the maximum value.

```csharp
public Expr ArgMax()
```

---

### ArgMin

Get the index of the minimum value.

```csharp
public Expr ArgMin()
```

---

### ArgSort

Get the index values that would sort this expression.

```csharp
public Expr ArgSort(bool descending = false, bool nullsLast = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `descending` | `bool` | If true, sort in descending order. Default is false. |
| `nullsLast` | `bool` | If true, place null values last. Default is false. |

---

### ArgTrue

Return indices where expression evaluates True.

```csharp
public Expr ArgTrue()
```

**Returns:** Expression of data type UInt32.

---

### ArgUnique

Get the index of the unique values.

```csharp
public Expr ArgUnique()
```

---

### BackwardFill

Fill null values with a specific strategy (Backward).

```csharp
public Expr BackwardFill(uint limit = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `limit` | `uint` | Max number of consecutive nulls to fill. (Default null = infinite) |

---

### BitwiseAnd

Perform an aggregation of bitwise ANDs.

```csharp
public Expr BitwiseAnd()
```

---

### BitwiseCountOnes

Evaluate the number of set bits.

```csharp
public Expr BitwiseCountOnes()
```

---

### BitwiseCountZeros

Evaluate the number of unset bits.

```csharp
public Expr BitwiseCountZeros()
```

---

### BitwiseLeadingOnes

Evaluate the number most-significant set bits before seeing an unset bit.

```csharp
public Expr BitwiseLeadingOnes()
```

---

### BitwiseLeadingZeros

Evaluate the number most-significant unset bits before seeing a set bit.

```csharp
public Expr BitwiseLeadingZeros()
```

---

### BitwiseOr

Perform an aggregation of bitwise Ors.

```csharp
public Expr BitwiseOr()
```

---

### BitwiseTrailingOnes

Evaluate the number least-significant set bits before seeing an unset bit.

```csharp
public Expr BitwiseTrailingOnes()
```

---

### BitwiseTrailingZeros

Evaluate the number least-significant unset bits before seeing a set bit.

```csharp
public Expr BitwiseTrailingZeros()
```

---

### BitwiseXor

Perform an aggregation of bitwise Xors.

```csharp
public Expr BitwiseXor()
```

---

### BottomK

Get the bottom k smallest values. 

This is much faster than Sort().Tail(k) for large datasets.

```csharp
public Expr BottomK(int k)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `k` | `int` | - |

---

### BottomKBy

Get the bottom `k` rows according to the sorting criteria defined by `by`.

Get the bottom `k` rows according to a single sorting criterion. 

This is a convenience overload for a single expression.

```csharp
public Expr BottomKBy(int k, Expr[] by, bool[] reverse)
public Expr BottomKBy(int k, Expr by, bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `k` | `int` | Number of rows to return. |
| `by` | `Expr[]` | The expressions (columns) to sort by. |
| `reverse` | `bool[]` | Controls the sorting direction for each expression in `by`. <br>For BottomK: - `false` (default): Sorts ascending (picks smallest values). - `true`: Sorts descending (picks largest values, acting like TopK for this column).<br> Length must match `by`. |

**Returns:** A new expression.

---

### Cast

Cast expression to another data type.

```csharp
public Expr Cast(IntoDataTypeExpr dtype, bool strict = true, bool wrapNumerical = false)
public Expr Cast<T>(bool strict = true, bool wrapNumerical = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dtype` | `IntoDataTypeExpr` | The target type (can be .NET Type, Polars DataType, or DataTypeExpr) |
| `strict` | `bool` | Throws an error if conversion had overflows. |
| `wrapNumerical` | `bool` | Allows wrapping numerical overflow. |

---

### Cbrt

Calculate the cube root of the expression.

```csharp
public Expr Cbrt()
```

---

### Ceil

Rounds up to the nearest integer.

```csharp
public Expr Ceil()
```

---

### Clip

Set values outside the given boundaries to the boundary value.

```csharp
public Expr Clip(IntoExprColumn? lowerBound = null, IntoExprColumn? upperBound = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `lowerBound` | `IntoExprColumn?` | Lower bound. Accepts expression input. Non-expression inputs are parsed as literals. Strings are parsed as column names. |
| `upperBound` | `IntoExprColumn?` | Upper bound. Accepts expression input. Non-expression inputs are parsed as literals. Strings are parsed as column names. |

---

### Clone

Clone Expr

```csharp
public Expr Clone()
```

---

### Coalesce

Returns the first non-null value between this expression and other expressions. Syntactic sugar for `Pl.Coalesce(this, others)`.

```csharp
public Expr Coalesce(params IntoExprColumn[] others)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `others` | `IntoExprColumn[]` | Fallback expressions, column names, or literals. |

**Returns:** A new coalesced expression.

**Example**

```csharp
// Fill nulls in "val_a" with values from "val_b", and fallback to 0 if both are null
df.Select(
    Pl.Col("val_a").Coalesce("val_b", 0).Alias("merged_val")
);
```

---

### Cos

Compute the element-wise cosine.

```csharp
public Expr Cos()
```

---

### Cosh

```csharp
public Expr Cosh()
```

---

### Cot

Compute the element-wise value for the cotangent.

```csharp
public Expr Cot()
```

---

### Count

Return the number of non-null elements in the column.

```csharp
public Expr Count()
```

---

### CumCount

Get an array with the cumulative count computed at every element.

```csharp
public Expr CumCount(bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `reverse` | `bool` | Reverse the operation. |

---

### CumMax

Get an array with the cumulative max computed at every element.

```csharp
public Expr CumMax(bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `reverse` | `bool` | Reverse the operation. |

---

### CumMin

Get an array with the cumulative min computed at every element.

```csharp
public Expr CumMin(bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `reverse` | `bool` | Reverse the operation. |

---

### CumProd

Get an array with the cumulative prod computed at every element.

```csharp
public Expr CumProd(bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `reverse` | `bool` | Reverse the operation. |

---

### CumSum

Get an array with the cumulative sum computed at every element.

```csharp
public Expr CumSum(bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `reverse` | `bool` | Reverse the operation. |

---

### CumulativeEval

Run an expression over a sliding window that increases 1 slot every iteration. This can be really slow as it can have O(n^2) complexity. Don’t use this for operations that visit all elements.

```csharp
public Expr CumulativeEval(Expr expr, int minSamples = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr` | Expression to evaluate |
| `minSamples` | `int` | Number of valid values there should be in the window before the expression is evaluated. valid values = length - null_count |

---

### Cut

Bin continuous values into discrete categories.

```csharp
public Expr Cut(
    ReadOnlySpan<double> breaks,
    string[]? labels = null,
    bool leftClosed = false,
    bool includeBreaks = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `breaks` | `ReadOnlySpan<double>` | List of unique cut points. |
| `labels` | `string[]?` | Names of the categories. The number of labels must be equal to the number of cut points plus one. |
| `leftClosed` | `bool` | Set the intervals to be left-closed instead of right-closed. |
| `includeBreaks` | `bool` | Include a column with the right endpoint of the bin each observation falls in. This will change the data type of the output from an Enum to a Struct. |

**Returns:** Expression/Series of data type Enum if include_breaks is set to False (default), otherwise an expression of data type Struct.

---

### Degrees

Convert from radians to degrees.

```csharp
public Expr Degrees()
```

---

### Diff

Calculate the difference with the previous value (n-th lag).

Calculate the difference with the previous value (1-st lag).

```csharp
public Expr Diff(IntoExprColumn n, NullBehavior nullBehavior = NullBehavior.Ignore)
public Expr Diff(NullBehavior nullBehavior = NullBehavior.Ignore)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `IntoExprColumn` | - |
| `nullBehavior` | `NullBehavior` | - |

---

### Dot

Compute the dot/inner product between two expressions. 

The dot product is the sum of the products of the corresponding entries of the two sequences of numbers.

```csharp
public Expr Dot(Expr other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Expr` | The other expression to compute the dot product with. |

**Returns:** A scalar expression representing the dot product result.

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    a = new[] { 1, 2, 3 },
    b = new[] { 4, 5, 6 }
});

// (1*4) + (2*5) + (3*6) = 4 + 10 + 18 = 32
df.Select(Pl.Col("a").Dot(Col("b"))).Show();
```

---

### DropNans

Drop nan values.

```csharp
public Expr DropNans()
```

---

### DropNulls

Drop null values.

```csharp
public Expr DropNulls()
```

---

### Entropy

Computes the entropy. Uses the formula -sum(pk * log(pk)) where pk are discrete probabilities.

```csharp
public Expr Entropy(double baseVal = 2.718281828459045, bool normalize = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `baseVal` | `double` | Given base, defaults to e |
| `normalize` | `bool` | Normalize pk if it doesn’t sum to 1. |

---

### EqMissing

Check if this expression is equal to another, treating nulls as valid values. (e.g., Null == Null is True, 5 == Null is False)

```csharp
public Expr EqMissing(IntoExprColumn other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExprColumn` | - |

---

### EwmMean

Compute exponentially-weighted moving average.

```csharp
public Expr EwmMean(
    double alpha,
    bool adjust = true,
    bool bias = true,
    int minPeriods = 1,
    bool ignoreNulls = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `alpha` | `double` | Specify smoothing factor alpha directly. <br>Constraint: `0 < alpha <= 1` |
| `adjust` | `bool` | If `true`, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). If `false`, assume infinite history. |
| `bias` | `bool` | If `true`, use a biased estimator (Standard deviation uses `N` in denominator). If `false`, use an unbiased estimator (Standard deviation uses `N-1`). <br>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true. |
| `minPeriods` | `int` | Minimum number of observations in window required to have a value (otherwise result is null). |
| `ignoreNulls` | `bool` | Ignore missing values when calculating weights. |

**Returns:** A new expression representing the EWM mean.

---

### EwmMeanBy

Compute exponentially-weighted moving average based on a temporal or index column.

```csharp
public Expr EwmMeanBy(Expr by, string halfLife)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `Expr` | The column used to determine the distance between observations. <br>Supported data types: `Date`, `DateTime`, `UInt64`, `UInt32`, `Int64`, or `Int32`. |
| `halfLife` | `string` | The unit over which an observation decays to half its value. <br>Supported string formats:<br><br>- Time units`ns` (nanosecond), `us` (microsecond), `ms` (millisecond), `s` (second), `m` (minute), `h` (hour), `d` (day), `w` (week). - Index units`i` (index count). Example: `"2i"` means decay by half every 2 index steps. - CompoundExample: `"3d12h4m25s"`.<br><br>Warning:`halfLife` is treated as a constant duration. Calendar durations such as months (`mo`) or years (`y`) are NOT supported because they vary in length. Please express such durations in hours (e.g. use `'730h'` instead of `'1mo'`). |

**Returns:** A new expression representing the time/index-based EWM mean.

---

### EwmStd

Compute exponentially-weighted moving standard deviation.

```csharp
public Expr EwmStd(
    double alpha,
    bool adjust = true,
    bool bias = true,
    int minPeriods = 1,
    bool ignoreNulls = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `alpha` | `double` | Specify smoothing factor alpha directly. <br>Constraint: `0 < alpha <= 1` |
| `adjust` | `bool` | If `true`, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). If `false`, assume infinite history. |
| `bias` | `bool` | If `true`, use a biased estimator (Standard deviation uses `N` in denominator). If `false`, use an unbiased estimator (Standard deviation uses `N-1`). <br>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true. |
| `minPeriods` | `int` | Minimum number of observations in window required to have a value (otherwise result is null). |
| `ignoreNulls` | `bool` | Ignore missing values when calculating weights. |

**Returns:** A new expression representing the EWM standard deviation.

---

### EwmSum

Compute exponentially-weighted moving sum.

```csharp
public Expr EwmSum(
    double alpha,
    bool adjust = true,
    bool bias = true,
    int minPeriods = 1,
    bool ignoreNulls = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `alpha` | `double` | Specify smoothing factor alpha directly. <br>Constraint: `0 < alpha <= 1` |
| `adjust` | `bool` | If `true`, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). If `false`, assume infinite history. |
| `bias` | `bool` | If `true`, use a biased estimator (Standard deviation uses `N` in denominator). If `false`, use an unbiased estimator (Standard deviation uses `N-1`). <br>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true. |
| `minPeriods` | `int` | Minimum number of observations in window required to have a value (otherwise result is null). |
| `ignoreNulls` | `bool` | Ignore missing values when calculating weights. |

**Returns:** A new expression representing the EWM variance.

---

### EwmSumBy

Compute time-based exponentially-weighted moving sum.

```csharp
public Expr EwmSumBy(Expr by, string halfLife)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `Expr` | Column to use as reference for the time decay. |
| `halfLife` | `string` | Half-life of the exponential decay. |

---

### EwmVar

Compute exponentially-weighted moving variance.

```csharp
public Expr EwmVar(
    double alpha,
    bool adjust = true,
    bool bias = true,
    int minPeriods = 1,
    bool ignoreNulls = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `alpha` | `double` | Specify smoothing factor alpha directly. <br>Constraint: `0 < alpha <= 1` |
| `adjust` | `bool` | If `true`, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). If `false`, assume infinite history. |
| `bias` | `bool` | If `true`, use a biased estimator (Standard deviation uses `N` in denominator). If `false`, use an unbiased estimator (Standard deviation uses `N-1`). <br>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true. |
| `minPeriods` | `int` | Minimum number of observations in window required to have a value (otherwise result is null). |
| `ignoreNulls` | `bool` | Ignore missing values when calculating weights. |

**Returns:** A new expression representing the EWM variance.

---

### Exclude

```csharp
public Expr Exclude(params string[] names)
public Expr Exclude(ReadOnlySpan<DataType> dtypes)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `names` | `string[]` | - |
| `dtypes` | `ReadOnlySpan<DataType>` | - |

---

### Exp

Calculate the power of the Euler's number.

```csharp
public Expr Exp()
```

---

### Explode

Explode a list expression. 

This turns a list column into a long column (flattening).

Warning: When used in `DataFrame.Select` with other columns, it may cause a length mismatch error if the other columns are not broadcasted. Use `DataFrame.Explode` for safely exploding columns while repeating others.

```csharp
public Expr Explode(bool emptyAsNull = true, bool keepNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `emptyAsNull` | `bool` | If `true`, empty lists are exploded into a single `null` value. If `false`, rows with empty lists are removed from the result. |
| `keepNulls` | `bool` | If `true`, `null` values in the column are preserved as `null` in the result. If `false`, rows with `null` values are removed. |

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    id = new[] { 1, 2 },
    tags = new[] { new[] { "a", "b" }, new[] { "c" } }
});

// Example 1: Expression Explode (Flatten single column)
df.Select(
    Pl.Col("tags").Explode().Alias("tags_flat")
).Show();
/* Output:
shape: (3, 1)
┌───────────┐
│ tags_flat │
│ ---       │
│ str       │
╞═══════════╡
│ a         │
│ b         │
│ c         │
└───────────┘
*/

// Example 2: To keep 'id' aligned, use DataFrame.Explode:
// df.Explode("tags").Show();
```

---

### ExtendConstant

Extremely fast method for extending the Series with ‘n’ copies of a value.

```csharp
public Expr ExtendConstant(IntoExpr value, Expr n)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `value` | `IntoExpr` | A constant literal value or a unit expression with which to extend the expression result Series; can pass None to extend with nulls. |
| `n` | `Expr` | The number of additional values that will be added. |

---

### FillNan

Fill floating point NaN values with a specified value. This is different from FillNull. It only handles IEEE 754 NaN.

```csharp
public Expr FillNan(IntoExpr value)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `value` | `IntoExpr` | - |

---

### FillNull

Fill null values using the specified value

Fill null values using the specified strategy

```csharp
public Expr FillNull(IntoExpr value)
public Expr FillNull(FillNullStrategy strategy, uint? limit = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `value` | `IntoExpr` | Value used to fill null values. |
| `strategy` | `FillNullStrategy` | Strategy used to fill null values. |
| `limit` | `uint?` | Number of consecutive null values to fill when using the ‘forward’ or ‘backward’ strategy. |

---

### Filter

Filter a single column.  Mostly useful in `group_by` context or when you want to filter an expression based on another expression within a `Select` context.

```csharp
public Expr Filter(Expr predicate)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `predicate` | `Expr` | Boolean expression used to filter the current expression. |

**Returns:** A new expression with filtered values.

---

### First

Get the first value of the group/series.

```csharp
public Expr First(bool ignoreNulls = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | - |

**Returns:** A new expression representing the first value.

---

### Floor

Rounds down to the nearest integer.

```csharp
public Expr Floor()
```

---

### FloorDiv

Integer division (floor division).

```csharp
public Expr FloorDiv(Expr other)
public Expr FloorDiv(object other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Expr / object` | - |

---

### ForwardFill

Fill null values with a specific strategy (Forward).

```csharp
public Expr ForwardFill(uint limit = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `limit` | `uint` | Max number of consecutive nulls to fill. (Default null = infinite) |

---

### Gather

Gather values by an index expression.

```csharp
public Expr Gather(IntoExprColumn indices)
public Expr Gather(ReadOnlySpan<int> indices)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `indices` | `IntoExprColumn / ReadOnlySpan<int>` | - |

---

### GatherEvery

Take every nth value starting from an offset.

```csharp
public Expr GatherEvery(ulong n, ulong offset = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `ulong` | - |
| `offset` | `ulong` | - |

---

### Get

Get a single value by index. Returns a scalar.

```csharp
public Expr Get(Expr index, bool nullOnOutOfBounds = false)
public Expr Get(ulong index, bool nullOnOutOfBounds = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `index` | `Expr / ulong` | The index expression. |
| `nullOnOutOfBounds` | `bool` | If true, returns Null when the index is out of bounds instead of raising an error. |

---

### HasNulls

Check whether the expression contains one or more null values.

```csharp
public Expr HasNulls()
```

---

### Hash

Hash the elements in the selection.The hash value is of type UInt64.

```csharp
public Expr Hash(
    ulong seed = 0,
    ulong? seed1 = null,
    ulong? seed2 = null,
    ulong? seed3 = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `seed` | `ulong` | Random seed parameter. Defaults to 0. |
| `seed1` | `ulong?` | Random seed parameter. Defaults to seed if not set. |
| `seed2` | `ulong?` | Random seed parameter. Defaults to seed if not set. |
| `seed3` | `ulong?` | Random seed parameter. Defaults to seed if not set. |

---

### Head

Get the first n rows.

```csharp
public Expr Head(int n = 10)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | Number of rows to return. |

---

### Hist

Bin values into buckets and count their occurrences.

```csharp
public Expr Hist(
    IntoExpr? bins = null,
    int? binCount = null,
    bool includeCategory = false,
    bool includeBreakPoint = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `bins` | `IntoExpr?` | Bin edges. If None given, we determine the edges based on the data. |
| `binCount` | `int?` | If bins is not provided, binCount uniform bins are created that fully encompass the data. |
| `includeCategory` | `bool` | Include a column that indicates the upper breakpoint. |
| `includeBreakPoint` | `bool` | Include a column that shows the intervals as categories. |

---

### Implode

Aggregate values into a list. 

This is the opposite of `Expr.Explode`. It collects values from multiple rows into a single list.

```csharp
public Expr Implode(bool maintainOrder = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `maintainOrder` | `bool` | - |

**Example**

```csharp
// Collect all IDs and Tags into single lists
df.Select(
    Pl.Col("id").Implode().Alias("all_ids"), 
    Pl.Col("tags").Implode().Alias("nested_tags")
).Show();
/* Output:
shape: (1, 2)
┌───────────┬─────────────────────┐
│ all_ids   ┆ nested_tags         │
│ ---       ┆ ---                 │
│ list[i32] ┆ list[list[str]]     │
╞═══════════╪═════════════════════╡
│ [1, 2]    ┆ [["a", "b"], ["c"]] │
└───────────┴─────────────────────┘
*/
```

---

### IndexOf

Find the index of the first occurrence of a specific value.

```csharp
public Expr IndexOf(IntoExpr element)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `element` | `IntoExpr` | The element expression to search for. |

---

### Inspect

Print the value that this expression evaluates to and pass on the value.

```csharp
public Expr Inspect(string format = "{}")
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `format` | `string` | - |

---

### Interpolate

Interpolate intermediate values. The interpolation method can be configured. 

Nulls at the beginning and end of the series remain null.

```csharp
public Expr Interpolate(InterpolationMethod method = InterpolationMethod.Linear)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `method` | `InterpolationMethod` | Interpolation method (Linear or Nearest). |

---

### InterpolateBy

Interpolate intermediate values based on the values of another column. 

This is useful when the data is not equally spaced, for example when interpolating based on a timestamp column.

```csharp
public Expr InterpolateBy(IntoExprColumn by)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IntoExprColumn` | The column to use for interpolation (e.g. a timestamp column). |

**Returns:** A new expression with interpolated values.

---

### IsBetween

Check if the value is between lower and upper bounds (inclusive).

```csharp
public Expr IsBetween(IntoExprColumn lower, IntoExprColumn upper, ClosedInterval closedInterval = ClosedInterval.Both)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `lower` | `IntoExprColumn` | - |
| `upper` | `IntoExprColumn` | - |
| `closedInterval` | `ClosedInterval` | - |

---

### IsClose

Check if this expression is close, i.e. almost equal, to the other expression.

```csharp
public Expr IsClose(
    IntoExprColumn other,
    double absTol = 0,
    double relTol = 1E-09,
    bool nansEqual = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExprColumn` | A literal or expression value to compare with. |
| `absTol` | `double` | Absolute tolerance. This is the maximum allowed absolute difference betweentwo values. Must be non-negative. |
| `relTol` | `double` | Relative tolerance. This is the maximum allowed difference between two values, relative to the larger absolute value. Must be non-negative. |
| `nansEqual` | `bool` | Whether NaN values should be considered equal. |

**Returns:** Expression/Series of data type Boolean.

---

### IsDuplicated

Create a boolean expression indicating whether the value is duplicated.

```csharp
public Expr IsDuplicated()
```

---

### IsEmpty

Return whether the column is empty.

```csharp
public Expr IsEmpty(bool ignoreNulls = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | If true a column containing only nulls will also be considered empty. The default is false. |

---

### IsFinite

Returns a boolean Series indicating which values are finite.

```csharp
public Expr IsFinite()
```

---

### IsFirstDistinct

Return a boolean mask indicating the first occurrence of each distinct value.

```csharp
public Expr IsFirstDistinct()
```

**Returns:** Expression of data type Boolean.

---

### IsIn

Check if elements of this expression are present in the other collection.

```csharp
public Expr IsIn(Expr other, bool nullsEqual = false)
public Expr IsIn(Series other, bool nullsEqual = false)
public Expr IsIn<T>(IEnumerable<T> other, bool nullsEqual = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `Expr / Series / IEnumerable<T>` | Series or sequence of primitive type. |
| `nullsEqual` | `bool` | If True, treat null as a distinct value. Null values will not propagate. |

---

### IsInfinite

Returns a boolean Series indicating which values are infinite.

```csharp
public Expr IsInfinite()
```

**Returns:** Expression of data type Boolean.

---

### IsLastDistinct

Return a boolean mask indicating the last occurrence of each distinct value.

```csharp
public Expr IsLastDistinct()
```

**Returns:** Expression of data type Boolean.

---

### IsNan

Returns a boolean Series indicating which values are NaN.

```csharp
public Expr IsNan()
```

---

### IsNotNan

Returns a boolean Series indicating which values are not NaN.

```csharp
public Expr IsNotNan()
```

---

### IsNotNull

Evaluate whether the expression is not null.

```csharp
public Expr IsNotNull()
```

---

### IsNull

Evaluate whether the expression is null.

```csharp
public Expr IsNull()
```

---

### IsSorted

Checks if an expression is sorted. 

If descending and/or nulls_last are None, it will check True and False for the unspecified option(s), and return True if the expression is sorted under any combination of those settings.

```csharp
public Expr IsSorted(bool? descending = false, bool? nullsLast = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `descending` | `bool?` | Checks if the expression is sorted in descending order.Defaults to False. |
| `nullsLast` | `bool?` | Consider null values as being ordered last when checking sortedness.Defaults to False. |

**Returns:** Expression of Boolean

---

### IsUnique

Create a boolean expression indicating whether the value is unique.

```csharp
public Expr IsUnique()
```

---

### Item

Return the single value in the group or series. 

This is strict: it expects the group/series to contain exactly one element.

```csharp
public Expr Item(bool allowEmpty = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `allowEmpty` | `bool` | If `true` and the group is empty, it returns `null` instead of throwing an error. Default is `true`. |

**Returns:** A new expression representing the single item.

> [!NOTE]
> If the group contains more than one element, this will throw an error at runtime. It is safer than `Expr.First` when you expect uniqueness (e.g., getting the ID of a group).

---

### Kurtosis

Compute the kurtosis (Fisher or Pearson) of a dataset.

```csharp
public Expr Kurtosis(bool fisher = true, bool bias = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fisher` | `bool` | If True, Fisher’s definition is used (normal ==&gt; 0.0). If False, Pearson’s definition is used (normal ==&gt; 3.0). |
| `bias` | `bool` | If False, the calculations are corrected for statistical bias. |

**Returns:** A series which length is 1

---

### Last

Get the last value of the group/series.

```csharp
public Expr Last(bool ignoreNulls = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ignoreNulls` | `bool` | - |

**Returns:** A new expression representing the last value.

---

### Len

Return the number of elements in the column. Null values count towards the total.

```csharp
public Expr Len()
```

---

### Ln

```csharp
public Expr Ln()
```

---

### Log

Compute the logarithm to a given base,defaults to e.

```csharp
public Expr Log(Expr baseVal)
public Expr Log()
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `baseVal` | `Expr` | Given base, defaults to e |

---

### Log10

Compute the base 10 logarithm of the input array, element-wise.

```csharp
public Expr Log10()
```

---

### Log1p

Compute the natural logarithm of each element plus one.This computes log(1 + x) but is more numerically stable for x close to zero.

```csharp
public Expr Log1p()
```

---

### LowerBound

Calculate the lower bound. Returns a unit Series with the lowest value possible for the dtype of this expression.

```csharp
public Expr LowerBound()
```

---

### Map

Apply a custom C# function to the expression. This runs locally in the .NET runtime, converting data between Polars and .NET.

Apply a custom C# function to the expression (High-Level).

Apply a multi-column UDF that receives multiple Arrow arrays and returns a single new column. The first expression is `this`.

```csharp
public Expr Map<TInput, TOutput>(Func<TInput, TOutput> function, DataType? outputType = null)
public Expr Map(Func<IReadOnlyList<IArrowArray>, IArrowArray> function, DataType outputType, params IntoExprColumn[] additionalExprs)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `function` | `Func<TInput, TOutput> / Func<IReadOnlyList<IArrowArray>, IArrowArray>` | The function to apply. |
| `outputType` | `DataType?` | Optional target DataType. If null, the DataType is automatically inferred from `TOutput`. |
| `additionalExprs` | `IntoExprColumn[]` | Other columns/expressions as IntoExprColumn. |

---

### MapArrow

Apply a raw Arrow-to-Arrow UDF. (Advanced / Internal use)

```csharp
public Expr MapArrow(Func<IArrowArray, IArrowArray> function, DataType outputType)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `function` | `Func<IArrowArray, IArrowArray>` | - |
| `outputType` | `DataType` | - |

---

### Max

Compute the max of an expression

```csharp
public Expr Max()
```

---

### MaxBy

Get maximum value, ordered by another expression. If the by expression has multiple values equal to the maximum it is not defined which value will be chosen.

```csharp
public Expr MaxBy(IntoExprColumn by)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IntoExprColumn` | Column used to determine the largest element. Accepts expression input. Strings are parsed as column names. |

---

### Mean

Compute the mean of an expression

```csharp
public Expr Mean()
```

---

### Median

Get the median value.

```csharp
public Expr Median()
```

**Returns:** A series which length is 1

---

### Min

Compute the min of an expression

```csharp
public Expr Min()
```

---

### MinBy

Get minimum value, ordered by another expression. If the by expression has multiple values equal to the minimum it is not defined which value will be chosen.

```csharp
public Expr MinBy(IntoExprColumn by)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IntoExprColumn` | Column used to determine the smallest element. Accepts expression input. Strings are parsed as column names. |

---

### Mode

Get the mode value.

```csharp
public Expr Mode()
```

---

### NUnique

Count unique values. Notes: Null is considered to be a unique value for the purposes of this operation.

```csharp
public Expr NUnique()
```

---

### NanMax

Get maximum value, but propagate/poison encountered NaN values.

```csharp
public Expr NanMax()
```

---

### NanMin

Get minimum value, but propagate/poison encountered NaN values.

```csharp
public Expr NanMin()
```

---

### NeqMissing

Check if this expression is NOT equal to another, treating nulls as valid values. (e.g., Null != Null is False, 5 != Null is True)

```csharp
public Expr NeqMissing(IntoExprColumn other)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `other` | `IntoExprColumn` | - |

---

### NullCount

Count the number of null.

```csharp
public Expr NullCount()
```

---

### Over

Apply a window function over a subgroup. 

This is similar to SQL's `OVER (PARTITION BY ...)` clause. Unlike `DataFrame.GroupBy`, this does not reduce the number of rows. The result is broadcasted back to the original rows.

Compute expressions over the given groups. This expression is similar to performing a group by aggregation and joining the result back into the original DataFrame. The outcome is similar to how window functions work in PostgreSQL.

```csharp
public Expr Over(params IntoExprColumn[] partitionBy)
public Expr Over(
    IEnumerable<IntoExprColumn>? partitionBy = null,
    IEnumerable<IntoExprColumn>? orderBy = null,
    bool descending = false,
    bool nullsLast = false,
    bool multithreaded = true,
    bool maintainOrder = false,
    WindowMappingStrategy mappingStrategy = WindowMappingStrategy.GroupsToRows
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `partitionBy` | `IntoExprColumn[] / IEnumerable<IntoExprColumn>?` | The columns to partition by. |
| `orderBy` | `IEnumerable<IntoExprColumn>?` | Order the window functions/aggregations with the partitioned groups by the result of the expression passed to order_by. |
| `descending` | `bool` | In case ‘order_by’ is given, indicate whether to order in ascending or descending order. |
| `nullsLast` | `bool` | In case ‘order_by’ is given, indicate whether to order the nulls in last position. |
| `multithreaded` | `bool` | - |
| `maintainOrder` | `bool` | - |
| `mappingStrategy` | `WindowMappingStrategy` | - |

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    group = new[] { "A", "A", "A", "B", "B" },
    val = new[] { 10, 20, 30, 100, 200 }
});

// Calculate mean per group and subtract it from the value
// The result has the same shape as the original DataFrame (5 rows)
df.Select(
    Pl.Col("group"),
    Pl.Col("val"),
    Pl.Col("val").Mean().Over("group").Alias("group_mean"),
    (Pl.Col("val") - Pl.Col("val").Mean().Over("group")).Alias("diff_from_mean")
).Show();
/* Output:
shape: (5, 4)
┌───────┬─────┬────────────┬────────────────┐
│ group ┆ val ┆ group_mean ┆ diff_from_mean │
│ ---   ┆ --- ┆ ---        ┆ ---            │
│ str   ┆ i32 ┆ f64        ┆ f64            │
╞═══════╪═════╪════════════╪════════════════╡
│ A     ┆ 10  ┆ 20.0       ┆ -10.0          │
│ A     ┆ 20  ┆ 20.0       ┆ 0.0            │
...
└───────┴─────┴────────────┴────────────────┘
*/
```

---

### PctChange

Computes percentage change between values. Percentage change (as fraction) between current element and most-recent non-null element at least n period(s) before the current element. Computes the change from the previous row by default.

```csharp
public Expr PctChange(int n = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | periods to shift for forming percent change. |

**Returns:** A series which length is 1

---

### PeakMax

Get a boolean mask of the local maximum peaks.

```csharp
public Expr PeakMax()
```

---

### PeakMin

Get a boolean mask of the local minimum peaks.

```csharp
public Expr PeakMin()
```

---

### Pipe

Apply a user-defined function to this expression, returning the result of the function. This allows encapsulating a sequence of Polars expression operations into a reusable function.

```csharp
public T Pipe<T>(Func<Expr, T> function)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `function` | `Func<Expr, T>` | A function that receives the current expression and returns a value of type `T`. Typically this function wraps several Polars API calls that operate on the given expression. |

**Returns:** The result of applying `function` to this expression.

---

### Pow

Calculate the power of the expression with a given exponent expression.

Calculate the power of the expression with a given numeric exponent.

```csharp
public Expr Pow(Expr exponent)
public Expr Pow(double exponent)
public Expr Pow(float exponent)
public Expr Pow(long exponent)
public Expr Pow(int exponent)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `exponent` | `Expr / double / float / long / int` | - |

---

### Product

Compute the product of an expression

```csharp
public Expr Product()
```

---

### QCut

Bin continuous values into discrete categories based on their quantiles.

```csharp
public Expr QCut(
    ReadOnlySpan<double> quantiles,
    string[]? labels = null,
    bool leftClosed = false,
    bool allowDuplicates = false,
    bool includeBreaks = false
)

public Expr QCut(
    int quantiles,
    string[]? labels = null,
    bool leftClosed = false,
    bool allowDuplicates = false,
    bool includeBreaks = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `quantiles` | `ReadOnlySpan<double> / int` | Either a list of quantile probabilities between 0 and 1 or a positive integer determining the number of bins with uniform probability. |
| `labels` | `string[]?` | Names of the categories. The number of labels must be equal to the number of categories. |
| `leftClosed` | `bool` | Set the intervals to be left-closed instead of right-closed. |
| `allowDuplicates` | `bool` | If set to True, duplicates in the resulting quantiles are dropped, rather than raising a DuplicateError. This can happen even with unique probabilities, depending on the data. |
| `includeBreaks` | `bool` | Include a column with the right endpoint of the bin each observation falls in. This will change the data type of the output from a Categorical to a Struct. |

**Returns:** Expression/Series of data type Categorical if include_breaks is set to False (default), otherwise an expression of data type Struct.

---

### Quantile

Get the quantile value.

```csharp
public Expr Quantile(double quantile, QuantileMethod method = QuantileMethod.Linear)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `quantile` | `double` | Quantile between 0.0 and 1.0. |
| `method` | `QuantileMethod` | ['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method. |

**Returns:** A series which length is 1

---

### Radians

Convert from degrees to radians.

```csharp
public Expr Radians()
```

---

### Rank

Assign ranks to data, dealing with ties appropriately.

```csharp
public Expr Rank(RankMethod method = RankMethod.Average, bool descending = false, ulong? seed = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `method` | `RankMethod` | The method used to assign ranks to tied elements. See `RankMethod` for details. Default is `RankMethod.Average`. |
| `descending` | `bool` | Rank in descending order. |
| `seed` | `ulong?` | If method="random", use this as seed. |

---

### Rechunk

Create a single chunk of memory for this Series.

```csharp
public Expr Rechunk()
```

---

### Reinterpret

Reinterpret the underlying bits as a signed/unsigned integer. This operation is only allowed for numeric types of the same size. For lower bits numbers, you can safely use the cast operation.

Reinterpret the underlying bits as a signed/unsigned integer or float.

```csharp
public Expr Reinterpret(bool signed = true)
public Expr Reinterpret(DataType dtype)
public Expr Reinterpret<T>()
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `signed` | `bool` | If True, reinterpret as signed integer. Otherwise, reinterpret as unsigned integer. |
| `dtype` | `DataType` | DataType to reinterpret to. |

---

### RepeatBy

Repeat the elements in this Series as specified in the given expression. The repeated elements are expanded into a List.

```csharp
public Expr RepeatBy(IntoExpr by)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IntoExpr` | Numeric column that determines how often the values will be repeated. The column will be coerced to UInt32. Give this dtype to make the coercion a no-op. |

**Returns:** Expression/Series of data type List, where the inner data type is equal to the original data type.

---

### Replace

Replace the given values by different values of the same data type.

Replace values using a mapping (Dictionary/IEnumerable of KeyValuePair).

Replace values using two sequences (IEnumerable) for old and new values.

```csharp
public Expr Replace(IntoExpr old, IntoExpr newExpr)
public Expr Replace<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> mapping)
public Expr Replace<TOld, TNew>(IEnumerable<TOld> oldValues, IEnumerable<TNew> newValues)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `old` | `IntoExpr` | Value or sequence of values to replace. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals. |
| `newExpr` | `IntoExpr` | Value or sequence of values to replace by. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals. |
| `mapping` | `IEnumerable<KeyValuePair<TKey, TValue>>` | - |
| `oldValues` | `IEnumerable<TOld>` | - |
| `newValues` | `IEnumerable<TNew>` | - |

---

### ReplaceStrict

Replace all values by different values.

Replace values strictly using a mapping.

Replace values strictly using two sequences (IEnumerable).

```csharp
public Expr ReplaceStrict(
    IntoExpr old,
    IntoExpr newExpr,
    IntoExpr? defaultExpr = null,
    IntoDataTypeExpr? returnDataType = null
)

public Expr ReplaceStrict<TKey, TValue>(IEnumerable<KeyValuePair<TKey, TValue>> mapping, IntoExpr? defaultExpr = null, IntoDataTypeExpr? returnDataType = null)
public Expr ReplaceStrict<TOld, TNew>(
    IEnumerable<TOld> oldValues,
    IEnumerable<TNew> newValues,
    IntoExpr? defaultExpr = null,
    IntoDataTypeExpr? returnDataType = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `old` | `IntoExpr` | Value or sequence of values to replace. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals. |
| `newExpr` | `IntoExpr` | Value or sequence of values to replace by. Accepts expression input. Sequences are parsed as Series, other non-expression inputs are parsed as literals. Length must match the length of old or have length 1. |
| `defaultExpr` | `IntoExpr?` | Set values that were not replaced to this value. If no default is specified, (default), an error is raised if any values were not replaced. Accepts expression input. Non-expression inputs are parsed as literals. |
| `returnDataType` | `IntoDataTypeExpr?` | The data type of the resulting expression. If set to null (default), the data type is determined automatically based on the other inputs. |
| `mapping` | `IEnumerable<KeyValuePair<TKey, TValue>>` | - |
| `oldValues` | `IEnumerable<TOld>` | - |
| `newValues` | `IEnumerable<TNew>` | - |

---

### Reshape

Reshape the column into a multi-dimensional array.

```csharp
public Expr Reshape(ReadOnlySpan<long> dimensions)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dimensions` | `ReadOnlySpan<long>` | Tuple of the dimension sizes. If a -1 is used, that dimension is inferred. |

---

### Reverse

Reverse the selection. 

This is useful in a GroupBy context to reverse the order of the group.

```csharp
public Expr Reverse()
```

**Returns:** A new expression with the order reversed.

---

### Rle

Compress the column data using run-length encoding. Run-length encoding (RLE) encodes data by storing each run of identical values as a single value and its length.

```csharp
public Expr Rle()
```

**Returns:** Expression/Series of data type Struct with fields len of data type UInt32 and value of the original data type.

---

### RleId

Get a distinct integer ID for each run of identical values. The ID starts at 0 and increases by one each time the value of the column changes.

```csharp
public Expr RleId()
```

**Returns:** Expression/Series of data type UInt32.

---

### Rolling

Create rolling groups based on a temporal or integer column.

```csharp
public Expr Rolling(
    IntoExprColumn indexColumn,
    IntoDuration period,
    IntoDuration? offset = null,
    ClosedInterval closed = ClosedInterval.Right
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `indexColumn` | `IntoExprColumn` | Column used to group based on the time window. Often of type Date/Datetime. This column must be sorted in ascending order. In case of a rolling group by on indices, dtype needs to be one of {UInt32, UInt64, Int32, Int64}. The first three get temporarily cast to Int64, so if performance matters use an Int64 column. |
| `period` | `IntoDuration` | Length of the window - must be non-negative. |
| `offset` | `IntoDuration?` | Offset of the window. Default is -period. |
| `closed` | `ClosedInterval` | Define which sides of the temporal interval are closed (inclusive). |

---

### RollingKurtosis

Apply a rolling kurtosis (moving kurtosis) over a window.

```csharp
public Expr RollingKurtosis(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false,
    bool fisher = true,
    bool bias = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |
| `fisher` | `bool` | - |
| `bias` | `bool` | - |

**Returns:** A new expression representing the rolling mean.

**Example**

```csharp
// Rolling mean of 3 rows ("3i"), centered
df.Select(
    Col("val").RollingMean("3i", minPeriods: 1, center: true).Alias("roll_mean")
);
```

---

### RollingMax

Apply a rolling max (moving max) over a window.

```csharp
public Expr RollingMax(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new expression representing the rolling mean.

**Example**

```csharp
// Rolling mean of 3 rows ("3i"), centered
df.Select(
    Col("val").RollingMean("3i", minPeriods: 1, center: true).Alias("roll_mean")
);
```

---

### RollingMaxBy

Apply the rolling maximum over a dynamic window defined by the values in the `by` column.

```csharp
public Expr RollingMaxBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new expression representing the dynamic rolling maximum.

---

### RollingMean

Apply a rolling mean (moving average) over a window.

```csharp
public Expr RollingMean(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new expression representing the rolling mean.

**Example**

```csharp
// Rolling mean of 3 rows ("3i"), centered
df.Select(
    Col("val").RollingMean("3i", minPeriods: 1, center: true).Alias("roll_mean")
);
```

---

### RollingMeanBy

Apply a rolling mean (moving average) over a dynamic window defined by the values in the `by` column. 

Unlike standard fixed-size rolling windows (which operate on row counts), this operates on values (typically time).

```csharp
public Expr RollingMeanBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new expression representing the dynamic rolling mean.

**Example**

```csharp
// Python: pl.col("index").rolling_mean_by("date", window_size="2h", closed="both")
// C#:
Col("index").RollingMeanBy("2h", Col("date"), closed: ClosedInterval.Both);
```

> [!NOTE]
> The `by` column must be sorted in ascending order.

---

### RollingMedian

Apply a rolling median (moving median) over a window.

```csharp
public Expr RollingMedian(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new expression representing the rolling mean.

**Example**

```csharp
// Rolling mean of 3 rows ("3i"), centered
df.Select(
    Col("val").RollingMean("3i", minPeriods: 1, center: true).Alias("roll_mean")
);
```

---

### RollingMedianBy

Apply the rolling median over a dynamic window defined by the values in the `by` column.

```csharp
public Expr RollingMedianBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new expression representing the dynamic rolling median.

---

### RollingMin

Apply a rolling min (moving min) over a window.

```csharp
public Expr RollingMin(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new expression representing the rolling mean.

**Example**

```csharp
// Rolling mean of 3 rows ("3i"), centered
df.Select(
    Col("val").RollingMean("3i", minPeriods: 1, center: true).Alias("roll_mean")
);
```

---

### RollingMinBy

Apply the rolling minimum over a dynamic window defined by the values in the `by` column.

```csharp
public Expr RollingMinBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new expression representing the dynamic rolling minimum.

---

### RollingQuantile

Apply a rolling quantile over a fixed window.

```csharp
public Expr RollingQuantile(
    double quantile,
    QuantileMethod method,
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `quantile` | `double` | Quantile between 0.0 and 1.0 (e.g., 0.5 for median). |
| `method` | `QuantileMethod` | Interpolation method when the quantile lies between two data points. |
| `windowSize` | `IntoDuration` | The size of the window. <br>Format: `"3i"` (3 rows) or just a number string `"3"`.<br><br>For time-based windows (e.g. "2h"), use `Expr.RollingQuantileBy` instead. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights for the window. The length must match the parsed window size. <br>If `null`, equal weights are used. |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new expression representing the rolling quantile.

---

### RollingQuantileBy

Compute the rolling quantile over a dynamic window defined by the values in the `by` column.

```csharp
public Expr RollingQuantileBy(
    double quantile,
    QuantileMethod method,
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `quantile` | `double` | Quantile between 0.0 and 1.0 (e.g., 0.5 for median). |
| `method` | `QuantileMethod` | Interpolation method when the quantile lies between two data points. |
| `windowSize` | `IntoDuration` | The size of the time window as a `System.TimeSpan`. <br>This will be automatically converted to a Polars duration string (e.g., `01:30:00` -&gt; `"1h30m"`). |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new expression representing the dynamic rolling quantile.

---

### RollingRank

Apply a rolling rank (moving rank) over a window.

```csharp
public Expr RollingRank(
    IntoDuration windowSize,
    int minPeriods = 1,
    RankMethod method = RankMethod.Average,
    ulong? seed = null,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `method` | `RankMethod` | The method used to assign ranks to tied elements. See `RankMethod` for details. Default is `RankMethod.Average`. |
| `seed` | `ulong?` | If method="random", use this as seed. |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new expression representing the rolling mean.

**Example**

```csharp
// Rolling mean of 3 rows ("3i"), centered
df.Select(
    Col("val").RollingMean("3i", minPeriods: 1, center: true).Alias("roll_mean")
);
```

---

### RollingRankBy

Compute the rolling rank over a dynamic window defined by the values in the `by` column.

```csharp
public Expr RollingRankBy(
    IntoDuration windowSize,
    Expr by,
    RollingRankMethod method = RollingRankMethod.Average,
    ulong? seed = null,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `method` | `RollingRankMethod` | The method used to assign ranks to tied elements. |
| `seed` | `ulong?` | Seed for the random method (only relevant when method is Random). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new expression representing the dynamic rolling rank.

---

### RollingSkew

Apply a rolling skew (moving skew) over a window.

```csharp
public Expr RollingSkew(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false,
    bool bias = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |
| `bias` | `bool` | - |

**Returns:** A new expression representing the rolling mean.

**Example**

```csharp
// Rolling mean of 3 rows ("3i"), centered
df.Select(
    Col("val").RollingMean("3i", minPeriods: 1, center: true).Alias("roll_mean")
);
```

---

### RollingStd

Apply a rolling standard deviation (moving std) over a window.

```csharp
public Expr RollingStd(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new expression representing the rolling mean.

**Example**

```csharp
// Rolling mean of 3 rows ("3i"), centered
df.Select(
    Col("val").RollingMean("3i", minPeriods: 1, center: true).Alias("roll_mean")
);
```

---

### RollingStdBy

Apply the rolling standard deviation over a dynamic window defined by the values in the `by` column.

```csharp
public Expr RollingStdBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new expression representing the dynamic rolling standard deviation.

---

### RollingSum

Apply a rolling sum (moving sum) over a window.

```csharp
public Expr RollingSum(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |

**Returns:** A new expression representing the rolling mean.

**Example**

```csharp
// Rolling mean of 3 rows ("3i"), centered
df.Select(
    Col("val").RollingMean("3i", minPeriods: 1, center: true).Alias("roll_mean")
);
```

---

### RollingSumBy

Apply a rolling sum over a dynamic window defined by the values in the `by` column.

```csharp
public Expr RollingSumBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |

**Returns:** A new expression representing the dynamic rolling sum.

---

### RollingVar

Apply a rolling variance (moving var) over a window.

```csharp
public Expr RollingVar(
    IntoDuration windowSize,
    int minPeriods = 1,
    double[]? weights = null,
    bool center = false,
    byte ddof = 1
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the window formatted as a string duration. Or use .NET TimeSpan directly. <br>Examples: `"3i"` (3 index rows), `"1d"` (1 day), `"1h"` (1 hour). |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a value (otherwise `null`). |
| `weights` | `double[]?` | Optional weights to apply to the window. <br>The length of the array should match the window size (if using fixed row windows).<br><br>Default is `null` (unweighted). |
| `center` | `bool` | If `true`, the window is centered on the current observation. <br>Default is `false` (right-aligned window, `[i-window, i]`). |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. <br>By default ddof is 1. |

**Returns:** A new expression representing the rolling variance.

---

### RollingVarBy

Apply the rolling variance over a dynamic window defined by the values in the `by` column.

```csharp
public Expr RollingVarBy(
    IntoDuration windowSize,
    Expr by,
    int minPeriods = 1,
    ClosedInterval closed = ClosedInterval.Left,
    byte ddof = 1
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `windowSize` | `IntoDuration` | The size of the dynamic window. <br>Supported duration strings: `"1d"`, `"2h"`, `"10s"`, `"500ms"`, etc. |
| `by` | `Expr` | The column used to define the window (the "time" axis). <br>Typically a `Date` or `DateTime` column, but can also be monotonic integers. |
| `minPeriods` | `int` | The minimum number of observations in the window required to have a non-null result. |
| `closed` | `ClosedInterval` | Defines how the window interval is closed. Default is `ClosedInterval.Left``[t - window, t)`. |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. <br>By default ddof is 1. |

**Returns:** A new expression representing the dynamic rolling variance.

---

### Round

Round underlying floating point data by decimals digits.

```csharp
public Expr Round(uint decimals = 0, RoundMode mode = RoundMode.HalfToEven)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `decimals` | `uint` | Number of decimals to round by. |
| `mode` | `RoundMode` | The rounding strategy used. A “rounded value” is a value with at most decimals decimal places (e.g. integers when decimals=0, multiples of 0.1 when decimals=1, 0.01 when decimals=2, and so on). Strategies that start with half_ round all values to the nearest rounded value, only using the strategy to break ties when a value falls exactly between two rounded values (e.g. 0.5 when decimals=0, 0.05 when decimals=1). Other rounding strategies specify explicitly which rounded value is chosen and always apply (not just for tiebreaks). |

---

### RoundSigFigs

Round to a number of significant figures.

```csharp
public Expr RoundSigFigs(int digits)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `digits` | `int` | Number of significant figures to round to. |

---

### Sample

Sample from this expression.

```csharp
public Expr Sample(
    ulong n = 1,
    bool withReplacement = false,
    bool? shuffle = null,
    ulong? seed = null
)

public Expr Sample(
    double fraction,
    bool withReplacement = false,
    bool? shuffle = null,
    ulong? seed = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `ulong` | Number of items to return. Default to 1 |
| `withReplacement` | `bool` | Allow values to be sampled more than once. |
| `shuffle` | `bool?` | Shuffle the order of sampled data points. |
| `seed` | `ulong?` | Seed for the random number generator. If set to None (default), a random seed is generated for each sample operation. |
| `fraction` | `double` | Fraction of items to return. |

---

### SearchSorted

Find indices where elements should be inserted to maintain order (Binary Search).

```csharp
public Expr SearchSorted(IntoExpr element, SearchSortedSide side = SearchSortedSide.Any, bool descending = false)
public Expr SearchSorted<T>(IEnumerable<T> element, SearchSortedSide side = SearchSortedSide.Any, bool descending = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `element` | `IntoExpr / IEnumerable<T>` | The element expression to insert/search. |
| `side` | `SearchSortedSide` | The insertion side (Any, Left, Right). Default is Any. |
| `descending` | `bool` | Whether the target column is sorted in descending order. Default is false. |

---

### SetSorted

Indicate that the expression is sorted. This is a hint to the optimizer and does not actually sort the data.

```csharp
public Expr SetSorted(bool descending = false, bool nullsLast = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `descending` | `bool` | Whether the column is sorted in descending order. |
| `nullsLast` | `bool` | Whether null values appear last. (Placeholder for Polars 0.54+) |

**Returns:** A new expression with the sorted flag set.

---

### Shift

Shift values by the given number of indices. Positive values shift downstream, negative values shift upstream.

Shift values by 1 index downstream.

```csharp
public Expr Shift(Expr n)
public Expr Shift()
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `Expr` | - |

---

### Shuffle

Shuffle the contents of this expression.Note this is shuffled independently of any other column or Expression. If you want each row to stay the same use df.sample(shuffle=True)

```csharp
public Expr Shuffle(ulong? seed = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `seed` | `ulong?` | Seed for the random number generator. If set to None (default), a random seed is generated each time the shuffle is called. |

---

### Sign

Compute the element-wise sign (-1, 0, 1).

```csharp
public Expr Sign()
```

---

### Sin

Compute the element-wise sine.

```csharp
public Expr Sin()
```

---

### Sinh

```csharp
public Expr Sinh()
```

---

### Skew

Compute the sample skewness of a data set.

```csharp
public Expr Skew(bool bias = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `bias` | `bool` | If False, the calculations are corrected for statistical bias. |

**Returns:** A series which length is 1

---

### Slice

Get a slice of this expression.

```csharp
public Expr Slice(long offset, ulong? length = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `offset` | `long` | Start index. Negative indexing is supported. |
| `length` | `ulong?` | Length of the slice. If set to None, all rows starting at the offset will be selected. |

---

### Sort

Sort the expression.

```csharp
public Expr Sort(
    bool descending = false,
    bool nullsLast = false,
    bool multithreaded = true,
    bool maintainOrder = false,
    uint? limit = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `descending` | `bool` | If true, sort in descending order. Default is false. |
| `nullsLast` | `bool` | Whether to place null values last. Default is false. |
| `multithreaded` | `bool` | If true, sort in multiple threads. Default is true. |
| `maintainOrder` | `bool` | If true, maintain the order of equal elements. Default is false. |
| `limit` | `uint?` | Limit the sort output (for optimization purposes). |

---

### SortBy

Sort this column by the ordering of other columns. 

When used in a projection/selection context, the whole column is sorted.

When used in a group by context, the groups are sorted.

Sort this column by the ordering of other columns using uniform sorting options.

```csharp
public Expr SortBy(
    IEnumerable<Expr> by,
    bool[]? descending = null,
    bool[]? nullsLast = null,
    bool multithreaded = true,
    bool maintainOrder = false
)

public Expr SortBy(
    IEnumerable<Expr> by,
    bool descending = false,
    bool nullsLast = false,
    bool multithreaded = true,
    bool maintainOrder = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `IEnumerable<Expr>` | Column(s) to sort by. |
| `descending` | `bool[]?` | Sort in descending order. When sorting by multiple columns, can be specified per column by passing a sequence of booleans. |
| `nullsLast` | `bool[]?` | Place null values last; can specify a single boolean applying to all columns or a sequence of booleans for per-column control. |
| `multithreaded` | `bool` | Sort using multiple threads. |
| `maintainOrder` | `bool` | Whether the order should be maintained if elements are equal. |

---

### Sqrt

Calculate the square root of the expression.

```csharp
public Expr Sqrt()
```

---

### Std

Get the standard deviation.

```csharp
public Expr Std(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1. |

**Returns:** A series which length is 1

---

### Sum

Calculate the sum of the values in the group or column. 

Behavior depends on context: 

- In `DataFrame.GroupBy`: Calculates the sum for each group.
- In `DataFrame.Select`: Calculates the sum of the entire column (scalar result).

```csharp
public Expr Sum()
```

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    group = new[] { "A", "A", "B", "B" },
    val = new[] { 1, 2, 3, 4 }
});

// 1. GroupBy Aggregation
df.GroupBy("group").Agg(
    Pl.Col("val").Sum().Alias("sum"),
    Pl.Col("val").Mean().Alias("mean")
).Show();
/* Output:
shape: (2, 3)
┌───────┬─────┬──────┐
│ group ┆ sum ┆ mean │
│ ---   ┆ --- ┆ ---  │
│ str   ┆ i32 ┆ f64  │
╞═══════╪═════╪══════╡
│ A     ┆ 3   ┆ 1.5  │
│ B     ┆ 7   ┆ 3.5  │
└───────┴─────┴──────┘
*/

// 2. Global Aggregation (Select)
df.Select(
    Pl.Col("val").Sum().Alias("total_sum"),
    Pl.Col("val").Count().Alias("total_count")
).Show();
/* Output:
shape: (1, 2)
┌───────────┬─────────────┐
│ total_sum ┆ total_count │
│ ---       ┆ ---         │
│ i32       ┆ u32         │
╞═══════════╪═════════════╡
│ 10        ┆ 4           │
└───────────┴─────────────┘
*/
```

---

### Tail

Get the last n rows.

```csharp
public Expr Tail(int n = 10)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int` | Number of rows to return. |

---

### Take

LINQ-like alias for Gather.

```csharp
public Expr Take(IntoExprColumn indices)
public Expr Take(ReadOnlySpan<int> indices)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `indices` | `IntoExprColumn / ReadOnlySpan<int>` | - |

---

### Tan

Compute the element-wise tangent.

```csharp
public Expr Tan()
```

---

### Tanh

```csharp
public Expr Tanh()
```

---

### ToHtml

Generates a rich HTML view of the expression including its compact string representation and AST tree.

```csharp
public string ToHtml(PolarsSchema? schema = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `schema` | `PolarsSchema?` | Optional schema context to resolve field types. |

**Returns:** An HTML representation formatted for interactive environments.

---

### ToPhysical

Cast to physical representation of the logical dtype.

```csharp
public Expr ToPhysical()
```

---

### ToSelector

```csharp
public Selector ToSelector()
```

---

### TopK

Get the top k largest values. 

This is much faster than Sort().Head(k) for large datasets.

```csharp
public Expr TopK(int k)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `k` | `int` | - |

---

### TopKBy

Get the top `k` rows according to the sorting criteria defined by `by`.

Get the top `k` rows according to a single sorting criterion. 

This is a convenience overload for a single expression.

```csharp
public Expr TopKBy(int k, Expr[] by, bool[] reverse)
public Expr TopKBy(int k, Expr by, bool reverse = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `k` | `int` | Number of rows to return. |
| `by` | `Expr[]` | The expressions (columns) to sort by. |
| `reverse` | `bool[]` | Controls the sorting direction for each expression in `by`. <br>For TopK: - `false` (default): Sorts descending (picks largest values). - `true`: Sorts ascending (picks smallest values, acting like BottomK for this column).<br> Length must match `by`. |

**Returns:** A new expression.

---

### Truncate

Truncate numeric data toward zero to decimals number of decimal places.

```csharp
public Expr Truncate(uint decimals = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `decimals` | `uint` | Number of decimal places to truncate to. |

---

### Unique

Get unique values of this expression.

```csharp
public Expr Unique(bool maintainOrder = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `maintainOrder` | `bool` | Maintain order of data. This requires more work. |

---

### UniqueCounts

Return a count of the unique values in the order of appearance. This method differs from value_counts in that it does not return the values, only the counts and might be faster

```csharp
public Expr UniqueCounts()
```

---

### UpperBound

Calculate the upper bound. Returns a unit Series with the highest value possible for the dtype of this expression.

```csharp
public Expr UpperBound()
```

---

### ValueCounts

Count the occurrence of unique values.

```csharp
public Expr ValueCounts(
    bool sort = false,
    bool parallel = false,
    string? name = null,
    bool normalize = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `sort` | `bool` | Sort the output by count, in descending order. If set to False (default), the order is non-deterministic. |
| `parallel` | `bool` | Execute the computation in parallel.This option should likely not be enabled in a group by context, as the computation will already be parallelized per group. |
| `name` | `string?` | Give the resulting count column a specific name; if normalize is True this defaults to “proportion”, otherwise defaults to “count”. |
| `normalize` | `bool` | If True, the count is returned as the relative frequency of unique values normalized to 1.0. |

**Returns:** Expression of type Struct, mapping unique values to their count (or proportion).

---

### Var

Get the variance.

```csharp
public Expr Var(byte ddof = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ddof` | `byte` | “Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1. |

**Returns:** A series which length is 1

---

