---
uid: Polars.CSharp.Expr.Meta
title: "Expr.Meta (MetaOps)"
---

# Expr.Meta (MetaOps)

Namespace: `Polars.CSharp`

## Instance Methods

### AsExpression

Return the original expression.

```csharp
public Expr AsExpression()
```

---

### FormatTree

Format the expression as a tree.

```csharp
public string FormatTree(bool displayAsDot = false, PolarsSchema? schema = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `displayAsDot` | `bool` | - |
| `schema` | `PolarsSchema?` | - |

---

### HasMultipleOutputs

Indicate if this expression expands into multiple expressions.

```csharp
public bool HasMultipleOutputs()
```

---

### IsColumn

Indicate if this expression is a basic (non-regex) unaliased column.

```csharp
public bool IsColumn()
```

---

### IsColumnSelection

Indicate if this expression only selects columns (optionally with aliasing). This can include bare columns, columns matched by regex or dtype, selectors and exclude ops, and (optionally) column/expression aliasing.

```csharp
public bool IsColumnSelection(bool allowAliasing = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `allowAliasing` | `bool` | If False (default), any aliasing is not considered to be column selection. Set True to allow for column selection that also includes aliasing. |

---

### IsLiteral

Indicate if this expression is a literal value (optionally aliased).

```csharp
public bool IsLiteral(bool allowAliasing = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `allowAliasing` | `bool` | If False (default), any aliasing is not considered to be column selection. Set True to allow for column selection that also includes aliasing. |

---

### IsRegexProjection

Indicate if this expression expands to columns that match a regex pattern.

```csharp
public bool IsRegexProjection()
```

---

### NotEquals

Indicate if this expression is NOT the same as another expression.

```csharp
public bool NotEquals(Expr? expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `Expr?` | - |

---

### OutputName

Get the column name that this expression would produce. It may not always be possible to determine the output name.

```csharp
public string OutputName()
```

---

### Pop

Pop the latest expression and return the input(s) of the popped expression.

```csharp
public Expr[] Pop()
```

---

### RootNames

Get a list with the root column name.

```csharp
public string[] RootNames()
```

---

### UndoAliases

Indicate if this expression expands into multiple expressions.

```csharp
public Expr UndoAliases()
```

---

