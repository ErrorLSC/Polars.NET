---
uid: Polars.CSharp.Then
title: Then
---

# Then

Namespace: `Polars.CSharp`

## Instance Methods

### Otherwise

Define a default for the `when-then-otherwise` expression.

```csharp
public Expr Otherwise(IntoExprColumn statement)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `statement` | `IntoExprColumn` | - |

---

### When

```csharp
public ChainedWhen When(IntoExprColumn condition)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `condition` | `IntoExprColumn` | - |

---

