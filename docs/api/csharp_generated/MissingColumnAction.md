---
uid: Polars.CSharp.MissingColumnAction
title: MissingColumnAction
---

# MissingColumnAction

Namespace: `Polars.CSharp`

Represents the action to take when a column from the target schema is missing in the current frame.

## Constructors & Static Methods

### Insert

Insert the missing column with null values.

```csharp
public static MissingColumnAction Insert()
```

---

### InsertWith

Insert the missing column and fill it with the evaluated expression or literal value.

```csharp
public static MissingColumnAction InsertWith(IntoExprColumn expr)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `expr` | `IntoExprColumn` | The expression or primitive value to fill the missing column with. |

---

### Raise

Raise an error if the column is missing.

```csharp
public static MissingColumnAction Raise()
```

---

