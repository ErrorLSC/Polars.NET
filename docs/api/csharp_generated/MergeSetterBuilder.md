---
uid: Polars.CSharp.MergeSetterBuilder
title: MergeSetterBuilder
---

# MergeSetterBuilder

Namespace: `Polars.CSharp`

## Instance Methods

### Set

Update multiple columns using a Selector. The value builder provides the current column name.

```csharp
public MergeSetterBuilder Set(string columnName, Func<MergeContext, IntoExprColumn> valueBuilder)
public MergeSetterBuilder Set(string columnName, IntoExprColumn value)
public MergeSetterBuilder Set(IntoSelector selector, Func<MergeContext, string, IntoExprColumn> valueBuilder)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columnName` | `string` | - |
| `valueBuilder` | `Func<MergeContext, IntoExprColumn> / Func<MergeContext, string, IntoExprColumn>` | - |
| `value` | `IntoExprColumn` | - |
| `selector` | `IntoSelector` | - |

---

