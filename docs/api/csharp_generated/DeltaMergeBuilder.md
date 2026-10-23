---
uid: Polars.CSharp.DeltaMergeBuilder
title: DeltaMergeBuilder
---

# DeltaMergeBuilder

Namespace: `Polars.CSharp`

A builder for constructing ordered Delta Lake Merge operations. This ensures that matched and not-matched conditions are evaluated exactly in the order they are chained by the user, adhering to standard SQL MERGE semantics.

## Instance Methods

### Execute

Executes the constructed merge operation against the Delta Table.

```csharp
public void Execute()
```

---

### WhenMatchedDelete

Delete the matched target row.

Delete the matched target row using a safe context builder.

```csharp
public DeltaMergeBuilder WhenMatchedDelete(Expr? condition = null)
public DeltaMergeBuilder WhenMatchedDelete(Func<MergeContext, Expr> conditionBuilder)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `condition` | `Expr?` | Optional condition for the delete to occur. If null, deletes all matched rows. |
| `conditionBuilder` | `Func<MergeContext, Expr>` | - |

---

### WhenMatchedUpdate

Update the matched target row with source data.

Update the matched target row with source data using a safe context builder.

```csharp
public DeltaMergeBuilder WhenMatchedUpdate(Expr? condition = null)
public DeltaMergeBuilder WhenMatchedUpdate(Func<MergeContext, Expr> conditionBuilder)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `condition` | `Expr?` | Optional condition for the update to occur. If null, updates all matched rows. |
| `conditionBuilder` | `Func<MergeContext, Expr>` | - |

---

### WhenNotMatchedBySourceDelete

Delete the target row when it does not exist in the source data.

Delete the target row when it does not exist in the source data using a safe context builder.

```csharp
public DeltaMergeBuilder WhenNotMatchedBySourceDelete(Expr? condition = null)
public DeltaMergeBuilder WhenNotMatchedBySourceDelete(Func<MergeContext, Expr> conditionBuilder)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `condition` | `Expr?` | Optional condition for the target delete to occur. If null, deletes all unmatched target rows. |
| `conditionBuilder` | `Func<MergeContext, Expr>` | - |

---

### WhenNotMatchedInsert

Insert a new row from the source data when it does not match any target row.

Insert a new row from the source data using a safe context builder.

```csharp
public DeltaMergeBuilder WhenNotMatchedInsert(Expr? condition = null)
public DeltaMergeBuilder WhenNotMatchedInsert(Func<MergeContext, Expr> conditionBuilder)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `condition` | `Expr?` | Optional condition for the insert to occur. If null, inserts all unmatched source rows. |
| `conditionBuilder` | `Func<MergeContext, Expr>` | - |

---

