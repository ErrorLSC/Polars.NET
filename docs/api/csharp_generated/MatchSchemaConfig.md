---
uid: Polars.CSharp.MatchSchemaConfig
title: MatchSchemaConfig
---

# MatchSchemaConfig

Namespace: `Polars.CSharp`

Configuration for schema matching policies during a MatchToSchema operation. Provides fine-grained control over how missing columns, struct fields, and type castings are handled.

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `ExtraStructFields` | `ExtraColumnsPolicy` | Policy for handling extra fields within nested Struct columns that are not in the target schema. Defaults to `ExtraColumnsPolicy.Raise`. |
| `FloatCast` | `UpcastOrForbid` | Policy for handling floating-point type casting (e.g., allowing upcast from Float32 to Float64). Defaults to `UpcastOrForbid.Forbid`. |
| `IntegerCast` | `UpcastOrForbid` | Policy for handling integer type casting (e.g., allowing upcast from Int32 to Int64). Defaults to `UpcastOrForbid.Forbid`. |
| `MissingColumns` | `MissingColumnAction` | Policy for handling top-level columns present in the target schema but missing in the frame. Defaults to `MissingColumnAction.Raise`. |
| `MissingStructFields` | `MissingColumnsPolicy` | Policy for handling missing fields within nested Struct columns. Defaults to `MissingColumnsPolicy.Raise`. |

## Constructors & Static Methods

### MatchSchemaConfig

```csharp
public MatchSchemaConfig()
```

---

