---
uid: Polars.CSharp.PolarsConfigScope
title: PolarsConfigScope
---

# PolarsConfigScope

Namespace: `Polars.CSharp`

Represents a lightweight scope that manages the lifecycle of local configuration changes. Stores a backup snapshot of the system state upon creation and restores it when disposed, preventing side effects from leaking outside the scope.

## Constructors & Static Methods

### PolarsConfigScope

Represents a lightweight scope that manages the lifecycle of local configuration changes. Stores a backup snapshot of the system state upon creation and restores it when disposed, preventing side effects from leaking outside the scope.

```csharp
public PolarsConfigScope(string backupPayload)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `backupPayload` | `string` | - |

---

