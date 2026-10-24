---
uid: Polars.CSharp.AsOfTolerance
title: "AsOfTolerance"
---

# AsOfTolerance

Namespace: `Polars.CSharp`

Smart tolerance type for AsOf joins. Implicitly converts from string ("2h"), TimeSpan, int, long, or double.

## Operators

### implicit operator

```csharp
public static implicit operator AsOfTolerance(string val)
public static implicit operator AsOfTolerance(TimeSpan val)
public static implicit operator AsOfTolerance(long val)
public static implicit operator AsOfTolerance(int val)
public static implicit operator AsOfTolerance(double val)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `val` | `string / TimeSpan / long / int / double` | - |

---

