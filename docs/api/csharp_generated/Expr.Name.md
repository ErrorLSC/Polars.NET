---
uid: Polars.CSharp.Expr.Name
title: Expr.Name (NameOps)
---

# Expr.Name (NameOps)

Namespace: `Polars.CSharp`

Offers methods for renaming columns.

## Instance Methods

### Keep

Keep the original root name of the expression.

```csharp
public Expr Keep()
```

---

### Map

Rename the output of an expression by mapping a function over the root name.

```csharp
public Expr Map(Func<string, string> function)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `function` | `Func<string, string>` | Function that maps a root name to a new name. |

---

### MapFields

Rename fields of a struct by mapping a function over the field name(s).

```csharp
public Expr MapFields(Func<string, string> function)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `function` | `Func<string, string>` | Function that maps a field name to a new name. |

---

### Prefix

Prefix the column name with a specified string.

```csharp
public Expr Prefix(string prefix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `prefix` | `string` | - |

---

### PrefixFields

Add a prefix to all field names of a struct.

```csharp
public Expr PrefixFields(string prefix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `prefix` | `string` | Prefix to add to the field name. |

---

### Replace

Replace matching regex/literal substring in the name with a new value.

```csharp
public Expr Replace(string pattern, string value, bool literal = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `pattern` | `string` | A valid regular expression pattern, compatible with the regex crate. |
| `value` | `string` | String that will replace the matched substring. |
| `literal` | `bool` | Treat pattern as a literal string, not a regex. |

---

### Suffix

Suffix the column name with a specified string.

```csharp
public Expr Suffix(string suffix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `suffix` | `string` | - |

---

### SuffixFields

Add a suffix to all field names of a struct.

```csharp
public Expr SuffixFields(string suffix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `suffix` | `string` | Suffix to add to the field name. |

---

### ToLowercase

Make the root column name lowercase.

```csharp
public Expr ToLowercase()
```

---

### ToUppercase

Make the root column name uppercase.

```csharp
public Expr ToUppercase()
```

---

