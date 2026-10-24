---
uid: Polars.CSharp.Categories
title: "Categories"
---

# Categories

Namespace: `Polars.CSharp`

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `Hash` | `ulong` | - |

## Constructors & Static Methods

### Categories

```csharp
public Categories(string? name = null, string? nameSpace = "", CategoricalPhysical physical = CategoricalPhysical.U32)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `name` | `string?` | - |
| `nameSpace` | `string?` | - |
| `physical` | `CategoricalPhysical` | - |

---

### Global

```csharp
public static Categories Global()
```

---

### Random

```csharp
public static Categories Random(string nameSpace = "", CategoricalPhysical physical = CategoricalPhysical.U32)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `nameSpace` | `string` | - |
| `physical` | `CategoricalPhysical` | - |

---

## Operators

### operator == (Equality)

```csharp
public static bool operator ==(Categories? left, Categories? right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Categories?` | - |
| `right` | `Categories?` | - |

---

### operator != (Inequality)

```csharp
public static bool operator !=(Categories? left, Categories? right)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `left` | `Categories?` | - |
| `right` | `Categories?` | - |

---

## Instance Methods

### Freeze

```csharp
public FrozenCategories Freeze()
```

---

### IsGlobal

```csharp
public bool IsGlobal()
```

---

### Name

```csharp
public string Name()
```

---

### NameSpace

```csharp
public string NameSpace()
```

---

### Physical

```csharp
public CategoricalPhysical Physical()
```

---

