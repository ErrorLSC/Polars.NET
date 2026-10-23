---
uid: Polars.CSharp.ExtensionFactory
title: ExtensionFactory
---

# ExtensionFactory

Namespace: `Polars.CSharp`

## Constructors & Static Methods

### ExtensionFactory

```csharp
public ExtensionFactory(object object, IntPtr method)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `object` | `object` | - |
| `method` | `IntPtr` | - |

---

## Instance Methods

### BeginInvoke

```csharp
public IAsyncResult BeginInvoke(
    DataType storage,
    string? metadata,
    AsyncCallback callback,
    object object
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `storage` | `DataType` | - |
| `metadata` | `string?` | - |
| `callback` | `AsyncCallback` | - |
| `object` | `object` | - |

---

### EndInvoke

```csharp
public BaseExtension EndInvoke(IAsyncResult result)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `result` | `IAsyncResult` | - |

---

### Invoke

```csharp
public BaseExtension Invoke(DataType storage, string? metadata)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `storage` | `DataType` | - |
| `metadata` | `string?` | - |

---

