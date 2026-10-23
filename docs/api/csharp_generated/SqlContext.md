---
uid: Polars.CSharp.SqlContext
title: SqlContext
---

# SqlContext

Namespace: `Polars.CSharp`

A context for executing SQL queries against DataFrames and LazyFrames. 

Polars supports a subset of ANSI SQL. The query is converted into a logical plan and optimized/executed by the Polars engine.

## Constructors & Static Methods

### SqlContext

Create a new SQL Context.

```csharp
public SqlContext()
```

---

## Instance Methods

### Execute

Execute a SQL query.

```csharp
public LazyFrame Execute(string query)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `query` | `string` | The SQL query string. |

**Returns:** A new LazyFrame representing the query result.

---

### GetTables

Get the names of all registered tables, in sorted order.

```csharp
public string[] GetTables()
```

**Returns:** An array of registered table names.

---

### Register

Register a LazyFrame as a table in the SQL context.

Register a DataFrame as a table (convenience method). Converts DataFrame to LazyFrame internally.

```csharp
public void Register(string tableName, LazyFrame lf)
public void Register(string tableName, DataFrame df)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `tableName` | `string` | The name to use in SQL queries (e.g., 'SELECT * FROM tableName') |
| `lf` | `LazyFrame` | The LazyFrame to register. |
| `df` | `DataFrame` | - |

---

### UnRegister

UnRegister a LazyFrame as a table.

```csharp
public void UnRegister(string tableName)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `tableName` | `string` | - |

---

