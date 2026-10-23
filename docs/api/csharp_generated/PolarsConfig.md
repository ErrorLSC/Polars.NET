---
uid: Polars.CSharp.PolarsConfig
title: PolarsConfig
---

# PolarsConfig

Namespace: `Polars.CSharp`

Config for Polars.NET

## Properties & Indexers

| Property | Type | Description |
| :--- | :--- | :--- |
| `ClientEvaluationPolicy` | `ClientEvaluationPolicy` | Gets the current client-side evaluation policy. |
| `this[string key]` | `string` | string style = cfg["decimal_separator"]; |

## Constructors & Static Methods

### PolarsConfig

Create a new PolarsConfig instance.

```csharp
public PolarsConfig()
```

---

### SetEnvVar

Inject Environment var to Rust

```csharp
public static void SetEnvVar(string key, string value)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `key` | `string` | - |
| `value` | `string` | - |

---

### SetEnvVars

Inject Environment var to Rust by KeyValuePair

Inject Environment var to Rust by KeyValuePair tuple

```csharp
public static void SetEnvVars(IEnumerable<KeyValuePair<string, string>> variables)
public static void SetEnvVars(params (string, string)[] variables)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `variables` | `IEnumerable<KeyValuePair<string, string>> / (string, string)[]` | Collection or dictionary contains environment var keyvalue pair. |

---

## Instance Methods

### BeginScope

Activates this config profile by backing up the current global state and applying overrides.

```csharp
public PolarsConfigScope BeginScope()
```

---

### Load

Load (and set) previously saved Config options from a JSON string.

```csharp
public void Load(string cfgJson)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `cfgJson` | `string` | - |

---

### LoadFromFile

Load (and set) previously saved Config options from file.

```csharp
public void LoadFromFile(string path)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | - |

---

### RestoreDefaults

Reset all polars Config settings to their default state.

```csharp
public void RestoreDefaults()
```

---

### Save

Save the current set of Config options as a JSON string.

```csharp
public string Save(bool ifSet = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ifSet` | `bool` | By default this will save the state of all configuration options; set to False to save only those that have been set to a non-default value. |

---

### SaveToFile

Save the current set of Config options as a JSON file.

```csharp
public void SaveToFile(string path, bool ifSet = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `path` | `string` | - |
| `ifSet` | `bool` | - |

---

### SetAsciiTables

Use ASCII characters to display table outlines. Set False to revert to the standard UTF8_FULL_CONDENSED formatting style.

```csharp
public PolarsConfig SetAsciiTables(bool? active = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `active` | `bool?` | - |

---

### SetClientEvaluationPolicy

Sets the client-side evaluation policy for operations or expressions that cannot be pushed down to the native Polars engine.

```csharp
public PolarsConfig SetClientEvaluationPolicy(ClientEvaluationPolicy policy = ClientEvaluationPolicy.Strict)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `policy` | `ClientEvaluationPolicy` | Policy mode: Strict (default), Warn, or Allow. |

---

### SetDecimalSeparator

Set the decimal separator character.

```csharp
public PolarsConfig SetDecimalSeparator(char? separator = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `separator` | `char?` | Character to use as the decimal separator. Set to None to revert to the default ('.'). |

---

### SetDeltaMaxRetries

Set the maximum Retires for Delta Table operations.

```csharp
public PolarsConfig SetDeltaMaxRetries(int? retries)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `retries` | `int?` | Max retries for delta module |

---

### SetEngineAffinity

Set which engine to use by default.

```csharp
public PolarsConfig SetEngineAffinity(Engine? engine = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `engine` | `Engine?` | The default execution engine Polars will attempt to use when calling .Collect(). However, the query is not guaranteed to execute with the specified engine. |

---

### SetFloatPrecision

Control the number of decimal places displayed for floating point values.

```csharp
public PolarsConfig SetFloatPrecision(int? precision = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `precision` | `int?` | Number of decimal places to display; set to None to revert to the default/standard behaviour. |

---

### SetFormatFloat

Control how floating point values are displayed.

```csharp
public PolarsConfig SetFormatFloat(FloatFormat? format = 0)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `format` | `FloatFormat?` | How to format floating point numbers |

---

### SetFormatStringLength

Set the number of characters used to display string values.

```csharp
public PolarsConfig SetFormatStringLength(int? n)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int?` | Number of characters to display. |

---

### SetFormatTableCellListLength

Set the number of elements to display for List values. Empty lists will always print “[]”. 

Negative values will result in all values being printed.

A value of 0 will always “[…]” for lists with contents.

 A value of 1 will print only the final item in the list.

```csharp
public PolarsConfig SetFormatTableCellListLength(int? n)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int?` | Number of values to display. |

---

### SetQueryCacheCapacity

Set the maximum capacity for the LINQ Query Plan Cache.

```csharp
public PolarsConfig SetQueryCacheCapacity(int? capacity = 1024)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `capacity` | `int?` | Maximum number of query plan templates to retain. Pass null to reset to default. |

---

### SetQueryCacheEnabled

Enable or disable the LINQ Query Plan Cache globally.

```csharp
public PolarsConfig SetQueryCacheEnabled(bool? enabled = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `enabled` | `bool?` | True to enable caching, false to disable, null to reset to default. |

---

### SetStreamingChunkSize

Overwrite chunk size used in streaming engine. 

By default, the chunk size is determined by the schema and size of the thread pool.

 For some datasets (esp. when you have large string elements) this can be too optimistic and lead to Out of Memory errors.

```csharp
public PolarsConfig SetStreamingChunkSize(ulong? size)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `size` | `ulong?` | Number of rows per chunk. Every thread will process chunks of this size. |

---

### SetTableCellAlignment

Set table cell alignment.

```csharp
public PolarsConfig SetTableCellAlignment(Alignment? format)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `format` | `Alignment?` | - |

---

### SetTableCellNumericAlignment

Set table cell alignment for numeric columns.

```csharp
public PolarsConfig SetTableCellNumericAlignment(Alignment? format)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `format` | `Alignment?` | - |

---

### SetTableCols

Set the number of columns that are visible when displaying tables.

```csharp
public PolarsConfig SetTableCols(int? n)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int?` | Number of columns to display; if n is less than 0 (eg: -1), display all columns. |

---

### SetTableColumnDataTypeInline

Display the data type next to the column name (to the right, in parentheses).

```csharp
public PolarsConfig SetTableColumnDataTypeInline(bool? active = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `active` | `bool?` | - |

---

### SetTableDataFrameShapeBelow

Print the DataFrame shape information below the data when displaying tables.

```csharp
public PolarsConfig SetTableDataFrameShapeBelow(bool? active = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `active` | `bool?` | - |

---

### SetTableFormatting

Sets the text/ASCII formatting style for outputting DataFrames.

```csharp
public PolarsConfig SetTableFormatting(TableFormatting? format = null, bool? roundedCorners = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `format` | `TableFormatting?` | - |
| `roundedCorners` | `bool?` | - |

---

### SetTableHideColumnDataTypes

Hide table column data types (i64, f64, str etc.).

```csharp
public PolarsConfig SetTableHideColumnDataTypes(bool? active = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `active` | `bool?` | - |

---

### SetTableHideColumnNames

Hide table column names.

```csharp
public PolarsConfig SetTableHideColumnNames(bool? active = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `active` | `bool?` | - |

---

### SetTableHideDataFrameShape

Hide the DataFrame shape information when displaying tables.

```csharp
public PolarsConfig SetTableHideDataFrameShape(bool? active = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `active` | `bool?` | - |

---

### SetTableHideDataTypeSeparator

Hide the ‘—’ separator displayed between the column names and column types.

```csharp
public PolarsConfig SetTableHideDataTypeSeparator(bool? active = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `active` | `bool?` | - |

---

### SetTableRows

Set the max number of rows used to draw the table (both Dataframe and Series).

```csharp
public PolarsConfig SetTableRows(int? n)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `int?` | Number of rows to display; if n is less than 0 (eg: -1), display all rows (DataFrame) and all elements (Series). |

---

### SetTableWidthChars

Set the maximum width of a table in characters.

```csharp
public PolarsConfig SetTableWidthChars(int? width)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `width` | `int?` | Maximum table width in characters; if n is less than 0 (eg: -1), display full width. |

---

### SetThousandsSeparator

Configures the thousands separator with a shortcut boolean.

Configures the thousands separator with a specific single character. Set to null to clear/reset the separator.

```csharp
public PolarsConfig SetThousandsSeparator(bool useDefaultFormat)
public PolarsConfig SetThousandsSeparator(char? separator = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `useDefaultFormat` | `bool` | If true, applies standard English format: sets thousands separator to ',' and decimal separator to '.'. If false, clears the thousands separator. |
| `separator` | `char?` | - |

---

### SetTrimDecimalZeros

Strip trailing zeros from Decimal data type values.

```csharp
public PolarsConfig SetTrimDecimalZeros(bool? active = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `active` | `bool?` | Enable stripping of trailing ‘0’ characters from Decimal values. |

---

### SetVerbose

Configures whether Polars should run in verbose mode, printing query profiles and optimization decisions.

```csharp
public PolarsConfig SetVerbose(bool? verbose = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `verbose` | `bool?` | - |

---

### Status

Show the current state of all Config variables in the environment as a dict.

```csharp
public Dictionary<string, string> Status(bool ifSet = false, bool envOnly = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `ifSet` | `bool` | By default this will show the state of all Config environment variables. change this to True to restrict the returned dictionary to include only those that have been set to a specific value. |
| `envOnly` | `bool` | Include only Config environment variables in the output; some options (such as “set_fmt_float”) are set directly, not via an environment variable. |

---

