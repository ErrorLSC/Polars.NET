---
uid: Polars.CSharp.Expr.Str
title: Expr.Str (StringOps)
---

# Expr.Str (StringOps)

Namespace: `Polars.CSharp`

Offers multiple methods for checking, parsing, and transforming string columns. Access this via `Expr.Str`.

## Instance Methods

### Contains

Check if the string contains a substring or regex pattern.

```csharp
public Expr Contains(StringOrExpr pattern, bool literal = false, bool strict = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `pattern` | `StringOrExpr` | - |
| `literal` | `bool` | - |
| `strict` | `bool` | - |

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    code = new[] { "ID-001", "ID-002", "ID-999", "XX-000", "ID-ABC" },
    text = new[] { "Polars", "Data", "Rust", null, "123" }
});

df.Select(
    Pl.Col("code"),
    // Replace "-" with "_"
    Pl.Col("code").Str.ReplaceAll("-", "_").Alias("replaced"),
    // Extract numbers using Regex group 1
    Pl.Col("code").Str.Extract(@"(\d+)", 1).Alias("extracted_num"),
    // Check if text contains "a"
    Pl.Col("text").Str.Contains("a").Alias("has_a")
).Show();
/* Output:
shape: (5, 4)
┌────────┬──────────┬───────────────┬───────┐
│ code   ┆ replaced ┆ extracted_num ┆ has_a │
│ ---    ┆ ---      ┆ ---           ┆ ---   │
│ str    ┆ str      ┆ str           ┆ bool  │
╞════════╪══════════╪═══════════════╪═══════╡
│ ID-001 ┆ ID_001   ┆ 001           ┆ true  │
│ ID-002 ┆ ID_002   ┆ 002           ┆ true  │
│ ID-999 ┆ ID_999   ┆ 999           ┆ false │
│ XX-000 ┆ XX_000   ┆ 000           ┆ null  │
│ ID-ABC ┆ ID_ABC   ┆ null          ┆ false │
└────────┴──────────┴───────────────┴───────┘
*/
```

---

### ContainsAny

Use the Aho-Corasick algorithm to find matches. Determines if any of the patterns are contained in the string.

```csharp
public Expr ContainsAny(StringOrExpr patterns, bool asciiCaseInsensitive = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `patterns` | `StringOrExpr` | String patterns to search. |
| `asciiCaseInsensitive` | `bool` | Enable ASCII-aware case-insensitive matching. When this option is enabled, searching will be performed without respect to case for ASCII letters (a-z and A-Z) only. |

---

### CountMatches

Count all successive non-overlapping regex matches.

```csharp
public Expr CountMatches(StringOrExpr pattern, bool literal = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `pattern` | `StringOrExpr` | A valid regular expression pattern, compatible with the regex crate. |
| `literal` | `bool` | Treat pattern as a literal string, not as a regular expression. |

**Returns:** Expression of data type UInt32. Returns null if the original value is null.

---

### Decode

Decode values using the provided encoding.

```csharp
public Expr Decode(TransferEncoding encoding, bool strict = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `encoding` | `TransferEncoding` | The encoding to use. |
| `strict` | `bool` | Raise an error if the underlying value cannot be decoded, otherwise mask out with a null value. |

**Returns:** Expression of data type Binary.

---

### Encode

Encode values using the provided encoding.

```csharp
public Expr Encode(TransferEncoding encoding)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `encoding` | `TransferEncoding` | The encoding to use. |

**Returns:** Expression of data type String.

---

### EndsWith

Check if the string ends with the given suffix.

```csharp
public Expr EndsWith(StringOrExpr suffix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `suffix` | `StringOrExpr` | - |

---

### EscapeRegex

Returns string values with all regular expression meta characters escaped.

```csharp
public Expr EscapeRegex()
```

---

### Extract

Extract the first match of a regex pattern.

```csharp
public Expr Extract(StringOrExpr pattern, int groupIndex = 1)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `pattern` | `StringOrExpr` | Regex pattern with capture groups. |
| `groupIndex` | `int` | The index of the capture group to extract (usually 1). |

---

### ExtractAll

Extract all matches for the given regex pattern. Extract each successive non-overlapping regex match in an individual string as a list. If the haystack string is null, null is returned.

```csharp
public Expr ExtractAll(StringOrExpr pattern)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `pattern` | `StringOrExpr` | A valid regular expression pattern, compatible with the regex crate. |

**Returns:** Expression of data type List(String).

---

### ExtractGroups

Extract all capture groups for the given regex pattern.

```csharp
public Expr ExtractGroups(string pattern)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `pattern` | `string` | A valid regular expression pattern containing at least one capture group, compatible with the regex crate. |

**Returns:** Expression of data type Struct with fields of data type String.

---

### ExtractMany

Use the Aho-Corasick algorithm to extract many matches.This method supports matching on string literals only, and does not support regular expression matching.

```csharp
public Expr ExtractMany(
    IntoExprColumn patterns,
    bool asciiCaseInsensitive = false,
    bool overlapping = false,
    bool leftmost = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `patterns` | `IntoExprColumn` | String patterns to search.Notice:string will be parsed as column name |
| `asciiCaseInsensitive` | `bool` | Enable ASCII-aware case-insensitive matching. When this option is enabled, searching will be performed without respect to case for ASCII letters (a-z and A-Z) only. |
| `overlapping` | `bool` | Whether matches may overlap. |
| `leftmost` | `bool` | Guarantees in case there are overlapping matches that the leftmost match is used. In case there are multiple candidates for the leftmost match the pattern which comes first in patterns is used. May not be used together with overlapping = True. |

---

### Find

Return the bytes offset of the first substring matching a pattern. If the pattern is not found, returns None.

```csharp
public Expr Find(StringOrExpr pattern, bool literal = false, bool strict = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `pattern` | `StringOrExpr` | A valid regular expression pattern, compatible with the regex crate. |
| `literal` | `bool` | Treat pattern as a literal string, not as a regular expression. |
| `strict` | `bool` | Raise an error if the underlying pattern is not a valid regex, otherwise mask out with a null value. |

---

### FindMany

Use the Aho-Corasick algorithm to find many matches.This method supports matching on string literals only, and does not support regular expression matching.

```csharp
public Expr FindMany(
    StringOrExpr patterns,
    bool asciiCaseInsensitive = false,
    bool overlapping = false,
    bool leftmost = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `patterns` | `StringOrExpr` | String patterns to search. |
| `asciiCaseInsensitive` | `bool` | Enable ASCII-aware case-insensitive matching. When this option is enabled, searching will be performed without respect to case for ASCII letters (a-z and A-Z) only. |
| `overlapping` | `bool` | Whether matches may overlap. |
| `leftmost` | `bool` | Guarantees in case there are overlapping matches that the leftmost match is used. In case there are multiple candidates for the leftmost match the pattern which comes first in patterns is used. May not be used together with overlapping = True. |

**Returns:** The function will return the bytes offset of the start of each match. The return type will be List{UInt32}

---

### Head

Return the first n characters of each string in a String Series.

```csharp
public Expr Head(IntOrExpr n)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `IntOrExpr` | Length of the slice (integer or expression) <br>When the n input is negative, head returns characters up to the n`th from the end of the string. For example, if n = -3, then all characters except the last three are returned. |

**Returns:** Expression of data type String.

---

### Join

Vertically concatenate the string values in the column to a single string value.

```csharp
public Expr Join(string delimiter = "", bool ignoreNulls = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `delimiter` | `string` | The delimiter to insert between consecutive string values. |
| `ignoreNulls` | `bool` | Ignore null values (default). If set to False, null values will be propagated. This means that if the column contains any null values, the output is null. |

**Returns:** Expression of data type String.

---

### JsonDecode

Parse string values as JSON. Throws an error if invalid JSON strings are encountered.

```csharp
public Expr JsonDecode(IntoDataTypeExpr dtype)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dtype` | `IntoDataTypeExpr` | The datatype to cast the extracted value to. |

---

### JsonPathMatch

Extract the first match from a JSON string using the provided JSONPath. Throws errors if invalid JSON strings are encountered. All return values are cast to String, regardless of the original value.

```csharp
public Expr JsonPathMatch(StringOrExpr jsonPath)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `jsonPath` | `StringOrExpr` | A valid JSONPath query string. |

**Returns:** Expression of data type String. Contains null values if original value is null or the json_path returns nothing.

---

### LenBytes

Get length in bytes. 

Note: Multi-byte characters (like emojis or CJK) count as > 1 byte.

```csharp
public Expr LenBytes()
```

---

### LenChars

Return the length of each string as the number of characters.

```csharp
public Expr LenChars()
```

**Returns:** Expression of data type UInt32.

---

### Normalize

Returns the Unicode normal form of the string values.

```csharp
public Expr Normalize(NormalizationForm form)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `form` | `NormalizationForm` | Unicode form to use. |

---

### PadEnd

Pad the end of the string until it reaches the given length.

```csharp
public Expr PadEnd(IntOrExpr length, string fillChar = " ")
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `length` | `IntOrExpr` | Pad the string until it reaches this length. Strings with length equal to or greater than this value are returned as-is. |
| `fillChar` | `string` | The character to pad the string with. |

---

### PadStart

Pad the start of the string until it reaches the given length.

```csharp
public Expr PadStart(IntOrExpr length, string fillChar = " ")
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `length` | `IntOrExpr` | Pad the string until it reaches this length. Strings with length equal to or greater than this value are returned as-is. |
| `fillChar` | `string` | The character to pad the string with. |

---

### Replace

Replace first matching regex/literal substring with a new string value.

```csharp
public Expr Replace(
    StringOrExpr pattern,
    StringOrExpr value,
    bool literal = false,
    int n = 1
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `pattern` | `StringOrExpr` | A valid regular expression pattern, compatible with the regex crate. |
| `value` | `StringOrExpr` | String that will replace the matched substring. |
| `literal` | `bool` | Treat pattern as a literal string, not a regex. |
| `n` | `int` | Number of matches to replace. |

---

### ReplaceAll

Replace all occurrences of a pattern with a value.

```csharp
public Expr ReplaceAll(StringOrExpr pattern, StringOrExpr value, bool literal = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `pattern` | `StringOrExpr` | The pattern to search for. |
| `value` | `StringOrExpr` | The value to replace with. |
| `literal` | `bool` | Whether to interpret the pattern as literal value(not Regex). |

---

### ReplaceMany

Use the Aho-Corasick algorithm to replace many matches.

Use the Aho-Corasick algorithm to replace many matches using a dictionary mapping.

```csharp
public Expr ReplaceMany(
    IntoExprColumn patterns,
    IntoExprColumn replaceWith,
    bool asciiCaseInsensitive = false,
    bool leftmost = false
)

public Expr ReplaceMany(IReadOnlyDictionary<string, string> mapping, bool asciiCaseInsensitive = false, bool leftmost = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `patterns` | `IntoExprColumn` | Expression yielding string patterns to search and replace. |
| `replaceWith` | `IntoExprColumn` | Strings to replace where a pattern was a match. Length must match the length of patterns or have length 1. This can be broadcasted, so it supports many:one and many:many. |
| `asciiCaseInsensitive` | `bool` | Enable ASCII-aware case-insensitive matching. |
| `leftmost` | `bool` | Guarantees in case there are overlapping matches that the leftmost match is used. |
| `mapping` | `IReadOnlyDictionary<string, string>` | A mapping of patterns to their replacement. |

---

### Reverse

Returns string values in reversed order.

```csharp
public Expr Reverse()
```

---

### Slice

Slice the string by offset and length.

```csharp
public Expr Slice(IntOrExpr offset, IntOrExpr? length = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `offset` | `IntOrExpr` | Start index. |
| `length` | `IntOrExpr?` | Length of the slice. |

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    text = new[] { "Polars", "  Data  ", "Rust", null, "123-abc" }
});

// 1. ToUpper
// 2. Len (bytes)
// 3. Slice (offset=1, len=3)
df.Select(
    Pl.Col("text"),
    Pl.Col("text").Str.ToUpper().Alias("upper"),
    Pl.Col("text").Str.Len().Alias("len_bytes"), 
    Pl.Col("text").Str.Slice(1, 3).Alias("sliced") 
).Show();
/* Output:
shape: (5, 4)
┌──────────┬──────────┬───────────┬────────┐
│ text     ┆ upper    ┆ len_bytes ┆ sliced │
│ ---      ┆ ---      ┆ ---       ┆ ---    │
│ str      ┆ str      ┆ u32       ┆ str    │
╞══════════╪══════════╪═══════════╪════════╡
│ Polars   ┆ POLARS   ┆ 6         ┆ ola    │
│   Data   ┆   DATA   ┆ 8         ┆  Da    │
│ Rust     ┆ RUST     ┆ 4         ┆ ust    │
│ null     ┆ null     ┆ null      ┆ null   │
│ 123-abc  ┆ 123-ABC  ┆ 7         ┆ 23-    │
└──────────┴──────────┴───────────┴────────┘
*/
```

---

### Split

Split the string by a substring.

```csharp
public Expr Split(
    StringOrExpr by,
    bool inclusive = false,
    bool literal = true,
    bool strict = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `StringOrExpr` | Substring to split by. |
| `inclusive` | `bool` | If True, include the split character/string in the results. |
| `literal` | `bool` | Treat by as a literal string, not as a regular expression. |
| `strict` | `bool` | Raise an error if the underlying pattern is not a valid regex, otherwise mask out with a null value. |

**Returns:** Expression/Series of data type String.

---

### SplitExact

Split the string by a substring using n splits. Results in a struct of n+1 fields. If it cannot make n splits, the remaining field elements will be null.

```csharp
public Expr SplitExact(StringOrExpr by, int n, bool inclusive = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `StringOrExpr` | Substring to split by. |
| `n` | `int` | Number of splits to make. |
| `inclusive` | `bool` | If True, include the split character/string in the results. |

**Returns:** Expression/Series of data type Struct with fields of data type String.

---

### SplitN

Split the string by a substring, restricted to returning at most n items. 

If the number of possible splits is less than n-1, the remaining field elements will be null. If the number of possible splits is n-1 or greater, the last (nth) substring will contain the remainder of the string.

```csharp
public Expr SplitN(StringOrExpr by, int n)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `StringOrExpr` | Substring to split by. |
| `n` | `int` | Max number of items to return. |

**Returns:** Expression/Series of data type Struct with fields of data type String.

---

### StartsWith

Check if the string starts with the given prefix.

```csharp
public Expr StartsWith(StringOrExpr prefix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `prefix` | `StringOrExpr` | - |

---

### StripChars

Remove leading and trailing characters. If matches is null, whitespace is removed.

```csharp
public Expr StripChars(StringOrExpr? characters = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `characters` | `StringOrExpr?` | The set of characters to be removed. All combinations of this set of characters will be stripped from the start and end of the string. If set to None (default), all leading and trailing whitespace is removed instead. |

**Example**

```csharp
df.Select(
    // "  Data  " -> "Data"
    Pl.Col("text").Str.StripChars().Alias("trimmed"),
    // Remove "ID-" prefix
    Pl.Col("code").Str.StripPrefix("ID-").Alias("no_prefix")
);
```

---

### StripCharsEnd

Remove trailing characters. If matches is null, whitespace is removed.

```csharp
public Expr StripCharsEnd(StringOrExpr? characters = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `characters` | `StringOrExpr?` | - |

---

### StripCharsStart

Remove leading characters. If matches is null, whitespace is removed.

```csharp
public Expr StripCharsStart(StringOrExpr? characters = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `characters` | `StringOrExpr?` | - |

---

### StripPrefix

Remove a specific prefix string.

```csharp
public Expr StripPrefix(StringOrExpr prefix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `prefix` | `StringOrExpr` | - |

---

### StripSuffix

Remove a specific suffix string.

```csharp
public Expr StripSuffix(StringOrExpr suffix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `suffix` | `StringOrExpr` | - |

---

### Strptime

Convert a String column into a Date/Datetime/Time column.

```csharp
public Expr Strptime(
    IntoDataTypeExpr dtype,
    string? format = null,
    bool strict = true,
    bool exact = true,
    bool cache = true,
    AmbiguousArg? ambiguous = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `dtype` | `IntoDataTypeExpr` | The data type to convert into. Can be either Date, Datetime, or Time. |
| `format` | `string?` | The parsing format (e.g., "yyyy-MM-dd"). Null for auto-inference. |
| `strict` | `bool` | Raise an error if any conversion fails. |
| `exact` | `bool` | Require an exact format match. If False, allow the format to match anywhere in the target string. Conversion to the Time type is always exact. |
| `cache` | `bool` | Use a cache of unique, converted dates to apply the datetime conversion. |
| `ambiguous` | `AmbiguousArg?` | Determine how to deal with ambiguous datetimes: 'raise' (default): raise 'earliest': use the earliest datetime 'latest': use the latest datetime 'null': set to null |

---

### Tail

Return the last n characters of each string in a String Series.

```csharp
public Expr Tail(IntOrExpr n)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `IntOrExpr` | Length of the slice (integer or expression) <br>When the n input is negative, head returns characters up to the n`th from the start of the string. For example, if n = -3, then all characters except the first three are returned. |

**Returns:** Expression of data type String.

---

### ToDate

Convert string to Date using the specified format. 

Strict parsing: Format mismatches result in null. Format string is similar to `strftime` (e.g. "%Y-%m-%d").

```csharp
public Expr ToDate(
    string? format = null,
    bool strict = true,
    bool exact = true,
    bool cache = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `format` | `string?` | The parsing format (e.g., "%Y-%m-%d","yyyy/MM/dd"). Null for auto-inference. |
| `strict` | `bool` | If true, raises an error on parsing failure. If false, returns nulls. |
| `exact` | `bool` | If true, requires an exact match. If false, allows matching substrings. |
| `cache` | `bool` | Use a cache of unique converted dates to speed up parsing. |

**Example**

```csharp
var dateDf = DataFrame.FromColumns(new { 
    raw = new[] { "2024-01-01", "2024/02/01", "invalid" } 
});

dateDf.Select(
    Pl.Col("raw"),
    Pl.Col("raw").Str.ToDate("%Y-%m-%d").Alias("fmt_dash"), 
    Pl.Col("raw").Str.ToDate("%Y/%m/%d").Alias("fmt_slash")
).Show();
/* Output:
shape: (3, 3)
┌────────────┬────────────┬────────────┐
│ raw        ┆ fmt_dash   ┆ fmt_slash  │
│ ---        ┆ ---        ┆ ---        │
│ str        ┆ date       ┆ date       │
╞════════════╪════════════╪════════════╡
│ 2024-01-01 ┆ 2024-01-01 ┆ null       │
│ 2024/02/01 ┆ null       ┆ 2024-02-01 │
│ invalid    ┆ null       ┆ null       │
└────────────┴────────────┴────────────┘
*/
```

---

### ToDatetime

Convert string to Datetime. If format is null, Polars will attempt to infer it.

```csharp
public Expr ToDatetime(
    string? format = null,
    TimeUnit? timeUnit = null,
    string? timeZone = null,
    bool strict = true,
    bool exact = true,
    bool cache = true,
    AmbiguousArg? ambiguous = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `format` | `string?` | The parsing format (e.g., "%Y-%m-%d", "yyyy-MM-dd"). Null for auto-inference. |
| `timeUnit` | `TimeUnit?` | Target time unit. Null to use default (usually Microseconds). |
| `timeZone` | `string?` | Target time zone (e.g., "UTC", "Asia/Shanghai"). |
| `strict` | `bool` | If true, raises an error on parsing failure. If false, returns nulls. |
| `exact` | `bool` | If true, requires an exact match. If false, allows matching substrings. |
| `cache` | `bool` | Use a cache of unique converted dates to speed up parsing. |
| `ambiguous` | `AmbiguousArg?` | Determine how to deal with ambiguous datetimes: 'raise' (default): raise 'earliest': use the earliest datetime 'latest': use the latest datetime 'null': set to null |

---

### ToDecimal

Convert a String column into a Decimal column.

```csharp
public Expr ToDecimal(int scale)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `scale` | `int` | Number of digits after the comma to use for the decimals. |

---

### ToInteger

Convert a String column into an Integer column with the specified radix (base).

```csharp
public Expr ToInteger(IntOrExpr? radix = null, DataType? dtype = null, bool strict = true)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `radix` | `IntOrExpr?` | Positive integer or expression which is the base of the string we are parsing. Default is 10. |
| `dtype` | `DataType?` | Integer data type to cast the result to. Default is Int64. |
| `strict` | `bool` | If true, raises ComputeError on failure. If false, silently converts to Null. |

**Returns:** Expression of destinated integer data type.

---

### ToLowercase

Convert string to lowercase.

```csharp
public Expr ToLowercase()
```

---

### ToTime

Convert a String column into a Time column.

```csharp
public Expr ToTime(
    string? format = null,
    bool strict = true,
    bool exact = true,
    bool cache = true
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `format` | `string?` | The parsing format (e.g., "%H:%M:%S", "HH-mm-ss"). Null for auto-inference. |
| `strict` | `bool` | Raise an error if any conversion fails. |
| `exact` | `bool` | If true, requires an exact match. If false, allows matching substrings. |
| `cache` | `bool` | Use a cache of unique, converted times to apply the conversion. |

---

### ToTitlecase

Modify strings to their titlecase equivalent. 

This is a form of case transform where the first letter of each word is capitalized, with the rest of the word in lowercase. Non-alphanumeric characters define the word boundaries.

```csharp
public Expr ToTitlecase()
```

---

### ToUppercase

Convert string to uppercase.

```csharp
public Expr ToUppercase()
```

---

### Zfill

Pad the start of the string with zeros until it reaches the given length. 

A sign prefix (-) is handled by inserting the padding after the sign character rather than before.

This method is intended for padding numeric strings. If your data contains non-ASCII characters, use pad_start() instead.

```csharp
public Expr Zfill(IntOrExpr length)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `length` | `IntOrExpr` | Pad the string until it reaches this length. Strings with length equal to or greater than this value are returned as-is. |

---

