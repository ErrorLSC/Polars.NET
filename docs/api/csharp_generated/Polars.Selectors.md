---
uid: Polars.CSharp.Polars.Selectors
title: "Polars.Selectors"
---

# Polars.Selectors

Namespace: `Polars.CSharp`

String matching selectors namespace. Usage: Cs.StartsWith("A") or Pl.Cs.StartWith("A")

## Constructors & Static Methods

### All

Select all columns.

```csharp
public static Selector All()
```

---

### Alpha

Select all columns with alphabetic names.

```csharp
public static Selector Alpha(bool asciiOnly = false, bool ignoreSpaces = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `asciiOnly` | `bool` | - |
| `ignoreSpaces` | `bool` | - |

---

### Alphanumeric

Select all columns with alphanumeric names.

```csharp
public static Selector Alphanumeric(bool asciiOnly = false, bool ignoreSpaces = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `asciiOnly` | `bool` | - |
| `ignoreSpaces` | `bool` | - |

---

### Array

Select array columns. Optionally filter by inner data type and fixed width.

```csharp
public static Selector Array(Selector? inner = null, long? width = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `inner` | `Selector?` | - |
| `width` | `long?` | - |

---

### Binary

```csharp
public static Selector Binary()
```

---

### Boolean

```csharp
public static Selector Boolean()
```

---

### ByDtype

Select columns by specific DataType.

Select columns by Generic Type.

```csharp
public static Selector ByDtype(DataType type)
public static Selector ByDtype(Type type)
public static Selector ByDtype<T>()
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `type` | `DataType` | - |

---

### ByIndex

Select columns by their index. Usage: Cs.ByIndex(0, 2, 4)

Select columns by their index with strictness control.

```csharp
public static Selector ByIndex(ReadOnlySpan<long> indices)
public static Selector ByIndex(ReadOnlySpan<long> indices, bool strict)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `indices` | `ReadOnlySpan<long>` | - |
| `strict` | `bool` | - |

---

### ByName

Select columns.

```csharp
public static Selector ByName(params string[] columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `columns` | `string[]` | - |

---

### CJK

[EN] Select columns whose names consist entirely of CJK scripts (Han, Hiragana, Katakana, Hangul). The 'chinese' option enables \p{Han}, which also includes Japanese Kanji and Korean Hanja.

[ZH] 选择列名完全由中日韩字符（Han / 平假名 / 片假名 / 韩文）组成的列。 注意：'chinese' 实际匹配 \p{Han}，包含日文汉字与韩文汉字。

[JA] 列名がCJK文字（漢字・ひらがな・カタカナ・ハングル）のみで構成される列を選択します。 ※ 'chinese' は \p{Han}（日本・韓国の漢字を含む）を有効にします。

[KO] 열 이름이 CJK 문자(한자, 히라가나, 가타카나, 한글)로만 구성된 열을 선택합니다. ※ 'chinese'는 \p{Han}을 의미하며 일본/한국 한자도 포함합니다.

```csharp
public static Selector CJK(
    bool chinese = true,
    bool japanese = true,
    bool korean = true,
    bool ignoreSpaces = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `chinese` | `bool` | - |
| `japanese` | `bool` | - |
| `korean` | `bool` | - |
| `ignoreSpaces` | `bool` | - |

---

### CJKAlphanumeric

[EN] Select columns whose names consist of CJK scripts, Unicode digits (\p{N}), and optionally ASCII/full-width Latin letters.

[ZH] 选择列名由中日韩字符、数字（\p{N}，含全/半角）以及可选英文字母（全/半角）组成的列。

[JA] 列名がCJK文字・数字（\p{N}、全角/半角）および英字（全角/半角）で構成される列を選択します。

[KO] 열 이름이 CJK 문자, 숫자(\p{N}, 전각/반각) 및 영문자(전각/반각)로 구성된 열을 선택합니다.

```csharp
public static Selector CJKAlphanumeric(
    bool chinese = true,
    bool japanese = true,
    bool korean = true,
    bool includeLetters = true,
    bool ignoreSpaces = false
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `chinese` | `bool` | - |
| `japanese` | `bool` | - |
| `korean` | `bool` | - |
| `includeLetters` | `bool` | - |
| `ignoreSpaces` | `bool` | - |

---

### Contains

Select column whose name contains given string.

```csharp
public static Selector Contains(string str)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `str` | `string` | - |

---

### Date

Select all date columns.

```csharp
public static Selector Date()
```

---

### Datetime

Select all datetime columns (both with and without timezones).

```csharp
public static Selector Datetime(TimeUnit? timeUnit = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `timeUnit` | `TimeUnit?` | - |

---

### DatetimeAware

Select ONLY timezone-aware datetime columns (any timezone).

```csharp
public static Selector DatetimeAware(TimeUnit? timeUnit = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `timeUnit` | `TimeUnit?` | - |

---

### DatetimeExact

Select datetime columns matching a specific timezone (e.g., "UTC", "Asia/Shanghai").

```csharp
public static Selector DatetimeExact(string timeZone, TimeUnit? timeUnit = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `timeZone` | `string` | - |
| `timeUnit` | `TimeUnit?` | - |

---

### DatetimeNaive

Select ONLY timezone-naive datetime columns (no timezone set).

```csharp
public static Selector DatetimeNaive(TimeUnit? timeUnit = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `timeUnit` | `TimeUnit?` | - |

---

### Decimal

```csharp
public static Selector Decimal()
```

---

### Duration

Select all duration columns. Optionally match a specific TimeUnit.

```csharp
public static Selector Duration(TimeUnit? timeUnit = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `timeUnit` | `TimeUnit?` | - |

---

### Empty

```csharp
public static Selector Empty()
```

---

### EndsWith

Select column whose name ends with given suffix.

```csharp
public static Selector EndsWith(string suffix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `suffix` | `string` | - |

---

### Enum

```csharp
public static Selector Enum()
```

---

### Exclude

Select all columns EXCEPT the specified Selectors.

Select all columns EXCEPT the specified Data Types.

Select all columns EXCEPT the specified Column names.

```csharp
public static Selector Exclude(ReadOnlySpan<Selector> selectors)
public static Selector Exclude(ReadOnlySpan<DataType> dtypes)
public static Selector Exclude(params string[] columns)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `selectors` | `ReadOnlySpan<Selector>` | - |
| `dtypes` | `ReadOnlySpan<DataType>` | - |
| `columns` | `string[]` | - |

---

### ExpandSelector

Expand a Selector against a DataFrame to get the matched column names.

Expand an Expr against a DataFrame to get the matched column names.

```csharp
public static string[] ExpandSelector(DataFrame target, Selector selector)
public static string[] ExpandSelector(DataFrame target, Expr expr)
public static string[] ExpandSelector(LazyFrame target, Selector selector)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `target` | `DataFrame / LazyFrame` | - |
| `selector` | `Selector` | - |
| `expr` | `Expr` | - |

---

### First

Select the first column.

```csharp
public static Selector First()
```

---

### Float

```csharp
public static Selector Float()
```

---

### Integer

```csharp
public static Selector Integer()
```

---

### Last

Select the last column.

```csharp
public static Selector Last()
```

---

### List

Select list columns. Optionally filter by the inner data type. Example: Cs.List(Cs.Integer())

```csharp
public static Selector List(Selector? inner = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `inner` | `Selector?` | - |

---

### Matches

Select column whose name matches given string.

```csharp
public static Selector Matches(string regex)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `regex` | `string` | Regular Expression |

---

### Nested

```csharp
public static Selector Nested()
```

---

### Numeric

Select all numeric columns (Int, Float, etc.).

```csharp
public static Selector Numeric()
```

---

### SignedInteger

```csharp
public static Selector SignedInteger()
```

---

### StartsWith

Select column whose name starts with given prefix.

```csharp
public static Selector StartsWith(string prefix)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `prefix` | `string` | - |

---

### String

Select all string/utf8 columns.

```csharp
public static Selector String()
```

---

### Struct

```csharp
public static Selector Struct()
```

---

### Temporal

```csharp
public static Selector Temporal()
```

---

### UnsignedInteger

```csharp
public static Selector UnsignedInteger()
```

---

