---
uid: Polars.CSharp.Series.Dt
title: "Series.Dt (SeriesDtOps)"
---

# Series.Dt (SeriesDtOps)

Namespace: `Polars.CSharp`

Date Ops Namespace

## Instance Methods

### AddBusinessDays

Add business days to the date column. 

Automatically skips weekends (by default) and specified holidays.

```csharp
public Series AddBusinessDays(
    Expr n,
    bool[]? weekMask = null,
    IntoDateExpr? holidays = null,
    Roll roll = Roll.Raise
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `n` | `Expr` | Number of business days to add (can be negative). |
| `weekMask` | `bool[]?` | Array of 7 bools indicating business days, starting from Monday. Default is Mon-Fri [true, true, true, true, true, false, false]. |
| `holidays` | `IntoDateExpr?` | List of holidays to skip. |
| `roll` | `Roll` | Strategy for handling non-business days. |

**Example**

```csharp
// Add 1 business day (skipping weekends)
// Fri 2024-03-01 -> Mon 2024-03-04
df.Select(
    Pl.Col("dt").Dt.AddBusinessDays(1)
);
```

---

### BaseUtcOffset

Base offset from UTC. This is usually constant for all datetimes in a given time zone, but may vary in the rare case that a country switches time zone, like Samoa (Apia) did at the end of 2011.

```csharp
public Series BaseUtcOffset()
```

**Returns:** Expression/Series of data type Duration.

---

### Century

Extract the century from underlying representation.Returns the century number in the calendar date.

```csharp
public Series Century()
```

**Returns:** Expression/Series of data type Int32.

---

### Combine

Combine the date from the underlying date/datetime with the time from another expression. 

The resulting Series will have the specified TimeUnit.

```csharp
public Series Combine(Expr time, TimeUnit timeUnit)
public Series Combine(Series time, TimeUnit timeUnit)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `time` | `Expr / Series` | An expression yielding the Time component. |
| `timeUnit` | `TimeUnit` | The desired TimeUnit for the resulting Datetime. <br>Note: Only sub-second units (`TimeUnit.Nanoseconds`, `TimeUnit.Microseconds`, `TimeUnit.Milliseconds`) are supported. |

---

### ConvertTimeZone

Convert from one timezone to another. 

This changes the physical time (wall clock time) to match the target timezone. The input Series must already have a timezone assigned (e.g. via `DtOps.ReplaceTimeZone`).

```csharp
public Series ConvertTimeZone(string timeZone)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `timeZone` | `string` | Target time zone string (IANA database, e.g. "Asia/Shanghai", "America/New_York"). |

**Returns:** A new expression/Series with the converted time.

**Example**

```csharp
// 1. Start with a naive datetime (noon)
var df = DataFrame.FromColumns(new
{
    dt = new[] { new DateTime(2024, 1, 1, 12, 0, 0) } 
});

// 2. Operations:
// - ReplaceTimeZone("UTC"): Define metadata (it is now UTC noon)
// - ConvertTimeZone("Asia/Shanghai"): Shift to +8 hours (20:00)
// - ConvertTimeZone("America/New_York"): Shift to -5 hours (07:00)
df.Select(
    Pl.Col("dt").Alias("naive"),

    Pl.Col("dt").Dt.ReplaceTimeZone("UTC").Alias("utc_tagged"),

    Pl.Col("dt").Dt.ReplaceTimeZone("UTC")
              .Dt.ConvertTimeZone("Asia/Shanghai")
              .Alias("shanghai_time"),

    Pl.Col("dt").Dt.ReplaceTimeZone("UTC")
              .Dt.ConvertTimeZone("America/New_York")
              .Alias("ny_time")
).Show();
/* Output:
shape: (1, 4)
┌─────────────────────┬─────────────────────────┬─────────────────────────────┬────────────────────────────────┐
│ naive               ┆ utc_tagged              ┆ shanghai_time               ┆ ny_time                        │
│ ---                 ┆ ---                     ┆ ---                         ┆ ---                            │
│ datetime[μs]        ┆ datetime[μs, UTC]       ┆ datetime[μs, Asia/Shanghai] ┆ datetime[μs, America/New_York] │
╞═════════════════════╪═════════════════════════╪═════════════════════════════╪════════════════════════════════╡
│ 2024-01-01 12:00:00 ┆ 2024-01-01 12:00:00 UTC ┆ 2024-01-01 20:00:00 CST     ┆ 2024-01-01 07:00:00 EST        │
└─────────────────────┴─────────────────────────┴─────────────────────────────┴────────────────────────────────┘
*/
```

---

### Date

Extract date from date(time).

```csharp
public Series Date()
```

**Returns:** Expression/Series of data type Date.

---

### Day

Extract day from underlying Date representation.Returns the day of month starting from 1. The return value ranges from 1 to 31. (The last day of month differs by months.)

```csharp
public Series Day()
```

**Returns:** Expression/Series of data type Int8.

---

### DaysInMonth

Extract the number of days in the month from the underlying Date representation.Returns the number of days in the month. The return value ranges from 28 to 31.

```csharp
public Series DaysInMonth()
```

**Returns:** Expression/Series of data type Int8.

---

### DstOffset

Additional offset currently in effect (typically due to daylight saving time).

```csharp
public Series DstOffset()
```

**Returns:** Expression/Series of data type Duration.

---

### Epoch

Get the time passed since the Unix epoch (1970-01-01 00:00:00).

```csharp
public Series Epoch(EpochTimeUnit timeUnit = EpochTimeUnit.Microseconds)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `timeUnit` | `EpochTimeUnit` | The time unit to compute the epoch for. Supported units: Nanoseconds, Microseconds, Milliseconds, Second, Day. (default: Microseconds) |

**Returns:** A new expression/Series representing the epoch time.

---

### Hour

Extract hour from underlying DateTime representation.Returns the hour number from 0 to 23.

```csharp
public Series Hour()
```

**Returns:** Expression/Series of data type Int8.

---

### IsBusinessDay

Determine whether each day lands on a business day.

```csharp
public Series IsBusinessDay(bool[]? weekMask = null, IntoDateExpr? holidays = null)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `weekMask` | `bool[]?` | Which days of the week to count. The default is Monday to Friday. If you wanted to count only Monday to Thursday, you would pass (True, True, True, True, False, False, False). |
| `holidays` | `IntoDateExpr?` | Holidays to exclude from the count. |

**Returns:** Expression/Series of data type Boolean.

---

### IsLeapYear

Determine whether the year of the underlying date is a leap year.

```csharp
public Series IsLeapYear()
```

**Returns:** Expression/Series of data type Boolean.

---

### IsoYear

Extract ISO year from underlying Date representation. Applies to Date and Datetime columns. Returns the year number in the ISO standard. This may not correspond with the calendar year.

```csharp
public Series IsoYear()
```

**Returns:** Expression/Series of data type Int32.

---

### Microsecond

Extract microseconds from underlying DateTime representation.

```csharp
public Series Microsecond()
```

**Returns:** Expression/Series of data type Int32.

---

### Millennium

Extract the millennium from underlying representation. Applies to Date and Datetime columns. Returns the millennium number in the calendar date.

```csharp
public Series Millennium()
```

**Returns:** Expression/Series of data type Int32.

---

### Millisecond

Extract milliseconds from underlying DateTime representation.

```csharp
public Series Millisecond()
```

**Returns:** Expression/Series of data type Int32.

---

### Minute

Extract minutes from underlying DateTime representation.Returns the minute number from 0 to 59.

```csharp
public Series Minute()
```

**Returns:** Expression/Series of data type Int8.

---

### Month

Extract month from underlying Date representation.Returns the month number starting from 1. The return value ranges from 1 to 12.

```csharp
public Series Month()
```

**Returns:** Expression/Series of data type Int8.

---

### MonthEnd

Roll forward to the last day of the month. For datetimes, the time-of-day is preserved.

```csharp
public Series MonthEnd()
```

**Returns:** Expression/Series of data type Date or Datetime.

---

### MonthStart

Roll backward to the first day of the month. For datetimes, the time-of-day is preserved.

```csharp
public Series MonthStart()
```

**Returns:** Expression/Series of data type Date or Datetime.

---

### Nanosecond

Extract nanoseconds from underlying DateTime representation.

```csharp
public Series Nanosecond()
```

**Returns:** Expression/Series of data type Int32.

---

### OffsetBy

Offset the datetimes by a constant duration string (e.g., "1d", "-2h").

```csharp
public Series OffsetBy(DurationOrExpr by)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `by` | `DurationOrExpr` | - |

**Example**

```csharp
df.Select(
    // Add 2 days
    Pl.Col("dt").Dt.OffsetBy("2d"),
    // Subtract 1 hour
    Pl.Col("dt").Dt.OffsetBy("-1h")
);
```

---

### OrdinalDay

Extract ordinal day from underlying Date representation.Returns the day of year starting from 1. The return value ranges from 1 to 366. (The last day of year differs by years.)

```csharp
public Series OrdinalDay()
```

**Returns:** Expression/Series of data type Int16.

---

### Quarter

Extract quarter from underlying Date representation. Returns the quarter ranging from 1 to 4.

```csharp
public Series Quarter()
```

**Returns:** Expression/Series of data type Int8.

---

### Replace

Replace the datetime components of the underlying Datetime/Date.

```csharp
public Series Replace(
    IntoExprColumn? year = null,
    IntoExprColumn? month = null,
    IntoExprColumn? day = null,
    IntoExprColumn? hour = null,
    IntoExprColumn? minute = null,
    IntoExprColumn? second = null,
    IntoExprColumn? microsecond = null,
    AmbiguousArg? ambiguous = null
)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `year` | `IntoExprColumn?` | Year to replace |
| `month` | `IntoExprColumn?` | Month to replace |
| `day` | `IntoExprColumn?` | Day to replace |
| `hour` | `IntoExprColumn?` | Hour to replace |
| `minute` | `IntoExprColumn?` | Minute to replace |
| `second` | `IntoExprColumn?` | Second to replace |
| `microsecond` | `IntoExprColumn?` | Microsecond to replace |
| `ambiguous` | `AmbiguousArg?` | Determine how to deal with ambiguous datetimes. |

**Returns:** A new expression.

---

### ReplaceTimeZone

Replace the time zone of a column. 

This sets the time zone metadata without changing the underlying physical time (wall clock). Use this to assign a timezone to a naive datetime.

```csharp
public Series ReplaceTimeZone(string? timeZone, AmbiguousArg? ambiguous = null, NonExistent nonExistent = NonExistent.Raise)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `timeZone` | `string?` | The time zone to assign (e.g. "UTC", "Asia/Shanghai"). If null, removes timezone info. |
| `ambiguous` | `AmbiguousArg?` | How to handle ambiguous times (e.g. DST transitions). Default "raise". |
| `nonExistent` | `NonExistent` | How to handle non-existent times. Default "raise". |

---

### Round

Round the datetimes to the given interval.

```csharp
public Series Round(DurationOrExpr every)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `every` | `DurationOrExpr` | - |

---

### Second

Extract seconds from underlying DateTime representation.Returns the integer second number from 0 to 59, or a floating point number from 0 ~ 60 if fractional=True that includes any milli/micro/nanosecond component.

```csharp
public Series Second(bool fractional = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fractional` | `bool` | Whether to include the fractional component of the second. |

**Returns:** Expression/Series of data type Int8 or Float64.

---

### Strftime

Alias for `DtOps.ToString`.

```csharp
public Series Strftime(string format = "iso")
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `format` | `string` | - |

---

### Time

Extract time.Applies to Datetime columns only; fails on Date.

```csharp
public Series Time()
```

**Returns:** Expression/Series of data type Time.

---

### Timestamp

Convert the datetime to an integer timestamp (Unix epoch).

```csharp
public Series Timestamp(TimeUnit timeUnit = TimeUnit.Microseconds)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `timeUnit` | `TimeUnit` | The desired TimeUnit for the resulting Datetime. <br>Note: Only sub-second units (`TimeUnit.Nanoseconds`, `TimeUnit.Microseconds`, `TimeUnit.Milliseconds`) are supported. |

---

### TotalDays

Extract the total days from a Duration type.

```csharp
public Series TotalDays(bool fractional = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fractional` | `bool` | Whether to include the fractional component of the day. |

**Returns:** Expression/Series of data type Int64 or Float64 if fractional is set.

---

### TotalHours

Extract the total hours from a Duration type.

```csharp
public Series TotalHours(bool fractional = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fractional` | `bool` | Whether to include the fractional component of the hour. |

**Returns:** Expression/Series of data type Int64 or Float64 if fractional is set.

---

### TotalMicroseconds

Extract the total microseconds from a Duration type.

```csharp
public Series TotalMicroseconds(bool fractional = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fractional` | `bool` | Whether to include the fractional component of the microsecond. |

**Returns:** Expression/Series of data type Int64 or Float64 if fractional is set.

---

### TotalMilliseconds

Extract the total milliseconds from a Duration type.

```csharp
public Series TotalMilliseconds(bool fractional = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fractional` | `bool` | Whether to include the fractional component of the millisecond. |

**Returns:** Expression/Series of data type Int64 or Float64 if fractional is set.

---

### TotalMinutes

Extract the total minutes from a Duration type.

```csharp
public Series TotalMinutes(bool fractional = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fractional` | `bool` | Whether to include the fractional component of the minute. |

**Returns:** Expression/Series of data type Int64 or Float64 if fractional is set.

---

### TotalNanoseconds

Extract the total nanoseconds from a Duration type.

```csharp
public Series TotalNanoseconds(bool fractional = false)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `fractional` | `bool` | Whether to include return the result as a Float64. Because the smallest TimeUnit is 'ns', the fractional component will always be zero. |

**Returns:** Expression/Series of data type Int64 or Float64 if fractional is set.

---

### Truncate

Truncate the datetimes to the given interval (e.g. "1d", "1h", "15m"). 

This behaves like a "floor" operation for time.

```csharp
public Series Truncate(DurationOrExpr every)
```

**Parameters**

| Name | Type | Description |
| :--- | :--- | :--- |
| `every` | `DurationOrExpr` | - |

**Example**

```csharp
// Input: 2024-02-29 23:59:59
df.Select(
    Pl.Col("dt").Dt.Truncate("1h"), // Result: 23:00:00
    Pl.Col("dt").Dt.Round("1h")     // Result: 2024-03-01 00:00:00
);
```

---

### Weekday

Extract the week day from the underlying Date representation.Returns the ISO weekday number where monday = 1 and sunday = 7

```csharp
public Series Weekday()
```

**Returns:** Expression/Series of data type Int8.

---

### Year

Get the year from the underlying date/datetime.Returns the year number in the calendar date.

```csharp
public Series Year()
```

**Returns:** Expression/Series of data type Int32.

**Example**

```csharp
var df = DataFrame.FromColumns(new
{
    dt = new[] { new DateTime(2024, 1, 1, 10, 15, 30) } 
});

df.Select(
    Pl.Col("dt").Dt.Year(),    // 2024
    Pl.Col("dt").Dt.Month(),   // 1
    Pl.Col("dt").Dt.Weekday(), // 1 (Monday)
    Pl.Col("dt").Dt.ToString("%Y-%m-%d") // "2024-01-01"
).Show();
```

---

