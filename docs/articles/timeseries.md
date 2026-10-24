# Time Series & Advanced Analytics

Temporal and time series data are fundamental across quantitative finance, IoT sensor telemetry, system log analysis, and scientific computing. Polars.NET integrates the high-performance temporal query engine from Rust Polars, delivering sub-second timestamp precision, timezone-aware conversions, high-throughput dynamic rolling windows, and vectorized missing-value interpolation.

---

## 1. Core Temporal Types & Ingestion

Polars.NET features a comprehensive, strongly-typed temporal data system:
* `DataType.Date`: Calendar date (days since the Unix epoch 1970-01-01, stored as a 32-bit signed integer).
* `DataType.Datetime(TimeUnit, TimeZone)`: High-precision timestamp (supports `Nanoseconds`, `Microseconds`, and `Milliseconds`, with an optional IANA timezone string like `"UTC"` or `"America/New_York"`).
* `DataType.Time`: Time of day since midnight in nanoseconds (timezone-agnostic).
* `DataType.Duration(TimeUnit)`: Time differences and elapsed intervals.

### Parsing Strings to Dates & Timestamps

When reading CSV or Parquet files, temporal parsing can be activated automatically via `tryParseDates = true`. You can also parse strings explicitly within expression pipelines using `.Str.ToDate()` or `.Str.ToDatetime()`:

::: code-group

```csharp [C# (Fluent)]
using Polars.CSharp;
using static Polars.CSharp.Polars;

var df = DataFrame.FromColumns(new
{
    RawDate = new[] { "2024-01-15", "2024-02-20", "2024-03-25" },
    RawTimestamp = new[] { "2024-01-15 09:30:00", "2024-02-20 14:15:30", "2024-03-25 18:45:00" },
    Metric = new[] { 100.5, 204.2, 189.7 }
});

var parsed = df.Select(
    Col("RawDate").Str.ToDate("%Y-%m-%d").Alias("Date"),
    Col("RawTimestamp").Str.ToDatetime("%Y-%m-%d %H:%M:%S").Alias("Timestamp"),
    Col("Metric")
);
```

```fsharp [F# (Pipeline)]
open System
open Polars.FSharp

let df =
    DataFrame.create [
        pl.series "RawDate" [ "2024-01-15"; "2024-02-20"; "2024-03-25" ]
        pl.series "RawTimestamp" [ "2024-01-15 09:30:00"; "2024-02-20 14:15:30"; "2024-03-25 18:45:00" ]
        pl.series "Metric" [ 100.5; 204.2; 189.7 ]
    ]

let parsed =
    df
    |> pl.select [
        pl.col("RawDate").Str.ToDate("%Y-%m-%d").Alias "Date"
        pl.col("RawTimestamp").Str.ToDatetime("%Y-%m-%d %H:%M:%S").Alias "Timestamp"
        pl.col "Metric"
    ]
```

```csharp [C# (LINQ)]
using Polars.CSharp;
using Polars.CSharp.Linq;

// Seamlessly project typed DateTime fields in LINQ queries
var query = from r in parsed.AsQueryable<SensorRecord>()
            where r.Date >= new DateOnly(2024, 2, 1)
            select new { r.Date, r.Metric };
```

:::

---

## 2. Calendar Component Extraction & Arithmetic (`Expr.Dt`)

Polars.NET exposes temporal operations under the `Expr.Dt` sub-namespace. These operations execute directly over SIMD-vectorized columnar memory buffers without constructing intermediate managed `DateTime` instances:

| Operator Category | Core Methods | Description & Output Type |
| :--- | :--- | :--- |
| **Calendar Units** | `Year()`, `IsoYear()`, `Quarter()`, `Month()` | Year (Int32), Quarter 1-4 (Int8), Month 1-12 (Int8) |
| **Date Units** | `Day()`, `Weekday()`, `OrdinalDay()`, `DaysInMonth()` | Day 1-31 (Int8), Weekday 1-7 (Int8, Mon=1), Day of Year 1-366 (Int16) |
| **Time Units** | `Hour()`, `Minute()`, `Second(fractional: false)` | Hour 0-23, Minute 0-59, Second 0-59 |
| **Sub-second Units** | `Millisecond()`, `Microsecond()`, `Nanosecond()` | High-precision integer fractions |
| **Calendar Alignment** | `MonthStart()`, `MonthEnd()`, `IsLeapYear()` | Rolls to first/last day of month or checks leap year status |

### Temporal Truncation (`Truncate`) & Rounding (`Round`)

Bucketing timestamps into fixed intervals (binning) is essential for metric aggregation:
* `Truncate("1h")` / `Truncate("1d")`: Floors timestamps down to the nearest interval boundary.
* `Round("1h")`: Rounds timestamps to the closest interval boundary.
* `OffsetBy("2d")` / `OffsetBy("-1h")`: Shifts dates by a specified physical duration string.

::: code-group

```csharp [C# (Fluent)]
var features = df.Select(
    Col("Timestamp").Dt.Year().Alias("Year"),
    Col("Timestamp").Dt.Month().Alias("Month"),
    Col("Timestamp").Dt.Weekday().Alias("Weekday"),
    Col("Timestamp").Dt.Truncate("1h").Alias("HourBucket"),
    Col("Timestamp").Dt.Round("1d").Alias("DayRounded")
);
```

```fsharp [F# (Pipeline)]
let features =
    df
    |> pl.select [
        pl.col("Timestamp").Dt.Year().Alias "Year"
        pl.col("Timestamp").Dt.Month().Alias "Month"
        pl.col("Timestamp").Dt.Weekday().Alias "Weekday"
        pl.col("Timestamp").Dt.Truncate(Dur.String "1h").Alias "HourBucket"
        pl.col("Timestamp").Dt.Round(Dur.String "1d").Alias "DayRounded"
    ]
```

:::

---

## 3. Time Zone Awareness & Conversion

Polars follows modern temporal best practices by distinguishing between **Naive Datetime** (physical timestamp without timezone metadata) and **Time Zone Aware Datetime**:

<div class="pl-pipeline">
  <div class="pl-step">
    <div class="pl-step-badge">1. Naive Wall Clock</div>
    <div class="pl-step-name">2024-01-01 12:00:00</div>
    <div class="pl-step-desc">Unambiguous local clock value, no timezone metadata</div>
  </div>
  <div class="pl-arrow">&rarr;</div>
  <div class="pl-step">
    <div class="pl-step-badge">2. Tag via ReplaceTimeZone</div>
    <div class="pl-step-name">12:00:00 UTC</div>
    <div class="pl-step-desc">Assigns timezone metadata without modifying physical bits</div>
  </div>
  <div class="pl-arrow">&rarr;</div>
  <div class="pl-step">
    <div class="pl-step-badge">3. Shift via ConvertTimeZone</div>
    <div class="pl-step-name">20:00:00 Asia/Shanghai</div>
    <div class="pl-step-desc">Converts physical time into target timezone (+8h)</div>
  </div>
</div>

::: code-group

```csharp [C# (Fluent)]
var tzDf = df.Select(
    // 1. Tag naive timestamp as UTC
    Col("Timestamp").Dt.ReplaceTimeZone("UTC").Alias("UTC_Time"),

    // 2. Convert UTC timestamp to Asia/Shanghai (+8 hours)
    Col("Timestamp").Dt.ReplaceTimeZone("UTC")
        .Dt.ConvertTimeZone("Asia/Shanghai").Alias("Shanghai_Time"),

    // 3. Convert UTC timestamp to America/New_York (-5 hours)
    Col("Timestamp").Dt.ReplaceTimeZone("UTC")
        .Dt.ConvertTimeZone("America/New_York").Alias("NY_Time")
);
```

```fsharp [F# (Pipeline)]
let tzDf =
    df
    |> pl.select [
        pl.col("Timestamp").Dt.ReplaceTimeZone("UTC").Alias "UTC_Time"
        pl.col("Timestamp").Dt.ReplaceTimeZone("UTC")
            .Dt.ConvertTimeZone("Asia/Shanghai").Alias "Shanghai_Time"
        pl.col("Timestamp").Dt.ReplaceTimeZone("UTC")
            .Dt.ConvertTimeZone("America/New_York").Alias "NY_Time"
    ]
```

:::

---

## 4. Rolling Window Functions

Moving averages, rolling bounds, and moving volatilities serve as standard building blocks for indicators (such as Bollinger Bands and MACD).

Polars.NET provides dual rolling window modes:
1. **Index/Row-based windows**: `RollingMean("3i")` or `Dur.Index(3)` (fixed observation count).
2. **Time-based windows**: `RollingMean("1h")` or `Dur.TimeSpan(...)` (variable row counts within a time interval).

Supported rolling aggregations include:
* `RollingMean`: Moving average.
* `RollingSum`: Moving summation.
* `RollingMin` / `RollingMax`: Moving bounds.
* `RollingStd` / `RollingVar`: Moving standard deviation and sample variance.
* `RollingMedian` / `RollingQuantile`: Moving median and arbitrary percentiles.
* `RollingSkew` / `RollingKurtosis`: Rolling skewness and kurtosis.

::: code-group

```csharp [C# (Fluent)]
// Compute 2-observation moving average and 1-hour rolling sum
var rollingDf = df.WithColumns(
    Col("Reading").RollingMean("2i", minPeriods: 1).Alias("MA_2"),
    Col("Reading").RollingSum("1h", minPeriods: 1).Alias("Sum_1h"),
    Col("Reading").RollingStd("2i", minPeriods: 2).Alias("Std_2")
);
```

```fsharp [F# (Pipeline)]
let rollingDf =
    df
    |> pl.withColumns [
        (pl.col "Reading").RollingMean(Dur.Index 2, minPeriod = 1).Alias "MA_2"
        (pl.col "Reading").RollingSum(Dur.String "1h", minPeriod = 1).Alias "Sum_1h"
        (pl.col "Reading").RollingStd(Dur.Index 2, minPeriod = 2).Alias "Std_2"
    ]
```

:::

### Live Execution Output: Time Series Features & Rolling Calculations

<!--@include: ../snippets/timeseries_features.html-->

---

## 5. Dynamic Time Resampling (`GroupByDynamic`)

Standard `GroupBy` aggregates across discrete keys. In time series analysis, queries often require: **"Group into 1-hour dynamic windows, calculate average sensor readings, and partition by device ID"**.

Polars.NET provides `GroupByDynamic`:
* `indexColumn`: Monotonically ascending temporal index column.
* `every`: Window stride interval (e.g. `"1h"`).
* `period`: Window duration length (defaults to `every` if omitted; setting it larger creates overlapping sliding windows).
* `offset`: Window boundary offset.
* `groupBy`: Optional additional discrete grouping keys (e.g. Device ID, Region).

::: code-group

```csharp [C# (Fluent)]
var dynamicAgg = df.Lazy()
    .GroupByDynamic(
        indexColumn: "Timestamp",
        every: TimeSpan.FromHours(1),
        period: TimeSpan.FromHours(1),
        groupBy: new[] { Col("DeviceId") }
    )
    .Agg(
        Col("Reading").Mean().Alias("Avg_Reading"),
        Col("Reading").Count().Alias("Sample_Count")
    )
    .Collect();
```

```fsharp [F# (Pipeline)]
let dynamicAgg =
    df.Lazy()
        .GroupByDynamic(
            indexCol = "Timestamp",
            every = Dur.TimeSpan(TimeSpan.FromHours 1.0),
            period = Dur.TimeSpan(TimeSpan.FromHours 1.0),
            by = [ pl.col "DeviceId" ]
        )
    |> LazyFrame.agg [
        pl.col("Reading").Mean().Alias("Avg_Reading")
        pl.col("Reading").Count().Alias("Sample_Count")
    ]
    |> LazyFrame.collect
```

:::

### Live Execution Output: Dynamic Time-Window Aggregations

<!--@include: ../snippets/timeseries_dynamic_groupby.html-->

---

## 6. Missing Value Interpolation

Sensor disconnections and network drops often introduce missing (`null`) values in time series streams. Simple replacement with `FillNull(0)` can distort trend continuity.

Polars.NET provides vectorized interpolation algorithms:
* `Interpolate(InterpolationMethod.Linear)`: Standard equidistant linear interpolation.
* `Interpolate(InterpolationMethod.Nearest)`: Nearest neighbor interpolation.
* `InterpolateBy(byColumn)`: Non-equidistant interpolation guided by an independent variable column (such as timestamps or spatial coordinates).

::: code-group

```csharp [C# (Fluent)]
var interpolated = df.WithColumns(
    // 1. Uniform linear interpolation by row index
    Col("Reading").Interpolate(InterpolationMethod.Linear).Alias("LinearInterp"),

    // 2. Non-uniform interpolation weighted by exact timestamps
    Col("Reading").InterpolateBy(Col("Timestamp")).Alias("TimeAwareInterp")
);
```

```fsharp [F# (Pipeline)]
let interpolated =
    df
    |> pl.withColumns [
        (pl.col "Reading").Interpolate(InterpolationMethod.Linear).Alias "LinearInterp"
        (pl.col "Reading").InterpolateBy(pl.col "Timestamp").Alias "TimeAwareInterp"
    ]
```

```csharp [C# (LINQ)]
// Native LINQ extension methods for interpolation
var linqInterp = from r in df.AsQueryable<SensorPoint>()
                 select new {
                     Time = r.Timestamp,
                     Reading = r.Reading.Interpolate(InterpolationMethod.Linear)
                 };
```

:::

> [!TIP] Ensure Index Column Monotonicity
> Both `InterpolateBy` and `GroupByDynamic` require the temporal index column to be sorted in ascending order (`Sort("Timestamp")`). Polars asserts monotonicity in Debug mode and relies on sorted order for high-performance $O(N)$ single-pass evaluation in production.
