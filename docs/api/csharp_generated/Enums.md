---
uid: Polars.CSharp.Enums
title: Enums
---

# Enums

Namespace: `Polars.CSharp`

## AdbcIngestMode

Specifies the behavior when bulk ingesting a DataFrame into an ADBC database table.

```csharp
public enum AdbcIngestMode
{
    Create,
    Append,
    Replace
}
```

---

## Alignment

```csharp
public enum Alignment
{
    Left,
    Center,
    Right
}
```

---

## AmbiguousStrategy

```csharp
public enum AmbiguousStrategy
{
    Raise,
    Earliest,
    Latest,
    Null
}
```

---

## AsofStrategy

```csharp
public enum AsofStrategy
{
    Backward,
    Forward,
    Nearest
}
```

---

## AvroCompression

```csharp
public enum AvroCompression
{
    Uncompressed,
    Deflate,
    Snappy
}
```

---

## CatalogTableType

```csharp
public enum CatalogTableType
{
    Managed,
    External
}
```

---

## CategoricalPhysical

```csharp
public enum CategoricalPhysical
{
    U32,
    U16,
    U8
}
```

---

## ClosedInterval

```csharp
public enum ClosedInterval
{
    Left,
    Right,
    Both,
    None
}
```

---

## CloudProvider

Supported cloud providers for IO operations.

```csharp
public enum CloudProvider
{
    None,
    Aws,
    Azure,
    Gcp,
    Http,
    HuggingFace
}
```

---

## ConcatType

Concat Type Enum

```csharp
public enum ConcatType
{
    Vertical,
    Horizontal,
    Diagonal
}
```

---

## CorrelationMethod

```csharp
public enum CorrelationMethod
{
    Pearson,
    Spearman
}
```

---

## CsvEncoding

```csharp
public enum CsvEncoding
{
    UTF8,
    LossyUTF8
}
```

---

## DataTypeKind

Enum of DataTypeKind

```csharp
public enum DataTypeKind
{
    SameAsInput,
    Unknown,
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    UInt8,
    UInt16,
    UInt32,
    UInt64,
    Float32,
    Float64,
    String,
    Date,
    Datetime,
    Time,
    Duration,
    Binary,
    Null,
    Struct,
    List,
    Categorical,
    Decimal,
    Array,
    Int128,
    UInt128,
    Float16,
    Enum,
    Extension
}
```

---

## DeltaSaveMode

mode for saving delta lake table

```csharp
public enum DeltaSaveMode
{
    Append,
    Overwrite,
    ErrorIfExists,
    Ignore
}
```

---

## Endianness

```csharp
public enum Endianness
{
    Little,
    Big
}
```

---

## Engine

Execution engine for LazyFrame.Collect()

```csharp
public enum Engine
{
    Auto,
    InMemory,
    Streaming,
    Gpu
}
```

---

## EpochTimeUnit

```csharp
public enum EpochTimeUnit
{
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Second,
    Day
}
```

---

## ExternalCompression

```csharp
public enum ExternalCompression
{
    Uncompressed,
    Gzip,
    ZSTD
}
```

---

## ExtraColumnsPolicy

```csharp
public enum ExtraColumnsPolicy
{
    Raise,
    Ignore
}
```

---

## FillNullStrategy

```csharp
public enum FillNullStrategy
{
    Forward,
    Backward,
    Max,
    Min,
    Mean,
    Zero,
    One
}
```

---

## FloatFormat

```csharp
public enum FloatFormat
{
    Mixed,
    Full
}
```

---

## InterpolationMethod

```csharp
public enum InterpolationMethod
{
    Linear,
    Nearest
}
```

---

## IpcCompression

```csharp
public enum IpcCompression
{
    None,
    LZ4,
    ZSTD
}
```

---

## JoinCoalesce

```csharp
public enum JoinCoalesce
{
    JoinSpecific,
    CoalesceColumns,
    KeepColumns
}
```

---

## JoinMaintainOrder

```csharp
public enum JoinMaintainOrder
{
    None,
    Left,
    Right,
    LeftRight,
    RightLeft
}
```

---

## JoinSide

Specifies the strategy for the hash join build side. 

In a hash join, one dataframe is loaded into memory (the "build side") to construct a hash table, while the other is streamed (the "probe side") to find matches.

Generally, the smaller dataframe should be the build side for optimal performance and memory usage.

```csharp
public enum JoinSide
{
    LetPolarsDecide,
    PreferLeft,
    ForceLeft,
    PreferRight,
    ForceRight
}
```

---

## JoinType

Enums of RightTypes

```csharp
public enum JoinType
{
    Inner,
    Left,
    Outer,
    Cross,
    Semi,
    Anti,
    IEJoin
}
```

---

## JoinValidation

```csharp
public enum JoinValidation
{
    ManyToMany,
    ManyToOne,
    OneToMany,
    OneToOne
}
```

---

## JsonFormat

```csharp
public enum JsonFormat
{
    Json,
    JsonLines
}
```

---

## Label

Defines which boundary of the window to use for the label.

```csharp
public enum Label
{
    Left,
    Right,
    DataPoint
}
```

---

## MergeActionType

Defines the type of action to perform during a Merge operation.

```csharp
public enum MergeActionType
{
    MatchedUpdate,
    MatchedDelete,
    NotMatchedInsert,
    NotMatchedBySourceDelete
}
```

---

## MissingColumnsPolicy

```csharp
public enum MissingColumnsPolicy
{
    Raise,
    Insert
}
```

---

## NonExistent

```csharp
public enum NonExistent
{
    Raise,
    Null
}
```

---

## NullBehavior

```csharp
public enum NullBehavior
{
    Ignore,
    Drop
}
```

---

## ParallelStrategy

```csharp
public enum ParallelStrategy
{
    Auto,
    Columns,
    RowGroups,
    None
}
```

---

## ParquetCompression

```csharp
public enum ParquetCompression
{
    None,
    Snappy,
    Gzip,
    Brotli,
    ZSTD,
    Lz4Raw
}
```

---

## PivotAgg

Specifies the aggregation function for pivot operations.

```csharp
public enum PivotAgg
{
    First,
    Sum,
    Min,
    Max,
    Mean,
    Median,
    Count,
    Len,
    Last
}
```

---

## PivotColumnNaming

```csharp
public enum PivotColumnNaming
{
    Auto,
    Combine
}
```

---

## QuantileMethod

```csharp
public enum QuantileMethod
{
    Nearest,
    Higher,
    Lower,
    Midpoint,
    Linear,
    Equiprobable
}
```

---

## QuoteStyle

```csharp
public enum QuoteStyle
{
    Necessary,
    Always,
    NonNumeric,
    Never
}
```

---

## RankMethod

```csharp
public enum RankMethod
{
    Average,
    Min,
    Max,
    Dense,
    Ordinal,
    Random
}
```

---

## Roll

Strategy to handle dates that land on non-business days (weekends or holidays).

```csharp
public enum Roll
{
    Raise,
    Forward,
    Backward
}
```

---

## RollingRankMethod

```csharp
public enum RollingRankMethod
{
    Average,
    Min,
    Max,
    Dense,
    Random
}
```

---

## RoundMode

```csharp
public enum RoundMode
{
    HalfAwayFromZero,
    HalfToEven,
    ToZero
}
```

---

## SearchSortedSide

```csharp
public enum SearchSortedSide
{
    Any,
    Left,
    Right
}
```

---

## SizeUnit

```csharp
public enum SizeUnit
{
    Bytes,
    Kilobytes,
    Megabytes,
    Gigabytes,
    Terabytes
}
```

---

## SortStateFlags

```csharp
public enum SortStateFlags
{
    NotSorted,
    IsSorted,
    Descending,
    NullsLast
}
```

---

## StartBy

Defines the strategy for determining the start of the window.

```csharp
public enum StartBy
{
    WindowBound,
    DataPoint,
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday
}
```

---

## SyncOnClose

Controls the file synchronization behavior when closing the file.

```csharp
public enum SyncOnClose
{
    None,
    Data,
    All
}
```

---

## TableFormatting

```csharp
public enum TableFormatting
{
    AsciiFull,
    AsciiFullCondensed,
    AsciiNoBorders,
    AsciiBordersOnly,
    AsciiBordersOnlyCondensed,
    AsciiHorizontalOnly,
    AsciiMarkdown,
    Markdown,
    Utf8Full,
    Utf8FullCondensed,
    Utf8NoBorders,
    Utf8BordersOnly,
    Utf8HorizontalOnly,
    Nothing
}
```

---

## TimeUnit

TimeUnit Enums

```csharp
public enum TimeUnit
{
    Nanoseconds,
    Microseconds,
    Milliseconds
}
```

---

## TransferEncoding

```csharp
public enum TransferEncoding
{
    Hex,
    Base64
}
```

---

## UniqueKeepStrategy

```csharp
public enum UniqueKeepStrategy
{
    First,
    Last,
    Any,
    None
}
```

---

## UnstackDirection

```csharp
public enum UnstackDirection
{
    Vertical,
    Horizontal
}
```

---

## UpcastOrForbid

```csharp
public enum UpcastOrForbid
{
    Forbid,
    Upcast
}
```

---

## WindowMappingStrategy

```csharp
public enum WindowMappingStrategy
{
    GroupsToRows,
    Explode,
    Join
}
```

---

