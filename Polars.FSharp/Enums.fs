namespace Polars.FSharp

open Polars.NET.Core
open System

[<RequireQualifiedAccess>]
type TimeUnit = 
    | Nanoseconds
    | Microseconds
    | Milliseconds
    member internal this.ToNative() =
        match this with
        | Nanoseconds -> PlTimeUnit.Nanoseconds
        | Microseconds -> PlTimeUnit.Microseconds
        | Milliseconds -> PlTimeUnit.Milliseconds
// [<RequireQualifiedAccess>]
// type CorrelationMethod =
//     | Pearson
//     | Spearman
[<RequireQualifiedAccess>]
type EpochTimeUnit = 
    | Nanoseconds
    | Microseconds
    | Milliseconds
    | Second
    | Day
[<RequireQualifiedAccess>]
type TransferEncoding =
    | Base64
    | Hex
[<RequireQualifiedAccess>]
type Endianness =
    | Little
    | Big
[<RequireQualifiedAccess>]
type CategoricalPhysical =
    | U32
    | U16
    | U8
    member internal this.ToNative() =
        match this with
        | U32 -> PlCategoricalPhysical.U32
        | U16 -> PlCategoricalPhysical.U16
        | U8 -> PlCategoricalPhysical.U8
[<RequireQualifiedAccess>]
type UniqueKeepStrategy = 
    | First
    | Last
    | Any
    | NoKeep
    member internal this.ToNative() =
        match this with
        | First -> PlUniqueKeepStrategy.First
        | Last -> PlUniqueKeepStrategy.Last
        | Any -> PlUniqueKeepStrategy.Any
        | NoKeep -> PlUniqueKeepStrategy.None
/// <summary>
/// Represents the type of join operation to perform.
/// </summary>
[<RequireQualifiedAccess>]
type JoinType =
    | Inner
    | Left
    | Outer
    | Cross
    | Semi
    | Anti
    member internal this.ToNative() =
        match this with
        | Inner -> PlJoinType.Inner
        | Left -> PlJoinType.Left
        | Outer -> PlJoinType.Outer
        | Cross -> PlJoinType.Cross
        | Semi -> PlJoinType.Semi
        | Anti -> PlJoinType.Anti
[<RequireQualifiedAccess>]
type JoinSide =
    | LetPolarsDecide
    | PreferLeft
    | ForceLeft
    | PreferRight
    | ForceRight
    member internal this.ToNative() =
        match this with
        | LetPolarsDecide -> PlJoinSide.None
        | PreferRight -> PlJoinSide.PreferRight
        | PreferLeft -> PlJoinSide.PreferLeft
        | ForceLeft -> PlJoinSide.ForceLeft
        | ForceRight -> PlJoinSide.ForceRight
[<RequireQualifiedAccess>]
type WindowMappingStrategy = 
    | GroupsToRows
    | Explode
    | Join
    member internal this.ToNative() =
        match this with
        | GroupsToRows -> PlWindowMapping.GroupsToRows
        | Explode -> PlWindowMapping.Explode
        | Join -> PlWindowMapping.Join 

[<RequireQualifiedAccess>]
type PivotColumnNaming =
    /// <summary>
    /// The default; combine with separator if there are multiple
    /// values columns, otherwise just use the on_columns names.
    /// </summary>
    | Auto
    /// <summary>
    /// Always combine the values columns’ names with
    /// the on_columns names.
    /// </summary>
    | Combine
    member internal this.ToNative() =
        match this with
        | Auto -> PlPivotColumnNaming.Auto
        | Combine -> PlPivotColumnNaming.Combine


/// <summary>
/// Specifies the aggregation function for pivot operations.
/// </summary>
[<RequireQualifiedAccess>]
type PivotAgg =
    | First | Sum | Min | Max | Mean | Median | Count | Last
    
    member internal this.ToNative() =
        match this with
        | First -> PlPivotAgg.First
        | Sum -> PlPivotAgg.Sum
        | Min -> PlPivotAgg.Min
        | Max -> PlPivotAgg.Max
        | Mean -> PlPivotAgg.Mean
        | Median -> PlPivotAgg.Median
        | Count -> PlPivotAgg.Count
        | Last -> PlPivotAgg.Last

/// <summary>
/// Specifies the type of concat operations.
/// </summary>
[<RequireQualifiedAccess>]
type ConcatType =
    | Vertical
    | Horizontal
    | Diagonal
    
    member internal this.ToNative() =
        match this with
        | Vertical -> PlConcatType.Vertical
        | Horizontal -> PlConcatType.Horizontal
        | Diagonal -> PlConcatType.Diagonal
[<RequireQualifiedAccess>]
type Label =
    | Left 
    | Right 
    | DataPoint
    member internal this.ToNative() =
        match this with
        | Left -> PlLabel.Left
        | Right -> PlLabel.Right
        | DataPoint -> PlLabel.DataPoint 
[<RequireQualifiedAccess>]
type StartBy =
    | WindowBound
    | DataPoint
    | Monday
    | Tuesday
    | Wednesday
    | Thursday
    | Friday
    | Saturday
    | Sunday
    member internal this.ToNative() =
        match this with
        | WindowBound -> PlStartBy.WindowBound
        | DataPoint -> PlStartBy.DataPoint
        | Monday -> PlStartBy.Monday
        | Tuesday -> PlStartBy.Tuesday
        | Wednesday -> PlStartBy.Wednesday
        | Thursday -> PlStartBy.Thursday
        | Friday -> PlStartBy.Friday    
        | Saturday -> PlStartBy.Saturday
        | Sunday -> PlStartBy.Sunday
[<RequireQualifiedAccess>]
type ClosedWindow =
    | Left
    | Right
    | Both
    | NoWindow
    member internal this.ToNative() =
        match this with
        | Left -> PlClosedInterval.Left
        | Right -> PlClosedInterval.Right
        | Both -> PlClosedInterval.Both
        | NoWindow -> PlClosedInterval.None
[<RequireQualifiedAccess>]
type NonExistent =
    | Raise
    | SetNull
    member internal this.ToNative() =
        match this with
        | Raise -> PlNonExistent.Raise
        | SetNull -> PlNonExistent.Null
[<RequireQualifiedAccess>]
type Roll =
    | Raise 
    | Forward 
    | Backward
    member internal this.ToNative() =
        match this with
        | Raise -> PlRoll.Raise
        | Forward -> PlRoll.Forward
        | Backward -> PlRoll.Backward
[<RequireQualifiedAccess>]
type Engine =
    | Auto
    | InMemory
    | Gpu
    | Streaming
    member internal this.ToNative() =
        match this with
        | Auto -> PlEngine.Auto
        | InMemory -> PlEngine.InMemory
        | Gpu -> PlEngine.Gpu
        | Streaming -> PlEngine.Streaming
[<RequireQualifiedAccess>]
type QuantileMethod =
    | Nearest 
    | Higher 
    | Lower
    | Midpoint
    | Linear
    member internal this.ToNative() =
        match this with
        | Nearest -> PlQuantileMethod.Nearest
        | Higher -> PlQuantileMethod.Higher
        | Lower -> PlQuantileMethod.Lower
        | Midpoint -> PlQuantileMethod.Midpoint
        | Linear -> PlQuantileMethod.Linear
[<RequireQualifiedAccess>]
type RoundMode =
    | HalfAwayFromZero
    | HalfToEven
    | ToZero
    member internal this.ToNative() =
        match this with
        | HalfAwayFromZero -> PlRoundMode.HalfAwayFromZero
        | HalfToEven -> PlRoundMode.HalfToEven
        | ToZero -> PlRoundMode.ToZero
[<RequireQualifiedAccess>]
type NullBehavior = 
    | Ignore
    | Drop 
    member internal this.ToNative() =
        match this with
        | Ignore -> PlNullBehavior.Ignore
        | Drop -> PlNullBehavior.Drop
[<RequireQualifiedAccess>]
type FillNullStrategy = 
    | Forward
    | Backward
    | Max 
    | Min
    | Mean
    | Zero
    | One 
    member internal this.ToNative() =
        match this with
        | Forward -> PlFillNullStrategy.Forward
        | Backward -> PlFillNullStrategy.Backward
        | Max -> PlFillNullStrategy.Max
        | Min -> PlFillNullStrategy.Min
        | Mean -> PlFillNullStrategy.Mean
        | Zero -> PlFillNullStrategy.Zero
        | One -> PlFillNullStrategy.One
[<RequireQualifiedAccess>]
type RankMethod =
    | Average 
    | Min
    | Max
    | Dense
    | Ordinal
    | Random
    member internal this.ToNative() =
        match this with
        | Average -> PlRankMethod.Average
        | Min -> PlRankMethod.Min
        | Max -> PlRankMethod.Max
        | Dense -> PlRankMethod.Dense
        | Ordinal -> PlRankMethod.Ordinal
        | Random -> PlRankMethod.Random
[<RequireQualifiedAccess>]
type RollingRankMethod =
    | Average 
    | Min
    | Max
    | Dense
    | Random
    member internal this.ToNative() =
        match this with
        | Average -> PlRollingRankMethod.Average
        | Min -> PlRollingRankMethod.Min
        | Max -> PlRollingRankMethod.Max
        | Dense -> PlRollingRankMethod.Dense
        | Random -> PlRollingRankMethod.Random
[<RequireQualifiedAccess>]
type JoinValidation =
    | ManyToMany
    | ManyToOne
    | OneToMany
    | OneToOne
    member internal this.ToNative() =
        match this with
        | ManyToMany -> PlJoinValidation.ManyToMany
        | ManyToOne -> PlJoinValidation.ManyToOne
        | OneToMany -> PlJoinValidation.OneToMany
        | OneToOne -> PlJoinValidation.OneToOne
[<RequireQualifiedAccess>]
type JoinCoalesce =
    | JoinSpecific
    | CoalesceColumns
    | KeepColumns
    member internal this.ToNative() =
        match this with
        | JoinSpecific -> PlJoinCoalesce.JoinSpecific
        | CoalesceColumns -> PlJoinCoalesce.CoalesceColumns
        | KeepColumns -> PlJoinCoalesce.KeepColumns
[<RequireQualifiedAccess>]
type JoinMaintainOrder =
    | NotMaintainOrder
    | Left
    | Right
    | LeftRight
    | RightLeft
    member internal this.ToNative() =
        match this with
        | NotMaintainOrder -> PlJoinMaintainOrder.None
        | Left -> PlJoinMaintainOrder.Left
        | Right -> PlJoinMaintainOrder.Right
        | LeftRight -> PlJoinMaintainOrder.LeftRight
        | RightLeft -> PlJoinMaintainOrder.RightLeft
[<RequireQualifiedAccess>]
type AsofStrategy =
    | Backward
    | Forward
    | Nearest
    member internal this.ToNative() =
        match this with
        | Backward -> PlAsofStrategy.Backward
        | Forward -> PlAsofStrategy.Forward
        | Nearest -> PlAsofStrategy.Nearest
[<RequireQualifiedAccess>]
type ParallelStrategy =
    | Auto
    | Columns
    | RowGroups
    | NoParallel
    member internal this.ToNative() =
        match this with
        | Auto -> PlParallelStrategy.Auto
        | Columns -> PlParallelStrategy.Columns
        | RowGroups -> PlParallelStrategy.RowGroups
        | NoParallel -> PlParallelStrategy.None
[<RequireQualifiedAccess>]
type CsvEncoding =
    | UTF8
    | LossyUTF8
    member internal this.ToNative() =
        match this with
        | UTF8 -> PlCsvEncoding.UTF8
        | LossyUTF8 -> PlCsvEncoding.LossyUTF8
[<RequireQualifiedAccess>]
type JsonFormat =
    | Json
    | JsonLines
    member internal this.ToNative() =
        match this with
        | Json -> PlJsonFormat.Json
        | JsonLines -> PlJsonFormat.JsonLines
[<RequireQualifiedAccess>]
type IpcCompression =
    | NoCompression
    | LZ4
    | ZSTD
    member internal this.ToNative() =
        match this with
        | NoCompression -> PlIpcCompression.None
        | LZ4 -> PlIpcCompression.LZ4
        | ZSTD -> PlIpcCompression.ZSTD
[<RequireQualifiedAccess>]
type SyncOnClose =
    | NoSync
    | Data
    | All
    member internal this.ToNative() =
        match this with
        | NoSync -> PlSyncOnClose.None
        | Data -> PlSyncOnClose.Data
        | All -> PlSyncOnClose.All
[<RequireQualifiedAccess>]
type ParquetCompression =
    | Uncompressed
    | Snappy
    | Gzip
    | Brotli
    | Zstd
    | Lz4Raw
    member internal this.ToNative() =
        match this with
        | Uncompressed -> PlParquetCompression.Uncompressed
        | Snappy -> PlParquetCompression.Snappy
        | Gzip -> PlParquetCompression.Gzip
        | Brotli -> PlParquetCompression.Brotli
        | Zstd -> PlParquetCompression.ZSTD
        | Lz4Raw -> PlParquetCompression.Lz4Raw
[<RequireQualifiedAccess>]
type QuoteStyle =
    | Always
    | Necessary
    | Never
    | NonNumeric
    member internal this.ToNative() =
        match this with
        | Always -> PlQuoteStyle.Always
        | Necessary -> PlQuoteStyle.Necessary
        | Never -> PlQuoteStyle.Never
        | NonNumeric -> PlQuoteStyle.NonNumeric
[<RequireQualifiedAccess>]
type InterpolationMethod =
    | Nearest
    | Linear
    member internal this.ToNative() =
        match this with
        | Nearest -> PlInterpolationMethod.Nearest
        | Linear -> PlInterpolationMethod.Linear
[<RequireQualifiedAccess>]
type CloudProvider =
    | NotCloud
    | Aws
    | Azure
    | Gcp
    | Http
    | HuggingFace
    member internal this.ToNative() =
        match this with
        | NotCloud -> PlCloudProvider.None
        | Aws -> PlCloudProvider.Aws
        | Azure -> PlCloudProvider.Azure
        | Gcp -> PlCloudProvider.Gcp
        | Http -> PlCloudProvider.Http
        | HuggingFace -> PlCloudProvider.HuggingFace

/// <summary>
/// mode for saving delta lake table
/// </summary>
[<RequireQualifiedAccess>]
type DeltaSaveMode =
    | Append
    | Overwrite
    | ErrorIfExists
    | Ignore
    member internal this.ToNative() = 
        match this with 
        | Append -> PlDeltaSaveMode.Append
        | Overwrite -> PlDeltaSaveMode.Overwrite
        | ErrorIfExists -> PlDeltaSaveMode.ErrorIfExists
        | Ignore -> PlDeltaSaveMode.Ignore

[<RequireQualifiedAccess>]
type ExternalCompression =
    | Uncompressed
    | Gzip
    | ZSTD
    member internal this.ToNative() =
        match this with
        | Uncompressed -> PlExternalCompression.Uncompressed
        | Gzip -> PlExternalCompression.Gzip
        | ZSTD -> PlExternalCompression.ZSTD

[<RequireQualifiedAccess>]
type AvroCompression =
    | Uncompressed
    | Deflate
    | Snappy
    member internal this.ToNative() =
        match this with
        | Uncompressed -> PlAvroCompression.Uncompressed
        | Deflate -> PlAvroCompression.Deflate
        | Snappy -> PlAvroCompression.Snappy

/// <summary>
/// Defines the type of action to perform during a Delta Merge operation.
/// </summary>
type MergeActionType =
    | MatchedUpdate
    | MatchedDelete
    | NotMatchedInsert
    | NotMatchedBySourceDelete
    member internal this.ToNative() =
        match this with
        | MatchedUpdate -> PlMergeActionType.MatchedUpdate
        | MatchedDelete -> PlMergeActionType.MatchedDelete
        | NotMatchedInsert -> PlMergeActionType.NotMatchedInsert
        | NotMatchedBySourceDelete -> PlMergeActionType.NotMatchedBySourceDelete

/// <summary>
/// Specifies the behavior when bulk ingesting a DataFrame into an ADBC database table.
/// </summary>
[<RequireQualifiedAccess>]
type AdbcIngestMode =
    /// <summary>
    /// Creates a new table and inserts the data. 
    /// Fails if the target table already exists. (Default behavior)
    /// </summary>
    | Create
    /// <summary>
    /// Appends the data to an existing table. 
    /// Fails if the target table does not exist, or if the DataFrame schema doesn't match.
    /// </summary>
    | Append
    /// <summary>
    /// Drops the target table if it already exists, creates a new one, and inserts the data.
    /// Extremely useful for overriding temporary/staging tables.
    /// </summary>
    | Replace

[<RequireQualifiedAccess>]
type SearchSortedSide =
    | Any 
    | Left  
    | Right
    member internal this.ToNative() =
        match this with
        | Any -> PlSearchSortedSide.Any
        | Left -> PlSearchSortedSide.Left
        | Right -> PlSearchSortedSide.Right

[<RequireQualifiedAccess>]
type CatalogTableType = 
    | Managed
    | External
    member internal this.ToNative() =
        match this with
        | Managed -> PlCatalogTableType.Managed
        | External -> PlCatalogTableType.External

[<RequireQualifiedAccess>]
type SizeUnit =
    | Bytes
    | Kilobytes
    | Megabytes
    | Gigabytes
    | Terabytes
    
/// <summary>
/// Bitwise flags representing the sorting state of a Series or Column.
/// </summary>
[<Flags>]
type SortStateFlags =
    | NotSorted  = 0uy
    | IsSorted   = 1uy   // 001
    | Descending = 2uy   // 010
    | NullsLast  = 4uy   // 100

[<RequireQualifiedAccess>]
type MissingColumnsPolicy = 
    | Raise  
    | Insert
    member internal this.ToNative() =
        match this with
        | Raise -> PlMissingColumnsPolicy.Raise
        | Insert -> PlMissingColumnsPolicy.Insert

[<RequireQualifiedAccess>]
type UpcastOrForbid =
    | Forbid
    | Upcast
    member internal this.ToNative() =
        match this with
        | Forbid -> PlUpcastOrForbid.Forbid
        | Upcast -> PlUpcastOrForbid.Upcast

[<RequireQualifiedAccess>]
type ExtraColumnsPolicy =
    | Raise 
    | Ignore 
    member internal this.ToNative() =
        match this with
        | Raise -> PlExtraColumnsPolicy.Raise
        | Ignore -> PlExtraColumnsPolicy.Ignore
[<RequireQualifiedAccess>]
type UnstackDirection =
    | Vertical
    | Horizontal

