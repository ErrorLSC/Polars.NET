namespace Polars.FSharp

open Polars.NET.Core.Enums
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

    static member internal FromNative(timeUnit: PlTimeUnit) =
        match timeUnit with
        | PlTimeUnit.Nanoseconds -> TimeUnit.Nanoseconds
        | PlTimeUnit.Microseconds -> TimeUnit.Microseconds
        | PlTimeUnit.Milliseconds -> TimeUnit.Milliseconds
        | _ -> TimeUnit.Microseconds

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
    | First
    | Sum
    | Min
    | Max
    | Mean
    | Median
    | Count
    | Last

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
type ClosedInterval =
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
    /// <summary>
    /// Not supported: collecting with this engine throws <see cref="System.NotSupportedException"/>. The Polars Rust
    /// core has no GPU executor (GPU execution in Python Polars comes from the cudf-polars Python package), so the
    /// query would otherwise run in memory on the CPU.
    /// </summary>
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

/// <summary>
/// The text encoding of CSV read or written: UTF-8 by default (the default value is <c>CsvEncoding.UTF8</c>), any
/// encoding .NET knows, legacy code pages included (Shift_JIS, GBK, Big5, Windows-1252 …: no provider to register).
/// Given by its static members, <c>CsvEncoding.CodePage 932</c>, <c>CsvEncoding.Named "shift_jis"</c> or
/// <c>CsvEncoding.Of encoding</c>.
/// </summary>
/// <remarks>
/// Strict by default: text the encoding does not define is an error naming the row. <c>Lossy()</c> reads it as U+FFFD
/// and writes a character the encoding cannot hold as <c>?</c>. A byte order mark at the start of input read is
/// honoured (it names UTF-8 or UTF-16 whatever is given, unless another non-UTF-8 encoding is given: then an error).
/// </remarks>
[<Struct; CustomEquality; NoComparison>]
type CsvEncoding private (given: System.Text.Encoding, lossy: bool) =
    static member private Make(encoding: System.Text.Encoding, lossy) =
        CsvEncoding((if isNull encoding || encoding.CodePage = 65001 then null else encoding), lossy)

    /// <summary>UTF-8, strict (the default).</summary>
    static member UTF8 = CsvEncoding(null, false)
    /// <summary>UTF-8, invalid bytes read as U+FFFD.</summary>
    static member LossyUTF8 = CsvEncoding(null, true)
    /// <summary>UTF-16, little endian (code page 1200).</summary>
    static member Utf16LE = CsvEncoding(System.Text.Encoding.Unicode, false)
    /// <summary>UTF-16, big endian (code page 1201).</summary>
    static member Utf16BE = CsvEncoding(System.Text.Encoding.BigEndianUnicode, false)
    /// <summary>Shift_JIS as Windows writes it (code page 932: NEC and IBM extensions).</summary>
    static member ShiftJis = CsvEncoding.CodePage 932
    /// <summary>GBK, simplified Chinese (code page 936).</summary>
    static member Gbk = CsvEncoding.CodePage 936
    /// <summary>Big5, traditional Chinese (code page 950).</summary>
    static member Big5 = CsvEncoding.CodePage 950
    /// <summary>EUC-KR / Unified Hangul Code, Korean (code page 949).</summary>
    static member EucKr = CsvEncoding.CodePage 949
    /// <summary>Windows-1252, Western European (code page 1252).</summary>
    static member Windows1252 = CsvEncoding.CodePage 1252
    /// <summary>ISO-8859-1 (Latin-1).</summary>
    static member Latin1 = CsvEncoding(System.Text.Encoding.Latin1, false)

    /// <summary>The encoding of a code page (932, 936, 1252, 65001 …); an ArgumentException when .NET knows none.</summary>
    static member CodePage(codePage: int) = CsvEncoding.Make(Polars.NET.Core.Csv.ManagedCsv.EncodingOf codePage, false)
    /// <summary>The encoding of a name ("shift_jis", "gbk", "windows-1252" …); an ArgumentException when .NET knows none.</summary>
    static member Named(name: string) = CsvEncoding.Make(Polars.NET.Core.Csv.ManagedCsv.EncodingOf name, false)
    /// <summary>A .NET encoding (its own fallbacks are not used: strict, or lossy).</summary>
    static member Of(encoding: System.Text.Encoding) =
        if isNull encoding then nullArg (nameof encoding)
        CsvEncoding.Make(encoding, false)

    /// <summary>This encoding, lossy: invalid input read as U+FFFD, characters it cannot hold written as <c>?</c>.</summary>
    member _.Lossy() = CsvEncoding(given, true)
    /// <summary>Whether invalid text is replaced (else an error naming the row).</summary>
    member _.IsLossy = lossy
    /// <summary>The .NET encoding.</summary>
    member _.Encoding = if isNull given then System.Text.Encoding.UTF8 else given
    /// <summary>The encoding given to the readers and writers (null: UTF-8).</summary>
    member internal _.Given = given

    override this.Equals(other) =
        match other with
        | :? CsvEncoding as o -> o.IsLossy = lossy && o.Encoding.CodePage = this.Encoding.CodePage
        | _ -> false
    override this.GetHashCode() = System.HashCode.Combine(this.Encoding.CodePage, lossy)
    override this.ToString() = if lossy then this.Encoding.WebName + " (lossy)" else this.Encoding.WebName

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

/// <summary>What a partitioned write does with files under its path already.</summary>
[<RequireQualifiedAccess>]
type ExistingData =
    /// <summary>The write fails when any file is there (the default).</summary>
    | Error
    /// <summary>The files are added beside those there (named apart by the write's id).</summary>
    | Append
    /// <summary>The files there in the partitions written are deleted once every file of the write is published (a failed write leaves them); the other partitions are left as they are.</summary>
    | ReplacePartitions

    member internal this.ToNative() =
        match this with
        | Error -> Kaminari.Seseragi.Files.ExistingData.Error
        | Append -> Kaminari.Seseragi.Files.ExistingData.Append
        | ReplacePartitions -> Kaminari.Seseragi.Files.ExistingData.ReplacePartitions

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
    | Zstandard

    member internal this.ToNative() =
        match this with
        | Uncompressed -> PlAvroCompression.Uncompressed
        | Deflate -> PlAvroCompression.Deflate
        | Snappy -> PlAvroCompression.Snappy
        | Zstandard -> PlAvroCompression.Zstandard

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
    | NotSorted = 0uy
    | IsSorted = 1uy // 001
    | Descending = 2uy // 010
    | NullsLast = 4uy // 100

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

[<RequireQualifiedAccess>]
type TableFormatting =
    | AsciiFull
    | AsciiFullCondensed
    | AsciiNoBorders
    | AsciiBordersOnly
    | AsciiBordersOnlyCondensed
    | AsciiHorizontalOnly
    | AsciiMarkdown
    | Markdown
    | Utf8Full
    | Utf8FullCondensed
    | Utf8NoBorders
    | Utf8BordersOnly
    | Utf8HorizontalOnly
    | Nothing
    | ResetToDefault

    member internal this.ToNative() =
        match this with
        | AsciiFull -> Nullable PlTableFormatting.AsciiFull
        | AsciiFullCondensed -> Nullable PlTableFormatting.AsciiFullCondensed
        | AsciiNoBorders -> Nullable PlTableFormatting.AsciiNoBorders
        | AsciiBordersOnly -> Nullable PlTableFormatting.AsciiBordersOnly
        | AsciiBordersOnlyCondensed -> Nullable PlTableFormatting.AsciiBordersOnlyCondensed
        | AsciiHorizontalOnly -> Nullable PlTableFormatting.AsciiHorizontalOnly
        | AsciiMarkdown -> Nullable PlTableFormatting.AsciiMarkdown
        | Markdown -> Nullable PlTableFormatting.Markdown
        | Utf8Full -> Nullable PlTableFormatting.Utf8Full
        | Utf8FullCondensed -> Nullable PlTableFormatting.Utf8FullCondensed
        | Utf8NoBorders -> Nullable PlTableFormatting.Utf8NoBorders
        | Utf8BordersOnly -> Nullable PlTableFormatting.Utf8BordersOnly
        | Utf8HorizontalOnly -> Nullable PlTableFormatting.Utf8HorizontalOnly
        | Nothing -> Nullable PlTableFormatting.Nothing
        | ResetToDefault -> Nullable()

[<RequireQualifiedAccess>]
type Alignment =
    | Left
    | Center
    | Right
    | ResetToDefault

    member internal this.ToNative() =
        match this with
        | Left -> Nullable PlTableCellAlignment.Left
        | Center -> Nullable PlTableCellAlignment.Center
        | Right -> Nullable PlTableCellAlignment.Right
        | ResetToDefault -> Nullable()

[<RequireQualifiedAccess>]
type FloatFormat =

    /// <summary>
    /// Limit the number of decimal places and use scientific notation for large/small values.
    /// </summary>
    | Mixed
    /// <summary>
    /// Print the full precision of the floating point number.
    /// </summary>
    | Full

    | ResetToDefault

    member internal this.ToNative() =
        match this with
        | Mixed -> Nullable PlFloatFormat.Mixed
        | Full -> Nullable PlFloatFormat.Full
        | ResetToDefault -> Nullable()

/// <summary>
/// The policy for operations or expressions that cannot be pushed down to the native Polars engine and would
/// otherwise require client-side (.NET managed) evaluation.
/// </summary>
[<RequireQualifiedAccess>]
type ClientEvaluationPolicy =
    | Strict
    | Warn
    | Allow

    member internal this.ToNative() =
        match this with
        | Strict -> Kaminari.Samidare.Config.ClientEvaluationPolicy.Strict
        | Warn -> Kaminari.Samidare.Config.ClientEvaluationPolicy.Warn
        | Allow -> Kaminari.Samidare.Config.ClientEvaluationPolicy.Allow
