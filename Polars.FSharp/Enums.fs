namespace Polars.FSharp

open Polars.NET.Core
open Polars.NET.Core.Arrow
open System

type TimeUnit = 
    | Nanoseconds
    | Microseconds
    | Milliseconds
    member internal this.ToNative() =
        match this with
        | Nanoseconds -> PlTimeUnit.Nanoseconds
        | Microseconds -> PlTimeUnit.Microseconds
        | Milliseconds -> PlTimeUnit.Milliseconds

type Field = { Name: string; DataType: DataType }

/// <summary>
/// Polars data types for casting and schema definitions.
/// </summary>
and DataType =
    | Boolean
    | Int8 | Int16 | Int32 | Int64
    | UInt8 | UInt16 | UInt32 | UInt64
    | Float32 | Float64
    | String
    | Date | Datetime of TimeUnit * string option | Time
    | Duration of TimeUnit
    | Binary
    | Categorical
    | Decimal of precision: int option * scale: int option
    | Unknown | SameAsInput | Null | List of DataType | Array of DataType * width: uint64
    | Struct of Field list 
    | Int128 | UInt128 | Float16
    member this.Code : int =
        match this with
        | Unknown | SameAsInput -> 0
        | Boolean -> 1
        | Int8 -> 2
        | Int16 -> 3
        | Int32 -> 4
        | Int64 -> 5
        | UInt8 -> 6
        | UInt16 -> 7
        | UInt32 -> 8
        | UInt64 -> 9
        | Float32 -> 10
        | Float64 -> 11
        | String -> 12
        | Date -> 13
        | Datetime _ -> 14 
        | Time -> 15
        | Duration _ -> 16
        | Binary -> 17
        | Null -> 18
        | Struct _ -> 19
        | List _ -> 20
        | Categorical -> 21
        | Decimal _ -> 22
        | Array _ -> 23
        | Int128 -> 24
        | UInt128 -> 25
        | Float16 -> 26
    static member FromHandle (handle: DataTypeHandle) : DataType =
        let kind = PolarsWrapper.GetDataTypeKind handle

        match kind with
        | 1 -> Boolean
        | 2 -> Int8
        | 3 -> Int16
        | 4 -> Int32
        | 5 -> Int64
        | 6 -> UInt8
        | 7 -> UInt16
        | 8 -> UInt32
        | 9 -> UInt64
        | 10 -> Float32
        | 11 -> Float64
        | 12 -> String 
        | 13 -> Date
        | 24 -> Int128
        | 25 -> UInt128
        | 26 -> Float16
        
        // --- Complex Type ---
        
        // Datetime
        | 14 -> 
            let unitCode = PolarsWrapper.GetTimeUnit handle
            let unit = 
                match unitCode with 
                | 0 -> Nanoseconds 
                | 1 -> Microseconds 
                | 2 -> Milliseconds 
                | _ -> Microseconds
            
            let tz = Option.ofObj (PolarsWrapper.GetTimeZone handle)
            Datetime(unit, tz)

        | 15 -> Time
        
        // Duration
        | 16 -> 
            let unitCode = PolarsWrapper.GetTimeUnit handle
            let unit = 
                match unitCode with 
                | 0 -> Nanoseconds 
                | 1 -> Microseconds 
                | 2 -> Milliseconds 
                | _ -> Microseconds
            Duration unit

        | 17 -> Binary
        | 18 -> Null
        
        // Struct
        | 19 -> 
            let len = PolarsWrapper.GetStructLen handle
            let fields = 
                [ for i in 0UL .. len - 1UL do
                    let mutable name = Unchecked.defaultof<string>
                    let mutable fieldHandle = Unchecked.defaultof<DataTypeHandle>
                    
                    PolarsWrapper.GetStructField(handle, i, &name, &fieldHandle)

                    use h = fieldHandle 
                    yield { Name = name; DataType = DataType.FromHandle h }
                ]
            Struct fields

        // List
        | 20 -> 
            use innerHandle = PolarsWrapper.GetInnerType handle
            let innerType = DataType.FromHandle innerHandle
            List innerType

        | 21 -> Categorical

        // Decimal
        | 22 -> 
            let mutable prec = 0
            let mutable scale = 0
            PolarsWrapper.GetDecimalInfo(handle, &prec, &scale)
            Decimal(Some prec,Some scale)

        | 23 -> 
            use innerHandle = PolarsWrapper.GetInnerType handle
            let width = PolarsWrapper.DataTypeGetArrayWidth handle;
            let innerType = DataType.FromHandle innerHandle
            Array(innerType,width)

        | _ -> Unknown

    member this.IsNumeric =
        match this with
        | UInt8 | UInt16 | UInt32 | UInt64
        | Int8 | Int16 | Int32 | Int64
        | Float32 | Float64 | Int128 | Float16
        | Decimal _ -> true
        | _ -> false

    /// <summary>
    /// Creates a native Polars DataTypeHandle from this F# DataType.
    /// Recursive structures (List, Struct) are handled automatically.
    /// </summary>
    member internal this.CreateHandle() : DataTypeHandle =
        
        let toUnitCode tu = 
            match tu with 
            | Nanoseconds -> 0 
            | Microseconds -> 1 
            | Milliseconds -> 2

        match this with
        | SameAsInput -> PolarsWrapper.NewPrimitiveType 0
        | Null -> PolarsWrapper.NewPrimitiveType 18
        | Boolean -> PolarsWrapper.NewPrimitiveType 1
        | Int8 -> PolarsWrapper.NewPrimitiveType 2
        | Int16 -> PolarsWrapper.NewPrimitiveType 3
        | Int32 -> PolarsWrapper.NewPrimitiveType 4
        | Int64 -> PolarsWrapper.NewPrimitiveType 5
        | UInt8 -> PolarsWrapper.NewPrimitiveType 6
        | UInt16 -> PolarsWrapper.NewPrimitiveType 7
        | UInt32 -> PolarsWrapper.NewPrimitiveType 8
        | UInt64 -> PolarsWrapper.NewPrimitiveType 9
        | Float32 -> PolarsWrapper.NewPrimitiveType 10
        | Float64 -> PolarsWrapper.NewPrimitiveType 11
        | String -> PolarsWrapper.NewPrimitiveType 12
        | Binary -> PolarsWrapper.NewPrimitiveType 17
        | Date -> PolarsWrapper.NewPrimitiveType 13
        | Time -> PolarsWrapper.NewPrimitiveType 15
        | Int128 -> PolarsWrapper.NewPrimitiveType 24
        | UInt128 -> PolarsWrapper.NewPrimitiveType 25
        | Float16 -> PolarsWrapper.NewPrimitiveType 26
        
        // --- Complex Type ---

        // Datetime: Unit and Timezone
        | Datetime(unit, tz) ->
            let code = toUnitCode unit
            let tzStr = Option.toObj tz // None -> null
            PolarsWrapper.NewDateTimeType(byte code, tzStr)

        // Duration
        | Duration unit ->
            let code = toUnitCode unit
            PolarsWrapper.NewDurationType (byte code)

        // Categorical
        | Categorical -> 
            PolarsWrapper.NewCategoricalType()

        // Decimal: precision (p, s)
        | Decimal(p, s) ->
            let prec = defaultArg p 0
            let scale = defaultArg s 0 
            PolarsWrapper.NewDecimalType(prec, scale)

        // List:
        | List innerType ->
            use innerHandle = innerType.CreateHandle()
            
            PolarsWrapper.NewListType innerHandle

        // Struct
        | Struct fields ->
            let names = fields |> List.map (fun f -> f.Name) |> List.toArray

            let typeHandles = fields |> List.map (fun f -> f.DataType.CreateHandle()) |> List.toArray
            
            try
                PolarsWrapper.NewStructType(names, typeHandles)
            finally
                for h in typeHandles do h.Dispose()

        | Array(innerType,width) ->
            use innerHandle = innerType.CreateHandle()
            PolarsWrapper.NewArrayType (innerHandle,width)

        | Unknown -> PolarsWrapper.NewPrimitiveType 0
        
    interface IDisposable with
        member this.Dispose() = 
            ()

    interface IPolarsDataType with
        member this.GetArrowType (): Apache.Arrow.Types.IArrowType = 
            use handle = this.CreateHandle()
            
            ArrowFfiBridge.ImportDataType handle
        
/// <summary>
/// Represents the type of join operation to perform.
/// </summary>
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


/// <summary>
/// Specifies the aggregation function for pivot operations.
/// </summary>
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
type ConcatType =
    | Vertical
    | Horizontal
    | Diagonal
    
    // 内部转换 helper
    member internal this.ToNative() =
        match this with
        | Vertical -> PlConcatType.Vertical
        | Horizontal -> PlConcatType.Horizontal
        | Diagonal -> PlConcatType.Diagonal

type Label =
    | Left 
    | Right 
    | DataPoint
    member internal this.ToNative() =
        match this with
        | Left -> PlLabel.Left
        | Right -> PlLabel.Right
        | DataPoint -> PlLabel.DataPoint 

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

type ClosedWindow =
    | Left
    | Right
    | Both
    | NoWindow
    member internal this.ToNative() =
        match this with
        | Left -> PlClosedWindow.Left
        | Right -> PlClosedWindow.Right
        | Both -> PlClosedWindow.Both
        | NoWindow -> PlClosedWindow.None

type Roll =
    | Raise 
    | Forward 
    | Backward
    member internal this.ToNative() =
        match this with
        | Raise -> PlRoll.Raise
        | Forward -> PlRoll.Forward
        | Backward -> PlRoll.Backward

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

type JoinCoalesce =
    | JoinSpecific
    | CoalesceColumns
    | KeepColumns
    member internal this.ToNative() =
        match this with
        | JoinSpecific -> PlJoinCoalesce.JoinSpecific
        | CoalesceColumns -> PlJoinCoalesce.CoalesceColumns
        | KeepColumns -> PlJoinCoalesce.KeepColumns

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

type AsofStrategy =
    | Backward
    | Forward
    | Nearest
    member internal this.ToNative() =
        match this with
        | Backward -> PlAsofStrategy.Backward
        | Forward -> PlAsofStrategy.Forward
        | Nearest -> PlAsofStrategy.Nearest

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

type CsvEncoding =
    | UTF8
    | LossyUTF8
    member internal this.ToNative() =
        match this with
        | UTF8 -> PlCsvEncoding.UTF8
        | LossyUTF8 -> PlCsvEncoding.LossyUTF8

type JsonFormat =
    | Json
    | JsonLines
    member internal this.ToNative() =
        match this with
        | Json -> PlJsonFormat.Json
        | JsonLines -> PlJsonFormat.JsonLines

type IpcCompression =
    | NoCompression
    | LZ4
    | ZSTD
    member internal this.ToNative() =
        match this with
        | NoCompression -> PlIpcCompression.None
        | LZ4 -> PlIpcCompression.LZ4
        | ZSTD -> PlIpcCompression.ZSTD

type SyncOnClose =
    | NoSync
    | Data
    | All
    member internal this.ToNative() =
        match this with
        | NoSync -> PlSyncOnClose.None
        | Data -> PlSyncOnClose.Data
        | All -> PlSyncOnClose.All

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

type InterpolationMethod =
    | Nearest
    | Linear
    member internal this.ToNative() =
        match this with
        | Nearest -> PlInterpolationMethod.Nearest
        | Linear -> PlInterpolationMethod.Linear

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

type ExternalCompression =
    | Uncompressed
    | Gzip
    | ZSTD
    member internal this.ToNative() =
        match this with
        | Uncompressed -> PlExternalCompression.Uncompressed
        | Gzip -> PlExternalCompression.Gzip
        | ZSTD -> PlExternalCompression.ZSTD

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

type SearchSortedSide =
    | Any 
    | Left  
    | Right
    member internal this.ToNative() =
        match this with
        | Any -> PlSearchSortedSide.Any
        | Left -> PlSearchSortedSide.Left
        | Right -> PlSearchSortedSide.Right

type CatalogTableType = 
    | Managed
    | External
    member internal this.ToNative() =
        match this with
        | Managed -> PlCatalogTableType.Managed
        | External -> PlCatalogTableType.External

    