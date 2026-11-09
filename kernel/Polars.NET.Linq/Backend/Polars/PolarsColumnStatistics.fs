namespace Polars.NET.Linq.Backend.Polars

open System
open System.Collections.Concurrent
open Apache.Arrow.Types
open Polars.NET.Core
open Polars.NET.Core.Enums
open Polars.NET.Core.Wrappers
open Polars.NET.Linq.IR

/// Column statistics read from the file behind a LazyFrame while a query is translated.
///
/// Only a small file table is read, and only a column that a filter tests, once per process for each scan of a given
/// plan: what the statistics change is the choice of plan, so a wrong or stale one costs speed, never correctness. The
/// tables that filters most often reduce are dimensions, which is what makes reading them whole affordable. In-memory
/// frames have no stable identity to remember the result by, and are small enough for fixed selectivities.
module internal PolarsColumnStatistics =

    let private maxTableRows = 500_000L
    let private maxListedValues = 32
    /// Above this many distinct values the frequent values are not worth reading back.
    let private maxValuesRead = 4096L
    let private maxRemembered = 20_000

    let private cache = ConcurrentDictionary<struct (string * string), Lazy<ColumnStatistics option>>()
    let private ranges = ConcurrentDictionary<struct (string * string), Lazy<(LiteralValue * LiteralValue) option>>()

    let private isNumberLike (t: LogicalType) =
        match t with
        | LogicalType.Arrow(:? Int8Type | :? Int16Type | :? Int32Type | :? Int64Type | :? UInt8Type | :? UInt16Type | :? UInt32Type | :? UInt64Type) -> true
        | LogicalType.Arrow(:? FloatType | :? DoubleType | :? Decimal128Type | :? Date32Type) -> true
        | _ -> false

    let private isText (t: LogicalType) =
        match t with
        | LogicalType.Arrow(:? StringType | :? StringViewType | :? LargeStringType) -> true
        | _ -> false

    let private read (handle: LazyFrameHandle) (column: string) (numeric: bool) (tableRows: int64) : ColumnStatistics option =
        let key =
            if numeric then
                PolarsScalarLowerer.lower (ScalarExpr.Cast(ScalarExpr.Column column, LogicalTypes.Float64, false))
            else
                PolarsWrapper.Col column
        let nonNull =
            PolarsWrapper.LazyFilter(
                PolarsWrapper.LazySelect(PolarsWrapper.LazyClone handle, [| PolarsWrapper.Alias(key, "value") |]),
                PolarsScalarLowerer.lower (ScalarExpr.Unary(UnaryOp.IsNotNull, ScalarExpr.Column "value"))
            )
        let counted =
            PolarsWrapper.LazySelect(
                PolarsWrapper.LazyGroupByAgg(nonNull, [| PolarsWrapper.Col "value" |], [| PolarsWrapper.Alias(PolarsWrapper.Len(), "n") |], null, false),
                [|
                    PolarsWrapper.Col "value"
                    PolarsScalarLowerer.lower (ScalarExpr.Alias(ScalarExpr.Cast(ScalarExpr.Column "n", LogicalTypes.Float64, false), "n"))
                |]
            )
        use groups = PolarsWrapper.LazyCollect(counted, PlEngine.Auto)
        let distinct = PolarsWrapper.DataFrameHeight groups
        use values = PolarsWrapper.DataFrameGetColumn(groups, "value")
        use counts = PolarsWrapper.DataFrameGetColumn(groups, "n")
        let countOf (i: int64) = PolarsWrapper.SeriesGetDoubleFast(counts, i)
        let nonNullRows = Seq.init (int distinct) (fun i -> countOf (int64 i)) |> Seq.sum
        let share rows = rows / float (max 1L tableRows)
        let valueOf (i: int64) =
            if numeric then
                LiteralValue.Float64(PolarsWrapper.SeriesGetDoubleFast(values, i))
            else
                LiteralValue.String(PolarsWrapper.SeriesGetStringFast(values, i))
        let frequent =
            if distinct > maxValuesRead then
                []
            else
                [ for i in 0L .. distinct - 1L -> i ]
                |> List.sortByDescending countOf
                |> List.truncate maxListedValues
                |> List.map (fun i -> valueOf i, share (countOf i))
        let bounds =
            if numeric && distinct > 0L then
                let all = Seq.init (int distinct) (fun i -> PolarsWrapper.SeriesGetDoubleFast(values, int64 i)) |> Array.ofSeq
                Some(LiteralValue.Float64(Array.min all)), Some(LiteralValue.Float64(Array.max all))
            else
                None, None
        Some
            {
                NullFraction = Some(max 0.0 (1.0 - share nonNullRows))
                DistinctCount = Some(float distinct)
                Min = fst bounds
                Max = snd bounds
                FrequentValues = frequent
                Origin = StatisticsOrigin.Exact
            }

    let private readRange (handle: LazyFrameHandle) (column: string) : (LiteralValue * LiteralValue) option =
        let bound (name: string) (aggregate: ExprHandle -> ExprHandle) =
            let value = PolarsScalarLowerer.lower (ScalarExpr.Cast(ScalarExpr.Column column, LogicalTypes.Float64, false))
            PolarsWrapper.Alias(aggregate value, name)
        let present =
            PolarsScalarLowerer.lower (
                ScalarExpr.Alias(ScalarExpr.Cast(ScalarExpr.Aggregation(AggKind.Count, Some(ScalarExpr.Column column), false), LogicalTypes.Float64, false), "present")
            )
        let frame = PolarsWrapper.LazySelect(PolarsWrapper.LazyClone handle, [| bound "low" PolarsWrapper.Min; bound "high" PolarsWrapper.Max; present |])
        use bounds = PolarsWrapper.LazyCollect(frame, PlEngine.Auto)
        use low = PolarsWrapper.DataFrameGetColumn(bounds, "low")
        use high = PolarsWrapper.DataFrameGetColumn(bounds, "high")
        use count = PolarsWrapper.DataFrameGetColumn(bounds, "present")
        let lowest, highest = PolarsWrapper.SeriesGetDoubleFast(low, 0L), PolarsWrapper.SeriesGetDoubleFast(high, 0L)
        // The native getter reads a null as zero, so a column without values must be recognized by its count.
        if PolarsWrapper.SeriesGetDoubleFast(count, 0L) = 0.0 || Double.IsNaN lowest || Double.IsNaN highest then
            None
        else
            Some(LiteralValue.Float64 lowest, LiteralValue.Float64 highest)

    /// The lowest and highest values of a number or date column of a file table, reading that column once. Parquet
    /// statistics would answer this without decoding it, and can replace the read behind the same interface.
    let tryGetRange (handle: LazyFrameHandle) (schema: LogicalSchema) (column: string) : (LiteralValue * LiteralValue) option =
        match schema.Fields |> List.tryFind (fun field -> field.Name = column) with
        | Some field when isNumberLike field.DataType ->
            let identity = PolarsWrapper.Explain(handle, false)
            if not (identity.Contains "SCAN [") then
                None
            else
                if ranges.Count >= maxRemembered then
                    ranges.Clear()
                let entry =
                    ranges.GetOrAdd(
                        struct (identity, column),
                        fun _ ->
                            lazy
                                (try
                                    readRange handle column
                                 with _ ->
                                     None)
                    )
                entry.Value
        | _ -> None

    /// The statistics of a filtered column of a small file table, or None when reading it is not worthwhile.
    let tryGet (handle: LazyFrameHandle) (schema: LogicalSchema) (rows: int64 option) (column: string) : ColumnStatistics option =
        match rows, schema.Fields |> List.tryFind (fun field -> field.Name = column) with
        | Some tableRows, Some field when tableRows <= maxTableRows && (isNumberLike field.DataType || isText field.DataType) ->
            let identity = PolarsWrapper.Explain(handle, false)
            if not (identity.Contains "SCAN [") then
                None
            else
                if cache.Count >= maxRemembered then
                    cache.Clear()
                let entry =
                    cache.GetOrAdd(
                        struct (identity, column),
                        fun _ ->
                            lazy
                                (try
                                    read handle column (isNumberLike field.DataType) tableRows
                                 with _ ->
                                     None)
                    )
                entry.Value
        | _ -> None
