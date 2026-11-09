namespace Polars.FSharp.Tests

open System
open System.IO
open Apache.Arrow.Types
open Xunit
open Polars.FSharp
open Polars.NET.Core.Wrappers
open Polars.NET.Linq.IR
open Polars.NET.Linq.IR.ScalarExprOps
open Polars.NET.Linq.IR.Passes
open Polars.NET.Linq.Backend.Polars

/// The estimator's use of column statistics, and the statistics read from a file.
module ColumnStatisticsTests =

    let private schema = LogicalSchema.OfFields [ ("country", LogicalTypes.String); ("year", LogicalTypes.Int64); ("note", LogicalTypes.String); ("price", LogicalTypes.Float64) ]
    let private table = LogicalPlan.Scan(LogicalPlanSource.Slot 0, schema)

    let private statistics frequent distinct low high nulls : ColumnStatistics =
        {
            NullFraction = nulls
            DistinctCount = distinct
            Min = low
            Max = high
            FrequentValues = frequent
            Histogram = None
            Origin = StatisticsOrigin.Exact
        }

    let private columns =
        Map.ofList
            [
                "country", statistics [ LiteralValue.String "US", 0.97 ] (Some 2.0) None None None
                "year", statistics [] (Some 200.0) (Some(LiteralValue.Float64 1900.0)) (Some(LiteralValue.Float64 2100.0)) (Some 0.1)
                "price",
                { statistics [] (Some 100.0) (Some(LiteralValue.Float64 0.0)) (Some(LiteralValue.Float64 100.0)) (Some 0.0) with
                    Histogram = Some [| 0.0; 1.0; 2.0; 3.0; 100.0 |] }
            ]

    let private estimates: JoinReordering.SourceEstimates = { Rows = (fun _ -> 1000.0); Column = (fun _ column -> columns.TryFind column); KeyRange = (fun _ _ -> None); Observed = (fun _ -> None) }
    let private rowsOf plan = fst (JoinReordering.estimate estimates plan)
    let private equals column value = ScalarExpr.Binary(BinaryOp.Eq, col column, value)
    let private close (expected: float) (actual: float) = Assert.Equal(expected, actual, 6)

    [<Fact>]
    let ``A frequent value keeps its share`` () = close 970.0 (rowsOf (LogicalPlan.Filter(table, equals "country" (lit "US"))))

    [<Fact>]
    let ``Other values share what the frequent ones leave`` () = close 30.0 (rowsOf (LogicalPlan.Filter(table, equals "country" (lit "CA"))))

    [<Fact>]
    let ``A range over two filters keeps its width of the column's span`` () =
        let lower = LogicalPlan.Filter(table, ScalarExpr.Binary(BinaryOp.GtEq, col "year", lit 2000L))
        close 45.0 (rowsOf (LogicalPlan.Filter(lower, ScalarExpr.Binary(BinaryOp.LtEq, col "year", lit 2010L))))

    [<Fact>]
    let ``A range written with the literals first counts the same`` () =
        let lower = LogicalPlan.Filter(table, ScalarExpr.Binary(BinaryOp.LtEq, lit 2000L, col "year"))
        close 45.0 (rowsOf (LogicalPlan.Filter(lower, ScalarExpr.Binary(BinaryOp.GtEq, lit 2010L, col "year"))))

    [<Fact>]
    let ``A histogram follows a skewed column`` () =
        let lower = LogicalPlan.Filter(table, ScalarExpr.Binary(BinaryOp.GtEq, col "price", lit 1.0))
        close 250.0 (rowsOf (LogicalPlan.Filter(lower, ScalarExpr.Binary(BinaryOp.LtEq, col "price", lit 2.0))))

    [<Fact>]
    let ``A histogram interpolates inside a bucket`` () =
        close 125.0 (rowsOf (LogicalPlan.Filter(table, ScalarExpr.Binary(BinaryOp.And, ScalarExpr.Binary(BinaryOp.GtEq, col "price", lit 1.0), ScalarExpr.Binary(BinaryOp.LtEq, col "price", lit 1.5)))))

    [<Fact>]
    let ``Null checks read the null fraction`` () = close 100.0 (rowsOf (LogicalPlan.Filter(table, ScalarExpr.Unary(UnaryOp.IsNull, col "year"))))

    [<Fact>]
    let ``A column without statistics keeps the fixed selectivity`` () = close 100.0 (rowsOf (LogicalPlan.Filter(table, equals "note" (lit "x"))))

    [<Fact>]
    let ``Statistics are read from a CSV file`` () =
        let path = Path.Combine(Path.GetTempPath(), $"column-statistics-{Guid.NewGuid():N}.csv")
        File.WriteAllLines(path, [ "name,age,day"; "a,10,2020-01-01"; "a,20,2020-01-02"; "a,,2020-01-03"; "b,30,2020-01-05" ])
        try
            let lf = LazyFrame.ScanCsv(path, tryParseDates = true)
            let fields = LogicalSchema.OfFields [ ("name", LogicalTypes.String); ("age", LogicalTypes.Int64); ("day", LogicalType.Arrow Date32Type.Default) ]
            let read column = (PolarsColumnStatistics.tryGet lf.Handle fields (Some 4L) column).Value

            let name = read "name"
            Assert.Equal(Some 2.0, name.DistinctCount)
            Assert.Equal<(LiteralValue * float) list>([ LiteralValue.String "a", 0.75; LiteralValue.String "b", 0.25 ], name.FrequentValues)

            let age = read "age"
            Assert.Equal(Some 0.25, age.NullFraction)
            Assert.Equal(Some(LiteralValue.Float64 10.0), age.Min)
            Assert.Equal(Some(LiteralValue.Float64 30.0), age.Max)
            let boundaries = age.Histogram.Value
            Assert.Equal(10.0, boundaries.[0])
            Assert.Equal(30.0, boundaries.[boundaries.Length - 1])
            Assert.True(boundaries |> Array.pairwise |> Array.forall (fun (a, b) -> a <= b))

            let epochDays = float (DateOnly(2020, 1, 1).DayNumber - DateOnly(1970, 1, 1).DayNumber)
            Assert.Equal(Some(LiteralValue.Float64 epochDays), (read "day").Min)
        finally
            File.Delete path

    [<Fact>]
    let ``A column's range is read from a CSV file`` () =
        let path = Path.Combine(Path.GetTempPath(), $"column-statistics-{Guid.NewGuid():N}.csv")
        File.WriteAllLines(path, [ "name,age,empty"; "a,10,"; "b,,"; "c,30," ])
        try
            let lf = LazyFrame.ScanCsv(path)
            let fields = LogicalSchema.OfFields [ ("name", LogicalTypes.String); ("age", LogicalTypes.Int64); ("empty", LogicalTypes.Int64) ]
            let range column = PolarsColumnStatistics.tryGetRange lf.Handle fields column

            Assert.Equal(Some(LiteralValue.Float64 10.0, LiteralValue.Float64 30.0), range "age")
            Assert.Equal(None, range "name")
            Assert.Equal(None, range "empty")
        finally
            File.Delete path

    [<Fact>]
    let ``A Parquet column's range comes from the footer`` () =
        let csv = Path.Combine(Path.GetTempPath(), $"column-statistics-{Guid.NewGuid():N}.csv")
        let parquet = Path.ChangeExtension(csv, "parquet")
        File.WriteAllLines(csv, [ "age,day,name"; "10,2020-01-01,a"; ",2020-01-02,b"; "30,2020-01-05,c" ])
        try
            LazyFrame.ScanCsv(csv, tryParseDates = true).Collect().WriteParquet(parquet, statistics = true) |> ignore
            let lf = LazyFrame.ScanParquet(parquet)
            let fields = LogicalSchema.OfFields [ ("age", LogicalTypes.Int64); ("day", LogicalType.Arrow Date32Type.Default); ("name", LogicalTypes.String) ]
            let epochDays (year, month, day) = float (DateOnly(year, month, day).DayNumber - DateOnly(1970, 1, 1).DayNumber)

            let struct (low, high) = PolarsWrapper.LazyParquetIntRange(lf.Handle, "age").Value
            Assert.Equal((10L, 30L), (low, high))
            Assert.False(PolarsWrapper.LazyParquetIntRange(lf.Handle, "name").HasValue)
            Assert.False(PolarsWrapper.LazyParquetIntRange(lf.Handle, "missing").HasValue)
            Assert.Equal(Some(LiteralValue.Float64 10.0, LiteralValue.Float64 30.0), PolarsColumnStatistics.tryGetRange lf.Handle fields "age")
            Assert.Equal(
                Some(LiteralValue.Float64(epochDays (2020, 1, 1)), LiteralValue.Float64(epochDays (2020, 1, 5))),
                PolarsColumnStatistics.tryGetRange lf.Handle fields "day")
            Assert.False(PolarsWrapper.LazyParquetIntRange(LazyFrame.ScanCsv(csv).Handle, "age").HasValue)
        finally
            File.Delete csv
            File.Delete parquet

    [<Fact>]
    let ``A Parquet file without statistics falls back to reading the column`` () =
        let csv = Path.Combine(Path.GetTempPath(), $"column-statistics-{Guid.NewGuid():N}.csv")
        let parquet = Path.ChangeExtension(csv, "parquet")
        File.WriteAllLines(csv, [ "age"; "10"; "30" ])
        try
            LazyFrame.ScanCsv(csv).Collect().WriteParquet(parquet, statistics = false) |> ignore
            let lf = LazyFrame.ScanParquet(parquet)
            let fields = LogicalSchema.OfFields [ ("age", LogicalTypes.Int64) ]

            Assert.False(PolarsWrapper.LazyParquetIntRange(lf.Handle, "age").HasValue)
            Assert.Equal(Some(LiteralValue.Float64 10.0, LiteralValue.Float64 30.0), PolarsColumnStatistics.tryGetRange lf.Handle fields "age")
        finally
            File.Delete csv
            File.Delete parquet

    [<Fact>]
    let ``A table too large to read has no statistics`` () =
        let path = Path.Combine(Path.GetTempPath(), $"column-statistics-{Guid.NewGuid():N}.csv")
        File.WriteAllLines(path, [ "name"; "a" ])
        try
            let lf = LazyFrame.ScanCsv(path)
            let fields = LogicalSchema.OfFields [ ("name", LogicalTypes.String) ]
            Assert.Equal(None, PolarsColumnStatistics.tryGet lf.Handle fields (Some 10_000_000L) "name")
        finally
            File.Delete path
