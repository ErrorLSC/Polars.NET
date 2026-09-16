namespace Polars.FSharp.Tests

module UdfLogic =
    open Apache.Arrow

    let intToString (arr: IArrowArray) : IArrowArray =
        match arr with
        | :? Int64Array as i64Arr ->
            let builder = new StringViewArray.Builder()
            for i in 0 .. i64Arr.Length - 1 do
                if i64Arr.IsNull(i) then builder.AppendNull() |> ignore
                else
                    let v = i64Arr.GetValue(i).Value
                    builder.Append $"Value: {v}" |> ignore
            builder.Build() :> IArrowArray

        | :? Int32Array as i32Arr ->
            let builder = new StringViewArray.Builder()
            for i in 0 .. i32Arr.Length - 1 do
                if i32Arr.IsNull i then
                    builder.AppendNull() |> ignore
                else
                    let v = i32Arr.GetValue(i).Value
                    builder.Append $"Value: {v}" |> ignore
            builder.Build() :> IArrowArray
        |_ -> failwith $"Expected Int32Array or Int64Array, but got: {arr.GetType().Name}"

    let alwaysFail (arr: IArrowArray) : IArrowArray =
        failwith "Boom! F# UDF Exploded!"

open Xunit
open Polars.FSharp
open Apache.Arrow
open System
open Polars.NET.Core

type ``UDF Tests`` () =

    [<Fact>]
    [<Trait("UDF","Basic")>]
    member _.``Map UDF can change data type (Int -> String)`` () =
        use csv = new TempCsv "num\n100\n200"
        let lf = LazyFrame.ScanCsv csv.Path

        let udf = UdfLogic.intToString

        let df =
            lf
            |> LazyFrame.withColumn (
                pl.col("num").MapArrow udf
                |> pl.alias "desc"
            )
            |> LazyFrame.select [ pl.col "desc" ]
            |> LazyFrame.collect

        let arrowBatch = df.ToArrow()
        let strCol = arrowBatch.Column "desc" :?> StringViewArray

        Assert.Equal("Value: 100", strCol.GetString 0)
        Assert.Equal("Value: 200", strCol.GetString 1)

    [<Fact>]
    member _.``Map UDF error is propagated to F#`` () =
        use csv = new TempCsv "num\n1"
        let lf = LazyFrame.ScanCsv csv.Path

        let udf = UdfLogic.alwaysFail

        let ex = Assert.Throws<PolarsException>(fun () ->
            lf
            |> LazyFrame.withColumn (
                pl.col("num").MapArrow(udf, DataType.SameAsInput)
            )
            |> LazyFrame.collect
            |> ignore
        )

        Assert.Contains("Boom! F# UDF Exploded!", ex.Message)
        Assert.Contains("C# UDF Failed", ex.Message)

    [<Fact>]
    member _.``Generic Map UDF with Lambda (Int -> String)`` () =
        use csv = new TempCsv "num\n100\n"
        let lf = LazyFrame.ScanCsv csv.Path

        let myLogic = fun (x: int) -> sprintf "Num: %d" (x + 1)

        let df =
            lf
            |> LazyFrame.withColumn (
                pl.col("num").Map myLogic
                |> pl.alias "res"
            )
            |> LazyFrame.select [ pl.col "res" ]
            |> LazyFrame.collect

        let arrow = df.ToArrow()
        let col = arrow.Column "res" :?> StringViewArray

        Assert.Equal("Num: 101", col.GetString 0)
        Assert.Equal(1, col.Length)
    [<Fact>]
    member _.``UDF: Map with Option (Null Handling)`` () =
        // [10, 20, null]
        use csv = new TempCsv "val\n10\n20\n"
        let lf = LazyFrame.ScanCsv csv.Path

        let logic (opt: int option) =
            match opt with
            | Some x when x > 15 -> Some (x * 2)
            | _ -> None

        let df =
            lf
            |> LazyFrame.withColumn (
                pl.col("val").MapOption logic
                |> pl.alias "res"
            )
            |> LazyFrame.collect

        let arrow = df.ToArrow()
        let col = arrow.Column "res" :?> Int32Array

        // Row 0: 10 -> (<=15) -> None
        Assert.True(col.IsNull 0)

        // Row 1: 20 -> (>15) -> 40
        Assert.Equal(40, col.GetValue(1).Value)

        // Row 2: null -> (None) -> None
        Assert.True(col.IsNull 2)
    [<Fact>]
    [<Trait("UDF","SeriesValueOption")>]
    member _.``Series UDF: MapValueOption (Score to Risk Grade)`` () =

        let data = [
            {| Score = ValueSome 95 |}
            {| Score = ValueSome 75 |}
            {| Score = ValueSome 30 |}
            {| Score = ValueSome 999 |}
            {| Score = ValueNone |}
        ]

        use df = DataFrame.ofRecords data

        let scoreSeries = df.Column "Score"

        let calculateGrade (opt: int voption): string voption =
            match opt with
            | ValueSome s when s >= 90 && s <= 100 -> ValueSome "S"
            | ValueSome s when s >= 70 && s < 90   -> ValueSome "A"
            | ValueSome s when s >= 0  && s < 70   -> ValueSome "B"
            | _ -> ValueNone

        use gradeSeries = scoreSeries |> Series.mapValueOption calculateGrade

        // MapValueOption(calculateGrade, DataType.String)

        use resultDf = df.WithColumns(pl.litSeries(gradeSeries).Alias "Grade")

        Assert.Equal(5L, resultDf.Height)

        Assert.Equal("S", resultDf.Cell<string>("Grade", 0))
        Assert.Equal("A", resultDf.Cell<string>("Grade", 1))
        Assert.Equal("B", resultDf.Cell<string>("Grade", 2))

        Assert.Null(resultDf.Cell<string>("Grade", 3))

        Assert.Null(resultDf.Cell<string>("Grade", 4))
    [<Fact>]
    member _.``UDF: Decimal Map (Series Native)`` () =
        // String ["10.50", "20.25", null]
        let data = ["10.50"; "20.25"; null]
        let s = Series.create("str_vals", data)

        // Cast to Decimal(10, 2)
        let sDec = s |> Series.cast(DataType.Decimal(10, 2))

        let logic (opt: decimal option) =
            opt |> Option.map (fun d -> d * 2m)

        let res = sDec |> Series.mapOption logic

        // 10.50 * 2 = 21.00
        Assert.Equal(21.00m, res.GetValue<decimal> 0)

        // 20.25 * 2 = 40.50
        Assert.Equal(40.50m, res.GetValue<decimal> 1)

        // null -> null
        Assert.True(res.GetValue<decimal option>(2).IsNone)
    [<Fact>]
    member _.``Series: Map (Basic UDF)`` () =
        // Data: [1, 2, 3]
        let s = Series.create("nums", [1; 2; 3])

        // Apply to Series
        let sRes = s |> Series.map (fun x -> x * 10)

        // Verify
        Assert.Equal(10, sRes.GetValue<int> 0)
        Assert.Equal(30, sRes.GetValue<int> 2)

    [<Fact>]
    [<Trait("UDF", "Option")>]
    member _.``Series: Map (Option Handling)`` () =
        // Data: [10, null, 30]
        let s = Series.create("vals", [Some 10; None; Some 30])

        // Logic: Some(x) -> Some(x + 1), None -> Some(-1)
        let logic (opt: int option) =
            match opt with
            | Some x -> Some (x + 1)
            | None -> Some -1

        // Apply
        let sRes = s |> Series.mapOption logic

        // Verify
        Assert.Equal(11, sRes.GetValue<int> 0)
        Assert.Equal(-1, sRes.GetValue<int> 1) // None became -1

    [<Fact>]
    member _.``Series: Map (F# Lambda Sugar)`` () =
        // Data: ["a", "b"]
        let s = Series.create("txt", ["a"; "b"])

        let sRes = s |> Series.map (fun x -> x + "_suffix")

        Assert.Equal("a_suffix", sRes.GetValue<string> 0)
    [<Fact>]
    [<Trait("UDF","ValueOption")>]
    member _.``UDF: Map with ValueOption (String Parsing to Int)`` () =
        let data = [
            {| Code = ValueSome "EMP-1024" |}
            {| Code = ValueSome "EMP-0042" |}
            {| Code = ValueSome "ADMIN-1" |}
            {| Code = ValueSome "EMP-ERR" |}
            {| Code = ValueNone |}
        ]

        let lf = DataFrame.ofRecords(data).Lazy()

        //  string voption -> int voption
        let parseEmpId (opt: string voption) =
            match opt with
            | ValueSome s when s.StartsWith "EMP-" ->
                match Int32.TryParse(s.Substring 4) with
                | true, num -> ValueSome num
                | _ -> ValueNone
            | _ -> ValueNone

        let df =
            lf
            |> LazyFrame.withColumn (
                pl.col "Code"
                |> fun e -> e.MapValueOption parseEmpId
                |> pl.alias "EmpId"
            )
            |> LazyFrame.collect
        // shape: (5, 2)
        // ┌──────────┬───────┐
        // │ Code     ┆ EmpId │
        // │ ---      ┆ ---   │
        // │ str      ┆ i32   │
        // ╞══════════╪═══════╡
        // │ EMP-1024 ┆ 1024  │
        // │ EMP-0042 ┆ 42    │
        // │ ADMIN-1  ┆ null  │
        // │ EMP-ERR  ┆ null  │
        // │ null     ┆ null  │
        // └──────────┴───────┘
        Assert.Equal(5L, df.Height)

        // Row 0: "EMP-1024" -> 1024
        Assert.Equal(1024, df.Cell<int>("EmpId", 0))

        // Row 1: "EMP-0042" -> 42
        Assert.Equal(42, df.Cell<int>("EmpId", 1))

        // Row 2: "ADMIN-1" -> ValueNone -> null
        Assert.False(df.Cell<Nullable<int>>("EmpId", 2).HasValue)

        // Row 3: "EMP-ERR" -> TryParse Fail -> ValueNone -> null
        Assert.False(df.Cell<Nullable<int>>("EmpId", 3).HasValue)

        // Row 4: null -> ValueNone -> null
        Assert.False(df.Cell<Nullable<int>>("EmpId", 4).HasValue)

    [<Fact>]
    [<Trait("UDF","Mapi")>]
    member _.``Series.mapi transforms elements using index and value`` () =
        // Arrange
        let s = pl.series "data" [| 10; 20; 30; 40 |]

        // Act: element + index (10+0, 20+1, 30+2, 40+3)
        let mapped = s |> Series.mapi (fun i v -> v + i)

        // Assert
        Assert.Equal(4L, mapped.Length)
        Assert.Equal<int>(10, mapped.GetValue<int>(0))
        Assert.Equal<int>(21, mapped.GetValue<int>(1))
        Assert.Equal<int>(32, mapped.GetValue<int>(2))
        Assert.Equal<int>(43, mapped.GetValue<int>(3))

    [<Fact>]
    [<Trait("UDF","Mapi")>]
    member _.``Series.mapi allows changing data type based on index`` () =
        // Arrange: int series to formatted string labels
        let s = pl.series "items" [| 100; 200; 300 |]

        // Act
        let labels = s |> Series.mapi (fun i v -> sprintf "#%d: %d" (i + 1) v)

        // Assert
        Assert.Equal(3L, labels.Length)
        Assert.Equal<string>("#1: 100", labels.GetValue<string>(0))
        Assert.Equal<string>("#2: 200", labels.GetValue<string>(1))
        Assert.Equal<string>("#3: 300", labels.GetValue<string>(2))
    [<Fact>]
    [<Trait("UDF", "Mapi")>]
    member _.``Series.mapiOption correctly processes and emits null values`` () =
        // Arrange: [Some 10; None; Some 30]
        let s = pl.series "input" [| Some 10; None; Some 30 |]

        // Act:
        // Index 0: 10 + 0 = 10 -> "Item_0_10"
        // Index 1: None -> None (Preserve null)
        // Index 2: 30 + 2 = 32 -> "Item_2_32"
        let mapped =
            s |> Series.mapiOption (fun i opt ->
                match opt with
                | Some v -> Some (sprintf "Item_%d_%d" i (v + i))
                | None -> None
            )

        // Assert
        Assert.Equal(3L, mapped.Length)
        Assert.False(mapped.IsNullAt 0L)
        Assert.Equal<string>("Item_0_10", mapped.GetValue<string>(0L))

        Assert.True(mapped.IsNullAt 1L)

        Assert.False(mapped.IsNullAt 2L)
        Assert.Equal<string>("Item_2_32", mapped.GetValue<string>(2L))

    [<Fact>]
    [<Trait("UDF", "Mapi")>]
    member _.``Series.mapiValueOption correctly handles ValueOption with zero heap overhead`` () =
        // Arrange: [Some 100.0; None; Some 300.0]
        let s = pl.series "weights" [| ValueSome 100.0; ValueNone; ValueSome 300.0 |]

        // Act: Fill missing with index default, scale existing by index
        let result =
            s |> Series.mapiValueOption (fun i vopt ->
                match vopt with
                | ValueSome v -> ValueSome (v * float (i + 1))
                | ValueNone -> ValueSome (float (i * 999)) // Impute on the fly
            )

        // Assert
        Assert.Equal(3L, result.Length)
        Assert.False(result.IsNullAt 0L)
        Assert.Equal(100.0, result.GetValue<double>(0L))  // 100.0 * 1

        Assert.False(result.IsNullAt 1L)
        Assert.Equal(999.0, result.GetValue<double>(1L))  // Imputed: 1 * 999

        Assert.False(result.IsNullAt 2L)
        Assert.Equal(900.0, result.GetValue<double>(2L))  // 300.0 * 3
    [<Fact>]
    [<Trait("UDF", "Map2")>]
    member _.``Series.map2 element-wise combines two Series matching Array.map2 behavior`` () =
        // Arrange
        let s1 = pl.series "prices" [| 100.0; 200.0; 300.0 |]
        let s2 = pl.series "discounts" [| 0.1; 0.2; 0.05 |]

        // Act: (s1, s2) ||> Series.map2 f
        let netPrices =
            (s1, s2)
            ||> Series.map2 (fun price discount -> price * (1.0 - discount))

        // Assert
        Assert.Equal(3L, netPrices.Length)
        Assert.Equal(90.0, netPrices.GetValue<double>(0))
        Assert.Equal(160.0, netPrices.GetValue<double>(1))
        Assert.Equal(285.0, netPrices.GetValue<double>(2))

    [<Fact>]
    [<Trait("UDF", "Map2")>]
    member _.``Series.mapi2 includes 0-based row index during pairwise mapping`` () =
        // Arrange
        let s1 = pl.series "prefix" [| "Item"; "Item"; "Item" |]
        let s2 = pl.series "sku" [| 101; 202; 303 |]

        // Act: format like "0: Item_101"
        let formatted =
            (s1, s2)
            ||> Series.mapi2 (fun i pref sku -> sprintf "%d: %s_%d" i pref sku)

        // Assert
        Assert.Equal(3L, formatted.Length)
        Assert.Equal<string>("0: Item_101", formatted.GetValue<string>(0))
        Assert.Equal<string>("1: Item_202", formatted.GetValue<string>(1))
        Assert.Equal<string>("2: Item_303", formatted.GetValue<string>(2))

    [<Fact>]
    [<Trait("UDF", "Map2")>]
    member _.``Series.map2 throws ArgumentException when lengths mismatch`` () =
        // Arrange
        let s1 = pl.series "short" [| 1; 2 |]
        let s2 = pl.series "long" [| 1; 2; 3 |]

        // Act & Assert
        Assert.Throws<ArgumentException>(fun () ->
            (s1, s2) ||> Series.map2 (+) |> ignore
        ) |> ignore
