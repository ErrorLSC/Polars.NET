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
    member _.``Map UDF can change data type (Int -> String)`` () =
        use csv = new TempCsv "num\n100\n200"
        let lf = LazyFrame.ScanCsv csv.Path
        
        let udf = Func<IArrowArray, IArrowArray> UdfLogic.intToString

        let df = 
            lf 
            |> pl.withColumnLazy (
                pl.col "num"
                |> fun e -> e.Map(udf, DataType.String)
                |> pl.alias "desc"
            )
            |> pl.selectLazy [ pl.col "desc" ]
            |> pl.collect

        let arrowBatch = df.ToArrow()
        let strCol = arrowBatch.Column "desc" :?> StringViewArray
        
        Assert.Equal("Value: 100", strCol.GetString 0)
        Assert.Equal("Value: 200", strCol.GetString 1)

    [<Fact>]
    member _.``Map UDF error is propagated to F#`` () =
        use csv = new TempCsv "num\n1"
        let lf = LazyFrame.ScanCsv csv.Path
        
        let udf = Func<IArrowArray, IArrowArray> UdfLogic.alwaysFail

        let ex = Assert.Throws<PolarsException>(fun () -> 
            lf 
            |> pl.withColumnLazy (
                pl.col "num" 
                |> fun e -> e.Map(udf, DataType.SameAsInput)
            )
            |> pl.collect 
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
            |> pl.withColumnLazy (
                pl.col "num"

                |> fun e -> e.Map(Udf.map myLogic, DataType.String) 
                |> pl.alias "res"
            )
            |> pl.selectLazy [ pl.col "res" ]
            |> pl.collect

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
            |> pl.withColumnLazy (
                pl.col "val"
                |> fun e -> e.Map(Udf.mapOption logic, DataType.Int32)
                |> pl.alias "res"
            )
            |> pl.collect

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

        let calculateGrade (opt: int voption) =
            match opt with
            | ValueSome s when s >= 90 && s <= 100 -> ValueSome "S"
            | ValueSome s when s >= 70 && s < 90   -> ValueSome "A"
            | ValueSome s when s >= 0  && s < 70   -> ValueSome "B"
            | _ -> ValueNone 

        use gradeSeries = scoreSeries.MapValueOption(calculateGrade, DataType.String)
        
        use resultDf = df.WithColumn(pl.litSeries(gradeSeries).Alias "Grade")
        
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
        let sDec = s.Cast(DataType.Decimal(10, 2))

        let logic (opt: decimal option) =
            opt |> Option.map (fun d -> d * 2m)

        let res = sDec.MapOption(logic, DataType.Decimal(10, 2))

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

        // Logic: x * 10
        let doubleFunc (x: int) = x * 10
        
        // int -> int
        let udf = Udf.map doubleFunc

        // Apply to Series
        let sRes = s.Map(udf, DataType.Int32)

        // Verify
        Assert.Equal(10, sRes.GetValue<int> 0)
        Assert.Equal(30, sRes.GetValue<int> 2)

    [<Fact>]
    member _.``Series: Map (Option Handling)`` () =
        // Data: [10, null, 30]
        let s = Series.create("vals", [Some 10; None; Some 30])

        // Logic: Some(x) -> Some(x + 1), None -> Some(-1)
        let logic (opt: int option) =
            match opt with
            | Some x -> Some (x + 1)
            | None -> Some -1

        // Apply
        let sRes = s.Map(Udf.mapOption logic, DataType.Int32)

        // Verify
        Assert.Equal(11, sRes.GetValue<int> 0)
        Assert.Equal(-1, sRes.GetValue<int> 1) // None became -1

    [<Fact>]
    member _.``Series: Map (F# Lambda Sugar)`` () =
        // Data: ["a", "b"]
        let s = Series.create("txt", ["a"; "b"])

        let sRes = s.Map<string, string>((fun x -> x + "_suffix"), DataType.String)
        
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
            |> pl.withColumnLazy (
                pl.col "Code"
                |> fun e -> e.Map(Udf.mapValueOption parseEmpId, DataType.Int32)
                |> pl.alias "EmpId"
            )
            |> pl.collect
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