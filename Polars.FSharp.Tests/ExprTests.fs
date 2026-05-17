namespace Polars.FSharp.Tests

open System
open Xunit
open Polars.FSharp

type ``Expression Logic Tests`` () =
    [<Fact>]
        member _.``Select inline style (Pythonic)`` () =
            use csv = new TempCsv "name,birthdate,weight,height\nQinglei,2025-11-25,70,1.80"
            let df = DataFrame.ReadCsv (path=csv.Path,tryParseDates=true) 

            let res = 
                df
                |> pl.select [
                    col "name"
                    
                    col "birthdate" |> alias "b_date"
                    
                    (col "birthdate").Dt.Year().Alias "year"
                    
                    col "weight" / (col "height" * col "height")
                    |> alias "bmi"
                ]

            Assert.Equal(4L, res.Width) // name, b_date, year, bmi

            // Qinglei
            Assert.Equal("Qinglei", res.String("name", 0).Value) 
            // BMI ≈ 21.6
            Assert.True(res.Float("bmi", 0).Value > 21.6)
    [<Fact>]
    member _.``Filter by numeric value (> operator)`` () =
        use csv = new TempCsv "val\n10\n20\n30"
        let df = DataFrame.ReadCsv csv.Path
        
        let res = df |> pl.filter (col "val" .> lit 15)
        
        Assert.Equal(2L, res.Rows)
    [<Fact>]
    member _.``Filter by numeric value (< operator)`` () =
        use csv = new TempCsv "name,birthdate,weight,height\nBen Brown,1985-02-15,72.5,1.77\nQinglei,2025-11-25,70.0,1.80\nZhang,2025-10-31,55,1.75"
        let df = DataFrame.ReadCsv (path=csv.Path,tryParseDates=true)

        let res = df |> pl.filter ((col "birthdate").Dt.Year() .< lit 1990 )

        Assert.Equal(1L,res.Rows)

    [<Fact>]
    member _.``Filter by string value (== operator)`` () =
        use csv = new TempCsv "name\nAlice\nBob\nAlice"
        let df = DataFrame.ReadCsv csv.Path
        
        // SRTP 魔法测试
        let res = df |> pl.filter (pl.col "name" .== pl.lit "Alice")
        
        Assert.Equal(2L, res.Rows)

    [<Fact>]
    member _.``Filter by double value (== operator)`` () =
        use csv = new TempCsv "value\n3.36\n4.2\n5\n3.36"
        let df = DataFrame.ReadCsv csv.Path
        
        let res = df |> pl.filter (col "value" .== lit 3.36)
        
        Assert.Equal(2L, res.Rows)

    [<Fact>]
    member _.``Null handling works`` () =
        // age: 10, null, 30
        use csv = new TempCsv "age\n10\n\n30" 
        let lf = LazyFrame.ScanCsv csv.Path

        let res = 
            lf 
            |> pl.withColumnLazy (
                col "age" 
                |> pl.fillNull (pl.lit 0) 
                |> pl.alias "age_filled"
            )
            |> pl.filterLazy (col "age_filled" .>= lit 0)
            |> pl.collect
        Assert.Equal(3L, res.Rows)

        let df= DataFrame.ReadCsv csv.Path 
        let nulls = df |> pl.filter (pl.col "age" |> pl.isNull)
        Assert.Equal(1L, nulls.Rows)
    [<Fact>]
    member _.``IsBetween with DateTime Literals`` () =

        use csv = new TempCsv "name,birthdate,height\nQinglei,1990-05-20,1.80\nTooOld,1980-01-01,1.80\nTooShort,1990-05-20,1.60"
        
        let df = DataFrame.ReadCsv (path=csv.Path,tryParseDates=true)

        // filter(
        //    col("birthdate").is_between(date(1982,12,31), date(1996,1,1)),
        //    col("height") > 1.7
        // )
        
        let startDt = DateTime(1982, 12, 31)
        let endDt = DateTime(1996, 1, 1)

        let res = 
            df 
            |> pl.filter (
                (pl.col "birthdate").IsBetween(pl.lit startDt, pl.lit endDt)
                .&&
                (pl.col "height" .> pl.lit 1.7)
            )

        Assert.Equal(1L, res.Rows)
        Assert.Equal("Qinglei", res.String("name", 0).Value)
    [<Fact>]
    member _.``Expr: DateTime Ops (Truncate, Offset, Timestamp)`` () =
        // ["2023-01-01 10:15:00", "2023-01-01 10:45:00"]
        let s = Series.create("ts", ["2023-01-01 10:15:00"; "2023-01-01 10:45:00"])
        use df_origin = DataFrame.create [s]
        let df =
            df_origin 
            |> pl.select([
            pl.col("ts").Str.ToDatetime("%Y-%m-%d %H:%M:%S").Alias "ts"
            ])

        let res = 
            df
            |> pl.select([
                pl.col "ts"

                // Truncate to 1 hour (10:15 -> 10:00)
                pl.col("ts").Dt.Truncate(Dur.String "1h").Alias "truncated"

                // Round to 1 hour (10:45 -> 11:00)
                pl.col("ts").Dt.Round(Dur.String "1h").Alias "rounded"

                // Offset by 30m (10:15 -> 10:45)
                pl.col("ts").Dt.OffsetBy(Dur.String "30m").Alias "offset"

                // Timestamp (Micros)
                pl.col("ts").Dt.TimestampMicros().Alias "micros"
            ])
            |> pl.show
        
        // Truncate: 10:15 -> 10:00
        let t0 = res.DateTime("truncated", 0).Value
        Assert.Equal(10, t0.Hour)
        Assert.Equal(0, t0.Minute)

        // Round: 10:45 (Row 1) -> 11:00
        let r1 = res.DateTime("rounded", 1).Value
        Assert.Equal(11, r1.Hour)
        Assert.Equal(0, r1.Minute)

        // Offset: 10:15 -> 10:45
        let o0 = res.DateTime("offset", 0).Value
        Assert.Equal(10, o0.Hour)
        Assert.Equal(45, o0.Minute)
        
        // Timestamp should be > 0
        Assert.True(res.Int("micros", 0).Value > 0L)

type ``String Logic Tests`` () =

    [<Fact>]
    member _.``Expr: String Cleaning & Parsing (Strip, Anchor, Date)`` () =

        let s = Series.create("raw", [
            "  abc  "           // 0
            "https://pl.rs" // 1
            "data.csv"          // 2
            "__key__"           // 3
            "20250101"          // 4
            "  2025-12-31  "    // 5
        ])
        
        use df = DataFrame.create [s]

        let res = 
            df
            |> pl.select([
                pl.col "raw"

                // "  abc  " -> "abc"
                pl.col("raw").Str.Strip().Alias "strip_default"
                
                // "  abc  " -> "abc  " / "  abc"
                pl.col("raw").Str.LStrip().Alias "lstrip"
                pl.col("raw").Str.RStrip().Alias "rstrip"

                // "__key__" -> "key"
                pl.col("raw").Str.Strip(matches="_").Alias "strip_custom"

                // "https://pl.rs" -> "pl.rs"
                // "data.csv" -> "data"
                pl.col("raw").Str.StripPrefix("https://").Alias "strip_prefix"
                pl.col("raw").Str.StripSuffix(".csv").Alias "strip_suffix"

                //  Anchors (StartsWith / EndsWith) -> Boolean
                pl.col("raw").Str.StartsWith("https").Alias "is_url"
                pl.col("raw").Str.EndsWith(".csv").Alias "is_csv"

                // ToDate
                // "20250101" -> Date
                pl.col("raw").Str.ToDate("%Y%m%d",false).Alias "parsed_date"

                // "  2025-12-31  " -> "2025-12-31" -> Date
                pl.col("raw").Str.Strip().Str.ToDate("%Y-%m-%d",false).Alias "chain_date"
            ])

        // Strip
        Assert.Equal("abc", res.String("strip_default", 0).Value)
        Assert.Equal("abc  ", res.String("lstrip", 0).Value)
        Assert.Equal("  abc", res.String("rstrip", 0).Value)

        // Custom Strip
        Assert.Equal("key", res.String("strip_custom", 3).Value) // __key__ -> key

        // Prefix / Suffix
        Assert.Equal("pl.rs", res.String("strip_prefix", 1).Value)
        Assert.Equal("data", res.String("strip_suffix", 2).Value) // data.csv -> data

        // Anchors (Boolean)
        Assert.Equal(Some true, res.Bool("is_url", 1)) // https://...
        Assert.Equal(Some false, res.Bool("is_url", 0))
        Assert.Equal(Some true, res.Bool("is_csv", 2)) // ...csv

        // ToDate
        // Row 4: "20250101"
        let d1 = res.Date("parsed_date", 4).Value
        Assert.Equal(2025, d1.Year)
        Assert.Equal(1, d1.Month)
        Assert.Equal(1, d1.Day)

        // ToDate
        // Row 0: "  abc  " parsed as null
        Assert.True(res.IsNullAt("parsed_date", 0))

        // Strip + ToDate)
        // Row 5: "  2025-12-31  "
        let d2 = res.Date("chain_date", 5).Value
        Assert.Equal(2025, d2.Year)
        Assert.Equal(12, d2.Month)
        Assert.Equal(31, d2.Day)

        Assert.Equal(DataType.Date, res.Schema.["parsed_date"])
    [<Fact>]
    member _.``Math Ops (BMI Calculation with Pow)`` () =
        use csv = new TempCsv "name,height,weight\nAlice,1.65,60\nBob,1.80,80"
        let df = DataFrame.ReadCsv csv.Path

        // weight / (height ^ 2)
        let bmiExpr = 
            pl.col "weight" / pl.col "height" .** pl.lit 2
            |> pl.alias "bmi"

        let res = 
            df 
            |> pl.select [
                pl.col "name"
                bmiExpr
                (pl.col "height").Sqrt().Alias "sqrt_h"
            ]

        let bobBmi = res.Float("bmi", 1).Value
        Assert.True(bobBmi > 24.69 && bobBmi < 24.70)

        let aliceSqrt = res.Float("sqrt_h", 0).Value
        Assert.True(aliceSqrt > 1.28 && aliceSqrt < 1.29)

    [<Fact>]
    member _.``Temporal Ops (Components, Format, Cast)`` () =

        let csvContent = "ts\n2023-12-25 15:30:00\n2024-01-01 00:00:00"
        use csv = new TempCsv(csvContent)
        
        let df = DataFrame.ReadCsv (path=csv.Path,tryParseDates=true)

        let res =
            df
            |> pl.select [
                pl.col "ts"

                (pl.col "ts").Dt.Year().Alias "y"
                (pl.col "ts").Dt.Month().Alias "m"
                (pl.col "ts").Dt.Day().Alias "d"
                (pl.col "ts").Dt.Hour().Alias "h"

                (pl.col "ts").Dt.Weekday().Alias "w_day"

                (pl.col "ts").Dt.ToString("%Y/%m/%d").Alias "fmt_custom"
                
                (pl.col "ts").Dt.Date().Alias "date_only"
            ]
        // --- Row 0: 2023-12-25 15:30:00 ---
        
        Assert.Equal(2023L, res.Int("y", 0).Value)
        Assert.Equal(12L, res.Int("m", 0).Value)
        Assert.Equal(25L, res.Int("d", 0).Value)

        Assert.Equal(15L, res.Int("h", 0).Value)

        Assert.Equal(1L, res.Int("w_day", 0).Value)

        Assert.Equal("2023/12/25", res.String("fmt_custom", 0).Value)

        Assert.Equal(DateOnly(2023,12,25), res.Date("date_only", 0).Value)

        // --- Row 1: 2024-01-01 00:00:00 ---
        Assert.Equal(2024L, res.Int("y", 1).Value)
        Assert.Equal(1L, res.Int("m", 1).Value)
        Assert.Equal(0L, res.Int("h", 1).Value) 

    [<Fact>]
    member _.``Cast Ops: Int to Float, String to Int`` () =
        use csv = new TempCsv "val_str,val_int\n\"100\",1000\n\"200\",2000"
            
        let df = DataFrame.ReadCsv csv.Path

        let res = 
            df 
            |> pl.select [
                // String -> Int64
                (pl.col "val_str").Cast(DataType.Int64).Alias "str_to_int"
                
                // Int64 -> Float64
                (pl.col "val_int").Cast(DataType.Float64).Alias "int_to_float"
            ]

        // "100" -> 100
        let v1 = res.Int("str_to_int", 0).Value
        Assert.Equal(100L, v1)

        // 1000 -> 1000.0
        let v2 = res.Float("int_to_float", 0).Value
        Assert.Equal(1000.0, v2)
    [<Fact>]
    member _.``Control Flow: IfElse (When/Then/Otherwise)`` () =
        // 构造成绩数据
        use csv = new TempCsv "student,score\nAlice,95\nBob,70\nCharlie,50"
        let df = DataFrame.ReadCsv csv.Path

        // if score >= 90 then "A"
        // else if score >= 60 then "Pass"
        // else "Fail"
        
        let gradeExpr = 
            pl.ifElse 
                (pl.col "score" .>= pl.lit 90) 
                (pl.lit "A") 
                (
                    pl.ifElse 
                        (pl.col "score" .>= pl.lit 60)
                        (pl.lit "Pass")
                        (pl.lit "Fail")
                )
            |> pl.alias "grade"

        let res = 
            df 
            |> pl.withColumn gradeExpr
            |> pl.sort (pl.col "score", true) 

        // Alice (95) -> A
        Assert.Equal("A", res.String("grade", 0).Value)
        // Bob (70) -> Pass
        Assert.Equal("Pass", res.String("grade", 1).Value)
        // Charlie (50) -> Fail
        Assert.Equal("Fail", res.String("grade", 2).Value)

    [<Fact>]
    member _.``String Regex: Replace and Extract`` () =
        use csv = new TempCsv "text\nUser: 12345\nID: 999"
        let df = DataFrame.ReadCsv csv.Path

        let res = 
            df 
            |> pl.select [
                // 1. Regex Replace: number into #
                (pl.col "text").Str.ReplaceAll("\d+", "#", literal=false).Alias "masked"
                
                // 2. Regex Extract: extract number
                (pl.col "text").Str.Extract("(\d+)", 1).Alias "extracted_id"
            ]

        // "User: 12345" -> "User: #"
        Assert.Equal("User: #", res.String("masked", 0).Value)

        // "User: 12345" -> "12345"
        Assert.Equal("12345", res.String("extracted_id", 0).Value)
        Assert.Equal("999", res.String("extracted_id", 1).Value)
    [<Fact>]
    member _.``Dt: Add Business Days (Standard Week)`` () =

        let start = DateOnly(2023, 1, 1)
        let df = DataFrame.ofRecords [ {| Date = start |} ]
        
        let res = 
            df.Select([
                pl.col("Date").Dt.AddBusinessDays(1, roll=Roll.Forward).Alias "Next"
            ])
            
        // 2023-01-03
        Assert.Equal(DateOnly(2023, 1, 3), res.Cell<DateOnly>("Next",0))

    [<Fact>]
    member _.``Dt: Add Business Days (With Holidays)`` () =
        // 2023-01-04 (Wed)
        // 2023-01-05 (Thu) -> Holiday
        // 2023-01-06 (Fri)
        // 2023-01-07 (Sat)
        // 2023-01-08 (Sun)
        // 2023-01-09 (Mon)
        
        let start = DateOnly(2023, 1, 4) // Wed
        let holidays = [ DateOnly(2023, 1, 5) ] // Thu is holiday
        
        let df = DataFrame.ofRecords [ {| Date = start |} ]

        // Wed + 2 Business Days
        // Day 1: Thu (Skip/Holiday) -> Fri
        // Day 2: Sat (Skip) -> Sun (Skip) -> Mon
        // Result should be Mon Jan 09
        
        let res = 
            df.Select([
                pl.col("Date").Dt
                    .AddBusinessDays(2, holidays=holidays)
                    .Alias "Result"
            ])
            
        Assert.Equal(DateOnly(2023, 1, 9), res.Cell<DateOnly>("Result", 0))

    [<Fact>]
    member _.``Dt: Custom Week Mask (Weekend is Fri/Sat)`` () =
        // Mid-East Work week
        // Mask: Mon, Tue, Wed, Thu, Fri, Sat, Sun
        let customWeek = [| true; true; true; true; false; false; true |]
        
        // 2023-01-05 (周四)
        // + 1 BD -> Fri(Skip), Sat(Skip) -> Sun (2023-01-08)
        let start = DateOnly(2023, 1, 5) 
        let df = DataFrame.ofRecords [ {| Date = start |} ]
        
        let res = 
            df.Select([
                pl.col("Date").Dt
                    .AddBusinessDays(1, weekMask=customWeek)
                    .Alias "Result"
            ])
            
        Assert.Equal(DateOnly(2023, 1, 8), res.Cell<DateOnly>("Result",0))

    [<Fact>]
    member _.``Dt: Is Business Day`` () =
        let dates = [
            DateOnly(2023, 1, 6) // Fri
            DateOnly(2023, 1, 7) // Sat
            DateOnly(2023, 1, 8) // Sun
            DateOnly(2023, 1, 9) // Mon
        ]
        
        let df = 
            DataFrame.ofRecords [ 
                for d in dates do yield {| Date = d |} 
            ]

        let res = 
            df.WithColumns([
                pl.col("Date").Dt.IsBusinessDay().Alias "IsBiz"
            ])
        
        // Fri -> True
        Assert.True(res.Cell<bool>("IsBiz",0))
        // Sat -> False
        Assert.False(res.Cell<bool>("IsBiz",1))
        // Sun -> False
        Assert.False(res.Cell<bool>("IsBiz",2))
        // Mon -> True
        Assert.True(res.Cell<bool>("IsBiz",3))

    [<Fact>]
    member _.``Dt: Is Business Day (With Holidays)`` () =
        let df = DataFrame.ofRecords [ {| Date = DateOnly(2023, 1, 2) |} ] // Mon
        let hols = [ DateOnly(2023, 1, 2) ] // Monday is holiday
        
        let res = df.Select([
            pl.col("Date").Dt.IsBusinessDay(holidays=hols)
        ])
        
        Assert.False(res.Cell<bool>("Date", 0))
    [<Fact>]
    member _.``Array: Basic Aggregation & Operations`` () =
        // Row 0: [1, 2, 3]
        // Row 1: [4, 5, 6]
        let data = [
            {| Vals = [1; 2; 3] |}
            {| Vals = [4; 5; 6] |}
        ]
        let df = DataFrame.ofRecords data
        
        let lf = 
            df.Lazy()
              .WithColumns([
                  pl.col("Vals").Cast(DataType.Array(DataType.Int32, [|3u|]))
              ])

        // Arr.Sum, Min, Max
        let res = 
            lf.Select([
                pl.col("Vals").Array.Sum().Alias "Sum"
                pl.col("Vals").Array.Min().Alias "Min"
                pl.col("Vals").Array.Max().Alias "Max"
                pl.col("Vals").Array.Mean().Alias "Mean"
            ]).Collect()

        // Row 0: Sum=6, Min=1, Max=3, Mean=2.0
        Assert.Equal(6, res.Cell<int>("Sum",0))
        Assert.Equal(1, res.Cell<int>("Min",0))
        Assert.Equal(2.0, res.Cell<double>("Mean",0))
        
        // Row 1: Sum=15
        Assert.Equal(15, res.Cell<int>("Sum",1))

    [<Fact>]
    member _.``Array: Set Operations & Sort`` () =
        let data = [
            {| Vals = ["3"; "1"; "2"] |}
            {| Vals = ["1"; "1"; "2"] |}
        ]
        let lf = 
            DataFrame.ofRecords(data).Lazy()
                .WithColumns([
                    pl.col("Vals").Cast(DataType.Array(DataType.String, [|3u|]))
                ])

        let res = 
            lf.Select([
                // Sort Descending
                pl.col("Vals").Array.Sort(descending=true).Alias "Sorted_Str"
                // Unique
                pl.col("Vals").Array.Unique().List.Sort().Alias "Unique_Str"
                // Join
                pl.col("Vals").Array.Join("-").Alias "Joined"
            ]).Collect()

        // --- Row 0 ---
        // ["3", "1", "2"]
        
        // Sort Descending -> "3,2,1"
        let sorted = res.CellList<string>("Sorted_Str",0)
        Assert.Equal<string list>(["3"; "2"; "1"], sorted)
        
        // Join -> "3-1-2"
        Assert.Equal("3-1-2", res.Cell<string>("Joined",0))

        // --- Row 1 ---
        // ["1", "1", "2"]
        
        // Unique -> "1,2"
        let unique = res.CellList<string>("Unique_Str",1)
        Assert.Equal<string list>(["1"; "2"],unique)

    [<Fact>]
    member _.``Array: Search & Get`` () =
        let data = [
            {| Vals = [10; 20; 30] |}
        ]
        let lf = 
            DataFrame.ofRecords(data).Lazy()
                .WithColumns([
                    pl.col("Vals").Cast(DataType.Array(DataType.Int32,[|3u|]))
                ])

        let res = 
            lf.Select([
                // Get by Index
                pl.col("Vals").Array.Get(1).Alias "Get_1"
                // Contains
                pl.col("Vals").Array.Contains(20).Alias "Has_20"
                pl.col("Vals").Array.Contains(99).Alias "Has_99"
                // ArgMax
                pl.col("Vals").Array.ArgMax().Alias "ArgMax"
            ]).Collect()

        // Get(1) -> 20
        Assert.Equal(20, res.Cell<int>("Get_1",0))
        
        // Contains
        Assert.True(res.Cell<bool>("Has_20",0))
        Assert.False(res.Cell<bool>("Has_99",0))
        
        // ArgMax -> 2 (index of 30)
        let argMax = res.Cell<int>("ArgMax",0)
        Assert.Equal(2, argMax)
    [<Fact>]
    member _.``Math: Trig and Rounding`` () =
        // 0.0, PI/2, PI
        let data = [
            {| Val = 0.0 |}
            {| Val = Math.PI / 2.0 |}
            {| Val = Math.PI |}
            {| Val = -1.5 |} 
        ]
        
        let lf = DataFrame.ofRecords(data).Lazy()

        let res = 
            lf.Select([
                // Trig
                pl.col("Val").Sin().Alias "Sin"
                pl.col("Val").Cos().Alias "Cos"
                
                // Rounding
                pl.col("Val").Ceil().Alias "Ceil"
                pl.col("Val").Floor().Alias "Floor"
                
                // Sign
                pl.col("Val").Sign().Alias "Sign"
                
                // Cbrt (Cube Root of 8 = 2)
                pl.lit(8.0).Cbrt().Alias "Cbrt_8"
            ]).Collect()

        // Row 1 is PI/2
        Assert.Equal(1.0, res.Cell<double>("Sin",1), 5) 

        // Cos(PI) = -1.0
        // Row 2 is PI
        Assert.Equal(-1.0, res.Cell<double>("Cos",2), 5)

        // Rounding (-1.5)
        // Row 3
        Assert.Equal(-1.0, res.Cell<double>("Ceil",3))  // Ceil(-1.5) -> -1.0
        Assert.Equal(-2.0, res.Cell<double>("Floor",3)) // Floor(-1.5) -> -2.0
        
        // Sign
        // Row 0 (0.0) -> 0
        Assert.Equal(0.0, res.Cell<double>("Sign",0))
        // Row 1 (Positive) -> 1
        Assert.Equal(1.0, res.Cell<double>("Sign",1))
        // Row 3 (Negative) -> -1
        Assert.Equal(-1.0, res.Cell<double>("Sign",3))

        // Cbrt
        Assert.Equal(2.0, res.Cell<double>("Cbrt_8",0))
    [<Fact>]
    member _.``List: ConcatList (Explicit Columns)`` () =
        // Data:
        // A: 1, 2
        // B: 3, 4
        let data = [
            {| A = 1; B = 3 |}
            {| A = 2; B = 4 |}
        ]
        let df = DataFrame.ofRecords data

        let res = 
            df.Select([
                pl.concatList([ pl.col "A"; pl.col "B" ]).Alias "Merged"
            ])
        
        // Row 0: [1, 3]
        let l0 = res.CellList<int>("Merged", 0)
        Assert.Equal<int list>([1; 3], l0)

        // Row 1: [2, 4]
        let l1 = res.CellList<int>("Merged", 1)
        Assert.Equal<int list>([2; 4], l1)

    [<Fact>]
    member _.``List: ConcatList with Selector (The Magic)`` () =
        // Data:
        // A(int): 1
        // B(int): 10
        // C(str): "ignore"
        let data = [
            {| A = 1; B = 10; C = "ignore" |}
            {| A = 2; B = 20; C = "skip" |}
        ]
        let df = DataFrame.ofRecords data

        let res = 
            df.Select([
                pl.concatList([ pl.cs.numeric() ]).Alias "Features"
            ])
        
        // Row 0: A=1, B=10 -> [1, 10]
        let l0 = res.CellList<int>("Features", 0)
        Assert.Equal<int list>([1; 10], l0)
        
        // Row 1: A=2, B=20 -> [2, 20]
        let l1 = res.CellList<int>("Features", 1)
        Assert.Equal<int list>([2; 20], l1)
    [<Fact>]
    member _.``List: Concat (Fluent API)`` () =
        // Data: A=1, B=2, C=3
        let data = [
            {| A = 1; B = 2; C = 3 |}
        ]
        let df = DataFrame.ofRecords data

        let res = 
            df.Select([
                pl.concatList([ pl.col "A"; pl.col "B"; pl.col "C" ]).Alias "Func"

                pl.col("A").List.Concat([ pl.col "B"; pl.col "C" ]).Alias "Fluent_List"
                
                pl.col("A").List.Concat(pl.col "B").Alias "Fluent_Single"
            ])
        
        let lFunc = res.CellList<int>("Func", 0)
        let lFluent = res.CellList<int>("Fluent_List", 0)
        
        Assert.Equal<int list>([1; 2; 3], lFunc)
        Assert.Equal<int list>([1; 2; 3], lFluent)

        let lSingle = res.CellList<int>("Fluent_Single", 0)
        Assert.Equal<int list>([1; 2], lSingle)
    [<Fact>]
    member _.``Bitwise: Left Shift (<<<)`` () =
        // Data: [1, 2, 4] (Binary: 001, 010, 100)
        let s = Series.create("vals", [1; 2; 4])

        // Operation: << 1
        // Expr style via DataFrame
        let df = s.ToFrame()
        let resDf = 
            df.Select([
                (pl.col "vals" <<< 1).Alias "shifted"
            ])
        
        // Expected: [2, 4, 8]
        let sRes = resDf.Column "shifted"
        Assert.Equal(2, sRes.GetValue<int> 0)
        Assert.Equal(4, sRes.GetValue<int> 1)
        Assert.Equal(8, sRes.GetValue<int> 2)

    [<Fact>]
    member _.``Bitwise: Right Shift (>>>)`` () =
        // Data: [8, 4, 2]
        let s = Series.create("vals", [Some 8;Some 4; None])

        // Operation: >> 2 (Series direct op)
        // 8 (1000) >> 2 = 2 (0010)
        // 4 (0100) >> 2 = 1 (0001)
        // 2 (0010) >> 2 = 0 (0000)
        let sRes = s >>> 2
        
        Assert.Equal(2, sRes.GetValue<int> 0)
        Assert.Equal(1, sRes.GetValue<int> 1)
        Assert.Equal(None,sRes.GetValue<int option> 2)
    [<Fact>]
    member _.``Expr: TopK & BottomK`` () =
        // Data: [1, 5, 2, 4, 3]
        let s = Series.create("vals", [1; 5; 2; 4; 3])

        // TopK(2) -> [5, 4]
        let sTop = s.TopK 2
        
        Assert.Equal(2L, sTop.Length)
        Assert.Equal(5, sTop.GetValue<int> 0)
        Assert.Equal(4, sTop.GetValue<int> 1)

        // BottomK(2) -> [1, 2]
        let sBot = s.BottomK 2
        Assert.Equal(2L, sBot.Length)
        Assert.Equal(1, sBot.GetValue<int> 0)
        Assert.Equal(2, sBot.GetValue<int> 1)

    [<Fact>]
    member _.``Expr: TopKBy (Series vs Series)`` () =
        // Scores: [10, 50, 20]
        // Names:  ["A", "B", "C"]
        let sNames = Series.create("Name", ["A"; "B"; "C"])
        let sScores = Series.create("Score", [10; 50; 20])

        let sRes = sNames.TopKBy(2, sScores)

        Assert.Equal(2L, sRes.Length)
        Assert.Equal("B", sRes.GetValue<string> 0)
        Assert.Equal("C", sRes.GetValue<string> 1)

    [<Fact>]
    member _.``Expr: TopKBy with Reverse`` () =
        // Scores: [10, 50, 20]
        // Names:  ["A", "B", "C"]
        let df = 
            DataFrame.ofRecords [
                {| Name = "A"; Score = 10 |}
                {| Name = "B"; Score = 50 |}
                {| Name = "C"; Score = 20 |}
            ]

        // TopKBy(Score, reverse=true) 
        let res = 
            df.Select([
                pl.col("Name").TopKBy(2, pl.col "Score", reverse=true).Alias "Res"
            ])
        
        // Reverse=true, Top 2 -> 10(A), 20(C)
        let s = res.Column "Res"
        Assert.Equal("A", s.GetValue<string> 0)
        Assert.Equal("C", s.GetValue<string> 1)
    [<Fact>]
    [<Trait("Expr", "Fold")>]
    member _.``Fold should accumulate sum horizontally across columns`` () =

        use a = Series.create("a", [| 1; 2; 3 |])
        use b = Series.create("b", [| 4; 5; 6 |])
        use c = Series.create("c", [| 7; 8; 9 |])
        use df = DataFrame.create(a, b, c)

        let exprs = [ pl.col "a"; pl.col "b"; pl.col "c" ]

        let foldExpr = 
            exprs 
            |> pl.fold (fun acc x -> acc + x) (pl.lit 10)
            |> pl.alias "folded_sum"

        use resultDf = df.Select [| foldExpr |]
        let col = resultDf.Column "folded_sum"

        // 验证结果
        Assert.Equal(22, col.GetValue<int>(0))
        Assert.Equal(25, col.GetValue<int>(1))
        Assert.Equal(28, col.GetValue<int>(2))


    [<Fact>]
    [<Trait("Expr", "Reduce")>]
    member _.``Reduce should concatenate strings horizontally across columns`` () =
        use part1 = Series.create("part1", [| "A"; "X" |])
        use part2 = Series.create("part2", [| "B"; "Y" |])
        use part3 = Series.create("part3", [| "C"; "Z" |])
        use df = DataFrame.create(part1, part2, part3)

        let exprs = [ pl.col "part1"; pl.col "part2"; pl.col "part3" ]

        let reduceExpr = 
            exprs 
            |> pl.reduce (fun acc x -> acc + pl.lit "-" + x)
            |> pl.alias "reduced_str"

        use resultDf = df.Select [| reduceExpr |]
        let col = resultDf.Column "reduced_str"

        Assert.Equal("A-B-C", col.GetValue<string>(0))
        Assert.Equal("X-Y-Z", col.GetValue<string>(1))


    [<Fact>]
    [<Trait("Expr", "Reduce")>]
    member _.``Reduce should throw ArgumentException when sequence is empty`` () =
        let emptyExprs = Seq.empty<Expr>
        
        let ex = Assert.Throws<ArgumentException>(fun () -> 
            emptyExprs 
            |> pl.reduce (fun acc x -> acc + x) 
            |> ignore 
        )
        
        Assert.Contains("empty", ex.Message.ToLower())