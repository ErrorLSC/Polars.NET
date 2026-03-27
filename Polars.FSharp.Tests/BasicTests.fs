namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp
open System
open System.IO

type DisposableFile (extension: string, ?content: string) =
    let ext = if extension.StartsWith "." then extension else "." + extension
    let path = Path.Combine(Path.GetTempPath(), Guid.NewGuid().ToString() + ext)
    
    do
        match content with
        | Some text -> File.WriteAllText(path, text)
        | None -> () 

    member _.Path = path

    interface IDisposable with
        member _.Dispose() =
            try 
                if File.Exists path then File.Delete path
            with _ -> ()
type UserRecord = {
        name: string
        age: int          // Int64 -> Int32
        score: float option // Nullable Float
        joined: System.DateTime option // Timestamp -> DateTime
    }

type TestUser = {
    Name: string
    Age: int
    Score: float
    IsActive: bool
    JoinDate: DateTime
}
[<CLIMutable>]
type SensorData = {
    Id: int
    Value: string
    Timestamp: DateTime
}

// 场景 2: 过滤测试
[<CLIMutable>]
type Student = {
    Id: int
    Group: string
    Score: double
}

// 场景 3: Join 测试 (Multi-pass)
[<CLIMutable>]
type JoinItem = {
    Key: int
    Val: int
}
type ``Basic Functionality Tests`` () =

    [<Fact>]
    member _.``Streaming: Debug Sink`` () =
        use csv = new DisposableFile(".csv", "a,b\n1,2\n3,4")
        use parquetEager = new DisposableFile ".parquet"
        use parquetSink = new DisposableFile ".parquet"

        printfn "CSV Path: %s" csv.Path
        printfn "Target Sink Path: %s" parquetSink.Path

        let lf = LazyFrame.ScanCsv(csv.Path)
        use df = lf.Collect()
        
        Assert.Equal(2L, df.Rows)
        printfn "Step A: Collect Success. Rows: %d" df.Rows

        df.WriteParquet parquetEager.Path |> ignore
        Assert.True(System.IO.File.Exists parquetEager.Path, "Step B: Eager Write Failed")
        printfn "Step B: Eager Write Success"

        LazyFrame.ScanCsv(csv.Path)
            .SinkParquet parquetSink.Path
    
        Assert.True(System.IO.File.Exists parquetSink.Path, "Step C: Lazy Sink Failed")
        printfn "Step C: Lazy Sink Success"
    [<Fact>]
    member _.``Metadata: Schema and Dtype`` () =
        use s1 = Series.create("id", [1; 2; 3])
        use s2 = Series.create("score", [1.1; 2.2; 3.3])
        use s3 = Series.create("is_active", [true; false; true])
        
        use df = DataFrame.create [s1; s2; s3]

        Assert.Equal("i32", s1.DtypeStr)   
        Assert.Equal("f64", s2.DtypeStr)
        Assert.Equal("bool", s3.DtypeStr)

        let schema = df.Schema
        Assert.Equal(3L, df.Len)
        Assert.Equal(DataType.Int32, schema.["id"])
        Assert.Equal(DataType.Float64, schema.["score"])
        Assert.Equal(DataType.Boolean, schema.["is_active"])
        
        Console.WriteLine "------Test DataFrame PrintSchema START------"
        df.PrintSchema()
        Console.WriteLine "------Test DataFrame PrintSchema END------"
    [<Fact>]
    member _.``Lazy Introspection: Schema and Explain`` () =
        use csv = new TempCsv "a,b\n1,2"
        let lf = LazyFrame.ScanCsv (path=csv.Path, tryParseDates=false)
        
        let lf2 = 
            lf 
            |> pl.withColumnLazy (
                (pl.col "a" * pl.lit 2).Alias "a_double"
            )
            |> pl.filterLazy (pl.col "b" .> pl.lit 0)

        use pSchema = lf2.Schema 
        
        let schema = pSchema.ToMap()
        
        Assert.True(schema.ContainsKey "a")
        Assert.True(schema.ContainsKey "b")
        Assert.True(schema.ContainsKey "a_double")

        Assert.Equal(DataType.Int64, schema.["a"])
        Assert.Equal(DataType.Int64, schema.["b"])
        Assert.Equal(DataType.Int64, schema.["a_double"])

        let plan = lf2.Explain false
        printfn "\n=== Query Plan ===\n%s\n==================" plan
        Assert.Contains("FILTER", plan) 
        Assert.Contains("WITH_COLUMNS", plan)

        let planOptimized = lf2.Explain true
        printfn "\n=== Query Plan Optimized===\n%s\n==================" planOptimized
        Assert.Contains("Csv SCAN", planOptimized)
    [<Fact>]
    member _.``Arrow Integration: Import C# Arrow Data to Polars`` () =

        let builder = new Apache.Arrow.Int64Array.Builder()
        builder.Append 100L |> ignore
        builder.Append 200L |> ignore
        builder.AppendNull() |> ignore 
        let colArray = builder.Build()

        let field = new Apache.Arrow.Field("num", new Apache.Arrow.Types.Int64Type(), true)
        let schema = new Apache.Arrow.Schema([| field |], null)
        
        use batch = new Apache.Arrow.RecordBatch(schema, [| colArray |], 3)

        let df = DataFrame.FromArrow batch

        Assert.Equal(3L, df.Rows)
        Assert.Equal(100L, df.Int("num", 0).Value)
        Assert.Equal(200L, df.Int("num", 1).Value)
        Assert.True(df.Int("num", 2).IsNone) 
    [<Fact>]
    member _.``Series: AsSeq Lifecycle & Complex Types`` () =

        let data = [
            Some(DateTime(2023, 1, 1))
            None
            Some(DateTime(2024, 1, 1))
        ]
        use s = Series.ofSeq("dt", data)

        let seqData = s.AsSeq<DateTime>()

        let listData = seqData |> Seq.toList

        Assert.Equal(3, listData.Length)
        Assert.Equal(Some(DateTime(2023, 1, 1)), listData.[0])
        Assert.True(listData.[1].IsNone)

    [<Fact>]
    member _.``DataFrame: Create from Series`` () =
        use s1 = Series.create("id", [1; 2; 3])
        use s2 = Series.create("name", ["a"; "b"; "c"])

        use df = DataFrame.create [s1; s2]

        Assert.Equal(3L, df.Rows)
        Assert.Equal(2L, df.Width)
        Assert.Equal<string seq>(["id"; "name"], df.ColumnNames)
        
        Assert.Equal(3L, s1.Length)

        pl.show df |> ignore
    [<Fact>]
    member _.``Convenience: Drop, Rename, DropNulls, Sample`` () =
        // Test DataFrame
        let s1 = Series.create("a", [Some 1; Some 2; None])
        let s2 = Series.create("b", ["x"; "y"; "z"])
        use df = DataFrame.create [s1; s2]

        // Drop
        let dfDrop = df.Drop "a"
        Assert.Equal(1L, dfDrop.Width)
        Assert.Equal<string seq>(["b"], dfDrop.ColumnNames)
        Assert.Equal(2L, df.Width) |> ignore
        Assert.Equal<string seq>(["a"; "b"], df.ColumnNames)

        // Rename
        let dfRenamed = df.Rename("b", "b_new")
        Assert.Equal<string seq>(["a"; "b_new"], dfRenamed.ColumnNames)

        // DropNulls
        let dfClean = df.DropNulls()
        Assert.Equal(2L, dfClean.Rows) 
        Assert.Equal(Some 1L, dfClean.Int("a", 0))
        Assert.Equal(Some 2L, dfClean.Int("a", 1))

        // Sample (n=1)
        let dfSample = df.Sample(n=1, seed=12345UL)
        Assert.Equal(1L, dfSample.Rows)
        
        // Sample (frac=0.5) -> 3 * 0.5 = 1.5 -> 1 or 2 rows depending on algo, usually round/floor
        // Polars sample_frac usually works well. 3 * 0.6 = 1.8. 
        let dfSampleFrac = df.Sample(frac=1.0) 
        Assert.Equal(3L, dfSampleFrac.Rows)
    [<Fact>]
    member _.``Full Temporal Types: Create & Retrieve`` () =
        let date = DateOnly(2023, 1, 1)
        let time = TimeOnly(12, 30, 0)
        let dur = TimeSpan.FromHours 1.5 // 90 mins

        use sDate = Series.create("d", [date])
        use sTime = Series.create("t", [time])
        use sDur = Series.create("dur", [dur])

        Assert.Equal(pl.date, sDate.DataType)
        Assert.Equal(pl.time, sTime.DataType)
        Assert.Equal(DataType.Duration TimeUnit.Microseconds, sDur.DataType)


        Assert.Equal(date, sDate.GetValue<DateOnly> 0)
        Assert.Equal(time, sTime.GetValue<TimeOnly> 0)
        Assert.Equal(dur, sDur.GetValue<TimeSpan> 0)
        
        Assert.Equal(Some date, unbox<DateOnly option> sDate.[0])

        let records = [
            {| Id = 1; DoB = date; WakeUp = time; Shift = dur |}
        ]
        let df = DataFrame.ofRecords records
        
        let sDoB = df.["DoB"]
        Assert.Equal(pl.date, sDoB.DataType)
        
        Assert.Equal(Some date, df.Cell<DateOnly option>("DoB",0))
        Assert.Equal(Some time, df.Cell<TimeOnly option>("WakeUp",0))
        Assert.Equal(Some dur,  unbox<TimeSpan option> df.[0, "Shift"])
        
        Assert.Equal(Some date, sDoB.GetValueOption<DateOnly> 0)
    [<Fact>]
    member _.``Expr: TimeZone Ops (Convert & Replace)`` () =
        let s = Series.create("ts", [DateTime(2023, 1, 1, 12, 0, 0)])
        use df = DataFrame.create [s]

        let res = 
            df
            |> pl.select([
                pl.col "ts"
                
                // Convert (Naive -> Error, so we must Replace first)
                pl.col("ts")
                    .Dt.ReplaceTimeZone("UTC")
                    .Dt.ConvertTimeZone("Asia/Shanghai")
                    .Alias "shanghai"

                // Replace with Strategy (Full signature check)
                pl.col("ts")
                    .Dt.ReplaceTimeZone("Europe/London", ambiguous="earliest", nonExistent="null")
                    .Alias "london_explicit"
                
                // Unset TimeZone (Make Naive)
                pl.col("ts")
                    .Dt.ReplaceTimeZone("UTC")
                    .Dt.ReplaceTimeZone(None) // Set back to None
                    .Alias "naive"
            ])

        // Shanghai (+08:00)
        let shRow = res.Column("shanghai").AsSeq<DateTimeOffset>() |> Seq.head |> Option.get
        Assert.Equal(TimeSpan.FromHours 8, shRow.Offset)
        Assert.Equal(20, shRow.Hour) // 12:00 UTC -> 20:00 Shanghai

        // London (Naive 12:00 -> London 12:00 +00:00 in Jan)
        let ldRow = res.Column("london_explicit").AsSeq<DateTimeOffset>() |> Seq.head |> Option.get
        Assert.Equal(0, ldRow.Offset.Hours) 
        
        // Naive (Unset)
        let naiveRow = res.Column("naive").AsSeq<DateTime>() |> Seq.head |> Option.get
        Assert.Equal(DateTimeKind.Unspecified, naiveRow.Kind)
    [<Fact>]
    member _.``Conversion: DataFrame -> Lazy -> DataFrame`` () =
        use df = DataFrame.ofRecords [ { name = "Qinglei"; age = 18 ; score = Some 99.5; joined = Some (System.DateTime(2023,1,1)) }; { name = "Someone"; age = 20; score = None; joined = None } ]
        
        let lf = df.Lazy()
        
        let res = 
            lf
            |> pl.filterLazy(pl.col "age" .> pl.lit 18)
            |> pl.collect

        Assert.Equal(1L, res.Rows)
        Assert.Equal(20L, res.Int("age", 0).Value)

        Assert.Equal(2L, df.Rows)
    [<Fact>]
    member _.``EDA: Describe (Manual Implementation)`` () =
        let s = Series.create("nums", [1.0; 2.0; 3.0; 4.0; 5.0])
        use df = DataFrame.create [s]

        let desc = df.Describe()
        
        pl.show desc |> ignore
        
        Assert.Equal(9L, desc.Rows)
        
        // 0: count, 1: null_count, 2: mean
        let meanVal = desc.Float("nums", 2).Value
        Assert.Equal(3.0, meanVal)
        
        // std
        let stdVal = desc.Float("nums", 3).Value
        Assert.True(abs(stdVal - 1.58113883) < 0.0001)
    [<Fact>]
    member _.``Reshaping: Concat Diagonal`` () =
        // df1: [a, b]
        use csv1 = new TempCsv "a,b\n1,2"
        // df2: [a, c] 
        use csv2 = new TempCsv "a,c\n3,4"

        let df1 = DataFrame.ReadCsv (path=csv1.Path, tryParseDates=false)
        let df2 = DataFrame.ReadCsv (path=csv2.Path, tryParseDates=false)

        // Row 1 (from df1): a=1, b=2, c=null
        // Row 2 (from df2): a=3, b=null, c=4
        let res = pl.concatDiagonal [df1; df2]

        Assert.Equal(2L, res.Rows)
        Assert.Equal(3L, res.Width)
        
        let cols = res.ColumnNames
        Assert.Contains("a", cols)
        Assert.Contains("b", cols)
        Assert.Contains("c", cols)

        Assert.Equal(1L, res.Int("a", 0).Value)
        Assert.Equal(2L, res.Int("b", 0).Value)
        Assert.True(res.Int("c", 0).IsNone) 

        Assert.Equal(3L, res.Int("a", 1).Value)
        Assert.True(res.Int("b", 1).IsNone) 
        Assert.Equal(4L, res.Int("c", 1).Value)
    [<Fact>]
    member _.``Scalar Access: IsNullAt`` () =
        // [1, null, 3]
        use s = Series.create("a", [Some 1; None; Some 3])
        use df = DataFrame.create [s]

        // Series 
        Assert.False(s.IsNullAt 0)
        Assert.True(s.IsNullAt 1)
        Assert.False(s.IsNullAt 2)
        Assert.False(s.IsNullAt 999)

        // DataFrame 
        Assert.False(df.IsNullAt("a", 0))
        Assert.True(df.IsNullAt("a", 1))
    [<Fact>]
    member _.``Metadata: NullCount`` () =
        // 1, null, 3, null
        let s = Series.create("a", [Some 1; None; Some 3; None])

        Assert.Equal(2L, s.NullCount)
        Assert.Equal(4L, s.Length)

        use df = DataFrame.create [s]
        Assert.Equal(2L, df.NullCount "a")
 
    [<Fact>]
    member _.``Async: Collect LazyFrame`` () =
        use csv1 = new TempCsv "a,b\n1,2\n3,4"
        let df = 
            LazyFrame.ScanCsv (path=csv1.Path, tryParseDates=false)
            |> pl.filterLazy (pl.col "a" .> pl.lit 0)
            |> pl.collectAsync 
            |> Async.RunSynchronously 

        Assert.Equal(2L, df.Rows)
        Assert.Equal(1L, df.Int("a", 0).Value)
    [<Fact>]
    member _.``Series: Arithmetic & Aggregation (Pandas Style)`` () =

        use demand = Series.create("demand", [100.0; 200.0; 300.0])
        use weight = Series.create("weight", [0.5; 1.5; 1.0])

        // weighted_mean = (demand * weight).Sum() / weight.Sum()
        
        let sProd = demand * weight    // [50.0, 300.0, 300.0]
        let sSumProd = sProd.Sum()     // [650.0]
        let sSumW = weight.Sum()       // [3.0]
        
        // Broadcasting: Scalar / Scalar
        let sWeightedMean = sSumProd / sSumW 
        
        Assert.Equal(1L, sWeightedMean.Length)
        
        // 650 / 3 = 216.666...
        let valMean = sWeightedMean.Float(0).Value
        Assert.True(abs(valMean - 216.6666) < 0.001)
        
        let mask = demand .> 0.0 

        let countPos = mask.Sum()
        
        // Polars boolean sum returns UInt32 usually.
        let countVal = countPos.Cast(DataType.Float64).Float(0).Value
        Assert.Equal(3.0, countVal)
        
        // zero_ratio = (demand == 0).mean()
        let zeroMask = demand .= 0.0
        let zeroRatio = zeroMask.Mean() // Mean on boolean = ratio of true
        
        // 0 / 3 = 0.0
        Assert.Equal(0.0, zeroRatio.Float(0).Value)
    [<Fact>]
    member _.``Series: Arithmetic & Aggregation (F# Pipeline Style)`` () =
        use demand = Series.create("demand", [100.0; 200.0; 300.0])
        use weight = Series.create("weight", [0.5; 1.5; 1.0])
        
        let sWeightedMean = 
            demand
            |> Series.mul weight          // Element-wise multiplication
            |> Series.sum                 // Sum result
            |> Series.div (weight |> Series.sum) // Divide by scalar (series of len 1)

        Assert.Equal(1L, sWeightedMean.Length)
        let valMean = sWeightedMean.Float(0).Value
        Assert.True(abs(valMean - 216.6666) < 0.001)
        
        let countVal = 
            demand
            |> Series.gtLit 0.0           // Broadcasting comparison (> 0.0)
            |> Series.sum                 // Count true values
            |> Series.cast DataType.Float64 
            |> fun s -> s.Float(0).Value 

        Assert.Equal(3.0, countVal)

        let zeroRatio = 
            demand
            |> Series.eqLit 0.0           // Broadcasting comparison (= 0.0)
            |> Series.mean                // Mean of boolean
            |> fun s -> s.Float(0).Value

        Assert.Equal(0.0, zeroRatio)
    [<Fact>]
    member _.``Series: NaN and Infinity Checks`` () =
        // [1.0, NaN, Inf, -Inf, 5.0]
        let s = Series.create("f", [1.0; Double.NaN; Double.PositiveInfinity; Double.NegativeInfinity; 5.0])

        // IsNan -> [F, T, F, F, F]
        let maskNan = s.IsNan()
        Assert.Equal(Some true, maskNan.Bool 1) // NaN
        Assert.Equal(Some false, maskNan.Bool 0)

        // IsInfinite -> [F, F, T, T, F]
        let maskInf = s.IsInfinite()
        Assert.Equal(Some true, maskInf.Bool 2) // +Inf
        Assert.Equal(Some true, maskInf.Bool 3) // -Inf
        Assert.Equal(Some false, maskInf.Bool 1) // NaN is NOT Infinite

        // IsFinite -> [T, F, F, F, T]
        let maskFin = s.IsFinite()
        Assert.Equal(Some true, maskFin.Bool 0)
        Assert.Equal(Some false, maskFin.Bool 1) // NaN not finite
        Assert.Equal(Some false, maskFin.Bool 2) // Inf not finite
    // ---------------------------------------------------
    // Streaming Tests
    // ---------------------------------------------------

    [<Fact>]
    member _.``Stream: Eager Ingestion (ofSeqStream)`` () =
        let count = 100_000
        let data = Seq.init count (fun i -> 
            { Id = i; Value = $"Val_{i}"; Timestamp = DateTime(2023, 1, 1).AddSeconds(float i) }
        )

        use df = DataFrame.ofSeqStream(data, batchSize = 10_000)

        Assert.Equal(int64 count, df.Rows)
        Assert.Equal("Val_99999", df.Column("Value").AsSeq<string>() |> Seq.last |> Option.get)
        let expectedType = Datetime(Microseconds, Some "")
        
        Assert.Equal(expectedType, df.Schema.["Timestamp"])

    [<Fact>]
    member _.``Stream: Lazy Scan (scanSeq) with Filter`` () =
        let data = [
            { Id = 1; Group = "A"; Score = 10.0 }
            { Id = 2; Group = "B"; Score = 20.0 }
            { Id = 3; Group = "A"; Score = 30.0 }
        ]

        // 2. Lazy Scan -> Filter -> Collect
        let res = 
            LazyFrame.scanSeq data
                |> pl.filterLazy(pl.col "Group" .== pl.lit "A")
                |> pl.collect

        Assert.Equal(2L, res.Rows) 
        Assert.Equal(1L, res.Int("Id", 0).Value)
        Assert.Equal(3L, res.Int("Id", 1).Value)

    [<Fact>]
    member _.``Stream: Lazy Multi-pass Scan (Self Join)`` () =
        
        let data = Seq.init 10 (fun i -> { Key = i % 3; Val = i }) // Key: 0, 1, 2 重复
        
        let lf = LazyFrame.scanSeq data
        
        // Self Join: lf.Join(lf, on="Key")
        let res = 
            lf
            |> pl.joinLazy lf [pl.col "Key"] [pl.col "Key"] JoinType.Left
            |> pl.collect

        // 0: 4 items -> 4*4 = 16
        // 1: 3 items -> 3*3 = 9
        // 2: 3 items -> 3*3 = 9
        // Total = 34
        Assert.Equal(34L, res.Rows)
    [<Fact>]
    member _.``Series: Uniqueness and ApplyExpr`` () =
        // Data: [1, 2, 2, 3]
        let s = Series.create("nums", [1; 2; 2; 3])

        // NUnique (Native)
        Assert.Equal(3L, s.NUnique()) // 1, 2, 3

        // Unique (Native)
        let sUniq = s.Unique().Sort false // Sort to compare deterministically
        Assert.Equal(3L, sUniq.Length)
        Assert.Equal(1, sUniq.GetValue<int> 0)
        Assert.Equal(2, sUniq.GetValue<int> 1)
        Assert.Equal(3, sUniq.GetValue<int> 2)

        // [1, 2, 2, 3] -> IsUnique -> [T, F, F, T]
        let sIsUniq = s.IsUnique()
        Assert.Equal(4L, sIsUniq.Length)
        Assert.True(sIsUniq.GetValue<bool> 0)  // 1 is unique
        Assert.False(sIsUniq.GetValue<bool> 1) // 2 is not
        Assert.False(sIsUniq.GetValue<bool> 2) // 2 is not
        Assert.True(sIsUniq.GetValue<bool> 3)  // 3 is unique

        // [1, 2, 2, 3] -> IsDup -> [F, T, T, F]
        let sIsDup = s.IsDuplicated()
        Assert.False(sIsDup.GetValue<bool> 0)
        Assert.True(sIsDup.GetValue<bool> 1)
    [<Fact>]
    member _.``Series: Sort with Options (Nulls & Stable)`` () =
        // [2, null, 1, 2]
        let s = Series.create("nums", [Some 2; None; Some 1; Some 2])

        // Pnulls_last=false (Nulls are smallest)
        let sDef = s.Sort()
        Assert.True(sDef.IsNullAt 0)
        Assert.Equal(1,sDef .% 1)

        // Ascending Nulls Last -> [1, 2, 2, null]
        let sNullLast = s.Sort(nullsLast = true)
        Assert.Equal(1, sNullLast .% 0)
        Assert.True(sNullLast.IsNullAt 3)

        // Descending + Nulls Last -> [2, 2, 1, null]
        let sDescNullLast = s.Sort(descending = true, nullsLast = true)
        Assert.Equal(2, sDescNullLast .% 0)
        Assert.True(sDescNullLast.IsNullAt 3)
        
        let sStable = s.Sort(maintainOrder = true)
        Assert.Equal(4L, sStable.Length)
    [<Fact>]
    member _.``DataFrame: Sort Advanced (Multi-Column & Nulls)`` () =
        // Group: A, B
        // Val: 1, 2, null
        let data = [
            {| Group = "A"; Val = Some 1 |}
            {| Group = "A"; Val = None |}   // Null
            {| Group = "A"; Val = Some 2 |}
            {| Group = "B"; Val = Some 1 |}
        ]
        let df = DataFrame.ofRecords data

        let sorted1 = df.Sort( "Val", descending=false, nullsLast=true)
        
        // 1, 1, 2, null
        Assert.Equal(1, sorted1.Cell<int>("Val",0))
        Assert.True(sorted1.IsNullAt("Val",0) |> not)
        Assert.True(sorted1.IsNullAt("Val",3)) 

        let sorted2 = df.Sort(
            columns = [ pl.col "Group"; pl.col "Val" ],
            descending = [ true; false ],  // Group=Desc, Val=Asc
            nullsLast = [ false; true ]    // Group=Default, Val=Last
        )

        // 1. B, 1
        // 2. A, 1
        // 3. A, 2
        // 4. A, null

        // Row 0: B, 1
        Assert.Equal("B", sorted2.Cell<string>("Group",0))
        Assert.Equal(1, sorted2.Cell<int>("Val",0))

        // Row 3: A, null
        Assert.Equal("A", sorted2.Cell<string>("Group",3))
        Assert.True(sorted2.IsNullAt("Val",3))

    [<Fact>]
    member _.``LazyFrame: Sort Advanced`` () =
        let data = [
            {| A = 1; B = 3 |}
            {| A = 1; B = 1 |}
            {| A = 2; B = 2 |}
        ]
        let lf = DataFrame.ofRecords(data).Lazy()

        // A ascending, B descending
        let res = 
            lf.Sort(
                columns = [ pl.col "A"; pl.col "B" ], 
                descending = [false; true], 
                nullsLast = [false; false]
            ).Collect()

        // 1. A=1, B=3
        // 2. A=1, B=1
        // 3. A=2, B=2
        
        Assert.Equal(1, res.Cell<int>("A",0))
        Assert.Equal(3, res.Cell<int>("B",0)) 
        
        Assert.Equal(1, res.Cell<int>("A",1))
        Assert.Equal(1, res.Cell<int>("B",1))
    [<Fact>]
    member _.``ScanSeq Streaming Mode - Should convert data correctly`` () =
        let now = DateTime.Now
        let data = [
            { Name = "Alice"; Age = 25; Score = 99.5; IsActive = true; JoinDate = now }
            { Name = "Bob";   Age = 30; Score = 85.0; IsActive = false; JoinDate = now.AddDays -1.0 }
            { Name = "Charlie"; Age = 35; Score = 70.0; IsActive = true; JoinDate = now.AddDays -10.0 }
        ]

        // Act
        use lf = LazyFrame.scanSeq data
        
        use df = lf.Collect()

        // Assert
        Assert.Equal(3L, df.Height)
        
        let row0_Name = df.["Name"].[0] :?> string option
        let row0_Score = df.["Score"].[0] :?> double option
        
        Assert.Equal(Some "Alice", row0_Name)
        Assert.Equal(Some 99.5, row0_Score)

    [<Fact>]
    member _. ``ScanSeq Buffered Mode - Should handle IO correctly`` () =
        let data = seq {
            for i in 1 .. 1000 do
                yield { 
                    Name = sprintf "User_%d" i
                    Age = i
                    Score = float i * 1.5
                    IsActive = i % 2 = 0
                    JoinDate = DateTime.Now 
                }
        }

        use lf = LazyFrame.scanSeq(data, useBuffered = true, batchSize = 100)
        
        use df = lf.Collect()

        // Assert
        Assert.Equal(1000L, df.Height)
        
        let lastRowScore = df.["Score"].[999] :?> double option
        
        Assert.Equal(Some (1000.0 * 1.5), lastRowScore)

    [<Fact>]
    member _.``ScanSeq Empty Stream - Should preserve Schema without crashing`` () =
        let emptyData = Seq.empty<TestUser>

        use lf = LazyFrame.scanSeq emptyData
        
        use df = lf.Collect()
        Assert.Equal(0L, df.Height)
        
        let columns = df.ColumnNames
        Assert.Contains("Name", columns)
        Assert.Contains("Age", columns)
        Assert.Contains("Score", columns)
        Assert.Contains("IsActive", columns)
        Assert.Contains("JoinDate", columns)

type LitTests() =

    let df = DataFrame.create [ Series.create("dummy", [1]) ]

    [<Fact>]
    member _.``Lit: All Primitive Types (Explicit Overload Check)``() =
        
        // ==========================================
        // Signed Integers
        // ==========================================
        
        // int8 (sbyte) -> suffix 'y'
        let v_i8 = 123y
        let res_i8 = df.Select(pl.lit v_i8).Column(0).GetValue<sbyte>(0)
        Assert.Equal(v_i8, res_i8)

        // int16 (short) -> suffix 's'
        let v_i16 = 12345s
        let res_i16 = df.Select(pl.lit v_i16).Column(0).GetValue<int16>(0)
        Assert.Equal(v_i16, res_i16)

        // int32 (int)
        let v_i32 = 123456789
        let res_i32 = df.Select(pl.lit v_i32).Column(0).GetValue<int>(0)
        Assert.Equal(v_i32, res_i32)

        // int64 (long) -> suffix 'L'
        let v_i64 = 1234567890123456789L
        let res_i64 = df.Select(pl.lit v_i64).Column(0).GetValue<int64>(0)
        Assert.Equal(v_i64, res_i64)

        // Int128 (.NET 7+)
        let v_i128 = Int128.Parse "1000000000000000000000000000000" 
        let res_i128 = df.Select(pl.lit v_i128).Column(0).GetValue<Int128>(0)
        Assert.Equal(v_i128, res_i128)

        // ==========================================
        // Unsigned Integers
        // ==========================================

        // uint8 (byte) -> suffix 'uy'
        let v_u8 = 200uy
        let res_u8 = df.Select(pl.lit v_u8).Column(0).GetValue<byte>(0)
        Assert.Equal(v_u8, res_u8)

        // uint16 (ushort) -> suffix 'us'
        let v_u16 = 40000us
        let res_u16 = df.Select(pl.lit v_u16).Column(0).GetValue<uint16>(0)
        Assert.Equal(v_u16, res_u16)

        // uint32 (uint) -> suffix 'u'
        let v_u32 = 3000000000u
        let res_u32 = df.Select(pl.lit v_u32).Column(0).GetValue<uint>(0)
        Assert.Equal(v_u32, res_u32)

        // uint64 (ulong) -> suffix 'UL'
        let v_u64 = 10000000000000000000UL
        let res_u64 = df.Select(pl.lit v_u64).Column(0).GetValue<uint64>(0)
        Assert.Equal(v_u64, res_u64)

        // ==========================================
        // Floating Point & Decimal
        // ==========================================

        // float32 (single) -> suffix 'f'
        let v_f32 = 123.456f
        let res_f32 = df.Select(pl.lit v_f32).Column(0).GetValue<float32>(0)
        Assert.Equal(v_f32, res_f32)

        // float64 (double)
        let v_f64 = 123.456789123
        let res_f64 = df.Select(pl.lit v_f64).Column(0).GetValue<double>(0)
        Assert.Equal(v_f64, res_f64)

        // decimal -> suffix 'm'
        let v_dec = 123.456789m
        let res_dec = df.Select(pl.lit v_dec).Column(0).GetValue<decimal>(0)
        Assert.Equal(v_dec, res_dec)

        // ==========================================
        // String & Boolean
        // ==========================================

        // string
        let v_str = "Polars.NET 🚀"
        let res_str = df.Select(pl.lit v_str).Column(0).GetValue<string>(0)
        Assert.Equal(v_str, res_str)

        // bool
        let v_bool = true
        let res_bool = df.Select(pl.lit v_bool).Column(0).GetValue<bool>(0)
        Assert.True res_bool

        // ==========================================
        // Temporal Types
        // ==========================================

        // DateTime
        let v_dt = DateTime(2023, 10, 1, 12, 30, 45)
        let res_dt = df.Select(pl.lit v_dt).Column(0).GetValue<DateTime>(0)
        Assert.Equal(v_dt, res_dt)

        // DateOnly
        let v_date = DateOnly(2023, 10, 1)
        let res_date = df.Select(pl.lit v_date).Column(0).GetValue<DateOnly>(0)
        Assert.Equal(v_date, res_date)

        // TimeOnly
        let v_time = TimeOnly(12, 30, 45)
        let res_time = df.Select(pl.lit v_time).Column(0).GetValue<TimeOnly>(0)
        Assert.Equal(v_time, res_time)

        // TimeSpan
        let v_span = TimeSpan.FromHours(25.5)
        let res_span = df.Select(pl.lit v_span).Column(0).GetValue<TimeSpan>(0)
        Assert.Equal(v_span, res_span)

        // DateTimeOffset
        let v_dto = DateTimeOffset(2023, 10, 1, 12, 0, 0, TimeSpan.FromHours(8.0))
        let res_dto = df.Select(pl.lit v_dto).Column(0).GetValue<DateTimeOffset>(0)
        Assert.Equal(v_dto, res_dto)
    [<Fact>]
    member _.``Lit: Collections (Lists & Arrays)``() =

        let df = DataFrame.create [ Series.create("dummy", [0]) ]

        // ==========================================
        // F# Lists (int list, string list...)
        // ==========================================
        
        // Integer List
        let listInt = [1; 2; 3]
        let dfInt = df.Select(pl.lit listInt)
        
        Assert.Equal(3L, dfInt.Height) 
        Assert.Equal(1, dfInt.Column(0).GetValue<int>(0))
        Assert.Equal(2, dfInt.Column(0).GetValue<int>(1))
        Assert.Equal(3, dfInt.Column(0).GetValue<int>(2))

        // String List
        let listStr = ["A"; "B"; "C"]
        let dfStr = df.Select(pl.lit listStr)
        
        Assert.Equal(3L, dfStr.Height)
        Assert.Equal("A", dfStr.Column(0).GetValue<string>(0))
        Assert.Equal("B", dfStr.Column(0).GetValue<string>(1))

        // ==========================================
        // F# Arrays (int[], float[]...)
        // ==========================================

        // Double Array
        let arrF64 = [| 1.1; 2.2; 3.3; 4.4 |]
        let dfF64 = df.Select(pl.lit arrF64)

        Assert.Equal(4L, dfF64.Height)
        Assert.Equal(1.1, dfF64.Column(0).GetValue<double>(0))

        // Bool Array
        let arrBool = [| true; false |]
        let dfBool = df.Select(pl.lit arrBool)
        
        Assert.Equal(2L, dfBool.Height)
        Assert.True(dfBool.Column(0).GetValue<bool>(0))
        Assert.False(dfBool.Column(0).GetValue<bool>(1))

        // ==========================================
        // Nullable Collections (Option List)
        // ==========================================

        // Int Option List (with None)
        let listOpt = [Some 10; None; Some 30]
        let dfOpt = df.Select(pl.lit listOpt)

        Assert.Equal(3L, dfOpt.Height)
        Assert.Equal(10, dfOpt.Column(0).GetValue<int>(0))
        Assert.True(dfOpt.Column(0).IsNullAt(1)) // Index 1 is Null
        Assert.Equal(30, dfOpt.Column(0).GetValue<int>(2))

        // String Option List
        let listStrOpt = [Some "Valid"; None]
        let dfStrOpt = df.Select(pl.lit listStrOpt)
        
        Assert.Equal(2L, dfStrOpt.Height)
        Assert.Equal("Valid", dfStrOpt.Column(0).GetValue<string>(0))
        Assert.True(dfStrOpt.Column(0).IsNullAt(1))

    [<Fact>]
    member _.``Lit: Empty Collections``() =
        let df = DataFrame.create [ Series.create("dummy", [0]) ]
        
        // Empty List -> Empty Series -> Empty Column
        let emptyList: int list = []
        let dfEmpty = df.Select(pl.lit emptyList)
        
        Assert.Equal(0L, dfEmpty.Height)
    [<Fact>]
    member _.``Test HStack and VStack``() =

        // a: [1, 2, 3]
        use df1 = 
            DataFrame.create [
                Series.create("a", [1; 2; 3])
            ]

        // b: [10, 20, 30]
        use s_new = Series.create("b", [10; 20; 30])

        use h_stacked = 
            df1 
            |> pl.hstack [s_new]

        Assert.Equal(3L, h_stacked.Height)
        Assert.Equal(2L, h_stacked.Width)
        
        let cols = h_stacked.ColumnNames
        Assert.Equal("a", cols.[0])
        Assert.Equal("b", cols.[1])

        Assert.Equal(Some 10,unbox h_stacked.[0,"b"]) // Row 0, Col "b"

        // DF2: [a, b]
        // a: [4, 5]
        // b: [40, 50]
        use df2 = 
            DataFrame.create [
                Series.create("a", [4; 5])
                Series.create("b", [40; 50])
            ]

        use v_stacked = 
            h_stacked 
            |> pl.vstack df2

        Assert.Equal(5L, v_stacked.Height)
        Assert.Equal(2L, v_stacked.Width)

        Assert.Equal(Some 1,unbox v_stacked.[0, 0])
        
        Assert.Equal(Some 4,unbox v_stacked.[3,"a"])

        Assert.Equal(Some 50,unbox v_stacked.[4,"b"])
    [<Fact>]
    [<Trait("DataFrame","AsTensor")>]
    member _.``DataFrame: AsTensor extracts all columns to Row-Major 2D Tensor`` () =

        let s1 = Series.From("feature1", [| 1.1f; 2.1f; 3.1f |])
        let s2 = Series.From("feature2", [| 1.2f; 2.2f; 3.2f |])
        use df = DataFrame.FromColumns [| s1; s2 |] 

        // Act
        let tensor = df.AsTensor<float32>()

        // Assert
        Assert.Equal(2, tensor.Rank)
        Assert.Equal(3, int tensor.Lengths.[0]) 
        Assert.Equal(2, int tensor.Lengths.[1]) 

        let valAt r c = tensor.[ReadOnlySpan<nativeint>([| nativeint r; nativeint c |])]

        Assert.Equal(1.1f, valAt 0 0)
        Assert.Equal(1.2f, valAt 0 1)
        
        Assert.Equal(2.1f, valAt 1 0)
        Assert.Equal(2.2f, valAt 1 1)

        Assert.Equal(3.1f, valAt 2 0)
        Assert.Equal(3.2f, valAt 2 1)

    [<Fact>]
    member _.``DataFrame: AsTensor extracts specifically selected columns`` () =
        let s1 = Series.From("id", [| 1; 2 |])
        let s2 = Series.From("feature1", [| 0.1f; 0.2f |])
        let s3 = Series.From("feature2", [| 0.9f; 0.8f |])
        use df = DataFrame.FromColumns [| s1; s2; s3 |]

        let tensor = df.AsTensor<float32>("feature1", "feature2")

        Assert.Equal(2, tensor.Rank)
        Assert.Equal(2, int tensor.Lengths.[0]) // 2 Rows
        Assert.Equal(2, int tensor.Lengths.[1]) // 2 Columns

        let valAt r c = tensor.[ReadOnlySpan<nativeint>([| nativeint r; nativeint c |])]

        Assert.Equal(0.1f, valAt 0 0)
        Assert.Equal(0.9f, valAt 0 1)
        
        Assert.Equal(0.2f, valAt 1 0)
        Assert.Equal(0.8f, valAt 1 1)

    [<Fact>]
    [<Trait("DataFrame","AsTensorEmpty")>]
    member _.``DataFrame: AsTensor throws InvalidOperationException on empty DataFrame`` () =
        use df = DataFrame.create([||])

        let ex = Assert.Throws<InvalidOperationException>(fun () -> 
            df.AsTensor<float32>() |> ignore 
        )

        Assert.Contains("Cannot create a Tensor from an empty DataFrame", ex.Message)

    [<Fact>]
    [<Trait("DataFrame","AsTensorException")>]
    member _.``DataFrame: AsTensor throws Exception on type mismatch`` () =
        // Arrange
        let s1 = Series.From("age", [| 25; 30 |]) 
        let s2 = Series.From("salary", [| 5000.5f; 6000.5f |]) 
        use df = DataFrame.FromColumns [| s1; s2 |]

        let ex = Assert.ThrowsAny<Exception>(fun () -> 
            df.AsTensor<float32>() |> ignore
        )
        
        Assert.NotNull ex