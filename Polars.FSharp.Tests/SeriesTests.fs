namespace Polars.FSharp.Tests

open Xunit
open Polars.FSharp
open Apache.Arrow
open System
open System.Numerics.Tensors

type ``Series Tests`` () =
    let count = 100_000 

    [<Fact>]
    member _.``Series: Create Strings with Nulls`` () =
        let data = [Some "hello"; None; Some "world"]
        use s = Series.create("strings", data)
        
        let arrow = s.ToArrow() 

        match arrow with
        | :? StringViewArray as sa ->
            Assert.Equal("hello", sa.GetString 0)
            Assert.True(sa.IsNull 1)
            Assert.Equal("world", sa.GetString 2)
        | :? StringArray as sa -> // Fallback logic
            Assert.Equal("hello", sa.GetString 0)
            Assert.True(sa.IsNull 1)
        | _ -> failwithf "Unexpected arrow type: %s" (arrow.GetType().Name)

    [<Fact>]
    member _.``Series: Rename`` () =
        use s = Series.create("a", [1;2])
        Assert.Equal("a", s.Name)
        
        s.Rename("b") |> ignore
        Assert.Equal("b", s.Name)

    [<Fact>]
    member _.``Series: Float with Nulls`` () =
        let data = [Some 1.5; None; Some 3.14]
        use s = Series.create("floats", data)
        
        let arrow = s.ToArrow() :?> DoubleArray
        Assert.Equal(1.5, arrow.GetValue(0).Value)
        Assert.True(arrow.IsNull(1))
        Assert.Equal(3.14, arrow.GetValue(2).Value)
    [<Fact>]
    member _.``Interop: DataFrame <-> Series`` () =

        use csv = new TempCsv "name,age\nalice,10\nbob,20"
        let df = DataFrame.ReadCsv csv.Path
                
        use sName = df.Column "name"
        Assert.Equal("name", sName.Name)
        Assert.Equal(2L, sName.Length)
        sName |> pl.showSeries |> ignore

        use sAge = df.Column 1
        Assert.Equal("age", sAge.Name)

        use sAge2 = df.[1]
        Assert.Equal("age", sAge2.Name)

        // Series -> DataFrame
        let dfNew = sAge.ToFrame()
        Assert.Equal(1L, dfNew.Width)
        Assert.Equal(2L, dfNew.Rows)
        Assert.Equal("age", dfNew.ColumnNames.[0])
    [<Fact>]
    member _.``Series: Cast to Categorical`` () =
        let data = ["apple"; "banana"; "apple"; "apple"; "banana"]
        use s = Series.create("fruits", data)
        
        use sCat = s.Cast(DataType.Categorical())
        
        let arrow = sCat.ToArrow()
        
        Assert.IsAssignableFrom<Apache.Arrow.DictionaryArray> arrow |> ignore
        
        let dictArr = arrow :?> Apache.Arrow.DictionaryArray
        
        let indices = dictArr.Indices
        Assert.IsAssignableFrom<Apache.Arrow.UInt32Array> indices |> ignore
        
        let values = dictArr.Dictionary
        Assert.True(values :? Apache.Arrow.StringArray || values :? Apache.Arrow.StringViewArray)
        
        Assert.Equal(2, values.Length)

    [<Fact>]
    member _.``Series: Cast to Decimal (From String)`` () =

        let data = ["1.23"; "4.56"; "7.89"]
        use s = Series.create("money", data)
        
        // String -> Decimal (Precision=10, Scale=2)
        use sDec = s.Cast(DataType.Decimal(10,2))
        
        let arrow = sDec.ToArrow()
        let decArr = arrow :?> Decimal128Array
        
        Assert.Equal(1.23m, decArr.GetValue(0).Value)
        Assert.Equal(4.56m, decArr.GetValue(1).Value) 
        Assert.Equal(7.89m, decArr.GetValue(2).Value)
    [<Fact>]
    member _.``Series: Create Decimal (High Performance)`` () =

        let data = [1.23m; 4.56m; 7.89m] 
        
        use s = Series.create("money", data)

        let arrow = s.ToArrow() :?> Decimal128Array
        
        Assert.Equal(4.56m, arrow.GetValue(1).Value)
    [<Fact>]
    member _.``Scalar Access: Series & DataFrame`` () =
        // Series 
        use s = Series.create("d", [1.23m; 4.56m])
        Assert.Equal(Some 1.23m, s.Decimal 0)
        Assert.Equal(Some 4.56m, s.Decimal 1)
        
        // DataFrame (Redirect)
        use df = DataFrame.create [s]
        Assert.Equal(Some 1.23m, df.Decimal("d", 0))
    [<Fact>]
    member _.``Series: IsNull / IsNotNull`` () =
        // 1, null, 3
        let s = Series.create("a", [Some 1; None; Some 3])

        // IsNull -> [false, true, false]
        let maskNull = s.IsNull()
        Assert.Equal("bool", maskNull.DtypeStr)
        Assert.Equal(Some false, maskNull.Bool 0)
        Assert.Equal(Some true, maskNull.Bool 1)

        // IsNotNull -> [true, false, true]
        let maskNotNull = s.IsNotNull()
        Assert.Equal(Some true, maskNotNull.Bool 0)
        Assert.Equal(Some false, maskNotNull.Bool 1)
    [<Fact>]
    member _.``Series: Dt Extraction`` () =
        // 2023-01-01 10:30:00
        let dt = DateTime(2023, 1, 1, 10, 30, 0)
        let s = Series.create("dates", [dt])

        // Year
        let sYear = s.Dt.Year()
        Assert.Equal(2023, sYear.GetValue<int> 0)

        // Month
        let sMonth = s.Dt.Month()
        Assert.Equal(1, sMonth.GetValue<int> 0)

        // Hour
        let sHour = s.Dt.Hour()
        Assert.Equal(10, sHour.GetValue<int> 0)

    [<Fact>]
    member _.``Series: Dt Manipulation (Offset & Truncate)`` () =
        let dt = DateTime(2023, 1, 1, 10, 30, 45)
        let s = Series.create("dates", [dt])

        // Truncate to 1h -> 10:00:00
        let sTrunc = s.Dt.Truncate(Dur.String "1h")
        let valTrunc = sTrunc.GetValue<DateTime>(0)
        Assert.Equal(DateTime(2023, 1, 1, 10, 0, 0), valTrunc)

        // Offset by 1d -> 2023-01-02
        let sOffset = s.Dt.OffsetBy(Dur.String "1d")
        let valOffset = sOffset.GetValue<DateTime>(0)
        Assert.Equal(DateTime(2023, 1, 2, 10, 30, 45), valOffset)

    [<Fact>]
    member _.``Series: Dt Business Days`` () =
        // 2023-01-06 (Friday)
        let d = DateOnly(2023, 1, 6)
        let s = Series.create("dates", [d])

        // Add 1 Business Day -> Mon 2023-01-09
        let sNextBiz = s.Dt.AddBusinessDays(1)
        let valNext = sNextBiz.GetValue<DateOnly>(0)
        
        Assert.Equal(DateOnly(2023, 1, 9), valNext)

        // Is Business Day
        let sIsBiz = s.Dt.IsBusinessDay()
        Assert.True(sIsBiz.GetValue<bool>(0))
    [<Fact>]
    member _.``Series: Str Basic Ops (Case, Slice, Len)`` () =
        let s = Series.create("txt", ["Hello"; "World"; "Polars"])

        // ToUpper
        let sUpper = s.Str.ToUpper()
        Assert.Equal("HELLO", sUpper.GetValue<string>(0))

        // Slice (Offset 1, Len 2) -> "el", "or", "ol"
        let sSlice = s.Str.Slice(1L, 2UL)
        Assert.Equal("el", sSlice.GetValue<string> 0)
        Assert.Equal("or", sSlice.GetValue<string> 1)
        
        // Len
        let sLen = s.Str.Len()
        Assert.Equal(5u, sLen.GetValue<uint32> 0) // Polars len returns uint32

    [<Fact>]
    member _.``Series: Str Regex & Replace`` () =
        let s = Series.create("txt", ["a1b"; "c2d"])

        // Replace Digit with * (Regex)
        let sRep = s.Str.ReplaceAll("\d", "*", literal=false)
        Assert.Equal("a*b", sRep.GetValue<string> 0)
        Assert.Equal("c*d", sRep.GetValue<string> 1)

        // Contains "b"
        let sHasB = s.Str.Contains "b"
        Assert.True(sHasB.GetValue<bool> 0)
        Assert.False(sHasB.GetValue<bool> 1)

    [<Fact>]
    member _.``Series: Str Split (Returns List)`` () =
        let s = Series.create("csv", ["a,b,c"; "x,y"])
        
        // Split -> List<String>
        let sList = s.Str.Split(",")
        
        // Row 0: ["a", "b", "c"]
        let l0 = sList.GetList<string>(0)
        Assert.Equal<string list>(["a"; "b"; "c"], l0)

        // Row 1: ["x", "y"]
        let l1 = sList.GetList<string>(1)
        Assert.Equal<string list>(["x"; "y"], l1)

    [<Fact>]
    member _.``Series: Str Parsing (ToDate)`` () =
        let s = Series.create("dates", ["2023-01-01"; "2023-12-31"])
        
        // Parse String to Date
        let sDate = s.Str.ToDate("%Y-%m-%d")
        
        Assert.Equal(DateOnly(2023, 1, 1), sDate.GetValue<DateOnly> 0)
        Assert.Equal(DateOnly(2023, 12, 31), sDate.GetValue<DateOnly> 1)

    [<Fact>]
    member _.``Series: Str Strip & Trim`` () =
        let s = Series.create("txt", ["  hello  "; "__world__"])

        // Strip Whitespace
        let sTrim = s.Str.Strip()
        Assert.Equal("hello", sTrim.GetValue<string> 0)

        // Strip custom chars
        let sStripCustom = s.Str.Strip("_")
        Assert.Equal("world", sStripCustom.GetValue<string> 1)
    [<Fact>]
    member _.``Series: List Basic Ops`` () =
        // Data: [[1, 2], [3]]
        let data = [
            {| Vals = [1; 2] |}
            {| Vals = [3] |}
        ]

        let df = DataFrame.ofRecords data
        let s = df.Column "Vals"

        // Len
        let sLen = s.List.Len()
        Assert.Equal(2u, sLen.GetValue<uint32> 0)
        Assert.Equal(1u, sLen.GetValue<uint32> 1)

        // Sum
        let sSum = s.List.Sum()
        Assert.Equal(3, sSum.GetValue<int> 0) // 1+2
        Assert.Equal(3, sSum.GetValue<int> 1) // 3

    [<Fact>]
    member _.``Series: List Concat (Binary Op)`` () =
        // s1: [1], [2]
        let s1 = Series.create("A", [1; 2])
        // s2: [10], [20]
        let s2 = Series.create("B", [10; 20])

        // Concat: A + B -> [[1, 10], [2, 20]]
        let sRes = s1.List.Concat(s2)
        
        // Row 0: [1, 10]
        let l0 = sRes.GetList<int>(0)
        Assert.Equal<int list>([1; 10], l0)

    [<Fact>]
    member _.``Series: List Concat Name Collision`` () =
        let s1 = Series.create("SameName", [1])
        let s2 = Series.create("SameName", [99])

        let sRes = s1.List.Concat s2
        
        // [1, 99]
        let l0 = sRes.GetList<int> 0
        Assert.Equal<int list>([1; 99], l0)
    [<Fact>]
    member _.``Series: Array Aggregations`` () =

        let data = [
            {| Vals = [1; 2; 3] |}
            {| Vals = [4; 5; 6] |}
        ]

        let df = 
            DataFrame.ofRecords(data)
                .WithColumns([
                    pl.col("Vals").Cast(DataType.Array(DataType.Int32, [|3u|]))
                ])

        let s = df.Column "Vals"

        // Row 0: 1+2+3=6
        // Row 1: 4+5+6=15
        let sSum = s.Array.Sum()
        Assert.Equal(6, sSum.GetValue<int> 0)
        Assert.Equal(15, sSum.GetValue<int> 1)

        let sMin = s.Array.Min()
        Assert.Equal(1, sMin.GetValue<int> 0)
        Assert.Equal(4, sMin.GetValue<int> 1)

    [<Fact>]
    member _.``Series: Array Operations (Sort & Get)`` () =
        let data = [
            {| Vals = [3; 1; 2] |}
        ]
        let df = 
            DataFrame.ofRecords(data)
                .WithColumns([
                    pl.col("Vals").Cast(DataType.Array(DataType.Int32, [|3u|]))
                ])
        
        let s = df.Column "Vals"

        // Sort -> [1, 2, 3]
        let sSorted = s.Array.Sort()
        
        let l0 = sSorted.GetList<int> 0
        Assert.Equal<int list>([1; 2; 3], l0)

        let sGet = s.Array.Get 1
        Assert.Equal(1, sGet.GetValue<int> 0)

    [<Fact>]
    member _.``Series: Array Join (String)`` () =
        let data = [
            {| Vals = ["a"; "b"; "c"] |}
        ]
        let df = 
            DataFrame.ofRecords(data)
                .WithColumns([
                    pl.col("Vals").Cast(DataType.Array(DataType.String, [|3u|]))
                ])
        
        let s = df.Column "Vals"

        // Join -> "a-b-c"
        let sJoined = s.Array.Join "-"
        Assert.Equal("a-b-c", sJoined.GetValue<string> 0)
    [<Fact>]
    member _.``Series: Struct Field Access (Heterogeneous)`` () =

        let data = [
            {| ID = 1; Name = "Alice" |}
            {| ID = 2; Name = "Bob"   |}
        ]
        let df = DataFrame.ofRecords data

        let dfStruct = 
            df.Select([
                pl.asStruct([ pl.col "ID"; pl.col "Name" ]).Alias "User"
            ])
        
        let s = dfStruct.Column "User" // Struct<ID: i32, Name: str>

        // Field (ByName)
        let fId = s.Struct.Field "ID"
        Assert.Equal(1, fId.GetValue<int> 0)

        let fName = s.Struct.Field "Name"
        Assert.Equal("Alice", fName.GetValue<string> 0)

        // Field (ByIndex)
        let fIndex1 = s.Struct.Field 1 // Index 1 is Name
        Assert.Equal("Bob", fIndex1.GetValue<string> 1)

    [<Fact>]
    member _.``Series: Struct Rename & Json`` () =
        let df = 
            DataFrame.ofRecords([ {| A = 10; B = 20 |} ])
                .Select([
                    pl.asStruct([ pl.col "A"; pl.col "B" ]).Alias "Data"
                ])
        let s = df.Column "Data"

        // Rename Fields
        // A -> X, B -> Y
        let sRenamed = s.Struct.RenameFields [|"X"; "Y"|]

        let valX = sRenamed.Struct.Field("X")
        Assert.Equal(10, valX.GetValue<int> 0)

        // Json Encode
        let sJson = sRenamed.Struct.JsonEncode()
        let jsonStr = sJson.GetValue<string> 0
        
        Assert.Contains("X", jsonStr)
        Assert.Contains("10", jsonStr)
        Assert.Contains("Y", jsonStr)
        Assert.Contains("20", jsonStr)
    [<Fact>]
    member _.``Series: Trig & Hyperbolic`` () =
        // 准备数据: [0, PI/2, PI]
        let data = [0.0; System.Math.PI / 2.0; System.Math.PI]
        let s = Series.create("angle", data)

        // Sin(0)=0, Sin(PI/2)=1, Sin(PI)~0
        let sSin = s.Sin()
        Assert.Equal(0.0, sSin.GetValue<double> 0, 5)
        Assert.Equal(1.0, sSin.GetValue<double> 1, 5)

        let sRoundTrip = sSin.ArcSin()
        Assert.Equal(0.0, sRoundTrip.GetValue<double> 0, 5)
        Assert.Equal(System.Math.PI / 2.0, sRoundTrip.GetValue<double> 1, 5)

        // Cosh(0) = 1
        let sCosh = s.Cosh()
        Assert.Equal(1.0, sCosh.GetValue<double> 0, 5)
    [<Fact>]
    member _.``Series: Statistics (Std, Var, Quantile)`` () =
        let s = Series.create("vals", [1.0; 2.0; 3.0])

        // Std (ddof=1): sqrt((1+0+1)/2) = 1.0
        Assert.Equal(1.0, s.Std().GetValue<double> 0)

        // Var (ddof=1): 1.0
        Assert.Equal(1.0, s.Var().GetValue<double> 0)

        // Median: 2.0
        Assert.Equal(2.0, s.Median().GetValue<double> 0)

        // Quantile (0.5) == Median
        Assert.Equal(2.0, s.Quantile(0.5).GetValue<double> 0)

    [<Fact>]
    member _.``Series: FillNull (Scalar vs Series)`` () =
        // s1: [1, null, 3]
        let s1 = Series.create("A", [Some 1; None; Some 3])
        
        // Fill with Scalar (0)
        let sFilledScalar = s1.FillNull(0)
        // [1, 0, 3]
        Assert.Equal(0, sFilledScalar.GetValue<int> 1)

        // Fill with Series
        // s2: [10, 20, 30]
        let s2 = Series.create("B", [10; 20; 30])
        
        // [1, 20, 3]
        let sFilledSeries = s1.FillNull s2
        
        Assert.Equal(1, sFilledSeries.GetValue<int> 0)
        Assert.Equal(20, sFilledSeries.GetValue<int> 1) // Filled from s2
        Assert.Equal(3, sFilledSeries.GetValue<int> 2)

    [<Fact>]
    member _.``Series: FillNan`` () =
        // [1.0, NaN, 3.0]
        let s = Series.create("vals", [1.0; Double.NaN; 3.0])
        
        // Fill Nan with 0.0
        let sNoNan = s.FillNan 0.0
        
        Assert.Equal(0.0, sNoNan.GetValue<double> 1)
    [<Fact>]
    member _.``Series: Shift and Diff`` () =
        // [10, 20, 30]
        let s = Series.create("vals", [10; 20; 30])

        // Shift(1) -> [null, 10, 20]
        let sShift = s.Shift(1)
        Assert.True(sShift.IsNullAt 0)
        Assert.Equal(10, sShift.GetValue<int> 1)

        // Diff(1) -> [null, 10, 10]
        // 20-10=10, 30-20=10
        let sDiff = s.Diff(1)
        Assert.True(sDiff.IsNullAt 0)
        Assert.Equal(10, sDiff.GetValue<int> 1)
        Assert.Equal(10, sDiff.GetValue<int> 2)

    [<Fact>]
    member _.``Series: Forward Fill`` () =
        // [1, null, null, 4]
        let s = Series.create("vals", [Some 1; None; None; Some 4])

        // FFill -> [1, 1, 1, 4]
        let sFill = s.ForwardFill()
        
        Assert.Equal(1, sFill.GetValue<int> 1)
        Assert.Equal(1, sFill.GetValue<int> 2)
        Assert.Equal(4, sFill.GetValue<int> 3)

        // FFill(limit=1) -> [1, 1, null, 4]
        let sLimit = s.ForwardFill(limit=1)
        Assert.Equal(1, sLimit.GetValue<int> 1)
        Assert.True(sLimit.IsNullAt 2)

    [<Fact>]
    member _.``Series: Rolling Sum (Index Based)`` () =
        // [1, 2, 3, 4]
        let s = Series.create("vals", [1; 2; 3; 4])

        // Window size "2i" (2 rows based on index)
        // Rolling Sum:
        // 0: 1 (null? depends on min_periods) -> min_periods=1 -> 1
        // 1: 1+2=3
        // 2: 2+3=5
        // 3: 3+4=7
        let sRoll = s.RollingSum(Dur.String "2i", minPeriod=1)
        
        Assert.Equal(1, sRoll.GetValue<int> 0)
        Assert.Equal(3, sRoll.GetValue<int> 1)
        Assert.Equal(5, sRoll.GetValue<int> 2)
        Assert.Equal(7, sRoll.GetValue<int> 3)
    

    // ==========================================
    // 1. 测试标准 Option<'T> (引用类型包装)
    // ==========================================

    [<Fact>]
    member _.``Series.ofOptionSeq: Int32 (Fast Path)`` () =
        // 准备数据: [Some 0, Some 1, Some 2, Some 3, None, Some 5 ...]
        let data = 
            Array.init count (fun i -> 
                if i % 5 = 4 then None else Some i
            )
        
        let s = Series.ofOptionSeq("Ints", data)

        Assert.Equal("Ints", s.Name)
        Assert.Equal(int64 count, s.Length)

        Assert.Equal(20000L, s.NullCount)

        Assert.Equal(0, s.GetValue<int>(0))
        Assert.True(s.IsNullAt 4)

    [<Fact>]
    member _.``Series.ofOptionSeq: String (Pointer Unwrapped)`` () =
        let data = 
            Array.init count (fun i -> 
                if i % 5 = 4 then None else Some $"Str_{i}"
            )
        
        let s = Series.ofOptionSeq("Strs", data)

        Assert.Equal(int64 count, s.Length)
        Assert.Equal(20000L, s.NullCount)
        Assert.Equal("Str_0", s.GetValue<string>(0))
        Assert.True(s.IsNullAt 4)

    [<Fact>]
    member _.``Series.ofOptionSeq: DateTime (Turbocharged)`` () =
        let start = DateTime(2023, 1, 1)
        let data = 
            Array.init count (fun i -> 
                if i % 5 = 4 then None else Some (start.AddDays(float i))
            )
        
        let s = Series.ofOptionSeq("Dates", data)

        Assert.Equal(int64 count, s.Length)
        Assert.Equal(20000L, s.NullCount)

        Assert.Equal(start, s.GetValue<DateTime>(0))

    [<Fact>]
    member _.``Series.ofOptionSeq: Decimal (Scale Auto-Detect)`` () =

        let data = 
            Array.init count (fun i -> 
                if i % 5 = 4 then None 
                else Some (decimal i + 0.5m) 
            )
        
        let s = Series.ofOptionSeq("Decimals", data)

        Assert.Equal(int64 count, s.Length)
        Assert.Equal(0.5m, s.GetValue<decimal> 0)

    [<Fact>]
    member _.``Series.ofVOptionSeq: Int64 (Zero Allocation Path)`` () =
        let data = 
            Array.init count (fun i -> 
                if i % 5 = 4 then ValueNone else ValueSome (int64 i * 1000L)
            )
        
        let s = Series.ofVOptionSeq("BigInts", data)

        Assert.Equal(int64 count, s.Length)
        Assert.Equal(20000L, s.NullCount)
        Assert.Equal(0L, s.GetValue<int64> 0)
        Assert.Equal(1000L, s.GetValue<int64> 1)

    [<Fact>]
    member _.``Series.ofVOptionSeq: Bool (Bitpacked)`` () =

        let data = 
            Array.init count (fun i -> 
                if i % 5 = 4 then ValueNone 
                else ValueSome (i % 2 = 0)
            )
        
        let s = Series.ofVOptionSeq("Bools", data)

        Assert.Equal(int64 count, s.Length)
        Assert.True(s.GetValue<bool> 0)  // 0 is even -> true
        Assert.False(s.GetValue<bool> 1) // 1 is even -> false
        Assert.True(s.IsNullAt 4)

    [<Fact>]
    member _.``Series.ofVOptionSeq: TimeOnly`` () =
        let start = TimeOnly(12, 0, 0)
        let data = 
            Array.init count (fun i -> 
                if i % 5 = 4 then ValueNone 
                else ValueSome (start.AddMinutes(float i))
            )
        
        let s = Series.ofVOptionSeq("Times", data)
        
        Assert.Equal(int64 count, s.Length)
        Assert.Equal(start, s.GetValue<TimeOnly> 0)
    [<Fact>]
    member _.``Test Decimal Matrix with Auto-Scaling in FSharp``() =

        let data = array2D [
            [ 1.1M;  2.22M; 3.333M ]   // Row 0
            [ 100M;  0.01M; -1.5M  ]   // Row 1
        ]
        
        using (Series.ofArray2D("decimal_matrix", data)) (fun s ->
            
            // 3. 验证
            Assert.Equal(2L, s.Length)
            
            let result = s.ToArray<decimal[]>()
            
            Assert.Equal(1.1M, result.[0].[0])
            Assert.Equal(2.22M, result.[0].[1])
            Assert.Equal(3.333M, result.[0].[2])
            
            Assert.Equal(0.01M, result.[1].[1])
        )

    [<Fact>]
    member _.``Test Primitive Matrix (double) Performance Path``() =

        let data = array2D [
            [ 1.0; 2.0 ]
            [ 3.0; 4.0 ]
            [ 5.0; 6.0 ]
        ]
        
        using (Series.ofArray2D("double_matrix", data)) (fun s ->
            Assert.Equal(3L, s.Length)
            let result = s.ToArray<double[]>()
            Assert.Equal(6.0, result.[2].[1])
        )

    [<Fact>]
    member _.``Test Int128 Matrix with Byte Swap``() =
        let val1 = Int128.MaxValue
        let val2 = Int128.One
        let data = array2D [ [ val1; val2 ] ]
        
        let s = Series.ofArray2D("i128_matrix", data)
        s.Show()
        Assert.Throws<NotSupportedException>(fun () -> s.ToArray() |> ignore)

    [<Fact>]
    member _.``Test Decimal Matrix Overflow in FSharp``() =
        let huge = Decimal.MaxValue
        let tiny = 0.0000000000000000000000000001M // Scale 28
        
        let data = array2D [ [ huge ]; [ tiny ] ]
        
        Assert.Throws<OverflowException>(fun () -> 
            Series.ofArray2D("overflow_test", data) |> ignore
        )

    [<Fact>]
    [<Trait("Series", "AsReadOnlySpan")>]
    member _.``ToReadOnlySpan - Valid 1D Numeric Series - Returns ZeroCopy Span`` () =

        use series = Series.create("float_features", [| 1.5f; 2.5f; 3.5f; 4.5f |])

        let span = series.AsReadOnlySpan<float32>()

        Assert.Equal(4, span.Length)
        Assert.Equal(1.5f, span.[0])
        Assert.Equal(2.5f, span.[1])
        Assert.Equal(3.5f, span.[2])
        Assert.Equal(4.5f, span.[3])

    [<Fact>]
    [<Trait("Series", "AsReadOnlySpanException")>]
    member _.``ToReadOnlySpan - String Series - Throws FSharp Layer Exception`` () =

        use series = Series.create("string_tags", [| "hello"; "polars"; "fsharp" |])

        let ex = Assert.Throws<InvalidOperationException>(fun () -> 
            let _ = series.AsReadOnlySpan<int>()
            ()
        )
        Assert.Contains("Cannot create Tensor/Span from a String Series", ex.Message)
        Assert.Contains("Machine learning models and Spans require numeric inputs", ex.Message)

    [<Fact>]
    [<Trait("Series", "TensorSpanNull")>]
    member _.``ToReadOnlySpan - Nullable Numeric Series - Throws Core Layer Exception`` () =
        let dataWithNull = [| Some 1; None; Some 2 |]
        use series = Series.create("dirty_data", dataWithNull)

        let ex = Assert.Throws<InvalidOperationException>(fun () -> 

            let _ = series.AsReadOnlySpan<int>()
            ()
        )
        Assert.Contains("Cannot extract Tensor memory: contains null values.", ex.Message)
    [<Fact>]
    [<Trait("Series", "AsTensorSpan")>]
    member _.``AsTensorSpan - 1D Series - Promotes To Column Vector`` () =

        let data = [| 10; 20; 30 |]
        use series = Series.create("1d_features", data)

        let tensor = series.AsTensorSpan<int>()

        Assert.Equal(2, tensor.Rank)
        
        Assert.Equal(3, int tensor.Lengths.[0]) 
        Assert.Equal(1, int tensor.Lengths.[1]) 
        
        Assert.Equal(10, tensor.Item(ReadOnlySpan<nativeint> [| 0n; 0n |]))
        Assert.Equal(20, tensor.Item(ReadOnlySpan<nativeint> [| 1n; 0n |]))
        Assert.Equal(30, tensor.Item(ReadOnlySpan<nativeint> [| 2n; 0n |]))


    [<Fact>]
    [<Trait("Series", "AsTransposedTensorSpan")>]
    member _.``AsTransposedTensorSpan - 2D Series - Returns Transposed View`` () =
        let matrix = array2D [
            [ 1.1f; 1.2f ]
            [ 2.1f; 2.2f ]
            [ 3.1f; 3.2f ]
        ]
        use series = Series.ofArray2D("embeddings", matrix)

        let transposed = series.AsTransposedTensorSpan<float32>()

        Assert.Equal(2, transposed.Rank)
        Assert.Equal(2, int transposed.Lengths.[0])
        Assert.Equal(3, int transposed.Lengths.[1])

        Assert.Equal(2.1f, transposed.Item(ReadOnlySpan<nativeint> [| 0n; 1n |]))
        Assert.Equal(1.2f, transposed.Item(ReadOnlySpan<nativeint> [| 1n; 0n |]))


    [<Fact>]
    [<Trait("Series", "AsTensorSpan3D")>]
    member _.``FromTensor and AsTensorSpan - 3D Shape - Closed Loop`` () =
        let flatData = [| 1.0f .. 8.0f |]

        let shapeArray = [| 2n; 2n; 2n |] 
        let shapeSpan = ReadOnlySpan<nativeint> shapeArray
        
        let tensorIn = ReadOnlyTensorSpan<float32>(flatData, shapeSpan)

        use series = Series.ofTensor("image_batch", tensorIn)
        Assert.Equal(2L, series.Length) 

        let tensorOut = series.AsTensorSpan<float32> shapeSpan

        Assert.Equal(3, tensorOut.Rank)
        Assert.Equal(2, int tensorOut.Lengths.[0])
        Assert.Equal(2, int tensorOut.Lengths.[1])
        Assert.Equal(2, int tensorOut.Lengths.[2])

        Assert.Equal(8.0f, tensorOut.Item(ReadOnlySpan<nativeint> [| 1n; 1n; 1n |]))
    [<Fact>]
    [<Trait("Series", "AsTensor")>]
    member _.``AsTensor - 1D Series - Performs Deep Copy And Promotes To 2D`` () =
        let data = [| 10; 20; 30; 40 |]
        let series = Series.create("heap_tensor_1d", data)

        let heapTensor = series.AsTensor<int>()

        (series :> IDisposable).Dispose()

        Assert.Equal(2, heapTensor.Rank)
        Assert.Equal(4, int heapTensor.Lengths.[0]) // Rows
        Assert.Equal(1, int heapTensor.Lengths.[1]) // Cols

        Assert.Equal(10, heapTensor.Item(ReadOnlySpan<nativeint> [| 0n; 0n |]))
        Assert.Equal(40, heapTensor.Item(ReadOnlySpan<nativeint> [| 3n; 0n |]))


    [<Fact>]
    [<Trait("Series", "AsTensor3D")>]
    member _.``AsTensor - With Shape - Performs Deep Copy of 3D`` () =
        let flatData = [| 1f; 2f; 3f; 4f; 5f; 6f; 7f; 8f |]
        let series = Series.create("heap_tensor_3d", flatData)

        let shape3D = [| 2n; 2n; 2n |]
        let shapeSpan = ReadOnlySpan<nativeint> shape3D

        let heapTensor = series.AsTensor<float32> shapeSpan

        (series :> IDisposable).Dispose()

        Assert.Equal(3, heapTensor.Rank)
        Assert.Equal(2, int heapTensor.Lengths.[0])
        Assert.Equal(2, int heapTensor.Lengths.[1])
        Assert.Equal(2, int heapTensor.Lengths.[2])

        Assert.Equal(8f, heapTensor.Item(ReadOnlySpan<nativeint> [| 1n; 1n; 1n |]))


    [<Fact>]
    [<Trait("Series", "AsUnmanagedTensor")>]
    member _.``AsUnmanagedTensor - Returns Valid Memory For FFI`` () =

        let matrix = array2D [
            [ 1.1f; 1.2f; 1.3f ]
            [ 2.1f; 2.2f; 2.3f ]
        ]
        
        use series = Series.ofArray2D("ffi_matrix", matrix)

        let struct (ptr, shape) = series.AsDangerousUnmanagedTensor<float32>()

        Assert.Equal(2, shape.Length)
        Assert.Equal(2L, shape.[0]) 
        Assert.Equal(3L, shape.[1]) 

        let totalElements = int (shape.[0] * shape.[1])
        
        let ptrVoid = ptr.ToPointer() 
        let nativeSpan = ReadOnlySpan<float32>(ptrVoid, totalElements)

        Assert.Equal(1.1f, nativeSpan.[0])
        Assert.Equal(1.3f, nativeSpan.[2]) 
        Assert.Equal(2.1f, nativeSpan.[3])
        Assert.Equal(2.3f, nativeSpan.[5]) 