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
        sName |> Series.show |> ignore

        use sAge = df.Column 1
        Assert.Equal("age", sAge.Name)

        use sAge2 = df.[1]
        Assert.Equal("age", sAge2.Name)

        // Series -> DataFrame
        let dfNew = sAge.ToFrame()
        Assert.Equal(1L, dfNew.Width)
        Assert.Equal(2L, dfNew.Height)
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
        Assert.Equal(Some 1.23m, s.GetValueOption<decimal>(0))
        Assert.Equal(Some 4.56m, s.GetValueOption<decimal>(1))

        // DataFrame (Redirect)
        use df = DataFrame.create [s]
        Assert.Equal(Some 1.23m, df.["d"].GetValueOption<decimal>(0))
    [<Fact>]
    member _.``Series: IsNull / IsNotNull`` () =
        // 1, null, 3
        let s = Series.create("a", [Some 1; None; Some 3])

        // IsNull -> [false, true, false]
        let maskNull = s.IsNull()
        Assert.Equal("bool", maskNull.DtypeStr)
        Assert.Equal(Some false, maskNull.GetValueOption<bool>(0))
        Assert.Equal(Some true, maskNull.GetValueOption<bool>(1))

        // IsNotNull -> [true, false, true]
        let maskNotNull = s.IsNotNull()
        Assert.Equal(Some true, maskNotNull.GetValueOption<bool>(0))
        Assert.Equal(Some false, maskNotNull.GetValueOption<bool>(1))
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
        let sUpper = s.Str.ToUppercase()
        Assert.Equal("HELLO", sUpper.GetValue<string>(0))

        // Slice (Offset 1, Len 2) -> "el", "or", "ol"
        let sSlice = s.Str.Slice(1L, 2UL)
        Assert.Equal("el", sSlice.GetValue<string> 0)
        Assert.Equal("or", sSlice.GetValue<string> 1)

        // Len
        let sLen = s.Str.LenBytes()
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
        let sDate = s.Str.ToDate "%Y-%m-%d"

        Assert.Equal(DateOnly(2023, 1, 1), sDate.GetValue<DateOnly> 0)
        Assert.Equal(DateOnly(2023, 12, 31), sDate.GetValue<DateOnly> 1)

    [<Fact>]
    member _.``Series: Str Strip & Trim`` () =
        let s = Series.create("txt", ["  hello  "; "__world__"])

        // Strip Whitespace
        let sTrim = s.Str.StripChars()
        Assert.Equal("hello", sTrim.GetValue<string> 0)

        // Strip custom chars
        let sStripCustom = s.Str.StripChars("_")
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
        let sRes = s1.List.Concat s2

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
        Assert.Equal(Some 1.0, s.Std())

        // Var (ddof=1): 1.0
        Assert.Equal(Some 1.0, s.Var())

        // Median: 2.0
        Assert.Equal(Some 2.0, s.Median())

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

    [<Fact>]
    [<Trait("Series", "Fold")>]
    member _.``Series.fold correctly accumulates numeric values via primitive fast-path`` () =
        // Arrange: primitive int32 array triggers the zero-copy Arrow PrimitiveArray path
        let s = pl.series "ints" [| 1; 2; 3; 4; 5 |]

        // Act: sum and product using fold
        let sum = s |> Series.fold (+) 0
        let product = s |> Series.fold (*) 1

        // Assert
        Assert.Equal(15, sum)
        Assert.Equal(120, product)

    [<Fact>]
    [<Trait("Series", "Fold")>]
    member _.``Series.fold correctly accumulates non-primitive values via fallback path`` () =
        // Arrange: string Series uses the fallback path with GetValue<'T>(i)
        let s = pl.series "words" [| "Polars"; "."; "NET"; "!"; "F#" |]

        // Act
        let concatenated = s |> Series.fold (fun acc elem -> acc + elem) ""

        // Assert
        Assert.Equal("Polars.NET!F#", concatenated)

    [<Fact>]
    [<Trait("Series", "Reduce")>]
    member _.``Series.reduce correctly reduces elements without initial seed`` () =
        // Arrange
        let s = pl.series "values" [| 12; 45; 68; 23; 99; 5 |]

        // Act: find maximum and minimum
        let maxVal = s |> Series.reduce max
        let minVal = s |> Series.reduce min

        // Assert
        Assert.Equal(99, maxVal)
        Assert.Equal(5, minVal)

    [<Fact>]
    [<Trait("Series", "Reduce")>]
    member _.``Series.reduce throws InvalidOperationException when Series is empty`` () =
        // Arrange
        let emptySeries = pl.series "empty" Array.empty<int>

        // Act & Assert
        Assert.Throws<InvalidOperationException>(fun () ->
            emptySeries |> Series.reduce (+) |> ignore
        ) |> ignore

    [<Fact>]
    [<Trait("Series", "Scan")>]
    member _.``Series.scan generates a new Series with intermediate accumulated states`` () =
        // Arrange
        let s = pl.series "deltas" [| 10; 20; 30; 40 |]

        // Act: prefix sum scan
        let cumulative = s |> Series.scan (+) 0

        // Assert
        Assert.Equal("deltas_scanned", cumulative.Name)
        Assert.Equal(4L, cumulative.Length)
        Assert.Equal<int>(10, cumulative.GetValue<int>(0))
        Assert.Equal<int>(30, cumulative.GetValue<int>(1))
        Assert.Equal<int>(60, cumulative.GetValue<int>(2))
        Assert.Equal<int>(100, cumulative.GetValue<int>(3))

    [<Fact>]
    [<Trait("Series", "Scan")>]
    member _.``Series.scan correctly handles state accumulation over string Series`` () =
        // Arrange
        let s = pl.series "chars" [| "a"; "b"; "c" |]

        // Act
        let scanned = s |> Series.scan (fun acc item -> acc + item) ""

        // Assert
        Assert.Equal(3L, scanned.Length)
        Assert.Equal<string>("a", scanned.GetValue<string>(0))
        Assert.Equal<string>("ab", scanned.GetValue<string>(1))
        Assert.Equal<string>("abc", scanned.GetValue<string>(2))

    [<Fact>]
    [<Trait("Series", "Unfold")>]
    member _.``Series.unfold generates Fibonacci sequence capped by maxLen`` () =
        // Generator state: (prev, curr)
        let fibGenerator (a, b) =
            Some (a, (b, a + b))

        // Act: Generate first 7 Fibonacci numbers
        let s = Series.unfold fibGenerator (0, 1) 7 "fib"

        // Assert
        Assert.Equal("fib", s.Name)
        Assert.Equal(7L, s.Length)
        let expected = [| 0; 1; 1; 2; 3; 5; 8 |]
        Assert.Equal<int[]>(expected, s.ToArray<int>())

    [<Fact>]
    [<Trait("Series", "Unfold")>]
    member _.``Series.unfold terminates early when generator returns None`` () =
        // Countdown from 5 to 1, terminates when reaching 0
        let countdownGen n =
            if n > 0 then Some (sprintf "T-%d" n, n - 1)
            else None

        // Act: maxLen is 10, but generator should stop at 5 items
        let s = Series.unfold countdownGen 5 10 "countdown"

        // Assert
        Assert.Equal(5L, s.Length)
        Assert.Equal<string>("T-5", s.GetValue<string>(0))
        Assert.Equal<string>("T-1", s.GetValue<string>(4))

    [<Fact>]
    [<Trait("Series", "Unfold")>]
    member _.``Series.unfold handles zero maxLen properly`` () =
        let s = Series.unfold (fun n -> Some (n, n + 1)) 1 0 "empty"

        Assert.Equal(0L, s.Length)
        Assert.Equal("empty", s.Name)

    [<Fact>]
    [<Trait("Series", "ZipWith")>]
    member _.``Series.zipWith performs ternary selection matching Polars native behavior`` () =
        // Arrange
        let s1 = pl.series "a" [| 10; 20; 30; 40 |]
        let s2 = pl.series "b" [| 99; 88; 77; 66 |]
        let mask = pl.series "m" [| true; false; true; false |]

        // Act: if mask then s1 else s2
        let result = s1 |> Series.zipWith mask s2

        // Assert
        Assert.Equal(4L, result.Length)
        Assert.Equal<int>(10, result.GetValue<int>(0))
        Assert.Equal<int>(88, result.GetValue<int>(1))
        Assert.Equal<int>(30, result.GetValue<int>(2))
        Assert.Equal<int>(66, result.GetValue<int>(3))

    [<Fact>]
    [<Trait("Series", "Zip")>]
    member _.``Series.zip combines two Series into an array of tuples`` () =
        // Arrange
        let names = pl.series "names" [| "Alice"; "Bob"; "Charlie" |]
        let scores = pl.series "scores" [| 85; 92; 78 |]

        // Act
        let pairs = names |> Series.zip scores

        // Assert
        Assert.Equal(3, pairs.Length)
        Assert.Equal((Some "Alice", Some 85), pairs.[0])
        Assert.Equal((Some "Bob", Some 92), pairs.[1])
        Assert.Equal((Some "Charlie", Some 78), pairs.[2])

    [<Fact>]
    [<Trait("Series", "Zip")>]
    member _.``Series.zip handles null values correctly`` () =
        // Arrange
        let names = pl.series "names" [| Some "Alice"; None; Some "Charlie" |]
        let scores = pl.series "scores" [| Some 85; Some 92; None |]

        // Act
        let pairs = names |> Series.zip scores

        // Assert
        Assert.Equal(3, pairs.Length)
        Assert.Equal((Some "Alice", Some 85), pairs.[0])
        Assert.Equal((None, Some 92), pairs.[1])
        Assert.Equal((Some "Charlie", None), pairs.[2])
    [<Fact>]
    [<Trait("Series", "Choose")>]
    member _.``Series.choose filters and maps elements simultaneously`` () =
        let s = pl.series "nums" [| 1; 2; 3; 4; 5; 6 |]

        // Keep even numbers and convert them to formatted labels
        let evens =
            s |> Series.choose (fun x ->
                if x % 2 = 0 then Some (sprintf "Even_%d" x)
                else None)

        Assert.Equal(3L, evens.Length)
        Assert.Equal<string>("Even_2", evens.GetValue<string>(0))
        Assert.Equal<string>("Even_4", evens.GetValue<string>(1))
        Assert.Equal<string>("Even_6", evens.GetValue<string>(2))

    [<Fact>]
    [<Trait("Series", "Exists")>]
    member _.``Series.exists and Series.existsExpr verify predicate matching`` () =
        let s = pl.series "data" [| 10; 25; 30; 45 |]

        // F# closure test (with early short-circuit)
        Assert.True(s |> Series.exists (fun x -> x > 40))
        Assert.False(s |> Series.exists (fun x -> x > 100))

        // Native Polars Expr test
        Assert.True(s |> Series.existsSeries (s .> 40))
        Assert.False(s |> Series.existsSeries (s .> 100))

    [<Fact>]
    [<Trait("Series", "Forall")>]
    member _.``Series.forall and Series.forallExpr verify universal predicate matching`` () =
        let s = pl.series "positives" [| 2; 4; 6; 8 |]

        // F# closure test (with early short-circuit)
        Assert.True(s |> Series.forall (fun x -> x % 2 = 0))
        Assert.False(s |> Series.forall (fun x -> x > 5))

        // Native Series test
        Assert.True(s |> Series.forallSeries (s .> 0))
        Assert.False(s |> Series.forallSeries (s .> 5))

    [<Fact>]
    [<Trait("Series", "PairwiseMap")>]
    member _.``Series.pairwiseMap computes delta between consecutive rows`` () =
        let s = pl.series "prices" [| 100.0; 105.0; 102.0; 110.0 |]

        // Compute step deltas (x_{i+1} - x_i)
        let deltas = s |> Series.pairwiseMap (fun (curr:double) (next:double) -> next - curr)

        Assert.Equal(3L, deltas.Length)
        Assert.Equal(5.0, deltas.GetValue<double>(0))
        Assert.Equal(-3.0, deltas.GetValue<double>(1))
        Assert.Equal(8.0, deltas.GetValue<double>(2))

    [<Fact>]
    [<Trait("Series", "PairwiseMap")>]
    member _.``Series.pairwiseMap on short series yields empty series`` () =
        let single = pl.series "single" [| 42 |]
        let empty = pl.series "empty" Array.empty<int>

        let resSingle = single |> Series.pairwiseMap (+)
        let resEmpty = empty |> Series.pairwiseMap (+)

        Assert.Equal(0L, resSingle.Length)
        Assert.Equal(0L, resEmpty.Length)

    [<Fact>]
    [<Trait("Series", "TryFind")>]
    member _.``Series.tryFind and tryFindIndex handle values and skip nulls safely`` () =
        // Series with null values: [null, 15, 30, null, 45]
        let s = pl.series "items" [| None; Some 15; Some 30; None; Some 45 |]

        // Act: find first multiple of 10
        let foundVal = s |> Series.tryFind (fun x -> x % 10 = 0)
        let foundIdx = s |> Series.tryFindIndex (fun x -> x % 10 = 0)

        // Assert
        Assert.Equal(Some 30, foundVal)
        Assert.Equal(Some 2L, foundIdx)

        // Act: element not present
        let notFound = s |> Series.tryFind (fun x -> x > 100)
        let notFoundIdx = s |> Series.tryFindIndex (fun x -> x > 100)

        Assert.Equal(None, notFound)
        Assert.Equal(None, notFoundIdx)

    [<Fact>]
    [<Trait("Series", "TryFind")>]
    member _.``Series.tryFindV returns ValueOption without heap allocation`` () =
        let s = pl.series "values" [| None; Some 100; Some 200 |]

        let hit = s |> Series.tryFindV (fun x -> x > 150)
        let miss = s |> Series.tryFindV (fun x -> x > 500)

        Assert.Equal(ValueSome 200, hit)
        Assert.Equal(ValueNone, miss)

    [<Fact>]
    [<Trait("Series", "Append")>]
    member _.``Series.Append and its operator preserve original Series immutability`` () =
        // Arrange
        let s1 = pl.series "part1" [| 1; 2; 3 |]
        let s2 = pl.series "part2" [| 4; 5 |]

        // Act: Immutable append using @ operator
        let s3 = s1 .@ s2

        // Assert
        // s1 must remain untouched (length = 3)
        Assert.Equal(3L, s1.Length)
        Assert.Equal(2L, s2.Length)

        // Combined Series must have full length and correct chunk aggregation
        Assert.Equal(5L, s3.Length)
        Assert.Equal(1, s3.GetValue<int>(0L))
        Assert.Equal(5, s3.GetValue<int>(4L))

    [<Fact>]
    [<Trait("Series", "Append")>]
    member _.``Series.AppendMut alters original Series in place`` () =
        // Arrange
        let s1 = pl.series "mut" [| 10; 20 |]
        let s2 = pl.series "other" [| 30 |]

        // Act
        s1.AppendInplace s2

        // Assert
        Assert.Equal(3L, s1.Length)
        Assert.Equal(30, s1.GetValue<int>(2L))
    [<Fact>]
    [<Trait("Series", "Filter")>]
    member _.``Series.Filter with predicate Series`` () =
        let s = pl.series "ints" [| 1; 2; 3; 4; 5 |]
        let predicate = pl.series "predicate" [| true; false; true; false; true |]
        use filtered = s |> Series.filter predicate
        Assert.Equal(3L, filtered.Length)
        Assert.Equal(1, filtered.GetValue<int>(0L))
        Assert.Equal(5, filtered.GetValue<int>(2L))
    [<Fact>]
    [<Trait("Series", "Enumerator")>]
    member _.``Series.As iterates over primitive integers correctly`` () =
        let expected = [| 10; 20; 30; 40; 50 |]
        use s = pl.series "integers" expected

        let actual = ResizeArray<int>()
        for v in s.As<int>() do
            actual.Add(v)

        Assert.Equal<int seq>(expected, actual)

    [<Fact>]
    [<Trait("Series", "Enumerator")>]
    member _.``Series.As handles nullable types with FSharpOption`` () =
        let data = [| Some 100L; None; Some 200L; None; Some 300L |]
        use s = pl.series "nullable_int64" data

        let actual = ResizeArray<int64 option>()
        for v in s.As<int64 option>() do
            actual.Add(v)

        Assert.Equal<int64 option seq>(data, actual)

    [<Fact>]
    [<Trait("Series", "Enumerator")>]
    member _.``Series.As handles nullable types with ValueOption`` () =
        let data = [| ValueSome 1.5; ValueNone; ValueSome 3.5 |]
        use s = pl.series "voption_float" data

        let actual = ResizeArray<double voption>()
        for v in s.As<double voption>() do
            actual.Add(v)

        Assert.Equal<double voption seq>(data, actual)

    [<Fact>]
    [<Trait("Series", "Enumerator")>]
    member _.``Series.As supports 16-byte Guid with zero-allocation`` () =
        let g1 = Guid.NewGuid()
        let g2 = Guid.NewGuid()
        let g3 = Guid.Empty
        let expected = [| g1; g2; g3 |]
        use s = pl.series "guids" expected

        let actual = ResizeArray<Guid>()
        for g in s.As<Guid>() do
            actual.Add(g)

        Assert.Equal<Guid seq>(expected, actual)

    [<Fact>]
    [<Trait("Series", "Enumerator")>]
    member _.``Series.As supports strings and Categorical/Enum mapping`` () =
        let expected = [| "alpha"; "beta"; "gamma" |]
        use s = pl.series "strings" expected

        let actual = ResizeArray<string>()
        for str in s.As<string>() do
            actual.Add(str)

        Assert.Equal<string seq>(expected, actual)

    [<Fact>]
    [<Trait("Series", "Enumerator")>]
    member _.``Series.As iterates empty series without failure`` () =
        let expected: int array = [||]
        use s = pl.series "empty" expected

        let actual = ResizeArray<int>()
        for v in s.As<int>() do
            actual.Add(v)

        Assert.Empty(actual)
    [<Fact>]
    [<Trait("Series", "FilterWith")>]
    member _.``Series filterWith filters primitive integers using managed predicate`` () =
        // Arrange: Create a Series with sequential numbers
        use s = pl.series "numbers" [| 1; 2; 3; 4; 5; 6; 7; 8; 9; 10 |]

        // Act: Filter even numbers greater than 4 via pipeline
        use result =
            s
            |> Series.filterWith (fun (x: int) -> x > 4 && x % 2 = 0)

        // Assert: Verify length and filtered elements
        Assert.Equal(3L, result.Length)
        Assert.Equal("numbers", result.Name)
        Assert.Equal(6, result.GetValue<int>(0L))
        Assert.Equal(8, result.GetValue<int>(1L))
        Assert.Equal(10, result.GetValue<int>(2L))

    [<Fact>]
    [<Trait("Series", "FilterWith")>]
    member _.``Series filterWith handles string transformations and complex predicates`` () =
        // Arrange: Create a string Series
        use s = pl.series "fruits" [| "apple"; "banana"; "apricot"; "cherry"; "avocado" |]

        // Act: Filter strings starting with 'a' and longer than 5 chars
        use result =
            s.FilterWith<string>(fun name ->
                name.StartsWith("a", StringComparison.OrdinalIgnoreCase) && name.Length > 5
            )

        // Assert: "apricot" (7) and "avocado" (7) match; "apple" (5) is excluded
        Assert.Equal(2L, result.Length)
        Assert.Equal("apricot", result.GetValue<string>(0L))
        Assert.Equal("avocado", result.GetValue<string>(1L))

    [<Fact>]
    [<Trait("Series", "FilterWith")>]
    member _.``Series filterWith returning all false yields empty Series with schema intact`` () =
        // Arrange: Numeric Series
        use s = pl.series "vals" [| 10.0; 20.0; 30.0 |]

        // Act: Filter out all elements
        use result =
            s
            |> Series.filterWith (fun (x: float) -> x < 0.0)

        // Assert: Length is 0, name and data type preserved
        Assert.Equal(0L, result.Length)
        Assert.Equal("vals", result.Name)
        Assert.Equal(s.DataType.Kind, result.DataType.Kind)

    [<Fact>]
    [<Trait("Series", "FilterWith")>]
    member _.``Series filterWith on empty series returns empty series safely`` () =
        // Arrange: Empty Series
        use s = pl.series "empty" Array.empty<int>

        // Act: Filter with dummy predicate
        use result =
            s
            |> Series.filterWith (fun (x: int) -> x > 0)

        // Assert: Remains empty with correct schema
        Assert.Equal(0L, result.Length)
        Assert.Equal("empty", result.Name)

    [<Fact>]
    [<Trait("Series", "FilterWith")>]
    member _.``Series filterWith throws ArgumentNullException when predicate is null`` () =
        // Arrange: Series instance
        use s = pl.series "data" [| 1; 2; 3 |]

        // Act & Assert: Passing null predicate delegate
        let action = fun () ->
            s.FilterWith<int>(Unchecked.defaultof<int -> bool>) |> ignore

        Assert.Throws<ArgumentNullException>(action) |> ignore
    [<Fact>]
    [<Trait("Series", "FilterWith")>]
    member _.``Series filterWith ignores null values safely`` () =
        // Arrange: Create a numeric Series containing nulls using nullable or Option
        use s = pl.series "nullable_nums" [| Some 10; None; Some 25; None; Some 5 |]

        // Act: Filter values greater than 8 (nulls should be safely skipped and evaluated to false)
        use result =
            s.FilterWith<int>(fun x -> x > 8)

        // Assert: Only 10 and 25 should pass
        Assert.Equal(2L, result.Length)
        Assert.Equal(10, result.GetValue<int>(0L))
        Assert.Equal(25, result.GetValue<int>(1L))
    [<Fact>]
    [<Trait("Series", "Iter2")>]
    member _.``Series iter2 executes action pairwise and ignores nulls`` () =
        // Arrange: Two series with one containing null
        use s1 = pl.series "x" [| 10; 20; 30 |]
        use s2 = pl.series "y" [| Some 1.5; None; Some 3.5 |]
        let observed = ResizeArray<int * float>()

        // Act: Zip iterate
        Series.iter2 (fun (x: int) (y: float) ->
            observed.Add((x, y))
        ) s1 s2

        // Assert: Index 1 had None in s2, so only 2 pairs executed
        Assert.Equal(2, observed.Count)
        Assert.Equal((10, 1.5), observed.[0])
        Assert.Equal((30, 3.5), observed.[1])

    [<Fact>]
    [<Trait("Series", "Iter2")>]
    member _.``Series iter2 throws ArgumentException on mismatched lengths`` () =
        // Arrange: Length 2 vs Length 3
        use s1 = pl.series "x" [| 1; 2 |]
        use s2 = pl.series "y" [| 1; 2; 3 |]

        // Act & Assert
        let action = fun () -> s1.Iter2<int, int>(s2, fun _ _ -> ())
        Assert.Throws<ArgumentException>(action) |> ignore
    [<Fact>]
    [<Trait("Series", "IterOpt")>]
    member _.``Series iterOpt explicitly captures and handles None without skipping`` () =
        // Arrange: Series with nulls
        use s = pl.series "values" [| Some 42; None; Some 100 |]
        let logOutput = ResizeArray<string>()

        // Act: Observe both Some and None explicitly
        s
        |> Series.iterOpt<int> (function
            | ValueSome v -> logOutput.Add $"Val: {v}"
            | ValueNone   -> logOutput.Add "Missing!"
        )

        // Assert: 3 elements observed, None was NOT skipped
        Assert.Equal(3, logOutput.Count)
        Assert.Equal("Val: 42", logOutput.[0])
        Assert.Equal("Missing!", logOutput.[1])
        Assert.Equal("Val: 100", logOutput.[2])

    [<Fact>]
    [<Trait("Series", "Iter2Opt")>]
    member _.``Series iter2Opt captures pair alignments with nulls present`` () =
        // Arrange: s1 and s2 with interspersed nulls
        use s1 = pl.series "x" [| Some 10; None; Some 30 |]
        use s2 = pl.series "y" [| Some "A"; Some "B"; None |]
        let pairs = ResizeArray<string>()

        // Act: Pairwise observation
        Series.iter2Opt (fun (xOpt: int voption) (yOpt: string voption) ->
            match xOpt, yOpt with
            | ValueSome x, ValueSome y -> pairs.Add $"{x}-{y}"
            | ValueNone,   ValueSome y -> pairs.Add $"NA-{y}"
            | ValueSome x, ValueNone   -> pairs.Add $"{x}-NA"
            | ValueNone,   ValueNone   -> pairs.Add "NA-NA"
        ) s1 s2

        // Assert: All rows synchronized and null states retained
        Assert.Equal(3, pairs.Count)
        Assert.Equal("10-A", pairs.[0])
        Assert.Equal("NA-B", pairs.[1])
        Assert.Equal("30-NA", pairs.[2])
    [<Fact>]
    [<Trait("Series", "Iter")>]
    member _.``Series iter traverses all non-null scalar elements sequentially`` () =
        // Arrange: Numeric series with primitive values
        use s = pl.series "numbers" [| 10; 20; 30; 40 |]
        let observed = ResizeArray<int>()

        // Act: Sequentially consume elements via iter
        s
        |> Series.iter<int> (fun x -> observed.Add x)

        // Assert: Elements observed in exact physical order
        Assert.Equal(4, observed.Count)
        Assert.Equal<int>([ 10; 20; 30; 40 ], observed)

    [<Fact>]
    [<Trait("Series", "Iter")>]
    member _.``Series iter automatically skips null values in sparse series`` () =
        // Arrange: Sparse series containing interspersed nulls
        use s = pl.series "sparse" [| Some 100; None; Some 200; None; Some 300 |]
        let observed = ResizeArray<int>()

        // Act: Dense iter should automatically ignore missing values
        s
        |> Series.iter<int> (fun v -> observed.Add v)

        // Assert: Only the 3 present values should be emitted
        Assert.Equal(3, observed.Count)
        Assert.Equal<int>([ 100; 200; 300 ], observed)

    [<Fact>]
    [<Trait("Series", "Iter")>]
    member _.``Series iteri accurately pairs element with its row index`` () =
        // Arrange: String series with null element
        use s = pl.series "tags" [| Some "alpha"; None; Some "gamma" |]
        let indexedResults = ResizeArray<int64 * string>()

        // Act: Track index and valid scalar value
        s
        |> Series.iteri<string> (fun idx item ->
            indexedResults.Add((idx, item))
        )

        // Assert: Preserves original 64-bit row index even when row 1 is skipped
        Assert.Equal(2, indexedResults.Count)
        Assert.Equal((0L, "alpha"), indexedResults.[0])
        Assert.Equal((2L, "gamma"), indexedResults.[1])

    // =========================================================================
    // 2. Series.iterOpt & Series.iteriOpt (Option-Aware Iteration Tests)
    // =========================================================================

    [<Fact>]
    [<Trait("Series", "IterOpt")>]
    member _.``Series iterOpt captures both ValueSome and ValueNone explicitly without skipping`` () =
        // Arrange: Series with known null positions
        use s = pl.series "measurements" [| Some 1.5; None; Some 3.5 |]
        let logs = ResizeArray<string>()

        // Act: Explicitly handle presence and absence via ValueOption pattern matching
        s
        |> Series.iterOpt<float> (function
            | ValueSome v -> logs.Add $"Val:{v:F1}"
            | ValueNone   -> logs.Add "Missing"
        )

        // Assert: 3 total rows observed, None is preserved and handled
        Assert.Equal(3, logs.Count)
        Assert.Equal("Val:1.5", logs.[0])
        Assert.Equal("Missing", logs.[1])
        Assert.Equal("Val:3.5", logs.[2])

    [<Fact>]
    [<Trait("Series", "IterOpt")>]
    member _.``Series iteriOpt yields strictly contiguous row indices for all rows`` () =
        // Arrange: Sparse series
        use s = pl.series "flags" [| Some true; None; Some false |]
        let captured = ResizeArray<int64 * bool voption>()

        // Act: Iterate through every row with its index
        s
        |> Series.iteriOpt<bool> (fun idx opt ->
            captured.Add((idx, opt))
        )

        // Assert: Indices 0L, 1L, 2L must all be present
        Assert.Equal(3, captured.Count)
        Assert.Equal((0L, ValueSome true), captured.[0])
        Assert.Equal((1L, ValueNone), captured.[1])
        Assert.Equal((2L, ValueSome false), captured.[2])

    // =========================================================================
    // 3. Series.iter2 & Series.iter2Opt (Dual Series Zip Iteration Tests)
    // =========================================================================

    [<Fact>]
    [<Trait("Series", "Iter2")>]
    member _.``Series iter2 executes dense pairwise action only when both sides are valid`` () =
        // Arrange: Two series of equal length
        use s1 = pl.series "x" [| Some 1; Some 2; None;   Some 4 |]
        use s2 = pl.series "y" [| Some 10; None;   Some 30; Some 40 |]
        let pairs = ResizeArray<int * int>()

        // Act: Only invoke action when both elements exist
        Series.iter2 (fun (x: int) (y: int) ->
            pairs.Add((x, y))
        ) s1 s2

        // Assert: Rows 1 and 2 have at least one null, so only rows 0 and 3 are executed
        Assert.Equal(2, pairs.Count)
        Assert.Equal((1, 10), pairs.[0])
        Assert.Equal((4, 40), pairs.[1])

    [<Fact>]
    [<Trait("Series", "Iter2")>]
    member _.``Series iter2Opt enables full matrix alignment of dual series with nulls`` () =
        // Arrange: Dual series
        use s1 = pl.series "id" [| Some 101; None |]
        use s2 = pl.series "label" [| Some "A"; Some "B" |]
        let diffs = ResizeArray<string>()

        // Act: Inspect pairwise options
        Series.iter2Opt (fun (idOpt: int voption) (labelOpt: string voption) ->
            match idOpt, labelOpt with
            | ValueSome id, ValueSome lbl -> diffs.Add $"{id}:{lbl}"
            | ValueNone,    ValueSome lbl -> diffs.Add $"NA:{lbl}"
            | ValueSome id, ValueNone     -> diffs.Add $"{id}:NA"
            | ValueNone,    ValueNone     -> diffs.Add "NA:NA"
        ) s1 s2

        // Assert: Complete observation of both aligned positions
        Assert.Equal(2, diffs.Count)
        Assert.Equal("101:A", diffs.[0])
        Assert.Equal("NA:B", diffs.[1])

    [<Fact>]
    [<Trait("Series", "Iter2")>]
    member _.``Series iter2 throws ArgumentException when series lengths mismatch`` () =
        // Arrange: Length 2 vs Length 3
        use s1 = pl.series "a" [| 1; 2 |]
        use s2 = pl.series "b" [| 1; 2; 3 |]

        // Act & Assert
        let action = fun () -> s1.Iter2<int, int>(s2, fun _ _ -> ())
        let ex = Assert.Throws<ArgumentException>(action)
        Assert.Contains("lengths do not match", ex.Message)

    // =========================================================================
    // 4. Boundary & Defensive Tests (Empty & Null Guards)
    // =========================================================================

    [<Fact>]
    [<Trait("Series", "Iter")>]
    member _.``Series iter on empty series executes without side-effects`` () =
        // Arrange: Empty series
        use empty = pl.series "empty" Array.empty<int>
        let mutable invoked = false

        // Act: Iterate over empty
        empty |> Series.iter<int> (fun _ -> invoked <- true)

        // Assert: Action never executed
        Assert.False(invoked)

    [<Fact>]
    [<Trait("Series", "Iter")>]
    member _.``Series iter throws ArgumentNullException when action is null`` () =
        // Arrange: Valid series
        use s = pl.series "data" [| 1; 2; 3 |]

        // Act & Assert
        let action = fun () -> s.Iter<int>(Unchecked.defaultof<int -> unit>)
        Assert.Throws<ArgumentNullException>(action) |> ignore
