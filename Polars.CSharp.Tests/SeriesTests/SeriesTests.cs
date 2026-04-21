#nullable enable

using System.Numerics.Tensors;
using Apache.Arrow;
using Apache.Arrow.Types;
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class SeriesTests
{
    [Fact]
    public void Test_Series_Creation_And_Arrow()
    {
        using var s = Series.From("my_series", [1, 2, 3]);
        
        Assert.Equal(3, s.Length);
        Assert.Equal("my_series", s.Name);

        var arrowArray = s.ToArrow();
        Assert.IsType<Int32Array>(arrowArray);
        Assert.Equal(2, ((Int32Array)arrowArray).GetValue(1));

        // Rename
        s.Name = "renamed";
        Assert.Equal("renamed", s.Name);
    }
    [Fact]
    public void Test_Series_FromArrow_ComplexList()
    {
        var valueBuilder = new Int64Array.Builder();
        valueBuilder.Append(1);
        valueBuilder.Append(2);
        valueBuilder.Append(3);
        using var valuesArray = valueBuilder.Build();

        var offsetsBuilder = new Int32Array.Builder();
        offsetsBuilder.Append(0); // Start
        offsetsBuilder.Append(2); // End of row 0 (0->2, len=2)
        offsetsBuilder.Append(2); // End of row 1 (2->2, len=0, is_null)
        offsetsBuilder.Append(3); // End of row 2 (2->3, len=1)
        using var offsetsArray = offsetsBuilder.Build();
        
        // Build Validity Bitmap: 1, 0, 1 (Row 1 is null)
        var validityBuilder = new BooleanArray.Builder();
        validityBuilder.Append(true);
        validityBuilder.Append(false); // null
        validityBuilder.Append(true);
        using var validityArray = validityBuilder.Build();

        // Build ListArray
        // (IArrowType type, int length, ArrowBuffer valueOffsets, IArrowArray values, ArrowBuffer nullBitmap, int nullCount = 0, int offset = 0)
        using var listArray = new ListArray(
            new ListType(new Int64Type()), // Type
            3,                             // Rows
            offsetsArray.ValueBuffer,      // offsets buffer
            valuesArray,                   // Values array
            validityArray.ValueBuffer,     // validity buffer
            1                              // Null Count
        );

        var checkOffsets = listArray.ValueOffsets;
        Assert.Equal(0, checkOffsets[0]); 
        Assert.Equal(2, checkOffsets[1]);
        Assert.Equal(2, checkOffsets[2]);
        Assert.Equal(3, checkOffsets[3]);

        using var s = Series.FromArrow("arrow_list_manual", listArray);

        using var df = DataFrame.FromSeries(s);

        using var exploded = df.Explode("arrow_list_manual");
        // [1, 2] -> 2 rows
        // null   -> 1 rows
        // [3]    -> 1 row
        // Total 4 rows
        Assert.Equal(4, exploded.Height);
        
        Assert.Equal(1, exploded.GetValue<long>(0, "arrow_list_manual"));
        Assert.Equal(2, exploded.GetValue<long>(1, "arrow_list_manual"));
        Assert.Equal(3, exploded.GetValue<long>(3, "arrow_list_manual"));
    }
    [Fact]
    [Trait("Series","Factory")]
    public void Test_Series_HighLevel_Create()
    {
        var data = new List<List<int?>?>
        {
            new() { 1, 2 },
            null,
            new() { 3, null, 5 }
        };

        using var s = Series.From("my_list", data);

        using var df = DataFrame.FromSeries(s);
        
        // Schema check
        Assert.Equal(DataTypeKind.List, s.DataType.Kind);
        
        // Explode
        using var exploded = df.Explode("my_list");

        Assert.Equal(6, exploded.Height); 
    }
    [Fact]
    public void Test_Series_FromArrow_Struct_With_List()
    {
        // =============================================================
        // Struct<Name: Utf8, Scores: List<i64>>
        // Row 0: { "Alice", [10, 20] }
        // Row 1: null (entire Struct as null)
        // Row 2: { "Bob", [30] }
        // =============================================================

        int length = 3;

        var nameBuilder = new StringArray.Builder();
        nameBuilder.Append("Alice");
        nameBuilder.Append("Ignored");
        nameBuilder.Append("Bob");
        using var nameArray = nameBuilder.Build();

        // B: "Scores" (ListArray<i64>)
        // Row 0: [10, 20]
        // Row 1: null ([])
        // Row 2: [30]
        
        // Values
        var valBuilder = new Int64Array.Builder();
        valBuilder.Append(10); valBuilder.Append(20); valBuilder.Append(30);
        using var valArray = valBuilder.Build();
        
        // Offsets: [0, 2, 2, 3] (Row 1 null)
        var offBuilder = new Int32Array.Builder();
        offBuilder.Append(0); offBuilder.Append(2); offBuilder.Append(2); offBuilder.Append(3);
        using var offArray = offBuilder.Build();
        
        // Validity for List
        var listValidBuilder = new BooleanArray.Builder();
        listValidBuilder.Append(true); listValidBuilder.Append(false); listValidBuilder.Append(true);
        using var listValid = listValidBuilder.Build();

        using var scoresArray = new ListArray(
            new ListType(new Int64Type()), 
            length, offArray.ValueBuffer, valArray, listValid.ValueBuffer, 1
        );

        var structValidBuilder = new BooleanArray.Builder();
        structValidBuilder.Append(true);
        structValidBuilder.Append(false); // <--- Struct Null
        structValidBuilder.Append(true);
        using var structValid = structValidBuilder.Build();

        // Build StructArray
        var fields = new List<Field>
        {
            new("Name", new StringType(), false),
            new("Scores", new ListType(new Int64Type()), true)
        };
        var structType = new StructType(fields);

        var children = new List<IArrowArray> { nameArray, scoresArray };

        using var structArray = new StructArray(
            structType,
            length,
            children,
            structValid.ValueBuffer,
            1
        );

        using var s = Series.FromArrow("my_struct", structArray);

        using var df = DataFrame.FromSeries(s);

        Assert.Equal(DataTypeKind.Struct, s.DataType.Kind);
        
        using var res = df.Select(
            Pl.Col("my_struct").Struct.Field("Name"),
            Pl.Col("my_struct").Struct.Field("Scores")
        );
        
        Assert.Equal("Alice", res.GetValue<string>(0, "Name"));
    }
    private class Student
    {
        public string? Name { get; set; }
        public int Age { get; set; }
    }

    [Fact]
    public void Test_Series_From_ObjectList()
    {
        var students = new List<Student>
        {
            new() { Name = "Alice", Age = 20 },
            null!, // Struct Null
            new() { Name = "Bob", Age = 22 }
        };

        using var s = Series.From("students", students);
        
        using var df = DataFrame.FromSeries(s);
        
        Assert.Equal(DataTypeKind.Struct, s.DataType.Kind);
        
        using var unnested = df.Unnest("students");
        Assert.Equal("Alice", unnested.GetValue<string>(0, "Name"));
        Assert.Equal(22, unnested.GetValue<int>(2, "Age"));
    }
    [Fact]
    public void Test_Series_Recursive_List_Of_List()
    {
        // List<List<int>> 
        // Row 0: [1, 2]
        // Row 1: null
        // Row 2: [3, 4, 5]
        var data = new List<List<int>?>
        {
            new() { 1, 2 },
            null,
            new() { 3, 4, 5 }
        };

        using var s = Series.From("nested_list", data);

        using var df = DataFrame.FromSeries(s);


        Assert.Equal(DataTypeKind.List, s.DataType.Kind);
        Assert.Equal(3, s.Length);
        Assert.Equal(1, s.NullCount);

        using var exploded = df.Explode("nested_list");
        exploded.Show();
        // 1, 2, null, 3, 4, 5 -> 5 rows
        Assert.Equal(6, exploded.Height);
        Assert.Equal(1, exploded.GetValue<int>(0, "nested_list")); // int
        Assert.Equal(4, exploded.GetValue<int>(4, "nested_list"));
    }

    [Fact]
    public void Test_Series_Deep_Recursive()
    {
        var data = new List<List<string>?>
        {
            new() { "a", "b" },
            new() { "c" }
        };
        
        using var s = Series.From("strs", data);
        Assert.Equal(DataTypeKind.List, s.DataType.Kind);
        
        using var df = DataFrame.FromSeries(s);
        using var exp = df.Explode("strs");
        Assert.Equal("a", exp.GetValue<string>(0, "strs"));
    }
    [Fact]
    [Trait("Series","String")]
    public void Test_Series_String_And_Nulls()
    {
        using var s = Series.From("strings", 
            ["a", null, "原神启动","錕斤拷燙燙燙1231659846156516あいうえおへへへへっへへへへへっへへｈ","🍉",""]
        );
        Assert.Equal(6, s.Length);

        Assert.Equal("a", s[0]);
        Assert.Null(s[1]);
        Assert.Equal("原神启动", s[2]);
        Assert.Contains("錕斤拷燙燙燙", s.GetValue<string>(3));
        Assert.Equal("🍉",s[4]);
        Assert.Equal("",s[5]);
    }
    [Fact]
    [Trait("Series","CastDecimal")]
    public void Test_Series_Cast_Decimal()
    {
        // Create Double Series 
        using var s = Series.From("prices", new double?[] {10.5, 20.0, double.NaN, null,double.MaxValue});

        // Cast to Decimal(10, 2)
        using var sDecimal = s.Cast(DataType.Decimal(10, 2),strict:false);

        Assert.Equal(DataType.Decimal(10,2),sDecimal.DataType);
        Assert.Equal(10.50m,sDecimal[0]);
        Assert.Null(sDecimal[2]);
        Assert.Null(sDecimal[3]);
        Assert.Null(sDecimal[4]);
    }
    [Fact]
    public void Test_Series_Constructor_DateTimeOffset()
    {
        var now = DateTimeOffset.Now;
        var data = new DateTimeOffset[] { now, now.AddHours(1) };

        using var s1 = Series.From("dto", data);
        Assert.Equal("dto", s1.Name);
        Assert.Equal(2, s1.Length);

        var v0 = s1.GetValue<DateTimeOffset>(0);
        Assert.Equal(now.UtcTicks / 10 * 10, v0.UtcTicks);
        
        var dataNull = new DateTimeOffset?[] { now, null };
        using var s2 = Series.From("dto_null", dataNull);
        
        Assert.Equal(2, s2.Length);
        Assert.Null(s2.GetValue<DateTimeOffset?>(1));
        Assert.NotNull(s2.GetValue<DateTimeOffset?>(0));
    }
    [Fact]
    public void Test_NullCount()
    {
        using var sInt = new Series("nums", (int?[])[1, null, 3, null, 5]);
        
        Assert.Equal(2, sInt.NullCount);
        Assert.Equal(5, sInt.Length);

        using var sStr = Series.From("str", ["a", null, "b"]);
        
        Assert.Equal(1, sStr.NullCount);
        
        using var sAllNull = new Series("nulls", new string?[] { null, null });
        Assert.Equal(2, sAllNull.NullCount);
        
        using var sClean = new Series("clean", [1, 2, 3]);
        Assert.Equal(0, sClean.NullCount);
    }
    [Fact]
    public void Test_Series_Arithmetic()
    {
        using var s1 = new Series("a", [1, 2, 3]);
        using var s2 = new Series("b", [10, 20, 30]);

        // Test Add (+)
        using var sum = s1 + s2;
        Assert.Equal(11, sum.GetValue<int>(0));
        Assert.Equal(22, sum.GetValue<int>(1));
        Assert.Equal(33, sum.GetValue<int>(2));

        // Test Mul (*)
        using var prod = s1 * s2;
        Assert.Equal(10, prod.GetValue<int>(0));
        Assert.Equal(90, prod.GetValue<int>(2)); // 3 * 30
    }

    [Fact]
    public void Test_Series_Comparison()
    {
        using var s1 = new Series("a", [1, 5, 10]);
        using var s2 = new Series("b", [1, 4, 20]);

        // Test Eq (1==1, 5!=4, 10!=20) -> [true, false, false]
        using var eq = s1.Eq(s2);
        Assert.True(eq.GetValue<bool>(0));
        Assert.False(eq.GetValue<bool>(1));

        // Test Gt (>) (1>1 false, 5>4 true, 10>20 false)
        using var gt = s1 > s2;
        Assert.False(gt.GetValue<bool>(0));
        Assert.True(gt.GetValue<bool>(1));
        Assert.False(gt.GetValue<bool>(2));
    }

    [Fact]
    public void Test_Series_Aggregations()
    {
        using var s = new Series("nums", [1, 2, 3, 4, 5]);

        // Sum: 15
        using var sumSeries = s.Sum();
        Assert.Equal(1, sumSeries.Length);
        Assert.Equal(15, sumSeries.GetValue<int>(0));
        
        Assert.Equal(15, s.Sum<int>());

        // Mean: 3
        Assert.Equal(3, s.Mean<double>());
        
        // Min/Max
        Assert.Equal(1, s.Min<int>());
        Assert.Equal(5, s.Max<int>());
    }

    [Fact]
    public void Test_Series_FloatChecks()
    {
        using var s = Series.From("f", [1.0, double.NaN, double.PositiveInfinity]);

        // IsNan -> [false, true, false]
        using var isNan = s.IsNan();
        Assert.False(isNan.GetValue<bool>(0));
        Assert.True(isNan.GetValue<bool>(1));
        Assert.False(isNan.GetValue<bool>(2));

        // IsInfinite -> [false, false, true]
        using var isInf = s.IsInfinite();
        Assert.True(isInf.GetValue<bool>(2));
    }
    [Fact]
    public void Test_Series_Unique_Composite()
    {
        using var s = Series.From("nums", [1, 2, 2, 3]);

        // NUnique
        Assert.Equal(3L, s.NUnique()); 

        // IsDuplicated
        using var dupMask = s.IsDuplicated();
        Assert.Equal(DataTypeKind.Boolean, dupMask.DataType.Kind);
        
        Assert.False((bool)dupMask[0]!); // 1
        Assert.True((bool)dupMask[1]!);  // 2
        Assert.True((bool)dupMask[2]!);  // 2
        Assert.False((bool)dupMask[3]!); // 3

        using var uniq = s.UniqueStable();
        Assert.Equal(3, uniq.Length);
        Assert.Equal(1, uniq[0]);
        Assert.Equal(2, uniq[1]);
        Assert.Equal(3, uniq[2]);
    }
    [Fact]
    public void Test_Series_Sort_Options()
    {
        using var s = Series.From("nums", new int?[] { 3, null, 1, 3, 2 });

        //  Ascending
        // [null, 1, 2, 3, 3]
        using var sAsc = s.Sort(descending: false, nullsLast: false);
        
        Assert.Null(sAsc[0]); 
        Assert.Equal(1, sAsc[1]);
        Assert.Equal(3, sAsc[4]);

        // Descending
        // [3, 3, 2, 1, null] 
        // Ascending: null, ..., max
        // Descending: max, ..., null
        using var sDesc = s.Sort(descending: true, nullsLast: true);
        
        Assert.Equal(3, sDesc[0]);
        Assert.Equal(3, sDesc[1]);
        Assert.Equal(1, sDesc[3]);
        Assert.Null(sDesc[4]);

        // Nulls Last (Ascending)
        // [1, 2, 3, 3, null]
        using var sNullsLast = s.Sort(descending: false, nullsLast: true);
        
        Assert.Equal(1, sNullsLast[0]);
        Assert.Null(sNullsLast[4]);

        // Stable Sort (maintainOrder)
        using var sStable = s.Sort(maintainOrder: true);
        Assert.Equal(5, sStable.Length);
    }
    [Fact]
    public void Test_Series_Sort_Strings()
    {
        using var s = Series.From("chars", ["c", "a", "b"]);
        
        using var sorted = s.Sort();
        Assert.Equal("a", sorted[0]);
        Assert.Equal("b", sorted[1]);
        Assert.Equal("c", sorted[2]);
    }
 
 
    [Fact]
    [Trait("Series", "BitShift")]
    public void Test_Series_Bitwise_Shift()
    {
        // Signed Int32
        // -8 (111...1000) >> 2 = -2 (111...1110)
        using var sInt = Series.From("signed", [1, -8]);
        
        using var sIntShl = sInt << 2; // 1<<2=4, -8<<2=-32
        using var sIntShr = sInt >> 2; // 1>>2=0, -8>>2=-2

        Assert.Equal(4, sIntShl[0]);
        Assert.Equal(-32, sIntShl[1]);
        
        Assert.Equal(0, sIntShr[0]);
        Assert.Equal(-2, sIntShr[1]);

        // Unsigned UInt32
        // 0xF0000000 >> 4 = 0x0F000000
        uint bigNum = 0xF0000000;
        uint[] unsignedArray = [bigNum];
        using var sUint = Series.From("unsigned", unsignedArray);
        sUint.Show();
        using var sUintShr = sUint >> 4;
        
    }
    [Fact]
    [Trait("Series","Show")]
    public void TestToString_And_Show()
    {
        var s = Series.From("my_series", [1, 2, 3, 4, 5]);

        var str = s.ToString();

        Assert.NotEmpty(str);
        Assert.True(str.Contains("my_series"), "Output should contain series name");
        Assert.True(str.Contains("shape: (5,)"), "Output should contain series shape info");
        
        s.Show();
        
        var sNull = Series.From("nulls", new int?[] { 1, null, 3 });
        var strNull = sNull.ToString();
        Assert.True(strNull.Contains("null"), "Output should represent null values");
    }

    [Fact]
    public void TestValueCounts()
    {
        var s = Series.From("fruit", [ 
            "apple", "apple", "orange", "banana", "apple", "orange" 
        ]);

        var dfCounts = s.ValueCounts();
        dfCounts.Show();
        
        Assert.Equal(3, dfCounts.Height);
        
        Assert.Equal("apple", dfCounts[0][0]); // Column 0 (fruit) Row 0
        Assert.Equal(3u, dfCounts[1][0]);         // Column 1 (count) Row 0

        // --- Normalize ---
        // normalize=true, name="prob"
        var dfNorm = s.ValueCounts(sort: true, parallel: true, name: "prob", normalize: true);
        
        Assert.Equal("prob", dfNorm.Columns[1]);
        
        var probApple = dfNorm["prob"][0];
        Assert.Equal(0.5, probApple); 
    }
    [Fact]
    public void Test_Series_TopKBy_BottomKBy_With_StringLength()
    {
        var s = new Series("words", ["a", "ccc", "bb"]);

        var byLength = Pl.Col("words").Str.LenBytes();

        var top2 = s.TopKBy(2, byLength);
        
        Assert.Equal(2, top2.Length);
        Assert.Equal("ccc", top2[0]);
        Assert.Equal("bb", top2[1]);

        var bot2 = s.BottomKBy(2, byLength);
        
        Assert.Equal(2, bot2.Length);
        Assert.Equal("a", bot2[0]);
        Assert.Equal("bb", bot2[1]);
    }
    [Fact]
    public void Test_Series_Statistics_Methods()
    {
        // ---------------------------------------------------
        // Std, Var, Median
        // Data: [1, 2, 3, 4, 5]
        // Avg: 3
        // Std (ddof=1): ((1-3)^2 + ... + (5-3)^2) / 4 = 10 / 4 = 2.5
        // Var: sqrt(2.5) ≈ 1.58113883
        // ---------------------------------------------------
        var s1 = new Series("s1", [1, 2, 3, 4, 5]);

        Assert.Equal(2.5, (double)s1.Var<double>(ddof: 1)!, precision: 5);

        Assert.Equal(1.58114, (double)s1.Std<double>(ddof: 1)!, precision: 5);

        Assert.Equal(3.0, s1.Median<double>());

        // ---------------------------------------------------
        // Quantile
        // ---------------------------------------------------
        Assert.Equal(3.0, s1.Quantile(0.5, QuantileMethod.Linear)[0]);

        // ---------------------------------------------------
        // PctChange
        // Data: [10, 20, 10]
        // 10 -> 20: +100% (1.0)
        // 20 -> 10: -50% (-0.5)
        // ---------------------------------------------------
        var s2 = new Series("s2", [10, 20, 10]);
        var pct = s2.PctChange(); 

        Assert.Null(pct[0]); 
        Assert.Equal(1.0, (double)pct[1]!, precision: 2);
        Assert.Equal(-0.5, (double)pct[2]!, precision: 2);

        // ---------------------------------------------------
        // Rank
        // Data: [10, 20, 20, 10]
        // Sorted: 10(pos1), 10(pos2), 20(pos3), 20(pos4)
        // Method=Average:
        // 10 rank = (1+2)/2 = 1.5
        // 20 rank = (3+4)/2 = 3.5
        // ---------------------------------------------------
        var s3 = new Series("s3", [10, 20, 20, 10]);
        var ranks = s3.Rank(RankMethod.Average);

        Assert.Equal(1.5, ranks[0]);
        Assert.Equal(3.5, ranks[1]);
        Assert.Equal(3.5, ranks[2]);
        Assert.Equal(1.5, ranks[3]);
    }
    [Fact]
    public void Test_Series_Cumulative_Methods()
    {
        var s = new Series("nums", [1, 3, 2, 4]);

        // ---------------------------------------------------
        // CumSum: [1, 1+3, 1+3+2, 1+3+2+4] -> [1, 4, 6, 10]
        // ---------------------------------------------------
        var cumSum = s.CumSum();
        var arrSum = cumSum.ToArray<int>();
        
        Assert.Equal([1, 4, 6, 10], arrSum);

        // ---------------------------------------------------
        // CumMax :[1, 3, 3, 4]
        // ---------------------------------------------------
        var cumMax = s.CumMax();
        var arrMax = cumMax.ToArray<int>();

        Assert.Equal([1, 3, 3, 4], arrMax);

        // ---------------------------------------------------
        // CumMin: [1, 1, 1, 1]
        // ---------------------------------------------------
        var cumMin = s.CumMin();
        var arrMin = cumMin.ToArray<int>();

        Assert.Equal([1, 1, 1, 1], arrMin);

        // ---------------------------------------------------
        // CumProd: [1, 1*3, 3*2, 6*4] -> [1, 3, 6, 24]
        // ---------------------------------------------------
        var cumProd = s.CumProd();
        var arrProd = cumProd.ToArray<int>();

        Assert.Equal([1, 3, 6, 24], arrProd);

        // ---------------------------------------------------
        // Reverse
        // Data: [1, 3, 2]
        // CumSum Reverse: 
        // index 2 (val 2) -> 2
        // index 1 (val 3) -> 3 + 2 = 5
        // index 0 (val 1) -> 1 + 5 = 6
        // Result: [6, 5, 2]
        // ---------------------------------------------------
        var sRev = new Series("rev", [1, 3, 2]);
        var revSum = sRev.CumSum(reverse: true);
        var arrRev = revSum.ToArray<int>();

        Assert.Equal([6, 5, 2], arrRev);
    }
    [Fact]
    public void Test_Series_Ewm_Methods()
    {
        // ---------------------------------------------------
        // 1. EwmMean (Standard)
        // Data: [10, 20, 40]
        // Alpha = 0.5 (Com = 1)
        // Adjust = false (Infinite history approximation for simple math)
        // 
        // t0: 10
        // t1: (1-0.5)*10 + 0.5*20 = 5 + 10 = 15
        // t2: (1-0.5)*15 + 0.5*40 = 7.5 + 20 = 27.5
        // ---------------------------------------------------
        var s = new Series("val", [10.0, 20.0, 40.0]);
        
        var ewm = s.EwmMean(alpha: 0.5, adjust: false);
        
        Assert.Equal(10.0, ewm[0]);
        Assert.Equal(15.0, ewm[1]);
        Assert.Equal(27.5, ewm[2]);

        // ---------------------------------------------------
        // EwmVar
        // ---------------------------------------------------
        var ewmVar = s.EwmVar(alpha: 0.5);
        Assert.NotNull(ewmVar[0]);
        Assert.True((double?)ewmVar[1] >= 0);
    }

    [Fact]
    public void Test_Series_EwmMeanBy_Time()
    {
        var times = new[] 
        { 
            new DateTime(2023, 1, 1), 
            new DateTime(2023, 1, 11)
        };
        var values = new[] { 10.0, 20.0 };

        using var df = DataFrame.FromColumns(new
        {
            tm = times,
            val = values
        });

        // Case 1: HalfLife = "1d"
        var resDecay = df.Select(
            Pl.Col("val").EwmMeanBy(Pl.Col("tm"), halfLife: "1d").Alias("ewm")
        );
        
        var arrDecay = resDecay["ewm"].ToArray<double>();
        Assert.Equal(20.0, arrDecay[1], precision: 1); 

        // Case 2: HalfLife = "100d" 
        var resStable = df.Select(
            Pl.Col("val").EwmMeanBy(Pl.Col("tm"), halfLife: "100d").Alias("ewm")
        );
        var arrStable = resStable["ewm"].ToArray<double>();
        Assert.True(arrStable[1] < 19.0);
        Assert.True(arrStable[1] > 10.0);
    }
    [Fact]
    public void Test_RollingMeanBy_TimeWindow_And_ClosedBoundary()
    {
        var start = new DateTime(2023, 1, 1, 9, 0, 0);
        var times = new[] 
        { 
            start, 
            start.AddHours(1), // 10:00
            start.AddHours(2)  // 11:00
        };
        var values = new[] { 10.0, 20.0, 30.0 };

        using var df = DataFrame.FromColumns(new { tm = times, val = values });

        // ---------------------------------------------------
        // Case 1: Window = 2h, Closed = Left [t-w, t)
        // ---------------------------------------------------
        // At 11:00 (t): Window is [09:00, 11:00)
        // Contain: 09:00 (10), 10:00 (20) -> Not contain 11:00 
        // Mean = (10 + 20) / 2 = 15.0
        // ---------------------------------------------------
        var resLeft = df.Select(
            Pl.Col("val").RollingMeanBy(
                windowSize: TimeSpan.FromHours(2), 
                by: Pl.Col("tm"), 
                closed: ClosedWindow.Left          
            ).Alias("mean_left")
        );

        var leftArr = resLeft["mean_left"].ToArray<double>();
        // Polars Left closed definition: [t - period, t). 
        // Window [09:00, 11:00). Contains 09:00, 10:00.
        // Result should be 15.0.
        
        Assert.Equal(15.0, leftArr[2]); 
        // ---------------------------------------------------
        // Case 2: Window = 2h, Closed = Both [t-w, t]
        // ---------------------------------------------------
        // At 11:00 (t): Window is [09:00, 11:00]
        // 09:00 (10), 10:00 (20), 11:00 (30)
        // Mean = (10+20+30) / 3 = 20.0
        // ---------------------------------------------------
        var resBoth = df.Select(
            Pl.Col("val").RollingMeanBy(
                windowSize: TimeSpan.FromHours(2), 
                by: Pl.Col("tm"), 
                closed: ClosedWindow.Both
            ).Alias("mean_both")
        );

        var bothArr = resBoth["mean_both"].ToArray<double>();
        Assert.Equal(20.0, bothArr[2]);
    }
    [Fact]
    public void Test_RollingQuantileBy_ComplexSignature()
    {
        var times = Enumerable.Range(0, 5).Select(i => new DateTime(2023, 1, 1).AddSeconds(i)).ToArray();
        var values = new[] { 1.0, 2.0, 3.0, 4.0, 5.0 };

        using var df = DataFrame.FromColumns(new { tm = times, val = values });

        // Quantile 0.5
        // Window="3s", Closed="Both"
        // At t=4 (00:00:04, val=5): Window [00:00:01, 00:00:04] -> {2, 3, 4, 5}
        // Median of {2,3,4,5} -> (3+4)/2 = 3.5 (Linear interpolation)
        
        var res = df.Select(
            Pl.Col("val").RollingQuantileBy(
                quantile: 0.5,
                method: QuantileMethod.Linear,
                windowSize: TimeSpan.FromSeconds(3), // "3s"
                by: Pl.Col("tm"),
                closed: ClosedWindow.Both
            ).Alias("q50")
        );

        var arr = res["q50"].ToArray<double>();
        
        // Check last element
        Assert.Equal(3.5, arr[4]); 
    }
    [Fact]
    public void Test_Series_Direct_Aggregations()
    {
        var s = new Series("nums", [1, 2, 3]);
        var sRev = s.Reverse();
        Assert.Equal([3, 2, 1], sRev.ToArray<int>());

        Assert.Equal(1, s.First()[0]);
        Assert.Equal(3, s.Last()[0]);

        var sBool = new Series("bools", [true, false, null]); 
        // Any (ignoreNulls=false): true | false | null -> true 
        Assert.Equal(true, sBool.Any(ignoreNulls: false)[0]);
        
        // All (ignoreNulls=true): true & false -> false
        Assert.Equal(false, sBool.All(ignoreNulls: true)[0]);
        
        var sAllTrue = new Series("all_true", [true, true]);
        Assert.Equal(true, sAllTrue.All()[0]);
    }
    [Fact]
    public void Test_Series_Long_Nullable_Constructor()
    {
        long bigNumber = 3_000_000_000L;

        var sLong = new Series("longs", [bigNumber, null, 123L]);

        Assert.Equal(bigNumber, sLong.GetValue<long?>(0));
        Assert.Null(sLong.GetValue<long?>(1));
        Assert.Equal(123L, sLong.GetValue<long?>(2));
    }
    [Fact]
    public void Test_Bool_Packing_Boundaries_And_Performance()
    {
        bool[] dataSmall = [true, false, true, true, false, true, false, false]; // Len 8
        using var sSmall = new Series("small", dataSmall);
        
        Assert.Equal(dataSmall, sSmall.ToArray<bool>());
        
        bool[] dataExact = new bool[64];
        for(int i=0; i<64; i++) dataExact[i] = (i % 2 == 0); // T, F, T, F...
        
        using var sExact = new Series("exact", dataExact);
        Assert.Equal(dataExact, sExact.ToArray<bool>());

        int lenMixed = 100;
        bool[] dataMixed = new bool[lenMixed];
        for (int i = 0; i < lenMixed; i++)
        {
            dataMixed[i] = (i % 3 == 0) || (i % 5 != 0);
        }

        using var sMixed = new Series("mixed", dataMixed);
        var resMixed = sMixed.ToArray<bool>();

        for (int i = 0; i < lenMixed; i++)
        {
            if (dataMixed[i] != resMixed[i])
            {
                throw new Exception($"Mismatch at index {i}. Expected {dataMixed[i]}, Got {resMixed[i]}. " +
                                    $"This is likely a SIMD/Scalar boundary issue around index {i - (i % 32)}.");
            }
        }
        Assert.Equal(dataMixed, resMixed);
        Console.WriteLine("Case 3 (Hybrid 100): Passed");

        int lenHuge = 10_000_000;
        var dataHuge = new bool[lenHuge];
        Parallel.For(0, lenHuge, i => 
        {
            dataHuge[i] = i % 2 == 0; 
        });

        var sw = System.Diagnostics.Stopwatch.StartNew();
        
        using var sHuge = new Series("huge", dataHuge);
        
        sw.Stop();
        Console.WriteLine($"Case 4 (10M Rows): Time = {sw.ElapsedMilliseconds} ms");

        var resHuge = sHuge.ToArray<bool>();
        Assert.Equal(dataHuge[0], resHuge[0]);
        Assert.Equal(dataHuge[lenHuge - 1], resHuge[lenHuge - 1]);
        Assert.Equal(dataHuge[lenHuge / 2], resHuge[lenHuge / 2]);
    }
    // =================================================================================
    // [Stride 2] Int8 / SByte 
    // =================================================================================
    [Fact]
    public void Test_Int8_Simd_Boundaries()
    {
        int count = 35; 
        sbyte?[] data = new sbyte?[count];
        for (int i = 0; i < count; i++)
        {
            if (i % 3 == 0) data[i] = null;
            else data[i] = (sbyte)(i % 127);
        }

        using var s = new Series("s8", data);
        
        var result = s.ToArray<sbyte?>();
        Assert.Equal(data, result);
    }

    // =================================================================================
    // [Stride 4] Int16 / Short 
    // =================================================================================
    [Fact]
    public void Test_Int16_Simd_Boundaries()
    {
        int count = 19;
        short?[] data = new short?[count];
        for (int i = 0; i < count; i++)
        {
            if (i % 3 == 0) data[i] = null;
            else data[i] = (short)(i * 100);
        }

        using var s = new Series("s16", data);
        
        var result = s.ToArray<short?>();
        Assert.Equal(data, result);
    }

    // =================================================================================
    // [Stride 8] Int32 / Int
    // =================================================================================
    [Fact]
    public void Test_Int32_Simd_Boundaries()
    {
        int count = 13;
        int?[] data = new int?[count];
        for (int i = 0; i < count; i++)
        {
            if (i % 3 == 0) data[i] = null;
            else data[i] = i * 1000;
        }

        using var s = new Series("s32", data);
        
        var result = s.ToArray<int?>();
        Assert.Equal(data, result);
    }

    // =================================================================================
    // [Stride 16] Int64 / Long
    // =================================================================================
    [Fact]
    public void Test_Int64_Simd_Boundaries()
    {
        Console.WriteLine("Testing Int64 (long)...");

        int count = 5;
        long?[] data = new long?[count];
        for (int i = 0; i < count; i++)
        {
            if (i % 3 == 0) data[i] = null;
            else data[i] = i * 10000L;
        }

        using var s = new Series("s64", data);
        
        var result = s.ToArray<long?>();
        Assert.Equal(data, result);
    }

    // =================================================================================
    // [Stride 32] Int128 
    // =================================================================================
    [Fact]
    public void Test_Int128_Simd_Layout()
    {
        Console.WriteLine("Testing Int128...");

        Int128?[] data =
        [
            Int128.MaxValue,
            Int128.One, 
            null,           
            Int128.Zero 
        ];

        using var s = new Series("s128", data);
        
        Assert.Equal(data[0], s[0]); // MaxValue Check
        Assert.Equal(data[1], s[1]);
        Assert.Null(s[2]);
        Assert.Equal(data[3], s[3]);
    }

    // =================================================================================
    // [UInt32]
    // =================================================================================
    [Fact]
    public void Test_UInt32_Unboxing_Fix()
    {
        Console.WriteLine("Testing UInt32 Unboxing...");
        
        uint?[] data = [uint.MaxValue, 0, null, 123];
        
        using var s = new Series("u32", data);
        
        var result = s.ToArray<uint?>();
        Assert.Equal(data, result);
    }

    // =================================================================================
    // [Performance]
    // =================================================================================
    [Fact]
    public void Test_Performance_10M()
    {
        int len = 10_000_000;
        var data = new int?[len];
        Parallel.For(0, len, i => 
        {
            if (i % 31 == 0) data[i] = null;
            else data[i] = i;
        });

        var sw = System.Diagnostics.Stopwatch.StartNew();
        
        using var s = new Series("perf", data);
        
        sw.Stop();
        Console.WriteLine($"Nullable Int32 (10M) Pack Time: {sw.ElapsedMilliseconds} ms");
        
        Assert.Null(s.GetValue<int?>(0));
        Assert.Equal(1, s.GetValue<int?>(1));
    }
    [Fact]
    public void Test_Series_Dot_Product()
    {
        using var s1 = Series.From("a", [1, 2, 3]);
        using var s2 = Series.From("b", [4, 5, 6]);

        using var resSeries = s1.Dot(s2);
        Assert.Equal(1, resSeries.Length);
        Assert.Equal(32L, resSeries.GetValue<long>(0));

        var resScalar = s1.Dot<long>(s2);
        Assert.Equal(32L, resScalar);
        
        using var s3 = Series.From("a", [10, 20, 30]);
        var resSameName = s1.Dot<long>(s3);
        // 1*10 + 2*20 + 3*30 = 10 + 40 + 90 = 140
        Assert.Equal(140L, resSameName);
    }
    [Fact]
    public void Test_InterpolateBy_Series_Timestamp()
    {
        // T1: 10:00 (Val=10)
        // T2: 10:15 (Val=null)
        // T3: 11:00 (Val=70)
        
        // Val = 10 + (70-10) * 0.25 = 10 + 15 = 25.

        var baseTime = new DateTime(2024, 1, 1, 10, 0, 0);
        
        using var times = Series.From("time",
        [
            baseTime,
            baseTime.AddMinutes(15), 
            baseTime.AddHours(1)
        ]);

        using var values = Series.From("val", new double?[] { 10.0, null, 70.0 });

        // Act
        using var interpolated = values.InterpolateBy(times);

        // Assert
        var result = interpolated.GetValue<double>(1);
        Assert.Equal(25.0, result);
    }
    [Fact]
    [Trait("Series","ReadOnlySpanInt")]
    public void AsReadOnlySpan_ValidInt32Series_ReturnsCorrectSpan()
    {

        int[] expectedData = [10, 20, 30, 40, 50];

        var series = Series.From("test_int", expectedData); 

        ReadOnlySpan<int> span = series.AsReadOnlySpan<int>();

        Assert.Equal(expectedData.Length, span.Length);

        Assert.True(span.SequenceEqual(expectedData));
    }

    [Fact]
    [Trait("Series","ReadOnlySpanDouble")]
    public void AsReadOnlySpan_ValidFloat64Series_ReturnsCorrectSpan()
    {
        double[] expectedData = [1.1, 2.2, 3.3];
        var series = Series.From("test_float", expectedData);

        ReadOnlySpan<double> span = series.AsReadOnlySpan<double>();

        Assert.True(span.SequenceEqual(expectedData));
    }

    [Fact]
    [Trait("Series","ReadOnlySpanNull")]
    public void AsReadOnlySpan_SeriesWithNulls_ThrowsInvalidOperationException()
    {
        int?[] dataWithNulls = [1, 2, null, 4];
        var series = Series.From("test_nulls", dataWithNulls);
        Assert.True(series.HasNulls());
        var exception = Assert.Throws<InvalidOperationException>(() => 
        {
            series.AsReadOnlySpan<int>();
        });

        Assert.Contains("Cannot extract Tensor memory", exception.Message);
    }

    [Fact]
    [Trait("Series","ReadOnlySpanString")]
    public void AsReadOnlySpan_StringSeries_ThrowsInvalidOperationException()
    {
        string[] stringData = ["apple", "banana", "cherry"];
        var series = Series.From("test_strings", stringData);

        var exception = Assert.Throws<InvalidOperationException>(() => 
        {
            series.AsReadOnlySpan<byte>(); 
        });
        Assert.Contains("numeric inputs", exception.Message);
        Assert.Contains("encode your strings into numbers", exception.Message);
    }
    [Fact]
    [Trait("Series", "AsTensorSpan")]
    public void As2DTensorSpan_Valid2DArray_ReturnsCorrectShapeAndData()
    {
        float[,] matrixData = new float[,] 
        {
            { 1.1f, 1.2f },
            { 2.1f, 2.2f },
            { 3.1f, 3.2f }
        };
        
        using var series = Series.From("embedding", matrixData);

        var tensor = series.AsTensorSpan<float>();

        Assert.Equal(2, tensor.Rank); 
        
        // tensor.Lengths is ReadOnlySpan<nint>
        Assert.Equal(3, tensor.Lengths[0]); // Rows
        Assert.Equal(2, tensor.Lengths[1]); // Cols
        
        // Check FlattenedLength
        Assert.Equal(6, tensor.FlattenedLength); 
        
        float[] expectedFlat = [1.1f, 1.2f, 2.1f, 2.2f, 3.1f, 3.2f];

        Span<float> dataSpan = new float[6];
        tensor.FlattenTo(dataSpan);
        
        Assert.True(dataSpan.SequenceEqual(expectedFlat));
    }
    [Fact]
    [Trait("Series", "AsTensorSpanJagged")]
    public void As2DTensorSpan_JaggedList_ThrowsInvalidOperationException()
    {
        int[][] jaggedData =
        [
            [1, 2],       
            [3, 4, 5]     
        ];
        
        using var series = Series.From("jagged", jaggedData);

        // Act & Assert
        var exception = Assert.Throws<InvalidOperationException>(() => 
        {
            var tensor = series.AsTensorSpan<int>();
        });

        Assert.Contains("Type mismatch!", exception.Message);
    }
    [Fact]
    [Trait("Series", "AsTensorSpan1D")]
    public void AsTensorSpan_1DSeries_PromotesToColumnVector()
    {
        using var series = Series.From("1d_data", [1, 2, 3]);

        var tensor = series.AsTensorSpan<int>();

        Assert.Equal(2, tensor.Rank);
        Assert.Equal(3, tensor.Lengths[0]); // Rows
        Assert.Equal(1, tensor.Lengths[1]); // Cols
        
        Assert.Equal(1, tensor[0, 0]);
        Assert.Equal(2, tensor[1, 0]);
        Assert.Equal(3, tensor[2, 0]);
    }
    [Fact]
    [Trait("Series", "AsTensorSpanHighDim")]
    public void AsTensorSpan_With4DShape_ReturnsCorrectHighDimensionalTensor()
    {
        float[] flatPixelData = [.. Enumerable.Range(1, 24).Select(i => (float)i)];

        using var series = Series.From("image_batch", flatPixelData);

        ReadOnlySpan<nint> shape4D = [2, 3, 2, 2];

        var tensor = series.AsTensorSpan<float>(shape4D);

        Assert.Equal(4, tensor.Rank); // 4D Tensor
        Assert.Equal(2, tensor.Lengths[0]); // Batch:2
        Assert.Equal(3, tensor.Lengths[1]); // Channels (3 : R, G, B)
        Assert.Equal(2, tensor.Lengths[2]); // Height (2)
        Assert.Equal(2, tensor.Lengths[3]); // Width (2)
        Assert.Equal(24, tensor.FlattenedLength); 

        Assert.Equal(1.0f, tensor[0, 0, 0, 0]);

        Assert.Equal(5.0f, tensor[0, 1, 0, 0]);

        Assert.Equal(24.0f, tensor[1, 2, 1, 1]);
    }
    [Fact]
    [Trait("Series", "AsTransposedTensorSpan")]
    public void AsTransposedTensorSpan_Valid2DArray_ReturnsZeroCopyTransposedView()
    {
        // [ 1.1, 1.2 ]
        // [ 2.1, 2.2 ]
        // [ 3.1, 3.2 ]
        float[,] matrixData = new float[,] 
        {
            { 1.1f, 1.2f },
            { 2.1f, 2.2f },
            { 3.1f, 3.2f }
        };
        
        using var series = Series.From("embedding", matrixData);

        var transposedTensor = series.AsTransposedTensorSpan<float>();

        Assert.Equal(2, transposedTensor.Rank);
        Assert.Equal(2, transposedTensor.Lengths[0]); 
        Assert.Equal(3, transposedTensor.Lengths[1]); 
        Assert.Equal(6, transposedTensor.FlattenedLength);

        // [ 1.1, 2.1, 3.1 ]
        // [ 1.2, 2.2, 3.2 ]
        
        Assert.Equal(1.1f, transposedTensor[0, 0]);
        Assert.Equal(2.1f, transposedTensor[0, 1]); 
        Assert.Equal(3.1f, transposedTensor[0, 2]); 
        
        Assert.Equal(1.2f, transposedTensor[1, 0]); 
        Assert.Equal(2.2f, transposedTensor[1, 1]);
        Assert.Equal(3.2f, transposedTensor[1, 2]);

        float[] expectedSequentialRead = [1.1f, 2.1f, 3.1f, 1.2f, 2.2f, 3.2f];
        
        int i = 0;
        foreach (float val in transposedTensor)
        {
            Assert.Equal(expectedSequentialRead[i], val);
            i++;
        }
    }
    [Fact]
    [Trait("Series", "FromTensor1D")]
    public void FromTensor_1D_CreatesFlatSeries()
    {
        float[] expectedData = [1.1f, 2.2f, 3.3f, 4.4f];
        var tensor = new ReadOnlyTensorSpan<float>(expectedData);

        // Tensor -> Polars
        using var series = Series.FromTensor("scores", tensor);

        Assert.Equal(4, series.Length); 
        
        // Polars -> Tensor
        var readBackSpan = series.AsReadOnlySpan<float>();
        Assert.True(readBackSpan.SequenceEqual(expectedData));
    }

    [Fact]
    [Trait("Series", "FromTensor2D")]
    public void FromTensor_2D_CreatesFixedSizeList()
    {
        // 3 x 2
        float[,] matrix = new float[,]
        {
            { 1.1f, 1.2f },
            { 2.1f, 2.2f },
            { 3.1f, 3.2f }
        };
        var tensor = new ReadOnlyTensorSpan<float>(matrix);

        // Tensor -> Polars
        using var series = Series.FromTensor("embeddings", tensor);

        // Assert
        Assert.Equal(3, series.Length); 

        // Polars -> Tensor
        var readBackTensor = series.AsTensorSpan<float>();
        
        Assert.Equal(2, readBackTensor.Rank);
        Assert.Equal(3, readBackTensor.Lengths[0]);
        Assert.Equal(2, readBackTensor.Lengths[1]);
        
        Assert.Equal(3.2f, readBackTensor[2, 1]); 
    }

    [Fact]
    [Trait("Series", "FromTensor3D")]
    public void FromTensor_3D_CreatesNestedFixedSizeList()
    {
        // [Batch=2, Height=2, Width=2] 
        float[] flatData = [.. Enumerable.Range(1, 8).Select(i => (float)i)];
        ReadOnlySpan<nint> shape3D = [2, 2, 2];
        var tensor = new ReadOnlyTensorSpan<float>(flatData, shape3D);

        using var series = Series.FromTensor("image_batch", tensor);

        Assert.Equal(2, series.Length);

        var readBackTensor = series.AsTensorSpan<float>(shape3D);

        Assert.Equal(3, readBackTensor.Rank);
        Assert.Equal(2, readBackTensor.Lengths[0]);
        Assert.Equal(2, readBackTensor.Lengths[1]);
        Assert.Equal(2, readBackTensor.Lengths[2]);

        Assert.Equal(8.0f, readBackTensor[1, 1, 1]);
    }
    [Fact]
    [Trait("Series", "AsTensor")]
    public void AsTensor_PerformsDeepCopy_SurvivesSeriesDisposal()
    {
        int[] data = [10, 20, 30, 40];
        var series = Series.From("heap_tensor_test", data);

        var heapTensor = series.AsTensor<int>();

        series.Dispose(); 

        Assert.Equal(2, heapTensor.Rank); 
        Assert.Equal(4, heapTensor.Lengths[0]); 
        Assert.Equal(1, heapTensor.Lengths[1]); 

        Assert.Equal(10, heapTensor[0, 0]);
        Assert.Equal(40, heapTensor[3, 0]);
    }
    [Fact]
    [Trait("Series", "TensorUnsafe")]
    public unsafe void GetNativePointers_ReturnsValidMemory_ForFFI()
    {
        float[,] matrix = new float[,]
        {
            { 1.1f, 1.2f, 1.3f },
            { 2.1f, 2.2f, 2.3f }
        };
        
        using var series = Series.From("ffi_matrix", matrix);

        var (ptr, shape) = series.AsDangerousUnmanagedTensor<float>();

        Assert.Equal(2, shape.Length);
        Assert.Equal(2L, shape[0]); 
        Assert.Equal(3L, shape[1]); 

        int totalElements = (int)(shape[0] * shape[1]); 
        
        float* rawFloatPtr = (float*)ptr.ToPointer();
        
        var nativeSpan = new ReadOnlySpan<float>(rawFloatPtr, totalElements);

        Assert.Equal(1.1f, nativeSpan[0]);
        Assert.Equal(1.3f, nativeSpan[2]); 
        Assert.Equal(2.1f, nativeSpan[3]); 
        Assert.Equal(2.3f, nativeSpan[5]); 
    }
    [Fact]
    [Trait("Series", "AsDangerousUnmanagedTensor")]
    public unsafe void AsDangerousUnmanagedTensor_WithValidShape_ReturnsPointerAndReshapedMetadata()
    {
        using var s = Series.From("data", [1.0f, 2.0f, 3.0f, 4.0f, 5.0f, 6.0f]);
        
        // 2 * 3 = 6
        nint[] newShape = [2, 3]; 

        // Act
        var (ptr, shape) = s.AsDangerousUnmanagedTensor<float>(newShape);

        // Assert
        Assert.NotEqual(IntPtr.Zero, ptr);
        
        Assert.Equal(2, shape.Length);
        Assert.Equal(2, shape[0]);
        Assert.Equal(3, shape[1]);

        float* floatPtr = (float*)ptr.ToPointer();
        Assert.Equal(1.0f, floatPtr[0]); 
        Assert.Equal(6.0f, floatPtr[5]); 
    }

    [Fact]
    [Trait("Series", "AsDangerousUnmanagedTensor")]
    public void AsDangerousUnmanagedTensor_WithInvalidShape_ThrowsArgumentException()
    {
        using var s = Series.From("data", [1, 2, 3, 4, 5, 6]);
        
        nint[] badShape = [2, 4]; 

        // Act & Assert
        var ex = Assert.Throws<ArgumentException>(() =>
        {
            s.AsDangerousUnmanagedTensor<int>(badShape);
        });

        Assert.Contains("Shape mismatch", ex.Message);
        Assert.Contains("requires 8 elements", ex.Message);
        Assert.Contains("only has 6 elements", ex.Message);
    }

    [Fact]
    [Trait("Series", "AsDangerousUnmanagedTensor")]
    public void AsDangerousUnmanagedTensor_NotContiguous_ThrowsInvalidOperationException()
    {
       
        using var s1 = Series.From("data", [1, 2]);
        using var s2 = Series.From("data", [3, 4]);
        
       
        s1.Append(s2); 

        nint[] targetShape = [2, 2];

        var ex = Assert.Throws<InvalidOperationException>(() =>
        {
            s1.AsDangerousUnmanagedTensor<int>(targetShape);
        });

        Assert.Contains("fragmented", ex.Message);
        Assert.Contains("call .Rechunk()", ex.Message);
    }
    [Fact]
    [Trait("Series", "AsDangerousUnmanagedTensor")]
    public void AsDangerousUnmanagedTensor_Contiguous()
    {
       
        using var s1 = Series.From("data", [1, 2]);
        using var s2 = Series.From("data", [3, 4]);
       
        s1.Extend(s2); 

        nint[] targetShape = [2, 2];

        var (ptr, shape) = s1.AsDangerousUnmanagedTensor<int>(targetShape);

        Assert.NotEqual(IntPtr.Zero, ptr);
    }
    [Fact]
    [Trait("Series", "AsTensor3D")]
    public void AsTensor_WithShape_PerformsDeepCopyOf3D()
    {
        float[] flatData = [1f, 2f, 3f, 4f, 5f, 6f, 7f, 8f];
        ReadOnlySpan<nint> shape3D = [2, 2, 2];
        
        var series = Series.From("3d_test", flatData);

        var heapTensor = series.AsTensor<float>(shape3D);

        series.Dispose(); 

        Assert.Equal(3, heapTensor.Rank);
        Assert.Equal(2, heapTensor.Lengths[0]);
        Assert.Equal(2, heapTensor.Lengths[1]);
        Assert.Equal(2, heapTensor.Lengths[2]);

        Assert.Equal(8f, heapTensor[1, 1, 1]);
    }
    [Fact]
    [Trait("Series", "SortedFlag")]
    public void Test_Series_Sorted_Flags_And_Verification()
    {
        using var s = new Series("vals", [1, 2, 3, 4, 5]);
        
        Assert.Equal(SortStateFlags.NotSorted, s.SortedFlags);

        Assert.True(s.IsSorted(descending: false));
        Assert.False(s.IsSorted(descending: true));

        using var sAsc = s.SetSorted(descending: false);
        
        Assert.True(sAsc.SortedFlags.HasFlag(SortStateFlags.IsSorted));
        Assert.False(sAsc.SortedFlags.HasFlag(SortStateFlags.Descending));

        using var sDesc = s.SetSorted(descending: true);

        Assert.True(sDesc.SortedFlags.HasFlag(SortStateFlags.IsSorted));
        Assert.True(sDesc.SortedFlags.HasFlag(SortStateFlags.Descending));


        Assert.Equal(SortStateFlags.NotSorted, s.SortedFlags);

        Assert.True(sDesc.IsSorted(descending: false)); 
    }
 
    [Fact]
    [Trait("Series","ChunkLengths")]
    public void Test_Series_ChunkLengths_ReturnsCorrectLengths()
    {
        int[] data = [10, 20, 30, 40, 50];
        
        var series = Pl.Series("test_chunk_lengths", data);

        long[] chunkLengths = series.ChunkLengths();

        Assert.NotNull(chunkLengths);
        
        Assert.Single(chunkLengths); 
        
        Assert.Equal(5L, chunkLengths[0]); 

        Assert.True(series.EstimatedSize(SizeUnit.Bytes)>0);
        
        long totalLength = chunkLengths.Sum();
        Assert.Equal(series.Len(), totalLength);
    }
    [Fact]
    [Trait("Series","IsFirstDistinct")]
    public void Test_Series_IsFirstDistinct()
    {
        int[] data = [114514,1919,810,114514];
        
        var series = Pl.Series("Yajyusenpai", data);
        var firstDistinct = series.IsFirstDistinct();
        var lastDistinct = series.IsLastDistinct();
        Assert.True((bool)firstDistinct.Eq(~lastDistinct)[0]!);
        Assert.True((bool)firstDistinct[0]!);
        Assert.False((bool)firstDistinct[3]!);
    }
    [Fact]
    [Trait("Series","IsInSeries")]
    public void Test_IsIn_With_Another_Series()
    {
        // Arrange
        var s1 = Pl.Series("s1", [1, 2, 3, 4, 5]);
        var s2 = Pl.Series("s2", [2, 4, 6]);
        // Act
        using var result = s2.IsIn(s1);

        // Assert
        Assert.Equal(3, result.Len());

        var boolResult = result.ToArray<bool>(); 
        
        Assert.Equal([true, true, false], boolResult);
    }

    [Fact]
    [Trait("Series","IsInCollection")]
    public void Test_IsIn_With_IEnumerable()
    {
        // Arrange
        var s1 = Pl.Series("fruits", ["apple", "banana", "cherry"]);
        var collection = new List<string> { "banana", "date", "apple" };

        // Act
        using var result = s1.IsIn(collection);

        // Assert
        var boolResult = result.ToArray<bool>(); 
        Assert.Equal([true, true, false], boolResult);
    }

    [Fact]
    [Trait("Series","IsInNull")]
    public void Test_IsIn_With_NullsEqual()
    {
        // Arrange
        var s1 = Pl.Series("with_nulls", new int?[] { 1, null, 3 });
        var s2 = Pl.Series("lookup", new int?[] { null, 2 });

        // Act
        // nullsEqual = false (null != null)
        using var resultFalse = s1.IsIn(s2, nullsEqual: false);
        var boolsFalse = resultFalse.ToArray<bool>();
        Assert.Equal([false, false, false], boolsFalse);

        // nullsEqual = true (null == null)
        using var resultTrue = s1.IsIn(s2, nullsEqual: true);
        var boolsTrue = resultTrue.ToArray<bool>();
        Assert.Equal([false, true, false], boolsTrue);
    }
    [Fact]
    [Trait("Series","Bound")]
    public void Test_Series_LowerBound_And_UpperBound_Int32()
    {
        // Arrange
        var series = Pl.Series("test_int32", [1, 50, 100]);

        // Act
        using var lowerBoundSeries = series.LowerBound();
        using var upperBoundSeries = series.UpperBound();

        // Assert
        Assert.Equal(1, lowerBoundSeries.Len());
        Assert.Equal(1, upperBoundSeries.Len());

        var lowerVal = lowerBoundSeries.ToArray<int>()[0];
        var upperVal = upperBoundSeries.ToArray<int>()[0];

        Assert.Equal(int.MinValue, lowerVal); // -2147483648
        Assert.Equal(int.MaxValue, upperVal); //  2147483647
    }

    [Fact]
    [Trait("Series","Bound")]
    public void Test_Series_LowerBound_And_UpperBound_UInt8()
    {
        // Arrange
        var series = Pl.Series("test_uint8", new byte[] { 10, 20 });

        // Act
        using var lowerBoundSeries = series.LowerBound();
        using var upperBoundSeries = series.UpperBound();

        // Assert
        var lowerVal = lowerBoundSeries.ToArray<byte>()[0];
        var upperVal = upperBoundSeries.ToArray<byte>()[0];

        // Byte / UInt8 lower bound is , upper bound is 255
        Assert.Equal(byte.MinValue, lowerVal); // 0
        Assert.Equal(byte.MaxValue, upperVal); // 255
    }
    [Fact]
    [Trait("Series","UniqueCounts")]
    public void Test_Series_UniqueCounts()
    {
        var series = Pl.Series("id", ["a", "b", "b", "c", "c", "c"]);

        var uc = series.UniqueCounts();

        Assert.Equal([1,2,3],uc.ToArray<int>());
    }
    [Fact]
    [Trait("Series","MaxBy")]
    public void Test_Series_MaxBy_And_MinBy_With_Series()
    {
        var target = Pl.Series("values", [10, 20, 30, 40]);
        
        var bySeries = Pl.Series("weights", [2.5, 9.9, 3.2, 0.5]);

        int? maxByResult = target.MaxBy<int>(bySeries);

        int? minByResult = target.MinBy<int>(bySeries);

        Assert.Equal(20, maxByResult);
        Assert.Equal(40, minByResult);
    }

    [Fact]
    [Trait("Series","MaxByNull")]
    public void Test_Series_MaxBy_Empty_Returns_Null()
    {
        // Arrange
        var target = Pl.Series("empty_vals", System.Array.Empty<int>());
        var bySeries = Pl.Series("empty_weights", System.Array.Empty<int>());
        // // Act
        int? maxByResult = target.MaxBy<int>(bySeries);
        int? minByResult = target.MinBy<int>(bySeries);

        // Assert
        Assert.Null(maxByResult);
        Assert.Null(minByResult);
    }
    [Fact]
    [Trait("Series","MaxBy")]
    public void Test_Series_MaxBy_With_Expression()
    {
        // Arrange
        var target = Pl.Series("values", [-5, 2, -10, 8]);

        // Act
        int? maxByAbsResult = target.MaxBy<int>(Pl.Col("values").Abs());

        // Assert
        Assert.Equal(-10, maxByAbsResult);
    }
    [Fact]
    [Trait("Series","Mode")]
    public void Test_Series_Mode_Returns_Multiple_Values()
    {
        // Arrange
        var series = Pl.Series("multi_mode", [10, 20, 10, 20, 30]);

        // Act
        using var modeSeries = series.Mode();

        // Assert
        Assert.Equal(2, modeSeries.Len());

        var resultVals = modeSeries.ToArray<int>();
        
        Assert.Contains(10, resultVals);
        Assert.Contains(20, resultVals);
        Assert.DoesNotContain(30, resultVals);
    }
    [Fact]
    [Trait("Series","TimeSpanStatistics")]
    public void Test_Series_TimeSpan_Statistics()
    {
        // Arrange (准备阶段)
        // 构造一个间隔均匀的延迟时间序列 (100ms, 120ms, 140ms)
        var latencies = Pl.Series("latency",
        [
            TimeSpan.FromMilliseconds(100), 
            TimeSpan.FromMilliseconds(120), 
            TimeSpan.FromMilliseconds(140) 
        ]);

        // Act (执行阶段)
        TimeSpan? meanVal = latencies.Mean<TimeSpan>();
        
        TimeSpan? medianVal = latencies.Median<TimeSpan>();

        // 4. Std (标准差, ddof=1)
        TimeSpan? stdVal = latencies.Std<TimeSpan>();

        // Assert (断言阶段)
        Assert.Equal(TimeSpan.FromMilliseconds(120), meanVal);
        Assert.Equal(TimeSpan.FromMilliseconds(120), medianVal);
        // Assert.Equal(TimeSpan.FromMilliseconds(400), varVal);
        Assert.Equal(TimeSpan.FromMilliseconds(20), stdVal);
    }
    [Fact]
    [Trait("Series","DateTimeStatistics")]
    public void Test_Series_DateTime_Statistics()
    {
        // Arrange
        var dtSeries = Pl.Series("datetimes",
        [
            new DateTime(2024, 1, 1, 10, 0, 0),
            new DateTime(2024, 1, 1, 12, 0, 0),
            new DateTime(2024, 1, 1, 14, 0, 0)
        ]);

        // Act & Assert
        var median = dtSeries.Median<DateTime>();
        Assert.Equal(new DateTime(2024, 1, 1, 12, 0, 0), median);
        var mean = dtSeries.Mean<DateTime>();
        Assert.Equal(new DateTime(2024, 1, 1, 12, 0, 0), mean);
    }
    [Fact]
    [Trait("Series","DateOnlyStatistics")]
    public void Test_Series_DateOnly_Statistics()
    {
        // Arrange
        var dateSeries = Pl.Series("dates",
        [
            new DateOnly(2024, 1, 1),
            new DateOnly(2024, 1, 3),
            new DateOnly(2024, 1, 5)
        ]);

        // Act & Assert
        var median = dateSeries.Median<DateTime>();
        Assert.Equal(new DateTime(2024, 1, 3), median);
        var mean = dateSeries.Mean<DateTime>();
        Assert.Equal(new DateTime(2024, 1, 3), mean);
    }
    [Fact]
    [Trait("Series","TimeOnlyStatistics")]
    public void Test_Series_TimeOnly_Statistics()
    {
        // Arrange
        var timeSeries = Pl.Series("times",
        [
            new TimeOnly(9, 0),
            new TimeOnly(9, 30),
            new TimeOnly(10, 0)
        ]);

        // Act & Assert
        var median = timeSeries.Median<TimeOnly>();
        Assert.Equal(new TimeOnly(9, 30), median);
        var mean = timeSeries.Mean<TimeOnly>();
        Assert.Equal(new TimeOnly(9, 30), mean);
    }
    [Fact]
    [Trait("Series","NanMaxString")]
    public void Test_Series_NanMax_String_Lexicographical()
    {
        // Arrange
        var strSeries = Pl.Series("words", ["apple", "zebra", null, "banana"]);

        // Act
        var maxStr = strSeries.NanMaxString();
        var minStr = strSeries.NanMinString();

        // Assert
        Assert.Equal("zebra", maxStr);
        Assert.Equal("apple", minStr);
    }
    [Fact]
    [Trait("Series","NanMax")]
    public void Test_Series_NanMax_Float_Ignores_NaN()
    {
        // Arrange
        var floatSeries = Pl.Series("floats", new double?[] { 10.5, double.NaN, null, 42.0, 3.14 });

        var normalMax = floatSeries.Max<double>();

        var nanMax = floatSeries.NanMax<double>();
        var nanMin = floatSeries.NanMin<double>();

        // Assert
        Assert.Equal(42.0, normalMax);                 
        Assert.Equal(double.NaN, nanMax);        
        Assert.Equal(double.NaN, nanMin);              
    }
    [Fact]
    [Trait("Series","Bitwise")]
    public void Test_Bitwise_Aggregations_Return_Scalar()
    {
        // Arrange
        // 15 = 1111
        // 11 = 1011
        // 13 = 1101
        var series = Pl.Series("flags", [15, 11, 13]);
        
        // Act
        var andAgg = series.BitwiseAnd<int>();
        var orAgg  = series.BitwiseOr<int>();
        var xorAgg = series.BitwiseXor<int>();

        // AND : 1111 & 1011 & 1101 = 1001 (9)
        Assert.Equal(9, andAgg); 

        // OR : 1111 | 1011 | 1101 = 1111 (15)
        Assert.Equal(15, orAgg);

        // XOR : 1111 ^ 1011 ^ 1101 = 1001 (9)
        Assert.Equal(9, xorAgg);
    }

    [Fact]
    [Trait("Series","Bitwise")]
    public void Test_Bitwise_ElementWise_Transformations()
    {
        // Arrange
        // 0: 00000000 
        // 5: 00000101 
        // 7: 00000111 
        var series = Pl.Series("nums", [0, 5, 7]);

        // Act
        using var countOnes = series.BitwiseCountOnes();
        using var trailingZeros = series.BitwiseTrailingZeros();

        // Assert
        Assert.Equal(3, countOnes.Len());

        var onesResult = countOnes.ToArray<uint>(); 
        Assert.Equal(new uint[] { 0, 2, 3 }, onesResult);
    }
    [Fact]
    [Trait("Series", "ReshapeExplicit")]
    public void Test_Series_Reshape_Explicit_Dimensions()
    {
        using Series s = Pl.Series("a", [1, 2, 3, 4, 5, 6]);

        using Series reshaped = s.Reshape([2, 3]);
        
        Assert.Equal(2, reshaped.Len());
        
        Assert.Equal(DataType.Array(typeof(int), 3), reshaped.DataType);

        using Series structSeries = reshaped.Array.ToStruct(["c1", "c2", "c3"]);
        using Series c1 = structSeries.Struct.Field("c1");
        using Series c2 = structSeries.Struct.Field("c2");
        using Series c3 = structSeries.Struct.Field("c3");
        
        Assert.Equal([1, 4], c1.ToArray<int>());
        Assert.Equal([2, 5], c2.ToArray<int>());
        Assert.Equal([3, 6], c3.ToArray<int>());

        using Series backed = reshaped.Reshape([-1]);
        Assert.Equal([1,2,3,4,5,6],backed.ToArray<int>());
    }

    [Fact]
    [Trait("Series", "ReshapeInferred")]
    public void Test_Series_Reshape_Inferred_Dimensions()
    {
        using Series s = Pl.Series("a", [1, 2, 3, 4, 5, 6]);

        using Series reshaped = s.Reshape([-1, 2]);
        
        Assert.Equal(3, reshaped.Len());
        Assert.Equal(DataType.Array(typeof(int), 2), reshaped.DataType);

        using Series structSeries = reshaped.Array.ToStruct(["c1", "c2"]);
        using Series c1 = structSeries.Struct.Field("c1");
        using Series c2 = structSeries.Struct.Field("c2");
        
        Assert.Equal([1, 3, 5], c1.ToArray<int>());
        Assert.Equal([2, 4, 6], c2.ToArray<int>());
    }

    [Fact]
    [Trait("Series", "ReshapeError")]
    public void Test_Series_Reshape_Mismatch_Throws()
    {
        using Series s = Pl.Series("a", [1, 2, 3, 4, 5, 6]);

        
        var ex = Assert.Throws<PolarsException>(() => 
        {
            using Series badReshape = s.Reshape([5, 5]); 
        });

        Assert.Contains("cannot reshape", ex.Message, StringComparison.OrdinalIgnoreCase);
    }
    [Fact]
    [Trait("Series", "Conditional")]
    public void Test_Series_ZipWith()
    {
        int[] data1 = [1, 2, 3, 4, 5];
        using Series s1 = Pl.Series("s1", data1);

        int[] data2 = [10, 20, 30, 40, 50];
        using Series s2 = Pl.Series("s2", data2);

        bool[] maskData = [true, false, true, false, true];
        using Series mask = Pl.Series("mask", maskData);

        using Series result = s1.ZipWith(mask, s2);

        // mask: [true, false, true, false, true]
        // s1:   [1,    2,     3,    4,     5]
        // s2:   [10,   20,    30,   40,    50]
        // Except: [1,   20,    3,    40,    5]
        Assert.Equal([1, 20, 3, 40, 5], result.ToArray<int>());
        
        Assert.Equal("s1", result.Name);
    }
    [Fact]
    [Trait("Series", "ToDummies")]
    public void Test_Series_ToDummies()
    {
        int[] data1 = [1, 2, 3, 4, 5];
        using Series s1 = Pl.Series("s1", data1);

        using DataFrame d1 = s1.ToDummies("nihao");
        Assert.Contains("s1nihao1",d1.Columns);
        Assert.Equal(1,(byte)d1[0][0]!);

        using DataFrame d2 = s1.ToDummies(dropFirst:true);
        Assert.Equal(4,d2.Width);

        int?[] data2 = [1,2,null,4,5];
        using Series s2 = Pl.Series("s2",data2);
        using DataFrame d3 = s2.ToDummies(dropNulls:true);
        Assert.DoesNotContain("s2_3",d3.ColumnNames);
    }
    [Fact]
    [Trait("Series","NewFromIndex")]
    public void Test_Series_FromIndex()
    {
        string?[] genshin = ["114514","卧槽，原！",null];
        using Series s1 = Pl.Series("bilibili",genshin);

        using Series s1n = s1.NewFromIndex(1,5);
        Assert.Equal(5,s1n.Length);
        Assert.Equal("卧槽，原！",s1n[0]);

        using Series s2n = s1.NewFromIndex(2,5);
        Assert.Null(s2n[2]);
    }
    [Fact]
    [Trait("Series","Rle")]
    public void Test_Series_Rle()
    {
        int?[] genshin = [1, 1, 2, 1, null, 1, 3, 3];
        using Series s1 = Pl.Series("bilibili",genshin);

        using DataFrame d1 = s1.Rle().Unnest();
        Assert.Equal(2u,d1["len"][0]);
        Assert.Equal(1,d1["value"][0]);
        Assert.Equal(1u,d1["len"][3]);
        Assert.Null(d1["value"][3]);

        using Series s2 = s1.RleId();
        Assert.Equal(8,s2.Length);
    }
    [Fact]
    [Trait("Series", "Replace")]
    public void Test_Series_Replace()
    {
        int[] data = [1, 2, 3, 2, 1];
        using Series s = Pl.Series("test_replace", data);

        using Series s1 = s.Replace(2, 20);
        Assert.Equal([1, 20, 3, 20, 1], s1.ToArray<int>());

        var mapping = new Dictionary<int, int>
        {
            { 1, 10 },
            { 3, 30 }
        };
        using Series s2 = s.Replace(mapping);
        Assert.Equal([10, 2, 30, 2, 10], s2.ToArray<int>());

        int[] oldVals = [1, 2];
        int[] newVals = [100, 200];
        using Series s3 = s.Replace(oldVals, newVals);
        Assert.Equal([100, 200, 3, 200, 100], s3.ToArray<int>());

        using Series s4 = s.ReplaceStrict(2, 2000, defaultExpr: 0);
        Assert.Equal([0, 2000, 0, 2000, 0], s4.ToArray<int>());
        
        Assert.Equal("test_replace", s1.Name);
    }
    [Fact]
    [Trait("Series", "Peak")]
    public void Test_Series_Peaks()
    {
        int[] data1 = [1, 2, 114514, -2200, 59796016];
        using Series s1 = Pl.Series("s1", data1);

        using Series sp1 = s1.PeakMax();
        using Series sp2 = s1.PeakMin();
        Assert.False((bool)sp1[0]!);
        Assert.True((bool)sp1[4]!);
        Assert.True((bool)sp2[3]!);
    }
    [Fact]
    [Trait("Series", "Binning")]
    public void Test_Series_Cut_And_QCut()
    {
        double[] data = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0];
        using Series s = Pl.Series("values", data);

        // Breaks: 3.0, 7.0 -> (-inf, 3.0], (3.0, 7.0], (7.0, inf]
        ReadOnlySpan<double> breaks = [3.0, 7.0];
        string[] cutLabels = ["Low", "Medium", "High"];
        
        using Series sCut = s.Cut(breaks, labels: cutLabels);
        sCut.Show();
        Assert.Equal(10, sCut.Length);
        Assert.Equal(DataTypeKind.Categorical,sCut.DataType.Kind);

        // quantiles: 0.5  -> (-inf, 50%], (50%, inf]
        ReadOnlySpan<double> probs = [0.5];
        string[] qcutLabels = ["Bottom_Half", "Top_Half"];
        
        using Series sQCutProbs = s.QCut(probs, labels: qcutLabels);
        
        Assert.Equal(10, sQCutProbs.Length);
        Assert.Equal(DataTypeKind.Categorical,sQCutProbs.DataType.Kind);

        string[] uniformLabels = ["Q1", "Q2", "Q3", "Q4"];
        using Series sQCutUniform = s.QCut(4, labels: uniformLabels);
        
        Assert.Equal(10, sQCutUniform.Length);
        Assert.Equal(DataTypeKind.Categorical,sQCutUniform.DataType.Kind);
        
        using Series sCutWithBreaks = s.Cut(breaks, includeBreaks: true);
        Assert.Equal(DataTypeKind.Struct, sCutWithBreaks.DataType.Kind);
    }
    [Fact]
    [Trait("Series", "IsClose")]
    public void Test_Series_IsClose()
    {
        using Series s = Pl.Series("nihao",[114514,1919,810,725000]);
        using Series sC = s.IsClose(114515,absTol: 2.0);
        Assert.True((bool)sC[0]!);
        Assert.False((bool)sC[3]!);
    }
    [Fact]
    [Trait("Series", "CumulativeEval")]
    public void Test_Series_CumEval()
    {
        using Series s = Pl.Series("nihao",[1,2,3,4,5]);
        using Series sE = s.CumulativeEval(Pl.Element().First() - Pl.Element().Last().Pow(2));
        Assert.Equal(-24.0,sE[4]);
    }
    [Fact]
    [Trait("Series", "Log")]
    public void Test_Series_Log()
    {
        using Series s = Pl.Series("nihao",[100.0,Math.E,8]);
        using Series sL = s.Log();
        using Series s10 = s.Log10();
        using Series s2 = s.Log(2);
        using Series s1p = s.Log1p();
        Assert.Equal(1.0,sL[1]);
        Assert.Equal(2.0,s10[0]);
        Assert.Equal(3.0,s2[2]);
        Assert.Equal(4.615121,(double)s1p[0]!,1e-5);
    }
}