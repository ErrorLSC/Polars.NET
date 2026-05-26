using Apache.Arrow;
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars; 

namespace Polars.CSharp.Tests;

public static class UdfLogic
{
    public static IArrowArray IntToString(IArrowArray arr)
    {
        if (arr is Int64Array i64Arr)
        {
            var builder = new StringViewArray.Builder();
            for (int i = 0; i < i64Arr.Length; i++)
            {
                if (i64Arr.IsNull(i))
                {
                    builder.AppendNull();
                }
                else
                {
                    long? v = i64Arr.GetValue(i);
                    builder.Append($"Value: {v}");
                }
            }
            return builder.Build();
        }
        
        if (arr is Int32Array i32Arr)
        {
            var builder = new StringViewArray.Builder();
            for (int i = 0; i < i32Arr.Length; i++)
            {
                if (i32Arr.IsNull(i))
                {
                    builder.AppendNull();
                }
                else
                {
                    int? v = i32Arr.GetValue(i);
                    builder.Append($"Value: {v}");
                }
            }
            return builder.Build();
        }

        throw new ArgumentException($"Expected Int32Array or Int64Array, but got: {arr.GetType().Name}");
    }

    public static IArrowArray AlwaysFail(IArrowArray arr)
    {
        throw new Exception("Boom! C# UDF Exploded!");
    }
    public static IArrowArray IntToDouble(IArrowArray arr)
    {
        if (arr is Int64Array i64Arr)
        {
            var builder = new DoubleArray.Builder();
            for (int i = 0; i < i64Arr.Length; i++)
            {
                if (i64Arr.IsNull(i)) 
                {
                    builder.AppendNull();
                }
                else 
                {
                    double val = i64Arr.GetValue(i).Value;
                    builder.Append(val / 2.0);
                }
            }
            return builder.Build();
        }
        if (arr is Int32Array i32Arr)
        {
            var builder = new DoubleArray.Builder();
            for (int i = 0; i < i32Arr.Length; i++)
            {
                if (i32Arr.IsNull(i)) builder.AppendNull();
                else builder.Append(i32Arr.GetValue(i).Value / 2.0);
            }
            return builder.Build();
        }

        throw new ArgumentException($"Expected Int32/64, got {arr.GetType().Name}");
    }
}

public class UdfTests
{
    [Fact]
    public void Map_UDF_Memory_Data_Test()
    {
        int rowCount = 5;
        var builder = new Int64Array.Builder();
        for (int i = 0; i < rowCount; i++) builder.Append(i * 10); // 0, 10, 20, 30, 40
        var arrowArray = builder.Build();

        using var df = DataFrame.FromArrow(
            new RecordBatch.Builder()
                .Append("num", false, col => col.Int64(arr => arr.AppendRange(Enumerable.Range(0, rowCount).Select(x => (long)x * 10))))
                .Build()
        );

        Assert.Equal(5, df.Height);

        Func<IArrowArray, IArrowArray> udf = UdfLogic.IntToDouble;

        using var res = df.Select(
            Pl.Col("num").Map(udf, Pl.Float64).Alias("res")
        );

        Assert.Equal(5, res.Height);

        Assert.NotNull(res.Column("res"));
        Assert.Equal(5, res.Column("res").Length);
        Assert.Equal(0.0, res.Column("res").GetValue<double>(0)); // 0 / 2
        Assert.Equal(20.0, res.Column("res").GetValue<double>(4)); // 40 / 2
    }
    [Fact]
    public void Test_UDF_Map_Stable()
    {
        using var csv = new DisposableFile("num\n15\n25\n" +
                                          "35\n45\n55\n",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);
        Assert.Equal(5, df.Height); 

        // UDF (Int64 -> Int64)
        var udf = Pl.Col("num").Map<long, long>(x => x * 2, typeof(long)).Alias("res");

        using var res = df.Select(
            Pl.Col("num"),
            udf
        );
        Assert.Equal(5, res.Height); 
        
        Assert.Equal(30, res.Column("res").GetValue<long>(0));
        Assert.Equal(50, res.Column("res").GetValue<long>(1));
        Assert.Equal(70, res.Column("res").GetValue<long>(2));
    }
    [Fact]
    public void Map_UDF_Can_Change_Data_Type_Int_To_String()
    {
        using var csv = new DisposableFile("num\n100\n200\n",".csv");
        using var lf = LazyFrame.ScanCsv(csv.Path);

        Func<IArrowArray, IArrowArray> udf = UdfLogic.IntToString;

        using var df = lf.Select(
            Pl.Col("num")
            .Map(udf, DataType.String) 
            .Alias("desc")
        ).Collect();
        df.Show();
        
        Assert.NotNull(df.Column("desc"));
        Assert.Equal("Value: 100", df.Column("desc").GetValue<string>(0)); 
        Assert.Equal("Value: 200", df.Column("desc").GetValue<string>(1));
    }

    [Fact]
    public void Map_UDF_Error_Is_Propagated_To_CSharp()
    {
        using var csv = new DisposableFile("num\n1",".csv");
        using var lf = LazyFrame.ScanCsv(csv.Path);

        Func<IArrowArray, IArrowArray> udf = UdfLogic.AlwaysFail;

        var ex = Assert.Throws<PolarsException>(() => 
        {
            lf.Select(
                Pl.Col("num").Map(udf, Pl.SameAsInput)
            ).Collect();
        });

        Assert.Contains("Boom! C# UDF Exploded!", ex.Message);
    }
    [Fact]
    public void Test_UDF_HighLevel_Numeric()
    {
        using var csv = new DisposableFile("num\n10\n20\n30\n",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        var doubleExpr = Pl.Col("num")
            .Map<long, long>(x => x * 2, Pl.Int64)
            .Alias("doubled");

        using var res = df.Select(Pl.Col("num"), doubleExpr);
        
        Assert.Equal(20, res.Column("doubled").GetValue<long>(0)); // 10 * 2
        Assert.Equal(60, res.Column("doubled").GetValue<long>(2)); // 30 * 2
    }

    [Fact]
    public void Test_UDF_HighLevel_String_Manipulation()
    {
        using var csv = new DisposableFile("name\nAlice\nBob\n",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // UDF: "Hello, {name}!"
        var greetExpr = Pl.Col("name")
            .Map<string, string>(name => $"Hello, {name}!", typeof(string))
            .Alias("greeting");

        using var res = df.Select(Pl.Col("name"), greetExpr);
        
        Assert.Equal("Hello, Alice!", res.Column("greeting").GetValue<string>(0));
        Assert.Equal("Hello, Bob!", res.Column("greeting").GetValue<string>(1));
    }

    [Fact]
    public void Test_UDF_HighLevel_Type_Conversion()
    {
        // UDF Int64 -> String
        
        using var csv = new DisposableFile("id\n1001\n1002\n",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        var formatExpr = Pl.Col("id")
            .Map<long, string>(id => $"Order-{id}", typeof(string))
            .Alias("order_id");

        using var res = df.Select(Pl.Col("id"), formatExpr);
        
        Assert.Equal("Order-1001", res.Column("order_id").GetValue<string>(0));
        Assert.Equal("Order-1002", res.Column("order_id").GetValue<string>(1));
    }
    [Fact]
    public void Test_UDF_Nullable_Output()
    {
        using var csv = new DisposableFile("num\n10\n0\n20\n",".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // 0 -> null (C# null)
        var cleanExpr = Pl.Col("num")
            .Map<long, long?>(x => x == 0 ? null : x, Pl.Int64)
            .Alias("cleaned");

        using var res = df.Select(Pl.Col("num"), cleanExpr);
        
        Assert.Equal(10, res.Column("cleaned").GetValue<long>(0));
        Assert.Null(res.Column("cleaned").GetValue<long?>(1)); // 0 -> Null
        Assert.Equal(20, res.Column("cleaned").GetValue<long>(2));
    }
    [Fact]
    public void Test_UDF_Nullable_Input()
    {
        using var csv = new DisposableFile("num\n10\n\n",".csv"); 
        using var df = DataFrame.ReadCsv(csv.Path);

        // int? -> string
        // null -> "FoundNull" or "Value:{x}"
        var checkNullExpr = Pl.Col("num")
            .Map<long?, string>(x => x.HasValue ? $"Value:{x}" : "FoundNull", Pl.Utf8)
            .Alias("status");

        using var res = df.Select(Pl.Col("num"), checkNullExpr);
        
        Assert.Equal("Value:10", res.Column("status").GetValue<string>(0));
        Assert.Equal("FoundNull", res.Column("status").GetValue<string>(1)); 
    }
    [Fact]
    public void Test_GroupBy_Agg_With_HighLevel_Lambda()
    {
        using var df = DataFrame.FromSeries(
            new Series("key", ["A", "A", "B", "B"]),
            new Series("val", [1L, 2L, 3L, 4L]) 
        );

        static long myGroupLogic(long[] nums)
        {
            if (nums == null || nums.Length == 0) return 0;
            return nums.Max() + 10;
        }

        // GroupBy Agg
        var res = df.GroupBy("key")
                    .Agg(
                        Pl.Col("val")
                        .Implode() 
                        .Map((Func<long[], long>)myGroupLogic, typeof(long)) 
                        .Alias("custom_agg")
                    )
                    .Sort("key");

        // A: [1, 2] -> Max 2 -> +10 = 12
        Assert.Equal(12, res["custom_agg"].GetValue<long>(0));
        
        // B: [3, 4] -> Max 4 -> +10 = 14
        Assert.Equal(14, res["custom_agg"].GetValue<long>(1));
    }
    [Fact]
    [Trait("UDF", "MapMany")]
    public void Test_ExprMap_MultiColumn_Add_ArrowRaw()
    {
        // Arrange
        using var df = Pl.CreateDataFrame(
            ("a", new int[] { 1, 2, 3, 4, 5 }),
            ("b", new int[] { 10, 20, 30, 40, 50 })
        );

        // Act
        using var result = df.Select(
            Pl.Col("a").Map(
                function: arrays => {
                    // arrays[0] = col a, arrays[1] = col b
                    var a = (Int32Array)arrays[0];
                    var b = (Int32Array)arrays[1];
                    var builder = new Int32Array.Builder();
                    for (int i = 0; i < a.Length; i++)
                    {
                        if (a.IsNull(i) || b.IsNull(i))
                            builder.AppendNull();
                        else
                            builder.Append(a.GetValue(i)!.Value + b.GetValue(i)!.Value);
                    }
                    return builder.Build();
                },
                outputType: DataType.Int32,
                additionalExprs: Pl.Col("b")
            )
        );

        // Assert
        var summed = result[0].ToArray<int>();
        var expected = new int[] { 11, 22, 33, 44, 55 };
        Assert.Equal(expected, summed);
    }
}
