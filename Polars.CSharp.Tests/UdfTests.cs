using System.Text.RegularExpressions;
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

public partial class UdfTests
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
            Pl.Col("num").MapBatches(udf, Pl.Float64).Alias("res")
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
        var udf = Pl.Col("num").MapBatches<long, long>(x => x * 2).Alias("res");

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
            .MapBatches(udf, DataType.String)
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
                Pl.Col("num").MapBatches(udf, Pl.SameAsInput)
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
            .MapBatches<long, long>(x => x * 2)
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
            .MapBatches<string, string>(name => $"Hello, {name}!")
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
            .MapBatches<long, string>(id => $"Order-{id}")
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
            .MapBatches<long, long?>(x => x == 0 ? null : x)
            .Alias("cleaned");

        using var res = df.Select(Pl.Col("num"), cleanExpr);

        Assert.Equal(10, res.Column("cleaned").GetValue<long>(0));
        Assert.Null(res.Column("cleaned").GetValue<long?>(1)); // 0 -> Null
        Assert.Equal(20, res.Column("cleaned").GetValue<long>(2));
    }
    [Fact]
    public void Test_UDF_Nullable_Input()
    {
        int? [] values = [10,null,null];
        using DataFrame df = [Pl.CreateSeries("num", values)];

        // int? -> string
        // null -> "FoundNull" or "Value:{x}"
        var checkNullExpr = Pl.Col("num")
            .MapBatches<int?, string>(x => x.HasValue ? $"Value:{x}" : "FoundNull")
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
                        .MapBatches((Func<long[], long>)myGroupLogic)
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
            Pl.Col("a").MapBatches(
                function: arrays =>
                {
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
    private sealed class EmployeeInput
    {
        public string Name { get; set; } = string.Empty;
        public int Age { get; set; }
        public double BaseSalary { get; set; }
        public double Bonus { get; set; }
    }

    private sealed class EmployeeOutput
    {
        public string Name { get; set; } = string.Empty;
        public double TotalCompensation { get; set; }
        public bool IsSenior { get; set; }
    }

    [Fact]
    [Trait("UDF", "MapRows")]
    public void Test_MapRows_PrimitiveProjection()
    {
        var df = DataFrame.FromRows(
        [
            new EmployeeInput { Name = "Alice", Age = 32, BaseSalary = 100_000, Bonus = 15_000 },
            new EmployeeInput { Name = "Bob",   Age = 25, BaseSalary = 70_000,  Bonus = 5_000 },
            new EmployeeInput { Name = "Charlie", Age = 45, BaseSalary = 130_000, Bonus = 30_000 }
        ]);

        var res = df.MapRows<EmployeeInput, EmployeeOutput>(emp => new EmployeeOutput
        {
            Name = emp.Name.ToUpperInvariant(),
            TotalCompensation = emp.BaseSalary + emp.Bonus,
            IsSenior = emp.Age >= 30
        });

        Assert.Equal(3, res.Height);
        Assert.Equal(3, res.Width);
        Assert.Equal(["Name", "TotalCompensation", "IsSenior"], res.Columns);

        // Verify values
        Assert.Equal("ALICE", res.GetValue<string>(0, "Name"));
        Assert.Equal(115_000.0, res.GetValue<double>(0, "TotalCompensation"));
        Assert.True(res.GetValue<bool>(0, "IsSenior"));

        Assert.Equal("BOB", res.GetValue<string>(1, "Name"));
        Assert.Equal(75_000.0, res.GetValue<double>(1, "TotalCompensation"));
        Assert.False(res.GetValue<bool>(1, "IsSenior"));
    }

    [Fact]
    [Trait("UDF", "MapRows")]
    public void Test_MapRows_EmptyDataFrame()
    {
        var emptyDf = DataFrame.FromRows(System.Array.Empty<EmployeeInput>());

        var res = emptyDf.MapRows<EmployeeInput, EmployeeOutput>(emp => new EmployeeOutput
        {
            Name = emp.Name,
            TotalCompensation = emp.BaseSalary + emp.Bonus,
            IsSenior = emp.Age >= 30
        });

        Assert.Equal(0, res.Height);
        Assert.Equal(3, res.Width);
        Assert.Equal(["Name", "TotalCompensation", "IsSenior"], res.Columns);
    }
    // C# Source Generated Regex: matches passwords that satisfy complex requirements (contains uppercase, lowercase, digit via lookahead)
    [GeneratedRegex(@"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d).+$")]
    private static partial Regex StrongPasswordRegex();

    // C# Source Generated Regex: masks domain names or extracts specific patterns
    [GeneratedRegex(@"@(?=[a-zA-Z0-9.-]+)")]
    private static partial Regex EmailAtLookaheadRegex();

    [Fact]
    [Trait("UDF", "Regex")]
    public void Test_UDF_Source_Generated_Regex_Mapping()
    {
        using var df = DataFrame.FromColumns([
            Series.From("User", ["Alice", "Bob", "Charlie", "David"]),
            Series.From("Email", ["alice@example.com", "bob_at_home", "charlie@polars.net", "david@corp.org"]),
            Series.From("Password", ["P@ssw0rd1", "weakpass", "Admin2026", "123456"])
        ]);

        // 1. Transform Mapping: using SG Regex to mask email with lookahead
        var maskEmailExpr = Pl.Col("Email")
            .MapBatches<string, string>(email => EmailAtLookaheadRegex().Replace(email, "[at]"))
            .Alias("MaskedEmail");

        // 2. Boolean Mapping: using complex lookahead SG Regex to validate passwords
        var isValidPwdExpr = Pl.Col("Password")
            .MapBatches<string, bool>(pwd => StrongPasswordRegex().IsMatch(pwd))
            .Alias("IsValidPassword");

        using var result = df.Select(
            Pl.Col("User"),
            maskEmailExpr,
            isValidPwdExpr
        );

        // Verify Transform Mapping
        Assert.Equal("alice[at]example.com", result.Column("MaskedEmail").GetValue<string>(0));
        Assert.Equal("bob_at_home", result.Column("MaskedEmail").GetValue<string>(1));
        Assert.Equal("charlie[at]polars.net", result.Column("MaskedEmail").GetValue<string>(2));

        // Verify Boolean Validation Mapping
        Assert.True(result.Column("IsValidPassword").GetValue<bool>(0));  // "P@ssw0rd1" -> Valid
        Assert.False(result.Column("IsValidPassword").GetValue<bool>(1)); // "weakpass"  -> Invalid
        Assert.True(result.Column("IsValidPassword").GetValue<bool>(2));  // "Admin2026"  -> Valid
        Assert.False(result.Column("IsValidPassword").GetValue<bool>(3)); // "123456"     -> Invalid
    }

    [Fact]
    [Trait("UDF", "MapGroups")]
    public void Test_MapGroups_SingleColumn_Aggregation()
    {
        using var df = DataFrame.FromColumns([
            Series.From("group", ["A", "A", "A", "B", "B"]),
            Series.From("val", [10.0, 20.0, 30.0, 100.0, 200.0])
        ]);

        // Group custom aggregation: range = max - min
        using var res = df.GroupBy("group")
            .Agg(
                Pl.Col("val").MapGroups(series =>
                {
                    double min = series.Min<double>()!.Value;
                    double max = series.Max<double>()!.Value;
                    return Series.From("range", [max - min]);
                }, DataType.Float64).Alias("range")
            )
            .Sort("group");

        Assert.Equal(2, res.Height);
        Assert.Equal("A", res.GetValue<string>(0, "group"));
        Assert.Equal(20.0, res.GetValue<double>(0, "range")); // 30 - 10
        Assert.Equal("B", res.GetValue<string>(1, "group"));
        Assert.Equal(100.0, res.GetValue<double>(1, "range")); // 200 - 100
    }

    [Fact]
    [Trait("UDF", "MapGroups")]
    public void Test_MapGroups_MultiColumn_Aggregation()
    {
        using var df = DataFrame.FromColumns([
            Series.From("dept", ["Sales", "Sales", "Eng", "Eng"]),
            Series.From("salary", [5000.0, 7000.0, 8000.0, 12000.0]),
            Series.From("bonus", [500.0, 1000.0, 1000.0, 2000.0])
        ]);

        // Multi-column MapGroups: total compensation = sum(salary + bonus)
        using var res = df.GroupBy("dept")
            .Agg(
                Pl.Col("salary").MapGroups(
                    seriesList =>
                    {
                        var salary = seriesList[0].ToArray<double>();
                        var bonus = seriesList[1].ToArray<double>();
                        double total = 0;
                        for (int i = 0; i < salary.Length; i++)
                        {
                            total += salary[i] + bonus[i];
                        }
                        return Series.From("total_comp", [total]);
                    },
                    DataType.Float64,
                    additionalExprs: [Pl.Col("bonus")]
                ).Alias("total_comp")
            )
            .Sort("dept");

        Assert.Equal(2, res.Height);
        // Eng: 8000+1000 + 12000+2000 = 23000
        Assert.Equal("Eng", res.GetValue<string>(0, "dept"));
        Assert.Equal(23000.0, res.GetValue<double>(0, "total_comp"));
        // Sales: 5000+500 + 7000+1000 = 13500
        Assert.Equal("Sales", res.GetValue<string>(1, "dept"));
        Assert.Equal(13500.0, res.GetValue<double>(1, "total_comp"));
    }

    [Fact]
    [Trait("UDF", "MapGroups")]
    public void Test_GroupByBuilder_MapGroups_DataFrame()
    {
        using var df = DataFrame.FromColumns([
            Series.From("dept", ["A", "A", "A", "B", "B"]),
            Series.From("score", [10, 50, 30, 80, 20])
        ]);

        // Per group top 1 by score
        using var res = df.GroupBy("dept")
            .MapGroups(group => group.Sort("score", descending: true).Head(1))
            .Sort("dept");

        Assert.Equal(2, res.Height);
        Assert.Equal("A", res.GetValue<string>(0, "dept"));
        Assert.Equal(50, res.GetValue<int>(0, "score"));
        Assert.Equal("B", res.GetValue<string>(1, "dept"));
        Assert.Equal(80, res.GetValue<int>(1, "score"));
    }

    [Fact]
    [Trait("UDF", "MapGroups")]
    public void Test_LazyGroupBy_MapGroups_DataFrame()
    {
        using var df = DataFrame.FromColumns([
            Series.From("dept", ["A", "A", "A", "B", "B"]),
            Series.From("score", [10, 50, 30, 80, 20])
        ]);

        // Per group top 1 by score on LazyGroupBy, returning LazyFrame
        using var res = df.Lazy()
            .GroupBy("dept")
            .MapGroups(group => group.Sort("score", descending: true).Head(1))
            .Sort("dept")
            .Collect();

        Assert.Equal(2, res.Height);
        Assert.Equal("A", res.GetValue<string>(0, "dept"));
        Assert.Equal(50, res.GetValue<int>(0, "score"));
        Assert.Equal("B", res.GetValue<string>(1, "dept"));
        Assert.Equal(80, res.GetValue<int>(1, "score"));
    }

    [Fact]
    [Trait("UDF", "MapGroups")]
    public void Test_LazyGroupBy_MapGroups_With_Having()
    {
        using var df = DataFrame.FromColumns([
            Series.From("dept", ["A", "A", "A", "B", "B"]),
            Series.From("score", [10, 50, 30, 80, 20])
        ]);

        // Having filter on total score > 90, so only group B (80+20 = 100 > 90) remains (group A is 10+50+30 = 90 not > 90)
        using var res = df.Lazy()
            .GroupBy("dept")
            .Having(Pl.Col("score").Sum() > 90)
            .MapGroups(group => group.Sort("score", descending: true).Head(1))
            .Collect();

        Assert.Equal(1, res.Height);
        Assert.Equal("B", res.GetValue<string>(0, "dept"));
        Assert.Equal(80, res.GetValue<int>(0, "score"));
    }

    [Fact]
    [Trait("UDF", "MapGroups")]
    public void Test_Pl_MapGroups_StaticHelper()
    {
        using var df = DataFrame.FromColumns([
            Series.From("k", ["X", "X", "Y"]),
            Series.From("v1", [1.0, 2.0, 3.0]),
            Series.From("v2", [10.0, 20.0, 30.0])
        ]);

        using var res = df.GroupBy("k")
            .Agg(
                Pl.MapGroups(
                    seriesList =>
                    {
                        var v1Sum = seriesList[0].Sum<double>() ?? 0.0;
                        var v2Sum = seriesList[1].Sum<double>() ?? 0.0;
                        return Series.From("sum_prod", [v1Sum + v2Sum]);
                    },
                    DataType.Float64,
                    Pl.Col("v1"), Pl.Col("v2")
                ).Alias("sum_prod")
            )
            .Sort("k");

        // X: (1+2) + (10+20) = 33
        // Y: 3 + 30 = 33
        Assert.Equal(33.0, res.GetValue<double>(0, "sum_prod"));
        Assert.Equal(33.0, res.GetValue<double>(1, "sum_prod"));
    }
}
