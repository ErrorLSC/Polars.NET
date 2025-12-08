using Xunit;
using Polars.CSharp;
using Polars.Native;
using Apache.Arrow;
using Apache.Arrow.Types;
using System;
using static Polars.CSharp.Polars; // 方便使用 Col, Lit

namespace Polars.CSharp.Tests;

// 1. 复刻 F# 的 UdfLogic 模块
public static class UdfLogic
{
    // 场景 A: 类型转换 (Int32/64 -> StringView)
    // 完全手动操作 Arrow，不依赖 UdfUtils
    public static IArrowArray IntToString(IArrowArray arr)
    {
        // F# match arr with | :? Int64Array ...
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

    // 场景 B: 必定报错
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
                    // 简单的数值计算
                    double val = (double)i64Arr.GetValue(i).Value;
                    builder.Append(val / 2.0);
                }
            }
            return builder.Build();
        }
        // 为了兼容性处理 Int32
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

// 2. 复刻 F# 的测试类
public class UdfTests
{
    [Fact]
    public void Map_UDF_Memory_Data_Test()
    {
        // 1. 纯内存构造数据 (0..4, 共5行)
        // 使用 FromArrow 构造，绝对干净
        int rowCount = 5;
        var builder = new Int64Array.Builder();
        for (int i = 0; i < rowCount; i++) builder.Append(i * 10); // 0, 10, 20, 30, 40
        var arrowArray = builder.Build();

        using var df = DataFrame.FromArrow(
            new RecordBatch.Builder()
                .Append("num", false, col => col.Int64(arr => arr.AppendRange(Enumerable.Range(0, rowCount).Select(x => (long)x * 10))))
                .Build()
        );

        Assert.Equal(5, df.Height); // 确保源头是 5

        // 2. 转为 Lazy
        // using var lf = df.Lazy();

        // 3. 构造 UDF (Int64 -> Double)
        Func<IArrowArray, IArrowArray> udf = UdfLogic.IntToDouble;

        // 4. 执行
        using var res = df.Select(
            Col("num").Map(udf, DataType.Float64).Alias("res")
        );
        // 5. 验证
        // res.Show(); // 可以打开看看
        Assert.Equal(5, res.Height); // 应该是 5

        using var arrowBatch = res.ToArrow();
        var col = arrowBatch.Column("res") as DoubleArray;
        
        Assert.NotNull(col);
        Assert.Equal(5, col.Length);
        Assert.Equal(0.0, col.GetValue(0)); // 0 / 2
        Assert.Equal(20.0, col.GetValue(4)); // 40 / 2
    }
    [Fact]
    public void Test_UDF_Map_Stable()
    {
        // 1. 准备数据 (5行)
        
        using var csv = new DisposableCsv("num\n15\n25\n" +
                                          "35\n45\n55\n");
        using var df = DataFrame.ReadCsv(csv.Path);
        Assert.Equal(5, df.Height); // 确保数据是 5 行

        // 2. 构造 UDF (Int64 -> Int64)
        // 使用 Lambda，UdfUtils 会处理
        var udf = Col("num").Map<long, long>(x => x * 2, DataType.Int64).Alias("res");

        // 3. 执行
        using var res = df.Select(
            Col("num"),
            udf
        );
        // 看看是不是 3 行

        // 4. 验证
        Assert.Equal(5, res.Height); // 关键断言：行数不能丢

        using var batch = res.ToArrow();
        var col = batch.Column("res");
        
        Assert.Equal(30, col.GetInt64Value(0));
        Assert.Equal(50, col.GetInt64Value(1));
        Assert.Equal(70, col.GetInt64Value(2));
    }
    [Fact]
    public void Map_UDF_Can_Change_Data_Type_Int_To_String()
    {
        // 1. 准备数据
        // F#: use csv = new TempCsv "num\n100\n200"
        using var csv = new DisposableCsv("num\n100\n200\n");
        using var lf = LazyFrame.ScanCsv(csv.Path);

        // 2. 构造 C# 委托
        // F#: let udf = Func<IArrowArray, IArrowArray>(UdfLogic.intToString)
        Func<IArrowArray, IArrowArray> udf = UdfLogic.IntToString;

        // 3. 执行 Polars 查询
        using var df = lf.Select(
            Col("num")
            .Map(udf, DataType.String) 
            .Alias("desc")
        ).Collect();
        df.Show();
        // 4. 验证结果
        using var arrowBatch = df.ToArrow();
        // F# let strCol = arrowBatch.Column "desc" :?> StringViewArray
        // 我们用 helper 或者直接 cast
        var strCol = arrowBatch.Column("desc") as StringViewArray;
        
        Assert.NotNull(strCol);
        Assert.Equal("Value: 100", strCol.GetStringValue(0)); // 使用之前修好的 GetStringValue
        Assert.Equal("Value: 200", strCol.GetStringValue(1));
    }

    [Fact]
    public void Map_UDF_Error_Is_Propagated_To_CSharp()
    {
        // 1. 准备数据
        using var csv = new DisposableCsv("num\n1");
        using var lf = LazyFrame.ScanCsv(csv.Path);

        Func<IArrowArray, IArrowArray> udf = UdfLogic.AlwaysFail;

        // 2. 断言会抛出异常
        var ex = Assert.Throws<Exception>(() => 
        {
            lf.Select(
                Col("num").Map(udf, DataType.SameAsInput)
            ).Collect();
        });

        // 3. 验证异常信息
        Assert.Contains("Boom! C# UDF Exploded!", ex.Message);
        // Assert.Contains("C# UDF Failed", ex.Message); // 取决于 Native 层是否加了前缀
    }
    [Fact]
    public void Test_UDF_HighLevel_Numeric()
    {
        // 场景 1: 简单的数值计算 (Int64 -> Int64)
        // 用户不需要知道 IArrowArray，只需要写 Lambda
        
        using var csv = new DisposableCsv("num\n10\n20\n30\n");
        using var df = DataFrame.ReadCsv(csv.Path);

        // 定义逻辑: x * 2
        var doubleExpr = Col("num")
            .Map<long, long>(x => x * 2, DataType.Int64)
            .Alias("doubled");

        using var res = df.Select(Col("num"), doubleExpr);
        
        // 验证
        using var batch = res.ToArrow();
        var col = batch.Column("doubled");
        Assert.Equal(20, col.GetInt64Value(0)); // 10 * 2
        Assert.Equal(60, col.GetInt64Value(2)); // 30 * 2
    }

    [Fact]
    public void Test_UDF_HighLevel_String_Manipulation()
    {
        // 场景 2: 字符串处理 (String -> String)
        // 模拟业务逻辑：给名字加前缀
        
        using var csv = new DisposableCsv("name\nAlice\nBob\n");
        using var df = DataFrame.ReadCsv(csv.Path);

        // 定义逻辑: "Hello, {name}!"
        var greetExpr = Col("name")
            .Map<string, string>(name => $"Hello, {name}!", DataType.String)
            .Alias("greeting");

        using var res = df.Select(Col("name"), greetExpr);
        
        // 验证
        using var batch = res.ToArrow();
        var col = batch.Column("greeting"); // 这里的 Column 会自动处理 LargeString/StringView
        
        Assert.Equal("Hello, Alice!", col.GetStringValue(0));
        Assert.Equal("Hello, Bob!", col.GetStringValue(1));
    }

    [Fact]
    public void Test_UDF_HighLevel_Type_Conversion()
    {
        // 场景 3: 跨类型转换 (Int64 -> String)
        // 这是最常用的场景之一，比如格式化数字
        
        using var csv = new DisposableCsv("id\n1001\n1002\n");
        using var df = DataFrame.ReadCsv(csv.Path);

        // 定义逻辑: 将 ID 格式化为 "Order-#ID"
        // 输入是 long, 输出是 string
        var formatExpr = Col("id")
            .Map<long, string>(id => $"Order-{id}", DataType.String)
            .Alias("order_id");

        using var res = df.Select(Col("id"), formatExpr);
        
        // 验证
        using var batch = res.ToArrow();
        var col = batch.Column("order_id");
        
        Assert.Equal("Order-1001", col.GetStringValue(0));
        Assert.Equal("Order-1002", col.GetStringValue(1));
    }
    [Fact]
    public void Test_UDF_Nullable_Output()
    {
        // 构造数据: 10, 0, 20
        // 我们想把 0 变成 Null
        using var csv = new DisposableCsv("num\n10\n0\n20\n");
        using var df = DataFrame.ReadCsv(csv.Path);

        // 逻辑: 如果是 0 返回 null (C# null)，否则返回原值
        // 关键点：泛型参数是 <long, long?>
        var cleanExpr = Col("num")
            .Map<long, long?>(x => x == 0 ? null : x, DataType.Int64)
            .Alias("cleaned");

        using var res = df.Select(Col("num"), cleanExpr);
        
        // 验证
        using var batch = res.ToArrow();
        var col = batch.Column("cleaned");
        
        Assert.Equal(10, col.GetInt64Value(0));
        Assert.True(col.IsNull(1)); // 0 变成了 Null
        Assert.Equal(20, col.GetInt64Value(2));
    }
    [Fact]
    public void Test_UDF_Nullable_Input()
    {
        // 数据: 10, null
        using var csv = new DisposableCsv("num\n10\n\n"); // 第二行是空
        using var df = DataFrame.ReadCsv(csv.Path);

        // 逻辑: 输入 int? -> 输出 string
        // 如果输入是 null，返回 "FoundNull"，否则返回 "Value:{x}"
        var checkNullExpr = Col("num")
            .Map<long?, string>(x => x.HasValue ? $"Value:{x}" : "FoundNull", DataType.String)
            .Alias("status");

        using var res = df.Select(Col("num"), checkNullExpr);
        
        using var batch = res.ToArrow();
        var col = batch.Column("status");
        
        Assert.Equal("Value:10", col.GetStringValue(0));
        Assert.Equal("FoundNull", col.GetStringValue(1)); // 成功捕获了 Null 输入！
    }
}
