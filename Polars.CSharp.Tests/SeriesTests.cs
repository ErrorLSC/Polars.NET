using Xunit;
using Polars.CSharp;
using Apache.Arrow;

namespace Polars.CSharp.Tests;

public class SeriesTests
{
    [Fact]
    public void Test_Series_Creation_And_Arrow()
    {
        // 1. 创建 Series (Int32)
        using var s = new Series("my_series", [1, 2, 3]);
        
        Assert.Equal(3, s.Length);
        Assert.Equal("my_series", s.Name);

        // 2. 转 Arrow
        var arrowArray = s.ToArrow();
        Assert.IsType<Int32Array>(arrowArray);
        Assert.Equal(2, ((Int32Array)arrowArray).GetValue(1));

        // 3. Rename
        s.Name = "renamed";
        Assert.Equal("renamed", s.Name);
    }

    [Fact]
    public void Test_Series_String_And_Nulls()
    {
        // 1. 创建 String Series (带 Null)
        using var s = new Series("strings", ["a", null, "c"]);
        
        Assert.Equal(3, s.Length);
        
        // Polars 0.50 默认可能是 StringViewArray 或 LargeStringArray
        // 我们用之前的扩展方法来验证
        Assert.Equal("a", s.GetValue<string>(0));
        Assert.Null(s.GetValue<string>(1));
        Assert.Equal("c", s.GetValue<string>(2));
    }

    [Fact]
    public void Test_Series_Cast_Decimal()
    {
        // 1. 创建 Double Series
        using var s = new Series("prices", [10.5, 20.0]);

        // 2. Cast 到 Decimal(10, 2)
        // 这需要 DataType 类发挥作用
        using var sDecimal = s.Cast(DataType.Decimal(10, 2));
        
        // 验证 Cast 后的 Arrow 类型
        var arrowArray = sDecimal.ToArrow();
        // Apache Arrow C# 会把 Decimal128 映射为 Decimal128Array
        Assert.IsType<Decimal128Array>(arrowArray);
    }
}