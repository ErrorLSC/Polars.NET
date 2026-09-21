using Pl = Polars.CSharp.Polars;
using Polars.NET.Core;
using System.Globalization;

namespace Polars.CSharp.Tests;

public class SeriesOperatorTests
{
    [Fact]
    [Trait("Series", "Add")]
    public void Test_Series_Add()
    {
        using Series s1 = Pl.CreateSeries("s1", [1, 2, 3]);
        using Series s2 = Pl.CreateSeries("s2", [4, 5, 6]);
        using Series result = s1 + s2;
        Assert.Equal([5, 7, 9], result.ToArray<int>());
    }
    [Fact]
    [Trait("Series", "Add")]
    public void Test_Series_Add_String()
    {
        using Series s1 = Pl.CreateSeries("s1", ["a", "b", "c"]);
        using Series s2 = Pl.CreateSeries("s2", ["d", "e", "f"]);
        using Series result = s1 + s2;
        Assert.Equal(["ad", "be", "cf"], result.ToArray<string>());
    }
    [Fact]
    [Trait("Series", "Add")]
    public void Test_Series_Add_Decimal()
    {
        using Series s1 = Pl.CreateSeries("s1", [1.5m, 2.5m, 3.5m]);
        using Series s2 = Pl.CreateSeries("s2", [2.5000m, 3.5000m, 4.5000m]);
        using Series result = s1 + s2;
        Assert.Equal([4.0000m, 6.0000m, 8.0000m], result.ToArray<decimal>());
    }
    [Fact]
    [Trait("Series", "Add")]
    public void Test_Series_Add_Boolean()
    {
        using Series s1 = Pl.CreateSeries("s1", [false, false, true]);
        using Series s2 = Pl.CreateSeries("s2", [false, true, false]);
        using Series result = s1 + s2;
        Assert.Equal([0u,1u,1u], result.ToArray<uint>());
    }
    [Fact]
    [Trait("Series", "Add")]
    public void Test_Series_Add_TimeSpan()
    {
        using Series s1 = Pl.CreateSeries("s1", [new DateTime(2021, 1, 1), new DateTime(2021, 2, 1), new DateTime(2021, 3, 1)]);
        using Series s2 = Pl.CreateSeries("s2", [new TimeSpan(3, 0, 0), new TimeSpan(3, 0, 0), new TimeSpan(3, 0, 0)]);
        using Series result1 = s1 + s2;
        Assert.Equal([new DateTime(2021, 1, 1, 3, 0, 0), new DateTime(2021, 2, 1, 3, 0, 0), new DateTime(2021, 3, 1, 3, 0, 0)], result1.ToArray<DateTime>());

        using Series s3 = Pl.CreateSeries("s3", [new DateOnly(2021, 1, 1), new DateOnly(2021, 2, 1), new DateOnly(2021, 3, 1)]);
        using Series s4 = Pl.CreateSeries("s4", [TimeSpan.FromDays(3), TimeSpan.FromDays(3), TimeSpan.FromDays(3)]);
        using Series result2 = s3 + s4;
        Assert.Equal([new DateOnly(2021, 1, 4), new DateOnly(2021, 2, 4), new DateOnly(2021, 3, 4)], result2.ToArray<DateOnly>());
    }
    [Fact]
    [Trait("Series", "Sub")]
    public void Test_Series_Sub_String_Throws_Exception()
    {
        using Series s1 = Pl.CreateSeries("s1", ["a", "b", "c"]);
        using Series s2 = Pl.CreateSeries("s2", ["d", "e", "f"]);

        Assert.Throws<PolarsException>(() => s1 - s2);
    }
    [Fact]
    [Trait("Series", "Sub")]
    public void Test_Series_Sub_TimeSpan()
    {
        using Series s1 = Pl.CreateSeries("s1", [new DateTime(2021, 1, 1), new DateTime(2021, 2, 1), new DateTime(2021, 3, 1)]);
        using Series s2 = Pl.CreateSeries("s2", [new TimeSpan(3, 0, 0), new TimeSpan(3, 0, 0), new TimeSpan(3, 0, 0)]);
        using Series result1 = s1 - s2;
        Assert.Equal([new DateTime(2020, 12, 31, 21, 0, 0), new DateTime(2021, 1, 31, 21, 0, 0), new DateTime(2021, 2, 28, 21, 0, 0)], result1.ToArray<DateTime>());

        using Series s3 = Pl.CreateSeries("s3", [new DateOnly(2021, 1, 1), new DateOnly(2021, 2, 1), new DateOnly(2021, 3, 1)]);
        using Series s4 = Pl.CreateSeries("s4", [TimeSpan.FromDays(3), TimeSpan.FromDays(3), TimeSpan.FromDays(3)]);
        using Series result2 = s3 - s4;
        Assert.Equal([new DateOnly(2020, 12, 29), new DateOnly(2021, 1, 29), new DateOnly(2021, 2, 26)], result2.ToArray<DateOnly>());
    }
    [Fact]
    [Trait("Series", "Sub")]
    public void Test_Series_Sub_Decimal()
    {
        using Series s1 = Pl.CreateSeries("s1", [1.5m, 2.5m, 3.5m]);
        using Series s2 = Pl.CreateSeries("s2", [2.5000m, 3.5000m, 4.5000m]);
        using Series result = s1 - s2;
        Assert.Equal([-1.0000m, -1.0000m, -1.0000m], result.ToArray<decimal>());
    }
    [Fact]
    [Trait("Series", "Mul")]
    public void Test_Series_Mul_Decimal()
    {
        using Series s1 = Pl.CreateSeries("s1", [1.5m, 2.5m, 3.5m]);
        using Series s2 = Pl.CreateSeries("s2", [2.5000m, 3.5000m, 4.5000m]);
        using Series result = s1 * s2;
        Assert.Equal([3.7500m, 8.7500m, 15.7500m], result.ToArray<decimal>());
    }
    [Fact]
    [Trait("Series", "Div")]
    public void Test_Series_Div_Decimal()
    {
        using Series s1 = Pl.CreateSeries("s1", [1.5m, 2.5m, 3.5m]);
        using Series s2 = Pl.CreateSeries("s2", [2.5000m, 3.5000m, 4.5000m]);
        using Series result = s1 / s2;
        Assert.Equal([0.6000m, 0.7143m, 0.7778m], result.ToArray<decimal>());
    }
    [Fact]
    [Trait("Series", "Gt")]
    public void Test_Series_Gt()
    {
        using Series s1 = Pl.CreateSeries("s1", [1, 2, 3]);
        using Series s2 = Pl.CreateSeries("s2", [2, 3, 4]);
        using Series result = s1 > s2;
        Assert.Equal([false, false, false], result.ToArray<bool>());
    }
    [Fact]
    [Trait("Series", "Gt")]
    public void Test_Series_Gt_Temporal()
    {
        using Series s1 = Pl.CreateSeries("s1", [new DateTime(2021, 1, 1), new DateTime(2021, 2, 1), new DateTime(2021, 3, 1)]);
        using Series s2 = Pl.CreateSeries("s2", [new DateTime(2018, 1, 1), new DateTime(2033, 2, 1), new DateTime(2077, 3, 1)]);
        using Series result1 = s1 > s2;
        Assert.Equal([true, false, false], result1.ToArray<bool>());

        using Series s3 = Pl.CreateSeries("s3", [new DateTime(2018, 1, 1), new DateTime(2033, 2, 1), new DateTime(2077, 3, 1)]);
        using Series s4 = Pl.CreateSeries("s4", [new DateOnly(2021, 1, 1), new DateOnly(2021, 2, 1), new DateOnly(2021, 3, 1)]);
        using Series result2 = s3 > s4;
        Assert.Equal([false, true, true], result2.ToArray<bool>());

        using Series s5 = Pl.CreateSeries("s5", [new TimeOnly(5, 12, 3), new TimeOnly(10, 2, 1), new TimeOnly(15, 3, 1)]);
        using Series s6 = Pl.CreateSeries("s6", [new TimeOnly(1, 1, 1), new TimeOnly(12, 2, 2), new TimeOnly(3, 3, 3)]);
        using Series result3 = s5 > s6;
        Assert.Equal([true, false, true], result3.ToArray<bool>());

        using Series s7 = Pl.CreateSeries("s7", [TimeSpan.FromDays(5), TimeSpan.FromDays(10), TimeSpan.FromDays(15)]);
        using Series s8 = Pl.CreateSeries("s8", [TimeSpan.FromDays(1), TimeSpan.FromDays(12), TimeSpan.FromDays(3)]);
        using Series result4 = s7 > s8;
        Assert.Equal([true, false, true], result4.ToArray<bool>());
    }
    [Fact]
    [Trait("Series", "Gt")]
    public void Test_Series_Gt_Decimal()
    {
        using Series s1 = Pl.CreateSeries("s1", [1.5m, 2.5m, 8.5m]);
        using Series s2 = Pl.CreateSeries("s2", [2.5000m, 3.5000m, 4.5000m]);
        using Series result = s1 > s2;
        Assert.Equal([false, false, true], result.ToArray<bool>());
    }
    [Fact]
    [Trait("Series", "Gt")]
    public void Test_Series_Gt_String()
    {
        using Series s1 = Pl.CreateSeries("s1", ["a", "b", "c"]);
        using Series s2 = Pl.CreateSeries("s2", ["bob", "ath", "114514"]);
        using Series result = s1 > s2;
        Assert.Equal([false, true, true], result.ToArray<bool>());
    }
    [Fact]
    [Trait("Series", "Not")]
    public void Test_Series_Not()
    {
        using Series s1 = Pl.CreateSeries("s1", [true, false, true]);
        using Series result = !s1;
        using Series result2 = ~s1;
        Assert.Equal([false, true, false], result.ToArray<bool>());
        Assert.Equal(result, result2);
    }
    [Fact]
    [Trait("Series", "EqMissing")]
    public void Test_Series_EqMissing()
    {
        using Series s1 = Pl.CreateSeries("s1", ["a", null, null]);
        using Series s2 = Pl.CreateSeries("s2", ["bob", "ath", null]);
        using Series result1 = s1.EqMissing(s2);
        using Series result2 = s1 == s2;
        Assert.Equal([false, false, true], result1.ToArray<bool>());
        Assert.Equal([false, false, false], result2.ToArray<bool>());
    }
    [Fact]
    [Trait("Series", "NeqMissing")]
    public void Test_Series_NeqMissing()
    {
        using Series s1 = Pl.CreateSeries("s1", ["a", null, null]);
        using Series s2 = Pl.CreateSeries("s2", ["bob", "ath", null]);
        using Series result1 = s1.NeqMissing(s2);
        using Series result2 = s1 != s2;
        Assert.Equal([true, true, false], result1.ToArray<bool>());
        Assert.Equal([true, false, false], result2.ToArray<bool>());
    }
    [Fact]
    [Trait("Series", "Pow")]
    public void Test_Series_Pow()
    {
        using Series s1 = Pl.CreateSeries("s1", [2, 3, 4]);
        using Series result1 = s1.Pow(2);
        Assert.Equal([4, 9, 16], result1.ToArray<int>());

        using Series s2 = Pl.CreateSeries("s2", [2, 3, 4]);
        using Series result2 = s2.Pow(2.0);
        Assert.Equal([4.0, 9.0, 16.0], result2.ToArray<double>());

        using Series s3 = Pl.CreateSeries("s3", [2.0f, 3.0f, 4.0f]);
        using Series result3 = s3.Pow(2.0f);
        Assert.Equal([4.0f, 9.0f, 16.0f], result3.ToArray<float>());
    }
    [Fact]
    [Trait("Series", "Pow")]
    public void Test_Series_Pow_Series()
    {
        using Series s1 = Pl.CreateSeries("s1", [2, 3, 4]);
        using Series s2 = Pl.CreateSeries("s2", [1, 2, 3]);
        using Series result1 = s1.Pow(s2);
        Assert.Equal([2, 9, 64], result1.ToArray<int>());
    }
    [Fact]
    [Trait("Series", "Pow")]
    public void Test_Series_Pow_Series_TypePromotion()
    {
        using Series s1 = Pl.CreateSeries("s1", [2, 3, 4]);
        using Series s2 = Pl.CreateSeries("s2", [1.0, 2.0, 3.0]);
        using Series result1 = s1.Pow(s2);
        Assert.Equal([2.0, 9.0, 64.0], result1.ToArray<double>());
    }
}
