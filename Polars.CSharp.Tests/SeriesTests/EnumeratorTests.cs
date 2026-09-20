using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class SeriesEnumeratorTests
{
    [Fact]
    [Trait("Series", "Enumerator")]
    public void Test_Enumerator_Int32_Primitives()
    {
        int[] expected = [114514, 1919810, 23545615, 7546];
        using Series s = Pl.CreateSeries("test", expected);

        var actual = new List<int>();
        foreach (int e in s.As<int>())
        {
            actual.Add(e);
        }

        Assert.Equal(expected, actual);
    }

    [Fact]
    [Trait("Series", "Enumerator")]
    public void Test_Enumerator_Int64_WithNulls()
    {
        long?[] expected = [100L, null, 300L, null, 500L];
        using Series s = Pl.CreateSeries("long_nullable", expected);

        var actual = new List<long?>();
        foreach (long? e in s.As<long?>())
        {
            actual.Add(e);
        }

        Assert.Equal(expected, actual);
    }

    [Fact]
    [Trait("Series", "Enumerator")]
    public void Test_Enumerator_Float64_Accumulation()
    {
        double[] values = [1.5, 2.5, 3.0, 4.0];
        using Series s = Pl.CreateSeries("doubles", values);

        double sum = 0.0;
        foreach (double v in s.As<double>())
        {
            sum += v;
        }

        Assert.Equal(11.0, sum);
    }

    [Fact]
    [Trait("Series", "Enumerator")]
    public void Test_Enumerator_Guid_FixedBinary()
    {
        Guid g1 = Guid.NewGuid();
        Guid g2 = Guid.Empty;
        Guid g3 = Guid.NewGuid();

        Guid?[] expected = [g1, null, g2, g3];
        using Series s = Pl.CreateSeries("guids", expected);

        var actual = new List<Guid?>();
        foreach (Guid? e in s.As<Guid?>())
        {
            actual.Add(e);
        }

        Assert.Equal(expected, actual);
    }

    [Fact]
    [Trait("Series", "Enumerator")]
    public void Test_Enumerator_Strings_WithNulls()
    {
        string[] expected = ["polars", null, "dotnet", "fast_zero_copy", null];
        using Series s = Pl.CreateSeries("strings", expected);

        var actual = new List<string>();
        foreach (string e in s.As<string>())
        {
            actual.Add(e);
        }

        Assert.Equal(expected, actual);
    }

    [Fact]
    [Trait("Series", "Enumerator")]
    public void Test_Enumerator_Temporal_DateOnly()
    {
        DateOnly d1 = new(2026, 1, 1);
        DateOnly d2 = new(2026, 9, 20);
        DateOnly?[] expected = [d1, null, d2];

        using Series s = Pl.CreateSeries("dates", expected);

        var actual = new List<DateOnly?>();
        foreach (DateOnly? e in s.As<DateOnly?>())
        {
            actual.Add(e);
        }

        Assert.Equal(expected, actual);
    }

    [Fact]
    [Trait("Series", "Enumerator")]
    public void Test_Enumerator_Empty_Series()
    {
        using Series s = Pl.CreateSeries("empty", Array.Empty<int>());

        int count = 0;
        foreach (int _ in s.As<int>())
        {
            count++;
        }

        Assert.Equal(0, count);
    }

    [Fact]
    [Trait("Series", "Enumerator")]
    public void Test_Enumerator_Default_Object_Iteration()
    {
        // Tests duck-typed GetEnumerator() on Series instance (foreach (var item in series))
        int[] original = [1, 2, 3];
        using Series s = Pl.CreateSeries("obj_test", original);

        var actual = new List<object>();
        foreach (object item in s)
        {
            actual.Add(item);
        }

        Assert.Equal(3, actual.Count);
        Assert.Equal(1, Convert.ToInt32(actual[0]));
        Assert.Equal(2, Convert.ToInt32(actual[1]));
        Assert.Equal(3, Convert.ToInt32(actual[2]));
    }
}
