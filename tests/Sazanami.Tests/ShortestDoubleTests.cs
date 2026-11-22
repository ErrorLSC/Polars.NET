using System.Globalization;
using Kaminari.Sazanami.Text;

namespace Sazanami.Tests;

/// <summary>Ryū's shortest digits of doubles: the digits .NET's shortest round-trip formatting writes, and they read back.</summary>
public class ShortestDoubleTests
{
    // .NET's shortest digits of a positive double, as (digits, exponent of the last digit).
    private static (ulong Digits, int Exponent) Net(double value)
    {
        string r = value.ToString("R", CultureInfo.InvariantCulture);
        int e = r.IndexOf('E');
        string mantissa = e < 0 ? r : r[..e];
        int exponent = e < 0 ? 0 : int.Parse(r[(e + 1)..], CultureInfo.InvariantCulture);
        int point = mantissa.IndexOf('.');
        string digits = mantissa.Replace(".", "");
        if (point >= 0) exponent -= mantissa.Length - point - 1;
        digits = digits.TrimStart('0');
        while (digits.Length > 1 && digits.EndsWith('0'))
        {
            digits = digits[..^1];
            exponent++;
        }
        return (ulong.Parse(digits, CultureInfo.InvariantCulture), exponent);
    }

    private static void Check(double value)
    {
        ShortestDouble.Digits(value, out ulong digits, out int exponent);
        while (digits % 10 == 0)
        {
            digits /= 10;
            exponent++;
        }
        Assert.True(Net(value) == (digits, exponent), $"{value:R}: ryu {digits}e{exponent}, .NET {Net(value)}");
        Assert.Equal(value, double.Parse($"{digits}e{exponent}", CultureInfo.InvariantCulture));
    }

    [Fact]
    public void RandomBitPatterns_HaveDotNetsShortestDigits()
    {
        var random = new Random(5);
        for (int i = 0; i < 300_000; i++)
        {
            double value = BitConverter.Int64BitsToDouble(random.NextInt64() & long.MaxValue);
            if (double.IsFinite(value) && value != 0) Check(value);
        }
    }

    [Fact]
    public void EdgesAndCommonValues_HaveDotNetsShortestDigits()
    {
        double[] values = [double.Epsilon, double.MaxValue, 2.2250738585072014E-308, 2.225073858507201E-308, 1, 0.1, 0.3, 0.1 + 0.2,
            1e15, 1e16, 1e21, 1e22, 1e23, 9007199254740993, 5e-324, 1.7976931348623157e308, 123456789.125, 1.5e-6, 0.00001];
        foreach (double v in values) Check(v);
        for (int i = 1; i < 200_000; i++)
        {
            Check(i * 0.25);
            Check(i / 1000.0);
            Check(i * 1e-7);
            Check(Math.Pow(10, i % 600 - 300));
        }
    }
}
