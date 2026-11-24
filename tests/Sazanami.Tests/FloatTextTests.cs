using System.Globalization;
using System.Text;
using Kaminari.Sazanami.Text;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="FloatText.Shortest{T}"/> of doubles: .NET's shortest round-trip digits laid out as the reference engine writes
/// them (decimal notation for exponents −5 to 15, a whole number ending ".0", else d.ddde±x).
/// </summary>
public class FloatTextTests
{
    // The layout built from .NET's own shortest digits.
    private static string Expected(double value)
    {
        if (value == 0) return double.IsNegative(value) ? "-0.0" : "0.0";
        string shortest = double.Abs(value).ToString("R", CultureInfo.InvariantCulture);
        // Digits and exponent of the first digit from "R" ("1.5E-06", "123", "0.0001").
        int e = shortest.IndexOf('E');
        string mantissa = e < 0 ? shortest : shortest[..e];
        int point = mantissa.IndexOf('.');
        string digits = mantissa.Replace(".", "");
        int lead = 0;
        while (lead < digits.Length - 1 && digits[lead] == '0') lead++;
        int exponent = (point < 0 ? mantissa.Length : point) - 1 - lead + (e < 0 ? 0 : int.Parse(shortest[(e + 1)..], CultureInfo.InvariantCulture));
        digits = digits[lead..].TrimEnd('0');
        if (digits.Length == 0) digits = "0";
        var text = new StringBuilder(double.IsNegative(value) ? "-" : "");
        if (exponent is < -5 or > 15)
        {
            text.Append(digits[0]);
            if (digits.Length > 1) text.Append('.').Append(digits[1..]);
            text.Append('e').Append(exponent < 0 ? '-' : '+').Append(Math.Abs(exponent));
        }
        else if (exponent >= digits.Length - 1) text.Append(digits).Append('0', exponent - digits.Length + 1).Append(".0");
        else if (exponent >= 0) text.Append(digits[..(exponent + 1)]).Append('.').Append(digits[(exponent + 1)..]);
        else text.Append("0.").Append('0', -exponent - 1).Append(digits);
        return text.ToString();
    }

    private static void Check(double value)
    {
        Span<byte> room = stackalloc byte[FloatText.MaxShortestBytes];
        int length = FloatText.Shortest(value, room);
        string got = Encoding.ASCII.GetString(room[..length]);
        Assert.True(Expected(value) == got, $"{value:R}: wrote {got}, expected {Expected(value)}");
    }

    [Fact]
    public void RandomDoubles_AreLaidOutFromTheirShortestDigits()
    {
        var random = new Random(11);
        for (int i = 0; i < 300_000; i++)
        {
            double value = BitConverter.Int64BitsToDouble(random.NextInt64());
            if (double.IsFinite(value)) Check(value);
            // Values whose exponents fall inside and around the decimal range.
            Check((random.NextDouble() - 0.5) * Math.Pow(10, random.Next(-8, 19)));
            Check(Math.Round(random.NextDouble() * Math.Pow(10, random.Next(0, 17)), random.Next(0, 6)));
        }
    }

    [Fact]
    public void WholeNumbersAndEdges_AreLaidOutFromTheirShortestDigits()
    {
        double[] values = [0.0, -0.0, 1, -1, 10, 1e15, 1e16, 9.999999999999998e15, 123456789012345.6, 1e-5, 1e-6, 9.99999e-6, 0.1, 0.3,
            double.Epsilon, double.MaxValue, -double.MaxValue, 5e-324, 1e21, 1e22, 1e23, 9007199254740993, 4503599627370495.5, 1154907.0];
        foreach (double v in values) Check(v);
        for (long k = 0; k < 100_000; k++)
        {
            Check(k);
            Check(-k * 1000.0);
            Check((1L << 53) - k);
            Check(k / 100.0);
            Check(k * 1e-9);
        }
        for (int e = -330; e <= 310; e++)
        {
            foreach (double v in (double[])[Math.Pow(10, e), -3 * Math.Pow(10, e)])
                if (double.IsFinite(v)) Check(v);
        }
    }
}
