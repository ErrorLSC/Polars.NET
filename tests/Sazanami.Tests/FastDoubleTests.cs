using System.Globalization;
using System.Numerics;
using System.Text;
using Kaminari.Sazanami.Text;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="FastDouble"/>: bit for bit the runtime's correctly rounded <c>double.Parse</c> / <c>float.Parse</c> on hard
/// cases and random numbers of every shape, and the grammar of Polars' float columns (fast-float's).
/// </summary>
public class FastDoubleTests
{
    private static readonly string[] HardCases =
    [
        "0", "-0", "0.0", "-0.0", "0e999999999", "-0e-999999999", "1", "-1", "0.1", "0.2", "0.3", "1.5", "2.5", "3.5",
        "9007199254740992", "9007199254740993", "9007199254740994", "9007199254740995", "18014398509481985",
        "1e22", "1e23", "1e-22", "1e-23", "8.98846567431158e307", "1.7976931348623157e308", "1.7976931348623158e308",
        "1.7976931348623158079e308", "1.797693134862315807937289714053e308", "1.7976931348623159e308", "1e308", "1e309",
        "2.2250738585072011e-308", "2.2250738585072012e-308", "2.2250738585072013e-308", "2.2250738585072014e-308",
        "2.225073858507201136057409796709131975934819546351645648e-308", "4.9406564584124654e-324", "4.9e-324", "5e-324",
        "2.4703282292062327e-324", "2.4703282292062328e-324", "2.47032822920623272e-324", "2.4703282292062327209e-324",
        "7.4109846876186982e-324", "1e-320", "1e-400", "1e+400", "-1e400", "1e-99999999999", "1e99999999999",
        "0.000000000000000000000000000000000000000000001", "123456789012345678901234567890", "1.00000000000000011102230246251565404236316680908203125",
        "1.00000000000000011102230246251565404236316680908203124", "1.00000000000000011102230246251565404236316680908203126",
        "0.1000000000000000055511151231257827021181583404541015625", "0.1000000000000000055511151231257827",
        "1234567890123456789", "12345678901234567890", "18446744073709551615", "18446744073709551616", "99999999999999999999",
        "9999999999999999999", "10000000000000000000", "1000000000000000000000000000000000000000000000000000000000000",
        "3.4028234663852886e38", "3.4028235677973366e38", "3.402823466385288598117041834845169254400e38", "1.1754943508222875e-38",
        "1.401298464324817e-45", "7.006492321624085e-46", "7.006492321624086e-46", "1.00000005960464477539062500001",
        "1.000000059604644775390625", "1.00000005960464477539062499999", "16777216", "16777217", "16777219", "33554435",
        "1e10", "1e11", "1e38", "1e39", "1e-45", "1e-46", "3e-46", "0.7006492321624085e-45",
        "+1.5", "-.5", "5.", "1.e5", "00.5", "007", "1E5", "1.5e+3", "1.5E-3", "1e0001", "0.0000000000000000000000000000000001e40",
        "4.35", "4.35e10", "92233720368547758079223372036854775807", "2.718281828459045235360287471352662497757",
    ];

    [Fact]
    public void HardCases_AreTheRuntimesBitForBit()
    {
        foreach (string text in HardCases.Concat(LongDigits()))
        {
            AssertDouble(text);
            AssertSingle(text);
        }

        // Hundreds of digits, the dropped ones deciding (or not) the rounding.
        static IEnumerable<string> LongDigits()
        {
            yield return "2.4703282292062327208828439643411068618252990130716238221279284125033775363510437593264991818081799618989828234772285886546332835517796989819938739800539093906315035659515570226392290858392449105184435931802849936536152500319370457678249219365623669863658480757001585769269903706311928279558551332927834338409351978015531246597263579574622766465272827220056374006485499977096599470454020828166226237857393450736339007967761930577506740176324673600968951340535537458516661134223766678604162159680461914467291840300530057530849048765391711386591646239524912623653881879636239373280423891018672348497668235089863388587925628302755995657524455507255189313690836254779186948667994968324049705821028513185451396213837722826145437693412532098591327667236328125e-324";
            yield return "2.4703282292062327208828439643411068618252990130716238221279284125033775363510437593264991818081799618989828234772285886546332835517796989819938739800539093906315035659515570226392290858392449105184435931802849936536152500319370457678249219365623669863658480757001585769269903706311928279558551332927834338409351978015531246597263579574622766465272827220056374006485499977096599470454020828166226237857393450736339007967761930577506740176324673600968951340535537458516661134223766678604162159680461914467291840300530057530849048765391711386591646239524912623653881879636239373280423891018672348497668235089863388587925628302755995657524455507255189313690836254779186948667994968324049705821028513185451396213837722826145437693412532098591327667236328126e-324";
            yield return "1." + new string('0', 800) + "1";
            yield return "0." + new string('0', 340) + "247032822920623272088284396434110686182529901307162382212792841250337753635104375932649918180817996189898282347722858865463328355177969898199387398005390939063150356595155702263922908583924491051844359318028499365361525003193704576782492193656236698636584807570015857692699037063119282795585513329278343384093519780155312465972635795746227664652728272200563740064854999770965994704540208281662262378573934507363390079677619305775067401763246736009689513405355374585166611342237666786041621596804619144672918403005300575308490487653917113865916462395249126236538818796362393732804238910186723484976682350898633885879256283027559956575244555072551893136908362547791869486679949683240497058210285131854513962138377228261454376934125320985913276672363281255";
            yield return new string('9', 400);
            yield return "1" + new string('0', 400) + "e-400";
            yield return "0." + new string('0', 500) + "1e500";
        }
    }

    /// <summary>The 128-bit approximations of 5^q are fast_float's (table_generation.py), recomputed exactly.</summary>
    [Fact]
    public void PowersTable_MatchesBigInteger()
    {
        ReadOnlySpan<ulong> powers = FastDouble.Powers;
        Assert.Equal(2 * (308 + 342 + 1), powers.Length);
        BigInteger two128 = BigInteger.One << 128;
        for (int q = -342; q <= 308; q++)
        {
            BigInteger c;
            if (q < 0)
            {
                BigInteger p = BigInteger.Pow(5, -q);
                long z = (long)p.GetBitLength();
                c = BigInteger.Pow(2, (int)(q >= -27 ? z + 127 : 2 * z + 128)) / p + 1;
                while (c >= two128) c /= 2;
            }
            else
            {
                c = BigInteger.Pow(5, q);
                while (c < BigInteger.One << 127) c *= 2;
                while (c >= two128) c /= 2;
            }
            int index = 2 * (q + 342);
            Assert.True((ulong)(c >> 64) == powers[index] && (ulong)(c & ulong.MaxValue) == powers[index + 1], $"5^{q}");
        }
    }

    /// <summary>Polars' typed float columns (fast-float2's grammar), probed on the native reader: accepted with the value, or refused.</summary>
    [Theory]
    [InlineData("1.5", 1.5)]
    [InlineData("+1.5", 1.5)]
    [InlineData("-.5", -0.5)]
    [InlineData("5.", 5.0)]
    [InlineData("1.e5", 1e5)]
    [InlineData("00.5", 0.5)]
    [InlineData("007", 7.0)]
    [InlineData("1E5", 1e5)]
    [InlineData("1.5e+3", 1500.0)]
    [InlineData("1.5E-3", 0.0015)]
    [InlineData("1e0001", 10.0)]
    [InlineData("-0", -0.0)]
    [InlineData("1e400", double.PositiveInfinity)]
    [InlineData("-1e400", double.NegativeInfinity)]
    [InlineData("1e-400", 0.0)]
    [InlineData("inf", double.PositiveInfinity)]
    [InlineData("-inf", double.NegativeInfinity)]
    [InlineData("+inf", double.PositiveInfinity)]
    [InlineData("Inf", double.PositiveInfinity)]
    [InlineData("INF", double.PositiveInfinity)]
    [InlineData("infinity", double.PositiveInfinity)]
    [InlineData("-Infinity", double.NegativeInfinity)]
    [InlineData("INFINITY", double.PositiveInfinity)]
    [InlineData("12345678901234567890123", 1.2345678901234568E+22)]
    public void Grammar_Accepted(string text, double expected)
    {
        Assert.True(FastDouble.TryParse(Encoding.UTF8.GetBytes(text), out double value), text);
        Assert.Equal(BitConverter.DoubleToInt64Bits(expected), BitConverter.DoubleToInt64Bits(value));
        Assert.True(FastDouble.TryParse(Encoding.UTF8.GetBytes(text), out float single), text);
        float expectedSingle = double.IsFinite(expected) ? float.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture) : (float)expected;
        Assert.Equal(BitConverter.SingleToInt32Bits(expectedSingle), BitConverter.SingleToInt32Bits(single));
    }

    [Theory]
    [InlineData("")]
    [InlineData(".")]
    [InlineData("-")]
    [InlineData("+")]
    [InlineData("e5")]
    [InlineData(".e5")]
    [InlineData("1e")]
    [InlineData("1e+")]
    [InlineData("1e-")]
    [InlineData("0x10")]
    [InlineData("1_000")]
    [InlineData("1.5.5")]
    [InlineData("1.5d")]
    [InlineData("1.5f")]
    [InlineData("infin")]
    [InlineData("infinityy")]
    [InlineData("nan(1)")]
    [InlineData("∞")]
    [InlineData("１")]
    [InlineData(" 1.5")]
    [InlineData("1.5 ")]
    [InlineData("\t1.5")]
    [InlineData("1 5")]
    [InlineData("--1")]
    [InlineData("+-1")]
    [InlineData("1e5.5")]
    [InlineData("12345678.12345678x")]
    [InlineData("12345678x12345678")]
    public void Grammar_Refused(string text)
    {
        Assert.False(FastDouble.TryParse(Encoding.UTF8.GetBytes(text), out double _), text);
        Assert.False(FastDouble.TryParse(Encoding.UTF8.GetBytes(text), out float _), text);
    }

    [Fact]
    public void NaN_IsTheQuietNaN_SignFromTheText()
    {
        foreach ((string text, bool negative) in (ReadOnlySpan<(string, bool)>)[("nan", false), ("NaN", false), ("NAN", false), ("+NaN", false), ("-nan", true)])
        {
            Assert.True(FastDouble.TryParse(Encoding.ASCII.GetBytes(text), out double value));
            Assert.Equal(negative ? 0xFFF8_0000_0000_0000 : 0x7FF8_0000_0000_0000, BitConverter.DoubleToUInt64Bits(value));
            Assert.True(FastDouble.TryParse(Encoding.ASCII.GetBytes(text), out float single));
            Assert.Equal(negative ? 0xFFC0_0000u : 0x7FC0_0000u, BitConverter.SingleToUInt32Bits(single));
        }
    }

    [Fact]
    public void RandomDoubles_ShortestAnd17Digits()
    {
        var random = new Random(20261123);
        Span<byte> bits = stackalloc byte[8];
        for (int n = 0; n < 1_000_000; n++)
        {
            random.NextBytes(bits);
            double x = BitConverter.ToDouble(bits);
            if (!double.IsFinite(x)) continue;
            AssertDouble(x.ToString("R", CultureInfo.InvariantCulture));
            AssertDouble(x.ToString("G17", CultureInfo.InvariantCulture));
            if (n % 8 == 0) AssertDouble(Midpoint(x));
        }
    }

    [Fact]
    public void RandomSingles_ShortestAnd9Digits()
    {
        var random = new Random(20261124);
        for (int n = 0; n < 1_000_000; n++)
        {
            float x = BitConverter.Int32BitsToSingle(random.Next() ^ (random.Next(2) << 31));
            if (!float.IsFinite(x)) continue;
            AssertSingle(x.ToString("R", CultureInfo.InvariantCulture));
            AssertSingle(x.ToString("G9", CultureInfo.InvariantCulture));
            string midpoint = Midpoint(x);
            AssertSingle(midpoint);
            // Doubles a few ulps from the halfway point between two floats: a float read through a double would round twice.
            double near = BitConverter.Int64BitsToDouble(BitConverter.DoubleToInt64Bits(double.Parse(midpoint, CultureInfo.InvariantCulture)) + random.Next(-2, 3));
            AssertSingle(near.ToString("R", CultureInfo.InvariantCulture));
        }
    }

    [Fact]
    public void RandomDecimals_AnyDigitsAnyExponent()
    {
        var random = new Random(1123);
        for (int n = 0; n < 1_000_000; n++)
        {
            string text = Decimal(random);
            AssertDouble(text);
            AssertSingle(text);
        }
    }

    /// <summary>A random decimal in the CSV grammar: 1–25 digits (or many), a point anywhere, an exponent −350…+320.</summary>
    private static string Decimal(Random random)
    {
        var text = new StringBuilder();
        if (random.Next(3) == 0) text.Append(random.Next(4) == 0 ? '+' : '-');
        int digits = random.Next(8) == 0 ? random.Next(26, 60) : random.Next(1, 26);
        int point = random.Next(4) == 0 ? -1 : random.Next(0, digits + 1);
        for (int d = 0; d < digits; d++)
        {
            if (d == point) text.Append('.');
            text.Append((char)('0' + (d == 0 && random.Next(4) != 0 ? random.Next(1, 10) : random.Next(10))));
        }
        if (point == digits) text.Append('.');
        if (random.Next(3) != 0)
        {
            text.Append(random.Next(2) == 0 ? 'e' : 'E');
            int exponent = random.Next(-350, 321);
            text.Append(exponent < 0 ? "-" : random.Next(2) == 0 ? "+" : "");
            if (random.Next(5) == 0) text.Append('0');
            text.Append(Math.Abs(exponent));
        }
        return text.ToString();
    }

    /// <summary>The exact decimal halfway between <paramref name="x"/> and the next double up (ties to even decides).</summary>
    private static string Midpoint(double x) => ExactHalfway(BitConverter.DoubleToInt64Bits(Math.Abs(x)), 52, 1075, x < 0);

    private static string Midpoint(float x) => ExactHalfway(BitConverter.SingleToInt32Bits(Math.Abs(x)), 23, 150, x < 0);

    /// <summary>(2m + 1) × 2^(e − 1) printed exactly, m and e the mantissa and power of two of the positive value <paramref name="bits"/>.</summary>
    private static string ExactHalfway(long bits, int mantissaBits, int bias, bool negative)
    {
        long exponentBits = bits >> mantissaBits;
        BigInteger m = bits & ((1L << mantissaBits) - 1);
        if (exponentBits != 0) m += BigInteger.One << mantissaBits;
        int e = (int)Math.Max(exponentBits, 1) - bias;
        BigInteger odd = 2 * m + 1;
        // odd × 2^(e − 1): an integer when e ≥ 1, else odd × 5^(1 − e) × 10^(e − 1).
        string digits = e >= 1 ? (odd << (e - 1)).ToString() : (odd * BigInteger.Pow(5, 1 - e)).ToString() + "e" + (e - 1);
        return (negative ? "-" : "") + digits;
    }

    private static void AssertDouble(string text)
    {
        double expected = double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
        Assert.True(FastDouble.TryParse(Encoding.ASCII.GetBytes(text), out double value), text);
        Assert.True(BitConverter.DoubleToInt64Bits(expected) == BitConverter.DoubleToInt64Bits(value), $"double {text}: {value:R} instead of {expected:R}");
    }

    private static void AssertSingle(string text)
    {
        float expected = float.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
        Assert.True(FastDouble.TryParse(Encoding.ASCII.GetBytes(text), out float value), text);
        Assert.True(BitConverter.SingleToInt32Bits(expected) == BitConverter.SingleToInt32Bits(value), $"float {text}: {value:R} instead of {expected:R}");
    }
}
