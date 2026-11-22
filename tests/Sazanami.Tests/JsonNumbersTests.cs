using System.Globalization;
using System.Text;
using Kaminari.Sazanami.Text;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="JsonNumbers"/>: every number its fast paths take parses bit for bit as the runtime's correctly rounded
/// parser parses it (random numbers in JSON's grammar, and edge cases); numbers outside them are left to that parser.
/// </summary>
public class JsonNumbersTests
{
    private static readonly string[] Edges =
    [
        "0", "-0", "0.0", "-0.0", "1", "-1", "0.1", "0.2", "0.3", "1.5", "-2.5e-3", "1e22", "1e-22", "9007199254740992", "9007199254740993",
        "123456789012345678", "1234567890123456789", "0.000001234", "12.34E+5", "5e0", "1E1", "3.14159265358979", "2.2250738585072014e-308",
        "1.7976931348623157e308", "1e23", "1e-23", "100000000000000000000000", "0.1000000000000000000001", "4.35", "1e400", "-1e-400",
        "999999999999999999", "-999999999999999999", "1000000000000000000",
    ];

    /// <summary>A random number in JSON's grammar: an integer part, maybe a fraction, maybe an exponent.</summary>
    private static string Number(Random random)
    {
        var text = new StringBuilder();
        if (random.Next(3) == 0) text.Append('-');
        int digits = random.Next(4) switch { 0 => 1, 1 => random.Next(1, 6), 2 => random.Next(6, 17), _ => random.Next(17, 22) };
        text.Append(digits == 1 ? (char)('0' + random.Next(10)) : (char)('1' + random.Next(9)));
        for (int d = 1; d < digits; d++) text.Append((char)('0' + random.Next(10)));
        if (random.Next(2) == 0)
        {
            text.Append('.');
            int fraction = random.Next(1, random.Next(3) == 0 ? 25 : 8);
            for (int d = 0; d < fraction; d++) text.Append((char)('0' + random.Next(10)));
        }
        if (random.Next(3) == 0)
        {
            text.Append(random.Next(2) == 0 ? 'e' : 'E');
            if (random.Next(2) == 0) text.Append(random.Next(2) == 0 ? '-' : '+');
            text.Append(random.Next(random.Next(2) == 0 ? 30 : 400));
        }
        return text.ToString();
    }

    [Fact]
    public void Doubles_AreTheRuntimesBitForBit()
    {
        var random = new Random(1125);
        int fast = 0;
        foreach (string text in Edges.Concat(Enumerable.Range(0, 200_000).Select(_ => Number(random))))
        {
            if (!JsonNumbers.TryExactDouble(Encoding.ASCII.GetBytes(text), out double value)) continue;
            fast++;
            double expected = double.Parse(text, NumberStyles.Float, CultureInfo.InvariantCulture);
            Assert.True(BitConverter.DoubleToInt64Bits(expected) == BitConverter.DoubleToInt64Bits(value), $"{text}: {value:R} instead of {expected:R}");
        }
        Assert.True(fast > 50_000, $"only {fast} numbers took the fast path");
        // Outside the fast path: too many digits, too large a power, a mantissa past 2^53.
        foreach (string text in (string[])["1234567890123456789012", "1e23", "9007199254740993", "1e400", "0.12345678901234567890123"])
            Assert.False(JsonNumbers.TryExactDouble(Encoding.ASCII.GetBytes(text), out _), text);
    }

    [Fact]
    public void Integers_AreTheRuntimesForWholeNumbersOf18Digits()
    {
        var random = new Random(1126);
        foreach (string text in Edges.Concat(Enumerable.Range(0, 100_000).Select(_ => Number(random))))
        {
            bool whole = long.TryParse(text, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out long expected);
            bool fast = JsonNumbers.TryInt64(Encoding.ASCII.GetBytes(text), out long value);
            if (fast) Assert.True(whole && expected == value, text);
            else Assert.True(!whole || text.TrimStart('-').Length > 18, text);
        }
    }
}
