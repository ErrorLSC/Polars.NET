using System.Globalization;
using System.Linq.Expressions;
using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests.Linq;

/// <summary>
/// A value's text (string concatenation and interpolation, string.Format, string.Concat and string.Join, ToString(),
/// ToString(culture), Convert.ToString) is .NET's, under the current culture or the one given: doubles and floats as
/// .NET lays out their shortest round-trip digits, integers and decimals with the culture's signs and separator,
/// booleans as "True" / "False", dates and times by the culture's general patterns. The engine's own text differed
/// ("2.0", "1e+16", "true", "2023-12-31 10:05:00.000000").
/// </summary>
public class LinqValueTextTests
{
    private const string Native = "[Query Plan - 100% Native Pushdown]";

    public record TextRow(int Id, double D, float F, int I, long L, bool B, DateTime T, double? ND)
    {
        public DateOnly Day => DateOnly.FromDateTime(T);
        public TimeOnly Time => TimeOnly.FromDateTime(T);
    }

    private static readonly TextRow[] Rows = BuildRows();

    private static TextRow[] BuildRows()
    {
        var random = new Random(6);
        double[] specials =
        [
            2.0, 1e16, 1e17, 1.2345678901234568E+17, 0.0001, 1e-5, -0.0, 1e15, double.NaN, double.PositiveInfinity,
            double.NegativeInfinity, 0.1, -123.456, double.MaxValue, double.Epsilon,
            630179542433601.25, 630179542433601.75, // two shortest forms equally near: .NET writes the even one
        ];
        return
        [
            .. specials
                .Concat(Enumerable.Range(0, 2000).Select(_ => random.Next(3) == 0
                    ? BitConverter.Int64BitsToDouble(random.NextInt64())
                    : Math.Round((random.NextDouble() - 0.5) * Math.Pow(10, random.Next(-6, 20)), random.Next(0, 8))))
                .Select((d, i) => new TextRow(i, d, (float)d, random.Next(-1000, 1000), random.NextInt64(long.MinValue, long.MaxValue),
                    i % 2 == 0, new DateTime(2024, 1, 1).AddSeconds(random.Next(0, 300_000_000)), i % 5 == 0 ? null : d)),
        ];
    }

    private static void AssertSame<T>(Expression<Func<TextRow, T>> selector, bool native = true)
    {
        using var df = DataFrame.From(Rows);
        var query = df.AsQueryable<TextRow>().Select(selector);
        if (native) Assert.Contains(Native, query.Explain().Split('\n')[0]);
        Assert.Equal(Rows.Select(selector.Compile()), query.ToList());
    }

    private static void InCulture(string name, Action test)
    {
        var saved = CultureInfo.CurrentCulture;
        CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo(name);
        try { test(); }
        finally { CultureInfo.CurrentCulture = saved; }
    }

    [Theory]
    [InlineData("en-US")]
    [InlineData("de-DE")]
    [InlineData("")]
    [InlineData("fr-FR")]
    [InlineData("ja-JP")]
    [InlineData("sv-SE")] // U+2212 MINUS SIGN as the negative sign
    public void Value_Text_Is_Dotnet_Text_Under_The_Current_Culture(string culture) => InCulture(culture, () =>
    {
        AssertSame(r => "v=" + r.D);
        AssertSame(r => $"{r.D}|{r.F}|{r.I}|{r.L}|{r.B}");
        AssertSame(r => r.D.ToString());
        AssertSame(r => r.F.ToString());
        AssertSame(r => r.I.ToString());
        AssertSame(r => r.L.ToString());
        AssertSame(r => r.B.ToString());
        AssertSame(r => Convert.ToString(r.D));
        AssertSame(r => Convert.ToString(r.L));
        AssertSame(r => r.ND.ToString());
        AssertSame(r => "n=" + r.ND);
        AssertSame(r => string.Concat(r.D));
        AssertSame(r => string.Join("/", new object[] { r.D, r.I, r.B }));
        AssertSame(r => "t=" + r.T);
        AssertSame(r => r.T.ToString());
        AssertSame(r => r.Day.ToString());
        AssertSame(r => r.Time.ToString());
        // A culture named in the call wins over the current one.
        AssertSame(r => r.D.ToString(CultureInfo.InvariantCulture));
        AssertSame(r => Convert.ToString(r.D, CultureInfo.GetCultureInfo("de-DE")));
        AssertSame(r => string.Format(CultureInfo.InvariantCulture, "{0};{1}", r.D, r.T));
    });

    [Fact]
    public void Runtime_Independent_Text_Of_Doubles()
    {
        InCulture("en-US", () =>
        {
            using var df = DataFrame.From(Rows);
            var texts = df.AsQueryable<TextRow>().Select(r => "v=" + r.D).ToList();
            Assert.Equal(
                ["v=2", "v=10000000000000000", "v=1E+17", "v=1.2345678901234568E+17", "v=0.0001", "v=1E-05", "v=-0", "v=1000000000000000", "v=NaN", "v=∞", "v=-∞", "v=0.1", "v=-123.456", "v=1.7976931348623157E+308", "v=5E-324", "v=630179542433601.2", "v=630179542433601.8"],
                texts.Take(17));
        });
    }

    // A culture whose dates the engine cannot write (non-English AM/PM, a calendar other than the Gregorian one) leaves
    // a date's text to .NET: right, as a UDF.
    [Theory]
    [InlineData("ko-KR")] // 오전 / 오후
    [InlineData("ar-SA")]
    [InlineData("th-TH")] // Buddhist era: 2024 is 2567
    [InlineData("fa-IR")] // Persian calendar
    public void Dates_Of_Other_Calendars_And_Designators_Run_As_Udf(string culture) => InCulture(culture, () =>
    {
        AssertSame(r => r.T.ToString(), native: false);
        AssertSame(r => "v=" + r.D);
    });

    public record PriceRow(int Id, decimal Price);

    // A decimal's text keeps the column's scale (.NET keeps each value's own): equal to .NET where every value has the
    // column's scale, as values read from a decimal(10, 2) column do.
    [Fact]
    public void Decimal_Text_Keeps_The_Column_Scale()
    {
        decimal[] prices = [360.80m, -1.00m, 0.05m, 1234567.89m];
        using var df = DataFrame.FromColumns(new { Id = new[] { 1, 2, 3, 4 }, Price = prices });
        foreach (var culture in new[] { "en-US", "de-DE", "sv-SE" })
        {
            InCulture(culture, () =>
            {
                var query = df.AsQueryable<PriceRow>().Select(r => "p=" + r.Price);
                Assert.Contains(Native, query.Explain().Split('\n')[0]);
                Assert.Equal(prices.Select(p => "p=" + p), query.ToList());
            });
        }
    }
}
