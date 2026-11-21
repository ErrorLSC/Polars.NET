using System.Linq.Expressions;
using System.Text;
using System.Text.RegularExpressions;
using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests.Linq;

/// <summary>
/// BCL members whose translation used to differ from LINQ to Objects: each result is compared with it, and the
/// execution tier is checked where the member has a native form.
/// </summary>
public class LinqBclTranslationFixesTests
{
    public record BclRow(int Id, string Text, string Csv, double Value, TimeSpan Span, DateTime At);

    private static readonly BclRow[] Rows =
    [
        new(1, "Hello World", "a,b,c", 2.5, TimeSpan.FromHours(26.5) + TimeSpan.FromMilliseconds(1234), new DateTime(2024, 3, 15, 10, 20, 30)),
        new(2, "  trim me ", "x", -3.5, TimeSpan.FromMinutes(-90), new DateTime(2023, 12, 31, 23, 59, 59)),
    ];

    private static void AssertSame<T>(Expression<Func<BclRow, T>> selector, string tier)
    {
        using var df = DataFrame.FromRows(Rows);
        var query = df.AsQueryable<BclRow>().Select(selector);

        Assert.Contains(tier, query.Explain().Split('\n')[0]);
        Assert.Equal(Rows.AsQueryable().Select(selector).ToList(), query.ToList());
    }

    private const string Native = "[Query Plan - 100% Native Pushdown]";
    private const string Udf = "(with C# UDF Callback)";

    [Fact]
    public void IndexOf_With_Start_Searches_From_Start()
    {
        AssertSame(r => r.Text.IndexOf("o", 5), Native);
        AssertSame(r => r.Text.IndexOf('o', 5), Native);
        AssertSame(r => r.Text.IndexOf("o", 5, 4), Native);
        AssertSame(r => r.Text.IndexOf("O", 5, StringComparison.OrdinalIgnoreCase), Native);
        AssertSame(r => r.Text.IndexOf("o", r.Id), Native);
    }

    [Fact]
    public void Split_Returns_Only_The_Pieces()
    {
        using var df = DataFrame.FromRows(Rows);
        void Same(Expression<Func<BclRow, string[]>> selector, string tier)
        {
            var query = df.AsQueryable<BclRow>().Select(selector);
            Assert.Contains(tier, query.Explain().Split('\n')[0]);
            Assert.Equal(Rows.AsQueryable().Select(selector).ToList(), query.ToList());
        }

        Same(r => r.Csv.Split(',', 2), Native);
        Same(r => r.Csv.Split(',', 10), Native);
        Same(r => r.Csv.Split(',', 10, StringSplitOptions.None), Native);
        Same(r => r.Csv.Split(','), Native);
        Same(r => (r.Csv + ",,").Split(',', 10, StringSplitOptions.RemoveEmptyEntries), Udf);
        Same(r => (r.Csv + ",,").Split(',', StringSplitOptions.RemoveEmptyEntries), Udf);
    }

    [Fact]
    public void Split_With_Count_Keeps_A_Null_String_Null()
    {
        using var df = DataFrame.FromColumns(new { Text = new[] { "a,b", null } });
        var pieces = df.AsQueryable<NullableText>().Select(x => x.Text.Split(',', 2)).ToList();

        Assert.Equal(["a", "b"], pieces[0]);
        Assert.Null(pieces[1]);
    }

    public class NullableText { public string Text { get; set; } }

    [Fact]
    public void Format_Specifiers_Use_DotNet_Formatting()
    {
        AssertSame(r => string.Format("{0:F3}", r.Value), Udf);
        AssertSame(r => string.Format("{0,8}|", r.Id), Udf);
        AssertSame(r => string.Format("{{{0}}}", r.Id), Udf);
        AssertSame(r => $"{r.Value:F2}-{r.Id}", Udf);
        AssertSame(r => string.Format("{0}/{1}", r.Text, r.Id), Native);
    }

    [Fact]
    public void Concat_Of_One_Argument_Is_A_String()
    {
        AssertSame(r => string.Concat(r.Id), Udf);
        AssertSame(r => string.Concat((object)r.Text), Native);
    }

    [Fact]
    public void Matches_Select_Honors_The_Selector()
    {
        AssertSame(r => Regex.Matches(r.Text, "l+").Select(m => m.Value).ToList(), Native);
        AssertSame(r => Regex.Matches(r.Text, "l+").Select(m => m.Index).ToList(), Udf);
        AssertSame(r => Regex.Matches(r.Text, "l+").Select(m => m.Length).ToArray(), Udf);
        AssertSame(r => Regex.Matches(r.Text, "l+").Select(m => m.Value.Length).Sum(), Udf);
    }

    [Fact]
    public void Udf_Reads_And_Returns_Char()
    {
        AssertSame(r => IsVowel(r.Text[1]), Udf);
        AssertSame(r => Upper(r.Text[0]), Udf);
        // char.IsDigit and its kin are native (see Char_Classification_Matches_Dotnet_On_Every_Char).
        AssertSame(r => char.IsDigit(r.Text[0]), Native);
        AssertSame(r => char.IsLetter(r.Text[1]), Native);
    }

    public static char Upper(char c) => char.ToUpperInvariant(c);

    public static bool IsVowel(char c) => "aeiou".Contains(c);

    [Fact]
    public void GetByteCount_Is_Native_Only_For_Utf8()
    {
        AssertSame(r => Encoding.UTF8.GetByteCount(r.Text), Native);
        AssertSame(r => Encoding.Unicode.GetByteCount(r.Text), Udf);
        AssertSame(r => Encoding.ASCII.GetByteCount(r.Text), Udf);
    }

    [Fact]
    public void TimeSpan_Components_Are_Native()
    {
        AssertSame(r => r.Span.TotalHours, Native);
        AssertSame(r => r.Span.TotalSeconds, Native);
        AssertSame(r => r.Span.TotalMilliseconds, Native);
        AssertSame(r => r.Span.Days, Native);
        AssertSame(r => r.Span.Hours, Native);
        AssertSame(r => r.Span.Minutes, Native);
        AssertSame(r => r.Span.Seconds, Native);
        AssertSame(r => r.Span.Milliseconds, Native);
        AssertSame(r => (new DateTime(2020, 1, 1) - r.At).Hours, Native);
    }

    [Fact]
    public void TimeSpan_Member_Filters_Natively()
    {
        using var df = DataFrame.FromRows(Rows);
        var query = df.AsQueryable<BclRow>().Where(r => r.Span.TotalHours > 1).Select(r => r.Id);

        Assert.Contains(Native, query.Explain().Split('\n')[0]);
        Assert.Equal([1], query.ToList());
    }

    // ---------------------------------------------------------------------------------------------
    // System.Convert between numbers and booleans, double.IsNaN and its kin, and decimal-to-integer conversions run
    // natively, with .NET's values: Convert rounds half to even, a decimal cast truncates (checked or not),
    // Convert.ToBoolean(NaN) is true; where .NET throws OverflowException (out of range, NaN) the value is null.
    // ---------------------------------------------------------------------------------------------

    public record NumRow(int Id, float F, double D, decimal M, long L, bool B, Half H);

    // Ties at .5 both ways, negative values, -0.0; doubles and floats exact at 18 decimals, so (decimal)x needs no rounding.
    private static readonly NumRow[] Nums =
    [
        new(1, 0.5f, 2.5, 2.5m, 7, true, (Half)1.5),
        new(2, -1.5f, -3.5, -2.9m, -300, false, (Half)(-2)),
        new(3, 254.5f, 0.375, -488023.5m, 40_000, true, (Half)0),
        new(4, -0.0f, -0.0, 0.5m, 0, false, (Half)0.25),
        new(5, 1e6f + 0.5f, 1e9 + 0.5, 123456789.987654321m, long.MaxValue, true, (Half)8),
    ];

    private static void AssertSameOver<TRow, T>(TRow[] rows, Expression<Func<TRow, T>> selector)
    {
        using var df = DataFrame.FromRows(rows);
        var query = df.AsQueryable<TRow>().Select(selector);
        Assert.Contains(Native, query.Explain().Split('\n')[0]);
        Assert.Equal(rows.AsQueryable().Select(selector).ToList(), query.ToList());
    }

    private static void AssertNullWhereDotnetThrows<TRow, T>(TRow row, Expression<Func<TRow, T?>> selector) where T : struct
    {
        TRow[] rows = [row];
        using var df = DataFrame.FromRows(rows);
        Assert.Throws<OverflowException>(() => rows.Select(selector.Compile()).ToList());
        Assert.Equal([(T?)null], df.AsQueryable<TRow>().Select(selector).ToList());
    }

    [Fact]
    public void Convert_Between_Numbers_Runs_Natively()
    {
        AssertSameOver(Nums, r => Convert.ToInt32(r.D));
        AssertSameOver(Nums, r => Convert.ToInt32(r.F));
        AssertSameOver(Nums, r => Convert.ToInt64(r.M));
        AssertSameOver(Nums, r => Convert.ToInt32(r.M));
        AssertSameOver(Nums, r => Convert.ToByte(r.Id));
        AssertSameOver(Nums, r => Convert.ToInt16(r.Id * 1000));
        AssertSameOver(Nums, r => Convert.ToUInt64(r.Id));
        AssertSameOver(Nums, r => Convert.ToDouble(r.M));
        AssertSameOver(Nums, r => Convert.ToSingle(r.M));
        AssertSameOver(Nums, r => Convert.ToDouble(r.L));
        AssertSameOver(Nums, r => Convert.ToDecimal(r.D));
        AssertSameOver(Nums, r => Convert.ToDecimal(r.F));
        AssertSameOver(Nums, r => Convert.ToDecimal(r.Id));
        AssertSameOver(Nums, r => Convert.ToBoolean(r.L));
        AssertSameOver(Nums, r => Convert.ToBoolean(r.D));
        AssertSameOver(Nums, r => Convert.ToInt32(r.B));
        AssertSameOver(Nums, r => Convert.ToDouble(r.B));
        AssertSameOver(Nums, r => Convert.ToDouble(r.D, System.Globalization.CultureInfo.InvariantCulture));
        AssertSameOver(Nums, r => Convert.ToInt32(r.M, System.Globalization.CultureInfo.InvariantCulture));
        // In a group's aggregate.
        using var df = DataFrame.FromRows(Nums);
        var grouped = df.AsQueryable<NumRow>().GroupBy(r => r.B).Select(g => new { g.Key, S = Convert.ToDouble(g.Sum(r => r.M)) }).OrderBy(r => r.Key);
        Assert.Contains(Native, grouped.Explain().Split('\n')[0]);
        Assert.Equal(Nums.GroupBy(r => r.B).Select(g => new { g.Key, S = Convert.ToDouble(g.Sum(r => r.M)) }).OrderBy(r => r.Key), grouped.ToList());
    }

    [Fact]
    public void Convert_Gives_Null_Out_Of_Range_And_For_NaN()
    {
        AssertNullWhereDotnetThrows(Nums[0] with { D = double.NaN }, r => (int?)Convert.ToInt32(r.D));
        AssertNullWhereDotnetThrows(Nums[0] with { D = 2147483647.5 }, r => (int?)Convert.ToInt32(r.D)); // rounds to 2^31
        AssertNullWhereDotnetThrows(Nums[0] with { M = 255.5m }, r => (byte?)Convert.ToByte(r.M)); // rounds to 256
        AssertNullWhereDotnetThrows(Nums[0] with { L = 40_000 }, r => (short?)Convert.ToInt16(r.L));
        AssertNullWhereDotnetThrows(Nums[0] with { F = -0.6f }, r => (uint?)Convert.ToUInt32(r.F)); // rounds to -1
    }

    [Fact]
    public void Decimal_To_Integer_Truncates_Checked_Or_Not()
    {
        AssertSameOver(Nums, r => (int)r.M);
        AssertSameOver(Nums, r => checked((int)r.M));
        AssertSameOver(Nums, r => (long)r.M);
        AssertSameOver(Nums, r => (short)(r.M / 10000m));
        AssertSameOver(Nums, r => (int?)(r.Id > 2 ? r.M : (decimal?)null));
        AssertSameOver(Nums, r => decimal.ToInt32(r.M));
        AssertSameOver(Nums, r => decimal.ToInt64(r.M));
        AssertSameOver(Nums, r => decimal.ToDouble(r.M));
        // Runtime-independent: truncated toward zero (the engine's cast rounded: 3, -3, -488024).
        using var df = DataFrame.FromRows(Nums);
        Assert.Equal([2, -2, -488023, 0, 123456789], df.AsQueryable<NumRow>().Select(r => (int)r.M).ToList());
        AssertNullWhereDotnetThrows(Nums[0] with { M = -1.5m }, r => (byte?)r.M);
        AssertNullWhereDotnetThrows(Nums[0] with { M = 3e9m }, r => (int?)r.M);
        AssertNullWhereDotnetThrows(Nums[0] with { M = 3e9m }, r => (int?)decimal.ToInt32(r.M));
    }

    [Fact]
    public void Floating_Classification_Runs_Natively()
    {
        NumRow[] rows =
        [
            new(1, float.NaN, double.PositiveInfinity, 0m, 0, false, Half.NaN),
            new(2, float.NegativeInfinity, double.NaN, 0m, 0, false, Half.NegativeInfinity),
            new(3, 1.5f, -0.0, 0m, 0, false, Half.PositiveInfinity),
            new(4, float.PositiveInfinity, double.NegativeInfinity, 0m, 0, false, (Half)2),
        ];
        AssertSameOver(rows, r => double.IsNaN(r.D));
        AssertSameOver(rows, r => float.IsNaN(r.F));
        AssertSameOver(rows, r => Half.IsNaN(r.H));
        AssertSameOver(rows, r => double.IsInfinity(r.D));
        AssertSameOver(rows, r => float.IsInfinity(r.F));
        AssertSameOver(rows, r => Half.IsInfinity(r.H));
        AssertSameOver(rows, r => double.IsPositiveInfinity(r.D));
        AssertSameOver(rows, r => float.IsNegativeInfinity(r.F));
        AssertSameOver(rows, r => double.IsFinite(r.D));
        AssertSameOver(rows, r => float.IsFinite(r.F));
        AssertSameOver(rows, r => Half.IsFinite(r.H));
        AssertSameOver(rows, r => double.IsRealNumber(r.D));
        using var df = DataFrame.FromRows(rows);
        var filtered = df.AsQueryable<NumRow>().Where(r => !double.IsNaN(r.D) && double.IsFinite(r.F)).Select(r => r.Id);
        Assert.Contains(Native, filtered.Explain().Split('\n')[0]);
        Assert.Equal(rows.Where(r => !double.IsNaN(r.D) && double.IsFinite(r.F)).Select(r => r.Id), filtered.ToList());
    }

    // ---------------------------------------------------------------------------------------------
    // char.IsDigit and its kin are anchored Unicode-category matches of a char's one-character string; compared with
    // .NET on every char a string column can hold (all but the lone surrogates). The remaining classifications of
    // double, float and Half (normal, subnormal, integer, even, odd) are comparisons; Half is checked on every value.
    // ---------------------------------------------------------------------------------------------

    public record CharRow(int Code, char C, string S);

    private static readonly CharRow[] EveryChar =
        [.. Enumerable.Range(0, 65536).Where(c => c is < 0xD800 or > 0xDFFF).Select(c => new CharRow(c, (char)c, "a" + (char)c))];

    [Fact]
    public void Char_Classification_Matches_Dotnet_On_Every_Char()
    {
        Expression<Func<CharRow, bool>>[] classifications =
        [
            r => char.IsDigit(r.C), r => char.IsNumber(r.C), r => char.IsLetter(r.C), r => char.IsLetterOrDigit(r.C),
            r => char.IsUpper(r.C), r => char.IsLower(r.C), r => char.IsPunctuation(r.C), r => char.IsSymbol(r.C),
            r => char.IsSeparator(r.C), r => char.IsControl(r.C), r => char.IsWhiteSpace(r.C), r => char.IsAscii(r.C),
            r => char.IsAsciiDigit(r.C), r => char.IsAsciiLetter(r.C), r => char.IsAsciiLetterLower(r.C),
            r => char.IsAsciiLetterUpper(r.C), r => char.IsAsciiLetterOrDigit(r.C), r => char.IsAsciiHexDigit(r.C),
            r => char.IsAsciiHexDigitLower(r.C), r => char.IsAsciiHexDigitUpper(r.C),
            r => char.IsDigit(r.S, 1), r => char.IsWhiteSpace(r.S, 1), r => char.IsLetter(r.S, 1),
        ];
        foreach (var classification in classifications)
            AssertSameOver(EveryChar, classification);
        using var df = DataFrame.FromRows(EveryChar);
        var filtered = df.AsQueryable<CharRow>().Where(r => char.IsUpper(r.C) && !char.IsAscii(r.C)).Select(r => r.Code);
        Assert.Contains(Native, filtered.Explain().Split('\n')[0]);
        Assert.Equal(EveryChar.Where(r => char.IsUpper(r.C) && !char.IsAscii(r.C)).Select(r => r.Code), filtered.ToList());
    }

    public record HalfRow(int Bits, Half H);
    public record FloatRow(int Id, double D, float F);

    [Fact]
    public void Floating_Integer_And_Normal_Classification_Runs_Natively()
    {
        HalfRow[] halves = [.. Enumerable.Range(0, 65536).Select(b => new HalfRow(b, BitConverter.UInt16BitsToHalf((ushort)b)))];
        AssertSameOver(halves, r => Half.IsNormal(r.H));
        AssertSameOver(halves, r => Half.IsSubnormal(r.H));
        AssertSameOver(halves, r => Half.IsInteger(r.H));
        AssertSameOver(halves, r => Half.IsEvenInteger(r.H));
        AssertSameOver(halves, r => Half.IsOddInteger(r.H));

        var random = new Random(2);
        double[] specials =
        [
            0, -0.0, double.Epsilon, -double.Epsilon, Math.ScaleB(1, -1022), Math.BitDecrement(Math.ScaleB(1, -1022)),
            double.MaxValue, double.NaN, double.PositiveInfinity, double.NegativeInfinity, 1, -1, 2, 3, -3, 2.5, 1e300,
            4503599627370497.0, -4503599627370497.0, 9007199254740992.0, 1.5e-45, 1.2e-38,
        ];
        FloatRow[] rows =
        [
            .. specials.Concat(Enumerable.Range(0, 4000).Select(_ => BitConverter.Int64BitsToDouble(random.NextInt64())))
                .Concat(Enumerable.Range(0, 2000).Select(_ => random.Next(-100000, 100000) / 2.0))
                .Select((d, i) => new FloatRow(i, d, (float)d)),
        ];
        AssertSameOver(rows, r => double.IsNormal(r.D));
        AssertSameOver(rows, r => double.IsSubnormal(r.D));
        AssertSameOver(rows, r => double.IsInteger(r.D));
        AssertSameOver(rows, r => double.IsEvenInteger(r.D));
        AssertSameOver(rows, r => double.IsOddInteger(r.D));
        AssertSameOver(rows, r => float.IsNormal(r.F));
        AssertSameOver(rows, r => float.IsSubnormal(r.F));
        AssertSameOver(rows, r => float.IsInteger(r.F));
        AssertSameOver(rows, r => float.IsEvenInteger(r.F));
        AssertSameOver(rows, r => float.IsOddInteger(r.F));
    }

    public record ConvertRow(int Id, int? NI, string S, bool B);

    [Fact]
    public void Convert_Of_Nulls_Boxed_Values_And_Bools_Runs_Natively()
    {
        ConvertRow[] rows = [new(1, null, null, true), new(2, 5, "42", false), new(3, -7, "-3", true)];
        // A null string or a boxed null converts to 0, as Convert does.
        AssertSameOver(rows, r => Convert.ToInt32(r.S));
        AssertSameOver(rows, r => Convert.ToDouble(r.S));
        AssertSameOver(rows, r => Convert.ToInt64((object)r.NI));
        AssertSameOver(rows, r => Convert.ToDouble((object)r.NI, System.Globalization.CultureInfo.InvariantCulture));
        AssertSameOver(rows, r => Convert.ToBoolean((object)r.NI));
        AssertSameOver(rows, r => Convert.ToInt32((object)r.S));
        AssertSameOver(rows, r => Convert.ToString(r.B));
        AssertSameOver(rows, r => Convert.ToString(r.B, System.Globalization.CultureInfo.InvariantCulture));
    }

    // ---------------------------------------------------------------------------------------------
    // DateTime.Parse (and Convert.ToDateTime, DateOnly.Parse, TimeOnly.Parse) read ISO 8601 text row by row: the
    // engine's own parse infers one format from the first value and applied it to every row, so a date-only first row
    // dropped the time of the others. bool.Parse and Convert.ToBoolean(string) read "true" / "false" ignoring case and
    // white space (the engine casts no string to a boolean: the query failed).
    // ---------------------------------------------------------------------------------------------

    public record TextRow(int Id, string At, string Flag, string Day, string Time);

    private static readonly TextRow[] Texts =
    [
        new(1, "2024-01-02", "true", "2024-01-02", "10:00"),
        new(2, "2023-12-31 10:00:00", " FALSE ", "2024-02-03", "10:00:30"),
        new(3, "2023-12-31T10:00:00", "True", "2024-03-04", "10:00:30.25"),
        new(4, "2023-12-31 10:00:00.123", "False", "2024-04-05", "23:59"),
        new(5, " 2023-12-31T10:05 ", "\ttrue\n", "2024-05-06", "00:00:00.123456"),
        new(6, "2023-12-31 23:59:59.999999", "false", "2024-06-07", "12:34:56"),
    ];

    [Fact]
    public void Date_And_Time_Parse_Read_Every_Row_In_Its_Own_Iso_Form()
    {
        AssertSameOver(Texts, r => DateTime.Parse(r.At));
        AssertSameOver(Texts, r => Convert.ToDateTime(r.At));
        AssertSameOver(Texts, r => DateOnly.Parse(r.Day));
        AssertSameOver(Texts, r => TimeOnly.Parse(r.Time));
        // Runtime-independent: the times of rows after a date-only first row are kept.
        using var df = DataFrame.FromRows(Texts);
        Assert.Equal(new DateTime(2023, 12, 31, 10, 0, 0), df.AsQueryable<TextRow>().Select(r => DateTime.Parse(r.At)).ToList()[1]);
        // Convert takes a null string as DateTime.MinValue.
        TextRow[] withNull = [new(1, null, "true", "2024-01-02", "10:00")];
        AssertSameOver(withNull, r => Convert.ToDateTime(r.At));
    }

    [Fact]
    public void Boolean_Parse_Ignores_Case_And_White_Space()
    {
        AssertSameOver(Texts, r => bool.Parse(r.Flag));
        AssertSameOver(Texts, r => Convert.ToBoolean(r.Flag));
        // Other text, where .NET throws FormatException, is null; Convert takes a null string as false.
        TextRow[] other = [new(1, "", "yes", "", ""), new(2, "", null, "", "")];
        using var df = DataFrame.FromRows(other);
        Assert.Equal([null, null], df.AsQueryable<TextRow>().Select(r => (bool?)bool.Parse(r.Flag)).ToList());
        Assert.Equal([null, false], df.AsQueryable<TextRow>().Select(r => (bool?)Convert.ToBoolean(r.Flag)).ToList());
    }
}
