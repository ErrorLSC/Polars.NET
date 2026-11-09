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
        AssertSame(r => char.IsDigit(r.Text[0]), Udf);
        AssertSame(r => char.IsLetter(r.Text[1]), Udf);
        AssertSame(r => Upper(r.Text[0]), Udf);
    }

    public static char Upper(char c) => char.ToUpperInvariant(c);

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
}
