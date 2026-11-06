#nullable enable

using Xunit;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class Utf8StringInteropTests
{
    [Fact]
    public void Test_DataFrame_ToString_SmallAndLarge()
    {
        // 1. Small DataFrame (<= 512 bytes on stack)
        using var smallDf = new DataFrame(
            Series.From("a", [1, 2, 3]),
            Series.From("b", ["x", "y", "z"])
        );
        var smallStr = smallDf.ToString();
        Assert.NotNull(smallStr);
        Assert.Contains("shape: (3, 2)", smallStr);
        Assert.Contains("a", smallStr);
        Assert.Contains("b", smallStr);

        // 2. Large DataFrame (> 512 bytes on heap)
        var col1 = Enumerable.Range(0, 100).ToArray();
        var col2 = Enumerable.Range(0, 100).Select(i => $"very_long_string_entry_value_{i}").ToArray();
        using var largeDf = new DataFrame(
            Series.From("id", col1),
            Series.From("desc", col2)
        );
        var largeStr = largeDf.ToString();
        Assert.NotNull(largeStr);
        Assert.True(largeStr.Length > 512, $"Expected large string > 512 chars, got {largeStr.Length}");
        Assert.Contains("shape: (100, 2)", largeStr);
    }

    [Fact]
    public void Test_DataFrame_ToString_UnicodeAndNulls()
    {
        // Unicode (Chinese, Japanese, Emoji)
        using var dfUnicode = new DataFrame(
            Series.From("name", ["张三", "李四", "山田太郎", "🚀 Rocket"]),
            Series.From("val", [10, 20, 30, 40])
        );
        var unicodeStr = dfUnicode.ToString();
        Assert.Contains("张三", unicodeStr);
        Assert.Contains("李四", unicodeStr);
        Assert.Contains("山田太郎", unicodeStr);
        Assert.Contains("🚀 Rocket", unicodeStr);

        // Empty DataFrame
        using var emptyDf = new DataFrame();
        var emptyStr = emptyDf.ToString();
        Assert.NotNull(emptyStr);
        Assert.Contains("shape: (0, 0)", emptyStr);
    }

    [Fact]
    public void Test_Series_ToString_SmallAndLarge()
    {
        // 1. Small Series (<= 512 bytes on stack)
        using var sSmall = Series.From("s_small", [10, 20, 30]);
        var strSmall = sSmall.ToString();
        Assert.NotNull(strSmall);
        Assert.Contains("s_small", strSmall);
        Assert.Contains("shape: (3,)", strSmall);

        // 2. Large Series (Polars formats preview with truncation)
        var largeItems = Enumerable.Range(0, 100).Select(i => $"item_value_number_{i}").ToArray();
        using var sLarge = Series.From("s_large", largeItems);
        var strLarge = sLarge.ToString();
        Assert.NotNull(strLarge);
        Assert.Contains("shape: (100,)", strLarge);
        Assert.Contains("item_value_number_0", strLarge);

        // 3. Unicode Series
        using var sUnicode = Series.From("s_utf8", ["你好", "世界", "Polars.NET 🚀"]);
        var strUnicode = sUnicode.ToString();
        Assert.Contains("你好", strUnicode);
        Assert.Contains("Polars.NET 🚀", strUnicode);
    }

    [Fact]
    public void Test_Expr_ToString()
    {
        // 1. Simple Expr
        var expr1 = Pl.Col("a") + Pl.Lit(42);
        var str1 = expr1.ToString();
        Assert.NotNull(str1);
        Assert.Contains("col(\"a\")", str1);

        // 2. Nested / Large Expr
        Expr complexExpr = Pl.Col("col_0");
        for (int i = 1; i < 50; i++)
        {
            complexExpr += Pl.Col($"col_{i}");
        }
        var strComplex = complexExpr.ToString();
        Assert.NotNull(strComplex);
        Assert.True(strComplex.Length > 512);
        Assert.Contains("col_49", strComplex);
    }

    [Fact]
    public void Test_Expr_FormatTree()
    {
        var expr = (Pl.Col("foo") * 2) + Pl.Col("bar");

        // FormatTree text
        var tree = expr.Meta.FormatTree(displayAsDot: false);
        Assert.NotNull(tree);
        Assert.Contains("binary: +", tree);

        // FormatTree dot
        var dot = expr.Meta.FormatTree(displayAsDot: true);
        Assert.NotNull(dot);
        Assert.Contains("graph {", dot);
    }

    [Fact]
    public void Test_LazyFrame_ToString_And_Explain()
    {
        using var lf = new DataFrame(
            Series.From("a", [1, 2, 3]),
            Series.From("b", [10, 20, 30])
        ).Lazy()
         .Filter(Pl.Col("a") > 1)
         .Select(Pl.Col("a"), Pl.Col("b") * 2);

        // 1. LazyFrame.ToString()
        var lfStr = lf.ToString();
        Assert.NotNull(lfStr);
        Assert.Contains("shape: (2, 2)", lfStr);

        // 2. LazyFrame.Explain() optimized & unoptimized
        var planOptimized = lf.Explain(optimized: true);
        Assert.NotNull(planOptimized);
        Assert.Contains("FILTER", planOptimized);

        var planUnoptimized = lf.Explain(optimized: false);
        Assert.NotNull(planUnoptimized);
        Assert.Contains("FILTER", planUnoptimized);

        // 3. ExplainAll
        using var lf2 = new DataFrame(Series.From("x", ["hello", "world"])).Lazy();
        var explainAll = Pl.ExplainAll([lf, lf2]);
        Assert.NotNull(explainAll);
        Assert.Contains("FILTER", explainAll);
        Assert.Contains("x", explainAll);
    }
}
