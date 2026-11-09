using Polars.CSharp;
using Polars.NET.Core.Helpers;
using Xunit;
using Kaminari.Sazanami.Helpers;

namespace Polars.CSharp.Tests;

public class TerminalDisplayHelpersTests
{
    [Fact]
    public void SanitizeForDisplay_NullOrEmpty_ReturnsSame()
    {
        Assert.Equal(string.Empty, TerminalDisplayHelpers.SanitizeForDisplay(null));
        Assert.Equal(string.Empty, TerminalDisplayHelpers.SanitizeForDisplay(string.Empty));
    }

    [Fact]
    public void SanitizeForDisplay_CleanString_ReturnsSameInstance()
    {
        string clean = "Hello, Polars.NET!\tIndented and fine.\nSecond Line.\r\nWindows Line.\n";
        string result = TerminalDisplayHelpers.SanitizeForDisplay(clean);

        Assert.Same(clean, result);
    }

    [Fact]
    public void SanitizeForDisplay_PreservesTabsAndNewlines()
    {
        string text = "\tcol1\tcol2\n\tval1\tval2\r\n";
        string result = TerminalDisplayHelpers.SanitizeForDisplay(text);

        Assert.Equal(text, result);
    }

    [Fact]
    public void SanitizeForDisplay_UserPoisonPayload_SanitizesExpectedly()
    {
        string poison = "Frame \0 \n \b \r Null";
        string sanitized = TerminalDisplayHelpers.SanitizeForDisplay(poison);

        // \0 -> ␀ (U+2400)
        // \n -> preserved
        // \b -> ␈ (U+2408)
        // \r (isolated) -> ␍ (U+240D)
        string expected = "Frame \u2400 \n \u2408 \u240D Null";
        Assert.Equal(expected, sanitized);
        Assert.Equal(poison.Length, sanitized.Length);
    }

    [Fact]
    public void SanitizeForDisplay_IsolatedCarriageReturn_ReplacedWithSymbol()
    {
        // Isolated CR at the end
        Assert.Equal("end␍", TerminalDisplayHelpers.SanitizeForDisplay("end\r"));

        // Isolated CR in middle
        Assert.Equal("abc␍def", TerminalDisplayHelpers.SanitizeForDisplay("abc\rdef"));

        // CR followed by CR and then LF: first CR is isolated, second is CRLF
        Assert.Equal("a␍\r\nb", TerminalDisplayHelpers.SanitizeForDisplay("a\r\r\nb"));
    }

    [Fact]
    public void SanitizeForDisplay_AnsiEscapeSequence_Neutralized()
    {
        // ESC \x1b -> ␛ (U+241B)
        string ansi = "\x1B[31mRed text\x1B[0m";
        string sanitized = TerminalDisplayHelpers.SanitizeForDisplay(ansi);

        Assert.Equal("\u241B[31mRed text\u241B[0m", sanitized);
        Assert.DoesNotContain("\x1B", sanitized, StringComparison.Ordinal);
    }

    [Fact]
    public void SanitizeForDisplay_AllControlPictures_MappedCorrectly()
    {
        // Test BEL (\a), BS (\b), VT (\v), FF (\f), DEL (\x7F)
        string controls = "\a\b\v\f\u007F";
        string sanitized = TerminalDisplayHelpers.SanitizeForDisplay(controls);

        Assert.Equal("\u2407\u2408\u240B\u240C\u2421", sanitized);
    }

    [Fact]
    public void SanitizeForDisplay_PreservesSurrogatePairsAndCJK()
    {
        string text = "数据火箭 🚀 极速测试 ␀ \0 \r\n";
        string sanitized = TerminalDisplayHelpers.SanitizeForDisplay(text);

        Assert.Equal("数据火箭 🚀 极速测试 ␀ ␀ \r\n", sanitized);
    }

    [Fact]
    public void DataFrameShow_WithPoisonedPayload_OutputsSanitized()
    {
        var records = new[]
        {
            new { id = 1, name = "Data 🚀" },
            new { id = 2, name = "Frame \0 \n \b \r Null" }
        };
        using var df = DataFrame.FromRows(records);

        var originalOut = Console.Out;
        try
        {
            using var sw = new StringWriter();
            Console.SetOut(sw);

            df.Show();

            string output = sw.ToString();
            Assert.Contains("␀", output);
            Assert.Contains("␈", output);
            Assert.Contains("␍", output);
            Assert.DoesNotContain("\0", output, StringComparison.Ordinal);
            Assert.DoesNotContain("\b", output, StringComparison.Ordinal);
        }
        finally
        {
            Console.SetOut(originalOut);
        }
    }
}
