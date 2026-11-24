using Kaminari.Sazanami.Text;

namespace Sazanami.Tests;

/// <summary>
/// <see cref="TimeZoneOffsets"/> abbreviations (chrono's %Z, as the reference engine writes them): from the TZif files'
/// local time types, local mean time before the first transition, and the footer's rule past the last transition
/// (standard and daylight time told apart).
/// </summary>
public class TimeZoneOffsetsTests
{
    private static long Utc(int year, int month, int day) => new DateTimeOffset(year, month, day, 12, 0, 0, TimeSpan.Zero).ToUnixTimeSeconds();

    [Fact]
    public void Abbreviations_FromTheFiles_TheFooter_AndFixedOffsets()
    {
        Assert.Equal("UTC", TimeZoneOffsets.For("UTC")!.AbbreviationAt(0));
        Assert.Equal("+05:30", TimeZoneOffsets.For("+05:30")!.AbbreviationAt(0));
        Assert.Equal("-03:00", TimeZoneOffsets.For("-0300")!.AbbreviationAt(0));
        TimeZoneOffsets newYork = TimeZoneOffsets.For("America/New_York")!;
        if (!newYork.HasAbbreviations) return; // no time zone database files (Windows)
        Assert.Equal("LMT", newYork.AbbreviationAt(Utc(1800, 7, 1)));
        Assert.Equal("EST", newYork.AbbreviationAt(Utc(2024, 1, 15)));
        Assert.Equal("EDT", newYork.AbbreviationAt(Utc(2024, 7, 15)));
        // Past the files' last transition: the footer's rule (EST5EDT,M3.2.0,M11.1.0).
        Assert.Equal("EST", newYork.AbbreviationAt(Utc(2100, 1, 15)));
        Assert.Equal("EDT", newYork.AbbreviationAt(Utc(2100, 7, 15)));
        Assert.Equal("EDT", newYork.AbbreviationAt(Utc(9999, 7, 15)));
        TimeZoneOffsets lordHowe = TimeZoneOffsets.For("Australia/Lord_Howe")!;
        Assert.Equal("+11", lordHowe.AbbreviationAt(Utc(2100, 1, 15)));
        Assert.Equal("+1030", lordHowe.AbbreviationAt(Utc(2100, 7, 15)));
        Assert.Equal("JST", TimeZoneOffsets.For("Asia/Tokyo")!.AbbreviationAt(Utc(2100, 7, 15)));
    }
}
