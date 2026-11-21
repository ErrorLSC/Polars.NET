using System.Text;
using Kaminari.Sazanami.Text;

namespace Sazanami.Tests;

/// <summary>
/// ISO 8601 text read exactly (to the nanosecond): dates, date-times with offsets, times of day, durations; what is
/// not one of these forms is refused (against .NET's own parsing of the same instants).
/// </summary>
public class IsoTemporalTests
{
    private static ReadOnlySpan<byte> U(string s) => Encoding.UTF8.GetBytes(s);

    [Theory]
    [InlineData("1970-01-01", 0)]
    [InlineData("2024-02-29", 19782)]
    [InlineData("1969-12-31", -1)]
    [InlineData("0001-01-01", -719162)]
    [InlineData("9999-12-31", 2932896)]
    public void Dates_AreDaysSinceTheEpoch(string text, int days)
    {
        Assert.True(IsoTemporal.TryParseDate(U(text), out int d));
        Assert.Equal(days, d);
    }

    [Theory]
    [InlineData("2023-02-29")]
    [InlineData("2024-13-01")]
    [InlineData("2024-1-01")]
    [InlineData("24-01-01")]
    [InlineData("0000-01-01")]
    [InlineData("2024/01/01")]
    [InlineData("2024-01-01T00:00")]
    public void NotDates_AreRefused(string text) => Assert.False(IsoTemporal.TryParseDate(U(text), out _));

    [Theory]
    [InlineData("2024-01-02T03:04:05", "2024-01-02T03:04:05Z", false)]
    [InlineData("2024-01-02 03:04:05.5", "2024-01-02T03:04:05.5Z", false)]
    [InlineData("2024-01-02t03:04", "2024-01-02T03:04:00Z", false)]
    [InlineData("2024-01-02", "2024-01-02T00:00:00Z", false)]
    [InlineData("2024-01-02T03:04:05Z", "2024-01-02T03:04:05Z", true)]
    [InlineData("2024-01-02T03:04:05+09:00", "2024-01-01T18:04:05Z", true)]
    [InlineData("2024-01-02T03:04:05-0530", "2024-01-02T08:34:05Z", true)]
    [InlineData("2024-01-02T03:04:05.123+01", "2024-01-02T02:04:05.123Z", true)]
    [InlineData("1969-12-31T23:59:59.9999999", "1969-12-31T23:59:59.9999999Z", false)]
    public void DateTimes_AreTheirInstants(string text, string utc, bool zoned)
    {
        Assert.True(IsoTemporal.TryParseDatetime(U(text), out Int128 nanos, out bool z));
        Assert.Equal(zoned, z);
        var expected = DateTimeOffset.Parse(utc, System.Globalization.CultureInfo.InvariantCulture);
        Assert.Equal((Int128)(expected.UtcTicks - DateTime.UnixEpoch.Ticks) * 100, nanos);
    }

    [Fact]
    public void Nanoseconds_AreKeptExactly()
    {
        Assert.True(IsoTemporal.TryParseDatetime(U("2024-01-02T03:04:05.123456789Z"), out Int128 nanos, out _));
        Assert.Equal(1_704_164_645_123_456_789, nanos);
    }

    [Theory]
    [InlineData("2024-01-02T25:00:00")]
    [InlineData("2024-01-02T03:04:60")]
    [InlineData("2024-01-02T03:04:05.")]
    [InlineData("2024-01-02T03:04:05.1234567890")]
    [InlineData("2024-01-02T03:04:05+24:00")]
    [InlineData("2024-01-02X03:04:05")]
    [InlineData("2024-01-02T3:04:05")]
    [InlineData("2024-01-02T03:04:05 ")]
    public void NotDateTimes_AreRefused(string text) => Assert.False(IsoTemporal.TryParseDatetime(U(text), out _, out _));

    [Theory]
    [InlineData("00:00", 0L)]
    [InlineData("03:04:05.5", 11_045_500_000_000L)]
    [InlineData("23:59:59.999999999", 86_399_999_999_999L)]
    public void Times_AreNanosecondsSinceMidnight(string text, long nanos)
    {
        Assert.True(IsoTemporal.TryParseTime(U(text), out long n));
        Assert.Equal(nanos, n);
    }

    [Theory]
    [InlineData("24:00:00")]
    [InlineData("3:04:05")]
    [InlineData("03:04:5")]
    [InlineData("03")]
    public void NotTimes_AreRefused(string text) => Assert.False(IsoTemporal.TryParseTime(U(text), out _));

    [Theory]
    [InlineData("PT1.000005S", 1_000_005_000L)]
    [InlineData("-PT1S", -1_000_000_000L)]
    [InlineData("P1DT2H3M4.5S", 93_784_500_000_000L)]
    [InlineData("P2W", 1_209_600_000_000_000L)]
    [InlineData("PT0S", 0L)]
    [InlineData("pt90m", 5_400_000_000_000L)]
    public void Durations_AreNanoseconds(string text, long nanos)
    {
        Assert.True(IsoTemporal.TryParseDuration(U(text), out Int128 n));
        Assert.Equal(nanos, n);
    }

    [Theory]
    [InlineData("P")]
    [InlineData("PT")]
    [InlineData("P1H")]
    [InlineData("PT1D")]
    [InlineData("PT1M2H")]
    [InlineData("P1.5D")]
    [InlineData("PT1S2S")]
    [InlineData("1S")]
    public void NotDurations_AreRefused(string text) => Assert.False(IsoTemporal.TryParseDuration(U(text), out _));
}
