using Polars.CSharp;
using Polars.NET.Core.Exceptions;

namespace Polars.Config.Tests;

/// <summary>
/// Delta.SessionTimeZone: the zone the SQL expressions of a table read instants in, as Spark's session time zone
/// (serial project: the setting is process-wide).
/// </summary>
public sealed class DeltaSessionTimeZoneTests
{
    private sealed record Event(int Id, DateTimeOffset Ts, DateOnly? D);
    private sealed record Arrival(int Id, DateTimeOffset Ts);

    /// <summary>2021-03-05 04:00 UTC: 2021-03-04 20:00 in Los Angeles.</summary>
    private static readonly DateTimeOffset Instant = new(2021, 3, 5, 4, 0, 0, TimeSpan.Zero);

    private static void InZone(string zone, Action<string> body)
    {
        string path = Path.Combine(Path.GetTempPath(), $"delta_zone_{Guid.NewGuid():N}");
        string before = Delta.SessionTimeZone;
        try
        {
            Delta.SessionTimeZone = zone;
            body(path);
        }
        finally
        {
            Delta.SessionTimeZone = before;
            if (Directory.Exists(path)) Directory.Delete(path, true);
        }
    }

    private static DateOnly? GeneratedDate(string path)
    {
        Delta.CreateTable(path, PolarsSchema.From<Event>(), generatedColumns: [new DeltaGeneratedColumn("D", "CAST(Ts AS DATE)")]);
        using DataFrame rows = DataFrame.From([new Arrival(1, Instant)]);
        rows.WriteDelta(path, mode: DeltaSaveMode.Append);
        using DataFrame read = DataFrame.ReadDelta(path);
        return read.AsEnumerable<Event>().Single().D;
    }

    [Fact]
    public void Generated_columns_and_constraints_read_instants_in_the_session_zone()
    {
        Assert.Equal("UTC", Delta.SessionTimeZone);
        InZone("UTC", path => Assert.Equal(new DateOnly(2021, 3, 5), GeneratedDate(path)));
        InZone("America/Los_Angeles", path =>
        {
            Assert.Equal(new DateOnly(2021, 3, 4), GeneratedDate(path));
            Delta.AddConstraint(path, "evening", "hour(Ts) = 20");
        });
        InZone("UTC", path =>
        {
            GeneratedDate(path);
            Assert.StartsWith("1 rows in ", Assert.Throws<PolarsException>(() => Delta.AddConstraint(path, "evening", "hour(Ts) = 20")).Message);
        });
    }

    [Theory]
    [InlineData("Mars/Olympus_Mons")]
    [InlineData("Pacific Standard Time")]
    [InlineData(" ")]
    public void A_name_that_is_not_an_IANA_zone_is_refused(string zone)
    {
        Assert.ThrowsAny<ArgumentException>(() => Delta.SessionTimeZone = zone);
        Assert.Equal("UTC", Delta.SessionTimeZone);
    }
}
