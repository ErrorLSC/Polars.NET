using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;
namespace Polars.CSharp.Tests;

public class RangeTests
{
    [Fact]
    [Trait("Range","Int")]
    public void IntRange_SingleArgument_ShouldStartFromZero()
    {
        // Arrange & Act
        var expr = Pl.IntRange(5);
        using var df = new DataFrame().Select(expr);

        // Assert
        var series = df[0];
        Assert.Equal(5, series.Length);

        var arr = series.ToArray<long?>();
        Assert.Equal(0L, arr[0]);
        Assert.Equal(1L, arr[1]);
        Assert.Equal(4L, arr[4]);
    }

    [Fact]
    [Trait("Range","Int")]
    public void IntRange_WithStartEndAndStep_ShouldWork()
    {
        // Arrange & Act
        var expr = Pl.IntRange(1, 10, step: 2);
        using var df = new DataFrame().Select(expr);

        // Assert
        var arr = df[0].ToArray<long?>();
        
        // [1, 3, 5, 7, 9]
        Assert.Equal(5, arr.Length);
        Assert.Equal([1, 3, 5, 7, 9], arr);
    }

    [Fact]
    [Trait("Range","Int")]
    public void IntRange_WithCustomDtype_ShouldCastCorrectly()
    {
        var expr = Pl.IntRange(0, 3, datatype: typeof(int));
        using var df = new DataFrame().Select(expr);

        // Assert
        var series = df[0];
        Assert.Equal(DataType.Int32, series.DataType);
        
        var arr = series.ToArray<int?>();
        Assert.Equal([0, 1, 2], arr);
    }

    [Fact]
    [Trait("Range","IntAsSeries")]
    public void IntRangeAsSeries_SingleArgument_ShouldMaterializeCorrectly()
    {
        using var series = Pl.IntRangeAsSeries(3, name: "my_range");

        // Assert
        Assert.Equal("my_range", series.Name);
        Assert.Equal(3, series.Length);

        var arr = series.ToArray<long?>();
        Assert.Equal([0, 1, 2], arr);
    }

    [Fact]
    [Trait("Range","IntRanges")]
    public void IntRanges_RowWise_ShouldGenerateLists()
    {
        // Arrange
        var df = DataFrame.FromColumns(new
        {
            start_col = new[] { 1, 5, 10 },
            end_col = new[] { 3, 8, 10 }
        });

        // Act
        using var result = df.Select(Pl.IntRanges("start_col", "end_col").Alias("ranges"));

        // Assert
        var listSeries = result["ranges"];
        
        Assert.Equal(DataType.List(DataType.Int64), listSeries.DataType);
        Assert.Equal(3, listSeries.Length);

        using var exploded = result.Explode(["ranges"],emptyAsNull:false);
        Assert.Equal(5, exploded.Height);

        var values = exploded["ranges"].ToArray<long?>();
        Assert.Equal([1, 2, 5, 6, 7], values);
    }
    [Fact]
    [Trait("Range","DateRange")]
    public void DateRange_StartEnd_WithDefaultInterval_ShouldWork()
    {
        var series = Pl.DateRangeAsSeries(new DateTime(2026, 1, 1),new DateTime(2026, 1, 5));

        Assert.Equal(DataType.Date, series.DataType);
        Assert.Equal(5, series.Length); 

        Assert.Equal(new DateOnly(2026, 1, 1), series[0]);
        Assert.Equal(new DateOnly(2026, 1, 5), series[4]);

        var series2 = Pl.DateRangeAsSeries(new DateOnly(2026, 1, 1), new DateOnly(2026, 1, 2), interval: TimeSpan.FromDays(1));

        Assert.Equal(2, series2.Length);
        Assert.Equal(new DateOnly(2026, 1, 1), series2[0]);
        Assert.Equal(new DateOnly(2026, 1, 2), series2[1]);
    }
    [Fact]
    [Trait("Range","DateRanges")]
    public void DateRanges_StartEnd_WithDefaultInterval_ShouldWork()
    {
        var series = Pl.DateRangesAsSeries(new DateOnly(2026, 1, 1),new DateOnly(2026, 1, 5));

        Assert.Equal(DataType.List(DataType.Date), series.DataType);
        Assert.Equal(1, series.Length); 

        var exploded = series.Explode();
        Assert.Equal(exploded.ToArray<DateOnly>(),Pl.DateRangeAsSeries(new DateOnly(2026, 1, 1),new DateOnly(2026, 1, 5)).ToArray<DateOnly>());

        var series2 = Pl.DateRangesAsSeries(new DateOnly(2026, 1, 1), new DateOnly(2026, 1, 2), interval: TimeSpan.FromDays(1));

        Assert.Equal(1, series2.Length);
    }
    [Fact]
    [Trait("Range","DatetimeRangeByDate")]
    public void DatetimeRangeAsSeries_DefaultInterval_ShouldGenerateDaily()
    {
        // Arrange
        var start = new DateOnly(2026, 1, 1);
        var end = new DateOnly(2026, 1, 3);

        using var series = Pl.DatetimeRangeAsSeries(start, end);
        // Assert
        Assert.Equal(3, series.Length);
        Assert.Equal(DataType.Datetime(TimeUnit.Microseconds), series.DataType);
        Assert.Equal(new DateTime(2026, 1, 1, 0, 0, 0), series[0]);
        Assert.Equal(new DateTime(2026, 1, 2, 0, 0, 0), series[1]);
        Assert.Equal(new DateTime(2026, 1, 3, 0, 0, 0), series[2]);
    }
    [Fact]
    [Trait("Range","DatetimeRange")]
    public void DatetimeRangeAsSeries_WithTimeSpanInterval_ShouldGenerateHourly()
    {
        // Arrange
        var start = new DateTime(2026, 1, 1, 10, 0, 0);
        var end = new DateTime(2026, 1, 1, 13, 0, 0);
        using var series = Pl.DatetimeRangeAsSeries(start, end, interval: TimeSpan.FromHours(1));

        Assert.Equal(4, series.Length); // 10:00, 11:00, 12:00, 13:00
        
        Assert.Equal(new DateTime(2026, 1, 1, 10, 0, 0), series[0]);
        Assert.Equal(new DateTime(2026, 1, 1, 11, 0, 0), series[1]);
        Assert.Equal(new DateTime(2026, 1, 1, 12, 0, 0), series[2]);
        Assert.Equal(new DateTime(2026, 1, 1, 13, 0, 0), series[3]);
    }
    [Fact]
    [Trait("Range","DatetimeRange")]
    public void DatetimeRangeAsSeries_WithTimeZoneAndUnit_ShouldRespectParameters()
    {
        var start = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc);

        using var series = Pl.DatetimeRangeAsSeries(
            start, 
            end, 
            unit: TimeUnit.Milliseconds, 
            timeZone: "UTC"
        );

        // Assert
        Assert.Equal(DataType.Datetime(TimeUnit.Milliseconds,timeZone:"UTC"), series.DataType);
        Assert.Equal(2, series.Length);

        Assert.Equal(new DateTimeOffset(2026, 1, 1, 0, 0, 0,TimeSpan.Zero), series[0]);
        Assert.Equal(new DateTimeOffset(2026, 1, 2, 0, 0, 0, TimeSpan.Zero), series[1]);
    }
    [Fact]
    [Trait("Range","DatetimeOffsetRange")]
    public void DatetimeRangeAsSeries_WithTimeZoneAndUnit_DateTimeOffset()
    {
        var start = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.FromHours(8));
        var end = new DateTimeOffset(2026, 1, 2, 0, 0, 0, TimeSpan.FromHours(8));

        using var series = Pl.DatetimeRangeAsSeries(
            start, 
            end, 
            unit: TimeUnit.Milliseconds, 
            timeZone: "UTC"
        );
        series.Show();
        // Assert
        Assert.Equal(DataType.Datetime(TimeUnit.Milliseconds,timeZone:"UTC"), series.DataType);
        Assert.Equal(2, series.Length);

        Assert.Equal(new DateTimeOffset(2026, 1, 1, 0, 0, 0,TimeSpan.FromHours(8)), series[0]);
        Assert.Equal(new DateTimeOffset(2026, 1, 2, 0, 0, 0, TimeSpan.FromHours(8)), series[1]);
    }
    [Fact]
    [Trait("Range","DatetimeRanges")]
    public void DatetimeRanges_RowWise_ShouldGenerateLists()
    {

        var start1 = new DateTime(2026, 1, 1, 0, 0, 0);
        var end1 = new DateTime(2026, 1, 3, 0, 0, 0);
        
        var start2 = new DateTime(2026, 2, 1, 0, 0, 0);
        var end2 = new DateTime(2026, 2, 2, 0, 0, 0);

        var df = DataFrame.FromColumns(new
        {
            start_col = new[] { start1, start2 },
            end_col = new[] { end1, end2 }
        });

        // Act

        using var result = df.Select(
            Pl.DatetimeRanges("start_col", "end_col", interval: TimeSpan.FromDays(1)).Alias("ranges")
        );

        // Assert
        var listSeries = result["ranges"];
        
        Assert.Equal(DataType.List(DataType.Datetime(TimeUnit.Microseconds)), listSeries.DataType);
        Assert.Equal(2, listSeries.Length); 

        using var exploded = result.Explode("ranges");
        var flatSeries = exploded["ranges"];
  
        Assert.Equal(5, flatSeries.Length);

        Assert.Equal(start1, (DateTime)flatSeries[0]);
        Assert.Equal(start1.AddDays(1), (DateTime)flatSeries[1]);
        Assert.Equal(start1.AddDays(2), (DateTime)flatSeries[2]);
        
        Assert.Equal(start2, (DateTime)flatSeries[3]);
        Assert.Equal(start2.AddDays(1), (DateTime)flatSeries[4]);
    }

    [Fact]
    [Trait("Range","DatetimeRanges")]
    public void DatetimeRangesAsSeries_WithTimeZone_ShouldMapToDateTimeOffset()
    {
        // Arrange
        var start = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        var end = new DateTime(2026, 1, 2, 0, 0, 0, DateTimeKind.Utc);

        // Act
        using var listSeries = Pl.DatetimeRangesAsSeries(
            start, 
            end, 
            interval: "1d",
            unit: TimeUnit.Milliseconds, 
            timeZone: "UTC"
        );

        // Assert
        Assert.Equal(DataType.List(DataType.Datetime(TimeUnit.Milliseconds,"UTC")), listSeries.DataType);

        var flatSeries = listSeries.Explode();

        Assert.Equal(2, flatSeries.Length);
        
        Assert.Equal(new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero), (DateTimeOffset)flatSeries[0]);
        Assert.Equal(new DateTimeOffset(2026, 1, 2, 0, 0, 0, TimeSpan.Zero), (DateTimeOffset)flatSeries[1]);
    }
    [Fact]
    [Trait("Range", "TimeRange_Defaults")]
    public void TimeRangeAsSeries_DefaultBounds_ShouldCoverFullDay()
    {
        using var series = Pl.TimeRangeAsSeries();

        // Assert
        Assert.Equal(typeof(TimeOnly), series.DataType);
        Assert.Equal(24, series.Length);

        Assert.Equal(TimeOnly.MinValue, (TimeOnly)series[0]); // 00:00
        Assert.Equal(new TimeOnly(1, 0), (TimeOnly)series[1]);  // 01:00
        Assert.Equal(new TimeOnly(23, 0), (TimeOnly)series[23]); // 23:00
    }

    [Fact]
    [Trait("Range", "TimeRangeExplicit")]
    public void TimeRangeAsSeries_ExplicitBounds_WithTimeSpanInterval()
    {
        // Arrange
        var start = new TimeOnly(10, 0);
        var end = new TimeOnly(11, 30);

        using var series = Pl.TimeRangeAsSeries(
            start, 
            end, 
            interval: TimeSpan.FromMinutes(30)
        );

        // Assert
        Assert.Equal(4, series.Length); 
        
        // 10:00, 10:30, 11:00, 11:30
        Assert.Equal(new TimeOnly(10, 0), (TimeOnly)series[0]);
        Assert.Equal(new TimeOnly(10, 30), (TimeOnly)series[1]);
        Assert.Equal(new TimeOnly(11, 0), (TimeOnly)series[2]);
        Assert.Equal(new TimeOnly(11, 30), (TimeOnly)series[3]);
    }

    [Fact]
    [Trait("Range", "TimeRangesRowWise")]
    public void TimeRanges_RowWise_ShouldGenerateLists()
    {
        // Arrange
        var start1 = new TimeOnly(8, 0);
        var end1 = new TimeOnly(10, 0);
        
        var start2 = new TimeOnly(20, 0);
        var end2 = new TimeOnly(21, 0);

        var df = DataFrame.FromColumns(new
        {
            start_col = new[] { start1, start2 },
            end_col = new[] { end1, end2 }
        });

        // Act
        using var result = df.Select(
            Pl.TimeRanges("start_col", "end_col", interval: "1h").Alias("ranges")
        );

        // Assert
        var listSeries = result["ranges"];
        Assert.Equal(DataType.List(typeof(TimeOnly)), listSeries.DataType);
        Assert.Equal(2, listSeries.Length);

        using var exploded = result.Explode("ranges");
        var flatSeries = exploded["ranges"];

        Assert.Equal(5, flatSeries.Length);

        Assert.Equal(new TimeOnly(8, 0), (TimeOnly)flatSeries[0]);
        Assert.Equal(new TimeOnly(9, 0), (TimeOnly)flatSeries[1]);
        Assert.Equal(new TimeOnly(10, 0), (TimeOnly)flatSeries[2]);
        
        Assert.Equal(new TimeOnly(20, 0), (TimeOnly)flatSeries[3]);
        Assert.Equal(new TimeOnly(21, 0), (TimeOnly)flatSeries[4]);
    }
    [Fact]
    [Trait("Range", "LinearSpaceDefault")]
    public void LinearSpaceAsSeries_DefaultBothClosed_ShouldIncludeEndpoints()
    {
        // Arrange & Act
        using var series = Pl.LinearSpaceAsSeries(0.0, 10.0, 5);

        // Assert
        Assert.Equal(DataType.Float64, series.DataType);
        Assert.Equal(5, series.Length);

        Assert.Equal(0.0, series[0]);
        Assert.Equal(2.5, series[1]);
        Assert.Equal(5.0, series[2]);
        Assert.Equal(7.5, series[3]);
        Assert.Equal(10.0, series[4]);
    }

    [Fact]
    [Trait("Range", "LinearSpaceOpen")]
    public void LinearSpaceAsSeries_OpenInterval_ShouldExcludeEndpoints()
    {
        // Arrange & Act
        using var series = Pl.LinearSpaceAsSeries(
            0.0, 
            12.0, 
            5, 
            closed: ClosedInterval.None
        );

        Assert.Equal(5, series.Length);
        
        Assert.Equal(2.0, series[0]);
        Assert.Equal(4.0, series[1]);
        Assert.Equal(6.0, series[2]);
        Assert.Equal(8.0, series[3]);
        Assert.Equal(10.0, series[4]);
    }

    [Fact]
    [Trait("Range", "LinearSpacesRowWise")]
    public void LinearSpaces_RowWise_ShouldGenerateLists()
    {
        var df = DataFrame.FromColumns(new
        {
            start_col = new double[] { 0, 10 },
            end_col = new double[] { 10, 20 }
        });

        using var result = df.Select(
            Pl.LinearSpaces("start_col", "end_col", 3).Alias("spaces")
        );

        var listSeries = result["spaces"];
        
        Assert.Equal(DataType.List(typeof(double)), listSeries.DataType);

        using var exploded = result.Explode("spaces");
        var flatArr = exploded["spaces"];

        Assert.Equal(0.0, flatArr[0]);
        Assert.Equal(5.0, flatArr[1]);
        Assert.Equal(10.0, flatArr[2]);
        
        Assert.Equal(10.0, flatArr[3]);
        Assert.Equal(15.0, flatArr[4]);
        Assert.Equal(20.0, flatArr[5]);
    }

    [Fact]
    [Trait("Range", "LinearSpacesAsArray")]
    public void LinearSpaces_WithAsArrayTrue_ShouldGenerateFixedSizeArrays()
    {
        var df = DataFrame.FromColumns(new
        {
            start_col = new double[] { 0, 10 },
            end_col = new double[] { 10, 20 }
        });

        // Act
        using var result = df.Select(
            Pl.LinearSpaces("start_col", "end_col", numSamples: 3, asArray: true).Alias("spaces")
        );

        Assert.Equal(DataType.Array(typeof(double),3), result["spaces"].DataType);
        Assert.Equal(2, result.Height);

        using var exploded = result.Explode(Cs.Array());
        Assert.Equal(6, exploded.Height);
    }
    [Fact]
    [Trait("Range", "LinearSpaceDateOnly")]
    public void LinearSpaceAsSeries_WithDateOnly_ShouldPromoteToDatetime()
    {
        // Arrange
        var start = new DateOnly(2026, 1, 1);
        var end = new DateOnly(2026, 1, 2);

        // Act
        using var series = Pl.LinearSpaceAsSeries(start, end, numSamples: 3);

        // Assert
        Assert.Equal(DataType.Datetime(TimeUnit.Microseconds), series.DataType);
        Assert.Equal(3, series.Length);

        Assert.Equal(new DateTime(2026, 1, 1, 0, 0, 0),series[0]);
        Assert.Equal(new DateTime(2026, 1, 1, 12, 0, 0),series[1]);
        Assert.Equal(new DateTime(2026, 1, 2, 0, 0, 0), series[2]);
    }

    [Fact]
    [Trait("Range", "LinearSpaceDateTime")]
    public void LinearSpaceAsSeries_WithDateTime_ShouldGenerateEquallySpacedTime()
    {
        var start = new DateTime(2026, 1, 1, 0, 0, 0);
        var end = new DateTime(2026, 1, 1, 2, 0, 0);

        using var series = Pl.LinearSpaceAsSeries(start, end, numSamples: 5);

        // Assert
        Assert.Equal(DataType.Datetime(TimeUnit.Microseconds), series.DataType);
        Assert.Equal(5, series.Length);

        Assert.Equal(new DateTime(2026, 1, 1, 0, 0, 0), series[0]);
        Assert.Equal(new DateTime(2026, 1, 1, 0, 30, 0), series[1]);
        Assert.Equal(new DateTime(2026, 1, 1, 1, 0, 0), series[2]);
        Assert.Equal(new DateTime(2026, 1, 1, 1, 30, 0), series[3]);
        Assert.Equal(new DateTime(2026, 1, 1, 2, 0, 0), series[4]);
    }
    
}
