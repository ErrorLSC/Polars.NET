using Pl = Polars.CSharp.Polars;

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
        var expr = Pl.IntRange(0, 3, dtype: DataType.Int32);
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
        var series = Pl.DateRangeAsSeries(new DateOnly(2026, 1, 1),new DateOnly(2026, 1, 5));

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
}