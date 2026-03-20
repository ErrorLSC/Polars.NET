using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class TimeSeriesTests
{
    [Fact]
    public void Test_GroupByDynamic_Basic_TimeSpan()
    {
        // 10:00, 10:10, 10:20, 10:30, 10:40, 10:50
        var start = new DateTime(2024, 1, 1, 10, 0, 0);
        var dates = Enumerable.Range(0, 6).Select(i => start.AddMinutes(i * 10)).ToArray();
        var values = Enumerable.Range(0, 6).Select(i => i).ToArray(); // 0, 1, 2, 3, 4, 5

        var df = DataFrame.FromColumns(new {Time =dates,Val =values}); 

        // Group 1 [10:00, 10:30): 10:00(0), 10:10(1), 10:20(2) -> Sum = 3
        // Group 2 [10:30, 11:00): 10:30(3), 10:40(4), 10:50(5) -> Sum = 12
        var q = df.Lazy()
            .GroupByDynamic(
                indexColumn: "Time",
                every: TimeSpan.FromMinutes(30),
                closedWindow: ClosedWindow.Left // [ )
            )
            .Agg(
                Col("Val").Sum().Alias("SumVal"),
                Col("Val").Count().Alias("Count")
            );

        using var res = q.Collect();

        Assert.Equal(2, res.Height);
        
        Assert.Equal(3, res.GetValue<int>(0, "SumVal"));
        Assert.Equal(3, res.GetValue<int>(0, "Count"));
        
        Assert.Equal(12, res.GetValue<int>(1, "SumVal"));
        Assert.Equal(3, res.GetValue<int>(1, "Count"));
    }
    [Fact]
    public void Test_GroupByDynamic_Advanced_Rolling()
    {
        var start = new DateTime(2024, 1, 1, 10, 0, 0);
        var dates = Enumerable.Range(0, 10).Select(i => start.AddMinutes(i)).ToArray();
        var values = Enumerable.Range(0, 10).Select(i => 1).ToArray();

        var df = DataFrame.FromColumns(new {Time = dates,Val = values});

        // Every: 5m 
        // Period: 10m 
        // Label: Right
        // IncludeBoundaries: True
        var res = df
            .GroupByDynamic(
                indexColumn: "Time",
                every: TimeSpan.FromMinutes(5),  
                period: TimeSpan.FromMinutes(10),
                label: Label.Right,             
                includeBoundaries: true,         
                closedWindow: ClosedWindow.Left  
            )
            .Agg(
                Col("Val").Count().Alias("Count")
            );

        // Window 1: [09:55, 10:05) -> Label 10:05. 
        // Window 2: [10:00, 10:10) -> Label 10:10. 
        
        Assert.Contains("_lower_boundary", res.ColumnNames);
        Assert.Contains("_upper_boundary", res.ColumnNames);
        
        var firstTime = res.GetValue<DateTime>(0, "Time"); 
        Assert.True(res.Height > 0);
    }
    [Fact]
    public void Test_GroupByDynamic_Nanoseconds_Columnar()
    {
        var start = new DateTime(2024, 1, 1).Ticks;
        var dates = Enumerable.Range(0, 100)
            .Select(i => new DateTime(start + i)) 
            .ToArray();
        
        using var df = DataFrame.FromColumns(new { 
            Ts = dates,       // DateTime[]
            Val = dates       // DateTime[]
        });

        var us1 = TimeSpan.FromTicks(10); 

        using var res = df
            .GroupByDynamic(
                indexColumn: "Ts",
                every: us1
            )
            .Agg(
                Col("Val").Count().Alias("Count")
            );

        Assert.Equal(10, res.Height);
        
        Assert.Equal(10, res.GetValue<int>(0, "Count"));
    }
    [Fact]
    public void TestDtCombine()
    {
        var df = DataFrame.FromColumns(new
        {
            date = new[] { new DateOnly(2024, 1, 1), new DateOnly(2024, 12, 31) },
            time = new[] { new TimeOnly(10, 30, 0), new TimeOnly(23, 59, 59, 123) } // 123ms
        });

        // Note: Only sub-second units (Nanoseconds, Microseconds, Milliseconds) are supported here.
        var res = df.Select(
            Col("date").Dt.Combine(Col("time"), TimeUnit.Milliseconds).Alias("dt_ms"),
            Col("date").Dt.Combine(Col("time"), TimeUnit.Microseconds).Alias("dt_us")
        );

        Assert.Equal(new DateTime(2024, 1, 1, 10, 30, 0), res["dt_ms"][0]);
        Assert.Equal(new DateTime(2024, 1, 1, 10, 30, 0),res["dt_us"][0]);

        Assert.Equal(new DateTime(2024, 12, 31, 23, 59, 59, 123),res["dt_ms"][1]); 
        Assert.Equal(new DateTime(2024, 12, 31, 23, 59, 59, 123), res["dt_us"][1]);

        Assert.Equal(DataTypeKind.Datetime, res.Schema["dt_ms"].Kind);
        Assert.Equal(DataTypeKind.Datetime, res.Schema["dt_us"].Kind);
    }
}