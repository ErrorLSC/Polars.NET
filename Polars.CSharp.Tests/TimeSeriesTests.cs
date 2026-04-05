using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp.Tests;

public class TimeSeriesTests
{
    [Fact]
    [Trait("TimeSeries","DynamicGroupBy")]
    public void Test_GroupByDynamic_Basic_TimeSpan()
    {
        // 10:00, 10:10, 10:20, 10:30, 10:40, 10:50
        var start = new DateTime(2024, 1, 1, 10, 0, 0);
        var end = new DateTime(2024, 1, 1, 10, 50, 0); 
        var values = new[] { 0, 1, 2, 3, 4, 5 }; 

        var df = DataFrame.FromColumns(new { Val = values })
            .WithColumns(
                Pl.DatetimeRange(start, end, "10m").Alias("Time")
            );

        // Group 1 [10:00, 10:30): 10:00(0), 10:10(1), 10:20(2) -> Sum = 3
        // Group 2 [10:30, 11:00): 10:30(3), 10:40(4), 10:50(5) -> Sum = 12
        var res = df
            .GroupByDynamic(
                indexColumn: Cs.Temporal(),
                every: TimeSpan.FromMinutes(30),
                closedWindow: ClosedWindow.Left // [ )
            )
            .Agg(
                Pl.Col("Val").Sum().Alias("SumVal"),
                Pl.Col("Val").Count().Alias("Count")
            );

        Assert.Equal(2, res.Height);
        
        Assert.Equal(3, res.GetValue<int>(0, "SumVal"));
        Assert.Equal(3, res.GetValue<int>(0, "Count"));
        
        Assert.Equal(12, res.GetValue<int>(1, "SumVal"));
        Assert.Equal(3, res.GetValue<int>(1, "Count"));
    }
    [Fact]
    [Trait("TimeSeries","DynamicGroupBy")]
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
                indexColumn: Cs.Datetime(),
                every: TimeSpan.FromMinutes(5),  
                period: TimeSpan.FromMinutes(10),
                label: Label.Right,             
                includeBoundaries: true,         
                closedWindow: ClosedWindow.Left  
            )
            .Agg(
                Pl.Col("Val").Count().Alias("Count")
            );

        // Window 1: [09:55, 10:05) -> Label 10:05. 
        // Window 2: [10:00, 10:10) -> Label 10:10. 
        
        Assert.Contains("_lower_boundary", res.ColumnNames);
        Assert.Contains("_upper_boundary", res.ColumnNames);
        
        var firstTime = res.GetValue<DateTime>(0, "Time"); 
        Assert.True(res.Height > 0);
    }
    [Fact]
    [Trait("TimeSeries", "DynamicGroupBy")]
    public void Test_GroupByDynamic_Having_And_MultipleAggregations()
    {
        var start = new DateTime(2024, 1, 1, 10, 0, 0);
        var end = new DateTime(2024, 1, 1, 11, 20, 0); 
        
        // Group 1 [10:00, 10:30): 10, 15, 5 -> Sum: 30, Max: 15, Min: 5
        // Group 2 [10:30, 11:00):  2,  3, 4 -> Sum: 9,  Max: 4,  Min: 2  
        // Group 3 [11:00, 11:30): 20, 25, 5 -> Sum: 50, Max: 25, Min: 5
        int[] values = [10, 15, 5, 2, 3, 4, 20, 25, 5];

        var df = DataFrame.FromColumns(new { Val = values })
            .WithColumns(
                Pl.DatetimeRange(start, end, TimeSpan.FromMinutes(10)).Alias("Time")
            );

        var res = df
            .GroupByDynamic(
                indexColumn: "Time",
                every: TimeSpan.FromMinutes(30),
                closedWindow: ClosedWindow.Left
            )
            .Having(Pl.Col("Val").Sum() > 20) 
            .Agg(
                Pl.Col("Val").Sum().Alias("SumVal"),
                Pl.Col("Val").Max().Alias("MaxVal"),
                Pl.Col("Val").Min().Alias("MinVal")
            );

        Assert.Equal(2, res.Height);

        Assert.Equal(30, res.GetValue<int>(0, "SumVal"));
        Assert.Equal(15, res.GetValue<int>(0, "MaxVal"));
        Assert.Equal(5, res.GetValue<int>(0, "MinVal"));
        
        Assert.Equal(50, res.GetValue<int>(1, "SumVal"));
        Assert.Equal(25, res.GetValue<int>(1, "MaxVal"));
        Assert.Equal(5, res.GetValue<int>(1, "MinVal"));
    }

    [Fact]
    [Trait("TimeSeries", "DynamicGroupByHaving")]
    public void Test_GroupByDynamic_With_By_Column_And_Having()
    {
        var start = new DateTime(2024, 1, 1, 10, 0, 0);
        var dates = new[] 
        {
            start, start.AddMinutes(30), 
            start, start.AddMinutes(30) 
        };
        var symbols = new[] { "A", "A", "B", "B" };
        var values = new[] { 10, 20, 100, 200 };

        var df = DataFrame.FromColumns(new { Time = dates, Symbol = symbols, Val = values });

        var res = df
            .GroupByDynamic(
                indexColumn: "Time",
                every: "1h", 
                groupBy: [Pl.Col("Symbol")]
            )
            .Having(Pl.Col("Val").Mean() > 50) 
            .Agg(
                Pl.Col("Val").Mean().Alias("MeanVal"),
                Pl.Col("Val").First().Alias("FirstVal"),
                Pl.Col("Val").Last().Alias("LastVal")
            );
        Assert.Equal(1, res.Height);
        
        Assert.Equal("B", res.GetValue<string>(0, "Symbol"));
        Assert.Equal(150.0, res.GetValue<double>(0, "MeanVal")); 
        Assert.Equal(100, res.GetValue<int>(0, "FirstVal"));
        Assert.Equal(200, res.GetValue<int>(0, "LastVal"));
    }
    [Fact]
    [Trait("TimeSeries", "DynamicGroupBy")]
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
                Pl.Col("Val").Count().Alias("Count")
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
            Pl.Col("date").Dt.Combine(Pl.Col("time"), TimeUnit.Milliseconds).Alias("dt_ms"),
            Pl.Col("date").Dt.Combine(Pl.Col("time"), TimeUnit.Microseconds).Alias("dt_us")
        );

        Assert.Equal(new DateTime(2024, 1, 1, 10, 30, 0), res["dt_ms"][0]);
        Assert.Equal(new DateTime(2024, 1, 1, 10, 30, 0),res["dt_us"][0]);

        Assert.Equal(new DateTime(2024, 12, 31, 23, 59, 59, 123),res["dt_ms"][1]); 
        Assert.Equal(new DateTime(2024, 12, 31, 23, 59, 59, 123), res["dt_us"][1]);

        Assert.Equal(DataTypeKind.Datetime, res.Schema["dt_ms"].Kind);
        Assert.Equal(DataTypeKind.Datetime, res.Schema["dt_us"].Kind);
    }
    [Fact]
    [Trait("TimeSeries", "RollingBasic")]
    public void Test_GroupByRolling_Basic()
    {
        var start = new DateTime(2024, 1, 1, 10, 0, 0);
        var end = new DateTime(2024, 1, 1, 10, 40, 0);
        var values = new[] { 10, 20, 30, 40, 50 };

        var df = DataFrame.FromColumns(new { Val = values })
            .WithColumns(Pl.DatetimeRange(start, end, "10m").Alias("Time"));

        var res = df
            .Rolling(
                indexColumn: "Time",
                period: "20m",
                closedWindow: ClosedWindow.Both 
            )
            .Agg(
                Pl.Col("Val").Sum().Alias("SumVal"),
                Pl.Col("Val").Count().Alias("Count")
            );
        Assert.Equal(5, res.Height);

        Assert.Equal(10, res.GetValue<int>(0, "SumVal"));
        Assert.Equal(1, res.GetValue<int>(0, "Count"));

        Assert.Equal(30, res.GetValue<int>(1, "SumVal"));
        Assert.Equal(2, res.GetValue<int>(1, "Count"));

        Assert.Equal(60, res.GetValue<int>(2, "SumVal"));
        Assert.Equal(3, res.GetValue<int>(2, "Count"));
    }

    [Fact]
    [Trait("TimeSeries", "Rolling")]
    public void Test_GroupByRolling_Advanced_Selector_And_By()
    {
        var start = new DateTime(2024, 1, 1, 10, 0, 0);
        var dates = new[]
        {
            start, start.AddMinutes(10), start.AddMinutes(20), // Stock A
            start, start.AddMinutes(10), start.AddMinutes(20)  // Stock B
        };
        var symbols = new[] { "A", "A", "A", "B", "B", "B" };
        var values = new[] { 1, 2, 3, 100, 200, 300 };

        var df = DataFrame.FromColumns(new { Time = dates, Symbol = symbols, Val = values });

        var res = df
            .Rolling(
                indexColumn: Cs.Temporal(), 
                period: TimeSpan.FromMinutes(30),
                groupBy: ["Symbol"], 
                closedWindow: ClosedWindow.Right 
            )
            .Agg(
                Pl.Col("Val").Max().Alias("MaxVal")
            );

        Assert.Equal(6, res.Height);

        Assert.Equal("A", res.GetValue<string>(2, "Symbol"));
        Assert.Equal(3, res.GetValue<int>(2, "MaxVal"));

        Assert.Equal("B", res.GetValue<string>(4, "Symbol"));
        Assert.Equal(200, res.GetValue<int>(4, "MaxVal"));
    }
}