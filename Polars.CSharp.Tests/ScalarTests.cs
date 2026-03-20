using static Polars.CSharp.Polars;
namespace Polars.CSharp.Tests;

public class ScalarTests
{
    [Fact]
    public void Test_Direct_Scalar_Access_All_Types_Pro_Max()
    {
        var now = DateTime.UtcNow;
        now = new DateTime(now.Ticks - (now.Ticks % 10), DateTimeKind.Utc); 
        
        var date = DateOnly.FromDateTime(now);
        var time = new TimeOnly(12, 30, 0, 100); // 12:30:00.100
        var duration = TimeSpan.FromHours(1.5) + TimeSpan.FromMicroseconds(50); 

        using var df = DataFrame.FromSeries([
            new Series("i", [100]),
            new Series("f", [1.23]),
            new Series("s", ["hello"]),
            new Series("b", [true]),
            new Series("d", [123.456m]),
            new Series("dt", [now]),
            new Series("date", [date]),
            new Series("time", [time]),
            new Series("dur", [duration])
        ]);

        // --- Primitives ---
        Assert.Equal(100, df.GetValue<int>(0, "i"));
        Assert.Equal(1.23, df.GetValue<double>(0, "f"));
        Assert.Equal("hello", df.GetValue<string>(0, "s"));
        Assert.True(df.GetValue<bool>(0, "b"));
        
        // --- Decimal ---
        Assert.Equal(123.456m, df.GetValue<decimal>(0, "d"));
        
        // --- DateTime (Naive ticks check) ---
        var dtOut = df.GetValue<DateTime>(0, "dt");
        Assert.Equal(now.Ticks, dtOut.Ticks);
        Assert.Equal(DateTimeKind.Unspecified, dtOut.Kind);
        
        // --- DateOnly ---
        Assert.Equal(date, df.GetValue<DateOnly>(0, "date"));
        
        // --- TimeOnly ---
        Assert.Equal(time, df.GetValue<TimeOnly>(0, "time"));
        
        // --- Duration (TimeSpan) ---
        Assert.Equal(duration, df.GetValue<TimeSpan>(0, "dur"));

        Assert.IsType<DateTime>(df[0, "dt"]); 
        Assert.IsType<bool>(df[0,"b"]);
        Assert.IsType<string>(df[0,"s"]);
        Assert.IsType<TimeSpan>(df[0, "dur"]);
        Assert.IsType<DateOnly>(df[0, "date"]);
        Assert.IsType<TimeOnly>(df[0, "time"]);
        Assert.IsType<decimal>(df[0, "d"]);
    }
    [Fact]
    public void Test_SyntaxSugar_Indexer()
    {
        var df = DataFrame.FromColumns(new 
        {
            Id = new[] { 1, 2, 3 },
            Name = new[] { "Alice", "Bob", "Charlie" },
            Score = new[] { 99.5, 88.0, 77.5 }
        });

        Assert.Equal("Alice", df.GetValue<string>(0, "Name"));

        // [RowIndex, ColumnName]
        Assert.Equal("Alice", df[0, "Name"]); 
        
        // [RowIndex, ColumnIndex]
        Assert.Equal(1, df[0, 0]); // Id

        // df["Name"][0]
        Assert.Equal("Bob", df["Name"][1]);

        int id = (int)df[0, "Id"]!; 
        Assert.Equal(1, id);

        double score = (double)df[2, "Score"]!;
        Assert.Equal(77.5, score);
    }
    [Fact]
    public void Test_Datetime_TimeZone_Cast()
    {
        var df = DataFrame.FromColumns(new 
        {
            Ts = new[] { DateTime.Parse("2024-01-01 12:00:00") } 
        });

        var targetType = DataType.Datetime(TimeUnit.Milliseconds, "Asia/Tokyo");

        var res = df.Select(
            Col("Ts")
                .Cast(targetType) 
                .Alias("Ts_Tokyo")
        );

        Assert.Equal(targetType ,res.Schema["Ts_Tokyo"]); 
    }
}