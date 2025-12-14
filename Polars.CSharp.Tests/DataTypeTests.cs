using static Polars.CSharp.Polars;
namespace Polars.CSharp.Tests;

public class DataTypeTests
{
    public class TradeRecord
    {
        public string Ticker { get; set; }
        public int Qty { get; set; }        // C# int <-> Polars Int64
        public decimal Price { get; set; }  // C# decimal <-> Polars Decimal(18,2)
        public double? Factor { get; set; } // C# double <-> Polars Float64
        public float Risk { get; set; }     // C# float <-> Polars Float64 (downcast)
    }

    [Fact]
    public void Test_DataFrame_RoundTrip_POCO()
    {
        // 1. 原始数据
        var trades = new List<TradeRecord>
        {
            new() { Ticker = "AAPL", Qty = 100, Price = 150.50m, Factor = 1.1, Risk = 0.5f },
            new() { Ticker = "GOOG", Qty = 50,  Price = 2800.00m, Factor = null, Risk = 0.1f },
            new() { Ticker = "MSFT", Qty = 200, Price = 300.25m, Factor = 0.95, Risk = 0.2f }
        };

        // 2. From: List -> DataFrame
        using var df = DataFrame.From(trades);
        
        Assert.Equal(3, df.Height);
        
        // 3. To: DataFrame -> List (Rows<T>)
        var resultList = df.Rows<TradeRecord>().ToList();

        Assert.Equal(3, resultList.Count);

        // 4. 验证数据
        var row0 = resultList[0];
        Assert.Equal("AAPL", row0.Ticker);
        Assert.Equal(100, row0.Qty);
        Assert.Equal(150.50m, row0.Price);
        Assert.Equal(1.1, row0.Factor);
        Assert.Equal(0.5f, row0.Risk);

        var row1 = resultList[1];
        Assert.Equal("GOOG", row1.Ticker);
        Assert.Null(row1.Factor); // 验证 Null 透传
    }
    public class LogEntry
    {
        public int Id { get; set; }
        public string Message { get; set; }
        public DateTime Timestamp { get; set; } // 非空
        public DateTime? ProcessedAt { get; set; } // 可空
    }

    [Fact]
    public void Test_DataFrame_DateTime_RoundTrip()
    {
        var now = DateTime.Now;
        // 去掉 Tick 级精度差异，因为 Microseconds 会丢失 100ns (Ticks) 的精度
        // 我们把精度截断到秒或毫秒来做测试，或者容忍微小误差
        now = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second);

        var logs = new[]
        {
            new LogEntry { Id = 1, Message = "Start", Timestamp = now, ProcessedAt = null },
            new LogEntry { Id = 2, Message = "End", Timestamp = now.AddMinutes(1), ProcessedAt = now.AddMinutes(2) }
        };

        // 1. From (C# -> Polars)
        using var df = DataFrame.From(logs);
        
        Assert.Equal(2, df.Height);

        // 2. To (Polars -> C#)
        var result = df.Rows<LogEntry>().ToList();

        // 3. 验证
        var row1 = result[0];
        Assert.Equal(1, row1.Id);
        Assert.Equal(now, row1.Timestamp);
        Assert.Null(row1.ProcessedAt);

        var row2 = result[1];
        Assert.Equal(now.AddMinutes(1), row2.Timestamp);
        Assert.Equal(now.AddMinutes(2), row2.ProcessedAt);
    }
    private class NestedItem
    {
        public string Key { get; set; }
        public List<double> Values { get; set; }
    }

    private class ComplexContainer
    {
        public int Id { get; set; }
        public NestedItem Info { get; set; } // Struct
    }

    [Fact]
    public void Test_DataFrame_RoundTrip_ComplexStruct()
    {
        // 1. 准备数据
        var data = new List<ComplexContainer>
        {
            new() { 
                Id = 1, 
                Info = new NestedItem { Key = "A", Values = new List<double> { 1.1, 2.2 } } 
            },
            new() { 
                Id = 2, 
                Info = null // Struct Null
            },
            new() { 
                Id = 3, 
                Info = new NestedItem { Key = "B", Values = new List<double> { 3.3 } } 
            }
        };

        // 2. POCO -> DataFrame (Series.From + DataFrame)
        // 这里用到了我们之前的 ArrowConverter + ArrowFfiBridge
        using var s = Series.From("data", data); 
        using var df = new DataFrame(s).Unnest("data"); // 炸开成 Id, Info

        // Expected:
        // Id (i64), Info (Struct)
        
        // 3. DataFrame -> POCO (Rows<T>)
        // 这里用到刚写的 ArrowReader 递归逻辑
        var results = df.Rows<ComplexContainer>().ToList();

        // 4. 验证
        Assert.Equal(3, results.Count);
        
        // Row 0
        Assert.Equal(1, results[0].Id);
        Assert.Equal("A", results[0].Info.Key);
        Assert.Equal(2, results[0].Info.Values.Count);
        Assert.Equal(2.2, results[0].Info.Values[1]);

        // Row 1 (Struct Null)
        Assert.Equal(2, results[1].Id);
        Assert.Null(results[1].Info); // 完美还原 null

        // Row 2
        Assert.Equal("B", results[2].Info.Key);
        Assert.Single(results[2].Info.Values);
    }
    private class ModernTypesPoco
    {
        public string Cat { get; set; } // Polars 里是 cat，C# 里读成 string
        public DateOnly Date { get; set; }
        public TimeOnly Time { get; set; }
    }

    [Fact]
    public void Test_DataFrame_ModernTypes_And_Categorical()
    {
        // 1. 写入测试 (DateOnly / TimeOnly)
        var data = new List<ModernTypesPoco>
        {
            new() { 
                Cat = "A", 
                Date = new DateOnly(2023, 1, 1), 
                Time = new TimeOnly(12, 0, 0) 
            },
            new() { 
                Cat = "B", 
                Date = new DateOnly(2024, 2, 29), 
                Time = new TimeOnly(23, 59, 59) 
            }
        };

        using var s = Series.From("modern", data);
        using var df = new DataFrame(s).Unnest("modern");

        // 2. 模拟 Categorical
        // 目前我们写入的是 String，我们在 Polars 端强转为 Categorical
        // 这样可以测试读取 DictionaryArray 的逻辑
        using var dfCat = df.WithColumns(Col("Cat").Cast(DataType.Categorical));

        // Schema 检查
        Assert.Equal(DataTypeKind.Categorical, dfCat.Schema["Cat"].Kind);
        Assert.Equal(DataTypeKind.Date, dfCat.Schema["Date"].Kind);
        Assert.Equal(DataTypeKind.Time, dfCat.Schema["Time"].Kind);

        // 3. 读取测试 (Round Trip)
        var rows = dfCat.Rows<ModernTypesPoco>().ToList();

        Assert.Equal(2, rows.Count);
        
        // 验证 Categorical -> String 读取
        Assert.Equal("A", rows[0].Cat);
        Assert.Equal("B", rows[1].Cat);

        // 验证 DateOnly
        Assert.Equal(new DateOnly(2023, 1, 1), rows[0].Date);

        // 验证 TimeOnly
        Assert.Equal(new TimeOnly(12, 0, 0), rows[0].Time);
    }
    private class TimeFamily
    {
        public DateOnly Date { get; set; }
        public TimeOnly Time { get; set; }
        public DateTime Stamp { get; set; }
        public TimeSpan Duration { get; set; } // 新兄弟
    }

    [Fact]
    public void Test_TimeFamily_Reunion()
    {
        // 1. 准备数据
        var data = new List<TimeFamily>
        {
            new() {
                Date = new DateOnly(2025, 1, 1),
                Time = new TimeOnly(14, 30, 0),
                Stamp = new DateTime(2025, 1, 1, 14, 30, 0),
                Duration = TimeSpan.FromHours(1.5) + TimeSpan.FromMicroseconds(50) // 1.5小时 + 50微秒
            },
            new() {
                Date = new DateOnly(1999, 12, 31),
                Time = new TimeOnly(23, 59, 59),
                Stamp = DateTime.UnixEpoch,
                Duration = TimeSpan.FromDays(365) // 1年
            }
        };

        // 2. 写入 Polars (ArrowConverter 生效)
        using var s = Series.From("times", data);
        using var df = new DataFrame(s).Unnest("times");

        Console.WriteLine(df);
        // 检查 Schema，Duration 应该被识别
        Assert.Equal(DataTypeKind.Duration, df.Schema["Duration"].Kind);

        // 3. 读取 Polars (ArrowReader + ArrowExtensions 生效)
        var rows = df.Rows<TimeFamily>().ToList();

        // 4. 验证 Duration
        // Row 0
        Assert.Equal(TimeSpan.FromHours(1.5) + TimeSpan.FromMicroseconds(50), rows[0].Duration);
        
        // Row 1
        Assert.Equal(TimeSpan.FromDays(365), rows[1].Duration);

        // 顺手验证其他兄弟
        Assert.Equal(new DateOnly(2025, 1, 1), rows[0].Date);
        Assert.Equal(new TimeOnly(14, 30, 0), rows[0].Time);
    }
}