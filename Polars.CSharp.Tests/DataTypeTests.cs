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
    [Fact]
    public void Test_DateTime_WallClock_Consistency()
    {
        // 场景：用户输入了 "2025-01-01 12:00:00"
        // 无论这个 DateTime 对象的 .Kind 是什么（Local/Utc/Unspecified）
        // 我们都视为用户只想存 "12:00:00" 这个墙上时间。
        
        var dtLocal = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Local);
        var dtUtc   = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var dtUnspec= new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Unspecified);

        // 存入 Series
        using var s = Series.From("dates", new[] { dtLocal, dtUtc, dtUnspec });

        // 读取回来 (使用 ToArray<DateTime>)
        var results = s.ToArray<DateTime>();

        // 验证
        // 允许 10us (100 ticks) 的微秒截断误差
        long tolerance = 100; 

        // 1. 验证字面量（Ticks）一致性
        // 输入是 12:00，读出来必须还是 12:00 的 Ticks
        long expectedLiteralTicks = new DateTime(2025, 1, 1, 12, 0, 0).Ticks;

        Assert.InRange(results[0].Ticks - expectedLiteralTicks, -tolerance, tolerance);
        Assert.InRange(results[1].Ticks - expectedLiteralTicks, -tolerance, tolerance);
        Assert.InRange(results[2].Ticks - expectedLiteralTicks, -tolerance, tolerance);

        // 2. 验证 .Kind 被重置为 Unspecified (或者是 Utc，取决于你 ConvertTimestamp 最后的 return 策略)
        // 按照我们在 ArrowExtensions 里的最新修改 (EpochUtc.AddTicks)，ArrowReader 读出来的是 UTC。
        // *如果你之前的 ConvertTimestamp 按照我建议的改成了返回 UTC，这里就 Assert Utc*
        // *如果你改成了返回 Unspecified，这里就 Assert Unspecified*
        
        // 假设你用了我推荐的 EpochUtc.AddTicks(ticks) 方案，读回来默认是 UTC
        Assert.Equal(DateTimeKind.Unspecified, results[0].Kind); 
        Assert.Equal(DateTimeKind.Unspecified, results[1].Kind);
        Assert.Equal(DateTimeKind.Unspecified, results[2].Kind);
    }
    [Fact]
    public void Test_DateTimeOffset_Absolute_Consistency()
    {
        // 1. 准备数据
        // 北京时间 12:00 (+08:00) 
        // 绝对时间是 UTC 04:00
        var offsetNow = new DateTimeOffset(2025, 1, 1, 12, 0, 0, TimeSpan.FromHours(8));
        
        // 存入 Series
        using var s = Series.From("offsets", new[] { offsetNow });

        // 读取回来
        var results = s.ToArray<DateTimeOffset>();

        // 验证
        long tolerance = 100;

        // 1. 读回来必须是 UTC (Offset 为 0)
        // 因为 Arrow 内部归一化存储了
        Assert.Equal(TimeSpan.Zero, results[0].Offset);

        // 2. 绝对时间点 (UtcTicks) 必须相等
        // 输入的 12:00+8 等于 UTC 的 04:00
        // 读出来的 04:00+0 等于 UTC 的 04:00
        Assert.InRange(results[0].UtcTicks - offsetNow.UtcTicks, -tolerance, tolerance);
        
        // 3. 验证字面量变化
        // 输入是 12点，读出来应该是 4点
        Assert.Equal(4, results[0].Hour);
    }
    [Fact]
    public void Test_WallClock_Consistency()
    {
        // 场景：用户从 CSV 读了一行 "2025-01-01 12:00:00"
        // 用户的机器可能是 +8，也可能是 -5，但他只在乎 "12:00" 这个点
        
        var dtLocal = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Local);
        var dtUtc   = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var dtUnspec= new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Unspecified);

        // 存入 Series
        // 我们期望 Polars 内部把它们都当做 "2025-01-01 12:00:00"
        using var df = DataFrame.From(new [] 
        { 
            new { A = dtLocal, B = dtUtc, C = dtUnspec } 
        });

        // 验证 2: 读取回来
        // 无论是 Local, Utc 还是 Unspecified，只要字面量是 12点，读回来就是 12点
        var row = df.Rows<dynamic>().First(); // 或者用具体的 POCO
        
        // 必须严格相等 (Ticks 差值由微秒精度决定，但在秒级必须一致)
        DateTime valA = df.GetValue<DateTime>(0, "A");
        DateTime valB = df.GetValue<DateTime>(0, "B");
        DateTime valC = df.GetValue<DateTime>(0, "C");

        // 允许 10us 误差
        long tolerance = 100; 

        // 验证 Wall Clock 一致性
        // 输入 12:00 -> 输出 12:00 (而不是转成了 UTC 的 04:00)
        Assert.InRange(valA.Ticks - new DateTime(2025, 1, 1, 12, 0, 0).Ticks, -tolerance, tolerance);
        
        // 验证 Kind 被抹除为 Unspecified
        Assert.Equal(DateTimeKind.Unspecified, valA.Kind);
        Assert.Equal(DateTimeKind.Unspecified, valB.Kind);
        
        // 验证 A, B, C 在 Polars 里是相等的
        Assert.InRange(valA.Ticks - valB.Ticks, -tolerance, tolerance);
        Assert.InRange(valA.Ticks - valC.Ticks, -tolerance, tolerance);
    }

    [Fact]
    public void Test_GetValue_Complex()
    {
        // 1. 准备 Struct 数据
        var data = new List<ComplexContainer>
        {
            new() { Id = 1, Info = new NestedItem { Key = "K1" } },
            new() { Id = 2, Info = new NestedItem { Key = "K2" } }
        };
        using var s = Series.From("data", data); // Struct Series

        // 2. 直接 GetValue<T> (Struct)
        var item1 = s.GetValue<ComplexContainer>(0);
        Assert.Equal("K1", item1.Info.Key);

        var item2 = s.GetValue<ComplexContainer>(1);
        Assert.Equal("K2", item2.Info.Key);
    }
    
    [Fact]
    public void Test_GetValue_List()
    {
        // 1. 准备 List 数据
        var data = new List<List<int>>
        {
            new() { 1, 2 },
            new() { 3 }
        };
        using var s = Series.From("list", data);

        // 2. 直接 GetValue<List<int>>
        var list0 = s.GetValue<List<int>>(0);
        Assert.Equal(2, list0.Count);
        Assert.Equal(2, list0[1]);
    }
    
}