using System.Diagnostics;
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
    [Trait("DataType","DateTime")]
    public void Test_DataFrame_DateTime_RoundTrip()
    {
        var now = DateTime.Now;
        // 去掉 Tick 级精度差异，因为 Microseconds 会丢失 100ns (Ticks) 的精度
        // 我们把精度截断到秒或毫秒来做测试，或者容忍微小误差
        now = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second);

        var logs = new[]
        {
            new LogEntry { Id = 1, Message = "原神启动", Timestamp = now, ProcessedAt = null },
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
        using var df = DataFrame.FromSeries(s).Unnest("data"); // 炸开成 Id, Info

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
        using var df = DataFrame.FromSeries(s).Unnest("modern");

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
        using var df = DataFrame.FromSeries(s).Unnest("times");

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
    public void Test_DateTimeOffset_Nullable_And_Normalization()
    {
        // 1. 准备数据：同一时刻，不同时区
        // 2025-01-01 00:00:00 UTC
        var utcPoint = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        
        // 北京: 08:00 (+8) -> 对应 UTC 00:00
        var beijingPoint = new DateTimeOffset(2025, 1, 1, 8, 0, 0, TimeSpan.FromHours(8));
        
        // 纽约: 19:00 (-5, 前一天) -> 对应 UTC 00:00
        var nyPoint = new DateTimeOffset(2024, 12, 31, 19, 0, 0, TimeSpan.FromHours(-5));

        // 构造混合数组，包含 Null
        var data = new DateTimeOffset?[] 
        { 
            utcPoint,      // Index 0
            null,          // Index 1 (Null)
            beijingPoint,  // Index 2 (绝对时间等于 Index 0)
            nyPoint,       // Index 3 (绝对时间等于 Index 0)
            null           // Index 4
        };

        // 2. 存入 Series
        // 这会命中 UnzipDateTimeOffsetToUs(DateTimeOffset?[])
        using var s = new Series("mixed_offsets", data);

        // 3. 验证基础属性
        Assert.Equal(5, s.Length);
        Assert.Equal(2, s.NullCount);

        // 4. 读取验证 (假设 Series 有泛型 ToArray 或索引器支持)
        // 注意：Polars 里的 DateTimeOffset 读出来永远是 UTC (+00:00)
        var results = s.ToArray<DateTimeOffset?>(); // 需要你的 ToArray 支持 Nullable

        // 验证 Index 0 (UTC)
        Assert.NotNull(results[0]);
        Assert.Equal(TimeSpan.Zero, results[0]!.Value.Offset);
        Assert.Equal(utcPoint.UtcTicks / 10, results[0]!.Value.UtcTicks / 10); // 容忍微秒截断

        // 验证 Index 1 (Null)
        Assert.Null(results[1]);

        // 验证 Index 2 (北京 -> UTC)
        Assert.NotNull(results[2]);
        Assert.Equal(TimeSpan.Zero, results[2]!.Value.Offset); // 必须归一化为 UTC
        Assert.Equal(0, results[2]!.Value.Hour); // 变成 0 点 (UTC) 而不是 8 点
        // 绝对时间必须相等
        Assert.Equal(results[0]!.Value.UtcTicks, results[2]!.Value.UtcTicks);

        // 验证 Index 3 (纽约 -> UTC)
        Assert.NotNull(results[3]);
        Assert.Equal(TimeSpan.Zero, results[3]!.Value.Offset);
        Assert.Equal(results[0]!.Value.UtcTicks, results[3]!.Value.UtcTicks);        
    }
    [Fact]
    public void Test_DateTimeOffset_FastPath_LargeScale()
    {
        // 1. 准备 100 万数据
        int count = 1_000_000;
        var data = new DateTimeOffset[count];
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        // 制造“脏”数据：不同的 Offset 混在一起
        // 验证我们的算法是否能正确处理每一行的 Offset 减法
        for (int i = 0; i < count; i++)
        {
            // 基础时间递增 1 秒
            var baseTime = start.AddSeconds(i);
            
            // 随机分配时区
            int mode = i % 4;
            if (mode == 0) data[i] = baseTime.ToOffset(TimeSpan.Zero); // UTC
            else if (mode == 1) data[i] = baseTime.ToOffset(TimeSpan.FromHours(8)); // Beijing
            else if (mode == 2) data[i] = baseTime.ToOffset(TimeSpan.FromHours(-5)); // NY
            else data[i] = baseTime.ToOffset(TimeSpan.FromHours(5.5)); // India
        }

        // 2. 极速写入
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var s = new Series("fast_offsets", data);
        sw.Stop();
        Console.WriteLine($"Processed {count} DateTimeOffsets in {sw.Elapsed.TotalMilliseconds} ms");

        // 3. 验证
        Assert.Equal(count, s.Length);
        Assert.Equal(0, s.NullCount);

        // 4. 抽样检查精度
        // 我们需要验证：Input.UtcTicks 约等于 Output.UtcTicks
        // 允许误差：10 Ticks (1微秒)，因为我们做了除法截断
        long tolerance = 10; 

        // 定义个局部验证函数
        void CheckIndex(int idx)
        {
            object val = s[idx];
            DateTimeOffset result;

            if (val is DateTimeOffset dto) 
            {
                result = dto;
            }
            else if (val is DateTime dt)
            {
                // 如果返回的是 DateTime (Kind=Unspecified), 视作 UTC
                result = new DateTimeOffset(dt, TimeSpan.Zero);
            }
            else
            {
                // 如果返回的是 long (microsecond)
                long micros = Convert.ToInt64(val);
                long ticks = micros * 10 + 621355968000000000;
                result = new DateTimeOffset(ticks, TimeSpan.Zero);
            }

            // 核心校验：绝对时间差值
            long diff = Math.Abs(data[idx].UtcTicks - result.UtcTicks);
            
            if (diff > tolerance)
            {
                Assert.Fail($"Mismatch at {idx}. Input Offset: {data[idx].Offset}. Diff: {diff} Ticks.");
            }
            
            // 确保输出永远是 UTC
            Assert.Equal(TimeSpan.Zero, result.Offset);
        }

        // 验证头、尾、中
        CheckIndex(0);
        CheckIndex(count / 2);
        CheckIndex(count - 1);

        // 随机验证 1000 个
        var rng = new Random(12345);
        for (int k = 0; k < 1000; k++)
        {
            CheckIndex(rng.Next(0, count));
        }
        
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
    [Fact]
    public void Test_TimeZone_Operations_EndToEnd()
    {
        // 1. 准备数据：无时区时间 (Naive DateTime)
        // 2023-01-01 10:00:00
        var dt = new DateTime(2023, 1, 1, 10, 0, 0);
        
        using var df = DataFrame.FromColumns(new 
        {
            // 创建一个名为 "ts" 的列
            ts = new[] { dt } 
        });

        // --- 测试 1: ReplaceTimeZone (Naive -> Asia/Shanghai) ---
        // 预期：元数据变为 Shanghai，但时间数值仍然是 10:00
        
        using var df1 = df.Select(
            Col("ts")
                .Dt
                .ReplaceTimeZone("Asia/Shanghai")
                .Alias("ts_shanghai")
        );

        // 验证 Schema
        var schema1 = df1.Schema["ts_shanghai"];
        Assert.Equal(DataTypeKind.Datetime, schema1.Kind);
        Assert.Equal("Asia/Shanghai", schema1.TimeZone); // 验证 Rust 接收到了时区字符串

        object valReplace = df1["ts_shanghai"][0];

        Assert.IsType<DateTimeOffset>(valReplace); // 验证类型自动转换
        var dtoReplace = (DateTimeOffset)valReplace;

        // 验证墙上时间 (Wall Time)
        Assert.Equal(10, dtoReplace.Hour); 
        // 验证偏移量 (Offset) -> 上海是 +8
        Assert.Equal(TimeSpan.FromHours(8), dtoReplace.Offset);
        
        // --- 测试 2: ConvertTimeZone (Asia/Shanghai -> UTC) ---
        // 预期：上海时间 10:00 对应 UTC 时间 02:00
        // 墙上时间应该发生变化 (-8小时)

        using var df2 = df1.Select(
            Col("ts_shanghai")
            .Dt
            .ConvertTimeZone("UTC")
            .Alias("ts_utc")
        );

        var schema2 = df2.Schema["ts_utc"];
        Assert.Equal("UTC", schema2.TimeZone);

        // 验证值：10:00 Shanghai -> 02:00 UTC
        // 如果你的索引器返回的是 DateTimeOffset，它应该是 02:00
        var valUtc = (DateTimeOffset)df2["ts_utc"][0];
        Assert.Equal(2, valUtc.Hour); 


        // --- 测试 3: 链式操作 (Naive -> UTC -> Shanghai) ---
        // 输入 10:00 (视为UTC) -> 转为 Shanghai (应该变成 18:00)
        
        using var df3 = df.Select(
            Col("ts").Dt
            .ReplaceTimeZone("UTC").Dt           // 标记为 UTC (10:00)
            .ConvertTimeZone("Asia/Shanghai")  // 转为 Shanghai (+8h -> 18:00)
            .Alias("ts_converted")
        );

        var schema3 = df3.Schema["ts_converted"];
        Assert.Equal("Asia/Shanghai", schema3.TimeZone);

        var valConverted = df3["ts_converted"][0];
        // 2. [修正] 验证数值
        // 不要直接取 C# DateTime (它看到的是底层的 UTC)
        // 而是让 Polars 计算 "这个时区下的小时是多少"
        using var dfCheck = df3.Select(
            Col("ts_converted").Dt.Hour().Alias("h")
        );
        // Polars 知道是上海时间，所以它会返回 18(Int8)
        var hour = dfCheck["h"][0];
        Assert.Equal((sbyte)18, hour);

        // --- 测试 4: Remove TimeZone (Aware -> Naive) ---
        // 将上海时间移除时区，变回 Naive
        
        using var df4 = df3.Select(
            Col("ts_converted").Dt
            .ReplaceTimeZone(null) // 传入 null
            .Alias("ts_naive")
        );

        var schema4 = df4.Schema["ts_naive"];
        Assert.Equal("",schema4.TimeZone); // 验证时区被移除了
        
        // 值应该保持 18:00 (Replace 不改值)
        var valNaive = (DateTime)df4["ts_naive"][0];
        Assert.Equal(18, valNaive.Hour);
    }
    [Fact]
    public void Test_DataType_Array()
    {
        // 创建 Array(Int32, 3)
        var dtype = DataType.Array(DataType.Int32, 3);
        
        // 验证 Kind
        // 注意：需要确保你的 PlDataTypeKind 枚举里已经加了 Array = 23
        Assert.Equal(DataTypeKind.Array, dtype.Kind);
        
        // 验证 Width (通过我们刚加的 API)
        Assert.Equal(3UL, dtype.ArrayWidth);
        
        // 验证 Inner Type (复用已有的 Inner 逻辑)
        Assert.Equal(DataTypeKind.Int32, dtype.InnerType.Kind);
    }
    [Fact]
    public void Test_Float_Double_Half_Resolution()
    {
        // 1. 测试 Double (f64) - C# 默认行为
        var sF64 = new Series("f64", [1.1, 2.2, null]); 
        Assert.Equal(DataType.Float64, sF64.DataType); // 验证类型推断
        Assert.Equal(1.1, sF64.GetValue<double?>(0));
        Assert.Null(sF64.GetValue<double?>(2));
        
        // 2. 测试 Float (f32) - 必须加 'f' 后缀
        var sF32 = new Series("f32", [1.1f, 2.2f, null]);
        Assert.Equal(DataType.Float32, sF32.DataType);
        Assert.Equal(1.1f, sF32.GetValue<float?>(0));
        Assert.Null(sF32.GetValue<float?>(2));

        // 3. [NEW] 测试 Float16 (f16) - Polars 0.53 新特性
        // 注意：.NET 中对应的是 System.Half
        // C# 没有 f16 的字面量后缀，所以必须显式强转 (Half)
        // 集合表达式会自动推断为 Half?[]
        var sF16 = new Series("f16", [(Half)1.1f, (Half)2.2f, null]);

        // 验证 Polars 是否正确识别为 Float16
        // 前提：你的 DataType 枚举里要加上 Float16 = 12 (或者是对应的值)
        Assert.Equal(DataType.Float16, sF16.DataType);

        // 验证读取
        // GetValue 泛型需要支持 Half?
        Assert.Equal((Half)1.1f, sF16.GetValue<Half?>(0));
        Assert.Equal((Half)2.2f, sF16.GetValue<Half?>(1));
        Assert.Null(sF16.GetValue<Half?>(2));
        
        // 4. 验证混合类型报错机制 (Compile Time Safety)
        // var sMixed = new Series("mixed", [1.1, (Half)2.2]); // 编译报错：无法统一类型
    }

    [Fact]
    public void Test_Tiny_Integers_i8_u8_i16_u16()
    {
        // --- i8 (SByte) [-128, 127] ---
        var sI8 = new Series("i8", [ (sbyte)-10, (sbyte)127, null ]);
        Assert.Equal((sbyte)-10, sI8.GetValue<sbyte?>(0));
        Assert.Equal((sbyte)127, sI8.GetValue<sbyte?>(1));

        // --- u8 (Byte) [0, 255] ---
        // 注意：C# 字面量 255 默认是 int，在集合表达式里如果目标确认为 byte[] 会自动转换
        // 但这里为了保险和明确，我们看看 new Series("u8", [10, 255]) 能否工作
        // 编译器需要知道我们要调哪个构造函数，对于 Collection Expression 有时需要指引
        byte[] u8Raw = [10, 255]; 
        var sU8 = new Series("u8", u8Raw); // 显式传数组肯定没问题
        
        // 测试直接推断 (Nullable):
        var sU8_Null = new Series("u8_n", [(byte)10, (byte)255, null]);
        Assert.Equal((byte)255, sU8_Null.GetValue<byte?>(1));

        // --- i16 (Short) ---
        var sI16 = new Series("i16", [(short)-30000, null, (short)30000]);
        Assert.Equal((short)-30000, sI16.GetValue<short?>(0));

        // --- u16 (UShort) ---
        var sU16 = new Series("u16", [(ushort)60000, null, (ushort)0]);
        Assert.Equal((ushort)60000, sU16.GetValue<ushort?>(0));
    }

    [Fact]
    public void Test_Large_Unsigned_Integers_u32_u64()
    {
        // --- u32 (UInt) ---
        // 超过 Int32.MaxValue (21亿) 来测试
        uint bigUInt = 3_000_000_000u; // 注意 u 后缀
        var sU32 = new Series("u32", [bigUInt, null, 0u]);
        
        Assert.Equal(bigUInt, sU32.GetValue<uint?>(0));

        // --- u64 (ULong) ---
        // 超过 Int64.MaxValue
        ulong hugeULong = 10_000_000_000_000_000ul; // 注意 ul 后缀
        var sU64 = new Series("u64", [hugeULong, null, 123ul]);
        
        Assert.Equal(hugeULong, sU64.GetValue<ulong?>(0));
    }
    [Fact]
    public void Test_Float16_Million_Rows_SIMD_Stress()
    {
        const int RowCount = 1_000_000;
        
        // 1. 准备数据：100万个可空 Half
        // 模式：0, 1, 2 ... 99, 0, 1 ... (循环)
        // 每逢整除 100 为 Null (Index 0, 100, 200...)
        var rawData = new Half?[RowCount];
        
        // 数据生成不计入 Polars 耗时
        for (int i = 0; i < RowCount; i++)
        {
            if (i % 100 == 0)
                rawData[i] = null;
            else
                rawData[i] = (Half)(i % 100);
        }

        Console.WriteLine($"🚀 Starting ingestion of {RowCount:N0} f16 rows (SIMD Check)...");

        // 2. 核心时刻：Series 构造
        // 这一步会触发你的 SIMD 优化的 Arrow Buffer 构建逻辑 (Bitmask + Value Buffer Copy)
        var sw = Stopwatch.StartNew();
        
        using var s = new Series("f16_million", rawData);
        
        sw.Stop();
        
        // 打印耗时，看看有没有跑进 5ms
        Console.WriteLine($"✅ Ingestion took: {sw.Elapsed.TotalMilliseconds:F2} ms");
        Console.WriteLine($"Throughput: {RowCount / sw.Elapsed.TotalSeconds / 1_000_000:F2} M rows/sec");

        // 3. 验证元数据 (Sanity Check)
        Assert.Equal(RowCount, s.Length);
        Assert.Equal("f16_million", s.Name);
        
        // [关键] 必须是 Float16，不能是 Float32
        // 这证明数据确实以 2字节/元素 的形式存在于 Rust 内存中
        Assert.Equal(DataType.Float16, s.DataType); 
        
        // 验证 Null 数量 (1,000,000 / 100 = 10,000)
        Assert.Equal(10_000, s.NullCount);

        // 4. 验证数据值 (抽样检查 - 验证内存没有错位)
        // Index 0 应该是 Null
        Assert.Null(s.GetValue<Half?>(0));
        
        // Index 1 应该是 1.0
        Assert.Equal((Half)1, s.GetValue<Half?>(1));
        
        // Index 99 应该是 99.0
        Assert.Equal((Half)99, s.GetValue<Half?>(99));
        
        // Index 100 又是 Null
        Assert.Null(s.GetValue<Half?>(100));

        // 5. 验证计算能力 (Compute Check)
        // 逻辑验证：
        // 每一组 0..99 (index 0 是 null)，剩下的和是 1+2+...+99 = 4950
        // 一共 10,000 组
        // 预期总和 = 4950 * 10,000 = 49,500,000
        
        // Polars 计算 Sum 时通常会将 f16 提升为 f64 以防溢出
        double sum = s.Cast(DataType.Float64).Sum<double>(); 
        
        Assert.Equal(49_500_000.0, sum);
    }
    [Fact]
    public void Test_Edge_Case_Mixed_Numeric_Types()
    {
        // 这是一个特别有意思的测试
        // Polars (Rust) 实际上非常严格。
        // 如果我们在 C# 这边把 sbyte 转成 int 传进去，它就是 Int32 类型。
        
        sbyte[] smallData = [1, 2, 3];
        // 隐式转换：Series(string, int[]) 会被匹配吗？
        // 不会，因为 sbyte[] 无法隐式转换为 int[] (这是协变问题，C# 数组不支持值类型协变)
        // 所以用户必须明确类型，这非常好，避免了意外的内存拷贝或类型提升。
        
        // var s = new Series("fail", smallData); // 应该编译报错，因为没匹配到 sbyte[] 的非空重载？
        // 只要我们定义了 sbyte[] 重载，它就会匹配到 sbyte 版本。
        
        var s = new Series("sbyte", smallData);
        // 验证它没有被提升为 int
        // 如果它被提升为 int，GetValue<sbyte> 可能会报错或者需要转换
        Assert.Equal((sbyte)1, s.GetValue<sbyte>(0));
    }
    [Fact]
    public void Test_Int128_Beyond_Int64_Range()
    {
        // 1. 构造一个 Int64 绝对装不下的大数
        // Int64.MaxValue 大约是 9 x 10^18
        // 我们构造一个 2^100，这绝对需要 128 位才能存下
        Int128 bigVal = (Int128)1 << 100; 
        
        // 验证一下它真的很大
        Assert.True(bigVal > Int64.MaxValue);

        // 2. 创建 Series
        // C# 还没有 Int128 字面量后缀，所以这里用显式转换
        var s = new Series("big_i128", [bigVal, -bigVal, null]);

        // 3. 验证取出
        // 如果底层按照 i64 截断，这个数会变成 0 或者其他奇怪的数
        Assert.Equal(bigVal, s.GetValue<Int128?>(0));
        Assert.Equal(-bigVal, s.GetValue<Int128?>(1));
        Assert.Null(s.GetValue<Int128?>(2));

        Assert.Throws<NotSupportedException>(s.ToArrow);
        // Assert.Equal(bigVal,sArrow.GetInt128Value(0));
    }

    [Fact]
    public void Test_UInt128_Max_Value()
    {
        // 测试无符号最大值：340282366920938463463374607431768211455
        UInt128 maxVal = UInt128.MaxValue;
        
        // 构造 Series
        var s = new Series("max_u128", [maxVal, UInt128.MinValue,null]);
        // 验证
        // 如果底层当作 signed i128 处理，MaxValue 可能会变成 -1，所以这个测试能验证 signed/unsigned 没搞混
        Assert.Equal(maxVal, s.GetValue<UInt128?>(0));
        Assert.Equal(UInt128.Zero, s.GetValue<UInt128?>(1));
        Assert.Null(s.GetValue<UInt128?>(2));
    }

    [Fact]
    public void Test_Int128_Span_Optimization()
    {
        // 验证我们那个通用的 UnzipNullable<T> 泛型方法是否对 16字节的结构体生效
        Int128?[] data = [1, null, 2];
        
        var s = new Series("opt_test", data);
        
        Assert.Equal((Int128)1, s.GetValue<Int128?>(0));
        Assert.Null(s.GetValue<Int128?>(1));
    }
    [Fact]
    public void Test_Empty_Arrays_Preserve_Schema()
    {
        // 1. Int32 Empty
        int[] emptyInt = [];
        using var sInt = new Series("empty_int", emptyInt);
        Assert.Equal(0, sInt.Length);

        // 2. DateTime Empty (测试 UnzipDateTimeToUs 逻辑)
        DateTime?[] emptyDt = [];
        using var sDt = new Series("empty_dt", emptyDt);
        Assert.Equal(0, sDt.Length);

        // 3. String Empty (测试 StringPacker 逻辑)
        string[] emptyStr = [];
        using var sStr = new Series("empty_str", emptyStr);
        Assert.Equal(0, sStr.Length);
        
        // 4. DataFrame Schema Alignment
        // 验证空 Series 能否组装成 DataFrame 且 Schema 正确
        var df = new DataFrame(sInt, sDt, sStr);
        Assert.Equal((0,3), df.Shape);
        
        // 验证 Schema 并没有丢失
        var schema = df.Schema;
        Assert.Equal(DataType.Int32, schema["empty_int"]);
        Assert.Equal(DataType.Datetime(TimeUnit.Microseconds), schema["empty_dt"]);
        Assert.Equal(DataType.String, schema["empty_str"]);
    }
    [Fact]
    public void Test_DateTime_Array_Large_Scale_Accuracy()
    {
        // =========================================================================
        // 1. 准备数据：100万个日期 (约 8MB 数据量)
        // =========================================================================
        int count = 1_000_000;
        var dateArray = new DateTime[count];
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        
        // 模拟真实场景：
        // 偶数索引放 UTC，奇数索引放 Local
        // 确保 Mask 逻辑把 Kind 位剥离干净，只保留数值
        for (int i = 0; i < count; i++)
        {
            // AddSeconds 保证了 Ticks 肯定是 10 的倍数，避免 /10 的精度损失干扰测试
            var dt = start.AddSeconds(i); 
            
            if (i % 2 == 0)
                dateArray[i] = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            else
                dateArray[i] = dt.ToLocalTime(); // 转为 Local，Kind位会变化
        }

        // =========================================================================
        // 2. 走屠龙刀通道 (Series -> DataFrame)
        // =========================================================================
        // 这里会直接命中 Series(string, DateTime[]) -> UnzipDateTimeToUs (Unroll 8)
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var s = new Series("large_dt", dateArray);
        sw.Stop();
        Console.WriteLine($"Series Created in: {sw.Elapsed.TotalMilliseconds} ms");

        // =========================================================================
        // 3. 验证 (使用索引器 s[i])
        // =========================================================================
        Assert.Equal(count, s.Length);
        Assert.Equal(0, s.NullCount);

        // 验证首尾和中间值
        CheckValue(s, dateArray, 0);
        CheckValue(s, dateArray, count / 2);
        CheckValue(s, dateArray, count - 1);

        // 随机抽查 1000 个点，确保没有 Off-by-one 错误
        var rng = new Random(42);
        for (int k = 0; k < 1000; k++)
        {
            int idx = rng.Next(0, count);
            CheckValue(s, dateArray, idx);
        }
    }

    private void CheckValue(Series s, DateTime[] source, int index)
    {
        // 假设 s[i] 返回的是 object (boxed DateTime)
        // 如果你的索引器是泛型的 s.GetValue<DateTime>(i) 那更好
        object val = s[index];
        
        Assert.NotNull(val);
        Assert.IsType<DateTime>(val);
        
        DateTime actual = (DateTime)val!;
        DateTime expected = source[index];

        // 核心验证逻辑：
        // 1. 我们存进去的是 Naive 的物理值，所以出来的 Kind 应该是 Unspecified (或者 UTC，取决于 Rust 返回什么)
        //    我们不比较 Kind，只比较 Ticks 数值。
        // 2. 我们的存储精度是微秒 (us)，1 us = 10 Ticks。
        //    所以比较时，(Expected.Ticks / 10) 必须等于 (Actual.Ticks / 10)
        
        // 注意：Local 时间必须先转回逻辑上的“表盘时间”才能比较 Ticks
        // 比如 source 是 "12:00 Local"，s[i] 出来的是 "12:00 Unspecified"
        // 它们的 Ticks 属性值应该是一样的（忽略 Kind 位）
        
        long expectedTicks = expected.Ticks; // 这里包含了时间值
        long actualTicks = actual.Ticks;

        // 允许 1us 的精度损失 (虽然上面的 AddSeconds 不会产生损失)
        long expectedUs = expectedTicks / 10;
        long actualUs = actualTicks / 10;

        if (expectedUs != actualUs)
        {
             // 只有出错时才生成详细信息
             Assert.Fail($"Mismatch at index {index}. \n" +
                         $"Expected: {expected} ({expected.Kind}, Ticks={expectedTicks})\n" +
                         $"Actual:   {actual} ({actual.Kind}, Ticks={actualTicks})");
        }
    }
    [Fact]
    public void Test_DateOnly_SIMD_Path()
    {
        // 1. 准备数据：长度 2050 (故意不是 16 的倍数，测试 SIMD + Tail)
        int count = 2050;
        var data = new DateOnly[count];
        var start = new DateOnly(2000, 1, 1);

        for (int i = 0; i < count; i++)
        {
            data[i] = start.AddDays(i); // 日期递增
        }

        // 2. 创建 Series (这里会根据 CPU 自动命中 AVX-512 或 AVX2)
        using var s = new Series("simd_dates", data);

        // 3. 验证基础
        Assert.Equal(count, s.Length);
        Assert.Equal(0, s.NullCount);

        // 4. 边界验证 (Simd Boundary Checks)
        // 验证第 15, 16, 17 个元素 (AVX-512 边界)
        // 验证第 7, 8, 9 个元素 (AVX2 边界)
        CheckIndex(s, data, 7);
        CheckIndex(s, data, 8);
        CheckIndex(s, data, 15);
        CheckIndex(s, data, 16);
        
        // 5. 尾部验证 (Tail Loop)
        CheckIndex(s, data, count - 1);
        CheckIndex(s, data, count - 2);

        // 6. 随机抽查
        var rng = new Random(42);
        for (int i = 0; i < 100; i++)
        {
            CheckIndex(s, data, rng.Next(0, count));
        }
    }

    // 泛型验证辅助函数
    private void CheckIndex<T>(Series s, T[] expectedData, int index)
    {
        // 利用万能索引器
        object actual = s[index];
        Assert.Equal(expectedData[index], actual);
    }
    [Fact]
    public void Test_TimeOnly_SIMD_Path()
    {
        // 1. 准备数据：长度 2050
        int count = 2050;
        var data = new TimeOnly[count];
        var start = new TimeOnly(0, 0, 0);

        for (int i = 0; i < count; i++)
        {
            // 每行增加 1 秒 + 100ns (Ticks=1)
            // 这样能测试到 Ticks * 100 的乘法是否正确
            data[i] = start.Add(TimeSpan.FromTicks(i * 10000000L + 1)); 
        }

        // 2. 创建 Series (命中 AVX-512/AVX2 Mul)
        using var s = new Series("simd_times", data);

        // 3. 验证
        Assert.Equal(count, s.Length);

        // 4. 关键点验证
        // 验证 SIMD 块的边缘，确保没有 Off-by-one
        CheckIndex(s, data, 0);
        CheckIndex(s, data, 7);  // AVX-512 end of first block? (0-7)
        CheckIndex(s, data, 8);  // Start of next?
        CheckIndex(s, data, 15);
        CheckIndex(s, data, 16);

        // 验证尾部
        CheckIndex(s, data, count - 1);
    }
    [Fact]
    public void Test_TimeSpan_ILP_Path()
    {
        // 1. 准备数据：长度 2050
        int count = 2050;
        var data = new TimeSpan[count];

        for (int i = 0; i < count; i++)
        {
            // 构造 10 Ticks 的倍数，确保 Assert.Equal 能通过 (无截断)
            // 如果不是 10 的倍数，我们需要手动计算 Expected 值
            data[i] = TimeSpan.FromTicks(i * 10); 
        }

        // 2. 创建 Series (命中 Unroll 8 标量除法)
        using var s = new Series("ilp_durations", data);

        // 3. 验证
        Assert.Equal(count, s.Length);

        // 4. 验证 Unroll 边界
        // Unroll 8 意味着每 8 个处理一次
        CheckIndex(s, data, 7);
        CheckIndex(s, data, 8);
        CheckIndex(s, data, 15);
        CheckIndex(s, data, 16);

        // 验证尾部 (Tail Loop)
        // 2050 % 8 = 2，意味着最后两个元素走 Tail Loop
        CheckIndex(s, data, count - 1); // Tail
        CheckIndex(s, data, count - 2); // Tail
        CheckIndex(s, data, count - 3); // Main Loop End
    }
    [Fact]
    public void Test_Decimal_Integration_MixedScale()
    {
        // 1. 准备数据：标度大乱炖
        // C# Decimal 的特性：1.2m (Scale=1) != 1.20m (Scale=2)
        var data = new decimal?[]
        {
            1.5m,               // Scale 1 -> 补齐到 5
            -2.123m,            // Scale 3 -> 补齐到 5
            100m,               // Scale 0 -> 补齐到 5
            0.00005m,           // Scale 5 (Max) -> 决定了 Series 的 Scale
            decimal.MaxValue,   // 验证 96位 整数提取
            decimal.MinValue,   // 验证 负号处理
            null
        };

        // 2. 创建 Series (命中 DecimalPacker.Pack 可空路径)
        using var s = new Series("mixed_decimal", data);

        // 3. 验证基础
        Assert.Equal(7, s.Length);
        Assert.Equal(1, s.NullCount);

        // 4. 验证数值
        // 注意：Series[i] 取出来的 decimal 可能会带有统一的 Scale (5)
        // 但 C# decimal.Equals(1.50000m, 1.5m) 是 True，所以直接比较即可
        Assert.Equal(data[0], s[0]);
        Assert.Equal(data[1], s[1]);
        Assert.Equal(data[2], s[2]);
        Assert.Equal(data[3], s[3]);
        Assert.Equal(data[4], s[4]);
        Assert.Equal(data[5], s[5]);
        Assert.Null(s[6]);

        // 5. 验证底层 Scale (可选)
        // 这一步虽然我们没暴露 Scale 属性，但通过数值正确性侧面验证了
        // 比如 100m 如果 Scale 没补对，取出来可能变成 0.001m 或者 100000m
    }
    [Fact]
    public void Test_Decimal_NonNullable_FastPath()
    {
        // 1. 准备数据
        var data = new decimal[]
        {
            1234567890.1234567890m, // High Precision
            0.0000000000000000001m, // High Scale (19)
            -99.9m,                 // Negative
            0m                      // Zero
        };

        // 2. 创建 Series (命中 DecimalPacker.Pack 非空路径)
        using var s = new Series("fast_decimal", data);

        // 3. 验证
        Assert.Equal(4, s.Length);
        Assert.Equal(0, s.NullCount);

        Assert.Equal(data[0], s[0]);
        Assert.Equal(data[1], s[1]);
        Assert.Equal(data[2], s[2]);
        Assert.Equal(data[3], s[3]);
    }
    [Fact]
    public void Test_Decimal_LargeScale_Stress()
    {
        // 1. 准备 100 万数据
        int count = 1_000_000;
        var data = new decimal[count];
        
        // 构造数据：
        // 偶数索引 Scale=0 (整数)
        // 奇数索引 Scale=2 (小数)
        // 这样 Packer 必须把偶数索引乘 100 来补齐
        for (int i = 0; i < count; i++)
        {
            if (i % 2 == 0) data[i] = (decimal)i;       // Scale 0
            else data[i] = (decimal)i + 0.55m;          // Scale 2
        }

        // 2. 极速打包
        var sw = System.Diagnostics.Stopwatch.StartNew();
        using var s = new Series("stress_decimal", data);
        sw.Stop();
        
        // 3. 验证
        Console.WriteLine($"Decimal Packed {count} items in {sw.Elapsed.TotalMilliseconds} ms");
        Assert.Equal(count, s.Length);

        // 4. 抽样检查
        // Index 0: 0 -> 0.00
        Assert.Equal(0m, s[0]);
        // Index 1: 1.55 -> 1.55
        Assert.Equal(1.55m, s[1]);
        // Index 100: 100 -> 100.00
        Assert.Equal(100m, s[100]);
        // Index End
        Assert.Equal(data[count - 1], s[count - 1]);

        // 随机抽查
        var rng = new Random(999);
        for (int k = 0; k < 100; k++)
        {
            int idx = rng.Next(0, count);
            Assert.Equal(data[idx], s[idx]);
        }
    }
    [Fact]
    public void Test_Decimal_MaxScale_Limit()
    {
        // 构造一个 Scale = 28 的数 (C# 极限)
        decimal extreme = 0.0000000000000000000000000001m; // 1e-28
        Assert.Equal(28, extreme.Scale);

        var data = new decimal[] { 1m, extreme };
        
        // 此时 MaxScale = 28
        // 1m 会被乘 10^28 (变成 huge integer)
        // extreme 会保持 1
        
        using var s = new Series("limit_decimal", data);
        
        Assert.Equal(1m, s[0]);
        Assert.Equal(extreme, s[1]);
    }
    [Fact]
    public void Test_FixedSizeList_Double_Image()
    {
        // 1. 准备数据：2x2 的像素块
        double[,] pixels = new double[,] 
        { 
            { 0.1, 0.2 }, 
            { 0.8, 0.9 } 
        };

        // 2. 传递
        using var s = new Series("pixels", pixels);

        // 3. 验证
        Assert.Equal(2, s.Length);
        
        // 此时 Rust 端应该持有一个 FixedSizeList(2) of Float64
        Console.WriteLine(s); // 手动看一下输出是否是 [[0.1, 0.2], [0.8, 0.9]]
    }
    [Fact]
    public void Test_FixedSizeList_Performance_Large()
    {
        // 1. 准备 100万个 double (8MB 数据)
        int size = 1000;
        double[,] largeMatrix = new double[size, size];
        
        // 随便填点数据，确保内存被 touch 过
        largeMatrix[0, 0] = 1.0;
        largeMatrix[size-1, size-1] = 99.0;

        var sw = System.Diagnostics.Stopwatch.StartNew();
        
        // 2. 瞬移！
        using var s = new Series("large_matrix", largeMatrix);
        
        sw.Stop();
        Console.WriteLine($"Transferred 1,000,000 doubles (2D) in {sw.Elapsed.TotalMilliseconds} ms");

        // Zero-Copy 理论上耗时应该极短（< 1ms 或者仅包含一些 P/Invoke 开销）
        // 如果这里超过了 10ms，那就说明发生了拷贝
        Assert.Equal(size, s.Length);
    }
    [Fact]
    public void Test_FixedSizeList_Int128_Layout_Check()
    {
        // 1. 准备数据
        // 使用 2x2 矩阵
        Int128[,] data = new Int128[2, 2];
        
        // Case A: 只有低位有值
        data[0, 0] = 1; 
        
        // Case B: 只有高位有值 (2^64)
        // C# 中 Int128 构造比较麻烦，可以用位移
        Int128 highBit = (Int128)1 << 64; 
        data[0, 1] = highBit;

        // Case C: 高低位都有值 (Max)
        data[1, 0] = Int128.MaxValue;

        // Case D: 负数 (验证符号位是否在最高字节)
        data[1, 1] = -1;

        // 2. 传递给 Rust
        using var s = new Series("int128_matrix", data);

        // 3. 验证 (Rust 传回来的值)
        Console.WriteLine($"Int128 Check Passed. Series: {s}");
    }
    [Fact]
    public void Test_Decimal_Matrix_AutoScaling()
    {
        // 1. Arrange: 准备一个 "刺激" 的 decimal 矩阵
        // 我们混合不同的精度 (Scale)，测试 Packer 是否能自动检测到 MaxScale = 3
        // 结构: 2行 x 3列
        decimal[,] data = new decimal[2, 3] 
        {
            { 1.1m,      2.22m,     3.333m }, // Row 0: Max Scale 3
            { 100m,      0.00005m,  -1.5m  }  // Row 1: Max Scale 5 (注意这个 0.00005)
        };
        
        // 实际上上面的初始化中：
        // 3.333 -> Scale 3
        // 0.00005 -> Scale 5
        // 所以整个 Series 的 Scale 应该被提升到 5。
        // 1.1 会变成 1.10000
        
        string name = "decimal_matrix";

        // 2. Act: 通过 Wrapper 创建 Series
        // 注意：这是 C# 侧的调用，假设你有一个 C# Series 类或者直接调 Wrapper
        // 这里模拟直接调用 Wrapper 并封装 (类似 F# 的行为)
        
        using var series = new Series(name, data); // 假设 C# 也有类似的 Create<T>(string, T[,])

        // 3. Assert: 验证结果
        
        // A. 验证类型和形状
        // 假设 Polars 返回的类型字符串类似于 "FixedSizeList[3] (Decimal(38, 5))"
        // 或者是 List<Decimal>
        Assert.Equal(DataType.Array(DataType.Decimal(38,5),3),series.DataType); 
        
        // B. 验证数据正确性 (Round-trip)
        // 我们把数据读回来变成 decimal[,] 或者 decimal[][]
        decimal[][] rows = series.ToArray<decimal[]>();

        Assert.Equal(2, rows.Length); // 应该有 2 行

        // Row 0 Check
        Assert.Equal(3, rows[0].Length); // 宽度 3
        Assert.Equal(1.1m, rows[0][0]);
        Assert.Equal(2.22m, rows[0][1]);
        Assert.Equal(3.333m, rows[0][2]);

        // Row 1 Check
        Assert.Equal(3, rows[1].Length); // 宽度 3
        Assert.Equal(100m, rows[1][0]);
        Assert.Equal(0.00005m, rows[1][1]);
        Assert.Equal(-1.5m, rows[1][2]);

        // C. 验证 Scale 提升逻辑 (可选，如果能访问到底层 Arrow 类型)
        // 1.1m 在内存里应该变成了 110000 (因为 Scale=5)
        // 0.00005m 在内存里是 5
    }

    [Fact]
    public void Test_Decimal_OverflowException()
    {
        // Test Decimal Scale Overflow exception
        decimal huge = decimal.MaxValue; 
        decimal tiny = 0.0000000000000000000000000001m; // Decimal.MinValue (Scale 28)

        decimal[,] data = new decimal[2, 1] 
        {
            { huge },
            { tiny }
        };

        Assert.Throws<OverflowException>(() => new Series("huge_decimal", data));
    }
}