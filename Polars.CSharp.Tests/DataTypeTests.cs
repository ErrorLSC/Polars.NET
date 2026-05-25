using System.Diagnostics;
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;
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
        var trades = new List<TradeRecord>
        {
            new() { Ticker = "AAPL", Qty = 100, Price = 150.50m, Factor = 1.1, Risk = 0.5f },
            new() { Ticker = "GOOG", Qty = 50,  Price = 2800.00m, Factor = null, Risk = 0.1f },
            new() { Ticker = "MSFT", Qty = 200, Price = 300.25m, Factor = 0.95, Risk = 0.2f }
        };

        // From: List -> DataFrame
        using var df = DataFrame.From(trades);
        
        Assert.Equal(3, df.Height);
    
        // To: DataFrame -> List (Rows<T>)
        var resultList = df.Rows<TradeRecord>().ToList();

        Assert.Equal(3, resultList.Count);

        var row0 = resultList[0];
        Assert.Equal("AAPL", row0.Ticker);
        Assert.Equal(100, row0.Qty);
        Assert.Equal(150.50m, row0.Price);
        Assert.Equal(1.1, row0.Factor);
        Assert.Equal(0.5f, row0.Risk);

        var row1 = resultList[1];
        Assert.Equal("GOOG", row1.Ticker);
        Assert.Null(row1.Factor); 
    }
    public class LogEntry
    {
        public int Id { get; set; }
        public string Message { get; set; }
        public DateTime Timestamp { get; set; }
        public DateTime? ProcessedAt { get; set; } 
    }

    [Fact]
    [Trait("DataType","DateTime")]
    public void Test_DataFrame_DateTime_RoundTrip()
    {
        var now = DateTime.Now;

        now = new DateTime(now.Year, now.Month, now.Day, now.Hour, now.Minute, now.Second);

        var logs = new[]
        {
            new LogEntry { Id = 1, Message = "原神启动", Timestamp = now, ProcessedAt = null },
            new LogEntry { Id = 2, Message = "End", Timestamp = now.AddMinutes(1), ProcessedAt = now.AddMinutes(2) }
        };

        // From (C# -> Polars)
        using var df = DataFrame.From(logs);

        Assert.Equal(2, df.Height);

        // To (Polars -> C#)
        var result = df.Rows<LogEntry>().ToList();

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
        using var s = Series.From("data", data); 
        using var df = DataFrame.FromSeries(s).Unnest("data"); 

        // Expected:
        // Id (i64), Info (Struct)
        
        // DataFrame -> POCO (Rows<T>)
        var results = df.Rows<ComplexContainer>().ToList();

        Assert.Equal(3, results.Count);
        
        // Row 0
        Assert.Equal(1, results[0].Id);
        Assert.Equal("A", results[0].Info.Key);
        Assert.Equal(2, results[0].Info.Values.Count);
        Assert.Equal(2.2, results[0].Info.Values[1]);

        // Row 1 (Struct Null)
        Assert.Equal(2, results[1].Id);
        Assert.Null(results[1].Info); 

        // Row 2
        Assert.Equal("B", results[2].Info.Key);
        Assert.Single(results[2].Info.Values);
    }
    private class ModernTypesPoco
    {
        public string Cat { get; set; } 
        public DateOnly Date { get; set; }
        public TimeOnly Time { get; set; }
    }

    [Fact]
    [Trait("DataType","Categorical")]
    public void Test_DataFrame_ModernTypes_And_Categorical()
    {
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

        using var dfCat = df.WithColumns(Pl.Col("Cat").Cast(DataType.Categorical("time")));

        Assert.Equal(DataType.Categorical("time"), dfCat.Schema["Cat"]);
        Assert.Equal(DataTypeKind.Date, dfCat.Schema["Date"].Kind);
        Assert.Equal(DataTypeKind.Time, dfCat.Schema["Time"].Kind);

        var rows = dfCat.Rows<ModernTypesPoco>().ToList();

        Assert.Equal(2, rows.Count);
        
        Assert.Equal("A", rows[0].Cat);
        Assert.Equal("B", rows[1].Cat);

        Assert.Equal(new DateOnly(2023, 1, 1), rows[0].Date);

        Assert.Equal(new TimeOnly(12, 0, 0), rows[0].Time);
    }
    private class TimeFamily
    {
        public DateOnly Date { get; set; }
        public TimeOnly Time { get; set; }
        public DateTime Stamp { get; set; }
        public TimeSpan Duration { get; set; } 
    }

    [Fact]
    public void Test_TimeFamily_Reunion()
    {
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

        using var s = Series.From("times", data);
        using var df = DataFrame.FromSeries(s).Unnest("times");

        Assert.Equal(DataTypeKind.Duration, df.Schema["Duration"].Kind);

        var rows = df.Rows<TimeFamily>().ToList();

        // Row 0
        Assert.Equal(TimeSpan.FromHours(1.5) + TimeSpan.FromMicroseconds(50), rows[0].Duration);
        
        // Row 1
        Assert.Equal(TimeSpan.FromDays(365), rows[1].Duration);

        Assert.Equal(new DateOnly(2025, 1, 1), rows[0].Date);
        Assert.Equal(new TimeOnly(14, 30, 0), rows[0].Time);
    }
    [Fact]
    public void Test_DateTimeOffset_Nullable_And_Normalization()
    {
        // 2025-01-01 00:00:00 UTC
        var utcPoint = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);
        
        // Beijing: 08:00 (+8) ->  UTC 00:00
        var beijingPoint = new DateTimeOffset(2025, 1, 1, 8, 0, 0, TimeSpan.FromHours(8));
        
        // New York: 19:00 (-5, 前一天) -> 对应 UTC 00:00
        var nyPoint = new DateTimeOffset(2024, 12, 31, 19, 0, 0, TimeSpan.FromHours(-5));

        var data = new DateTimeOffset?[] 
        { 
            utcPoint,      // Index 0
            null,          // Index 1 (Null)
            beijingPoint,  // Index 2 
            nyPoint,       // Index 3 
            null           // Index 4
        };

        // UnzipDateTimeOffsetToUs(DateTimeOffset?[])
        using var s = new Series("mixed_offsets", data);

        Assert.Equal(5, s.Length);
        Assert.Equal(2, s.NullCount);

        var results = s.ToArray<DateTimeOffset?>(); 

        //  Index 0 (UTC)
        Assert.NotNull(results[0]);
        Assert.Equal(TimeSpan.Zero, results[0]!.Value.Offset);
        Assert.Equal(utcPoint.UtcTicks / 10, results[0]!.Value.UtcTicks / 10); 

        //  Index 1 (Null)
        Assert.Null(results[1]);

        //  Index 2 (Beijing -> UTC)
        Assert.NotNull(results[2]);
        Assert.Equal(TimeSpan.Zero, results[2]!.Value.Offset); 
        Assert.Equal(0, results[2]!.Value.Hour); 
        Assert.Equal(results[0]!.Value.UtcTicks, results[2]!.Value.UtcTicks);

        //  Index 3 (New York -> UTC)
        Assert.NotNull(results[3]);
        Assert.Equal(TimeSpan.Zero, results[3]!.Value.Offset);
        Assert.Equal(results[0]!.Value.UtcTicks, results[3]!.Value.UtcTicks);        
    }
    [Fact]
    public void Test_DateTimeOffset_FastPath_LargeScale()
    {
        int count = 1_000_000;
        var data = new DateTimeOffset[count];
        var start = new DateTimeOffset(2025, 1, 1, 0, 0, 0, TimeSpan.Zero);

        for (int i = 0; i < count; i++)
        {
            var baseTime = start.AddSeconds(i);
            
            int mode = i % 4;
            if (mode == 0) data[i] = baseTime.ToOffset(TimeSpan.Zero); // UTC
            else if (mode == 1) data[i] = baseTime.ToOffset(TimeSpan.FromHours(8)); // Beijing
            else if (mode == 2) data[i] = baseTime.ToOffset(TimeSpan.FromHours(-5)); // NY
            else data[i] = baseTime.ToOffset(TimeSpan.FromHours(5.5)); // India
        }

        var sw = Stopwatch.StartNew();
        using var s = new Series("fast_offsets", data);
        sw.Stop();
        
        Console.WriteLine($"Processed {count} DateTimeOffsets in {sw.Elapsed.TotalMilliseconds} ms");
        Assert.Equal(count, s.Length);
        Assert.Equal(0, s.NullCount);

        long tolerance = 10; 

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
                result = new DateTimeOffset(dt, TimeSpan.Zero);
            }
            else
            {
                long micros = Convert.ToInt64(val);
                long ticks = micros * 10 + 621355968000000000;
                result = new DateTimeOffset(ticks, TimeSpan.Zero);
            }

            long diff = Math.Abs(data[idx].UtcTicks - result.UtcTicks);
            
            if (diff > tolerance)
            {
                Assert.Fail($"Mismatch at {idx}. Input Offset: {data[idx].Offset}. Diff: {diff} Ticks.");
            }
            
            Assert.Equal(TimeSpan.Zero, result.Offset);
        }

        CheckIndex(0);
        CheckIndex(count / 2);
        CheckIndex(count - 1);

        var rng = new Random(12345);
        for (int k = 0; k < 1000; k++)
        {
            CheckIndex(rng.Next(0, count));
        }
        
    }
    [Fact]
    public void Test_WallClock_Consistency()
    {
        
        var dtLocal = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Local);
        var dtUtc   = new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Utc);
        var dtUnspec= new DateTime(2025, 1, 1, 12, 0, 0, DateTimeKind.Unspecified);


        using var df = DataFrame.From(
        [
            new { A = dtLocal, B = dtUtc, C = dtUnspec } 
        ]);


        var row = df.Rows<dynamic>().First();
        
        DateTime valA = df.GetValue<DateTime>(0, "A");
        DateTime valB = df.GetValue<DateTime>(0, "B");
        DateTime valC = df.GetValue<DateTime>(0, "C");

        long tolerance = 100; 

        Assert.InRange(valA.Ticks - new DateTime(2025, 1, 1, 12, 0, 0).Ticks, -tolerance, tolerance);
        
        Assert.Equal(DateTimeKind.Unspecified, valA.Kind);
        Assert.Equal(DateTimeKind.Unspecified, valB.Kind);
        
        Assert.InRange(valA.Ticks - valB.Ticks, -tolerance, tolerance);
        Assert.InRange(valA.Ticks - valC.Ticks, -tolerance, tolerance);
    }

    [Fact]
    public void Test_GetValue_Complex()
    {

        var data = new List<ComplexContainer>
        {
            new() { Id = 1, Info = new NestedItem { Key = "K1" } },
            new() { Id = 2, Info = new NestedItem { Key = "K2" } }
        };
        using var s = Series.From("data", data); // Struct Series

        var item1 = s.GetValue<ComplexContainer>(0);
        Assert.Equal("K1", item1.Info.Key);

        var item2 = s.GetValue<ComplexContainer>(1);
        Assert.Equal("K2", item2.Info.Key);
    }
    
    [Fact]
    public void Test_GetValue_List()
    {
        var data = new List<List<int>>
        {
            new() { 1, 2 },
            new() { 3 }
        };
        using var s = Series.From("list", data);

        var list0 = s.GetValue<List<int>>(0);
        Assert.Equal(2, list0.Count);
        Assert.Equal(2, list0[1]);
    }
    [Fact]
    [Trait("DataType","TimeZone")]
    public void Test_TimeZone_Operations_EndToEnd()
    {
        // 2023-01-01 10:00:00
        var dt = new DateTime(2023, 1, 1, 10, 0, 0);
        
        using var df = DataFrame.FromColumns(new 
        {
            ts = new[] { dt } 
        });

        // --- ReplaceTimeZone (Naive -> Asia/Shanghai) ---
        
        using var df1 = df.Select(
            Pl.Col("ts")
                .Dt
                .ReplaceTimeZone("Asia/Shanghai")
                .Alias("ts_shanghai")
        );

        var schema1 = df1.Schema["ts_shanghai"];
        Assert.Equal(DataTypeKind.Datetime, schema1.Kind);
        Assert.Equal("Asia/Shanghai", schema1.TimeZone);

        object valReplace = df1["ts_shanghai"][0];

        Assert.IsType<DateTimeOffset>(valReplace); 
        var dtoReplace = (DateTimeOffset)valReplace;

        Assert.Equal(10, dtoReplace.Hour); 
        Assert.Equal(TimeSpan.FromHours(8), dtoReplace.Offset);
        
        // --- ConvertTimeZone (Asia/Shanghai -> UTC) ---

        using var df2 = df1.Select(
            Pl.Col("ts_shanghai")
            .Dt
            .ConvertTimeZone("UTC")
            .Alias("ts_utc")
        );

        var schema2 = df2.Schema["ts_utc"];
        Assert.Equal("UTC", schema2.TimeZone);

        var valUtc = (DateTimeOffset)df2["ts_utc"][0];
        Assert.Equal(2, valUtc.Hour); 


        // --- Naive -> UTC -> Shanghai ---
        
        using var df3 = df.Select(
            Pl.Col("ts").Dt
            .ReplaceTimeZone("UTC").Dt         
            .ConvertTimeZone("Asia/Shanghai") 
            .Alias("ts_converted")
        );

        var schema3 = df3.Schema["ts_converted"];
        Assert.Equal("Asia/Shanghai", schema3.TimeZone);

        var valConverted = df3["ts_converted"][0];

        using var dfCheck = df3.Select(
            Pl.Col("ts_converted").Dt.Hour().Alias("h")
        );

        var hour = dfCheck["h"][0];
        Assert.Equal((sbyte)18, hour);

        // --- Remove TimeZone (Aware -> Naive) ---
        
        using var df4 = df3.Select(
            Pl.Col("ts_converted").Dt
            .ReplaceTimeZone(null) 
            .Alias("ts_naive")
        );

        var schema4 = df4.Schema["ts_naive"];
        Assert.Equal("",schema4.TimeZone); 
        
        var valNaive = (DateTime)df4["ts_naive"][0];
        Assert.Equal(18, valNaive.Hour);
    }
    [Fact]
    public void Test_DataType_Array()
    {
        var dtype = DataType.Array(DataType.Int32, 3);

        Assert.Equal(DataTypeKind.Array, dtype.Kind);
        
        Assert.Equal(3UL, dtype.ArrayWidth);
        
        Assert.Equal(DataType.Int32, dtype.InnerType);
    }
    [Fact]
    public void Test_Float_Double_Half_Resolution()
    {

        var sF64 = new Series("f64", [1.1, 2.2, null]); 
        Assert.Equal(DataType.Float64, sF64.DataType); 
        Assert.Equal(1.1, sF64.GetValue<double?>(0));
        Assert.Null(sF64.GetValue<double?>(2));
        
        var sF32 = new Series("f32", [1.1f, 2.2f, null]);
        Assert.Equal(DataType.Float32, sF32.DataType);
        Assert.Equal(1.1f, sF32.GetValue<float?>(0));
        Assert.Null(sF32.GetValue<float?>(2));

        var sF16 = new Series("f16", [(Half)1.1f, (Half)2.2f, null]);

        Assert.Equal(DataType.Float16, sF16.DataType);

        Assert.Equal((Half)1.1f, sF16.GetValue<Half?>(0));
        Assert.Equal((Half)2.2f, sF16.GetValue<Half?>(1));
        Assert.Null(sF16.GetValue<Half?>(2));

    }

    [Fact]
    [Trait("DataType","TinyInteger")]
    public void Test_Tiny_Integers_i8_u8_i16_u16()
    {
        // --- i8 (SByte) [-128, 127] ---
        sbyte?[] i8Array = [-5,127,null];
        var sI8 = Series.From("i8",i8Array);
        Assert.Equal((sbyte)-5, sI8[0]);
        Assert.Equal((sbyte)127, sI8[1]);

        // --- u8 (Byte) [0, 255] ---

        byte[] u8Raw = [10, 255]; 
        var sU8 = Series.From("u8", u8Raw); 
        Assert.Equal((byte)255,sU8[1]);

        var sU8_Null = new Series("u8_n", [10, 255, null]);
        Assert.Equal((byte)255, sU8_Null.GetValue<byte?>(1));

        // --- i16 (Short) ---
        var sI16 = new Series("i16", [-30000, null, 30000]);
        Assert.Equal((short)-30000, sI16.GetValue<short?>(0));

        // --- u16 (UShort) ---
        var sU16 = new Series("u16", [60000, null, 0]);
        Assert.Equal((ushort)60000, sU16.GetValue<ushort?>(0));
    }

    [Fact]
    public void Test_Large_Unsigned_Integers_u32_u64()
    {
        // --- u32 (UInt) ---
        uint bigUInt = 3_000_000_000u; 
        var sU32 = new Series("u32", [bigUInt, null, 0u]);
        
        Assert.Equal(bigUInt, sU32.GetValue<uint?>(0));

        // --- u64 (ULong) ---
        ulong hugeULong = 10_000_000_000_000_000ul; 
        var sU64 = new Series("u64", [hugeULong, null, 123ul]);
        
        Assert.Equal(hugeULong, sU64.GetValue<ulong?>(0));
    }
    [Fact]
    public void Test_Float16_Million_Rows_SIMD_Stress()
    {
        const int RowCount = 1_000_000;
        
        var rawData = new Half?[RowCount];
        
        for (int i = 0; i < RowCount; i++)
        {
            if (i % 100 == 0)
                rawData[i] = null;
            else
                rawData[i] = (Half)(i % 100);
        }

        Console.WriteLine($"Starting ingestion of {RowCount:N0} f16 rows (SIMD Check)...");

        var sw = Stopwatch.StartNew();
        
        using var s = Series.From("f16_million", rawData);
        
        sw.Stop();
        
        Console.WriteLine($"Ingestion took: {sw.Elapsed.TotalMilliseconds:F2} ms");
        Console.WriteLine($"Throughput: {RowCount / sw.Elapsed.TotalSeconds / 1_000_000:F2} M rows/sec");

        Assert.Equal(RowCount, s.Length);
        Assert.Equal("f16_million", s.Name);
        
        Assert.Equal(DataType.Float16, s.DataType); 
        
        Assert.Equal(10_000, s.NullCount);

        // Index 0 Null
        Assert.Null(s.GetValue<Half?>(0));
        
        // Index 1 1.0
        Assert.Equal((Half)1, s.GetValue<Half?>(1));
        
        // Index 99 99.0
        Assert.Equal((Half)99, s.GetValue<Half?>(99));
        
        // Index 100 Null
        Assert.Null(s.GetValue<Half?>(100));

        double? sum = s.Cast<double>().Sum<double>(); 
        
        Assert.Equal(49_500_000.0, sum);
    }
    [Fact]
    public void Test_Edge_Case_Mixed_Numeric_Types()
    {
        
        sbyte[] smallData = [1, 2, 3];
        
        var s = new Series("sbyte", smallData);

        Assert.Equal((sbyte)1, s.GetValue<sbyte>(0));
    }
    [Fact]
    [Trait("Series","Int128")]
    public void Test_Int128_Beyond_Int64_Range()
    {
        Int128 bigVal = (Int128)1 << 100; 
        
        Assert.True(bigVal > long.MaxValue);

        var s = Series.FromSpan("big_i128",new Int128?[] {bigVal, -bigVal, null}.AsSpan());

        Assert.Equal(bigVal, s.GetValue<Int128?>(0));
        Assert.Equal(-bigVal, s.GetValue<Int128?>(1));
        Assert.Null(s.GetValue<Int128?>(2));

        Assert.Throws<NotSupportedException>(s.ToArrow);
    }

    [Fact]
    public void Test_UInt128_Max_Value()
    {
        // UInt128 Max：340282366920938463463374607431768211455
        UInt128 maxVal = UInt128.MaxValue;
        
        var s = new Series("max_u128", [maxVal, UInt128.MinValue,null]);

        Assert.Equal(maxVal, s.GetValue<UInt128?>(0));
        Assert.Equal(UInt128.Zero, s.GetValue<UInt128?>(1));
        Assert.Null(s.GetValue<UInt128?>(2));
    }

    [Fact]
    public void Test_Int128_Span_Optimization()
    {
        Int128?[] data = [1, null, 2];
        
        var s = Series.From("opt_test", data);
        
        Assert.Equal((Int128)1, s.GetValue<Int128?>(0));
        Assert.Null(s.GetValue<Int128?>(1));
    }
    [Fact]
    public void Test_Empty_Arrays_Preserve_Schema()
    {
        // Int32 Empty
        int[] emptyInt = [];
        using var sInt = Series.FromSpan("empty_int", emptyInt.AsSpan());
        Assert.Equal(0, sInt.Length);

        // DateTime Empty 
        DateTime?[] emptyDt = [];
        using var sDt = Series.From("empty_dt", emptyDt);
        Assert.Equal(0, sDt.Length);

        // String Empty
        string[] emptyStr = [];
        using var sStr = Series.From("empty_str", emptyStr);
        Assert.Equal(0, sStr.Length);
        
        // DataFrame Schema Alignment
        var df = new DataFrame(sInt, sDt, sStr);
        Assert.Equal((0,3), df.Shape);
        
        var schema = df.Schema;
        Assert.Equal(DataType.Int32, schema["empty_int"]);
        Assert.Equal(DataType.Datetime(TimeUnit.Microseconds), schema["empty_dt"]);
        Assert.Equal(DataType.String, schema["empty_str"]);
    }
    [Fact]
    public void Test_DateTime_Array_Large_Scale_Accuracy()
    {
        int count = 1_000_000;
        var dateArray = new DateTime[count];
        var start = new DateTime(2025, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        
        for (int i = 0; i < count; i++)
        {
            var dt = start.AddSeconds(i); 
            
            if (i % 2 == 0)
                dateArray[i] = DateTime.SpecifyKind(dt, DateTimeKind.Utc);
            else
                dateArray[i] = dt.ToLocalTime(); 
        }

        var sw = Stopwatch.StartNew();
        using var s = Series.From("large_dt", dateArray);
        sw.Stop();
        Console.WriteLine($"Series Created in: {sw.Elapsed.TotalMilliseconds} ms");

        Assert.Equal(count, s.Length);
        Assert.Equal(0, s.NullCount);

        CheckValue(s, dateArray, 0);
        CheckValue(s, dateArray, count / 2);
        CheckValue(s, dateArray, count - 1);

        var rng = new Random(42);
        for (int k = 0; k < 1000; k++)
        {
            int idx = rng.Next(0, count);
            CheckValue(s, dateArray, idx);
        }
    }

    private static void CheckValue(Series s, DateTime[] source, int index)
    {

        object val = s[index];
        
        Assert.NotNull(val);
        Assert.IsType<DateTime>(val);
        
        DateTime actual = (DateTime)val!;
        DateTime expected = source[index];
        
        long expectedTicks = expected.Ticks; 
        long actualTicks = actual.Ticks;

        long expectedUs = expectedTicks / 10;
        long actualUs = actualTicks / 10;

        if (expectedUs != actualUs)
        {
             Assert.Fail($"Mismatch at index {index}. \n" +
                         $"Expected: {expected} ({expected.Kind}, Ticks={expectedTicks})\n" +
                         $"Actual:   {actual} ({actual.Kind}, Ticks={actualTicks})");
        }
    }
    [Fact]
    public void Test_DateOnly_SIMD_Path()
    {
        int count = 2050;
        var data = new DateOnly[count];
        var start = new DateOnly(2000, 1, 1);

        for (int i = 0; i < count; i++)
        {
            data[i] = start.AddDays(i);
        }

        using var s = Series.From("simd_dates", data);

        Assert.Equal(count, s.Length);
        Assert.Equal(0, s.NullCount);

        // Simd Boundary Checks
        CheckIndex(s, data, 7);
        CheckIndex(s, data, 8);
        CheckIndex(s, data, 15);
        CheckIndex(s, data, 16);
        
        // Tail Loop
        CheckIndex(s, data, count - 1);
        CheckIndex(s, data, count - 2);

        var rng = new Random(42);
        for (int i = 0; i < 100; i++)
        {
            CheckIndex(s, data, rng.Next(0, count));
        }
    }

    private static void CheckIndex<T>(Series s, T[] expectedData, int index)
    {
        object actual = s[index];
        Assert.Equal(expectedData[index], actual);
    }
    [Fact]
    [Trait("DataType","TimeOnlySIMD")]
    public void Test_TimeOnly_SIMD_Path()
    {
        int count = 2050;
        var data = new TimeOnly[count];
        var start = new TimeOnly(0, 0, 0);

        for (int i = 0; i < count; i++)
        {
            data[i] = start.Add(TimeSpan.FromTicks(i * 10000000L + 1)); 
        }


        using var s = Series.From("simd_times", data);

        Assert.Equal(count, s.Length);

        CheckIndex(s, data, 0);
        CheckIndex(s, data, 7);  // AVX-512 end of first block? (0-7)
        CheckIndex(s, data, 8);  // Start of next?
        CheckIndex(s, data, 15);
        CheckIndex(s, data, 16);

        CheckIndex(s, data, count - 1);
    }
    [Fact]
    [Trait("DataType","TimeSpanILP")]
    public void Test_TimeSpan_ILP_Path()
    {
        int count = 2050;
        var data = new TimeSpan[count];

        for (int i = 0; i < count; i++)
        {
            data[i] = TimeSpan.FromTicks(i * 10); 
        }

        using var s = Series.From("ilp_durations", data);

        Assert.Equal(count, s.Length);

        CheckIndex(s, data, 7);
        CheckIndex(s, data, 8);
        CheckIndex(s, data, 15);
        CheckIndex(s, data, 16);

        CheckIndex(s, data, count - 1); // Tail
        CheckIndex(s, data, count - 2); // Tail
        CheckIndex(s, data, count - 3); // Main Loop End
    }
    [Fact]
    [Trait("DataType","DecimalMix")]
    public void Test_Decimal_Integration_MixedScale()
    {
        var data = new decimal?[]
        {
            1.5m,               // Scale 1 -> 5
            -2.123m,            // Scale 3 -> 5
            100m,               // Scale 0 -> 5
            0.00005m,           // Scale 5 (Max) 
            decimal.MaxValue,    
            decimal.MinValue,   
            null
        };

        using var s = Series.From("mixed_decimal", data);

        Assert.Equal(7, s.Length);
        Assert.Equal(1, s.NullCount);

        Assert.Equal(data[0], s[0]);
        Assert.Equal(data[1], s[1]);
        Assert.Equal(data[2], s[2]);
        Assert.Equal(data[3], s[3]);
        Assert.Equal(data[4], s[4]);
        Assert.Equal(data[5], s[5]);
        Assert.Null(s[6]);
    }
    [Fact]
    public void Test_Decimal_NonNullable_FastPath()
    {
        var data = new decimal[]
        {
            1234567890.1234567890m, // High Precision
            0.0000000000000000001m, // High Scale (19)
            -99.9m,                 // Negative
            0m                      // Zero
        };

        using var s = Series.From("fast_decimal", data);

        Assert.Equal(4, s.Length);
        Assert.Equal(0, s.NullCount);

        Assert.Equal(data[0], s[0]);
        Assert.Equal(data[1], s[1]);
        Assert.Equal(data[2], s[2]);
        Assert.Equal(data[3], s[3]);
    }
    [Fact]
    [Trait("DataType","DecimalStress")]
    public void Test_Decimal_LargeScale_Stress()
    {
        int count = 1_000_000;
        var data = new decimal[count];
        
        for (int i = 0; i < count; i++)
        {
            if (i % 2 == 0) data[i] = i;       // Scale 0
            else data[i] = i + 0.55m;          // Scale 2
        }
        ReadOnlySpan<decimal> dataSpan = data.AsSpan();

        var sw = Stopwatch.StartNew();
        using var s = Series.FromSpan("stress_decimal", dataSpan);
        sw.Stop();
        
        Console.WriteLine($"Decimal Packed {count} items in {sw.Elapsed.TotalMilliseconds} ms");
        Assert.Equal(count, s.Length);

        // Index 0: 0 -> 0.00
        Assert.Equal(0m, s[0]);
        // Index 1: 1.55 -> 1.55
        Assert.Equal(1.55m, s[1]);
        // Index 100: 100 -> 100.00
        Assert.Equal(100m, s[100]);
        // Index End
        Assert.Equal(data[count - 1], s[count - 1]);
        
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
        decimal extreme = 0.0000000000000000000000000001m; // 1e-28
        Assert.Equal(28, extreme.Scale);

        var data = new decimal[] { 1m, extreme };
        
        // MaxScale = 28
        
        using var s = new Series("limit_decimal", data);
        
        Assert.Equal(1m, s[0]);
        Assert.Equal(extreme, s[1]);
    }
    [Fact]
    public void Test_FixedSizeList_Double_Image()
    {
        double[,] pixels = new double[,] 
        { 
            { 0.1, 0.2 }, 
            { 0.8, 0.9 } 
        };

        using var s = new Series("pixels", pixels);

        Assert.Equal(2, s.Length);
        
        Console.WriteLine(s);  
    }
    [Fact]
    [Trait("DataType","Matrix")]
    public void Test_FixedSizeList_Performance_Large()
    {
        int size = 1000;
        double[,] largeMatrix = new double[size, size];
        
        largeMatrix[0, 0] = 1.0;
        largeMatrix[size-1, size-1] = 99.0;

        var sw = Stopwatch.StartNew();
        
        using var s = Series.From("large_matrix", largeMatrix);
        
        sw.Stop();
        Console.WriteLine($"Transferred 1,000,000 doubles (2D) in {sw.Elapsed.TotalMilliseconds} ms");

        Assert.Equal(size, s.Length);
    }
    [Fact]
    public void Test_FixedSizeList_Int128_Layout_Check()
    {
        Int128[,] data = new Int128[2, 2];
        
        data[0, 0] = 1; 
        
        Int128 highBit = (Int128)1 << 64; 
        data[0, 1] = highBit;

        data[1, 0] = Int128.MaxValue;

        data[1, 1] = -1;

        using var s = new Series("int128_matrix", data);

        Console.WriteLine($"Int128 Check Passed. Series: {s}");
    }
    [Fact]
    [Trait("DataType","DecimalMatrix")]
    public void Test_Decimal_Matrix_AutoScaling()
    {

        decimal[,] data = new decimal[2, 3] 
        {
            { 1.1m,      2.22m,     3.333m }, // Row 0: Max Scale 3
            { 100m,      0.00005m,  -1.5m  }  // Row 1: Max Scale 5 
        };
        
        // 3.333 -> Scale 3
        // 0.00005 -> Scale 5
        // Scale of the series will be 5
        // 1.1 -> 1.10000
        
        string name = "decimal_matrix";
        
        using var series = Series.From(name, data);

        Assert.Equal(DataType.Array(Pl.Decimal(38,5),3),series.DataType); 
        
        decimal[][] rows = series.ToArray<decimal[]>();

        Assert.Equal(2, rows.Length); 

        // Row 0 Check
        Assert.Equal(3, rows[0].Length); 
        Assert.Equal(1.1m, rows[0][0]);
        Assert.Equal(2.22m, rows[0][1]);
        Assert.Equal(3.333m, rows[0][2]);

        // Row 1 Check
        Assert.Equal(3, rows[1].Length); 
        Assert.Equal(100m, rows[1][0]);
        Assert.Equal(0.00005m, rows[1][1]);
        Assert.Equal(-1.5m, rows[1][2]);
    }

    [Fact]
    [Trait("DataType","DecimalOverflow")]
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

        Assert.Throws<OverflowException>(() => Series.From("huge_decimal", data));
    }
    [Fact]
    [Trait("DataType","Struct")]
    public void Test_LitStruct_Creation_And_Metadata_Extraction()
    {
        var inputObj = new 
        { 
            Id = 42, 
            Name = "Polars.NET", 
            Score = 99.5,
            IsActive = true 
        };

        using var df = new DataFrame()
            .Select(Pl.LitStruct(inputObj).Alias("my_struct"));

        var structType = df.Schema["my_struct"];
        var expectedType = Pl.Struct(
            Pl.Field("Id", Pl.Int32),
            Pl.Field("Name", Pl.String),
            Pl.Field("Score", Pl.Float64),
            Pl.Field("IsActive", Pl.Boolean)
        );

        Assert.Equal(expectedType, structType);
        Assert.Equal(4, structType.StructFields.Count);

        Assert.Equal("Id", structType.StructFields[0].Name);
        Assert.Equal(DataType.Int32, structType.StructFields[0].DataType);

        Assert.Equal("Name", structType.StructFields[1].Name);
        Assert.Equal(DataType.String, structType.StructFields[1].DataType);

        Assert.Equal("Score", structType.StructFields[2].Name);
        Assert.Equal(DataType.Float64, structType.StructFields[2].DataType);

        Assert.Equal("IsActive", structType.StructFields[3].Name);
        Assert.Equal(DataType.Boolean, structType.StructFields[3].DataType);

        using var unnestedDf = df.Unnest("my_struct");

        Assert.Equal(42, unnestedDf.GetValue<int>(0, "Id"));
        Assert.Equal("Polars.NET", unnestedDf.GetValue<string>(0, "Name"));
        Assert.Equal(99.5, unnestedDf.GetValue<double>(0, "Score"));
        Assert.True(unnestedDf.GetValue<bool>(0, "IsActive"));
    }
    [Fact]
    [Trait("DataType","128bytes")]
    public void Test_Int128_UInt128()
    {
        Expr exprI128 = Pl.Lit(Int128.MinValue).Alias("i128");
        var seriesI128 = Series.FromExpr(exprI128);
        Assert.Equal(Int128.MinValue,seriesI128[0]);
    }
    [Fact]
    [Trait("DataType", "Extension")]
    public void Test_DataType_Extension()
    {
        using DataType extIntType = DataType.Extension("my_ext.int", DataType.Int32);
        Assert.Equal(DataTypeKind.Extension, extIntType.Kind); 

        using DataType extGeoType = DataType.Extension("geoarrow.wkb", DataType.Binary, "{\"crs\":\"EPSG:4326\"}");
        Assert.Equal(DataTypeKind.Extension, extGeoType.Kind);

        int[] data = [1, 2, 3, 4, 5];
        using Series s = Pl.CreateSeries("values", data);

        using Series sExt = s.Ext.To(extIntType);

        Assert.Equal(extIntType, sExt.DataType);

        using DataFrame df = Pl.CreateDataFrame(sExt);
        Assert.Equal(1, df.Width);
        Assert.Equal(DataTypeKind.Extension, df.Schema["values"].Kind);

        Series sStorage = sExt.Ext.Storage();
        Assert.Equal(typeof(int),sStorage.DataType);
    }
    public sealed class UuidExtension : BaseExtension
    {
        public UuidExtension() : base("myapp.uuid", DataType.Binary) { }
    }

    [Fact]
    [Trait("DataType", "ExtensionRegistry")]
    public void Test_DataType_OOP_Registry_Interception()
    {
        Pl.RegisterExtensionType<UuidExtension>(
            "myapp.uuid", 
            (storage, metadata) => new UuidExtension()
        );

        Pl.RegisterExtensionType("myapp.transparent",asStorage:true);

        try
        {
            byte[][] uuidData = [[1, 2, 3], [4, 5, 6]]; 
            using Series sUuid = Pl.CreateSeries("uuids", uuidData)
                        .Cast(DataType.Binary) 
                        .Ext.To(new UuidExtension());
            using DataFrame df1 = Pl.CreateDataFrame(sUuid);

            DataType readType = df1.Schema["uuids"];
            Assert.IsType<UuidExtension>(readType); 
            
            var uuidType = (UuidExtension)readType;
            Assert.Equal("myapp.uuid", uuidType.ExtensionName);
            Assert.Equal(DataTypeKind.Binary, uuidType.Storage.Kind);

            using DataType transparentExt = DataType.Extension("myapp.transparent", DataType.Int64);
            using Series sTrans = Pl.CreateSeries("trans", [100L, 200L]).Ext.To(transparentExt);
            using DataFrame df2 = Pl.CreateDataFrame(sTrans);

            DataType readTransType = df2.Schema["trans"];
            Assert.IsNotType<BaseExtension>(readTransType, exactMatch: false); 
            Assert.Equal(DataType.Int64, readTransType);

            using DataType alienExt = DataType.Extension("alien.type", DataType.Float32, "{\"v\": 1}");
            using Series sAlien = Pl.CreateSeries("alien", [3.14f]).Ext.To(alienExt);
            using DataFrame df3 = Pl.CreateDataFrame(sAlien);

            DataType readAlienType = df3.Schema["alien"];
            Assert.IsType<UnknownExtension>(readAlienType);
            
            var alien = (UnknownExtension)readAlienType;
            Assert.Equal("alien.type", alien.ExtensionName);
            Assert.Equal("{\"v\": 1}", alien.Metadata);
            Assert.Equal(DataTypeKind.Float32, alien.Storage.Kind);
        }
        finally
        {
            Pl.UnregisterExtensionType("myapp.uuid");
            Pl.UnregisterExtensionType("myapp.transparent");
        }
    }
    [Fact]
    [Trait("API", "ExtensionRegistry")]
    public void Test_Pl_GetExtensionType_Returns_Correct_Info()
    {
        string classExtName = "test.dummy_class";
        string storageExtName = "test.dummy_storage";
        string missingExtName = "test.missing";

        ExtensionInfo missingInfo = Pl.GetExtensionType(missingExtName);
        Assert.IsType<ExtensionInfo.NotFound>(missingInfo);

        try
        {
            Pl.RegisterExtensionType<UuidExtension>(
                classExtName, 
                (storage, metadata) => new UuidExtension() 
            );

            ExtensionInfo classInfo = Pl.GetExtensionType(classExtName);
            
            var asClass = Assert.IsType<ExtensionInfo.AsClass>(classInfo);
            Assert.NotNull(asClass.Factory); 

            Pl.RegisterExtensionType(storageExtName, asStorage: true);
            
            ExtensionInfo storageInfo = Pl.GetExtensionType(storageExtName);
            Assert.IsType<ExtensionInfo.AsStorage>(storageInfo);
        }
        finally
        {
            Pl.UnregisterExtensionType(classExtName);
            Pl.UnregisterExtensionType(storageExtName);

            ExtensionInfo unregisteredClassInfo = Pl.GetExtensionType(classExtName);
            ExtensionInfo unregisteredStorageInfo = Pl.GetExtensionType(storageExtName);

            Assert.IsType<ExtensionInfo.NotFound>(unregisteredClassInfo);
            Assert.IsType<ExtensionInfo.NotFound>(unregisteredStorageInfo);
        }
    }
    public enum ProcessStatus
    {
        Pending,
        Running,
        Completed,
        Failed
    }

    public class EnumTestPoco
    {
        public string Status { get; set; } 
        public int TaskId { get; set; }
    }
    [Fact]
    [Trait("DataType", "Enum")]
    public void Test_DataFrame_CSharpEnum_To_PolarsEnum()
    {
        var data = new List<EnumTestPoco>
        {
            new() { TaskId = 101, Status = "Pending" },
            new() { TaskId = 102, Status = "Running" },
            new() { TaskId = 103, Status = "Completed" }
        };

        using var s = Series.From("tasks", data);
        using var df = DataFrame.FromSeries(s).Unnest("tasks");

        using var enumType = DataType.Enum<ProcessStatus>();
        using var dfEnum = df.Cast("Status",enumType);

        Assert.Equal(enumType, dfEnum.Schema["Status"]);
        Assert.Equal(DataType.Int32, dfEnum.Schema["TaskId"]);
        
        var castedEnumType = dfEnum.Schema["Status"];
        string[] expectedCategories = ["Pending", "Running", "Completed", "Failed"];
        Assert.Equal(expectedCategories, castedEnumType.EnumCategories.GetCategories());

        var rows = dfEnum.Rows<EnumTestPoco>().ToList();

        Assert.Equal(3, rows.Count);
        
        Assert.Equal(101, rows[0].TaskId);
        Assert.Equal("Pending", rows[0].Status);

        Assert.Equal(102, rows[1].TaskId);
        Assert.Equal("Running", rows[1].Status);

        Assert.Equal(103, rows[2].TaskId);
        Assert.Equal("Completed", rows[2].Status);
    }
    [Fact]
    [Trait("DataType", "MultidimensionalArray")]
    public void Test_Multidimensional_Array_Shape()
    {
        // 1D array
        var arr1d = DataType.Array(Pl.Int64, 5);
        Assert.Equal(new uint[] { 5 }, arr1d.ArrayShape);

        // 2D array
        var arr2d = DataType.Array(Pl.Int64, 2, 3);
        Assert.Equal(new uint[] { 2, 3 }, arr2d.ArrayShape);

        // 3D array
        var arr3d = DataType.Array(Pl.Float32, 4, 5, 6);
        Assert.Equal(new uint[] { 4, 5, 6 }, arr3d.ArrayShape);

        // Nested array built manually (Array inside Array)
        var nestedInner = DataType.Array(Pl.Utf8, 2);
        var nestedOuter = DataType.Array(nestedInner, 3);
        Assert.Equal(new uint[] { 3, 2 }, nestedOuter.ArrayShape);

        // Non-array type returns empty
        var plain = Pl.Utf8;
        Assert.Empty(plain.ArrayShape);
    }
}