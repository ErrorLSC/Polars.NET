namespace Polars.CSharp.Tests;

public class ScalarTests
{
    [Fact]
    public void Test_Direct_Scalar_Access_All_Types_Pro_Max()
    {
        // 1. 准备全类型数据
        var now = DateTime.UtcNow;
        // [重要] Polars/Arrow 精度是微秒 (us)，.NET 是 100ns。
        // 为了 Assert Equal，我们需要手动截断 .NET 时间的最后一位小数
        // 1 us = 10 ticks
        now = new DateTime(now.Ticks - (now.Ticks % 10), DateTimeKind.Utc); 
        
        var date = DateOnly.FromDateTime(now);
        var time = new TimeOnly(12, 30, 0, 100); // 12:30:00.100
        var duration = TimeSpan.FromHours(1.5) + TimeSpan.FromMicroseconds(50); 

        // 2. 创建 DF
        // 直接在构造函数里 new Series，防止外部变量 Dispose 导致的所有权问题
        using var df = new DataFrame([
            new Series("i", new int[]{100}),
            new Series("f", new double[]{1.23}),
            new Series("s", new string[]{"hello"}),
            new Series("b", new bool[]{true}),
            new Series("d", new decimal[]{123.456m}),
            new Series("dt", new DateTime[]{now}),
            new Series("date", new DateOnly[]{date}),
            new Series("time", new TimeOnly[]{time}),
            new Series("dur", new TimeSpan[]{duration})
        ]);

        // 3. 验证 DataFrame GetValue<T> (Direct Access)
        
        // --- Primitives ---
        Assert.Equal(100, df.GetValue<int>(0, "i"));
        Assert.Equal(1.23, df.GetValue<double>(0, "f"));
        Assert.Equal("hello", df.GetValue<string>(0, "s"));
        Assert.True(df.GetValue<bool>(0, "b"));
        
        // --- Decimal ---
        Assert.Equal(123.456m, df.GetValue<decimal>(0, "d"));
        
        // --- DateTime (Naive ticks check) ---
        // 我们之前修好了 ArrowReader，现在应该能直接读出 DateTime (Naive)
        var dtOut = df.GetValue<DateTime>(0, "dt");
        Assert.Equal(now.Ticks, dtOut.Ticks);
        Assert.Equal(DateTimeKind.Unspecified, dtOut.Kind); // 确保Naive Time
        
        // --- DateOnly ---
        Assert.Equal(date, df.GetValue<DateOnly>(0, "date"));
        
        // --- TimeOnly ---
        Assert.Equal(time, df.GetValue<TimeOnly>(0, "time"));
        
        // --- Duration (TimeSpan) ---
        Assert.Equal(duration, df.GetValue<TimeSpan>(0, "dur"));

        // 4. 验证索引器 (object 返回值类型检查)
        // 索引器返回的是 object，我们需要验证它是否被正确拆箱为强类型
        
        // DateTimeOffset 自动处理验证 (Series indexer 对于 Datetime 返回 DateTimeOffset)
        Assert.IsType<DateTime>(df[0, "dt"]); 
        Assert.IsType<Boolean>(df[0,"b"]);
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

        // ==========================================
        // 旧写法 (虽然强类型，但有点啰嗦)
        // ==========================================
        Assert.Equal("Alice", df.GetValue<string>(0, "Name"));

        // ==========================================
        // 新写法 (DataTable 风格)
        // ==========================================
        
        // 1. [行, 列名] - 最常用的
        // 就像 targetTable.Rows[0]["Name"]，但更短！
        Assert.Equal("Alice", df[0, "Name"]); 
        
        // 2. [行, 列索引]
        Assert.Equal(1, df[0, 0]); // Id

        // 3. 链式写法: df["Name"][0]
        // 这也很直观：先取列，再取第几行
        Assert.Equal("Bob", df["Name"][1]);

        // 4. 类型转换 (和 DataTable 一样，取出来是 object，需要强转)
        // 如果你需要 int，直接 cast，C# 会自动拆箱
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
            // 默认是 Microseconds, No Timezone
            Ts = new[] { DateTime.Parse("2024-01-01 12:00:00") } 
        });

        // 1. 定义目标类型：毫秒精度 + 东京时间
        var targetType = DataType.Datetime(TimeUnit.Milliseconds, "Asia/Tokyo");

        // 2. 使用 Cast
        var res = df.Select(
            Polars.Col("Ts")
                .Cast(targetType) // <--- 关键用法！
                .Alias("Ts_Tokyo")
        );
        
        // 验证
        // 打印 Schema 看看类型是不是变了
        Console.WriteLine(res.Schema["Ts_Tokyo"]); // output should indicate datetime[ms, Asia/Tokyo]
    }
}