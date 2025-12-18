namespace Polars.CSharp.Tests;

public class ScalarTests
{
    [Fact]
    public void Test_Direct_Scalar_Access_All_Types()
    {
        // 1. 准备全类型数据
        var now = DateTime.UtcNow;
        // Truncate to ms/us precision to match Polars (us) behavior
        now = new DateTime(now.Ticks - (now.Ticks % 10), DateTimeKind.Utc); 
        
        var date = DateOnly.FromDateTime(now);
        var time = new TimeOnly(12, 30, 0, 100); // 12:30:00.100
        var duration = TimeSpan.FromHours(1.5) + TimeSpan.FromMicroseconds(50); // 1.5h + 50us

        // 使用新增的构造函数 (解决编译错误的关键)
        using var sInt = new Series("i", [100]);
        using var sFloat = new Series("f", [1.23]);
        using var sStr = new Series("s", ["hello"]);
        using var sBool = new Series("b", [true]);
        using var sDec = new Series("d", [123.456m]); 
        
        // 时间类型
        using var sDt = new Series("dt", [now]);
        using var sDate = new Series("date", [date]);
        using var sTime = new Series("time", [time]);
        using var sDur = new Series("dur", [duration]);
        
        // 创建 DF (通过 Series 数组构造)
        using var df = new DataFrame([
            sInt, sFloat, sStr, sBool, 
            sDec, sDt, sDate, sTime, sDur
        ]);

        // 2. 验证 DataFrame GetValue<T> (Direct Access)
        
        // Basic
        Assert.Equal(100, df.GetValue<int>(0, "i"));
        Assert.Equal(1.23, df.GetValue<double>(0, "f"));
        Assert.Equal("hello", df.GetValue<string>(0, "s"));
        Assert.True(df.GetValue<bool>(0, "b"));
        Assert.Equal(123.456m, df.GetValue<decimal>(0, "d"));
        
        // DateTime (UTC ticks check)
        var dtOut = df.GetValue<DateTime>(0, "dt");
        Assert.Equal(now.ToUniversalTime().Ticks, dtOut!.Ticks);
        
        // DateOnly
        Assert.Equal(date, df.GetValue<DateOnly>(0, "date"));
        
        // TimeOnly
        Assert.Equal(time, df.GetValue<TimeOnly>(0, "time"));
        
        // Duration (TimeSpan)
        Assert.Equal(duration, df.GetValue<TimeSpan>(0, "dur"));

        // 3. 验证索引器 (ToString 检查类型)
        // 索引器返回的是 object，对于 Date/Time 等类型可能需要拆箱
        Assert.IsType<TimeSpan>(df[0, "dur"]);
        Assert.IsType<DateOnly>(df[0, "date"]);
    }
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
}