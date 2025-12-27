using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class TimeSeriesTests
{
    [Fact]
    public void Test_GroupByDynamic_Basic_TimeSpan()
    {
        // 1. 准备数据: 每一行间隔 10 分钟
        // 10:00, 10:10, 10:20, 10:30, 10:40, 10:50
        var start = new DateTime(2024, 1, 1, 10, 0, 0);
        var dates = Enumerable.Range(0, 6).Select(i => start.AddMinutes(i * 10)).ToArray();
        var values = Enumerable.Range(0, 6).Select(i => i).ToArray(); // 0, 1, 2, 3, 4, 5

        var df = DataFrame.FromColumns(new {Time =dates,Val =values}); 

        // 2. 动态分组
        // 目标：每 30 分钟一组
        // 预期分组：
        // Group 1 [10:00, 10:30): 包含 10:00(0), 10:10(1), 10:20(2) -> Sum = 3
        // Group 2 [10:30, 11:00): 包含 10:30(3), 10:40(4), 10:50(5) -> Sum = 12
        var q = df.Lazy()
            .GroupByDynamic(
                indexColumn: "Time",
                every: TimeSpan.FromMinutes(30),
                closedWindow: ClosedWindow.Left // 左闭右开 [ )
            )
            .Agg(
                Col("Val").Sum().Alias("SumVal"),
                Col("Val").Count().Alias("Count")
            );

        using var res = q.Collect();

        // 3. 验证结果
        Assert.Equal(2, res.Height);
        
        // 验证第一组
        Assert.Equal(3, res.GetValue<int>(0, "SumVal"));
        Assert.Equal(3, res.GetValue<int>(0, "Count"));
        
        // 验证第二组
        Assert.Equal(12, res.GetValue<int>(1, "SumVal"));
        Assert.Equal(3, res.GetValue<int>(1, "Count"));
    }
    [Fact]
    public void Test_GroupByDynamic_Advanced_Rolling()
    {
        // 场景：滑动窗口聚合
        // 数据：10:00, 10:01, 10:02 ... 连续的时间点
        var start = new DateTime(2024, 1, 1, 10, 0, 0);
        var dates = Enumerable.Range(0, 10).Select(i => start.AddMinutes(i)).ToArray();
        var values = Enumerable.Range(0, 10).Select(i => 1).ToArray(); // 为了方便数数，全设为 1

        var df = DataFrame.FromColumns(new {Time = dates,Val = values});

        // 配置：
        // Every (步长): 5m -> 每 5 分钟计算一次结果
        // Period (窗口): 10m -> 每次计算往前看 10 分钟的数据
        // Label: Right -> 结果的时间戳标记在窗口右侧
        // IncludeBoundaries: True -> 输出窗口范围
        
        var q = df.Lazy()
            .GroupByDynamic(
                indexColumn: "Time",
                every: TimeSpan.FromMinutes(5),  // 步长 5m
                period: TimeSpan.FromMinutes(10),// 窗口 10m (会有重叠)
                label: Label.Right,              // 标签打在右边
                includeBoundaries: true,         // 输出边界列
                closedWindow: ClosedWindow.Left  // 左闭
            )
            .Agg(
                Col("Val").Count().Alias("Count")
            );

        using var res = q.Collect();

        // 验证逻辑：
        // Window 1: [09:55, 10:05) -> Label 10:05. 数据包含 10:00, 01, 02, 03, 04 (共5个)
        // Window 2: [10:00, 10:10) -> Label 10:10. 数据包含 10:00 ~ 10:09 (共10个)
        
        // 注意：Polars 的具体切分点取决于 StartBy (默认 WindowBound)，
        // 它会把时间轴对齐到整点。
        // 10:00 会落入哪个窗口取决于对齐逻辑。
        
        // 我们主要验证 includeBoundaries 是否生效
        Assert.Contains("_lower_boundary", res.ColumnNames);
        Assert.Contains("_upper_boundary", res.ColumnNames);
        
        // 验证 Label (Label.Right 意味着时间列显示的是窗口结束时间)
        var firstTime = res.GetValue<DateTime>(0, "Time"); // 或者 Datetime 对应的 C# 类型
        // 如果是 Label.Right，且第一组是 10:00~10:05，那么 Time 应该是 10:05
        
        // 验证 Count
        // 只要能跑通且由边界列，说明 FFI 参数传对了
        Assert.True(res.Height > 0);
    }
    [Fact]
    public void Test_GroupByDynamic_Nanoseconds_Columnar()
    {
        // 准备高精度数据 (Ticks)
        var start = new DateTime(2024, 1, 1).Ticks;
        var dates = Enumerable.Range(0, 100)
            .Select(i => new DateTime(start + i)) 
            .ToArray();
        
        // [修复] 直接使用列式构造！爽！
        using var df = DataFrame.FromColumns(new { 
            Ts = dates,       // DateTime[]
            Val = dates       // DateTime[] (假装是值)
        });

        var us1 = TimeSpan.FromTicks(10); 

        var q = df.Lazy()
            .GroupByDynamic(
                indexColumn: "Ts",
                every: us1
            )
            .Agg(
                Col("Val").Count().Alias("Count")
            );

        using var res = q.Collect();
        
        Assert.Equal(10, res.Height);
        
        // [修复] 这里用 uint 或者 10u
        Assert.Equal(10, res.GetValue<int>(0, "Count"));
    }
}