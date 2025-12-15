using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class CleaningTests
{
    [Fact]
    public void Test_Forward_Backward_Fill()
    {
        var content = "val\n1\n\n\n2\n\n"; 
        
        using var csv = new DisposableFile(content, ".csv");
        using var df = DataFrame.ReadCsv(csv.Path);
        
        // Forward Fill (limit=null -> 0 -> Infinite)
        using var ff = df.Select(Col("val").ForwardFill().Alias("ff"));
        
        // 验证：
        // 1 (原值)
        // 1 (填充)
        // 1 (填充)
        // 2 (原值)
        // 2 (填充)
        Assert.Equal(1, ff.GetValue<int>(0,"ff"));
        Assert.Equal(1, ff.GetValue<int>(1,"ff")); 
        Assert.Equal(1, ff.GetValue<int>(2,"ff")); 
        Assert.Equal(2, ff.GetValue<int>(3,"ff"));
        Assert.Equal(2, ff.GetValue<int>(4,"ff")); 
    }
    [Fact]
    public void Test_Sampling()
    {
        // 准备 100 行数据
        var rows = Enumerable.Range(0, 100).Select(i => new { Val = i });
        using var df = DataFrame.From(rows);
        Assert.Equal(100, df.Height);

        // Sample N=10
        using var sampleN = df.Sample(n: 10, seed: 42);
        Assert.Equal(10, sampleN.Height);

        // Sample Frac=0.1 (10%)
        using var sampleFrac = df.Sample(fraction: 0.1, seed: 42);
        // 大约 10 行，具体取决于算法，但在 100 行这种小数据量下，Fixed Fraction 通常准确
        Assert.Equal(10, sampleFrac.Height);
    }
    [Fact]
    public void Test_Data_Cleaning_Trio()
    {
        // [关键] 无缩进 CSV
        var content = "A,B,C\n1,x,10\n,y,20\n3,,30\n";
        
        using var csv = new DisposableFile(content, ".csv");
        using var df = DataFrame.ReadCsv(csv.Path);

        // --- 1. FillNull ---
        using var filledDf = df.WithColumns(
            Col("A").FillNull(0), 
            Col("B").FillNull("unknown")
        );
        
        Assert.Equal(0, filledDf.GetValue<int>(1,"A")); // null -> 0
        Assert.Equal("unknown", filledDf.GetValue<string>(2,"B")); // null -> unknown

        // --- 2. DropNulls ---
        using var dfDirty = DataFrame.ReadCsv(csv.Path);
        using var droppedDf = dfDirty.DropNulls();
        
        // Row 0: 1, x, 10 (完整) -> 保留
        // Row 1: null, y, 20 -> 删
        // Row 2: 3, null, 30 -> 删
        Assert.Equal(1, droppedDf.Height); 
        Assert.Equal(1, droppedDf.GetValue<int>(0,"A"));
    }
}