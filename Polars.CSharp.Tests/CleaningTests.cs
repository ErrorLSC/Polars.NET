using Xunit;
using Polars.CSharp;
using static Polars.CSharp.Polars; // 方便使用 Col, Lit

namespace Polars.CSharp.Tests;

public class CleaningTests
{
    [Fact]
    public void Test_Forward_Backward_Fill()
    {
        // [关键] 绝对不要用 @"" 里的缩进空格
        // val
        // 1
        // null
        // null
        // 2
        // null
        var content = "val\n1\n\n\n2\n\n"; 
        
        using var csv = new DisposableCsv(content);
        using var df = DataFrame.ReadCsv(csv.Path);
        
        // Forward Fill (limit=null -> 0 -> Infinite)
        using var ff = df.Select(Col("val").ForwardFill().Alias("ff"));
        using var bff = ff.ToArrow();
        var col = bff.Column("ff");
        
        // 验证：
        // 1 (原值)
        // 1 (填充)
        // 1 (填充)
        // 2 (原值)
        // 2 (填充)
        Assert.Equal(1, col.GetInt64Value(0));
        Assert.Equal(1, col.GetInt64Value(1)); 
        Assert.Equal(1, col.GetInt64Value(2)); 
        Assert.Equal(2, col.GetInt64Value(3));
        Assert.Equal(2, col.GetInt64Value(4)); 
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
        
        using var csv = new DisposableCsv(content);
        using var df = DataFrame.ReadCsv(csv.Path);

        // --- 1. FillNull ---
        using var filledDf = df.WithColumns(
            Col("A").FillNull(0), 
            Col("B").FillNull("unknown")
        );
        
        using var batchFilled = filledDf.ToArrow();
        Assert.Equal(0, batchFilled.Column("A").GetInt64Value(1)); // null -> 0
        Assert.Equal("unknown", batchFilled.Column("B").GetStringValue(2)); // null -> unknown

        // --- 2. DropNulls ---
        using var dfDirty = DataFrame.ReadCsv(csv.Path);
        using var droppedDf = dfDirty.DropNulls();
        
        // Row 0: 1, x, 10 (完整) -> 保留
        // Row 1: null, y, 20 -> 删
        // Row 2: 3, null, 30 -> 删
        Assert.Equal(1, droppedDf.Height); 
        Assert.Equal(1, droppedDf.ToArrow().Column("A").GetInt64Value(0));
    }
}