using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class AsyncTests
{
    [Fact]
    public async Task Test_Async_IO_And_Execution()
    {
        // 1. 准备数据
        using var csv = new DisposableFile("id,val\n1,10\n2,20\n3,30\n", ".csv");

        // 2. 测试 DataFrame.ReadCsvAsync
        using var df = await DataFrame.ReadCsvAsync(csv.Path);

        Assert.Equal(3, df.Height);
        Assert.Equal(2, df.Width);

        // 3. 构造 Lazy 查询
        // 逻辑: Filter(val > 15) -> Select(id)
        using var lf = LazyFrame.ScanCsv(new DisposableFile("id,val\n1,10\n2,20\n3,30",".csv").Path);
        
        var query = lf
            .Filter(Col("val") > Lit(15))
            .Select(Col("id"));

        // 4. 测试 LazyFrame.CollectAsync
        // 这里模拟一个耗时操作的等待
        using var resultDf = await query.CollectAsync();

        // 5. 验证结果
        Assert.Equal(2, resultDf.Height); // 20, 30 符合条件
        
        Assert.Equal(2, resultDf.GetValue<int>(0,"id"));
        Assert.Equal(3, resultDf.GetValue<int>(1,"id"));
    }

    [Fact]
    public async Task Test_Async_Scan_And_Collect()
    {
        // 测试 Scan (Lazy Read) + Async Collect
        // Scan 本身通常很快（只读元数据），但 Collect 会触发实际读取
        
        using var csv = new DisposableFile("name,score\nAlice,99\nBob,59\n",".csv");
        
        // Scan 是同步的 (因为它只建立计划，不读数据)
        using var lf = LazyFrame.ScanCsv(csv.Path);
        
        // 复杂的 UDF 逻辑 (确保 Async 也能带着 UDF 跑)
        var passExpr = Col("score")
            .Map<long, string>(s => s >= 60 ? "Pass" : "Fail", DataType.String)
            .Alias("status");

        // Async Collect
        using var res = await lf.Select(Col("name"), passExpr).CollectAsync();

        Assert.Equal(2, res.Height);
        
        Assert.Equal("Pass", res.GetValue<string>(0,"status")); // Alice
        Assert.Equal("Fail", res.GetValue<string>(1,"status")); // Bob
    }
}