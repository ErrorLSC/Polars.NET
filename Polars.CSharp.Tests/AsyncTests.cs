using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class AsyncTests
{
    [Fact]
    public async Task Test_Async_IO_And_Execution()
    {
        using var csv = new DisposableFile("id,val\n1,10\n2,20\n3,30\n", ".csv");

        using var df = await DataFrame.ReadCsvAsync(csv.Path);

        Assert.Equal(3, df.Height);
        Assert.Equal(2, df.Width);

        // Filter(val > 15) -> Select(id)
        using var lf = LazyFrame.ScanCsv(new DisposableFile("id,val\n1,10\n2,20\n3,30",".csv").Path);
        
        var query = lf
            .Filter(Pl.Col("val") > 15)
            .Select(Pl.Col("id"));

        using var resultDf = await query.CollectAsync();

        Assert.Equal(2, resultDf.Height); 
        Assert.Equal(2, resultDf.GetValue<int>(0,"id"));
        Assert.Equal(3, resultDf.GetValue<int>(1,"id"));
    }

    [Fact]
    public async Task Test_Async_Scan_And_Collect()
    {
        using var csv = new DisposableFile("name,score\nAlice,99\nBob,59\n",".csv");
        using var lf = LazyFrame.ScanCsv(csv.Path);
        
        var passExpr = Pl.Col("score")
            .Map<long, string>(s => s >= 60 ? "Pass" : "Fail", DataType.String)
            .Alias("status");

        // Async Collect
        using var res = await lf.Select(Pl.Col("name"), passExpr).CollectAsync();

        Assert.Equal(2, res.Height);
        
        Assert.Equal("Pass", res.GetValue<string>(0,"status")); // Alice
        Assert.Equal("Fail", res.GetValue<string>(1,"status")); // Bob
    }
}