using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public class AsyncTests
{
    [Fact]
    [Trait("Async","IO")]
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
    [Trait("Async","Collect")]
    public async Task Test_Async_Scan_And_Collect()
    {
        using var csv = new DisposableFile("name,score\nAlice,99\nBob,59\n",".csv");
        using var lf = LazyFrame.ScanCsv(csv.Path);
        
        var passExpr = Pl.Col("score")
            .Map<long, string>(s => s >= 60 ? "Pass" : "Fail")
            .Alias("status");

        // Async Collect
        using var res = await lf.Select(Pl.Col("name"), passExpr).CollectAsync();

        Assert.Equal(2, res.Height);
        
        Assert.Equal("Pass", res.GetValue<string>(0,"status")); // Alice
        Assert.Equal("Fail", res.GetValue<string>(1,"status")); // Bob
    }
    [Fact]
    [Trait("Async", "CollectBatches")]
    public async Task Test_LazyFrame_CollectBatchesAsync_StreamsDataFramesSuccessfully()
    {
        // Arrange
        using var df = DataFrame.FromColumns([
            Series.From("Id", Enumerable.Range(0, 100).ToArray()),
            Series.From("Value", Enumerable.Range(0, 100).Select(i => i * 1.5).ToArray())
        ]);

        using var lazyDf = df.Lazy();
        long totalRows = 0;
        int batchCount = 0;

        // Act
        await foreach (var batch in lazyDf.CollectBatchesAsync(chunkSize: 25))
        {
            using (batch) // Ensure unmanaged batch memory is released immediately
            {
                batchCount++;
                totalRows += batch.Height;
                Assert.True(batch.Height > 0);
            }
        }

        // Assert
        Assert.Equal(100, totalRows);
        Assert.True(batchCount > 0);
    }

    [Fact]
    [Trait("Async", "CollectBatches")]
    public async Task Test_LazyFrame_CollectBatchesAsync_Cancellation_ThrowsOperationCanceledException()
    {
        // Arrange
        using var df = DataFrame.FromColumns([
            Series.From("Id", [1, 2, 3])
        ]);

        using var lazyDf = df.Lazy();
        using var cts = new CancellationTokenSource();
        cts.Cancel(); // Pre-cancelled token

        // Act & Assert
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in lazyDf.CollectBatchesAsync(cancellationToken: cts.Token))
            {
                // Should not be entered
            }
        });
    }
}