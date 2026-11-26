using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

public record AoRow(long Id, long Twice);

/// <summary>
/// The asynchronous batch and row streams give the query's rows in order by default (maintainOrder = true, as Polars'
/// default), also when the streaming engine works on many morsels side by side.
/// </summary>
public class AsyncOrderTests
{
    private const int Rows = 2_000_000;

    private static LazyFrame Query(DataFrame df) =>
        df.Lazy().Filter(Col("Id") % 3 != 0).Select(Col("Id"), (Col("Id") * 2).Alias("Twice"));

    private static long[] Expected() => [.. Enumerable.Range(0, Rows).Where(i => i % 3 != 0).Select(i => (long)i)];

    [Fact]
    public async Task Batches_And_Rows_Come_In_The_Querys_Order()
    {
        using var df = DataFrame.FromSeries(Series.From("Id", [.. Enumerable.Range(0, Rows).Select(i => (long)i)]));
        long[] expected = Expected();

        List<long> batched = [];
        using (var query = Query(df))
        {
            await foreach (var batch in query.CollectBatchesAsync())
                using (batch) batched.AddRange(batch["Id"].ToArray<long>());
        }
        Assert.Equal(expected, batched);

        List<long> rows = [];
        using (var query = Query(df))
        {
            await foreach (var row in query.ToAsyncEnumerable<AoRow>())
                rows.Add(row.Id);
        }
        Assert.Equal(expected, rows);

        List<long> pressed = [];
        using (var query = Query(df))
        {
            await foreach (var row in query.ToAsyncEnumerableWithBackpressure<AoRow>())
                pressed.Add(row.Id);
        }
        Assert.Equal(expected, pressed);
    }
}
