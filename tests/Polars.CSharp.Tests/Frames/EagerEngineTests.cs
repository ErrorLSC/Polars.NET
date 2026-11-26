using Apache.Arrow;
using static Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// Eager DataFrame and Series operations run on the in-memory engine, as Python Polars runs its eager methods; lazy
/// queries keep the default engine, which streams the input in morsels.
/// </summary>
public class EagerEngineTests
{
    private const int Rows = 400_000;

    private static DataFrame Frame() => DataFrame.FromSeries(Series.From("v", [.. Enumerable.Range(0, Rows)]), Series.From("k", [.. Enumerable.Range(0, Rows).Select(i => i % 7)]));

    /// <summary>The number of batches the UDF of the expression built by <paramref name="use"/> is called with.</summary>
    private static int Batches(Func<Expr, DataFrame> use)
    {
        int calls = 0;
        using var mapped = Col("v").MapBatches((IArrowArray a) => { Interlocked.Increment(ref calls); return a; }, DataType.Int32);
        using var result = use(mapped);
        return calls;
    }

    [Fact]
    public void Eager_Operations_Run_On_The_In_Memory_Engine()
    {
        using var df = Frame();
        // A batch UDF is called as often as when the same plan runs on the in-memory engine (which hands an elementwise
        // UDF the whole column, or row ranges evaluated side by side), not once per streamed morsel.
        Assert.Equal(1, Batches(e => df.Select(e.Alias("m"))));
        Assert.Equal(1, Batches(e => df.WithColumns(e.Alias("m"))));
        Assert.Equal(Batches(e => df.Lazy().Filter(e > 10).Collect(Engine.InMemory)), Batches(e => df.Filter(e > 10)));
        Assert.Equal(Batches(e => df.Lazy().GroupBy(["k"]).Agg(e.Sum().Alias("m")).Collect(Engine.InMemory)),
            Batches(e => df.GroupBy("k").Agg(e.Sum().Alias("m"))));
        Assert.Equal(1, Batches(e => df.Join(df.Select(Col("v"), e.Alias("m")), "v")));
    }

    [Fact]
    public void Lazy_Queries_Stream_By_Default()
    {
        using var df = Frame();
        Assert.True(Batches(e => df.Lazy().Select(e.Alias("m")).Collect()) > 1);
        Assert.Equal(1, Batches(e => df.Lazy().Select(e.Alias("m")).Collect(Engine.InMemory)));
    }
}
