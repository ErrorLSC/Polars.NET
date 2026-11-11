using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp.Tests;

/// <summary>
/// Engine.Gpu is rejected: the Polars Rust core has no GPU executor, so it would silently run on the CPU (#39).
/// </summary>
public class GpuEngineTests
{
    private static DataFrame Sample() => DataFrame.FromColumns([Series.From("x", [1, 2, 3])]);

    [Fact]
    [Trait("Engine", "Gpu")]
    public void Collect_Gpu_Throws_And_LazyFrame_Stays_Usable()
    {
        using var df = Sample();
        using var lf = df.Lazy();

        Assert.Throws<NotSupportedException>(() => lf.Collect(Engine.Gpu));

        using var collected = lf.Collect();
        Assert.Equal(3, collected.Height);
    }

    [Fact]
    [Trait("Engine", "Gpu")]
    public async Task CollectAsync_Gpu_Throws()
    {
        using var df = Sample();
        using var lf = df.Lazy();

        await Assert.ThrowsAsync<NotSupportedException>(() => lf.CollectAsync(Engine.Gpu));
    }

    [Fact]
    [Trait("Engine", "Gpu")]
    public async Task CollectAll_Gpu_Throws()
    {
        using var df = Sample();
        using var lf1 = df.Lazy();
        using var lf2 = df.Lazy();

        Assert.Throws<NotSupportedException>(() => Pl.CollectAll([lf1, lf2], Engine.Gpu));
        await Assert.ThrowsAsync<NotSupportedException>(() => Pl.CollectAllAsync([lf1, lf2], Engine.Gpu));
    }

    [Fact]
    [Trait("Engine", "Gpu")]
    public async Task CollectBatchesAsync_Gpu_Throws_On_First_Batch()
    {
        using var df = Sample();
        using var lf = df.Lazy();

        await Assert.ThrowsAsync<NotSupportedException>(async () =>
        {
            await foreach (var batch in lf.CollectBatchesAsync(engine: Engine.Gpu))
                batch.Dispose();
        });
    }
}
