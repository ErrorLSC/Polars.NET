using Cs = Polars.CSharp.Polars.Selectors;
using Pl = Polars.CSharp.Polars;
namespace Polars.CSharp.Tests;

/// <summary>
/// A null column name is refused with an <see cref="ArgumentNullException"/> / <see cref="ArgumentException"/> before it
/// reaches the engine (where a null pointer used to panic): in the string conversions to selectors and column
/// expressions, in <c>Col</c> and in the selectors by name.
/// </summary>
public class NullNameTests
{
    [Fact]
    public void NullNames_AreRefusedBeforeTheEngine()
    {
        string? none = null;
        Assert.Throws<ArgumentNullException>(() => { IntoSelector _ = none!; });
        Assert.Throws<ArgumentNullException>(() => { IntoExprColumn _ = none!; });
        Assert.Throws<ArgumentNullException>(() => Pl.Col(none!));
        Assert.Throws<ArgumentException>(() => Cs.ByName("a", none!));
        Assert.Throws<ArgumentException>(() => Cs.All().Exclude("a", none!));
    }

    [Fact]
    public void SinkDelta_WithANullStringPartition_IsRefused()
    {
        // The way it used to be met: a conditional partition column whose "none" branch was a null string.
        string dir = Path.Combine(Path.GetTempPath(), $"nullpart-{Guid.NewGuid():N}");
        using var df = DataFrame.From<Row>([new(1), new(2)]);
        string? partition = null;
        Assert.Throws<ArgumentNullException>(() => df.Lazy().SinkDelta(dir, partitionBy: partition!));
        Assert.False(Directory.Exists(dir));
    }

    private sealed record Row(int A);
}
