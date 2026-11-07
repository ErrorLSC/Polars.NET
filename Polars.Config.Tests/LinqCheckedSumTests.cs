using Polars.CSharp;
using Polars.CSharp.Linq;
using Pl = Polars.CSharp.Polars;

namespace Polars.Config.Tests;

/// <summary>
/// An integer Sum that overflows throws OverflowException through a managed check. With managed callbacks
/// disallowed the check cannot run, so the sum is narrowed with a strict cast instead: the query still fails
/// rather than returning a wrapped value.
/// </summary>
public class LinqCheckedSumTests
{
    public class Num
    {
        public long K { get; set; }
        public int I { get; set; }
        public long L { get; set; }
    }

    private static readonly Num[] Rows =
    [
        new() { K = 1, I = int.MaxValue, L = long.MaxValue },
        new() { K = 1, I = int.MaxValue, L = long.MaxValue },
        new() { K = 2, I = 3, L = 3 },
    ];

    [Fact]
    public void Overflowing_Sum_Fails_Without_Managed_Callbacks()
    {
        using var _ = Pl.Config.SetDisallowUdfPushdown(true).BeginScope();
        using var df = DataFrame.From(Rows);
        var q = df.AsQueryable<Num>();

        Assert.ThrowsAny<Exception>(() => q.Sum(x => x.I));
        Assert.ThrowsAny<Exception>(() => q.Sum(x => x.L));
        Assert.ThrowsAny<Exception>(() => q.GroupBy(x => x.K).Select(g => new { g.Key, S = g.Sum(x => x.I) }).ToList());
        Assert.Equal(3, q.Where(x => x.K == 2).Sum(x => x.I));
    }
}
