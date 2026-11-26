using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests;

public record CjOuter(int Id, int V);
public record CjInner(int Id, int W);

/// <summary>
/// Cross joins (<c>from x in a from y in b</c>) yield the outer rows in order and, for each, the inner rows in order,
/// also on the streaming engine and over many morsels; filtered by inequalities, they still run as the engine's
/// inequality join (IEJoin), the order restored afterwards.
/// </summary>
public class LinqCrossJoinOrderTests
{
    private static readonly CjOuter[] Outer = [.. Enumerable.Range(0, 600).Select(i => new CjOuter(i, (i * 37) % 600))];
    private static readonly CjInner[] Inner = [.. Enumerable.Range(0, 900).Select(i => new CjInner(i, (i * 53) % 900))];

    private static void AssertSameSequence<T>(Func<IQueryable<CjOuter>, IQueryable<CjInner>, IQueryable<T>> query, string planHas)
    {
        using var dfOuter = DataFrame.FromRows(Outer);
        using var dfInner = DataFrame.FromRows(Inner);
        var native = query(dfOuter.AsQueryable(Outer), dfInner.AsQueryable(Inner));
        Assert.Contains(planHas, native.Explain());
        List<T> expected = [.. query(Outer.AsQueryable(), Inner.AsQueryable())];
        for (int run = 0; run < 3; run++)
            Assert.Equal(expected, native.ToList());
    }

    [Fact]
    public void Cross_Join_Keeps_Outer_Then_Inner_Order() =>
        AssertSameSequence((a, b) => from x in a from y in b select new { x.Id, Other = y.Id }, "CROSS JOIN");

    [Fact]
    public void Inequality_Filtered_Cross_Join_Runs_As_IEJoin_In_Order() =>
        AssertSameSequence((a, b) => from x in a from y in b where x.V < y.W && y.W < x.V + 20 select new { x.Id, Other = y.Id }, "IEJOIN");
}
