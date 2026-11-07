using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests;

public record JoOrder(int Id, string Customer);
public record JoLine(int OrderId, string Item);

/// <summary>
/// Joins yield rows in LINQ order: outer rows in their order, and the matches of one outer row in inner order.
/// The inner rows are deliberately not grouped or sorted by key, so a hash join's natural order differs.
/// </summary>
public class LinqJoinOrderTests
{
    private static readonly JoOrder[] Orders = [new(3, "c"), new(1, "a"), new(4, "d"), new(2, "b")];

    private static readonly JoLine[] Lines =
    [
        new(2, "b1"), new(1, "a1"), new(3, "c1"), new(1, "a2"), new(2, "b2"),
        new(9, "x1"), new(3, "c2"), new(1, "a3"), new(2, "b3"), new(3, "c3"),
    ];

    private static void AssertSameSequence<T>(
        Func<IQueryable<JoOrder>, IQueryable<JoLine>, IQueryable<T>> query)
    {
        using var o = DataFrame.FromRows(Orders);
        using var l = DataFrame.FromRows(Lines);
        var expected = query(Orders.AsQueryable(), Lines.AsQueryable()).Select(r => r!.ToString()).ToList();

        // Repeated runs: an unordered join can happen to match once.
        for (var run = 0; run < 5; run++)
            Assert.Equal(expected, query(o.AsQueryable<JoOrder>(), l.AsQueryable<JoLine>()).ToList().Select(r => r!.ToString()));
    }

    [Fact]
    public void Inner_Join_Keeps_Outer_Then_Inner_Order() =>
        AssertSameSequence((o, l) => o.Join(l, x => x.Id, y => y.OrderId, (x, y) => new { x.Customer, y.Item }));

    [Fact]
    public void Left_Join_Keeps_Outer_Then_Inner_Order() =>
        AssertSameSequence((o, l) => o.LeftJoin(l, x => x.Id, y => y.OrderId, (x, y) => new { x.Customer, Item = y == null ? null : y.Item }));

    [Fact]
    public void Right_Join_Keeps_Inner_Order() =>
        AssertSameSequence((o, l) => o.RightJoin(l, x => x.Id, y => y.OrderId, (x, y) => new { Customer = x == null ? null : x.Customer, y.Item }));

    [Fact]
    public void Query_Syntax_Join_Keeps_Order() =>
        AssertSameSequence((o, l) => from x in o join y in l on x.Id equals y.OrderId select new { x, y });

    [Fact]
    public void Cross_Join_Keeps_Outer_Then_Inner_Order() =>
        AssertSameSequence((o, l) => from x in o from y in l select new { x.Customer, y.Item });

    [Fact]
    public void Distinct_After_Join_Keeps_First_Occurrence_Order() =>
        AssertSameSequence((o, l) => o.Join(l, x => x.Id, y => y.OrderId, (x, y) => x.Customer).Distinct());

    [Fact]
    public void GroupBy_After_Join_Keeps_First_Key_Order() =>
        AssertSameSequence((o, l) => o.Join(l, x => x.Id, y => y.OrderId, (x, y) => new { x.Customer, y.Item })
            .GroupBy(t => t.Customer)
            .Select(g => new { g.Key, N = g.Count() }));

    [Fact]
    public void Semi_Join_Keeps_Outer_Order() =>
        AssertSameSequence((o, l) => o.Where(x => l.Any(y => y.OrderId == x.Id)));
}
