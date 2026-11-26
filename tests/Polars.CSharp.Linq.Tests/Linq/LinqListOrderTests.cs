using Polars.CSharp.Linq;

namespace Polars.CSharp.Tests;

/// <summary>
/// A list built from a group (<c>ToList</c> over a group, a correlated subquery or a group join) holds its elements in
/// the input's order, also when the engine streams the input in several morsels and the groups' own order is free;
/// MaxBy and MinBy take the first of the tied rows, as LINQ's do.
/// </summary>
public class LinqListOrderTests
{
    // Enough rows for several morsels of the streaming engine; each key's rows are spread over all of them.
    private static readonly LoCustomer[] Customers = [.. Enumerable.Range(0, 500).Select(i => new LoCustomer((i * 37) % 500))];
    private static readonly LoOrder[] Orders = [.. Enumerable.Range(0, 400_000).Select(i => new LoOrder(i, (i * 7919) % 500))];
    private static readonly ILookup<int, int> IdsByCustomer = Orders.ToLookup(o => o.CustomerId, o => o.OrderId);

    private static List<string> Expected(IEnumerable<int> keys) => [.. keys.Select(k => k + ":" + string.Join(",", IdsByCustomer[k]))];

    private static List<string> Text(IEnumerable<LoList> rows) => [.. rows.Select(r => r.Key + ":" + string.Join(",", r.Ids))];

    [Fact]
    public void Correlated_ToList_Keeps_The_Element_Order()
    {
        using var dfC = DataFrame.FromRows(Customers);
        using var dfO = DataFrame.FromRows(Orders);
        var orders = dfO.AsQueryable(Orders);
        var query = dfC.AsQueryable(Customers)
            .Select(c => new LoList(c.Id, orders.Where(o => o.CustomerId == c.Id).Select(o => o.OrderId).ToList()))
            .OrderBy(r => r.Key);

        Assert.Equal(Expected(Customers.Select(c => c.Id).Order()), Text(query.ToList()));
    }

    [Fact]
    public void Grouped_ToList_Under_A_Key_Sort_Keeps_The_Element_Order()
    {
        using var dfO = DataFrame.FromRows(Orders);
        // The sort over the key frees the groups' order; the elements' order stays observed.
        var query = dfO.AsQueryable(Orders)
            .GroupBy(o => o.CustomerId)
            .Select(g => new LoList(g.Key, g.Select(o => o.OrderId).ToList()))
            .OrderBy(r => r.Key);

        Assert.Equal(Expected(IdsByCustomer.Select(g => g.Key).Order()), Text(query.ToList()));
    }

    [Fact]
    public void GroupJoin_ToList_Keeps_The_Element_Order()
    {
        using var dfC = DataFrame.FromRows(Customers);
        using var dfO = DataFrame.FromRows(Orders);
        var query = dfC.AsQueryable(Customers)
            .GroupJoin(dfO.AsQueryable(Orders), c => c.Id, o => o.CustomerId, (c, os) => new LoList(c.Id, os.Select(o => o.OrderId).ToList()));

        Assert.Equal(Expected(Customers.Select(c => c.Id)), Text(query.ToList()));
    }

    [Fact]
    public void MaxBy_And_MinBy_Take_The_First_Of_Tied_Rows()
    {
        // Every group holds many rows of each key (OrderId % 3), over several morsels: the first of the tied ones wins.
        using var dfO = DataFrame.FromRows(Orders);
        var query = dfO.AsQueryable(Orders)
            .GroupBy(o => o.CustomerId)
            .Select(g => new LoTie(g.Key, g.MaxBy(o => o.OrderId % 3)!.OrderId, g.MinBy(o => o.OrderId % 3)!.OrderId))
            .OrderBy(r => r.Key);
        List<LoTie> expected = [.. Orders.GroupBy(o => o.CustomerId)
            .Select(g => new LoTie(g.Key, g.MaxBy(o => o.OrderId % 3)!.OrderId, g.MinBy(o => o.OrderId % 3)!.OrderId))
            .OrderBy(r => r.Key)];
        for (int run = 0; run < 3; run++)
            Assert.Equal(expected, query.ToList());
    }
}

public record LoTie(int Key, int Max, int Min);

public record LoCustomer(int Id);

public record LoOrder(int OrderId, int CustomerId);

public record LoList(int Key, List<int> Ids);
