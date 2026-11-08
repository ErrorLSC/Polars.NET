using Polars.CSharp.Linq;
using Polars.NET.Linq;

namespace Polars.CSharp.Tests.Linq;

public enum NestedHue { Red = 1, Green = 2, Blue = 4 }

/// <summary>
/// Regression tests for backlog items: indexing a dictionary a UDF returns, a right join after a transparent
/// join, and enums nested in struct and list columns. Results are compared with LINQ to Objects.
/// </summary>
public class LinqBacklogRegressionTests
{
    public class Text
    {
        public long Id { get; set; }
        public string? Value { get; set; }
    }

    public static Dictionary<string, long> Lengths(string? s) => new() { ["len"] = s?.Length ?? -1, ["zero"] = 0 };

    [Fact]
    public void Indexing_A_Dictionary_Is_Not_A_List_Operation()
    {
        Text[] rows = [new() { Id = 1, Value = "abc" }, new() { Id = 2, Value = null }];
        using var df = DataFrame.From(rows);

        Assert.Equal(rows.Select(x => Lengths(x.Value)["len"]), df.AsQueryable<Text>().Select(x => Lengths(x.Value)["len"]).ToList());
        Assert.Equal(
            rows.Select(x => new { x.Id, n = Lengths(x.Value).Count, has = Lengths(x.Value).ContainsKey("len") }),
            df.AsQueryable<Text>().Select(x => new { x.Id, n = Lengths(x.Value).Count, has = Lengths(x.Value).ContainsKey("len") }).ToList());
    }

    public class A { public long K { get; set; } public string Va { get; set; } = ""; }
    public class B { public long K { get; set; } public string Vb { get; set; } = ""; }
    public class C { public long K { get; set; } public string Vc { get; set; } = ""; }

    [Fact]
    public void Right_Join_After_A_Transparent_Join_Reads_Both_Scopes()
    {
        A[] a = [new() { K = 1, Va = "a1" }, new() { K = 2, Va = "a2" }];
        B[] b = [new() { K = 1, Vb = "b1" }, new() { K = 2, Vb = "b2" }];
        C[] c = [new() { K = 2, Vc = "c2" }, new() { K = 3, Vc = "c3" }];
        using var adf = DataFrame.From(a);
        using var bdf = DataFrame.From(b);
        using var cdf = DataFrame.From(c);

        var other = (from x in adf.AsQueryable<A>() join y in bdf.AsQueryable<B>() on x.K equals y.K select new { x, y })
            .RightJoin(cdf.AsQueryable<C>(), t => t.x.K, z => z.K, (t, z) => new { Va = t == null ? null : t.x.Va, Vb = t == null ? null : t.y.Vb, z.Vc });
        var sameNames = (from x in adf.AsQueryable<A>() join y in bdf.AsQueryable<B>() on x.K equals y.K select new { x, y })
            .RightJoin(adf.AsQueryable<A>(), t => t.x.K, z => z.K, (t, z) => new { L = t == null ? null : t.x.Va, R = z.Va, Kl = t == null ? -1 : t.x.K, Kr = z.K });

        Assert.Equal(
            (from x in a join y in b on x.K equals y.K select new { x, y })
                .RightJoin(c, t => t.x.K, z => z.K, (t, z) => new { Va = t == null ? null : t.x.Va, Vb = t == null ? null : t.y.Vb, z.Vc }),
            other.ToList());
        Assert.Equal(
            (from x in a join y in b on x.K equals y.K select new { x, y })
                .RightJoin(a, t => t.x.K, z => z.K, (t, z) => new { L = t == null ? null : t.x.Va, R = z.Va, Kl = t == null ? -1 : t.x.K, Kr = z.K }),
            sameNames.ToList());
    }

    public class Paint
    {
        public NestedHue Hue { get; set; }
        public NestedHue? Maybe { get; set; }
    }

    public class Item
    {
        public long Id { get; set; }
        public Paint Paint { get; set; } = new();
        public List<NestedHue> Hues { get; set; } = [];
    }

    private static readonly Item[] Items =
    [
        new() { Id = 1, Paint = new() { Hue = NestedHue.Green, Maybe = null }, Hues = [NestedHue.Red, NestedHue.Blue] },
        new() { Id = 2, Paint = new() { Hue = NestedHue.Red, Maybe = NestedHue.Blue }, Hues = [NestedHue.Green] },
        new() { Id = 3, Paint = new() { Hue = NestedHue.Blue, Maybe = NestedHue.Red }, Hues = [] },
    ];

    [Fact]
    public void Enums_Nested_In_Struct_And_List_Columns_Translate()
    {
        using var df = DataFrame.From(Items);
        var q = df.AsQueryable<Item>();

        Assert.Equal(Items.Where(x => x.Paint.Hue == NestedHue.Green).Select(x => x.Id), q.Where(x => x.Paint.Hue == NestedHue.Green).Select(x => x.Id).ToList());
        Assert.Equal(Items.Where(x => x.Paint.Hue > NestedHue.Red).Select(x => x.Id), q.Where(x => x.Paint.Hue > NestedHue.Red).Select(x => x.Id).ToList());
        Assert.Equal(Items.Where(x => x.Paint.Maybe == null).Select(x => x.Id), q.Where(x => x.Paint.Maybe == null).Select(x => x.Id).ToList());
        Assert.Equal(Items.Where(x => x.Hues.Contains(NestedHue.Blue)).Select(x => x.Id), q.Where(x => x.Hues.Contains(NestedHue.Blue)).Select(x => x.Id).ToList());
        Assert.Equal(Items.OrderBy(x => x.Paint.Hue).Select(x => x.Id), q.OrderBy(x => x.Paint.Hue).Select(x => x.Id).ToList());
        Assert.Equal(
            Items.Select(x => new { x.Id, name = x.Paint.Hue.ToString(), value = (int)x.Paint.Hue, all = string.Join(",", x.Hues), n = x.Hues.Count }),
            q.Select(x => new { x.Id, name = x.Paint.Hue.ToString(), value = (int)x.Paint.Hue, all = string.Join(",", x.Hues), n = x.Hues.Count }).ToList());
        Assert.Equal(
            Items.Where(x => x.Hues.Count > 0).Select(x => new { x.Id, first = x.Hues[0] }),
            q.Where(x => x.Hues.Count > 0).Select(x => new { x.Id, first = x.Hues[0] }).ToList());
        Assert.Equal(
            Items.GroupBy(x => x.Paint.Hue).Select(g => new { g.Key, n = g.Count() }).OrderBy(r => r.Key),
            q.GroupBy(x => x.Paint.Hue).Select(g => new { g.Key, n = g.Count() }).OrderBy(r => r.Key).ToList());
    }

    // ---------------------------------------------------------------------------------------------
    // Scalar terminals over join rows (flat join scopes) read only the rows they need. Row 3 holds a null in a
    // non-nullable member: reading it would fail strict materialization, so a terminal that does not need it
    // must never read it.
    // ---------------------------------------------------------------------------------------------

    public class Order { public long Id { get; set; } public long Cust { get; set; } public long? Amount { get; set; } }
    public class StrictOrder { public long Id { get; set; } public long Cust { get; set; } public long Amount { get; set; } }
    public class Customer { public long Cust { get; set; } public string Name { get; set; } = ""; }

    private static readonly Order[] Orders =
        [new() { Id = 1, Cust = 1, Amount = 10 }, new() { Id = 2, Cust = 2, Amount = 20 }, new() { Id = 3, Cust = 1, Amount = null }];
    private static readonly Customer[] Custs = [new() { Cust = 1, Name = "a" }, new() { Cust = 2, Name = "b" }];

    [Fact]
    public void Scalar_Terminals_Over_Join_Rows_Read_Only_The_Rows_They_Need()
    {
        using var odf = DataFrame.From(Orders);
        using var cdf = DataFrame.From(Custs);
        IQueryable<(StrictOrder o, Customer c)> Pairs() =>
            from o in odf.AsQueryable<StrictOrder>() join c in cdf.AsQueryable<Customer>() on o.Cust equals c.Cust select ValueTuple.Create(o, c);

        // Reading every row fails: row 3 cannot be materialized.
        Assert.Throws<InvalidOperationException>(() => Pairs().ToList());

        Assert.Equal(1, Pairs().First().o.Id);
        Assert.Equal(2, Pairs().First(p => p.c.Name == "b").o.Id);
        Assert.Equal(2, Pairs().Single(p => p.o.Id == 2).o.Id);
        Assert.Equal(2, Pairs().ElementAt(1).o.Id);
        Assert.Equal(2, Pairs().Count(p => p.o.Cust == 1));
        Assert.True(Pairs().Any(p => p.c.Name == "b"));
        Assert.False(Pairs().All(p => p.c.Name == "a"));
        Assert.Equal(1, Pairs().Min(p => p.o.Id));
        Assert.Equal(6, Pairs().Sum(p => p.o.Id));
        Assert.Equal("b", Pairs().Max(p => p.c.Name));
    }

    [Fact]
    public void Scalar_Terminals_Over_Join_Rows_Match_Linq()
    {
        using var odf = DataFrame.From(Orders);
        using var cdf = DataFrame.From(Custs);
        var pairs = from o in odf.AsQueryable<Order>() join c in cdf.AsQueryable<Customer>() on o.Cust equals c.Cust select new { o, c };
        var expected = from o in Orders join c in Custs on o.Cust equals c.Cust select new { o, c };

        Assert.Equal(expected.First().o.Id, pairs.First().o.Id);
        Assert.Equal(expected.FirstOrDefault(p => p.c.Name == "z"), pairs.FirstOrDefault(p => p.c.Name == "z"));
        Assert.Equal(expected.Last().o.Id, pairs.Last().o.Id);
        Assert.Equal(expected.Last(p => p.c.Name == "a").o.Id, pairs.Last(p => p.c.Name == "a").o.Id);
        Assert.Equal(expected.ElementAtOrDefault(9), pairs.ElementAtOrDefault(9));
        Assert.Equal(expected.Count(p => p.o.Amount == null), pairs.Count(p => p.o.Amount == null));
        Assert.Equal(expected.LongCount(p => p.c.Name == "a"), pairs.LongCount(p => p.c.Name == "a"));
        Assert.Equal(expected.All(p => p.o.Id > 0), pairs.All(p => p.o.Id > 0));
        Assert.Equal(expected.Sum(p => p.o.Amount), pairs.Sum(p => p.o.Amount));
        Assert.Equal(expected.Average(p => p.o.Amount), pairs.Average(p => p.o.Amount));
        Assert.Throws<InvalidOperationException>(() => pairs.Single(p => p.o.Cust == 1));
        Assert.Throws<InvalidOperationException>(() => pairs.First(p => p.c.Name == "z"));
        Assert.Throws<ArgumentOutOfRangeException>(() => pairs.ElementAt(9));
    }

    public class Line { public long Order { get; set; } public long? Warehouse { get; set; } public string State { get; set; } = ""; }
    public class Return { public long Order { get; set; } }

    static readonly Line[] Lines =
    [
        new() { Order = 1, Warehouse = 1, State = "IL" }, new() { Order = 1, Warehouse = 2, State = "IL" },
        new() { Order = 2, Warehouse = 1, State = "IL" }, new() { Order = 2, Warehouse = 1, State = "GA" },
        new() { Order = 3, Warehouse = 1, State = "GA" }, new() { Order = 3, Warehouse = 2, State = "GA" },
        new() { Order = 4, Warehouse = 1, State = "IL" }, new() { Order = 4, Warehouse = null, State = "IL" },
        new() { Order = 5, Warehouse = 3, State = "IL" }, new() { Order = 5, Warehouse = 4, State = "IL" },
    ];

    static readonly Return[] Returns = [new() { Order = 5 }];

    /// TPC-DS Q94 shape: plain conjuncts in the same Where as correlated EXISTS / NOT EXISTS filter the outer rows
    /// before the joins the subqueries decorrelate into (they used to filter on top, after a self join of the
    /// whole table), with the same rows as LINQ to Objects.
    [Fact]
    public void Plain_Conjuncts_Filter_Before_Decorrelated_Exists()
    {
        using var ldf = DataFrame.From(Lines);
        using var rdf = DataFrame.From(Returns);
        var lines = ldf.AsQueryable<Line>();
        var returns = rdf.AsQueryable<Return>();

        var query = lines.Where(l1 => l1.State == "IL"
                                      && lines.Any(l2 => l2.Order == l1.Order && l1.Warehouse != null && l2.Warehouse != null && l2.Warehouse != l1.Warehouse)
                                      && !returns.Any(r => r.Order == l1.Order))
                         .Select(l => new { l.Order, l.Warehouse })
                         .OrderBy(x => x.Order).ThenBy(x => x.Warehouse);
        var expected = Lines.Where(l1 => l1.State == "IL"
                                         && Lines.Any(l2 => l2.Order == l1.Order && l1.Warehouse != null && l2.Warehouse != null && l2.Warehouse != l1.Warehouse)
                                         && !Returns.Any(r => r.Order == l1.Order))
                            .Select(l => new { l.Order, l.Warehouse })
                            .OrderBy(x => x.Order).ThenBy(x => x.Warehouse);

        Assert.Equal(expected.ToList(), query.ToList());

        // The state filter sits below the semi join (deeper in the printed tree), not above the anti join.
        var plan = query.ExplainLogicalPlan();
        int semi = plan.IndexOf("how=Semi", StringComparison.Ordinal);
        int anti = plan.IndexOf("how=Anti", StringComparison.Ordinal);
        int state = plan.IndexOf("col(\"State\") == \"IL\"", StringComparison.Ordinal);
        Assert.True(semi >= 0 && anti >= 0 && state >= 0, plan);
        Assert.True(state > semi && state > anti, plan);
    }

    /// An IN list held in memory (List, IReadOnlyList collection expression, array) filters by membership
    /// (is_in), not by a join with a snapshot of the list, with C# semantics for null (a null in the list matches
    /// a null value) and next to a correlated subquery in the same Where. Contains over a query stays a semi join.
    [Fact]
    public void In_Memory_Contains_Filters_By_Membership()
    {
        Line[] lines = [.. Lines, new() { Order = 6, Warehouse = null, State = "TX" }];
        using var ldf = DataFrame.From(lines);
        using var rdf = DataFrame.From(Returns);
        var q = ldf.AsQueryable<Line>();
        var returns = rdf.AsQueryable<Return>();

        List<long?> warehouses = [1, null];
        IReadOnlyList<string> states = ["IL", "TX"];
        long[] orders = [2, 4, 5, 6];

        var query = q.Where(l => warehouses.Contains(l.Warehouse) && states.Contains(l.State) && orders.Contains(l.Order)
                                 && !returns.Any(r => r.Order == l.Order))
                     .Select(l => new { l.Order, l.Warehouse, l.State })
                     .OrderBy(x => x.Order).ThenBy(x => x.Warehouse);
        var expected = lines.Where(l => warehouses.Contains(l.Warehouse) && states.Contains(l.State) && orders.Contains(l.Order)
                                        && !Returns.Any(r => r.Order == l.Order))
                            .Select(l => new { l.Order, l.Warehouse, l.State })
                            .OrderBy(x => x.Order).ThenBy(x => x.Warehouse);

        Assert.Equal(expected.ToList(), query.ToList());
        var plan = query.ExplainLogicalPlan();
        Assert.DoesNotContain("how=Semi", plan);
        Assert.Contains("how=Anti", plan);

        // Contains over a query is still a semi join.
        var returned = returns.Select(r => r.Order);
        var viaQuery = q.Where(l => returned.Contains(l.Order)).Select(l => l.Order).Distinct().OrderBy(o => o);
        Assert.Equal([5L], viaQuery.ToList());
        Assert.Contains("how=Semi", viaQuery.ExplainLogicalPlan());
    }
}
