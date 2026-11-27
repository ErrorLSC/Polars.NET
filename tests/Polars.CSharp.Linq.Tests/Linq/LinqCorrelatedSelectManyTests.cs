#nullable enable annotations
using Polars.CSharp.Linq;
using Polars.NET.Core.Linq;
using Kaminari.Samidare;
using Polars.NET.Core.Exceptions;

namespace Polars.CSharp.Tests.Linq;

/// <summary>
/// SelectMany over an independent sequence that the outer row filters or projects (C#'s lateral join, a correlated
/// subquery in the collection selector): run natively as a join, with LINQ to Objects' rows in its order; with
/// DefaultIfEmpty and key equalities, a left join. Also a renamed tuple row (<c>new { a = t.Item1 }</c>), which was
/// taken for the row passed through.
/// </summary>
public sealed class LinqCorrelatedSelectManyTests
{
    public class Kn
    {
        public long K { get; set; }
        public long? N { get; set; }
        public long V { get; set; }
    }

    private static readonly Kn[] Outer = [new() { K = 1, N = 1, V = 10 }, new() { K = 2, N = null, V = 20 }, new() { K = 3, N = 3, V = 30 }];
    private static readonly Kn[] Inner =
        [new() { K = 1, N = 1, V = 100 }, new() { K = 1, N = null, V = 5 }, new() { K = 2, N = null, V = 1 }, new() { K = 3, N = 4, V = 7 }, new() { K = 9, N = 9, V = 2 }];

    private static void Native<T>(IQueryable<T> query, IEnumerable<T> expected)
    {
        Assert.False(((IQueryExplainable)query).InspectPlan(true).IsFullFallback);
        Assert.Equal(expected, [.. query]);
    }

    [Fact]
    public void A_Filtered_Independent_Sequence_Is_A_Join()
    {
        using var l = DataFrame.From(Outer);
        using var r = DataFrame.From(Inner);
        IQueryable<Kn> c = l.AsQueryable<Kn>(), o = r.AsQueryable<Kn>();
        Native(c.SelectMany(x => o.Where(y => y.K == x.K), (x, y) => new { x.K, y.V }),
            Outer.SelectMany(x => Inner.Where(y => y.K == x.K), (x, y) => new { x.K, y.V }));
        Native(c.SelectMany(x => o.Where(y => y.K == x.K).Select(y => new { x.K, y.V })),
            Outer.SelectMany(x => Inner.Where(y => y.K == x.K).Select(y => new { x.K, y.V })));
        Native(from x in c from y in o.Where(y => y.K == x.K && y.V > 3) select new { x.K, y.V },
            from x in Outer from y in Inner.Where(y => y.K == x.K && y.V > 3) select new { x.K, y.V });
        Native(from x in c from y in o.Where(y => y.K == x.K) where y.V > 3 select new { x.K, y.V },
            from x in Outer from y in Inner.Where(y => y.K == x.K) where y.V > 3 select new { x.K, y.V });
        Native(c.SelectMany(x => o.Where(y => y.K == x.K).Select(y => y.V * 2).Where(v => v > x.V)),
            Outer.SelectMany(x => Inner.Where(y => y.K == x.K).Select(y => y.V * 2).Where(v => v > x.V)));
        // A key equality becomes a hash join, not a cross join and a filter.
        string plan = ((IQueryExplainable)c.SelectMany(x => o.Where(y => y.K == x.K), (x, y) => new { x.K, y.V })).InspectPlan(true).NativePlan;
        Assert.Contains("INNER JOIN", plan);
    }

    [Fact]
    public void Theta_And_Nullable_Conditions_Keep_LINQ_Semantics()
    {
        using var l = DataFrame.From(Outer);
        using var r = DataFrame.From(Inner);
        IQueryable<Kn> c = l.AsQueryable<Kn>(), o = r.AsQueryable<Kn>();
        Native(from x in c from y in o.Where(y => y.V > x.V) select new { x.K, y.V },
            from x in Outer from y in Inner.Where(y => y.V > x.V) select new { x.K, y.V });
        Native(from x in c from y in o.Where(y => y.K >= x.K) select new { x.K, y.V },
            from x in Outer from y in Inner.Where(y => y.K >= x.K) select new { x.K, y.V });
        // == on nullables: null equals null, as in C#.
        Native(from x in c from y in o.Where(y => y.N == x.N) select new { x.K, y.V },
            from x in Outer from y in Inner.Where(y => y.N == x.N) select new { x.K, y.V });
    }

    [Fact]
    public void DefaultIfEmpty_Over_Key_Equalities_Is_A_Left_Join()
    {
        using var l = DataFrame.From(Outer);
        using var r = DataFrame.From(Inner);
        IQueryable<Kn> c = l.AsQueryable<Kn>(), o = r.AsQueryable<Kn>();
        Native(from x in c from y in o.Where(y => y.K == x.K).DefaultIfEmpty() select new { x.K, V = y == null ? (long?)null : y.V },
            from x in Outer from y in Inner.Where(y => y.K == x.K).DefaultIfEmpty() select new { x.K, V = y == null ? (long?)null : y.V });
        Native(from x in c from y in o.Where(y => y.K == x.K && y.V > 3).DefaultIfEmpty() select new { x.K, V = y == null ? (long?)null : y.V },
            from x in Outer from y in Inner.Where(y => y.K == x.K && y.V > 3).DefaultIfEmpty() select new { x.K, V = y == null ? (long?)null : y.V });
        Native(from x in c from y in o.Where(y => x.K == y.K && y.V == x.V * 10).DefaultIfEmpty() select new { x.K, V = y == null ? (long?)null : y.V },
            from x in Outer from y in Inner.Where(y => x.K == y.K && y.V == x.V * 10).DefaultIfEmpty() select new { x.K, V = y == null ? (long?)null : y.V });
        Native(from x in c from y in o.Where(y => y.K == x.K).Where(y => y.V < 50).DefaultIfEmpty() select new { x.K, V = y == null ? (long?)null : y.V },
            from x in Outer from y in Inner.Where(y => y.K == x.K).Where(y => y.V < 50).DefaultIfEmpty() select new { x.K, V = y == null ? (long?)null : y.V });
        Native(c.SelectMany(x => o.Where(y => y.K == x.K).DefaultIfEmpty()).Select(y => y == null ? -1 : y.V),
            Outer.SelectMany(x => Inner.Where(y => y.K == x.K).DefaultIfEmpty()).Select(y => y == null ? -1 : y.V));
    }

    /// <summary>
    /// On nullable keys `==` matches nulls where join keys never do: such a lateral left join is not taken for a join
    /// (it is refused, never answered differently from LINQ to Objects).
    /// </summary>
    [Fact]
    public void DefaultIfEmpty_Over_Nullable_Keys_Is_Never_Answered_Wrong()
    {
        using var l = DataFrame.From(Outer);
        using var r = DataFrame.From(Inner);
        IQueryable<Kn> c = l.AsQueryable<Kn>(), o = r.AsQueryable<Kn>();
        var query = from x in c from y in o.Where(y => y.N == x.N).DefaultIfEmpty() select new { x.K, V = y == null ? (long?)null : y.V };
        var expected = from x in Outer from y in Inner.Where(y => y.N == x.N).DefaultIfEmpty() select new { x.K, V = y == null ? (long?)null : y.V };
        try
        {
            Assert.Equal(expected, [.. query]);
        }
        catch (PolarsPushdownException)
        {
            // Refused: the provider does not run it natively.
        }
    }

    [Fact]
    public void A_Tuple_Row_Renamed_Keeps_Its_Items_Apart()
    {
        using var l = DataFrame.From(Outer);
        using var r = DataFrame.From(Inner);
        IQueryable<Kn> c = l.AsQueryable<Kn>(), o = r.AsQueryable<Kn>();
        Native(c.SelectMany(x => o, (x, y) => Tuple.Create(x, y)).Select(t => new { a = t.Item1, b = t.Item2 }).Select(u => new { u.a.K, u.b.V }),
            Outer.SelectMany(x => Inner, (x, y) => Tuple.Create(x, y)).Select(t => new { a = t.Item1, b = t.Item2 }).Select(u => new { u.a.K, u.b.V }));
        Native(c.SelectMany(x => o, (x, y) => Tuple.Create(x, y)).Select(t => new { a = t.Item2, b = t.Item1 }).Select(u => new { u.a.K, u.b.V }),
            Outer.SelectMany(x => Inner, (x, y) => Tuple.Create(x, y)).Select(t => new { a = t.Item2, b = t.Item1 }).Select(u => new { u.a.K, u.b.V }));
    }
}
