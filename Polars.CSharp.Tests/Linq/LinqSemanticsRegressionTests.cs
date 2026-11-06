using Polars.CSharp.Linq;
using Polars.NET.Linq;
using Polars.NET.Core.Exceptions;

namespace Polars.CSharp.Tests.Linq;

/// <summary>
/// Regression tests for LINQ semantics the TPC-DS audit found broken: results are compared with LINQ to Objects
/// (or with hand-computed values for Polars-only operators such as Rank and Std).
/// </summary>
public class LinqSemanticsRegressionTests
{
    public class Kv
    {
        public long K { get; set; }
        public long V { get; set; }
    }

    public class KnV
    {
        public long K { get; set; }
        public long? V { get; set; }
    }

    private static readonly Kv[] Left = [new() { K = 1, V = 10 }, new() { K = 2, V = 20 }, new() { K = 3, V = 30 }];
    private static readonly Kv[] Right = [new() { K = 1, V = 100 }, new() { K = 1, V = 5 }, new() { K = 2, V = 1 }];

    private static bool IsFullFallback<T>(IQueryable<T> query) =>
        ((IPolarsExplainable)query).InspectPlan(true).IsFullFallback;

    // ---------------------------------------------------------------------------------------------
    // GroupJoin + SelectMany(g.<ops>.DefaultIfEmpty()): the group's operators used to be dropped.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void Left_Join_With_Group_Filter_Applies_The_Filter()
    {
        using var l = DataFrame.From(Left);
        using var r = DataFrame.From(Right);
        var query = from a in l.AsQueryable<Kv>()
                    join b in r.AsQueryable<Kv>() on a.K equals b.K into g
                    from x in g.Where(b => b.V > 50).DefaultIfEmpty()
                    select new { a.K, xv = x == null ? (long?)null : x.V };
        var expected = from a in Left
                       join b in Right on a.K equals b.K into g
                       from x in g.Where(b => b.V > 50).DefaultIfEmpty()
                       select new { a.K, xv = x == null ? (long?)null : x.V };

        Assert.False(IsFullFallback(query));
        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void Left_Join_With_Group_Projection_Yields_Default_For_Missing_Matches()
    {
        using var l = DataFrame.From(Left);
        using var r = DataFrame.From(Right);
        var query = from a in l.AsQueryable<Kv>()
                    join b in r.AsQueryable<Kv>() on a.K equals b.K into g
                    from x in g.Select(b => b.V * 7).DefaultIfEmpty()
                    select new { a.K, x };
        var expected = from a in Left
                       join b in Right on a.K equals b.K into g
                       from x in g.Select(b => b.V * 7).DefaultIfEmpty()
                       select new { a.K, x };

        Assert.False(IsFullFallback(query));
        Assert.Equal(expected.OrderBy(t => t.K).ThenBy(t => t.x), query.ToList().OrderBy(t => t.K).ThenBy(t => t.x));
    }

    [Fact]
    public void Left_Join_With_Filter_Projection_And_Default_Value()
    {
        using var l = DataFrame.From(Left);
        using var r = DataFrame.From(Right);
        var query = from a in l.AsQueryable<Kv>()
                    join b in r.AsQueryable<Kv>() on a.K equals b.K into g
                    from x in g.Where(b => b.V < 50).Select(b => b.V + 1).DefaultIfEmpty(-1)
                    select new { a.K, x };
        var expected = from a in Left
                       join b in Right on a.K equals b.K into g
                       from x in g.Where(b => b.V < 50).Select(b => b.V + 1).DefaultIfEmpty(-1)
                       select new { a.K, x };

        Assert.Equal(expected.OrderBy(t => t.K), query.ToList().OrderBy(t => t.K));
    }

    [Fact]
    public void Left_Join_With_Correlated_Group_Filter_Is_Not_Translated()
    {
        using var l = DataFrame.From(Left);
        using var r = DataFrame.From(Right);
        // The filter reads the outer row, so it cannot run on the inner side before the join (it is a non-equi
        // left join). The query is not translated, and the default policy rejects the full fallback rather than
        // returning rows from a join without the filter.
        var query = from a in l.AsQueryable<Kv>()
                    join b in r.AsQueryable<Kv>() on a.K equals b.K into g
                    from x in g.Where(b => b.V > a.V).DefaultIfEmpty()
                    select new { a.K, xv = x == null ? (long?)null : x.V };

        Assert.True(IsFullFallback(query));
        Assert.Throws<PolarsPushdownException>(() => query.ToList());
    }

    // ---------------------------------------------------------------------------------------------
    // A join result selector that cannot be projected natively used to be dropped (members read as defaults).
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void Join_With_Untranslatable_Selector_Computes_Every_Member()
    {
        using var l = DataFrame.From(Left);
        using var r = DataFrame.From(Right);
        var query = from a in l.AsQueryable<Kv>()
                    join b in r.AsQueryable<Kv>() on a.K equals b.K
                    select new { a.K, z = new string('a', (int)b.V % 5), w = Tuple.Create(b.V).Item1 };
        var expected = from a in Left
                       join b in Right on a.K equals b.K
                       select new { a.K, z = new string('a', (int)b.V % 5), w = Tuple.Create(b.V).Item1 };

        Assert.False(IsFullFallback(query));
        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void Cross_Join_With_Untranslatable_Selector_Computes_Every_Member()
    {
        using var l = DataFrame.From(Left);
        using var r = DataFrame.From(Right);
        var query = from a in l.AsQueryable<Kv>()
                    from b in r.AsQueryable<Kv>()
                    select new { a.K, bk = b.K, z = new string('b', (int)(a.V + b.V) % 4) };
        var expected = from a in Left
                       from b in Right
                       select new { a.K, bk = b.K, z = new string('b', (int)(a.V + b.V) % 4) };

        Assert.Equal(expected.OrderBy(t => t.K).ThenBy(t => t.bk).ThenBy(t => t.z),
                     query.ToList().OrderBy(t => t.K).ThenBy(t => t.bk).ThenBy(t => t.z));
    }

    // ---------------------------------------------------------------------------------------------
    // Set operations return distinct elements (By forms: first element per key, in source order).
    // ---------------------------------------------------------------------------------------------

    private static readonly Kv[] Dupes =
        [new() { K = 1, V = 10 }, new() { K = 1, V = 11 }, new() { K = 2, V = 20 }, new() { K = 3, V = 30 }, new() { K = 3, V = 31 }];
    private static readonly Kv[] Keys = [new() { K = 2, V = 0 }, new() { K = 1, V = 0 }];

    [Fact]
    public void ExceptBy_Keeps_The_First_Element_Per_Key()
    {
        using var l = DataFrame.From(Dupes);
        using var r = DataFrame.From(Keys);
        var query = l.AsQueryable<Kv>().ExceptBy(r.AsQueryable<Kv>().Select(x => x.K), x => x.K).Select(x => new { x.K, x.V });
        var expected = Dupes.ExceptBy(Keys.Select(x => x.K), x => x.K).Select(x => new { x.K, x.V });

        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void ExceptBy_With_In_Memory_Keys_Keeps_The_First_Element_Per_Key()
    {
        using var l = DataFrame.From(Dupes);
        var query = l.AsQueryable<Kv>().ExceptBy(new long[] { 2 }, x => x.K).Select(x => new { x.K, x.V });
        var expected = Dupes.ExceptBy(new long[] { 2 }, x => x.K).Select(x => new { x.K, x.V });

        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void IntersectBy_Keeps_The_First_Element_Per_Key()
    {
        using var l = DataFrame.From(Dupes);
        using var r = DataFrame.From(Keys);
        var query = l.AsQueryable<Kv>().IntersectBy(r.AsQueryable<Kv>().Select(x => x.K), x => x.K).Select(x => new { x.K, x.V });
        var expected = Dupes.IntersectBy(Keys.Select(x => x.K), x => x.K).Select(x => new { x.K, x.V });

        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void Except_And_Intersect_Return_Distinct_Rows()
    {
        long[] a = [1, 1, 2, 3, 3, 4];
        long[] b = [2, 4, 4];
        using var dfA = DataFrame.FromColumns(new { V = a });
        using var dfB = DataFrame.FromColumns(new { V = b });
        var qa = dfA.AsQueryable<Kv>().Select(x => x.V);
        var qb = dfB.AsQueryable<Kv>().Select(x => x.V);

        Assert.Equal(a.Except(b), qa.Except(qb).ToList());
        Assert.Equal(a.Intersect(b), qa.Intersect(qb).ToList());
    }

    // ---------------------------------------------------------------------------------------------
    // Group-by: where (having) runs before select; a let before the where sees every group.
    // ---------------------------------------------------------------------------------------------

    private static readonly KnV[] Groups =
    [
        new() { K = 1, V = 1 }, new() { K = 2, V = 2 }, new() { K = 3, V = 3 }, new() { K = 4, V = 4 }, new() { K = 5, V = 5 },
    ];

    [Fact]
    public void Rank_In_Group_Select_Is_Computed_After_The_Having_Filter()
    {
        using var df = DataFrame.From(Groups);
        var query = from x in df.AsQueryable<KnV>()
                    group x by x.K into g
                    let s = g.Average(x => (double?)x.V)
                    where s > 2.0
                    select new { k = g.Key, rnk = (long)s.Rank(RankMethod.Min, false) };

        var rows = query.ToList().OrderBy(r => r.k).ToList();
        Assert.Equal([3L, 4L, 5L], rows.Select(r => r.k));
        Assert.Equal([1L, 2L, 3L], rows.Select(r => r.rnk));
    }

    [Fact]
    public void Window_In_A_Let_Before_The_Where_Sees_Every_Group()
    {
        using var df = DataFrame.From(Groups);
        // The average over all five groups is 3; only groups above it are kept, still carrying 3.
        var query = from x in df.AsQueryable<KnV>()
                    group x by x.K into g
                    let s = g.Sum(x => x.V)
                    let avgAll = s.Average().Over(1)
                    where s > avgAll
                    select new { k = g.Key, avgAll };

        var rows = query.ToList().OrderBy(r => r.k).ToList();
        Assert.False(IsFullFallback(query));
        Assert.Equal([4L, 5L], rows.Select(r => r.k));
        Assert.All(rows, r => Assert.Equal(3.0, r.avgAll));
    }

    [Fact]
    public void Filter_Is_Not_Pushed_Below_A_Rank_Projection()
    {
        using var df = DataFrame.From(Groups);
        var query = df.AsQueryable<KnV>()
            .Select(x => new { x.K, rnk = (long)((double?)x.V).Rank(RankMethod.Min, false) })
            .Where(x => x.K >= 4);

        // Ranks are over all five rows, not over the two that pass the filter.
        Assert.Equal([4L, 5L], query.ToList().OrderBy(r => r.K).Select(r => r.rnk));
    }

    [Fact]
    public void Group_Orderby_On_A_Composite_Let_Reuses_The_Result_Column()
    {
        KnV[] rows = [new() { K = 1, V = null }, new() { K = 1, V = null }, new() { K = 2, V = 3 }, new() { K = 3, V = 1 }];
        using var df = DataFrame.From(rows);
        var query = from x in df.AsQueryable<KnV>()
                    group x.V by x.K into g
                    let s = g.Count(v => v != null) > 0 ? g.Sum() : null
                    orderby s, g.Key
                    select new { k = g.Key, s };
        var expected = from x in rows
                       group x.V by x.K into g
                       let s = g.Count(v => v != null) > 0 ? g.Sum() : null
                       orderby s, g.Key
                       select new { k = g.Key, s };

        Assert.Equal(expected, query.ToList());
    }

    // ---------------------------------------------------------------------------------------------
    // Ordering, statistics and materialization of nulls.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void Descending_Nulls_First_Idiom_Keeps_Nulls_First()
    {
        KnV[] rows = [new() { K = 1, V = 5 }, new() { K = 2, V = null }, new() { K = 3, V = 7 }, new() { K = 4, V = null }];
        using var df = DataFrame.From(rows);
        var query = from x in df.AsQueryable<KnV>()
                    orderby x.V == null descending, x.V descending, x.K
                    select new { x.K, x.V };
        var expected = from x in rows
                       orderby x.V == null descending, x.V descending, x.K
                       select new { x.K, x.V };

        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void Std_And_Var_Are_Null_With_Fewer_Values_Than_Ddof_Plus_One()
    {
        KnV[] rows = [new() { K = 1, V = 5 }, new() { K = 2, V = 7 }, new() { K = 2, V = null }, new() { K = 3, V = 2 }, new() { K = 3, V = 4 }];
        using var df = DataFrame.From(rows);
        var query = from x in df.AsQueryable<KnV>()
                    group x by x.K into g
                    orderby g.Key
                    select new { k = g.Key, sd = g.Std(x => (double?)x.V), vr = g.Var(x => (double?)x.V) };

        var result = query.ToList();
        Assert.Null(result[0].sd);
        Assert.Null(result[1].sd);
        Assert.Null(result[1].vr);
        Assert.Equal(Math.Sqrt(2.0), result[2].sd!.Value, 9);
        Assert.Equal(2.0, result[2].vr!.Value, 9);
    }

    [Fact]
    public void Null_In_A_Non_Nullable_Value_Member_Throws()
    {
        KnV[] rows = [new() { K = 1, V = 5 }, new() { K = 2, V = null }];
        using var df = DataFrame.From(rows);
        var query = df.AsQueryable<KnV>().Select(x => new { x.K, v = (long)x.V! });

        var ex = Assert.Throws<InvalidOperationException>(() => query.ToList());
        Assert.Contains("non-nullable", ex.Message);
    }

    // ---------------------------------------------------------------------------------------------
    // Uncorrelated scalar subqueries run inside the plan (a one-row join), not eagerly at compile time.
    // ---------------------------------------------------------------------------------------------

    private static readonly Kv[] Facts =
        [new() { K = 1, V = 10 }, new() { K = 2, V = 40 }, new() { K = 3, V = 25 }, new() { K = 4, V = 5 }, new() { K = 5, V = 60 }];
    private static readonly Kv[] Others = [new() { K = 7, V = 20 }, new() { K = 8, V = 40 }, new() { K = 9, V = 120 }];

    private static string LogicalPlanOf<T>(IQueryable<T> query) =>
        (string)query.GetType().GetMethod("ExplainLogicalPlan")!.Invoke(query, null)!;

    private static void AssertScalarIsPlanned<T>(IQueryable<T> query)
    {
        Assert.False(IsFullFallback(query));
        Assert.Contains("__subquery_agg_u", LogicalPlanOf(query));
    }

    [Fact]
    public void Uncorrelated_Aggregate_In_A_Filter_Is_A_Plan_Join()
    {
        using var f = DataFrame.From(Facts);
        using var o = DataFrame.From(Others);
        var others = o.AsQueryable<Kv>();
        var query = f.AsQueryable<Kv>().Where(x => x.V > others.Average(y => (double)y.V) * 0.5).Select(x => new { x.K, x.V });
        var expected = Facts.Where(x => x.V > Others.Average(y => (double)y.V) * 0.5).Select(x => new { x.K, x.V });

        AssertScalarIsPlanned(query);
        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void Uncorrelated_Any_And_First_In_A_Filter_Are_Plan_Joins()
    {
        using var f = DataFrame.From(Facts);
        using var o = DataFrame.From(Others);
        var others = o.AsQueryable<Kv>();
        var query = f.AsQueryable<Kv>()
            .Where(x => others.Any(y => y.V > 100) && x.V < others.OrderBy(y => y.V).Select(y => y.V).First())
            .Select(x => new { x.K, x.V });
        var expected = Facts
            .Where(x => Others.Any(y => y.V > 100) && x.V < Others.OrderBy(y => y.V).Select(y => y.V).First())
            .Select(x => new { x.K, x.V });

        AssertScalarIsPlanned(query);
        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void Uncorrelated_Aggregate_In_A_Projection_Is_A_Plan_Join()
    {
        using var f = DataFrame.From(Facts);
        using var o = DataFrame.From(Others);
        var others = o.AsQueryable<Kv>();
        var query = f.AsQueryable<Kv>().Select(x => new { x.K, share = (double)x.V / others.Sum(y => y.V) });
        var expected = Facts.Select(x => new { x.K, share = (double)x.V / Others.Sum(y => y.V) }).ToList();

        AssertScalarIsPlanned(query);
        var actual = query.ToList();
        Assert.Equal(expected.Select(e => e.K), actual.Select(a => a.K));
        Assert.All(expected.Zip(actual), p => Assert.Equal(p.First.share, p.Second.share, 12));
    }

    [Fact]
    public void Uncorrelated_Aggregate_In_A_Having_Is_A_Plan_Join()
    {
        using var f = DataFrame.From(Facts);
        using var o = DataFrame.From(Others);
        var others = o.AsQueryable<Kv>();
        var query = from x in f.AsQueryable<Kv>()
                    group x by x.K % 2 into g
                    let total = g.Sum(x => x.V)
                    where total > others.Max(y => y.V) * 0.5
                    orderby g.Key
                    select new { k = g.Key, total };
        var expected = from x in Facts
                       group x by x.K % 2 into g
                       let total = g.Sum(x => x.V)
                       where total > Others.Max(y => y.V) * 0.5
                       orderby g.Key
                       select new { k = g.Key, total };

        AssertScalarIsPlanned(query);
        Assert.Equal(expected, query.ToList());
    }

    // ---------------------------------------------------------------------------------------------
    // C# null semantics (as EF Core's default): `==` / `!=` between two nullable operands treat null as equal
    // to null; an explicit join still never matches null keys (as Enumerable.Join).
    // ---------------------------------------------------------------------------------------------

    public class Ab
    {
        public long Id { get; set; }
        public string? A { get; set; }
        public string? B { get; set; }
    }

    private static readonly Ab[] Pairs =
    [
        new() { Id = 1, A = "x", B = "x" }, new() { Id = 2, A = "x", B = "y" }, new() { Id = 3, A = null, B = "y" },
        new() { Id = 4, A = "x", B = null }, new() { Id = 5, A = null, B = null },
    ];

    [Fact]
    public void Equality_Between_Nullable_Columns_Treats_Null_As_Equal()
    {
        using var df = DataFrame.From(Pairs);
        var eq = df.AsQueryable<Ab>().Where(x => x.A == x.B).Select(x => x.Id);
        var ne = df.AsQueryable<Ab>().Where(x => x.A != x.B).Select(x => x.Id);
        var proj = df.AsQueryable<Ab>().Select(x => new { x.Id, same = x.A == x.B });

        Assert.Equal(Pairs.Where(x => x.A == x.B).Select(x => x.Id), eq.ToList());
        Assert.Equal(Pairs.Where(x => x.A != x.B).Select(x => x.Id), ne.ToList());
        Assert.Equal(Pairs.Select(x => new { x.Id, same = x.A == x.B }), proj.ToList());
    }

    [Fact]
    public void Equality_With_A_Captured_Null_Is_A_Null_Check()
    {
        using var df = DataFrame.From(Pairs);
        string? missing = null;
        var query = df.AsQueryable<Ab>().Where(x => x.A == missing).Select(x => x.Id);

        Assert.Equal(Pairs.Where(x => x.A == missing).Select(x => x.Id), query.ToList());
    }

    [Fact]
    public void Correlated_Equality_Matches_Null_With_Null()
    {
        using var df = DataFrame.From(Pairs);
        using var other = DataFrame.From(Pairs);
        var others = other.AsQueryable<Ab>();
        var query = df.AsQueryable<Ab>().Where(x => others.Any(y => y.B == x.A && y.Id != x.Id)).Select(x => x.Id);
        var expected = Pairs.Where(x => Pairs.Any(y => y.B == x.A && y.Id != x.Id)).Select(x => x.Id);

        Assert.False(IsFullFallback(query));
        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void Explicit_Join_Never_Matches_Null_Keys()
    {
        using var l = DataFrame.From(Pairs);
        using var r = DataFrame.From(Pairs);
        var query = from x in l.AsQueryable<Ab>()
                    join y in r.AsQueryable<Ab>() on x.A equals y.B
                    select new { x = x.Id, y = y.Id };
        var expected = from x in Pairs
                       join y in Pairs on x.A equals y.B
                       select new { x = x.Id, y = y.Id };

        Assert.Equal(expected.OrderBy(t => t.x).ThenBy(t => t.y), query.ToList().OrderBy(t => t.x).ThenBy(t => t.y));
    }

    // ---------------------------------------------------------------------------------------------
    // Lifted comparisons follow C#: `a > b` over a null operand is false (not null), `a != lit` is true on a
    // null `a`, and NOT over either keeps that. A filter drops the compensation where null and false agree.
    // ---------------------------------------------------------------------------------------------

    private static readonly KnV[] Nullables =
        [new() { K = 1, V = 10 }, new() { K = 2, V = null }, new() { K = 3, V = 30 }, new() { K = 4, V = null }];

    [Fact]
    public void Lifted_Comparison_In_A_Projection_Is_False_On_Null()
    {
        using var df = DataFrame.From(Nullables);
        var query = df.AsQueryable<KnV>().Select(x => new { x.K, gt = x.V > 15, le = x.V <= 15, notGt = !(x.V > 15), eq = x.V == 10 });
        var expected = Nullables.Select(x => new { x.K, gt = x.V > 15, le = x.V <= 15, notGt = !(x.V > 15), eq = x.V == 10 });

        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void Negated_Lifted_Comparison_In_A_Filter_Keeps_Null_Rows()
    {
        using var df = DataFrame.From(Nullables);
        var notGt = df.AsQueryable<KnV>().Where(x => !(x.V > 15)).Select(x => x.K);
        var notEq = df.AsQueryable<KnV>().Where(x => x.V != 10).Select(x => x.K);
        var notEqNeg = df.AsQueryable<KnV>().Where(x => !(x.V == 10)).Select(x => x.K);

        Assert.Equal(Nullables.Where(x => !(x.V > 15)).Select(x => x.K), notGt.ToList());
        Assert.Equal(Nullables.Where(x => x.V != 10).Select(x => x.K), notEq.ToList());
        Assert.Equal(Nullables.Where(x => !(x.V == 10)).Select(x => x.K), notEqNeg.ToList());
    }

    [Fact]
    public void Lifted_Comparison_In_All_And_Count_Is_False_On_Null()
    {
        using var df = DataFrame.From(Nullables);
        var q = df.AsQueryable<KnV>();

        Assert.Equal(Nullables.All(x => x.V > 5), q.All(x => x.V > 5));
        Assert.Equal(Nullables.Count(x => !(x.V > 15)), q.Count(x => !(x.V > 15)));
    }

    [Fact]
    public void Filter_Keeps_Plain_Comparisons_Where_Null_And_False_Agree()
    {
        using var df = DataFrame.From(Nullables);
        var query = df.AsQueryable<KnV>().Where(x => x.V > 5 && (x.V == 10 || x.V >= 30)).Select(x => x.K);
        var plan = LogicalPlanOf(query);

        Assert.DoesNotContain("COALESCE", plan);
        Assert.DoesNotContain("<=>", plan);
        Assert.Equal(Nullables.Where(x => x.V > 5 && (x.V == 10 || x.V >= 30)).Select(x => x.K), query.ToList());
    }

    // ---------------------------------------------------------------------------------------------
    // A condition on the outer row inside a correlated subquery predicate is decorrelated, not a fallback:
    // `Any(y => p && q(x))` is `q(x) && Any(y => p)`.
    // ---------------------------------------------------------------------------------------------

    private static readonly KnV[] Outer =
        [new() { K = 1, V = 10 }, new() { K = 1, V = null }, new() { K = 2, V = 5 }, new() { K = 3, V = null }, new() { K = 4, V = 7 }];

    [Fact]
    public void Outer_Only_Condition_In_Any_Is_Hoisted()
    {
        using var o = DataFrame.From(Outer);
        using var i = DataFrame.From(Right);
        var inner = i.AsQueryable<Kv>();
        var any = o.AsQueryable<KnV>().Where(x => inner.Any(y => y.K == x.K && x.V != null)).Select(x => new { x.K, x.V });
        var notAny = o.AsQueryable<KnV>().Where(x => !inner.Any(y => y.K == x.K && x.V != null)).Select(x => new { x.K, x.V });
        var withTheta = o.AsQueryable<KnV>().Where(x => inner.Any(y => y.K == x.K && y.V > x.V && x.V > 6)).Select(x => new { x.K, x.V });

        Assert.False(IsFullFallback(any));
        Assert.False(IsFullFallback(notAny));
        Assert.False(IsFullFallback(withTheta));
        Assert.Equal(Outer.Where(x => Right.Any(y => y.K == x.K && x.V != null)).Select(x => new { x.K, x.V }), any.ToList());
        Assert.Equal(Outer.Where(x => !Right.Any(y => y.K == x.K && x.V != null)).Select(x => new { x.K, x.V }), notAny.ToList());
        Assert.Equal(Outer.Where(x => Right.Any(y => y.K == x.K && y.V > x.V && x.V > 6)).Select(x => new { x.K, x.V }), withTheta.ToList());
    }

    [Fact]
    public void Outer_Only_Condition_In_A_Correlated_Count_Is_Part_Of_The_Predicate()
    {
        using var o = DataFrame.From(Outer);
        using var i = DataFrame.From(Right);
        var inner = i.AsQueryable<Kv>();
        var query = o.AsQueryable<KnV>().Select(x => new { x.K, x.V, n = inner.Count(y => y.K == x.K && x.V != null) });
        var expected = Outer.Select(x => new { x.K, x.V, n = Right.Count(y => y.K == x.K && x.V != null) });

        Assert.False(IsFullFallback(query));
        Assert.Equal(expected, query.ToList());
    }

    // ---------------------------------------------------------------------------------------------
    // .NET string building reads null as the empty string (String.Concat / Format / Join), and Contains over
    // an in-memory collection matches a null item to a null element.
    // ---------------------------------------------------------------------------------------------

    public class Strs
    {
        public long Id { get; set; }
        public string? A { get; set; }
        public string? B { get; set; }
        public long? N { get; set; }
        public List<string?> Tags { get; set; } = [];
    }

    private static readonly Strs[] StrRows =
    [
        new() { Id = 1, A = "x", B = null, N = 5, Tags = ["a", null, "b"] },
        new() { Id = 2, A = null, B = "y", N = null, Tags = ["c"] },
        new() { Id = 3, A = null, B = null, N = null, Tags = [null] },
    ];

    [Fact]
    public void String_Concatenation_Reads_Null_As_Empty()
    {
        using var df = DataFrame.From(StrRows);
        var query = df.AsQueryable<Strs>().Select(x => new
        {
            plus = x.A + x.B,
            mixed = x.A + "-" + x.N,
            concat = string.Concat(x.A, x.B),
            format = string.Format("{0}|{1}", x.A, x.B),
            interpolated = $"{x.A}:{x.N}",
            joinArgs = string.Join(",", new[] { x.A, x.B }),
            joinList = string.Join(",", x.Tags),
        });
        var expected = StrRows.Select(x => new
        {
            plus = x.A + x.B,
            mixed = x.A + "-" + x.N,
            concat = string.Concat(x.A, x.B),
            format = string.Format("{0}|{1}", x.A, x.B),
            interpolated = $"{x.A}:{x.N}",
            joinArgs = string.Join(",", new[] { x.A, x.B }),
            joinList = string.Join(",", x.Tags),
        });

        Assert.Equal(expected, query.ToList());
    }

    [Fact]
    public void Contains_Over_A_Collection_Matches_Null_To_Null()
    {
        using var df = DataFrame.From(StrRows);
        string?[] withNull = ["x", null];
        string?[] withoutNull = ["x"];
        var q = df.AsQueryable<Strs>();

        Assert.Equal(StrRows.Where(x => withNull.Contains(x.A)).Select(x => x.Id), q.Where(x => withNull.Contains(x.A)).Select(x => x.Id).ToList());
        Assert.Equal(StrRows.Where(x => !withNull.Contains(x.A)).Select(x => x.Id), q.Where(x => !withNull.Contains(x.A)).Select(x => x.Id).ToList());
        Assert.Equal(StrRows.Where(x => !withoutNull.Contains(x.A)).Select(x => x.Id), q.Where(x => !withoutNull.Contains(x.A)).Select(x => x.Id).ToList());
        Assert.Equal(StrRows.Select(x => x.Tags.Contains(null)), q.Select(x => x.Tags.Contains(null)).ToList());
    }

    // ---------------------------------------------------------------------------------------------
    // Uncorrelated First / Single / ElementAt run inside the plan; a wrong row count raises LINQ's exception
    // while the query runs (not while it compiles), and OrDefault yields default(T).
    // ---------------------------------------------------------------------------------------------

    private static Exception? Caught(Action action)
    {
        try { action(); return null; }
        catch (Exception e) { return e; }
    }

    private static void AssertSameFailure(Action linqToObjects, Action polars)
    {
        var expected = Caught(linqToObjects);
        var actual = Caught(polars);
        Assert.NotNull(expected);
        Assert.NotNull(actual);
        Assert.IsType(expected!.GetType(), actual);
        Assert.Equal(expected.Message, actual!.Message);
    }

    [Fact]
    public void Single_First_And_ElementAt_In_A_Filter_Are_Plan_Joins()
    {
        using var f = DataFrame.From(Facts);
        using var o = DataFrame.From(Others);
        var others = o.AsQueryable<Kv>();
        var single = f.AsQueryable<Kv>().Where(x => x.V > others.Single(y => y.K == 8).V).Select(x => x.K);
        var first = f.AsQueryable<Kv>().Where(x => x.V < others.OrderBy(y => y.V).First().V).Select(x => x.K);
        var elementAt = f.AsQueryable<Kv>().Where(x => x.V < others.Select(y => y.V).ElementAt(1)).Select(x => x.K);

        AssertScalarIsPlanned(single);
        AssertScalarIsPlanned(first);
        AssertScalarIsPlanned(elementAt);
        Assert.Equal(Facts.Where(x => x.V > Others.Single(y => y.K == 8).V).Select(x => x.K), single.ToList());
        Assert.Equal(Facts.Where(x => x.V < Others.OrderBy(y => y.V).First().V).Select(x => x.K), first.ToList());
        Assert.Equal(Facts.Where(x => x.V < Others.Select(y => y.V).ElementAt(1)).Select(x => x.K), elementAt.ToList());
    }

    [Fact]
    public void Wrong_Row_Count_Raises_The_Linq_Exception_When_The_Query_Runs()
    {
        using var f = DataFrame.From(Facts);
        using var o = DataFrame.From(Others);
        var others = o.AsQueryable<Kv>();
        var q = f.AsQueryable<Kv>();

        var none = q.Where(x => x.V > others.Single(y => y.K == 99).V).Select(x => x.K);
        var many = q.Where(x => x.V > others.Single(y => y.K > 7).V).Select(x => x.K);
        var manyNoPredicate = q.Where(x => x.V > others.Select(y => y.V).Single()).Select(x => x.K);
        var firstNone = q.Where(x => x.V > others.First(y => y.K == 99).V).Select(x => x.K);
        var orDefaultMany = q.Where(x => x.V > others.Select(y => y.V).SingleOrDefault()).Select(x => x.K);
        var outOfRange = q.Where(x => x.V > others.Select(y => y.V).ElementAt(5)).Select(x => x.K);

        // Compiling (Explain) does not run the subquery.
        Assert.Contains("__subquery_agg_u", LogicalPlanOf(none));

        AssertSameFailure(() => Facts.Where(x => x.V > Others.Single(y => y.K == 99).V).ToList(), () => none.ToList());
        AssertSameFailure(() => Facts.Where(x => x.V > Others.Single(y => y.K > 7).V).ToList(), () => many.ToList());
        AssertSameFailure(() => Facts.Where(x => x.V > Others.Select(y => y.V).Single()).ToList(), () => manyNoPredicate.ToList());
        AssertSameFailure(() => Facts.Where(x => x.V > Others.First(y => y.K == 99).V).ToList(), () => firstNone.ToList());
        AssertSameFailure(() => Facts.Where(x => x.V > Others.Select(y => y.V).SingleOrDefault()).ToList(), () => orDefaultMany.ToList());
        AssertSameFailure(() => Facts.Where(x => x.V > Others.Select(y => y.V).ElementAt(5)).ToList(), () => outOfRange.ToList());
    }

    [Fact]
    public void OrDefault_Over_No_Rows_Is_The_Default_Value()
    {
        using var f = DataFrame.From(Facts);
        using var o = DataFrame.From(Others);
        var others = o.AsQueryable<Kv>();
        var q = f.AsQueryable<Kv>();

        var first = q.Where(x => x.V > others.Where(y => y.K == 99).Select(y => y.V).FirstOrDefault()).Select(x => x.K);
        var single = q.Where(x => x.V > others.Where(y => y.K == 99).Select(y => y.V).SingleOrDefault()).Select(x => x.K);
        var elementAt = q.Where(x => x.V > others.Select(y => y.V).ElementAtOrDefault(7)).Select(x => x.K);

        AssertScalarIsPlanned(first);
        Assert.Equal(Facts.Where(x => x.V > Others.Where(y => y.K == 99).Select(y => y.V).FirstOrDefault()).Select(x => x.K), first.ToList());
        Assert.Equal(Facts.Where(x => x.V > Others.Where(y => y.K == 99).Select(y => y.V).SingleOrDefault()).Select(x => x.K), single.ToList());
        Assert.Equal(Facts.Where(x => x.V > Others.Select(y => y.V).ElementAtOrDefault(7)).Select(x => x.K), elementAt.ToList());
    }

    [Fact]
    public void Last_Reads_The_Row_Order_And_Raises_On_No_Rows()
    {
        using var f = DataFrame.From(Facts);
        using var o = DataFrame.From(Others);
        var others = o.AsQueryable<Kv>();
        var q = f.AsQueryable<Kv>();

        var last = q.Where(x => x.V < others.Last().V).Select(x => x.K);
        var lastMatching = q.Where(x => x.V > others.Last(y => y.V < 100).V).Select(x => x.K);
        // Groups keep first-appearance order (LINQ), so the last group key is that of the last new key.
        var lastGroup = q.Where(x => x.K == others.GroupBy(y => y.K - 5).Select(g => g.Key).Last()).Select(x => x.K);
        var orDefault = q.Where(x => x.V > others.Where(y => y.K == 99).Select(y => y.V).LastOrDefault()).Select(x => x.K);
        var none = q.Where(x => x.V > others.Last(y => y.K == 99).V).Select(x => x.K);

        AssertScalarIsPlanned(last);
        AssertScalarIsPlanned(lastGroup);
        Assert.Equal(Facts.Where(x => x.V < Others.Last().V).Select(x => x.K), last.ToList());
        Assert.Equal(Facts.Where(x => x.V > Others.Last(y => y.V < 100).V).Select(x => x.K), lastMatching.ToList());
        Assert.Equal(Facts.Where(x => x.K == Others.GroupBy(y => y.K - 5).Select(g => g.Key).Last()).Select(x => x.K), lastGroup.ToList());
        Assert.Equal(Facts.Where(x => x.V > Others.Where(y => y.K == 99).Select(y => y.V).LastOrDefault()).Select(x => x.K), orDefault.ToList());
        AssertSameFailure(() => Facts.Where(x => x.V > Others.Last(y => y.K == 99).V).ToList(), () => none.ToList());
    }
}
