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
        Assert.Equal(expected, [.. query]);
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
        Assert.Equal(expected, [.. query]);
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

        Assert.Equal(expected, [.. query]);
    }

    [Fact]
    public void ExceptBy_With_In_Memory_Keys_Keeps_The_First_Element_Per_Key()
    {
        using var l = DataFrame.From(Dupes);
        var query = l.AsQueryable<Kv>().ExceptBy(new long[] { 2 }, x => x.K).Select(x => new { x.K, x.V });
        var expected = Dupes.ExceptBy(new long[] { 2 }, x => x.K).Select(x => new { x.K, x.V });

        Assert.Equal(expected, [.. query]);
    }

    [Fact]
    public void IntersectBy_Keeps_The_First_Element_Per_Key()
    {
        using var l = DataFrame.From(Dupes);
        using var r = DataFrame.From(Keys);
        var query = l.AsQueryable<Kv>().IntersectBy(r.AsQueryable<Kv>().Select(x => x.K), x => x.K).Select(x => new { x.K, x.V });
        var expected = Dupes.IntersectBy(Keys.Select(x => x.K), x => x.K).Select(x => new { x.K, x.V });

        Assert.Equal(expected, [.. query]);
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

        Assert.Equal(a.Except(b), [.. qa.Except(qb)]);
        Assert.Equal(a.Intersect(b), [.. qa.Intersect(qb)]);
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

        Assert.Equal(expected, [.. query]);
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

        Assert.Equal(expected, [.. query]);
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
        Assert.Equal(expected, [.. query]);
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
        Assert.Equal(expected, [.. query]);
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
        Assert.Equal(expected, [.. query]);
    }

    // ---------------------------------------------------------------------------------------------
    // C# null semantics (as EF Core's default): `==` / `!=` between two nullable operands treat null as equal
    // to null; an explicit join still never matches null keys (as Enumerable.Join).
    // ---------------------------------------------------------------------------------------------

    public class Ab
    {
        public long Id { get; set; }
        public string A { get; set; }
        public string B { get; set; }
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

        Assert.Equal(Pairs.Where(x => x.A == x.B).Select(x => x.Id), [.. eq]);
        Assert.Equal(Pairs.Where(x => x.A != x.B).Select(x => x.Id), [.. ne]);
        Assert.Equal(Pairs.Select(x => new { x.Id, same = x.A == x.B }), [.. proj]);
    }

    [Fact]
    public void Equality_With_A_Captured_Null_Is_A_Null_Check()
    {
        using var df = DataFrame.From(Pairs);
        string missing = null;
        var query = df.AsQueryable<Ab>().Where(x => x.A == missing).Select(x => x.Id);

        Assert.Equal(Pairs.Where(x => x.A == missing).Select(x => x.Id), [.. query]);
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
        Assert.Equal(expected, [.. query]);
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

        Assert.Equal(expected, [.. query]);
    }

    [Fact]
    public void Negated_Lifted_Comparison_In_A_Filter_Keeps_Null_Rows()
    {
        using var df = DataFrame.From(Nullables);
        var notGt = df.AsQueryable<KnV>().Where(x => !(x.V > 15)).Select(x => x.K);
        var notEq = df.AsQueryable<KnV>().Where(x => x.V != 10).Select(x => x.K);
        var notEqNeg = df.AsQueryable<KnV>().Where(x => !(x.V == 10)).Select(x => x.K);

        Assert.Equal(Nullables.Where(x => !(x.V > 15)).Select(x => x.K), [.. notGt]);
        Assert.Equal(Nullables.Where(x => x.V != 10).Select(x => x.K), [.. notEq]);
        Assert.Equal(Nullables.Where(x => !(x.V == 10)).Select(x => x.K), [.. notEqNeg]);
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
        Assert.Equal(Nullables.Where(x => x.V > 5 && (x.V == 10 || x.V >= 30)).Select(x => x.K), [.. query]);
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
        Assert.Equal(Outer.Where(x => Right.Any(y => y.K == x.K && x.V != null)).Select(x => new { x.K, x.V }), [.. any]);
        Assert.Equal(Outer.Where(x => !Right.Any(y => y.K == x.K && x.V != null)).Select(x => new { x.K, x.V }), [.. notAny]);
        Assert.Equal(Outer.Where(x => Right.Any(y => y.K == x.K && y.V > x.V && x.V > 6)).Select(x => new { x.K, x.V }), [.. withTheta]);
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
        Assert.Equal(expected, [.. query]);
    }

    // ---------------------------------------------------------------------------------------------
    // .NET string building reads null as the empty string (String.Concat / Format / Join), and Contains over
    // an in-memory collection matches a null item to a null element.
    // ---------------------------------------------------------------------------------------------

    public class Strs
    {
        public long Id { get; set; }
        public string A { get; set; }
        public string B { get; set; }
        public long? N { get; set; }
        public List<string> Tags { get; set; } = [];
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

        Assert.Equal(expected, [.. query]);
    }

    [Fact]
    public void Contains_Over_A_Collection_Matches_Null_To_Null()
    {
        using var df = DataFrame.From(StrRows);
        string[] withNull = ["x", null];
        string[] withoutNull = ["x"];
        var q = df.AsQueryable<Strs>();

        Assert.Equal(StrRows.Where(x => withNull.Contains(x.A)).Select(x => x.Id), [.. q.Where(x => withNull.Contains(x.A)).Select(x => x.Id)]);
        Assert.Equal(StrRows.Where(x => !withNull.Contains(x.A)).Select(x => x.Id), [.. q.Where(x => !withNull.Contains(x.A)).Select(x => x.Id)]);
        Assert.Equal(StrRows.Where(x => !withoutNull.Contains(x.A)).Select(x => x.Id), [.. q.Where(x => !withoutNull.Contains(x.A)).Select(x => x.Id)]);
        Assert.Equal(StrRows.Select(x => x.Tags.Contains(null)), [.. q.Select(x => x.Tags.Contains(null))]);
    }

    // ---------------------------------------------------------------------------------------------
    // Uncorrelated First / Single / ElementAt run inside the plan; a wrong row count raises LINQ's exception
    // while the query runs (not while it compiles), and OrDefault yields default(T).
    // ---------------------------------------------------------------------------------------------

    private static Exception Caught(Action action)
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
        Assert.Equal(Facts.Where(x => x.V > Others.Single(y => y.K == 8).V).Select(x => x.K), [.. single]);
        Assert.Equal(Facts.Where(x => x.V < Others.OrderBy(y => y.V).First().V).Select(x => x.K), [.. first]);
        Assert.Equal(Facts.Where(x => x.V < Others.Select(y => y.V).ElementAt(1)).Select(x => x.K), [.. elementAt]);
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
        Assert.Equal(Facts.Where(x => x.V > Others.Where(y => y.K == 99).Select(y => y.V).FirstOrDefault()).Select(x => x.K), [.. first]);
        Assert.Equal(Facts.Where(x => x.V > Others.Where(y => y.K == 99).Select(y => y.V).SingleOrDefault()).Select(x => x.K), [.. single]);
        Assert.Equal(Facts.Where(x => x.V > Others.Select(y => y.V).ElementAtOrDefault(7)).Select(x => x.K), [.. elementAt]);
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
        Assert.Equal(Facts.Where(x => x.V < Others.Last().V).Select(x => x.K), [.. last]);
        Assert.Equal(Facts.Where(x => x.V > Others.Last(y => y.V < 100).V).Select(x => x.K), [.. lastMatching]);
        Assert.Equal(Facts.Where(x => x.K == Others.GroupBy(y => y.K - 5).Select(g => g.Key).Last()).Select(x => x.K), [.. lastGroup]);
        Assert.Equal(Facts.Where(x => x.V > Others.Where(y => y.K == 99).Select(y => y.V).LastOrDefault()).Select(x => x.K), [.. orDefault]);
        AssertSameFailure(() => Facts.Where(x => x.V > Others.Last(y => y.K == 99).V).ToList(), () => none.ToList());
    }

    // ---------------------------------------------------------------------------------------------
    // .NET floating semantics (double, float, Half): a comparison involving NaN is false (`!=` true), ordering
    // puts NaN before every number (after null), and Min is NaN as soon as one value is NaN.
    // ---------------------------------------------------------------------------------------------

    public class Floats
    {
        public long Id { get; set; }
        public double? D { get; set; }
        public double? E { get; set; }
        public float F { get; set; }
        public Half? H { get; set; }
    }

    private static readonly Floats[] FloatRows =
    [
        new() { Id = 1, D = double.NaN, E = double.NaN, F = float.NaN, H = Half.NaN },
        new() { Id = 2, D = double.NaN, E = 1.0, F = 2f, H = (Half)2 },
        new() { Id = 3, D = 1.0, E = null, F = 7f, H = null },
        new() { Id = 4, D = null, E = double.NaN, F = float.NaN, H = (Half)7 },
        new() { Id = 5, D = 7.0, E = 7.0, F = 0.5f, H = (Half)0.5 },
    ];

    [Fact]
    public void Comparisons_Involving_NaN_Follow_DotNet()
    {
        using var df = DataFrame.From(FloatRows);
        var query = df.AsQueryable<Floats>().Select(x => new
        {
            x.Id,
            eq = x.D == x.E, ne = x.D != x.E, lt = x.D < x.E, le = x.D <= x.E, gt = x.D > x.E, ge = x.D >= x.E,
            gtLit = x.D > 5.0, ltLit = x.D < 5.0, eqNaN = x.D == double.NaN,
            fGt = x.F > 1f, fLe = x.F <= 1f, hGt = x.H > (Half)1, hEq = x.H == x.H,
        });
        var expected = FloatRows.Select(x => new
        {
            x.Id,
            eq = x.D == x.E, ne = x.D != x.E, lt = x.D < x.E, le = x.D <= x.E, gt = x.D > x.E, ge = x.D >= x.E,
            gtLit = x.D > 5.0, ltLit = x.D < 5.0, eqNaN = x.D == double.NaN,
            fGt = x.F > 1f, fLe = x.F <= 1f, hGt = x.H > (Half)1, hEq = x.H == x.H,
        });

        Assert.Equal(expected, [.. query]);
        Assert.Equal(FloatRows.Where(x => x.D > 5.0).Select(x => x.Id), [.. df.AsQueryable<Floats>().Where(x => x.D > 5.0).Select(x => x.Id)]);
        Assert.Equal(FloatRows.Where(x => x.D != x.E).Select(x => x.Id), [.. df.AsQueryable<Floats>().Where(x => x.D != x.E).Select(x => x.Id)]);
        Assert.Equal(FloatRows.Where(x => x.H > (Half)1).Select(x => x.Id), [.. df.AsQueryable<Floats>().Where(x => x.H > (Half)1).Select(x => x.Id)]);
    }

    [Fact]
    public void Ordering_Puts_NaN_Before_Numbers_After_Null()
    {
        using var df = DataFrame.From(FloatRows);
        var q = df.AsQueryable<Floats>();

        Assert.Equal(FloatRows.OrderBy(x => x.D).ThenBy(x => x.Id).Select(x => x.Id), [.. q.OrderBy(x => x.D).ThenBy(x => x.Id).Select(x => x.Id)]);
        Assert.Equal(FloatRows.OrderByDescending(x => x.D).ThenBy(x => x.Id).Select(x => x.Id), [.. q.OrderByDescending(x => x.D).ThenBy(x => x.Id).Select(x => x.Id)]);
        Assert.Equal(FloatRows.OrderBy(x => x.F).Select(x => x.Id), [.. q.OrderBy(x => x.F).Select(x => x.Id)]);
        Assert.Equal(FloatRows.OrderByDescending(x => x.H).Select(x => x.Id), [.. q.OrderByDescending(x => x.H).Select(x => x.Id)]);
        Assert.Equal(
            FloatRows.GroupBy(x => x.D).Select(g => new { g.Key, n = g.Count() }).OrderBy(r => r.Key),
            [.. q.GroupBy(x => x.D).Select(g => new { g.Key, n = g.Count() }).OrderBy(r => r.Key)]);
    }

    [Fact]
    public void Min_Is_NaN_When_A_Value_Is_NaN()
    {
        using var df = DataFrame.From(FloatRows);
        var q = df.AsQueryable<Floats>();

        Assert.Equal(FloatRows.Min(x => x.D), q.Min(x => x.D));
        Assert.Equal(FloatRows.Max(x => x.D), q.Max(x => x.D));
        Assert.Equal(FloatRows.Where(x => x.Id > 2).Min(x => x.D), q.Where(x => x.Id > 2).Min(x => x.D));
        Assert.Equal(
            FloatRows.GroupBy(x => x.Id % 2).Select(g => new { g.Key, min = g.Min(x => x.F), max = g.Max(x => x.F) }).OrderBy(r => r.Key),
            [.. q.GroupBy(x => x.Id % 2).Select(g => new { g.Key, min = g.Min(x => x.F), max = g.Max(x => x.F) }).OrderBy(r => r.Key)]);
    }

    [Fact]
    public void Group_Sequence_Ordering_Places_Null_And_NaN_As_Linq()
    {
        using var df = DataFrame.From(FloatRows);
        var query = df.AsQueryable<Floats>().GroupBy(x => x.Id % 2).Select(g => new
        {
            g.Key,
            asc = g.OrderBy(x => x.D).Select(x => x.Id).First(),
            desc = g.OrderByDescending(x => x.D).Select(x => x.Id).First(),
            lowest = g.Select(x => x.F).Order().First(),
        }).OrderBy(r => r.Key);
        var expected = FloatRows.GroupBy(x => x.Id % 2).Select(g => new
        {
            g.Key,
            asc = g.OrderBy(x => x.D).Select(x => x.Id).First(),
            desc = g.OrderByDescending(x => x.D).Select(x => x.Id).First(),
            lowest = g.Select(x => x.F).Order().First(),
        }).OrderBy(r => r.Key);

        Assert.Equal(expected, [.. query]);
    }

    [Fact]
    public void MinBy_And_MaxBy_Treat_NaN_As_The_Least_Key()
    {
        using var df = DataFrame.From(FloatRows);
        var q = df.AsQueryable<Floats>();

        Assert.Equal(FloatRows.MinBy(x => x.D)!.Id, q.MinBy(x => x.D)!.Id);
        Assert.Equal(FloatRows.MaxBy(x => x.D)!.Id, q.MaxBy(x => x.D)!.Id);
        Assert.Equal(FloatRows.MinBy(x => x.F)!.Id, q.MinBy(x => x.F)!.Id);
        Assert.Equal(FloatRows.MaxBy(x => x.H)!.Id, q.MaxBy(x => x.H)!.Id);
    }

    [Fact]
    public void Window_Ordering_Places_Null_And_NaN_As_Linq()
    {
        using var df = DataFrame.From(FloatRows);
        var query = df.AsQueryable<Floats>().Select(x => new { x.Id, running = x.Id.CumSum().Over(1, x.D) });
        // The running sum of Id in LINQ order of D (null, NaN, numbers), read back per row.
        var ordered = FloatRows.OrderBy(x => x.D).ToList();
        var running = ordered.Select((x, i) => (x.Id, sum: ordered.Take(i + 1).Sum(y => y.Id))).ToDictionary(t => t.Id, t => t.sum);

        Assert.Equal(FloatRows.Select(x => new { x.Id, running = running[x.Id] }), [.. query]);
    }

    // ---------------------------------------------------------------------------------------------
    // .NET arithmetic: `/` over integers and `%` over any number truncate toward zero (the engine floors),
    // and decimal division keeps 18 fractional digits (not the dividend's scale).
    // ---------------------------------------------------------------------------------------------

    public class Nums
    {
        public long Id { get; set; }
        public int I { get; set; }
        public int J { get; set; }
        public long? L { get; set; }
        public double D { get; set; }
        public decimal M { get; set; }
    }

    private static readonly Nums[] NumRows =
    [
        new() { Id = 1, I = -7, J = 2, L = -9, D = -7.5, M = 2.5m },
        new() { Id = 2, I = 7, J = -2, L = null, D = 7.5, M = -1.125m },
        new() { Id = 3, I = -8, J = -3, L = 9, D = -0.5, M = 0.125m },
        new() { Id = 4, I = 0, J = 5, L = -1, D = 4.0, M = 10m },
    ];

    [Fact]
    public void Integer_Division_And_Remainder_Truncate_Toward_Zero()
    {
        using var df = DataFrame.From(NumRows);
        var query = df.AsQueryable<Nums>().Select(x => new
        {
            x.Id, div = x.I / x.J, rem = x.I % x.J, divLit = x.I / 3, remLit = x.I % 3, ldiv = x.L / 2, lrem = x.L % 4,
            drem = x.D % 2.0, scaled = x.I / x.J * 1.0,
        });
        var expected = NumRows.Select(x => new
        {
            x.Id, div = x.I / x.J, rem = x.I % x.J, divLit = x.I / 3, remLit = x.I % 3, ldiv = x.L / 2, lrem = x.L % 4,
            drem = x.D % 2.0, scaled = x.I / x.J * 1.0,
        });

        Assert.Equal(expected, query.ToList());
        Assert.Equal(NumRows.Where(x => x.I % 2 == -1).Select(x => x.Id), df.AsQueryable<Nums>().Where(x => x.I % 2 == -1).Select(x => x.Id).ToList());
    }

    [Fact]
    public void Decimal_Division_Keeps_Eighteen_Fractional_Digits()
    {
        using var df = DataFrame.From(NumRows);
        var result = df.AsQueryable<Nums>().Select(x => x.M / 3).ToList();

        Assert.Equal(NumRows.Select(x => Math.Round(x.M / 3, 18)), result);
    }

    // Contract: native string order is ordinal (code point), not culture.
    [Fact]
    public void Strings_Order_And_Compare_Ordinally()
    {
        Text2[] rows = [new() { Id = 1, S = "äpfel", T = "zebra" }, new() { Id = 2, S = "apple", T = "Apple" }, new() { Id = 3, S = "Banana", T = "banana" }];
        using var df = DataFrame.From(rows);
        var q = df.AsQueryable<Text2>();

        Assert.Equal(rows.OrderBy(x => x.S, StringComparer.Ordinal).Select(x => x.Id), q.OrderBy(x => x.S).Select(x => x.Id).ToList());
        Assert.Equal(rows.Select(x => Math.Sign(string.CompareOrdinal(x.S, x.T))), q.Select(x => x.S.CompareTo(x.T)).ToList());
    }

    public class Text2
    {
        public long Id { get; set; }
        public string S { get; set; } = "";
        public string T { get; set; } = "";
    }

    // ---------------------------------------------------------------------------------------------
    // C# short-circuit evaluation: a UDF on the right of `&&` / `||` / `??`, or in a `?:` branch, runs only on
    // the rows that reach it, so a guard such as `x.S != null && F(x.S)` protects the function.
    // ---------------------------------------------------------------------------------------------

    public class Guarded
    {
        public long Id { get; set; }
        public string S { get; set; }
        public int N { get; set; }
    }

    private static readonly Guarded[] GuardedRows =
        [new() { Id = 1, S = "1ab", N = 5 }, new() { Id = 2, S = null, N = 0 }, new() { Id = 3, S = "", N = 2 }, new() { Id = 4, S = "x", N = -1 }];

    public static bool StartsWithDigit(string s) => char.IsDigit(s[0]);
    public static int Hundredth(int n) => 100 / n;
    public static string Third(string s) => s.Substring(2, 1);
    public static string NullFor(int n) => n > 0 ? null : "neg";

    [Fact]
    public void Udfs_Run_Only_Where_Short_Circuit_Evaluation_Reaches_Them()
    {
        using var df = DataFrame.From(GuardedRows);
        var q = df.AsQueryable<Guarded>();

        Assert.Equal(
            GuardedRows.Where(x => x.S != null && x.S.Length > 0 && StartsWithDigit(x.S)).Select(x => x.Id),
            q.Where(x => x.S != null && x.S.Length > 0 && StartsWithDigit(x.S)).Select(x => x.Id).ToList());
        Assert.Equal(
            GuardedRows.Where(x => x.N != 0 && Hundredth(x.N) > 10).Select(x => x.Id),
            q.Where(x => x.N != 0 && Hundredth(x.N) > 10).Select(x => x.Id).ToList());
        Assert.Equal(
            GuardedRows.Where(x => x.N == 0 || Hundredth(x.N) > 10).Select(x => x.Id),
            q.Where(x => x.N == 0 || Hundredth(x.N) > 10).Select(x => x.Id).ToList());
        Assert.Equal(
            GuardedRows.Select(x => x.S != null && x.S.Length >= 3 ? Third(x.S) : "-"),
            q.Select(x => x.S != null && x.S.Length >= 3 ? Third(x.S) : "-").ToList());
        Assert.Equal(
            GuardedRows.Select(x => x.N == 0 ? 0 : Hundredth(x.N)),
            q.Select(x => x.N == 0 ? 0 : Hundredth(x.N)).ToList());
        Assert.Equal(
            GuardedRows.Select(x => x.N != 0 ? (x.N > 0 ? Hundredth(x.N) : -Hundredth(x.N)) : 0),
            q.Select(x => x.N != 0 ? (x.N > 0 ? Hundredth(x.N) : -Hundredth(x.N)) : 0).ToList());
        Assert.Equal(
            GuardedRows.Select(x => x.S ?? Third("ab" + x.N)),
            q.Select(x => x.S ?? Third("ab" + x.N)).ToList());
        Assert.Equal(
            GuardedRows.Select(x => NullFor(x.N) ?? "pos"),
            q.Select(x => NullFor(x.N) ?? "pos").ToList());
    }

    [Fact]
    public void Udfs_Without_Short_Circuit_Still_Run_On_Every_Row()
    {
        using var df = DataFrame.From(GuardedRows);
        var q = df.AsQueryable<Guarded>();

        // `&` evaluates both sides in C# too: the function throws on N = 0 either way.
        Assert.ThrowsAny<Exception>(() => GuardedRows.Where(x => x.N != 0 & Hundredth(x.N) > 10).ToList());
        Assert.ThrowsAny<Exception>(() => q.Where(x => x.N != 0 & Hundredth(x.N) > 10).ToList());
        Assert.Equal(GuardedRows.Select(x => Third(x.S + "___")), q.Select(x => Third(x.S + "___")).ToList());
    }

    public class Edge
    {
        public long Id { get; set; }
        public double? D { get; set; }
        public decimal M { get; set; }
        public decimal N { get; set; }
    }

    private static readonly Edge[] Edges =
    [
        new() { Id = 1, D = 0.0, M = -7.5m, N = 2m },
        new() { Id = 2, D = double.NegativeInfinity, M = 7.25m, N = -2m },
        new() { Id = 3, D = double.NaN, M = -0.5m, N = 0.3m },
        new() { Id = 4, D = -0.0, M = 9m, N = 4m },
        new() { Id = 5, D = null, M = 1m, N = 1m },
        new() { Id = 6, D = double.PositiveInfinity, M = -9m, N = -4m },
        new() { Id = 7, D = -1.5, M = 0m, N = 3m },
    ];

    [Fact]
    public void Float_Order_Key_Matches_Comparer_Default_And_Decimal_Remainder_Truncates()
    {
        using var df = DataFrame.From(Edges);
        var q = df.AsQueryable<Edge>();

        // -0.0 and 0.0 compare equal, so a stable sort keeps their input order.
        Assert.Equal(Edges.OrderBy(x => x.D).Select(x => x.Id), q.OrderBy(x => x.D).Select(x => x.Id).ToList());
        Assert.Equal(Edges.OrderByDescending(x => x.D).Select(x => x.Id), q.OrderByDescending(x => x.D).Select(x => x.Id).ToList());
        Assert.Equal(Edges.Where(x => x.D >= double.NegativeInfinity).Select(x => x.Id), q.Where(x => x.D >= double.NegativeInfinity).Select(x => x.Id).ToList());
        Assert.Equal(Edges.Where(x => x.D < double.PositiveInfinity).Select(x => x.Id), q.Where(x => x.D < double.PositiveInfinity).Select(x => x.Id).ToList());
        Assert.Equal(Edges.Select(x => x.M % x.N), q.Select(x => x.M % x.N).ToList());
    }

    // ---------------------------------------------------------------------------------------------
    // Findings of the differential fuzzer (Polars.NET.Linq.Fuzz) and the aggregation probe.
    // ---------------------------------------------------------------------------------------------

    public class Fz
    {
        public long Id { get; set; }
        public int I { get; set; }
        public double D { get; set; }
        public decimal M { get; set; }
        public string S { get; set; } = "";
        public bool B { get; set; }
        public bool? NB { get; set; }
        public List<int> Xs { get; set; } = [];
    }

    private static readonly Fz[] FzRows =
    [
        new() { Id = 0, I = 7, D = double.NaN, M = 2.5m, S = "ab", B = true, NB = true, Xs = [1, 2] },
        new() { Id = 1, I = -7, D = 2.5, M = 0m, S = "", B = false, NB = null, Xs = [] },
        new() { Id = 2, I = 0, D = double.PositiveInfinity, M = -1m, S = "zz", B = true, NB = false, Xs = [3] },
        new() { Id = 3, I = 3, D = double.NegativeInfinity, M = 10m, S = "a", B = false, NB = false, Xs = [4, 5, 6] },
        new() { Id = 4, I = -1, D = -2.7, M = 0.125m, S = "abc", B = true, NB = null, Xs = [] },
    ];

    [Fact]
    public void Constant_False_Filter_Keeps_The_Columns_Above_It()
    {
        using var df = DataFrame.From(FzRows);
        var q = df.AsQueryable<Fz>();

        Assert.Empty(q.Where(x => false).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        Assert.Empty(q.Where(x => "ab".Contains("zz")).Select(x => x.D).ToList());
        Assert.Empty(q.Where(x => false).Select(x => 0.5).Distinct().ToList());
        Assert.Equal(0, q.Where(x => false).Count());
    }

    [Fact]
    public void Constant_Sort_Keys_Keep_The_Order()
    {
        using var df = DataFrame.From(FzRows);
        var q = df.AsQueryable<Fz>();

        Assert.Equal(FzRows.OrderBy(x => 1).ThenBy(x => x.S).Select(x => x.Id), q.OrderBy(x => 1).ThenBy(x => x.S).Select(x => x.Id).ToList());
        Assert.Equal(FzRows.Select(x => x.Id), q.OrderBy(x => "a").Select(x => x.Id).ToList());
    }

    [Fact]
    public void Lengths_Are_Ints_And_Can_Be_Negated()
    {
        using var df = DataFrame.From(FzRows);
        var q = df.AsQueryable<Fz>();

        Assert.Equal(FzRows.Select(x => -x.S.Length), q.Select(x => -x.S.Length).ToList());
        Assert.Equal(FzRows.Select(x => -x.Xs.Count), q.Select(x => -x.Xs.Count).ToList());
        Assert.Equal(FzRows.OrderBy(x => -x.S.Length).ThenBy(x => x.Id).Select(x => x.Id), q.OrderBy(x => -x.S.Length).ThenBy(x => x.Id).Select(x => x.Id).ToList());
    }

    [Fact]
    public void Arithmetic_On_Literal_Conditionals_And_Typed_Nulls()
    {
        using var df = DataFrame.From(FzRows);
        var q = df.AsQueryable<Fz>();

        Assert.Equal(FzRows.Select(x => (x.B ? 2 : 0) / 3), q.Select(x => (x.B ? 2 : 0) / 3).ToList());
        Assert.Equal(FzRows.Select(x => (x.B ? 7 : 5) % 3), q.Select(x => (x.B ? 7 : 5) % 3).ToList());
        Assert.Equal(FzRows.Select(x => (x.B ? 2.0 : 0.0) % 3.0), q.Select(x => (x.B ? 2.0 : 0.0) % 3.0).ToList());
        Assert.Equal(FzRows.Select(x => -(x.B ? (int?)null : (int?)null)), q.Select(x => -(x.B ? (int?)null : (int?)null)).ToList());
        Assert.Equal(FzRows.Select(x => !(x.B ? (bool?)null : (bool?)null)), q.Select(x => !(x.B ? (bool?)null : (bool?)null)).ToList());
        Assert.Equal(
            FzRows.Where(x => ((bool?)null & x.NB) ?? false).Select(x => x.Id),
            q.Where(x => ((bool?)null & x.NB) ?? false).Select(x => x.Id).ToList());
    }

    [Fact]
    public void Floating_To_Integer_Conversion_Saturates_And_Math_Min_Keeps_NaN()
    {
        using var df = DataFrame.From(FzRows);
        var q = df.AsQueryable<Fz>();

        Assert.Equal(FzRows.Select(x => (int)x.D), q.Select(x => (int)x.D).ToList());
        Assert.Equal(FzRows.Select(x => (long)x.D), q.Select(x => (long)x.D).ToList());
        Assert.Equal(FzRows.Select(x => (byte)x.D), q.Select(x => (byte)x.D).ToList());
        Assert.Equal(FzRows.Select(x => (short)x.D), q.Select(x => (short)x.D).ToList());
        // Literal expectations (NaN, 2.5, +Inf, -Inf, -2.7), .NET 11 saturating semantics, runtime-independent.
        Assert.Equal(new sbyte[] { 0, 2, sbyte.MaxValue, sbyte.MinValue, -2 }, q.Select(x => (sbyte)x.D).ToList());
        Assert.Equal(new byte[] { 0, 2, byte.MaxValue, 0, 0 }, q.Select(x => (byte)x.D).ToList());
        Assert.Equal(new short[] { 0, 2, short.MaxValue, short.MinValue, -2 }, q.Select(x => (short)x.D).ToList());
        Assert.Equal(new ushort[] { 0, 2, ushort.MaxValue, 0, 0 }, q.Select(x => (ushort)x.D).ToList());
        Assert.Equal(new uint[] { 0, 2, uint.MaxValue, 0, 0 }, q.Select(x => (uint)x.D).ToList());
        Assert.Equal(new ulong[] { 0, 2, ulong.MaxValue, 0, 0 }, q.Select(x => (ulong)x.D).ToList());
        Assert.Equal(new long[] { 0, 2, long.MaxValue, long.MinValue, -2 }, q.Select(x => (long)x.D).ToList());
        // Finite values beyond the narrow range (I * 100.5 = 703.5, -703.5, 0, 301.5, -100.5).
        Assert.Equal(new byte[] { 255, 0, 0, 255, 0 }, q.Select(x => (byte)(x.I * 100.5)).ToList());
        Assert.Equal(new sbyte[] { sbyte.MaxValue, sbyte.MinValue, 0, sbyte.MaxValue, -100 }, q.Select(x => (sbyte)(x.I * 100.5)).ToList());
        // Unchecked integer narrowing wraps.
        Assert.Equal(FzRows.Select(x => (byte)(x.I * 100)), q.Select(x => (byte)(x.I * 100)).ToList());
        Assert.Equal(FzRows.Select(x => (sbyte)(x.I * 100)), q.Select(x => (sbyte)(x.I * 100)).ToList());
        Assert.Equal(FzRows.Where(x => (int)x.D == 0).Select(x => x.Id), q.Where(x => (int)x.D == 0).Select(x => x.Id).ToList());
        Assert.Equal(FzRows.Select(x => Math.Min(x.D, 0.5)), q.Select(x => Math.Min(x.D, 0.5)).ToList());
        Assert.Equal(FzRows.Select(x => Math.Min(0.5, x.D)), q.Select(x => Math.Min(0.5, x.D)).ToList());
        Assert.Equal(FzRows.Select(x => Math.Max(x.D, 0.5)), q.Select(x => Math.Max(x.D, 0.5)).ToList());
    }

    [Fact]
    public void Decimal_Division_By_Zero_Is_Null_And_Average_Stays_Decimal()
    {
        using var df = DataFrame.From(FzRows);
        var q = df.AsQueryable<Fz>();

        // C# short-circuits the division; the provider must not fail on the rows it would not reach.
        Assert.Equal(FzRows.Where(x => x.I == x.I || x.M / 0m > 1m).Select(x => x.Id), q.Where(x => x.I == x.I || x.M / 0m > 1m).Select(x => x.Id).ToList());
        Assert.Equal(Math.Round(FzRows.Average(x => x.M), 18), q.Average(x => x.M));
        Assert.Equal(
            FzRows.GroupBy(x => x.B).Select(g => new { g.Key, a = Math.Round(g.Average(x => x.M), 18) }).OrderBy(r => r.Key),
            q.GroupBy(x => x.B).Select(g => new { g.Key, a = g.Average(x => x.M) }).OrderBy(r => r.Key).ToList());
    }

    [Fact]
    public void Empty_Aggregates_Raise_Linq_Messages()
    {
        using var df = DataFrame.From(FzRows);
        var q = df.AsQueryable<Fz>();

        AssertSameFailure(() => FzRows.Where(x => x.Id > 100).Average(x => x.I), () => q.Where(x => x.Id > 100).Average(x => x.I));
        AssertSameFailure(() => FzRows.Where(x => x.Id > 100).Min(x => x.I), () => q.Where(x => x.Id > 100).Min(x => x.I));
    }

    // A comparison lifted to null (VB / hand-built trees: `bool?` result, null on a null operand) is not
    // translated natively, so it is never answered with C#'s two-valued result.
    [Fact]
    public void Comparison_Lifted_To_Null_Keeps_Its_Null_Result()
    {
        var rows = new[] { new KnV { K = 1, V = 5 }, new KnV { K = 2, V = null }, new KnV { K = 3, V = 7 } };
        using var df = DataFrame.From(rows);
        var x = System.Linq.Expressions.Expression.Parameter(typeof(KnV), "x");
        var v = System.Linq.Expressions.Expression.Property(x, nameof(KnV.V));
        var five = System.Linq.Expressions.Expression.Constant(5L, typeof(long?));
        foreach (var make in new Func<System.Linq.Expressions.Expression, System.Linq.Expressions.Expression, System.Linq.Expressions.BinaryExpression>[]
        {
            (a, b) => System.Linq.Expressions.Expression.Equal(a, b, true, null),
            (a, b) => System.Linq.Expressions.Expression.NotEqual(a, b, true, null),
            (a, b) => System.Linq.Expressions.Expression.GreaterThan(a, b, true, null),
        })
        {
            var lambda = System.Linq.Expressions.Expression.Lambda<Func<KnV, bool?>>(make(v, five), x);
            Assert.Equal(rows.Select(lambda.Compile()), [.. df.AsQueryable<KnV>().Select(lambda)]);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Checked sums: Sum of int / long and Average of long throw OverflowException when the exact sum leaves
    // the type's range, as Enumerable.Sum (the engine wraps).
    // ---------------------------------------------------------------------------------------------

    public class Num
    {
        public long K { get; set; }
        public int I { get; set; }
        public long L { get; set; }
        public int? NI { get; set; }
    }

    private static readonly Num[] Overflowing =
    [
        new() { K = 1, I = int.MaxValue, L = long.MaxValue, NI = int.MaxValue },
        new() { K = 1, I = int.MaxValue, L = long.MaxValue, NI = null },
        new() { K = 1, I = 1, L = 1, NI = int.MaxValue },
        new() { K = 2, I = 3, L = 3, NI = 3 },
    ];

    private static readonly Num[] NearTheLimit =
    [
        new() { K = 1, I = int.MaxValue, L = long.MaxValue, NI = int.MinValue },
        new() { K = 1, I = -5, L = -5, NI = null },
        new() { K = 1, I = 3, L = 3, NI = 1 },
        new() { K = 2, I = int.MinValue, L = long.MinValue, NI = 0 },
    ];

    [Fact]
    public void Integer_Sums_Throw_On_Overflow()
    {
        using var df = DataFrame.From(Overflowing);
        var q = df.AsQueryable<Num>();
        Assert.False(IsFullFallback(q.GroupBy(x => x.K).Select(g => new { g.Key, S = g.Sum(x => x.I) })));
        Assert.False(IsFullFallback(q.Where(x => q.Where(y => y.K == x.K).Sum(y => y.I) > 0)));
        AssertSameFailure(() => Overflowing.Sum(x => x.I), () => q.Sum(x => x.I));
        AssertSameFailure(() => Overflowing.Sum(x => x.L), () => q.Sum(x => x.L));
        AssertSameFailure(() => Overflowing.Sum(x => x.NI), () => q.Sum(x => x.NI));
        AssertSameFailure(() => Overflowing.Average(x => x.L), () => q.Average(x => x.L));
        AssertSameFailure(() => Overflowing.Select(x => x.I).Distinct().Sum(), () => q.Select(x => x.I).Distinct().Sum());
        AssertSameFailure(
            () => Overflowing.GroupBy(x => x.K).Select(g => new { g.Key, S = g.Sum(x => x.I) }).ToList(),
            () => q.GroupBy(x => x.K).Select(g => new { g.Key, S = g.Sum(x => x.I) }).ToList());
        AssertSameFailure(
            () => Overflowing.GroupBy(x => x.K).Select(g => new { g.Key, A = g.Average(x => x.L) }).ToList(),
            () => q.GroupBy(x => x.K).Select(g => new { g.Key, A = g.Average(x => x.L) }).ToList());
        AssertSameFailure(
            () => Overflowing.Where(x => Overflowing.Where(y => y.K == x.K).Sum(y => y.I) > 0).ToList(),
            () => q.Where(x => q.Where(y => y.K == x.K).Sum(y => y.I) > 0).ToList());
    }

    [Fact]
    public void Integer_Sums_Near_The_Limit_Are_Exact()
    {
        using var df = DataFrame.From(NearTheLimit);
        var q = df.AsQueryable<Num>();
        Assert.Equal(NearTheLimit.Sum(x => x.I), q.Sum(x => x.I));
        Assert.Equal(NearTheLimit.Sum(x => x.L), q.Sum(x => x.L));
        Assert.Equal(NearTheLimit.Sum(x => x.NI), q.Sum(x => x.NI));
        Assert.Equal(NearTheLimit.Average(x => x.I), q.Average(x => x.I));
        Assert.Equal(
            NearTheLimit.GroupBy(x => x.K).Select(g => new { g.Key, S = g.Sum(x => x.I), T = g.Sum(x => x.L) }).OrderBy(x => x.Key),
            [.. q.GroupBy(x => x.K).Select(g => new { g.Key, S = g.Sum(x => x.I), T = g.Sum(x => x.L) }).OrderBy(x => x.Key)]);
        Assert.Equal(
            NearTheLimit.Where(x => NearTheLimit.Where(y => y.K == x.K).Sum(y => y.L) > 0).Select(x => x.I),
            [.. q.Where(x => q.Where(y => y.K == x.K).Sum(y => y.L) > 0).Select(x => x.I)]);
    }

    // ---------------------------------------------------------------------------------------------
    // A subquery held in a variable keeps its operators when a Select / Distinct follows it (the Where and Take
    // of `ob` used to be dropped under `ob.Select(..).Contains(..)`).
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void Captured_Subquery_Keeps_Its_Operators_Under_Select_And_Distinct()
    {
        using var l = DataFrame.From(Facts);
        using var r = DataFrame.From(Others);
        var q = l.AsQueryable<Kv>();
        var others = r.AsQueryable<Kv>();
        // Each operator removes the row that would match (V = 40, K = 8) or changes the count.
        var filtered = others.Where(y => y.K != 8);
        var last = others.OrderByDescending(y => y.K).Take(1);
        var filteredRows = Others.Where(y => y.K != 8).ToArray();
        var lastRows = Others.OrderByDescending(y => y.K).Take(1).ToArray();

        // Each query runs twice: the second run may reuse a cached plan.
        for (int run = 0; run < 2; run++)
        {
            Assert.Equal(
                Facts.Where(x => filteredRows.Select(y => y.V).Contains(x.V)).Select(x => x.K),
                [.. q.Where(x => filtered.Select(y => y.V).Contains(x.V)).Select(x => x.K)]);
            Assert.Equal(
                Facts.Where(x => !filteredRows.Select(y => y.V).Contains(x.V)).Select(x => x.K),
                [.. q.Where(x => !filtered.Select(y => y.V).Contains(x.V)).Select(x => x.K)]);
            Assert.Equal(
                Facts.Where(x => lastRows.Select(y => y.V).Contains(x.V)).Select(x => x.K),
                [.. q.Where(x => last.Select(y => y.V).Contains(x.V)).Select(x => x.K)]);
            Assert.Equal(
                Facts.Where(x => filteredRows.Select(y => y.K).Distinct().Count() > x.K).Select(x => x.K),
                [.. q.Where(x => filtered.Select(y => y.K).Distinct().Count() > x.K).Select(x => x.K)]);
        }
    }

    [Fact]
    public void Query_Rooted_On_A_Composed_Query_Constant_Keeps_Its_Operators()
    {
        using var r = DataFrame.From(Others);
        var others = r.AsQueryable<Kv>();
        var filtered = others.Where(y => y.K != 8);
        var y = System.Linq.Expressions.Expression.Parameter(typeof(Kv), "y");
        var sel = System.Linq.Expressions.Expression.Lambda<Func<Kv, long>>(System.Linq.Expressions.Expression.Property(y, "V"), y);
        var call = System.Linq.Expressions.Expression.Call(typeof(Queryable), "Select", [typeof(Kv), typeof(long)],
            System.Linq.Expressions.Expression.Constant(filtered, typeof(IQueryable<Kv>)), System.Linq.Expressions.Expression.Quote(sel));
        var q = others.Provider.CreateQuery<long>(call);
        Assert.Equal(new long[] { 20, 120 }, q.ToArray());

        var count = System.Linq.Expressions.Expression.Call(typeof(Queryable), "Count", [typeof(Kv)],
            System.Linq.Expressions.Expression.Constant(filtered, typeof(IQueryable<Kv>)));
        Assert.Equal(2, others.Provider.Execute<int>(count));
    }

    // ---------------------------------------------------------------------------------------------
    // GroupJoin result selectors: every aggregate of the group, with or without a predicate, under any member
    // name (a name shared with a source column used to read that column), and the group held in a row.
    // ---------------------------------------------------------------------------------------------

    public class Gj
    {
        public long Id { get; set; }
        public string S { get; set; } = "";
        public int I { get; set; }
        public bool B { get; set; }
        public int? NI { get; set; }
    }

    public record GjOut
    {
        public long A { get; init; }
        public long B { get; init; }
    }

    private static readonly Gj[] GjOuter =
    [
        new() { Id = 0, S = "a", I = 1, B = true, NI = 1 }, new() { Id = 1, S = "b", I = 2, B = false, NI = null },
        new() { Id = 2, S = "a", I = 3, B = true, NI = 3 }, new() { Id = 3, S = "c", I = 2, B = false, NI = null },
    ];

    private static readonly Gj[] GjInner =
    [
        new() { Id = 0, S = "a", I = 1, B = true, NI = 1 }, new() { Id = 1, S = "b", I = 2, B = false, NI = null },
        new() { Id = 2, S = "a", I = 3, B = false, NI = 3 }, new() { Id = 3, S = "z", I = 9, B = false, NI = null },
    ];

    private static void AssertGroupJoin<T>(Func<IQueryable<Gj>, IQueryable<Gj>, IQueryable<T>> query, bool native = true)
    {
        using var o = DataFrame.From(GjOuter);
        using var i = DataFrame.From(GjInner);
        var expected = query(GjOuter.AsQueryable(), GjInner.AsQueryable()).ToList();
        var actual = query(o.AsQueryable<Gj>(), i.AsQueryable<Gj>());
        if (native) Assert.False(IsFullFallback(actual), ((IPolarsExplainable)actual).InspectPlan(true).FormatReport());
        Assert.Equal(expected, actual.ToList());
    }

    [Fact]
    public void GroupJoin_Aggregates_Under_Any_Member_Name()
    {
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, B = g.Count() }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, I = g.Count() }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, S = g.LongCount() }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new GjOut { A = a.Id, B = g.Count() }).OrderBy(x => x.A));
    }

    [Fact]
    public void GroupJoin_Aggregates_Keep_Their_Predicates_And_Selectors()
    {
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Count(b => b.B) }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.LongCount(b => b.I > 100) }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Where(b => b.I > 1).Count() }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Sum(b => b.I) }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Max(b => b.NI) }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Any() }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Any(b => b.B), M = g.Count() }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) =>
            from a in s
            join b in o on a.S equals b.S into g
            orderby a.Id
            select new { a.Id, N = g.Count(b => b.B) });
    }

    [Fact]
    public void GroupJoin_Never_Matches_Null_Keys()
    {
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => a.NI, b => b.NI, (a, g) => new { a.Id, N = g.Count() }).OrderBy(x => x.Id));
    }

    [Fact]
    public void GroupJoin_Row_Holding_The_Group_Is_Composed_Into_Later_Stages()
    {
        AssertGroupJoin((s, o) =>
            s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a, g }).Select(t => new { t.a.Id, N = t.g.Count() }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) =>
            s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a, g }).Select(t => new GjOut { A = t.a.Id, B = t.g.Sum(b => (long)b.I) }).OrderBy(x => x.A));
        AssertGroupJoin((s, o) =>
            from a in s
            join b in o on a.S equals b.S into g
            where g.Count() > 0
            orderby a.Id descending
            select new { a.Id, N = g.Count(b => b.B) });
    }

    // ---------------------------------------------------------------------------------------------
    // Set operations over objects whose sides assign different members: an unassigned member holds its
    // default, and the sides are matched by member, never by position.
    // ---------------------------------------------------------------------------------------------

    public record SetRow
    {
        public long A { get; init; }
        public long? B { get; init; }
        public string C { get; init; }
        public double D { get; init; }
        public bool E { get; init; }
    }

    private static void AssertSetOp<T>(Func<IQueryable<Gj>, IQueryable<Gj>, IQueryable<T>> query)
    {
        using var o = DataFrame.From(GjOuter);
        using var i = DataFrame.From(GjInner);
        var expected = query(GjOuter.AsQueryable(), GjInner.AsQueryable()).ToList();
        var actual = query(o.AsQueryable<Gj>(), i.AsQueryable<Gj>());
        Assert.False(IsFullFallback(actual), ((IPolarsExplainable)actual).InspectPlan(true).FormatReport());
        Assert.Equal(expected, actual.ToList());
    }

    [Fact]
    public void Set_Operations_Match_Sides_By_Member()
    {
        AssertSetOp((s, o) => s.Select(x => new SetRow { C = x.S, D = x.I }).Concat(o.Select(x => new SetRow { A = x.I, C = x.S })));
        AssertSetOp((s, o) => s.Select(x => new SetRow { A = x.Id, C = x.S }).Concat(o.Select(x => new SetRow { A = x.Id, B = x.NI })));
        AssertSetOp((s, o) => s.Select(x => new SetRow { A = x.I, E = x.B }).Union(o.Select(x => new SetRow { A = x.I, B = x.Id })));
        AssertSetOp((s, o) => s.Select(x => new SetRow { A = x.I }).Intersect(o.Select(x => new SetRow { A = x.I, B = null })));
        AssertSetOp((s, o) => s.Select(x => new SetRow { A = x.I }).Except(o.Select(x => new SetRow { A = x.I, E = x.B })));
        AssertSetOp((s, o) => s.Select(x => new SetRow { B = x.Id, A = x.I }).Concat(o.Select(x => new SetRow { A = x.I, B = x.Id })));
    }

    // ---------------------------------------------------------------------------------------------
    // A comparison over a floating aggregate follows .NET NaN semantics, as over a column.
    // ---------------------------------------------------------------------------------------------

    public class Fl
    {
        public string S { get; set; } = "";
        public double D { get; set; }
    }

    [Fact]
    public void Comparisons_Of_Floating_Aggregates_Follow_NaN_Semantics()
    {
        Fl[] rows = [new() { S = "a", D = double.NaN }, new() { S = "a", D = double.NaN }, new() { S = "b", D = 2.5 }, new() { S = "b", D = 0.5 }];
        using var df = DataFrame.From(rows);
        var q = df.AsQueryable<Fl>();

        Assert.Equal(
            rows.GroupBy(x => x.S).Where(g => g.Max(x => x.D) > 1).Select(g => g.Key),
            [.. q.GroupBy(x => x.S).Where(g => g.Max(x => x.D) > 1).Select(g => g.Key).OrderBy(k => k)]);
        Assert.Equal(
            rows.GroupBy(x => x.S).Select(g => new { g.Key, M = g.Max(x => x.D) > 1, S = g.Sum(x => x.D) > 1, L = g.Average(x => x.D) < 10 }),
            [.. q.GroupBy(x => x.S).Select(g => new { g.Key, M = g.Max(x => x.D) > 1, S = g.Sum(x => x.D) > 1, L = g.Average(x => x.D) < 10 }).OrderBy(x => x.Key)]);
        Assert.Equal(
            rows.GroupBy(x => x.S, x => x.D).Select(g => new { g.Key, M = g.Max() > 1 }),
            [.. q.GroupBy(x => x.S, x => x.D).Select(g => new { g.Key, M = g.Max() > 1 }).OrderBy(x => x.Key)]);
    }

    [Fact]
    public void Group_And_Decorrelated_Expressions_Follow_CSharp_Arithmetic_And_Nulls()
    {
        KnV[] rows = [new() { K = 1, V = -7 }, new() { K = 1, V = null }, new() { K = 2, V = 5 }, new() { K = 3, V = null }];
        using var df = DataFrame.From(rows);
        var q = df.AsQueryable<KnV>();

        // Truncating division and a nullable comparison in a group result selector and in a having.
        Assert.Equal(
            rows.GroupBy(x => x.K).Select(g => new { g.Key, H = g.Sum(x => x.V) / 2, P = g.Max(x => x.V) != 5 }),
            [.. q.GroupBy(x => x.K).Select(g => new { g.Key, H = g.Sum(x => x.V) / 2, P = g.Max(x => x.V) != 5 }).OrderBy(x => x.Key)]);
        Assert.Equal(
            rows.GroupBy(x => x.K).Where(g => g.Max(x => x.V) != 5).Select(g => g.Key),
            [.. q.GroupBy(x => x.K).Where(g => g.Max(x => x.V) != 5).Select(g => g.Key).OrderBy(k => k)]);
        Assert.Equal(
            rows.GroupBy(x => x.K).Where(g => g.Sum(x => x.V) / 2 == -3).Select(g => g.Key),
            [.. q.GroupBy(x => x.K).Where(g => g.Sum(x => x.V) / 2 == -3).Select(g => g.Key)]);

        // A correlated aggregate scaled by a literal divisor truncates.
        Assert.Equal(
            rows.Where(x => x.K == 1 && -3 == rows.Where(y => y.K == x.K).Sum(y => y.V) / 2).Select(x => x.V),
            [.. q.Where(x => x.K == 1 && -3 == q.Where(y => y.K == x.K).Sum(y => y.V) / 2).Select(x => x.V)]);
    }

    // ---------------------------------------------------------------------------------------------
    // A group key member may share its name with a result member that holds something else.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void Group_Key_Members_May_Share_Names_With_Other_Result_Members()
    {
        AssertGroupJoin((s, _) => s.GroupBy(x => new { x.B }).Select(h => new GjOut { A = h.Count(), B = h.Sum(x => (long)x.I) }).OrderBy(x => x.B));
        AssertGroupJoin((s, _) => s.GroupBy(x => new { A = x.I, x.S }).Select(h => new GjOut { A = h.Count(), B = h.Key.A }).OrderBy(x => x.B).ThenBy(x => x.A));
        AssertGroupJoin((s, _) => s.GroupBy(x => new { x.S, x.B }).Select(h => new { B = h.Count(), h.Key.S, K = h.Key.B }).OrderBy(x => x.S).ThenBy(x => x.K));
        AssertGroupJoin((s, _) => s.GroupBy(x => new { x.B, x.NI }).Select(h => new { NI = h.Key.B, B = h.Key.NI, N = h.Count() }).OrderBy(x => x.NI).ThenBy(x => x.B));
    }

    // ---------------------------------------------------------------------------------------------
    // Several correlated subqueries in one projection, some of which the plan cannot hold, and a correlated
    // subquery reading a key column of a composite-key correlation.
    // ---------------------------------------------------------------------------------------------

    [Fact]
    public void Projection_Mixing_Planned_And_Unplanned_Correlated_Subqueries()
    {
        AssertGroupJoin((s, o) => s.Select(a => new { a.Id, N = o.Where(b => b.S == a.S).Count(b => b.B), E = o.Where(b => b.S == a.S).All(b => b.NI != null) }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a, g }).Select(t => new { t.a.Id, N = t.g.Count(b => b.B), E = t.g.All(b => b.NI != null) }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.Where(a => o.Where(b => b.S == a.S).All(b => b.B)).Select(a => a.Id));
        AssertGroupJoin((s, o) => s.Where(a => !o.All(b => b.I != a.I)).Select(a => a.Id));
        AssertGroupJoin((s, o) =>
            s.GroupJoin(o, a => new { k = a.S.Length, a.B }, b => new { k = b.I % 4, b.B }, (a, g) => new { a, g })
                .Select(t => new { t.a.Id, N = t.g.Count(b => b.B), E = t.g.All(b => b.NI != null) }).OrderBy(x => x.Id));
        // A theta-decorrelated filter leaves the outer rows' columns only, so a later correlated join can suffix again.
        AssertGroupJoin((s, o) => s.Where(a => o.All(b => b.I != a.I || b.B)).Select(a => new { a.Id, N = o.Where(b => b.I == a.I).Count(b => b.B) }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.Where(a => o.All(b => b.I != a.I || b.B)).Select(a => new { a.Id, N = o.Where(b => b.I == a.I && b.Id > a.Id).Count() }).OrderBy(x => x.Id));
    }

    [Fact]
    public void Correlated_Aggregate_Reads_A_Key_Column_Of_A_Composite_Correlation()
    {
        AssertGroupJoin((s, o) => s.Select(a => new { a.Id, N = o.Where(b => b.I == a.I && b.S == a.S && b.Id != a.Id).Sum(b => (long)b.I) }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.GroupJoin(o, a => new { a.I, a.S }, b => new { b.I, b.S }, (a, g) => new { a.Id, N = g.Sum(b => (long)b.I) }).OrderBy(x => x.Id));
        // A computed key named like a column of the inner side (`b.I % 2` is named I), and one not idempotent.
        AssertGroupJoin((s, o) => s.Select(a => new { a.Id, N = o.Where(b => b.I * 2 == a.I).Count() }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.Where(a => o.Where(b => b.I * 2 == a.I).Sum(b => b.I) > 0).Select(a => a.Id));
        AssertGroupJoin((s, o) => s.Select(a => new { a.Id, N = o.Where(b => b.I % 2 == a.I % 2 && b.Id != a.Id).Sum(b => (long)b.I) }).OrderBy(x => x.Id));
        AssertGroupJoin((s, o) => s.Where(a => !o.Any(b => b.I % 2 == a.I % 2 && b.I > a.I)).Select(a => a.Id));
    }

    // The long check splits each value into 32-bit halves; the total decides, at both ends of the range.
    [Theory]
    [InlineData(new[] { long.MaxValue, 1L })]
    [InlineData(new[] { long.MinValue, -1L })]
    [InlineData(new[] { long.MaxValue, long.MaxValue, long.MinValue })]
    [InlineData(new[] { long.MinValue, long.MinValue, long.MaxValue, 1L })]
    [InlineData(new[] { long.MaxValue, 0L })]
    [InlineData(new[] { long.MinValue, 0L })]
    [InlineData(new[] { long.MaxValue, long.MaxValue, long.MaxValue })]
    [InlineData(new[] { -1L, -1L, 4294967295L, -4294967296L, 4294967296L })]
    [InlineData(new[] { 4611686018427387904L, 4611686018427387904L })]
    [InlineData(new[] { 4611686018427387904L, 4611686018427387903L })]
    public void Long_Sum_Is_Checked_On_The_Exact_Total(long[] values)
    {
        var rows = values.Select((v, i) => new Num { K = i % 2, L = v }).ToArray();
        using var df = DataFrame.From(rows);
        var q = df.AsQueryable<Num>();
        var total = values.Aggregate(System.Numerics.BigInteger.Zero, (acc, v) => acc + v);
        bool fits = total >= long.MinValue && total <= long.MaxValue;

        if (fits)
        {
            Assert.Equal((long)total, q.Sum(x => x.L));
            Assert.Equal((double)(decimal)total / values.Length, q.Average(x => x.L), 6);
        }
        else
        {
            Assert.Throws<OverflowException>(() => q.Sum(x => x.L));
            Assert.Throws<OverflowException>(() => q.Average(x => x.L));
        }
    }
}
