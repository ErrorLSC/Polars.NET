using System.Collections;
using System.Linq.Expressions;
using System.Text;
using Polars.CSharp;
using Polars.CSharp.Linq;
using Polars.NET.Core.Exceptions;

namespace Polars.NET.Linq.Fuzz;

// Uniform projection record used by the structural generator (value equality => Distinct/Union work in LINQ to Objects).
public record FzP
{
    public long A { get; set; }
    public long? B { get; set; }
    public string? C { get; set; }
    public double? D { get; set; }
    public bool? E { get; set; }
}

public class FzG<K>
{
    public K Key { get; set; } = default!;
    public int N { get; set; }
    public long LN { get; set; }
    public long Sum { get; set; }
    public long? SN { get; set; }
    public double Mn { get; set; }
    public int Mx { get; set; }
    public int? MxN { get; set; }
    public double Av { get; set; }
    public double? AvN { get; set; }
    public bool Ay { get; set; }
    public bool Al { get; set; }
    public int Cp { get; set; }
    public int CpN { get; set; }
    public string? MS { get; set; }
    public string? MNS { get; set; }
    public int Dc { get; set; }
    public long Fi { get; set; }
    public double SumD { get; set; }
    public decimal SumM { get; set; }
}

// A built query: the sequence (for description), an optional scalar/element terminal, and whether positional comparison applies.
public sealed record Plan(string Family, IQueryable Q, Func<object?>? Term, string TermName, bool Ordered) { public string OExpr { get; init; } = ""; }

public sealed class Spec
{
    public string Family = "";
    public Func<IQueryable<FzRow>, IQueryable<FzRow>, Plan> Build = null!;
}

public static class StructGen
{
    // When true, the generator steers around the bugs already found (see Program.cs header) to look for new ones.
    public static bool AvoidKnown;

    sealed class Replacer(ParameterExpression from, Expression to) : ExpressionVisitor
    {
        protected override Expression VisitParameter(ParameterExpression node) => node == from ? to : base.VisitParameter(node);
    }

    static Plan Seq<T>(string fam, IQueryable<T> q, bool ordered) => new(fam, q, null, "", ordered);
    static Plan Term<T, R>(string fam, IQueryable<T> q, string name, Func<IQueryable<T>, R> f) => new(fam, q, () => f(q), name, true);

    // ================= pre-stages on FzRow =================
    static Expression<Func<FzRow, bool>>[] SubPreds(IQueryable<FzRow> o) =>
    [
        x => o.Any(y => y.I == x.I),
        x => !o.Any(y => y.S == x.S),
        x => o.Any(y => y.NI == x.NI),
        x => o.All(y => y.I != x.I || y.B),
        x => o.Count(y => y.S == x.S) > 1,
        x => o.Where(y => y.I == x.I).Count() > 0,
        x => o.Where(y => y.NS == x.NS && y.B).Any(),
        x => o.Where(y => y.I == x.I).Sum(y => (long)y.I) > x.I,
        x => o.Where(y => y.S == x.S).Max(y => (int?)y.I) > x.I,
        x => x.I > o.Max(y => (int?)y.I),
        x => o.Select(y => y.I).Contains(x.I),
        x => o.Where(y => y.B).Select(y => y.NI).Contains(x.NI),
        x => x.L > o.Count(),
        x => x.Id < o.LongCount(y => y.B),
        x => o.Min(y => y.D) < x.D,
        x => o.Any(y => y.Id == x.Id && y.D > x.D),
        x => o.Average(y => (double?)y.D) > x.D,
        x => o.Where(y => y.NI == x.NI).Select(y => y.S).Contains(x.S),
        x => o.Count(y => y.NI == x.NI) == 0,
        x => o.Any(y => y.S == x.S) && o.Any(y => y.I == x.I),
        x => o.Select(y => y.S).Distinct().Count() > x.S.Length,
    ];
    static readonly string[] SubSelFields = ["AB", "AB", "AD", "AB", "AE", "AE", "AC", "ABD", "AB", "ABC", "ABD"];
    static Expression<Func<FzRow, FzP>>[] SubSels(IQueryable<FzRow> o) =>
    [
        x => new FzP { A = x.Id, B = o.Count(y => y.I == x.I) },
        x => new FzP { A = x.Id, B = o.Where(y => y.S == x.S).Sum(y => (long)y.I) },
        x => new FzP { A = x.Id, D = o.Where(y => y.I == x.I).Sum(y => y.D) },
        x => new FzP { A = x.Id, B = o.Where(y => y.NI == x.NI).Max(y => (long?)y.NI) },
        x => new FzP { A = x.Id, E = o.Any(y => y.S == x.S && y.B) },
        x => new FzP { A = x.Id, E = o.All(y => y.I != x.I) },
        x => new FzP { A = x.Id, C = o.Where(y => y.I == x.I).Min(y => y.S) },
        x => new FzP { A = x.Id, B = o.Count(), D = o.Where(y => y.B).Average(y => (double?)y.D) },
        x => new FzP { A = x.Id, B = o.Select(y => y.I).Contains(x.I) ? 1 : 0 },
        x => new FzP { A = x.Id, B = o.Where(y => y.I == x.I).Max(y => (long?)y.Id), C = o.Where(y => y.S == x.S).Max(y => y.NS) },
        x => new FzP { A = x.Id, B = o.Count(y => y.D > x.D), D = o.Where(y => y.Id == x.Id).Sum(y => y.D) },
    ];
    static readonly Expression<Func<FzRow, bool>>[] SimplePreds =
    [
        x => x.I > 0, x => x.NI != null, x => x.B, x => x.S != "", x => x.NS == null || x.NS.Length > 1, x => x.D == x.D, x => x.NB == true,
    ];

    static Func<IQueryable<FzRow>, IQueryable<FzRow>, IQueryable<FzRow>> MakePre(Random r, Gen g, bool withSub)
    {
        int k0 = r.Next(10);
        int n = k0 < 4 ? 0 : k0 < 8 ? 1 : 2;
        var steps = new List<Func<IQueryable<FzRow>, IQueryable<FzRow>, IQueryable<FzRow>>>();
        for (int i = 0; i < n; i++)
        {
            int k = r.Next(withSub ? 5 : 2);
            if (k == 0) { var p = Expression.Lambda<Func<FzRow, bool>>(g.G(T.Bool, 2), g.X); steps.Add((q, o) => q.Where(p)); }
            else if (k == 1) { int j = r.Next(SimplePreds.Length); steps.Add((q, o) => q.Where(SimplePreds[j])); }
            else { int j = r.Next(SubPreds(null!).Length); steps.Add((q, o) => q.Where(SubPreds(o)[j])); }
        }
        return (q, o) => { foreach (var st in steps) q = st(q, o); return q; };
    }

    // ================= post-stages on FzP =================
    static readonly (string need, Expression<Func<FzP, bool>> e)[] PostPreds =
    [
        ("A", p => p.A > 0), ("B", p => p.B > 1), ("E", p => p.E == true), ("C", p => p.C != null), ("D", p => p.D < 1.0), ("AB", p => p.B == null || p.A > 2),
        ("C", p => p.C == "a" || p.C == "ab"), ("E", p => p.E != false),
    ];
    // Only predicates over members that the projection actually assigns.
    static IQueryable<FzP> WherePost(IQueryable<FzP> q, string fields, int c)
    {
        var ok = PostPreds.Where(x => x.need.All(ch => fields.Contains(ch))).ToArray();
        return ok.Length == 0 ? q : q.Where(ok[c % ok.Length].e);
    }

    // uniqA: A is unique per row, so OrderBy(A) is a total order.
    static Plan PostFzP(string fam, IQueryable<FzP> q, string fields, int c, bool uniqA, bool ordered)
    {
        return (c % 9) switch
        {
            0 => Seq(fam, q, ordered),
            1 => Seq(fam, WherePost(q, fields, c / 9), ordered),
            2 => Seq(fam, q.Distinct(), false),
            3 => Term(fam, q, "Count", x => x.Count()),
            4 => Term(fam, WherePost(q, fields, c / 9), "Any", x => x.Any()),
            5 when uniqA => Seq(fam, q.OrderBy(p => p.A), true),
            6 when uniqA => Seq(fam, q.OrderByDescending(p => p.A).Skip(c / 9 % 3).Take(c / 27 % 6), true),
            7 when uniqA => Term(fam, q.OrderBy(p => p.A), "FirstOrDefault", x => x.FirstOrDefault()),
            8 when uniqA => Seq(fam, WherePost(q, fields, c / 9).OrderBy(p => p.A).Reverse(), true),
            _ => Seq(fam, q, ordered),
        };
    }

    // ================= GroupBy =================
    static (string, LambdaExpression)[] Aggs<K>() =>
    [
        ("N", (Expression<Func<IGrouping<K, FzRow>, int>>)(g => g.Count())),
        ("LN", (Expression<Func<IGrouping<K, FzRow>, long>>)(g => g.LongCount())),
        ("Sum", (Expression<Func<IGrouping<K, FzRow>, long>>)(g => g.Sum(x => (long)x.I))),
        ("SN", (Expression<Func<IGrouping<K, FzRow>, long?>>)(g => g.Sum(x => (long?)x.NI))),
        ("Mn", (Expression<Func<IGrouping<K, FzRow>, double>>)(g => g.Min(x => x.D))),
        ("Mx", (Expression<Func<IGrouping<K, FzRow>, int>>)(g => g.Max(x => x.I))),
        ("MxN", (Expression<Func<IGrouping<K, FzRow>, int?>>)(g => g.Max(x => x.NI))),
        ("Av", (Expression<Func<IGrouping<K, FzRow>, double>>)(g => g.Average(x => x.D))),
        ("AvN", (Expression<Func<IGrouping<K, FzRow>, double?>>)(g => g.Average(x => x.NI))),
        ("Ay", (Expression<Func<IGrouping<K, FzRow>, bool>>)(g => g.Any(x => x.B))),
        ("Al", (Expression<Func<IGrouping<K, FzRow>, bool>>)(g => g.All(x => x.NI != null))),
        ("Cp", (Expression<Func<IGrouping<K, FzRow>, int>>)(g => g.Count(x => x.I > 0))),
        ("CpN", (Expression<Func<IGrouping<K, FzRow>, int>>)(g => g.Count(x => x.NB == true))),
        ("MS", (Expression<Func<IGrouping<K, FzRow>, string?>>)(g => g.Min(x => x.S))),
        ("MNS", (Expression<Func<IGrouping<K, FzRow>, string?>>)(g => g.Max(x => x.NS))),
        ("Dc", (Expression<Func<IGrouping<K, FzRow>, int>>)(g => g.Select(x => x.S).Distinct().Count())),
        ("Fi", (Expression<Func<IGrouping<K, FzRow>, long>>)(g => g.OrderBy(x => x.Id).First().Id)),
        ("SumD", (Expression<Func<IGrouping<K, FzRow>, double>>)(g => g.Sum(x => x.D))),
        ("SumM", (Expression<Func<IGrouping<K, FzRow>, decimal>>)(g => g.Sum(x => x.M))),
    ];
    static Expression<Func<IGrouping<K, FzRow>, bool>>[] Havings<K>() =>
    [
        g => g.Count() > 1, g => g.Any(x => x.B), g => g.Sum(x => (long)x.I) > 0, g => g.Max(x => x.I) > 0,
        g => g.Count(x => x.NI == null) == 0, g => g.Min(x => x.D) < 1.0, g => g.All(x => x.I != 0), g => g.Count() == g.Count(x => x.B),
        g => g.Count(x => x.I > 0) >= 2 && g.Any(x => x.NB == null),
    ];

    static Plan Group<K>(IQueryable<FzRow> s, Expression<Func<FzRow, K>> key, int[] c)
    {
        var gq = s.GroupBy(key);
        var havs = Havings<K>();
        if (c[0] % 3 != 0) gq = gq.Where(havs[c[0] / 3 % havs.Length]);
        var aggs = Aggs<K>();
        var chosen = new SortedSet<string>();
        // aggregate subset: c[1] bitmask-ish; force N and Sum when needed by post stages
        int post = c[2] % 5, order = c[3] % 5;
        if (post == 1 || post == 2 || order == 2 || order == 4) chosen.Add("N");
        if (post == 2) chosen.Add("Sum");
        for (int i = 0; i < aggs.Length; i++) if (((c[1] * 2654435761u >> (i % 16)) & 3) == 0 || (c[1] % 7 == i % 7)) chosen.Add(aggs[i].Item1);
        var gp = Expression.Parameter(typeof(IGrouping<K, FzRow>), "g");
        var ot = typeof(FzG<K>);
        var binds = new List<MemberBinding> { Expression.Bind(ot.GetProperty("Key")!, Expression.Property(gp, "Key")) };
        foreach (var (name, lam) in aggs)
            if (chosen.Contains(name))
                binds.Add(Expression.Bind(ot.GetProperty(name)!, new Replacer(lam.Parameters[0], gp).Visit(lam.Body)));
        var sel = Expression.Lambda<Func<IGrouping<K, FzRow>, FzG<K>>>(Expression.MemberInit(Expression.New(ot), binds), gp);
        IQueryable<FzG<K>> q = gq.Select(sel);
        if (post == 1) q = q.Where(x => x.N >= 2);
        else if (post == 2) q = q.Where(x => x.N > 1 || x.Sum > 0);
        bool ordered = false;
        switch (order)
        {
            case 1: q = q.OrderBy(x => x.Key); ordered = true; break;
            case 2: q = q.OrderByDescending(x => x.N).ThenBy(x => x.Key); ordered = true; break;
            case 3: q = q.OrderBy(x => x.Key).Skip(1).Take(3); ordered = true; break;
            case 4: q = q.Where(x => x.N > 0).OrderByDescending(x => x.Key).Take(4); ordered = true; break;
        }
        return Seq("Group", q, ordered);
    }

    static Plan Group2(IQueryable<FzRow> s, int v, int[] c)
    {
        bool hav = c[0] % 3 == 0;
        switch (v)
        {
            case 0:
            {
                var g = s.GroupBy(x => new { a = x.I % 2, x.S });
                var gh = hav ? g.Where(h => h.Count() > 1) : g;
                var q = gh.Select(h => new FzP { A = h.Key.a, C = h.Key.S, B = h.Count(), D = h.Sum(x => x.D), E = h.Any(x => x.B) });
                if (c[3] % 2 == 0) return Seq("Group2", q, false);
                return Seq("Group2", q.OrderBy(p => p.A).ThenBy(p => p.C), true);
            }
            case 1:
            {
                var g = s.GroupBy(x => new { x.NI, x.NS });
                var gh = hav ? g.Where(h => h.Any(x => x.B)) : g;
                var q = gh.Select(h => new FzP { B = h.Key.NI, C = h.Key.NS, A = h.LongCount(), D = h.Max(x => x.D), E = h.All(x => x.NB != false) });
                if (c[3] % 2 == 0) return Seq("Group2", q, false);
                return Seq("Group2", q.OrderBy(p => p.B).ThenBy(p => p.C), true);
            }
            case 2:
            {
                var q = AvoidKnown
                    ? s.GroupBy(x => new { kb = x.B, x.NB }).Select(h => new FzP { E = h.Key.NB, A = h.Count(x => x.B), B = h.Sum(x => (long)x.I), C = h.Key.kb ? "t" : "f", D = h.Average(x => x.D) })
                    : s.GroupBy(x => new { x.B, x.NB }).Select(h => new FzP { E = h.Key.NB, A = h.Count(x => x.B), B = h.Sum(x => (long)x.I), C = h.Key.B ? "t" : "f", D = h.Average(x => x.D) });
                return Seq("Group2", hav ? q.Where(p => p.A > 0) : q, false);
            }
            case 3:
            {
                var g = s.GroupBy(x => new { k = x.I > 0 ? x.S : x.NS, m = x.S.Length });
                var q = g.Select(h => new FzP { C = h.Key.k, A = h.Key.m, B = h.Count(), D = h.Min(x => x.D) });
                return Seq("Group2", hav ? q.Where(p => p.B > 1) : q, false);
            }
            case 4:
            {
                var q = s.GroupBy(x => x.S, x => x.D).Select(h => new FzP { C = h.Key, B = h.Count(), D = h.Sum(), E = h.Max() > 1 });
                return Seq("Group2", hav ? q.Where(p => p.B > 1) : q, false);
            }
            case 5:
            {
                var q = s.GroupBy(x => x.I % 3, (k, h) => new FzP { A = k, B = h.Count(), D = h.Sum(x => x.D), C = h.Min(x => x.S) });
                return Seq("Group2", hav ? q.Where(p => p.B > 1) : q, false);
            }
            case 6:
            {
                var q = s.GroupBy(x => x.NI).Select(h => new FzP { B = h.Key, A = h.Count(), D = h.Average(x => x.NI), E = h.Key == null });
                return Seq("Group2", hav ? q.Where(p => p.A > 1) : q, false);
            }
            default:
            {
                var q = s.GroupBy(x => x.S).Select(h => new FzP { C = h.Key, A = h.Count(), B = h.OrderByDescending(x => x.Id).First().Id, D = h.Where(x => x.B).Sum(x => x.D), E = h.Any(x => x.NI == null) });
                return Seq("Group2", hav ? q.Where(p => p.A > 1) : q, false);
            }
        }
    }

    // ================= Joins =================
    static Plan JoinP<K>(IQueryable<FzRow> s, IQueryable<FzRow> o, Expression<Func<FzRow, K>> ka, Expression<Func<FzRow, K>> kb, int[] c)
    {
        int mode = c[4] % 6;
        if (AvoidKnown && mode == 3) mode = 0;
        switch (mode)
        {
            case 0: // simple inner join, three selectors
            {
                IQueryable<FzP> q = (c[5] % 3) switch
                {
                    0 => s.Join(o, ka, kb, (a, b) => new FzP { A = a.Id, B = b.Id, C = a.S, D = a.D, E = b.B }),
                    1 => s.Join(o, ka, kb, (a, b) => new FzP { A = a.I, B = b.NI, C = b.NS, D = b.D + a.D, E = a.NB }),
                    _ => s.Join(o, ka, kb, (a, b) => new FzP { A = a.Id * 100 + b.Id, B = a.NI + b.NI, C = a.S + b.S, D = b.ND, E = a.B && b.B }),
                };
                return PostFzP("Join", q, "ABCDE", c[6], (c[5] % 3) != 1, false);
            }
            case 1: // join, then Where/OrderBy/Select over both sides
            {
                var j = s.Join(o, ka, kb, (a, b) => new { a, b });
                var w = (c[5] % 8) switch
                {
                    0 => j.Where(t => t.a.D > t.b.D),
                    1 => j.Where(t => t.a.I < t.b.I),
                    2 => j.Where(t => t.a.B && !t.b.B),
                    3 => j.Where(t => t.a.NI != t.b.NI),
                    4 => j.Where(t => t.a.S == t.b.S),
                    5 => j.Where(t => t.b.NB == true),
                    6 => j.Where(t => t.a.Id < t.b.Id && t.b.NS != null),
                    _ => j,
                };
                if (c[7] % 3 != 0)
                {
                    var o2 = (c[7] % 4) switch
                    {
                        0 => w.OrderBy(t => t.a.Id).ThenBy(t => t.b.Id),
                        1 => w.OrderBy(t => t.b.NI).ThenBy(t => t.a.Id).ThenBy(t => t.b.Id),
                        2 => w.OrderByDescending(t => t.a.S).ThenBy(t => t.b.Id).ThenBy(t => t.a.Id),
                        _ => w.OrderBy(t => t.a.NS).ThenByDescending(t => t.a.Id).ThenBy(t => t.b.Id),
                    };
                    var q = o2.Select(t => new FzP { A = t.a.Id, B = t.b.Id, C = t.a.S + t.b.S, D = t.a.D - t.b.D, E = t.a.NB });
                    if (c[8] % 3 == 0) return Seq("JoinOrd", q.Skip(1).Take(7), true);
                    return Seq("JoinOrd", q, true);
                }
                return Seq("JoinWhere", w.Select(t => new FzP { A = t.a.Id, B = t.b.Id, C = t.b.NS, D = t.a.D * 2, E = t.b.NB }), false);
            }
            case 2: // left join: GroupJoin + SelectMany + DefaultIfEmpty (+ group operators)
            {
                var gj = s.GroupJoin(o, ka, kb, (a, g) => new { a, g });
                var sm = (c[5] % 6) switch
                {
                    0 => gj.SelectMany(t => t.g.DefaultIfEmpty(), (t, b) => new { t.a, b }),
                    1 => gj.SelectMany(t => t.g.Where(b => b.D > 0).DefaultIfEmpty(), (t, b) => new { t.a, b }),
                    2 => gj.SelectMany(t => t.g.Where(b => b.B).DefaultIfEmpty(), (t, b) => new { t.a, b }),
                    3 => gj.SelectMany(t => t.g.Where(b => b.I > t.a.I).DefaultIfEmpty(), (t, b) => new { t.a, b }),
                    4 => gj.SelectMany(t => t.g.OrderBy(b => b.Id).Take(1).DefaultIfEmpty(), (t, b) => new { t.a, b }),
                    _ => gj.SelectMany(t => t.g.Where(b => b.NS == t.a.NS || b.NI == null).DefaultIfEmpty(), (t, b) => new { t.a, b }),
                };
                IQueryable<FzP> q = (c[6] % 3) switch
                {
                    0 => sm.Select(u => new FzP { A = u.a.Id, B = u.b == null ? (long?)null : u.b.Id, C = u.b == null ? null : u.b.S, D = u.a.D }),
                    1 => sm.Select(u => new FzP { A = u.a.Id, B = u.b == null ? null : (long?)u.b.I, E = u.b != null, C = u.a.S }),
                    _ => sm.Select(u => new FzP { A = u.a.Id, B = u.b.NI, C = u.b.NS, D = u.b.D, E = u.b.NB }),
                };
                if (c[6] % 3 == 2) return Seq("LeftJoinNavNull", q, false);
                if (c[7] % 2 == 0) return Seq("LeftJoin", q, false);
                var ord = sm.OrderBy(u => u.a.Id).ThenBy(u => u.b == null ? -1L : u.b.Id)
                    .Select(u => new FzP { A = u.a.Id, B = u.b == null ? (long?)null : u.b.Id, C = u.b == null ? null : u.b.S, D = u.a.D });
                return Seq("LeftJoinOrd", ord, true);
            }
            case 3: // GroupJoin with aggregates
            {
                var gj = s.GroupJoin(o, ka, kb, (a, g) => new { a, g });
                IQueryable<FzP> q = (c[5] % 5) switch
                {
                    0 => gj.Select(t => new FzP { A = t.a.Id, B = t.g.Count(), E = t.g.Any() }),
                    1 => gj.Select(t => new FzP { A = t.a.Id, B = t.g.Sum(b => (long)b.I), D = t.g.Sum(b => b.D) }),
                    2 => gj.Select(t => new FzP { A = t.a.Id, B = t.g.Max(b => (long?)b.Id), C = t.g.Max(b => b.NS) }),
                    3 => gj.Select(t => new FzP { A = t.a.Id, B = t.g.Count(b => b.B), E = t.g.All(b => b.NI != null) }),
                    _ => gj.Where(t => t.g.Any(b => b.D > t.a.D)).Select(t => new FzP { A = t.a.Id, B = t.g.Count(b => b.D > t.a.D) }),
                };
                if (c[7] % 2 == 0) return Seq("GroupJoinAgg", q, false);
                return Seq("GroupJoinAggOrd", q.OrderBy(p => p.A), true);
            }
            case 4: // join then GroupBy
            {
                var j = s.Join(o, ka, kb, (a, b) => new { a, b });
                IQueryable<FzP> q = (c[5] % 4) switch
                {
                    0 => j.GroupBy(t => t.a.S).Select(h => new FzP { C = h.Key, B = h.Count(), D = h.Sum(t => t.b.D), A = h.Max(t => t.a.Id) }),
                    1 => j.GroupBy(t => t.a.NI).Select(h => new FzP { B = h.Key, A = h.Count(), D = h.Average(t => t.b.D), E = h.Any(t => t.b.B) }),
                    2 => j.GroupBy(t => t.b.I % 3).Select(h => new FzP { A = h.Key, B = h.Sum(t => (long)t.a.I), E = h.All(t => t.a.B || t.b.B) }),
                    _ => j.GroupBy(t => new { t.a.B, t.b.NB }).Select(h => new FzP { E = h.Key.NB, A = h.Count(), C = h.Key.B ? "t" : "f" }),
                };
                return Seq("JoinGroup", c[7] % 3 == 0 ? q.Where(p => p.A > 1) : q, false);
            }
            default: // join over projections of both sides / reverse inner join with outer filter
            {
                var q = s.Where(x => x.I > 0).Join(o.Where(y => y.B), ka, kb, (a, b) => new FzP { A = a.Id, B = b.Id, C = b.S, D = a.D });
                if (c[7] % 2 == 0) return Seq("JoinFiltered", q, false);
                return Seq("JoinFilteredOrd", q.OrderBy(p => p.A).ThenBy(p => p.B), true);
            }
        }
    }

    static Plan GroupThenJoin(IQueryable<FzRow> s, IQueryable<FzRow> o, int v)
    {
        if (v == 0)
        {
            var l = s.GroupBy(x => x.I % 3).Select(g => new { g.Key, N = g.Count(), S = g.Sum(x => (long)x.I) });
            var r2 = o.GroupBy(y => y.I % 3).Select(g => new { g.Key, N = g.Count(), M = g.Max(y => y.NI) });
            return Seq("GroupThenJoin", l.Join(r2, a => a.Key, b => b.Key, (a, b) => new FzP { A = a.Key, B = a.N * 100 + b.N, C = b.M.ToString(), D = a.S }), false);
        }
        else
        {
            var l = s.GroupBy(x => x.S).Select(g => new { g.Key, N = g.Count() });
            var q = l.GroupJoin(o, a => a.Key, y => y.S, (a, ys) => new FzP { C = a.Key, A = a.N, B = ys.Count(), D = ys.Sum(y => y.D) });
            return Seq("GroupThenGroupJoin", q, false);
        }
    }

    // ================= Subquery family =================
    static Plan SubqueryP(IQueryable<FzRow> s, IQueryable<FzRow> o, int[] c)
    {
        var preds = SubPreds(o); var sels = SubSels(o);
        var q0 = c[5] % 3 == 0 ? s : s.Where(preds[c[1] % preds.Length]);
        if (c[6] % 4 == 0) q0 = q0.Where(preds[c[7] % preds.Length]);
        switch (c[2] % 3)
        {
            case 0: return Seq("SubWhere", q0.OrderBy(x => x.Id).Select(x => x.Id), true);
            case 1:
            {
                IQueryable<FzP> q = q0.Select(sels[c[3] % sels.Length]);
                return PostFzP("SubSelect", q, SubSelFields[c[3] % sels.Length], c[4], true, false);
            }
            default:
            {
                // projection first, then filter on a subquery-derived value
                var q = s.Select(sels[c[3] % sels.Length]);
                return PostFzP("SubSelect2", WherePost(q, SubSelFields[c[3] % sels.Length], c[4]), SubSelFields[c[3] % sels.Length], c[8], true, false);
            }
        }
    }

    // ================= set operations / terminals =================
    static readonly Expression<Func<FzRow, FzP>>[] Projs =
    [
        x => new FzP { A = x.I, C = x.S },
        x => new FzP { A = x.I % 3, B = x.NI },
        x => new FzP { A = x.B ? 1 : 0, E = x.NB },
        x => new FzP { A = x.S.Length, C = x.NS },
        x => new FzP { C = x.S, D = x.D },
    ];
    static IOrderedQueryable<FzP> SortP(IQueryable<FzP> q, int j) => j switch
    {
        0 => q.OrderBy(p => p.A).ThenBy(p => p.C),
        1 => q.OrderBy(p => p.A).ThenBy(p => p.B),
        2 => q.OrderBy(p => p.A).ThenBy(p => p.E),
        3 => q.OrderBy(p => p.A).ThenBy(p => p.C),
        _ => q.OrderBy(p => p.C).ThenBy(p => p.D),
    };

    static readonly string[] ProjFields = ["AC", "AB", "AE", "AC", "CD"];
    static Plan SetOpP(IQueryable<FzRow> s, IQueryable<FzRow> o, int[] c)
    {
        int j = c[1] % Projs.Length;
        int j2 = c[8] % 4 == 0 && !AvoidKnown ? c[9] % Projs.Length : j;
        IQueryable<FzP> a = s.Select(Projs[j]);
        IQueryable<FzP> b = (c[2] % 3) switch { 0 => o.Select(Projs[j2]), 1 => s.Where(x => x.B).Select(Projs[j2]), _ => o.Where(y => y.I > 0).Select(Projs[j2]) };
        IQueryable<FzP> q;
        string op;
        switch (c[3] % 5)
        {
            case 0: q = a.Concat(b); op = "Concat"; break;
            case 1: q = a.Union(b); op = "Union"; break;
            case 2: q = a.Intersect(b); op = "Intersect"; break;
            case 3: q = a.Except(b); op = "Except"; break;
            default: q = a.Distinct(); op = "Distinct"; break;
        }
        string flds = j2 == j ? ProjFields[j] : "";
        return (c[4] % 5) switch
        {
            0 => Seq(op, q, false),
            1 => Term(op, q, "Count", x => x.Count()),
            2 => Seq(op, SortP(q, j), j2 == j && j != 3 && j != 4 ? true : false),
            3 => Seq(op, SortP(WherePost(q, flds, c[5]), j).Skip(1).Take(5), j2 == j && j != 3 && j != 4 && false),
            _ => Seq(op, WherePost(q, flds, c[5]), false),
        };
    }

    static IOrderedQueryable<FzRow> Ord<K>(IQueryable<FzRow> q, Expression<Func<FzRow, K>> k, bool desc) =>
        desc ? q.OrderByDescending(k).ThenBy(x => x.Id) : q.OrderBy(k).ThenBy(x => x.Id);

    static Plan TerminalP(IQueryable<FzRow> s, IQueryable<FzRow> o, int[] c)
    {
        bool desc = c[2] % 3 == 0;
        IOrderedQueryable<FzRow> q = (c[1] % 8) switch
        {
            0 => Ord(s, x => x.I, desc), 1 => Ord(s, x => x.NI, desc), 2 => Ord(s, x => x.S, desc), 3 => Ord(s, x => x.NS, desc),
            4 => Ord(s, x => x.D, desc), 5 => Ord(s, x => x.NB, desc), 6 => Ord(s, x => x.M, desc), _ => Ord(s, x => x.Id, desc),
        };
        int k = c[4] % 4;
        return (c[3] % 14) switch
        {
            0 => Term("Terminal", q.Select(x => x.Id), "First", z => z.First()),
            1 => Term("Terminal", q.Select(x => x.Id), "FirstOrDefault", z => z.FirstOrDefault()),
            2 => Term("Terminal", q.Select(x => x.Id), "Last", z => z.Last()),
            3 => Term("Terminal", q.Select(x => x.Id), "LastOrDefault", z => z.LastOrDefault()),
            4 => Term("Terminal", q.Select(x => x.NI), "ElementAtOrDefault", z => z.ElementAtOrDefault(k)),
            5 => Term("Terminal", q.Select(x => new FzP { A = x.Id, C = x.NS }), "ElementAt", z => z.ElementAt(k)),
            6 => Term("Terminal", q.Where(x => x.Id == k), "SingleOrDefault", z => z.SingleOrDefault()),
            7 => Term("Terminal", q.Where(x => x.Id == k + 1), "Single", z => z.Single()),
            8 => Seq("Terminal", q.Skip(k).Take(c[5] % 6).Select(x => x.Id), true),
            9 => Seq("Terminal", q.Select(x => x.Id).Reverse(), true),
            10 => Seq("Terminal", q.Skip(c[5] % 5).Select(x => new FzP { A = x.Id, B = x.NI, C = x.NS, D = x.D }), true),
            11 => Seq("Terminal", s.DistinctBy(x => x.I % 3).Select(x => x.Id), true),
            12 => Seq("Terminal", q.DistinctBy(x => x.S).Select(x => x.Id), true),
            _ => Term("Terminal", q.Where(x => x.B).Select(x => new FzP { A = x.Id, C = x.S }), "FirstOrDefault", z => z.FirstOrDefault()),
        };
    }

    // ================= top-level =================
    public static Spec Make(Random r, Gen g)
    {
        var c = Enumerable.Range(0, 12).Select(_ => r.Next(1 << 20)).ToArray();
        int fam = r.Next(100);
        bool needSub = fam >= 60 && fam < 78;
        var preS = MakePre(r, g, true);
        var preO = MakePre(r, g, false);
        int keyIdx = c[10];
        var spec = new Spec();
        spec.Build = (s0, o0) =>
        {
            var o = AvoidKnown ? o0 : preO(o0, o0);
            var s = preS(s0, o);
            var pl = Build2(fam, keyIdx, c, s, o);
            return pl with { OExpr = o.Expression.ToString().Replace("System.Linq.EnumerableQuery`1[Polars.NET.Linq.Fuzz.FzRow]", "O") };
        };
        spec.Family = fam < 30 ? "Group" : fam < 38 ? "Group2" : fam < 60 ? "Join" : fam < 64 ? "GroupJoin2" : fam < 82 ? "Subquery" : fam < 92 ? "SetOp" : "Terminal";
        return spec;
    }

    static Plan Build2(int fam, int keyIdx, int[] c, IQueryable<FzRow> s, IQueryable<FzRow> o)
    {
        {
            if (fam < 30)
            {
                return (keyIdx % 12) switch
                {
                    0 => Group(s, x => x.I % 3, c),
                    1 => Group(s, x => x.I, c),
                    2 => Group(s, x => x.NI, c),
                    3 => Group(s, x => x.S, c),
                    4 => Group(s, x => x.NS, c),
                    5 => Group(s, x => x.B, c),
                    6 => Group(s, x => x.NB, c),
                    7 => Group(s, x => x.S.Length, c),
                    8 => Group(s, x => x.I > 0 ? x.S : x.NS, c),
                    9 => Group(s, x => x.NI % 2, c),
                    10 => Group(s, x => x.S + "!", c),
                    _ => Group(s, x => x.Id % 4, c),
                };
            }
            if (fam < 38) return Group2(s, keyIdx % 8, c);
            if (fam < 60)
            {
                return (keyIdx % 14) switch
                {
                    0 => JoinP(s, o, a => a.I, b => b.I, c),
                    1 => JoinP(s, o, a => a.NI, b => b.NI, c),
                    2 => JoinP(s, o, a => a.S, b => b.S, c),
                    3 => JoinP(s, o, a => a.NS, b => b.NS, c),
                    4 => JoinP(s, o, a => a.NI, b => (int?)b.I, c),
                    5 => JoinP(s, o, a => a.Id, b => b.Id, c),
                    6 => JoinP(s, o, a => new { a.I, a.S }, b => new { b.I, b.S }, c),
                    7 => AvoidKnown ? JoinP(s, o, a => new { a.I, a.B }, b => new { b.I, b.B }, c) : JoinP(s, o, a => new { a.NI, a.B }, b => new { b.NI, b.B }, c),
                    8 => AvoidKnown ? JoinP(s, o, a => new { a.S, a.I }, b => new { b.S, b.I }, c) : JoinP(s, o, a => new { a.NS, a.NI }, b => new { b.NS, b.NI }, c),
                    9 => JoinP(s, o, a => a.S.Length, b => b.I % 5, c),
                    10 => JoinP(s, o, a => a.L, b => (long)b.I, c),
                    11 => JoinP(s, o, a => a.NB, b => b.NB, c),
                    12 => JoinP(s, o, a => a.I % 3, b => b.Id % 3, c),
                    _ => JoinP(s, o, a => new { k = a.S.Length, a.B }, b => new { k = b.I % 4, b.B }, c),
                };
            }
            if (fam < 64) return GroupThenJoin(s, o, AvoidKnown ? 0 : keyIdx % 2);
            if (fam < 82) return SubqueryP(s, o, c);
            if (fam < 92) return SetOpP(s, o, c);
            return TerminalP(s, o, c);
        }
    }

    // ================= execution & comparison =================
    public static string F(object? o)
    {
        switch (o)
        {
            case null: return "null";
            case double d: return double.IsNaN(d) ? "NaN" : double.IsInfinity(d) ? (d > 0 ? "+Inf" : "-Inf") : d == 0 ? "0" : d.ToString("G9", System.Globalization.CultureInfo.InvariantCulture);
            case float f: return F((double)f);
            case decimal m: return (m / 1.0000000000000000000000000000m).ToString(System.Globalization.CultureInfo.InvariantCulture);
            case string s: return "\"" + s + "\"";
            case bool or int or long or short or byte: return Convert.ToString(o, System.Globalization.CultureInfo.InvariantCulture)!;
            case IEnumerable e: return "[" + string.Join(",", e.Cast<object?>().Select(F)) + "]";
        }
        var t = o.GetType();
        return "{" + string.Join(",", t.GetProperties().Select(p => p.Name + "=" + F(p.GetValue(o)))) + "}";
    }

    static (List<string>? res, Exception? ex) RunPlan(Func<IQueryable<FzRow>, IQueryable<FzRow>, Plan> build, IQueryable<FzRow> s, IQueryable<FzRow> o, bool timeout, out Plan? plan)
    {
        Plan? pl = null;
        List<string> Run()
        {
            pl = build(s, o);
            if (pl.Term != null) return [F(pl.Term())];
            var l = new List<string>();
            foreach (var x in pl.Q) l.Add(F(x));
            return l;
        }
        try
        {
            if (!timeout) { var r = Run(); plan = pl; return (r, null); }
            var t = Task.Run(Run);
            if (!t.Wait(30000)) { plan = pl; return (null, new TimeoutException("case exceeded 30s")); }
            plan = pl; return (t.Result, null);
        }
        catch (AggregateException ae) { plan = pl; return (null, ae.InnerException ?? ae); }
        catch (Exception e) { plan = pl; return (null, e); }
    }

    static bool Roundtrips(FzRow[] rows, DataFrame df)
    {
        try
        {
            var back = df.AsQueryable<FzRow>().OrderBy(r => r.Id).ToList();
            if (back.Count != rows.Length) return false;
            for (int k = 0; k < rows.Length; k++) if (F(rows[k]) != F(back[k])) return false;
            return true;
        }
        catch { return false; }
    }

    sealed class SigV : ExpressionVisitor
    {
        public SortedSet<string> Set = [];
        public override Expression? Visit(Expression? n)
        {
            if (n is MethodCallExpression mc && mc.Method.DeclaringType != typeof(string) && mc.Method.DeclaringType != typeof(Math)) Set.Add(mc.Method.Name);
            return base.Visit(n);
        }
    }

    static string Desc(Plan p)
    {
        var s = p.Q.Expression.ToString().Replace("System.Linq.EnumerableQuery`1[Polars.NET.Linq.Fuzz.FzRow]", "S");
        return s + (p.OExpr.Length > 1 && p.OExpr != "O" ? "\n       where o = " + p.OExpr : "") + (p.Term != null ? $"  then .{p.TermName}()" : "") + (p.Ordered ? "  [ordered]" : "  [unordered]");
    }

    public static void RunOne(long seed, int idx, FzRow[] rows, FzRow[] other, Spec spec,
        Dictionary<string, int> counts, Action<string, string, string, string, string, string, long, int> report, Dictionary<string, int> unsupported)
    {
        var (refRes, refEx) = RunPlan(spec.Build, rows.AsQueryable(), other.AsQueryable(), false, out var refPlan);
        if (refEx != null) { counts["REF_THROWS"]++; return; }
        using var df = DataFrame.From(rows);
        using var df2 = DataFrame.From(other);
        bool tainted = !Roundtrips(rows, df) || !Roundtrips(other, df2);
        if (tainted) counts["INGEST_ROUNDTRIP_DIFF"]++;
        var (pRes, pEx) = RunPlan(spec.Build, df.AsQueryable<FzRow>(), df2.AsQueryable<FzRow>(), true, out var pPlan);
        var sv = new SigV(); if (refPlan != null) sv.Visit(refPlan.Q.Expression);
        string sig = spec.Family + ":" + string.Join(",", sv.Set) + (refPlan?.TermName ?? "");
        string rowsTxt = "S:\n" + string.Join("\n", rows.Select(RowStr)) + "\nO:\n" + string.Join("\n", other.Select(RowStr));
        if (pEx != null)
        {
            if (pEx is TimeoutException) { counts["TIMEOUT"]++; report("TIMEOUT", "timeout:" + sig, Desc(refPlan!), rowsTxt, "", "timeout", seed, idx); return; }
            bool pd = false; for (var e = pEx; e != null; e = e.InnerException) if (e is PolarsPushdownException) pd = true;
            if (pd)
            {
                counts["UNSUPPORTED"]++;
                var m = pEx.Message.Split('\n')[0]; if (m.Length > 300) m = m[..300];
                m = "[" + sig + "] " + m.Replace("[PolarsPushdownException] ", ""); unsupported[m] = unsupported.GetValueOrDefault(m) + 1;
                return;
            }
            counts["PROVIDER_THROWS"]++;
            var sb = new StringBuilder(); for (Exception? x = pEx; x != null; x = x.InnerException) sb.Append(x.GetType().Name).Append(": ").Append(x.Message.Split('\n')[0]).Append(" <- ");
            report("PROVIDER_THROWS", "throws:" + sig + ":" + System.Text.RegularExpressions.Regex.Replace(sb.ToString(), @"\d+", "N"), Desc(refPlan!), rowsTxt, "ref=" + Cut(string.Join(" | ", refRes!)), sb.ToString(), seed, idx);
            return;
        }
        var a = refRes!; var b = pRes!;
        bool ordered = refPlan!.Ordered;
        var sa = ordered ? a : [.. a.OrderBy(x => x, StringComparer.Ordinal)];
        var sb2 = ordered ? b : [.. b.OrderBy(x => x, StringComparer.Ordinal)];
        if (sa.SequenceEqual(sb2)) { counts["OK"]++; return; }
        if (tainted) { counts["MISMATCH_INGEST_TAINTED"]++; return; }
        counts["MISMATCH"]++;
        var onlyRef = sa.Except(sb2).Take(3); var onlyProv = sb2.Except(sa).Take(3);
        string diff = $"count ref={a.Count} prov={b.Count}; only-in-ref=[{string.Join(" | ", onlyRef)}] only-in-prov=[{string.Join(" | ", onlyProv)}]";
        if (ordered) { for (int i = 0; i < Math.Min(a.Count, b.Count); i++) if (a[i] != b[i]) { diff += $"; first positional diff at [{i}]: ref={a[i]} prov={b[i]}"; break; } }
        report("MISMATCH", "mismatch:" + sig + (ordered ? ":ord" : ""), Desc(refPlan), rowsTxt, "ref=" + Cut(string.Join(" | ", a)), "prov=" + Cut(string.Join(" | ", b)) + "\n" + diff, seed, idx);
    }

    static string Cut(string s) => s.Length > 600 ? s[..600] + "..." : s;
    public static string RowStr(FzRow r) => $"  Id={r.Id} I={r.I} NI={F(r.NI)} L={r.L} D={F(r.D)} ND={F(r.ND)} M={r.M} S={F(r.S)} NS={F(r.NS)} B={r.B} NB={F(r.NB)}";
}
