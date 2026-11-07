using Polars.CSharp;
using Polars.CSharp.Linq;

namespace Polars.NET.Linq.Fuzz;

// Minimal repro probes for the structural findings (run with: -- probe2).
public static class Probe2
{
    static FzRow R(long id, int i, int? ni = null, string s = "", string? ns = null, bool b = false, bool? nb = null, double d = 0) =>
        new() { Id = id, I = i, NI = ni, L = id, D = d, ND = null, M = 1m, S = s, NS = ns, B = b, NB = nb };

    public static void Run()
    {
        var sRows = new[] { R(0, 1, 1, "a", "x", true), R(1, 2, null, "b", null, false, true), R(2, 3, 3, "a", null, true, null), R(3, 2, null, "c", "x", false, false) };
        var oRows = new[] { R(0, 1, 1, "a", "x", true), R(1, 2, null, "b", null, false), R(2, 3, 3, "a", "y", false), R(3, 9, null, "z", null, false) };
        using var sdf = DataFrame.From(sRows); using var odf = DataFrame.From(oRows);
        void P(string name, Func<IQueryable<FzRow>, IQueryable<FzRow>, object> f)
        {
            string Go(IQueryable<FzRow> s, IQueryable<FzRow> o)
            {
                try { var v = f(s, o); return v is System.Collections.IEnumerable e && v is not string ? "[" + string.Join(" | ", e.Cast<object?>().Select(StructGen.F)) + "]" : StructGen.F(v); }
                catch (Exception ex) { var r = ex; while (r.InnerException != null) r = r.InnerException; return "THROWS " + r.GetType().Name + ": " + r.Message.Split('\n')[0]; }
            }
            var a = Go(sRows.AsQueryable(), oRows.AsQueryable()); var b = Go(sdf.AsQueryable<FzRow>(), odf.AsQueryable<FzRow>());
            Console.WriteLine($"{(a == b ? "same" : "DIFF")} {name}\n   ref:  {a}\n   prov: {b}");
        }
        Console.WriteLine("-- legacy shape-2 finding (seed 7 case 109): constant-folded Not(True) != (NI > NI) as a condition");
        {
            var px = System.Linq.Expressions.Expression.Parameter(typeof(FzRow), "x");
            var cond = System.Linq.Expressions.Expression.NotEqual(System.Linq.Expressions.Expression.Not(System.Linq.Expressions.Expression.Constant(true)),
                System.Linq.Expressions.Expression.GreaterThan(System.Linq.Expressions.Expression.Property(px, "NI"), System.Linq.Expressions.Expression.Property(px, "NI")));
            var ite = System.Linq.Expressions.Expression.Condition(cond, System.Linq.Expressions.Expression.Constant("then"), System.Linq.Expressions.Expression.Property(px, "S"));
            var lam = System.Linq.Expressions.Expression.Lambda<Func<FzRow, string>>(ite, px);
            var condLam = System.Linq.Expressions.Expression.Lambda<Func<FzRow, bool>>(cond, px);
            P("Select(x => (!true) != (x.NI > x.NI) ? \"then\" : x.S)   [NI==null rows: NI>NI is false => expect S]", (s, o) => s.OrderBy(x => x.Id).Select(lam).ToList());
            {
                var E = typeof(System.Linq.Expressions.Expression);
                var nsP = System.Linq.Expressions.Expression.Property(px, "NS");
                var inner = System.Linq.Expressions.Expression.Coalesce(System.Linq.Expressions.Expression.Condition(System.Linq.Expressions.Expression.Property(px, "B"), nsP, System.Linq.Expressions.Expression.Constant(null, typeof(string))), System.Linq.Expressions.Expression.Constant("abc"));
                var full = System.Linq.Expressions.Expression.Lambda<Func<FzRow, string>>(System.Linq.Expressions.Expression.Condition(cond, inner, nsP), px);
                P("Select(x => cond ? ((x.B ? x.NS : null) ?? \"abc\") : x.NS)  full tree", (s, o) => s.OrderBy(x => x.Id).Select(full).ToList());
                P("Select(x => (x.B ? x.NS : null) ?? \"abc\")", (s, o) => s.OrderBy(x => x.Id).Select(System.Linq.Expressions.Expression.Lambda<Func<FzRow, string>>(inner, px)).ToList());
                var mi = System.Linq.Expressions.Expression.Lambda<Func<FzRow, FzOut>>(System.Linq.Expressions.Expression.MemberInit(System.Linq.Expressions.Expression.New(typeof(FzOut)),
                    System.Linq.Expressions.Expression.Bind(typeof(FzOut).GetProperty("Id")!, System.Linq.Expressions.Expression.Property(px, "Id")),
                    System.Linq.Expressions.Expression.Bind(typeof(FzOut).GetProperty("C")!, System.Linq.Expressions.Expression.Convert(System.Linq.Expressions.Expression.Condition(cond, inner, nsP), typeof(string)))), px);
                P("Select(new FzOut{C = <full tree>})", (s, o) => s.OrderBy(x => x.Id).Select(mi).ToList());
            }
            P("Select(x => (!true) != (x.NI > x.NI))", (s, o) => s.OrderBy(x => x.Id).Select(condLam).ToList());
            P("Select(x => (x.NI > x.NI))", (s, o) => s.OrderBy(x => x.Id).Select(x => x.NI > x.NI).ToList());
            P("Select(x => false != (x.NI > x.NI))", (s, o) => s.OrderBy(x => x.Id).Select(x => false != (x.NI > x.NI)).ToList());
        }
        Console.WriteLine("-- NaN through aggregates");
        {
            var nr = new[] { R(0, 1, s: "a", d: double.NaN), R(1, 2, s: "b", d: 2.5), R(2, 3, s: "b", d: double.NaN) };
            using var ndf = DataFrame.From(nr);
            void Q(string name, Func<IQueryable<FzRow>, object> f)
            {
                string Go(IQueryable<FzRow> q) { try { var v = f(q); return "[" + string.Join(" | ", ((System.Collections.IEnumerable)v).Cast<object?>().Select(StructGen.F)) + "]"; } catch (Exception ex) { return "THROWS " + ex.GetType().Name; } }
                var a = Go(nr.AsQueryable()); var b = Go(ndf.AsQueryable<FzRow>());
                Console.WriteLine($"{(a == b ? "same" : "DIFF")} {name}\n   ref:  {a}\n   prov: {b}");
            }
            Q("Select(x => x.D > 1)  (plain column control)", q => q.OrderBy(x => x.Id).Select(x => x.D > 1).ToList());
            Q("GroupBy(S, D).Select(Key, Max() > 1)", q => q.GroupBy(x => x.S, x => x.D).Select(h => new { h.Key, M = h.Max() > 1 }).ToList().OrderBy(a => a.Key, StringComparer.Ordinal).ToList());
            Q("GroupBy(S).Select(Key, Max(D) > 1)", q => q.GroupBy(x => x.S).Select(h => new { h.Key, M = h.Max(x => x.D) > 1 }).ToList().OrderBy(a => a.Key, StringComparer.Ordinal).ToList());
            Q("GroupBy(S).Select(Key, Max(D))", q => q.GroupBy(x => x.S).Select(h => new { h.Key, M = h.Max(x => x.D) }).ToList().OrderBy(a => a.Key, StringComparer.Ordinal).ToList());
            Q("GroupBy(S).Select(Key, Min(D) < 1)", q => q.GroupBy(x => x.S).Select(h => new { h.Key, M = h.Min(x => x.D) < 1 }).ToList().OrderBy(a => a.Key, StringComparer.Ordinal).ToList());
            Q("GroupBy(S).Where(Max(D) > 1).Select(Key)", q => q.GroupBy(x => x.S).Where(h => h.Max(x => x.D) > 1).Select(h => h.Key).ToList().OrderBy(a => a, StringComparer.Ordinal).ToList());
            Q("GroupBy(S).Select(Key, Sum(D) > 1)", q => q.GroupBy(x => x.S).Select(h => new { h.Key, M = h.Sum(x => x.D) > 1 }).ToList().OrderBy(a => a.Key, StringComparer.Ordinal).ToList());
        }
        Console.WriteLine("-- subquery source carries a Where");
        P("s.Where(x => o.Where(y=>y.B).Select(y=>y.I).Contains(x.I)).Select(Id)", (s, o) => s.Where(x => o.Where(y => y.B).Select(y => y.I).Contains(x.I)).Select(x => x.Id).ToList());
        P("s.Where(x => o.Where(y=>y.B).Any(y=>y.I==x.I)).Select(Id)", (s, o) => s.Where(x => o.Where(y => y.B).Any(y => y.I == x.I)).Select(x => x.Id).ToList());
        P("ob=o.Where(B); s.Where(x => ob.Select(y=>y.I).Contains(x.I))", (s, o) => { var ob = o.Where(y => y.B); return s.Where(x => ob.Select(y => y.I).Contains(x.I)).Select(x => x.Id).ToList(); });
        P("ob=o.Where(B); s.Where(x => ob.Any(y=>y.I==x.I))", (s, o) => { var ob = o.Where(y => y.B); return s.Where(x => ob.Any(y => y.I == x.I)).Select(x => x.Id).ToList(); });
        P("ob=o.Where(B); s.Where(x => ob.Select(y=>y.S).Distinct().Count() > x.S.Length)", (s, o) => { var ob = o.Where(y => y.B); return s.Where(x => ob.Select(y => y.S).Distinct().Count() > x.S.Length).Select(x => x.Id).ToList(); });
        P("ob=o.Where(B); s.Where(x => x.I > ob.Max(y=>(int?)y.I))", (s, o) => { var ob = o.Where(y => y.B); return s.Where(x => x.I > ob.Max(y => (int?)y.I)).Select(x => x.Id).ToList(); });
        P("ob=o.Where(B); s.Where(x => x.L < ob.Count())", (s, o) => { var ob = o.Where(y => y.B); return s.Where(x => x.L < ob.Count()).Select(x => x.Id).ToList(); });
        P("ob=o.Where(false); s.Where(x => ob.Any())", (s, o) => { var ob = o.Where(y => y.I > 100); return s.Where(x => ob.Any()).Select(x => x.Id).ToList(); });
        P("ob=o.Where(B); s.Where(x => o.Count(y=>y.I==x.I) > 0) (control, no Where)", (s, o) => s.Where(x => o.Count(y => y.I == x.I) > 0).Select(x => x.Id).ToList());
        Console.WriteLine("-- captured filtered subquery variable: which operators drop the Where");
        P("ob.Select(I).Contains(x.I)  [ob=o.Where(I>1)]", (s, o) => { var ob = o.Where(y => y.I > 1); return s.Where(x => ob.Select(y => y.I).Contains(x.I)).Select(x => x.Id).ToList(); });
        P("ob.Where(true).Select(I).Contains(x.I)", (s, o) => { var ob = o.Where(y => y.I > 1); return s.Where(x => ob.Where(y => y.NI == null || y.NI != null).Select(y => y.I).Contains(x.I)).Select(x => x.Id).ToList(); });
        P("ob.Select(S).Distinct().Count() > 1", (s, o) => { var ob = o.Where(y => y.I > 1); return s.Where(x => ob.Select(y => y.S).Distinct().Count() > 2).Select(x => x.Id).ToList(); });
        P("ob.Select(S).Count() > 1  (no Distinct)", (s, o) => { var ob = o.Where(y => y.I > 1); return s.Where(x => ob.Select(y => y.S).Count() > 2).Select(x => x.Id).ToList(); });
        P("ob.LongCount(y => y.B) > Id", (s, o) => { var ob = o.Where(y => y.I > 1); return s.Where(x => x.Id < ob.LongCount(y => y.B)).Select(x => x.Id).ToList(); });
        P("ob.Count(y => y.B) > Id", (s, o) => { var ob = o.Where(y => y.I > 1); return s.Where(x => x.Id < ob.Count(y => y.B)).Select(x => x.Id).ToList(); });
        P("ob.Count() > Id", (s, o) => { var ob = o.Where(y => y.I > 1); return s.Where(x => x.Id < ob.Count()).Select(x => x.Id).ToList(); });
        P("Select(x => new FzP{B = ob.Select(I).Contains(x.I) ? 1 : 0})", (s, o) => { var ob = o.Where(y => y.I > 1); return s.Select(x => new FzP { A = x.Id, B = ob.Select(y => y.I).Contains(x.I) ? 1 : 0 }).ToList(); });
        Console.WriteLine("-- more captured-variable operators (correlated thresholds)");
        P("ob.Select(S).Count() > x.Id  [ob=o.Where(I>1) -> 2 rows]", (s, o) => { var ob = o.Where(y => y.I > 1); return s.Where(x => ob.Select(y => y.S).Count() > x.Id).Select(x => x.Id).ToList(); });
        P("ob.Select(S).Distinct().Count() > x.Id", (s, o) => { var ob = o.Where(y => y.I > 1); return s.Where(x => ob.Select(y => y.S).Distinct().Count() > x.Id).Select(x => x.Id).ToList(); });
        P("ob.Select(I).Max() > x.I", (s, o) => { var ob = o.Where(y => y.I < 3); return s.Where(x => ob.Select(y => y.I).Max() > x.I).Select(x => x.Id).ToList(); });
        P("ob.Max(y => y.I) > x.I", (s, o) => { var ob = o.Where(y => y.I < 3); return s.Where(x => ob.Max(y => y.I) > x.I).Select(x => x.Id).ToList(); });
        P("ob.Select(I).Contains(x.I) where ob = o.Where(I<3) (inline-equivalent)", (s, o) => s.Where(x => o.Where(y => y.I < 3).Select(y => y.I).Contains(x.I)).Select(x => x.Id).ToList());
        P("ob.Select(I).Contains(x.I) where ob = o (no Where)", (s, o) => { var ob = o; return s.Where(x => ob.Select(y => y.I).Contains(x.I)).Select(x => x.Id).ToList(); });
        P("ob.Select(I).Contains(x.I) where ob = o.OrderBy(Id).Take(2)", (s, o) => { var ob = o.OrderBy(y => y.Id).Take(2); return s.Where(x => ob.Select(y => y.I).Contains(x.I)).Select(x => x.Id).ToList(); });
        P("ob.Any(y => y.I == x.I) where ob = o.Take(1)", (s, o) => { var ob = o.OrderBy(y => y.Id).Take(1); return s.Where(x => ob.Any(y => y.I == x.I)).Select(x => x.Id).ToList(); });
        P("ob.Count() > x.Id where ob = o.Take(2)", (s, o) => { var ob = o.OrderBy(y => y.Id).Take(2); return s.Where(x => ob.Count() > x.Id).Select(x => x.Id).ToList(); });
        Console.WriteLine("-- captured filtered subquery: Select(...).Distinct().Count() / Count with real discrimination");
        P("ob=o.Where(B)[1 row S=a]; ob.Select(S).Count() > x.Id", (s, o) => { var ob = o.Where(y => y.B); return s.Where(x => ob.Select(y => y.S).Count() > x.Id).Select(x => x.Id).ToList(); });
        P("ob=o.Where(B)[1 row]; ob.Select(S).Distinct().Count() > x.Id", (s, o) => { var ob = o.Where(y => y.B); return s.Where(x => ob.Select(y => y.S).Distinct().Count() > x.Id).Select(x => x.Id).ToList(); });
        P("ob=o.Where(B)[1 row]; ob.Select(S).Distinct().Count() > x.S.Length", (s, o) => { var ob = o.Where(y => y.B); return s.Where(x => ob.Select(y => y.S).Distinct().Count() > x.S.Length).Select(x => x.Id).ToList(); });
        P("ob=o.Where(B)[1 row]; ob.Select(S).Contains(x.S)", (s, o) => { var ob = o.Where(y => y.B); return s.Where(x => ob.Select(y => y.S).Contains(x.S)).Select(x => x.Id).ToList(); });
        P("ob=o.Where(B)[1 row]; ob.Select(NI).Contains(x.NI)", (s, o) => { var ob = o.Where(y => y.B); return s.Where(x => ob.Select(y => y.NI).Contains(x.NI)).Select(x => x.Id).ToList(); });
        P("ob=o.Where(B)[1 row]; ob.Where(true).Count() > x.Id", (s, o) => { var ob = o.Where(y => y.B); return s.Where(x => ob.Where(y => y.I > 0).Count() > x.Id).Select(x => x.Id).ToList(); });
        P("ob=o.Where(B)[1 row]; ob.Select(S).Count() > x.Id  (ob as IEnumerable via AsEnumerable? no)", (s, o) => { var ob = o.Where(y => y.B); return s.Where(x => ob.Select(y => y.I).Sum() > x.I).Select(x => x.Id).ToList(); });
        Console.WriteLine("-- Contains with nullable");
        P("s.Where(x => o.Select(y=>y.NI).Contains(x.NI))", (s, o) => s.Where(x => o.Select(y => y.NI).Contains(x.NI)).Select(x => x.Id).ToList());

        Console.WriteLine("-- joins with null keys");
        P("Join single nullable key NI", (s, o) => s.Join(o, a => a.NI, b => b.NI, (a, b) => new FzP { A = a.Id, B = b.Id }).ToList());
        P("Join composite {NI,B}", (s, o) => s.Join(o, a => new { a.NI, a.B }, b => new { b.NI, b.B }, (a, b) => new FzP { A = a.Id, B = b.Id }).ToList());
        P("Join composite {NS,NI}", (s, o) => s.Join(o, a => new { a.NS, a.NI }, b => new { b.NS, b.NI }, (a, b) => new FzP { A = a.Id, B = b.Id }).ToList());
        P("Join composite {I,S}", (s, o) => s.Join(o, a => new { a.I, a.S }, b => new { b.I, b.S }, (a, b) => new FzP { A = a.Id, B = b.Id }).ToList());
        P("Join key NB", (s, o) => s.Join(o, a => a.NB, b => b.NB, (a, b) => new FzP { A = a.Id, B = b.Id }).ToList());
        P("GroupJoin key NI, Count", (s, o) => s.GroupJoin(o, a => a.NI, b => b.NI, (a, g) => new FzP { A = a.Id, B = g.Count() }).ToList());
        P("GroupJoin composite {NS,NI}, Count", (s, o) => s.GroupJoin(o, a => new { a.NS, a.NI }, b => new { b.NS, b.NI }, (a, g) => new FzP { A = a.Id, B = g.Count() }).ToList());
        P("left join composite {NS,NI}", (s, o) => s.GroupJoin(o, a => new { a.NS, a.NI }, b => new { b.NS, b.NI }, (a, g) => new { a, g }).SelectMany(t => t.g.DefaultIfEmpty(), (t, b) => new FzP { A = t.a.Id, B = b == null ? (long?)null : b.Id }).ToList());

        Console.WriteLine("-- set ops with different assigned members");
        P("Concat A,C then A,B", (s, o) => s.Select(x => new FzP { A = x.I, C = x.S }).Concat(o.Select(x => new FzP { A = x.I, B = x.NI })).ToList());
        P("Concat C,D then A,C", (s, o) => s.Select(x => new FzP { C = x.S, D = x.D }).Concat(o.Select(x => new FzP { A = x.I, C = x.NS })).ToList());
        P("Union A,E then A,B", (s, o) => s.Select(x => new FzP { A = x.I, E = x.NB }).Union(o.Select(x => new FzP { A = x.I, B = x.NI })).ToList());
        P("Concat same members A,C", (s, o) => s.Select(x => new FzP { A = x.I, C = x.S }).Concat(o.Select(x => new FzP { A = x.I, C = x.S })).ToList());
        P("Concat A,B then A,B (different order of binding)", (s, o) => s.Select(x => new FzP { B = x.NI, A = x.I }).Concat(o.Select(x => new FzP { A = x.I, B = x.NI })).ToList());

        Console.WriteLine("-- GroupBy key name clashes with output member");
        P("GroupBy({B,NB}).Select(FzP{E=NB, A=Count, B=Sum, C=Key.B})", (s, o) => s.GroupBy(x => new { x.B, x.NB }).Select(h => new FzP { E = h.Key.NB, A = h.Count(x => x.B), B = h.Sum(x => (long)x.I), C = h.Key.B ? "t" : "f" }).ToList());
        P("GroupBy({B,NB}).Select(FzP{E=NB, A=Count, C=Key.B})", (s, o) => s.GroupBy(x => new { x.B, x.NB }).Select(h => new FzP { E = h.Key.NB, A = h.Count(x => x.B), C = h.Key.B ? "t" : "f" }).ToList());
        P("GroupBy(S).Select(FzP{C=Key, A=Count, B=Sum})", (s, o) => s.GroupBy(x => x.S).Select(h => new FzP { C = h.Key, A = h.Count(), B = h.Sum(x => (long)x.I) }).ToList());
        P("GroupBy({I,S}).Select(FzP{A=Key.I, C=Key.S, B=Count})", (s, o) => s.GroupBy(x => new { x.I, x.S }).Select(h => new FzP { A = h.Key.I, C = h.Key.S, B = h.Count() }).ToList());

        P("GroupBy({A=I,S}).Select(FzP{A=Count, C=Key.S})", (s, o) => s.GroupBy(x => new { A = x.I, x.S }).Select(h => new FzP { A = h.Count(), C = h.Key.S }).ToList());
        P("GroupBy({B=B}).Select(FzP{A=Count, B=Sum})", (s, o) => s.GroupBy(x => new { x.B }).Select(h => new FzP { A = h.Count(), B = h.Sum(x => (long)x.I) }).ToList());
        P("GroupBy({B,NB}).Select(FzP{A=Count, B=Sum, E=Key.NB})", (s, o) => s.GroupBy(x => new { x.B, x.NB }).Select(h => new FzP { A = h.Count(), B = h.Sum(x => (long)x.I), E = h.Key.NB }).ToList());
        P("GroupBy({B,NB}).Select(FzP{A=Count, B=Sum})  [Key.B unused]", (s, o) => s.GroupBy(x => new { x.B, x.NB }).Select(h => new FzP { A = h.Count(), B = h.Sum(x => (long)x.I) }).ToList());
        P("GroupBy(x=>x.B).Select(FzP{B=Sum, E=Key})", (s, o) => s.GroupBy(x => x.B).Select(h => new FzP { B = h.Sum(x => (long)x.I), E = h.Key }).ToList());
        P("GroupBy({S,NS}).Select(anon{B=Count, S=Key.S})", (s, o) => s.GroupBy(x => new { x.S, x.NS }).Select(h => new { B = h.Count(), h.Key.S }).ToList());
        P("GroupBy({S,B}).Select(anon{B=Count, S=Key.S})", (s, o) => s.GroupBy(x => new { x.S, x.B }).Select(h => new { B = h.Count(), h.Key.S }).ToList());
        Console.WriteLine("-- GroupJoin: output member names that collide with FzRow columns");
        P("GroupJoin anon{a.Id, B=g.Count()}", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, B = g.Count() }).ToList());
        P("GroupJoin anon{a.Id, D=g.Count()}", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, D = g.Count() }).ToList());
        P("GroupJoin anon{a.Id, X=g.Count()} (no clash)", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, X = g.Count() }).ToList());
        P("GroupJoin anon{Id=a.Id, I=g.Count()} (clash I)", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { Id = a.Id, I = g.Count() }).ToList());
        P("GroupJoin anon{a.Id, S=g.Count()} (clash S, int into string col)", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, S = g.Count() }).ToList());
        P("GroupJoin anon{a.Id, N=g.Count()} -> Where(N>0)", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Count() }).Where(t => t.N > 0).ToList());
        P("GroupJoin anon{a.Id, N=g.Sum(b=>b.I)} (non-count agg)", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Sum(b => b.I) }).ToList());
        P("GroupJoin (a,g)->anon{a.Id, Any}  then Select via query syntax", (s, o) => (from a in s join b in o on a.S equals b.S into g select new { a.Id, N = g.Any() }).ToList());
        P("GroupJoin (a,g)->anon{a.Id, Sum(I)} query syntax", (s, o) => (from a in s join b in o on a.S equals b.S into g select new { a.Id, N = g.Sum(b => b.I) }).ToList());
        P("GroupJoin (a,g)->anon{a.Id, Max(NI)} query syntax", (s, o) => (from a in s join b in o on a.S equals b.S into g select new { a.Id, N = g.Max(b => b.NI) }).ToList());
        P("GroupJoin (a,g)->anon{a.Id, Count(pred)} query syntax", (s, o) => (from a in s join b in o on a.S equals b.S into g select new { a.Id, N = g.Count(b => b.B) }).ToList());
        P("GroupJoin anon{a.Id, N=g.Count(b => b.B)} (Count with predicate, no name clash)", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Count(b => b.B) }).ToList());
        P("GroupJoin anon{a.Id, N=g.Count(b => b.I > 100)} (predicate never true)", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Count(b => b.I > 100) }).ToList());
        P("GroupJoin anon{a.Id, N=g.Where(b => b.I > 100).Count()}", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Where(b => b.I > 100).Count() }).ToList());
        P("GroupJoin anon{a.Id, N=g.LongCount(b => b.I > 100)}", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.LongCount(b => b.I > 100) }).ToList());
        P("GroupBy(S).Select(anon{Key, N=Count(I>100)}) (control)", (s, o) => s.GroupBy(x => x.S).Select(g => new { g.Key, N = g.Count(b => b.I > 100) }).ToList());
        P("empty-inner GroupJoin after GroupBy: anon", (s, o) => s.GroupBy(x => x.S).Select(g => new { g.Key, N = g.Count() }).GroupJoin(o.Where(y => y.I > 100), a => a.Key, y => y.S, (a, ys) => new { a.Key, a.N, M = ys.Count() }).ToList());
        P("nonempty-inner GroupJoin after GroupBy: anon", (s, o) => s.GroupBy(x => x.S).Select(g => new { g.Key, N = g.Count() }).GroupJoin(o, a => a.Key, y => y.S, (a, ys) => new { a.Key, a.N, M = ys.Count() }).ToList());
        P("empty-inner GroupJoin no GroupBy: anon", (s, o) => s.GroupJoin(o.Where(y => y.I > 100), a => a.S, y => y.S, (a, ys) => new { a.Id, M = ys.Count() }).ToList());
        P("GroupBy then Join(o) inner (control)", (s, o) => s.GroupBy(x => x.S).Select(g => new { g.Key, N = g.Count() }).Join(o, a => a.Key, y => y.S, (a, y) => new { a.Key, a.N, y.Id }).ToList());
        Console.WriteLine("-- GroupJoin shapes");
        P("GroupJoin direct anon{a.Id, N=g.Count()}", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Count() }).ToList());
        P("GroupJoin direct anon{a.Id, N=g.Count()} key I", (s, o) => s.GroupJoin(o, a => a.I, b => b.I, (a, g) => new { a.Id, N = g.Count() }).ToList());
        P("GroupJoin direct anon{a.Id, Any}", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Any() }).ToList());
        P("GroupJoin direct anon{a.Id, Sum(I)}", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Sum(b => b.I) }).ToList());
        P("GroupJoin direct anon{a.Id, Sum(L)}", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Sum(b => b.L) }).ToList());
        P("join into g select anon{a.Id, g.Count()} (query syntax)", (s, o) => (from a in s join b in o on a.S equals b.S into g select new { a.Id, N = g.Count() }).ToList());
        P("join into g select anon{a.Id, g.Count()} on Id", (s, o) => (from a in s join b in o on a.Id equals b.Id into g select new { a.Id, N = g.Count() }).ToList());
        P("GroupJoin(o.Where(false)) anon{a.Id, N=g.Count()} ", (s, o) => s.GroupJoin(o.Where(y => y.I > 100), a => a.S, b => b.S, (a, g) => new { a.Id, N = g.Count() }).ToList());
        P("GroupJoin(a,g)->Select(FzP{A=a.Id,B=g.Count()})", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a, g }).Select(t => new FzP { A = t.a.Id, B = t.g.Count() }).ToList());
        P("GroupJoin result selector direct FzP{A=a.Id,B=g.Count()}", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new FzP { A = a.Id, B = g.Count() }).ToList());
        P("GroupJoin(a,g)->Select(FzP{A=a.Id,E=g.Any()})", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a, g }).Select(t => new FzP { A = t.a.Id, E = t.g.Any() }).ToList());
        P("GroupJoin(a,g)->Select(FzP{A=a.Id,B=g.Count(b=>b.B)})", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a, g }).Select(t => new FzP { A = t.a.Id, B = t.g.Count(b => b.B) }).ToList());
        P("GroupJoin(a,g)->Select(anon{a.Id, Count})", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a, g }).Select(t => new { t.a.Id, N = t.g.Count() }).ToList());
        P("GroupJoin(a,g)->Select(FzP{A=a.Id,B=g.Sum(b=>(long)b.I)})", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a, g }).Select(t => new FzP { A = t.a.Id, B = t.g.Sum(b => (long)b.I) }).ToList());
        P("grouped-left GroupJoin(o)->FzP{C=a.Key,A=a.N,B=ys.Count()}", (s, o) => s.GroupBy(x => x.S).Select(g => new { g.Key, N = g.Count() }).GroupJoin(o, a => a.Key, y => y.S, (a, ys) => new FzP { C = a.Key, A = a.N, B = ys.Count() }).ToList());
        P("grouped-left GroupJoin(o) ys.Sum(D)", (s, o) => s.GroupBy(x => x.S).Select(g => new { g.Key, N = g.Count() }).GroupJoin(o, a => a.Key, y => y.S, (a, ys) => new FzP { C = a.Key, A = a.N, D = ys.Sum(y => y.D) }).ToList());
        P("grouped-left GroupJoin(o.Where(false))", (s, o) => s.GroupBy(x => x.S).Select(g => new { g.Key, N = g.Count() }).GroupJoin(o.Where(y => y.I > 100), a => a.Key, y => y.S, (a, ys) => new FzP { C = a.Key, A = a.N, B = ys.Count() }).ToList());
        P("GroupJoin(o.Where(false)) Count", (s, o) => s.GroupJoin(o.Where(y => y.I > 100), a => a.S, b => b.S, (a, g) => new FzP { A = a.Id, B = g.Count() }).ToList());
        P("left join with empty inner", (s, o) => s.GroupJoin(o.Where(y => y.I > 100), a => a.S, b => b.S, (a, g) => new { a, g }).SelectMany(t => t.g.DefaultIfEmpty(), (t, b) => new FzP { A = t.a.Id, B = b == null ? (long?)null : b.Id }).ToList());
        P("left join g.Where(B).DefaultIfEmpty -> b.NI (null deref ref throws)", (s, o) => s.GroupJoin(o, a => a.S, b => b.S, (a, g) => new { a, g }).SelectMany(t => t.g.Where(b => b.B).DefaultIfEmpty(), (t, b) => new { t.a, b }).Select(u => new FzP { A = u.a.Id, B = u.b == null ? (long?)null : u.b.NI }).ToList());
    }
}
