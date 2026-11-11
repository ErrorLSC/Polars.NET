using System.Linq.Expressions;
using Polars.CSharp;
using Polars.CSharp.Linq;
using Polars.NET.Linq;
using Kaminari.Samidare;

namespace Polars.NET.Linq.Fuzz;

// Hand-written minimal repro probes for patterns found by the fuzzer (run with: -- probe).
public static class Probe
{
    static FzRow R(long id, int i, int? ni, double d, double? nd, string s, string? ns, bool b, bool? nb) =>
        new() { Id = id, I = i, NI = ni, L = id, D = d, ND = nd, M = 1m, S = s, NS = ns, B = b, NB = nb };

    public static void Run()
    {
        FzRow[] rows = [
            R(0, -7, null, double.NaN, null, "a", null, true, false),
            R(1, 2, 3, 2.5, 0.5, "ab", "b", false, true),
            R(2, 0, -1, -2.5, double.NaN, "", "zz", true, null),
            R(3, 7, 0, 0.5, 1.0, "ba", "a", false, false),
            R(4, -1, 5, double.PositiveInfinity, -0.0, "zz", null, false, true),
            R(5, 3, null, double.NegativeInfinity, 2.5, "abc", "", true, null),
            R(6, int.MinValue, 1, 1.5, 0.0, "b", "ab", false, false),
        ];
        using var df = DataFrame.From(rows);
        void P(string name, Func<IQueryable<FzRow>, object> f)
        {
            string Go(IQueryable<FzRow> s)
            {
                try { var o = f(s); return o is System.Collections.IEnumerable e && o is not string ? "[" + string.Join(", ", e.Cast<object?>().Select(F)) + "]" : F(o); }
                catch (Exception ex) { var r = ex; while (r.InnerException != null) r = r.InnerException; return "THROWS " + r.GetType().Name + ": " + r.Message.Split('\n')[0]; }
            }
            var a = Go(rows.AsQueryable()); var b = Go(df.AsQueryable<FzRow>());
            Console.WriteLine($"{(a == b ? "same" : "DIFF")} {name}\n   ref:  {a}\n   prov: {b}");
        }
        static string F(object? o) => o switch { null => "null", double d => d.ToString("R"), string s => "\"" + s + "\"", FzOut f => $"{{Id={f.Id},A={F(f.A)},B={F(f.B)},C={F(f.C)},D={F(f.Dbool)}}}", _ => o.ToString()! };

        P("Select(x => x.NB)", s => s.OrderBy(x => x.Id).Select(x => x.NB).ToList());
        P("Select(x => (bool?)x.B)", s => s.OrderBy(x => x.Id).Select(x => (bool?)x.B).ToList());
        P("Select(new FzOut{Dbool = x.NB})", s => s.OrderBy(x => x.Id).Select(x => new FzOut { Id = x.Id, Dbool = x.NB }).ToList());
        P("Select(new FzOut{Dbool = !x.NB})", s => s.OrderBy(x => x.Id).Select(x => new FzOut { Id = x.Id, Dbool = !x.NB }).ToList());
        P("Where(x => x.NB ?? true)", s => s.Where(x => x.NB ?? true).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => x.NB == null)", s => s.Where(x => x.NB == null).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => x.NB != null)", s => s.Where(x => x.NB != null).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => x.B || x.NB == null)", s => s.Where(x => x.B || x.NB == null).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => false)", s => s.Where(x => false).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => true)", s => s.Where(x => true).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Count(x => false)", s => s.Count(x => false));
        P("Count(x => true)", s => s.Count(x => true));
        P("Where(x => false).Select(x=>x.D).Distinct()", s => s.Where(x => false).Select(x => x.D).Distinct().ToList());
        P("Where(x => x.B && false).Select(x=>x.D).Distinct()", s => s.Where(x => x.B && false).Select(x => x.D).Distinct().ToList());
        P("OrderBy(x => 1.5).ThenBy(Id)", s => s.OrderBy(x => 1.5).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("OrderBy(x => 1).ThenBy(Id)", s => s.OrderBy(x => 1).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("OrderBy(x => -x.S.Length).ThenBy(Id)", s => s.OrderBy(x => -x.S.Length).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("OrderBy(x => -Math.Abs(x.S.Length)).ThenBy(Id)", s => s.OrderBy(x => -Math.Abs(x.S.Length)).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("OrderBy(x => x.S.Length).ThenBy(Id)", s => s.OrderBy(x => x.S.Length).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("Select(x => -x.S.Length)", s => s.OrderBy(x => x.Id).Select(x => -x.S.Length).ToList());
        P("Select(x => Math.Min(x.D, 1.0))", s => s.OrderBy(x => x.Id).Select(x => Math.Min(x.D, 1.0)).ToList());
        P("Select(x => Math.Max(x.D, 1.0))", s => s.OrderBy(x => x.Id).Select(x => Math.Max(x.D, 1.0)).ToList());
        P("Select(x => Math.Min(x.D / x.D, x.D))", s => s.OrderBy(x => x.Id).Select(x => Math.Min(x.D / x.D, x.D)).ToList());
        P("Select(x => x.D / x.D)", s => s.OrderBy(x => x.Id).Select(x => x.D / x.D).ToList());
        P("Select(x => Math.Round(x.D))", s => s.OrderBy(x => x.Id).Select(x => Math.Round(x.D)).ToList());
        P("Select(x => Math.Floor(x.D))", s => s.OrderBy(x => x.Id).Select(x => Math.Floor(x.D)).ToList());
        P("Select(x => (int)x.D)", s => s.OrderBy(x => x.Id).Select(x => (int)x.D).ToList());
        P("Select(x => (int)Math.Round(x.D))", s => s.OrderBy(x => x.Id).Select(x => (int)Math.Round(x.D)).ToList());
        P("Select(x => x.I % 2)", s => s.OrderBy(x => x.Id).Select(x => x.I % 2).ToList());
        P("Select(x => x.D % 2.0)", s => s.OrderBy(x => x.Id).Select(x => x.D % 2.0).ToList());
        P("Select(x => x.D % x.D)", s => s.OrderBy(x => x.Id).Select(x => x.D % x.D).ToList());
        P("Select(x => x.I * 2)", s => s.OrderBy(x => x.Id).Select(x => x.I * 2).ToList());
        P("Select(x => -x.I)", s => s.OrderBy(x => x.Id).Select(x => -x.I).ToList());
        P("Select(x => Math.Abs(x.D))", s => s.OrderBy(x => x.Id).Select(x => Math.Abs(x.D)).ToList());
        P("Select(x => x.ND / x.ND)", s => s.OrderBy(x => x.Id).Select(x => x.ND / x.ND).ToList());
        P("Where(x => x.D == x.D)", s => s.Where(x => x.D == x.D).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => x.D != x.D)", s => s.Where(x => x.D != x.D).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => x.D < 1.0)", s => s.Where(x => x.D < 1.0).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => x.D >= 1.0)", s => s.Where(x => x.D >= 1.0).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => x.ND == x.ND)", s => s.Where(x => x.ND == x.ND).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => x.ND != x.D)", s => s.Where(x => x.ND != x.D).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => (double?)x.D == (double?)x.D ?? x.B)", s => s.Where(x => ((double?)x.D == (double?)x.D) == true).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("OrderBy(x => x.D).ThenBy(Id)", s => s.OrderBy(x => x.D).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("OrderBy(x => (int)(x.ND ?? double.NegativeInfinity)).ThenBy(Id)", s => s.OrderBy(x => (int)(x.ND ?? double.NegativeInfinity)).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("GroupBy(x => (int)x.D)", s => s.GroupBy(x => (int)x.D).Select(g => new FzAggInt { Key = g.Key, Count = g.Count() }).ToList());
        P("GroupBy(x => x.B)", s => s.GroupBy(x => x.B).Select(g => new FzAggBool { Key = g.Key, Count = g.Count(), Sum = g.Sum(x => x.L), Min = g.Min(x => x.D), Max = g.Max(x => x.I), Avg = g.Average(x => x.D) }).ToList().OrderBy(a => a.Key).ToList().Select(a => (object)$"{a.Key}:{a.Count},{a.Sum},{F(a.Min)},{a.Max},{F(a.Avg)}").ToList());
        P("Max(x => x.I)", s => s.Max(x => x.I));
        P("Select(x => x.S + x.S)", s => s.OrderBy(x => x.Id).Select(x => x.S + x.S).ToList());
        P("Select(x => string.Concat(x.S, \"ba\"))", s => s.OrderBy(x => x.Id).Select(x => string.Concat(x.S, "ba")).ToList());
        P("Select(x => x.NS ?? x.S)", s => s.OrderBy(x => x.Id).Select(x => x.NS ?? x.S).ToList());
        P("Select(x => x.NS + x.S)", s => s.OrderBy(x => x.Id).Select(x => x.NS + x.S).ToList());
        P("Select(x => x.S.ToUpper() + \"ba\")", s => s.OrderBy(x => x.Id).Select(x => x.S.ToUpper() + "ba").ToList());
        P("GroupBy(x => x.S + x.S)", s => s.GroupBy(x => x.S + x.S).Select(g => new FzAggStr { Key = g.Key, Count = g.Count() }).ToList().OrderBy(a => a.Key, StringComparer.Ordinal).Select(a => (object)$"{a.Key}:{a.Count}").ToList());

        // Expression-API probes (identical tree shape to the fuzzer: Convert(value, member type) in a MemberInit)
        var px = Expression.Parameter(typeof(FzRow), "x");
        Expression Pr(string n) => Expression.Property(px, n);
        Func<IQueryable<FzRow>, object> OutQ(Func<Expression>? a, Func<Expression>? b, Func<Expression>? c, Func<Expression>? d) => s =>
        {
            var ot = typeof(FzOut);
            var binds = new List<MemberBinding> { Expression.Bind(ot.GetProperty("Id")!, Pr("Id")) };
            if (a != null) binds.Add(Expression.Bind(ot.GetProperty("A")!, Expression.Convert(a(), typeof(long?))));
            if (b != null) binds.Add(Expression.Bind(ot.GetProperty("B")!, Expression.Convert(b(), typeof(double?))));
            if (c != null) binds.Add(Expression.Bind(ot.GetProperty("C")!, Expression.Convert(c(), typeof(string))));
            if (d != null) binds.Add(Expression.Bind(ot.GetProperty("Dbool")!, Expression.Convert(d(), typeof(bool?))));
            var sel = Expression.Lambda<Func<FzRow, FzOut>>(Expression.MemberInit(Expression.New(ot), binds), px);
            return s.OrderBy(x => x.Id).Select(sel).ToList();
        };
        P("MemberInit{Dbool=Convert(x.NB,bool?)}", OutQ(null, null, null, () => Pr("NB")));
        P("MemberInit{A=Convert(x.L), Dbool=Convert(x.NB)}", OutQ(() => Pr("L"), null, null, () => Pr("NB")));
        P("MemberInit{C=Convert(x.NS), Dbool=Convert(x.NB)}", OutQ(null, null, () => Pr("NS"), () => Pr("NB")));
        P("MemberInit{B=-Abs(x.D), Dbool=Convert(x.NB)}", OutQ(null, () => Expression.Negate(Expression.Call(typeof(Math).GetMethod("Abs", [typeof(double)])!, Pr("D"))), null, () => Pr("NB")));
        P("MemberInit{B=x.ND, Dbool=x.NB}", OutQ(null, () => Pr("ND"), null, () => Pr("NB")));
        P("MemberInit{A=x.L,B=x.D,C=x.S,Dbool=x.NB}", OutQ(() => Pr("L"), () => Pr("D"), () => Pr("S"), () => Pr("NB")));
        P("MemberInit{Dbool=x.NB, A=x.L}", OutQ(() => Pr("L"), null, null, () => Pr("NB")));

        // Round trip of bool? column for random row sets
        int bad = 0;
        for (int k = 0; k < 200 && bad < 3; k++)
        {
            var rr = Pools.MakeRows(new Random(k));
            using var d2 = DataFrame.From(rr);
            var e = rr.OrderBy(x => x.Id).Select(x => x.NB).ToList();
            var g = d2.AsQueryable<FzRow>().OrderBy(x => x.Id).Select(x => x.NB).ToList();
            if (!e.SequenceEqual(g)) { bad++; Console.WriteLine($"NB roundtrip DIFF seed={k}\n  ref:  {string.Join(",", e.Select(v => F(v)))}\n  prov: {string.Join(",", g.Select(v => F(v)))}"); }
        }
        Console.WriteLine($"NB roundtrip bad={bad}");
        bad = 0;
        for (int k = 0; k < 200 && bad < 3; k++)
        {
            var rr = Pools.MakeRows(new Random(k));
            using var d2 = DataFrame.From(rr);
            var e = rr.OrderBy(x => x.Id).Select(x => new FzOut { Id = x.Id, Dbool = x.NB, A = x.L }).ToList();
            var g = d2.AsQueryable<FzRow>().OrderBy(x => x.Id).Select(x => new FzOut { Id = x.Id, Dbool = x.NB, A = x.L }).ToList();
            if (!e.Select(o => o.Dbool).SequenceEqual(g.Select(o => o.Dbool))) { bad++; Console.WriteLine($"FzOut NB DIFF seed={k}\n  ref:  {string.Join(",", e.Select(o => F(o.Dbool)))}\n  prov: {string.Join(",", g.Select(o => F(o.Dbool)))}"); }
        }
        Console.WriteLine($"FzOut NB bad={bad}");

        for (int n = 1; n <= 20; n++)
        {
            var rr = Enumerable.Range(0, n).Select(i => R(i, 0, null, 0, null, "", null, false, i % 3 == 2 ? null : false)).ToArray();
            using var d3 = DataFrame.From(rr);
            var g1 = d3.AsQueryable<FzRow>().OrderBy(x => x.Id).Select(x => x.NB).ToList();
            var g2 = d3.AsQueryable<FzRow>().OrderBy(x => x.Id).ToList().Select(x => x.NB).ToList();
            var e1 = rr.Select(x => x.NB).ToList();
            Console.WriteLine($"n={n} NB all-false/null: select-proj ok={e1.SequenceEqual(g1)} row-materialize ok={e1.SequenceEqual(g2)}");
        }

        Console.WriteLine("=============== round 2 ===============");
        P("Select(x => (int?)(int)x.D)", s => s.OrderBy(x => x.Id).Select(x => (int?)(int)x.D).ToList());
        P("Select(x => (long?)(int)x.D)", s => s.OrderBy(x => x.Id).Select(x => (long?)(int)x.D).ToList());
        P("Where(x => (int)x.D == 0)", s => s.Where(x => (int)x.D == 0).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => (int)x.D > 5)", s => s.Where(x => (int)x.D > 5).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Select(x => Math.Min(x.D, 0.5)) NaN", s => s.OrderBy(x => x.Id).Select(x => Math.Min(x.D, 0.5)).ToList());
        P("Select(x => Math.Min(0.5, x.D)) NaN", s => s.OrderBy(x => x.Id).Select(x => Math.Min(0.5, x.D)).ToList());
        P("Select(x => Math.Max(0.5, x.D)) NaN", s => s.OrderBy(x => x.Id).Select(x => Math.Max(0.5, x.D)).ToList());
        P("Select(x => Math.Min(x.D, x.D))", s => s.OrderBy(x => x.Id).Select(x => Math.Min(x.D, x.D)).ToList());
        P("Where(x => false).Select(x => 0.5).Distinct()", s => s.Where(x => false).Select(x => 0.5).Distinct().ToList());
        P("Where(x => x.I != x.I).Select(x => x.D).Distinct()", s => s.Where(x => x.I != x.I).Select(x => x.D).Distinct().ToList());
        P("Where(x => x.I != x.I).Select(x => 0.5).Distinct()", s => s.Where(x => x.I != x.I).Select(x => 0.5).Distinct().ToList());
        P("Where(x => x.I != x.I).Select(x => 0.5 + x.D*0).Distinct()", s => s.Where(x => x.I != x.I).Select(x => 0.5 + x.D * 0).Distinct().ToList());
        P("Where(x => x.I == x.I || x.M / 0m > 1m)", s => s.Where(x => x.I == x.I || x.M / 0m > 1m).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => x.I == x.I || x.I / 0 > 1)", s => s.Where(x => x.I == x.I || x.I / 0 > 1).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => x.I == x.I || x.I % 0 > 1)", s => s.Where(x => x.I == x.I || x.I % 0 > 1).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Select(x => x.B ? x.I / x.I : 5) (I=0 row)", s => s.OrderBy(x => x.Id).Select(x => x.I == 0 ? 5 : 10 / x.I).ToList());
        P("OrderBy(x => x.S.ToUpper() + x.S).ThenBy(Id)", s => s.OrderBy(x => x.S.ToUpper() + x.S).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("OrderBy(x => x.S + x.S.ToUpper()).ThenBy(Id)", s => s.OrderBy(x => x.S + x.S.ToUpper()).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("OrderBy(x => x.S.ToUpper()).ThenBy(Id)", s => s.OrderBy(x => x.S.ToUpper()).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("OrderBy(x => x.S).ThenBy(Id)", s => s.OrderBy(x => x.S).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("OrderBy(x => x.S, StringComparer.Ordinal).ThenBy(Id)", s => s.OrderBy(x => x.S, StringComparer.Ordinal).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        object W(IQueryable<FzRow> s, Expression<Func<FzRow, bool?>> l) => s.Where(Expression.Lambda<Func<FzRow, bool>>(Expression.Coalesce(l.Body, Expression.Constant(false)), l.Parameters)).OrderBy(x => x.Id).Select(x => x.Id).ToList();
        Expression NullI = Expression.Constant(null, typeof(int?)), NullD = Expression.Constant(null, typeof(double?)), NullB = Expression.Constant(null, typeof(bool?));
        Expression DN = Expression.Convert(Pr("D"), typeof(double?));
        object WL(IQueryable<FzRow> s, Expression body) => W(s, Expression.Lambda<Func<FzRow, bool?>>(body, px));
        P("Where((null != x.NI) [liftToNull] ?? false)", s => WL(s, Expression.NotEqual(NullI, Pr("NI"), true, null)));
        P("Where((x.NI != null) [liftToNull] ?? false)", s => WL(s, Expression.NotEqual(Pr("NI"), NullI, true, null)));
        P("Where((x.NI == null) [liftToNull] ?? false)", s => WL(s, Expression.Equal(Pr("NI"), NullI, true, null)));
        P("Where((x.NI == x.NI) [liftToNull] ?? false)", s => WL(s, Expression.Equal(Pr("NI"), Pr("NI"), true, null)));
        P("Where((x.NI == 3) [liftToNull] ?? false)", s => WL(s, Expression.Equal(Pr("NI"), Expression.Constant(3, typeof(int?)), true, null)));
        P("Where((x.NI < 3) [liftToNull] ?? false)", s => WL(s, Expression.LessThan(Pr("NI"), Expression.Constant(3, typeof(int?)), true, null)));
        P("Where((x.ND == x.ND) [liftToNull] ?? false)", s => WL(s, Expression.Equal(Pr("ND"), Pr("ND"), true, null)));
        P("Where((x.ND != x.ND) [liftToNull] ?? false)", s => WL(s, Expression.NotEqual(Pr("ND"), Pr("ND"), true, null)));
        P("Where(((double?)x.D == (double?)x.D) [liftToNull] ?? false)", s => WL(s, Expression.Equal(DN, DN, true, null)));
        P("Where(((double?)x.D != (double?)x.D) [liftToNull] ?? false)", s => WL(s, Expression.NotEqual(DN, DN, true, null)));
        P("Where((x.ND != null-double) [liftToNull] ?? false)", s => WL(s, Expression.NotEqual(Pr("ND"), NullD, true, null)));
        P("Select(x => x.NI != null) [non-lifted] / Where", s => s.Where(x => x.NI != null).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(((bool?)null & x.NB) ?? false)", s => WL(s, Expression.And(NullB, Pr("NB"))));
        P("Where(((bool?)null | x.NB) ?? false)", s => WL(s, Expression.Or(NullB, Pr("NB"))));
        P("Where(!(bool?)null ?? false)", s => WL(s, Expression.Not(NullB)));
        P("Where(((bool?)null) ?? x.B)", s => s.Where(Expression.Lambda<Func<FzRow, bool>>(Expression.Coalesce(NullB, Pr("B")), px)).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Select(-(int?)null)", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, int?>>(Expression.Negate(NullI), px)).ToList());
        P("Select(-(int?)null ?? x.I)", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, int>>(Expression.Coalesce(Expression.Negate(NullI), Pr("I")), px)).ToList());
        P("Select((int?)null + x.NI)", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, int?>>(Expression.Add(NullI, Pr("NI")), px)).ToList());
        P("Select(x => x.B ? x.NS : null)", s => s.OrderBy(x => x.Id).Select(x => x.B ? x.NS : null).ToList());
        P("Select(x => x.B ? null : x.NS)", s => s.OrderBy(x => x.Id).Select(x => x.B ? null : x.NS).ToList());
        P("Select(x => x.B ? null : null)", s => s.OrderBy(x => x.Id).Select(x => (string?)(x.B ? null : null)).ToList());
        P("Select(x => (string?)null)", s => s.OrderBy(x => x.Id).Select(x => (string?)null).ToList());
        P("Select(x => x.NS ?? null)", s => s.OrderBy(x => x.Id).Select(x => x.NS ?? null).ToList());
        P("FzOut{C = IIF(B, NS, IIF(B, NS, null))}", OutQ(null, null, () => Expression.Condition(Pr("B"), Pr("NS"), Expression.Condition(Pr("B"), Pr("NS"), Expression.Constant(null, typeof(string)))), null));
        P("FzOut{C = IIF(False, IIF(B, NS, null), S)}", OutQ(null, null, () => Expression.Condition(Expression.Constant(false), Expression.Condition(Pr("B"), Pr("NS"), Expression.Constant(null, typeof(string))), Pr("S")), null));
        P("FzOut{C = null const}", OutQ(null, null, () => Expression.Constant(null, typeof(string)), null));
        P("FzOut{B = 0.5 const, A = 1 const}", OutQ(() => Expression.Constant(1L), () => Expression.Constant(0.5), null, () => Expression.Constant(true)));
        P("FzOut{A = null long?}", OutQ(() => Expression.Constant(null, typeof(long?)), null, null, null));
        P("FzOut{Dbool = (bool?)null}", OutQ(null, null, null, () => NullB));
        P("OrderBy(x => 1.5).ThenBy(Id) -> Where(true) first", s => s.Where(x => true).OrderBy(x => 1.5).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("OrderBy(x => \"a\").ThenBy(Id)", s => s.OrderBy(x => "a").ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("OrderBy(x => x.I * 0).ThenBy(Id)", s => s.OrderBy(x => x.I * 0).ThenBy(x => x.Id).Select(x => x.Id).ToList());
        P("GroupBy(x => 1)", s => s.GroupBy(x => 1).Select(g => g.Count()).ToList());
        P("GroupBy(x => (int)x.D).Select(count) nullable key", s => s.GroupBy(x => (int?)(int)x.D).Select(g => new { g.Key, C = g.Count() }).ToList().OrderBy(a => a.Key).Select(a => (object)$"{F(a.Key)}:{a.C}").ToList());

        Console.WriteLine("=============== round 3 ===============");
        P("Select(x => (x.B ? 2 : 0) % 3)", s => s.OrderBy(x => x.Id).Select(x => (x.B ? 2 : 0) % 3).ToList());
        P("Select(x => (x.B ? 2 : 0) + 3)", s => s.OrderBy(x => x.Id).Select(x => (x.B ? 2 : 0) + 3).ToList());
        P("Select(x => (x.B ? 2 : 0) * x.I)", s => s.OrderBy(x => x.Id).Select(x => (x.B ? 2 : 0) * x.I).ToList());
        P("Select(x => (x.B ? 2 : 0) % x.I) (I=0 row -> ref throws)", s => s.OrderBy(x => x.Id).Select(x => (x.B ? 2 : 0) % 3 + x.I).ToList());
        P("Select(x => x.B ? 2 : 0)", s => s.OrderBy(x => x.Id).Select(x => x.B ? 2 : 0).ToList());
        P("Select(x => (x.B ? x.I : 0) % 3)", s => s.OrderBy(x => x.Id).Select(x => (x.B ? x.I : 0) % 3).ToList());
        P("Select(x => (x.I + 1) % 3)", s => s.OrderBy(x => x.Id).Select(x => (x.I + 1) % 3).ToList());
        P("Select(x => (x.B ? 2L : 0L) % 3L)", s => s.OrderBy(x => x.Id).Select(x => (x.B ? 2L : 0L) % 3L).ToList());
        P("Select(x => (x.B ? 2.0 : 0.0) % 3.0)", s => s.OrderBy(x => x.Id).Select(x => (x.B ? 2.0 : 0.0) % 3.0).ToList());
        P("Select(x => (x.B ? 2 : 0) / 3)", s => s.OrderBy(x => x.Id).Select(x => (x.B ? 2 : 0) / 3).ToList());
        P("Select(x => (x.B ? 7 : 5) % 3)", s => s.OrderBy(x => x.Id).Select(x => (x.B ? 7 : 5) % 3).ToList());
        P("Select(x => (x.B ? 2 : 0) % 3 (nullable convert))", s => s.OrderBy(x => x.Id).Select(x => (int?)((x.B ? 2 : 0) % 3)).ToList());
        P("Select(x => !(x.NB & (bool?)null))", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, bool?>>(Expression.Not(Expression.And(Pr("NB"), NullB)), px)).ToList());
        P("Select(x => x.NB & (bool?)null)", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, bool?>>(Expression.And(Pr("NB"), NullB), px)).ToList());
        P("Select(x => x.NB | (bool?)null)", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, bool?>>(Expression.Or(Pr("NB"), NullB), px)).ToList());
        P("Select(x => (bool?)null & (bool?)null)", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, bool?>>(Expression.And(NullB, NullB), px)).ToList());
        P("Select(x => x.NB & x.NB)", s => s.OrderBy(x => x.Id).Select(x => x.NB & x.NB).ToList());
        P("Select(x => x.NB & x.B)", s => s.OrderBy(x => x.Id).Select(x => x.NB & x.B).ToList());
        P("Select(x => x.NB | x.B)", s => s.OrderBy(x => x.Id).Select(x => x.NB | x.B).ToList());
        P("Select(x => x.NB | true)", s => s.OrderBy(x => x.Id).Select(x => x.NB | true).ToList());
        P("Select(x => x.NB & false)", s => s.OrderBy(x => x.Id).Select(x => x.NB & false).ToList());
        P("Select(x => !x.NB)", s => s.OrderBy(x => x.Id).Select(x => !x.NB).ToList());
        P("Select(x => -x.NI)", s => s.OrderBy(x => x.Id).Select(x => -x.NI).ToList());
        P("Select(x => -(x.NI ?? 5))", s => s.OrderBy(x => x.Id).Select(x => -(x.NI ?? 5)).ToList());

        Console.WriteLine("=============== round 4 ===============");
        P("Select(x => x.B ? x.NB : !(x.NB & (bool?)null))", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, bool?>>(Expression.Condition(Pr("B"), Pr("NB"), Expression.Not(Expression.And(Pr("NB"), NullB))), px)).ToList());
        P("Select(x => x.B ? x.NB : !(bool?)null)", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, bool?>>(Expression.Condition(Pr("B"), Pr("NB"), Expression.Not(NullB)), px)).ToList());
        P("Select(x => !(bool?)null)", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, bool?>>(Expression.Not(NullB), px)).ToList());
        P("Select(x => x.B ? x.NB : (bool?)null)", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, bool?>>(Expression.Condition(Pr("B"), Pr("NB"), NullB), px)).ToList());
        P("Select(x => -(x.B ? (int?)null : (int?)null))", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, int?>>(Expression.Negate(Expression.Condition(Pr("B"), NullI, NullI)), px)).ToList());
        P("Select(x => !(x.B ? (bool?)null : (bool?)null))", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, bool?>>(Expression.Not(Expression.Condition(Pr("B"), NullB, NullB)), px)).ToList());
        P("Select(x => (x.B ? (int?)null : (int?)null) + 1)", s => s.OrderBy(x => x.Id).Select(Expression.Lambda<Func<FzRow, int?>>(Expression.Add(Expression.Condition(Pr("B"), NullI, NullI), Expression.Constant(1, typeof(int?))), px)).ToList());
        P("Where(x => ((bool?)null & x.NB) ?? false)  [again]", s => WL(s, Expression.And(NullB, Pr("NB"))));
        P("Where(x => ((bool?)null & x.NB) ?? x.B)", s => s.Where(Expression.Lambda<Func<FzRow, bool>>(Expression.Coalesce(Expression.And(NullB, Pr("NB")), Pr("B")), px)).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => ((bool?)null | x.NB) ?? x.B)", s => s.Where(Expression.Lambda<Func<FzRow, bool>>(Expression.Coalesce(Expression.Or(NullB, Pr("NB")), Pr("B")), px)).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Where(x => x.NB & (bool?)null ?? x.B)", s => s.Where(Expression.Lambda<Func<FzRow, bool>>(Expression.Coalesce(Expression.And(Pr("NB"), NullB), Pr("B")), px)).OrderBy(x => x.Id).Select(x => x.Id).ToList());
        P("Select(x => x.D <= 2.5 ? true : x.B) / Where(x.NB ?? (x.D<=2.5))", s => s.Where(x => x.NB ?? (x.D <= 2.5)).OrderBy(x => x.Id).Select(x => x.Id).ToList());
    }
}
