using System.Collections;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using Polars.CSharp;
using Polars.CSharp.Linq;
using Polars.NET.Core.Exceptions;
using Polars.NET.Linq;

namespace Polars.NET.Linq.Fuzz;

public record Case(int Shape, List<LambdaExpression> Lambdas, Func<Expression, Expression> Build, bool Scalar, Func<List<object?>, List<object?>>? Post);

public record Finding(string Kind, string Sig, int Shape, long Seed, int Index, string Exprs, string Rows, string Ref, string Prov, string Msg);

public static class Program
{
    static readonly Dictionary<string, (int count, Finding best)> Findings = new();
    static readonly Dictionary<string, int> IngestDiffs = new();
    static readonly Dictionary<string, int> Unsupported = new();
    static readonly Dictionary<string, int> Counts = new() { ["OK"] = 0, ["REF_THROWS"] = 0, ["UNSUPPORTED"] = 0, ["PROVIDER_THROWS"] = 0, ["MISMATCH"] = 0, ["TIMEOUT"] = 0, ["MISMATCH_INGEST_TAINTED"] = 0, ["INGEST_ROUNDTRIP_DIFF"] = 0 };
    static readonly Dictionary<int, int> ShapeCount = new();

    public static int Main(string[] args)
    {
        if (args.Length > 0 && args[0] == "probe") { Probe.Run(); return 0; }
        if (args.Length > 2 && args[0] == "case") { Bisect(long.Parse(args[1]), int.Parse(args[2]), args.Length > 3 ? int.Parse(args[3]) : 20); return 0; }
        long seed = args.Length > 0 ? long.Parse(args[0]) : 1;
        int count = args.Length > 1 ? int.Parse(args[1]) : 100;
        Pools.MaxRows = args.Length > 2 ? int.Parse(args[2]) : 20;
        for (int i = 0; i < count; i++)
        {
            var r = new Random(unchecked((int)(seed * 1_000_003 + i)));
            var rows = Pools.MakeRows(r);
            var g = new Gen(r);
            var c = MakeCase(r, g, null);
            RunCase(seed, i, rows, c);
        }
        Console.WriteLine($"SUMMARY seed={seed} count={count} " + string.Join(" ", Counts.Select(kv => $"{kv.Key}={kv.Value}")));
        Console.WriteLine("shapes: " + string.Join(" ", ShapeCount.OrderBy(k => k.Key).Select(k => $"{k.Key}:{k.Value}")));
        Console.WriteLine("=== DataFrame.From -> ToList round trip differences ===");
        foreach (var kv in IngestDiffs) Console.WriteLine($"[{kv.Value}x] {kv.Key}");
        Console.WriteLine($"\n=== UNSUPPORTED distinct messages (first 5) ===");
        foreach (var kv in Unsupported.Take(5)) Console.WriteLine($"[{kv.Value}x] {kv.Key}");
        Console.WriteLine($"\n=== FINDINGS: {Findings.Count} distinct signatures ===");
        foreach (var kv in Findings.OrderBy(k => k.Value.best.Kind).ThenBy(k => k.Key))
        {
            var f = kv.Value.best;
            Console.WriteLine($"\n--- {f.Kind} x{kv.Value.count} shape={f.Shape} seed={f.Seed} case={f.Index} sig={f.Sig}");
            Console.WriteLine("query: " + f.Exprs);
            Console.WriteLine("rows:\n" + f.Rows);
            Console.WriteLine("ref:  " + f.Ref);
            Console.WriteLine("prov: " + f.Prov);
            if (f.Msg.Length > 0) Console.WriteLine("msg:  " + f.Msg);
        }
        return 0;
    }

    // Regenerates one case and, for shape 2, runs each FzOut member binding on its own to find the offending member.
    static void Bisect(long seed, int idx, int maxRows)
    {
        Pools.MaxRows = maxRows;
        var r = new Random(unchecked((int)(seed * 1_000_003 + idx)));
        var rows = Pools.MakeRows(r);
        var c = MakeCase(r, new Gen(r), null);
        Console.WriteLine("shape " + c.Shape + "\n" + string.Join("\n", c.Lambdas.Select(l => l.ToString())));
        using var df = DataFrame.From(rows);
        var sel = c.Lambdas.LastOrDefault();
        if (c.Shape != 2 || sel == null) return;
        var mi = (MemberInitExpression)sel.Body;
        var idQ = Lam(Expression.Property(sel.Parameters[0], "Id"), sel.Parameters[0]);
        foreach (var b in mi.Bindings.Skip(1))
        {
            var one = Expression.Lambda(Expression.MemberInit(mi.NewExpression, mi.Bindings[0], b), sel.Parameters);
            var cc = new Case(2, new List<LambdaExpression> { one }, src => Q("Select", new[] { typeof(FzRow), typeof(FzOut) }, Q("OrderBy", new[] { typeof(FzRow), typeof(long) }, src, Quote(idQ)), Quote(one)), false, null);
            var (a, ea) = Exec(rows.AsQueryable(), cc, false);
            var (pv, ep) = Exec(df.AsQueryable<FzRow>(), cc, true);
            Console.WriteLine($"member {b.Member.Name}: {((MemberAssignment)b).Expression}\n   ref {(ea != null ? "THROWS " + ea.Message : Describe(a, a, rows).Length >= 0 ? "ok" : "")}  prov {(ep != null ? "THROWS " + Root(ep) : Eq(a, pv) ? "same" : "DIFF: " + Describe(a, pv, rows))}");
        }
    }

    // ---------- case construction ----------
    static LambdaExpression Lam(Expression body, ParameterExpression p) => Expression.Lambda(body, p);

    static Expression Q(string name, Type[] targs, params Expression[] args) => Expression.Call(typeof(Queryable), name, targs, args);
    static Expression Quote(LambdaExpression l) => Expression.Quote(l);

    static Case MakeCase(Random r, Gen g, int? forceShape)
    {
        int shape = forceShape ?? r.Next(1, 7);
        var x = g.X;
        var R = typeof(FzRow);
        var lams = new List<LambdaExpression>();
        LambdaExpression Pred() { var l = Lam(g.G(T.Bool, 3), x); lams.Add(l); return l; }
        switch (shape)
        {
            case 1:
            {
                var p = Pred();
                var id = Lam(Expression.Property(x, "Id"), x);
                return new Case(1, lams, s => Q("Select", new[] { R, typeof(long) }, Q("OrderBy", new[] { R, typeof(long) }, Q("Where", new[] { R }, s, Quote(p)), Quote(id)), Quote(id)), false, null);
            }
            case 2:
            {
                Expression A = r.Next(2) == 0 ? g.G(T.Long, 3) : g.G(T.NInt, 3);
                Expression Bx = r.Next(2) == 0 ? g.G(T.NDouble, 3) : g.G(T.Double, 3);
                Expression C = g.G(T.NStr, 3);
                Expression D = r.Next(2) == 0 ? g.G(T.NBool, 3) : g.G(T.Bool, 3);
                var ot = typeof(FzOut);
                var init = Expression.MemberInit(Expression.New(ot),
                    Expression.Bind(ot.GetProperty("Id")!, Expression.Property(x, "Id")),
                    Expression.Bind(ot.GetProperty("A")!, Expression.Convert(A, typeof(long?))),
                    Expression.Bind(ot.GetProperty("B")!, Expression.Convert(Bx, typeof(double?))),
                    Expression.Bind(ot.GetProperty("C")!, Expression.Convert(C, typeof(string))),
                    Expression.Bind(ot.GetProperty("Dbool")!, Expression.Convert(D, typeof(bool?))));
                var sel = Lam(init, x); lams.Add(sel);
                var id = Lam(Expression.Property(x, "Id"), x);
                return new Case(2, lams, s => Q("Select", new[] { R, ot }, Q("OrderBy", new[] { R, typeof(long) }, s, Quote(id)), Quote(sel)), false, null);
            }
            case 3:
            {
                var p = Pred();
                var kt = new[] { T.Int, T.Double, T.Str }[r.Next(3)];
                var key = Lam(g.G(kt, 3), x); lams.Add(key);
                var id = Lam(Expression.Property(x, "Id"), x);
                var kc = Gen.Clr(kt);
                return new Case(3, lams, s => Q("Select", new[] { R, typeof(long) },
                    Q("ThenBy", new[] { R, typeof(long) }, Q("OrderBy", new[] { R, kc }, Q("Where", new[] { R }, s, Quote(p)), Quote(key)), Quote(id)), Quote(id)), false, null);
            }
            case 4:
            {
                int k = r.Next(3);
                var kt = new[] { T.Int, T.Str, T.Bool }[k];
                var kc = Gen.Clr(kt);
                var key = Lam(g.G(kt, 3), x); lams.Add(key);
                var aggT = new[] { typeof(FzAggInt), typeof(FzAggStr), typeof(FzAggBool) }[k];
                var ig = typeof(IGrouping<,>).MakeGenericType(kc, R);
                var gp = Expression.Parameter(ig, "g");
                Expression E(string name, Type[] ta, params Expression[] a) => Expression.Call(typeof(Enumerable), name, ta, a);
                var xp = Expression.Parameter(R, "x");
                LambdaExpression F(string prop, Type t) => Expression.Lambda(Expression.Property(xp, prop), xp);
                var init = Expression.MemberInit(Expression.New(aggT),
                    Expression.Bind(aggT.GetProperty("Key")!, Expression.Property(gp, "Key")),
                    Expression.Bind(aggT.GetProperty("Count")!, E("Count", new[] { R }, gp)),
                    Expression.Bind(aggT.GetProperty("Sum")!, E("Sum", new[] { R }, gp, F("L", typeof(long)))),
                    Expression.Bind(aggT.GetProperty("Min")!, E("Min", new[] { R, typeof(double) }, gp, F("D", typeof(double)))),
                    Expression.Bind(aggT.GetProperty("Max")!, E("Max", new[] { R, typeof(int) }, gp, F("I", typeof(int)))),
                    Expression.Bind(aggT.GetProperty("Avg")!, E("Average", new[] { R }, gp, F("D", typeof(double)))));
                var sel = Expression.Lambda(init, gp); lams.Add(sel);
                return new Case(4, lams, s => Q("Select", new[] { ig, aggT }, Q("GroupBy", new[] { R, kc }, s, Quote(key)), Quote(sel)), false,
                    list => list.OrderBy(o => aggT.GetProperty("Key")!.GetValue(o), Comparer<object?>.Create(CmpKey)).ToList());
            }
            case 5:
            {
                int k = r.Next(5);
                var id = Lam(Expression.Property(x, "Id"), x);
                switch (k)
                {
                    case 0: { var p = Pred(); return new Case(5, lams, s => Q("Count", new[] { R }, s, Quote(p)), true, null); }
                    case 1: { var p = Pred(); return new Case(5, lams, s => Q("Any", new[] { R }, s, Quote(p)), true, null); }
                    case 2: { var p = Pred(); var l = Lam(Expression.Property(x, "L"), x); return new Case(5, lams, s => Q("Sum", new[] { R }, Q("Where", new[] { R }, s, Quote(p)), Quote(l)), true, null); }
                    case 3: { var l = Lam(g.G(T.Int, 3), x); lams.Add(l); return new Case(5, lams, s => Q("Max", new[] { R, typeof(int) }, s, Quote(l)), true, null); }
                    default:
                    {
                        var p = Pred();
                        return new Case(5, lams, s => Expression.Call(typeof(Queryable), "FirstOrDefault", new[] { typeof(long) },
                            Q("Select", new[] { R, typeof(long) }, Q("OrderBy", new[] { R, typeof(long) }, Q("Where", new[] { R }, s, Quote(p)), Quote(id)), Quote(id))), true, null);
                    }
                }
            }
            default:
            {
                var p = Pred();
                var d = Lam(g.G(T.Double, 3), x); lams.Add(d);
                return new Case(6, lams, s => Q("Distinct", new[] { typeof(double) }, Q("Select", new[] { R, typeof(double) }, Q("Where", new[] { R }, s, Quote(p)), Quote(d))), false,
                    list => list.OrderBy(o => (double)o!).ToList());
            }
        }
    }

    static int CmpKey(object? a, object? b)
    {
        if (a == null) return b == null ? 0 : -1;
        if (b == null) return 1;
        if (a is string sa && b is string sb) return string.CompareOrdinal(sa, sb);
        return ((IComparable)a).CompareTo(b);
    }

    // ---------- execution ----------
    static (object? res, Exception? ex) Exec(IQueryable<FzRow> src, Case c, bool timeout)
    {
        object? Run()
        {
            var expr = c.Build(src.Expression);
            object? raw;
            if (c.Scalar) raw = src.Provider.Execute(expr);
            else
            {
                var q = src.Provider.CreateQuery(expr);
                var list = new List<object?>();
                foreach (var o in q) list.Add(o);
                raw = c.Post != null ? c.Post(list) : list;
            }
            return raw;
        }
        try
        {
            if (!timeout) return (Run(), null);
            var t = Task.Run(Run);
            if (!t.Wait(30000)) return (null, new TimeoutException("case exceeded 30s"));
            return (t.Result, null);
        }
        catch (AggregateException ae) { return (null, ae.InnerException ?? ae); }
        catch (Exception e) { return (null, e); }
    }

    static bool IsPushdown(Exception? e)
    {
        for (; e != null; e = e.InnerException) if (e is PolarsPushdownException) return true;
        return false;
    }

    static string Root(Exception e)
    {
        var s = new StringBuilder();
        for (Exception? x = e; x != null; x = x.InnerException) s.Append(x.GetType().Name).Append(": ").Append(x.Message.Split('\n')[0]).Append(" <- ");
        return s.ToString();
    }

    static void RunCase(long seed, int idx, FzRow[] rows, Case c)
    {
        ShapeCount[c.Shape] = ShapeCount.GetValueOrDefault(c.Shape) + 1;
        var (refRes, refEx) = Exec(rows.AsQueryable(), c, false);
        if (refEx != null) { Counts["REF_THROWS"]++; return; }
        using var df = DataFrame.From(rows);
        bool tainted = false;
        try
        {
            var back = df.AsQueryable<FzRow>().OrderBy(r => r.Id).ToList();
            var bad = new SortedSet<string>();
            if (back.Count != rows.Length) bad.Add("count");
            else for (int k = 0; k < rows.Length; k++)
                foreach (var p in typeof(FzRow).GetProperties()) if (!Eq(p.GetValue(rows[k]), p.GetValue(back[k]))) bad.Add(p.Name);
            if (bad.Count > 0)
            {
                tainted = true; Counts["INGEST_ROUNDTRIP_DIFF"]++;
                var key = "INGEST|rows=" + (rows.Length >= 16 ? ">=16" : "<16") + "|" + string.Join(",", bad);
                IngestDiffs[key] = IngestDiffs.GetValueOrDefault(key) + 1;
            }
        }
        catch (Exception e) { IngestDiffs["INGEST_THROWS|" + Normalize(Root(e))] = IngestDiffs.GetValueOrDefault("INGEST_THROWS|" + Normalize(Root(e))) + 1; }
        var (pRes, pEx) = Exec(df.AsQueryable<FzRow>(), c, true);
        if (pEx != null)
        {
            if (pEx is TimeoutException) { Counts["TIMEOUT"]++; Add("TIMEOUT", "timeout", c, seed, idx, rows, refRes, null, pEx.Message); return; }
            if (IsPushdown(pEx))
            {
                Counts["UNSUPPORTED"]++;
                var m = pEx.Message.Split('\n')[0];
                if (m.Length > 300) m = m[..300];
                Unsupported[m] = Unsupported.GetValueOrDefault(m) + 1;
                return;
            }
            Counts["PROVIDER_THROWS"]++;
            Add("PROVIDER_THROWS", "throws:" + Normalize(Root(pEx)), c, seed, idx, rows, refRes, null, Root(pEx));
            return;
        }
        if (Eq(refRes, pRes)) { Counts["OK"]++; return; }
        if (tainted) { Counts["MISMATCH_INGEST_TAINTED"]++; return; }
        Counts["MISMATCH"]++;
        Add("MISMATCH", "mismatch:s" + c.Shape + ":" + Signature(c), c, seed, idx, rows, refRes, pRes, "", Describe(refRes, pRes, rows));
    }

    static string Normalize(string s) => System.Text.RegularExpressions.Regex.Replace(s, @"\d+", "N");

    sealed class SigVisitor : ExpressionVisitor
    {
        public SortedSet<string> Set = new();
        public override Expression? Visit(Expression? n)
        {
            if (n != null)
            {
                if (n is MethodCallExpression mc) Set.Add(mc.Method.Name);
                else if (n is MemberExpression me) { if (me.Member.Name == "Length") Set.Add("Length"); }
                else if (n is UnaryExpression u && u.NodeType == ExpressionType.Convert) Set.Add("Convert:" + Short(u.Operand.Type) + ">" + Short(u.Type));
                else if (n is BinaryExpression b) Set.Add(b.NodeType + (b.Left.Type.IsGenericType ? "?" : ""));
                else if (n.NodeType is ExpressionType.Conditional or ExpressionType.Negate or ExpressionType.Not or ExpressionType.Coalesce) Set.Add(n.NodeType.ToString());
            }
            return base.Visit(n);
        }
        static string Short(Type t) => Nullable.GetUnderlyingType(t) is { } u ? u.Name + "?" : t.Name;
    }

    static string Signature(Case c)
    {
        var v = new SigVisitor();
        foreach (var l in c.Lambdas) v.Visit(l.Body);
        return string.Join(",", v.Set);
    }

    static string RowStr(FzRow r) => $"Id={r.Id} I={r.I} NI={Fmt(r.NI)} L={r.L} D={Fmt(r.D)} ND={Fmt(r.ND)} M={r.M} S=\"{r.S}\" NS={Fmt(r.NS)} B={r.B} NB={Fmt(r.NB)}";

    static string Describe(object? a, object? b, FzRow[] rows)
    {
        if (a is List<object?> la && b is List<object?> lb)
        {
            var sb = new StringBuilder($"count ref={la.Count} prov={lb.Count}; ");
            if (la.Count > 0 && la[0] is long)
            {
                var sa = la.Cast<long>().ToList(); var sbb = lb.Cast<long>().ToList();
                var onlyRef = sa.Except(sbb).ToList(); var onlyProv = sbb.Except(sa).ToList();
                if (onlyRef.Count + onlyProv.Count > 0)
                {
                    sb.Append("only-in-ref=[" + string.Join(",", onlyRef) + "] only-in-prov=[" + string.Join(",", onlyProv) + "]");
                    foreach (var id in onlyRef.Concat(onlyProv).Take(2)) sb.Append("\n    row " + RowStr(rows[id]));
                }
                else sb.Append("same set, different order: ref=[" + string.Join(",", sa) + "] prov=[" + string.Join(",", sbb) + "]");
                return sb.ToString();
            }
            for (int i = 0; i < Math.Min(la.Count, lb.Count); i++)
                if (!Eq(la[i], lb[i]))
                {
                    sb.Append($"first diff at [{i}]: ref={Fmt(la[i])} prov={Fmt(lb[i])}");
                    if (la[i] is FzOut fo) sb.Append("\n    row " + RowStr(rows[fo.Id]));
                    return sb.ToString();
                }
            return sb.ToString() + "(tail differs) ref=" + Fmt(a) + " prov=" + Fmt(b);
        }
        return "ref=" + Fmt(a) + " prov=" + Fmt(b);
    }

    static void Add(string kind, string sig, Case c, long seed, int idx, FzRow[] rows, object? rf, object? pv, string msg, string? diff = null)
    {
        var exprs = string.Join("\n       ", c.Lambdas.Select(l => l.ToString())) + $"   [terminal shape {c.Shape}]";
        var rs = string.Join("\n", rows.Select(r => $"  Id={r.Id} I={r.I} NI={Fmt(r.NI)} L={r.L} D={Fmt(r.D)} ND={Fmt(r.ND)} M={r.M} S=\"{r.S}\" NS={Fmt(r.NS)} B={r.B} NB={Fmt(r.NB)}"));
        var f = new Finding(kind, sig, c.Shape, seed, idx, exprs, rs, diff != null ? "(see diff)" : Fmt(rf), diff ?? Fmt(pv), msg);
        var key = kind + "|" + c.Shape + "|" + sig;
        if (Findings.TryGetValue(key, out var e))
            Findings[key] = (e.count + 1, f.Exprs.Length < e.best.Exprs.Length ? f : e.best);
        else Findings[key] = (1, f);
    }

    // ---------- comparison ----------
    static bool Eq(object? a, object? b)
    {
        if (a == null || b == null) return a == null && b == null;
        if (a is double da) return b is double db && DEq(da, db);
        if (a is decimal ma) return b is decimal mb && ma == mb;
        if (a is List<object?> la)
        {
            if (b is not List<object?> lb || la.Count != lb.Count) return false;
            for (int i = 0; i < la.Count; i++) if (!Eq(la[i], lb[i])) return false;
            return true;
        }
        if (a.GetType() != b.GetType()) return false;
        if (a.GetType().Name.StartsWith("FzOut") || a.GetType().Name.StartsWith("FzAgg"))
        {
            foreach (var p in a.GetType().GetProperties()) if (!Eq(p.GetValue(a), p.GetValue(b))) return false;
            return true;
        }
        return a.Equals(b);
    }

    static bool DEq(double a, double b)
    {
        if (double.IsNaN(a) && double.IsNaN(b)) return true;
        if (double.IsInfinity(a) || double.IsInfinity(b)) return a == b;
        return Math.Abs(a - b) <= 1e-9 * Math.Max(1, Math.Max(Math.Abs(a), Math.Abs(b)));
    }

    static string Fmt(object? o)
    {
        switch (o)
        {
            case null: return "null";
            case double d: return d.ToString("R");
            case string s: return "\"" + s + "\"";
            case List<object?> l: return "[" + string.Join(", ", l.Select(Fmt)) + $"] (n={l.Count})";
            case bool or int or long or decimal: return Convert.ToString(o, System.Globalization.CultureInfo.InvariantCulture)!;
        }
        var t = o.GetType();
        if (t.Name.StartsWith("FzOut") || t.Name.StartsWith("FzAgg"))
            return "{" + string.Join(", ", t.GetProperties().Select(p => p.Name + "=" + Fmt(p.GetValue(o)))) + "}";
        return o.ToString() ?? "";
    }
}
