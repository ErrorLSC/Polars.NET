using System.Linq.Expressions;
using System.Reflection;

namespace Polars.NET.Linq.Fuzz;

public class FzRow
{
    public long Id { get; set; }
    public int I { get; set; }
    public int? NI { get; set; }
    public long L { get; set; }
    public double D { get; set; }
    public double? ND { get; set; }
    public decimal M { get; set; }
    public string S { get; set; } = "";
    public string? NS { get; set; }
    public bool B { get; set; }
    public bool? NB { get; set; }
}

public class FzOut
{
    public long Id { get; set; }
    public long? A { get; set; }
    public double? B { get; set; }
    public string? C { get; set; }
    public bool? Dbool { get; set; }
}

public class FzAggInt { public int Key { get; set; } public int Count { get; set; } public long Sum { get; set; } public double Min { get; set; } public int Max { get; set; } public double Avg { get; set; } }
public class FzAggStr { public string Key { get; set; } = ""; public int Count { get; set; } public long Sum { get; set; } public double Min { get; set; } public int Max { get; set; } public double Avg { get; set; } }
public class FzAggBool { public bool Key { get; set; } public int Count { get; set; } public long Sum { get; set; } public double Min { get; set; } public int Max { get; set; } public double Avg { get; set; } }

public enum T { Bool, Int, NInt, Long, Double, NDouble, Decimal, Str, NStr, NBool }

public static class Pools
{
    public static int MaxRows = 20;
    public static readonly int[] Ints = { -7, -1, 0, 1, 2, 3, 7, 100, int.MaxValue, int.MinValue };
    public static readonly long[] Longs = { -7, -1, 0, 1, 2, 3, 7, 100, 1000000000000L, int.MaxValue, long.MaxValue };
    public static readonly double[] Doubles = { -2.5, -0.0, 0.0, 0.5, 1.0, 2.5, double.NaN, double.PositiveInfinity, double.NegativeInfinity };
    public static readonly decimal[] Decimals = { -2.5m, 0m, 0.125m, 1m, 2.5m, 10m };
    public static readonly string[] Strings = { "", "a", "ab", "abc", "b", "ba", "zz" };

    public static int PickInt(Random r) { for (; ; ) { var v = Ints[r.Next(Ints.Length)]; if ((v == int.MaxValue || v == int.MinValue) && r.Next(3) != 0) continue; return v; } }
    public static long PickLong(Random r) { for (; ; ) { var v = Longs[r.Next(Longs.Length)]; if ((v == long.MaxValue || v == int.MaxValue) && r.Next(4) != 0) continue; return v; } }
    public static double PickDouble(Random r) { for (; ; ) { var v = Doubles[r.Next(Doubles.Length)]; if ((double.IsNaN(v) || double.IsInfinity(v)) && r.Next(2) == 0) continue; return v; } }
    public static decimal PickDecimal(Random r) => Decimals[r.Next(Decimals.Length)];
    public static string PickStr(Random r) => Strings[r.Next(Strings.Length)];

    public static FzRow[] MakeRows(Random r)
    {
        int n = r.Next(8, MaxRows + 1);
        var rows = new FzRow[n];
        for (int k = 0; k < n; k++)
            rows[k] = new FzRow
            {
                Id = k,
                I = PickInt(r),
                NI = r.Next(4) == 0 ? null : PickInt(r),
                L = PickLong(r),
                D = PickDouble(r),
                ND = r.Next(4) == 0 ? null : PickDouble(r),
                M = PickDecimal(r),
                S = PickStr(r),
                NS = r.Next(4) == 0 ? null : PickStr(r),
                B = r.Next(2) == 0,
                NB = r.Next(4) == 0 ? null : r.Next(2) == 0,
            };
        return rows;
    }
}

public class Gen(Random r)
{
    readonly Random r = r;
    public readonly ParameterExpression X = Expression.Parameter(typeof(FzRow), "x");
    static readonly MethodInfo MAbsI = typeof(Math).GetMethod("Abs", new[] { typeof(int) })!;
    static readonly MethodInfo MAbsD = typeof(Math).GetMethod("Abs", new[] { typeof(double) })!;
    static readonly MethodInfo MFloor = typeof(Math).GetMethod("Floor", new[] { typeof(double) })!;
    static readonly MethodInfo MRound = typeof(Math).GetMethod("Round", new[] { typeof(double) })!;
    static readonly MethodInfo MMaxI = typeof(Math).GetMethod("Max", new[] { typeof(int), typeof(int) })!;
    static readonly MethodInfo MMinI = typeof(Math).GetMethod("Min", new[] { typeof(int), typeof(int) })!;
    static readonly MethodInfo MMaxD = typeof(Math).GetMethod("Max", new[] { typeof(double), typeof(double) })!;
    static readonly MethodInfo MMinD = typeof(Math).GetMethod("Min", new[] { typeof(double), typeof(double) })!;
    static readonly MethodInfo SUpper = typeof(string).GetMethod("ToUpper", Type.EmptyTypes)!;
    static readonly MethodInfo SContains = typeof(string).GetMethod("Contains", new[] { typeof(string) })!;
    static readonly MethodInfo SStarts = typeof(string).GetMethod("StartsWith", new[] { typeof(string) })!;
    static readonly MethodInfo SConcat = typeof(string).GetMethod("Concat", new[] { typeof(string), typeof(string) })!;

    MemberExpression P(string n) => Expression.Property(X, n);
    T Pick(params T[] ts) => ts[r.Next(ts.Length)];
    Expression Pick(params Func<Expression>[] fs) => fs[r.Next(fs.Length)]();

    public static Type Clr(T t) => t switch
    {
        T.Bool => typeof(bool), T.Int => typeof(int), T.NInt => typeof(int?), T.Long => typeof(long),
        T.Double => typeof(double), T.NDouble => typeof(double?), T.Decimal => typeof(decimal),
        T.Str => typeof(string), T.NStr => typeof(string), T.NBool => typeof(bool?), _ => throw new Exception()
    };

    Expression Leaf(T t)
    {
        switch (t)
        {
            case T.Bool: return r.Next(10) < 7 ? P("B") : Expression.Constant(r.Next(2) == 0);
            case T.Int: return r.Next(10) < 6 ? P("I") : Expression.Constant(Pools.PickInt(r));
            case T.NInt: { int k = r.Next(10); return k < 6 ? P("NI") : k < 8 ? Expression.Constant(null, typeof(int?)) : Expression.Constant((int?)Pools.PickInt(r), typeof(int?)); }
            case T.Long: { int k = r.Next(10); return k < 4 ? P("L") : k < 6 ? P("Id") : Expression.Constant(Pools.PickLong(r)); }
            case T.Double: return r.Next(10) < 6 ? P("D") : Expression.Constant(Pools.PickDouble(r));
            case T.NDouble: { int k = r.Next(10); return k < 6 ? P("ND") : k < 8 ? Expression.Constant(null, typeof(double?)) : Expression.Constant((double?)Pools.PickDouble(r), typeof(double?)); }
            case T.Decimal: return r.Next(10) < 6 ? P("M") : Expression.Constant(Pools.PickDecimal(r));
            case T.Str: return r.Next(10) < 6 ? P("S") : Expression.Constant(Pools.PickStr(r));
            case T.NStr: { int k = r.Next(10); return k < 7 ? P("NS") : Expression.Constant(null, typeof(string)); }
            case T.NBool: { int k = r.Next(10); return k < 6 ? P("NB") : k < 8 ? Expression.Constant(null, typeof(bool?)) : Expression.Constant((bool?)(r.Next(2) == 0), typeof(bool?)); }
        }
        throw new Exception();
    }

    BinaryExpression Arith(T t, int d)
    {
        var a = G(t, d - 1); var b = G(t, d - 1);
        return r.Next(5) switch
        {
            0 => Expression.Add(a, b), 1 => Expression.Subtract(a, b), 2 => Expression.Multiply(a, b),
            3 => Expression.Divide(a, b), _ => Expression.Modulo(a, b)
        };
    }

    BinaryExpression Cmp(T t, int d, bool lifted = false)
    {
        var a = G(t, d - 1); var b = G(t, d - 1);
        bool eqOnly = t is T.Bool or T.NBool or T.Str or T.NStr;
        int k = r.Next(eqOnly ? 2 : 6);
        if (lifted)
            return k switch
            {
                0 => Expression.Equal(a, b, true, null), 1 => Expression.NotEqual(a, b, true, null),
                2 => Expression.LessThan(a, b, true, null), 3 => Expression.LessThanOrEqual(a, b, true, null),
                4 => Expression.GreaterThan(a, b, true, null), _ => Expression.GreaterThanOrEqual(a, b, true, null)
            };
        return k switch
        {
            0 => Expression.Equal(a, b), 1 => Expression.NotEqual(a, b),
            2 => Expression.LessThan(a, b), 3 => Expression.LessThanOrEqual(a, b),
            4 => Expression.GreaterThan(a, b), _ => Expression.GreaterThanOrEqual(a, b)
        };
    }

    public Expression G(T t, int d)
    {
        if (d <= 0 || r.Next(10) < 2) return Leaf(t);
        int n = d;
        return t switch
        {
            T.Bool => Pick(
                                () => Cmp(Pick(T.Int, T.Long, T.Double, T.Decimal, T.NInt, T.NDouble, T.Bool, T.NBool, T.Str, T.NStr), n),
                                () => Cmp(Pick(T.Int, T.Double, T.NInt, T.NDouble), n),
                                () => Expression.AndAlso(G(T.Bool, n - 1), G(T.Bool, n - 1)),
                                () => Expression.OrElse(G(T.Bool, n - 1), G(T.Bool, n - 1)),
                                () => Expression.Not(G(T.Bool, n - 1)),
                                () => Expression.Condition(G(T.Bool, n - 1), G(T.Bool, n - 1), G(T.Bool, n - 1)),
                                () => Expression.Coalesce(G(T.NBool, n - 1), G(T.Bool, n - 1)),
                                () => { var tt = Pick(T.NInt, T.NDouble, T.NStr, T.NBool); var e = G(tt, n - 1); var c = Expression.Constant(null, Clr(tt)); return r.Next(2) == 0 ? Expression.Equal(e, c) : Expression.NotEqual(e, c); },
                                () => Expression.Call(G(T.Str, n - 1), SContains, Expression.Constant(Pools.PickStr(r))),
                                () => Expression.Call(G(T.Str, n - 1), SStarts, Expression.Constant(Pools.PickStr(r)))),
            T.Int => Pick(
                                () => Arith(T.Int, n), () => Arith(T.Int, n),
                                () => Expression.Negate(G(T.Int, n - 1)),
                                () => Expression.Condition(G(T.Bool, n - 1), G(T.Int, n - 1), G(T.Int, n - 1)),
                                () => Expression.Coalesce(G(T.NInt, n - 1), G(T.Int, n - 1)),
                                () => Expression.Convert(G(T.Double, n - 1), typeof(int)),
                                () => Expression.Call(MAbsI, G(T.Int, n - 1)),
                                () => Expression.Call(r.Next(2) == 0 ? MMaxI : MMinI, G(T.Int, n - 1), G(T.Int, n - 1)),
                                () => Expression.Property(G(T.Str, n - 1), "Length")),
            T.NInt => Pick(
                                () => Arith(T.NInt, n),
                                () => Expression.Negate(G(T.NInt, n - 1)),
                                () => Expression.Condition(G(T.Bool, n - 1), G(T.NInt, n - 1), G(T.NInt, n - 1)),
                                () => Expression.Coalesce(G(T.NInt, n - 1), G(T.NInt, n - 1)),
                                () => Expression.Convert(G(T.Int, n - 1), typeof(int?))),
            T.Long => Pick(
                                () => Arith(T.Long, n),
                                () => Expression.Negate(G(T.Long, n - 1)),
                                () => Expression.Condition(G(T.Bool, n - 1), G(T.Long, n - 1), G(T.Long, n - 1)),
                                () => Expression.Convert(G(T.Int, n - 1), typeof(long))),
            T.Double => Pick(
                                () => Arith(T.Double, n), () => Arith(T.Double, n),
                                () => Expression.Negate(G(T.Double, n - 1)),
                                () => Expression.Condition(G(T.Bool, n - 1), G(T.Double, n - 1), G(T.Double, n - 1)),
                                () => Expression.Coalesce(G(T.NDouble, n - 1), G(T.Double, n - 1)),
                                () => Expression.Convert(G(T.Int, n - 1), typeof(double)),
                                () => Expression.Call(MAbsD, G(T.Double, n - 1)),
                                () => Expression.Call(MFloor, G(T.Double, n - 1)),
                                () => Expression.Call(MRound, G(T.Double, n - 1)),
                                () => Expression.Call(r.Next(2) == 0 ? MMaxD : MMinD, G(T.Double, n - 1), G(T.Double, n - 1))),
            T.NDouble => Pick(
                                () => Arith(T.NDouble, n),
                                () => Expression.Negate(G(T.NDouble, n - 1)),
                                () => Expression.Condition(G(T.Bool, n - 1), G(T.NDouble, n - 1), G(T.NDouble, n - 1)),
                                () => Expression.Coalesce(G(T.NDouble, n - 1), G(T.NDouble, n - 1)),
                                () => Expression.Convert(G(T.Double, n - 1), typeof(double?))),
            T.Decimal => Pick(
                                () => Arith(T.Decimal, n),
                                () => Expression.Negate(G(T.Decimal, n - 1)),
                                () => Expression.Condition(G(T.Bool, n - 1), G(T.Decimal, n - 1), G(T.Decimal, n - 1))),
            T.Str => Pick(
                                () => Expression.Call(G(T.Str, n - 1), SUpper),
                                () => Expression.Add(G(T.Str, n - 1), G(T.Str, n - 1), SConcat),
                                () => Expression.Call(SConcat, G(T.Str, n - 1), G(T.Str, n - 1)),
                                () => Expression.Condition(G(T.Bool, n - 1), G(T.Str, n - 1), G(T.Str, n - 1)),
                                () => Expression.Coalesce(G(T.NStr, n - 1), G(T.Str, n - 1))),
            T.NStr => Pick(
                                () => Expression.Condition(G(T.Bool, n - 1), G(T.NStr, n - 1), G(T.NStr, n - 1)),
                                () => Expression.Coalesce(G(T.NStr, n - 1), G(T.NStr, n - 1)),
                                () => G(T.Str, n - 1)),
            T.NBool => Pick(
                                () => Expression.Convert(G(T.Bool, n - 1), typeof(bool?)),
                                () => Expression.Not(G(T.NBool, n - 1)),
                                () => Expression.Condition(G(T.Bool, n - 1), G(T.NBool, n - 1), G(T.NBool, n - 1)),
                                () => Expression.Coalesce(G(T.NBool, n - 1), G(T.NBool, n - 1)),
                                () => Expression.And(G(T.NBool, n - 1), G(T.NBool, n - 1)),
                                () => Expression.Or(G(T.NBool, n - 1), G(T.NBool, n - 1)),
                                () => Cmp(Pick(T.NInt, T.NDouble), n, true)),
            _ => throw new Exception(),
        };
    }
}
