using System.Text;
using Kaminari.Samidare.IR;

namespace Seseragi.Files.Tests;

/// <summary>
/// The IR's semantics on one row (Samidare's IR/SEMANTICS.md), written independently of the compiler: Kleene logic,
/// null comparisons, NaN greater than every number and equal to NaN, strings in code point order. Values are the CLR
/// values IR literals hold; struct values are dictionaries of their fields; <see cref="Missing"/> marks a column the
/// caller cannot give (read as null).
/// </summary>
internal static class IrEvaluator
{
    public sealed class Missing
    {
        public static readonly Missing Instance = new();
    }

    public static object? Eval(ScalarExpr e, Func<string, object?> column)
    {
        switch (e)
        {
            case ScalarExpr.Column c:
                return column(c.name) is Missing ? null : column(c.name);
            case ScalarExpr.Literal l:
                return l.value.BoxedValue;
            case ScalarExpr.Cast c:
                return Eval(c.expr, column);
            case ScalarExpr.Alias a:
                return Eval(a.expr, column);
            case ScalarExpr.Function f when f.func.IsStructFieldByName:
                return Eval(f.args.Head, column) is IReadOnlyDictionary<string, object?> fields
                       && fields.TryGetValue((string)Eval(f.args.Tail.Head, column)!, out object? field) ? field : null;
            case ScalarExpr.Unary u when u.op.IsNot:
                return Eval(u.expr, column) is bool inner ? !inner : null;
            case ScalarExpr.Unary u when u.op.IsIsNull:
                return Eval(u.expr, column) is null;
            case ScalarExpr.Unary u when u.op.IsIsNotNull:
                return Eval(u.expr, column) is not null;
            case ScalarExpr.Unary u when u.op.IsIsNan:
                return Eval(u.expr, column) is { } operand ? IsNaN(operand) : null;
            case ScalarExpr.Binary b when b.op.IsAnd:
            {
                object? l = Eval(b.left, column), r = Eval(b.right, column);
                return l is false || r is false ? false : l is true && r is true ? true : null;
            }
            case ScalarExpr.Binary b when b.op.IsOr:
            {
                object? l = Eval(b.left, column), r = Eval(b.right, column);
                return l is true || r is true ? true : l is false && r is false ? false : null;
            }
            case ScalarExpr.Binary b when b.op.IsStartsWith:
            {
                object? l = Eval(b.left, column), r = Eval(b.right, column);
                return l is null || r is null ? null : ((string)l).StartsWith((string)r, StringComparison.Ordinal);
            }
            case ScalarExpr.Binary b when b.op.IsEqMissing || b.op.IsNotEqMissing:
            {
                object? l = Eval(b.left, column), r = Eval(b.right, column);
                bool equal = l is null || r is null ? l is null && r is null : Compare(l, r) == 0;
                return b.op.IsEqMissing ? equal : !equal;
            }
            case ScalarExpr.Binary b:
            {
                object? l = Eval(b.left, column), r = Eval(b.right, column);
                if (l is null || r is null) return null;
                int c = Compare(l, r);
                return b.op switch
                {
                    _ when b.op.IsEq => c == 0,
                    _ when b.op.IsNotEq => c != 0,
                    _ when b.op.IsLt => c < 0,
                    _ when b.op.IsLtEq => c <= 0,
                    _ when b.op.IsGt => c > 0,
                    _ when b.op.IsGtEq => c >= 0,
                    _ => throw new NotSupportedException(b.op.ToString()),
                };
            }
            case ScalarExpr.InList i:
            {
                object? value = Eval(i.expr, column);
                LiteralValue[] items = [.. ((InListSource.Literals)i.source).Item];
                bool hasNull = items.Any(x => x is LiteralValue.Null);
                bool? result;
                if (value is null) result = i.nullsEqual ? hasNull : null;
                else if (items.Any(x => x is not LiteralValue.Null && Compare(value, x.BoxedValue!) == 0)) result = true;
                else result = hasNull && !i.nullsEqual ? null : false;
                return i.negated ? !result : result;
            }
            case ScalarExpr.Coalesce c:
                return c.exprs.Select(x => Eval(x, column)).FirstOrDefault(x => x is not null);
            default:
                throw new NotSupportedException(e.GetType().Name);
        }
    }

    public static bool IsNaN(object v) => v switch
    {
        double d => double.IsNaN(d),
        float f => float.IsNaN(f),
        Half h => Half.IsNaN(h),
        _ => false,
    };

    public static int Compare(object a, object b)
    {
        if (a is string x && b is string y) return Math.Sign(Encoding.UTF8.GetBytes(x).AsSpan().SequenceCompareTo(Encoding.UTF8.GetBytes(y)));
        if (a is bool p && b is bool q) return p.CompareTo(q);
        if (IsFloating(a) || IsFloating(b))
        {
            double l = ToDouble(a), r = ToDouble(b);
            return double.IsNaN(l) ? (double.IsNaN(r) ? 0 : 1) : double.IsNaN(r) ? -1 : l < r ? -1 : l > r ? 1 : 0;
        }
        if (ToExact(a) is decimal m && ToExact(b) is decimal n) return m.CompareTo(n);
        return Ticks(a).CompareTo(Ticks(b));

        static bool IsFloating(object v) => v is double or float or Half;
        static double ToDouble(object v) => v switch
        {
            double d => d,
            float f => f,
            Half h => (double)h,
            decimal m => (double)m,
            _ => Convert.ToDouble(v, System.Globalization.CultureInfo.InvariantCulture),
        };
        static decimal? ToExact(object v) => v switch
        {
            sbyte or byte or short or ushort or int or uint or long or ulong => Convert.ToDecimal(v, System.Globalization.CultureInfo.InvariantCulture),
            decimal m => m,
            _ => null,
        };
        static long Ticks(object v) => v switch
        {
            DateOnly d => d.DayNumber * TimeSpan.TicksPerDay,
            DateTime t => t.Ticks,
            DateTimeOffset o => o.UtcTicks,
            TimeOnly t => t.Ticks,
            TimeSpan s => s.Ticks,
            _ => throw new NotSupportedException($"compare {v.GetType().Name}"),
        };
    }
}
