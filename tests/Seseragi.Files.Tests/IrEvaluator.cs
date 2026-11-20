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
                return Cast(Eval(c.expr, column), c.targetType);
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
            case ScalarExpr.Binary b when b.op.IsEndsWith:
            {
                object? l = Eval(b.left, column), r = Eval(b.right, column);
                return l is null || r is null ? null : ((string)l).EndsWith((string)r, StringComparison.Ordinal);
            }
            case ScalarExpr.Function { func.IsStrContains: true, args: [var target, var pattern, ScalarExpr.Literal { value: LiteralValue.Bool { Item: true } }] }:
            {
                object? l = Eval(target, column), r = Eval(pattern, column);
                return l is null || r is null ? null : ((string)l).Contains((string)r, StringComparison.Ordinal);
            }
            case ScalarExpr.Unary u when u.op.IsNegate:
                return Eval(u.expr, column) switch
                {
                    null => null,
                    sbyte v => (sbyte)-v,
                    short v => (short)-v,
                    int v => unchecked(-v),
                    long v => unchecked(-v),
                    float v => -v,
                    double v => -v,
                    var v => throw new NotSupportedException($"-{v.GetType().Name}"),
                };
            case ScalarExpr.Binary b when IsArithmetic(b.op):
            {
                object? l = Eval(b.left, column), r = Eval(b.right, column);
                if (l is null || r is null) return null;
                return (l, r) switch
                {
                    (sbyte x, sbyte y) => Integer(b.op, x, y),
                    (byte x, byte y) => Integer(b.op, x, y),
                    (short x, short y) => Integer(b.op, x, y),
                    (ushort x, ushort y) => Integer(b.op, x, y),
                    (int x, int y) => Integer(b.op, x, y),
                    (uint x, uint y) => Integer(b.op, x, y),
                    (long x, long y) => Integer(b.op, x, y),
                    (ulong x, ulong y) => Integer(b.op, x, y),
                    (float x, float y) => Floating(b.op, x, y),
                    (double x, double y) => Floating(b.op, x, y),
                    _ => throw new NotSupportedException($"{l.GetType().Name} {b.op} {r.GetType().Name}"),
                };
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

    private static bool IsArithmetic(BinaryOp op) =>
        op.IsAdd || op.IsSubtract || op.IsMultiply || op.IsDivide || op.IsTruncatedDivide || op.IsFloorDivide || op.IsModulo || op.IsPower;

    /// <summary>
    /// Integer arithmetic in the operands' type (SEMANTICS.md, Arithmetic): wrapping; truncated division and the
    /// remainder by zero null, MinValue / -1 wrapping (remainder 0); floor division by its definition; Power by repeated
    /// multiplication (wrapping).
    /// </summary>
    private static object? Integer<T>(BinaryOp op, T x, T y) where T : System.Numerics.IBinaryInteger<T>
    {
        T minusOne = T.Zero - T.One;
        bool signed = T.IsNegative(minusOne);
        if (op.IsAdd) return x + y;
        if (op.IsSubtract) return x - y;
        if (op.IsMultiply) return x * y;
        if (op.IsPower)
        {
            T p = T.One;
            for (T i = T.Zero; i < y; i++) p *= x;
            return p;
        }
        if (y == T.Zero) return null;
        if (signed && y == minusOne) return op.IsModulo ? T.Zero : T.Zero - x;
        if (op.IsTruncatedDivide) return x / y;
        if (op.IsModulo) return x % y;
        if (op.IsFloorDivide)
        {
            T q = x / y;
            return x % y != T.Zero && (T.IsNegative(x) != T.IsNegative(y)) ? q - T.One : q;
        }
        throw new NotSupportedException($"integer {op}");
    }

    /// <summary>Floating arithmetic in the operands' width (the remainder .NET's, truncated).</summary>
    private static object? Floating<T>(BinaryOp op, T x, T y) where T : System.Numerics.IFloatingPointIeee754<T>
    {
        if (op.IsAdd) return x + y;
        if (op.IsSubtract) return x - y;
        if (op.IsMultiply) return x * y;
        if (op.IsDivide) return x / y;
        if (op.IsModulo) return x % y;
        if (op.IsPower) return T.Pow(x, y);
        throw new NotSupportedException($"floating {op}");
    }

    /// <summary>
    /// A value cast as SEMANTICS.md says (Casts): numbers to integer and floating types as .NET's unchecked
    /// conversion; anything else unchanged (casts that keep values).
    /// </summary>
    private static object? Cast(object? value, LogicalType target)
    {
        if (value is null || target is not LogicalType.Arrow { arrowType: var type }) return value;
        if (value is not (sbyte or byte or short or ushort or int or uint or long or ulong or float or double)) return value;
        return type switch
        {
            Apache.Arrow.Types.Int8Type => (object)Convert<sbyte>(value),
            Apache.Arrow.Types.UInt8Type => Convert<byte>(value),
            Apache.Arrow.Types.Int16Type => Convert<short>(value),
            Apache.Arrow.Types.UInt16Type => Convert<ushort>(value),
            Apache.Arrow.Types.Int32Type => Convert<int>(value),
            Apache.Arrow.Types.UInt32Type => Convert<uint>(value),
            Apache.Arrow.Types.Int64Type => Convert<long>(value),
            Apache.Arrow.Types.UInt64Type => Convert<ulong>(value),
            Apache.Arrow.Types.FloatType => value switch
            {
                long l => (float)l,
                ulong u => (float)u,
                double d => (float)d,
                _ => Convert<float>(value),
            },
            Apache.Arrow.Types.DoubleType => value switch
            {
                long l => (double)l,
                ulong u => (double)u,
                _ => Convert<double>(value),
            },
            _ => value,
        };

        static T Convert<T>(object v) where T : System.Numerics.INumberBase<T> => v switch
        {
            sbyte x => T.CreateTruncating(x),
            byte x => T.CreateTruncating(x),
            short x => T.CreateTruncating(x),
            ushort x => T.CreateTruncating(x),
            int x => T.CreateTruncating(x),
            uint x => T.CreateTruncating(x),
            long x => T.CreateTruncating(x),
            ulong x => T.CreateTruncating(x),
            float x => T.CreateTruncating(x),
            double x => T.CreateTruncating(x),
            _ => throw new NotSupportedException(v.GetType().Name),
        };
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
