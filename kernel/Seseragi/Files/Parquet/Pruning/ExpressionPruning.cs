using Apache.Arrow.Types;

namespace Kaminari.Seseragi.Files.Parquet.Pruning;

/// <summary>
/// The values an integer-valued expression may take over the rows of a unit: a closed interval when bounded (no
/// row's value lies outside it), whether a row may be null and whether one may hold a value.
/// </summary>
internal readonly record struct Interval(Int128 Lo, Int128 Hi, bool Bounded, bool Nulls, bool Values)
{
    public static Interval Unbounded(bool nulls, bool values) => new(0, 0, false, nulls, values);
}

/// <summary>
/// An integer-valued expression over leaves (Samidare's IR arithmetic on integers: wrapping in its type), bounded by
/// interval arithmetic on statistics. Where a row's value might wrap, divide by -1 out of range or the bounds are
/// unknown, the interval is unbounded: pruning only weakens.
/// </summary>
internal abstract class IntegerRange(string key, IArrowType? type)
{
    /// <summary>What the values are (only ranges of one key compare): "int" for integers, else the leaf's shape, unit, scale.</summary>
    public string Key { get; } = key;

    /// <summary>The integer type values are computed in ("int" ranges only).</summary>
    public IArrowType? Type { get; } = type;

    /// <summary>The interval over the unit's rows; null when the unit has no statistics for a leaf read.</summary>
    public abstract Interval? Evaluate(IUnitStatistics unit);

    /// <summary>The values of an integer type, or null for none.</summary>
    public static (Int128 Min, Int128 Max)? Limits(IArrowType? type) => type switch
    {
        Int8Type => (sbyte.MinValue, sbyte.MaxValue),
        UInt8Type => (0, byte.MaxValue),
        Int16Type => (short.MinValue, short.MaxValue),
        UInt16Type => (0, ushort.MaxValue),
        Int32Type => (int.MinValue, int.MaxValue),
        UInt32Type => (0, uint.MaxValue),
        Int64Type => (long.MinValue, long.MaxValue),
        UInt64Type => (0, ulong.MaxValue),
        _ => null,
    };

    /// <summary>The interval kept where every value fits the type (no row wraps), else unbounded.</summary>
    protected Interval Within(Int128 lo, Int128 hi, bool nulls, bool values) =>
        Limits(Type) is var (min, max) && lo >= min && hi <= max ? new(lo, hi, true, nulls, values) : Interval.Unbounded(nulls, values);
}

/// <summary>A leaf's stored values (an integer domain), bounded by its min / max.</summary>
internal sealed class LeafRange(LeafDomain domain, string key, IArrowType? type) : IntegerRange(key, type)
{
    public LeafDomain Domain => domain;

    public override Interval? Evaluate(IUnitStatistics unit)
    {
        if (!unit.TryGet(domain.Leaf, domain, out LeafStatistics s)) return null;
        bool nulls = s.NullsPossible, values = s.ValuesPossible;
        if (s.Min is { } min && s.Max is { } max && domain.TryDecodeInteger(min, out Int128 lo) && domain.TryDecodeInteger(max, out Int128 hi) && lo <= hi)
            return new Interval(lo, hi, true, nulls, values);
        return Interval.Unbounded(nulls, values);
    }
}

/// <summary>A value on every row.</summary>
internal sealed class ConstantRange(Int128 value, string key, IArrowType? type) : IntegerRange(key, type)
{
    public override Interval? Evaluate(IUnitStatistics unit) => new Interval(value, value, true, false, true);
}

/// <summary>
/// Arithmetic between two ranges of one integer type: + - * from the corners; truncated division and the remainder
/// over the divisor's values but zero (a zero divisor makes nulls possible).
/// </summary>
internal sealed class ArithmeticRange(Kaminari.Sazanami.Expressions.ArithmeticOp op, IntegerRange left, IntegerRange right, IArrowType type)
    : IntegerRange("int", type)
{
    public override Interval? Evaluate(IUnitStatistics unit)
    {
        if (left.Evaluate(unit) is not { } a || right.Evaluate(unit) is not { } b) return null;
        bool nulls = a.Nulls || b.Nulls, values = a.Values && b.Values;
        if (op is Kaminari.Sazanami.Expressions.ArithmeticOp.Divide or Kaminari.Sazanami.Expressions.ArithmeticOp.Remainder)
        {
            // A zero divisor gives null; only the other divisors give values.
            if (!b.Bounded) return Interval.Unbounded(true, values);
            if (b.Lo <= 0 && b.Hi >= 0) nulls = true;
            if (b.Lo == 0 && b.Hi == 0) return new Interval(0, 0, false, nulls, false);
            if (!a.Bounded) return Interval.Unbounded(nulls, values);
            Int128 lo = Int128.MaxValue, hi = Int128.MinValue;
            foreach ((Int128 c, Int128 d) in (ReadOnlySpan<(Int128, Int128)>)[(b.Lo, Int128.Min(b.Hi, -1)), (Int128.Max(b.Lo, 1), b.Hi)])
            {
                if (c > d) continue; // no divisor of this sign
                if (op == Kaminari.Sazanami.Expressions.ArithmeticOp.Divide)
                {
                    // Truncated division is monotonic in each operand for divisors of one sign: the corners bound it.
                    foreach (Int128 q in (ReadOnlySpan<Int128>)[a.Lo / c, a.Lo / d, a.Hi / c, a.Hi / d])
                    {
                        lo = Int128.Min(lo, q);
                        hi = Int128.Max(hi, q);
                    }
                }
                else
                {
                    // |a % d| < |d|, of the dividend's sign, never beyond the dividend.
                    Int128 m = Int128.Max(Int128.Abs(c), Int128.Abs(d)) - 1;
                    lo = Int128.Min(lo, a.Lo >= 0 ? 0 : Int128.Max(a.Lo, -m));
                    hi = Int128.Max(hi, a.Hi <= 0 ? 0 : Int128.Min(a.Hi, m));
                }
            }
            return Within(lo, hi, nulls, values);
        }
        if (!a.Bounded || !b.Bounded) return Interval.Unbounded(nulls, values);
        try
        {
            return op switch
            {
                Kaminari.Sazanami.Expressions.ArithmeticOp.Add => Within(checked(a.Lo + b.Lo), checked(a.Hi + b.Hi), nulls, values),
                Kaminari.Sazanami.Expressions.ArithmeticOp.Subtract => Within(checked(a.Lo - b.Hi), checked(a.Hi - b.Lo), nulls, values),
                _ => Corners(checked(a.Lo * b.Lo), checked(a.Lo * b.Hi), checked(a.Hi * b.Lo), checked(a.Hi * b.Hi), nulls, values),
            };
        }
        catch (OverflowException)
        {
            return Interval.Unbounded(nulls, values);
        }
    }

    private Interval Corners(Int128 w, Int128 x, Int128 y, Int128 z, bool nulls, bool values) =>
        Within(Int128.Min(Int128.Min(w, x), Int128.Min(y, z)), Int128.Max(Int128.Max(w, x), Int128.Max(y, z)), nulls, values);
}

/// <summary><c>-x</c> of a signed integer.</summary>
internal sealed class NegateRange(IntegerRange operand) : IntegerRange("int", operand.Type)
{
    public override Interval? Evaluate(IUnitStatistics unit) => operand.Evaluate(unit) is not { } a ? null
        : a.Bounded ? Within(-a.Hi, -a.Lo, a.Nulls, a.Values) : a;
}

/// <summary>An integer cast to another integer type: the same values where they all fit it, else unbounded (wrapping).</summary>
internal sealed class CastRange(IntegerRange operand, IArrowType type) : IntegerRange("int", type)
{
    public override Interval? Evaluate(IUnitStatistics unit) => operand.Evaluate(unit) is not { } a ? null
        : a.Bounded ? Within(a.Lo, a.Hi, a.Nulls, a.Values) : a;
}

/// <summary>
/// <c>left op right</c> over two integer ranges of one key: the signs <c>left - right</c> may take decide true and false;
/// nulls are possible where either side's are (with <paramref name="nullsEqual"/>, an equality: two nulls equal, one
/// null unequal).
/// </summary>
internal sealed class RangeCompareNode(IntegerRange left, LeafTest test, IntegerRange right, bool nullsEqual) : PruneNode
{
    public override Outcomes Evaluate(IUnitStatistics unit)
    {
        if (left.Evaluate(unit) is not { } a || right.Evaluate(unit) is not { } b) return Outcomes.Any;
        Signs signs = Signs.Any;
        if (a.Bounded && b.Bounded)
        {
            Int128 lo = a.Lo - b.Hi, hi = a.Hi - b.Lo;
            signs = (lo < 0 ? Signs.Less : 0) | (lo <= 0 && hi >= 0 ? Signs.Equal : 0) | (hi > 0 ? Signs.Greater : 0);
        }
        return ComparePruning.Outcomes(signs, test, a.Nulls, a.Values, b.Nulls, b.Values, nullsEqual);
    }
}

/// <summary>Two floating or string leaves compared (statistics bounds of each; NaN, where possible, may be anything).</summary>
internal sealed class LeafPairNode(LeafDomain left, LeafTest test, LeafDomain right, bool nullsEqual, bool excludeNaN) : PruneNode
{
    public override Outcomes Evaluate(IUnitStatistics unit)
    {
        if (!unit.TryGet(left.Leaf, left, out LeafStatistics a) || !unit.TryGet(right.Leaf, right, out LeafStatistics b)) return Outcomes.Any;
        Signs signs = Signs.Any;
        if (left.Kind == DomainKind.Bytes)
        {
            if (a.Min is { } aMin && a.Max is { } aMax && b.Min is { } bMin && b.Max is { } bMax)
            {
                signs = (aMin.AsSpan().SequenceCompareTo(bMax) < 0 ? Signs.Less : 0)
                        | (aMin.AsSpan().SequenceCompareTo(bMax) <= 0 && bMin.AsSpan().SequenceCompareTo(aMax) <= 0 ? Signs.Equal : 0)
                        | (aMax.AsSpan().SequenceCompareTo(bMin) > 0 ? Signs.Greater : 0);
            }
        }
        else if (excludeNaN || !left.NaNPossible(a) && !right.NaNPossible(b))
        {
            if (a.Min is { } aMin && a.Max is { } aMax && b.Min is { } bMin && b.Max is { } bMax
                && left.TryDecodeFloat(aMin, out double x0) && left.TryDecodeFloat(aMax, out double x1)
                && right.TryDecodeFloat(bMin, out double y0) && right.TryDecodeFloat(bMax, out double y1)
                && !double.IsNaN(x0) && !double.IsNaN(x1) && !double.IsNaN(y0) && !double.IsNaN(y1) && x0 <= x1 && y0 <= y1)
            {
                signs = (x0 < y1 ? Signs.Less : 0) | (x0 <= y1 && y0 <= x1 ? Signs.Equal : 0) | (x1 > y0 ? Signs.Greater : 0);
            }
        }
        return ComparePruning.Outcomes(signs, test, a.NullsPossible, a.ValuesPossible, b.NullsPossible, b.ValuesPossible, nullsEqual);
    }
}

internal static class ComparePruning
{
    /// <summary>The outcomes of a comparison from the signs <c>left - right</c> may take and each side's nulls and values.</summary>
    public static Outcomes Outcomes(Signs signs, LeafTest test, bool leftNulls, bool leftValues, bool rightNulls, bool rightValues, bool nullsEqual)
    {
        Signs satisfying = test switch
        {
            LeafTest.Eq => Signs.Equal,
            LeafTest.NotEq => Signs.Less | Signs.Greater,
            LeafTest.Lt => Signs.Less,
            LeafTest.LtEq => Signs.Less | Signs.Equal,
            LeafTest.Gt => Signs.Greater,
            _ => Signs.Greater | Signs.Equal,
        };
        Outcomes r = Pruning.Outcomes.None;
        if (leftValues && rightValues)
        {
            if ((signs & satisfying) != 0) r |= Pruning.Outcomes.True;
            if ((signs & ~satisfying) != 0) r |= Pruning.Outcomes.False;
        }
        if (nullsEqual)
        {
            if (leftNulls && rightNulls) r |= Pruning.Outcomes.True;
            if (leftNulls && rightValues || rightNulls && leftValues) r |= Pruning.Outcomes.False;
        }
        else if (leftNulls || rightNulls)
        {
            r |= Pruning.Outcomes.Null;
        }
        return r;
    }
}
