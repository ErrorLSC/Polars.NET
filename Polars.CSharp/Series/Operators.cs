#pragma warning disable CS1591
using Polars.NET.Core;

namespace Polars.CSharp;

public partial class Series : IDisposable,IPolarsSeries
{
    // ==========================================
    // Arithmetic Operators
    // ==========================================
    /// <summary>
    /// Add Series
    /// </summary>
    public static Series operator +(Series left, Series right)
        => new(PolarsWrapper.SeriesAdd(left.Handle, right.Handle));

    /// <summary>
    /// Minus Series
    /// </summary>
    public static Series operator -(Series left, Series right)
        => new(PolarsWrapper.SeriesSub(left.Handle, right.Handle));
    /// <summary>
    /// Multiple Series
    /// </summary>
    public static Series operator *(Series left, Series right)
        => new(PolarsWrapper.SeriesMul(left.Handle, right.Handle));
    /// <summary>
    /// Divide Series
    /// </summary>
    public static Series operator /(Series left, Series right)
        => new(PolarsWrapper.SeriesDiv(left.Handle, right.Handle));
    /// <summary>
    /// Mod Series
    /// </summary>
    public static Series operator %(Series left, Series right)
        => new(PolarsWrapper.SeriesRem(left.Handle, right.Handle));
    /// <summary>
    /// Exponentiation Series
    /// </summary>
    public static Series operator ^ (Series left, Series right)
        => left.Pow(right);
    // ==========================================
    // Bitwise Operators (<<, >>)
    // ==========================================

    /// <summary>
    /// Bitwise left shift operation.
    /// </summary>
    public static Series operator <<(Series left, int right)
        => left.ApplyExpr(Polars.Col(left.Name) << right);

    /// <summary>
    /// Bitwise right shift operation.
    /// <para>
    /// For signed integers, this is arithmetic shift.
    /// For unsigned integers, this is logical shift.
    /// </para>
    /// </summary>
    public static Series operator >>(Series left, int right)
        => left.ApplyExpr(Polars.Col(left.Name) >> right);
    // ==========================================
    // Comparison Methods & Operators
    // ==========================================
    /// <summary>
    /// Compare whether two Series is equal
    /// </summary>
    public Series Eq(Series other) => new(PolarsWrapper.SeriesEq(Handle, other.Handle));
    /// <summary>
    /// Compare whether two Series is equal,This differs from the standard eq where null values are propagated.
    /// </summary>
    public Series EqMissing(Series other) => new(PolarsWrapper.SeriesEqMissing(Handle, other.Handle));
    /// <summary>
    /// Compare whether two Series is not equal
    /// </summary>
    public Series Neq(Series other) => new(PolarsWrapper.SeriesNeq(Handle, other.Handle));
    /// <summary>
    /// Compare whether two Series is not equal,This differs from the standard neq where null values are propagated.
    /// </summary>
    public Series NeqMissing(Series other) => new(PolarsWrapper.SeriesNeqMissing(Handle, other.Handle));
    public static Series operator == (Series left, Series right) => left.Eq(right);
    public static Series operator != (Series left, Series right) => left.Neq(right);
    /// <summary>
    /// Compare whether left series is greater than right series
    /// </summary>
    public static Series operator >(Series left, Series right)
        => new(PolarsWrapper.SeriesGt(left.Handle, right.Handle));
    /// <summary>
    /// Compare whether left series is less than right series
    /// </summary>
    public static Series operator <(Series left, Series right)
        => new(PolarsWrapper.SeriesLt(left.Handle, right.Handle));
    /// <summary>
    /// Compare whether left series is greater than or equal to right series
    /// </summary>
    public static Series operator >=(Series left, Series right)
        => new(PolarsWrapper.SeriesGtEq(left.Handle, right.Handle));
    /// <summary>
    /// Compare whether left series is less than or equal to right series
    /// </summary>
    public static Series operator <=(Series left, Series right)
        => new(PolarsWrapper.SeriesLtEq(left.Handle, right.Handle));

    /// <summary>
    /// Bitwise NOT operator
    /// </summary>
    public Series Not() => new(PolarsWrapper.SeriesNot(Handle));
    public static Series operator ~ (Series booleanSeries)
        => new(PolarsWrapper.SeriesNot(booleanSeries.Handle));
    public static Series operator ! (Series booleanSeries)
        => new(PolarsWrapper.SeriesNot(booleanSeries.Handle));

    /// <summary>
    /// Compare whether left series is greater than right series
    /// </summary>
    public Series Gt(Series other) => this > other;
    /// <summary>
    /// Compare whether left series is less than right series
    /// </summary>
    public Series Lt(Series other) => this < other;
    /// <summary>
    /// Compare whether left series is greater than or equal to right series
    /// </summary>
    public Series GtEq(Series other) => this >= other;
    /// <summary>
    /// Compare whether left series is less than or equal to right series
    /// </summary>
    public Series LtEq(Series other) => this <= other;
    // ==========================================
    // Scalar
    // ==========================================
    public static Series operator >(Series left, IntoExpr right)
    {
        using var rightSeries = Series.FromExpr(right.Consume());
        return left > rightSeries;
    }

    public static Series operator <(Series left, IntoExpr right)
    {
        using var rightSeries = Series.FromExpr(right.Consume());
        return left < rightSeries;
    }

    public static Series operator ==(Series left, IntoExpr right)
    {
        using var rightSeries = Series.FromExpr(right.Consume());
        return left == rightSeries;
    }

    public static Series operator !=(Series left, IntoExpr right)
    {
        using var rightSeries = Series.FromExpr(right.Consume());
        return left != rightSeries;
    }

    public static Series operator >=(Series left, IntoExpr right)
    {
        using var rightSeries = Series.FromExpr(right.Consume());
        return left >= rightSeries;
    }

    public static Series operator <=(Series left, IntoExpr right)
    {
        using var rightSeries = Series.FromExpr(right.Consume());
        return left <= rightSeries;
    }
    public static Series operator >(IntoExpr left, Series right) => right < left;

    public static Series operator <(IntoExpr left, Series right) => right > left;

    public static Series operator ==(IntoExpr left, Series right) => right == left;

    public static Series operator !=(IntoExpr left, Series right) => right != left;

    public static Series operator >=(IntoExpr left, Series right) => right <= left;

    public static Series operator <=(IntoExpr left, Series right) => right >= left;
    public static Series operator +(Series left, IntoExpr right)
    {
        using var rightSeries = Series.FromExpr(right.Consume());
        return left + rightSeries;
    }

    public static Series operator -(Series left, IntoExpr right)
    {
        using var rightSeries = Series.FromExpr(right.Consume());
        return left - rightSeries;
    }

    public static Series operator *(Series left, IntoExpr right)
    {
        using var rightSeries = Series.FromExpr(right.Consume());
        return left * rightSeries;
    }

    public static Series operator /(Series left, IntoExpr right)
    {
        using var rightSeries = Series.FromExpr(right.Consume());
        return left / rightSeries;
    }
    public static Series operator +(IntoExpr left, Series right) => right + left;

    public static Series operator -(IntoExpr left, Series right) => right - left;

    public static Series operator *(IntoExpr left, Series right) => right * left;

    public static Series operator /(IntoExpr left, Series right) => right / left;
}
