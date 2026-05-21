using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

public partial class Expr : IDisposable,IEquatable<Expr>
{
    /// <summary>
    /// Check whether the expression contains one or more null values.
    /// </summary>
    public Expr HasNulls() => NullCount() > 0;
    /// <summary>
    /// Check if the value is between lower and upper bounds (inclusive).
    /// </summary>
    public Expr IsBetween(IntoExprColumn lower, IntoExprColumn upper)
        => new(PolarsWrapper.IsBetween(CloneHandle(), lower.Consume().CloneHandle(), upper.Consume().CloneHandle()));
    /// <summary>
    /// Check if elements of this expression are present in the other collection.
    /// </summary>
    /// <param name="other">Series or sequence of primitive type.</param>
    /// <param name="nullsEqual">If True, treat null as a distinct value. Null values will not propagate.</param>
    /// <returns></returns>
    public Expr IsIn(Expr other, bool nullsEqual = false)
        => new(PolarsWrapper.IsIn(CloneHandle(),other.CloneHandle(),nullsEqual));
    /// <inheritdoc cref="Expr.IsIn(Expr, bool)"/>
    public Expr IsIn(Series other, bool nullsEqual = false)
        => IsIn(Pl.Lit(other),nullsEqual);
    /// <inheritdoc cref="Expr.IsIn(Expr, bool)"/>
    public Expr IsIn<T>(IEnumerable<T> other,bool nullsEqual = false)
        => IsIn(Pl.Lit(other).Implode(),nullsEqual);
    /// <summary>
    /// Create a boolean expression indicating whether the value is unique.
    /// </summary>
    public Expr IsUnique() => new(PolarsWrapper.ExprIsUnique(CloneHandle()));
    /// <summary>
    /// Create a boolean expression indicating whether the value is duplicated.
    /// </summary>
    public Expr IsDuplicated() => new(PolarsWrapper.ExprIsDuplicated(CloneHandle()));
    /// <summary>
    /// Evaluate whether the expression is null.
    /// </summary>
    public Expr IsNull() => new(PolarsWrapper.IsNull(CloneHandle()));
    /// <summary>
    /// Evaluate whether the expression is not null.
    /// </summary>
    public Expr IsNotNull() => new(PolarsWrapper.IsNotNull(CloneHandle()));
    /// <summary>
    /// Returns a boolean Series indicating which values are NaN.
    /// </summary>
    public Expr IsNan() => new(PolarsWrapper.ExprIsNan(CloneHandle()));
    /// <summary>
    /// Returns a boolean Series indicating which values are not NaN.
    /// </summary>
    public Expr IsNotNan() => new(PolarsWrapper.ExprIsNotNan(CloneHandle()));
    /// <summary>
    /// Returns a boolean Series indicating which values are infinite.
    /// </summary>
    /// <returns>Expression of data type Boolean.</returns>
    public Expr IsInfinite() => new(PolarsWrapper.ExprIsInfinite(CloneHandle()));
    /// <summary>
    /// Returns a boolean Series indicating which values are finite.
    /// </summary>
    /// <returns></returns>
    public Expr IsFinite() => new(PolarsWrapper.ExprIsFinite(CloneHandle()));
    /// <summary>
    /// Return a boolean mask indicating the first occurrence of each distinct value.
    /// </summary>
    /// <returns>Expression of data type Boolean.</returns>
    public Expr IsFirstDistinct() => new(PolarsWrapper.ExprIsFirstDistinct(CloneHandle()));
    /// <summary>
    /// Return a boolean mask indicating the last occurrence of each distinct value.
    /// </summary>
    /// <returns>Expression of data type Boolean.</returns>
    public Expr IsLastDistinct() => new(PolarsWrapper.ExprIsLastDistinct(CloneHandle()));
    /// <summary>
    /// Check if this expression is close, i.e. almost equal, to the other expression.
    /// </summary>
    /// <param name="other">A literal or expression value to compare with.</param>
    /// <param name="absTol"> Absolute tolerance. This is the maximum allowed absolute difference betweentwo values. Must be non-negative.</param>
    /// <param name="relTol">Relative tolerance. This is the maximum allowed difference between two values, relative to the larger absolute value. Must be non-negative.</param>
    /// <param name="nansEqual">Whether NaN values should be considered equal.</param>
    /// <returns>Expression/Series of data type Boolean.</returns>
    public Expr IsClose(IntoExprColumn other,double absTol=0.0,double relTol=1e-09,bool nansEqual=false)
        => new(PolarsWrapper.ExprIsClose(CloneHandle(),other.Consume().Handle,absTol,relTol,nansEqual));
}