#pragma warning disable CS1591 
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

public partial class Series : IDisposable,IPolarsSeries
{
    /// <summary>
    /// Calculate absolute value.
    /// <para>Implemented via Expr composition.</para>
    /// </summary>
    public Series Abs() => ApplyExpr(Pl.Col(Name).Abs());
    /// <summary>
    /// Calculate square value.
    /// <para>Implemented via Expr composition.</para>
    /// </summary>
    public Series Sqrt() => ApplyExpr(Pl.Col(Name).Sqrt());
    /// <summary>
    /// Calculate the cube root of the expression.
    /// </summary>
    public Series Cbrt() => ApplyExpr(Pl.Col(Name).Cbrt());
    /// <summary>
    /// Calculate exponent value.
    /// <para>Implemented via Expr composition.</para>
    /// </summary>
    public Series Pow(double exponent) => ApplyExpr(Pl.Col(Name).Pow(exponent));
    /// <summary>
    /// Calculate the power of the Euler's number.
    /// </summary>
    public Series Exp() =>  ApplyExpr(Pl.Col(Name).Exp());
    /// <summary>
    /// Calculate the ln of Number 
    /// </summary>
    /// <param name="baseVal"></param>
    /// <returns></returns>
    public Series Ln(double baseVal = Math.E) => ApplyExpr(Pl.Col(Name).Ln(baseVal));
    // ==========================================
    // Linear Algebra (Dot Product)
    // ==========================================

    /// <summary>
    /// Compute the dot/inner product between two Series.
    /// <para>
    /// The behavior is equivalent to `(this * other).Sum()`.
    /// </para>
    /// </summary>
    /// <param name="other">The other Series to compute the dot product with.</param>
    /// <returns>A Series of length 1 containing the result.</returns>
    public Series Dot(Series other)
        => ApplyBinaryExpr(other, (left, right) => left.Dot(right));
    /// <summary>
    /// Compute the dot/inner product and return the scalar value directly.
    /// </summary>
    /// <typeparam name="T">The type of the result (e.g. double, long).</typeparam>
    /// <param name="other">The other Series.</param>
    /// <returns>The dot product value.</returns>
    public T? Dot<T>(Series other) => Dot(other).GetValue<T>(0);
    /// <summary>
    /// Round the number
    /// </summary>
    /// <param name="decimals"></param>
    /// <returns></returns>
    public Series Round(uint decimals) => ApplyExpr(Pl.Col(Name).Round(decimals));
    /// <summary>Compute the element-wise sign (-1, 0, 1).</summary>
    public Series Sign() => ApplyExpr(Pl.Col(Name).Sign());

    /// <summary>Rounds up to the nearest integer.</summary>
    public Series Ceil() => ApplyExpr(Pl.Col(Name).Ceil());

    /// <summary>Rounds down to the nearest integer.</summary>
    public Series Floor() => ApplyExpr(Pl.Col(Name).Floor());
    // ==========================================
    // Trigonometry
    // ==========================================

    /// <summary>Compute the element-wise sine.</summary>
    public Series Sin() => ApplyExpr(Pl.Col(Name).Sin());

    /// <summary>Compute the element-wise cosine.</summary>
    public Series Cos() => ApplyExpr(Pl.Col(Name).Cos());

    /// <summary>Compute the element-wise tangent.</summary>
    public Series Tan() => ApplyExpr(Pl.Col(Name).Tan());

    /// <summary>Compute the element-wise inverse sine.</summary>
    public Series ArcSin() => ApplyExpr(Pl.Col(Name).ArcSin());

    /// <summary>Compute the element-wise inverse cosine.</summary>
    public Series ArcCos() => ApplyExpr(Pl.Col(Name).ArcCos());

    /// <summary>Compute the element-wise inverse tangent.</summary>
    public Series ArcTan() => ApplyExpr(Pl.Col(Name).ArcTan());

    // Hyperbolic
    /// <summary>
    /// Compute the element-wise hyperbolic sine.
    /// </summary>
    public Series Sinh() => ApplyExpr(Pl.Col(Name).Sinh());

    /// <summary>
    /// Compute the element-wise hyperbolic cosine.
    /// </summary>
    public Series Cosh() => ApplyExpr(Pl.Col(Name).Cosh());

    /// <summary>
    /// Compute the element-wise hyperbolic tangent.
    /// </summary>
    public Series Tanh() => ApplyExpr(Pl.Col(Name).Tanh());

    /// <summary>
    /// Compute the element-wise inverse hyperbolic sine.
    /// </summary>
    public Series ArcSinh() => ApplyExpr(Pl.Col(Name).ArcSinh());

    /// <summary>
    /// Compute the element-wise inverse hyperbolic cosine.
    /// </summary>
    public Series ArcCosh() => ApplyExpr(Pl.Col(Name).ArcCosh());

    /// <summary>
    /// Compute the element-wise inverse hyperbolic tangent.
    /// </summary>
    public Series ArcTanh() => ApplyExpr(Pl.Col(Name).ArcTanh());
    // ==========================================
    // Cumulative Functions
    // ==========================================
    /// <summary>
    /// <inheritdoc cref="Expr.CumSum(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.CumSum(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the cumulative sum.</returns>
    public Series CumSum(bool reverse = false) 
        => ApplyExpr(Pl.Col(Name).CumSum(reverse));

    /// <summary>
    /// <inheritdoc cref="Expr.CumMax(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.CumMax(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the cumulative maximum.</returns>
    public Series CumMax(bool reverse = false) 
        => ApplyExpr(Pl.Col(Name).CumMax(reverse));

    /// <summary>
    /// <inheritdoc cref="Expr.CumMin(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.CumMin(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the cumulative minimum.</returns>
    public Series CumMin(bool reverse = false) 
        => ApplyExpr(Pl.Col(Name).CumMin(reverse));

    /// <summary>
    /// <inheritdoc cref="Expr.CumProd(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.CumProd(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the cumulative product.</returns>
    public Series CumProd(bool reverse = false) 
        => ApplyExpr(Pl.Col(Name).CumProd(reverse));

    /// <summary>
    /// <inheritdoc cref="Expr.CumCount(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.CumCount(bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the cumulative count.</returns>
    public Series CumCount(bool reverse = false) 
        => ApplyExpr(Pl.Col(Name).CumCount(reverse));
    // ==========================================
    // EWM Functions
    // ==========================================
    /// <summary>
    /// <inheritdoc cref="Expr.EwmMean(double, bool, bool, int, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.EwmMean(double, bool, bool, int, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the EWM mean.</returns>
    public Series EwmMean(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => ApplyExpr(Pl.Col(Name).EwmMean(alpha, adjust, bias, minPeriods, ignoreNulls));

    /// <summary>
    /// <inheritdoc cref="Expr.EwmStd(double, bool, bool, int, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.EwmStd(double, bool, bool, int, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the EWM standard deviation.</returns>
    public Series EwmStd(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => ApplyExpr(Pl.Col(Name).EwmStd(alpha, adjust, bias, minPeriods, ignoreNulls));

    /// <summary>
    /// <inheritdoc cref="Expr.EwmVar(double, bool, bool, int, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.EwmVar(double, bool, bool, int, bool)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the EWM variance.</returns>
    public Series EwmVar(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => ApplyExpr(Pl.Col(Name).EwmVar(alpha, adjust, bias, minPeriods, ignoreNulls));
    
    // -------------------------------------------------------------------------
    // EWM By (Time/Index based)
    // -------------------------------------------------------------------------

    /// <summary>
    /// <inheritdoc cref="Expr.EwmMeanBy(Expr, string)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.EwmMeanBy(Expr, string)" path="/param"/>
    /// <returns>A new <see cref="Series"/> with the time/index-based EWM mean.</returns>
    public Series EwmMeanBy(Expr by, string halfLife)
        => ApplyExpr(Pl.Col(Name).EwmMeanBy(by, halfLife));
    // -------------------------------------------------------------------------
    // BitWise
    // -------------------------------------------------------------------------
    /// <inheritdoc cref="Expr.BitwiseCountOnes"/>
    public Series BitwiseCountOnes() => ApplyExpr(Pl.Col(Name).BitwiseCountOnes());
    /// <inheritdoc cref="Expr.BitwiseCountZeros"/>
    public Series BitwiseCountZeros() => ApplyExpr(Pl.Col(Name).BitwiseCountZeros());
    /// <inheritdoc cref="Expr.BitwiseLeadingOnes"/>
    public Series BitwiseLeadingOnes() => ApplyExpr(Pl.Col(Name).BitwiseLeadingOnes());
    /// <inheritdoc cref="Expr.BitwiseLeadingZeros"/>
    public Series BitwiseLeadingZeros() => ApplyExpr(Pl.Col(Name).BitwiseLeadingZeros());
    /// <inheritdoc cref="Expr.BitwiseTrailingOnes"/>
    public Series BitwiseTrailingOnes() => ApplyExpr(Pl.Col(Name).BitwiseTrailingOnes());
    /// <inheritdoc cref="Expr.BitwiseTrailingZeros"/>
    public Series BitwiseTrailingZeros() => ApplyExpr(Pl.Col(Name).BitwiseTrailingZeros());
    
}