#pragma warning disable CS1591
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

public partial class Expr : IDisposable,IEquatable<Expr>
{
    /// <summary>
    /// Calculate the absolute value of the expression.
    /// </summary>
    public Expr Abs() => new(PolarsWrapper.Abs(CloneHandle()));

    /// <summary>
    /// Calculate the square root of the expression.
    /// </summary>
    public Expr Sqrt() => new(PolarsWrapper.Sqrt(CloneHandle()));

    /// <summary>
    /// Calculate the cube root of the expression.
    /// </summary>
    public Expr Cbrt() => new(PolarsWrapper.Cbrt(CloneHandle()));

    /// <summary>
    /// Calculate the power of the expression with a given exponent expression.
    /// </summary>
    public Expr Pow(Expr exponent) => new(PolarsWrapper.Pow(CloneHandle(), exponent.CloneHandle()));

    /// <summary>
    /// Calculate the power of the expression with a given numeric exponent.
    /// </summary>
    public Expr Pow(double exponent) => new(PolarsWrapper.Pow(CloneHandle(), PolarsWrapper.Lit(exponent)));
    /// <summary>
    /// Compute the dot/inner product between two expressions.
    /// <para>
    /// The dot product is the sum of the products of the corresponding entries of the two sequences of numbers.
    /// </para>
    /// </summary>
    /// <param name="other">The other expression to compute the dot product with.</param>
    /// <returns>A scalar expression representing the dot product result.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new 
    /// {
    ///     a = new[] { 1, 2, 3 },
    ///     b = new[] { 4, 5, 6 }
    /// });
    /// 
    /// // (1*4) + (2*5) + (3*6) = 4 + 10 + 18 = 32
    /// df.Select(Col("a").Dot(Col("b"))).Show();
    /// </code>
    /// </example>
    public Expr Dot(Expr other) => new(PolarsWrapper.Dot(CloneHandle(), other.CloneHandle()));
    /// <summary>
    /// Calculate the power of the Euler's number.
    /// </summary>
    public Expr Exp() => new(PolarsWrapper.Exp(CloneHandle()));

    /// <summary>
    /// Calculate the ln of Number 
    /// </summary>
    /// <param name="baseVal"></param>
    /// <returns></returns>
    public Expr Ln(double baseVal = Math.E) => new(PolarsWrapper.Log(CloneHandle(), baseVal));

    // ==========================================
    // Trigonometry
    // ==========================================

    /// <summary>Compute the element-wise sine.</summary>
    public Expr Sin() => new(PolarsWrapper.Sin(CloneHandle()));

    /// <summary>Compute the element-wise cosine.</summary>
    public Expr Cos() => new(PolarsWrapper.Cos(CloneHandle()));

    /// <summary>Compute the element-wise tangent.</summary>
    public Expr Tan() => new(PolarsWrapper.Tan(CloneHandle()));

    /// <summary>Compute the element-wise inverse sine.</summary>
    public Expr ArcSin() => new(PolarsWrapper.ArcSin(CloneHandle()));

    /// <summary>Compute the element-wise inverse cosine.</summary>
    public Expr ArcCos() => new(PolarsWrapper.ArcCos(CloneHandle()));

    /// <summary>Compute the element-wise inverse tangent.</summary>
    public Expr ArcTan() => new(PolarsWrapper.ArcTan(CloneHandle()));

    // Hyperbolic
    public Expr Sinh() => new(PolarsWrapper.Sinh(CloneHandle()));
    public Expr Cosh() => new(PolarsWrapper.Cosh(CloneHandle()));
    public Expr Tanh() => new(PolarsWrapper.Tanh(CloneHandle()));

    public Expr ArcSinh() => new(PolarsWrapper.ArcSinh(CloneHandle()));
    public Expr ArcCosh() => new(PolarsWrapper.ArcCosh(CloneHandle()));
    public Expr ArcTanh() => new(PolarsWrapper.ArcTanh(CloneHandle()));

    // ==========================================
    // Bitwise
    // ==========================================
    /// <summary>
    /// Evaluate the number of set bits.
    /// </summary>
    /// <returns></returns>
    public Expr BitwiseCountOnes() => new(PolarsWrapper.BitwiseCountOnes(CloneHandle()));
    /// <summary>
    /// Evaluate the number of unset bits.
    /// </summary>
    /// <returns></returns>
    public Expr BitwiseCountZeros() => new(PolarsWrapper.BitwiseCountZeros(CloneHandle()));
    /// <summary>
    /// Evaluate the number most-significant set bits before seeing an unset bit.
    /// </summary>
    /// <returns></returns>
    public Expr BitwiseLeadingOnes() => new(PolarsWrapper.BitwiseLeadingOnes(CloneHandle()));
    /// <summary>
    /// Evaluate the number most-significant unset bits before seeing a set bit.
    /// </summary>
    /// <returns></returns>
    public Expr BitwiseLeadingZeros() => new(PolarsWrapper.BitwiseLeadingZeros(CloneHandle()));
    /// <summary>
    /// Evaluate the number least-significant set bits before seeing an unset bit.
    /// </summary>
    /// <returns></returns>
    public Expr BitwiseTrailingOnes() => new(PolarsWrapper.BitwiseTrailingOnes(CloneHandle()));
    /// <summary>
    /// Evaluate the number least-significant unset bits before seeing a set bit.
    /// </summary>
    /// <returns></returns>
    public Expr BitwiseTrailingZeros() => new(PolarsWrapper.BitwiseTrailingZeros(CloneHandle()));
}