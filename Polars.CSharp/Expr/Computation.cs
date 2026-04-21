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
    /// <summary>
    /// Compute the element-wise value for the cotangent.
    /// </summary>
    public Expr Cot() => new(PolarsWrapper.Cot(CloneHandle()));

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
    /// <summary>
    /// Convert from degrees to radians.
    /// </summary>
    /// <returns></returns>
    public Expr Radians() => new(PolarsWrapper.Radians(CloneHandle()));
    /// <summary>
    /// Convert from radians to degrees.
    /// </summary>
    /// <returns></returns>
    public Expr Degrees() => new(PolarsWrapper.Degrees(CloneHandle()));

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
    // ==========================================
    // Cumulative Functions
    // ==========================================
    /// <summary>
    /// Get an array with the cumulative sum computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumSum(bool reverse = false) => new(PolarsWrapper.CumSum(CloneHandle(), reverse));
    /// <summary>
    /// Get an array with the cumulative max computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumMax(bool reverse = false) => new(PolarsWrapper.CumMax(CloneHandle(), reverse));
    /// <summary>
    /// Get an array with the cumulative min computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumMin(bool reverse = false) => new(PolarsWrapper.CumMin(CloneHandle(), reverse));
    /// <summary>
    /// Get an array with the cumulative prod computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumProd(bool reverse = false) => new(PolarsWrapper.CumProd(CloneHandle(), reverse));
    /// <summary>
    /// Get an array with the cumulative count computed at every element.
    /// </summary>
    /// <param name="reverse">Reverse the operation.</param>
    /// <returns></returns>
    public Expr CumCount(bool reverse = false) => new(PolarsWrapper.CumCount(CloneHandle(), reverse));
    /// <summary>
    /// Run an expression over a sliding window that increases 1 slot every iteration.
    /// This can be really slow as it can have O(n^2) complexity. Don’t use this for operations that visit all elements.
    /// </summary>
    /// <param name="expr">Expression to evaluate</param>
    /// <param name="minSamples">Number of valid values there should be in the window before the expression is evaluated. valid values = length - null_count</param>
    /// <returns></returns>
    public Expr CumulativeEval(Expr expr,int minSamples=1) => new(PolarsWrapper.CumulativeEval(CloneHandle(), expr.CloneHandle(),minSamples));
    /// <summary>
    /// Calculate the difference with the previous value (n-th lag).
    /// </summary>
    public Expr Diff(IntoExprColumn n,NullBehavior nullBehavior=NullBehavior.Ignore) => new(PolarsWrapper.Diff(CloneHandle(), n.Consume().CloneHandle(),nullBehavior.ToNative()));
    /// <summary>
    /// Calculate the difference with the previous value (1-st lag).
    /// </summary>
    public Expr Diff(NullBehavior nullBehavior = NullBehavior.Ignore) => Diff(1, nullBehavior);
}