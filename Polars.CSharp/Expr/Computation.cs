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
    /// Computes the entropy.
    /// Uses the formula -sum(pk * log(pk)) where pk are discrete probabilities.
    /// </summary>
    /// <param name="baseVal">Given base, defaults to e</param>
    /// <param name="normalize">Normalize pk if it doesn’t sum to 1.</param>
    /// <returns></returns>
    public Expr Entropy(double baseVal=Math.E, bool normalize=true) => new(PolarsWrapper.Entropy(CloneHandle(),baseVal,normalize));
    /// <summary>
    /// Hash the elements in the selection.The hash value is of type UInt64.
    /// </summary>
    /// <param name="seed">Random seed parameter. Defaults to 0.</param>
    /// <param name="seed1">Random seed parameter. Defaults to seed if not set.</param>
    /// <param name="seed2">Random seed parameter. Defaults to seed if not set.</param>
    /// <param name="seed3">Random seed parameter. Defaults to seed if not set.</param>
    /// <returns></returns>
    public Expr Hash(ulong seed=0,ulong? seed1=null,ulong? seed2=null,ulong? seed3=null)
        => new(PolarsWrapper.ExprHash(CloneHandle(),seed,seed1,seed2,seed3));
    /// <summary>
    /// Compute the logarithm to a given base,defaults to e.
    /// </summary>
    /// <param name="baseVal">Given base, defaults to e</param>
    public Expr Log(Expr baseVal) => new(PolarsWrapper.Log(CloneHandle(), baseVal.CloneHandle()));
    /// <inheritdoc cref="Expr.Log(Expr)"/> 
    public Expr Log() => Ln();
    /// <summary>
    /// Compute the base 10 logarithm of the input array, element-wise.
    /// </summary>
    public Expr Log10() => Log(10.0);
    public Expr Ln() => Log(Math.E);
    /// <summary>
    /// Compute the natural logarithm of each element plus one.This computes log(1 + x) but is more numerically stable for x close to zero.
    /// </summary>
    public Expr Log1p() => new(PolarsWrapper.Log1p(CloneHandle()));

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
    /// <summary>
    /// Compute exponentially-weighted moving average.
    /// </summary>
    /// <param name="alpha">
    /// Specify smoothing factor alpha directly. 
    /// <para>Constraint: <c>0 &lt; alpha &lt;= 1</c></para>
    /// </param>
    /// <param name="adjust">
    /// If <c>true</c>, divide by decaying adjustment factor in beginning periods to account for imbalance in relative weightings (viewing data as finite history). 
    /// If <c>false</c>, assume infinite history.
    /// </param>
    /// <param name="bias">
    /// If <c>true</c>, use a biased estimator (Standard deviation uses <c>N</c> in denominator). 
    /// If <c>false</c>, use an unbiased estimator (Standard deviation uses <c>N-1</c>).
    /// <para>Note: This is primarily relevant for Variance/StdDev. For Mean, it typically defaults to true.</para>
    /// </param>
    /// <param name="minPeriods">Minimum number of observations in window required to have a value (otherwise result is null).</param>
    /// <param name="ignoreNulls">Ignore missing values when calculating weights.</param>
    /// <returns>A new expression representing the EWM mean.</returns>
    public Expr EwmMean(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => new(PolarsWrapper.EwmMean(CloneHandle(), alpha, adjust, bias, minPeriods, ignoreNulls));
    /// <summary>
    /// Compute exponentially-weighted moving standard deviation.
    /// </summary>
    /// <inheritdoc cref="EwmMean"/>
    /// <returns>A new expression representing the EWM standard deviation.</returns>
    public Expr EwmStd(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => new(PolarsWrapper.EwmStd(CloneHandle(), alpha, adjust, bias, minPeriods, ignoreNulls));
    /// <summary>
    /// Compute exponentially-weighted moving variance.
    /// </summary>
    /// <inheritdoc cref="EwmMean"/>
    /// <returns>A new expression representing the EWM variance.</returns>
    public Expr EwmVar(double alpha, bool adjust = true, bool bias = true, int minPeriods = 1, bool ignoreNulls = false)
        => new(PolarsWrapper.EwmVar(CloneHandle(), alpha, adjust, bias, minPeriods, ignoreNulls));
    /// <summary>
    /// Compute exponentially-weighted moving average based on a temporal or index column.
    /// </summary>
    /// <param name="by">
    /// The column used to determine the distance between observations.
    /// <para>Supported data types: <c>Date</c>, <c>DateTime</c>, <c>UInt64</c>, <c>UInt32</c>, <c>Int64</c>, or <c>Int32</c>.</para>
    /// </param>
    /// <param name="halfLife">
    /// The unit over which an observation decays to half its value.
    /// <para>Supported string formats:</para>
    /// <list type="bullet">
    ///     <item><term>Time units</term><description><c>ns</c> (nanosecond), <c>us</c> (microsecond), <c>ms</c> (millisecond), <c>s</c> (second), <c>m</c> (minute), <c>h</c> (hour), <c>d</c> (day), <c>w</c> (week).</description></item>
    ///     <item><term>Index units</term><description><c>i</c> (index count). Example: <c>"2i"</c> means decay by half every 2 index steps.</description></item>
    ///     <item><term>Compound</term><description>Example: <c>"3d12h4m25s"</c>.</description></item>
    /// </list>
    /// <para>
    /// <b>Warning:</b> <paramref name="halfLife"/> is treated as a constant duration. 
    /// Calendar durations such as months (<c>mo</c>) or years (<c>y</c>) are <b>NOT</b> supported because they vary in length. 
    /// Please express such durations in hours (e.g. use <c>'730h'</c> instead of <c>'1mo'</c>).
    /// </para>
    /// </param>
    /// <returns>A new expression representing the time/index-based EWM mean.</returns>
    public Expr EwmMeanBy(Expr by, string halfLife)
        => new(PolarsWrapper.EwmMeanBy(
            CloneHandle(),
            by.CloneHandle(),
            halfLife
        ));

    /// <summary>
    /// Get unique values.
    /// </summary>
    public Expr Unique() => new(PolarsWrapper.ExprUnique(CloneHandle()));
    /// <summary>
    /// Get unique values, maintaining order.
    /// </summary>
    public Expr UniqueStable() => new(PolarsWrapper.ExprUniqueStable(CloneHandle()));
    /// <summary>
    /// Return a count of the unique values in the order of appearance.
    /// This method differs from value_counts in that it does not return the values, only the counts and might be faster
    /// </summary>
    /// <returns></returns>
    public Expr UniqueCounts() => new(PolarsWrapper.ExprUniqueCounts(CloneHandle()));
    /// <summary>
    /// Count the occurrence of unique values.
    /// </summary>
    /// <param name="sort">Sort the output by count, in descending order. If set to False (default), the order is non-deterministic.</param>
    /// <param name="parallel">Execute the computation in parallel.This option should likely not be enabled in a group_by context, as the computation will already be parallelized per group.</param>
    /// <param name="name">Give the resulting count column a specific name; if normalize is True this defaults to “proportion”, otherwise defaults to “count”.</param>
    /// <param name="normalize">If True, the count is returned as the relative frequency of unique values normalized to 1.0.</param>
    /// <returns>Expression of type Struct, mapping unique values to their count (or proportion).</returns>
    public Expr ValueCounts(bool sort=false,bool parallel=false,string? name=null,bool normalize=false) 
        => new(PolarsWrapper.ValueCounts(CloneHandle(),sort,parallel,name,normalize));
    /// <summary>
    /// Bin values into buckets and count their occurrences.
    /// </summary>
    /// <param name="bins">Bin edges. If None given, we determine the edges based on the data.</param>
    /// <param name="binCount">If bins is not provided, bin_count uniform bins are created that fully encompass the data.</param>
    /// <param name="includeCategory">Include a column that indicates the upper breakpoint.</param>
    /// <param name="includeBreakPoint">Include a column that shows the intervals as categories.</param>
    /// <returns></returns>
    public Expr Hist(IntoExpr? bins=null,int? binCount=null,bool includeCategory=false,bool includeBreakPoint=false)
    {
        ExprHandle? binsHandle = bins?.Consume().Handle;
        return new(PolarsWrapper.ExprHist(CloneHandle(),binsHandle,binCount,includeCategory,includeBreakPoint));
    }

}