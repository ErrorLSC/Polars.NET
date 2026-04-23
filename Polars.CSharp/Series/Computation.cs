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
    /// <inheritdoc cref="Expr.Log(Expr)"/> 
    public Series Log(Expr baseVal) => ApplyExpr(Pl.Col(Name).Log(baseVal));
    public Series Ln() => Log(Math.E);
    public Series Log() => Ln();
    /// <inheritdoc cref="Expr.Log10"/> 
    public Series Log10() => Log(10.0);
    /// <inheritdoc cref="Expr.Log1p"/> 
    public Series Log1p() => ApplyExpr(Pl.Col(Name).Log1p());
    /// <inheritdoc cref="Expr.Entropy"/> 
    public double? Entropy(double baseVal=Math.E,bool normalize=true) => ExtractScalar<double>(Pl.Col(Name).Entropy(baseVal,normalize));
    /// <inheritdoc cref="Expr.Hash"/> 
    public Series Hash(ulong seed=0, ulong? seed1=null,ulong? seed2=null,ulong? seed3=null)
        => ApplyExpr(Pl.Col(Name).Hash(seed,seed1,seed2,seed3));
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
    /// <summary>
    /// Compute the element-wise value for the cotangent.
    /// </summary>
    /// <returns></returns>
    public Series Cot() => ApplyExpr(Pl.Col(Name).Cot());

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
    /// <inheritdoc cref="Expr.CumulativeEval"/>
    public Series CumulativeEval(Expr expr,int minSamples=1)
        => ApplyExpr(Pl.Col(Name).CumulativeEval(expr,minSamples));
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
    /// <summary>
    /// Calculate the difference with the previous value (n-th lag).
    /// </summary>
    public Series Diff(long n = 1) => ApplyExpr(Pl.Col(Name).Diff(n));
    /// <summary>
    /// Get the unique elements of this Series.
    /// </summary>
    public Series Unique() => new(PolarsWrapper.SeriesUnique(Handle));

    /// <summary>
    /// Get the unique elements of this Series, maintaining the order of appearance.
    /// </summary>
    public Series UniqueStable() => new(PolarsWrapper.SeriesUniqueStable(Handle));
    /// <inheritdoc cref="Expr.Hist"/>
    public DataFrame Hist(IntoExpr? bins=null,int? binCount=null,bool includeCategory=true,bool includeBreakPoint=true)
    {
        Series res = ApplyExpr(Pl.Col(Name).Hist(bins,binCount,includeCategory,includeBreakPoint));
        return (includeCategory | includeBreakPoint)? res.Unnest() : res.ToFrame();
    }
    /// <inheritdoc cref="Expr.IndexOf(IntoExpr)"/>
    public int? IndexOf(IntoExpr element)
        => ExtractScalar<int>(Pl.Col(Name).IndexOf(element));

    /// <inheritdoc cref="Expr.SearchSorted(IntoExpr, SearchSortedSide, bool)"/>
    public Series SearchSorted(IntoExpr? element, SearchSortedSide side = SearchSortedSide.Any, bool descending = false)
    {
        Expr eleExpr = element?.Consume() ?? Pl.LitNull();
        return ApplyExpr(Pl.Col(Name).SearchSorted(eleExpr, side, descending));
    }
    /// <inheritdoc cref="Expr.SearchSorted(IntoExpr, SearchSortedSide, bool)"/>
    public Series SearchSorted<T>(IEnumerable<T> element, SearchSortedSide side = SearchSortedSide.Any, bool descending = false)
        => ApplyExpr(Pl.Col(Name).SearchSorted(element, side, descending));
    /// <summary>
    /// Searches for a single scalar value and returns its insertion index.
    /// </summary>
    public int SearchSortedIndex(object? scalar, SearchSortedSide side = SearchSortedSide.Any, bool descending = false)
    {
        Expr eleExpr = scalar is null ? Pl.LitNull(): Expr.MakeLit(scalar) ;
        
        using var series = ApplyExpr(Pl.Col(Name).SearchSorted(eleExpr, side, descending));
        
        return (int)series.Cast<int>()[0]!; 
    }
    /// <summary>
    /// <inheritdoc cref="Expr.Skew(bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Skew(bool)" path="/param"/>
    public double? Skew(bool bias = true) => ExtractScalar<double>(Pl.Col(Name).Skew(bias));

    /// <summary>
    /// <inheritdoc cref="Expr.Kurtosis(bool, bool)" path="/summary"/>
    /// </summary>
    /// <inheritdoc cref="Expr.Kurtosis(bool, bool)" path="/param"/>
    public double? Kurtosis(bool fisher = true, bool bias = true) 
        => ExtractScalar<double>(Pl.Col(Name).Kurtosis(fisher, bias));
    /// <summary>
    /// Get index values where Boolean Series evaluate True.
    /// </summary>
    /// <returns>Series of data type UInt32.</returns>
    public Series ArgTrue() => Pl.ArgWhereAsSeries(this);
}