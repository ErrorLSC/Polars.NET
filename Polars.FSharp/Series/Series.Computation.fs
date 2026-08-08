namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module SeriesComputationOps = 
    type Series with
        /// <summary> Absolute value. </summary>
        member this.Abs() = this.ApplyExpr(Expr.Col(this.Name).Abs())
        /// <summary> Square root. </summary>
        member this.Sqrt() = this.ApplyExpr(Expr.Col(this.Name).Sqrt())

        /// <summary> Cube root. </summary>
        member this.Cbrt() = this.ApplyExpr(Expr.Col(this.Name).Cbrt())
        /// <summary> Power with scalar exponent. </summary>
        member this.Pow(exponent: double) = 
            this.ApplyExpr(Expr.Col(this.Name).Pow exponent)

        /// <summary> Power with integer exponent. </summary>
        member this.Pow(exponent: int) = 
            this.ApplyExpr(Expr.Col(this.Name).Pow exponent)
        /// <summary> Power with Series exponent. </summary>
        member this.Pow(exponent: Series) = 
            this.ApplyBinaryExpr(exponent, fun l r -> l.Pow r)
        /// <summary> Exponential (e^x). </summary>
        member this.Exp() = this.ApplyExpr(Expr.Col(this.Name).Exp())
        /// <summary> Logarithm with scalar base. </summary>
        member this.Log(baseVal: double) = 
            this.ApplyExpr(Expr.Col(this.Name).Log baseVal)
        /// <summary> Natural logarithm (ln). </summary>
        member this.Ln()= this.ApplyExpr(Expr.Col(this.Name).Ln())
        member this.Log10() = this.Log(10.0)
        member this.Log1p() = this.ApplyExpr(Expr.Col(this.Name).Log1p())
        member this.Entropy(?baseval,?normalize) =
            let ba = defaultArg baseval System.Math.E
            let no = defaultArg normalize true
            this.ExtractScalar<double>(fun () -> this.ApplyExpr(Expr.Col(this.Name).Entropy(ba,no)))
        member this.Hash(?seed,?seed1,?seed2,?seed3) =
            let s0 = defaultArg seed 0UL
            let s1 = defaultArg seed1 s0
            let s2 = defaultArg seed2 s0
            let s3 = defaultArg seed3 s0
            this.ApplyExpr(Expr.Col(this.Name).Hash(s0,s1,s2,s3))
        member this.Dot(other: Series) = 
            this.ApplyBinaryExpr(other, fun l r -> l.Dot r)
        member this.Dot<'T>(other:Series) =
            this.ExtractScalar<'T>(fun () -> this.Dot(other))
        /// <summary> Round to given decimals. </summary>
        member this.Round(decimals: uint,?mode: RoundMode) =
            this.ApplyExpr(Expr.Col(this.Name).Round(decimals,?mode=mode))
        member this.RoundSigFigs(digits) =
            this.ApplyExpr(Expr.Col(this.Name).RoundSigFigs(digits))
        member this.Truncate(?decimals) = 
            this.ApplyExpr(Expr.Col(this.Name).Truncate(?decimals=decimals))
        /// <summary> Element-wise sign. </summary>
        member this.Sign() = this.ApplyExpr(Expr.Col(this.Name).Sign())

        /// <summary> Round up to the nearest integer. </summary>
        member this.Ceil() = this.ApplyExpr(Expr.Col(this.Name).Ceil())
        /// <summary> Round down to the nearest integer. </summary>
        member this.Floor() = this.ApplyExpr(Expr.Col(this.Name).Floor())
        // ==========================================
        // Math: Trigonometry
        // ==========================================

        /// <summary> Compute the element-wise sine. </summary>
        member this.Sin() = this.ApplyExpr(Expr.Col(this.Name).Sin())

        /// <summary> Compute the element-wise cosine. </summary>
        member this.Cos() = this.ApplyExpr(Expr.Col(this.Name).Cos())

        /// <summary> Compute the element-wise tangent. </summary>
        member this.Tan() = this.ApplyExpr(Expr.Col(this.Name).Tan())
        /// <summary>
        /// Compute the element-wise value for the cotangent.
        /// </summary>
        member this.Cot() = this.ApplyExpr(Expr.Col(this.Name).Cot())

        /// <summary> Compute the element-wise inverse sine. </summary>
        member this.ArcSin() = this.ApplyExpr(Expr.Col(this.Name).ArcSin())

        /// <summary> Compute the element-wise inverse cosine. </summary>
        member this.ArcCos() = this.ApplyExpr(Expr.Col(this.Name).ArcCos())

        /// <summary> Compute the element-wise inverse tangent. </summary>
        member this.ArcTan() = this.ApplyExpr(Expr.Col(this.Name).ArcTan())

        // ==========================================
        // Math: Hyperbolic
        // ==========================================

        /// <summary> Compute the element-wise hyperbolic sine. </summary>
        member this.Sinh() = this.ApplyExpr(Expr.Col(this.Name).Sinh())

        /// <summary> Compute the element-wise hyperbolic cosine. </summary>
        member this.Cosh() = this.ApplyExpr(Expr.Col(this.Name).Cosh())

        /// <summary> Compute the element-wise hyperbolic tangent. </summary>
        member this.Tanh() = this.ApplyExpr(Expr.Col(this.Name).Tanh())

        /// <summary> Compute the element-wise inverse hyperbolic sine. </summary>
        member this.ArcSinh() = this.ApplyExpr(Expr.Col(this.Name).ArcSinh())

        /// <summary> Compute the element-wise inverse hyperbolic cosine. </summary>
        member this.ArcCosh() = this.ApplyExpr(Expr.Col(this.Name).ArcCosh())

        /// <summary> Compute the element-wise inverse hyperbolic tangent. </summary>
        member this.ArcTanh() = this.ApplyExpr(Expr.Col(this.Name).ArcTanh())
       // ==========================================
        // Cumulative Functions
        // ==========================================
        /// <summary>
        /// Get an array with the cumulative sum computed at every element.
        /// </summary>
        /// <param name="reverse">Reverse the operation.</param>
        /// <returns></returns>
        member this.CumSum(?reverse:bool) = 
            this.ApplyExpr(Expr.Col(this.Name).CumSum(?reverse=reverse))
        /// <summary>
        /// Get an array with the cumulative min computed at every element.
        /// </summary>
        /// <param name="reverse">Reverse the operation.</param>
        /// <returns></returns>
        member this.CumMin(?reverse:bool) = 
            this.ApplyExpr(Expr.Col(this.Name).CumMin(?reverse=reverse))
        /// <summary>
        /// Get an array with the cumulative max computed at every element.
        /// </summary>
        /// <param name="reverse">Reverse the operation.</param>
        /// <returns></returns>
        member this.CumMax(?reverse:bool) = 
            this.ApplyExpr(Expr.Col(this.Name).CumMax(?reverse=reverse))
        /// <summary>
        /// Get an array with the cumulative prod computed at every element.
        /// </summary>
        /// <param name="reverse">Reverse the operation.</param>
        /// <returns></returns>
        member this.CumProd(?reverse:bool) = 
            this.ApplyExpr(Expr.Col(this.Name).CumProd(?reverse=reverse))    
        /// <summary>
        /// Get an array with the cumulative count computed at every element.
        /// </summary>
        /// <param name="reverse">Reverse the operation.</param>
        /// <returns></returns>
        member this.CumCount(?reverse:bool) = 
            this.ApplyExpr(Expr.Col(this.Name).CumCount(?reverse=reverse)) 
        member this.CumulativeEval(expr:Expr,?minSamples) =
            this.ApplyExpr(Expr.Col(this.Name).CumulativeEval(expr,?minSamples=minSamples)) 
        // ==========================================
        // EWM Functions
        // ==========================================
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
        member this.EwmMean(alpha:float,?adjust:bool,?bias:bool,?minPeriods:int,?ignoreNulls:bool) =
            this.ApplyExpr(Expr.Col(this.Name).EwmMean(alpha=alpha,?adjust=adjust,?bias=bias,?minPeriods=minPeriods,?ignoreNulls=ignoreNulls))
        /// <summary>
        /// Compute exponentially-weighted moving standard deviation.
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
        /// <returns>A new expression representing the EWM standard deviation.</returns>
        member this.EwmStd(alpha:float,?adjust:bool,?bias:bool,?minPeriods:int,?ignoreNulls:bool) =
            this.ApplyExpr(Expr.Col(this.Name).EwmStd(alpha=alpha,?adjust=adjust,?bias=bias,?minPeriods=minPeriods,?ignoreNulls=ignoreNulls))
        /// <summary>
        /// Compute exponentially-weighted moving variance.
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
        /// <returns>A new expression representing the EWM variance.</returns>
        member this.EwmVar(alpha:float,?adjust:bool,?bias:bool,?minPeriods:int,?ignoreNulls:bool) =
            this.ApplyExpr(Expr.Col(this.Name).EwmVar(alpha=alpha,?adjust=adjust,?bias=bias,?minPeriods=minPeriods,?ignoreNulls=ignoreNulls))
        /// <summary>
        /// Compute exponentially-weighted moving sum.
        /// </summary>
        member this.EwmSum(alpha:float,?adjust:bool,?bias:bool,?minPeriods:int,?ignoreNulls:bool) =
            this.ApplyExpr(Expr.Col(this.Name).EwmSum(alpha=alpha,?adjust=adjust,?bias=bias,?minPeriods=minPeriods,?ignoreNulls=ignoreNulls))
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
        member this.EwmMeanBy(by:Expr,halfLife:string) =
            this.ApplyExpr(Expr.Col(this.Name).EwmMeanBy(by=by,halfLife=halfLife))
        /// <summary>
        /// Compute exponentially-weighted moving sum based on a temporal or index column.
        /// </summary>
        member this.EwmSumBy(by:Expr,halfLife:string) =
            this.ApplyExpr(Expr.Col(this.Name).EwmSumBy(by=by,halfLife=halfLife))
        member this.BitwiseCountOnes() = this.ApplyExpr(Expr.Col(this.Name).BitwiseCountOnes())
        member this.BitwiseCountZeros() = this.ApplyExpr(Expr.Col(this.Name).BitwiseCountZeros())
        member this.BitwiseLeadingOnes() = this.ApplyExpr(Expr.Col(this.Name).BitwiseLeadingOnes())
        member this.BitwiseLeadingZeros() = this.ApplyExpr(Expr.Col(this.Name).BitwiseLeadingZeros())
        member this.BitwiseTrailingOnes() = this.ApplyExpr(Expr.Col(this.Name).BitwiseTrailingOnes())
        member this.BitwiseTrailingZeros() = this.ApplyExpr(Expr.Col(this.Name).BitwiseTrailingZeros())
        
        /// <summary>
        /// Calculate the difference with a given period.
        /// </summary>
        member this.Diff(n: int64) = 
            this.ApplyExpr(Expr.Col(this.Name).Diff n)

        member this.Diff(n: int) = this.Diff(int64 n)
        
        /// <summary> Diff by 1. </summary>
        member this.Diff() = this.Diff(1L)
        /// <summary>
        /// Get unique values of this series.
        /// </summary>
        /// <param name="maintainOrder">Maintain order of data. This requires more work.</param>
        member this.Unique(?maintainOrder) =
            let mo = defaultArg maintainOrder false
            if mo = false then 
                new Series(PolarsWrapper.SeriesUnique(this.Handle))
            else
                new Series(PolarsWrapper.SeriesUniqueStable(this.Handle))
        member this.Hist(?bins: Expr, ?binCount: int, ?includeCategory: bool, ?includeBreakPoint: bool) =
            let histExpr = Expr.Col(this.Name).Hist(?bins = bins, ?binCount = binCount, ?includeCategory = includeCategory, ?includeBreakPoint = includeBreakPoint)
            
            use res = this.ApplyExpr(histExpr)
            
            match includeCategory, includeBreakPoint with
            | Some true, _ 
            | _, Some true -> res.Unnest()
            | _ -> res.ToFrame()
        /// <summary>
        /// Find the index of the first occurrence of a specific value.
        /// </summary>
        /// <param name="element">The element expression to search for.</param>
        member this.IndexOf(element: Expr) =
            this.ExtractScalar<int>(fun () -> this.ApplyExpr(Expr.Col(this.Name).IndexOf element))

        /// <summary>
        /// Find indices where elements should be inserted to maintain order (Binary Search).
        /// </summary>
        /// <param name="element">The element expression to insert/search.</param>
        /// <param name="side">The insertion side (Any, Left, Right). Default is Any.</param>
        /// <param name="descending">Whether the target column is sorted in descending order. Default is false.</param>
        member this.SearchSorted(element: Expr, ?side: SearchSortedSide, ?descending: bool) =
            this.ExtractScalar<int>(fun () -> this.ApplyExpr(Expr.Col(this.Name).SearchSorted(element, ?side = side, ?descending = descending)))
        /// <summary>
        /// Get the Skew.
        /// </summary>
        /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
        member this.Skew(?bias:bool) = 
            this.ExtractScalar<double>(fun () -> this.ApplyExpr(Expr.Col(this.Name).Skew(?bias=bias)))
        /// <summary>
        /// Get the Kurtosis.
        /// </summary>
        /// <param name="fisher">If True, Fisher’s definition is used (normal ==> 0.0). If False, Pearson’s definition is used (normal ==> 3.0).</param>
        /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
        member this.Kurtosis(?fisher:bool,?bias:bool) = 
            this.ExtractScalar<double>(fun () -> this.ApplyExpr(Expr.Col(this.Name).Kurtosis(?fisher=fisher,?bias=bias)))
        /// <summary>
        /// Get index values where Boolean Series evaluate True.
        /// </summary>
        /// <returns>Series of data type UInt32.</returns>
        member this.ArgTrue() =
            this.ApplyExpr(Expr.Col(this.Name).ArgTrue())