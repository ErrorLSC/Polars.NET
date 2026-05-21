namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module ExprComputaion =
    type Expr with
        /// <summary>
        /// Calculate the absolute value of the expression.
        /// </summary>
        member this.Abs() = new Expr(PolarsWrapper.Abs (this.CloneHandle()))
        /// <summary>
        /// Calculate the square root of the expression.
        /// </summary>
        member this.Sqrt() = new Expr(PolarsWrapper.Sqrt(this.CloneHandle()))
        /// <summary>
        /// Calculate the cube root of the expression.
        /// </summary>
        member this.Cbrt() = new Expr(PolarsWrapper.Cbrt(this.CloneHandle()))
        /// <summary>
        /// Calculate the power of the Euler's number.
        /// </summary>
        member this.Exp() = new Expr(PolarsWrapper.Exp(this.CloneHandle()))
        /// <summary>
        /// Compute the dot/inner product between two expressions.
        /// <para>
        member this.Dot(other:Expr) = new Expr(PolarsWrapper.Dot(this.CloneHandle(),other.CloneHandle()))
        /// <summary>
        /// Computes the entropy.
        /// Uses the formula -sum(pk * log(pk)) where pk are discrete probabilities.
        /// </summary>
        /// <param name="baseVal">Given base, defaults to e</param>
        /// <param name="normalize">Normalize pk if it doesn’t sum to 1.</param>
        member this.Entropy(?baseval,?normalize) =
            let bv = defaultArg baseval System.Math.E
            let nor = defaultArg normalize true
            new Expr(PolarsWrapper.Entropy(this.CloneHandle(),bv,nor))
        /// <summary>
        /// Hash the elements in the selection.The hash value is of type UInt64.
        /// </summary>
        /// <param name="seed">Random seed parameter. Defaults to 0.</param>
        /// <param name="seed1">Random seed parameter. Defaults to seed if not set.</param>
        /// <param name="seed2">Random seed parameter. Defaults to seed if not set.</param>
        /// <param name="seed3">Random seed parameter. Defaults to seed if not set.</param>
        member this.Hash(?seed,?seed1,?seed2,?seed3) =
            let s0 = defaultArg seed 0UL
            let s1 = defaultArg seed1 s0
            let s2 = defaultArg seed2 s0
            let s3 = defaultArg seed3 s0
            new Expr(PolarsWrapper.ExprHash(this.CloneHandle(),s0,s1,s2,s3))
        /// <summary> Calculate the logarithm with the given base. </summary>
        member this.Log(baseVal: double) = 
            new Expr(PolarsWrapper.Log(this.CloneHandle(), PolarsWrapper.Lit baseVal))
        member this.Log() = this.Ln()
        member this.Log(baseExpr: Expr) = this.Ln() / baseExpr.Ln()
        /// <summary>
        /// Compute the base 10 logarithm of the input array, element-wise.
        /// </summary>
        member this.Log10() = this.Log(10.0)
        /// <summary> Calculate the natural logarithm (base e). </summary>
        member this.Ln() = this.Log System.Math.E
        /// <summary>
        /// Compute the natural logarithm of each element plus one.This computes log(1 + x) but is more numerically stable for x close to zero.
        /// </summary>
        member this.Log1p() = new Expr(PolarsWrapper.Log1p(this.CloneHandle()))
        member this.Sin() = new Expr(PolarsWrapper.Sin(this.CloneHandle()))
        member this.Cos() = new Expr(PolarsWrapper.Cos(this.CloneHandle()))
        member this.Tan() = new Expr(PolarsWrapper.Tan(this.CloneHandle()))
        member this.Cot() = new Expr(PolarsWrapper.Cot(this.CloneHandle()))
        member this.ArcSin() = new Expr(PolarsWrapper.ArcSin(this.CloneHandle()))
        member this.ArcCos() = new Expr(PolarsWrapper.ArcCos(this.CloneHandle()))
        member this.ArcTan() = new Expr(PolarsWrapper.ArcTan(this.CloneHandle()))
        member this.Sinh() = new Expr(PolarsWrapper.Sinh(this.CloneHandle()))
        member this.Cosh() = new Expr(PolarsWrapper.Cosh(this.CloneHandle()))
        member this.Tanh() = new Expr(PolarsWrapper.Tanh(this.CloneHandle()))
        member this.ArcSinh() = new Expr(PolarsWrapper.ArcSinh(this.CloneHandle()))
        member this.ArcCosh() = new Expr(PolarsWrapper.ArcCosh(this.CloneHandle()))
        member this.ArcTanh() = new Expr(PolarsWrapper.ArcTanh(this.CloneHandle()))
        /// <summary>
        /// Convert from degrees to radians.
        /// </summary>
        member this.Radians() = new Expr(PolarsWrapper.Radians(this.CloneHandle()))
        /// <summary>
        /// Convert from radians to degrees.
        /// </summary>
        member this.Degrees() = new Expr(PolarsWrapper.Degrees(this.CloneHandle()))
        /// <summary>
        /// Evaluate the number of set bits.
        /// </summary>
        member this.BitwiseCountOnes() = new Expr(PolarsWrapper.BitwiseCountOnes(this.CloneHandle()))
        /// <summary>
        /// Evaluate the number of unset bits.
        /// </summary>
        member this.BitwiseCountZeros() = new Expr(PolarsWrapper.BitwiseCountZeros(this.CloneHandle()))
        /// <summary>
        /// Evaluate the number most-significant set bits before seeing an unset bit.
        /// </summary>
        member this.BitwiseLeadingOnes() = new Expr(PolarsWrapper.BitwiseLeadingOnes(this.CloneHandle()))
        /// <summary>
        /// Evaluate the number most-significant unset bits before seeing a set bit.
        /// </summary>
        member this.BitwiseLeadingZeros() = new Expr(PolarsWrapper.BitwiseLeadingZeros(this.CloneHandle()))
        /// <summary>
        /// Evaluate the number least-significant set bits before seeing an unset bit.
        /// </summary>
        member this.BitwiseTrailingOnes() = new Expr(PolarsWrapper.BitwiseTrailingOnes(this.CloneHandle()))
        /// <summary>
        /// Evaluate the number least-significant unset bits before seeing a set bit.
        /// </summary>
        member this.BitwiseTrailingZeros() = new Expr(PolarsWrapper.BitwiseTrailingZeros(this.CloneHandle()))
        // ==========================================
        // Cumulative Functions
        // ==========================================
        /// <summary>
        /// Get an array with the cumulative sum computed at every element.
        /// </summary>
        /// <param name="reverse">Reverse the operation.</param>
        /// <returns></returns>
        member this.CumSum(?reverse: bool) = 
            let r = defaultArg reverse true
            new Expr(PolarsWrapper.CumSum(this.CloneHandle(), r))
        /// <summary>
        /// Get an array with the cumulative max computed at every element.
        /// </summary>
        /// <param name="reverse">Reverse the operation.</param>
        /// <returns></returns>
        member this.CumMax(?reverse: bool) = 
            let r = defaultArg reverse true
            new Expr(PolarsWrapper.CumMax(this.CloneHandle(), r))
        /// <summary>   
        /// Get an array with the cumulative min computed at every element.
        /// </summary>
        /// <param name="reverse">Reverse the operation.</param>
        /// <returns></returns>
        member this.CumMin(?reverse: bool) = 
            let r = defaultArg reverse true
            new Expr(PolarsWrapper.CumMin(this.CloneHandle(), r))
        /// <summary>
        /// Get an array with the cumulative prod computed at every element.
        /// </summary>
        /// <param name="reverse">Reverse the operation.</param>
        /// <returns></returns>
        member this.CumProd(?reverse: bool) = 
            let r = defaultArg reverse true
            new Expr(PolarsWrapper.CumProd(this.CloneHandle(), r))        
        /// <summary>
        /// Get an array with the cumulative count computed at every element.
        /// </summary>
        /// <param name="reverse">Reverse the operation.</param>
        /// <returns></returns>
        member this.CumCount(?reverse: bool) = 
            let r = defaultArg reverse true
            new Expr(PolarsWrapper.CumCount(this.CloneHandle(), r))
        /// <summary>
        /// Run an expression over a sliding window that increases 1 slot every iteration.
        /// This can be really slow as it can have O(n^2) complexity. Don’t use this for operations that visit all elements.
        /// </summary>
        /// <param name="expr">Expression to evaluate</param>
        /// <param name="minSamples">Number of valid values there should be in the window before the expression is evaluated. valid values = length - null_count</param>
        member this.CumulativeEval(expr:Expr,?minSamples) =
            let min = defaultArg minSamples 1
            new Expr(PolarsWrapper.CumulativeEval(this.CloneHandle(),expr.CloneHandle(),min))
        /// <summary>
        /// Calculate the difference with the previous value (n-th lag).
        /// Null values are propagated.
        /// </summary>
        member this.Diff(?n: int64, ?nullBehavior: NullBehavior) = 
            let n1 = defaultArg n 1L
            let nb = defaultArg nullBehavior NullBehavior.Ignore
            new Expr(PolarsWrapper.Diff(this.CloneHandle(), PolarsWrapper.Lit n1,nb.ToNative()))
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
        member this.EwmMean(alpha: float,?adjust: bool,?bias:bool,?minPeriods:int, ?ignoreNulls:bool) = 
            let adj = defaultArg adjust true
            let b = defaultArg bias true
            let ig = defaultArg ignoreNulls false
            let min = defaultArg minPeriods 1
            new Expr(PolarsWrapper.EwmMean(this.CloneHandle(),alpha,adj,b,min,ig))
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
        member this.EwmStd(alpha: float,?adjust: bool,?bias:bool,?minPeriods:int, ?ignoreNulls:bool) = 
            let adj = defaultArg adjust true
            let b = defaultArg bias true
            let ig = defaultArg ignoreNulls false
            let min = defaultArg minPeriods 1
            new Expr(PolarsWrapper.EwmStd(this.CloneHandle(),alpha,adj,b,min,ig))
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
        member this.EwmVar(alpha: float,?adjust: bool,?bias:bool,?minPeriods:int, ?ignoreNulls:bool) = 
            let adj = defaultArg adjust true
            let b = defaultArg bias true
            let ig = defaultArg ignoreNulls false
            let min = defaultArg minPeriods 1
            new Expr(PolarsWrapper.EwmVar(this.CloneHandle(),alpha,adj,b,min,ig))
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
            new Expr(PolarsWrapper.EwmMeanBy(this.CloneHandle(),by.CloneHandle(),halfLife))
        /// <summary>
        /// Get unique values of this expression.
        /// </summary>
        member this.Unique(?maintainOrder) =
            let mo = defaultArg maintainOrder false
            if mo = false then 
                new Expr(PolarsWrapper.ExprUnique(this.CloneHandle()))
            else
                new Expr(PolarsWrapper.ExprUniqueStable(this.CloneHandle()))
        /// <summary>
        /// Return a count of the unique values in the order of appearance.
        /// This method differs from value_counts in that it does not return the values, only the counts and might be faster
        /// </summary>
        member this.UniqueCounts() = new Expr(PolarsWrapper.ExprUniqueCounts(this.CloneHandle()))
        /// <summary>
        /// Count the occurrence of unique values.
        /// </summary>
        /// <param name="sort">Sort the output by count, in descending order. If set to False (default), the order is non-deterministic.</param>
        /// <param name="parallel">Execute the computation in parallel.This option should likely not be enabled in a group by context, as the computation will already be parallelized per group.</param>
        /// <param name="name">Give the resulting count column a specific name; if normalize is True this defaults to “proportion”, otherwise defaults to “count”.</param>
        /// <param name="normalize">If True, the count is returned as the relative frequency of unique values normalized to 1.0.</param>
        /// <returns>Expression of type Struct, mapping unique values to their count (or proportion).</returns>
        member this.ValueCounts(?sort,?parallelOn,?name:string,?normalize) =
            let so = defaultArg sort false
            let pa = defaultArg parallelOn false
            let na = defaultArg name null
            let no = defaultArg normalize false
            new Expr(PolarsWrapper.ValueCounts(this.CloneHandle(),so,pa,na,no))
        /// <summary>
        /// Bin values into buckets and count their occurrences.
        /// </summary>
        /// <param name="bins">Bin edges. If None given, we determine the edges based on the data.</param>
        /// <param name="binCount">If bins is not provided, binCount uniform bins are created that fully encompass the data.</param>
        /// <param name="includeCategory">Include a column that indicates the upper breakpoint.</param>
        /// <param name="includeBreakPoint">Include a column that shows the intervals as categories.</param>
        member this.Hist(?bins:Expr,?binCount:int,?includeCategory,?includeBreakPoint) =
            let bE = 
                match bins with
                | Some bins -> bins.CloneHandle()
                | None -> null
            let bC = binCount |> Option.toNullable
            let inc = defaultArg includeCategory false
            let inb = defaultArg includeBreakPoint false
            new Expr(PolarsWrapper.ExprHist(this.CloneHandle(),bE,bC,inc,inb))
        /// <summary>
        /// Find the index of the first occurrence of a specific value.
        /// </summary>
        /// <param name="element">The element expression to search for.</param>
        member this.IndexOf(element: Expr) =
            new Expr(PolarsWrapper.IndexOf(this.CloneHandle(), element.CloneHandle()))

        /// <summary>
        /// Find indices where elements should be inserted to maintain order (Binary Search).
        /// </summary>
        /// <param name="element">The element expression to insert/search.</param>
        /// <param name="side">The insertion side (Any, Left, Right). Default is Any.</param>
        /// <param name="descending">Whether the target column is sorted in descending order. Default is false.</param>
        member this.SearchSorted(element: Expr, ?side: SearchSortedSide, ?descending: bool) =
            let side = defaultArg side SearchSortedSide.Any
            let descending = defaultArg descending false
            new Expr(PolarsWrapper.SearchSorted(this.CloneHandle(), element.CloneHandle(), side.ToNative(), descending))

