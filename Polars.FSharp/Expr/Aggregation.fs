namespace Polars.FSharp

open System
open Polars.NET.Core

[<AutoOpen>]
module ExprAggregation =
    type Expr with
        /// <summary>
        /// Check if <b>all</b> values in the boolean expression are <c>true</c>.
        /// <para>This is a boolean aggregation.</para>
        /// </summary>
        /// <param name="ignoreNulls">
        /// If <c>true</c>, null values are ignored. 
        /// If <c>false</c> (default), the result propagates nulls (i.e., if there is a null and no false, the result might be null).
        /// </param>
        /// <returns>A new expression representing the boolean result.</returns>
        member this.All(?ignoreNulls:bool) = 
            let ignore = defaultArg ignoreNulls true
            new Expr(PolarsWrapper.All(this.CloneHandle(),ignore))
        /// <summary>
        /// Check if <b>any</b> value in the boolean expression is <c>true</c>.
        /// <para>This is a boolean aggregation.</para>
        /// </summary>
        /// <param name="ignoreNulls">
        /// If <c>true</c>, null values are ignored. 
        /// If <c>false</c> (default), the result propagates nulls.
        /// </param>
        /// <returns>A new expression representing the boolean result.</returns>
        member this.Any(?ignoreNulls:bool) = 
            let ignore = defaultArg ignoreNulls true
            new Expr(PolarsWrapper.Any(this.CloneHandle(),ignore))
        /// <summary>
        /// Calculate the sum of the values in the group or column.
        /// </summary>
        member this.Sum() = new Expr(PolarsWrapper.Sum (this.CloneHandle()))
        /// <summary>
        /// Calculate the average of the values in the group or column.
        /// </summary>
        member this.Mean() = new Expr(PolarsWrapper.Mean (this.CloneHandle()))
        /// <summary>
        /// Get the mode value.
        /// </summary>
        member this.Mode() =new Expr(PolarsWrapper.Mode (this.CloneHandle()))
        /// <summary>
        /// Get the max value.
        /// </summary>
        member this.Max() = new Expr(PolarsWrapper.Max (this.CloneHandle()))
        /// <summary>
        /// Get maximum value, ordered by another expression.
        /// If the by expression has multiple values equal to the maximum it is not defined which value will be chosen.
        /// </summary>
        member this.MaxBy(by:Expr) = new Expr(PolarsWrapper.MaxBy(this.CloneHandle(),by.CloneHandle()))
        /// <summary>
        /// Get maximum value, but propagate/poison encountered NaN values.
        /// </summary>
        member this.NanMax() = new Expr(PolarsWrapper.NanMax (this.CloneHandle()))
        /// <summary>
        /// Get the min value.
        /// </summary>
        member this.Min() = new Expr(PolarsWrapper.Min (this.CloneHandle()))
        /// <summary>
        /// Get minimum value, ordered by another expression.
        /// If the by expression has multiple values equal to the minimum it is not defined which value will be chosen.
        /// </summary>
        member this.MinBy(by:Expr) = new Expr(PolarsWrapper.MinBy(this.CloneHandle(),by.CloneHandle()))
        /// <summary>
        /// Get minimum value, but propagate/poison encountered NaN values.
        /// </summary>
        member this.NanMin() = new Expr(PolarsWrapper.NanMin (this.CloneHandle()))
        /// <summary>
        /// Count the number of null.
        /// </summary>
        member this.NullCount() = new Expr(PolarsWrapper.NullCount (this.CloneHandle()))
        /// <summary>
        /// Count unique values.
        /// Notes: Null is considered to be a unique value for the purposes of this operation.
        /// </summary>
        member this.NUnique() = new Expr(PolarsWrapper.NUnique (this.CloneHandle()))
        /// <summary>
        /// Approximate count of unique values.
        /// This is done using the HyperLogLog++ algorithm for cardinality estimation.
        /// </summary>
        member this.ApproxNUnique() = new Expr(PolarsWrapper.ApproxNUnique (this.CloneHandle()))
        /// <summary>
        /// Compute the product of an expression
        /// </summary>
        member this.Product() = new Expr(PolarsWrapper.Product (this.CloneHandle()))
        /// <summary>
        /// Get the first n rows.
        /// </summary>
        /// <param name="n">Number of rows to return.</param>
        /// <returns></returns>
        member this.Head(?n:int) =
            let n10 = defaultArg n 10
            new Expr(PolarsWrapper.Head(this.CloneHandle(),n10));
        /// <summary>
        /// Get the last n rows.
        /// </summary>
        /// <param name="n">Number of rows to return.</param>
        /// <returns></returns>
        member this.Tail(?n:int) =
            let n10 = defaultArg n 10
            new Expr(PolarsWrapper.Tail(this.CloneHandle(),n10));
        /// <summary>
        /// Get the first value of the group/series.
        /// </summary>
        /// <returns>A new expression representing the first value.</returns>
        member this.First(?ignoreNulls:bool) = 
            let ign = defaultArg ignoreNulls false
            new Expr(PolarsWrapper.First(this.CloneHandle(),ign))
        /// <summary>
        /// Get the last value of the group/series.
        /// </summary>
        /// <returns>A new expression representing the last value.</returns>
        member this.Last(?ignoreNulls:bool) =
            let ign = defaultArg ignoreNulls false 
            new Expr(PolarsWrapper.Last(this.CloneHandle(),ign))
        /// <summary>
        /// Get the index of the maximum value.
        /// </summary>
        member this.ArgMax() =
            new Expr(PolarsWrapper.ArgMax(this.CloneHandle()))

        /// <summary>
        /// Get the index of the minimum value.
        /// </summary>
        member this.ArgMin() =
            new Expr(PolarsWrapper.ArgMin(this.CloneHandle()))
        /// <summary> Implode multiple rows to a list. </summary>
        member this.Implode(?maintainOrder) = 
            let ma = defaultArg maintainOrder true
            new Expr(PolarsWrapper.Implode(this.CloneHandle(),ma))
        /// <summary>
        /// Count the number of valid (non-null) values.
        /// </summary>
        member this.Count() = new Expr(PolarsWrapper.Count(this.CloneHandle()))
        /// <summary>
        /// Return the number of elements in the column.
        /// Null values count towards the total.
        /// </summary>
        member this.Len() = new Expr(PolarsWrapper.ExprLen(this.CloneHandle()))
        /// <summary>
        /// Get the standard deviation value.
        /// </summary>
        /// <param name="ddof">Delta Degrees of Freedom. Default is 1.</param>
        member this.Std(?ddof: uint8) = 
            let d = defaultArg ddof 1uy // Default sample std dev
            new Expr(PolarsWrapper.Std(this.CloneHandle(), d))
        /// <summary>
        /// Get the variance value.
        /// </summary>
        /// <param name="ddof">Delta Degrees of Freedom. Default is 1.</param>
        member this.Var(?ddof: uint8) = 
            let d = defaultArg ddof 1uy
            new Expr(PolarsWrapper.Var(this.CloneHandle(), d))
        /// <summary>
        /// Get the median value.
        /// </summary>
        member this.Median() = new Expr(PolarsWrapper.Median (this.CloneHandle()))
        /// <summary>
        /// Compute the sample skewness of a data set.
        /// </summary>
        /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
        member this.Skew(?bias: bool) = 
            let b = defaultArg bias true
            new Expr(PolarsWrapper.Skew(this.CloneHandle(), b))
        /// <summary>
        /// Compute the kurtosis (Fisher or Pearson) of a dataset.
        /// </summary>
        /// <param name="fisher">If True, Fisher’s definition is used (normal ==> 0.0). If False, Pearson’s definition is used (normal ==> 3.0).</param>
        /// <param name="bias">If False, the calculations are corrected for statistical bias.</param>
        member this.Kurtosis(?fisher: bool, ?bias: bool) = 
            let f = defaultArg fisher true
            let b = defaultArg bias true
            new Expr(PolarsWrapper.Kurtosis(this.CloneHandle(), f,b))
        /// <summary>
        /// Get the quantile value.
        /// </summary>
        /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
        /// <param name="method">['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method.</param>
        member this.Quantile(q: float, ?interpolation: QuantileMethod) =
            let method = defaultArg interpolation QuantileMethod.Linear
            new Expr(PolarsWrapper.Quantile(this.CloneHandle(), q, method.ToNative()))
        /// <summary>
        /// Perform an aggregation of bitwise ANDs.
        /// </summary>
        member this.BitwiseAnd() = new Expr(PolarsWrapper.BitwiseAnd (this.CloneHandle()))
        /// <summary>
        /// Perform an aggregation of bitwise Ors.
        /// </summary>
        member this.BitwiseOr() = new Expr(PolarsWrapper.BitwiseOr (this.CloneHandle()))
        /// <summary>
        /// Perform an aggregation of bitwise Xors.
        /// </summary>
        member this.BitwiseXor() = new Expr(PolarsWrapper.BitwiseXor (this.CloneHandle()))