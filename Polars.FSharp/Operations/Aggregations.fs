namespace Polars.FSharp

open System
open Polars.NET.Core

[<AutoOpen>]
module AggregateOps = 
    type LazyFrame with
        /// <summary>
        /// Return the number of non-null elements for each column.
        /// </summary>
        /// <returns></returns>
        member this.Count() = 
            this.Select(Expr.All().Count())
        /// <summary>
        /// Aggregate the columns in the Frame to their sum value.
        /// </summary>
        /// <returns></returns>
        member this.Sum() =
            this.Select(Expr.All().Sum())
        /// <summary>
        /// Aggregate the columns in the Frame to their maximum value.
        /// </summary>
        /// <returns></returns>
        member this.Max() =
            this.Select(Expr.All().Max())
        /// <summary>
        /// Aggregate the columns in the Frame to their minimum value.
        /// </summary>
        /// <returns></returns>
        member this.Min() =
            this.Select(Expr.All().Min())
        /// <summary>
        /// Aggregate the columns in the Frame to their mean value.
        /// </summary>
        /// <returns></returns>
        member this.Mean() =
            this.Select(Expr.All().Mean())
        /// <summary>
        /// Aggregate the columns in the Frame to their median value.
        /// </summary>
        /// <returns></returns>
        member this.Median() =
            this.Select(Expr.All().Median())
        /// <summary>
        /// Aggregate the columns in the Frame as the sum of their null value count.
        /// </summary>
        /// <returns></returns>
        member this.NullCount() = 
            this.Select(Expr.All().NullCount())
        /// <summary>
        /// Aggregate the columns in the Frame to their standard deviation value.
        /// </summary>
        /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
        /// <returns></returns>
        member this.Std(?ddof:int) = 
            let d = defaultArg ddof 1
            this.Select(Expr.All().Std(ddof=d))
        /// <summary>
        /// Aggregate the columns in the Frame to their variance value.
        /// </summary>
        /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
        /// <returns></returns>
        member this.Var(?ddof:int) =
            let d = defaultArg ddof 1
            this.Select(Expr.All().Var(ddof=d))
        /// <summary>
        /// Aggregate the columns in the Frame to their quantile value.
        /// </summary>
        /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
        /// <param name="method">['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method.</param>
        /// <returns></returns>
        member this.Quantile(quantile:float, ?method: QuantileMethod) =
            let met = defaultArg method QuantileMethod.Linear
            this.Select(Expr.All().Quantile(quantile,met))
    type DataFrame with
        /// <summary>
        /// Return the number of non-null elements for each column.
        /// </summary>
        /// <returns></returns>
        member this.Count() = 
            this.Lazy().Count()
        /// <summary>
        /// Aggregate the columns in the Frame to their sum value.
        /// </summary>
        /// <returns></returns>
        member this.Sum() =
            this.Lazy().Sum()
        /// <summary>
        /// Aggregate the columns in the Frame to their maximum value.
        /// </summary>
        /// <returns></returns>
        member this.Max() =
            this.Lazy().Max()
        /// Aggregate the columns in the Frame to their minimum value.
        /// </summary>
        /// <returns></returns>
        member this.Min() =
            this.Lazy().Min()
        /// <summary>
        /// Aggregate the columns in the Frame to their mean value.
        /// </summary>
        /// <returns></returns>
        member this.Mean() =
            this.Lazy().Mean()
        /// <summary>
        /// Aggregate the columns in the Frame to their median value.
        /// </summary>
        /// <returns></returns>
        member this.Median() =
            this.Lazy().Median()
        /// <summary>
        /// Aggregate the columns in the Frame as the sum of their null value count.
        /// </summary>
        /// <returns></returns>
        member this.NullCount() = 
            this.Lazy().NullCount()
        /// <summary>
        /// Aggregate the columns in the Frame to their standard deviation value.
        /// </summary>
        /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
        /// <returns></returns>
        member this.Std(?ddof:int) = 
            let d = defaultArg ddof 1
            this.Lazy().Std(ddof=d)
        /// <summary>
        /// Aggregate the columns in the Frame to their variance value.
        /// </summary>
        /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. By default ddof is 1.</param>
        /// <returns></returns>
        member this.Var(?ddof:int) =
            let d = defaultArg ddof 1
            this.Lazy().Var(ddof=d)
        /// <summary>
        /// Aggregate the columns in the Frame to their quantile value.
        /// </summary>
        /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
        /// <param name="method">['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method.</param>
        /// <returns></returns>
        member this.Quantile(quantile:float, ?method: QuantileMethod) =
            let met = defaultArg method QuantileMethod.Linear
            this.Lazy().Quantile(quantile,met)