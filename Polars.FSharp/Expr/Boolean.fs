namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module ExprBoolean =
    type Expr with
        /// <summary>
        /// Check whether the expression contains one or more null values.
        /// </summary>
        member this.HasNulls() = this.NullCount() .> new Expr(PolarsWrapper.Lit 0)
        /// <summary> Check if the value is between lower and upper bounds (inclusive). </summary>
        member this.IsBetween(lower: Expr, upper: Expr) =
            new Expr(PolarsWrapper.IsBetween(this.CloneHandle(), lower.CloneHandle(), upper.CloneHandle()))
        /// <summary>
        /// Check if the value is in given collection.
        /// </summary>
        member this.IsIn(other: Expr,?nullsEqual: bool) : Expr = 
            let nE = defaultArg nullsEqual false
            new Expr(PolarsWrapper.IsIn(this.CloneHandle(), other.CloneHandle(),nE))
        /// <summary>
        /// Get a boolean mask indicating which values are unique.
        /// </summary>
        member this.IsUnique() =
            new Expr(PolarsWrapper.ExprIsUnique(this.CloneHandle()))
        /// <summary>
        /// Get a boolean mask indicating which values are duplicated.
        /// </summary>
        member this.IsDuplicated() =
            new Expr(PolarsWrapper.ExprIsDuplicated(this.CloneHandle()))
        /// <summary>
        /// Evaluate whether the expression is null.
        /// </summary>    
        member this.IsNull() = 
            new Expr(PolarsWrapper.IsNull(this.CloneHandle()))
        /// <summary>
        /// Evaluate whether the expression is not null.
        /// </summary>    
        member this.IsNotNull() = 
            new Expr(PolarsWrapper.IsNotNull(this.CloneHandle()))
        /// <summary>
        /// Returns a boolean Series indicating which values are NaN.
        /// </summary>
        member this.IsNan() = new Expr(PolarsWrapper.ExprIsNan(this.CloneHandle()))
        /// <summary>
        /// Returns a boolean Series indicating which values are not NaN.
        /// </summary>
        member this.IsNotNan() = new Expr(PolarsWrapper.ExprIsNotNan(this.CloneHandle()))
        /// <summary>
        /// Returns a boolean Series indicating which values are finite.
        /// </summary>
        /// <returns>Expression of data type Boolean.</returns>
        member this.IsFinite() = new Expr(PolarsWrapper.ExprIsFinite(this.CloneHandle()))
        /// <summary>
        /// Returns a boolean Series indicating which values are infinite.
        /// </summary>
        /// <returns>Expression of data type Boolean.</returns>
        member this.IsInFinite() = new Expr(PolarsWrapper.ExprIsInfinite(this.CloneHandle()))
        /// <summary>
        /// Return a boolean mask indicating the first occurrence of each distinct value.
        /// </summary>
        /// <returns>Expression of data type Boolean.</returns>
        member this.IsFirstDistinct() = new Expr(PolarsWrapper.ExprIsFirstDistinct(this.CloneHandle()))
        /// <summary>
        /// Return a boolean mask indicating the last occurrence of each distinct value.
        /// </summary>
        /// <returns>Expression of data type Boolean.</returns>
        member this.IsLastDistinct() = new Expr(PolarsWrapper.ExprIsLastDistinct(this.CloneHandle()))
        /// <summary>
        /// Check if this expression is close, i.e. almost equal, to the other expression.
        /// </summary>
        /// <param name="other">A literal or expression value to compare with.</param>
        /// <param name="absTol"> Absolute tolerance. This is the maximum allowed absolute difference betweentwo values. Must be non-negative.</param>
        /// <param name="relTol">Relative tolerance. This is the maximum allowed difference between two values, relative to the larger absolute value. Must be non-negative.</param>
        /// <param name="nansEqual">Whether NaN values should be considered equal.</param>
        /// <returns>Expression/Series of data type Boolean.</returns>
        member this.IsClose(other:Expr,?absTol:double,?relTol:double,?nansEqual:bool) =
            let abs = defaultArg absTol 0.0
            let rel = defaultArg relTol 1e-09
            let nan = defaultArg nansEqual false
            new Expr(PolarsWrapper.ExprIsClose(this.CloneHandle(),other.CloneHandle(),abs,rel,nan))
        /// <summary>
        /// Checks if an expression is sorted.
        /// <para>If descending and/or nulls_last are None, it will check True and False
        /// for the unspecified option(s), and return True if the expression is sorted
        /// under any combination of those settings.</para>
        /// </summary>
        /// <param name="descending">Checks if the expression is sorted in descending order.Defaults to False.</param>
        /// <param name="nullsLast">Consider null values as being ordered last when checking sortedness.Defaults to False.</param>
        /// <returns>Expression of Boolean</returns>
        member this.IsSorted(?descending,?nullsLast) =
            let des = defaultArg descending false
            let nul = defaultArg nullsLast false
            new Expr(PolarsWrapper.ExprIsSorted(this.CloneHandle(),des,nul))
