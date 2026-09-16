namespace Polars.FSharp

[<AutoOpen>]
module ExprManipulation =
    type Expr with
        /// <summary>
        /// Replace scalar values where matched with a new scalar value.
        /// Employs SRTP to automatically wrap scalars into literal expressions.
        /// </summary>
        member inline this.Replace(oldVal: ^Old, newVal: ^New) : Expr =
            this.Replace(pl.lit oldVal, pl.lit newVal)
        /// <summary>
        /// Fill null values with a scalar literal.
        /// </summary>
        /// <param name="value">The scalar value to fill nulls with.</param>
        member inline this.FillNull(value:^T):Expr =
            this.FillNull(pl.lit value)
        // member inline this.IsBetween(lower)
