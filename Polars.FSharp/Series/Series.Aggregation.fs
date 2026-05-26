namespace Polars.FSharp

open Polars.NET.Core

[<AutoOpen>]
module SeriesAggregateOps = 
    type Series with
        member internal this.ExtractScalar<'T>(aggregator: unit -> Series) : 'T option =
            if box this = null || this.IsEmpty then 
                None
            else
                use tempSeries = aggregator()
                
                if tempSeries.Length = 0L then None
                else tempSeries.GetValueOption<'T>(0L)
        member this.First() = this.ApplyExpr(Expr.Col(this.Name).First())
        /// <summary>
        /// Compute the first non-null scalar value of this series.
        /// </summary>
        member this.First<'T>() : 'T option =
            this.ExtractScalar<'T>(fun () -> this.First())
        member this.Last() = this.ApplyExpr(Expr.Col(this.Name).Last()) 
        /// <summary>
        /// Compute the last non-null scalar value of this series.
        /// </summary>
        member this.Last<'T>() : 'T option =
            this.ExtractScalar<'T>(fun () -> this.Last())
        member this.Sum() = new Series(PolarsWrapper.SeriesSum this.Handle)
        member this.Sum<'T>() : 'T option =
            this.ExtractScalar<'T>(fun () -> this.Sum())
        member this.Mean() = new Series(PolarsWrapper.SeriesMean this.Handle)
        member this.Mean<'T>() : 'T option =
            this.ExtractScalar<'T>(fun () -> this.Mean())
         member this.Max() = new Series(PolarsWrapper.SeriesMax this.Handle) 
        /// <summary>
        /// Compute the maximum scalar value of this series.
        /// </summary>
        member this.Max<'T>() : 'T option =
            this.ExtractScalar<'T>(fun () -> this.Max())
        member this.Min() = new Series(PolarsWrapper.SeriesMin this.Handle)
        member this.Min<'T>() : 'T option =
            this.ExtractScalar<'T>(fun () -> this.Min())
        member this.NanMax() = this.ApplyExpr(Expr.Col(this.Name).NanMax()) 
        member this.NanMax<'T>() : 'T option =
            this.ExtractScalar<'T>(fun () -> this.NanMax())
        member this.NanMin() = this.ApplyExpr(Expr.Col(this.Name).NanMin()) 
        member this.NanMin<'T>() : 'T option =
            this.ExtractScalar<'T>(fun () -> this.NanMin())
        member this.MaxBy(by:Expr) = this.ApplyExpr(Expr.Col(this.Name).MaxBy(by))
        member this.MaxBy<'T>(by:Expr) : 'T option =
            this.ExtractScalar<'T>(fun () -> this.MaxBy(by))
        member this.MinBy(by:Expr) = this.ApplyExpr(Expr.Col(this.Name).MinBy(by))
        member this.MinBy<'T>(by:Expr) : 'T option =
            this.ExtractScalar<'T>(fun () -> this.MinBy(by))
        member this.Product() = this.ApplyExpr(Expr.Col(this.Name).Product()) 
        member this.Product<'T>() : 'T option =
            this.ExtractScalar<'T>(fun () -> this.Product())
        member this.ArgMax() = 
            this.ExtractScalar<int64>(fun () -> this.ApplyExpr(Expr.Col(this.Name).ArgMax()))
        member this.ArgMin() = 
            this.ExtractScalar<int64>(fun () -> this.ApplyExpr(Expr.Col(this.Name).ArgMin()))
        member this.Count() : int64 =
            let resultOption = this.ExtractScalar<int64>(fun () -> this.ApplyExpr(Expr.Col(this.Name).Count()))
            
            resultOption |> Option.defaultValue 0L
        /// <summary>
        /// Get the standard deviation.
        /// </summary>
        /// <param name="ddof">Delta Degrees of Freedom. Default is 1.</param>
        /// <returns>A new <see cref="Series"/> containing the Std (length 1).</returns>
        member this.Std<'T>(?ddof: uint8) = 
            let d = defaultArg ddof 1uy
            this.ExtractScalar<'T>(fun () -> this.ApplyExpr(Expr.Col(this.Name).Std(d)))

        /// <summary>
        /// Get the variance.
        /// </summary>
        /// <param name="ddof">Delta Degrees of Freedom. Default is 1.</param>
        /// <returns>A new <see cref="Series"/> containing the Var (length 1).</returns>
        member this.Var<'T>(?ddof: uint8) = 
            let d = defaultArg ddof 1uy
            this.ExtractScalar<'T>(fun () -> this.ApplyExpr(Expr.Col(this.Name).Var(d)))

        /// <summary>
        /// Get the median.
        /// </summary>
        /// <returns>A new <see cref="Series"/> containing the Median (length 1).</returns>
        member this.Median<'T>() = 
            this.ExtractScalar<'T>(fun () -> this.ApplyExpr(Expr.Col(this.Name).Median()))
        /// <summary>
        /// Get the mode.
        /// </summary>
        /// <returns>A new <see cref="Series"/> containing the Mode (length 1).</returns>
        member this.Mode(?maintainOrder) = 
            let ma = defaultArg maintainOrder false
            new Series(PolarsWrapper.SeriesMode(this.Handle,ma))
        /// <summary>
        /// Aggregate values into a list.
        /// Result is a Series with 1 row containing a List of all values.
        /// </summary>
        member this.Implode() =
            new Series(PolarsWrapper.SeriesImplode(this.Handle))
        member this.BitwiseAnd() =
            this.ApplyExpr(Expr.Col(this.Name).BitwiseAnd())
        member this.BitwiseAnd<'T>() : 'T option =
            this.ExtractScalar<'T>(fun () -> this.BitwiseAnd())
        member this.BitwiseOr() =
            this.ApplyExpr(Expr.Col(this.Name).BitwiseOr())
        member this.BitwiseOr<'T>() : 'T option =
            this.ExtractScalar<'T>(fun () -> this.BitwiseOr())
        member this.BitwiseXor() =
            this.ApplyExpr(Expr.Col(this.Name).BitwiseXor())
        member this.BitwiseXor<'T>() : 'T option =
            this.ExtractScalar<'T>(fun () -> this.BitwiseXor())
        member this.Any ?ignoreNulls =
            this.ExtractScalar<bool>(fun () -> this.ApplyExpr(Expr.Col(this.Name).Any(?ignoreNulls=ignoreNulls)))
        member this.All ?ignoreNulls =
            this.ExtractScalar<bool>(fun () -> this.ApplyExpr(Expr.Col(this.Name).All(?ignoreNulls=ignoreNulls)))
        
        