namespace Polars.FSharp

open System.ComponentModel
open Polars.NET.Core
open System

[<EditorBrowsable(EditorBrowsableState.Never)>]
type LitMechanism = LitMechanism with
    static member ($) (LitMechanism, v: int8) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: uint8) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: int16) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: uint16) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: int) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: uint) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: int64) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: uint64) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: Int128) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: string) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: Half) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: float32) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: double) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: DateTime) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: DateOnly) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: TimeOnly) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: DateTimeOffset) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: TimeSpan) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: decimal) = new Expr(PolarsWrapper.Lit v)
    static member ($) (LitMechanism, v: bool) = new Expr(PolarsWrapper.Lit v)
    // --- List ---
    static member ($) (LitMechanism, v: int list)      = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: int64 list)    = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: Half list)     = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: float list)    = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle)) // double
    static member ($) (LitMechanism, v: float32 list)  = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: string list)   = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: bool list)     = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: DateTime list) = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: DateOnly list) = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))

    // --- Array (High Performance) ---
    static member ($) (LitMechanism, v: int[])      = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: Half[])    = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: int64[])    = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: float[])    = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: float32[])  = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: string[])   = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: bool[])     = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: DateTime[]) = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))

    // --- Nullable List (Option) ---
    static member ($) (LitMechanism, v: int option list)    = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: float option list)  = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
    static member ($) (LitMechanism, v: string option list) = new Expr(PolarsWrapper.Lit(Series.create("", v).Handle))
