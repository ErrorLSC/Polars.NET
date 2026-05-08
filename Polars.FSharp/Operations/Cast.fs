namespace Polars.FSharp

open System
open Polars.NET.Core

[<AutoOpen>]
module ExprCastExtension =
    type Expr with
        /// <summary>Cast expression to another data type.</summary>
        member this.Cast(source: Dtype, ?strict: bool, ?wrapNumerical: bool) =
            let strict = defaultArg strict true
            let wn = defaultArg wrapNumerical false
            if strict && wn then
                invalidArg "strict/wrapNumerical" "Cannot set both 'strict' and 'wrapNumerical' to true."
            use target = Dtype.consume source
            let h = PolarsWrapper.ExprCast(this.CloneHandle(), target.handle, strict, wn)
            new Expr(h)

        /// <summary>Cast expression to the type of 'T.</summary>
        member this.Cast<'T>(?strict: bool, ?wrapNumerical: bool) =
            this.Cast(Dtype.NetType typeof<'T>, ?strict=strict, ?wrapNumerical=wrapNumerical)
        /// <summary>Cast expression to the polars datatype.</summary>
        member this.Cast(dtype:DataType,?strict:bool, ?wrapNumerical:bool) =
            this.Cast(Dtype.PlDataType dtype,?strict=strict, ?wrapNumerical=wrapNumerical)
[<AutoOpen>]
module SeriesCastExtension =
    type Series with
        member this.Cast(dtype: DataType,?strict: bool, ?wrapNumerical: bool) : Series =
            use typeHandle = dtype.CreateHandle()
            let st = defaultArg strict true
            let wN = defaultArg wrapNumerical false
            let newHandle = PolarsWrapper.SeriesCast(this.Handle, typeHandle,st,wN)
            new Series(newHandle)

