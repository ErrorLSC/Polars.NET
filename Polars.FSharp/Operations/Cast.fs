namespace Polars.FSharp

open System
open Polars.NET.Core
[<RequireQualifiedAccess>]
type Dtype =
    | DataTypeExpr of DataTypeExpr         
    | PlDataType of DataType        
    | NetType of Type            

[<RequireQualifiedAccess>]
module Dtype =
    let consume (src: Dtype) : DataTypeExpr =
        match src with
        | Dtype.DataTypeExpr e -> e.Clone()
        | Dtype.PlDataType dt -> dt.ToDataTypeExpr()
        | Dtype.NetType t ->
            let dt = DataType.op_Implicit t
            dt.ToDataTypeExpr()           

[<AutoOpen>]
module ExprCastExtension =
    type Expr with
        /// <summary>Cast expression to another data type.</summary>
        member internal this.CastImpl(source: Dtype, ?strict: bool, ?wrapNumerical: bool) =
            let strict = defaultArg strict true
            let wn = defaultArg wrapNumerical false
            if strict && wn then
                invalidArg "strict/wrapNumerical" "Cannot set both 'strict' and 'wrapNumerical' to true."
            use target = Dtype.consume source
            let h = PolarsWrapper.ExprCast(this.CloneHandle(), target.handle, strict, wn)
            new Expr(h)

        /// <summary>Cast expression to the type of 'T.</summary>
        member this.Cast<'T>(?strict: bool, ?wrapNumerical: bool) =
            this.CastImpl(Dtype.NetType typeof<'T>, ?strict=strict, ?wrapNumerical=wrapNumerical)
        /// <summary>Cast expression to the polars datatype.</summary>
        member this.Cast(dtype:DataType,?strict:bool, ?wrapNumerical:bool) =
            this.CastImpl(Dtype.PlDataType dtype,?strict=strict, ?wrapNumerical=wrapNumerical)
        member this.Cast(dtype:DataTypeExpr,?strict:bool, ?wrapNumerical:bool) =
            this.CastImpl(Dtype.DataTypeExpr dtype,?strict=strict, ?wrapNumerical=wrapNumerical)
[<AutoOpen>]
module SeriesCastExtension =
    type Series with
        /// <summary>
        /// Cast Series to another DataType.
        /// </summary>
        /// <param name="dtype">The target type</param>
        /// <param name="strict">Throws an error if conversion had overflows.</param>
        /// <param name="wrapNumerical">Allows wrapping numerical overflow.</param>
        member this.Cast(dtype: DataType,?strict: bool, ?wrapNumerical: bool) : Series =
            use typeHandle = dtype.CreateHandle()
            let st = defaultArg strict true
            let wN = defaultArg wrapNumerical false
            let newHandle = PolarsWrapper.SeriesCast(this.Handle, typeHandle,st,wN)
            new Series(newHandle)
        /// <summary>
        /// Cast the series to a specific .NET type.
        /// </summary>
        /// <typeparam name="T">Target .NET type (e.g., int, double, string).</typeparam>
        /// <param name="strict">If true, throw an error if the cast fails; if false, invalid values become null.</param>
        /// <param name="wrapNumerical">If true, wrap numerical values instead of saturating.</param>
        /// <returns>A new Series of the target type.</returns>
        member this.Cast<'T>(?strict,?wrapNumerical) =
            use tp = DataType.FromNetType<'T>()
            this.Cast(tp,?strict=strict,?wrapNumerical=wrapNumerical) 

