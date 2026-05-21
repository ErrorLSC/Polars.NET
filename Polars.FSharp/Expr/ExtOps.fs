namespace Polars.FSharp

open Polars.NET.Core

type [<Struct>] ExtensionOps(handle: ExprHandle) =
    /// <summary>
    /// Convert to an extension dtype.The input must be of the storage type of the extension dtype.
    /// </summary>
    member _.To(dtype:DataTypeExpr) = new Expr(PolarsWrapper.ExtTo(handle,dtype.CloneHandle()))
    /// <summary>
    /// Convert to an extension dtype.The input must be of the storage type of the extension dtype.
    /// </summary>
    member this.To(dtype:DataType) = this.To(dtype.ToDataTypeExpr())
    /// <summary>
    /// Get the storage values of an extension data type.
    /// If the input does not have an extension data type, it is returned as-is.
    /// </summary>
    member _.Storage() = new Expr(PolarsWrapper.ExtStorage(handle))