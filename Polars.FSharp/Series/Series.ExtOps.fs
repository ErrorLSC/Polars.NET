namespace Polars.FSharp

type SeriesExtOpsNameSpace(parent: Series) =
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr
    /// <summary>
    /// Convert to an extension dtype.The input must be of the storage type of the extension dtype.
    /// </summary>
    member _.To(dtype:DataTypeExpr) = apply (fun e -> e.Ext.To(dtype))
    /// <summary>
    /// Convert to an extension dtype.The input must be of the storage type of the extension dtype.
    /// </summary>
    member _.To(dtype:DataType) = apply (fun e -> e.Ext.To(dtype))
    /// <summary>
    /// Get the storage values of an extension data type.
    /// If the input does not have an extension data type, it is returned as-is.
    /// </summary>
    member _.Storage() = apply (fun e -> e.Ext.Storage())
