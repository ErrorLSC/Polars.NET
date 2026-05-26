namespace Polars.FSharp

type SeriesExtOpsNameSpace(parent: Series) =
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr
    member _.To(dtype:DataTypeExpr) = apply (fun e -> e.Ext.To(dtype))
    member _.To(dtype:DataType) = apply (fun e -> e.Ext.To(dtype))
    member _.Storage() = apply (fun e -> e.Ext.Storage())