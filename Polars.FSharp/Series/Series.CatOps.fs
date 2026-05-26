namespace Polars.FSharp

type SeriesCategoricalNameSpace(parent: Series) =
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr
    member _.GetCategories() = apply (fun e -> e.Cat.GetCategories())
    member _.LenBytes() = apply (fun e -> e.Cat.LenBytes())
    member _.LenChars() = apply (fun e -> e.Cat.LenChars())
    member _.StartsWith(prefix) = apply (fun e -> e.Cat.StartsWith prefix)
    member _.EndsWith(suffix) = apply (fun e -> e.Cat.EndsWith suffix)