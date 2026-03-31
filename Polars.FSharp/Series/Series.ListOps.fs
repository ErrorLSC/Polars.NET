namespace Polars.FSharp

type SeriesListNameSpace(parent: Series) =
    
    // Unary helper
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    // --- Unary Ops (Forward to Expr.List) ---
    
    member _.First() = apply (fun e -> e.List.First())
    member _.Get(index: int) = apply (fun e -> e.List.Get index)
    member _.Join(sep: string) = apply (fun e -> e.List.Join sep)
    member _.Len() = apply (fun e -> e.List.Len())
    member _.Sum() = apply (fun e -> e.List.Sum())
    member _.Min() = apply (fun e -> e.List.Min())
    member _.Max() = apply (fun e -> e.List.Max())
    member _.Mean() = apply (fun e -> e.List.Mean())
    member _.Reverse() = apply (fun e -> e.List.Reverse())
    
    member _.Sort(?descending, ?nullsLast, ?maintainOrder) =
        apply (fun e -> e.List.Sort(?descending=descending, ?nullsLast=nullsLast, ?maintainOrder=maintainOrder))

    // --- Binary Ops ---

    /// <summary>
    /// Concat with another Series.
    /// Logic: [this_val, other_val]
    /// </summary>
    member _.Concat(other: Series) =
        parent.ApplyBinaryExpr(other, (fun l r -> l.List.Concat r))

    // --- Search ---

    member _.Contains(item: int) = apply (fun e -> e.List.Contains item)
    member _.Contains(item: string) = apply (fun e -> e.List.Contains item)

