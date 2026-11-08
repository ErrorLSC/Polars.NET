namespace Polars.FSharp

type SeriesCategoricalNameSpace(parent: Series) =
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary>
    /// Get the categories of the categorical series.
    /// </summary>
    member _.GetCategories() = apply (fun e -> e.Cat.GetCategories())
    /// <summary>
    /// Get the length in bytes of the categorical series.
    /// </summary>
    member _.LenBytes() = apply (fun e -> e.Cat.LenBytes())
    /// <summary>
    /// Get the length in characters of the categorical series.
    /// </summary>
    member _.LenChars() = apply (fun e -> e.Cat.LenChars())

    /// <summary>
    /// Check if the categorical series starts with the given prefix.
    /// </summary>
    member _.StartsWith prefix =
        apply (fun e -> e.Cat.StartsWith prefix)

    /// <summary>
    /// Check if the categorical series ends with the given suffix.
    /// </summary>
    member _.EndsWith suffix = apply (fun e -> e.Cat.EndsWith suffix)
    /// <summary>
    /// Get the physical representation of the categorical series.
    /// </summary>
    member _.Physical() = apply (fun e -> e.Cat.Physical())

    /// <summary>
    /// Convert the categorical series to the given data type.
    /// </summary>
    member _.To(dtype, ?strict) =
        apply (fun e -> e.Cat.To(dtype, ?strict = strict))
