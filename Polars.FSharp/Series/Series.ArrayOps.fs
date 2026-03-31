namespace Polars.FSharp

type SeriesArrayNameSpace(parent: Series) =
    
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    // --- Aggregations ---
    member _.Sum() = apply (fun e -> e.Array.Sum())
    member _.Min() = apply (fun e -> e.Array.Min())
    member _.Max() = apply (fun e -> e.Array.Max())
    member _.Mean() = apply (fun e -> e.Array.Mean())
    member _.Median() = apply (fun e -> e.Array.Median())
    
    
    member _.Std(?ddof: int) = 
        apply (fun e -> e.Array.Std(?ddof=ddof))
    
    member _.Var(?ddof: int) = 
        apply (fun e -> e.Array.Var(?ddof=ddof))

    // --- Boolean / Search ---

    member _.Any() = apply (fun e -> e.Array.Any())
    member _.All() = apply (fun e -> e.Array.All())

    /// <summary> Check if array contains an Item (Expr). </summary>
    member _.Contains(item: Expr, ?nullsEqual: bool) =
        apply (fun e -> e.Array.Contains(item, ?nullsEqual=nullsEqual))

    /// <summary> Check if array contains a literal string. </summary>
    member _.Contains(item: string, ?nullsEqual: bool) =
        apply (fun e -> e.Array.Contains(item, ?nullsEqual=nullsEqual))

    /// <summary> Check if array contains a literal int. </summary>
    member _.Contains(item: int, ?nullsEqual: bool) =
        apply (fun e -> e.Array.Contains(item, ?nullsEqual=nullsEqual))

    // --- Operations ---

    member _.Unique(?stable: bool) = 
        apply (fun e -> e.Array.Unique(?stable=stable))

    member _.Join(separator: string, ?ignoreNulls: bool) =
        apply (fun e -> e.Array.Join(separator, ?ignoreNulls=ignoreNulls))

    member _.Sort(?descending: bool, ?nullsLast: bool, ?maintainOrder: bool) =
        apply (fun e -> 
            e.Array.Sort(
                ?descending=descending, 
                ?nullsLast=nullsLast, 
                ?maintainOrder=maintainOrder
            )
        )

    member _.Reverse() = apply (fun e -> e.Array.Reverse())

    member _.ArgMin() = apply (fun e -> e.Array.ArgMin())
    member _.ArgMax() = apply (fun e -> e.Array.ArgMax())

    member _.Explode(?emptyAsNull:bool,?keepNulls:bool) = apply (fun e -> e.Array.Explode(?emptyAsNull=emptyAsNull,?keepNulls=keepNulls))

    // --- Indexing ---

    member _.Get(index: Expr, ?nullOnOob: bool) =
        apply (fun e -> e.Array.Get(index, ?nullOnOob=nullOnOob))

    member _.Get(index: int, ?nullOnOob: bool) =
        apply (fun e -> e.Array.Get(index, ?nullOnOob=nullOnOob))

    // --- Conversion ---

    /// <summary> Convert to variable length List. </summary>
    member _.ToList() = apply (fun e -> e.Array.ToList())

    /// <summary> Convert to Struct. </summary>
    member _.ToStruct() = apply (fun e -> e.Array.ToStruct())