namespace Polars.FSharp

/// <summary> Operations on a Map series (map[key, value]). Access via <c>series.MapOps</c>. </summary>
type SeriesMapNameSpace(parent: Series) =

    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary> The entries of every map as a list of {key, value} structs, in entry order. </summary>
    member _.Entries() = apply (fun e -> e.MapOps.Entries())

    /// <summary> The keys of every map as a list, in entry order. </summary>
    member _.Keys() = apply (fun e -> e.MapOps.Keys())

    /// <summary> The values of every map as a list, in entry order. </summary>
    member _.Values() = apply (fun e -> e.MapOps.Values())

    /// <summary> The number of entries of every map (UInt32). </summary>
    member _.Len() = apply (fun e -> e.MapOps.Len())

    /// <summary> Whether every map holds the key (an expression). </summary>
    member _.ContainsKey(key: Expr) = apply (fun e -> e.MapOps.ContainsKey key)

    /// <summary> Whether every map holds the text key. </summary>
    member _.ContainsKey(key: string) = apply (fun e -> e.MapOps.ContainsKey key)

    /// <summary> Whether every map holds the integer key. </summary>
    member _.ContainsKey(key: int) = apply (fun e -> e.MapOps.ContainsKey key)

    /// <summary> Whether every map holds the integer key. </summary>
    member _.ContainsKey(key: int64) = apply (fun e -> e.MapOps.ContainsKey key)

    /// <summary> The value of the key (an expression) in every map; null where it is missing. </summary>
    member _.Get(key: Expr) = apply (fun e -> e.MapOps.Get key)

    /// <summary> The value of the text key in every map; null where it is missing. </summary>
    member _.Get(key: string) = apply (fun e -> e.MapOps.Get key)

    /// <summary> The value of the integer key in every map; null where it is missing. </summary>
    member _.Get(key: int) = apply (fun e -> e.MapOps.Get key)

    /// <summary> The value of the integer key in every map; null where it is missing. </summary>
    member _.Get(key: int64) = apply (fun e -> e.MapOps.Get key)
