namespace Polars.FSharp

type SeriesListNameSpace(parent: Series) =
    
    // Unary helper
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    member _.First() = apply (fun e -> e.List.First())
    member _.Last() = apply (fun e -> e.List.Last())
    member _.Get(index: int,?nullOnOob) = apply (fun e -> e.List.Get(index,?nullOnOob=nullOnOob))
    member _.Join(sep: string,?ignoreNulls) = apply (fun e -> e.List.Join(sep,?ignoreNulls=ignoreNulls))
    member _.Gather(indices,?nullOnOob) = apply (fun e->e.List.Gather(indices,?nullOnOob=nullOnOob))
    member _.GatherEvery(n,?offset) = apply (fun e->e.List.GatherEvery(n,?offset=offset))
    member _.Slice(offset,?length) = apply (fun e->e.List.Slice(offset,?length=length))
    member _.Head(?n) = apply (fun e->e.List.Head(?n=n))
    member _.Tail(?n) = apply (fun e->e.List.Tail(?n=n))
    member _.Agg(expr) = apply (fun e->e.List.Agg(expr))
    member _.Shift(?n) = apply (fun e->e.List.Shift(?n=n))
    member _.Diff(?n,?nullBehavior) = apply (fun e->e.List.Diff(?n=n,?nullBehavior=nullBehavior))
    member _.SampleN(?n,?withReplacement,?shuffle,?seed) = apply (fun e->e.List.SampleN(?n=n,?withReplacement=withReplacement,?shuffle=shuffle,?seed=seed))
    member _.SampleFrac(fraction,?withReplacement,?shuffle,?seed) = apply (fun e->e.List.SampleFrac(fraction,?withReplacement=withReplacement,?shuffle=shuffle,?seed=seed))
    member _.SetUnion(other) = apply (fun e->e.List.SetUnion(other))
    member _.SetIntersection(other) = apply (fun e->e.List.SetIntersection(other))
    member _.SetSymmetricDifference(other) = apply (fun e->e.List.SetSymmetricDifference(other))
    member _.SetDifference(other) = apply (fun e->e.List.SetDifference(other))
    member _.Len() = apply (fun e -> e.List.Len())
    member _.Sum() = apply (fun e -> e.List.Sum())
    member _.Min() = apply (fun e -> e.List.Min())
    member _.Max() = apply (fun e -> e.List.Max())
    member _.Mean() = apply (fun e -> e.List.Mean())
    member _.Median() = apply (fun e -> e.List.Median())
    member _.All(?ignoreNulls) = apply (fun e->e.List.All(?ignoreNulls=ignoreNulls))
    member _.Any(?ignoreNulls) = apply (fun e->e.List.Any(?ignoreNulls=ignoreNulls))
    member _.DropNulls() = apply (fun e->e.List.DropNulls())
    member _.NUnique() = apply (fun e->e.List.NUnique())
    member _.ArgMax() = apply (fun e-> e.List.ArgMax())
    member _.ArgMin() = apply (fun e-> e.List.ArgMin())
    member _.Std(?ddof) = apply (fun e-> e.List.Std(?ddof=ddof))
    member _.Var(?ddof) = apply (fun e-> e.List.Var(?ddof=ddof))
    member _.Reverse() = apply (fun e -> e.List.Reverse())
    member _.Eval(expr) = apply (fun e-> e.List.Eval(expr))
    member _.Unique(?maintainOrder) = apply (fun e->e.List.Unique(?maintainOrder=maintainOrder))
    member _.Sort(?descending, ?nullsLast, ?maintainOrder) =
        apply (fun e -> e.List.Sort(?descending=descending, ?nullsLast=nullsLast, ?maintainOrder=maintainOrder))
    /// <summary>
    /// Concat with another Series.
    /// Logic: [this_val, other_val]
    /// </summary>
    member _.Concat(other: Series) =
        parent.ApplyBinaryExpr(other, (fun l r -> l.List.Concat r))
    member _.Contains(item:Expr) = apply (fun e->e.List.Contains item)
    member _.Contains(item: int) = apply (fun e -> e.List.Contains item)
    member _.Contains(item: string) = apply (fun e -> e.List.Contains item)
    member _.Explode(?emptyAsNull,?keepNulls) = apply (fun e->e.List.Explode(?emptyAsNull=emptyAsNull,?keepNulls=keepNulls))
    member _.ToArray(width) = apply (fun e -> e.List.ToArray(width))
    member _.ToStruct(fields:seq<string>) = apply (fun e -> e.List.ToStruct(fields))
    member _.ToStruct(nameGenerator: int -> string,fieldCount:int) =
        apply (fun e-> e.List.ToStruct(nameGenerator,fieldCount))
    member _.ToStruct(upperBound:int) = apply (fun e->e.List.ToStruct(upperBound))
