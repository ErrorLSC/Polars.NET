namespace Polars.FSharp

type SeriesListNameSpace(parent: Series) =

    // Unary helper
    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary> Get the first element of the list. </summary>
    member _.First() = apply (fun e -> e.List.First())

    /// <summary> Get the last element of the list. </summary>
    member _.Last() = apply (fun e -> e.List.Last())

    /// <summary> Get the element at the given index. </summary>
    member _.Get(index: int, ?nullOnOob) =
        apply (fun e -> e.List.Get(index, ?nullOnOob = nullOnOob))

    /// <summary> Join list elements with separator. </summary>
    member _.Join(sep: string, ?ignoreNulls) =
        apply (fun e -> e.List.Join(sep, ?ignoreNulls = ignoreNulls))

    /// <summary> Gather elements by indices. </summary>
    member _.Gather(indices, ?nullOnOob) =
        apply (fun e -> e.List.Gather(indices, ?nullOnOob = nullOnOob))

    /// <summary> Gather every n-th element. </summary>
    member _.GatherEvery(n, ?offset) =
        apply (fun e -> e.List.GatherEvery(n, ?offset = offset))

    /// <summary> Slice the list. </summary>
    member _.Slice(offset, ?length) =
        apply (fun e -> e.List.Slice(offset, ?length = length))

    /// <summary> Get the first n elements of the list. </summary>
    member _.Head(?n) = apply (fun e -> e.List.Head(?n = n))

    /// <summary> Get the last n elements of the list. </summary>
    member _.Tail(?n) = apply (fun e -> e.List.Tail(?n = n))

    /// <summary> Aggregate elements in list. </summary>
    member _.Agg(expr) = apply (fun e -> e.List.Agg(expr))

    /// <summary> Shift elements in list. </summary>
    member _.Shift(?n) = apply (fun e -> e.List.Shift(?n = n))

    /// <summary> Diff elements in list. </summary>
    member _.Diff(?n, ?nullBehavior) =
        apply (fun e -> e.List.Diff(?n = n, ?nullBehavior = nullBehavior))

    /// <summary> Sample n elements from list. </summary>
    member _.SampleN(?n, ?withReplacement, ?shuffle, ?seed) =
        apply (fun e -> e.List.SampleN(?n = n, ?withReplacement = withReplacement, ?shuffle = shuffle, ?seed = seed))

    /// <summary> Sample fraction of elements from list. </summary>
    member _.SampleFrac(fraction, ?withReplacement, ?shuffle, ?seed) =
        apply (fun e ->
            e.List.SampleFrac(fraction, ?withReplacement = withReplacement, ?shuffle = shuffle, ?seed = seed))

    /// <summary> Set union of lists. </summary>
    member _.SetUnion(other) = apply (fun e -> e.List.SetUnion(other))

    /// <summary> Set intersection of lists. </summary>
    member _.SetIntersection(other) =
        apply (fun e -> e.List.SetIntersection(other))

    /// <summary> Set symmetric difference of lists. </summary>
    member _.SetSymmetricDifference(other) =
        apply (fun e -> e.List.SetSymmetricDifference(other))

    /// <summary> Set difference of lists. </summary>
    member _.SetDifference(other) =
        apply (fun e -> e.List.SetDifference(other))

    /// <summary> Get the length of the list. </summary>
    member _.Len() = apply (fun e -> e.List.Len())

    /// <summary> Get the sum of the list. </summary>
    member _.Sum() = apply (fun e -> e.List.Sum())

    /// <summary> Get the minimum of the list. </summary>
    member _.Min() = apply (fun e -> e.List.Min())

    /// <summary> Get the maximum of the list. </summary>
    member _.Max() = apply (fun e -> e.List.Max())

    /// <summary> Get the mean of the list. </summary>
    member _.Mean() = apply (fun e -> e.List.Mean())

    /// <summary> Get the median of the list. </summary>
    member _.Median() = apply (fun e -> e.List.Median())

    /// <summary> Check if all elements in list are true. </summary>
    member _.All(?ignoreNulls) =
        apply (fun e -> e.List.All(?ignoreNulls = ignoreNulls))

    /// <summary> Check if any element in list is true. </summary>
    member _.Any(?ignoreNulls) =
        apply (fun e -> e.List.Any(?ignoreNulls = ignoreNulls))

    /// <summary> Drop nulls from list. </summary>
    member _.DropNulls() = apply (fun e -> e.List.DropNulls())

    /// <summary> Get the number of unique elements in list. </summary>
    member _.NUnique() = apply (fun e -> e.List.NUnique())

    /// <summary> Get the index of the maximum element in list. </summary>
    member _.ArgMax() = apply (fun e -> e.List.ArgMax())

    /// <summary> Get the index of the minimum element in list. </summary>
    member _.ArgMin() = apply (fun e -> e.List.ArgMin())

    /// <summary> Get the standard deviation of the list. </summary>
    member _.Std(?ddof) =
        apply (fun e -> e.List.Std(?ddof = ddof))

    /// <summary> Get the variance of the list. </summary>
    member _.Var(?ddof) =
        apply (fun e -> e.List.Var(?ddof = ddof))

    /// <summary> Reverse the list. </summary>
    member _.Reverse() = apply (fun e -> e.List.Reverse())

    /// <summary> Evaluate an expression on the list. </summary>
    member _.Eval(expr) = apply (fun e -> e.List.Eval(expr))

    /// <summary> Get the unique elements of the list. </summary>
    member _.Unique(?maintainOrder) =
        apply (fun e -> e.List.Unique(?maintainOrder = maintainOrder))

    /// <summary> Sort the list. </summary>
    member _.Sort(?descending, ?nullsLast, ?maintainOrder) =
        apply (fun e -> e.List.Sort(?descending = descending, ?nullsLast = nullsLast, ?maintainOrder = maintainOrder))

    /// <summary>
    /// Concat with another Series.
    /// Logic: [this_val, other_val]
    /// </summary>
    member _.Concat(other: Series) =
        parent.ApplyBinaryExpr(other, (fun l r -> l.List.Concat r))

    /// <summary> Check if list contains item. </summary>
    member _.Contains(item: Expr) = apply (fun e -> e.List.Contains item)

    /// <summary> Check if list contains item. </summary>
    member _.Contains(item: int) = apply (fun e -> e.List.Contains item)

    /// <summary> Check if list contains item. </summary>
    member _.Contains(item: string) = apply (fun e -> e.List.Contains item)

    /// <summary> Explode list. </summary>
    member _.Explode(?emptyAsNull, ?keepNulls) =
        apply (fun e -> e.List.Explode(?emptyAsNull = emptyAsNull, ?keepNulls = keepNulls))

    /// <summary> Convert list to array. </summary>
    member _.ToArray(width) = apply (fun e -> e.List.ToArray width)

    /// <summary> Convert list to struct. </summary>
    member _.ToStruct(fields: seq<string>) = apply (fun e -> e.List.ToStruct fields)

    /// <summary> Convert list to struct. </summary>
    member _.ToStruct(nameGenerator: int -> string, fieldCount: int) =
        apply (fun e -> e.List.ToStruct(nameGenerator, fieldCount))

    /// <summary> Convert list to struct. </summary>
    member _.ToStruct(upperBound: int) =
        apply (fun e -> e.List.ToStruct upperBound)
