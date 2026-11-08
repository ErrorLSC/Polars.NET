namespace Polars.FSharp

/// <summary>
/// Namespace containing array operations for a <see cref="Series"/>.
/// </summary>
type SeriesArrayNameSpace(parent: Series) =

    let apply (op: Expr -> Expr) =
        let expr = Expr.Col parent.Name |> op
        parent.ApplyExpr expr

    /// <summary> Compute the sum of values in each array element. </summary>
    member _.Sum() = apply (fun e -> e.Array.Sum())

    /// <summary> Compute the minimum value in each array element. </summary>
    member _.Min() = apply (fun e -> e.Array.Min())

    /// <summary> Compute the maximum value in each array element. </summary>
    member _.Max() = apply (fun e -> e.Array.Max())

    /// <summary> Compute the mean of values in each array element. </summary>
    member _.Mean() = apply (fun e -> e.Array.Mean())

    /// <summary> Compute the length (number of elements) of each array element. </summary>
    member _.Len() = apply (fun e -> e.Array.Len())

    /// <summary> Compute the median of values in each array element. </summary>
    member _.Median() = apply (fun e -> e.Array.Median())

    /// <summary> Count the number of unique values in each array element. </summary>
    member _.NUnique() = apply (fun e -> e.Array.NUnique())

    /// <summary> Get the first element of each array. </summary>
    member _.First() = apply (fun e -> e.Array.First())

    /// <summary> Get the last element of each array. </summary>
    member _.Last() = apply (fun e -> e.Array.Last())

    /// <summary> Run any Polars aggregation expression on each array element. </summary>
    member _.Agg expr = apply (fun e -> e.Array.Agg expr)

    /// <summary> Run any Polars expression on each array element. </summary>
    member _.Eval(expr, ?asList) =
        apply (fun e -> e.Array.Eval(expr, ?asList = asList))

    /// <summary> Count how often the given expression matches in each array element. </summary>
    member _.CountMatches expr =
        apply (fun e -> e.Array.CountMatches expr)

    /// <summary> Compute the standard deviation of each array element. </summary>
    member _.Std(?ddof: int) =
        apply (fun e -> e.Array.Std(?ddof = ddof))

    /// <summary> Compute the variance of each array element. </summary>
    member _.Var(?ddof: int) =
        apply (fun e -> e.Array.Var(?ddof = ddof))

    /// <summary> Evaluate whether any boolean value is true for each array element. </summary>
    member _.Any(?ignoreNulls) =
        apply (fun e -> e.Array.Any(?ignoreNulls = ignoreNulls))

    /// <summary> Evaluate whether all boolean values are true for each array element. </summary>
    member _.All(?ignoreNulls) =
        apply (fun e -> e.Array.All(?ignoreNulls = ignoreNulls))

    /// <summary> Check if array contains an Item (Expr). </summary>
    member _.Contains(item: Expr, ?nullsEqual: bool) =
        apply (fun e -> e.Array.Contains(item, ?nullsEqual = nullsEqual))

    /// <summary> Check if array contains a literal string. </summary>
    member _.Contains(item: string, ?nullsEqual: bool) =
        apply (fun e -> e.Array.Contains(item, ?nullsEqual = nullsEqual))

    /// <summary> Check if array contains a literal int. </summary>
    member _.Contains(item: int, ?nullsEqual: bool) =
        apply (fun e -> e.Array.Contains(item, ?nullsEqual = nullsEqual))

    /// <summary> Get unique values from each array element. </summary>
    member _.Unique(?maintainOrder: bool) =
        apply (fun e -> e.Array.Unique(?maintainOrder = maintainOrder))

    /// <summary> Join all string items in the array into a single string using a delimiter. </summary>
    member _.Join(separator: string, ?ignoreNulls: bool) =
        apply (fun e -> e.Array.Join(separator, ?ignoreNulls = ignoreNulls))

    /// <summary> Sort the elements of each array. </summary>
    member _.Sort(?descending: bool, ?nullsLast: bool, ?maintainOrder: bool) =
        apply (fun e -> e.Array.Sort(?descending = descending, ?nullsLast = nullsLast, ?maintainOrder = maintainOrder))

    /// <summary> Reverse the elements of each array. </summary>
    member _.Reverse() = apply (fun e -> e.Array.Reverse())

    /// <summary> Retrieve the index of the minimum value in each array element. </summary>
    member _.ArgMin() = apply (fun e -> e.Array.ArgMin())

    /// <summary> Retrieve the index of the maximum value in each array element. </summary>
    member _.ArgMax() = apply (fun e -> e.Array.ArgMax())

    /// <summary> Explode the array into multiple rows. </summary>
    member _.Explode(?emptyAsNull: bool, ?keepNulls: bool) =
        apply (fun e -> e.Array.Explode(?emptyAsNull = emptyAsNull, ?keepNulls = keepNulls))

    /// <summary> Get the element at the given index from each array. </summary>
    member _.Get(index: int, ?nullOnOob: bool) =
        apply (fun e -> e.Array.Get(index, ?nullOnOob = nullOnOob))

    /// <summary> Convert to variable length List. </summary>
    member _.ToList() = apply (fun e -> e.Array.ToList())

    /// <summary> Convert to Struct. </summary>
    member _.ToStruct() = apply (fun e -> e.Array.ToStruct())
