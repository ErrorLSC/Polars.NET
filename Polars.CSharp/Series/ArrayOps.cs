namespace Polars.CSharp;

/// <summary>
/// Wrapper for Array (Fixed-Size List) operations on a Series.
/// </summary>
public readonly struct SeriesArrayOps
{
    private readonly Series _series;
    internal SeriesArrayOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Polars.Col(_series.Name)));

    // --- Aggregations ---
    
    /// <summary>Compute the max value of every sub-array.</summary>
    public Series Max() => Apply(e => e.Array.Max());

    /// <summary>Compute the min value of every sub-array.</summary>
    public Series Min() => Apply(e => e.Array.Min());

    /// <summary>Compute the sum of every sub-array.</summary>
    public Series Sum() => Apply(e => e.Array.Sum());

    /// <summary>Compute the mean of every sub-array.</summary>
    public Series Mean() => Apply(e => e.Array.Mean());

    /// <summary>Compute the median of every sub-array.</summary>
    public Series Median() => Apply(e => e.Array.Median());

    /// <summary>Compute the standard deviation of every sub-array.</summary>
    public Series Std(byte ddof = 1) => Apply(e => e.Array.Std(ddof));

    /// <summary>Compute the variance of every sub-array.</summary>
    public Series Var(byte ddof = 1) => Apply(e => e.Array.Var(ddof));

    // --- Boolean ---

    /// <summary>Check if any element in the sub-array is true.</summary>
    public Series Any() => Apply(e => e.Array.Any());

    /// <summary>Check if all elements in the sub-array are true.</summary>
    public Series All() => Apply(e => e.Array.All());

    // --- Sort & Search ---

    /// <summary>Sort elements in every sub-array.</summary>
    public Series Sort(bool descending = false, bool nullsLast = false, bool maintainOrder = false) 
        => Apply(e => e.Array.Sort(descending, nullsLast, maintainOrder));

    /// <summary>Reverse elements in every sub-array.</summary>
    public Series Reverse() => Apply(e => e.Array.Reverse());

    /// <summary>Get the index of the minimum value in every sub-array.</summary>
    public Series ArgMin() => Apply(e => e.Array.ArgMin());

    /// <summary>Get the index of the maximum value in every sub-array.</summary>
    public Series ArgMax() => Apply(e => e.Array.ArgMax());

    // --- Structure ---

    /// <summary>Get element at index from every sub-array.</summary>
    public Series Get(int index, bool nullOnOob = true) 
        => Apply(e => e.Array.Get(index, nullOnOob));

    /// <summary>Join elements with a separator.</summary>
    public Series Join(string separator, bool ignoreNulls = true) 
        => Apply(e => e.Array.Join(separator, ignoreNulls));

    /// <summary>
    /// Explode the array column into multiple rows.
    /// The resulting Series will be longer than the original.
    /// </summary>
    public Series Explode(bool emptyAsNull = true, bool keepNulls = true) => Apply(e => e.Array.Explode(emptyAsNull,keepNulls));

    /// <summary>
    /// Convert array to struct. Useful for splitting embeddings into feature columns.
    /// </summary>
    public Series ToStruct() => Apply(e => e.Array.ToStruct());

    /// <summary>
    /// Cast to variable-size List.
    /// </summary>
    public Series ToList() => Apply(e => e.Array.ToList());

    // --- Logic / Set ---

    /// <summary>Check if sub-array contains a specific item.</summary>
    public Series Contains(int item, bool nullsEqual = false) 
        => Apply(e => e.Array.Contains(item, nullsEqual));
    /// <summary>Check if sub-array contains a specific item.</summary>
    public Series Contains(double item, bool nullsEqual = false) 
        => Apply(e => e.Array.Contains(item, nullsEqual));
    /// <summary>Check if sub-array contains a specific item.</summary>   
    public Series Contains(Expr item, bool nullsEqual = false) 
        => Apply(e => e.Array.Contains(item, nullsEqual));

    /// <summary>Get unique elements in every sub-array.</summary>
    public Series Unique(bool stable = false) => Apply(e => e.Array.Unique(stable));
}