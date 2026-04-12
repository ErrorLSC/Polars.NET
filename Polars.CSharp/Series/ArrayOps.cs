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
    /// <summary>
    /// Get the first value of the sub-arrays.
    /// </summary>
    public Series First() => Get(0,true);
    /// <summary>
    /// Get the last value of the sub-arrays.
    /// </summary>
    public Series Last() => Get(-1,true);
    /// <summary>
    /// Return the number of elements in each array.
    /// </summary>
    public Series Len() => Apply(e => e.Array.Len());
    
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
    /// <summary>
    /// Count the number of unique values in every sub-arrays.
    /// </summary>
    public Series NUnique() => Apply(e => e.Array.NUnique());
    /// <summary>
    /// Run any polars aggregation expression against the arrays’ elements.
    /// </summary>
    /// <returns></returns>
    public Series Agg(Expr expr) => Apply(e => e.Array.Agg(expr));
    /// <summary>
    /// Count how often the value produced by element occurs.
    /// </summary>
    /// <param name="element">An expression that produces a single value</param>
    /// <returns></returns>
    public Series CountMatches(Expr element) => Apply(e => e.Array.CountMatches(element));
    /// <summary>
    /// Run any polars expression against the arrays’ elements.
    /// </summary>
    /// <param name="expr">Expression to run. Note that you can select an element with pl.element()</param>
    /// <param name="asList">Collect the resulting data as a list. This allows for expressions which output a variable amount of data.</param>
    /// <returns></returns>
    public Series Eval(Expr expr, bool asList=false) => Apply(e => e.Array.Eval(expr,asList));
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
    public Series Get(long index, bool nullOnOob = true) 
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
    public Series ToStruct(params string[] fields) => Apply(e => e.Array.ToStruct(fields));
    /// <inheritdoc cref="ArrayOps.ToStruct(Func{int,string},int)"/>
    public Series ToStruct(Func<int,string>nameGenerator,int fieldCount) => Apply(e => e.Array.ToStruct(nameGenerator,fieldCount));
    /// <summary>
    /// Cast to variable-size List.
    /// </summary>
    public Series ToList() => Apply(e => e.Array.ToList());

    // --- Logic / Set ---

    /// <summary>Check if sub-array contains a specific item.</summary>
    public Series Contains(Expr item, bool nullsEqual = false) 
        => Apply(e => e.Array.Contains(item, nullsEqual));

    /// <summary>Get unique elements in every sub-array.</summary>
    public Series Unique(bool stable = false) => Apply(e => e.Array.Unique(stable));
}