namespace Polars.CSharp;

/// <summary>
/// Series List Ops Namespace
/// </summary>
public readonly struct SeriesListOps
{
    private readonly Series _series;
    internal SeriesListOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Polars.Col(_series.Name)));

    /// <summary>
    /// Get the length of the arrays.
    /// </summary>
    public Series Len() => Apply(e => e.List.Len());

    /// <summary>
    /// Get the first element.
    /// </summary>
    public Series First() => Apply(e => e.List.First());

    /// <summary>
    /// Get the element at the given index.
    /// </summary>
    public Series Get(int index) => Apply(e => e.List.Get(index));

    /// <summary>
    /// Join elements with a separator.
    /// </summary>
    public Series Join(string separator) => Apply(e => e.List.Join(separator));

    /// <summary>
    /// Calculate the sum of the list elements (element-wise).
    /// </summary>
    public Series Sum() => Apply(e => e.List.Sum());

    /// <summary>
    /// Calculate the min of the list elements.
    /// </summary>
    public Series Min() => Apply(e => e.List.Min());

    /// <summary>
    /// Calculate the max of the list elements.
    /// </summary>
    public Series Max() => Apply(e => e.List.Max());

    /// <summary>
    /// Calculate the mean of the list elements.
    /// </summary>
    public Series Mean() => Apply(e => e.List.Mean());

    /// <summary>
    /// Sort the arrays in the list.
    /// </summary>
    public Series Sort(bool descending = false,bool nullsLast=false,bool maintainOrder= false) 
        => Apply(e => e.List.Sort(descending,nullsLast,maintainOrder));

    /// <summary>
    /// Check if the list contains the given item.
    /// </summary>
    public Series Contains(int item, bool nullsEqual = false) => Apply(e => e.List.Contains(item, nullsEqual));
    /// <summary>
    /// Check if the list contains the given item.
    /// </summary>
    public Series Contains(string item, bool nullsEqual= false) => Apply(e => e.List.Contains(item, nullsEqual));
    /// <summary>
    /// Concat this list series with another list series.
    /// Result is a new Series with the lists concatenated.
    /// </summary>
    public Series Concat(Series other)
        => _series.ApplyBinaryExpr(other, (left, right) => left.List.Concat(right));
    /// <summary>Reverse elements in list.</summary>
    public Series Reverse() => Apply(e => e.List.Reverse());
}