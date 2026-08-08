#pragma warning disable 1573
using Apache.Arrow;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;


/// <summary>
/// Series List Ops Namespace
/// </summary>
public readonly struct SeriesListOps
{
    private readonly Series _series;
    internal SeriesListOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Pl.Col(_series.Name)));
    /// <summary>
    /// Get the element at the given index.
    /// </summary>
    public Series Get(long index,bool nullOnOob=false) => Apply(e => e.List.Get(index,nullOnOob));
    /// <summary>
    /// Take sublists by multiple indices.The indices may be defined in a single column, or by sublists in another column of dtype List.
    /// </summary>
    /// <param name="indices">Indices to return per sublist</param>
    /// <param name="nullOnOob">Behavior if an index is out of bounds: True -> set as null False -> raise an error Note that defaulting to raising an error is much cheaper</param>
    public Series Gather(IntoExprColumn indices,bool nullOnOob=false) => Apply(e=>e.List.Gather(indices,nullOnOob));
    /// <inheritdoc cref="Gather(IntoExprColumn,bool)"/>
    public Series Gather(ReadOnlySpan<int> indices, bool maintainOrder = false)
    {
        Expr indexExpr = Pl.Lit(indices);
        return Apply(e => e.List.Gather(indexExpr, maintainOrder));
    }
    /// <summary>
    /// Take every n-th value start from offset in sublists.
    /// </summary>
    /// <param name="n">Gather every n-th element.</param>
    /// <param name="offset">Starting Index</param>
    /// <returns></returns>
    public Series GatherEvery(IntoExprColumn n,IntoExprColumn offset) => Apply(e=>e.List.GatherEvery(n,offset));
    /// <inheritdoc cref="GatherEvery(IntoExprColumn,IntoExprColumn)"/>
    public Series GatherEvery(IntoExprColumn n) => GatherEvery(n,0);
    /// <inheritdoc cref="ListOps.Slice"/>
    public Series Slice(IntoExprColumn offset, IntoExprColumn? length=null) => Apply(e=>e.List.Slice(offset,length));
    /// <summary>
    /// Slice the first n values of every sublist.
    /// </summary>
    /// <param name="n">Number of values to return for each sublist.</param>
    /// <returns></returns>
    public Series Head(IntoExprColumn n) =>Apply(e=>e.List.Head(n));
    /// <inheritdoc cref="Head(IntoExprColumn)"/>
    public Series Head(long n=5) =>Apply(e=>e.List.Head(n));
    /// <summary>
    /// Slice the last n values of every sublist.
    /// </summary>
    /// <param name="n">Number of values to return for each sublist.</param>
    /// <returns></returns>
    public Series Tail(IntoExprColumn n) =>Apply(e=>e.List.Tail(n));
    /// <inheritdoc cref="Tail(IntoExprColumn)"/>
    public Series Tail(long n=5) =>Apply(e=>e.List.Tail(n));
    /// <summary>
    /// Shift list values by the given number of indices.
    /// </summary>
    /// <param name="n">Number of indices to shift forward. 
    /// If a negative value is passed, values are shifted in the opposite direction instead.</param>
    public Series Shift(Expr n) => Apply(e=>e.List.Shift(n));
    /// <inheritdoc cref="Shift(Expr)"/>
    public Series Shift() => Shift(1);
    /// <summary>
    /// Calculate the first discrete difference between shifted items of every sublist.
    /// </summary>
    /// <param name="n">Number of slots to shift.</param>
    /// <param name="nullBehavior">How to handle null values.</param>
    public Series Diff(long n=1,NullBehavior nullBehavior=NullBehavior.Ignore) => Apply(e=>e.List.Diff(n,nullBehavior));
    /// <summary>
    /// Sample from this list.
    /// </summary>
    /// <param name="n">Number of items to return.Defaults to 1</param>
    /// <param name="withReplacement">Allow values to be sampled more than once.</param>
    /// <param name="shuffle">Shuffle the order of sampled data points.</param>
    /// <param name="seed">Seed for the random number generator. 
    /// If set to None (default), a random seed is generated for each sample operation.</param>
    public Series SampleN(IntoExprColumn n,bool withReplacement=false,bool shuffle=false,ulong? seed=null)
        => Apply(e=>e.List.SampleN(n,withReplacement,shuffle,seed));
    /// <inheritdoc cref="SeriesListOps.SampleN(IntoExprColumn,bool,bool,ulong?)"/>
    public Series SampleN(bool withReplacement=false,bool shuffle=false,ulong? seed=null)
        => SampleN(1,withReplacement,shuffle,seed);
    /// <inheritdoc cref="SeriesListOps.SampleN(IntoExprColumn,bool,bool,ulong?)"/>
    /// <param name="fraction">Fraction of items to return. </param>
    public Series SampleFrac(IntoExprColumn fraction,bool withReplacement=false,bool? shuffle=null,ulong? seed=null)
        =>Apply(e=>e.List.SampleFrac(fraction,withReplacement,shuffle,seed));
    /// <summary>
    /// Compute the SET UNION between the elements in this list and the elements of other.
    /// </summary>
    /// <param name="other">Right hand side of the set operation.</param>
    public Series SetUnion(IntoExprColumn other) => Apply(e=>e.List.SetUnion(other));
    /// <inheritdoc cref="SetUnion"/>
    public Series SetUnion<T>(ReadOnlySpan<T> other) => SetUnion(Pl.Lit(other).Implode());
    /// <inheritdoc cref="SetUnion"/>
    public Series SetUnion<T>(IEnumerable<T> other) => SetUnion(Pl.Lit(other).Implode());
    /// <summary>
    /// Compute the SET DIFFERENCE between the elements in this list and the elements of other.
    /// </summary>
    /// <param name="other">Right hand side of the set operation.</param>
    public Series SetDifference(IntoExprColumn other) => Apply(e=>e.List.SetDifference(other));
    /// <inheritdoc cref="SetDifference"/>
    public Series SetDifference<T>(ReadOnlySpan<T> other) => SetDifference(Pl.Lit(other).Implode());
    /// <inheritdoc cref="SetDifference"/>
    public Series SetDifference<T>(IEnumerable<T> other) => SetDifference(Pl.Lit(other).Implode());
    /// <summary>
    /// Compute the SET INTERSECTION between the elements in this list and the elements of other.
    /// </summary>
    /// <param name="other">Right hand side of the set operation.</param>
    public Series SetIntersection(IntoExprColumn other) => Apply(e=>e.List.SetIntersection(other));
    /// <inheritdoc cref="SetIntersection"/>
    public Series SetIntersection<T>(ReadOnlySpan<T> other) => SetIntersection(Pl.Lit(other).Implode());
    /// <inheritdoc cref="SetIntersection"/>
    public Series SetIntersection<T>(IEnumerable<T> other) => SetIntersection(Pl.Lit(other).Implode());
    /// <summary>
    /// Compute the SET SYMMETRIC DIFFERENCE between the elements in this list and the elements of other.
    /// </summary>
    /// <param name="other">Right hand side of the set operation.</param>
    public Series SetSymmetricDifference(IntoExprColumn other) => Apply(e=>e.List.SetSymmetricDifference(other));
    /// <inheritdoc cref="SetSymmetricDifference"/>
    public Series SetSymmetricDifference<T>(ReadOnlySpan<T> other) => SetSymmetricDifference(Pl.Lit(other).Implode());
    /// <inheritdoc cref="SetSymmetricDifference"/>
    public Series SetSymmetricDifference<T>(IEnumerable<T> other) => SetSymmetricDifference(Pl.Lit(other).Implode());
    /// <inheritdoc cref="ListOps.Join"/>
    public Series Join(string separator,bool ignoreNulls=true) => Apply(e => e.List.Join(separator,ignoreNulls));
    /// <summary>
    /// Run any polars aggregation expression against the list’ elements.
    /// </summary>
    /// <param name="expr">Expression to run. Note that you can select an element with Pl.Element().</param>
    public Series Agg(Expr expr) => Apply(e=>e.List.Agg(expr));
    /// <summary>
    /// Run any polars expression against the lists’ elements.
    /// </summary>
    /// <param name="expr">Expression to run. Note that you can select an element with Pl.Element().</param>
    public Series Eval(Expr expr) => Apply(e=>e.List.Eval(expr));
    /// <summary>
    /// Get the length of the sublists.
    /// </summary>
    public Series Len() => Apply(e => e.List.Len());

    /// <summary>
    /// Get the first value of the sublists.
    /// </summary>
    public Series First() => Apply(e => e.List.First());
    /// <summary>
    /// Get the first value of the sublists.
    /// </summary>
    /// <returns></returns>
    public Series Last() => Apply(e => e.List.Last());
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
    /// Calculate the median of the list elements.
    /// </summary>
    public Series Median() => Apply(e => e.List.Median());
    /// <summary>
    /// Evaluate whether all boolean values in a list are true.
    /// </summary>
    public Series All() => Apply(e => e.List.All());
    /// <summary>
    /// Evaluate whether any boolean value in a list is true.
    /// </summary>
    public Series Any() => Apply(e => e.List.Any());
    /// <summary>
    /// Drop all null values in the list.
    /// The original order of the remaining elements is preserved.
    /// </summary>
    public Series DropNulls() => Apply(e => e.List.DropNulls());
    /// <summary>
    /// Count the number of unique values in every sub-lists.
    /// </summary>
    public Series NUnique() => Apply(e => e.List.NUnique());
    /// <summary>
    /// Retrieve the index of the maximum value in every sublist.
    /// </summary>
    public Series ArgMax() => Apply(e => e.List.ArgMax());
    /// <summary>
    /// Retrieve the index of the minimum value in every sublist.
    /// </summary>
    public Series ArgMin() => Apply(e => e.List.ArgMin());
    /// <inheritdoc cref="Expr.Std"/>
    public Series Std(byte ddof=1) => Apply(e => e.List.Std(ddof));
    /// <inheritdoc cref="Expr.Var"/>
    public Series Var(byte ddof=1) => Apply(e => e.List.Var(ddof));
    /// <inheritdoc cref="ListOps.Sort"/>
    public Series Sort(bool descending = false,bool nullsLast=false,bool maintainOrder= false) 
        => Apply(e => e.List.Sort(descending,nullsLast,maintainOrder));
    /// <summary>
    /// Get the unique/distinct values in the list.
    /// </summary>
    /// <param name="maintainOrder">Maintain order of data. This requires more work.</param>
    public Series Unique(bool maintainOrder=false) => Apply(e => e.List.Unique(maintainOrder));

    /// <summary>
    /// Check if the list contains the given item.
    /// </summary>
    public Series Contains(Expr item, bool nullsEqual = false) => Apply(e => e.List.Contains(item, nullsEqual));
    /// <summary>
    /// Concat this list series with another list series or expression.
    /// </summary>
    public Series Concat(IntoExprColumn other) 
        => Apply(e => e.List.Concat(other.Consume()));

    /// <summary>
    /// Concat this list series with multiple list series or expressions.
    /// Evaluates all concatenations in a single optimized pass.
    /// </summary>
    public Series Concat(params IntoExprColumn[] others)
    {
        if (others == null || others.Length == 0) 
            return _series;

        return Apply(e => 
        {
            Expr current = e;
            foreach (var other in others)
            {
                current = current.List.Concat(other.Consume());
            }
            return current;
        });
    }

    /// <summary>
    /// Concat this list series with a constant array of values.
    /// </summary>
    public Series Concat<T>(ReadOnlySpan<T> other) 
        => Concat(Pl.Lit(other));

    /// <inheritdoc cref="Concat{T}(ReadOnlySpan{T})"/>
    public Series Concat<T>(IEnumerable<T> other) 
        => Concat(Pl.Lit(other));
    /// <summary>Reverse elements in list.</summary>
    public Series Reverse() => Apply(e => e.List.Reverse());
    /// <summary>
    /// Returns a column with a separate row for every list element.
    /// </summary>
    /// <param name="emptyAsNull">Explode an empty list into a null.</param>
    /// <param name="keepNulls">Explode a null list into a null.</param>
    /// <returns>Series with the data type of the list elements.</returns>
    public Series Explode(bool emptyAsNull=true,bool keepNulls=true) => Apply(e => e.List.Explode(emptyAsNull,keepNulls));
    /// <summary>
    /// Convert a List column into an Array column with the same inner data type.
    /// </summary>
    /// <param name="width">Width of the resulting Array column.</param>
    /// <returns>Series of data type Array.</returns>
    public Series ToArray(long width) => Apply(e => e.List.ToArray(width));
    /// <summary>
    /// Convert the series of type List to a series of type Struct.
    /// </summary>
    /// <inheritdoc cref="ArrayOps.ToStruct(string[])"/>
    public Series ToStruct(params string[] fields) => Apply(e => e.List.ToStruct(fields));
    /// <summary>
    /// Convert the series of type List to a series of type Struct. using a function to generate field names dynamically.
    /// </summary>
    /// <inheritdoc cref="ArrayOps.ToStruct(Func{int, string}, int)"/>
    public Series ToStruct(Func<int,string> nameGenerator,int fieldCount) => Apply(e=>e.List.ToStruct(nameGenerator,fieldCount));
    /// <summary>
    /// Convert the series of type List to a series of type Struct.
    /// </summary>
    /// <inheritdoc cref="ListOps.ToStruct(int)"/>
    public Series ToStruct(int upperBound) => Apply(e => e.List.ToStruct(upperBound));

}