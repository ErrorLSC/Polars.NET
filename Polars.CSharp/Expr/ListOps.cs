#pragma warning disable CS1591
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

// ==========================================
// ListOps Helper Class
// ==========================================
/// <summary>
/// Operations on List columns. Access via <see cref="Expr.List"/>.
/// </summary>
public readonly struct ListOps
{
    private readonly Expr _expr;
    internal ListOps(Expr expr) { _expr = expr; }

    private Expr Wrap(Func<ExprHandle, ExprHandle> op)
        => new(op(_expr.CloneHandle()));
    /// <summary>
    /// Get the first element of the list.
    /// </summary>
    public Expr First() => Get(0,true);
    /// <summary>
    /// Get the last element of the list.
    /// </summary>
    public Expr Last() => Get(-1,true);
    /// <summary>
    /// Get the value at a specific index.
    /// </summary>
    /// <param name="index">The index to retrieve (can be negative for reverse indexing).</param>
    /// <param name="nullOnOob">Behavior if an index is out of bounds:
    /// True -> set as null
    /// False -> raise an error</param>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     student = new[] { "Alice", "Bob", "Charlie" },
    ///     scores = new[] { 
    ///         new[] { 100, 90, 80 },
    ///         new[] { 60, 60 },
    ///         new int[] { }
    ///     }
    /// });
    /// 
    /// df.Select(
    ///     Col("student"),
    ///     Col("scores").List.Len().Alias("course_count"),
    ///     Col("scores").List.Sum().Alias("total_score"),
    ///     Col("scores").List.Get(0).Alias("first_score")
    /// ).Show();
    /// /* Output:
    /// shape: (3, 4)
    /// ┌─────────┬──────────────┬─────────────┬─────────────┐
    /// │ student ┆ course_count ┆ total_score ┆ first_score │
    /// │ ---     ┆ ---          ┆ ---         ┆ ---         │
    /// │ str     ┆ u32          ┆ i32         ┆ i32         │
    /// ╞═════════╪══════════════╪═════════════╪═════════════╡
    /// │ Alice   ┆ 3            ┆ 270         ┆ 100         │
    /// │ Bob     ┆ 2            ┆ 120         ┆ 60          │
    /// │ Charlie ┆ 0            ┆ 0           ┆ null        │
    /// └─────────┴──────────────┴─────────────┴─────────────┘
    /// */
    /// 
    /// // To Explode (Flatten) the list, use DataFrame.Explode:
    /// // df.Explode(Col("scores")).Show();
    /// </code>
    /// </example>
    public Expr Get(long index,bool nullOnOob=false)=> GetImpl(index,nullOnOob);
    internal Expr GetImpl(Expr index, bool nullOnOob= false)
        => new(PolarsWrapper.ListGet(_expr.CloneHandle(), index.CloneHandle(),nullOnOob));
    public Expr Gather(IntoColumnExpr indices,bool nullOnOob=false) => new(PolarsWrapper.ListGather(_expr.CloneHandle(),indices.Consume().Handle,nullOnOob));
    public Expr Gather(ReadOnlySpan<int> indices,bool nullOnOob=false) => Gather(Pl.Lit(indices),nullOnOob);
    public Expr GatherEvery(IntoColumnExpr n,IntoColumnExpr offset) => new(PolarsWrapper.ListGatherEvery(_expr.CloneHandle(),n.Consume().Handle,offset.Consume().Handle));
    public Expr GatherEvery(IntoColumnExpr n) => GatherEvery(n,0);
    /// <summary>
    /// Slice every sublist.
    /// </summary>
    /// <param name="offset">Start index. Negative indexing is supported.</param>
    /// <param name="length">Length of the slice. If null, the slice is taken to the end of the list.</param>
    public Expr Slice(IntoColumnExpr offset, IntoColumnExpr? length = null)
    {
        Expr offsetExpr = offset.Consume();

        Expr lengthExpr = length.HasValue ? length.Value.Consume() : Pl.LitNull();

        return new Expr(PolarsWrapper.ListSlice(
            _expr.CloneHandle(), 
            offsetExpr.CloneHandle(), 
            lengthExpr.CloneHandle()
        ));
    }
    public Expr Head(IntoColumnExpr n) => new(PolarsWrapper.ListHead(_expr.CloneHandle(),n.Consume().Handle));
    public Expr Head(long n=5) => Head(Pl.Lit(n));
    public Expr Tail(IntoColumnExpr n) => new(PolarsWrapper.ListTail(_expr.CloneHandle(),n.Consume().Handle));
    public Expr Tail(long n=5) => Tail(Pl.Lit(n));
    public Expr Agg(Expr expr) => new(PolarsWrapper.ListAgg(_expr.CloneHandle(),expr.CloneHandle()));
    public Expr Shift(Expr n) => new(PolarsWrapper.ListShift(_expr.CloneHandle(),n.CloneHandle()));
    public Expr Shift() => Shift(1);
    public Expr Diff(long n=1,NullBehavior nullBehavior=NullBehavior.Ignore) => new(PolarsWrapper.ListDiff(_expr.CloneHandle(),n,nullBehavior.ToNative()));
    public Expr SampleN(IntoColumnExpr n,bool withReplacement=false,bool shuffle=false,ulong? seed=null)
    {
        ExprHandle nE = n.Consume().Handle;
        return new Expr(PolarsWrapper.ListSampleN(_expr.CloneHandle(),nE,withReplacement,shuffle,seed));
    }
    public Expr SampleN(bool withReplacement=false,bool shuffle=false,ulong? seed=null) => SampleN(1,withReplacement,shuffle,seed);
    public Expr SampleFrac(IntoColumnExpr fraction,bool withReplacement=false,bool shuffle=false,ulong? seed=null)
        => new(PolarsWrapper.ListSampleFraction(_expr.CloneHandle(),fraction.Consume().Handle,withReplacement,shuffle,seed));
    public Expr SetUnion(IntoColumnExpr other) => new(PolarsWrapper.ListSetUnion(_expr.CloneHandle(),other.Consume().Handle));
    public Expr SetUnion<T>(ReadOnlySpan<T> other) => SetUnion(Pl.Lit(other));
    public Expr SetUnion<T>(IEnumerable<T> other) => SetUnion(Pl.Lit(other));
    public Expr SetDifference(IntoColumnExpr other) => new(PolarsWrapper.ListSetDifference(_expr.CloneHandle(),other.Consume().Handle));
    public Expr SetDifference<T>(ReadOnlySpan<T> other) => SetDifference(Pl.Lit(other));
    public Expr SetDifference<T>(IEnumerable<T> other) => SetDifference(Pl.Lit(other));
    public Expr SetIntersection(IntoColumnExpr other) => new(PolarsWrapper.ListSetIntersection(_expr.CloneHandle(),other.Consume().Handle));
    public Expr SetIntersection<T>(ReadOnlySpan<T> other) => SetIntersection(Pl.Lit(other));
    public Expr SetIntersection<T>(IEnumerable<T> other) => SetIntersection(Pl.Lit(other));
    public Expr SetSymmetricDifference(IntoColumnExpr other) => new(PolarsWrapper.ListSetSymmetricDifference(_expr.CloneHandle(),other.Consume().Handle));
    public Expr SetSymmetricDifference<T>(ReadOnlySpan<T> other) => SetSymmetricDifference(Pl.Lit(other));
    public Expr SetSymmetricDifference<T>(IEnumerable<T> other) => SetSymmetricDifference(Pl.Lit(other));
    
    // public Expr SampleFrac
    /// <summary>
    /// Return the number of elements in each list.
    /// </summary>
    public Expr Len() => Wrap(PolarsWrapper.ListLen);
    /// <summary>
    /// Join the list elements into a single string with a separator.
    /// </summary>
    /// <param name="separator">string to separate the items with</param>
    /// <param name="ignoreNulls">Ignore null values (default).
    /// If set to False, null values will be propagated. If the sub-list contains any null values, the output is null.</param>
    /// <returns></returns>
    public Expr Join(string separator,bool ignoreNulls=true)
        => new(PolarsWrapper.ListJoin(_expr.CloneHandle(), separator,ignoreNulls));
    /// <summary>
    /// Sort the list elements.
    /// </summary>
    /// <param name="descending"></param>
    /// <param name="nullsLast"></param>
    /// <param name="maintainOrder"></param>
    /// <returns></returns>
    public Expr Sort(bool descending = false, bool nullsLast = false, bool maintainOrder = false)
        => new(PolarsWrapper.ListSort(_expr.CloneHandle(), descending, nullsLast, maintainOrder));
    
    /// <summary>
    /// Calculate the sum of the values in the list (row-wise).
    /// </summary>
    /// <example>
    /// <code>
    /// // Input: [100, 90, 80]
    /// df.Select(Col("scores").List.Sum()); // Output: 270
    /// </code>
    /// </example>
    public Expr Sum() => Wrap(PolarsWrapper.ListSum);
    /// <summary>
    /// Calculate the minimum of the list elements.
    /// </summary>
    /// <returns></returns>
    public Expr Min() => Wrap(PolarsWrapper.ListMin);
    /// <summary>
    /// Calculate the maximum of the list elements.
    /// </summary>
    /// <returns></returns>
    public Expr Max() => Wrap(PolarsWrapper.ListMax);
    /// <summary>
    /// Calculate the mean of the list elements.
    /// </summary>
    /// <returns></returns>
    public Expr Mean() => Wrap(PolarsWrapper.ListMean);
    public Expr Median() => Wrap(PolarsWrapper.ListMedian);
    public Expr All() => Wrap(PolarsWrapper.ListAll);
    public Expr Any() => Wrap(PolarsWrapper.ListAny);
    public Expr DropNulls() => Wrap(PolarsWrapper.ListDropNulls);
    public Expr NUnique() => Wrap(PolarsWrapper.ListNUnique);
    public Expr ArgMax() => Wrap(PolarsWrapper.ListArgMax);
    public Expr ArgMin() => Wrap(PolarsWrapper.ListArgMin);
    /// <inheritdoc cref="Expr.Std"/>
    public Expr Std(byte ddof=1) => new(PolarsWrapper.ListStd(_expr.CloneHandle(),ddof));
    /// <inheritdoc cref="Expr.Var"/>
    public Expr Var(byte ddof=1) => new(PolarsWrapper.ListVar(_expr.CloneHandle(),ddof));
    public Expr Unique(bool maintainOrder=false) => new(PolarsWrapper.ListUnique(_expr.CloneHandle(),maintainOrder));
    /// <summary>
    /// Check if the list contains a specific item.
    /// </summary>
    /// <param name="item"></param>
    /// <param name="nullsEqual"></param>
    /// <returns></returns>
    public Expr Contains(Expr item, bool nullsEqual=false)
        => new(PolarsWrapper.ListContains(_expr.CloneHandle(), item.CloneHandle(),nullsEqual));
    /// <summary>
    /// Concat this list expression with other list expressions, column names, or series.
    /// Perfectly matches Python's list[Expr | str] | Expr | str | Series.
    /// </summary>
    /// <param name="others">Other expressions to append.</param>
    public Expr Concat(params IntoColumnExpr[] others)
    {
        if (others == null || others.Length == 0)
            return new Expr(_expr.CloneHandle());

        var allExprs = new ExprHandle[others.Length + 1];

        allExprs[0] = _expr.CloneHandle();

        for (int i = 0; i < others.Length; i++)
        {
            allExprs[i + 1] = others[i].Consume().CloneHandle();
        }

        return new Expr(PolarsWrapper.ConcatList(allExprs));
    }
    /// <summary>
    /// Concat this list expression with a constant array of values.
    /// The constant array will be broadcasted to every row.
    /// </summary>
    public Expr Concat<T>(ReadOnlySpan<T> other) 
        => Concat(Pl.Lit(other)); 

    // / <inheritdoc cref="Concat{T}(ReadOnlySpan{T})"/>
    public Expr Concat<T>(IEnumerable<T> other) 
        => Concat(Pl.Lit(other));
    public Expr Reverse() => Wrap(PolarsWrapper.ListReverse);
    public Expr Explode(bool emptyAsNull=true,bool keepNulls=true) => _expr.Explode(emptyAsNull,keepNulls);
    public Expr ToArray(long width) => new(PolarsWrapper.ListToArray(_expr.CloneHandle(),width));
    public Expr ToStruct(params string[] fields) => new(PolarsWrapper.ListToStruct(_expr.CloneHandle(),fields));
    public Expr ToStruct(Func<int, string> nameGenerator, int fieldCount)
    {
        if (fieldCount <= 0) 
            return ToStruct();

        string[] fields = new string[fieldCount];
        for (int i = 0; i < fieldCount; i++)
        {
            fields[i] = nameGenerator(i);
        }
        return ToStruct(fields);
    }

    /// <summary>
    /// Convert the list to a struct type.
    /// </summary>
    /// <param name="upperBound">
    /// The maximum number of struct fields to create. 
    /// Must be provided to allow schema inference if fields are not explicitly named.
    /// </param>
    public Expr ToStruct(int upperBound)
    {
        if (upperBound <= 0)
            return ToStruct();

        return ToStruct(i => $"field_{i}", upperBound);
    }

}