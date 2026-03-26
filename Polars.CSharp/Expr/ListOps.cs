#pragma warning disable CS1591
using Polars.NET.Core;

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
    /// <returns></returns>
    public Expr First() => Wrap(PolarsWrapper.ListFirst);
    /// <summary>
    /// Get the value at a specific index.
    /// </summary>
    /// <param name="index">The index to retrieve (can be negative for reverse indexing).</param>
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
    public Expr Get(int index)
        => new(PolarsWrapper.ListGet(_expr.CloneHandle(), index));
    
    /// <summary>
    /// Get the length of the lists.
    /// </summary>
    /// <example>
    /// <code>
    /// // Input: [100, 90, 80]
    /// df.Select(Col("scores").List.Len()); // Output: 3
    /// </code>
    /// </example>
    public Expr Len() => Wrap(PolarsWrapper.ListLen);
    /// <summary>
    /// Join the list elements into a single string with a separator.
    /// </summary>
    /// <param name="separator"></param>
    /// <returns></returns>
    public Expr Join(string separator)
        => new(PolarsWrapper.ListJoin(_expr.CloneHandle(), separator));
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
    /// <summary>
    /// Check if the list contains a specific item.
    /// </summary>
    /// <param name="item"></param>
    /// <param name="nullsEqual"></param>
    /// <returns></returns>
    public Expr Contains(Expr item, bool nullsEqual=false)
        => new(PolarsWrapper.ListContains(_expr.CloneHandle(), item.CloneHandle(),nullsEqual));
    
    /// <summary>
    /// Check if the list contains a specific integer or string item.
    /// </summary>
    /// <param name="item"></param>
    /// <param name="nullsEqual"></param>
    /// <returns></returns>
    public Expr Contains(int item, bool nullsEqual=false) => Contains(Polars.Lit(item), nullsEqual);
    /// <summary>
    /// Check if the list contains a specific string item.
    /// </summary>
    /// <param name="item"></param>
    /// <param name="nullsEqual"></param>
    /// <returns></returns>
    public Expr Contains(string item, bool nullsEqual=false) => Contains(Polars.Lit(item), nullsEqual);
    /// <summary>
    /// Concat this list expression with other list expressions.
    /// </summary>
    /// <param name="others">Other list expressions to append.</param>
    public Expr Concat(params Expr[] others)
    {
        var allExprs = new ExprHandle[others.Length + 1];

        allExprs[0] = _expr.CloneHandle();

        for (int i = 0; i < others.Length; i++)
        {
            allExprs[i + 1] = others[i].CloneHandle();
        }

        return new Expr(PolarsWrapper.ConcatList(allExprs));
    }

    public Expr Concat(Expr other) => Concat([other]);
    public Expr Reverse() => Wrap(PolarsWrapper.ListReverse);
}