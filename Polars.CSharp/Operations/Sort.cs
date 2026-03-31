using System.Data;
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Sort the LazyFrame by a single column.
    /// </summary>
    public LazyFrame Sort(
        string column, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
        => Sort([Pl.Col(column)], [descending], [nullsLast], maintainOrder);
    /// <summary>
    /// Sort using a single expression.
    /// </summary>
    public LazyFrame Sort(
        Expr expr, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
        => Sort([expr], [descending], [nullsLast], maintainOrder);
    
    /// <summary>
    /// Sort using multiple exprs and single option.
    /// </summary>
    /// <param name="exprs"></param>
    /// <param name="descending"></param>
    /// <param name="nullsLast"></param>
    /// <param name="maintainOrder"></param>
    /// <returns></returns>
    public LazyFrame Sort(
        Expr[] exprs, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
        => Sort(exprs, [descending], [nullsLast], maintainOrder);
    
    /// <summary>
    /// Sort the LazyFrame by multiple columns (all ascending or all descending).
    /// </summary>
    public LazyFrame Sort(
        string[] columns, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
        => Sort(columns.Select(Pl.Col).ToArray(), [descending], [nullsLast], maintainOrder);

    /// <summary>
    /// Lazily sort the DataFrame by multiple columns.
    /// <para>
    /// This operation is added to the logical plan. 
    /// Use <see cref="TopK(int, string, bool)"/> if you only need the top/bottom N rows, as it is more efficient.
    /// </para>
    /// </summary>
    /// <param name="columns">Names of the columns to sort by.</param>
    /// <param name="descending">Sort order for each column.</param>
    /// <param name="nullsLast">Whether nulls go last for each column.</param>
    /// <param name="maintainOrder">Whether to maintain the relative order of rows with equal keys.</param>
    /// <seealso cref="DataFrame.Sort(string[], bool[], bool[], bool)"/>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .Sort(
    ///       columns: new[] { "group", "val" }, 
    ///       descending: new[] { false, true }, 
    ///       nullsLast: new[] { false, false }
    ///   )
    ///   .Collect();
    /// /* Output:
    /// shape: (5, 2)
    /// ┌───────┬─────┐
    /// │ group ┆ val │
    /// │ ---   ┆ --- │
    /// │ str   ┆ i32 │
    /// ╞═══════╪═════╡
    /// │ A     ┆ 10  │
    /// │ A     ┆ 8   │
    /// │ ...   ┆ ... │
    /// └───────┴─────┘
    /// */
    /// </code>
    /// </example>
    public LazyFrame Sort(
        string[] columns, 
        bool[] descending, 
        bool[] nullsLast, 
        bool maintainOrder = false)
        => Sort(columns.Select(Pl.Col).ToArray(), descending, nullsLast, maintainOrder);

    /// <summary>
    /// Sort the LazyFrame by multiple exprs.
    /// </summary>
    public LazyFrame Sort(
        Expr[] exprs, 
        bool[] descending, 
        bool[] nullsLast, 
        bool maintainOrder = false)
    {
        var clonedHandles = new ExprHandle[exprs.Length];
        for (int i = 0; i < exprs.Length; i++)
        {
            clonedHandles[i] = PolarsWrapper.CloneExpr(exprs[i].Handle);
        }

        var h = PolarsWrapper.LazyFrameSort(
            Handle, 
            clonedHandles, 
            descending, 
            nullsLast, 
            maintainOrder
        );
        
        return new LazyFrame(h);
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Sort the DataFrame by a single column.
    /// </summary>
    public DataFrame Sort(
        string column, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort(
            [Polars.Col(column)], 
            [descending], 
            [nullsLast], 
            maintainOrder
        );
    }
    /// <summary>
    /// Sort the DataFrame by a single expr.
    /// </summary>
    public DataFrame Sort(
        Expr expr, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort(
            [expr], 
            [descending], 
            [nullsLast], 
            maintainOrder
        );
    }
    /// <summary>
    /// Sort the DataFrame by multiple columns (all ascending or all descending).
    /// </summary>
    public DataFrame Sort(
        string[] columns, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Sort(
            exprs, 
            [descending], 
            [nullsLast], 
            maintainOrder
        );
    }

    /// <summary>
    /// Sort the DataFrame by multiple columns with specific sort orders for each column.
    /// <para>
    /// This allows for complex sorting, such as sorting by Category (Ascending) and then by Price (Descending).
    /// </para>
    /// </summary>
    /// <param name="columns">Names of the columns to sort by.</param>
    /// <param name="descending">Array of booleans indicating sort order for each column (false=Ascending, true=Descending).</param>
    /// <param name="nullsLast">Array of booleans indicating whether nulls should be placed at the end for each column.</param>
    /// <param name="maintainOrder">Whether to maintain the relative order of rows with equal keys (stable sort). Expensive.</param>
    /// <returns>A new sorted DataFrame.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B", "A" },
    ///     val = new[] { 10, 5, 20, 15, 8 }
    /// });
    /// 
    /// // Sort by "group" (Ascending) then by "val" (Descending)
    /// var sorted = df.Sort(
    ///     columns: new[] { "group", "val" }, 
    ///     descending: new[] { false, true }, // false=Asc, true=Desc
    ///     nullsLast: new[] { false, false }
    /// );
    /// 
    /// sorted.Show();
    /// /* Output:
    /// shape: (5, 2)
    /// ┌───────┬─────┐
    /// │ group ┆ val │
    /// │ ---   ┆ --- │
    /// │ str   ┆ i32 │
    /// ╞═══════╪═════╡
    /// │ A     ┆ 10  │
    /// │ A     ┆ 8   │
    /// │ A     ┆ 5   │
    /// │ B     ┆ 20  │
    /// │ B     ┆ 15  │
    /// └───────┴─────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Sort(
        string[] columns, 
        bool[] descending, 
        bool[] nullsLast, 
        bool maintainOrder = false)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Sort(exprs, descending, nullsLast, maintainOrder);
    }

    /// <summary>
    /// Sort using multiple expressions (all ascending or all descending).
    /// </summary>
    public DataFrame Sort(
        Expr[] exprs, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort(
            exprs, 
            [descending], 
            [nullsLast], 
            maintainOrder
        );
    }

    /// <summary>
    /// Sort the DataFrame by multiple columns.
    /// </summary>
    public DataFrame Sort(
        Expr[] exprs, 
        bool[] descending, 
        bool[] nullsLast, 
        bool maintainOrder = false)
    {
        var lf = Lazy().Sort( 
            exprs, 
            descending, 
            nullsLast, 
            maintainOrder
        );
        
        return lf.Collect();
    }
}