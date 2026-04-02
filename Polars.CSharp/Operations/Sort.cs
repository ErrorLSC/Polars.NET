using System.Data;
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Master method: Sort by multiple Expressions with optional parallel boolean arrays.
    /// </summary>
    public LazyFrame Sort(IEnumerable<Expr> exprs, bool[]? descending = null, bool[]? nullsLast = null, bool maintainOrder = false)
    {
        var exprArray = exprs as Expr[] ?? [.. exprs];
        if (exprArray.Length == 0) return this;

        var desc = descending ?? [false];
        var nulls = nullsLast ?? [false];

        var clonedHandles = new ExprHandle[exprArray.Length];
        for (int i = 0; i < exprArray.Length; i++)
        {
            clonedHandles[i] = PolarsWrapper.CloneExpr(exprArray[i].Handle);
        }

        var h = PolarsWrapper.LazyFrameSort(CloneHandle(), clonedHandles, desc, nulls, maintainOrder);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Sort by multiple columns with specific descending directions using Tuples.
    /// Usage: lf.Sort( ("Age", true), ("Name", false), (Cs.Numeric(), true) )
    /// </summary>
    public LazyFrame Sort(params (IntoExpr By, bool Descending)[] sortConfigs)
    {
        if (sortConfigs.Length == 0) return this;

        var handles = new ExprHandle[sortConfigs.Length];
        var descArray = new bool[sortConfigs.Length];
        var nullsArray = new bool[sortConfigs.Length]; 

        for (int i = 0; i < sortConfigs.Length; i++)
        {
            using var safeExpr = sortConfigs[i].By.Consume();
            handles[i] = PolarsWrapper.CloneExpr(safeExpr.Handle);
            descArray[i] = sortConfigs[i].Descending;
            nullsArray[i] = false; 
        }

        var h = PolarsWrapper.LazyFrameSort(CloneHandle(), handles, descArray, nullsArray, maintainOrder: false);
        return new LazyFrame(h);
    }
    
    /// <summary>
    /// Sort the LazyFrame by multiple columns (all ascending or all descending).
    /// </summary>
    public LazyFrame Sort(
        string[] columns, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
        => Sort([.. columns.Select(Pl.Col)], [descending], [nullsLast], maintainOrder);

    /// <summary>
    /// Lazily sort the DataFrame by multiple columns.
    /// <para>
    /// This operation is added to the logical plan. 
    /// Use <see cref="LazyFrame.TopK(int, IEnumerable{Expr}, bool[])"/> if you only need the top/bottom N rows, as it is more efficient.
    /// </para>
    /// </summary>
    /// <param name="columns">Names of the columns to sort by.</param>
    /// <param name="descending">Sort order for each column.</param>
    /// <param name="nullsLast">Whether nulls go last for each column.</param>
    /// <param name="maintainOrder">Whether to maintain the relative order of rows with equal keys.</param>
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
    public LazyFrame Sort(IEnumerable<string> columns, bool descending = false, bool nullsLast = false, bool maintainOrder = false)
    {
        var cols = columns as string[] ?? [.. columns];
        if (cols.Length == 0) return this;

        var exprHandles = new ExprHandle[cols.Length];
        for (int i = 0; i < cols.Length; i++) 
        {
            exprHandles[i] = PolarsWrapper.CloneExpr(Pl.Col(cols[i]).Handle);
        }

        var h = PolarsWrapper.LazyFrameSort(CloneHandle(), exprHandles, [descending], [nullsLast], maintainOrder);
        return new LazyFrame(h);
    }

    /// <summary>
    /// Sort the LazyFrame by multiple exprs.
    /// </summary>
    public LazyFrame Sort(IntoExpr by, bool descending = false, bool nullsLast = false, bool maintainOrder = false)
    {
        using var safeExpr = by.Consume();
        
        var handles = new[] { PolarsWrapper.CloneExpr(safeExpr.Handle) };
        var h = PolarsWrapper.LazyFrameSort(CloneHandle(), handles, [descending], [nullsLast], maintainOrder);
        
        return new LazyFrame(h);
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Sort the DataFrame by a single column.
    /// </summary>
    public DataFrame Sort(
        IntoExpr column, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Lazy().Sort(
            column, 
            descending, 
            nullsLast, 
            maintainOrder
        ).Collect();
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
        return Lazy().Sort(
            columns, 
            descending, 
            nullsLast, 
            maintainOrder
        ).Collect();
    }
    /// <summary>
    /// Sort the DataFrame by multiple columns with specific sort orders for each column.
    /// <para>
    /// This allows for complex sorting, such as sorting by Category (Ascending) and then by Price (Descending).
    /// </para>
    /// </summary>
    /// <param name="columns">The columns to sort by.</param>
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
    public DataFrame Sort(IEnumerable<string> columns, bool descending = false, bool nullsLast = false, bool maintainOrder = false)
        => Lazy().Sort(columns,descending,nullsLast,maintainOrder).Collect();
    /// <summary>
    /// Master method: Sort by multiple Expressions with optional parallel boolean arrays.
    /// </summary>
    public DataFrame Sort(
        IEnumerable<Expr> exprs, 
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
    /// <summary>
    /// Sort by multiple columns with specific descending directions using Tuples.
    /// Usage: df.Sort( ("Age", true), ("Name", false), (Cs.Numeric(), true) )
    /// </summary>
    public DataFrame Sort(params (IntoExpr By, bool Descending)[] sortConfigs)
        => Lazy().Sort(sortConfigs).Collect();

}