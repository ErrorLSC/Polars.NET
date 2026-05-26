using System.Data;
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Sort the LazyFrame by a single column name, selector, or column expression.
    /// </summary>
    public LazyFrame Sort(
        IntoSelector by, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        using var safeSelector = by.Consume();
        using var expr = safeSelector.ToExpr();
        
        var handles = new[] { PolarsWrapper.CloneExpr(expr.Handle) };
        var h = PolarsWrapper.LazyFrameSort(CloneHandle(), handles, [descending], [nullsLast], maintainOrder);
        
        return new LazyFrame(h);
    }
    /// <summary>
    /// Lazily sort the DataFrame by multiple columns.
    /// <para>
    /// This operation is added to the logical plan. 
    /// Use <see cref="LazyFrame.TopK(int, IEnumerable{Expr}, bool[])"/> if you only need the top/bottom N rows, as it is more efficient.
    /// </para>
    /// </summary>
    /// <param name="by">Names of the columns to sort by.</param>
    /// <param name="descending">Sort order for each column.</param>
    /// <param name="nullsLast">Whether nulls go last for each column.</param>
    /// <param name="maintainOrder">Whether to maintain the relative order of rows with equal keys.</param>
    /// <example>
    /// <code>
    /// df.Lazy()
    ///   .Sort(
    ///       by: new[] { "group", "val" }, 
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
        IEnumerable<IntoSelector> by, 
        bool[]? descending = null, 
        bool[]? nullsLast = null, 
        bool maintainOrder = false)
    {
        var selectors = by as IntoSelector[] ?? [.. by];
        if (selectors.Length == 0) return this;

        var handles = new ExprHandle[selectors.Length];
        for (int i = 0; i < selectors.Length; i++)
        {
            using var sel = selectors[i].Consume();
            using var expr = sel.ToExpr();
            handles[i] = PolarsWrapper.CloneExpr(expr.Handle);
        }

        var desc = descending ?? [false];
        var nulls = nullsLast ?? [false];

        var h = PolarsWrapper.LazyFrameSort(CloneHandle(), handles, desc, nulls, maintainOrder);
        return new LazyFrame(h);
    }

    /// <summary>
    /// Sort the LazyFrame using specific sorting configurations for each column/selector.
    /// </summary>
    public LazyFrame Sort(params (IntoSelector By, bool Descending)[] sortConfigs)
    {
        if (sortConfigs.Length == 0) return this;

        var handles = new ExprHandle[sortConfigs.Length];
        var descArray = new bool[sortConfigs.Length];
        var nullsArray = new bool[sortConfigs.Length]; 

        for (int i = 0; i < sortConfigs.Length; i++)
        {
            using var sel = sortConfigs[i].By.Consume();
            using var expr = sel.ToExpr();
            
            handles[i] = PolarsWrapper.CloneExpr(expr.Handle);
            descArray[i] = sortConfigs[i].Descending;
            nullsArray[i] = false; 
        }

        var h = PolarsWrapper.LazyFrameSort(CloneHandle(), handles, descArray, nullsArray, maintainOrder: false);
        return new LazyFrame(h);
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Sort the LazyFrame by a single column name, selector, or column expression.
    /// </summary>
    public DataFrame Sort(
        IntoSelector by, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Lazy().Sort(
            by, 
            descending, 
            nullsLast, 
            maintainOrder
        ).Collect();
    }
    /// <summary>
    /// Sort the DataFrame by multiple columns (all ascending or all descending).
    /// </summary>
    public DataFrame Sort(
        IEnumerable<IntoSelector> by, 
        bool[]? descending = null, 
        bool[]? nullsLast = null, 
        bool maintainOrder = false)
    {
        return Lazy().Sort(
            by, 
            descending, 
            nullsLast, 
            maintainOrder
        ).Collect();
    }
    /// <summary>
    /// Sort the LazyFrame using specific sorting configurations for each column/selector.
    /// </summary>
    public DataFrame Sort(params (IntoSelector By, bool Descending)[] sortConfigs)
        => Lazy().Sort(sortConfigs).Collect();

}