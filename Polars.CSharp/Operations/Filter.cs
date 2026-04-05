using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Filter rows based on a boolean expression.
    /// <para>
    /// In a LazyFrame, this operation is added to the logical plan and is optimized before execution.
    /// Polars will attempt to push this filter down as close to the data source as possible (Predicate Pushdown).
    /// </para>
    /// </summary>
    /// <param name="expr">A boolean expression.</param>
    /// <returns>A new LazyFrame with the filter applied.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B", "C" },
    ///     val = new[] { 1, 2, 3, 4, 5 }
    /// });
    /// 
    /// // Build a lazy query:
    /// // 1. Filter out group 'C'
    /// // 2. Multiply 'val' by 2
    /// // 3. Select specific columns
    /// var q = df.Lazy()
    ///     .Filter(Col("group") != "C")
    ///     .WithColumns((Col("val") * 2).Alias("val_x_2"))
    ///     .Select("group", "val_x_2");
    /// 
    /// // Execute
    /// q.Collect().Show();
    /// /* Output:
    /// shape: (4, 2)
    /// ┌───────┬─────────┐
    /// │ group ┆ val_x_2 │
    /// │ ---   ┆ ---     │
    /// │ str   ┆ i32     │
    /// ╞═══════╪═════════╡
    /// │ A     ┆ 2       │
    /// │ A     ┆ 4       │
    /// │ B     ┆ 6       │
    /// │ B     ┆ 8       │
    /// └───────┴─────────┘
    /// */
    /// </code>
    /// </example>
    public LazyFrame Filter(Expr expr)
        => new(PolarsWrapper.LazyFilter(CloneHandle(), expr.CloneHandle()));
    /// <summary>
    /// Filter rows based on a boolean series.
    /// </summary>
    public LazyFrame Filter(Series series)
    {
        if (series.DataType != DataType.Boolean)
        {
            throw new InvalidOperationException("Can not filter by non-boolean series.");
        }
        
        using var expr = Pl.Lit(series); 
        
        return Filter(expr); 
    }
    /// <summary>
    /// Filter rows based on a boolean array.
    /// </summary>
    /// <param name="mask"></param>
    /// <returns></returns>
    public LazyFrame Filter(IEnumerable<bool> mask)
        => Filter(Pl.Lit(mask)); 

}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Filter rows based on a boolean expression (predicate).
    /// <para>
    /// Retains only the rows where the expression evaluates to true.
    /// </para>
    /// </summary>
    /// <param name="expr">A boolean expression to filter by (e.g., Col("a") > 5).</param>
    /// <returns>A new DataFrame containing only the matching rows.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     foo = new[] { 1, 2, 3 },
    ///     bar = new[] { 6, 7, 8 },
    ///     ham = new[] { "a", "b", "c" }
    /// });
    /// 
    /// // Keep rows where "foo" is greater than 1
    /// var filtered = df.Filter(Col("foo") > 1);
    /// 
    /// filtered.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬─────┬─────┐
    /// │ foo ┆ bar ┆ ham │
    /// │ --- ┆ --- ┆ --- │
    /// │ i32 ┆ i32 ┆ str │
    /// ╞═════╪═════╪═════╡
    /// │ 2   ┆ 7   ┆ b   │
    /// │ 3   ┆ 8   ┆ c   │
    /// └─────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Filter(Expr expr) => Lazy().Filter(expr).Collect();
    /// <summary>
    ///  Filter rows based on a boolean series.
    /// </summary>
    /// <param name="series">A boolean series as mask</param>
    /// <returns></returns>
    public DataFrame Filter(Series series) => Lazy().Filter(series).Collect();
    /// <summary>
    /// Filter rows based on a boolean array.
    /// </summary>
    /// <param name="mask">A boolean IEnumerable as mask</param>
    /// <returns></returns>
    public DataFrame Filter(IEnumerable<bool> mask) => Lazy().Filter(mask).Collect(); 

}