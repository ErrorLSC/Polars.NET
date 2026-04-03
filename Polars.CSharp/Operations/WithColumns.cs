using Polars.NET.Core;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Add or modify columns based on Expressions, Strings, Selectors, or Series.
    /// </summary>
    /// <example>
    /// <code>
    /// // Add a new column "c", overwrite with a Series, and select a string column!
    /// lf.WithColumns(Pl.Col("a") + 1, mySeries, "ExistingCol", Cs.Numeric() * 2);
    /// </code>
    /// </example>
    public LazyFrame WithColumns(params IntoExpr[] exprs)
    {
        if (exprs.Length == 0) return this;

        var handles = new ExprHandle[exprs.Length];
        for (int i = 0; i < exprs.Length; i++)
        {
            handles[i] = PolarsWrapper.CloneExpr(exprs[i].Consume().Handle);
        }

        return new LazyFrame(PolarsWrapper.LazyWithColumns(CloneHandle(), handles));
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Add new columns to the DataFrame or replace existing ones using expressions.
    /// <para>
    /// Unlike <see cref="Select(IntoExpr[])"/>, this method keeps all original columns in the DataFrame 
    /// and appends the new ones (or replaces them if the names match).
    /// </para>
    /// </summary>
    /// <param name="exprs">Expressions defining the new columns to add.</param>
    /// <returns>A new DataFrame with the original columns plus the new/modified columns.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     foo = new[] { 1, 2, 3 },
    ///     bar = new[] { 6, 7, 8 },
    ///     ham = new[] { "a", "b", "c" }
    /// });
    /// 
    /// // Add a "sum" column (foo + bar) while keeping others
    /// var withCols = df.WithColumns(
    ///     (Col("foo") + Col("bar")).Alias("sum")
    /// );
    /// 
    /// withCols.Show();
    /// /* Output:
    /// shape: (3, 4)
    /// ┌─────┬─────┬─────┬─────┐
    /// │ foo ┆ bar ┆ ham ┆ sum │
    /// │ --- ┆ --- ┆ --- ┆ --- │
    /// │ i32 ┆ i32 ┆ str ┆ i32 │
    /// ╞═════╪═════╪═════╪═════╡
    /// │ 1   ┆ 6   ┆ a   ┆ 7   │
    /// │ 2   ┆ 7   ┆ b   ┆ 9   │
    /// │ 3   ┆ 8   ┆ c   ┆ 11  │
    /// └─────┴─────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame WithColumns(params IntoExpr[] exprs) => Lazy().WithColumns(exprs).Collect();
}