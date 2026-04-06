using Polars.NET.Core;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Select columns from the LazyFrame.
    /// Accepts Expr, Selector, or string column names.
    /// </summary>
    /// <example>
    /// <code>
    /// // Select "a" and calculate "b" * 2
    /// lf.Select(Col("a"), (Col("b") * 2).Alias("b_double"));
    /// </code>
    /// </example>
    public LazyFrame Select(params IntoExpr[] exprs)
    {
        if (exprs.Length == 0) return this;

        var handles = new ExprHandle[exprs.Length];
        for (int i = 0; i < exprs.Length; i++)
        {
            using var safeExpr = exprs[i].Consume();
            handles[i] = PolarsWrapper.CloneExpr(safeExpr.Handle);
        }

        return new LazyFrame(PolarsWrapper.LazySelect(CloneHandle(), handles));
    }
    /// <summary>
    /// Bridge overload to support IEnumerable IntoExpr exprs.
    /// Usage: lf.Select(["Id", "Name", "Date"])
    /// </summary>
    public LazyFrame Select(params IEnumerable<IntoExpr> exprs) => Select(exprs.ToArray());
    /// <summary>
    /// Bridge overload to support C# 12 collection expressions.
    /// Usage: lf.Select(["Id", "Name", "Date"])
    /// </summary>
    public LazyFrame Select(IEnumerable<string> columns)
    {
        var cols = columns as string[] ?? [.. columns];
        if (cols.Length == 0) return this;

        var intoExprs = new IntoExpr[cols.Length];
        for (int i = 0; i < cols.Length; i++)
        {
            intoExprs[i] = cols[i]; 
        }

        return Select(intoExprs);
    }

    /// <summary>
    /// Select columns from the LazyFrame by column names.
    /// </summary>
    public LazyFrame Select(params string[] columns)
    {
        if (columns.Length == 0) return this;
        return Select((IEnumerable<string>)columns);
    }
    /// <summary>
    /// Bridge overload to support dynamic LINQ generation of Expressions.
    /// Usage: lf.Select(myColumns.Select(c => Pl.Col(c) * 2))
    /// </summary>
    public LazyFrame Select(IEnumerable<Expr> exprs)
    {
        var exprArray = exprs as Expr[] ?? [.. exprs];
        if (exprArray.Length == 0) return this;

        var handles = new ExprHandle[exprArray.Length];
        for (int i = 0; i < exprArray.Length; i++)
        {
            handles[i] = PolarsWrapper.CloneExpr(exprArray[i].Handle);
        }

        return new LazyFrame(PolarsWrapper.LazySelect(CloneHandle(), handles));
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Select columns from the DataFrame and apply expressions to them.
    /// <para>
    /// This is the primary way to project data, rename columns, or compute new columns based on existing ones.
    /// The result will only contain the columns specified in the expression list.
    /// </para>
    /// </summary>
    /// <param name="columns">A list of expressions defining the columns to select or compute.</param>
    /// <returns>A new DataFrame containing only the selected/computed columns.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     foo = new[] { 1, 2, 3 },
    ///     bar = new[] { 6, 7, 8 },
    ///     ham = new[] { "a", "b", "c" }
    /// });
    /// 
    /// // Select "foo" column and compute a new column "bar_x_2"
    /// var selected = df.Select(
    ///     Col("foo"),
    ///     (Col("bar") * 2).Alias("bar_x_2")
    /// );
    /// 
    /// selected.Show();
    /// /* Output:
    /// shape: (3, 2)
    /// ┌─────┬─────────┐
    /// │ foo ┆ bar_x_2 │
    /// │ --- ┆ ---     │
    /// │ i32 ┆ i32     │
    /// ╞═════╪═════════╡
    /// │ 1   ┆ 12      │
    /// │ 2   ┆ 14      │
    /// │ 3   ┆ 16      │
    /// └─────┴─────────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Select(params IntoExpr[] columns) => Lazy().Select(columns).Collect();
    /// <summary>
    /// Select columns by name (convenience overload).
    /// <para>
    /// This is a shortcut for creating <see cref="Polars.Col(string)"/> expressions for each column name.
    /// </para>
    /// </summary>
    /// <param name="columns">The names of the columns to select.</param>
    /// <returns>A new DataFrame containing only the selected columns.</returns>
    /// <remarks>
    /// For more advanced selections (renaming, calculations), use <see cref="Select(IntoExpr[])"/>.
    /// </remarks>
    public DataFrame Select(IEnumerable<string> columns) => Lazy().Select(columns).Collect();
    /// <summary>
    /// Select columns by name (convenience overload).
    /// </summary>
    public DataFrame Select(params string[] columns) => Lazy().Select(columns).Collect();
    /// <summary>
    /// Select columns by expressions (convenience overload).
    /// </summary>
    public DataFrame Select(IEnumerable<Expr> exprs) => Lazy().Select(exprs).Collect();
    /// <summary>
    /// Bridge overload to support IEnumerable IntoExpr exprs.
    /// Usage: lf.Select(["Id", "Name", "Date"])
    /// </summary>
    public DataFrame Select(params IEnumerable<IntoExpr> exprs) => Lazy().Select(exprs).Collect();
}