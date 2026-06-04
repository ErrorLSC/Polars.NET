using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

public partial class Expr : IDisposable,IEquatable<Expr>
{
    /// <summary>
    /// Check if <b>all</b> values in the boolean expression are <c>true</c>.
    /// <para>This is a boolean aggregation.</para>
    /// </summary>
    /// <param name="ignoreNulls">
    /// If <c>true</c>, null values are ignored. 
    /// If <c>false</c> (default), the result propagates nulls (i.e., if there is a null and no false, the result might be null).
    /// </param>
    /// <returns>A new expression representing the boolean result.</returns>
    public Expr All(bool ignoreNulls=false) => new(PolarsWrapper.All(CloneHandle(),ignoreNulls));
    /// <summary>
    /// Check if <b>any</b> value in the boolean expression is <c>true</c>.
    /// <para>This is a boolean aggregation.</para>
    /// </summary>
    /// <param name="ignoreNulls">
    /// If <c>true</c>, null values are ignored. 
    /// If <c>false</c> (default), the result propagates nulls.
    /// </param>
    /// <returns>A new expression representing the boolean result.</returns>
    public Expr Any(bool ignoreNulls=false) => new(PolarsWrapper.Any(CloneHandle(),ignoreNulls));
    /// <summary>
    /// Calculate the sum of the values in the group or column.
    /// <para>
    /// Behavior depends on context:
    /// <list type="bullet">
    /// <item>In <see cref="DataFrame.GroupBy(IntoExprColumn,bool)"/>: Calculates the sum for each group.</item>
    /// <item>In <see cref="DataFrame.Select(IntoExprColumn[])"/>: Calculates the sum of the entire column (scalar result).</item>
    /// </list>
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B" },
    ///     val = new[] { 1, 2, 3, 4 }
    /// });
    /// 
    /// // 1. GroupBy Aggregation
    /// df.GroupBy("group").Agg(
    ///     Col("val").Sum().Alias("sum"),
    ///     Col("val").Mean().Alias("mean")
    /// ).Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌───────┬─────┬──────┐
    /// │ group ┆ sum ┆ mean │
    /// │ ---   ┆ --- ┆ ---  │
    /// │ str   ┆ i32 ┆ f64  │
    /// ╞═══════╪═════╪══════╡
    /// │ A     ┆ 3   ┆ 1.5  │
    /// │ B     ┆ 7   ┆ 3.5  │
    /// └───────┴─────┴──────┘
    /// */
    /// 
    /// // 2. Global Aggregation (Select)
    /// df.Select(
    ///     Col("val").Sum().Alias("total_sum"),
    ///     Col("val").Count().Alias("total_count")
    /// ).Show();
    /// /* Output:
    /// shape: (1, 2)
    /// ┌───────────┬─────────────┐
    /// │ total_sum ┆ total_count │
    /// │ ---       ┆ ---         │
    /// │ i32       ┆ u32         │
    /// ╞═══════════╪═════════════╡
    /// │ 10        ┆ 4           │
    /// └───────────┴─────────────┘
    /// */
    /// </code>
    /// </example>
    public Expr Sum() => new(PolarsWrapper.Sum(CloneHandle()));

    /// <summary>
    /// Compute the mean of an expression
    /// </summary>
    public Expr Mean() => new(PolarsWrapper.Mean(CloneHandle()));

    /// <summary>
    /// Compute the max of an expression
    /// </summary>
    public Expr Max() => new(PolarsWrapper.Max(CloneHandle()));
    /// <summary>
    /// Get maximum value, but propagate/poison encountered NaN values.
    /// </summary>
    public Expr NanMax() => new(PolarsWrapper.NanMax(CloneHandle()));
    /// <summary>
    /// Get minimum value, but propagate/poison encountered NaN values.
    /// </summary>
    public Expr NanMin() => new(PolarsWrapper.NanMin(CloneHandle()));
    /// <summary>
    /// Compute the min of an expression
    /// </summary>
    public Expr Min() => new(PolarsWrapper.Min(CloneHandle()));
    /// <summary>
    /// Count the number of null.
    /// </summary>
    public Expr NullCount() => new(PolarsWrapper.NullCount(CloneHandle()));
    /// <summary>
    /// Count unique values.
    /// Notes: Null is considered to be a unique value for the purposes of this operation.
    /// </summary>
    public Expr NUnique() => new(PolarsWrapper.NUnique(CloneHandle()));
    /// <summary>
    /// Approximate count of unique values.
    /// This is done using the HyperLogLog++ algorithm for cardinality estimation.
    /// </summary>
    public Expr ApproxNUnique() => new(PolarsWrapper.ApproxNUnique(CloneHandle()));
    /// <summary>
    /// Compute the product of an expression
    /// </summary>
    public Expr Product() => new(PolarsWrapper.Product(CloneHandle()));
    /// <summary>
    /// Get maximum value, ordered by another expression.
    /// If the by expression has multiple values equal to the maximum it is not defined which value will be chosen.
    /// </summary>
    /// <param name="by">Column used to determine the largest element. Accepts expression input. Strings are parsed as column names.</param>
    public Expr MaxBy(IntoExprColumn by)
    {
        Expr realBy = by.Consume();
        return new(PolarsWrapper.MaxBy(CloneHandle(),realBy.Handle));
    }
    /// <summary>
    /// Get minimum value, ordered by another expression.
    /// If the by expression has multiple values equal to the minimum it is not defined which value will be chosen.
    /// </summary>
    /// <param name="by">Column used to determine the smallest element. Accepts expression input. Strings are parsed as column names.</param>
    public Expr MinBy(IntoExprColumn by)
    {
        Expr realBy = by.Consume();
        return new(PolarsWrapper.MinBy(CloneHandle(),realBy.Handle));
    }
    /// <summary>
    /// Get the first value of the group/series.
    /// </summary>
    /// <returns>A new expression representing the first value.</returns>
    public Expr First(bool ignoreNulls = false) => new(PolarsWrapper.First(CloneHandle(),ignoreNulls));
    /// <summary>
    /// Get the last value of the group/series.
    /// </summary>
    /// <returns>A new expression representing the last value.</returns>
    public Expr Last(bool ignoreNulls = false) => new(PolarsWrapper.Last(CloneHandle(),ignoreNulls));

    /// <summary>
    /// Get the first n rows.
    /// </summary>
    /// <param name="n">Number of rows to return.</param>
    /// <returns></returns>
    public Expr Head(int n = 10) => new(PolarsWrapper.Head(CloneHandle(),n));
    /// <summary>
    /// Get the last n rows.
    /// </summary>
    /// <param name="n">Number of rows to return.</param>
    /// <returns></returns>
    public Expr Tail(int n = 10) => new(PolarsWrapper.Tail(CloneHandle(),n));
    /// <summary>
    /// Get the index of the maximum value.
    /// </summary>
    public Expr ArgMax() => new(PolarsWrapper.ArgMax(CloneHandle()));

    /// <summary>
    /// Get the index of the minimum value.
    /// </summary>
    public Expr ArgMin() => new(PolarsWrapper.ArgMin(CloneHandle()));
    /// <summary>
    /// Aggregate values into a list.
    /// <para>
    /// This is the opposite of <see cref="Explode"/>. It collects values from multiple rows into a single list.
    /// </para>
    /// </summary>
    /// <example>
    /// <code>
    /// // Collect all IDs and Tags into single lists
    /// df.Select(
    ///     Col("id").Implode().Alias("all_ids"), 
    ///     Col("tags").Implode().Alias("nested_tags")
    /// ).Show();
    /// /* Output:
    /// shape: (1, 2)
    /// ┌───────────┬─────────────────────┐
    /// │ all_ids   ┆ nested_tags         │
    /// │ ---       ┆ ---                 │
    /// │ list[i32] ┆ list[list[str]]     │
    /// ╞═══════════╪═════════════════════╡
    /// │ [1, 2]    ┆ [["a", "b"], ["c"]] │
    /// └───────────┴─────────────────────┘
    /// */
    /// </code>
    /// </example>
    public Expr Implode(bool maintainOrder=true) => new(PolarsWrapper.Implode(CloneHandle(),maintainOrder));
    /// <summary>
    /// Return the number of non-null elements in the column.
    /// </summary>
    public Expr Count() => new(PolarsWrapper.Count(CloneHandle()));
    /// <summary>
    /// Return the number of elements in the column.
    /// Null values count towards the total.
    /// </summary>
    /// <returns></returns>
    public Expr Len() => new(PolarsWrapper.ExprLen(CloneHandle()));

    /// <summary>
    /// Get the standard deviation.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// By default ddof is 1.</param>
    /// <returns>A series which length is 1</returns>
    public Expr Std(byte ddof = 1) => new(PolarsWrapper.Std(CloneHandle(), ddof));

    /// <summary>
    /// Get the variance.
    /// </summary>
    /// <param name="ddof">“Delta Degrees of Freedom”: the divisor used in the calculation is N - ddof, where N represents the number of elements. 
    /// By default ddof is 1.</param>
    /// <returns>A series which length is 1</returns>
    public Expr Var(byte ddof = 1) => new(PolarsWrapper.Var(CloneHandle(), ddof));

    /// <summary>
    /// Get the median value.
    /// </summary>
    /// <returns>A series which length is 1</returns>
    public Expr Median() => new(PolarsWrapper.Median(CloneHandle()));
    /// <summary>
    /// Get the mode value.
    /// </summary>
    public Expr Mode() => new(PolarsWrapper.Mode(CloneHandle()));
    /// <summary>
    /// Get the quantile value.
    /// </summary>
    /// <param name="quantile">Quantile between 0.0 and 1.0.</param>
    /// <param name="method">['nearest’, ‘higher’, ‘lower’, ‘midpoint’, ‘linear’] Interpolation method.</param>
    /// <returns>A series which length is 1</returns>
    public Expr Quantile(double quantile, QuantileMethod method = QuantileMethod.Linear)
        => new(PolarsWrapper.Quantile(CloneHandle(), quantile, method.ToNative()));
    /// <summary>
    /// Perform an aggregation of bitwise ANDs.
    /// </summary>
    public Expr BitwiseAnd() => new(PolarsWrapper.BitwiseAnd(CloneHandle()));
    /// <summary>
    /// Perform an aggregation of bitwise Ors.
    /// </summary>
    public Expr BitwiseOr() => new(PolarsWrapper.BitwiseOr(CloneHandle()));
    /// <summary>
    /// Perform an aggregation of bitwise Xors.
    /// </summary>
    public Expr BitwiseXor() => new(PolarsWrapper.BitwiseXor(CloneHandle()));
}