
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Get an explanation of the optimized query plan.
    /// <para>
    /// Returns a string representation of the logical plan after Polars optimizers 
    /// (predicate pushdown, projection pushdown, etc.) have run.
    /// </para>
    /// </summary>
    /// <param name="optimized">If true, show the optimized plan. If false, show the logical plan as built.</param>
    /// <returns>The plan as a string.</returns>
    /// <example>
    /// <code>
    /// var q = df.Lazy()
    ///     .Filter(Col("group") != "C")
    ///     .WithColumns((Col("val") * 2).Alias("val_x_2"))
    ///     .Select("group", "val_x_2");
    /// 
    /// Console.WriteLine(q.Explain());
    /// /* Output (Optimized Plan):
    /// simple π 2/2 ["group", "val_x_2"]
    ///    WITH_COLUMNS:
    ///    [[(col("val")) * (2)].alias("val_x_2")] 
    ///     FILTER [(col("group")) != ("C")]
    ///     FROM
    ///       DF ["group", "val"]; PROJECT["group", "val"] 2/2 COLUMNS
    /// */
    /// </code>
    /// </example>
    public string Explain(bool optimized = true)
        => PolarsWrapper.Explain(Handle, optimized);
    /// <summary>
    /// Generate a summary statistics DataFrame (count, mean, std, min, 25%, 50%, 75%, max).
    /// Similar to pandas/polars describe().
    /// Notice: This will collect LazyFrame once, but LazyFrame won't be consumed.
    /// </summary>
    public DataFrame Describe()
        => Clone().Collect().Describe();

    /// <summary>
    /// Returns the string representation of the LazyFrame (ASCII table).
    /// This allows Console.WriteLine(lf) to print the table directly.
    /// </summary>
    public override string ToString()
    {
        if (Handle.IsInvalid) return "LazyFrame (Disposed)";
        return Clone().Collect().ToString();
    }

    /// <summary>
    /// Print the LazyFrame to Console.
    /// </summary>
    public void Show() => Console.WriteLine(ToString());
}