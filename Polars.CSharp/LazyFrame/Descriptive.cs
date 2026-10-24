
using System.Text;
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

public partial class LazyFrame : IDisposable
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
    ///     .Filter(Pl.Col("group") != "C")
    ///     .WithColumns((Pl.Col("val") * 2).Alias("val_x_2"))
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

    /// <summary>
    /// Generates an HTML visualization of the LazyFrame execution plan and schema for LINQPad.
    /// </summary>
    /// <param name="optimized">Whether to explain the optimized logical plan.</param>
    /// <returns>An HTML representation formatted with monospace styling.</returns>
    public string ToHtml(bool optimized = true)
    {
        string plan = Explain(optimized);

        using PolarsSchema schema = this.Schema;

        string schemaSummary = schema.ToString();

        var sb = new StringBuilder();
        sb.Append(@"
<style>
.pl-lazy-container { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; font-size: 13px; color: inherit; }
.pl-lazy-header { font-weight: bold; margin-bottom: 6px; font-size: 13px; }
.pl-lazy-tag { background: rgba(128, 128, 128, 0.2); padding: 2px 6px; border-radius: 4px; font-size: 11px; font-family: monospace; margin-left: 6px; }
.pl-lazy-schema { color: rgba(128, 128, 128, 0.85); font-family: 'Consolas', 'Monaco', monospace; font-size: 12px; margin-bottom: 10px; }
.pl-lazy-plan { font-family: 'Consolas', 'Monaco', monospace; font-size: 12px; background: rgba(128, 128, 128, 0.06); border: 1px solid rgba(128, 128, 128, 0.2); border-radius: 4px; padding: 10px; overflow-x: auto; white-space: pre; line-height: 1.4; color: inherit; }
.pl-lazy-tip { font-style: italic; opacity: 0.6; font-size: 11px; margin-top: 6px; }
</style>");

        sb.Append("<div class='pl-lazy-container'>");
        sb.Append("<div class='pl-lazy-header'>Polars LazyFrame <span class='pl-lazy-tag'>UNCOLLECTED</span></div>");

        if (!string.IsNullOrEmpty(schemaSummary))
        {
            sb.Append($"<div class='pl-lazy-schema'>{System.Net.WebUtility.HtmlEncode(schemaSummary)}</div>");
        }

        sb.Append("<div class='pl-lazy-plan'>");
        sb.Append(System.Net.WebUtility.HtmlEncode(plan));
        sb.Append("</div>");
        sb.Append("<div class='pl-lazy-tip'>Tip: Call .Collect() to eagerly execute this query plan.</div>");
        sb.Append("</div>");

        return sb.ToString();
    }

    /// <summary>
    /// Duck-typed method called automatically by LINQPad to customize Dump() rendering.
    /// Prevents eager query execution and visualizes the logical execution plan.
    /// </summary>
    object ToDump()
    {
        string html = ToHtml(optimized: true);

        var utilType = Type.GetType("LINQPad.Util, LINQPad")
                    ?? Type.GetType("LINQPad.Util, LINQPad.Runtime")
                    ?? AppDomain.CurrentDomain.GetAssemblies()
                        .FirstOrDefault(a => a.GetName().Name?.StartsWith("LINQPad") is true)
                        ?.GetType("LINQPad.Util");

        if (utilType != null)
        {
            var rawHtmlMethod = utilType.GetMethod("RawHtml", [typeof(string)]);
            if (rawHtmlMethod != null)
            {
                return rawHtmlMethod.Invoke(null, [html]) ?? html;
            }
        }

        // Fallback
        return new
        {
            Type = "Polars.LazyFrame (Uncollected)",
            Schema = this.Schema.ToString(),
            LogicalPlan = this.Explain(optimized: true)
        };
    }
}