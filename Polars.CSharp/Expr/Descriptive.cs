using System.Text;

namespace Polars.CSharp;

public partial class Expr
{
    /// <summary>
    /// Generates a rich HTML view of the expression including its compact string representation and AST tree.
    /// </summary>
    /// <param name="schema">Optional schema context to resolve field types.</param>
    /// <returns>An HTML representation formatted for interactive environments.</returns>
    public string ToHtml(PolarsSchema? schema = null)
    {
        string formula = this.ToString();
        string tree;

        try
        {
            tree = this.Meta.FormatTree(displayAsDot: false, schema: schema);
        }
        catch (Exception ex)
        {
            tree = $"[Failed to format expression tree: {ex.Message}]";
        }

        var sb = new StringBuilder();
        sb.Append(@"
<style>
.pl-expr-box { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Helvetica, Arial, sans-serif; font-size: 13px; color: inherit; }
.pl-expr-header { font-weight: bold; margin-bottom: 6px; }
.pl-expr-tag { background: rgba(128, 128, 128, 0.2); padding: 2px 6px; border-radius: 4px; font-size: 11px; font-family: monospace; margin-left: 6px; }
.pl-expr-formula { font-family: 'Consolas', 'Monaco', monospace; font-size: 13px; font-weight: 600; padding: 6px 10px; background: rgba(128, 128, 128, 0.08); border-left: 3px solid #007acc; border-radius: 2px; margin-bottom: 8px; color: inherit; word-break: break-all; }
.pl-expr-tree { font-family: 'Consolas', 'Monaco', monospace; font-size: 12px; background: rgba(128, 128, 128, 0.05); border: 1px solid rgba(128, 128, 128, 0.2); border-radius: 4px; padding: 8px 12px; overflow-x: auto; white-space: pre; line-height: 1.4; color: inherit; margin: 0; }
</style>");

        sb.Append("<div class='pl-expr-box'>");
        sb.Append("<div class='pl-expr-header'>Polars Expr <span class='pl-expr-tag'>AST</span></div>");
        sb.Append($"<div class='pl-expr-formula'>{System.Net.WebUtility.HtmlEncode(formula)}</div>");
        sb.Append("<pre class='pl-expr-tree'>");
        sb.Append(System.Net.WebUtility.HtmlEncode(tree));
        sb.Append("</pre>");
        sb.Append("</div>");

        return sb.ToString();
    }

    /// <summary>
    /// Custom rendering hook for LINQPad Dump().
    /// Visualizes both the concise expression string and the full AST format tree.
    /// </summary>
    object ToDump()
    {
        string html = ToHtml();

        var utilType = Type.GetType("LINQPad.Util, LINQPad")
                    ?? Type.GetType("LINQPad.Util, LINQPad.Runtime")
                    ?? AppDomain.CurrentDomain.GetAssemblies()
                        .FirstOrDefault(a => a.GetName().Name?.StartsWith("LINQPad") == true)
                        ?.GetType("LINQPad.Util");

        if (utilType != null)
        {
            var rawHtmlMethod = utilType.GetMethod("RawHtml", [typeof(string)]);
            if (rawHtmlMethod != null)
            {
                return rawHtmlMethod.Invoke(null, [html]) ?? html;
            }
        }

        return new
        {
            Type = "Polars.Expr",
            Formula = this.ToString(),
            Tree = this.Meta.FormatTree()
        };
    }
}