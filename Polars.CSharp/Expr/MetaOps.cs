#pragma warning disable CS1591
using Polars.NET.Core;

namespace Polars.CSharp;

public readonly struct MetaOps
{
    private readonly Expr _expr;

    internal MetaOps(Expr expr)
    {
        _expr = expr;
    }

    /// <summary>
    /// Return the original expression.
    /// </summary>
    /// <returns></returns>
    public Expr AsExpression() => _expr;
    /// <summary>
    /// Get the column name that this expression would produce.
    /// It may not always be possible to determine the output name.
    /// </summary>
    public string? OutputName()
        => PolarsWrapper.ExprGetOutputName(_expr.Handle);
    /// <summary>
    /// Indicate if this expression is a basic (non-regex) unaliased column.
    /// </summary>
    public bool IsColumn() => PolarsWrapper.IsColumn(_expr.Handle);

    /// <summary>
    /// Indicate if this expression expands to columns that match a regex pattern.
    /// </summary>
    public bool IsRegexProjection() => PolarsWrapper.IsRegexProjection(_expr.Handle);

    /// <summary>
    /// Indicate if this expression only selects columns (optionally with aliasing).
    /// This can include bare columns, columns matched by regex or dtype, selectors and exclude ops, and (optionally) column/expression aliasing.
    /// </summary>
    /// <param name="allowAliasing">If False (default), any aliasing is not considered to be column selection. Set True to allow for column selection that also includes aliasing.</param>
    public bool IsColumnSelection(bool allowAliasing = false) 
        => PolarsWrapper.IsColumnSelection(_expr.Handle, allowAliasing);

    /// <summary>
    /// Indicate if this expression is a literal value (optionally aliased).
    /// </summary>
    /// <param name="allowAliasing">If False (default), any aliasing is not considered to be column selection. Set True to allow for column selection that also includes aliasing.</param>
    public bool IsLiteral(bool allowAliasing = false) 
        => PolarsWrapper.IsLiteral(_expr.Handle, allowAliasing);
    /// <summary>
    /// Indicate if this expression expands into multiple expressions.
    /// </summary>
    public bool HasMultipleOutputs() => PolarsWrapper.HasMultipleOutputs(_expr.Handle);
    /// <summary>
    /// Indicate if this expression expands into multiple expressions.
    /// </summary>
    public Expr UndoAliases() => new(PolarsWrapper.UndoAlias(_expr.CloneHandle()));
    /// <summary>
    /// Get a list with the root column name.
    /// </summary>
    public string[] RootNames() => PolarsWrapper.RootNames(_expr.Handle);
    /// <summary>
    /// Format the expression as a tree.
    /// </summary>
    public string FormatTree(bool displayAsDot = false,PolarsSchema? schema=null) => PolarsWrapper.FormatTree(_expr.Handle,displayAsDot,schema?.Handle);
}