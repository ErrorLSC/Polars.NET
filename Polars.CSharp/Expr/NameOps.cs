using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Offers methods for renaming columns.
/// </summary>
public readonly struct NameOps
{
    private readonly Expr _expr;
    internal NameOps(Expr expr) { _expr = expr; }
    /// <summary>
    /// Prefix the column name with a specified string.
    /// </summary>
    /// <param name="prefix"></param>
    /// <returns></returns>
    public Expr Prefix(string prefix)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new(PolarsWrapper.Prefix(h, prefix));
    }

    /// <summary>
    /// Suffix the column name with a specified string.
    /// </summary>
    /// <param name="suffix"></param>
    /// <returns></returns>
    public Expr Suffix(string suffix)
    {
        var h = PolarsWrapper.CloneExpr(_expr.Handle);
        return new(PolarsWrapper.Suffix(h, suffix));
    }
}