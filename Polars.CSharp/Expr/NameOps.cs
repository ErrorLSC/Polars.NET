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
        => new(PolarsWrapper.Prefix(_expr.CloneHandle(), prefix));
    /// <summary>
    /// Suffix the column name with a specified string.
    /// </summary>
    /// <param name="suffix"></param>
    /// <returns></returns>
    public Expr Suffix(string suffix)
        => new(PolarsWrapper.Suffix(_expr.CloneHandle(), suffix));
}