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
    /// Get the column name that this expression would produce.
    /// It may not always be possible to determine the output name.
    /// </summary>
    public string? OutputName()
        => PolarsWrapper.ExprGetOutputName(_expr.Handle);
    
}