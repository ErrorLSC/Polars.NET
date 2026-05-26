using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// Operations on extension datatype columns. Access via <see cref="Expr.Ext"/>.
/// </summary>
public readonly struct ExtensionOps
{
    private readonly Expr _expr;
    internal ExtensionOps(Expr expr) { _expr = expr; }

    private Expr Wrap(Func<ExprHandle, ExprHandle> op)
        => new(op(_expr.CloneHandle()));
    /// <summary>
    /// Convert to an extension dtype.The input must be of the storage type of the extension dtype.
    /// </summary>
    /// <param name="dtype"></param>
    /// <returns></returns>
    public Expr To(IntoDataTypeExpr dtype) => new(PolarsWrapper.ExtTo(_expr.CloneHandle(),dtype.Consume().Handle));
    /// <summary>
    /// Get the storage values of an extension data type.
    /// If the input does not have an extension data type, it is returned as-is.
    /// </summary>
    /// <returns></returns>
    public Expr Storage() => Wrap(PolarsWrapper.ExtStorage);
}