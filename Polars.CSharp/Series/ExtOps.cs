using Pl = Polars.CSharp.Polars;
namespace Polars.CSharp;

/// <summary>
/// Wrapper for extensio datatype operations on a Series.
/// </summary>
public readonly struct SeriesExtensionOps
{
    private readonly Series _series;
    internal SeriesExtensionOps(Series series) { _series = series; }
    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Pl.Col(_series.Name)));
    /// <inheritdoc cref="ExtensionOps.To"/>
    public Series To(IntoDataTypeExpr dtype) => Apply(e=>e.Ext.To(dtype));
    /// <inheritdoc cref="ExtensionOps.Storage"/>
    public Series Storage() => Apply(e=>e.Ext.Storage());
}