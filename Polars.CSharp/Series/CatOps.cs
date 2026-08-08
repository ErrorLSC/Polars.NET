using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// Operations on Categorical series. Access via <see cref="Series.Cat"/>.
/// </summary>
public readonly struct SeriesCategoricalOps
{
    private readonly Series _series;
    internal SeriesCategoricalOps(Series series) { _series = series; }
    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Pl.Col(_series.Name)));
    /// <inheritdoc cref="CategoricalOps.GetCategories"/>
    public Series GetCategories() => Apply(e=>e.Cat.GetCategories());
    /// <inheritdoc cref="CategoricalOps.LenBytes"/>
    public Series LenBytes() => Apply(e=>e.Cat.LenBytes());
    /// <inheritdoc cref="CategoricalOps.LenChars"/>
    public Series LenChars() => Apply(e=>e.Cat.LenChars());
    /// <inheritdoc cref="CategoricalOps.StartsWith"/>
    public Series StartsWith(string prefix) => Apply(e=>e.Cat.StartsWith(prefix));
    /// <inheritdoc cref="CategoricalOps.EndsWith"/>
    public Series EndsWith(string suffix) => Apply(e=>e.Cat.EndsWith(suffix));
    /// <inheritdoc cref="CategoricalOps.Physical"/>
    public Series Physical() => Apply(e=>e.Cat.Physical());
    /// <inheritdoc cref="CategoricalOps.To"/>
    public Series To(IntoDataTypeExpr dtype,bool strict=true) => Apply(e=>e.Cat.To(dtype,strict)); 
}