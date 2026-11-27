using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// Operations on a Map series (<c>map[key, value]</c>). Access via <see cref="Series.MapOps"/>.
/// </summary>
/// <inheritdoc cref="MapOps"/>
public readonly struct SeriesMapOps
{
    private readonly Series _series;
    internal SeriesMapOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op)
        => _series.ApplyExpr(op(Pl.Col(_series.Name)));

    /// <inheritdoc cref="MapOps.Entries"/>
    public Series Entries() => Apply(static e => e.MapOps.Entries());

    /// <inheritdoc cref="MapOps.Keys"/>
    public Series Keys() => Apply(static e => e.MapOps.Keys());

    /// <inheritdoc cref="MapOps.Values"/>
    public Series Values() => Apply(static e => e.MapOps.Values());

    /// <inheritdoc cref="MapOps.Len"/>
    public Series Len() => Apply(static e => e.MapOps.Len());

    /// <inheritdoc cref="MapOps.ContainsKey(Expr)"/>
    public Series ContainsKey(Expr key) => Apply(e => e.MapOps.ContainsKey(key));

    /// <inheritdoc cref="MapOps.ContainsKey(string)"/>
    public Series ContainsKey(string key) => Apply(e => e.MapOps.ContainsKey(key));

    /// <inheritdoc cref="MapOps.Get(Expr)"/>
    public Series Get(Expr key) => Apply(e => e.MapOps.Get(key));

    /// <inheritdoc cref="MapOps.Get(string)"/>
    public Series Get(string key) => Apply(e => e.MapOps.Get(key));
}
