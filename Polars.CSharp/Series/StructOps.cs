using Polars.NET.Core;

namespace Polars.CSharp;

/// <summary>
/// Series Struct Ops Namespace
/// </summary>
public readonly struct SeriesStructOps
{
    private readonly Series _series;
    internal SeriesStructOps(Series series) { _series = series; }

    private Series Apply(Func<Expr, Expr> op) 
        => _series.ApplyExpr(op(Polars.Col(_series.Name)));

    /// <summary>
    /// Retrieve a field from the struct by name.
    /// Returns a new Series of that field's type.
    /// </summary>
    public Series Field(string name) => Apply(e => e.Struct.Field(name));

    /// <summary>
    /// Retrieve a field from the struct by index.
    /// </summary>
    public Series Field(int index) => Apply(e => e.Struct.Field(index));

    /// <summary>
    /// Rename the fields of the struct.
    /// </summary>
    public Series RenameFields(params string[] names) => Apply(e => e.Struct.RenameFields(names));

    /// <summary>
    /// Convert struct to JSON string.
    /// </summary>
    public Series JsonEncode() => Apply(e => e.Struct.JsonEncode());
    /// <summary>
    /// Unnest the struct column into a DataFrame.
    /// Each field of the struct becomes a separate column.
    /// </summary>
    public DataFrame Unnest()
    {
        var dfHandle = PolarsWrapper.SeriesStructUnnest(_series.Handle);
        return new DataFrame(dfHandle);
    }
}