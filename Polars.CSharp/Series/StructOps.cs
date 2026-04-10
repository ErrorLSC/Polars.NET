using Pl = Polars.CSharp.Polars;
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
    public Series Field(params string[] name) => Apply(e => e.Struct.Field(name));

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
    public DataFrame Unnest() => new(PolarsWrapper.SeriesStructUnnest(_series.Handle));

    /// <summary>
    /// Add or overwrite fields of this struct.This is similar to with_columns on DataFrame.
    /// </summary>
    /// <param name="expr">Field(s) to add, specified as positional arguments. Accepts expression input. Strings are parsed as column names, other non-expression inputs are parsed as literals.</param>
    /// <returns></returns>
    public Series WithFields(params IntoExpr[] expr) => Apply(e => e.Struct.WithFields(expr));

    /// <inheritdoc cref="Field(int)"/>
    public Series this[int index] => Field(index);

    /// <summary>
    /// Retrieve one of the fields of this Struct as a new Series.
    /// </summary>
    public Series this[string name] => Field(name);

    /// <inheritdoc cref="Field(string[])"/>
    public Series this[string[] names] 
    {
        get
        {
            return Apply(e => 
            {
                var exprs = names.Select(name => (IntoExpr)e.Struct.Field(name)).ToArray();
                return Pl.Struct(exprs);
            });
        }
    }
    /// <summary>
    /// Get the struct definition as a name/dtype schema.
    /// </summary>
    public PolarsSchema Schema
    {
        get
        {
            var fields = _series.DataType.StructFields;
            if (fields == null)
            {
                return new PolarsSchema(); 
            }
            
            return PolarsSchema.From(fields);
        }
    }

    /// <summary>
    /// Get the names of the fields.
    /// </summary>
    public string[] Fields
    {
        get
        {
            var fields = _series.DataType.StructFields;
            if (fields == null)
            {
                return [];
            }

            return [.. fields.Select(f => f.Name)];
        }
    }
}