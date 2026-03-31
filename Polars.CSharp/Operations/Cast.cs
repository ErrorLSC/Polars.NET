#pragma warning disable CS1573
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Cast LazyFrame column(s) to the specified dtype(s) using a dictionary mapping.
    /// </summary>
    public LazyFrame Cast(IDictionary<string, DataType> dtypes, bool strict = true)
    {
        ArgumentNullException.ThrowIfNull(dtypes);

        var castExprs = dtypes.Select(kvp =>
            Pl.Col(kvp.Key).Cast(kvp.Value, strict)
        );

        return WithColumns([.. castExprs]);
    }

    /// <summary>
    /// Cast all columns in the LazyFrame to the specified dtype.
    /// </summary>
    public LazyFrame Cast(DataType dtype, bool strict = true)
        => Select(Pl.All().Cast(dtype, strict));

    /// <summary>
    /// Cast columns matching a specific Expression or Selector to a DataType.
    /// Example: lf.Cast(Cs.EndsWith("Cm").ToExpr(), DataType.Float32)
    /// </summary>
    public LazyFrame Cast(Expr expr, DataType dtype, bool strict = true)
    {
        ArgumentNullException.ThrowIfNull(expr);
        
        return WithColumns(expr.Cast(dtype, strict));
    }

    /// <summary>
    /// Cast multiple expressions/selectors to their target DataTypes using tuples.
    /// </summary>
    public LazyFrame Cast(IEnumerable<(Expr Expr, DataType Dtype)> dtypes, bool strict = true)
    {
        ArgumentNullException.ThrowIfNull(dtypes);

        var castExprs = dtypes.Select(t => t.Expr.Cast(t.Dtype, strict)).ToArray();
        return WithColumns(castExprs);
    }

    /// <summary>
    /// lf.Cast((Cs.Numeric().ToExpr(), DataType.Float32), (Polars.Col("Id"), DataType.Int32))
    /// </summary>
    public LazyFrame Cast(params (Expr Expr, DataType Dtype)[] dtypes)
        =>Cast((IEnumerable<(Expr, DataType)>)dtypes, strict: true);
}
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Cast DataFrame column(s) to the specified dtype(s) using a dictionary mapping.
    /// </summary>
    public DataFrame Cast(IDictionary<string, DataType> dtypes, bool strict = true)
    {
        ArgumentNullException.ThrowIfNull(dtypes);

        var castExprs = dtypes.Select(kvp =>
            Pl.Col(kvp.Key).Cast(kvp.Value, strict)
        );

        return WithColumns([.. castExprs]);
    }

    /// <summary>
    /// Cast all columns in the DataFrame to the specified dtype.
    /// </summary>
    public DataFrame Cast(DataType dtype, bool strict = true)
        => Select(Pl.All().Cast(dtype, strict));

    /// <summary>
    /// Cast columns matching a specific Expression or Selector to a DataType.
    /// Example: lf.Cast(Cs.EndsWith("Cm").ToExpr(), DataType.Float32)
    /// </summary>
    public DataFrame Cast(Expr expr, DataType dtype, bool strict = true)
    {
        ArgumentNullException.ThrowIfNull(expr);
        
        return WithColumns(expr.Cast(dtype, strict));
    }

    /// <summary>
    /// Cast multiple expressions/selectors to their target DataTypes using tuples.
    /// </summary>
    public DataFrame Cast(IEnumerable<(Expr Expr, DataType Dtype)> dtypes, bool strict = true)
    {
        ArgumentNullException.ThrowIfNull(dtypes);

        var castExprs = dtypes.Select(t => t.Expr.Cast(t.Dtype, strict)).ToArray();
        return WithColumns(castExprs);
    }

    /// <summary>
    /// lf.Cast((Cs.Numeric().ToExpr(), DataType.Float32), (Polars.Col("Id"), DataType.Int32))
    /// </summary>
    public DataFrame Cast(params (Expr Expr, DataType Dtype)[] dtypes)
        =>Cast((IEnumerable<(Expr, DataType)>)dtypes, strict: true);  
}