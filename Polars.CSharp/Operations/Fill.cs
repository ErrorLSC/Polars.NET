using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Fill null values in all columns with a specified Expression.
    /// </summary>
    public LazyFrame FillNull(Expr fillValue)
    {
        ArgumentNullException.ThrowIfNull(fillValue);
        return WithColumns(Pl.All().FillNull(fillValue));
    }

    /// <summary>
    /// Fill null values in all columns with a specified literal value.
    /// </summary>
    public LazyFrame FillNull(object value)
        => FillNull(Expr.MakeLit(value));
    
    /// <summary>
    /// Fill NaN values in floating point columns with a specified Expression.
    /// </summary>
    public LazyFrame FillNan(Expr fillValue)
    {
        ArgumentNullException.ThrowIfNull(fillValue);
        
        return WithColumns(Cs.Float().ToExpr().FillNan(fillValue));
    }

    /// <summary>
    /// Fill NaN values in floating point columns with a specified literal value.
    /// </summary>
    public LazyFrame FillNan(object value)
        => FillNan(Expr.MakeLit(value));
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Fill null values in all columns with a specified Expression.
    /// </summary>
    public DataFrame FillNull(Expr fillValue)
    {
        ArgumentNullException.ThrowIfNull(fillValue);
        using var lf = Lazy();
        var filledLf = lf.FillNull(fillValue); 
        return filledLf.Collect();
    }

    /// <summary>
    /// Fill null values in all columns with a specified literal value.
    /// </summary>
    public DataFrame FillNull(object value)
    {
        using var lf = Lazy();
        var filledLf = lf.FillNull(value);
        return filledLf.Collect();
    }
    /// <summary>
    /// Fill NaN values in floating point columns with a specified Expression.
    /// </summary>
    public DataFrame FillNan(Expr fillValue)
    {
        ArgumentNullException.ThrowIfNull(fillValue);
        using var lf = Lazy();
        var filledLf = lf.FillNan(fillValue);
        return filledLf.Collect();
    }

    /// <summary>
    /// Fill NaN values in floating point columns with a specified literal value.
    /// </summary>
    public DataFrame FillNan(object value)
    {
        using var lf = Lazy();
        var filledLf = lf.FillNan(value);
        return filledLf.Collect();
    }
}