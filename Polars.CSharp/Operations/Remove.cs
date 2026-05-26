using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Remove rows that match the predicate.
    /// This is the exact opposite of Filter. Rows where the predicate evaluates to False or Null are kept.
    /// </summary>
    public LazyFrame Remove(Expr predicate)
    {
        using var litTrue = Pl.Lit(true);
        using var invertedExpr = predicate.NeqMissing(litTrue);
        
        return Filter(invertedExpr);
    }

    /// <summary>
    /// Remove rows based on a boolean series.
    /// </summary>
    public LazyFrame Remove(Series predicate)
    {
        if (predicate.DataType != DataType.Boolean)
        {
            throw new InvalidOperationException("Can not remove by non-boolean series.");
        }
        
        using var expr = Pl.Lit(predicate); 
        return Remove(expr); 
    }

    /// <summary>
    /// Remove rows based on a boolean array.
    /// </summary>
    public LazyFrame Remove(IEnumerable<bool> mask)
    {
        using var expr = Pl.Lit(mask);
        return Remove(expr);
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Remove rows that match the predicate.
    /// This is the exact opposite of Filter. Rows where the predicate evaluates to False or Null are kept.
    /// </summary>
    public DataFrame Remove(Expr predicate) => Lazy().Remove(predicate).Collect();

    /// <summary>
    /// Remove rows based on a boolean series.
    /// </summary>
    public DataFrame Remove(Series predicate) => Lazy().Remove(predicate).Collect();

    /// <summary>
    /// Remove rows based on a boolean array.
    /// </summary>
    public DataFrame Remove(IEnumerable<bool> mask) => Lazy().Remove(mask).Collect();
}