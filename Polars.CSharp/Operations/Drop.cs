#pragma warning disable CS1573
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Drop selected columns by selector.
    /// </summary>
    /// <param name="selector"></param>
    /// <returns></returns>
    public LazyFrame Drop(Selector selector)
    {
        var lfClone = CloneHandle();
        var sClone = selector.CloneHandle();
        var h = PolarsWrapper.LazyFrameDrop(lfClone, sClone);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Drop selected columns by column names.
    /// </summary>
    /// <param name="columns"></param>
    /// <returns></returns>
    public LazyFrame Drop(params string[] columns)
        => Drop(Cs.ByName(columns));

    /// <summary>
    /// Drop columns by specific Expressions.
    /// </summary>
    public LazyFrame Drop(params Expr[] exprs)
    {
        ArgumentNullException.ThrowIfNull(exprs);

        var currentLf = this;
        foreach (var expr in exprs)
        {
            currentLf = currentLf.Drop(expr.ToSelector());
        }
        
        return currentLf;
    }
    /// <summary>
    /// Drop rows containing one or more Null values.
    /// </summary>
    /// <param name="subset">Optional subset of columns to consider. If null, evaluates all columns.</param>
    public LazyFrame DropNulls(Selector? subset=null)
    {
        var lfClone = CloneHandle();
        var sClone = subset?.CloneHandle();
        var h = PolarsWrapper.LazyFrameDropNulls(lfClone, sClone);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Drop rows with Nulls in specific columns.
    /// </summary>
    public LazyFrame DropNulls(params string[] subset)
    {
        if (subset == null || subset.Length == 0) return DropNulls((Selector?)null);
        return DropNulls(Cs.ByName(subset));
    }
    /// <summary>
    /// DropNulls columns by specific Expressions.
    /// </summary>
    public LazyFrame DropNulls(params Expr[] exprs)
    {
        ArgumentNullException.ThrowIfNull(exprs);

        var currentLf = this;
        foreach (var expr in exprs)
        {
            currentLf = currentLf.DropNulls(expr.ToSelector());
        }
        
        return currentLf;
    }
    /// <summary>
    /// Drop rows containing one or more NaN values.
    /// </summary>
    /// <param name="subset">Optional subset of columns to consider. If null, evaluates all columns.</param>
    public LazyFrame DropNan(Selector? subset=null)
    {
        var lfClone = CloneHandle();
        var sClone = subset?.CloneHandle();
        var h = PolarsWrapper.LazyFrameDropNans(lfClone, sClone);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Drop rows with NaN in specific columns.
    /// </summary>
    public LazyFrame DropNan(params string[] subset)
    {
        if (subset == null || subset.Length == 0) return DropNan((Selector?)null);
        return DropNan(Cs.ByName(subset));
    }
    /// <summary>
    /// DropNaN columns by specific Expressions.
    /// </summary>
    public LazyFrame DropNan(params Expr[] exprs)
    {
        ArgumentNullException.ThrowIfNull(exprs);

        var currentLf = this;
        foreach (var expr in exprs)
        {
            currentLf = currentLf.DropNan(expr.ToSelector());
        }
        
        return currentLf;
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{    
    /// <summary>
    /// Drop one or more columns from the DataFrame.
    /// Returns a new DataFrame.
    /// </summary>
    public DataFrame Drop(params string[] columns)
    {
        if (columns == null || columns.Length == 0)
        {
            return new DataFrame(PolarsWrapper.CloneDataFrame(Handle));
        }

        var newHandle = PolarsWrapper.Drop(Handle, columns);
        return new DataFrame(newHandle);
    }
    /// <summary>
    /// Drop columns using Polars Selectors or Expressions.
    /// Example: df.Drop(Cs.EndsWith("_tmp").ToExpr())
    /// </summary>
    public DataFrame Drop(params Expr[] exprs)
    {
        ArgumentNullException.ThrowIfNull(exprs);

        using var lf = this.Lazy();
        using var droppedLf = lf.Drop(exprs);
        return droppedLf.Collect();
    }
    /// <summary>
    /// Drop a column in-place and return it as a Series.
    /// Note: This mutates the original DataFrame.
    /// </summary>
    /// <param name="name">The name of the column to drop.</param>
    /// <returns>The dropped column as a Series.</returns>
    public Series DropInPlace(string name)
    {
        ArgumentException.ThrowIfNullOrEmpty(name);

        return new Series(PolarsWrapper.DropInPlace(Handle, name));
    }
    /// <summary>
    /// Drop rows containing one or more Null values.
    /// </summary>
    public DataFrame DropNulls(Selector? subset = null)
    {
        using var lf = Lazy();
        var droppedLf = lf.DropNulls(subset);
        return droppedLf.Collect();
    }

    /// <summary>
    /// Drop rows with Nulls in specific columns.
    /// </summary>
    public DataFrame DropNulls(params string[] subset)
    {
        using var lf = Lazy();
        var droppedLf = lf.DropNulls(subset);
        return droppedLf.Collect();
    }

    /// <summary>
    /// Drop rows with Nulls by specific Expressions.
    /// </summary>
    public DataFrame DropNulls(params Expr[] exprs)
    {
        using var lf = Lazy();
        var droppedLf = lf.DropNulls(exprs);
        return droppedLf.Collect();
    }

    /// <summary>
    /// Drop rows containing one or more NaN values.
    /// </summary>
    public DataFrame DropNan(Selector? subset = null)
    {
        using var lf = Lazy();
        var droppedLf = lf.DropNan(subset);
        return droppedLf.Collect();
    }

    /// <summary>
    /// Drop rows with NaN in specific columns.
    /// </summary>
    public DataFrame DropNan(params string[] subset)
    {
        using var lf = Lazy();
        var droppedLf = lf.DropNan(subset);
        return droppedLf.Collect();
    }

    /// <summary>
    /// Drop rows with NaN by specific Expressions.
    /// </summary>
    public DataFrame DropNan(params Expr[] exprs)
    {
        using var lf = Lazy();
        var droppedLf = lf.DropNan(exprs);
        return droppedLf.Collect();
    }
}