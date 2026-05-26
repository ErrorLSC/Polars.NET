#pragma warning disable CS1573
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Drop columns based on Strings, Selectors, Types, or Expressions.
    /// Usage: lf.Drop("Id", "Name", Cs.Numeric(), typeof(float), Pl.Col("Status"));
    /// </summary>
    public LazyFrame Drop(params IntoSelector[] columnsToDrop)
    {
        if (columnsToDrop.Length == 0) return this;

        var combinedSelector = columnsToDrop[0].Consume();
        for (int i = 1; i < columnsToDrop.Length; i++)
        {
            combinedSelector |= columnsToDrop[i].Consume();
        }

        using var sClone = combinedSelector.CloneHandle();
        var h = PolarsWrapper.LazyFrameDrop(CloneHandle(), sClone);
        
        return new LazyFrame(h);
    }

    /// <summary>
    /// Bridge overload to support C# 12 collection expressions.
    /// Usage: lf.Drop(["Id", "Name"])
    /// </summary>
    public LazyFrame Drop(IEnumerable<string> columns)
    {
        var colsArray = columns as string[] ?? [.. columns];
        if (colsArray.Length == 0) return this;
        
        using var selector = Cs.ByName(colsArray);
        return Drop(selector);
    }
    /// <summary>
    /// Drop rows containing one or more Null values in ALL columns.
    /// </summary>
    public LazyFrame DropNulls()
        => new (PolarsWrapper.LazyFrameDropNulls(CloneHandle(), null));

    /// <summary>
    /// Drop rows with Nulls in a specific subset of columns (Selectors, Strings, Types, Exprs).
    /// Usage: lf.DropNulls("Id", Cs.Numeric(), typeof(string), Pl.Col("Status"));
    /// </summary>
    public LazyFrame DropNulls(params IntoSelector[] subsets)
    {
        if (subsets.Length == 0) return DropNulls();

        var combinedSelector = subsets[0].Consume();
        for (int i = 1; i < subsets.Length; i++)
        {
            combinedSelector |= subsets[i].Consume();
        }

        using var sClone = combinedSelector.CloneHandle();
        return new LazyFrame(PolarsWrapper.LazyFrameDropNulls(CloneHandle(), sClone));
    }

    /// <summary>
    /// Bridge overload to support C# 12 collection expressions.
    /// Usage: lf.DropNulls(["Id", "Name"])
    /// </summary>
    public LazyFrame DropNulls(IEnumerable<string> subset)
    {
        var colsArray = subset as string[] ?? [.. subset];
        if (colsArray.Length == 0) return DropNulls();
        
        using var selector = Cs.ByName(colsArray);
        return DropNulls(selector); 
    }
    /// <summary>
    /// Drop rows containing one or more NaN values.
    /// </summary>
    /// <param name="subsets">Optional subset of columns to consider. If null, evaluates all columns.</param>
    public LazyFrame DropNans(params IntoSelector[] subsets)
    {
        if (subsets.Length == 0) return this;
        var combinedSelector = subsets[0].Consume();
        for (int i = 1; i < subsets.Length; i++)
        {
            combinedSelector |= subsets[i].Consume();
        }

        using var sClone = combinedSelector.CloneHandle();
        var h = PolarsWrapper.LazyFrameDropNans(CloneHandle(), sClone);
        return new LazyFrame(h);
    }
    /// <summary>
    /// Drop rows with NaN in specific columns.
    /// </summary>
    public LazyFrame DropNans(IEnumerable<string> subset)
    {
        var colsArray = subset as string[] ?? [.. subset];
        if (colsArray.Length == 0) return DropNans();
        using var selector = Cs.ByName(colsArray);
        return DropNans(selector); 
    }
    /// <summary>
    /// Drop NaN in all columns.
    /// </summary>
    public LazyFrame DropNans()
        => new (PolarsWrapper.LazyFrameDropNans(CloneHandle(), null));
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
    public DataFrame Drop(params IntoSelector[] exprs)
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
    public DataFrame DropNulls()
        => Lazy().DropNulls().Collect();

    /// <summary>
    /// Drop rows with Nulls in specific columns.
    /// </summary>
    public DataFrame DropNulls(IEnumerable<string> subsets)
        => Lazy().DropNulls(subsets).Collect();

    /// <summary>
    /// Drop rows with Nulls by specific Expressions.
    /// </summary>
    public DataFrame DropNulls(params IntoSelector[] subsets)
        => Lazy().DropNulls(subsets).Collect();
    
    /// <summary>
    /// Drop rows containing one or more NaN values.
    /// </summary>
    public DataFrame DropNans()
        => Lazy().DropNans().Collect();
    /// <summary>
    /// Drop rows with NaN in specific columns.
    /// </summary>
    public DataFrame DropNans(params IntoSelector[] subsets)
        => Lazy().DropNans(subsets).Collect();

    /// <summary>
    /// Drop rows with NaN by specific Expressions.
    /// </summary>
    public DataFrame DropNans(IEnumerable<string > subsets)
        => Lazy().DropNans(subsets).Collect();
}