using Polars.NET.Core;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Keep unique rows (stable) based on a subset of columns defined by a Selector.
    /// </summary>
    /// <param name="subset">Selector defining the subset of columns. If null, uses all columns.</param>
    /// <param name="keep">Strategy to keep duplicates (First, Last, Any, None).</param>
    /// <param name="maintainOrder">Whether to maintain the original order of the rows (stable).</param>
    public LazyFrame Unique(Selector? subset = null, UniqueKeepStrategy keep = UniqueKeepStrategy.First, bool maintainOrder=false)
        => new (PolarsWrapper.LazyUnique(CloneHandle(), subset?.CloneHandle()!, keep.ToNative(),maintainOrder));
    
    /// <summary>
    /// Keep unique rows based on specific column names.
    /// </summary>
    /// <param name="columns">A collection of column names to group by.</param>
    /// <param name="keep">Strategy to keep duplicates (First, Last, Any, None).</param>
    /// <param name="maintainOrder">Whether to maintain the original order of the rows (stable).</param>
    public LazyFrame Unique(
        IEnumerable<string> columns, 
        UniqueKeepStrategy keep = UniqueKeepStrategy.First, 
        bool maintainOrder = false)
    {
        var columnsArray = columns as string[] ?? [.. columns];
        
        if (columnsArray.Length == 0)
        {
            return Unique(subset: null, keep, maintainOrder);
        }

        using var selector = Cs.ByName(columnsArray);
        
        return Unique(selector, keep, maintainOrder);
    }
    /// <summary>
    /// Keep unique rows based on a subset of columns (Selector, strings, Types, etc.).
    /// </summary>
    public LazyFrame Unique(IntoSelector subset, UniqueKeepStrategy keep = UniqueKeepStrategy.First, bool maintainOrder = false)
    {
        using var selector = subset.Consume();
        return new(PolarsWrapper.LazyUnique(CloneHandle(), selector.CloneHandle(), keep.ToNative(), maintainOrder));
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Returns a new DataFrame with unique rows based on ALL columns.
    /// </summary>
    /// <param name="keep">The strategy for which duplicate rows to retain (First, Last, Any, or None).</param>
    /// <param name="maintainOrder">Keep the same order as the original DataFrame.</param>
    /// <param name="offset">The starting index from which to begin the slice of unique results.</param>
    /// <param name="len">The maximum number of rows to include in the result.</param>
    public DataFrame Unique(
        UniqueKeepStrategy keep = UniqueKeepStrategy.First, 
        bool maintainOrder = false,
        long? offset = null, 
        long? len = null)
    {
        (long offset, ulong len)? slice = (offset.HasValue && len.HasValue) 
            ? (offset.Value, (ulong)Math.Max(0, len.Value)) 
            : null;

        var h = PolarsWrapper.DataFrameUnique(
            Handle, 
            null, 
            keep.ToNative(), 
            maintainOrder,
            slice
        );

        return new DataFrame(h);
    }
    /// <summary>
    /// Returns a new DataFrame with unique rows based on specific column names.
    /// Supports C# 12 collection expressions like ["Id", "Date"].
    /// </summary>
    public DataFrame Unique(
        IEnumerable<string> subset, 
        UniqueKeepStrategy keep = UniqueKeepStrategy.First, 
        bool maintainOrder = false,
        long? offset = null, 
        long? len = null)
    {
        var columns = subset as string[] ?? subset.ToArray();
        
        if (columns.Length == 0)
        {
            return Unique(keep, maintainOrder, offset, len);
        }

        (long offset, ulong len)? slice = (offset.HasValue && len.HasValue) 
            ? (offset.Value, (ulong)Math.Max(0, len.Value)) 
            : null;

        var h = PolarsWrapper.DataFrameUnique(
            Handle, 
            columns, 
            keep.ToNative(), 
            maintainOrder,
            slice
        );

        return new DataFrame(h);
    }
    /// <summary>
    /// Returns a new DataFrame with unique rows based on a subset of columns (Selector, Type, DataType).
    /// </summary>
    public DataFrame Unique(
        IntoSelector subset, 
        UniqueKeepStrategy keep = UniqueKeepStrategy.First, 
        bool maintainOrder = false,
        long? offset = null, 
        long? len = null)
    {
        using var selector = subset.Consume();
        
        string[] columns = Cs.ExpandSelector(this, selector);

        if (columns.Length == 0)
        {
            throw new ArgumentException("No Columns Selected. The given subset/selector did not match any columns.");
        }

        return Unique(columns, keep, maintainOrder, offset, len);
    }
}