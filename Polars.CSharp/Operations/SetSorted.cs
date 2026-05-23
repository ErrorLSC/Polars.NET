#pragma warning disable CS1591
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Mark one or multiple columns as sorted. 
    /// This is an optimizer hint and will not actually execute a sorting operation.
    /// </summary>
    /// <param name="columns">The columns to mark as sorted. Accepts strings, arrays, or Selectors (e.g. Cs.Temporal()).</param>
    /// <param name="descending">Whether the columns are sorted in descending order.</param>
    /// <param name="nullsLast">Whether null values appear last.</param>
    /// <returns>A new Frame with the sorted hints applied to the query plan.</returns>
    public LazyFrame SetSorted(IntoSelector columns, bool descending = false, bool nullsLast = false)
    {
        using var selector = columns.Consume();
        
        string[] cols = Cs.ExpandSelector(this, selector);
        
        if (cols.Length == 0)
        {
            return this.Clone();
        }

        var exprs = cols.Select(c => Pl.Col(c).SetSorted(descending, nullsLast));

        return WithColumns(exprs);
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <inheritdoc cref="LazyFrame.SetSorted(IntoSelector, bool, bool)"/>
    public DataFrame SetSorted(IntoSelector columns, bool descending = false, bool nullsLast = false) => Lazy().SetSorted(columns,descending,nullsLast).Collect();
}