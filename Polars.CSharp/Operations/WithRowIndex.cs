using Polars.NET.Core;
namespace Polars.CSharp;
public partial class LazyFrame : IDisposable, IPolarsLazyFrame
{
    /// <summary>
    /// Add a column at index 0 that counts the rows.
    /// <para>
    /// This can be useful to generate a unique index or to maintain order in operations 
    /// that would otherwise discard it.
    /// </para>
    /// </summary>
    /// <param name="name">The name of the new row index column. Defaults to "index".</param>
    /// <param name="offset">The starting value of the row index. Defaults to 0.</param>
    /// <returns>A new LazyFrame with the row index column added.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if offset is negative.</exception>
    public LazyFrame WithRowIndex(string name = "index", int? offset = null)
    {
        if (offset.HasValue && offset.Value < 0)
        {
            string issue = "negative";
            throw new ArgumentOutOfRangeException(
                nameof(offset), 
                $"`offset` input for `WithRowIndex` cannot be {issue}, got {offset.Value}"
            );
        }

        var lfClone = CloneHandle();
        
        var newHandle = PolarsWrapper.LazyFrameWithRowIndex(lfClone, name, offset);
        
        return new LazyFrame(newHandle);
    }
}

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    /// <summary>
    /// Add a column at index 0 that counts the rows.
    /// <para>
    /// This can be useful to generate a unique index or to maintain order in operations 
    /// that would otherwise discard it.
    /// </para>
    /// </summary>
    /// <param name="name">The name of the new row index column. Defaults to "index".</param>
    /// <param name="offset">The starting value of the row index. Defaults to 0.</param>
    /// <returns>A new DataFrame with the row index column added.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown if offset is negative.</exception>
    public DataFrame WithRowIndex(string name = "index", int? offset = null)
    {
        if (offset.HasValue && offset.Value < 0)
        {
            string issue = "negative";
            throw new ArgumentOutOfRangeException(
                nameof(offset), 
                $"`offset` input for `WithRowIndex` cannot be {issue}, got {offset.Value}"
            );
        }
        
        var newHandle = PolarsWrapper.DataFrameWithRowIndex(Handle, name, offset);
        
        return new DataFrame(newHandle);
    }
}