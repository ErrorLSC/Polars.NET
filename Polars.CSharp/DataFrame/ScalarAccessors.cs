using Polars.NET.Core;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{  
    /// <summary>
    /// Get a value from the DataFrame at the specified row and column.
    /// This is efficient for single-value lookups (no Arrow conversion).
    /// </summary>
    public T? GetValue<T>(long rowIndex, string colName)
    {
        var series = this[colName];
        
        return series.GetValue<T>(rowIndex);
    }
    /// <summary>
    /// Get a value from the DataFrame at the specified row and column.
    /// This is efficient for single-value lookups (no Arrow conversion).
    /// </summary>
    public T? GetValue<T>(string colName,long rowIndex)
    {
        var series = this[colName];
        return series.GetValue<T>(rowIndex);
    }

}