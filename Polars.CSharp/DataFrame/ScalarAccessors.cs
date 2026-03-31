using Polars.NET.Core;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{  
    // ==========================================
    // Scalar Access (Direct)
    // ==========================================

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

    /// <summary>
    /// Get value by row index and column name (object type).
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="colName"></param>
    /// <returns></returns>
    public object? this[int rowIndex, string colName]
    {
        get
        {
            var series = this[colName];
            return series[rowIndex];
        }
    }
    
    /// <summary>
    /// Get value by row index and column name (object type).
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="colName"></param>
    /// <returns></returns>
    public object? this[string colName,int rowIndex]
    {
        get
        {
            var series = this[colName];
            return series[rowIndex]; 
        }
    }
    
}