using System.Collections;
using Polars.NET.Core;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
       /// <summary>
    /// Get a column as a Series by name.
    /// </summary>
    public Series Column(string name)
    {
        var sHandle = PolarsWrapper.DataFrameGetColumn(Handle, name);
        
        return new Series(name, sHandle);
    }

    /// <summary>
    /// Get a column as a Series by name (Indexer syntax).
    /// Usage: var s = df["age"];
    /// </summary>
    public Series this[string columnName]
    {
        get => Column(columnName);
    }

    /// <exception cref="IndexOutOfRangeException"></exception>
    /// <summary>
    /// Get a column by its positional index (0-based).
    /// </summary>
    public Series Column(int index)
    {
        var h = PolarsWrapper.DataFrameGetColumnAt(Handle, index);
        return new Series(h);
    }
    IPolarsSeries IPolarsDataFrame.Column(int index) => Column(index);
    /// <summary>
    /// Get all columns as a list of Series.
    /// Order is guaranteed to match the physical column order.
    /// </summary>
    public Series[] GetColumns()
    {
        int width = (int)Width;
        var cols = new Series[width];
        
        for (int i = 0; i < width; i++)
        {
            cols[i] = Column(i); 
        }
        
        return cols;
    }
    
    /// <summary>
    /// Indexer to get a column by position.
    /// Usage: var s = df[0];
    /// </summary>
    public Series this[int index] => Column(index);
    /// <summary>
    /// Syntax Sugar
    /// </summary>
    /// <param name="rowIndex"></param>
    /// <param name="columnIndex"></param>
    /// <returns></returns>
    public object? this[int rowIndex, int columnIndex]
    {
        get
        {
            var series = Column(columnIndex);
            return series[rowIndex];
        }
    }
    /// <summary>
    /// Enable foreach (var series in df) { ... }
    /// </summary>
    /// <returns></returns>
    public IEnumerator<Series> GetEnumerator()
    {
        for (int i = 0; i < Width; i++)
        {
            yield return Column(i);
        }
    }
    /// <summary>
    /// Returns an iterator over the columns of this DataFrame.
    /// </summary>
    /// <returns></returns>
    public IEnumerator<Series> IterColumns() => GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}