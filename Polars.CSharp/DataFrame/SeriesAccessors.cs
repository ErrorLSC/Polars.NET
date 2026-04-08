using System.Collections;
using System.Runtime.CompilerServices;
using Polars.NET.Core;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

[CollectionBuilder(typeof(DataFrame), nameof(FromSeries))]
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
    /// <summary>
    /// Appends a new column to the end of the DataFrame, or replaces an existing column if the name already exists.
    /// </summary>
    /// <remarks>
    /// This method enables the C# collection initializer syntax (e.g., <c>var df = new DataFrame { s1, s2 };</c>).
    /// If the DataFrame already contains a column with the same name as the provided <paramref name="series"/>, 
    /// it performs an highly efficient in-place replacement. Otherwise, it appends the new column to the right.
    /// </remarks>
    /// <param name="series">The Series to add or update. The column name is determined by the Series name.</param>
    /// <exception cref="ArgumentNullException">Thrown when the provided series is null.</exception>
    public void Add(Series series)
    {
        ArgumentNullException.ThrowIfNull(series);

        if (System.Array.IndexOf(Columns, series.Name) >= 0)
        {
            ReplaceColumn(series.Name, series, keepName: true);
        }
        else
        {
            using var appendedDf = InsertColumn((int)Width, Pl.Lit(series));
            ReplaceInnerHandle(PolarsWrapper.CloneDataFrame(appendedDf.Handle));
        }
    }
}