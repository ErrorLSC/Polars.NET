using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;
using Polars.NET.Core;

namespace Polars.CSharp;

public partial class DataFrame : IDisposable,IEnumerable<Series>,IEquatable<DataFrame>,IPolarsDataFrame
{
    /// <summary>
    /// df["foo"] = series
    /// </summary>
    public Series this[string columnName]
    {
        get => Column(columnName);
        set 
        {
            throw new NotSupportedException(
                "DataFrame object does not support `Series` assignment by index.\n\nUse `DataFrame.WithColumns`.");
        }
    }

    /// <summary>
    /// df[rowIndex, colName] = value 
    /// </summary>
    public object? this[int rowIndex, string colName]
    {
        get
        {
            var series = this[colName];
            return series[rowIndex];
        }
        set
        {
            var targetSeries = this[colName];

            using var clonedSeries = targetSeries.Clone();

            clonedSeries[rowIndex] = value;

            ReplaceColumn(colName, clonedSeries, keepName: true);
        }
    }

    /// <summary>
    /// df[row_selection, colName]
    /// </summary>
    public object? this[Series rowMask, string colName]
    {
        get 
        {
            return Filter(rowMask); 
        }
        set
        {
            if (rowMask.DataType == DataType.Boolean)
            {
                throw new NotSupportedException(
                    "Not allowed to set DataFrame by boolean mask in the row position.\n\nConsider using `DataFrame.WithColumns`.");
            }
            throw new NotSupportedException($"Unexpected row selection type: {rowMask.DataType}");
        }
    }

    /// <summary>
    /// e.g. df[["A", "B"]] = df2[["X", "Y"]]; 
    /// e.g. df[["A", "B"]] = [s1, s2];
    /// </summary>
    public DataFrame this[string[] columnNames]
    {
        get => Select(columnNames);
        set
        {
            ArgumentNullException.ThrowIfNull(value);
            
            if (value.Width != columnNames.Length)
                throw new ArgumentException($"Provided DataFrame/Collection has {value.Width} columns, but {columnNames.Length} were expected.");

            var seriesArray = value.ToArray();

            for (int i = 0; i < columnNames.Length; i++)
            {
                string colName = columnNames[i];
                var newSeries = seriesArray[i];

                if (Array.IndexOf(Columns, colName) >= 0)
                {
                    this.ReplaceColumn(colName, newSeries, keepName: true);
                }
                else
                {
                    using var appendedDf = InsertColumn((int)this.Width, Pl.Lit(newSeries));
                    ReplaceInnerHandle(PolarsWrapper.CloneDataFrame(appendedDf.Handle));
                }
            }
        }
    }
    # region Range Indexer
    /// <summary>
    /// e.g. df[1..5] / df[..^1]
    /// </summary>
    public DataFrame this[Range rowRange]
    {
        get => Slice(rowRange);
    }

    /// <summary>
    /// e.g. df[1..10, ["A", "B"]]
    /// </summary>
    public DataFrame this[Range rowRange, string[] columnNames]
    {
        get
        {
            using var selectedDf = Select(columnNames);
            return selectedDf.Slice(rowRange);
        }
    }
    /// <summary>
    /// e.g. df[1..10, Cs.Numeric()], df[..^1, typeof(int)], df[1..5, "A"], df[0..2, Pl.Col("Id")]
    /// </summary>
    public DataFrame this[Range rowRange, IntoSelector columnSelector]
    {
        get
        {
            using var selector = columnSelector.Consume();

            string[] columnNames = Cs.ExpandSelector(this, selector);

            if (columnNames.Length == 0)
            {
                using var emptyDf = Clear();
                return emptyDf.Slice(rowRange);
            }

            using var selectedDf = Select(columnNames);
            return selectedDf.Slice(rowRange);
        }
    }

    /// <summary>
    /// e.g. df[1..10, "A"]
    /// </summary>
    public Series this[Range rowRange, string columnName]
    {
        get
        {
            var targetSeries = this[columnName];
            return targetSeries[rowRange]; 
        }
    }

    # endregion

    # region Column Indexer
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
    /// e.g. df[Cs.Numeric()], df[typeof(int)], df[Pl.Col("Id")]
    /// </summary>
    public DataFrame this[IntoSelector columnSelector]
    {
        get
        {
            using var selector = columnSelector.Consume();
            return this.Select(selector);
        }
    }
    # endregion
}