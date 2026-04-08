#pragma warning disable 1591
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
            ArgumentNullException.ThrowIfNull(value);
            if (value.Name != columnName) value.Rename(columnName);
            this.Add(value); 
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
    /// e.g. df[^1, "A"]
    /// </summary>
    public object? this[Index rowIndex, string colName]
    {
        get
        {
            var series = this[colName];
            long height = series.Length;
            long actualIndex = rowIndex.IsFromEnd ? height - rowIndex.Value : rowIndex.Value;
            return series[(int)actualIndex];
        }
        set
        {
            var series = this[colName];
            long height = series.Length;
            long actualIndex = rowIndex.IsFromEnd ? height - rowIndex.Value : rowIndex.Value;
            
            using var clonedSeries = series.Clone();
            clonedSeries[(int)actualIndex] = value;
            ReplaceColumn(colName, clonedSeries, keepName: true);
        }
    }
    #region Series Row Mask
    /// <summary>
    /// e.g. df[df["Age"] > 18] 
    /// </summary>
    public DataFrame this[Series rowMask]
    {
        get => Filter(rowMask);
    }
    /// <summary>
    /// e.g. var adultNames = df[df["Age"] >= 18, "Name"];
    /// </summary>
    public Series this[Series rowMask, string columnName]
    {
        get 
        {
            if (rowMask.DataType != DataType.Boolean)
            {
                throw new ArgumentException($"Indexer expected a boolean mask Series, but got {rowMask.DataType}.", nameof(rowMask));
            }

            var targetSeries = this[columnName]; 
            return targetSeries.Filter(rowMask);
        }
        set
        {
            throw new NotSupportedException(
                "Not allowed to set DataFrame by boolean mask in the row position.\n\nConsider using `DataFrame.WithColumns` and `When.Then.Otherwise` expressions.");
        }
    }
    /// <summary>
    /// e.g. df[df["Age"] > 18, ["Name", "Score"]]
    /// </summary>
    public DataFrame this[Series rowMask, string[] columnNames]
    {
        get
        {
            if (rowMask.DataType.Kind != DataTypeKind.Boolean)
            {
                throw new ArgumentException($"Indexer expected a boolean mask Series, but got {rowMask.DataType.Kind}.", nameof(rowMask));
            }
    
            using var selectedDf = Select(columnNames);
            return selectedDf.Filter(rowMask);
        }
        set
        {
            throw new NotSupportedException(
                "Not allowed to set DataFrame by boolean mask in the row position.\n\nConsider using `DataFrame.WithColumns` and `When.Then.Otherwise` expressions.");
        }
    }

    /// <summary>
    /// e.g. df[df["Age"] > 18, Cs.Numeric()]
    /// </summary>
    public DataFrame this[Series rowMask, IntoSelector columnSelector]
    {
        get
        {
            if (rowMask.DataType.Kind != DataTypeKind.Boolean)
            {
                throw new ArgumentException($"Indexer expected a boolean mask Series, but got {rowMask.DataType.Kind}.", nameof(rowMask));
            }

            using var selector = columnSelector.Consume();
            string[] columns = Cs.ExpandSelector(this, selector);

            if (columns.Length == 0)
            {
                return []; 
            }

            using var selectedDf = Select(columns);
            return selectedDf.Filter(rowMask);
        }
        set
        {
            throw new NotSupportedException(
                "Not allowed to set DataFrame by boolean mask in the row position.\n\nConsider using `DataFrame.WithColumns` and `When.Then.Otherwise` expressions.");
        }
    }
    #endregion

    #region Selector
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
        set
        {
            ArgumentNullException.ThrowIfNull(value);

            using var selector = columnSelector.Consume();
            string[] columns = Cs.ExpandSelector(this, selector);

            if (columns.Length == 0)
            {
                throw new ArgumentException("The provided selector did not match any columns in the DataFrame.");
            }

            this[columns] = value;
        }
    }

    #endregion
    # region Range Indexer
    /// <summary>
    /// e.g. df[1..5] / df[..^1]
    /// </summary>
    public DataFrame this[Range rowRange]
    {
        get => Slice(rowRange);
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
    /// <summary>
    /// e.g. df[1..10, ["A", "B"]]
    /// </summary>
    public DataFrame this[Range rowRange,string[] columnNames]
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

    # endregion
    /// <summary>
    /// e.g. df[0, ["A", "B"]], df[^1, ["A", "B"]]
    /// </summary>
    public DataFrame this[Index rowIndex, string[] columnNames]
    {
        get
        {
            long height = Height;
            long actualIndex = rowIndex.IsFromEnd ? height - rowIndex.Value : rowIndex.Value;

            if (actualIndex < 0 || actualIndex >= height)
            {
                throw new ArgumentOutOfRangeException(nameof(rowIndex), $"Row index {actualIndex} is out of bounds (Height: {height}).");
            }

            using var selectedDf = Select(columnNames);
            
            return selectedDf.Slice(actualIndex, 1);
        }
    }

    /// <summary>
    /// e.g. df[^2, Cs.Numeric()], df[5, typeof(double)], df[0, Pl.Col("Id")]
    /// </summary>
    public DataFrame this[Index rowIndex, IntoSelector columnSelector]
    {
        get
        {
            long height = Height;
            long actualIndex = rowIndex.IsFromEnd ? height - rowIndex.Value : rowIndex.Value;

            if (actualIndex < 0 || actualIndex >= height)
            {
                throw new ArgumentOutOfRangeException(nameof(rowIndex), $"Row index {actualIndex} is out of bounds (Height: {height}).");
            }

            using var selector = columnSelector.Consume();
            string[] columns = Cs.ExpandSelector(this, selector);

            if (columns.Length == 0)
            {
                using var emptyDf = this.Clear();
                return emptyDf.Slice(0, 0);
            }

            using var selectedDf = Select(columns);
            return selectedDf.Slice(actualIndex, 1);
        }
    }
    /// <summary>
    /// e.g. df + 1
    /// </summary>
    public static DataFrame operator +(DataFrame df, int scalar) => df.Select(Pl.All() + scalar);
    public static DataFrame operator +(DataFrame df, double scalar) => df.Select(Pl.All() + scalar);
    public static DataFrame operator -(DataFrame df, int scalar) => df.Select(Pl.All() - scalar);
    public static DataFrame operator -(DataFrame df, double scalar) => df.Select(Pl.All() - scalar);
    public static DataFrame operator *(DataFrame df, int scalar) => df.Select(Pl.All() * scalar);
    public static DataFrame operator *(DataFrame df, double scalar) => df.Select(Pl.All() * scalar);
    public static DataFrame operator /(DataFrame df, int scalar) => df.Select(Pl.All() / scalar);
    public static DataFrame operator /(DataFrame df, double scalar) => df.Select(Pl.All() / scalar);
}