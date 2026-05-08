using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using System.Data;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;
#pragma warning disable CS1591 

namespace Polars.CSharp;

/// <summary>
/// DataFrame represents a 2-dimensional labeled data structure similar to a table or spreadsheet.
/// </summary>
public partial class DataFrame : IDisposable,IEnumerable<Series>,IEquatable<DataFrame>,IPolarsDataFrame
{
    internal DataFrameHandle Handle { get; private set; }

    internal DataFrame(DataFrameHandle handle)
    {
        Handle = handle;
    }
    private void ReplaceInnerHandle(DataFrameHandle newHandle)
    {
        var oldHandle = Handle;
        Handle = newHandle;
        oldHandle?.Dispose(); 
    }
    // ==========================================
    // Metadata
    // ==========================================

    /// <summary>
    /// Gets the Schema of the DataFrame.
    /// Returns a disposable PolarsSchema object.
    /// </summary>
    public PolarsSchema Schema
    {
        get
        {
            var handle = PolarsWrapper.GetDataFrameSchema(Handle);
            return new PolarsSchema(handle);
        }
    }
    /// <summary>
    /// Gets the Schema of the DataFrame.Same as Schema field.
    /// </summary>
    /// <returns></returns>
    public PolarsSchema CollectSchema() => Schema;
    IPolarsSchema IPolarsDataFrame.Schema => Schema;
    /// <summary>
    /// Prints the schema of the DataFrame to the standard output.
    /// </summary>
    public void PrintSchema()
    {
        using var schema = Schema;
        var fields = schema.ToList(); 

        int maxNameLen = fields.Count > 0 ? fields.Max(f => f.Name.Length) : 15;
        maxNameLen = Math.Max(maxNameLen, 10); 

        Console.WriteLine("--- DataFrame Schema ---");
        
        foreach (var field in fields)
        {
            Console.WriteLine($"{field.Name.PadRight(maxNameLen)} | {field.Type}");
        }
        
        Console.WriteLine(new string('-', maxNameLen + 26));
    }
 
    // ==========================================
    // Properties
    // ==========================================
    /// <summary>
    /// Return DataFrame Height
    /// </summary>
    public long Height => PolarsWrapper.DataFrameHeight(Handle);
    /// <summary>
    /// Return DataFrame Height
    /// </summary>
    public long Len => PolarsWrapper.DataFrameHeight(Handle); 
    /// <summary>
    /// Return DataFrame Width
    /// </summary>
    public long Width => PolarsWrapper.DataFrameWidth(Handle);  
    /// <summary>
    /// Return DataFrame Shape(Len,Width)
    /// </summary>  
    public (long Len, long Width) Shape => (Len,Width);
    /// <summary>
    /// Return DataFrame Columns' Name
    /// </summary>
    public string[] Columns => PolarsWrapper.GetColumnNames(Handle);
    /// <summary>
    /// Get column names in order.
    /// </summary>
    public string[] ColumnNames => PolarsWrapper.GetColumnNames(Handle);
    /// <summary>
    /// Return DataFrame Columns' DataType
    /// </summary>
    public IReadOnlyList<DataType> DataTypes => Schema.DataTypes;
    /// <summary>
    /// Hash and combine the rows in this DataFrame.
    /// </summary>
    /// <param name="seed">Random seed parameter. Defaults to 42 for reproducible hashing.</param>
    /// <returns>A Series containing the UInt64 hashes.</returns>
    public Series HashRows(ulong? seed = 42) => new(PolarsWrapper.DataFrameHashRows(Handle, seed));
    /// <summary>
    /// Return the number of unique rows (based on all columns).
    /// </summary>
    public long NUnique()
    {
        using var df = Unique();
        return df.Height;
    }

    /// <summary>
    /// Return the number of unique rows based on a subset of columns (Selector, Type, string, etc.).
    /// </summary>
    public long NUnique(IntoSelector subset)
    {
        using var df = Unique(subset);
        return df.Height;
    }

    /// <summary>
    /// Return the number of unique rows based on specific column names (supports ["A", "B"] syntax).
    /// </summary>
    public long NUnique(IEnumerable<string> columns)
    {
        using var df = Unique(columns);
        return df.Height;
    }
    /// <summary>
    /// Get the number of chunks used by the first Column of this DataFrame.
    /// (Equivalent to strategy='first')
    /// </summary>
    public long NChunks()
    {
        if (Width == 0) return 0;
        return Column(0).NChunks; 
    }
    /// <summary>
    /// Get an array containing the number of chunks for all columns in this DataFrame.
    /// (Equivalent to strategy='all')
    /// </summary>
    public long[] NChunksAll()
        => [.. this.Select(s => s.NChunks)];
    /// <summary>
    /// Rechunk the data in this DataFrame to a contiguous allocation.
    /// This will make sure all subsequent operations have optimal and predictable performance.
    /// </summary>
    public DataFrame Rechunk() => new(PolarsWrapper.DataFrameRechunk(Handle));
    /// <summary>
    /// Align chunks
    /// </summary>
    /// <returns></returns>
    public DataFrame AlignChunks() => new(PolarsWrapper.DataFrameAlignChunks(Handle));
    /// <summary>
    /// Shrink DataFrame memory usage.This won't return a new DataFrame
    /// </summary>
    public void ShrinkToFitInplace() => PolarsWrapper.DataFrameShrinkToFit(Handle);
    /// <summary>
    /// Shrink DataFrame memory usage.
    /// </summary>
    /// <returns>A new DataFrame</returns>
    public DataFrame ShrinkToFit() 
    {
        var newDf = Clone();
        PolarsWrapper.DataFrameShrinkToFit(newDf.Handle);
        return newDf;
    }
    // ==========================================
    // DataFrame Operations
    // ==========================================

    /// <summary>
    /// Return head lines from a DataFrame
    /// </summary>
    /// <param name="n"></param>
    /// <returns></returns>
    public DataFrame Head(int n = 5) => new(PolarsWrapper.Head(Handle, (uint)n));
    /// <summary>
    /// Return tail lines from a DataFrame
    /// </summary>
    /// <param name="n"></param>
    /// <returns></returns>
    public DataFrame Tail(int n = 5) => new(PolarsWrapper.Tail(Handle, (uint)n));
    /// <summary>
    /// Rename a column.
    /// </summary>
    public DataFrame Rename(string oldName, string newName) => new(PolarsWrapper.Rename(Handle, oldName, newName));
    /// <summary>
    /// Rename a list of columns.
    /// </summary>
    public DataFrame Rename(string[] oldNames, string[] newNames) => new(PolarsWrapper.Rename(Handle, oldNames, newNames));
    /// <summary>
    /// Rename columns using a dictionary mapping old names to new names.
    /// </summary>
    public DataFrame Rename(IReadOnlyDictionary<string, string> mapping)
    {
        var oldNames = mapping.Keys.ToArray();
        var newNames = mapping.Values.ToArray();
        return Rename(oldNames, newNames);
    }
    /// <summary>
    /// Rename columns using a list of (OldName, NewName) tuples.
    /// </summary>
    /// <example>
    /// df.Rename(("old1", "new1"), ("old2", "new2"));
    /// </example>
    public DataFrame Rename(params (string OldName, string NewName)[] renames)
    {
        if (renames == null || renames.Length == 0) return this;

        var oldNames = new string[renames.Length];
        var newNames = new string[renames.Length];

        for (int i = 0; i < renames.Length; i++)
        {
            oldNames[i] = renames[i].OldName;
            newNames[i] = renames[i].NewName;
        }

        return Rename(oldNames, newNames);
    }

    /// <summary>
    /// Slice the DataFrame along the rows.
    /// </summary>
    /// <param name="offset">Start index. Negative values work as expected (counting from the end).</param>
    /// <param name="length">Length of the slice.</param>
    /// <returns>A new sliced DataFrame.</returns>
    public DataFrame Slice(long offset, ulong? length=null)
    {
        long absoluteOffset;
        
        if (offset < 0)
        {
            absoluteOffset = Height + offset;
        }
        else
        {
            absoluteOffset = offset;
        }

        if (absoluteOffset < 0 || absoluteOffset >= Height)
        {
            return new(PolarsWrapper.Slice(Handle, absoluteOffset, 0UL));
        }

        ulong realLength = length ?? (ulong)(Height - absoluteOffset);

        return new(PolarsWrapper.Slice(Handle, absoluteOffset, realLength));
    }
    /// <summary>
    /// e.g. df.Slice(1..5) 或 df.Slice(..^10)
    /// </summary>
    public DataFrame Slice(Range range)
    {
        long height = Height;
        
        long start = range.Start.IsFromEnd 
            ? height - range.Start.Value 
            : range.Start.Value;
            
        long end = range.End.IsFromEnd 
            ? height - range.End.Value 
            : range.End.Value;

        start = Math.Max(0, Math.Min(start, height));
        end = Math.Max(0, Math.Min(end, height));
        
        long length = end - start;
        
        if (length <= 0)
        {
            return Slice(0, 0); 
        }

        return Slice(start, (ulong)length);
    }
    /// <summary>
    /// Returns an iterator over slices of this DataFrame.
    /// </summary>
    /// <param name="nRows">The number of rows per slice. Default is 10,000.</param>
    /// <returns>An enumerable collection of DataFrame slices.</returns>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when nRows is less than or equal to zero.</exception>
    public IEnumerable<DataFrame> IterSlices(int nRows = 10_000)
    {
        if (nRows <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(nRows), "Number of rows per slice must be greater than zero.");
        }

        long totalRows = Height;
        for (long offset = 0; offset < totalRows; offset += nRows)
        {
            // Calculate the exact length for the current slice to prevent out-of-bounds on the last chunk
            ulong currentLength = (ulong)Math.Min(nRows, totalRows - offset);
            
            yield return Slice(offset, currentLength);
        }
    }

    /// <summary>
    /// Take rows by physical integer indices.
    /// Note: Negative indices are not supported. All values must be >= 0.
    /// </summary>
    /// <param name="indices">A Series containing integer indices (e.g., UInt32, Int32).</param>
    /// <exception cref="ArgumentException">Thrown when the series is not an integer type.</exception>
    public DataFrame Take(Series indices)
    {
        if (!indices.DataType.IsInteger)
        {
            throw new ArgumentException($"Take requires an integer Series, but got {indices.DataType.Kind}.", nameof(indices));
        }

        try
        {
            return new DataFrame(PolarsWrapper.DataFrameTake(Handle, indices.Handle));
        }
        catch (Exception ex) when (ex.Message.Contains("OutOfBounds") || ex.Message.Contains("out of bounds"))
        {
            throw new ArgumentOutOfRangeException(
                nameof(indices), 
                "Index out of bounds. This may be caused by index values exceeding the DataFrame's height, or by using negative indices which are not supported in Take.");
        }
    }

    // ==========================================
    // Sampling
    // ==========================================

    /// <summary>
    /// Sample from this DataFrame.
    /// </summary>
    /// <param name="n">Number of items to return. Defaults to 1</param>
    /// <param name="withReplacement">Allow values to be sampled more than once.</param>
    /// <param name="shuffle">If set to True, the order of the sampled rows will be shuffled. If set to False (default), the order of the returned rows will be neither stable nor fully random.</param>
    /// <param name="seed">Seed for the random number generator. If set to None (default), a random seed is generated for each time the sample is called.</param>
    public DataFrame Sample(ulong n=1, bool withReplacement = false, bool shuffle = false, ulong? seed = null)
        => new(PolarsWrapper.SampleNLiteral(Handle, n, withReplacement, shuffle, seed));

    /// <summary>
    /// Sample from this DataFrame.
    /// </summary>
    /// <param name="fraction">Fraction of items to return.</param>
    /// <param name="withReplacement">Allow values to be sampled more than once.</param>
    /// <param name="shuffle">If set to True, the order of the sampled rows will be shuffled. If set to False (default), the order of the returned rows will be neither stable nor fully random.</param>
    /// <param name="seed">Seed for the random number generator. If set to None (default), a random seed is generated for each time the sample is called.</param>
    /// <returns></returns>
    public DataFrame Sample(double fraction,bool withReplacement = false, bool shuffle = false, ulong? seed = null)
    {
        using Series frac = Pl.Series("",[fraction]);
        return new(PolarsWrapper.SampleFrac(Handle,frac.Handle , withReplacement, shuffle, seed));
    }
    /// <summary>
    /// Create an empty (n=0) or n-row null-filled (n>0) copy of the DataFrame.
    /// Returns a n-row null-filled DataFrame with an identical schema.
    /// </summary>
    /// <param name="n">Number of (null-filled) rows to return in the cleared frame.</param>
    /// <returns>A new DataFrame.</returns>
    public DataFrame Clear(long n = 0)
    {
        using var schema = this.Schema; 

        return schema.ToDataFrame(n);
    }
    /// <inheritdoc cref="LazyFrame.GatherEvery(int, int)"/>
    public DataFrame GatherEvery(int n, int offset = 0)
        => Select(Pl.All().GatherEvery((ulong)n, (ulong)offset));
    /// <inheritdoc cref="LazyFrame.Interpolate"/>
    public DataFrame Interpolate()
        => Select(Pl.All().Interpolate(InterpolationMethod.Linear));
    /// <summary>
    /// Insert a column into the DataFrame at a specified index.
    /// Accepts a string (column name), a Series, a primitive value, or an Expr.
    /// </summary>
    public DataFrame InsertColumn(int index, IntoExprColumn column)
    {
        int originalIndex = index;

        if (index < 0)
        {
            index = (int)(Width + index);
            if (index < 0)
                throw new ArgumentOutOfRangeException(nameof(index), 
                    $"Column index {originalIndex} is out of range (frame has {Width} columns)");
        }
        else if (index > Width)
        {
            throw new ArgumentOutOfRangeException(nameof(index), 
                $"Column index {originalIndex} is out of range (frame has {Width} columns)");
        }

        var exprToInsert = column.Consume();

        var cols = Columns.Select(Pl.Col).ToList();

        cols.Insert(index, exprToInsert);

        return Select(cols.ToArray());
    }
    /// <summary>
    /// Replace a column by its index in-place.
    /// Supports negative indexing (e.g., -1 replaces the last column).
    /// </summary>
    /// <param name="index">The index of the column to replace. Negative values count from the end.</param>
    /// <param name="newColumn">The new Series to insert.</param>
    /// <param name="keepName">If true, keeps the original column name. If false, uses the new Series's name. Default is false.</param>
    /// <returns>The current DataFrame instance to support method chaining.</returns>
    public DataFrame ReplaceColumn(int index, Series newColumn, bool keepName = false)
    {
        ArgumentNullException.ThrowIfNull(newColumn);

        long width = Width; 
        
        if (index < 0)
        {
            index = (int)width + index;
        }

        if (index < 0 || index >= width)
        {
            throw new ArgumentOutOfRangeException(nameof(index), $"Column index {index} is out of bounds. DataFrame width is {width}.");
        }

        if (keepName)
        {
            string originalName = Columns[index];
            PolarsWrapper.Replace(Handle, originalName, newColumn.Handle);
        }
        else
        {
            PolarsWrapper.ReplaceColumnAt(Handle, index, newColumn.Handle);
        }
        
        return this;
    }

    /// <summary>
    /// Replace a column by its name in-place.
    /// </summary>
    /// <param name="columnName">The name of the column to replace.</param>
    /// <param name="newColumn">The new Series to insert.</param>
    /// <param name="keepName">If true, keeps the original column name. If false, uses the new Series's name. Default is true.</param>
    /// <returns>The current DataFrame instance to support method chaining.</returns>
    public DataFrame ReplaceColumn(string columnName, Series newColumn, bool keepName = true)
    {
        ArgumentNullException.ThrowIfNull(newColumn);

        if (keepName)
        {
            PolarsWrapper.Replace(Handle, columnName, newColumn.Handle);
        }
        else
        {
            int index = Array.IndexOf(Columns, columnName);
            if (index == -1)
            {
                throw new ArgumentException($"Column '{columnName}' does not exist in the DataFrame.");
            }
            PolarsWrapper.ReplaceColumnAt(Handle, index, newColumn.Handle);
        }

        return this;
    }
    // ==========================================
    // Stack Ops
    // ==========================================

    /// <summary>
    /// Horizontally stack columns to the DataFrame.
    /// Returns a new DataFrame with the new columns appended.
    /// </summary>
    /// <param name="columns">The series to stack.</param>
    public DataFrame HStack(IEnumerable<Series> columns)
    {
        var colsArray = columns as Series[] ?? columns.ToArray();
        var handles = colsArray.Select(s => s.Handle).ToArray();
        
        return new DataFrame(PolarsWrapper.HStack(Handle, handles));
    }

    /// <summary>
    /// Horizontally stack columns to the DataFrame.
    /// </summary>
    public DataFrame HStack(params Series[] columns) => HStack((IEnumerable<Series>)columns);

    /// <summary>
    /// Vertically stack another DataFrame to this one.
    /// Checks that the schema matches.
    /// </summary>
    /// <param name="other">The DataFrame to stack vertically.</param>
    public DataFrame VStack(DataFrame other)
        => new(PolarsWrapper.VStack(Handle, other.Handle));
    /// <summary>
    /// Extend the memory backed by this DataFrame with the values from other.
    /// Different from vstack which adds the chunks from other to the chunks of this DataFrame, extend appends the data from other to the underlying memory locations and thus may cause a reallocation.
    /// If this does not cause a reallocation, the resulting data structure will not have any extra chunks and thus will yield faster queries.
    /// </summary>
    /// <param name="other">The DataFrame to extend.</param>
    public DataFrame Extend(DataFrame other)
    {
        PolarsWrapper.DataFrameExtend(Handle,other.Handle);
        return this;
    }
    /// <summary>
    /// Upsample a DataFrame at a regular frequency.
    /// </summary>
    /// <param name="timeColumn">The column used for the time/datetime index.</param>
    /// <param name="every">The interval to upsample to (e.g., "1d", "1h", or a TimeSpan).</param>
    /// <param name="groupBy">Optional. Group by these columns before upsampling. Accepts strings, Selectors, or Exprs.</param>
    /// <param name="maintainOrder">If true, maintains the original order of the groups.</param>
    /// <returns>A new DataFrame with missing time steps filled with nulls.</returns>
    public DataFrame Upsample(
        IntoSelector timeColumn, 
        IntoDuration every, 
        IntoSelector? groupBy = null, 
        bool maintainOrder = false)
    {
        using var timeSelector = timeColumn.Consume();
        string[] expandedTimeCols = Cs.ExpandSelector(this, timeSelector);

        if (expandedTimeCols.Length != 1)
        {
            throw new ArgumentException(
                $"The timeColumn selector must resolve to exactly one column, but it resolved to {expandedTimeCols.Length} columns: " +
                (expandedTimeCols.Length > 0 ? string.Join(", ", expandedTimeCols) : "None")
            );
        }
        string resolvedTimeColumn = expandedTimeCols[0];

        string[]? groupByCols = null;
        if (groupBy.HasValue)
        {
            using var groupSelector = groupBy.Value.Consume();
            groupByCols = Cs.ExpandSelector(this, groupSelector);
            
            if (groupByCols.Length == 0)
            {
                groupByCols = null; 
            }
        }

        var newHandle = PolarsWrapper.DataFrameUpsample(
            Handle, 
            resolvedTimeColumn, 
            every.Value, 
            groupByCols, 
            maintainOrder
        );

        return new DataFrame(newHandle);
    }
    /// <summary>
    /// Convert categorical/string variables into dummy/indicator variables (One-Hot Encoding).
    /// </summary>
    /// <param name="columns">Optional. The columns to encode. Accepts strings, string arrays, or Selectors (e.g. Cs.String()). If null, all string/categorical columns are encoded.</param>
    /// <param name="separator">The separator used in the generated column names.</param>
    /// <param name="dropFirst">Whether to drop the first dummy variable to avoid collinearity (k-1 dummies).</param>
    /// <param name="dropNulls">Whether to ignore null values when creating dummies.</param>
    /// <returns>A new DataFrame with one-hot encoded columns.</returns>
    public DataFrame ToDummies(
        IntoSelector? columns = null, 
        string separator = "_", 
        bool dropFirst = false, 
        bool dropNulls = false)
    {
        IntoSelector actualSelector = columns ?? (Cs.String() | Cs.ByDtype(DataType.Categorical()) | Cs.Enum());

        using var selector = actualSelector.Consume();
        string[] columnsArray = Cs.ExpandSelector(this, selector);

        if (columnsArray.Length == 0)
        {
            if (columns.HasValue)
            {
                throw new ArgumentException("The provided column selector did not match any columns in the DataFrame.");
            }
            else
            {
                return this.Clone();
            }
        }

        var newHandle = PolarsWrapper.DataFrameToDummies(
            Handle, 
            columnsArray, 
            separator, 
            dropFirst, 
            dropNulls
        );

        return new DataFrame(newHandle);
    }
    // ==========================================
    // LifeCycle
    // ==========================================
    /// <summary>
    /// Clone the DataFrame
    /// </summary>
    /// <returns></returns>
    public DataFrame Clone()
        => new(PolarsWrapper.CloneDataFrame(Handle));
    /// <summary>
    /// Dispose the DataFrame and release resources.
    /// </summary>
    private readonly List<IDisposable> _backingResources = [];

    internal void HoldResource(IDisposable resource)
    {
        if (resource != null) _backingResources.Add(resource);
    }
    /// <summary>
    /// Dispose the DataFrame and release resources.
    /// </summary>
    public void Dispose()
    {
        if (!Handle.IsInvalid) Handle.Dispose();

        foreach (var res in _backingResources)
        {
            res.Dispose();
        }
        _backingResources.Clear();
        GC.SuppressFinalize(this); 
    }
    
    // ==========================================
    // Object Mapping (To Records)
    // ==========================================

    /// <summary>
    /// Convert DataFrame to a list of strongly-typed objects.
    /// This triggers a conversion to Arrow format internally.
    /// </summary>
    public IEnumerable<T> Rows<T>() where T : new()
    {
        using var batch = ToArrow(); 

        foreach (var item in ArrowReader.ReadRecordBatch<T>(batch))
        {
            yield return item;
        }
    }
 
    /// <summary>
    /// Get data for selected row.
    /// </summary>
    public object?[] Row(int index)
    {
        if (index < 0 || index >= Height)
            throw new IndexOutOfRangeException($"Row index {index} is out of bounds. Height: {Height}");

        var rowData = new object?[Width];
        for (int i = 0; i < Width; i++)
        {
            rowData[i] = this[index, i];
        }
        return rowData;
    }
    // ==========================================
    // Equality (IEquatable)
    // ==========================================

    /// <summary>
    /// Check if this DataFrame is strictly equal to another DataFrame.
    /// By default, missing (null) values are considered equal to other missing values.
    /// </summary>
    public bool Equals(DataFrame? other)
    {
        return Equals(other, nullEqual: true);
    }

    /// <summary>
    /// Check if this DataFrame is strictly equal to another DataFrame.
    /// </summary>
    /// <param name="other">The other DataFrame to compare with.</param>
    /// <param name="nullEqual">If true, null values are considered equal to other null values.</param>
    public bool Equals(DataFrame? other, bool nullEqual=true)
    {
        if (other is null) return false;
        if (ReferenceEquals(this, other)) return true;

        if (Handle.IsInvalid || other.Handle.IsInvalid) return false;

        return PolarsWrapper.DataFrameEquals(Handle, other.Handle, nullEqual);
    }

    /// <summary>
    /// Object.Equals override.
    /// </summary>
    public override bool Equals(object? obj)
    {
        return Equals(obj as DataFrame);
    }

    /// <summary>
    /// GetHashCode override.
    /// </summary>
    public override int GetHashCode() => throw new NotSupportedException("DataFrames are large data structures and cannot be hashed directly. Do not use them as keys in collections.");

    public static bool operator ==(DataFrame? left, DataFrame? right)
    {
        if (left is null) return right is null;
        return left.Equals(right);
    }

    public static bool operator !=(DataFrame? left, DataFrame? right) => !(left == right);

}