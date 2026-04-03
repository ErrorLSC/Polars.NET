using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using System.Data;
using Pl = Polars.CSharp.Polars;
using Cs = Polars.CSharp.Polars.Selectors;

namespace Polars.CSharp;

/// <summary>
/// DataFrame represents a 2-dimensional labeled data structure similar to a table or spreadsheet.
/// </summary>
public partial class DataFrame : IDisposable,IEnumerable<Series>,IPolarsDataFrame
{
    internal DataFrameHandle Handle { get; }

    internal DataFrame(DataFrameHandle handle)
    {
        Handle = handle;
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
    public List<DataType> DataTypes => Schema.DataTypes;
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
    /// 
    /// </summary>
    /// <returns></returns>
    public DataFrame AlignChunks() => new(PolarsWrapper.DataFrameAlignChunks(Handle));
    
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
    public DataFrame Slice(long offset, ulong length) => new(PolarsWrapper.Slice(Handle, offset, length));
    /// <summary>
    /// Slice the DataFrame along the rows (Convenience overload for int).
    /// </summary>
    public DataFrame Slice(int offset, int length)
    {
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length), "Length must be non-negative.");
        return Slice(offset, (ulong)length);
    }

    // ==========================================
    // Sampling
    // ==========================================

    /// <summary>
    /// Sample n rows from the DataFrame.
    /// </summary>
    public DataFrame Sample(ulong n, bool withReplacement = false, bool shuffle = true, ulong? seed = null)
        => new(PolarsWrapper.SampleN(Handle, n, withReplacement, shuffle, seed));

    /// <summary>
    /// Sample a fraction of rows from the DataFrame.
    /// </summary>
    public DataFrame Sample(double fraction, bool withReplacement = false, bool shuffle = true, ulong? seed = null)
        => new(PolarsWrapper.SampleFrac(Handle, fraction, withReplacement, shuffle, seed));
 
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
    /// Get data foir selected row.
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
}