using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Apache.Arrow;
using System.Data;
using Pl = Polars.CSharp.Polars;

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
    public Series HashRows(ulong? seed = 42)
    {
        var h = PolarsWrapper.DataFrameHashRows(Handle, seed);
        return new Series(h);
    }

    /// <summary>
    /// Return the number of unique rows, or the number of unique row-subsets.
    /// </summary>
    /// <returns></returns>
    public long NUnique(string[]? subset = null)
    {
        using var df = Unique(subset);
        return df.Height;
    }

    /// <inheritdoc cref="DataFrame.NUnique(string[])"/>
    public long NUnique(params Expr[] subset)
    {
        using var df = Unique(subset);
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
    
    // ==========================================
    // DataFrame Operations
    // ==========================================
    /// <summary>
    /// Select columns from the DataFrame and apply expressions to them.
    /// <para>
    /// This is the primary way to project data, rename columns, or compute new columns based on existing ones.
    /// The result will only contain the columns specified in the expression list.
    /// </para>
    /// </summary>
    /// <param name="exprs">A list of expressions defining the columns to select or compute.</param>
    /// <returns>A new DataFrame containing only the selected/computed columns.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     foo = new[] { 1, 2, 3 },
    ///     bar = new[] { 6, 7, 8 },
    ///     ham = new[] { "a", "b", "c" }
    /// });
    /// 
    /// // Select "foo" column and compute a new column "bar_x_2"
    /// var selected = df.Select(
    ///     Col("foo"),
    ///     (Col("bar") * 2).Alias("bar_x_2")
    /// );
    /// 
    /// selected.Show();
    /// /* Output:
    /// shape: (3, 2)
    /// ┌─────┬─────────┐
    /// │ foo ┆ bar_x_2 │
    /// │ --- ┆ ---     │
    /// │ i32 ┆ i32     │
    /// ╞═════╪═════════╡
    /// │ 1   ┆ 12      │
    /// │ 2   ┆ 14      │
    /// │ 3   ┆ 16      │
    /// └─────┴─────────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Select(params Expr[] exprs)
      => Lazy().Select(exprs).Collect();
    
    /// <summary>
    /// Select columns by name (convenience overload).
    /// <para>
    /// This is a shortcut for creating <see cref="Polars.Col(string)"/> expressions for each column name.
    /// </para>
    /// </summary>
    /// <param name="columns">The names of the columns to select.</param>
    /// <returns>A new DataFrame containing only the selected columns.</returns>
    /// <remarks>
    /// For more advanced selections (renaming, calculations), use <see cref="Select(Expr[])"/>.
    /// </remarks>
    /// <seealso cref="Select(Expr[])"/>
    public DataFrame Select(params string[] columns)
        => Select(columns.Select(Pl.Col).ToArray());
    
    /// <summary>
    /// Filter rows based on a boolean expression (predicate).
    /// <para>
    /// Retains only the rows where the expression evaluates to true.
    /// </para>
    /// </summary>
    /// <param name="expr">A boolean expression to filter by (e.g., Col("a") > 5).</param>
    /// <returns>A new DataFrame containing only the matching rows.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     foo = new[] { 1, 2, 3 },
    ///     bar = new[] { 6, 7, 8 },
    ///     ham = new[] { "a", "b", "c" }
    /// });
    /// 
    /// // Keep rows where "foo" is greater than 1
    /// var filtered = df.Filter(Col("foo") > 1);
    /// 
    /// filtered.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬─────┬─────┐
    /// │ foo ┆ bar ┆ ham │
    /// │ --- ┆ --- ┆ --- │
    /// │ i32 ┆ i32 ┆ str │
    /// ╞═════╪═════╪═════╡
    /// │ 2   ┆ 7   ┆ b   │
    /// │ 3   ┆ 8   ┆ c   │
    /// └─────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Filter(Expr expr)
        => Lazy().Filter(expr).Collect();
    
    /// <summary>
    ///  Filter rows based on a boolean series.
    /// </summary>
    /// <param name="series"></param>
    /// <returns></returns>
    public DataFrame Filter(Series series)
        => Lazy().Filter(series).Collect();
    
    /// <summary>
    /// Add new columns to the DataFrame or replace existing ones using expressions.
    /// <para>
    /// Unlike <see cref="Select(Expr[])"/>, this method keeps all original columns in the DataFrame 
    /// and appends the new ones (or replaces them if the names match).
    /// </para>
    /// </summary>
    /// <param name="exprs">Expressions defining the new columns to add.</param>
    /// <returns>A new DataFrame with the original columns plus the new/modified columns.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     foo = new[] { 1, 2, 3 },
    ///     bar = new[] { 6, 7, 8 },
    ///     ham = new[] { "a", "b", "c" }
    /// });
    /// 
    /// // Add a "sum" column (foo + bar) while keeping others
    /// var withCols = df.WithColumns(
    ///     (Col("foo") + Col("bar")).Alias("sum")
    /// );
    /// 
    /// withCols.Show();
    /// /* Output:
    /// shape: (3, 4)
    /// ┌─────┬─────┬─────┬─────┐
    /// │ foo ┆ bar ┆ ham ┆ sum │
    /// │ --- ┆ --- ┆ --- ┆ --- │
    /// │ i32 ┆ i32 ┆ str ┆ i32 │
    /// ╞═════╪═════╪═════╪═════╡
    /// │ 1   ┆ 6   ┆ a   ┆ 7   │
    /// │ 2   ┆ 7   ┆ b   ┆ 9   │
    /// │ 3   ┆ 8   ┆ c   ┆ 11  │
    /// └─────┴─────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame WithColumns(params Expr[] exprs)
        => Lazy().WithColumns(exprs).Collect();
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
    /// Returns a new DataFrame with unique rows.
    /// </summary>
    /// <param name="subset">An optional array of column names to consider for identifying duplicate rows. If null, all columns are used.</param>
    /// <param name="keep">The strategy for which duplicate rows to retain (First, Last, or None).</param>
    /// <param name="maintainOrder">Keep the same order as the original DataFrame. This is more expensive to compute. Settings this to True blocks the possibility to run on the streaming engine.</param>
    /// <param name="offset">The starting index from which to begin the slice of unique results. If null, no offset is applied.</param>
    /// <param name="len">The maximum number of rows to include in the result. If null, all unique rows from the offset are returned.</param>
    /// <returns>A new <see cref="DataFrame"/> containing only unique rows based on the specified criteria.</returns>
    public DataFrame Unique(
        string[]? subset = null, 
        UniqueKeepStrategy keep = UniqueKeepStrategy.First, 
        bool maintainOrder = false,
        long? offset = null, 
        long? len = null)
    {
        (long offset, ulong len)? slice = null;
        if (offset.HasValue && len.HasValue)
        {
            slice = (offset.Value, (ulong)Math.Max(0, len.Value));
        }

        var h = PolarsWrapper.DataFrameUnique(
            Handle, 
            subset, 
            keep.ToNative(), 
            maintainOrder,
            slice
        );

        return new DataFrame(h);
    }
    /// <inheritdoc cref="DataFrame.Unique(string[], UniqueKeepStrategy, bool, long?, long?)"/>
    public DataFrame Unique(
        IEnumerable<Expr> subset, 
        UniqueKeepStrategy keep = UniqueKeepStrategy.First, 
        bool maintainOrder = false,
        long? offset = null, 
        long? len = null)
    {
        var resolvedColumnNames = new List<string>();

        foreach (var expr in subset)
        {
            var name = expr.Meta.OutputName();
            if (!string.IsNullOrEmpty(name))
            {
                resolvedColumnNames.Add(name);
            }
            else
            {
                try 
                {
                    var expandedNames = this.Head(0).Select(expr).Columns; 
                    resolvedColumnNames.AddRange(expandedNames);
                }
                catch (Exception ex)
                {
                    throw new ArgumentException(
                        $"Cannot parse this expression to column names: {ex.Message}", ex);
                }
            }
        }

        var finalSubset = resolvedColumnNames.Distinct().ToArray();

        if (finalSubset.Length == 0)
        {
            throw new ArgumentException("No Columns Selected");
        }

        return Unique(finalSubset, keep, maintainOrder, offset, len);
    }
    /// <summary>
    /// Slice the DataFrame along the rows.
    /// </summary>
    /// <param name="offset">Start index. Negative values work as expected (counting from the end).</param>
    /// <param name="length">Length of the slice.</param>
    /// <returns>A new sliced DataFrame.</returns>
    public DataFrame Slice(long offset, ulong length)
        => new(PolarsWrapper.Slice(Handle, offset, length));
    /// <summary>
    /// Slice the DataFrame along the rows (Convenience overload for int).
    /// </summary>
    public DataFrame Slice(int offset, int length)
    {
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length), "Length must be non-negative.");
        return Slice((long)offset, (ulong)length);
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
    /// Export DataFrame to Record Batch
    /// </summary>
    /// <param name="onBatchReceived">Receive RecordBatch Callback</param>
    public void ExportBatches(Action<RecordBatch> onBatchReceived)
        => PolarsWrapper.ExportBatches(Handle, onBatchReceived);

 
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