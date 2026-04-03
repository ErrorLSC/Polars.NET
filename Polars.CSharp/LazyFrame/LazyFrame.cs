#pragma warning disable CS1573

using Polars.NET.Core;
using Cs = Polars.CSharp.Polars.Selectors;
using Pl = Polars.CSharp.Polars;

namespace Polars.CSharp;

/// <summary>
/// Represents a lazily evaluated DataFrame.
/// Until the query is executed, operations are just recorded in a query plan.
/// Once executed, the data is materialized in memory.
/// </summary>
public partial class LazyFrame : IDisposable,IPolarsLazyFrame
{
    internal LazyFrameHandle Handle { get; }

    internal LazyFrame(LazyFrameHandle handle)
    {
        Handle = handle;
    }

    // ==========================================
    // Meta / Inspection
    // ==========================================

    /// <summary>
    /// Gets the Schema of the LazyFrame.
    /// Note: The returned PolarsSchema object is IDisposable. 
    /// Usage in 'using' block is recommended if accessed frequently.
    /// </summary>
    public PolarsSchema Schema
    {
        get
        {       
            return new PolarsSchema(PolarsWrapper.GetLazySchema(Handle));
        }
    }
    IPolarsSchema IPolarsLazyFrame.Schema => this.Schema;
    /// <summary>
    /// Prints the schema to the console.
    /// </summary>
    public void PrintSchema()
    {
        using var schema = Schema;
        
        Console.WriteLine("root");
        
        foreach (var name in schema.ColumnNames)
        {
            var type = schema[name]; 
            Console.WriteLine($" |-- {name}: {type.Kind}");
        }
    }
    /// <summary>
    /// Return LazyFrame Columns' Name
    /// </summary>
    public string[] Columns => [.. Schema.ColumnNames];

    /// <summary>
    /// Return LazyFrame Columns' Name
    /// </summary>
    public string[] ColumnNames => Columns;

    /// <summary>
    /// Return LazyFrame Columns' DataType
    /// </summary>
    public List<DataType> DataTypes => Schema.DataTypes;

    /// <summary>
    /// Clone the LazyFrame, creating a new independent copy.
    /// </summary>
    /// <returns></returns>
    public LazyFrame Clone()
        => new(PolarsWrapper.LazyClone(Handle));
    internal LazyFrameHandle CloneHandle()
        => PolarsWrapper.LazyClone(Handle);
    
    /// <summary>
    /// Renames columns in the <see cref="LazyFrame"/>.
    /// </summary>
    /// <param name="existing">An array of existing column names to be renamed.</param>
    /// <param name="newNames">An array of new column names, corresponding by index to the names in <paramref name="existing"/>.</param>
    /// <param name="strict">
    /// If <c>true</c>, an error is raised if any column in <paramref name="existing"/> is not found in the schema. 
    /// If <c>false</c>, columns that are not found are silently ignored. Default is <c>true</c>.
    /// </param>
    /// <returns>A new <see cref="LazyFrame"/> with the rename operation applied.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="existing"/> or <paramref name="newNames"/> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when the length of <paramref name="existing"/> does not match the length of <paramref name="newNames"/>.</exception>
    public LazyFrame Rename(string[] existing, string[] newNames, bool strict = true)
    {
        ArgumentNullException.ThrowIfNull(existing);
        ArgumentNullException.ThrowIfNull(newNames);

        var newHandle = PolarsWrapper.LazyRename(Handle, existing, newNames, strict);
        
        return new LazyFrame(newHandle);
    }

    /// <summary>
    /// Slice the LazyFrame.
    /// <para>This operation is lazy; it only affects the query plan.</para>
    /// </summary>
    /// <param name="offset">Start index. Negative values count from the end.</param>
    /// <param name="length">Number of rows to return.</param>
    public LazyFrame Slice(long offset, uint length)
        => new(PolarsWrapper.LazySlice(CloneHandle(), offset, length));
    /// <summary>
    /// Slice the LazyFrame (Convenience overload).
    /// </summary>
    public LazyFrame Slice(long offset, int length)
    {
        if (length < 0) throw new ArgumentOutOfRangeException(nameof(length), "Length must be non-negative.");
        return Slice(offset, (uint)length);
    }
    /// <summary>
    /// Create an empty (n=0) or n-row null-filled (n>0) copy of the LazyFrame.
    /// Returns a n-row null-filled LazyFrame with an identical schema.
    /// </summary>
    /// <param name="n">Number of (null-filled) rows to return in the cleared frame.</param>
    /// <returns>A new LazyFrame.</returns>
    public LazyFrame Clear(long n = 0)
    {
        using var schema = Schema; 

        return schema.ToLazyFrame(n);
    }
    /// <summary>
    /// Get the first n rows.
    /// </summary>
    /// <param name="n">Number of rows to return.</param>
    /// <returns></returns>
    public LazyFrame Limit(int n=5)
        => new(PolarsWrapper.LazyLimit(CloneHandle(), (uint)n));
    /// <inheritdoc cref="Limit"/>
    public LazyFrame Head(int n=5) => Limit(n);
    /// <summary>
    /// Gather every n-th row.
    /// </summary>
    /// <param name="n">Gather every n-th row.</param>
    /// <param name="offset">Starting Index</param>
    /// <returns></returns>
    public LazyFrame GatherEvery(int n, int offset = 0)
        => Select(Pl.All().GatherEvery((ulong)n, (ulong)offset));

    // ==========================================
    // Execution (Collect)
    // ==========================================

    /// <summary>
    /// Execute the query plan and return a DataFrame.
    /// </summary>
    public DataFrame Collect(bool useStreaming=false)
        => new(PolarsWrapper.LazyCollect(Handle,useStreaming));

    IPolarsDataFrame IPolarsLazyFrame.Collect(bool useStreaming)
        => Collect(useStreaming);

    /// <summary>
    /// Execute the query plan using the streaming engine.
    /// </summary>
    public DataFrame CollectStreaming()
        => new(PolarsWrapper.CollectStreaming(Handle));
    /// <summary>
    /// Execute the query plan asynchronously and return a DataFrame.
    /// </summary>
    public async Task<DataFrame> CollectAsync(bool useStreaming = false, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var dfHandle = await PolarsWrapper.LazyCollectAsync(Handle, useStreaming, cancellationToken)
                                          .ConfigureAwait(false);

        return new DataFrame(dfHandle);
    }

    async Task<IPolarsDataFrame> IPolarsLazyFrame.CollectAsync(bool useStreaming, CancellationToken cancellationToken)
        => await CollectAsync(useStreaming, cancellationToken).ConfigureAwait(false);

    /// <summary>
    /// Dispose the LazyFrame and release native resources.
    /// </summary>
    public void Dispose()
    {
        Handle?.Dispose();
        GC.SuppressFinalize(this); 
    }
}