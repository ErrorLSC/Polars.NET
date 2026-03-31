#pragma warning disable CS1573

using System.Data;
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
    // ==========================================
    // Transformations
    // ==========================================
    /// <summary>
    /// Select specific columns or expressions.
    /// </summary>
    /// <example>
    /// <code>
    /// // Select "a" and calculate "b" * 2
    /// lf.Select(Col("a"), (Col("b") * 2).Alias("b_double"));
    /// </code>
    /// </example>
    public LazyFrame Select(params Expr[] exprs)
    {
        var lfClone = CloneHandle();
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new LazyFrame(PolarsWrapper.LazySelect(lfClone, handles));
    }
    /// <summary>
    /// Select columns by name.
    /// <para>Syntactic sugar for <c>Select(Expr.Col(name))</c>.</para>
    /// </summary>
    public LazyFrame Select(params string[] columns)
    {
        var exprs = columns.Select(Pl.Col).ToArray();
        return Select(exprs);
    }
    /// <summary>
    /// Filter rows based on a boolean expression.
    /// <para>
    /// In a LazyFrame, this operation is added to the logical plan and is optimized before execution.
    /// Polars will attempt to push this filter down as close to the data source as possible (Predicate Pushdown).
    /// </para>
    /// </summary>
    /// <param name="expr">A boolean expression.</param>
    /// <returns>A new LazyFrame with the filter applied.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B", "C" },
    ///     val = new[] { 1, 2, 3, 4, 5 }
    /// });
    /// 
    /// // Build a lazy query:
    /// // 1. Filter out group 'C'
    /// // 2. Multiply 'val' by 2
    /// // 3. Select specific columns
    /// var q = df.Lazy()
    ///     .Filter(Col("group") != "C")
    ///     .WithColumns((Col("val") * 2).Alias("val_x_2"))
    ///     .Select("group", "val_x_2");
    /// 
    /// // Execute
    /// q.Collect().Show();
    /// /* Output:
    /// shape: (4, 2)
    /// ┌───────┬─────────┐
    /// │ group ┆ val_x_2 │
    /// │ ---   ┆ ---     │
    /// │ str   ┆ i32     │
    /// ╞═══════╪═════════╡
    /// │ A     ┆ 2       │
    /// │ A     ┆ 4       │
    /// │ B     ┆ 6       │
    /// │ B     ┆ 8       │
    /// └───────┴─────────┘
    /// */
    /// </code>
    /// </example>
    public LazyFrame Filter(Expr expr)
        => new(PolarsWrapper.LazyFilter(CloneHandle(), expr.CloneHandle()));
    /// <summary>
    ///  Filter rows based on a boolean series.
    /// </summary>
    public LazyFrame Filter(Series series)
    {
        if (series.DataType != DataType.Boolean)
        {
            throw new InvalidExpressionException("Can not Filter by non-boolean series.");
        }
        
        using var expr = Pl.Lit(series); 
        
        return Filter(expr); 
    }

    /// <summary>
    /// Add or modify columns based on expressions.
    /// </summary>
    /// <example>
    /// <code>
    /// // Add a new column "c" while keeping "a" and "b"
    /// lf.WithColumns((Col("a") + Col("b")).Alias("c"));
    /// </code>
    /// </example>
    public LazyFrame WithColumns(params Expr[] exprs)
    {
        var lfClone = CloneHandle();
        var handles = exprs.Select(e => PolarsWrapper.CloneExpr(e.Handle)).ToArray();
        return new LazyFrame(PolarsWrapper.LazyWithColumns(lfClone, handles));
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
    /// Limit the number of rows in the LazyFrame.
    /// </summary>
    /// <param name="n"></param>
    /// <returns></returns>
    public LazyFrame Limit(uint n)
        => new(PolarsWrapper.LazyLimit(CloneHandle(), n));

    /// <summary>
    /// Keep unique rows (stable) based on a subset of columns defined by a Selector.
    /// </summary>
    /// <param name="subset">Selector defining the subset of columns. If null, uses all columns.</param>
    /// <param name="keep">Strategy to keep duplicates (First, Last, Any, None).</param>
    public LazyFrame Unique(Selector? subset = null, UniqueKeepStrategy keep = UniqueKeepStrategy.First, bool maintainOrder=false)
        => new (PolarsWrapper.LazyUnique(CloneHandle(), subset?.CloneHandle()!, keep.ToNative(),maintainOrder));
    
    /// <summary>
    /// Keep unique rows based on specific column names.
    /// </summary>
    /// <param name="columns">A collection of column names to group by.</param>
    /// <param name="keep">Strategy to keep duplicates (First, Last, Any, None).</param>
    /// <param name="maintainOrder">Whether to maintain the original order of the rows (stable).</param>
    public LazyFrame Unique(
        IEnumerable<string> columns, 
        UniqueKeepStrategy keep = UniqueKeepStrategy.First, 
        bool maintainOrder = false)
    {
        var columnsArray = columns as string[] ?? [.. columns];
        
        if (columnsArray.Length == 0)
        {
            return Unique(subset: null, keep, maintainOrder);
        }

        using var selector = Cs.ByName(columnsArray);
        
        return Unique(selector, keep, maintainOrder);
    }
    /// <summary>
    /// Get an explanation of the optimized query plan.
    /// <para>
    /// Returns a string representation of the logical plan after Polars optimizers 
    /// (predicate pushdown, projection pushdown, etc.) have run.
    /// </para>
    /// </summary>
    /// <param name="optimized">If true, show the optimized plan. If false, show the logical plan as built.</param>
    /// <returns>The plan as a string.</returns>
    /// <example>
    /// <code>
    /// var q = df.Lazy()
    ///     .Filter(Col("group") != "C")
    ///     .WithColumns((Col("val") * 2).Alias("val_x_2"))
    ///     .Select("group", "val_x_2");
    /// 
    /// Console.WriteLine(q.Explain());
    /// /* Output (Optimized Plan):
    /// simple π 2/2 ["group", "val_x_2"]
    ///    WITH_COLUMNS:
    ///    [[(col("val")) * (2)].alias("val_x_2")] 
    ///     FILTER [(col("group")) != ("C")]
    ///     FROM
    ///       DF ["group", "val"]; PROJECT["group", "val"] 2/2 COLUMNS
    /// */
    /// </code>
    /// </example>
    public string Explain(bool optimized = true)
        => PolarsWrapper.Explain(Handle, optimized);
    /// <summary>
    /// Generate a summary statistics DataFrame (count, mean, std, min, 25%, 50%, 75%, max).
    /// Similar to pandas/polars describe().
    /// Notice: This will collect LazyFrame once, but LazyFrame won't be consumed.
    /// </summary>
    public DataFrame Describe()
        => Clone().Collect().Describe();

    /// <summary>
    /// Returns the string representation of the LazyFrame (ASCII table).
    /// This allows Console.WriteLine(lf) to print the table directly.
    /// </summary>
    public override string ToString()
    {
        if (Handle.IsInvalid) return "LazyFrame (Disposed)";
        return Clone().Collect().ToString();
    }

    /// <summary>
    /// Print the LazyFrame to Console.
    /// </summary>
    public void Show() => Console.WriteLine(ToString());

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