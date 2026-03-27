using Polars.NET.Core;
using Polars.NET.Core.Arrow;
using Apache.Arrow;
using System.Data;
using Polars.NET.Core.Data;
using System.Collections.Concurrent;
using System.Collections;
using System.Text;
using Apache.Arrow.Types;
using System.Reflection;
using Polars.NET.Core.Helpers;
using Apache.Arrow.Ipc;
using System.Data.Common;
using System.Threading.Channels;
using System.Diagnostics.CodeAnalysis;
using System.Numerics.Tensors;

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
    /// Return an estimation of the total (heap) allocated size of the DataFrame.
    /// Estimated size is given in the specified unit (bytes by default).
    /// </summary>
    /// <param name="unit">Scale the returned size to the given unit (uses 1024 base).</param>
    /// <returns>The estimated size as a double.</returns>
    public double EstimatedSize(SizeUnit unit = SizeUnit.Bytes)
    {
        long bytes = PolarsWrapper.DataFrameEstimatedSize(Handle);

        return unit switch
        {
            SizeUnit.Bytes     => bytes, 
            SizeUnit.Kilobytes => bytes / 1024.0,
            SizeUnit.Megabytes => bytes / Math.Pow(1024, 2),
            SizeUnit.Gigabytes => bytes / Math.Pow(1024, 3),
            SizeUnit.Terabytes => bytes / Math.Pow(1024, 4),
            _ => throw new ArgumentOutOfRangeException(nameof(unit), $"Unsupported size unit: {unit}")
        };
    }

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
    // ==========================================
    // Aggregation
    // ==========================================
    
    /// <inheritdoc cref="LazyFrame.Count"/>
    public DataFrame Count()
        => Lazy().Count().Collect();

    /// <inheritdoc cref="LazyFrame.Sum"/>
    public DataFrame Sum()
        => Lazy().Sum().Collect();

    /// <inheritdoc cref="LazyFrame.Max"/>
    public DataFrame Max()
        => Lazy().Max().Collect();

    /// <inheritdoc cref="LazyFrame.Min"/>
    public DataFrame Min()
        => Lazy().Min().Collect();

    /// <inheritdoc cref="LazyFrame.Mean"/>
    public DataFrame Mean()
        => Lazy().Mean().Collect();

    /// <inheritdoc cref="LazyFrame.Median"/>
    public DataFrame Median()
        => Lazy().Median().Collect();

    /// <inheritdoc cref="LazyFrame.NullCount"/>
    public DataFrame NullCount()
        => Lazy().NullCount().Collect();
    
    /// <inheritdoc cref="LazyFrame.Std"/>
    public DataFrame Std(int ddof=1)
        => Lazy().Std(ddof).Collect();

    /// <inheritdoc cref="LazyFrame.Var"/>
    public DataFrame Var(int ddof=1)
        => Lazy().Var(ddof).Collect();

    /// <inheritdoc cref="LazyFrame.Quantile"/>
    public DataFrame Quantile(double quantile,QuantileMethod method = QuantileMethod.Linear)
        => Lazy().Quantile(quantile,method).Collect();

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
    {
        var lf = Lazy().Select(exprs);
        return lf.Collect();
    }
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
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Select(exprs);
    }
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
    {
        var lf = Lazy().Filter(expr);
        return lf.Collect();
    }
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
    {
        var lf = this.Lazy().WithColumns(exprs);
        return lf.Collect();
    }
    /// <summary>
    /// Sort the DataFrame by a single column.
    /// </summary>
    public DataFrame Sort(
        string column, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort(
            [Polars.Col(column)], 
            [descending], 
            [nullsLast], 
            maintainOrder
        );
    }
    /// <summary>
    /// Sort the DataFrame by a single expr.
    /// </summary>
    public DataFrame Sort(
        Expr expr, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort(
            [expr], 
            [descending], 
            [nullsLast], 
            maintainOrder
        );
    }
    /// <summary>
    /// Sort the DataFrame by multiple columns (all ascending or all descending).
    /// </summary>
    public DataFrame Sort(
        string[] columns, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Sort(
            exprs, 
            [descending], 
            [nullsLast], 
            maintainOrder
        );
    }

    /// <summary>
    /// Sort the DataFrame by multiple columns with specific sort orders for each column.
    /// <para>
    /// This allows for complex sorting, such as sorting by Category (Ascending) and then by Price (Descending).
    /// </para>
    /// </summary>
    /// <param name="columns">Names of the columns to sort by.</param>
    /// <param name="descending">Array of booleans indicating sort order for each column (false=Ascending, true=Descending).</param>
    /// <param name="nullsLast">Array of booleans indicating whether nulls should be placed at the end for each column.</param>
    /// <param name="maintainOrder">Whether to maintain the relative order of rows with equal keys (stable sort). Expensive.</param>
    /// <returns>A new sorted DataFrame.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B", "A" },
    ///     val = new[] { 10, 5, 20, 15, 8 }
    /// });
    /// 
    /// // Sort by "group" (Ascending) then by "val" (Descending)
    /// var sorted = df.Sort(
    ///     columns: new[] { "group", "val" }, 
    ///     descending: new[] { false, true }, // false=Asc, true=Desc
    ///     nullsLast: new[] { false, false }
    /// );
    /// 
    /// sorted.Show();
    /// /* Output:
    /// shape: (5, 2)
    /// ┌───────┬─────┐
    /// │ group ┆ val │
    /// │ ---   ┆ --- │
    /// │ str   ┆ i32 │
    /// ╞═══════╪═════╡
    /// │ A     ┆ 10  │
    /// │ A     ┆ 8   │
    /// │ A     ┆ 5   │
    /// │ B     ┆ 20  │
    /// │ B     ┆ 15  │
    /// └───────┴─────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Sort(
        string[] columns, 
        bool[] descending, 
        bool[] nullsLast, 
        bool maintainOrder = false)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return Sort(exprs, descending, nullsLast, maintainOrder);
    }

    /// <summary>
    /// Sort using multiple expressions (all ascending or all descending).
    /// </summary>
    public DataFrame Sort(
        Expr[] exprs, 
        bool descending = false, 
        bool nullsLast = false, 
        bool maintainOrder = false)
    {
        return Sort(
            exprs, 
            [descending], 
            [nullsLast], 
            maintainOrder
        );
    }

    /// <summary>
    /// Sort the DataFrame by multiple columns.
    /// </summary>
    public DataFrame Sort(
        Expr[] exprs, 
        bool[] descending, 
        bool[] nullsLast, 
        bool maintainOrder = false)
    {
        var lf = Lazy().Sort( 
            exprs, 
            descending, 
            nullsLast, 
            maintainOrder
        );
        
        return lf.Collect();
    }
    // ==========================================
    // TopK / BottomK (Eager Shortcuts)
    // ==========================================

    /// <summary>
    /// Get the top k rows according to the given expressions.
    /// <para>This selects the largest values.</para>
    /// </summary>
    public DataFrame TopK(int k, Expr[] by, bool[] reverse) => Lazy().TopK(k, by, reverse) .Collect();

    /// <summary>
    /// Get the top k rows according to a single expression.
    /// </summary>
    public DataFrame TopK(int k, Expr by, bool reverse = false) => Lazy().TopK(k, by, reverse).Collect();

    /// <summary>
    /// Get the top k rows according to a column name.
    /// </summary>
    public DataFrame TopK(int k, string colName, bool reverse = false) => Lazy().TopK(k, colName, reverse).Collect();

    /// <summary>
    /// Get the bottom k rows according to the given expressions.
    /// <para>This selects the smallest values.</para>
    /// </summary>
    public DataFrame BottomK(int k, Expr[] by, bool[] reverse) => Lazy().BottomK(k, by, reverse).Collect();

    /// <summary>
    /// Get the bottom k rows according to a single expression.
    /// </summary>
    public DataFrame BottomK(int k, Expr by, bool reverse = false) => Lazy().BottomK(k, by, reverse).Collect();

    /// <summary>
    /// Get the bottom k rows according to a column name.
    /// </summary>
    public DataFrame BottomK(int k, string colName, bool reverse = false) => Lazy().BottomK(k, colName, reverse).Collect();
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
    /// Explode list/array columns into multiple rows.
    /// <para>
    /// This un-nests list columns. If multiple columns are provided, they are exploded in parallel.
    /// Note: The list columns being exploded must have the same length for each row.
    /// </para>
    /// </summary>
    /// <param name="columns">Expressions selecting the columns to explode.</param>
    /// <returns>A new DataFrame where list elements are expanded into rows.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     id = new[] { 1, 2 },
    ///     tags = new[] { new[] { "apple", "orange" }, new[] { "banana" } },
    ///     scores = new[] { new[] { 10, 20 }, new[] { 30 } }
    /// });
    /// 
    /// // Explode "tags" and "scores" columns simultaneously
    /// var exploded = df.Explode(["tags","scores"]);
    /// 
    /// exploded.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────┬────────┬────────┐
    /// │ id  ┆ tags   ┆ scores │
    /// │ --- ┆ ---    ┆ ---    │
    /// │ i32 ┆ str    ┆ i32    │
    /// ╞═════╪════════╪════════╡
    /// │ 1   ┆ apple  ┆ 10     │
    /// │ 1   ┆ orange ┆ 20     │
    /// │ 2   ┆ banana ┆ 30     │
    /// └─────┴────────┴────────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Explode(params string[] columns)
    {
        using var sel = Selector.Cols(columns);
        return Explode(sel);
    }
    /// <summary>
    /// Explode list/array columns into multiple rows using selector.
    /// </summary>
    /// <param name="selector">Column Selector</param>
    /// <param name="emptyAsNull">
    /// If <c>true</c>, empty lists are exploded into a single <c>null</c> value. 
    /// If <c>false</c>, rows with empty lists are removed from the result.
    /// </param>
    /// <param name="keepNulls">
    /// If <c>true</c>, <c>null</c> values in the column are preserved as <c>null</c> in the result. 
    /// If <c>false</c>, rows with <c>null</c> values are removed.
    /// </param>
    /// <returns></returns>
    public DataFrame Explode(Selector selector,bool emptyAsNull=true, bool keepNulls=true)
    {
        var lf = Lazy().Explode(selector,emptyAsNull,keepNulls);
        return lf.Collect();
    }
    /// <summary>
    /// Decompose a struct column into multiple columns.
    /// <para>
    /// This is useful for flattening nested JSON-like data.
    /// </para>
    /// </summary>
    /// <param name="columns">The names of the struct columns to unnest.</param>
    /// <param name="separator">
    /// Optional separator to append to the struct field names (e.g., "struct_col.field"). 
    /// If null, the field names replace the struct column name directly.
    /// </param>
    /// <returns>A new DataFrame with the struct columns expanded.</returns>
    /// <example>
    /// <code>
    /// var data = new[] { new { User = new { Name = "Alice", Age = 30 } } };
    /// var df = DataFrame.From(data);
    /// 
    /// // Unnest with a separator to avoid name collisions
    /// // Result columns: "User_Name", "User_Age"
    /// var unnested = df.Unnest(new[] { "User" }, separator: "_");
    /// unnested.Show();
    /// /* Output:
    /// shape: (1, 2)
    /// ┌───────────┬──────────┐
    /// │ User_Name ┆ User_Age │
    /// │ ---       ┆ ---      │
    /// │ str       ┆ i32      │
    /// ╞═══════════╪══════════╡
    /// │ Alice     ┆ 30       │
    /// └───────────┴──────────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Unnest(string[] columns,string? separator=null)
    {
        var newHandle = PolarsWrapper.Unnest(Handle, columns,separator);
        return new DataFrame(newHandle);
    }
    /// <summary>
    /// Decompose a struct column into multiple columns (one for each field in the struct).
    /// <para>
    /// This is useful for flattening nested JSON-like data or composite types.
    /// The original struct column is replaced by its fields.
    /// </para>
    /// </summary>
    /// <param name="columns">The names of the struct columns to unnest.</param>
    /// <returns>A new DataFrame with the struct columns expanded.</returns>
    /// <example>
    /// <code>
    /// // Create a DataFrame with a nested structure (simulating JSON)
    /// var nestedData = new[] 
    /// {
    ///     new { Id = 1, User = new { Name = "Alice", City = "NY" } },
    ///     new { Id = 2, User = new { Name = "Bob",   City = "LA" } }
    /// };
    /// 
    /// var df = DataFrame.From(nestedData);
    /// 
    /// // Unnest the "User" column into "Name" and "City"
    /// var unnested = df.Unnest("User");
    /// 
    /// unnested.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬───────┬──────┐
    /// │ Id  ┆ Name  ┆ City │
    /// │ --- ┆ ---   ┆ ---  │
    /// │ i32 ┆ str   ┆ str  │
    /// ╞═════╪═══════╪══════╡
    /// │ 1   ┆ Alice ┆ NY   │
    /// │ 2   ┆ Bob   ┆ LA   │
    /// └─────┴───────┴──────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Unnest(params string[] columns) => Unnest(columns, separator: null);
    // ==========================================
    // Data Cleaning / Structure Ops
    // ==========================================

    /// <summary>
    /// Drop a column by name.
    /// </summary>
    public DataFrame Drop(string columnName) => new(PolarsWrapper.Drop(Handle, columnName));

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
    /// Drop rows containing null values.
    /// </summary>
    /// <param name="subset">Column names to consider. If null/empty, checks all columns.</param>
    public DataFrame DropNulls(params string[]? subset) => new(PolarsWrapper.DropNulls(Handle, subset));
    /// <summary>
    /// Get unique rows of this DataFrame maintaining the original order.
    /// </summary>
    /// <param name="subset">Columns to consider. If null, use all columns.</param>
    /// <param name="keep">Strategy to keep duplicates.</param>
    /// <returns>New DataFrame with unique rows.</returns>
    public DataFrame Unique(string[]? subset = null, UniqueKeepStrategy keep = UniqueKeepStrategy.First)
    {
        var h = PolarsWrapper.DataFrameUniqueStable(Handle, subset, keep.ToNative(), null);
        return new DataFrame(h);
    }

    /// <summary>
    /// Get unique rows with slicing support.
    /// </summary>
    public DataFrame Unique(
        string[]? subset, 
        UniqueKeepStrategy keep, 
        long offset, 
        ulong len)
    {
        var h = PolarsWrapper.DataFrameUniqueStable(Handle, subset, keep.ToNative(), (offset, len));
        return new DataFrame(h);
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

    // ==========================================
    // Combining DataFrames
    // ==========================================
    /// <summary>
    /// Join with another DataFrame using expression expressions.
    /// </summary>
    /// <param name="other">DataFrame to join with.</param>
    /// <param name="leftOn">Left keys.</param>
    /// <param name="rightOn">Right keys.</param>
    /// <param name="how">Join type (Inner, Left, etc.).</param>
    /// <param name="suffix">Suffix to append to columns with same name in right DataFrame. Default "_right".</param>
    /// <param name="validation">Check if join keys are unique.</param>
    /// <param name="coalesce">How to coalesce the join keys.</param>
    /// <param name="maintainOrder">How to maintain the order of the join.</param>
    /// <param name="joinSide">Specifies the strategy for the hash join build side.</param>
    /// <param name="nullsEqual">Consider nulls as equal.</param>
    /// <param name="sliceOffset">Slice the result starting at this offset.</param>
    /// <param name="sliceLen">Length of the slice.</param>
    public DataFrame Join(
        DataFrame other, 
        Expr[] leftOn, 
        Expr[] rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lf = Lazy().Join(
            other.Lazy(),
            leftOn,
            rightOn,
            how,
            suffix,
            validation,
            coalesce,
            maintainOrder,
            joinSide,
            nullsEqual,
            sliceOffset,
            sliceLen
        );

        return lf.Collect();
    }
    /// <summary>
    /// Join with another DataFrame using column names.
    /// </summary>
    /// <param name="other">The right DataFrame to join with.</param>
    /// <param name="leftOn">Column names in the left DataFrame to join on.</param>
    /// <param name="rightOn">Column names in the right DataFrame to join on.</param>
    /// <param name="how">Type of join (Inner, Left, Outer, Cross, etc.). Default is Inner.</param>
    /// <param name="suffix">Suffix to append to columns with same name in right DataFrame. Default "_right".</param>
    /// <param name="validation">Check if join keys are unique.</param>
    /// <param name="coalesce">How to coalesce the join keys.</param>
    /// <param name="maintainOrder">How to maintain the order of the join.</param>
    /// <param name="joinSide">Specifies the strategy for the hash join build side.</param>
    /// <param name="nullsEqual">Consider nulls as equal.</param>
    /// <param name="sliceOffset">Slice the result starting at this offset.</param>
    /// <param name="sliceLen">Length of the slice.</param>
    /// <returns>A new DataFrame resulting from the join.</returns>
    /// <example>
    /// <code>
    /// var dfCustomers = DataFrame.FromColumns(new
    /// {
    ///     id = new[] { 1, 2, 3 },
    ///     name = new[] { "Alice", "Bob", "Charlie" }
    /// });
    /// 
    /// var dfOrders = DataFrame.FromColumns(new
    /// {
    ///     id = new[] { 1, 2, 4 },
    ///     amount = new[] { 100, 200, 500 }
    /// });
    /// 
    /// // Perform an Inner Join on the "id" column
    /// var joined = dfCustomers.Join(
    ///     dfOrders, 
    ///     leftOn: new[] { "id" }, 
    ///     rightOn: new[] { "id" }, 
    ///     how: JoinType.Inner
    /// );
    /// 
    /// joined.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬───────┬────────┐
    /// │ id  ┆ name  ┆ amount │
    /// │ --- ┆ ---   ┆ ---    │
    /// │ i32 ┆ str   ┆ i32    │
    /// ╞═════╪═══════╪════════╡
    /// │ 1   ┆ Alice ┆ 100    │
    /// │ 2   ┆ Bob   ┆ 200    │
    /// └─────┴───────┴────────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Join(
        DataFrame other, 
        string[] leftOn, 
        string[] rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        var lExprs = leftOn.Select(Polars.Col).ToArray();
        var rExprs = rightOn.Select(Polars.Col).ToArray();
        return Join(
            other, 
            lExprs, 
            rExprs, 
            how, 
            suffix, 
            validation, 
            coalesce, 
            maintainOrder, 
            joinSide,
            nullsEqual, 
            sliceOffset, 
            sliceLen
        );
    }

    /// <summary>
    /// Join with another DataFrame using a single column pair (Convenience overload).
    /// </summary>
    /// <param name="other">The right DataFrame to join with.</param>
    /// <param name="leftOn">The column name in the left DataFrame.</param>
    /// <param name="rightOn">The column name in the right DataFrame.</param>
    /// <param name="how">Type of join. Default is Inner.</param>
    /// <param name="suffix">Suffix to append to columns with same name in right DataFrame. Default "_right".</param>
    /// <param name="validation">Check if join keys are unique.</param>
    /// <param name="coalesce">How to coalesce the join keys.</param>
    /// <param name="maintainOrder">How to maintain the order of the join.</param>
    /// <param name="joinSide">Specifies the strategy for the hash join build side.</param>
    /// <param name="nullsEqual">Consider nulls as equal.</param>
    /// <param name="sliceOffset">Slice the result starting at this offset.</param>
    /// <param name="sliceLen">Length of the slice.</param>
    /// <seealso cref="Join(DataFrame, string[], string[], JoinType,string?,JoinValidation,JoinCoalesce,JoinMaintainOrder,JoinSide,bool,long?,ulong)"/>
    public DataFrame Join(
        DataFrame other, 
        string leftOn, 
        string rightOn, 
        JoinType how = JoinType.Inner,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return Join(
            other, 
            [leftOn], 
            [rightOn], 
            how, 
            suffix, 
            validation, 
            coalesce, 
            maintainOrder, 
            joinSide,
            nullsEqual, 
            sliceOffset, 
            sliceLen
        );
    }
    /// <inheritdoc cref="LazyFrame.JoinAsOf(LazyFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    /// <example>
    /// <code>
    /// // Trades: Events happening at specific times
    /// var trades = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 10, 20, 30 },
    ///     stock = new[] { "A", "A", "A" }
    /// });
    /// 
    /// // Quotes: Price updates (irregular intervals)
    /// // 9->100, 15->101, 25->102, 40->103
    /// var quotes = DataFrame.FromColumns(new
    /// {
    ///     time = new[] { 9, 15, 25, 40 },
    ///     bid = new[] { 100, 101, 102, 103 }
    /// });
    /// 
    /// // Find the latest quote BEFORE or AT the trade time
    /// var asof = trades.JoinAsOf(
    ///     quotes, 
    ///     leftOn: Col("time"), 
    ///     rightOn: Col("time"),
    ///     strategy: AsofStrategy.Backward
    /// );
    /// 
    /// asof.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌──────┬───────┬─────┐
    /// │ time ┆ stock ┆ bid │
    /// │ ---  ┆ ---   ┆ --- │
    /// │ i32  ┆ str   ┆ i32 │
    /// ╞══════╪═══════╪═════╡
    /// │ 10   ┆ A     ┆ 100 │ // Matches time 9
    /// │ 20   ┆ A     ┆ 101 │ // Matches time 15
    /// │ 30   ┆ A     ┆ 102 │ // Matches time 25
    /// └──────┴───────┴─────┘
    /// */
    /// </code>
    /// </example>
    internal DataFrame JoinAsOf(
        DataFrame other, 
        Expr leftOn, Expr rightOn, 
        string? toleranceStr = null,
        long? toleranceInt = null,
        double? toleranceFloat = null,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return this.Lazy().JoinAsOf(
            other.Lazy(),
            leftOn, rightOn,
            toleranceStr,
            toleranceInt,
            toleranceFloat,
            strategy,
            leftBy,
            rightBy,
            allowEq,
            checkSorted,
            suffix,
            validation,
            coalesce,
            maintainOrder,
            joinSide,
            nullsEqual,
            sliceOffset,
            sliceLen
        ).Collect();
    }

    /// <inheritdoc cref="JoinAsOf(DataFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    public DataFrame JoinAsOf(
        DataFrame other, 
        Expr leftOn, Expr rightOn, 
        string tolerance,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return JoinAsOf(
            other,
            leftOn,
            rightOn,
            tolerance,
            null,null,
            strategy,leftBy,rightBy,allowEq,checkSorted,suffix,
            validation,coalesce,maintainOrder,joinSide,nullsEqual,sliceOffset,sliceLen
        );
    }

    /// <inheritdoc cref="JoinAsOf(DataFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    public DataFrame JoinAsOf(
        DataFrame other, 
        Expr leftOn, Expr rightOn, 
        TimeSpan tolerance,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return JoinAsOf(
            other,
            leftOn,
            rightOn,
            DurationFormatter.ToPolarsString(tolerance),
            null,null,
            strategy,leftBy,rightBy,allowEq,checkSorted,suffix,
            validation,coalesce,maintainOrder,joinSide,nullsEqual,sliceOffset,sliceLen
        );
    }
    /// <inheritdoc cref="JoinAsOf(DataFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    public DataFrame JoinAsOf(
        DataFrame other, 
        Expr leftOn, Expr rightOn, 
        int tolerance,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return JoinAsOf(
            other,
            leftOn,
            rightOn,
            null,
            tolerance,null,
            strategy,leftBy,rightBy,allowEq,checkSorted,suffix,
            validation,coalesce,maintainOrder,joinSide,nullsEqual,sliceOffset,sliceLen
        );
    }
    /// <inheritdoc cref="JoinAsOf(DataFrame, Expr, Expr, string?, long?, double?, AsofStrategy, Expr[], Expr[], bool, bool, string?, JoinValidation, JoinCoalesce, JoinMaintainOrder,JoinSide, bool, long?, ulong)"/>
    public DataFrame JoinAsOf(
        DataFrame other, 
        Expr leftOn, Expr rightOn, 
        double tolerance,
        AsofStrategy strategy = AsofStrategy.Backward,
        Expr[]? leftBy = null,
        Expr[]? rightBy = null,
        bool allowEq = true,
        bool checkSorted = true,
        string? suffix = null,
        JoinValidation validation = JoinValidation.ManyToMany,
        JoinCoalesce coalesce = JoinCoalesce.JoinSpecific,
        JoinMaintainOrder maintainOrder = JoinMaintainOrder.None,
        JoinSide joinSide = JoinSide.LetPolarsDecide,
        bool nullsEqual = false,
        long? sliceOffset = null,
        ulong sliceLen = 0)
    {
        return JoinAsOf(
            other,
            leftOn,
            rightOn,
            null,
            null,tolerance,
            strategy,leftBy,rightBy,allowEq,checkSorted,suffix,
            validation,coalesce,maintainOrder,joinSide,nullsEqual,sliceOffset,sliceLen
        );
    }

    private static DataFrame ConcatInternal(
        IEnumerable<DataFrame> dfs, 
        PlConcatType how, 
        bool checkDuplicates,
        bool strict = true,
        bool unitLengthAsScalar = false)
    {
        var dfList = dfs.ToList();
        if (dfList.Count == 0) return new DataFrame();

        var handles = dfList.Select(df => df.Clone().Handle).ToArray();

        var h = PolarsWrapper.Concat(handles, how, checkDuplicates,strict,unitLengthAsScalar);
        return new DataFrame(h);
    }
    /// <summary>
    /// Concatenate multiple DataFrames either vertically (union) or horizontally.
    /// </summary>
    /// <param name="dfs">A collection of DataFrames to concatenate.</param>
    /// <returns>A new combined DataFrame.</returns>
    /// <example>
    /// <code>
    /// var df1 = DataFrame.FromColumns(new
    /// {
    ///     a = new[] { 1, 2 },
    ///     b = new[] { 10, 20 }
    /// });
    /// 
    /// var df2 = DataFrame.FromColumns(new
    /// {
    ///     a = new[] { 3, 4 },
    ///     b = new[] { 30, 40 }
    /// });
    /// 
    /// // Vertical Concat (Union rows)
    /// var catVertical = DataFrame.ConcatVertical(new[] { df1, df2 });
    /// catVertical.Show();
    /// /* Output:
    /// shape: (4, 2)
    /// ┌─────┬─────┐
    /// │ a   ┆ b   │
    /// │ --- ┆ --- │
    /// │ i32 ┆ i32 │
    /// ╞═════╪═════╡
    /// │ 1   ┆ 10  │
    /// │ 2   ┆ 20  │
    /// │ 3   ┆ 30  │
    /// │ 4   ┆ 40  │
    /// └─────┴─────┘
    /// */
    /// 
    /// // Horizontal Concat (Append columns)
    /// var df3 = DataFrame.FromColumns(new { c = new[] { "x", "y" } });
    /// var catHorizontal = DataFrame.ConcatHorizontal(new[] { df1, df3 });
    /// catHorizontal.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌─────┬─────┬─────┐
    /// │ a   ┆ b   ┆ c   │
    /// │ --- ┆ --- ┆ --- │
    /// │ i32 ┆ i32 ┆ str │
    /// ╞═════╪═════╪═════╡
    /// │ 1   ┆ 10  ┆ x   │
    /// │ 2   ┆ 20  ┆ y   │
    /// └─────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public static DataFrame Concat(IEnumerable<DataFrame> dfs)
        =>ConcatInternal(dfs, PlConcatType.Vertical, true);
    /// <summary>
    /// Horizontal concatenation of DataFrames.
    /// </summary>
    /// <param name="dfs">DataFrames to concat.</param>
    /// <param name="checkDuplicates">
    /// If true, check that the column names are unique. 
    /// If multiple columns have the same name, they will be dropped.
    /// </param>
    /// <param name="strict">For Horizontal: if true, error on height mismatch.</param>
    /// <param name="unitLengthAsScalar">For Horizontal: if true, broadcast length-1 DataFrames to match height.</param>
    public static DataFrame ConcatHorizontal(
        IEnumerable<DataFrame> dfs,
        bool checkDuplicates = true,
        bool strict = true,
        bool unitLengthAsScalar = false)
    {
        return ConcatInternal(dfs, PlConcatType.Horizontal, checkDuplicates, strict,unitLengthAsScalar);
    }
    /// <summary>
    /// Diagonal concatenation of DataFrames.
    /// </summary>
    public static DataFrame ConcatDiagonal(IEnumerable<DataFrame> dfs)
    {
        return ConcatInternal(dfs, PlConcatType.Diagonal, true);
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

    // ==========================================
    // GroupBy
    // ==========================================
    /// <summary>
    /// Start a GroupBy operation to perform aggregations on groups of data.
    /// <para>
    /// This returns a <see cref="GroupByBuilder"/> which allows you to specify the aggregation functions 
    /// (like Sum, Min, Max, Count) using the <c>.Agg()</c> method.
    /// </para>
    /// </summary>
    /// <param name="by">Expressions defining the columns to group by.</param>
    /// <returns>A builder object to define aggregations.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     group = new[] { "A", "A", "B", "B", "B" },
    ///     val1 = new[] { 1, 2, 3, 4, 5 },
    ///     val2 = new[] { 10, 20, 10, 20, 30 }
    /// });
    /// 
    /// // Group by "group" and calculate:
    /// // 1. Sum of val1
    /// // 2. Max of val2 (aliased to "max_val2")
    /// // 3. Count of elements in the group
    /// var grouped = df.GroupBy(Col("group")).Agg(
    ///     Col("val1").Sum(),
    ///     Col("val2").Max().Alias("max_val2"),
    ///     Col("val1").Count().Alias("count")
    /// );
    /// 
    /// grouped.Sort("group").Show();
    /// /* Output:
    /// shape: (2, 4)
    /// ┌───────┬──────┬──────────┬───────┐
    /// │ group ┆ val1 ┆ max_val2 ┆ count │
    /// │ ---   ┆ ---  ┆ ---      ┆ ---   │
    /// │ str   ┆ i32  ┆ i32      ┆ u32   │
    /// ╞═══════╪══════╪══════════╪═══════╡
    /// │ A     ┆ 3    ┆ 20       ┆ 2     │
    /// │ B     ┆ 12   ┆ 30       ┆ 3     │
    /// └───────┴──────┴──────────┴───────┘
    /// */
    /// </code>
    /// </example>
    public GroupByBuilder GroupBy(params Expr[] by) => new (this, by);
    /// <summary>
    /// Start a GroupBy operation using column names (convenience overload).
    /// </summary>
    /// <param name="columns">Names of the columns to group by.</param>
    /// <returns>A builder object to define aggregations.</returns>
    /// <remarks>
    /// For more complex grouping logic (expressions), use <see cref="GroupBy(Expr[])"/>.
    /// </remarks>
    /// <seealso cref="GroupBy(Expr[])"/>
    public GroupByBuilder GroupBy(params string[] columns)
    {
        var exprs = columns.Select(Polars.Col).ToArray();
        return GroupBy(exprs);
    }
    /// <summary>
    /// Group based on a time index using dynamic windows (Rolling/Resampling).
    /// <para>
    /// This is essential for time-series analysis, allowing you to downsample data (e.g., 1-minute data to 1-hour bars).
    /// </para>
    /// </summary>
    /// <param name="indexColumn">The column containing time/date data (must be sorted).</param>
    /// <param name="every">The interval at which to start a new window (e.g., "1h", "1d"). Also known as the step size.</param>
    /// <param name="period">The duration of each window. If null, defaults to <paramref name="every"/>.</param>
    /// <param name="offset">Offset to shift the window start times.</param>
    /// <param name="by">Optional extra columns to group by (e.g., group by "stock_symbol" AND time window).</param>
    /// <param name="label">Which label to use for the window (Left boundary, Right boundary, or Datapoint).</param>
    /// <param name="includeBoundaries">Whether to include the window boundaries in the output.</param>
    /// <param name="closedWindow">Which side of the window interval is closed (inclusive).</param>
    /// <param name="startBy">Strategy to determine the start of the first window.</param>
    /// <returns>A <see cref="DynamicGroupBy"/> object to define aggregations.</returns>
    /// <example>
    /// <code>
    /// var df = DataFrame.FromColumns(new
    /// {
    ///     time = new[] 
    ///     { 
    ///         new DateTime(2024, 1, 1, 10, 0, 0),
    ///         new DateTime(2024, 1, 1, 10, 30, 0), // Inside 10:00-11:00 window
    ///         new DateTime(2024, 1, 1, 11, 0, 0), // Start of next window
    ///         new DateTime(2024, 1, 1, 11, 15, 0),
    ///         new DateTime(2024, 1, 2, 09, 0, 0)
    ///     },
    ///     val = new[] { 1, 2, 3, 4, 5 }
    /// });
    /// 
    /// // Group into 1-hour windows based on "time"
    /// var dynamicGrouped = df.GroupByDynamic(
    ///     indexColumn: "time", 
    ///     every: TimeSpan.FromHours(1)
    /// ).Agg(
    ///     Col("val").Sum().Alias("total_val"),
    ///     Col("val").Count().Alias("count")
    /// );
    /// 
    /// dynamicGrouped.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────────────────────┬───────────┬───────┐
    /// │ time                ┆ total_val ┆ count │
    /// │ ---                 ┆ ---       ┆ ---   │
    /// │ datetime[μs]        ┆ i32       ┆ u32   │
    /// ╞═════════════════════╪═══════════╪═══════╡
    /// │ 2024-01-01 10:00:00 ┆ 3         ┆ 2     │ // 10:00 + 10:30
    /// │ 2024-01-01 11:00:00 ┆ 7         ┆ 2     │ // 11:00 + 11:15
    /// │ 2024-01-02 09:00:00 ┆ 5         ┆ 1     │
    /// └─────────────────────┴───────────┴───────┘
    /// */
    /// </code>
    /// </example>
    public DynamicGroupBy GroupByDynamic(
        string indexColumn,
        TimeSpan every,
        TimeSpan? period = null,
        TimeSpan? offset = null,
        Expr[]? by = null,
        Label label = Label.Left,
        bool includeBoundaries = false,
        ClosedWindow closedWindow = ClosedWindow.Left,
        StartBy startBy = StartBy.WindowBound
    )
    {
        return new DynamicGroupBy(
            this,
            indexColumn,
            every,
            period,
            offset,
            by,
            label,
            includeBoundaries,
            closedWindow,
            startBy
        );
    }

    // ==========================================
    // Pivot / Unpivot
    // ==========================================
    /// <summary>
    /// Pivot the DataFrame using Selectors for column selection. 
    /// This is the most flexible pivot method.
    /// </summary>
    /// <param name="index">Selector for the index column(s).</param>
    /// <param name="columns">Selector for the column(s) to pivot.</param>
    /// <param name="values">Selector for the value column(s).</param>
    /// <param name="aggregateExpr">Optional expression to aggregate the values. If null, uses <paramref name="aggregateFunction"/>.</param>
    /// <param name="aggregateFunction">Aggregation function to use if <paramref name="aggregateExpr"/> is null. Default is First.</param>
    /// <param name="sortColumns">Sort the transposed columns by name.</param>
    /// <param name="maintainOrder">Keep the original order of the rows (index).</param>
    /// <param name="separator">Separator used to combine column names when multiple value columns are selected.</param>
    public DataFrame Pivot(
        Selector index, 
        Selector columns, 
        Selector values, 
        Expr? aggregateExpr = null, 
        PivotAgg aggregateFunction = PivotAgg.First,
        bool sortColumns = false, 
        bool maintainOrder = true,
        string? separator = null)
    {
        using var indexH = index.CloneHandle();
        using var columnsH = columns.CloneHandle();
        using var valuesH = values.CloneHandle();
        using var aggExprH = aggregateExpr?.CloneHandle(); 

        var h = PolarsWrapper.Pivot(
            Handle,
            indexH,
            columnsH,
            valuesH,
            aggExprH, 
            aggregateFunction.ToNative(),
            sortColumns,
            maintainOrder,
            separator
        );

        return new DataFrame(h);
    }
    /// <summary>
    /// Pivot the DataFrame from long to wide format.
    /// <para>
    /// This creates a new column for each unique value in the <paramref name="columns"/> argument.
    /// The values in the new columns come from the <paramref name="values"/> column.
    /// </para>
    /// </summary>
    /// <param name="index">Column names to use as the index (the rows).</param>
    /// <param name="columns">Column names to use for the new column headers.</param>
    /// <param name="values">Column names to use for the values in the cells.</param>
    /// <param name="aggregateFunction">Aggregation function to use if multiple values exist for an index/column pair. Default is First.</param>
    /// <param name="sortColumns">Sort the transposed columns by name. Default is by order of discovery.</param>
    /// <param name="maintainOrder">Keep the original order of the rows.</param>
    /// <param name="separator">Used as separator/delimiter in generated column names in case of multiple values columns.</param>
    /// <returns>A wide-format DataFrame.</returns>
    /// <example>
    /// <code>
    /// var dfLong = DataFrame.FromColumns(new
    /// {
    ///     date = new[] { "2024-01-01", "2024-01-01", "2024-01-02", "2024-01-02" },
    ///     country = new[] { "US", "CN", "US", "CN" },
    ///     sales = new[] { 100, 200, 110, 220 }
    /// });
    /// 
    /// // Pivot: rows=date, cols=country, values=sales
    /// var pivoted = dfLong.Pivot(
    ///     index: new[] { "date" }, 
    ///     columns: new[] { "country" }, 
    ///     values: new[] { "sales" }
    /// );
    /// 
    /// pivoted.Show();
    /// /* Output:
    /// shape: (2, 3)
    /// ┌────────────┬─────┬─────┐
    /// │ date       ┆ US  ┆ CN  │
    /// │ ---        ┆ --- ┆ --- │
    /// │ str        ┆ i32 ┆ i32 │
    /// ╞════════════╪═════╪═════╡
    /// │ 2024-01-01 ┆ 100 ┆ 200 │
    /// │ 2024-01-02 ┆ 110 ┆ 220 │
    /// └────────────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Pivot(string[] index, string[] columns, string[] values, PivotAgg aggregateFunction = PivotAgg.First,bool sortColumns =false,bool maintainOrder = true,string? separator=null)
    {
        using var sIndex = Selector.Cols(index);
        using var sColumns = Selector.Cols(columns);
        using var sValues = Selector.Cols(values);

        return Pivot(
            sIndex, 
            sColumns, 
            sValues, 
            aggregateExpr: null, 
            aggregateFunction: aggregateFunction, 
            sortColumns: sortColumns, 
            maintainOrder: maintainOrder, 
            separator: separator
        );
    }
    /// <summary>
    /// Pivot a DataFrame with a custom aggregation expression.
    /// </summary>
    /// <param name="index">Column names to use as index.</param>
    /// <param name="columns">Column names to use as columns.</param>
    /// <param name="values">Column names to use as values.</param>
    /// <param name="aggregateExpr">
    /// A custom expression to aggregate the values (e.g., <c>pl.Col("val").Sum() * 100</c>).
    /// </param>
    /// <param name="sortColumns">
    /// Sort the transposed columns.
    /// </param>
    /// <param name="separator">
    /// Separator used to combine column names when multiple value columns are selected.
    /// </param>
    /// <param name="maintainOrder">Keep the original order of the rows.</param>
    /// <returns>Pivoted DataFrame.</returns>
    public DataFrame Pivot(
        string[] index,
        string[] columns,
        string[] values,
        Expr aggregateExpr,
        bool sortColumns = false,
        bool maintainOrder = true,
        string? separator = null)
    {
        using var sIndex = Selector.Cols(index);
        using var sColumns = Selector.Cols(columns);
        using var sValues = Selector.Cols(values);

        return Pivot(
            sIndex, 
            sColumns, 
            sValues, 
            aggregateExpr: aggregateExpr, 
            sortColumns: sortColumns, 
            maintainOrder: maintainOrder, 
            separator: separator
        );
    }
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// <para>
    /// This is the reverse of <see cref="Pivot(string[], string[], string[], PivotAgg, bool,bool, string?)"/>. It collapses multiple columns into key-value pairs.
    /// </para>
    /// </summary>
    /// <param name="index">Column names to keep as identifiers (id_vars).</param>
    /// <param name="on">Column names to unpivot/melt (value_vars).</param>
    /// <param name="variableName">Name for the new variable column (default "variable").</param>
    /// <param name="valueName">Name for the new value column (default "value").</param>
    /// <returns>A long-format DataFrame.</returns>
    /// <example>
    /// <code>
    /// // Using the 'pivoted' DataFrame from the Pivot example
    /// // shape: (2, 3) [date, US, CN]
    /// 
    /// // Unpivot back to long format
    /// var melted = pivoted.Unpivot(
    ///     index: new[] { "date" },
    ///     on: new[] { "CN", "US" },
    ///     variableName: "country",
    ///     valueName: "sales"
    /// );
    /// 
    /// melted.Sort(new[] { "date", "country" }).Show();
    /// /* Output:
    /// shape: (4, 3)
    /// ┌────────────┬─────────┬───────┐
    /// │ date       ┆ country ┆ sales │
    /// │ ---        ┆ ---     ┆ ---   │
    /// │ str        ┆ str     ┆ i32   │
    /// ╞════════════╪═════════╪═══════╡
    /// │ 2024-01-01 ┆ CN      ┆ 200   │
    /// │ 2024-01-01 ┆ US      ┆ 100   │
    /// │ 2024-01-02 ┆ CN      ┆ 220   │
    /// │ 2024-01-02 ┆ US      ┆ 110   │
    /// └────────────┴─────────┴───────┘
    /// */
    /// </code>
    /// </example>
    public DataFrame Unpivot(string[] index, string[]? on, string variableName = "variable", string valueName = "value")
    {
        using var sIndex = Selector.Cols(index);
        using var sOn = on is not null ? Selector.Cols(on) : null;

        return Unpivot(sIndex, sOn, variableName, valueName);
    }
    /// <summary>
    /// Unpivot (Melt) the DataFrame from wide to long format.
    /// </summary>
    /// <param name="index"></param>
    /// <param name="on"></param>
    /// <param name="variableName"></param>
    /// <param name="valueName"></param>
    /// <returns></returns>
    public DataFrame Unpivot(Selector index, Selector? on, string variableName = "variable", string valueName = "value")
    {
        var lf = Lazy().Unpivot(index,on,variableName,valueName);
        return lf.Collect();
    }
    /// <summary>
    /// Alias for <see cref="Unpivot(string[], string[], string, string)"/>. Melts the DataFrame from wide to long format.
    /// </summary>
    /// <seealso cref="Unpivot(string[], string[], string, string)"/>
    public DataFrame Melt(string[] index, string[]? on, string variableName = "variable", string valueName = "value") 
        => Unpivot(index, on, variableName, valueName);


    /// <summary>
    /// Export DataFrame to Record Batch
    /// </summary>
    /// <param name="onBatchReceived">Receive RecordBatch Callback</param>
    public void ExportBatches(Action<RecordBatch> onBatchReceived)
        => PolarsWrapper.ExportBatches(Handle, onBatchReceived);
    /// <summary>
    /// Export DataFrame As DbDataReader (Zero-Copy Enabled)
    /// </summary>    
    public DbDataReader AsDataReader(int bufferSize = 5, Dictionary<string, Type>? typeOverrides = null)
    {
        var channel = Channel.CreateBounded<RecordBatch>(new BoundedChannelOptions(bufferSize)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true, 
            SingleReader = true  
        });

        var cts = new CancellationTokenSource();

        var producerTask = Task.Run(async () => 
        {
            try
            {
                ExportBatches(batch => 
                {
                    channel.Writer.WriteAsync(batch, cts.Token).AsTask().Wait(cts.Token);
                });
                
                channel.Writer.Complete();
            }
            catch (OperationCanceledException)
            {

                channel.Writer.Complete();
            }
            catch (Exception ex)
            {
                channel.Writer.Complete(ex);
            }
        });

        var stream = channel.Reader.ReadAllAsync(cts.Token).ToBlockingEnumerable(cts.Token);
        
        var innerReader = new ArrowToDbStream(stream, typeOverrides);

        return new PolarsDataReader(innerReader, cts, producerTask); 
    }
 
    /// <summary>
    /// Generate a summary statistics DataFrame (count, mean, std, min, 25%, 50%, 75%, max).
    /// Similar to pandas/polars describe().
    /// </summary>
    public DataFrame Describe()
    {
        using var schema = Schema;
        
        var numericCols = new List<string>();

        foreach (var name in schema.ColumnNames)
        {
            using var dtype = schema[name];
            
            if (dtype.IsNumeric)
            {
                numericCols.Add(name);
            }
        }

        if (numericCols.Count == 0)
            throw new InvalidOperationException("No numeric columns to describe.");

        // 2. Define statistical metrics
        var metrics = new List<(string Name, Func<string, Expr> Op)>
        {
            ("count",      c => Polars.Col(c).Count().Cast(DataType.Float64)),
            ("null_count", c => Polars.Col(c).IsNull().Sum().Cast(DataType.Float64)),
            ("mean",       c => Polars.Col(c).Mean()),
            ("std",        c => Polars.Col(c).Std()),
            ("min",        c => Polars.Col(c).Min().Cast(DataType.Float64)),
            ("25%",        c => Polars.Col(c).Quantile(0.25, QuantileMethod.Nearest).Cast(DataType.Float64)),
            ("50%",        c => Polars.Col(c).Median().Cast(DataType.Float64)),
            ("75%",        c => Polars.Col(c).Quantile(0.75, QuantileMethod.Nearest).Cast(DataType.Float64)),
            ("max",        c => Polars.Col(c).Max().Cast(DataType.Float64))
        };

        var rowFrames = new List<DataFrame>();
        
        try
        {
            foreach (var (statName, op) in metrics)
            {
                var exprs = new List<Expr>
                {
                    Polars.Lit(statName).Alias("statistic")
                };

                foreach (var col in numericCols)
                {
                    exprs.Add(op(col));
                }

                rowFrames.Add(Select(exprs.ToArray()));
            }

            return Concat(rowFrames);
        }
        finally
        {
            foreach (var frame in rowFrames)
            {
                frame.Dispose();
            }
        }
    }
    // ==========================================
    // Display (Show)
    // ==========================================
    /// <summary>
    /// Returns the string representation of the DataFrame (ASCII table).
    /// This allows Console.WriteLine(df) to print the table directly.
    /// </summary>
    public override string ToString()
    {
        if (Handle.IsInvalid) return "DataFrame (Disposed)";
        return PolarsWrapper.DataFrameToString(Handle);
    }

    /// <summary>
    /// Print the DataFrame to Console.
    /// </summary>
    public void Show() => Console.WriteLine(ToString());
    // ==========================================
    // Interop
    // ==========================================
    /// <summary>
    /// Converts selected DataFrame columns into a managed 2D Row-Major Tensor [Rows, Columns].
    /// Automatically handles memory fragmentation and performs the Column-Major to Row-Major transposition 
    /// required by most Machine Learning frameworks (like ONNX, TorchSharp, ML.NET).
    /// </summary>
    /// <typeparam name="T">Unmanaged Type Only (e.g., float, double, int)</typeparam>
    /// <param name="columnNames">Specific columns to extract. If empty, extracts all columns.</param>
    /// <returns>A 2D <see cref="Tensor{T}"/> representing the feature matrix.</returns>
    public Tensor<T> AsTensor<T>(params string[] columnNames) where T : unmanaged
    {
        Series[] targetColumns;
        if (columnNames == null || columnNames.Length == 0)
        {
            targetColumns = GetColumns();
        }
        else
        {
            targetColumns = new Series[columnNames.Length];
            for (int i = 0; i < columnNames.Length; i++)
            {
                targetColumns[i] = Column(columnNames[i]); 
            }
        }

        if (targetColumns.Length == 0) 
            throw new InvalidOperationException("Cannot create a Tensor from an empty DataFrame.");

        int cols = targetColumns.Length;
        int rows = (int)Height; 

        T[] tensorData = new T[rows * cols];
        Span<T> destSpan = tensorData.AsSpan();

        try
        {
            for (int c = 0; c < cols; c++)
            {
                var col = targetColumns[c];

                bool needsRechunk = !col.IsContiguous;
                Series activeCol = needsRechunk ? col.Rechunk() : col;

                try
                {
                    ReadOnlySpan<T> span = activeCol.AsReadOnlySpan<T>();

                    for (int r = 0; r < rows; r++)
                    {
                        destSpan[r * cols + c] = span[r];
                    }
                }
                finally
                {
                    if (needsRechunk) activeCol.Dispose();
                }
            }

            return Tensor.Create(tensorData, [rows, cols]);
        }
        finally
        {
            for (int i = 0; i < cols; i++)
            {
                targetColumns[i]?.Dispose();
            }
        }
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
    // Object Mapping (From Records)
    // ==========================================
    /// <summary>
    /// Create a DataFrame from a collection of strongly-typed objects (POCOs).
    /// <para>
    /// This method uses reflection to inspect the properties of the class <typeparamref name="T"/> 
    /// and maps them to DataFrame columns.
    /// </para>
    /// </summary>
    /// <typeparam name="T">The class type of the records.</typeparam>
    /// <param name="data">The collection of objects to load.</param>
    /// <returns>A new DataFrame.</returns>
    /// <example>
    /// <code>
    /// public class Student
    /// {
    ///     public string Name { get; set; }
    ///     public int Age { get; set; }
    ///     public double GPA { get; set; }
    /// }
    /// 
    /// var students = new List&lt;Student&gt;
    /// {
    ///     new Student { Name = "Alice", Age = 20, GPA = 3.5 },
    ///     new Student { Name = "Bob", Age = 22, GPA = 3.8 },
    ///     new Student { Name = "Charlie", Age = 19, GPA = 3.2 }
    /// };
    /// 
    /// var df = DataFrame.From(students);
    /// df.Show();
    /// /* Output:
    /// shape: (3, 3)
    /// ┌─────────┬─────┬─────┐
    /// │ Name    ┆ Age ┆ GPA │
    /// │ ---     ┆ --- ┆ --- │
    /// │ str     ┆ i32 ┆ f64 │
    /// ╞═════════╪═════╪═════╡
    /// │ Alice   ┆ 20  ┆ 3.5 │
    /// │ Bob     ┆ 22  ┆ 3.8 │
    /// │ Charlie ┆ 19  ┆ 3.2 │
    /// └─────────┴─────┴─────┘
    /// */
    /// </code>
    /// </example>
    public static DataFrame From<T>(IEnumerable<T> data)
    {
        if (data == null) return new DataFrame();
        Type type = typeof(T);

        // =========================================================
        // 1. Primitive Types (Single Column)
        // =========================================================
        if (IsSimpleType(type))
        {
            var s = Series.From("value", data);
            return new DataFrame(s);
        }

        // =========================================================
        // 2. Complex Type: Pivot (Row -> Column)
        // =========================================================
        return FromPocoManual(data, type);
    }
    /// <inheritdoc cref="From"/>
    public static DataFrame FromRows<T>(IEnumerable<T> data)
        => From(data);
    
    private static DataFrame FromPocoManual<T>(IEnumerable<T> data, Type type)
    {
        var props = type.GetProperties(BindingFlags.Public | BindingFlags.Instance);
        if (props.Length == 0) return new DataFrame();

        int capacity = data is ICollection<T> c ? c.Count : 16;
        var buffers = new IColumnBuffer[props.Length];

        for (int i = 0; i < props.Length; i++)
        {
            buffers[i] = ColumnBufferFactory.Create(props[i].PropertyType, capacity);
        }

        foreach (var item in data)
        {
            for (int i = 0; i < props.Length; i++)
            {
                buffers[i].Add(props[i].GetValue(item));
            }
        }

        var seriesList = new Series[props.Length];
        for (int i = 0; i < props.Length; i++)
        {
            seriesList[i] = buffers[i].ToSeries(props[i].Name);
        }

        return new DataFrame(seriesList);
    }

    private static bool IsSimpleType(Type type)
    {
        return type.IsPrimitive || 
               type == typeof(string) || 
               type == typeof(DateOnly) || 
               type == typeof(decimal) || 
               type == typeof(DateTime) || 
               type == typeof(TimeSpan) ||
               type == typeof(TimeOnly) || 
               type == typeof(DateTimeOffset) || 
               (type.IsGenericType && type.GetGenericTypeDefinition() == typeof(Nullable<>) && IsSimpleType(Nullable.GetUnderlyingType(type)!));
    }
    /// <summary>
    /// Create a DataFrame from an object where properties represent columns (Arrays/Lists).
    /// This is useful for "Structure of Arrays" (SoA) data layout.
    /// </summary>
    /// <example>
    /// var df = DataFrame.FromColumns(new { 
    ///     Time = new[] { dt1, dt2 }, 
    ///     Val = new[] { 1.0, 2.0 } 
    /// });
    /// </example>
    [RequiresUnreferencedCode("Uses reflection and dynamic to extract properties from anonymous types.")]
    public static DataFrame FromColumns(object columns)
    {
        ArgumentNullException.ThrowIfNull(columns);

        var properties = columns.GetType().GetProperties(BindingFlags.Public | BindingFlags.Instance);
        
        var cols = new (string, object)[properties.Length];

        for (int i = 0; i < properties.Length; i++)
        {
            var p = properties[i];
            var val = p.GetValue(columns) ?? throw new ArgumentNullException($"Property '{p.Name}' cannot be null.");
            cols[i] = (p.Name, val);
        }

        return FromColumns(cols);
    }
    /// <summary>
    /// Create DataFrame from explicitly named columns.
    /// No reflection used. Best performance.
    /// </summary>
    public static DataFrame FromColumns(params (string Name, object Data)[] columns)
    {
        if (columns == null || columns.Length == 0)
            return new DataFrame(); // Return empty DF

        var seriesList = new List<Series>(columns.Length);

        foreach (var (name, val) in columns.AsSpan())
        {
            if (val == null)
                throw new ArgumentNullException($"Column '{name}' data cannot be null.");
            
            try 
            {
                
                if (val is System.Array arr)
                {
                    var handle = SeriesFactory.Create(name, arr);
                    seriesList.Add(new Series(handle));
                }
                else 
                {
                    seriesList.Add(Series.From(name, (dynamic)val));
                }
            }
            catch (Exception ex)
            {
                 throw new NotSupportedException($"Column '{name}' has unsupported data type: {val.GetType().Name}.", ex);
            }
        }

        return new DataFrame([.. seriesList]);
    }
    // =========================================================
    // Internal Column Buffers
    // =========================================================
    private interface IColumnBuffer
    {
        void Add(object? val);
        Series ToSeries(string name);
    }

    /// <summary>
    /// Strong Type Column Buffer 
    /// </summary>
    /// <typeparam name="TCol">Column Type(Example: int, string, DateTime?)</typeparam>
    private sealed class ColumnBuffer<TCol>(int capacity) : IColumnBuffer
    {
        private readonly List<TCol?> _data = new(capacity);

        public void Add(object? val)
        {
            _data.Add((TCol?)val);
        }

        public Series ToSeries(string name)
            => Series.From(name, _data.ToArray());
        
    }

    private static class ColumnBufferFactory
    {
        public static IColumnBuffer Create(Type propType, int capacity)
        {
            
            Type targetType = propType;

            if (propType.IsValueType && Nullable.GetUnderlyingType(propType) == null)
            {
                targetType = typeof(Nullable<>).MakeGenericType(propType);
            }

            var bufferType = typeof(ColumnBuffer<>).MakeGenericType(targetType);
            return (IColumnBuffer)Activator.CreateInstance(bufferType, capacity)!;
        }
    }
    /// <summary>
    /// Create a DataFrame from a list of Series.
    /// </summary>
    public DataFrame(params Series[] series)
    {
        if (series == null || series.Length == 0)
        {
            Handle = PolarsWrapper.DataFrameNew([]);
            return;
        }

        var handles = series.Select(s => s.Handle).ToArray();
        
        Handle = PolarsWrapper.DataFrameNew(handles);
    }
    /// <summary>
    /// Create a DataFrame from a collection of Series.
    /// <para>
    /// This is the functional equivalent of the constructor <c>new DataFrame(series)</c>, 
    /// provided for consistency with other <c>From...</c> factory methods.
    /// </para>
    /// </summary>
    /// <param name="series">The series to combine into a DataFrame.</param>
    /// <returns>A new DataFrame containing the provided series.</returns>
    /// <exception cref="ArgumentException">Thrown if series have different lengths.</exception>
    /// <example>
    /// <code>
    /// var s1 = new Series("id", new[] { 1, 2, 3 });
    /// var s2 = new Series("name", new[] { "Alice", "Bob", "Charlie" });
    /// 
    /// var df = DataFrame.FromSeries(s1, s2);
    /// Console.WriteLine(df);
    /// </code>
    /// </example>
    public static DataFrame FromSeries(params Series[] series)
        => new(series);
    /// <summary>
    /// Create a DataFrame from a collection of Series.
    /// <para>
    /// This is the syntax sugar for FromSeries().
    /// </para>
    /// </summary>
    public static DataFrame FromColumns(params Series[] series)
        => new(series);
    /// <summary>
    /// Create a DataFrame from a collection of Series.
    /// </summary>
    /// <param name="series">The series to combine.</param>
    public static DataFrame FromSeries(IEnumerable<Series> series)
        => new([.. series]);
    /// <summary>
    /// Stream C# objects into Polars.
    /// </summary>
    public static DataFrame FromEnumerable<T>(IEnumerable<T> data, int batchSize = 100_000, Schema? providedSchema = null)
    {
        var schema = providedSchema ?? ArrowConverter.GetSchemaFromType<T>();
        var stream = data.ToArrowBatches(batchSize);

        var handle = ArrowStreamInterop.ImportEager(stream, schema); 

        if (handle.IsInvalid) return From(Enumerable.Empty<T>());
        return new DataFrame(handle);
    }
    /// <summary>
    /// Stream data into Polars using Arrow C Stream Interface.
    /// This method supports datasets larger than available RAM by streaming chunks directly to Polars.
    /// </summary>
    /// <param name="data">Source data collection</param>
    /// <param name="batchSize">Rows per chunk (default 100,000)</param>
    /// <param name="providedSchema">Stream schema provided by user</param>
    [Obsolete("Renamed to FromEnumerable")]
    public static DataFrame FromArrowStream<T>(IEnumerable<T> data, int batchSize = 100_000,Schema? providedSchema = null)
    {
        var schema = providedSchema ?? ArrowConverter.GetSchemaFromType<T>();
        var stream = data.ToArrowBatches(batchSize);

        var handle = ArrowStreamInterop.ImportEager(stream,schema);

        if (handle.IsInvalid)
        {
            return From(Enumerable.Empty<T>());
        }

        return new DataFrame(handle);
    }

    /// <summary>   
    /// Safely consume any foreign Arrow C Stream (Strict mode).
    /// Adapts physical memory layouts (e.g., Utf8View) automatically.
    /// </summary>
    public static DataFrame FromArrowStream(IArrowArrayStream stream)
    {
        ArgumentNullException.ThrowIfNull(stream);

        var handle = ArrowStreamInterop.ImportForeignStream(stream);
        
        var df = new DataFrame(handle);
        df.HoldResource(stream); 
        return df;
    }
    /// <summary>
    /// Export DataFrame to Arrow C Data Interface Stream.
    /// Supports zero-copy and lazy chunked reading.
    /// </summary>
    /// <param name="columnIndices">Optional column indices to prune the export (Projection Pushdown).</param>
    /// <param name="seed">Optional random seed to shuffle chunks.</param>
    /// <returns>Standard IArrowArrayStream</returns>
    public IArrowArrayStream ToArrowStream(ReadOnlySpan<int> columnIndices = default, ulong? seed = null)
        => ArrowStreamInterop.ExportToStream(Handle, columnIndices, seed);
    
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

    // ==========================================
    // Conversion to Lazy
    // ==========================================

    /// <summary>
    /// Convert the DataFrame into a LazyFrame.
    /// This allows building a query plan and optimizing execution.
    /// </summary>
    public LazyFrame Lazy()
    {
        var clonedHandle = PolarsWrapper.CloneDataFrame(Handle);
        
        var lfHandle = PolarsWrapper.DataFrameToLazy(clonedHandle);
        
        return new LazyFrame(lfHandle);
    }
    // ==========================================
    // Series Access (Column Selection)
    // ==========================================

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

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    /// <summary>
    /// Generates an HTML representation of the DataFrame.
    /// Useful for rendering in Jupyter/Polyglot Notebooks.
    /// </summary>
    /// <param name="limit">Max rows to display (default 10).</param>
    public string ToHtml(int limit = 10)
    {
        int rowsToShow = (int)Math.Min(Height, limit);
        using var previewDf = this.Head(rowsToShow);
        
        using var batch = ArrowFfiBridge.ExportDataFrame(previewDf.Handle);
        var schema = batch.Schema;

        var sb = new StringBuilder();
        
        sb.Append(@"
<style>
.pl-dataframe { font-family: 'Consolas', 'Monaco', monospace; font-size: 13px; border-collapse: collapse; border: 1px solid #e0e0e0; }
.pl-dataframe th { background-color: #f0f0f0; font-weight: bold; text-align: left; padding: 6px 12px; border-bottom: 2px solid #ccc; }
.pl-dataframe td { padding: 6px 12px; border-bottom: 1px solid #f0f0f0; white-space: pre; color: #333; }
.pl-dataframe tr:nth-child(even) { background-color: #f9f9f9; }
.pl-dataframe tr:hover { background-color: #f1f1f1; }
.pl-dtype { font-size: 10px; color: #999; display: block; margin-top: 2px; font-weight: normal; }
.pl-null { color: #d0d0d0; font-style: italic; }
.pl-dim { font-family: sans-serif; font-size: 12px; color: #666; margin-bottom: 8px; }
</style>");

        // Dimensions Info
        sb.Append($"<div class='pl-dim'>Polars DataFrame: <b>({Height} rows, {Width} columns)</b></div>");
        
        sb.Append("<div style='overflow-x:auto'><table class='pl-dataframe'>");

        // Header (From Arrow Schema)
        sb.Append("<thead><tr>");
        foreach (var field in schema.FieldsList)
        {
            var colName = System.Net.WebUtility.HtmlEncode(field.Name);
            var colType = field.DataType.Name; 
            
            sb.Append($"<th>{colName}<span class='pl-dtype'>{colType}</span></th>");
        }
        sb.Append("</tr></thead>");

        // Body (From Arrow Batch)
        sb.Append("<tbody>");
        
        int rowCount = batch.Length;
        int colCount = batch.ColumnCount;

        for (int r = 0; r < rowCount; r++)
        {
            sb.Append("<tr>");
            for (int c = 0; c < colCount; c++)
            {
                var colArray = batch.Column(c);
                var field = schema.GetFieldByIndex(c);
                
                string valStr = colArray.FormatValue(r);

                if (valStr != "null")
                {
                    if (field.DataType.TypeId == ArrowTypeId.Double)
                    {
                        if (double.TryParse(valStr, out double d)) 
                            valStr = d.ToString("G10");
                    }
                    else if (field.DataType.TypeId == ArrowTypeId.Float)
                    {
                        if (float.TryParse(valStr, out float f)) 
                            valStr = f.ToString("G7");
                    }
                }

                if (valStr == "null")
                {
                    sb.Append("<td class='pl-null'>null</td>");
                }
                else
                {
                    if (valStr.Length > 100) valStr = valStr[..97] + "...";
                    sb.Append($"<td>{System.Net.WebUtility.HtmlEncode(valStr)}</td>");
                }
            }
            sb.Append("</tr>");
        }
        
        // Footer for hidden rows
        long remaining = Height - rowsToShow;
        if (remaining > 0)
        {
             sb.Append($"<tr><td colspan='{colCount}' style='text-align:center; font-style:italic; color:#999; padding: 10px'>... {remaining} more rows ...</td></tr>");
        }

        sb.Append("</tbody></table></div>");

        return sb.ToString();
    }
}